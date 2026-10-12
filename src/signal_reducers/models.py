"""Immutable contracts. Mutable JSON exists only as detached boundary copies."""

from __future__ import annotations

import base64
import json
import math
import re
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

REDUCER_IDS = ("conversation", "telemetry", "simulated_video")
MAX_PAYLOAD = 200_000
ThinkingLevel = Literal["none", "some", "extra"]
Resolution = Literal["unassessed", "no_action_required", "unresolved", "resolved"]
Consumption = Literal["pending", "presented", "acknowledged"]


class ContractError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def byte_limit(text: str, maximum: int, code: str = "text_too_large") -> str:
    if len(text.encode("utf-8")) > maximum:
        raise ContractError(code)
    return text


def validate_json(value: Any, depth: int = 0, max_depth: int = 32) -> None:
    if depth > max_depth:
        raise ContractError("json_depth_exceeded")
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ContractError("nonfinite_number")
    elif isinstance(value, dict):
        if not all(isinstance(k, str) for k in value):
            raise ContractError("non_string_key")
        for child in value.values():
            validate_json(child, depth + 1, max_depth)
    elif isinstance(value, (list, tuple)):
        for child in value:
            validate_json(child, depth + 1, max_depth)
    else:
        raise ContractError("not_json")


def canonical(value: Any, *, maximum: int | None = None, depth: int = 32) -> bytes:
    validate_json(value, max_depth=depth)
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    if maximum is not None and len(raw) > maximum:
        raise ContractError("json_capacity_exceeded")
    return raw


def stream_id(value: str, maximum: int = 128) -> str:
    if not re.fullmatch(r"[A-Za-z0-9._:-]{1," + str(maximum) + "}", value):
        raise ContractError("invalid_stream_id")
    return value


class Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @field_validator("*", mode="after", check_fields=False)
    @classmethod
    def aware_timestamps(cls, value):
        if isinstance(value, datetime):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ContractError("timezone_required")
            return value.astimezone(timezone.utc)
        return value

    def to_dict(self) -> dict[str, Any]:
        return public(self)


def public(value: Any) -> Any:
    if isinstance(value, BaseModel):
        out = {}
        for name in type(value).model_fields:
            child = getattr(value, name)
            if name.endswith("_json") and isinstance(child, bytes):
                out[name[:-5]] = json.loads(child)
            elif name == "payload_bytes":
                if value.payload_encoding == "json":
                    out["payload"] = json.loads(child)
                else:
                    out["payload_base64"] = base64.b64encode(child).decode("ascii")
            else:
                out[name] = public(child)
        return out
    if isinstance(value, dict):
        return {str(k): public(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [public(v) for v in value]
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value


class SignalInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: UUID = Field(default_factory=uuid4)
    schema_version: Literal["1.0"] = "1.0"
    source: str | None = None
    source_event_id: str | None = None
    source_seq: int | None = Field(default=None, strict=True, ge=0, le=2**63 - 1)
    modality: Literal[
        "text", "image", "audio", "video", "structured", "binary", "mixed"
    ]
    payload: Any
    mimetype: str | None = None
    occurred_at: datetime | None = None
    received_at: datetime | None = None

    @field_validator("source", "source_event_id")
    @classmethod
    def bounded_source(cls, value):
        return byte_limit(value, 256) if value is not None else None

    @field_validator("mimetype")
    @classmethod
    def bounded_mime(cls, value):
        return byte_limit(value, 128) if value is not None else None

    @field_validator("occurred_at")
    @classmethod
    def aware_time(cls, value):
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ContractError("timezone_required")
        return value.astimezone(timezone.utc) if value else None

    @field_validator("payload")
    @classmethod
    def payload_valid(cls, value):
        if isinstance(value, bytes):
            if len(value) > MAX_PAYLOAD:
                raise ContractError("payload_too_large")
        else:
            canonical(value, maximum=MAX_PAYLOAD)
        return value

    def accept(self) -> AcceptedSignal:
        data = self.model_dump(exclude={"payload", "received_at"})
        binary = isinstance(self.payload, bytes)
        return AcceptedSignal(
            **data,
            received_at=utcnow(),
            payload_encoding="binary" if binary else "json",
            payload_bytes=self.payload if binary else canonical(self.payload),
        )


class AcceptedSignal(Frozen):
    id: UUID
    schema_version: Literal["1.0"] = "1.0"
    source: str | None
    source_event_id: str | None
    source_seq: int | None = Field(strict=True, ge=0, le=2**63 - 1)
    modality: Literal[
        "text", "image", "audio", "video", "structured", "binary", "mixed"
    ]
    mimetype: str | None
    occurred_at: datetime | None
    received_at: datetime
    payload_encoding: Literal["json", "binary"]
    payload_bytes: bytes = Field(strict=True)

    @field_validator("source", "source_event_id")
    @classmethod
    def bounded_source(cls, value):
        return byte_limit(value, 256) if value is not None else None

    @field_validator("mimetype")
    @classmethod
    def bounded_mime(cls, value):
        return byte_limit(value, 128) if value is not None else None

    @model_validator(mode="after")
    def validated_payload(self):
        if len(self.payload_bytes) > MAX_PAYLOAD:
            raise ContractError("payload_too_large")
        if self.payload_encoding == "json":
            normalized = canonical(json.loads(self.payload_bytes), maximum=MAX_PAYLOAD)
            object.__setattr__(self, "payload_bytes", normalized)
        return self

    @property
    def payload(self) -> Any:
        return (
            json.loads(self.payload_bytes)
            if self.payload_encoding == "json"
            else self.payload_bytes
        )


class Delivery(Frozen):
    receipt_id: UUID = Field(default_factory=uuid4)
    signal: AcceptedSignal


class Receipt(Frozen):
    receipt_id: UUID
    signal_id: UUID


class EvidenceRef(Frozen):
    receipt_id: UUID
    signal_id: UUID


class Observation(Frozen):
    id: UUID = Field(default_factory=uuid4)
    reducer_id: Literal["conversation", "telemetry", "simulated_video"]
    subject_id: str
    subject_seq: int = Field(strict=True, ge=1)
    kind: Literal["snapshot", "event", "revision"]
    finality: Literal["provisional", "committed", "corrected", "retracted"]
    code: str
    content: str
    facts_json: bytes = Field(default=b"{}", strict=True)
    revises: UUID | None = None
    confidence: float | None = Field(default=None, strict=True, ge=0, le=1)
    evidence_refs: tuple[EvidenceRef, ...] = ()
    occurred_at: datetime | None = None
    produced_at: datetime = Field(default_factory=utcnow)

    @property
    def facts(self) -> dict:
        return json.loads(self.facts_json)

    @model_validator(mode="after")
    def bounded(self):
        byte_limit(self.subject_id, 256)
        if not self.subject_id:
            raise ContractError("empty_subject")
        byte_limit(self.code, 64)
        byte_limit(self.content, 1024)
        facts = self.facts
        if not isinstance(facts, dict):
            raise ContractError("facts_not_object")
        object.__setattr__(self, "facts_json", canonical(facts, maximum=512, depth=8))
        if len(self.evidence_refs) > 32:
            raise ContractError("evidence_capacity_exceeded")
        revision = self.kind == "revision"
        if revision != (self.revises is not None) or revision != (
            self.finality in ("corrected", "retracted")
        ):
            raise ContractError("invalid_revision")
        if self.finality == "retracted" and self.facts:
            raise ContractError("retraction_has_facts")
        if self.confidence is not None and not math.isfinite(self.confidence):
            raise ContractError("nonfinite_number")
        return self


class Record(Frozen):
    observation: Observation
    accepted_at: datetime = Field(default_factory=utcnow)
    accepted_version: int = Field(strict=True, ge=0)
    consumption: Consumption = "pending"
    resolution: Resolution = "unassessed"
    first_batch_id: UUID | None = None
    last_batch_id: UUID | None = None
    presentation_count: int = Field(default=0, strict=True, ge=0)
    reason: str = ""

    @property
    def id(self) -> UUID:
        return self.observation.id


class Snapshot(Frozen):
    observation_id: UUID
    version: int = Field(strict=True, ge=0)
    last_seen_at: datetime
    last_seen_seq: int | None = Field(default=None, strict=True, ge=0, le=2**63 - 1)


class Activity(Frozen):
    reducer_id: Literal["conversation", "telemetry", "simulated_video"]
    status: Literal["idle", "buffering", "processing", "failed"] = "idle"
    buffered_signals: int = Field(default=0, strict=True, ge=0)
    buffered_bytes: int = Field(default=0, strict=True, ge=0)
    active_processing: int = Field(default=0, strict=True, ge=0, le=1)
    last_input_at: datetime | None = None
    last_output_at: datetime | None = None
    last_error_code: str | None = None


class Disposition(Frozen):
    observation_id: UUID
    acknowledge: bool = Field(default=False, strict=True)
    resolution: Literal["no_action_required", "unresolved", "resolved"] | None = None
    reason: str = ""
    basis_observation_ids: tuple[UUID, ...] = ()
    basis_result_ids: tuple[UUID, ...] = ()

    @field_validator("reason")
    @classmethod
    def reason_limit(cls, value):
        return byte_limit(value, 512)


class Operation(Frozen):
    observation_ids: tuple[UUID, ...]
    tool_name: str | None = None
    arguments_json: bytes | None = None
    escalation_reason: str | None = None
    thinking_level: ThinkingLevel | None = None

    @property
    def arguments(self):
        return json.loads(self.arguments_json) if self.arguments_json else None


class GateDecision(Frozen):
    batch_id: UUID
    action: Literal["wait", "acknowledge", "act", "escalate"]
    reason: str = ""
    dispositions: tuple[Disposition, ...] = ()
    reviewed_result_ids: tuple[UUID, ...] = ()
    operation: Operation | None = None

    @model_validator(mode="after")
    def shape(self):
        byte_limit(self.reason, 512)
        if len(self.dispositions) > 16 or len(
            set(d.observation_id for d in self.dispositions)
        ) != len(self.dispositions):
            raise ContractError("invalid_dispositions")
        if self.action in ("wait", "acknowledge"):
            if self.operation is not None:
                raise ContractError("unexpected_operation")
        else:
            op = self.operation
            if (
                op is None
                or not 1 <= len(op.observation_ids) <= 8
                or len(set(op.observation_ids)) != len(op.observation_ids)
            ):
                raise ContractError("invalid_operation")
            if self.action == "escalate":
                if (
                    not op.escalation_reason
                    or op.thinking_level is None
                    or op.tool_name is not None
                    or op.arguments_json is not None
                ):
                    raise ContractError("invalid_escalation")
                byte_limit(op.escalation_reason, 512)
            elif (
                op.tool_name != "get_processing_status"
                or op.thinking_level is not None
                or op.escalation_reason is not None
            ):
                raise ContractError("invalid_tool")
            else:
                args = op.arguments
                if (
                    not isinstance(args, dict)
                    or set(args) != {"reducer_id"}
                    or args["reducer_id"] not in (None, *REDUCER_IDS)
                ):
                    raise ContractError("invalid_arguments")
        return self


class OperationResult(Frozen):
    id: UUID = Field(default_factory=uuid4)
    operation_id: UUID
    type: Literal["tool", "system2"]
    batch_id: UUID
    target_ids: tuple[UUID, ...]
    status: Literal["success", "partial", "failed", "timeout", "cancelled"]
    content: str = ""
    data_json: bytes = Field(default=b"{}", strict=True)
    evidence_ids: tuple[UUID, ...] = ()
    error_code: str | None = None
    started_at: datetime = Field(default_factory=utcnow)
    completed_at: datetime = Field(default_factory=utcnow)
    context_version: int = Field(strict=True, ge=0)
    applicable: bool = Field(default=True, strict=True)

    @property
    def data(self):
        return json.loads(self.data_json)

    @model_validator(mode="after")
    def bounded(self):
        byte_limit(self.content, 1024)
        data = self.data
        if not isinstance(data, dict):
            raise ContractError("data_not_object")
        object.__setattr__(self, "data_json", canonical(data, maximum=512, depth=8))
        if (
            not 1 <= len(self.target_ids) <= 8
            or len(set(self.target_ids)) != len(self.target_ids)
            or len(self.evidence_ids) > 8
            or len(set(self.evidence_ids)) != len(self.evidence_ids)
        ):
            raise ContractError("invalid_result_ids")
        return self


class ActiveOperation(Frozen):
    id: UUID = Field(default_factory=uuid4)
    batch_id: UUID
    action: Literal["act", "escalate"]
    target_ids: tuple[UUID, ...]
    initiator_id: UUID
    thinking_level: ThinkingLevel | None = None
    context_version: int = Field(strict=True, ge=0)
    started_at: datetime = Field(default_factory=utcnow)
    terminal: bool = Field(default=False, strict=True)


class Batch(Frozen):
    id: UUID = Field(default_factory=uuid4)
    run_id: UUID
    state_version: int = Field(strict=True, ge=0)
    input_generation: int = Field(strict=True, ge=0)
    created_at: datetime = Field(default_factory=utcnow)
    records: tuple[Record, ...]
    new_ids: tuple[UUID, ...] = ()
    unresolved_ids: tuple[UUID, ...] = ()
    snapshots: tuple[Snapshot, ...] = ()
    activity: tuple[Activity, ...] = ()
    operations: tuple[ActiveOperation, ...] = ()
    results: tuple[OperationResult, ...] = ()
    attempted_request_ids: tuple[UUID, ...] = ()
    deferred_json: bytes = Field(default=b"{}", strict=True)

    @model_validator(mode="after")
    def bounded_deferred(self):
        captured = {record.id for record in self.records}
        if len(set(self.attempted_request_ids)) != len(
            self.attempted_request_ids
        ) or any(
            request_id not in captured for request_id in self.attempted_request_ids
        ):
            raise ContractError("invalid_attempted_request_ids")
        deferred = self.deferred
        if not isinstance(deferred, dict):
            raise ContractError("deferred_not_object")
        object.__setattr__(
            self, "deferred_json", canonical(deferred, maximum=512, depth=8)
        )
        return self

    @property
    def deferred(self):
        return json.loads(self.deferred_json)


class EscalationRequest(Frozen):
    id: UUID
    batch_id: UUID
    run_id: UUID
    context_version: int = Field(strict=True, ge=0)
    target_ids: tuple[UUID, ...]
    reason: str
    thinking_level: ThinkingLevel
    context_json: bytes = Field(strict=True)
    requested_at: datetime = Field(default_factory=utcnow)
    deadline_at: datetime

    @property
    def context(self):
        return json.loads(self.context_json)

    @model_validator(mode="after")
    def bounded(self):
        byte_limit(self.reason, 512)
        if len(self.context_json) > 24_000:
            raise ContractError("context_capacity_exceeded")
        context = self.context
        if not isinstance(context, dict):
            raise ContractError("context_not_object")
        object.__setattr__(self, "context_json", canonical(context, maximum=24_000))
        if (
            not self.reason
            or not 1 <= len(self.target_ids) <= 8
            or len(set(self.target_ids)) != len(self.target_ids)
        ):
            raise ContractError("invalid_escalation")
        if self.deadline_at <= self.requested_at:
            raise ContractError("invalid_deadline")
        return self


ObservationUpdate = Observation
ObservationBatch = Batch
ReducerActivity = Activity
