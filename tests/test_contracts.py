"""Boundary tests for immutable contracts and untrusted signal framing."""

import json
import struct
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from signal_reducers.models import (
    Activity,
    Batch,
    ContractError,
    Delivery,
    EscalationRequest,
    EvidenceRef,
    GateDecision,
    Observation,
    Operation,
    OperationResult,
    Record,
    SignalInput,
    Snapshot,
    canonical,
    public,
    utcnow,
)
from signal_reducers.transport import decode, encode


def observation(**changes):
    values = dict(
        reducer_id="telemetry",
        subject_id="source/worker",
        subject_seq=1,
        kind="snapshot",
        finality="committed",
        code="telemetry.status",
        content="Reported healthy",
        facts_json=canonical({"status": "ok", "nested": {"value": 1}}),
    )
    values.update(changes)
    return Observation(**values)


@pytest.mark.parametrize("invalid", ["outside_capture", "duplicate"])
def test_batch_attempted_requests_must_be_unique_captured_records(invalid):
    obs = observation()
    attempted = (uuid4(),) if invalid == "outside_capture" else (obs.id, obs.id)
    with pytest.raises(ValueError, match="invalid_attempted_request_ids"):
        Batch(
            run_id=uuid4(),
            state_version=1,
            input_generation=1,
            records=(Record(observation=obs, accepted_version=1),),
            attempted_request_ids=attempted,
        )


def test_batch_attempt_metadata_is_immutable_and_boundary_copies_are_detached():
    obs = observation()
    value = Batch(
        run_id=uuid4(),
        state_version=1,
        input_generation=1,
        records=(Record(observation=obs, accepted_version=1),),
        attempted_request_ids=(obs.id,),
    )
    with pytest.raises(ValueError, match="frozen"):
        value.attempted_request_ids = ()
    boundary = public(value)
    assert boundary["attempted_request_ids"] == [str(obs.id)]
    boundary["attempted_request_ids"].clear()
    assert value.attempted_request_ids == (obs.id,)


def frame(header, body=b"{}"):
    raw = json.dumps(header, separators=(",", ":")).encode()
    return b"SR1\0" + struct.pack(">I", len(raw)) + raw + body


def header():
    delivery = Delivery(signal=SignalInput(modality="structured", payload={}).accept())
    value = public(delivery.signal)
    value.pop("payload")
    value["receipt_id"] = str(delivery.receipt_id)
    return value


@pytest.mark.parametrize("sequence", [True, False, -1, 2**63, 1.5, "1"])
def test_sequence_is_a_bounded_strict_integer(sequence):
    with pytest.raises(ValueError):
        SignalInput(modality="structured", payload={}, source_seq=sequence)


@pytest.mark.parametrize(
    "extra",
    [
        {"destinations": ["telemetry"]},
        {"finality": "committed"},
        {"schema_version": "2.0"},
    ],
)
def test_producer_cannot_supply_routing_or_lifecycle_authority(extra):
    with pytest.raises(ValueError):
        SignalInput(modality="structured", payload={}, **extra)


def test_exact_payload_boundary_counts_utf8_and_json_quotes():
    assert (
        len(SignalInput(modality="text", payload="é" * 99_999).accept().payload_bytes)
        == 200_000
    )
    assert (
        len(
            SignalInput(modality="binary", payload=b"x" * 200_000)
            .accept()
            .payload_bytes
        )
        == 200_000
    )
    for payload in ("é" * 100_000, b"x" * 200_001):
        with pytest.raises(ValueError):
            SignalInput(modality="binary", payload=payload)


def test_admission_revalidates_a_mutated_external_payload_and_does_not_share_values():
    data = {"nested": {"value": 1}}
    item = SignalInput(modality="structured", payload=data)
    accepted = item.accept()
    data["nested"]["value"] = 2
    item.payload["nested"]["later"] = "x" * 200_001
    assert accepted.payload == {"nested": {"value": 1}}
    with pytest.raises(ValueError):
        item.accept()
    item.payload = {"bad": float("nan")}
    with pytest.raises(ValueError):
        item.accept()


def test_input_timestamps_are_aware_and_receipt_is_owned_by_intake():
    before = utcnow()
    source_time = datetime(2026, 1, 1, 10, tzinfo=timezone(timedelta(hours=-4)))
    accepted = SignalInput(
        modality="text",
        payload="hello",
        occurred_at=source_time,
        received_at=before + timedelta(days=30),
    ).accept()
    assert before <= accepted.received_at <= utcnow()
    assert accepted.occurred_at == datetime(2026, 1, 1, 14, tzinfo=timezone.utc)
    assert accepted.occurred_at.tzinfo == timezone.utc
    with pytest.raises(ValueError):
        SignalInput(modality="text", payload="hello", occurred_at=datetime(2026, 1, 1))


@pytest.mark.parametrize(
    "field,value",
    [
        ("source", "é" * 129),
        ("source_event_id", "é" * 129),
        ("mimetype", "é" * 65),
        ("payload_bytes", b"not json"),
        ("payload_bytes", b"x" * 200_001),
        ("received_at", datetime(2026, 1, 1)),
    ],
)
def test_accepted_contract_cannot_bypass_envelope_validation(field, value):
    original = SignalInput(modality="structured", payload={}).accept()
    with pytest.raises(ValueError):
        type(original)(**{**original.model_dump(), field: value})


def test_nested_facts_and_batches_are_detached_and_frozen():
    obs = observation(facts_json=b' { "nested" : { "value" : 1 }, "status" : "ok" } ')
    record = Record(observation=obs, accepted_version=1)
    batch = Batch(
        run_id=uuid4(),
        state_version=1,
        input_generation=1,
        records=[record],
        new_ids=[obs.id],
    )
    obs.facts["nested"]["value"] = 2
    copied = batch.to_dict()
    copied["records"][0]["observation"]["facts"]["nested"]["value"] = 3
    assert batch.records[0].observation.facts["nested"]["value"] == 1
    assert isinstance(batch.records, tuple) and isinstance(batch.new_ids, tuple)
    assert obs.facts_json == canonical(obs.facts)
    with pytest.raises(ValueError):
        obs.content = "Changed"
    with pytest.raises(ValueError):
        record.resolution = "resolved"


@pytest.mark.parametrize(
    "changes",
    [
        {"confidence": True},
        {"confidence": float("nan")},
        {"content": "é" * 513},
        {"subject_seq": True},
        {"subject_seq": 0},
        {"facts_json": canonical({"oversize": "é" * 256})},
        {"facts_json": b"[]"},
        {"produced_at": datetime(2026, 1, 1)},
        {"lifecycle": "resolved"},
    ],
)
def test_observation_bounds_cannot_be_bypassed(changes):
    with pytest.raises(ValueError):
        observation(**changes)


def test_facts_depth_and_evidence_capacity_are_enforced():
    nested = {}
    for _ in range(9):
        nested = {"child": nested}
    with pytest.raises(ValueError):
        observation(facts_json=canonical(nested))
    refs = tuple(EvidenceRef(receipt_id=uuid4(), signal_id=uuid4()) for _ in range(33))
    with pytest.raises(ValueError):
        observation(evidence_refs=refs)
    assert len(observation(evidence_refs=refs[:32]).evidence_refs) == 32


@pytest.mark.parametrize(
    "changes",
    [
        {"kind": "revision"},
        {"finality": "corrected"},
        {"revises": uuid4()},
        {"kind": "revision", "revises": uuid4()},
        {"kind": "revision", "finality": "retracted", "revises": uuid4()},
    ],
)
def test_revision_shape_requires_matching_finality_and_empty_retraction(changes):
    with pytest.raises(ValueError):
        observation(**changes)
    valid = observation(
        kind="revision", finality="retracted", revises=uuid4(), facts_json=b"{}"
    )
    assert valid.facts == {}


@pytest.mark.parametrize(
    "factory",
    [
        lambda: Activity(reducer_id="telemetry", buffered_signals=True),
        lambda: Record(observation=observation(), accepted_version=True),
        lambda: Record(
            observation=observation(), accepted_version=1, presentation_count=True
        ),
        lambda: Snapshot(observation_id=uuid4(), version=True, last_seen_at=utcnow()),
        lambda: Batch(
            run_id=uuid4(), state_version=True, input_generation=1, records=()
        ),
    ],
)
def test_internal_counters_do_not_coerce_booleans(factory):
    with pytest.raises(ValueError):
        factory()


def test_thinking_level_belongs_to_escalations_only():
    target = uuid4()
    for level in ("none", "some", "extra"):
        decision = GateDecision(
            batch_id=uuid4(),
            action="escalate",
            operation=Operation(
                observation_ids=(target,),
                escalation_reason="Answer submitted question",
                thinking_level=level,
            ),
        )
        assert decision.operation.thinking_level == level
    with pytest.raises(ValueError):
        GateDecision(
            batch_id=uuid4(),
            action="escalate",
            operation=Operation(
                observation_ids=(target,), escalation_reason="Answer question"
            ),
        )
    with pytest.raises(ValueError):
        GateDecision(
            batch_id=uuid4(),
            action="act",
            operation=Operation(
                observation_ids=(target,),
                tool_name="get_processing_status",
                arguments_json=canonical({"reducer_id": None}),
                thinking_level="some",
            ),
        )
    with pytest.raises(ValueError):
        GateDecision(
            batch_id=uuid4(),
            action="wait",
            operation=Operation(observation_ids=(target,)),
        )


def test_escalation_context_is_bounded_immutable_and_has_a_valid_deadline():
    def build(**changes):
        values = dict(
            id=uuid4(),
            batch_id=uuid4(),
            run_id=uuid4(),
            context_version=1,
            target_ids=(uuid4(),),
            reason="Answer request",
            thinking_level="none",
            context_json=b' { "nested": { "value": 1 } } ',
            deadline_at=utcnow() + timedelta(seconds=90),
        )
        values.update(changes)
        return EscalationRequest(**values)

    request = build()
    request.context["nested"]["value"] = 2
    assert request.context["nested"]["value"] == 1
    assert request.context_json == canonical(request.context)
    for changes in (
        {"context_json": b"[]"},
        {"context_json": b"{}" + b" " * 24_000},
        {"deadline_at": utcnow() - timedelta(seconds=1)},
        {"target_ids": ()},
        {"context_version": True},
        {"reason": ""},
    ):
        with pytest.raises(ValueError):
            build(**changes)


def test_result_data_is_bounded_and_ids_cannot_repeat():
    target = uuid4()

    def build(**changes):
        values = dict(
            operation_id=uuid4(),
            type="system2",
            batch_id=uuid4(),
            target_ids=(target,),
            status="success",
            context_version=1,
            data_json=b' { "nested": { "value": 1 } } ',
        )
        values.update(changes)
        return OperationResult(**values)

    result = build()
    result.data["nested"]["value"] = 2
    assert result.data["nested"]["value"] == 1
    assert result.data_json == canonical(result.data)
    for changes in (
        {"target_ids": (target, target)},
        {"evidence_ids": (target, target)},
        {"data_json": b"[]"},
        {"applicable": "true"},
        {"context_version": True},
    ):
        with pytest.raises(ValueError):
            build(**changes)


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"SR0\0" + b"\0" * 4,
        b"SR1\0" + struct.pack(">I", 8193),
        b"SR1\0" + struct.pack(">I", 100) + b"{}",
        frame(None),
        frame(1),
        frame([]),
    ],
)
def test_corrupt_frames_are_safe_contract_errors(raw):
    with pytest.raises(ContractError):
        decode(raw)


@pytest.mark.parametrize(
    "updates",
    [
        {"receipt_id": "invalid"},
        {"id": "invalid"},
        {"source_seq": True},
        {"received_at": "2026-01-01T00:00:00"},
        {"payload_encoding": "unknown"},
        {"destinations": ["telemetry"]},
        {"schema_version": "2.0"},
    ],
)
def test_untrusted_frame_metadata_is_validated(updates):
    with pytest.raises(ContractError):
        decode(frame({**header(), **updates}))


def test_wire_json_is_canonical_and_received_time_is_utc():
    metadata = header()
    metadata["received_at"] = "2026-01-01T10:00:00-04:00"
    decoded = decode(frame(metadata, b' { "b" : [1], "a" : true } '))
    assert decoded.signal.payload_bytes == b'{"a":true,"b":[1]}'
    assert decoded.signal.received_at == datetime(2026, 1, 1, 14, tzinfo=timezone.utc)
    assert decoded.signal.received_at.tzinfo == timezone.utc


def test_binary_frame_preserves_bytes_and_independent_receipt_identity():
    signal = SignalInput(modality="binary", payload=bytes(range(256))).accept()
    first, second = Delivery(signal=signal), Delivery(signal=signal)
    assert first.receipt_id != second.receipt_id
    assert decode(encode(first)) == first
    assert decode(encode(second)) == second
    assert decode(encode(first)).signal.payload_bytes == bytes(range(256))
