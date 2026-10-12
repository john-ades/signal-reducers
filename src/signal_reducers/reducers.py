"""Independent, bounded reducers. Models summarize; runtime code owns facts and identity."""

from __future__ import annotations

import asyncio
import json
import math
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from .config import Config
from .models import (
    REDUCER_IDS,
    Activity,
    ContractError,
    Delivery,
    EvidenceRef,
    Observation,
    byte_limit,
    canonical,
    stream_id,
    utcnow,
)

VIDEO_MIME = "application/vnd.signal-reducers.sim-video+json"


@dataclass
class _Telemetry:
    highwater: int = -1
    coverage_incomplete: bool = False
    value: bytes | None = None
    status: str | None = None


@dataclass
class _Draft:
    source: str
    message_id: str
    text: str
    complete: bool
    source_seq: int
    last_input: float
    evidence: list[EvidenceRef] = field(default_factory=list)
    reservations: list[UUID] = field(default_factory=list)
    occurred_at: datetime | None = None
    last_emitted: tuple[str, bool] | None = None


@dataclass
class _Segment:
    source: str
    service: str
    scene: str
    revision: str | None
    intake_min: datetime
    intake_max: datetime
    delivery_deadline: float
    deliveries: list[Delivery] = field(default_factory=list)


@dataclass
class _VideoSubject:
    scene: str | None = None
    stalled_snapshot: UUID | None = None
    stalled_event: UUID | None = None


class ReducerWorker:
    """One serial worker, with nonblocking admission and timer-driven assembly.

    ``writer`` is bound to this reducer and implements ``emit``, ``activity`` and
    ``refresh``. ``model.summarize`` is the only remote inference boundary.
    Reservations include queued, active and assembled deliveries, so a blocked
    model cannot make another source's buffers grow beyond the mailbox budget.
    """

    def __init__(self, reducer_id: str, writer: Any, model: Any, config: Config):
        if reducer_id not in REDUCER_IDS:
            raise ValueError("unknown reducer")
        self.reducer_id = reducer_id
        self.writer = writer
        self.model = model
        self.config = config
        self.available = True
        self.counters: Counter = Counter()
        self._queue: asyncio.Queue[Delivery] = asyncio.Queue(config.mailbox_capacity)
        self._task: asyncio.Task | None = None
        self._closing = False
        self._reserved: dict[UUID, int] = {}
        self._active: set[UUID] = set()
        self._subjects: set[str] = set()
        self._subject_seq: dict[str, int] = {}
        self._telemetry: dict[str, _Telemetry] = {}
        self._drafts: dict[str, _Draft] = {}
        self._conversation_highwater: dict[str, int] = {}
        self._submitted: set[str] = set()
        self._segments: dict[str, _Segment] = {}
        self._video_highwater: dict[str, int] = {}
        self._video_subjects: dict[str, _VideoSubject] = {}
        self._status = "idle"
        self._last_input: datetime | None = None
        self._last_output: datetime | None = None
        self._last_error: str | None = None

    def start(self) -> None:
        if self._closing:
            raise RuntimeError("worker_closed")
        if self._task is None:
            self._task = asyncio.create_task(
                self._run(), name=f"reducer:{self.reducer_id}"
            )

    async def offer(self, delivery: Delivery) -> bool:
        size = len(delivery.signal.payload_bytes)
        if (
            not self.available
            or self._closing
            or delivery.receipt_id in self._reserved
            or len(self._reserved) >= self.config.mailbox_capacity
            or sum(self._reserved.values()) + size > self.config.mailbox_bytes
        ):
            self.counters["dropped"] += 1
            return False
        self._reserved[delivery.receipt_id] = size
        self._last_input = delivery.signal.received_at
        if self._status != "processing":
            self._status = "buffering"
        # Register activity before making the delivery visible to the worker.
        try:
            await self._publish_activity()
            self._queue.put_nowait(delivery)
        except BaseException:
            self._reserved.pop(delivery.receipt_id, None)
            raise
        self.counters["offered"] += 1
        return True

    async def join(self) -> None:
        """Wait for admitted items, not unsubmitted drafts or partial segments."""
        await self._queue.join()

    async def close(self) -> None:
        self._closing = True
        self.available = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        while not self._queue.empty():
            self._queue.get_nowait()
            self._queue.task_done()
            self.counters["cancelled"] += 1
        self._drafts.clear()
        self._segments.clear()
        self._reserved.clear()
        self._active.clear()
        self._status = "idle"
        await self._publish_activity()

    async def _publish_activity(self) -> None:
        buffered = {
            key: size for key, size in self._reserved.items() if key not in self._active
        }
        await self.writer.activity(
            Activity(
                reducer_id=self.reducer_id,
                status=self._status,
                buffered_signals=len(buffered),
                buffered_bytes=sum(buffered.values()),
                active_processing=int(bool(self._active)),
                last_input_at=self._last_input,
                last_output_at=self._last_output,
                last_error_code=self._last_error,
            )
        )

    def _release(self, ids) -> None:
        for receipt_id in ids:
            self._reserved.pop(receipt_id, None)
            self._active.discard(receipt_id)

    def _retained(self, receipt_id: UUID) -> bool:
        return any(
            receipt_id in draft.reservations for draft in self._drafts.values()
        ) or any(
            any(d.receipt_id == receipt_id for d in segment.deliveries)
            for segment in self._segments.values()
        )

    def _claim(self, subject: str) -> None:
        if subject not in self._subjects:
            if len(self._subjects) >= self.config.max_subjects:
                raise ContractError("reducer_subject_capacity_exceeded")
            self._subjects.add(subject)

    def _stream(self, delivery: Delivery) -> tuple[str, int]:
        signal = delivery.signal
        if signal.source is None or signal.source_seq is None:
            raise ContractError("stream_identity_required")
        return stream_id(signal.source), signal.source_seq

    @staticmethod
    def _evidence(deliveries) -> tuple[EvidenceRef, ...]:
        return tuple(
            EvidenceRef(receipt_id=d.receipt_id, signal_id=d.signal.id)
            for d in deliveries
        )[-32:]

    def _observation(self, subject: str, offset: int = 1, **values) -> Observation:
        return Observation(
            reducer_id=self.reducer_id,
            subject_id=subject,
            subject_seq=self._subject_seq.get(subject, 0) + offset,
            **values,
        )

    async def _emit(self, observations: tuple[Observation, ...]) -> tuple[UUID, ...]:
        ids = tuple(await self.writer.emit(observations))
        if len(ids) != len(observations) or ids != tuple(
            obs.id for obs in observations
        ):
            raise ContractError("invalid_writer_receipt")
        for obs in observations:
            self._subject_seq[obs.subject_id] = obs.subject_seq
        self._last_output = utcnow()
        self.counters["emitted"] += len(observations)
        return ids

    async def _summary(self, context: dict) -> dict:
        self._status = "processing"
        await self._publish_activity()
        self.counters["model_calls"] += 1
        response = await asyncio.wait_for(
            self.model.summarize(self.reducer_id, context), self.config.reducer_timeout
        )
        if not isinstance(response, dict) or not isinstance(
            response.get("summary"), str
        ):
            raise ContractError("invalid_model_summary")
        byte_limit(response["summary"], 1024, "model_summary_too_large")
        intent = response.get("intent")
        if intent is not None:
            if not isinstance(intent, str):
                raise ContractError("invalid_model_intent")
            byte_limit(intent, 64, "model_intent_too_large")
        return response

    async def _run(self) -> None:
        try:
            while not self._closing:
                await self._timers()
                deadline = self._next_deadline()
                try:
                    if deadline is None:
                        delivery = await self._queue.get()
                    else:
                        delay = max(0.001, deadline - asyncio.get_running_loop().time())
                        delivery = await asyncio.wait_for(self._queue.get(), delay)
                except TimeoutError:
                    continue
                self._active = {delivery.receipt_id}
                self._last_error = None
                try:
                    if self.reducer_id == "telemetry":
                        await self._reduce_telemetry(delivery)
                    elif self.reducer_id == "conversation":
                        await self._reduce_conversation(delivery)
                    else:
                        await self._reduce_video(delivery)
                    self.counters["processed"] += 1
                    self._status = (
                        "buffering" if self._retained(delivery.receipt_id) else "idle"
                    )
                except Exception as error:
                    self._clear_assembly(delivery.signal.source)
                    self._fail(error)
                finally:
                    self._active.clear()
                    if not self._retained(delivery.receipt_id):
                        self._release((delivery.receipt_id,))
                    if self._status != "failed":
                        self._status = "buffering" if self._reserved else "idle"
                    self._queue.task_done()
                    await self._publish_activity()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.available = False
            self._fail(error)
            try:
                await self._publish_activity()
            except Exception:
                pass

    def _fail(self, error: Exception) -> None:
        code = getattr(error, "code", None)
        if not isinstance(code, str) or not re.fullmatch(r"[A-Za-z0-9_:-]{1,64}", code):
            code = (
                "reducer_timeout"
                if isinstance(error, TimeoutError)
                else "reducer_item_failed"
            )
        self._last_error = code
        self._status = "failed"
        self.counters["failed"] += 1
        self.counters[code] += 1

    def _clear_assembly(self, source: str | None) -> None:
        if source in self._drafts:
            draft = self._drafts.pop(source)
            self._release(draft.reservations)
        if source in self._segments:
            segment = self._segments.pop(source)
            self._release(d.receipt_id for d in segment.deliveries)

    @staticmethod
    def _member(delivery: Delivery, reducer: str) -> dict:
        value = delivery.signal.payload
        if isinstance(value, dict) and value.get("type") == "bundle":
            value = value.get(reducer)
        if not isinstance(value, dict) or value.get("type") != reducer:
            raise ContractError("invalid_reducer_payload")
        return value

    async def _reduce_telemetry(self, delivery: Delivery) -> None:
        source, seq = self._stream(delivery)
        if (
            delivery.signal.modality not in ("structured", "mixed")
            or delivery.signal.payload_encoding != "json"
        ):
            raise ContractError("invalid_telemetry_modality")
        value = self._member(delivery, "telemetry")
        if set(value) - {"type", "service", "status", "metrics", "note"}:
            raise ContractError("invalid_telemetry_fields")
        service = value.get("service")
        if not isinstance(service, str):
            raise ContractError("invalid_service")
        stream_id(service, 64)
        status = value.get("status")
        metrics = value.get("metrics")
        note = value.get("note", "")
        if (
            status not in ("ok", "degraded", "down")
            or not isinstance(metrics, dict)
            or set(metrics) != {"error_rate", "queue_depth"}
        ):
            raise ContractError("invalid_telemetry_facts")
        rate, depth = metrics["error_rate"], metrics["queue_depth"]
        if (
            isinstance(rate, bool)
            or not isinstance(rate, (int, float))
            or not math.isfinite(rate)
            or not 0 <= rate <= 1
            or isinstance(depth, bool)
            or not isinstance(depth, int)
            or not 0 <= depth <= 2**63 - 1
            or not isinstance(note, str)
        ):
            raise ContractError("invalid_telemetry_facts")
        byte_limit(note, 256, "telemetry_note_too_large")
        subject = f"{source}/{service}"
        self._claim(subject)
        previous = self._telemetry.setdefault(subject, _Telemetry())
        if seq <= previous.highwater:
            self.counters["late"] += 1
            return
        previous.coverage_incomplete |= (
            previous.highwater >= 0 and seq != previous.highwater + 1
        )
        previous.highwater = seq
        key = canonical({"status": status, "metrics": metrics, "note": note})
        if key == previous.value:
            await self.writer.refresh(subject, seq, delivery.signal.received_at)
            self.counters["suppressed"] += 1
            return
        facts = {
            "service": service,
            "status": status,
            "metrics": metrics,
            "source_seq": seq,
            "coverage_incomplete": previous.coverage_incomplete,
        }
        summary = await self._summary(
            {"facts": facts, "note": note, "previous_status": previous.status}
        )
        snapshot = self._observation(
            subject,
            kind="snapshot",
            finality="committed",
            code="telemetry.status",
            content=summary["summary"],
            facts_json=canonical(facts),
            evidence_refs=self._evidence((delivery,)),
            occurred_at=delivery.signal.occurred_at,
        )
        output = (snapshot,)
        if previous.status is not None and status != previous.status:
            event = self._observation(
                subject,
                2,
                kind="event",
                finality="committed",
                code="telemetry.health_changed",
                content=summary["summary"],
                facts_json=canonical({**facts, "previous_status": previous.status}),
                evidence_refs=snapshot.evidence_refs,
                occurred_at=delivery.signal.occurred_at,
            )
            output += (event,)
        await self._emit(output)
        previous.value, previous.status = key, status

    async def _reduce_conversation(self, delivery: Delivery) -> None:
        source, seq = self._stream(delivery)
        signal = delivery.signal
        bare = (
            signal.modality == "text"
            and signal.payload_encoding == "json"
            and isinstance(signal.payload, str)
        )
        if bare:
            message_id, event, text = "unsubmitted", "append", signal.payload
        else:
            if (
                signal.modality not in ("structured", "mixed")
                or signal.payload_encoding != "json"
            ):
                raise ContractError("invalid_conversation_modality")
            value = self._member(delivery, "conversation")
            if set(value) != {"type", "message_id", "event", "text"}:
                raise ContractError("invalid_conversation_fields")
            message_id, event, text = value["message_id"], value["event"], value["text"]
            if not isinstance(message_id, str) or message_id == "unsubmitted":
                raise ContractError("invalid_message_id")
            stream_id(message_id, 64)
        if event not in ("replace", "append", "submit") or not isinstance(text, str):
            raise ContractError("invalid_conversation_fragment")
        byte_limit(text, 1024, "conversation_too_large")
        if event == "submit" and text:
            raise ContractError("submit_text_not_empty")
        highwater = self._conversation_highwater.get(source, -1)
        if seq <= highwater:
            self.counters["late"] += 1
            return
        subject = f"{source}/{message_id}"
        self._claim(subject)
        self._conversation_highwater[source] = seq
        if subject in self._submitted:
            self.counters["submitted_replay"] += 1
            return
        draft = self._drafts.get(source)
        if draft and draft.message_id != message_id:
            self._release(draft.reservations)
            self._drafts.pop(source)
            draft = None
            self.counters["abandoned"] += 1
        now = asyncio.get_running_loop().time()
        if draft is None:
            draft = _Draft(source, message_id, "", False, seq, now)
            self._drafts[source] = draft
        if highwater >= 0 and seq != highwater + 1:
            draft.complete = False
        if event == "replace":
            # Superseded raw fragments need no reservations or evidence.
            self._release(draft.reservations)
            draft.reservations.clear()
            draft.evidence.clear()
            draft.text = text
            draft.complete = not bare
        elif event == "append":
            draft.text += text
            byte_limit(draft.text, 1024, "conversation_too_large")
        draft.complete &= not bare
        draft.source_seq, draft.last_input, draft.occurred_at = (
            seq,
            now,
            signal.occurred_at,
        )
        draft.evidence.append(
            EvidenceRef(receipt_id=delivery.receipt_id, signal_id=signal.id)
        )
        draft.evidence = draft.evidence[-32:]
        draft.reservations.append(delivery.receipt_id)
        if event == "submit":
            if not draft.complete:
                raise ContractError("conversation_incomplete")
            await self._publish_draft(draft, submitted=True)
            self._submitted.add(subject)
            self._drafts.pop(source)
            self._release(draft.reservations)

    async def _publish_draft(self, draft: _Draft, *, submitted: bool) -> None:
        fingerprint = (draft.text, draft.complete)
        if not submitted and draft.last_emitted == fingerprint:
            return
        self._active = set(draft.reservations)
        response = await self._summary(
            {
                "text": draft.text,
                "submitted": submitted,
                "coverage_incomplete": not draft.complete,
                "message_id": draft.message_id,
            }
        )
        subject = f"{draft.source}/{draft.message_id}"
        facts = {
            "message_id": draft.message_id,
            "source_seq": draft.source_seq,
            "coverage_incomplete": not draft.complete,
            "submitted": submitted,
        }
        if response.get("intent") is not None:
            facts["intent"] = response["intent"]
        common = {
            "finality": "committed" if submitted else "provisional",
            "content": draft.text,
            "facts_json": canonical(facts),
            "evidence_refs": tuple(draft.evidence),
            "occurred_at": draft.occurred_at,
        }
        if submitted:
            event = self._observation(
                subject, kind="event", code="conversation.message", **common
            )
            snapshot = self._observation(
                subject,
                2,
                kind="snapshot",
                code="conversation.current_message",
                **common,
            )
            await self._emit((event, snapshot))
        else:
            snapshot = self._observation(
                subject, kind="snapshot", code="conversation.draft", **common
            )
            await self._emit((snapshot,))
        draft.last_emitted = fingerprint
        self._active.clear()

    async def _reduce_video(self, delivery: Delivery) -> None:
        source, seq = self._stream(delivery)
        signal = delivery.signal
        if (
            signal.modality != "video"
            or signal.payload_encoding != "binary"
            or signal.mimetype != VIDEO_MIME
        ):
            raise ContractError("invalid_simulation_modality")
        try:
            value = json.loads(signal.payload_bytes)
        except ValueError, UnicodeDecodeError:
            raise ContractError("invalid_simulation_packet") from None
        if not isinstance(value, dict) or set(value) != {
            "service",
            "scene",
            "revision",
        }:
            raise ContractError("invalid_simulation_packet")
        service, scene, revision = value["service"], value["scene"], value["revision"]
        if not isinstance(service, str):
            raise ContractError("invalid_service")
        stream_id(service, 64)
        if scene not in ("clear", "stalled", "reflection") or revision not in (
            None,
            "retract",
        ):
            raise ContractError("invalid_simulation_packet")
        self._claim(f"{source}/{service}")
        highwater = self._video_highwater.get(source, -1)
        if seq <= highwater:
            self.counters["late"] += 1
            return
        self._video_highwater[source] = seq
        now = asyncio.get_running_loop().time()
        now_utc = utcnow()
        intake = signal.received_at
        remaining = min(
            self.config.signal_max_age,
            max(
                0.0,
                (
                    intake + timedelta(seconds=self.config.signal_max_age) - now_utc
                ).total_seconds(),
            ),
        )
        segment = self._segments.get(source)
        if remaining <= 0:
            segment = self._segments.pop(source, None) or _Segment(
                source, service, scene, revision, intake, intake, now
            )
            segment.deliveries.append(delivery)
            await self._incomplete(segment, "simulation_delivery_expired")
            return
        if segment:
            reason = None
            if now >= segment.delivery_deadline:
                reason = "simulation_delivery_expired"
            elif seq != highwater + 1:
                reason = "simulation_sequence_gap"
            elif (service, scene, revision) != (
                segment.service,
                segment.scene,
                segment.revision,
            ):
                reason = "simulation_packet_conflict"
            elif (
                max(segment.intake_max, intake) - min(segment.intake_min, intake)
            ).total_seconds() > self.config.video_window:
                reason = "simulation_input_window_exceeded"
            if reason:
                await self._incomplete(segment, reason)
                self._segments.pop(source)
                segment = None
        if segment is None:
            segment = _Segment(
                source, service, scene, revision, intake, intake, now + remaining
            )
            self._segments[source] = segment
        else:
            segment.intake_min = min(segment.intake_min, intake)
            segment.intake_max = max(segment.intake_max, intake)
            # A reordered intake timestamp may shorten, never extend, the bound.
            segment.delivery_deadline = min(segment.delivery_deadline, now + remaining)
        segment.deliveries.append(delivery)
        if len(segment.deliveries) < 3:
            return
        self._segments.pop(source)
        self._active = {item.receipt_id for item in segment.deliveries}
        try:
            self._status = "processing"
            await self._publish_activity()
            await asyncio.sleep(self.config.video_delay)
            await self._publish_segment(segment)
        finally:
            self._release(item.receipt_id for item in segment.deliveries)

    async def _publish_segment(self, segment: _Segment) -> None:
        subject = f"{segment.source}/{segment.service}"
        previous = self._video_subjects.setdefault(subject, _VideoSubject())
        evidence = self._evidence(segment.deliveries)
        first_seq = segment.deliveries[0].signal.source_seq
        last_seq = segment.deliveries[-1].signal.source_seq
        occurred_at = segment.deliveries[-1].signal.occurred_at
        facts = {
            "service": segment.service,
            "scene": segment.scene,
            "first_seq": first_seq,
            "last_seq": last_seq,
            "coverage_incomplete": False,
        }
        if segment.revision == "retract" or segment.scene == "reflection":
            targets = (previous.stalled_snapshot, previous.stalled_event)
            if any(target is None for target in targets):
                raise ContractError("revision_target_missing")
            retract = segment.revision == "retract"
            replacement = {} if retract else {**facts, "scene": "clear"}
            summary = await self._summary(
                {
                    "facts": replacement,
                    "simulated": True,
                    "revision": "retract" if retract else "correct",
                }
            )
            output = tuple(
                self._observation(
                    subject,
                    index + 1,
                    kind="revision",
                    finality="retracted" if retract else "corrected",
                    code="simulation.scene_retracted"
                    if retract
                    else "simulation.scene_corrected",
                    content=self._simulation_content(summary["summary"]),
                    facts_json=canonical(replacement),
                    revises=target,
                    evidence_refs=evidence,
                    occurred_at=occurred_at,
                )
                for index, target in enumerate(targets)
            )
            ids = await self._emit(output)
            previous.stalled_snapshot, previous.stalled_event = (
                (None, None) if retract else ids
            )
            previous.scene = None if retract else "clear"
            return
        if segment.scene == previous.scene:
            await self.writer.refresh(
                subject, last_seq, segment.deliveries[-1].signal.received_at
            )
            self.counters["suppressed"] += 1
            return
        summary = await self._summary(
            {"facts": facts, "simulated": True, "previous_scene": previous.scene}
        )
        snapshot = self._observation(
            subject,
            kind="snapshot",
            finality="committed",
            code="simulation.scene",
            content=self._simulation_content(summary["summary"]),
            facts_json=canonical(facts),
            evidence_refs=evidence,
            occurred_at=occurred_at,
        )
        event = self._observation(
            subject,
            2,
            kind="event",
            finality="committed",
            code="simulation.scene_changed",
            content=snapshot.content,
            facts_json=canonical(facts),
            evidence_refs=evidence,
            occurred_at=occurred_at,
        )
        ids = await self._emit((snapshot, event))
        if segment.scene == "stalled":
            previous.stalled_snapshot, previous.stalled_event = ids
        previous.scene = segment.scene

    @staticmethod
    def _simulation_content(summary: str) -> str:
        content = "Simulated scene: " + summary
        return byte_limit(content, 1024, "model_summary_too_large")

    async def _incomplete(self, segment: _Segment, code: str) -> None:
        subject = f"{segment.source}/assembly"
        try:
            self._claim(subject)
            observation = self._observation(
                subject,
                kind="event",
                finality="provisional",
                code="simulation.incomplete",
                content="Incomplete simulated segment.",
                facts_json=canonical({"coverage_incomplete": True, "error_code": code}),
                evidence_refs=self._evidence(segment.deliveries),
            )
            await self._emit((observation,))
            self.counters["incomplete"] += 1
            self.counters[code] += 1
        finally:
            self._release(item.receipt_id for item in segment.deliveries)

    def _next_deadline(self) -> float | None:
        deadlines = [segment.delivery_deadline for segment in self._segments.values()]
        for draft in self._drafts.values():
            deadlines.append(draft.last_input + self.config.draft_expiry)
            if draft.last_emitted != (draft.text, draft.complete):
                deadlines.append(draft.last_input + self.config.draft_flush)
        return min(deadlines) if deadlines else None

    async def _timers(self) -> None:
        now = asyncio.get_running_loop().time()
        changed = False
        for source, segment in tuple(self._segments.items()):
            if now >= segment.delivery_deadline:
                self._segments.pop(source)
                try:
                    await self._incomplete(segment, "simulation_delivery_expired")
                    changed = True
                except Exception as error:
                    self._fail(error)
        for source, draft in tuple(self._drafts.items()):
            if now >= draft.last_input + self.config.draft_expiry:
                self._drafts.pop(source)
                self._release(draft.reservations)
                self.counters["expired"] += 1
                self._last_error = "conversation_draft_expired"
                changed = True
            elif (
                now >= draft.last_input + self.config.draft_flush
                and draft.last_emitted != (draft.text, draft.complete)
            ):
                try:
                    await self._publish_draft(draft, submitted=False)
                    changed = True
                except Exception as error:
                    self._drafts.pop(source)
                    self._release(draft.reservations)
                    self._fail(error)
                finally:
                    self._active.clear()
        if changed or self._status == "failed":
            if self._status != "failed":
                self._status = "buffering" if self._reserved else "idle"
            await self._publish_activity()
