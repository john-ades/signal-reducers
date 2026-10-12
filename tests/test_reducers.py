from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from signal_reducers.config import Config
from signal_reducers.models import ContractError, Delivery, SignalInput, canonical
from signal_reducers.reducers import VIDEO_MIME, ReducerWorker


class Writer:
    def __init__(self):
        self.groups = []
        self.activities = []
        self.refreshes = []
        self.reject = False

    async def emit(self, observations):
        if self.reject:
            raise ContractError("state_capacity_exceeded")
        self.groups.append(observations)
        return tuple(obs.id for obs in observations)

    async def activity(self, activity):
        self.activities.append(activity)

    async def refresh(self, subject_id, source_seq, received_at):
        self.refreshes.append((subject_id, source_seq, received_at))

    @property
    def observations(self):
        return [obs for group in self.groups for obs in group]


class Model:
    def __init__(self, delay=0, fail=False):
        self.calls = []
        self.delay = delay
        self.fail = fail

    async def summarize(self, reducer_id, context):
        self.calls.append((reducer_id, context))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail:
            raise ContractError("provider_failure")
        return {
            "summary": "Bounded source summary",
            "intent": "question" if reducer_id == "conversation" else None,
        }


def delivery(seq, payload, *, source="sensor", modality="structured", mimetype=None):
    return Delivery(
        signal=SignalInput(
            source=source,
            source_seq=seq,
            modality=modality,
            mimetype=mimetype,
            payload=payload,
        ).accept()
    )


def telemetry(
    seq, *, status="ok", error_rate=0, source="sensor", service="worker", note=""
):
    return delivery(
        seq,
        {
            "type": "telemetry",
            "service": service,
            "status": status,
            "metrics": {"error_rate": error_rate, "queue_depth": 0},
            "note": note,
        },
        source=source,
    )


def conversation(seq, event, text="", *, source="chat", message_id="m1"):
    return delivery(
        seq,
        {
            "type": "conversation",
            "message_id": message_id,
            "event": event,
            "text": text,
        },
        source=source,
    )


def video(seq, *, scene="stalled", revision=None, source="camera", service="worker"):
    return delivery(
        seq,
        canonical({"service": service, "scene": scene, "revision": revision}),
        source=source,
        modality="video",
        mimetype=VIDEO_MIME,
    )


def worker(reducer, **overrides):
    defaults = dict(
        draft_flush=0.02, draft_expiry=0.08, video_window=0.04, video_delay=0
    )
    config = Config(**(defaults | overrides))
    writer, model = Writer(), Model()
    instance = ReducerWorker(reducer, writer, model, config)
    instance.start()
    return instance, writer, model


async def feed(instance, *items):
    for item in items:
        assert await instance.offer(item)
    await instance.join()


async def eventually(predicate, deadline=0.5):
    async with asyncio.timeout(deadline):
        while not predicate():
            await asyncio.sleep(0.005)


async def test_identical_telemetry_burst_suppresses_remote_calls_and_tracks_freshness():
    instance, writer, model = worker("telemetry")
    try:
        await feed(instance, *(telemetry(seq) for seq in range(1, 101)))
        assert len(model.calls) == 1
        assert len(writer.observations) == 1
        assert len(writer.refreshes) == 99
        assert writer.refreshes[-1][:2] == ("sensor/worker", 100)
        assert writer.observations[0].facts["source_seq"] == 1
        assert instance.counters["suppressed"] == 99
        assert writer.activities[-1].status == "idle"
        assert writer.activities[-1].buffered_bytes == 0
    finally:
        await instance.close()


async def test_health_transition_is_atomic_and_preserves_reported_facts():
    instance, writer, model = worker("telemetry")
    try:
        await feed(
            instance, telemetry(1), telemetry(3, status="degraded", error_rate=0.2)
        )
        assert len(writer.groups) == 2
        snapshot, event = writer.groups[1]
        assert [snapshot.subject_seq, event.subject_seq] == [2, 3]
        assert snapshot.facts["status"] == "degraded"
        assert snapshot.facts["coverage_incomplete"] is True
        assert event.facts["previous_status"] == "ok"
        assert event.code == "telemetry.health_changed"
        assert model.calls[1][1]["facts"]["metrics"]["error_rate"] == 0.2
        await feed(instance, telemetry(2, status="down"))
        assert len(model.calls) == 2
        assert instance.counters["late"] == 1
    finally:
        await instance.close()


async def test_invalid_telemetry_never_uses_model():
    instance, writer, model = worker("telemetry")
    try:
        invalid = telemetry(1, error_rate=True)
        await feed(instance, invalid)
        assert not model.calls and not writer.observations
        assert writer.activities[-1].status == "failed"
        await feed(instance, telemetry(2))
        assert len(writer.observations) == 1
        assert writer.activities[-1].status == "idle"
    finally:
        await instance.close()


async def test_rejected_output_does_not_advance_subject_sequence_or_mutate_prior_snapshot():
    instance, writer, model = worker("telemetry")
    try:
        await feed(instance, telemetry(1))
        original = writer.observations[0]
        writer.reject = True
        await feed(instance, telemetry(2, status="down"))
        assert writer.observations == [original]
        writer.reject = False
        await feed(instance, telemetry(3, status="down"))
        assert writer.groups[-1][0].subject_seq == 2
        assert writer.groups[-1][1].subject_seq == 3
    finally:
        await instance.close()


async def test_complete_fragments_submit_once_with_full_content_and_provenance():
    instance, writer, model = worker("conversation")
    try:
        first, second, submit = (
            conversation(1, "replace", "Explain "),
            conversation(2, "append", "why?"),
            conversation(3, "submit"),
        )
        await feed(instance, first, second, submit)
        assert len(model.calls) == 1
        event, snapshot = writer.groups[0]
        assert event.content == snapshot.content == "Explain why?"
        assert event.facts["intent"] == "question"
        assert event.facts["submitted"] is True
        assert [ref.receipt_id for ref in event.evidence_refs] == [
            first.receipt_id,
            second.receipt_id,
            submit.receipt_id,
        ]
        await feed(
            instance, conversation(4, "replace", "Reopen?"), conversation(5, "submit")
        )
        assert len(writer.observations) == 2
        assert len(model.calls) == 1
        assert instance.counters["submitted_replay"] == 2
    finally:
        await instance.close()


async def test_conversation_gap_cannot_commit_until_full_replacement():
    instance, writer, model = worker("conversation")
    try:
        await feed(
            instance,
            conversation(1, "replace", "A"),
            conversation(3, "append", "C"),
            conversation(4, "submit"),
        )
        assert not writer.observations and not model.calls
        assert instance.counters["conversation_incomplete"] == 1
        await feed(
            instance, conversation(5, "replace", "ABC"), conversation(6, "submit")
        )
        assert writer.observations[0].content == "ABC"
        assert writer.observations[0].facts["coverage_incomplete"] is False
    finally:
        await instance.close()


async def test_initial_append_is_incomplete_and_oversize_clears_assembly():
    instance, writer, model = worker("conversation")
    try:
        await feed(instance, conversation(1, "append", "A"), conversation(2, "submit"))
        assert not writer.observations
        await feed(
            instance,
            conversation(3, "replace", "a" * 1024),
            conversation(4, "append", "b"),
        )
        assert instance.counters["conversation_too_large"] == 1
        await asyncio.sleep(0.1)
        assert not writer.observations and not model.calls
        assert writer.activities[-1].buffered_signals == 0
    finally:
        await instance.close()


async def test_draft_timer_emits_provisional_once_and_expiry_never_submits():
    instance, writer, model = worker("conversation")
    try:
        await feed(instance, conversation(1, "replace", "Unsubmitted question"))
        await eventually(lambda: len(writer.observations) == 1)
        draft = writer.observations[0]
        assert draft.code == "conversation.draft" and draft.finality == "provisional"
        assert draft.facts["submitted"] is False
        await eventually(lambda: instance.counters["expired"] == 1)
        assert len(writer.observations) == 1 and len(model.calls) == 1
        assert writer.activities[-1].buffered_signals == 0
    finally:
        await instance.close()


async def test_bare_text_is_always_incomplete_and_reserved_id_is_rejected():
    instance, writer, model = worker("conversation")
    try:
        await feed(instance, delivery(1, "Hello", source="chat", modality="text"))
        await eventually(lambda: bool(writer.observations))
        assert writer.observations[0].facts["coverage_incomplete"] is True
        await feed(
            instance, conversation(2, "replace", "Spoof", message_id="unsubmitted")
        )
        assert instance.counters["invalid_message_id"] == 1
        assert not any(obs.finality == "committed" for obs in writer.observations)
    finally:
        await instance.close()


async def test_three_packets_required_and_correction_retraction_target_live_leaves():
    instance, writer, model = worker("simulated_video")
    try:
        await feed(instance, video(1), video(2))
        assert not model.calls and not writer.observations
        assert writer.activities[-1].status == "buffering"
        await feed(instance, video(3))
        original = writer.groups[0]
        assert len(original) == 2
        assert original[0].facts["scene"] == "stalled"
        assert original[0].content.startswith("Simulated scene:")
        await feed(instance, *(video(seq, scene="reflection") for seq in range(4, 7)))
        corrected = writer.groups[1]
        assert [obs.revises for obs in corrected] == [obs.id for obs in original]
        assert all(
            obs.finality == "corrected" and obs.facts["scene"] == "clear"
            for obs in corrected
        )
        await feed(
            instance,
            *(video(seq, scene="clear", revision="retract") for seq in range(7, 10)),
        )
        retracted = writer.groups[2]
        assert [obs.revises for obs in retracted] == [obs.id for obs in corrected]
        assert all(obs.finality == "retracted" and obs.facts == {} for obs in retracted)
        assert len(model.calls) == 3
    finally:
        await instance.close()


async def test_video_gap_expiry_and_missing_revision_never_invent_scene():
    instance, writer, model = worker("simulated_video", signal_max_age=0.04)
    try:
        await feed(instance, video(1), video(3))
        assert writer.observations[0].code == "simulation.incomplete"
        assert "scene" not in writer.observations[0].facts
        await eventually(lambda: len(writer.observations) == 2)
        assert all(obs.finality == "provisional" for obs in writer.observations)
        await feed(instance, *(video(seq, scene="reflection") for seq in range(4, 7)))
        assert instance.counters["revision_target_missing"] == 1
        assert not model.calls
    finally:
        await instance.close()


async def test_slow_delivery_preserves_a_complete_one_second_intake_window():
    instance, writer, model = worker(
        "simulated_video", video_window=1, signal_max_age=2
    )
    try:
        # All three packets were admitted together, then a serial router delivers
        # each after a delay longer than the semantic input window.
        packets = tuple(video(seq) for seq in range(1, 4))
        assert (
            packets[-1].signal.received_at - packets[0].signal.received_at
        ).total_seconds() < 1
        await feed(instance, packets[0])
        await asyncio.sleep(0.55)
        await feed(instance, packets[1])
        await asyncio.sleep(0.55)
        await feed(instance, packets[2])
        assert len(writer.groups) == 1 and len(writer.groups[0]) == 2
        assert writer.groups[0][0].facts["first_seq"] == 1
        assert writer.groups[0][0].facts["last_seq"] == 3
        assert writer.groups[0][0].facts["coverage_incomplete"] is False
        assert len(model.calls) == 1
        assert instance.counters["incomplete"] == 0
    finally:
        await instance.close()


@pytest.mark.parametrize("timestamp_order", ["ascending", "descending"])
async def test_actual_intake_span_exceeding_one_second_is_rejected_even_on_fast_delivery(
    timestamp_order,
):
    instance, writer, model = worker(
        "simulated_video", video_window=1, signal_max_age=2
    )
    try:
        first = video(1)
        second = video(2)
        if timestamp_order == "ascending":
            first = first.model_copy(
                update={
                    "signal": first.signal.model_copy(
                        update={
                            "received_at": second.signal.received_at
                            - timedelta(seconds=1.001)
                        }
                    )
                }
            )
        else:
            second = second.model_copy(
                update={
                    "signal": second.signal.model_copy(
                        update={
                            "received_at": first.signal.received_at
                            - timedelta(seconds=1.001)
                        }
                    )
                }
            )
        await feed(instance, first, second)
        assert len(writer.observations) == 1
        incomplete = writer.observations[0]
        assert incomplete.code == "simulation.incomplete"
        assert incomplete.facts["error_code"] == "simulation_input_window_exceeded"
        assert "scene" not in incomplete.facts
        assert not model.calls
    finally:
        await instance.close()


async def test_delivery_expiry_is_bounded_separate_from_the_input_window():
    instance, writer, model = worker(
        "simulated_video", video_window=1, signal_max_age=0.03
    )
    try:
        await feed(instance, video(1))
        await eventually(lambda: bool(writer.observations))
        assert (
            writer.observations[0].facts["error_code"] == "simulation_delivery_expired"
        )
        assert writer.activities[-1].buffered_signals == 0
        assert not model.calls
        expired = video(2)
        expired = expired.model_copy(
            update={
                "signal": expired.signal.model_copy(
                    update={
                        "received_at": expired.signal.received_at - timedelta(seconds=1)
                    }
                )
            }
        )
        await feed(instance, expired)
        assert len(writer.observations) == 2
        assert (
            writer.observations[1].facts["error_code"] == "simulation_delivery_expired"
        )
        assert writer.activities[-1].buffered_signals == 0
    finally:
        await instance.close()


async def test_future_intake_timestamp_cannot_extend_delivery_wait_past_signal_max_age():
    instance, writer, model = worker(
        "simulated_video", video_window=1, signal_max_age=0.03
    )
    try:
        packet = video(1)
        packet = packet.model_copy(
            update={
                "signal": packet.signal.model_copy(
                    update={
                        "received_at": packet.signal.received_at + timedelta(seconds=10)
                    }
                )
            }
        )
        await feed(instance, packet)
        await eventually(lambda: bool(writer.observations))
        assert (
            writer.observations[0].facts["error_code"] == "simulation_delivery_expired"
        )
        assert writer.activities[-1].buffered_signals == 0
        assert not model.calls
    finally:
        await instance.close()


async def test_complete_video_waits_default_processing_delay_before_model_entry():
    class TimedModel(Model):
        entered_at = None

        async def summarize(self, reducer_id, context):
            self.entered_at = asyncio.get_running_loop().time()
            return await super().summarize(reducer_id, context)

    delay = Config().video_delay
    assert delay == 0.5
    instance, writer, _ = worker(
        "simulated_video", video_delay=delay, video_window=1, signal_max_age=2
    )
    model = TimedModel()
    instance.model = model
    try:
        packets = tuple(video(seq) for seq in range(1, 4))
        await feed(instance, *packets[:2])
        started = asyncio.get_running_loop().time()
        assert await instance.offer(packets[2])
        await asyncio.sleep(0.1)
        assert model.entered_at is None
        assert any(
            item.status == "processing" and item.active_processing == 1
            for item in writer.activities
        )
        await instance.join()
        assert model.entered_at - started >= 0.49
        assert len(model.calls) == 1
        assert len(writer.groups[0]) == 2
    finally:
        await instance.close()


async def test_exact_one_second_intake_boundary_is_complete():
    instance, writer, model = worker(
        "simulated_video", video_window=1, signal_max_age=2
    )
    try:
        packets = tuple(video(seq) for seq in range(1, 4))
        latest = packets[-1].signal.received_at
        packets = tuple(
            item.model_copy(
                update={
                    "signal": item.signal.model_copy(
                        update={"received_at": latest - timedelta(seconds=offset)}
                    )
                }
            )
            for item, offset in zip(packets, (1, 0.5, 0), strict=True)
        )
        assert (
            packets[-1].signal.received_at - packets[0].signal.received_at
        ).total_seconds() == 1
        await feed(instance, *packets)
        assert len(model.calls) == 1
        assert len(writer.groups) == 1 and len(writer.groups[0]) == 2
        assert writer.groups[0][0].facts["coverage_incomplete"] is False
        assert instance.counters["incomplete"] == 0
    finally:
        await instance.close()


@pytest.mark.parametrize(
    "changes", [{"service": "another"}, {"scene": "clear"}, {"revision": "retract"}]
)
async def test_packet_disagreement_emits_incomplete_without_claiming_a_scene(changes):
    instance, writer, model = worker(
        "simulated_video", video_window=1, signal_max_age=2
    )
    try:
        first = video(1)
        await feed(instance, first, video(2, **changes), video(3, **changes))
        assert len(writer.observations) == 1
        incomplete = writer.observations[0]
        assert incomplete.code == "simulation.incomplete"
        assert incomplete.finality == "provisional"
        assert incomplete.facts == {
            "coverage_incomplete": True,
            "error_code": "simulation_packet_conflict",
        }
        assert [item.receipt_id for item in incomplete.evidence_refs] == [
            first.receipt_id
        ]
        assert not model.calls
        assert not any(item.kind == "snapshot" for item in writer.observations)
    finally:
        await instance.close()


async def test_slow_remote_worker_does_not_block_another_reducer_and_has_bounded_admission():
    slow, slow_writer, slow_model = worker(
        "telemetry", mailbox_capacity=2, reducer_timeout=0.03
    )
    fast, fast_writer, fast_model = worker("conversation")
    slow_model.delay = 0.2
    try:
        assert await slow.offer(telemetry(1))
        await eventually(lambda: bool(slow_model.calls))
        assert await slow.offer(telemetry(2, status="degraded"))
        assert not await slow.offer(telemetry(3, status="down"))
        await feed(
            fast, conversation(1, "replace", "Question?"), conversation(2, "submit")
        )
        assert len(fast_writer.observations) == 2
        await slow.join()
        assert slow.counters["reducer_timeout"] == 2
        assert not slow_writer.observations
        assert slow.available
    finally:
        await slow.close()
        await fast.close()


async def test_idle_timer_does_not_call_models_and_subject_capacity_is_enforced():
    instance, writer, model = worker("telemetry", max_subjects=1)
    try:
        await asyncio.sleep(0.03)
        assert not model.calls
        await feed(instance, telemetry(1), telemetry(1, service="second"))
        assert len(model.calls) == 1
        assert instance.counters["reducer_subject_capacity_exceeded"] == 1
        assert instance.available
    finally:
        await instance.close()


async def test_fragment_byte_budget_includes_assembled_inputs():
    instance, writer, model = worker("conversation", mailbox_capacity=2)
    try:
        await feed(
            instance, conversation(1, "replace", "A"), conversation(2, "append", "B")
        )
        assert writer.activities[-1].buffered_signals == 2
        assert not await instance.offer(conversation(3, "append", "C"))
        await eventually(lambda: instance.counters["expired"] == 1)
        assert await instance.offer(conversation(4, "replace", "Full text"))
        await instance.join()
    finally:
        await instance.close()
