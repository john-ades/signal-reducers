import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest

from signal_reducers.config import Config
from signal_reducers.gate import GateScheduler
from signal_reducers.models import (
    Activity,
    ContractError,
    Disposition,
    GateDecision,
    Observation,
    Operation,
    OperationResult,
    Record,
    canonical,
    utcnow,
)
from signal_reducers.state import ObservationStore


def observation(
    seq=1,
    *,
    reducer="telemetry",
    subject="source/service",
    kind="snapshot",
    code="telemetry.status",
    facts=None,
    finality="committed",
    revises=None,
    **kw,
):
    return Observation(
        reducer_id=reducer,
        subject_id=subject,
        subject_seq=seq,
        kind=kind,
        code=code,
        facts_json=canonical(
            facts if facts is not None else {"status": "degraded", "service": "service"}
        ),
        content=kw.pop("content", "bounded observation"),
        finality=finality,
        revises=revises,
        **kw,
    )


async def request(store, seq=1, message="m1", subject="conversation/source"):
    event = observation(
        seq,
        reducer="conversation",
        subject=subject,
        kind="event",
        code="conversation.message",
        facts={"message_id": message, "intent": "request"},
    )
    snapshot = observation(
        seq + 1,
        reducer="conversation",
        subject=subject,
        code="conversation.current_message",
        facts={"message_id": message},
    )
    await store.writer("conversation").emit((event, snapshot))
    return event


def escalation(batch, event, *others):
    ids = (event.id, *(o.id for o in others))
    return GateDecision(
        batch_id=batch.id,
        action="escalate",
        reason="diagnose",
        dispositions=tuple(
            Disposition(
                observation_id=i,
                acknowledge=True,
                resolution="unresolved",
                reason="needs analysis",
            )
            for i in ids
        ),
        operation=Operation(
            observation_ids=ids, escalation_reason="diagnose", thinking_level="some"
        ),
    )


async def complete(store, batch, operation, *, status="success", evidence=()):
    value = OperationResult(
        operation_id=operation.id,
        type="system2",
        batch_id=batch.id,
        target_ids=operation.target_ids,
        status=status,
        content="captured diagnosis",
        context_version=operation.context_version,
        evidence_ids=evidence,
    )
    await store.add_result(value)
    return value


async def test_writer_isolation_revision_transactions_and_projection():
    store = ObservationStore(uuid4(), Config())
    first = observation()
    writer = store.writer("telemetry")
    await writer.emit((first,))
    await writer.emit((first,))
    assert (await store.inspect())["state_version"] == 1
    with pytest.raises(ContractError, match="wrong_reducer_writer"):
        await store.writer("conversation").emit((first,))
    with pytest.raises(ContractError, match="conflicting_observation_id"):
        await writer.emit((first.model_copy(update={"content": "changed"}),))
    newer = observation(2, facts={"status": "ok"})
    await writer.emit((newer,))
    correction = observation(
        3,
        kind="revision",
        finality="corrected",
        revises=first.id,
        facts={"status": "down"},
    )
    await writer.emit((correction,))
    assert (await store.inspect())["snapshots"][0]["observation_id"] == str(newer.id)
    with pytest.raises(ContractError, match="stale_revision_target"):
        await writer.emit(
            (observation(4, kind="revision", finality="corrected", revises=first.id),)
        )
    retraction = observation(
        4, kind="revision", finality="retracted", revises=newer.id, facts={}
    )
    await writer.emit((retraction,))
    assert not (await store.inspect())["snapshots"]
    await writer.emit((observation(5),))
    assert (await store.inspect())["snapshots"][0]["version"] == 4


async def test_cross_area_and_group_failure_leave_no_mutation():
    store = ObservationStore(uuid4(), Config())
    first = observation()
    await store.writer("telemetry").emit((first,))
    invalid = observation(
        2, subject="other", kind="revision", finality="corrected", revises=first.id
    )
    before = await store.inspect()
    with pytest.raises(ContractError, match="cross_area_revision"):
        await store.writer("telemetry").emit((observation(2), invalid))
    assert await store.inspect() == before


@pytest.mark.parametrize("finality", ["corrected", "retracted"])
async def test_revision_depth_eight_is_allowed_and_ninth_group_is_atomic(finality):
    store = ObservationStore(uuid4(), Config())
    writer = store.writer("telemetry")
    leaf = observation()
    await writer.emit((leaf,))
    for depth in range(1, 9):
        revision = observation(
            depth + 1, kind="revision", finality="corrected", revises=leaf.id
        )
        await writer.emit((revision,))
        leaf = revision
    before = await store.inspect()
    assert len(before["records"]) == 9
    assert before["snapshots"][0]["observation_id"] == str(leaf.id)
    valid_neighbor = observation(10, kind="event")
    ninth = observation(
        11,
        kind="revision",
        finality=finality,
        revises=leaf.id,
        facts={} if finality == "retracted" else {"status": "degraded"},
    )
    with pytest.raises(ContractError, match="revision_depth_exceeded"):
        await writer.emit((valid_neighbor, ninth))
    assert await store.inspect() == before
    # The valid member of the rejected group did not consume its sequence or ID.
    await writer.emit((valid_neighbor,))
    assert len((await store.inspect())["records"]) == 10


async def test_batch_immutable_and_exact_presentation():
    store = ObservationStore(uuid4(), Config())
    first = observation()
    await store.writer("telemetry").emit((first,))
    batch = await store.create_batch()
    captured = canonical(batch.to_dict())
    await store.writer("telemetry").emit((observation(2),))
    detached = batch.to_dict()
    detached["records"][0]["observation"]["facts"]["status"] = "tampered"
    assert canonical(batch.to_dict()) == captured
    assert batch.records[0].consumption == "presented"
    assert batch.new_ids == (first.id,)
    bad = GateDecision(
        batch_id=batch.id,
        action="acknowledge",
        dispositions=(Disposition(observation_id=uuid4(), acknowledge=True),),
    )
    before = await store.inspect()
    with pytest.raises(ContractError, match="out_of_batch_reference"):
        await store.apply(batch, bad)
    assert await store.inspect() == before


async def test_operation_atomicity_stale_and_unrelated_changes():
    store = ObservationStore(uuid4(), Config())
    event = await request(store)
    batch = await store.create_batch()
    await store.writer("telemetry").emit((observation(),))
    decision = escalation(batch, event)
    op = await store.apply(batch, decision)
    assert op is not None
    assert await store.apply(batch, decision) is None
    await store.finish_batch(batch)
    await complete(store, batch, op)
    second = await store.create_batch()
    before = await store.inspect()
    with pytest.raises(ContractError, match="initiator_already_attempted"):
        await store.apply(second, escalation(second, event))
    assert await store.inspect() == before
    await store.finish_batch(second)
    new_event = await request(store, 3, "m2")
    third = await store.create_batch()
    await request(store, 5, "m3")
    before = await store.inspect()
    with pytest.raises(ContractError, match="stale_operation_target"):
        await store.apply(third, escalation(third, new_event))
    assert await store.inspect() == before


async def test_health_needs_recovery_and_question_needs_success():
    store = ObservationStore(uuid4(), Config())
    health = observation(kind="event", code="telemetry.health_changed")
    await store.writer("telemetry").emit((health,))
    event = await request(store)
    batch = await store.create_batch()
    op = await store.apply(batch, escalation(batch, event, health))
    await store.finish_batch(batch)
    result = await complete(store, batch, op, evidence=(health.id,))
    later = await store.create_batch()
    invalid = GateDecision(
        batch_id=later.id,
        action="acknowledge",
        dispositions=(
            Disposition(
                observation_id=event.id,
                resolution="resolved",
                reason="answered",
                basis_result_ids=(result.id,),
            ),
            Disposition(
                observation_id=health.id,
                resolution="resolved",
                reason="diagnosed",
                basis_result_ids=(result.id,),
            ),
        ),
    )
    before = await store.inspect()
    with pytest.raises(ContractError, match="health_requires_recovery_evidence"):
        await store.apply(later, invalid)
    assert await store.inspect() == before
    valid = invalid.model_copy(
        update={
            "dispositions": invalid.dispositions[:1],
            "reviewed_result_ids": (result.id,),
        }
    )
    await store.apply(later, valid)
    await store.finish_batch(later)
    state = await store.inspect()
    assert (
        next(r for r in state["records"] if r["observation"]["id"] == str(event.id))[
            "resolution"
        ]
        == "resolved"
    )
    assert (
        next(r for r in state["records"] if r["observation"]["id"] == str(health.id))[
            "resolution"
        ]
        == "unresolved"
    )
    recovered = observation(2, facts={"status": "ok"})
    await store.writer("telemetry").emit((recovered,))
    final = await store.create_batch()
    await store.apply(
        final,
        GateDecision(
            batch_id=final.id,
            action="acknowledge",
            dispositions=(
                Disposition(
                    observation_id=health.id,
                    resolution="resolved",
                    reason="recovered",
                    basis_observation_ids=(recovered.id,),
                ),
            ),
        ),
    )


@pytest.mark.parametrize("status", ["timeout", "failed", "partial"])
async def test_result_correlation_terminal_dedup_and_no_authority(status):
    store = ObservationStore(uuid4(), Config())
    event = await request(store)
    batch = await store.create_batch()
    op = await store.apply(batch, escalation(batch, event))
    await store.finish_batch(batch)
    wrong = OperationResult(
        operation_id=op.id,
        type="system2",
        batch_id=batch.id,
        target_ids=(event.id,),
        status="success",
        context_version=op.context_version,
        evidence_ids=(uuid4(),),
    )
    with pytest.raises(ContractError, match="invalid_result_correlation"):
        await store.add_result(wrong)
    result = await complete(store, batch, op, status=status)
    late = result.model_copy(update={"id": uuid4(), "status": "success"})
    assert not await store.add_result(late)
    state = await store.inspect()
    assert len(state["results"]) == 1
    assert state["results"][0]["status"] == status
    assert (
        next(r for r in state["records"] if r["observation"]["id"] == str(event.id))[
            "resolution"
        ]
        == "unresolved"
    )
    next_batch = await store.create_batch()
    before = await store.inspect()
    with pytest.raises(ContractError, match="request_requires_successful_result"):
        await store.apply(
            next_batch,
            GateDecision(
                batch_id=next_batch.id,
                action="acknowledge",
                dispositions=(
                    Disposition(
                        observation_id=event.id,
                        resolution="resolved",
                        reason="bad",
                        basis_result_ids=(result.id,),
                    ),
                ),
            ),
        )
    assert await store.inspect() == before


async def test_capacity_pins_atomic_and_detached_inspect():
    store = ObservationStore(uuid4(), Config(state_records=32))
    writer = store.writer("telemetry")
    await writer.emit(tuple(observation(i, kind="event") for i in range(1, 33)))
    before = await store.inspect()
    before["records"][0]["observation"]["facts"]["status"] = "detached"
    assert (await store.inspect())["records"][0]["observation"]["facts"][
        "status"
    ] == "degraded"
    with pytest.raises(ContractError, match="state_capacity_exceeded"):
        await writer.emit(
            (observation(33, kind="event"), observation(34, kind="event"))
        )
    assert len((await store.inspect())["records"]) == 32


async def test_batch_exact_request_bound_and_paginate_only_after_progress():
    store = ObservationStore(uuid4(), Config())
    await store.writer("telemetry").emit(
        tuple(observation(i, kind="event") for i in range(1, 21))
    )

    def size(batch):
        return 10_000 + len(batch.records) * 500

    batch = await store.create_batch(size)
    assert len(batch.records) == 4
    assert len(batch.new_ids) == 4
    assert batch.deferred["new"] == 16
    await store.consume_wake()
    await store.apply(
        batch,
        GateDecision(
            batch_id=batch.id,
            action="acknowledge",
            dispositions=tuple(
                Disposition(
                    observation_id=i, acknowledge=True, resolution="no_action_required"
                )
                for i in batch.new_ids
            ),
        ),
    )
    assert store.wake.is_set()
    await store.finish_batch(batch)
    _, page = await store.consume_wake()
    assert page
    second = await store.create_batch(size)
    assert not set(second.new_ids) & set(batch.new_ids)


async def eventually(predicate, seconds=1):
    async with asyncio.timeout(seconds):
        while not predicate():
            await asyncio.sleep(0.002)


async def test_scheduler_wait_and_failure_are_bounded_and_freshness_idle():
    config = Config(
        debounce=0.005, max_debounce=0.015, wait_followup=0.02, retry_delay=0.01
    )
    store = ObservationStore(uuid4(), config)

    class Model:
        calls = 0
        fail = False

        async def evaluate(self, batch):
            self.calls += 1
            if self.fail:
                raise ContractError("invalid_model_response")
            return GateDecision(batch_id=batch.id, action="wait")

    async def no_operation(*_):
        pytest.fail("unexpected operation")

    model = Model()
    scheduler = GateScheduler(store, model, config, no_operation)
    scheduler.start()
    try:
        await asyncio.sleep(0.03)
        assert model.calls == 0
        await store.writer("telemetry").activity(
            Activity(reducer_id="telemetry", status="processing", active_processing=1)
        )
        await store.writer("telemetry").activity(Activity(reducer_id="telemetry"))
        await asyncio.sleep(0.03)
        assert model.calls == 0
        first = observation()
        await store.writer("telemetry").emit((first,))
        await eventually(lambda: model.calls == 2)
        await asyncio.sleep(0.05)
        assert model.calls == 2
        await store.writer("telemetry").activity(
            Activity(reducer_id="telemetry", buffered_signals=5)
        )
        await store.writer("telemetry").refresh(
            first.subject_id, 10, utcnow() + timedelta(seconds=1)
        )
        await asyncio.sleep(0.03)
        assert model.calls == 2
        model.fail = True
        await store.recheck()
        await eventually(lambda: model.calls == 4)
        await asyncio.sleep(0.05)
        assert model.calls == 4
        assert scheduler.needs_attention
    finally:
        await scheduler.close()


async def test_scheduler_no_lost_wake_and_one_evaluation():
    config = Config(debounce=0.005, max_debounce=0.015)
    store = ObservationStore(uuid4(), config)
    entered, release = asyncio.Event(), asyncio.Event()

    class Model:
        batches = []
        active = peak = 0

        async def evaluate(self, batch):
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.batches.append(batch)
            if len(self.batches) == 1:
                entered.set()
                await release.wait()
            self.active -= 1
            return GateDecision(
                batch_id=batch.id,
                action="acknowledge",
                dispositions=tuple(
                    Disposition(
                        observation_id=i,
                        acknowledge=True,
                        resolution="no_action_required",
                    )
                    for i in batch.new_ids
                ),
            )

    async def no_operation(*_):
        pass

    model = Model()
    scheduler = GateScheduler(store, model, config, no_operation)
    scheduler.start()
    try:
        await store.writer("telemetry").emit((observation(),))
        await asyncio.wait_for(entered.wait(), 1)
        second = observation(2)
        await store.writer("telemetry").emit((second,))
        release.set()
        await eventually(lambda: len(model.batches) == 2)
        assert second.id not in {r.id for r in model.batches[0].records}
        assert second.id in {r.id for r in model.batches[1].records}
        assert model.peak == 1
    finally:
        await scheduler.close()


async def test_unresolved_pagination_requires_progress_and_does_not_spin():
    config = Config(debounce=0.003, max_debounce=0.01)
    store = ObservationStore(uuid4(), config)

    class Model:
        calls = 0

        def request_size(self, batch):
            return 10_000 + len(batch.records) * 500

        async def evaluate(self, batch):
            self.calls += 1
            return GateDecision(
                batch_id=batch.id,
                action="acknowledge",
                dispositions=tuple(
                    Disposition(
                        observation_id=r.id, acknowledge=True, resolution="unresolved"
                    )
                    for r in batch.records
                ),
            )

    async def no_operation(*_):
        pass

    model = Model()
    scheduler = GateScheduler(store, model, config, no_operation)
    scheduler.start()
    try:
        await store.writer("telemetry").emit(
            tuple(observation(i, kind="event") for i in range(1, 13))
        )
        async with asyncio.timeout(1):
            while any(
                r["consumption"] != "acknowledged"
                for r in (await store.inspect())["records"]
            ):
                await asyncio.sleep(0.005)
        await asyncio.sleep(0.03)
        calls = model.calls
        assert calls >= 3
        await asyncio.sleep(0.05)
        assert model.calls == calls
        assert all(
            r["resolution"] == "unresolved" for r in (await store.inspect())["records"]
        )
    finally:
        await scheduler.close()


def large_observation(seq, **kwargs):
    return observation(
        seq, content="x" * 1024, facts={"status": "ok", "padding": "y" * 350}, **kwargs
    )


async def test_state_byte_budget_rejects_complete_group_atomically():
    config = Config(state_records=1000, state_bytes=24_000)
    store = ObservationStore(uuid4(), config)
    sample = large_observation(1, kind="event")
    size = len(canonical(Record(observation=sample, accepted_version=1).to_dict()))
    count = config.state_bytes // size - 1
    initial = tuple(large_observation(i, kind="event") for i in range(1, count + 1))
    await store.writer("telemetry").emit(initial)
    before = await store.inspect()
    # Both outputs form one semantic transaction. Even the first is not retained
    # when their combined bytes exceed the available pinned-state capacity.
    with pytest.raises(ContractError, match="state_capacity_exceeded"):
        await store.writer("telemetry").emit(
            (
                large_observation(count + 1, kind="event"),
                large_observation(count + 2, kind="event"),
            )
        )
    after = await store.inspect()
    assert after["records"] == before["records"]
    assert after["snapshots"] == before["snapshots"]
    assert after["state_version"] == before["state_version"]
    assert len(after["records"]) < config.state_records
    assert after["diagnostics"]["last_error_code"] == "state_capacity_exceeded"


async def test_byte_budget_evicts_terminal_records_but_pins_current_and_outstanding():
    config = Config(state_records=1000, state_bytes=24_000)
    store = ObservationStore(uuid4(), config)
    size = len(
        canonical(
            Record(observation=large_observation(1), accepted_version=1).to_dict()
        )
    )
    count = config.state_bytes // size - 2  # reserve lifecycle-metadata space
    current = large_observation(1)
    terminal = tuple(large_observation(i, kind="event") for i in range(2, count))
    outstanding = large_observation(count, kind="event")
    await store.writer("telemetry").emit((current, *terminal, outstanding))
    batch = await store.create_batch(lambda _: 0)
    assert len(batch.new_ids) == count
    await store.apply(
        batch,
        GateDecision(
            batch_id=batch.id,
            action="acknowledge",
            dispositions=tuple(
                Disposition(
                    observation_id=i,
                    acknowledge=True,
                    resolution="unresolved"
                    if i == outstanding.id
                    else "no_action_required",
                )
                for i in batch.new_ids
            ),
        ),
    )
    await store.finish_batch(batch)
    before = await store.inspect()
    assert len(before["records"]) == count
    new = tuple(large_observation(i, kind="event") for i in range(count + 1, count + 5))
    await store.writer("telemetry").emit(new)
    after = await store.inspect()
    retained = {r["observation"]["id"] for r in after["records"]}
    assert str(current.id) in retained
    assert str(outstanding.id) in retained
    assert all(str(o.id) in retained for o in new)
    assert any(str(o.id) not in retained for o in terminal)
    assert after["snapshots"][0]["observation_id"] == str(current.id)
    assert (
        next(
            r for r in after["records"] if r["observation"]["id"] == str(outstanding.id)
        )["resolution"]
        == "unresolved"
    )
    assert sum(len(canonical(r)) for r in after["records"]) <= config.state_bytes
    assert len(after["records"]) < config.state_records
