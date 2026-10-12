"""A reviewed unsuccessful answer cannot reopen a paid operation candidate."""

import asyncio
import json
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest

from signal_reducers.config import Config
from signal_reducers.models import (
    ContractError,
    Disposition,
    EscalationRequest,
    GateDecision,
    Observation,
    Operation,
    OperationResult,
    canonical,
    public,
    utcnow,
)
from signal_reducers.providers import DrexGate, OpenRouterClient, OpenRouterSystem2
from signal_reducers.state import ObservationStore


@pytest.mark.parametrize("result_status", ["failed", "partial", "timeout", "cancelled"])
async def test_reviewed_unsuccessful_attempt_is_not_reoffered_after_recheck_or_new_context(
    result_status,
):
    config = Config(system2_timeout=0.02 if result_status == "timeout" else 90)
    store = ObservationStore(uuid4(), config)
    request = Observation(
        reducer_id="conversation",
        subject_id="chat/question",
        subject_seq=1,
        kind="event",
        finality="committed",
        code="conversation.message",
        content="Why is checkout degraded?",
        facts_json=canonical({"intent": "diagnosis", "message_id": "question"}),
    )
    await store.writer("conversation").emit((request,))
    http_started = asyncio.Event()

    async def handler(http_request):
        body = json.loads(http_request.content)
        if http_request.url.path.endswith("/systemone"):
            choices = body["questions"]["action"]["criteria"]
            action = "ESCALATE_SOME" if "ESCALATE_SOME" in choices else "ACKNOWLEDGE"
            selected = {
                "action": action,
                "initiator": str(request.id) if action == "ESCALATE_SOME" else "NONE",
            }
            for name in body["questions"]:
                if name.startswith("ack_") or name.startswith("review_"):
                    selected[name] = "YES"
                elif name.startswith("resolution_"):
                    selected[name] = (
                        "UNRESOLVED" if action == "ESCALATE_SOME" else "KEEP"
                    )
                elif name.startswith("basis_"):
                    selected[name] = "NONE"
            return httpx.Response(
                200,
                json={
                    "answers": {
                        name: {
                            "type": "choice",
                            "choice": selected[name],
                            "confidence": 1,
                            "probabilities": {
                                candidate: float(candidate == selected[name])
                                for candidate in question["criteria"]
                            },
                        }
                        for name, question in body["questions"].items()
                    }
                },
            )
        assert http_request.url.path.endswith("/chat/completions")
        http_started.set()
        if result_status in ("timeout", "cancelled"):
            await asyncio.sleep(1)
            pytest.fail("A cancelled/timed-out HTTP attempt must not complete")
        if result_status == "failed":
            return httpx.Response(503, json={"error": {"message": "unavailable"}})
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "content": json.dumps(
                                {
                                    "status": "partial",
                                    "content": "Captured observations are insufficient.",
                                    "findings": [],
                                    "evidence_ids": [],
                                }
                            ),
                        },
                    }
                ]
            },
        )

    async with OpenRouterClient(
        config, "test-key", httpx.MockTransport(handler)
    ) as client:
        gate = DrexGate(client)
        initial = await store.create_batch(gate.request_size)
        assert initial.attempted_request_ids == ()
        operation = await store.apply(initial, await gate.evaluate(initial))
        assert operation is not None
        await store.finish_batch(initial)
        # Captured metadata remains frozen even after the authoritative reservation.
        assert initial.attempted_request_ids == ()
        escalation = EscalationRequest(
            id=operation.id,
            run_id=store.run_id,
            batch_id=initial.id,
            context_version=initial.state_version,
            target_ids=operation.target_ids,
            reason="Answer request",
            thinking_level="some",
            context_json=canonical(public(initial)),
            deadline_at=utcnow() + timedelta(seconds=config.system2_timeout),
        )
        task = asyncio.create_task(OpenRouterSystem2(client).execute(escalation))
        if result_status == "cancelled":
            await http_started.wait()
            task.cancel()
        result = await task
        assert result.status == result_status
        await store.add_result(result)

        review = await store.create_batch(gate.request_size)
        assert review.attempted_request_ids == (request.id,) and review.results == (
            result,
        )
        assert await store.apply(review, await gate.evaluate(review)) is None
        await store.finish_batch(review)

        async def verify_no_reopened_operation():
            captured = await store.create_batch(gate.request_size)
            assert captured.attempted_request_ids == (request.id,)
            assert not captured.results and not captured.operations
            body = gate.request(captured)
            assert set(body["questions"]["action"]["criteria"]) == {
                "WAIT",
                "ACKNOWLEDGE",
            }
            assert set(body["questions"]["initiator"]["criteria"]) == {"NONE"}
            assert body["state"]["attempted_request_ids"] == [str(request.id)]
            # Captured metadata assists the model; it does not replace runtime authority.
            before = await store.inspect()
            forged = GateDecision(
                batch_id=captured.id,
                action="escalate",
                dispositions=(
                    Disposition(observation_id=request.id, resolution="unresolved"),
                ),
                operation=Operation(
                    observation_ids=(request.id,),
                    thinking_level="extra",
                    escalation_reason="Retry",
                ),
            )
            with pytest.raises(ContractError, match="initiator_already_attempted"):
                await store.apply(captured, forged)
            assert await store.inspect() == before
            assert await store.apply(captured, await gate.evaluate(captured)) is None
            await store.finish_batch(captured)
            assert client.counters["system2"] == 1

        await store.recheck()
        await verify_no_reopened_operation()
        telemetry = Observation(
            reducer_id="telemetry",
            subject_id="sensor/checkout",
            subject_seq=1,
            kind="snapshot",
            finality="committed",
            code="telemetry.status",
            content="New service context.",
            facts_json=canonical(
                {
                    "service": "checkout",
                    "status": "ok",
                    "metrics": {"error_rate": 0, "queue_depth": 1},
                }
            ),
        )
        await store.writer("telemetry").emit((telemetry,))
        await verify_no_reopened_operation()
        await store.recheck()
        await verify_no_reopened_operation()
        assert client.counters["system2"] == 1 and client.counters["system1"] == 5
        view = await store.inspect()
        record = next(
            record
            for record in view["records"]
            if record["observation"]["id"] == str(request.id)
        )
        assert record["resolution"] == "unresolved"
        assert len(view["operations"]) == 1 and len(view["results"]) == 1


async def test_attempt_metadata_is_sized_before_capture_fitting_and_omitted_with_deferred_records():
    config = Config()
    store = ObservationStore(uuid4(), config)
    request = Observation(
        reducer_id="conversation",
        subject_id="chat/question",
        subject_seq=1,
        kind="event",
        finality="committed",
        code="conversation.message",
        content="Why is checkout degraded?",
        facts_json=canonical({"intent": "diagnosis", "message_id": "question"}),
    )
    await store.writer("conversation").emit((request,))
    initial = await store.create_batch()
    operation = await store.apply(
        initial,
        GateDecision(
            batch_id=initial.id,
            action="escalate",
            dispositions=(
                Disposition(
                    observation_id=request.id, acknowledge=True, resolution="unresolved"
                ),
            ),
            operation=Operation(
                observation_ids=(request.id,),
                thinking_level="some",
                escalation_reason="Answer",
            ),
        ),
    )
    await store.finish_batch(initial)
    result = OperationResult(
        operation_id=operation.id,
        type="system2",
        batch_id=initial.id,
        target_ids=(request.id,),
        status="failed",
        error_code="provider_rate_limit",
        context_version=initial.state_version,
    )
    await store.add_result(result)
    review = await store.create_batch()
    await store.apply(
        review,
        GateDecision(
            batch_id=review.id, action="acknowledge", reviewed_result_ids=(result.id,)
        ),
    )
    await store.finish_batch(review)
    context = Observation(
        reducer_id="telemetry",
        subject_id="sensor/other",
        subject_seq=1,
        kind="snapshot",
        finality="committed",
        code="telemetry.status",
        content="Other service context.",
        facts_json=canonical({"service": "other", "status": "ok"}),
    )
    await store.writer("telemetry").emit((context,))
    measured_attempts = []

    def request_size(candidate):
        measured_attempts.append(candidate.attempted_request_ids)
        assert set(candidate.attempted_request_ids) <= {
            record.id for record in candidate.records
        }
        # Reserve enough model-specific overhead to defer the attempted group.
        if candidate.attempted_request_ids:
            return config.batch_bytes + 1
        return len(canonical(public(candidate)))

    captured = await store.create_batch(request_size)
    assert (request.id,) in measured_attempts
    assert captured.attempted_request_ids == ()
    assert request.id not in {record.id for record in captured.records}
    assert context.id in {record.id for record in captured.records}
    assert captured.deferred["unresolved"] == 1
    await store.finish_batch(captured)
