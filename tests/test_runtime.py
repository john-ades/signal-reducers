"""Integrated fixture models with actual Redis and production lifecycle code."""

import asyncio
from uuid import uuid4

import pytest

from signal_reducers.config import Config
from signal_reducers.fixtures import FixtureGate, FixtureSystem2
from signal_reducers.models import Batch, Record, SignalInput, canonical
from signal_reducers.runtime import Runtime


def telemetry(seq, status="ok"):
    return SignalInput(
        source="sensor",
        source_seq=seq,
        modality="structured",
        payload={
            "type": "telemetry",
            "service": "demo.worker",
            "status": status,
            "metrics": {
                "error_rate": 0 if status == "ok" else 0.2,
                "queue_depth": 0 if status == "ok" else 20,
            },
            "note": "synthetic report",
        },
    )


def question(seq, event, text="", message="m1"):
    return SignalInput(
        source="chat",
        source_seq=seq,
        modality="structured",
        payload={
            "type": "conversation",
            "message_id": message,
            "event": event,
            "text": text,
        },
    )


async def wait_view(runtime, predicate, timeout=5):
    async with asyncio.timeout(timeout):
        while True:
            view = await runtime.inspect()
            if predicate(view):
                return view
            await asyncio.sleep(0.005)


def records(view, code):
    return [r for r in view["records"] if r["observation"]["code"] == code]


async def test_fixture_runtime_repeat_diagnosis_recovery_and_idle(redis_url):
    runtime = Runtime(
        uuid4(),
        Config(
            mode="fixture",
            redis_url=redis_url,
            debounce=0.03,
            max_debounce=0.1,
            wait_followup=0.1,
        ),
    )
    await runtime.start()
    try:
        await runtime.queue.publish([telemetry(seq) for seq in range(1, 101)])
        await wait_view(runtime, lambda v: v["transport"]["popped"] == 100)
        await runtime.workers["telemetry"].join()
        view = await wait_view(
            runtime,
            lambda v: (
                records(v, "telemetry.status")
                and records(v, "telemetry.status")[0]["consumption"] == "acknowledged"
            ),
        )
        assert len(records(view, "telemetry.status")) == 1
        assert runtime.reducer_model.calls["telemetry"] == 1
        assert runtime.system2_model.calls == 0
        assert runtime.gate_model.calls <= 3
        calls = runtime.gate_model.calls
        await asyncio.sleep(0.13)
        assert runtime.gate_model.calls == calls

        await runtime.queue.publish([telemetry(101, "degraded")])
        await wait_view(
            runtime,
            lambda v: any(
                r["resolution"] == "unresolved"
                for r in records(v, "telemetry.health_changed")
            ),
        )
        await runtime.queue.publish(
            [
                question(1, "replace", "Explain why "),
                question(2, "append", "demo.worker is degraded."),
                question(3, "submit"),
            ]
        )
        view = await wait_view(
            runtime,
            lambda v: any(
                r["resolution"] == "resolved"
                for r in records(v, "conversation.message")
            ),
        )
        assert runtime.system2_model.calls == 1
        assert len(view["operations"]) == 1
        assert view["operations"][0]["thinking_level"] == "some"
        assert all(
            r["resolution"] == "unresolved"
            for r in records(view, "telemetry.health_changed")
            if r["observation"]["facts"]["status"] == "degraded"
        )
        assert view["results"][0]["status"] == "success"
        await runtime.queue.publish([telemetry(102, "ok")])
        await wait_view(
            runtime,
            lambda v: all(
                r["resolution"] == "resolved"
                for r in records(v, "telemetry.health_changed")
                if r["observation"]["facts"]["status"] == "degraded"
            ),
        )
        assert not runtime.fatal_error
        assert runtime.scheduler.failures == 0
    finally:
        await runtime.close()


async def test_injected_live_client_is_shared_and_owned_without_native_loading():
    import httpx

    from signal_reducers.providers import OpenRouterClient
    from signal_reducers.router import FixtureClassifier

    config = Config(mode="live")
    client = OpenRouterClient(
        config,
        "synthetic",
        httpx.MockTransport(
            lambda request: (_ for _ in ()).throw(
                AssertionError("No HTTP call expected")
            )
        ),
    )
    runtime = Runtime(uuid4(), config, client=client, classifier=FixtureClassifier())
    try:
        assert runtime.client is client
        assert (
            runtime.reducer_model.client
            is runtime.gate_model.client
            is runtime.system2_model.client
            is client
        )
        assert not client.calls
    finally:
        await runtime.close()
    assert client._http.is_closed


async def test_diagnostic_failure_does_not_change_gate_rejection_or_retry_bounds(
    redis_url,
):
    from signal_reducers.models import ContractError

    class InvalidGate(FixtureGate):
        async def evaluate(self, batch):
            self.calls += 1
            raise ContractError("synthetic_invalid_decision")

    observed = []

    def diagnostic(batch, error, phase):
        observed.append((batch.id, error.code, phase))
        raise RuntimeError("Diagnostic failure must not change scheduling")

    runtime = Runtime(
        uuid4(),
        Config(
            mode="fixture",
            redis_url=redis_url,
            debounce=0.01,
            max_debounce=0.02,
            retry_delay=0.03,
        ),
        gate_model=InvalidGate(),
        gate_error_handler=diagnostic,
    )
    await runtime.start()
    try:
        await runtime.queue.publish([telemetry(1)])
        view = await wait_view(
            runtime, lambda value: value["diagnostics"].get("needs_attention")
        )
        assert (
            runtime.scheduler.calls == runtime.scheduler.failures == len(observed) == 2
        )
        assert all(
            code == "synthetic_invalid_decision" and phase == "compile"
            for _, code, phase in observed
        )
        assert not view["operations"] and not view["results"]
        assert all(
            record["consumption"] != "acknowledged" for record in view["records"]
        )
        assert view["diagnostics"]["gate_error"] == "synthetic_invalid_decision"
    finally:
        await runtime.close()


async def test_live_start_invalid_key_fails_before_catalog_router_or_queue_ownership(
    monkeypatch,
):
    import httpx
    import pytest

    import signal_reducers.runtime as module
    from signal_reducers.providers import OpenRouterClient, ProviderError
    from signal_reducers.router import FixtureClassifier

    paths, queue_calls = [], []

    class Queue:
        def __init__(self, *_):
            pass

        async def check(self):
            queue_calls.append("check")

        async def claim(self):
            raise AssertionError("Invalid credentials cannot claim a run")

        async def close(self):
            queue_calls.append("close")

    class Classifier(FixtureClassifier):
        async def prepare(self):
            raise AssertionError("Invalid credentials cannot prepare CPU model")

    def handler(req):
        paths.append(req.url.path)
        assert req.url.path == "/api/v1/key"
        return httpx.Response(
            401, json={"error": {"message": "private provider details"}}
        )

    client = OpenRouterClient(Config(), "invalid", httpx.MockTransport(handler))
    monkeypatch.setattr(module, "OpenRouterClient", lambda _: client)
    monkeypatch.setattr(module, "SignalQueue", Queue)
    runtime = Runtime(uuid4(), Config(mode="live"), classifier=Classifier())
    with pytest.raises(ProviderError, match="provider_authentication"):
        await runtime.start()
    assert not runtime.started and runtime.stopped.is_set()
    assert not runtime.tasks and not runtime.operation_tasks
    assert queue_calls == ["check", "close"]
    assert paths == ["/api/v1/key"] and client._http.is_closed


async def test_system2_captured_context_and_gate_progress_during_call(redis_url):
    class CapturingModel(FixtureSystem2):
        def __init__(self):
            super().__init__(delay=0)
            self.entered, self.release = asyncio.Event(), asyncio.Event()
            self.request = None
            self.context_before = None

        async def execute(self, request):
            self.request = request
            self.context_before = canonical(request.context)
            self.entered.set()
            await self.release.wait()
            assert canonical(request.context) == self.context_before
            return await super().execute(request)

    model = CapturingModel()
    runtime = Runtime(
        uuid4(),
        Config(mode="fixture", redis_url=redis_url, debounce=0.01, max_debounce=0.03),
        system2_model=model,
    )
    await runtime.start()
    try:
        await runtime.queue.publish(
            [
                telemetry(1, "degraded"),
                question(1, "replace", "Why is demo.worker degraded?"),
                question(2, "submit"),
            ]
        )
        await asyncio.wait_for(model.entered.wait(), 5)
        calls = runtime.gate_model.calls
        await runtime.queue.publish([telemetry(2, "ok")])
        view = await wait_view(
            runtime,
            lambda v: (
                runtime.gate_model.calls > calls
                and any(
                    r["observation"]["facts"].get("status") == "ok"
                    for r in records(v, "telemetry.status")
                )
            ),
        )
        assert not view["operations"][0]["terminal"]
        assert b'"status":"degraded"' in model.context_before
        assert canonical(model.request.context) == model.context_before
        model.release.set()
        await wait_view(
            runtime,
            lambda v: any(
                r["resolution"] == "resolved"
                for r in records(v, "conversation.message")
            ),
        )
        assert model.calls == 1
        assert runtime.scheduler.failures == 0
    finally:
        model.release.set()
        await runtime.close()


async def test_registered_status_tool_result_resolves_only_question(redis_url):
    runtime = Runtime(
        uuid4(),
        Config(mode="fixture", redis_url=redis_url, debounce=0.01, max_debounce=0.03),
    )
    await runtime.start()
    try:
        await runtime.queue.publish(
            [
                question(1, "replace", "What is the processing status?"),
                question(2, "submit"),
            ]
        )
        view = await wait_view(
            runtime,
            lambda v: any(
                r["resolution"] == "resolved"
                for r in records(v, "conversation.message")
            ),
        )
        assert runtime.system2_model.calls == 0
        assert view["operations"][0]["action"] == "act"
        assert view["results"][0]["type"] == "tool"
        assert len(view["results"][0]["data"]["activity"]) == 3
        assert runtime.scheduler.failures == 0
    finally:
        await runtime.close()


async def test_fixture_gate_keeps_historical_resolved_request_without_new_basis():
    from signal_reducers.models import Observation

    event = Observation(
        reducer_id="conversation",
        subject_id="chat/m1",
        subject_seq=1,
        kind="event",
        finality="committed",
        code="conversation.message",
        content="Earlier question",
        facts_json=canonical({"message_id": "m1"}),
    )
    record = Record(
        observation=event,
        accepted_version=1,
        consumption="acknowledged",
        resolution="resolved",
    )
    batch = Batch(
        run_id=uuid4(), state_version=1, input_generation=1, records=(record,)
    )
    decision = await FixtureGate().evaluate(batch)
    assert decision.operation is None
    assert decision.dispositions[0].resolution is None


async def test_complete_demo_includes_video_recovery_revisions_gaps_and_failure(
    redis_url, tmp_path
):
    from signal_reducers.demo import scenario

    summary = await scenario(
        Config(mode="fixture", redis_url=redis_url), tmp_path, condition_timeout=5
    )
    assert summary["passed"], summary["checks"]
    assert summary["checks"]["health_resolved_after_recovery"]
    assert summary["checks"]["correction_retraction_are_new_records"]
    assert summary["checks"]["gap_cannot_commit"]
    assert summary["checks"]["failure_preserves_prior_observation"]
    assert summary["checks"]["no_scheduler_errors"]
    assert summary["checks"]["diagnosis_grounded_in_service_evidence"]
    assert len(summary["timeline"]) == 9
    assert summary["models"]["system2_calls"] == 1


async def test_demo_rejects_diagnosis_citing_only_the_question(
    redis_url, tmp_path, monkeypatch
):
    import signal_reducers.demo as demo

    class QuestionOnlyEvidence(FixtureSystem2):
        async def execute(self, request):
            result = await super().execute(request)
            return result.model_copy(update={"evidence_ids": request.target_ids})

    def fixture_runtime(run_id, config):
        return Runtime(run_id, config, system2_model=QuestionOnlyEvidence())

    summary = await demo.scenario(
        Config(mode="fixture", redis_url=redis_url),
        tmp_path,
        condition_timeout=5,
        runtime_factory=fixture_runtime,
    )
    assert summary["checks"]["question_resolved"] is True
    assert summary["checks"]["diagnosis_does_not_resolve_health"] is True
    assert summary["checks"]["no_scheduler_errors"] is True
    assert summary["checks"]["diagnosis_grounded_in_service_evidence"] is False
    assert summary["passed"] is False


@pytest.mark.parametrize(
    "status,error_code", [("failed", "synthetic_provider_failure"), ("partial", None)]
)
async def test_demo_reports_terminal_system2_failure_without_waiting_or_retrying(
    redis_url, tmp_path, monkeypatch, status, error_code
):
    import signal_reducers.demo as demo

    class TerminalResult(FixtureSystem2):
        async def execute(self, request):
            self.request = request
            result = await super().execute(request)
            self.returned_at = asyncio.get_running_loop().time()
            return result.model_copy(
                update={"status": status, "error_code": error_code}
            )

    model = TerminalResult()
    runtimes = []

    def fixture_runtime(run_id, config):
        runtime = Runtime(run_id, config, system2_model=model)
        runtimes.append(runtime)
        return runtime

    summary = await demo.scenario(
        Config(mode="fixture", redis_url=redis_url),
        tmp_path,
        condition_timeout=5,
        runtime_factory=fixture_runtime,
    )
    elapsed_after_result = asyncio.get_running_loop().time() - model.returned_at
    assert (
        elapsed_after_result < 1.5
    )  # The five-second condition deadline must not be consumed.
    assert summary["checks"]["scenario_error"] == (
        "demo_system2_result:" + status + ":" + (error_code or "incomplete_answer")
    )
    assert summary["passed"] is False
    assert model.calls == summary["models"]["system2_calls"] == 1

    view = await runtimes[0].inspect()
    assert len(view["operations"]) == len(view["results"]) == 1
    operation, result = view["operations"][0], view["results"][0]
    assert operation["terminal"] is True
    assert result["status"] == status
    assert result["operation_id"] == str(model.request.id)
    assert result["batch_id"] == str(model.request.batch_id)
    assert result["target_ids"] == [str(value) for value in model.request.target_ids]
    assert records(view, "conversation.message")
    assert all(
        record["resolution"] == "unresolved"
        for record in records(view, "conversation.message")
    )
    assert all(
        record["resolution"] == "unresolved"
        for record in records(view, "telemetry.health_changed")
    )

    coverage = summary["lifecycle_check_coverage"]
    assert coverage["complete"] is False
    assert "diagnosis_does_not_resolve_health" in coverage["not_checked"]
    assert "single_system2_http_attempt" in coverage["not_checked"]
    assert summary["lifecycle_violation_scope"] == "evaluated_scenario_checks"


async def test_full_destination_drops_independently_and_other_reducer_gets_complete_bundle(
    redis_url,
):
    from signal_reducers.fixtures import FixtureReducer

    class BlockingTelemetry(FixtureReducer):
        def __init__(self):
            super().__init__()
            self.entered, self.release = asyncio.Event(), asyncio.Event()

        async def summarize(self, reducer_id, context):
            if reducer_id == "telemetry":
                self.entered.set()
                await self.release.wait()
            return await super().summarize(reducer_id, context)

    model = BlockingTelemetry()
    runtime = Runtime(
        uuid4(),
        Config(
            mode="fixture",
            redis_url=redis_url,
            mailbox_capacity=1,
            draft_flush=0.02,
            draft_expiry=0.2,
            debounce=0.01,
            max_debounce=0.03,
        ),
        reducer_model=model,
    )
    admitted_conversation = []
    original_offer = runtime.workers["conversation"].offer

    async def capture_offer(value):
        admitted_conversation.append(value)
        return await original_offer(value)

    runtime.workers["conversation"].offer = capture_offer
    await runtime.start()
    try:
        await runtime.queue.publish([telemetry(1)])
        await asyncio.wait_for(model.entered.wait(), 5)
        payload = {
            "type": "bundle",
            "conversation": {
                "type": "conversation",
                "message_id": "bundle",
                "event": "replace",
                "text": "This complete conversation survives a full telemetry mailbox.",
            },
            "telemetry": {
                "type": "telemetry",
                "service": "demo.worker",
                "status": "degraded",
                "metrics": {"error_rate": 0.2, "queue_depth": 20},
            },
        }
        signal = SignalInput(
            source="bundle.source", source_seq=1, modality="mixed", payload=payload
        )
        await runtime.queue.publish([signal])
        view = await wait_view(
            runtime,
            lambda value: (
                value["runtime"].get("dropped_telemetry") == 1
                and bool(records(value, "conversation.draft"))
            ),
        )
        assert view["runtime"]["delivered_conversation"] == 1
        assert (
            view["runtime"]["delivered_telemetry"] == 1
        )  # Only the first, still-active item.
        assert admitted_conversation[0].signal.id == signal.id
        assert admitted_conversation[0].signal.payload == payload
        assert admitted_conversation[0].signal.payload_bytes == canonical(payload)
        assert (
            records(view, "conversation.draft")[0]["observation"]["content"]
            == payload["conversation"]["text"]
        )
        assert not records(view, "telemetry.status")
        assert runtime.workers["telemetry"].counters["dropped"] == 1
        assert not runtime.fatal_error
    finally:
        model.release.set()
        await runtime.close()


async def test_system2_success_after_ignored_deadline_is_stored_as_timeout():
    from signal_reducers.models import Disposition, GateDecision, Observation, Operation

    class IgnoresDeadline(FixtureSystem2):
        def __init__(self):
            super().__init__(delay=0)
            self.attempts = 0
            self.ignored_cancellation = False

        async def execute(self, request):
            self.attempts += 1
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                self.ignored_cancellation = True
                await asyncio.sleep(0.005)
            return await super().execute(request)

    model = IgnoresDeadline()
    runtime = Runtime(
        uuid4(), Config(mode="fixture", system2_timeout=0.02), system2_model=model
    )
    try:
        event = Observation(
            reducer_id="conversation",
            subject_id="chat/late",
            subject_seq=1,
            kind="event",
            finality="committed",
            code="conversation.message",
            content="Explain this issue.",
            facts_json=canonical(
                {"message_id": "late", "submitted": True, "intent": "diagnosis"}
            ),
        )
        snapshot = Observation(
            reducer_id="conversation",
            subject_id="chat/late",
            subject_seq=2,
            kind="snapshot",
            finality="committed",
            code="conversation.current_message",
            content=event.content,
            facts_json=event.facts_json,
        )
        await runtime.store.writer("conversation").emit((event, snapshot))
        batch = await runtime.store.create_batch()
        decision = GateDecision(
            batch_id=batch.id,
            action="escalate",
            dispositions=(
                Disposition(
                    observation_id=event.id,
                    acknowledge=True,
                    resolution="unresolved",
                    reason="Needs answer",
                ),
            ),
            operation=Operation(
                observation_ids=(event.id,),
                escalation_reason="Answer captured question",
                thinking_level="none",
            ),
        )
        operation = await runtime.store.apply(batch, decision)
        await runtime.store.finish_batch(batch)
        await runtime._execute(operation, batch, decision)
        view = await runtime.store.inspect()
        assert model.ignored_cancellation
        assert model.attempts == model.calls == 1
        assert len(view["results"]) == 1
        assert view["results"][0]["status"] == "timeout"
        assert view["results"][0]["operation_id"] == str(operation.id)
        assert view["operations"][0]["terminal"] is True
        request_record = next(
            value
            for value in view["records"]
            if value["observation"]["id"] == str(event.id)
        )
        assert request_record["resolution"] == "unresolved"
    finally:
        await runtime.close()


async def test_live_schedule_with_explicit_fixture_models_keeps_qualification_separate(
    redis_url, tmp_path, monkeypatch
):
    """Exercise live publication timing, without claiming hosted model quality."""
    import time
    from types import SimpleNamespace

    import signal_reducers.demo as demo
    from signal_reducers.router import FixtureClassifier

    class SerialFixtureClassifier(FixtureClassifier):
        def predict(self, state, questions):
            # Make BUFFERING observable between serially delivered packets.
            time.sleep(0.03)
            return super().predict(state, questions)

    def fixture_runtime(run_id, config):
        return Runtime(
            run_id,
            config.model_copy(update={"mode": "fixture"}),
            classifier=SerialFixtureClassifier(),
        )

    original_sleep = asyncio.sleep

    async def scheduling_sleep(seconds):
        # Accelerate only the live publisher pacing; preserve polling, model,
        # reducer and scheduler clocks in their own production modules.
        await original_sleep(0.001 if seconds == 1 else seconds)

    monkeypatch.setattr(
        demo,
        "asyncio",
        SimpleNamespace(
            sleep=scheduling_sleep,
            timeout=asyncio.timeout,
            get_running_loop=asyncio.get_running_loop,
        ),
    )
    summary = await demo.scenario(
        Config(mode="live", redis_url=redis_url),
        tmp_path,
        condition_timeout=5,
        runtime_factory=fixture_runtime,
    )
    assert summary["passed"], summary["checks"]
    assert summary["live_scenario_passed"] is True
    assert summary["repeat_input_pattern"] == "paced_one_signal_per_second"
    assert "degradation-reviewed" in summary["timeline"]
    assert "degraded-with-buffering-video" in summary["timeline"]
    assert "missing-fragment-visible" in summary["timeline"]
    assert summary["checks"]["pending_video_visible"] is True
    assert summary["checks"]["gap_cannot_commit"] is True
    assert summary["checks"]["single_system2_operation"] is True
    assert summary["router_manifest"]["model"] == "fixture"
    assert summary["router_manifest"]["quality_qualified"] is False
    assert "live_qualified" not in summary
    assert "separate requirements" in summary["qualification"]
