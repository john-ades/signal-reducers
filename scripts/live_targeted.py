"""User-authorized, bounded live reducer/DREX checks against synthetic evidence.

Uses the real adapters and state authority. Synthetic successful results isolate
DREX review behavior; this script never executes a System 2 operation.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from uuid import uuid4

from _live_support import load_live_key

from signal_reducers.config import Config
from signal_reducers.models import (
    Activity,
    Disposition,
    GateDecision,
    Observation,
    Operation,
    OperationResult,
    canonical,
    public,
    utcnow,
)
from signal_reducers.providers import DrexGate, OpenRouterClient, OpenRouterReducer
from signal_reducers.state import ObservationStore


class DiagnosticClient(OpenRouterClient):
    def __init__(self, config):
        super().__init__(config)
        self.typed_answers = []

    async def post(self, endpoint, body, **kwargs):
        response = await super().post(endpoint, body, **kwargs)
        if endpoint == "systemone":
            # Closed-choice diagnostics only: never store the provider response,
            # generated text, raw request, credentials, or reasoning.
            answers = response.get("answers", {})
            self.typed_answers.append(
                {
                    name: {
                        "choice": answer.get("choice"),
                        "confidence": answer.get("confidence"),
                        "probabilities": answer.get("probabilities"),
                    }
                    for name, answer in answers.items()
                    if name in body["questions"] and isinstance(answer, dict)
                }
            )
        return response


def obs(reducer, subject, seq, kind, code, content, facts):
    return Observation(
        reducer_id=reducer,
        subject_id=subject,
        subject_seq=seq,
        kind=kind,
        code=code,
        content=content,
        facts_json=canonical(facts),
        finality="committed",
    )


async def submitted(store, intent, text):
    subject, message = "user/question", "question"
    event = obs(
        "conversation",
        subject,
        1,
        "event",
        "conversation.message",
        text,
        {"intent": intent, "message_id": message, "text": text},
    )
    snapshot = obs(
        "conversation",
        subject,
        2,
        "snapshot",
        "conversation.current_message",
        text,
        {"intent": intent, "message_id": message, "text": text},
    )
    await store.writer("conversation").emit((event, snapshot))
    return event


def health(seq=1, status="degraded"):
    return obs(
        "telemetry",
        "sensor/checkout",
        seq,
        "snapshot",
        "telemetry.status",
        "Checkout reports " + status + ".",
        {
            "service": "checkout",
            "status": status,
            "metrics": {
                "error_rate": 0.15 if status == "degraded" else 0,
                "queue_depth": 80 if status == "degraded" else 0,
            },
        },
    )


async def setup_case(name, gate, config):
    store = ObservationStore(uuid4(), config)
    named = {}
    if name == "buffering_wait":
        await store.writer("simulated_video").activity(
            Activity(
                reducer_id="simulated_video",
                status="buffering",
                buffered_signals=1,
                buffered_bytes=100,
            )
        )
    if name == "healthy_acknowledge":
        named["health"] = health(status="ok")
        await store.writer("telemetry").emit((named["health"],))
    if name in (
        "status_request",
        "informational_request",
        "diagnosis_request",
        "successful_result_review",
    ):
        intent, text = {
            "status_request": (
                "processing_status",
                "Are any reducers still processing right now?",
            ),
            "informational_request": (
                "informational",
                "Summarize the observations currently available.",
            ),
            "diagnosis_request": ("diagnosis", "Why is the checkout service degraded?"),
            "successful_result_review": (
                "diagnosis",
                "Why is the checkout service degraded?",
            ),
        }[name]
        if name in ("diagnosis_request", "successful_result_review"):
            named["health"] = health()
            await store.writer("telemetry").emit((named["health"],))
        named["request"] = await submitted(store, intent, text)
    if name == "successful_result_review":
        first = await store.create_batch(gate.request_size)
        ids = (named["request"].id, named["health"].id)
        decision = GateDecision(
            batch_id=first.id,
            action="escalate",
            reason="Synthetic prerequisite only",
            operation=Operation(
                observation_ids=(named["request"].id,),
                thinking_level="some",
                escalation_reason="Answer diagnosis",
            ),
            dispositions=tuple(
                Disposition(
                    observation_id=i,
                    acknowledge=True,
                    resolution="unresolved",
                    reason="Await answer or recovery",
                )
                for i in ids
            ),
        )
        active = await store.apply(first, decision)
        await store.finish_batch(first)
        result = OperationResult(
            operation_id=active.id,
            batch_id=first.id,
            type="system2",
            target_ids=active.target_ids,
            status="success",
            context_version=active.context_version,
            evidence_ids=(named["health"].id,),
            content="Reported errors and queue depth accompany degradation; a root cause is not established.",
        )
        await store.add_result(result)
        named["synthetic_result"] = result
    if name == "healthy_recovery":
        named["health"] = health()
        await store.writer("telemetry").emit((named["health"],))
        first = await store.create_batch(gate.request_size)
        await store.apply(
            first,
            GateDecision(
                batch_id=first.id,
                action="acknowledge",
                dispositions=(
                    Disposition(
                        observation_id=named["health"].id,
                        acknowledge=True,
                        resolution="unresolved",
                    ),
                ),
            ),
        )
        await store.finish_batch(first)
        named["recovery"] = health(2, "ok")
        await store.writer("telemetry").emit((named["recovery"],))
    return store, named


async def main(output: Path):
    source = load_live_key()
    config = Config(mode="live")
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "started_at": utcnow().isoformat(),
        "credential_source": source,
        "scope": "Synthetic captured inputs, real OpenRouter reducers and DREX, real atomic state validation; no System 2 call and no Laya in this targeted suite.",
        "reducers": [],
        "gate": [],
    }
    async with DiagnosticClient(config) as client:
        await client.check_authentication()
        reducer = OpenRouterReducer(client)
        reducer_cases = [
            (
                "telemetry_degraded",
                "telemetry",
                {
                    "facts": health().facts,
                    "note": "Errors rose after a deployment.",
                    "previous_status": "ok",
                },
                None,
            ),
            (
                "telemetry_healthy",
                "telemetry",
                {
                    "facts": health(status="ok").facts,
                    "note": "Recovered",
                    "previous_status": "degraded",
                },
                None,
            ),
            (
                "conversation_status",
                "conversation",
                {
                    "text": "Are any reducers still processing right now?",
                    "submitted": True,
                    "coverage_complete": True,
                },
                "processing_status",
            ),
            (
                "conversation_diagnosis",
                "conversation",
                {
                    "text": "Why is checkout degraded?",
                    "submitted": True,
                    "coverage_complete": True,
                },
                "diagnosis",
            ),
            (
                "conversation_informational",
                "conversation",
                {
                    "text": "Summarize the currently captured observations.",
                    "submitted": True,
                    "coverage_complete": True,
                },
                "informational",
            ),
            (
                "video_segment",
                "simulated_video",
                {
                    "facts": {
                        "service": "checkout",
                        "scene": "warning_banner",
                        "segment_id": "s1",
                        "source_seq_start": 1,
                        "source_seq_end": 3,
                    },
                    "simulated": True,
                    "previous_scene": None,
                },
                None,
            ),
        ]
        for name, reducer_id, context, expected_intent in reducer_cases:
            item = {"case": name, "expected_intent": expected_intent}
            try:
                result = await reducer.summarize(reducer_id, context)
                item.update(
                    {
                        "passed": result["intent"] == expected_intent,
                        "intent": result["intent"],
                        "summary": result["summary"],
                        "summary_bytes": len(result["summary"].encode()),
                    }
                )
            except Exception as error:
                item.update(
                    {
                        "passed": False,
                        "error_code": getattr(error, "code", type(error).__name__),
                    }
                )
            report["reducers"].append(item)
            print(
                json.dumps(
                    {
                        "component": "reducer",
                        "case": name,
                        "passed": item["passed"],
                        "error_code": item.get("error_code"),
                    }
                ),
                flush=True,
            )
        gate = DrexGate(client)
        actions = {
            "buffering_wait": "wait",
            "healthy_acknowledge": "acknowledge",
            "status_request": "act",
            "informational_request": "escalate",
            "diagnosis_request": "escalate",
            "successful_result_review": "acknowledge",
            "healthy_recovery": "acknowledge",
        }
        for name, expected_action in actions.items():
            store, named = await setup_case(name, gate, config)
            batch = await store.create_batch(gate.request_size)
            before = canonical(await store.inspect())
            item = {
                "case": name,
                "expected_action": expected_action,
                "request_bytes": gate.request_size(batch),
                "synthetic_prerequisite": name
                in ("successful_result_review", "healthy_recovery"),
            }
            count = len(client.typed_answers)
            try:
                decision = await gate.evaluate(batch)
                item["decision"] = public(decision)
                operation = await store.apply(batch, decision)
                state = await store.inspect()
                resolutions = {
                    label: next(
                        (
                            r["resolution"]
                            for r in state["records"]
                            if r["observation"]["id"] == str(value.id)
                        ),
                        None,
                    )
                    for label, value in named.items()
                    if isinstance(value, Observation)
                }
                item.update(
                    {
                        "atomic_validation_passed": True,
                        "resolutions": resolutions,
                        "operation_reserved": operation is not None,
                    }
                )
                checks = {"expected_action": decision.action == expected_action}
                if name in (
                    "healthy_acknowledge",
                    "successful_result_review",
                    "healthy_recovery",
                ):
                    # WAIT can still acknowledge/review and commit valid
                    # dispositions; qualify outcomes, not a decorative label.
                    checks = {
                        "valid_action": decision.action in ("acknowledge", "wait")
                    }
                if expected_action in ("act", "escalate"):
                    checks["request_unresolved"] = (
                        resolutions.get("request") == "unresolved"
                    )
                    checks["request_is_initiator"] = (
                        operation is not None
                        and operation.initiator_id == named["request"].id
                    )
                if name == "healthy_acknowledge":
                    checks["healthy_no_action"] = (
                        resolutions.get("health") == "no_action_required"
                    )
                if name == "successful_result_review":
                    checks.update(
                        {
                            "question_resolved": resolutions.get("request")
                            == "resolved",
                            "degraded_health_stays_unresolved": resolutions.get(
                                "health"
                            )
                            == "unresolved",
                            "result_reviewed": named["synthetic_result"].id
                            in decision.reviewed_result_ids,
                            "no_new_operation": operation is None,
                        }
                    )
                if name == "healthy_recovery":
                    checks["health_resolved_by_recovery"] = (
                        resolutions.get("health") == "resolved"
                    )
                item.update({"checks": checks, "passed": all(checks.values())})
            except Exception as error:
                item.update(
                    {
                        "passed": False,
                        "error_code": getattr(error, "code", type(error).__name__),
                        "rejection_was_atomic": before
                        == canonical(await store.inspect()),
                    }
                )
            item["typed_answers"] = client.typed_answers[count:]
            await store.finish_batch(batch)
            report["gate"].append(item)
            print(
                json.dumps(
                    {
                        "component": "gate",
                        "case": name,
                        "passed": item["passed"],
                        "error_code": item.get("error_code"),
                    }
                ),
                flush=True,
            )
        report.update(
            {
                "completed_at": utcnow().isoformat(),
                "request_counts": dict(client.counters),
                "requests": client.calls,
                "passed": all(
                    item["passed"]
                    for kind in ("reducers", "gate")
                    for item in report[kind]
                ),
            }
        )
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(report, handle, indent=2)
        handle.write("\n")
    print(
        json.dumps(
            {
                "report": str(output),
                "request_counts": report["request_counts"],
                "passed": report["passed"],
            }
        )
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    arguments = argparse.ArgumentParser(description=__doc__)
    arguments.add_argument(
        "--output", type=Path, default=Path("outputs/live/targeted.json")
    )
    try:
        code = asyncio.run(main(arguments.parse_args().output))
    except KeyboardInterrupt:
        code = 130
    except Exception as exc:
        print({"error": getattr(exc, "code", type(exc).__name__)}, file=sys.stderr)
        code = 1
    raise SystemExit(code)
