"""Real DREX checks for packed multi-domain context; no Laya/System 2 calls."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from uuid import uuid4

from _live_support import load_live_key
from live_targeted import DiagnosticClient

from signal_reducers.config import Config
from signal_reducers.models import (
    Disposition,
    EvidenceRef,
    GateDecision,
    Observation,
    Operation,
    OperationResult,
    canonical,
    public,
    utcnow,
)
from signal_reducers.providers import DrexGate
from signal_reducers.state import ObservationStore


def observation(
    reducer,
    subject,
    seq,
    kind,
    code,
    content,
    facts,
    *,
    finality="committed",
    revises=None,
    refs=1,
):
    return Observation(
        reducer_id=reducer,
        subject_id=subject,
        subject_seq=seq,
        kind=kind,
        code=code,
        content=content,
        facts_json=canonical(facts),
        finality=finality,
        revises=revises,
        evidence_refs=tuple(
            EvidenceRef(receipt_id=uuid4(), signal_id=uuid4()) for _ in range(refs)
        ),
    )


async def build_case(name, gate, config):
    store, named = ObservationStore(uuid4(), config), {}
    if name == "unsubmitted_context":
        named["draft"] = observation(
            "conversation",
            "chat/diagnosis",
            1,
            "snapshot",
            "conversation.draft",
            "Explain why demo.worker is degraded.",
            {
                "message_id": "diagnosis",
                "submitted": False,
                "intent": "informational",
                "coverage_incomplete": False,
            },
            finality="provisional",
        )
        named["request_snapshot"] = observation(
            "conversation",
            "chat/diagnosis",
            2,
            "snapshot",
            "conversation.current_message",
            "Explain why demo.worker is degraded.",
            {
                "message_id": "diagnosis",
                "submitted": True,
                "intent": "diagnosis",
                "coverage_incomplete": False,
            },
        )
        await store.writer("conversation").emit(tuple(named.values()))
        return store, named
    video_facts = {
        "service": "demo.worker",
        "scene": "stalled",
        "first_seq": 1,
        "last_seq": 3,
        "coverage_incomplete": False,
    }
    named["video"] = observation(
        "simulated_video",
        "camera/demo.worker",
        1,
        "snapshot",
        "simulation.scene",
        "Synthetic camera shows a stalled service display; this is scene evidence, not proof of root cause.",
        video_facts,
        refs=3,
    )
    if name == "revision_terminal_context":
        await store.writer("simulated_video").emit((named["video"],))
        first = await store.create_batch(gate.request_size)
        await store.apply(
            first,
            GateDecision(
                batch_id=first.id,
                action="acknowledge",
                dispositions=(
                    Disposition(
                        observation_id=named["video"].id,
                        acknowledge=False,
                        resolution="no_action_required",
                    ),
                ),
            ),
        )
        await store.finish_batch(first)
        named["correction"] = observation(
            "simulated_video",
            "camera/demo.worker",
            2,
            "revision",
            "simulation.scene_corrected",
            "Corrected synthetic scene is clear.",
            {**video_facts, "scene": "clear"},
            finality="corrected",
            revises=named["video"].id,
        )
        await store.writer("simulated_video").emit((named["correction"],))
        second = await store.create_batch(gate.request_size)
        await store.apply(
            second,
            GateDecision(
                batch_id=second.id,
                action="acknowledge",
                dispositions=(
                    Disposition(
                        observation_id=named["correction"].id,
                        acknowledge=False,
                        resolution="no_action_required",
                    ),
                ),
            ),
        )
        await store.finish_batch(second)
        named["retraction"] = observation(
            "simulated_video",
            "camera/demo.worker",
            3,
            "revision",
            "simulation.scene_retracted",
            "Synthetic scene withdrawn.",
            {},
            finality="retracted",
            revises=named["correction"].id,
        )
        await store.writer("simulated_video").emit((named["retraction"],))
        return store, named
    facts = {
        "service": "demo.worker",
        "status": "degraded",
        "metrics": {"error_rate": 0.45, "queue_depth": 50},
        "source_seq": 101,
        "coverage_incomplete": False,
    }
    named["health"] = observation(
        "telemetry",
        "sensor/demo.worker",
        1,
        "snapshot",
        "telemetry.status",
        "demo.worker reports degradation with 45% error rate and 50 queued items; root cause is unproven.",
        facts,
    )
    named["health_event"] = observation(
        "telemetry",
        "sensor/demo.worker",
        2,
        "event",
        "telemetry.health_changed",
        "demo.worker changed from healthy to degraded; reported errors and queued work increased.",
        {**facts, "previous_status": "ok"},
    )
    await store.writer("telemetry").emit((named["health"], named["health_event"]))
    first = await store.create_batch(gate.request_size)
    await store.apply(
        first,
        GateDecision(
            batch_id=first.id,
            action="acknowledge",
            dispositions=tuple(
                Disposition(
                    observation_id=named[key].id,
                    acknowledge=True,
                    resolution="unresolved",
                )
                for key in ("health", "health_event")
            ),
        ),
    )
    await store.finish_batch(first)
    text = "Explain why demo.worker is degraded."
    request_facts = {
        "message_id": "diagnosis",
        "source_seq": 3,
        "submitted": True,
        "intent": "diagnosis",
        "coverage_incomplete": False,
    }
    named["request"] = observation(
        "conversation",
        "chat/diagnosis",
        1,
        "event",
        "conversation.message",
        text,
        request_facts,
        refs=3,
    )
    named["request_snapshot"] = observation(
        "conversation",
        "chat/diagnosis",
        2,
        "snapshot",
        "conversation.current_message",
        text,
        request_facts,
        refs=3,
    )
    await store.writer("simulated_video").emit((named["video"],))
    await store.writer("conversation").emit(
        (named["request"], named["request_snapshot"])
    )
    if name == "integrated_diagnosis":
        return store, named
    second = await store.create_batch(gate.request_size)
    active = await store.apply(
        second,
        GateDecision(
            batch_id=second.id,
            action="escalate",
            reason="Synthetic prerequisite",
            operation=Operation(
                observation_ids=(named["request"].id,),
                thinking_level="some",
                escalation_reason="Answer captured diagnosis",
            ),
            dispositions=tuple(
                Disposition(
                    observation_id=value.id,
                    acknowledge=True,
                    resolution="unresolved"
                    if key in ("health", "health_event", "request")
                    else "no_action_required",
                )
                for key, value in named.items()
            ),
        ),
    )
    await store.finish_batch(second)
    result = OperationResult(
        operation_id=active.id,
        batch_id=second.id,
        target_ids=active.target_ids,
        type="system2",
        status="success",
        context_version=active.context_version,
        evidence_ids=(named["health"].id, named["health_event"].id, named["video"].id),
        content="demo.worker reports degradation, 45% errors and 50 queued items. The synthetic display is stalled. These observations establish symptoms, not a proven cause; source evidence does not establish recovery.",
    )
    await store.add_result(result)
    named["synthetic_result"] = result
    if name == "integrated_result_review":
        return store, named
    third = await store.create_batch(gate.request_size)
    await store.apply(
        third,
        GateDecision(
            batch_id=third.id,
            action="acknowledge",
            reviewed_result_ids=(result.id,),
            dispositions=(
                Disposition(
                    observation_id=named["request"].id,
                    resolution="resolved",
                    basis_result_ids=(result.id,),
                    reason="Successful answer",
                ),
            ),
        ),
    )
    await store.finish_batch(third)
    named["recovery"] = observation(
        "telemetry",
        "sensor/demo.worker",
        3,
        "snapshot",
        "telemetry.status",
        "demo.worker reports recovery: error rate 1% and queue depth 1.",
        {
            **facts,
            "status": "ok",
            "metrics": {"error_rate": 0.01, "queue_depth": 1},
            "source_seq": 102,
        },
    )
    await store.writer("telemetry").emit((named["recovery"],))
    return store, named


async def main(output):
    load_live_key()
    config = Config()
    report = {
        "started_at": utcnow().isoformat(),
        "scope": "Real DREX plus immutable state, synthetic multi-domain captured context and prerequisite results; no Laya or System 2 calls.",
        "cases": [],
    }
    async with DiagnosticClient(config) as client:
        await client.check_authentication()
        gate = DrexGate(client)
        for name in (
            "integrated_diagnosis",
            "integrated_result_review",
            "integrated_recovery",
            "revision_terminal_context",
            "unsubmitted_context",
        ):
            store, named = await build_case(name, gate, config)
            batch = await store.create_batch(gate.request_size)
            item = {
                "case": name,
                "request_bytes": gate.request_size(batch),
                "captured": [
                    {
                        "id": str(r.id),
                        "code": r.observation.code,
                        "facts": r.observation.facts,
                    }
                    for r in batch.records
                ],
                "deferred": batch.deferred,
            }
            before, count = canonical(await store.inspect()), len(client.typed_answers)
            try:
                decision = await gate.evaluate(batch)
                operation = await store.apply(batch, decision)
                state = await store.inspect()
                resolutions = {
                    key: next(
                        (
                            r["resolution"]
                            for r in state["records"]
                            if r["observation"]["id"] == str(value.id)
                        ),
                        None,
                    )
                    for key, value in named.items()
                    if isinstance(value, Observation)
                }
                checks = {
                    "valid_action": decision.action == "escalate"
                    if name == "integrated_diagnosis"
                    else decision.action in ("acknowledge", "wait")
                }
                if name == "integrated_diagnosis":
                    checks.update(
                        {
                            "all_five_captured": len(batch.records) == 5,
                            "health_evidence_captured": all(
                                named[key].id in {r.id for r in batch.records}
                                for key in ("health", "health_event")
                            ),
                            "request_reserved": operation is not None
                            and operation.initiator_id == named["request"].id,
                            "context_snapshot_not_an_issue": resolutions[
                                "request_snapshot"
                            ]
                            in ("unassessed", "no_action_required"),
                        }
                    )
                elif name == "integrated_result_review":
                    checks.update(
                        {
                            "question_resolved": resolutions["request"] == "resolved",
                            "health_stays_unresolved": all(
                                resolutions[key] == "unresolved"
                                for key in ("health", "health_event")
                            ),
                            "result_reviewed": named["synthetic_result"].id
                            in decision.reviewed_result_ids,
                        }
                    )
                elif name == "integrated_recovery":
                    checks["health_resolved"] = all(
                        resolutions[key] == "resolved"
                        for key in ("health", "health_event")
                    )
                elif name == "revision_terminal_context":
                    checks.update(
                        {
                            "terminal_ancestors_kept": all(
                                resolutions[key] == "no_action_required"
                                for key in ("video", "correction")
                            ),
                            "retraction_unassessed": resolutions["retraction"]
                            == "unassessed",
                            "all_ancestors_captured": len(batch.records) == 3,
                        }
                    )
                else:
                    checks.update(
                        {
                            "draft_unassessed": resolutions["draft"] == "unassessed",
                            "context_snapshot_not_an_issue": resolutions[
                                "request_snapshot"
                            ]
                            in ("unassessed", "no_action_required"),
                            "no_operation_without_event": operation is None,
                        }
                    )
                item.update(
                    {
                        "checks": checks,
                        "passed": all(checks.values()),
                        "decision": public(decision),
                        "resolutions": resolutions,
                    }
                )
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
            report["cases"].append(item)
            print(
                json.dumps(
                    {
                        "case": name,
                        "passed": item["passed"],
                        "request_bytes": item["request_bytes"],
                        "error_code": item.get("error_code"),
                    }
                ),
                flush=True,
            )
        report.update(
            {
                "completed_at": utcnow().isoformat(),
                "requests": client.calls,
                "request_counts": dict(client.counters),
                "passed": all(item["passed"] for item in report["cases"]),
            }
        )
    from signal_reducers.demo import write_private

    write_private(output, report)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path, default=Path("outputs/live/gate-contexts.json")
    )
    raise SystemExit(asyncio.run(main(parser.parse_args().output)))
