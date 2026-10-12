"""Explicit deterministic test doubles. Never selected as a live fallback."""

from __future__ import annotations

import asyncio
from collections import Counter

from .models import (
    Batch,
    Disposition,
    EscalationRequest,
    GateDecision,
    Operation,
    OperationResult,
    canonical,
)


class FixtureReducer:
    def __init__(self):
        self.calls = Counter()
        self.fail_next: str | None = None

    async def summarize(self, reducer_id, context):
        self.calls[reducer_id] += 1
        await asyncio.sleep(0)
        if self.fail_next == reducer_id:
            self.fail_next = None
            raise ValueError("fixture_model_failure")
        if reducer_id == "conversation":
            text = context.get("text", context.get("message", ""))
            lowered = str(text).lower()
            intent = (
                "processing_status"
                if "processing" in lowered and "status" in lowered
                else "diagnosis"
                if any(t in lowered for t in ("why", "explain"))
                else "informational"
            )
            return {"summary": str(text)[:120], "intent": intent}
        # Fixtures receive bounded structured source data; summaries do not supply facts.
        return {
            "summary": (
                "Simulated scene metadata."
                if reducer_id == "simulated_video"
                else "Reported worker health and metrics."
            ),
            "intent": None,
        }


class FixtureGate:
    def __init__(self):
        self.calls = 0

    async def evaluate(self, batch: Batch) -> GateDecision:
        self.calls += 1
        dispositions = []
        operation = None
        action = "acknowledge" if batch.records or batch.results else "wait"
        current = {s.observation_id for s in batch.snapshots}
        for record in batch.records[:16]:
            obs = record.observation
            facts = obs.facts
            resolution = "no_action_required"
            observation_basis = ()
            result_basis = ()
            if record.resolution == "resolved":
                # Historical resolved records can reappear as revision/result context.
                # Keeping their disposition needs no fresh resolution evidence.
                resolution = None
            elif obs.reducer_id == "telemetry" and facts.get("status") in (
                "degraded",
                "down",
            ):
                resolution = "unresolved"
                healthy = next(
                    (
                        r
                        for r in batch.records
                        if r.id in current
                        and r.observation.reducer_id == "telemetry"
                        and r.observation.subject_id == obs.subject_id
                        and r.observation.facts.get("status") == "ok"
                        and r.observation.subject_seq > obs.subject_seq
                    ),
                    None,
                )
                if healthy:
                    resolution = "resolved"
                    observation_basis = (healthy.id,)
            elif (
                obs.code == "conversation.message"
                and obs.kind == "event"
                and obs.finality == "committed"
            ):
                resolution = "unresolved"
                result = next(
                    (
                        r
                        for r in batch.results
                        if record.id in r.target_ids and r.status == "success"
                    ),
                    None,
                )
                attempted = any(
                    record.id in op.target_ids for op in batch.operations
                ) or any(record.id in r.target_ids for r in batch.results)
                if result:
                    resolution = "resolved"
                    result_basis = (result.id,)
                elif (
                    operation is None
                    and not attempted
                    and record.resolution != "resolved"
                ):
                    status_request = (
                        "processing" in obs.content.lower()
                        and "status" in obs.content.lower()
                    )
                    action = "act" if status_request else "escalate"
                    operation = Operation(
                        observation_ids=(record.id,),
                        tool_name="get_processing_status" if status_request else None,
                        arguments_json=canonical({"reducer_id": None})
                        if status_request
                        else None,
                        escalation_reason=None
                        if status_request
                        else "Explain the captured worker degradation.",
                        thinking_level=None if status_request else "some",
                    )
            dispositions.append(
                Disposition(
                    observation_id=record.id,
                    acknowledge=True,
                    resolution=resolution,
                    reason="Fixture decision from captured evidence.",
                    basis_observation_ids=observation_basis,
                    basis_result_ids=result_basis,
                )
            )
        return GateDecision(
            batch_id=batch.id,
            action=action,
            operation=operation,
            reason="Explicit fixture model.",
            dispositions=tuple(dispositions),
            reviewed_result_ids=tuple(r.id for r in batch.results),
        )


class FixtureSystem2:
    def __init__(self, delay=0.02):
        self.calls = 0
        self.delay = delay

    async def execute(self, request: EscalationRequest) -> OperationResult:
        self.calls += 1
        await asyncio.sleep(self.delay)
        records = request.context.get("records", [])
        telemetry = [
            r["observation"]
            for r in records
            if r["observation"]["reducer_id"] == "telemetry"
            and r["observation"].get("facts", {}).get("status") in ("degraded", "down")
        ]
        evidence = tuple(__import__("uuid").UUID(o["id"]) for o in telemetry[:8])
        status = "success" if telemetry else "partial"
        text = (
            "Captured telemetry reports degradation, elevated errors, and queued work. "
            "These facts support an incident diagnosis, but do not establish a root cause or recovery."
            if telemetry
            else "Captured evidence is insufficient to explain degradation."
        )
        return OperationResult(
            operation_id=request.id,
            type="system2",
            batch_id=request.batch_id,
            target_ids=request.target_ids,
            status=status,
            content=text,
            data_json=canonical(
                {"limitations": "Captured synthetic data only; no remediation."}
            ),
            evidence_ids=evidence,
            context_version=request.context_version,
        )
