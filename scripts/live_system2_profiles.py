"""One live System 2 attempt for each thinking level against captured evidence."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from _live_support import load_live_key, request_controls

from signal_reducers.config import Config
from signal_reducers.demo import write_private
from signal_reducers.models import (
    Batch,
    EscalationRequest,
    Observation,
    Record,
    canonical,
    public,
    utcnow,
)
from signal_reducers.providers import DrexGate, OpenRouterClient, OpenRouterSystem2


class DiagnosticClient(OpenRouterClient):
    def __init__(self, config):
        super().__init__(config)
        self.profiles = []

    async def post(self, endpoint, body, **kwargs):
        self.profiles.append(
            {"model": body["model"], "endpoint": endpoint, **request_controls(body)}
        )
        return await super().post(endpoint, body, **kwargs)


async def run(output, context="simple"):
    source = load_live_key()
    question = Observation(
        reducer_id="conversation",
        subject_id="user/diagnosis",
        subject_seq=1,
        kind="event",
        finality="committed",
        code="conversation.message",
        content="Why is checkout degraded?",
        facts_json=canonical(
            {
                "text": "Why is checkout degraded?",
                "intent": "diagnosis",
                "message_id": "diagnosis",
            }
        ),
    )
    health = Observation(
        reducer_id="telemetry",
        subject_id="sensor/checkout",
        subject_seq=1,
        kind="snapshot",
        finality="committed",
        code="telemetry.status",
        content="Checkout is degraded, with elevated errors and queue depth.",
        facts_json=canonical(
            {
                "service": "checkout",
                "status": "degraded",
                "metrics": {"error_rate": 0.15, "queue_depth": 80},
            }
        ),
    )
    batch = Batch(
        run_id=uuid4(),
        state_version=1,
        input_generation=1,
        records=tuple(
            Record(observation=obs, accepted_version=1) for obs in (question, health)
        ),
        new_ids=(question.id, health.id),
    )
    report = {
        "credential_source": source,
        "credentials_logged": False,
        "scope": "Direct live System 2 profiles against immutable synthetic captured context; integrated lifecycle is tested separately.",
        "context_kind": context,
        "profiles": [],
    }
    async with DiagnosticClient(Config(mode="live")) as client:
        await client.check_authentication()
        await client.check_models()
        store = None
        if context == "representative":
            from live_gate_contexts import build_case

            store, named = await build_case(
                "integrated_diagnosis", DrexGate(client), client.config
            )
            batch = await store.create_batch(DrexGate(client).request_size)
            question = named["request"]
        context_json = canonical(public(batch))
        context_hash = hashlib.sha256(context_json).hexdigest()
        by_id = {record.id: record.observation for record in batch.records}
        producer_ids = {
            key
            for key, observation in by_id.items()
            if observation.reducer_id == "telemetry"
            and observation.facts.get("service")
        }
        report.update(
            {
                "context_sha256": context_hash,
                "context_bytes": len(context_json),
                "captured_context": public(batch),
            }
        )
        model = OpenRouterSystem2(client)
        for level in ("none", "some", "extra"):
            request = EscalationRequest(
                id=uuid4(),
                run_id=batch.run_id,
                batch_id=batch.id,
                context_version=batch.state_version,
                target_ids=(question.id,),
                reason="Answer the submitted diagnosis question using only captured facts.",
                thinking_level=level,
                context_json=context_json,
                deadline_at=utcnow() + timedelta(seconds=90),
            )
            result = await model.execute(request)
            checks = {
                "valid_status": result.status in ("success", "partial"),
                "exact_correlation": result.operation_id == request.id
                and result.target_ids == request.target_ids,
                "no_completion_budget": not client.profiles[-1][
                    "completion_budget_present"
                ],
                "no_tools": not client.profiles[-1]["tools_present"],
                "immutable_context": request.context_json == context_json,
                "captured_evidence_only": set(result.evidence_ids) <= set(by_id),
            }
            if context == "representative":
                checks.update(
                    {
                        "successful_answer": result.status == "success",
                        "producer_source_cited": bool(
                            set(result.evidence_ids) & producer_ids
                        ),
                        "all_five_records_captured": len(batch.records) == 5,
                    }
                )
            item = {
                "thinking_level": level,
                "request": client.profiles[-1],
                "result": public(result),
                "call": client.calls[-1],
                "checks": checks,
                "passed": all(checks.values()),
                "context_sha256": context_hash,
                "content_bytes": len(result.content.encode()),
                "findings_bytes": sum(
                    len(f.encode()) for f in result.data.get("findings", [])
                ),
                "cited_observation_codes": [
                    by_id[key].code for key in result.evidence_ids if key in by_id
                ],
            }
            report["profiles"].append(item)
            print(
                json.dumps(
                    {
                        "thinking_level": level,
                        "status": result.status,
                        "error_code": result.error_code,
                        "latency_seconds": client.calls[-1]["latency_seconds"],
                        "passed": item["passed"],
                    }
                ),
                flush=True,
            )
        report.update(
            {
                "request_counts": dict(client.counters),
                "passed": all(p["passed"] for p in report["profiles"]),
                "completed_at": utcnow().isoformat(),
            }
        )
        if store is not None:
            await store.finish_batch(batch)
    write_private(output, report)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    arguments = argparse.ArgumentParser(description=__doc__)
    arguments.add_argument(
        "--output", type=Path, default=Path("outputs/live/system2-profiles.json")
    )
    arguments.add_argument(
        "--context", choices=("simple", "representative"), default="simple"
    )
    try:
        args = arguments.parse_args()
        code = asyncio.run(run(args.output, args.context))
    except KeyboardInterrupt:
        code = 130
    except Exception as exc:
        print({"error": getattr(exc, "code", type(exc).__name__)}, file=sys.stderr)
        code = 1
    raise SystemExit(code)
