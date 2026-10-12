"""Explicit paid numeric checks; factual answer review is recorded separately."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from _live_support import load_live_key, request_controls, source_manifest

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
from signal_reducers.providers import (
    OpenRouterClient,
    OpenRouterReducer,
    OpenRouterSystem2,
)


class NumericClient(OpenRouterClient):
    def __init__(self, config):
        super().__init__(config)
        self.controls = []

    async def post(self, endpoint, body, **kwargs):
        self.controls.append(
            {
                "component": kwargs.get("component"),
                "model": body["model"],
                **request_controls(body),
            }
        )
        return await super().post(endpoint, body, **kwargs)


def captured(rate: float) -> tuple[Batch, Observation, Observation]:
    question = Observation(
        reducer_id="conversation",
        subject_id="user/numeric",
        subject_seq=1,
        kind="event",
        finality="committed",
        code="conversation.message",
        content="Report checkout's error rate and queue depth. Is its underlying cause established?",
        facts_json=canonical(
            {"intent": "diagnosis", "submitted": True, "message_id": "numeric"}
        ),
    )
    health = Observation(
        reducer_id="telemetry",
        subject_id="sensor/checkout",
        subject_seq=1,
        kind="snapshot",
        finality="committed",
        code="telemetry.status",
        content=f"Checkout is degraded, with an error rate of {rate}% and queue depth 50.",
        facts_json=canonical(
            {
                "service": "checkout",
                "status": "degraded",
                "metrics": {"error_rate": rate, "queue_depth": 50},
            }
        ),
    )
    batch = Batch(
        run_id=uuid4(),
        state_version=1,
        input_generation=1,
        records=tuple(
            Record(observation=value, accepted_version=1)
            for value in (question, health)
        ),
        new_ids=(question.id, health.id),
    )
    return batch, question, health


async def run(output: Path) -> int:
    credential_source = load_live_key()
    report = {
        "schema_version": "1.0",
        "credential_source": credential_source,
        "credentials_logged": False,
        "manifest": source_manifest(),
        "scope": "Ten hosted telemetry summaries and six direct hosted System 2 calls on synthetic evidence; no Laya or DREX.",
        "unit_convention": "error_rate is a fraction; 0.45 means 45%, 0.0045 means 0.45%.",
        "manual_factual_review_required": True,
        "telemetry": [],
        "system2": [],
    }
    async with NumericClient(Config(mode="live")) as client:
        await client.check_authentication()
        await client.check_models()
        reducer, system2 = OpenRouterReducer(client), OpenRouterSystem2(client)
        for rate in (0.45, 0.0045):
            percent = float(Decimal(str(rate)) * 100)
            for repetition in range(1, 6):
                context = {
                    "service": "checkout",
                    "status": "degraded",
                    "metrics": {"error_rate": rate, "queue_depth": 50},
                    "source_seq": repetition,
                    "coverage_incomplete": False,
                }
                count = client.counters["reducer_telemetry"]
                item = {
                    "rate": rate,
                    "expected_percent": percent,
                    "repetition": repetition,
                }
                try:
                    result = await reducer.summarize("telemetry", context)
                    item.update(
                        {
                            "summary": result["summary"],
                            "intent": result["intent"],
                            "controls_passed": result["intent"] is None,
                        }
                    )
                except Exception as error:
                    item.update(
                        {
                            "controls_passed": False,
                            "error_code": getattr(error, "code", type(error).__name__),
                        }
                    )
                item["http_attempts"] = client.counters["reducer_telemetry"] - count
                item["call"] = client.calls[-1]
                item["controls_passed"] &= item["http_attempts"] == 1
                report["telemetry"].append(item)
                write_private(output, report)
                print(
                    json.dumps(
                        {
                            "component": "telemetry",
                            "rate": rate,
                            "repetition": repetition,
                            "controls_passed": item["controls_passed"],
                        }
                    ),
                    flush=True,
                )
            batch, question, health = captured(rate)
            context_json = canonical(public(batch))
            for level in ("none", "some", "extra"):
                request = EscalationRequest(
                    id=uuid4(),
                    batch_id=batch.id,
                    run_id=batch.run_id,
                    context_version=batch.state_version,
                    target_ids=(question.id,),
                    reason="Answer only from authoritative captured source facts; a generated summary may be wrong.",
                    thinking_level=level,
                    context_json=context_json,
                    deadline_at=utcnow() + timedelta(seconds=90),
                )
                count = client.counters["system2"]
                result = await system2.execute(request)
                controls = client.controls[-1]
                checks = {
                    "success": result.status == "success",
                    "one_attempt": client.counters["system2"] - count == 1,
                    "correlation": result.operation_id == request.id
                    and result.target_ids == request.target_ids,
                    "immutable_context": request.context_json == context_json,
                    "source_cited": health.id in result.evidence_ids,
                    "captured_evidence": set(result.evidence_ids)
                    <= {question.id, health.id},
                    "no_completion_budget": not controls["completion_budget_present"],
                    "no_tools": not controls["tools_present"],
                    "selected_reasoning": controls["reasoning"]
                    == {
                        "effort": {"none": "none", "some": "low", "extra": "high"}[
                            level
                        ],
                        "exclude": True,
                    },
                }
                item = {
                    "rate": rate,
                    "expected_percent": percent,
                    "thinking_level": level,
                    "adversarial_summary": health.content,
                    "context_sha256": hashlib.sha256(context_json).hexdigest(),
                    "result": public(result),
                    "controls": controls,
                    "call": client.calls[-1],
                    "checks": checks,
                    "controls_passed": all(checks.values()),
                }
                report["system2"].append(item)
                write_private(output, report)
                print(
                    json.dumps(
                        {
                            "component": "system2",
                            "rate": rate,
                            "thinking_level": level,
                            "controls_passed": item["controls_passed"],
                            "status": result.status,
                        }
                    ),
                    flush=True,
                )
        report.update(
            {
                "completed_at": utcnow().isoformat(),
                "request_counts": dict(client.counters),
                "controls_passed": all(
                    item["controls_passed"]
                    for key in ("telemetry", "system2")
                    for item in report[key]
                ),
                "provider_reported_cost": sum(
                    call.get("usage", {}).get("cost", 0) for call in client.calls
                ),
            }
        )
    write_private(output, report)
    return 0 if report["controls_passed"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=Path("outputs/live/numeric.json")
    )
    try:
        raise SystemExit(asyncio.run(run(parser.parse_args().output)))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as error:
        print(
            json.dumps({"error": getattr(error, "code", type(error).__name__)}),
            file=sys.stderr,
        )
        raise SystemExit(1) from None
