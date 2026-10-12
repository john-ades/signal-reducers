"""Qualify quiet-state behavior with native CPU Laya and real hosted models."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
import time
from pathlib import Path
from uuid import uuid4

from _live_support import load_live_key

from signal_reducers.config import Config
from signal_reducers.demo import dedicated_redis, records, telemetry, write_private
from signal_reducers.models import utcnow
from signal_reducers.runtime import Runtime


def inference_counts(runtime):
    components = (
        "reducer_conversation",
        "reducer_telemetry",
        "reducer_simulated_video",
        "system1",
        "system2",
    )
    return {
        "hosted": {name: runtime.client.counters[name] for name in components},
        "native_predictions": runtime.router.counters["calls"],
    }


async def run(output: Path):
    report = {
        "started_at": utcnow().isoformat(),
        "credential_source": load_live_key(),
        "credentials_logged": False,
        "scope": "Thirty-second idle window after one real CPU Laya routing and hosted telemetry/DREX evaluation; independent transient Redis.",
    }
    inspections = []
    with dedicated_redis() as redis_url:
        config = Config(mode="live", redis_url=redis_url)
        runtime = Runtime(uuid4(), config)
        try:
            # Runtime authenticates before loading CPU weights or paid inference.
            await runtime.start()
            await runtime.queue.publish([telemetry(1)])
            async with asyncio.timeout(90):
                while True:
                    view = await runtime.inspect()
                    if runtime.fatal_error:
                        raise RuntimeError("runtime_fatal_error")
                    snapshots = records(view, "telemetry.status")
                    if (
                        len(snapshots) == 1
                        and snapshots[0]["consumption"] == "acknowledged"
                    ):
                        break
                    await asyncio.sleep(0.05)
            await asyncio.gather(
                *(worker.join() for worker in runtime.workers.values())
            )
            # Allow the scheduler's existing bounded WAIT follow-up to settle.
            settle_seconds = config.wait_followup + 1
            await asyncio.sleep(settle_seconds)
            baseline = await runtime.inspect()
            baseline_counts = inference_counts(runtime)
            start = time.monotonic()
            deadline = start + 30
            while time.monotonic() < deadline:
                inspected_at = time.perf_counter()
                await runtime.inspect()
                inspections.append(time.perf_counter() - inspected_at)
                await asyncio.sleep(min(0.05, max(0, deadline - time.monotonic())))
            measured_seconds = time.monotonic() - start
            final = await runtime.inspect()
            final_counts = inference_counts(runtime)
            hosted_names = set(baseline_counts["hosted"]) | set(final_counts["hosted"])
            hosted_delta = {
                name: final_counts["hosted"].get(name, 0)
                - baseline_counts["hosted"].get(name, 0)
                for name in sorted(hosted_names)
            }
            prediction_delta = (
                final_counts["native_predictions"]
                - baseline_counts["native_predictions"]
            )
            p95 = sorted(inspections)[math.ceil(0.95 * len(inspections)) - 1]
            checks = {
                "one_native_prediction": baseline_counts["native_predictions"] == 1,
                "one_telemetry_inference": baseline_counts["hosted"].get(
                    "reducer_telemetry", 0
                )
                == 1,
                "healthy_snapshot_acknowledged": len(
                    records(baseline, "telemetry.status")
                )
                == 1
                and records(baseline, "telemetry.status")[0]["consumption"]
                == "acknowledged",
                "idle_window_at_least_thirty_seconds": measured_seconds >= 30,
                "zero_new_hosted_inferences": all(
                    delta == 0 for delta in hosted_delta.values()
                ),
                "zero_new_native_predictions": prediction_delta == 0,
                "inspection_p95_under_fifty_ms": p95 < 0.05,
                "state_unchanged_during_idle": final["state_version"]
                == baseline["state_version"],
                "no_runtime_fatal_error": final["fatal_error"] is None,
            }
            report.update(
                {
                    "settle_seconds": settle_seconds,
                    "measured_idle_seconds": measured_seconds,
                    "baseline_counts": baseline_counts,
                    "final_counts": final_counts,
                    "idle_inference_delta": {
                        "hosted": hosted_delta,
                        "native_predictions": prediction_delta,
                    },
                    "inspection_samples": len(inspections),
                    "inspection_p95_ms": p95 * 1000,
                    "inspection_max_ms": max(inspections) * 1000,
                    "checks": checks,
                    "passed": all(checks.values()),
                    "baseline_state_version": baseline["state_version"],
                    "final_state_version": final["state_version"],
                    "transport": final["transport"],
                    "router_manifest": final["router_manifest"],
                    "hosted_calls": final.get("model_calls", []),
                    "diagnostics": final["diagnostics"],
                    "configuration_sha256": runtime.configuration_sha256,
                }
            )
        except Exception as error:
            report.update(
                {
                    "passed": False,
                    "error_code": getattr(error, "code", type(error).__name__),
                    "inference_counts_at_failure": inference_counts(runtime),
                }
            )
        finally:
            await runtime.close()
    report["completed_at"] = utcnow().isoformat()
    write_private(output, report)
    print(
        json.dumps(
            {
                "report": str(output),
                "passed": report["passed"],
                "error_code": report.get("error_code"),
                "checks": report.get("checks"),
                "inspection_p95_ms": report.get("inspection_p95_ms"),
            }
        ),
        flush=True,
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    arguments = argparse.ArgumentParser(description=__doc__)
    arguments.add_argument(
        "--output", type=Path, default=Path("outputs/live/idle.json")
    )
    try:
        code = asyncio.run(run(arguments.parse_args().output))
    except KeyboardInterrupt:
        code = 130
    except Exception as error:
        print({"error": getattr(error, "code", type(error).__name__)}, file=sys.stderr)
        code = 1
    raise SystemExit(code)
