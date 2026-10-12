"""Test a real 100-report burst with CPU Laya and hosted reducer/gate models."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from uuid import uuid4

from _live_support import load_live_key

from signal_reducers.config import Config
from signal_reducers.demo import dedicated_redis, records, telemetry, write_private
from signal_reducers.models import public
from signal_reducers.runtime import Runtime


async def run(output):
    source = load_live_key()
    report = {
        "credential_source": source,
        "credentials_logged": False,
        "input_pattern": "100_report_burst",
        "scope": "Actual CPU Laya and live OpenRouter duplicate suppression; transport losses remain visible.",
    }
    with dedicated_redis() as redis_url:
        runtime = Runtime(uuid4(), Config(mode="live", redis_url=redis_url))
        try:
            await runtime.start()
            start = time.monotonic()
            receipts = await runtime.queue.publish(
                [telemetry(i) for i in range(1, 101)]
            )
            report["admitted"] = len(receipts)
            deadline = start + 180
            progress_at = start
            while time.monotonic() < deadline:
                view = await runtime.inspect()
                if view["fatal_error"]:
                    report["error_code"] = view["fatal_error"]
                    break
                snapshots = records(view, "telemetry.status")
                if (
                    view["transport"]["popped"] == 100
                    and len(snapshots) == 1
                    and all(r["consumption"] == "acknowledged" for r in snapshots)
                ):
                    break
                if time.monotonic() - progress_at >= 20:
                    print(
                        json.dumps(
                            {
                                "elapsed_seconds": round(time.monotonic() - start, 1),
                                "popped": view["transport"]["popped"],
                                "expired": view["transport"]["expired"],
                            }
                        ),
                        flush=True,
                    )
                    progress_at = time.monotonic()
                await asyncio.sleep(0.1)
            else:
                report["error_code"] = "burst_condition_timeout"
            await asyncio.gather(
                *(worker.join() for worker in runtime.workers.values())
            )
            report["elapsed_seconds"] = round(time.monotonic() - start, 3)
        except Exception as error:
            report["error_code"] = getattr(error, "code", type(error).__name__)
        finally:
            await runtime.close()
        view = await runtime.inspect()
        snapshots = records(view, "telemetry.status")
        checks = {
            "hundred_admitted": report.get("admitted") == 100,
            "hundred_popped": view["transport"]["popped"] == 100,
            "one_snapshot": len(snapshots) == 1,
            "one_telemetry_inference": view["models"].get("reducer_telemetry", 0) == 1,
            "zero_system2_inferences": view["models"].get("system2", 0) == 0,
            "at_most_three_system1_evaluations": runtime.scheduler.calls <= 3,
            "zero_rejected_gate_evaluations": runtime.scheduler.failures == 0,
            "snapshot_acknowledged": len(snapshots) == 1
            and snapshots[0]["consumption"] == "acknowledged",
        }
        report.update(
            {
                "checks": checks,
                "passed": all(checks.values()) and "error_code" not in report,
                "transport": view["transport"],
                "models": view["models"],
                "calls": view.get("model_calls", []),
                "router_manifest": view["router_manifest"],
                "router": view["router"],
                "state": public(view),
                "signal_max_age_seconds": runtime.config.signal_max_age,
            }
        )
    write_private(output, report)
    print(
        json.dumps(
            {
                "report": str(output),
                "passed": report["passed"],
                "checks": checks,
                "transport": report["transport"],
            }
        ),
        flush=True,
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    arguments = argparse.ArgumentParser(description=__doc__)
    arguments.add_argument(
        "--output", type=Path, default=Path("outputs/live/burst.json")
    )
    try:
        code = asyncio.run(run(arguments.parse_args().output))
    except KeyboardInterrupt:
        code = 130
    except Exception as exc:
        print({"error": getattr(exc, "code", type(exc).__name__)}, file=sys.stderr)
        code = 1
    raise SystemExit(code)
