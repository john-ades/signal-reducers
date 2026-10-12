from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys
from pathlib import Path
from uuid import UUID, uuid4

from . import __version__
from .config import load_config
from .demo import dedicated_redis, scenario, write_private
from .models import ContractError, SignalInput, public
from .providers import DrexGate, OpenRouterClient, OpenRouterReducer, OpenRouterSystem2
from .router import LayaClassifier, evaluate_router
from .runtime import Runtime
from .transport import SignalQueue


def parser():
    root = argparse.ArgumentParser(description="Signal Reducers P0")
    root.add_argument(
        "--version", action="version", version=f"signal-reducers {__version__}"
    )
    sub = root.add_subparsers(dest="command", required=True)
    for command in ("check", "run", "publish", "demo", "evaluate-router"):
        part = sub.add_parser(command)
        part.add_argument("--config", type=Path)
        part.add_argument("--redis-url")
        if command in ("check", "run", "demo"):
            part.add_argument("--mode", choices=("fixture", "live"))
        if command in ("run", "publish"):
            part.add_argument("--run-id", type=UUID, required=True)
        if command == "check":
            part.add_argument("--probe-models", action="store_true")
        elif command == "publish":
            part.add_argument("--file", type=Path, required=True)
        elif command == "demo":
            part.add_argument("--output", type=Path, default=Path("outputs/demo"))
            part.add_argument("--runs", type=int, default=1, choices=range(1, 11))
        elif command == "evaluate-router":
            part.add_argument("--dataset", type=Path)
            part.add_argument(
                "--split", choices=("development", "heldout"), default="heldout"
            )
            part.add_argument(
                "--output", type=Path, default=Path("outputs/router-evaluation.json")
            )
    return root


def workflow_qualification(reports, requested_runs, mode):
    successes = sum(report["passed"] for report in reports)
    required = 9 if requested_runs == 10 else requested_runs
    violations = sum(report["lifecycle_violations"] for report in reports)
    passed = (
        len(reports) == requested_runs and successes >= required and violations == 0
    )
    return {
        "mode": mode,
        "requested_runs": requested_runs,
        "completed_runs": len(reports),
        "successful_runs": successes,
        "required_successes": required,
        "lifecycle_violations": violations,
        "zero_lifecycle_violations": violations == 0,
        "passed": passed,
        "qualification_scope": "workflows_only",
        "router_qualification": "Separate passing held-out evaluate-router report required.",
    }


async def readiness(config, probe=False, *, client_factory=None):
    result = {"mode": config.mode, "python": sys.version.split()[0], "ready": True}
    queue = SignalQueue(uuid4(), config)
    try:
        result["redis"] = await queue.check()
    except Exception as exc:
        result["redis"] = {
            "ready": False,
            "error": getattr(exc, "code", "redis_unavailable"),
        }
        result["ready"] = False
    finally:
        await queue.close()
    if config.mode == "live":
        result["openrouter_key_present"] = bool(os.environ.get("OPENROUTER_API_KEY"))
        result["openrouter_authenticated"] = False
        result["ready"] &= result["openrouter_key_present"]
        classifier = LayaClassifier(config)
        client = None
        try:
            client = (client_factory or OpenRouterClient)(config)
            await client.check_authentication()
            result["openrouter_authenticated"] = True
            result["models"] = await client.check_models()
            if probe:
                # Explicitly requested inference probes are bounded and use synthetic data.
                from datetime import timedelta

                from .models import (
                    Batch,
                    EscalationRequest,
                    Observation,
                    Record,
                    canonical,
                    utcnow,
                )

                await classifier.prepare()
                result["laya"] = classifier.manifest
                await OpenRouterReducer(client).summarize(
                    "telemetry", {"service": "probe", "status": "ok"}
                )
                observation = Observation(
                    reducer_id="telemetry",
                    subject_id="probe/worker",
                    subject_seq=1,
                    kind="snapshot",
                    finality="committed",
                    code="telemetry.status",
                    content="Synthetic healthy probe.",
                    facts_json=canonical({"service": "probe", "status": "ok"}),
                )
                batch = Batch(
                    run_id=uuid4(),
                    state_version=1,
                    input_generation=1,
                    records=(Record(observation=observation, accepted_version=1),),
                    new_ids=(observation.id,),
                )
                await DrexGate(client).evaluate(batch)
                for level in ("none", "some", "extra"):
                    response = await OpenRouterSystem2(client).execute(
                        EscalationRequest(
                            id=uuid4(),
                            batch_id=batch.id,
                            run_id=batch.run_id,
                            context_version=0,
                            target_ids=(observation.id,),
                            reason="Describe this synthetic probe using only supplied evidence.",
                            thinking_level=level,
                            context_json=canonical(public(batch)),
                            deadline_at=utcnow()
                            + timedelta(seconds=config.system2_timeout),
                        )
                    )
                    if response.status not in ("success", "partial"):
                        raise ContractError("system2_probe_failed")
                result["probes_passed"] = True
            else:
                result["inference_qualified"] = False
                result["note"] = (
                    "Use --probe-models for inference checks; evaluate-router for held-out CPU qualification."
                )
        except Exception as exc:
            result["ready"] = False
            result["model_error"] = getattr(exc, "code", type(exc).__name__)
        finally:
            if client:
                await client.close()
    return result


async def execute(args, *, runtime_factory=None):
    config = load_config(
        args.config, redis_url=args.redis_url, mode=getattr(args, "mode", None)
    )
    if args.command == "check":
        result = await readiness(config, args.probe_models)
        print(json.dumps(result, indent=2))
        return 0 if result["ready"] else 1
    if args.command == "evaluate-router":
        report = await evaluate_router(config, args.dataset, split=args.split)
        write_private(args.output, report)
        print(json.dumps(report, indent=2))
        return 0 if report["quality_pass"] and report["performance_pass"] else 1
    if args.command == "publish":
        if args.file.stat().st_size > 8 * 1024 * 1024:
            raise ContractError("publisher_file_too_large")
        data = json.loads(args.file.read_bytes())
        if not isinstance(data, list):
            raise ContractError("publisher_requires_array")
        if len(data) > 100:
            raise ContractError("publisher_batch_capacity")
        signals = []
        for row in data:
            if not isinstance(row, dict) or ("payload_json" in row) == (
                "payload_base64" in row
            ):
                raise ContractError("publisher_payload_encoding_required")
            row = dict(row)
            row["payload"] = (
                row.pop("payload_json")
                if "payload_json" in row
                else base64.b64decode(row.pop("payload_base64"), validate=True)
            )
            signals.append(SignalInput(**row))
        queue = SignalQueue(args.run_id, config)
        try:
            receipts = await queue.publish(signals)
            print(json.dumps({"outcome": "accepted", "receipts": public(receipts)}))
        finally:
            await queue.close(release=False)
        return 0
    if args.command == "demo":
        reports = []

        async def run_all(active_config):
            for index in range(args.runs):
                destination = (
                    args.output
                    if args.runs == 1
                    else args.output / f"run-{index + 1:02d}"
                )
                report = await scenario(
                    active_config, destination, runtime_factory=runtime_factory
                )
                reports.append(report)
                print(
                    json.dumps(
                        {
                            "run": index + 1,
                            "mode": active_config.mode,
                            "passed": report["passed"],
                            "checks": report["checks"],
                        }
                    )
                )
                if not report["passed"] and any(
                    "key" in str(v) for v in report["checks"].values()
                ):
                    break

        if args.redis_url:
            await run_all(config)
        else:
            with dedicated_redis() as url:
                await run_all(config.model_copy(update={"redis_url": url}))
        qualification = workflow_qualification(reports, args.runs, config.mode)
        if args.runs > 1:
            write_private(args.output / "qualification.json", qualification)
        return 0 if qualification["passed"] else 1
    runtime = (runtime_factory or Runtime)(args.run_id, config)
    try:
        await runtime.start()
        print(
            json.dumps(
                {
                    "ready": True,
                    "run_id": str(args.run_id),
                    "mode": config.mode,
                    "commands": ["status", "recheck", "quit"],
                }
            ),
            flush=True,
        )
        if sys.stdin.isatty():
            loop = asyncio.get_running_loop()
            commands = asyncio.Queue(maxsize=32)

            def read_command():
                line = sys.stdin.readline()
                if not line:
                    loop.remove_reader(sys.stdin)
                    runtime.stopped.set()
                elif not commands.full():
                    commands.put_nowait(line.strip())

            loop.add_reader(sys.stdin, read_command)
            try:
                while not runtime.stopped.is_set():
                    try:
                        command = await asyncio.wait_for(commands.get(), timeout=0.5)
                    except TimeoutError:
                        continue
                    if command == "quit":
                        break
                    if command == "recheck":
                        await runtime.recheck()
                    elif command == "status":
                        print(json.dumps(await runtime.inspect()), flush=True)
            finally:
                loop.remove_reader(sys.stdin)
        else:
            await runtime.stopped.wait()
        return 1 if runtime.fatal_error else 0
    finally:
        await runtime.close()


def main():
    args = parser().parse_args()
    try:
        code = asyncio.run(execute(args))
    except KeyboardInterrupt:
        code = 130
    except Exception as exc:
        # Do not expose provider bodies, request payloads, URLs with secrets, or credentials.
        code = 1
        print(
            json.dumps(
                {
                    "outcome": getattr(exc, "outcome", "failed"),
                    "error": getattr(
                        exc,
                        "code",
                        "invalid_input"
                        if isinstance(exc, ValueError)
                        else type(exc).__name__,
                    ),
                }
            ),
            file=sys.stderr,
        )
    raise SystemExit(code)
