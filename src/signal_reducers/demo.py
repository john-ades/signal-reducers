"""A complete simulator scenario with explicit fixture/live evidence."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from .config import Config
from .models import ContractError, SignalInput, canonical
from .runtime import Runtime


def write_private(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as file:
        os.fchmod(file.fileno(), 0o600)
        json.dump(value, file, ensure_ascii=False, indent=2, allow_nan=False)
        file.write("\n")


@contextmanager
def dedicated_redis():
    binary = shutil.which("redis-server")
    if not binary:
        raise ContractError("redis_server_not_installed")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with tempfile.TemporaryDirectory(prefix="signal-reducers-redis-") as directory:
        process = subprocess.Popen(
            [
                binary,
                "--bind",
                "127.0.0.1",
                "--port",
                str(port),
                "--save",
                "",
                "--appendonly",
                "no",
                "--maxmemory",
                "256mb",
                "--maxmemory-policy",
                "noeviction",
                "--dir",
                directory,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise ContractError("redis_start_failed")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.02)
            else:
                raise ContractError("redis_start_timeout")
            yield f"redis://127.0.0.1:{port}/0"
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def telemetry(
    seq, status="ok", *, source="demo.telemetry", service="demo.worker", error=0.01
):
    return SignalInput(
        source=source,
        source_seq=seq,
        modality="structured",
        mimetype="application/json",
        payload={
            "type": "telemetry",
            "service": service,
            "status": status,
            "metrics": {
                "error_rate": error,
                "queue_depth": 1 if status == "ok" else 50,
            },
        },
    )


def conversation(seq, event, text="", *, message_id="diagnosis", source="demo.user"):
    return SignalInput(
        source=source,
        source_seq=seq,
        modality="structured",
        mimetype="application/json",
        payload={
            "type": "conversation",
            "message_id": message_id,
            "event": event,
            "text": text,
        },
    )


def video(seq, scene, revision=None):
    return SignalInput(
        source="demo.video",
        source_seq=seq,
        modality="video",
        mimetype="application/vnd.signal-reducers.sim-video+json",
        payload=canonical(
            {"service": "demo.worker", "scene": scene, "revision": revision}
        ),
    )


def records(view, code=None):
    return [
        r for r in view["records"] if code is None or r["observation"]["code"] == code
    ]


async def scenario(
    config: Config, output: Path, *, condition_timeout=180, runtime_factory=None
) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    runtime = (runtime_factory or Runtime)(uuid4(), config)
    timeline = []
    checks = {}
    view = None
    trace_limit = 10 * 1024 * 1024
    trace_path = output / "trace.jsonl"
    trace_descriptor = os.open(trace_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(trace_descriptor, 0o600)
    trace = os.fdopen(trace_descriptor, "w")
    trace_bytes = 0
    trace_truncated = False

    def system2_attempts():
        if runtime.client:
            return runtime.client.counters["system2"]
        return runtime.system2_model.calls

    def note(name, snapshot):
        nonlocal trace_bytes, trace_truncated
        event = {
            "stage": name,
            "state_version": snapshot["state_version"],
            "observations": len(snapshot["records"]),
            "activity": snapshot["activity"],
            "results": snapshot["results"],
            "models": snapshot["models"],
            "observation_lifecycle": [
                {
                    "id": record["observation"]["id"],
                    "code": record["observation"]["code"],
                    "reducer_id": record["observation"]["reducer_id"],
                    "subject_id": record["observation"]["subject_id"],
                    "subject_seq": record["observation"]["subject_seq"],
                    "status": record["observation"]["facts"].get("status"),
                    "finality": record["observation"]["finality"],
                    "consumption": record["consumption"],
                    "resolution": record["resolution"],
                }
                for record in snapshot["records"]
            ],
        }
        timeline.append(name)
        line = json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"
        size = len(line.encode())
        if trace_bytes + size <= trace_limit:
            trace.write(line)
            trace.flush()
            trace_bytes += size
        else:
            trace_truncated = True

    async def wait(name, predicate):
        deadline = asyncio.get_running_loop().time() + condition_timeout
        while asyncio.get_running_loop().time() < deadline:
            snapshot = await runtime.inspect()
            if runtime.fatal_error:
                raise ContractError(runtime.fatal_error)
            if name == "diagnosis-returned-to-gate":
                failed = next(
                    (
                        result
                        for result in snapshot["results"]
                        if result["type"] == "system2" and result["status"] != "success"
                    ),
                    None,
                )
                if failed:
                    raise ContractError(
                        "demo_system2_result:"
                        + failed["status"]
                        + ":"
                        + (failed.get("error_code") or "incomplete_answer")
                    )
            if predicate(snapshot):
                note(name, snapshot)
                return snapshot
            await asyncio.sleep(0.025)
        raise ContractError("demo_condition_timeout:" + name)

    try:
        async with asyncio.timeout(600):
            await runtime.start()
            if config.mode == "live":
                # The accepted CPU target is one signal/second. Keep the live
                # workflow within transient transport age limits; fixture mode
                # separately checks admission and suppression of a real burst.
                for index in range(1, 101):
                    await runtime.queue.publish([telemetry(index)])
                    if index < 100:
                        await asyncio.sleep(1)
            else:
                await runtime.queue.publish([telemetry(i) for i in range(1, 101)])
            initial = await wait(
                "healthy-repeat-burst",
                lambda v: (
                    v["transport"]["popped"] >= 100
                    and len(records(v, "telemetry.status")) == 1
                    and all(
                        r["consumption"] == "acknowledged"
                        for r in records(v, "telemetry.status")
                    )
                ),
            )
            checks["repeat_one_snapshot"] = (
                len(records(initial, "telemetry.status")) == 1
            )
            checks["repeat_no_system2"] = (
                system2_attempts() == 0 and not initial["results"]
            )
            checks["repeat_no_expiry"] = initial["transport"]["expired"] == 0
            checks["repeat_one_reducer_call"] = (
                runtime.workers["telemetry"].counters["model_calls"] == 1
            )
            checks["repeat_bounded_gate_calls"] = runtime.scheduler.calls <= 3

            if config.mode == "live":
                await runtime.queue.publish([telemetry(101, "degraded", error=0.45)])
                await wait(
                    "degradation-reviewed",
                    lambda v: any(
                        r["observation"]["facts"].get("status") == "degraded"
                        and r["resolution"] == "unresolved"
                        for r in records(v, "telemetry.health_changed")
                    ),
                )
                # A complete segment has a one-second intake window. Routing
                # and hosted decisions may take longer than that window.
                await runtime.queue.publish([video(i, "stalled") for i in range(1, 4)])
            else:
                await runtime.queue.publish(
                    [telemetry(101, "degraded", error=0.45), video(1, "stalled")]
                )
            degraded = await wait(
                "degraded-with-buffering-video",
                lambda v: (
                    any(
                        r["observation"]["facts"].get("status") == "degraded"
                        and r["resolution"] == "unresolved"
                        for r in records(v, "telemetry.health_changed")
                    )
                    and (
                        config.mode == "fixture"
                        or any(
                            a["reducer_id"] == "simulated_video"
                            and a["status"] == "buffering"
                            for a in v["activity"]
                        )
                    )
                ),
            )
            checks["pending_video_visible"] = any(
                a["reducer_id"] == "simulated_video" and a["status"] == "buffering"
                for a in degraded["activity"]
            )
            complete_video = (
                []
                if config.mode == "live"
                else [video(2, "stalled"), video(3, "stalled")]
            )
            await runtime.queue.publish(
                [
                    *complete_video,
                    conversation(1, "replace", "Explain why "),
                    conversation(2, "append", "demo.worker is degraded."),
                    conversation(3, "submit"),
                ]
            )
            diagnosed = await wait(
                "diagnosis-returned-to-gate",
                lambda v: any(
                    r["resolution"] == "resolved"
                    for r in records(v, "conversation.message")
                ),
            )
            checks["question_resolved"] = True
            service_evidence_ids = {
                r["observation"]["id"]
                for r in diagnosed["records"]
                if r["observation"]["reducer_id"] == "telemetry"
                and r["observation"]["facts"].get("service") == "demo.worker"
                and r["observation"]["facts"].get("status") in ("degraded", "down")
                and r["observation"]["evidence_refs"]
            }
            question_ids = {
                r["observation"]["id"]
                for r in records(diagnosed, "conversation.message")
                if r["resolution"] == "resolved"
            }
            checks["diagnosis_grounded_in_service_evidence"] = any(
                result["type"] == "system2"
                and result["status"] == "success"
                and result["applicable"]
                and question_ids.intersection(result["target_ids"])
                and service_evidence_ids.intersection(result["evidence_ids"])
                for result in diagnosed["results"]
            )
            checks["diagnosis_does_not_resolve_health"] = all(
                r["resolution"] == "unresolved"
                for r in records(diagnosed, "telemetry.health_changed")
                if r["observation"]["facts"].get("status") == "degraded"
            )
            checks["single_system2_operation"] = (
                len([r for r in diagnosed["results"] if r["type"] == "system2"]) == 1
            )
            await wait(
                "video-completed", lambda v: bool(records(v, "simulation.scene"))
            )

            await runtime.queue.publish([telemetry(102)])
            await wait(
                "recovery-evidence-reviewed",
                lambda v: all(
                    r["resolution"] == "resolved"
                    for r in records(v, "telemetry.health_changed")
                    if r["observation"]["facts"].get("status") == "degraded"
                ),
            )
            checks["health_resolved_after_recovery"] = True

            await runtime.queue.publish([video(i, "reflection") for i in range(4, 7)])
            await wait(
                "scene-corrected",
                lambda v: len(records(v, "simulation.scene_corrected")) == 2,
            )
            await runtime.queue.publish(
                [video(i, "clear", "retract") for i in range(7, 10)]
            )
            revised = await wait(
                "scene-retracted",
                lambda v: len(records(v, "simulation.scene_retracted")) == 2,
            )
            checks["correction_retraction_are_new_records"] = (
                len(records(revised, "simulation.scene_corrected")) == 2
            )

            # A source gap cannot produce a submitted request or authorize an operation.
            gap_failures = runtime.workers["conversation"].counters[
                "conversation_incomplete"
            ]
            await runtime.queue.publish(
                [
                    conversation(
                        1, "replace", "Incomplete ", source="demo.gap", message_id="gap"
                    ),
                    conversation(
                        3, "append", "question", source="demo.gap", message_id="gap"
                    ),
                    conversation(4, "submit", source="demo.gap", message_id="gap"),
                ]
            )
            await wait(
                "missing-fragment-visible",
                lambda v: (
                    v["reducers"]["conversation"].get("conversation_incomplete", 0)
                    > gap_failures
                ),
            )
            await runtime.workers["conversation"].join()
            checks["gap_cannot_commit"] = not any(
                r["observation"]["facts"].get("message_id") == "gap"
                for r in records(await runtime.inspect(), "conversation.message")
            )
            if config.mode == "fixture":
                runtime.reducer_model.fail_next = "telemetry"
                prior_count = len(records(await runtime.inspect(), "telemetry.status"))
                await runtime.queue.publish([telemetry(103, "down", error=0.99)])
                failed = await wait(
                    "model-failure-visible",
                    lambda v: v["reducers"]["telemetry"].get("failed", 0) > 0,
                )
                checks["failure_preserves_prior_observation"] = (
                    len(records(failed, "telemetry.status")) == prior_count
                )
            else:
                checks["failure_injection"] = "covered by deterministic fault tests"
            view = await runtime.inspect()
            checks["no_scheduler_errors"] = runtime.scheduler.failures == 0
    except Exception as exc:
        checks["scenario_error"] = getattr(exc, "code", type(exc).__name__)
        view = await runtime.inspect()
    finally:
        await runtime.close()
        view = await runtime.inspect()
        trace.close()
    if "single_system2_operation" in checks:
        checks["single_system2_operation"] = (
            len(
                [
                    operation
                    for operation in view["operations"]
                    if operation["action"] == "escalate"
                ]
            )
            == 1
            and len(
                [result for result in view["results"] if result["type"] == "system2"]
            )
            == 1
        )
        checks["single_system2_http_attempt"] = system2_attempts() == 1
    passed = all(
        value is True or isinstance(value, str) and value.startswith("covered")
        for value in checks.values()
    )
    lifecycle_checks = (
        "diagnosis_does_not_resolve_health",
        "gap_cannot_commit",
        "single_system2_operation",
        "single_system2_http_attempt",
    )
    violations = sum(checks.get(name) is False for name in lifecycle_checks)
    checked = [name for name in lifecycle_checks if name in checks]
    missing = [name for name in lifecycle_checks if name not in checks]
    summary = {
        "schema_version": "1.0",
        "mode": config.mode,
        "run_id": str(runtime.run_id),
        "passed": passed,
        "live_scenario_passed": config.mode == "live" and passed,
        "lifecycle_violations": violations,
        "lifecycle_violation_scope": "evaluated_scenario_checks",
        "lifecycle_check_coverage": {
            "checked": checked,
            "not_checked": missing,
            "complete": not missing,
        },
        "repeat_input_pattern": "paced_one_signal_per_second"
        if config.mode == "live"
        else "100_signal_burst",
        "checks": checks,
        "timeline": timeline,
        "trace_truncated": trace_truncated,
        "models": view.get("models", {}) if view else {},
        "configuration_sha256": runtime.configuration_sha256,
        "router_manifest": view.get("router_manifest", {}) if view else {},
        "qualification": "One scenario; ten independent live runs and held-out routing qualification are separate requirements.",
    }
    write_private(output / "summary.json", summary)
    write_private(output / "final-state.json", view or {})
    return summary
