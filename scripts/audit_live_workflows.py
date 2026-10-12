"""Independently audit saved live workflows without changing runs or making calls.

The audit checks recorded scenario stages and request/result correlation. It
does not prove every possible interleaving or grade generated prose accuracy.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from collections import Counter
from pathlib import Path

from signal_reducers.demo import write_private
from signal_reducers.models import utcnow

MODELS = {
    "reducer_conversation": "openai/gpt-4.1-mini",
    "reducer_telemetry": "openai/gpt-4.1-mini",
    "reducer_simulated_video": "openai/gpt-4.1-mini",
    "system1": "nace-ai/drex-v1.5",
    "system2": "openai/gpt-5.1",
}
LAYA = {
    "model": "convaiinnovations/laya-typed-decisions",
    "revision": "e929ae5cf69bc34259cd2f95c9e91145b818b1f0",
    "device": "cpu",
    "threads": 4,
    "max_len": 8192,
    "head_max_len": 256,
    "checkpoint_sha256": "4fa56de72383a9d3efa9cfa78955733c81b9fc8067a587ca4beb82c78107a24e",
}
STAGES = (
    "healthy-repeat-burst",
    "degradation-reviewed",
    "degraded-with-buffering-video",
    "diagnosis-returned-to-gate",
    "video-completed",
    "recovery-evidence-reviewed",
    "scene-corrected",
    "scene-retracted",
    "missing-fragment-visible",
)
REQUIRED = (
    "repeat_one_snapshot",
    "repeat_no_system2",
    "repeat_no_expiry",
    "repeat_one_reducer_call",
    "repeat_bounded_gate_calls",
    "pending_video_visible",
    "question_resolved",
    "diagnosis_grounded_in_service_evidence",
    "diagnosis_does_not_resolve_health",
    "single_system2_operation",
    "health_resolved_after_recovery",
    "correction_retraction_are_new_records",
    "gap_cannot_commit",
    "no_scheduler_errors",
    "single_system2_http_attempt",
)
LIFECYCLE = (
    "diagnosis_does_not_resolve_health",
    "gap_cannot_commit",
    "single_system2_operation",
    "single_system2_http_attempt",
)


def load_object(path: Path):
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError("expected_json_object")
    return value


def check(report, name, condition, *, missing=False, detail=None):
    report["checks"][name] = {
        "status": "missing" if missing else "pass" if condition else "fail"
    }
    if detail is not None:
        report["checks"][name]["detail"] = detail


def costs(calls):
    available = [c.get("usage", {}).get("cost") for c in calls]
    values = [
        value
        for value in available
        if isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    ]
    return {
        "provider_reported_cost": sum(values),
        "calls_with_reported_cost": len(values),
        "calls_without_reported_cost": len(calls) - len(values),
    }


def audit_run(path: Path, baseline_router: dict | None):
    report = {
        "run_directory": str(path),
        "checks": {},
        "scope": "Recorded scenario stages and invariant checks; prose requires manual review.",
    }
    files = (path / "summary.json", path / "final-state.json", path / "trace.jsonl")
    absent = [p.name for p in files if not p.is_file()]
    if absent:
        check(report, "required_run_files", False, missing=True, detail=absent)
        return finish_run(report), baseline_router
    try:
        summary, state = load_object(files[0]), load_object(files[1])
        trace = [
            json.loads(line)
            for line in files[2].read_text().splitlines()
            if line.strip()
        ]
        if any(not isinstance(stage, dict) for stage in trace):
            raise ValueError("invalid_trace")
        report["run_id"] = summary.get("run_id")
        report["recorded_workflow_passed"] = summary.get("passed") is True
        recorded = summary.get("checks", {})
        if not isinstance(recorded, dict):
            raise ValueError("invalid_summary_checks")
        required_absent = [name for name in REQUIRED if name not in recorded]
        required_failed = [
            name for name in REQUIRED if name in recorded and recorded[name] is not True
        ]
        check(
            report,
            "required_scenario_assertions",
            not required_absent and not required_failed,
            missing=bool(required_absent),
            detail={"missing": required_absent, "failed": required_failed},
        )
        computed_pass = not required_absent and all(
            value is True or isinstance(value, str) and value.startswith("covered")
            for value in recorded.values()
        )
        check(
            report,
            "summary_pass_matches_recorded_assertions",
            summary.get("passed") is computed_pass,
        )
        check(
            report,
            "live_run_identity",
            summary.get("mode") == state.get("mode") == "live"
            and summary.get("run_id") == state.get("run_id")
            and summary.get("live_scenario_passed") == (summary.get("passed") is True),
        )
        check(
            report,
            "configuration_identity",
            summary.get("configuration_sha256") == state.get("configuration_sha256"),
        )

        coverage = summary.get("lifecycle_check_coverage", {})
        checked = [name for name in LIFECYCLE if name in recorded]
        missing = [name for name in LIFECYCLE if name not in recorded]
        check(
            report,
            "lifecycle_coverage_is_explicit",
            coverage.get("checked") == checked
            and coverage.get("not_checked") == missing
            and coverage.get("complete") is (not missing),
            missing=not bool(coverage),
        )
        violations = sum(recorded.get(name) is False for name in LIFECYCLE)
        report["recorded_lifecycle_violations"] = summary.get("lifecycle_violations")
        report["recomputed_assertion_violations"] = violations
        check(
            report,
            "lifecycle_violation_count_consistent",
            summary.get("lifecycle_violations") == violations
            and summary.get("lifecycle_violation_scope") == "evaluated_scenario_checks",
        )
        check(
            report,
            "all_lifecycle_assertions_observed",
            not missing,
            missing=bool(missing),
            detail=missing,
        )

        stages = [entry.get("stage") for entry in trace]
        check(
            report,
            "all_required_stages_in_order",
            stages == list(STAGES),
            missing=any(name not in stages for name in STAGES),
            detail=stages,
        )
        check(
            report,
            "summary_trace_consistent",
            summary.get("timeline") == stages
            and summary.get("trace_truncated") is False,
        )
        lifecycle_available = bool(trace) and all(
            isinstance(entry.get("observation_lifecycle"), list) for entry in trace
        )
        check(
            report,
            "stage_lifecycle_projection_available",
            lifecycle_available,
            missing=not lifecycle_available,
        )
        by_stage = {entry.get("stage"): entry for entry in trace}
        healthy = by_stage.get("healthy-repeat-burst")
        if healthy and lifecycle_available:
            snapshots = [
                r
                for r in healthy["observation_lifecycle"]
                if r.get("code") == "telemetry.status"
            ]
            model_counts = healthy.get("models", {})
            check(
                report,
                "repeat_suppression_observed",
                len(snapshots) == 1
                and snapshots[0].get("consumption") == "acknowledged"
                and model_counts.get("reducer_telemetry", 0) == 1
                and model_counts.get("system2", 0) == 0
                and model_counts.get("system1", 0) <= 3,
            )
        else:
            check(report, "repeat_suppression_observed", False, missing=True)
        check(
            report,
            "live_repeat_pattern_disclosed",
            summary.get("repeat_input_pattern") == "paced_one_signal_per_second",
        )
        check(
            report,
            "live_transport_no_expiry",
            state.get("transport", {}).get("expired") == 0,
        )

        records = {
            record["observation"]["id"]: record for record in state.get("records", [])
        }
        source_health = {
            oid: record
            for oid, record in records.items()
            if record["observation"].get("reducer_id") == "telemetry"
            and record["observation"].get("facts", {}).get("service") == "demo.worker"
            and record["observation"]["facts"].get("status") in ("degraded", "down")
            and record["observation"].get("evidence_refs")
        }
        recovery = by_stage.get("recovery-evidence-reviewed")
        if recovery and lifecycle_available:
            recovery_index = stages.index("recovery-evidence-reviewed")
            premature = [
                {"stage": entry["stage"], "id": item.get("id")}
                for entry in trace[:recovery_index]
                for item in entry["observation_lifecycle"]
                if item.get("id") in source_health
                and item.get("resolution") == "resolved"
            ]
            observed = {
                item.get("id")
                for entry in trace[:recovery_index]
                for item in entry["observation_lifecycle"]
                if item.get("id") in source_health
            }
            report["sampled_pre_recovery_violations"] = len(premature)
            check(
                report,
                "no_sampled_pre_recovery_health_resolution",
                bool(source_health)
                and set(source_health) <= observed
                and not premature,
                detail={
                    "premature_resolution_samples": premature,
                    "observed_health_ids": sorted(observed),
                },
            )
            recovery_items = {
                item["id"]: item for item in recovery["observation_lifecycle"]
            }
            unsupported = []
            for oid in source_health:
                item = recovery_items.get(oid)
                healthy_evidence = item and any(
                    other.get("subject_id") == item.get("subject_id")
                    and other.get("subject_seq", 0) > item.get("subject_seq", 0)
                    and other.get("status") == "ok"
                    and other.get("finality") == "committed"
                    and records.get(other.get("id"), {})
                    .get("observation", {})
                    .get("kind")
                    == "snapshot"
                    for other in recovery_items.values()
                )
                if (
                    not item
                    or item.get("resolution") != "resolved"
                    or not healthy_evidence
                ):
                    unsupported.append(oid)
            check(
                report,
                "sampled_recovery_has_newer_source_health",
                bool(source_health) and not unsupported,
                detail=unsupported,
            )
        else:
            check(
                report, "no_sampled_pre_recovery_health_resolution", False, missing=True
            )
            check(
                report, "sampled_recovery_has_newer_source_health", False, missing=True
            )

        operations = state.get("operations", [])
        results = [
            result
            for result in state.get("results", [])
            if result.get("type") == "system2"
        ]
        escalations = [
            operation
            for operation in operations
            if operation.get("action") == "escalate"
        ]
        calls = state.get("model_calls", [])
        system2_calls = [call for call in calls if call.get("component") == "system2"]
        report["actual_http_call_counts"] = dict(
            Counter(call.get("component", "<missing>") for call in calls)
        )
        report.update(costs(calls))
        report["system2_operation_count"] = len(escalations)
        report["system2_result_count"] = len(results)
        report["system2_http_attempt_count"] = len(system2_calls)
        check(
            report,
            "recorded_model_call_counts_consistent",
            report["actual_http_call_counts"]
            == state.get("models")
            == summary.get("models"),
        )
        check(
            report,
            "configured_models_preserved",
            bool(calls)
            and all(
                call.get("component") in MODELS
                and call.get("model") == MODELS[call["component"]]
                for call in calls
            ),
        )
        initiators = [operation.get("initiator_id") for operation in operations]
        check(
            report,
            "no_duplicate_operation_initiators",
            None not in initiators and len(initiators) == len(set(initiators)),
        )
        check(
            report,
            "exactly_one_system2_operation_result_attempt",
            len(escalations) == len(results) == len(system2_calls) == 1,
        )
        if len(escalations) == len(results) == len(system2_calls) == 1:
            operation, result, http = escalations[0], results[0], system2_calls[0]
            diagnostic = http.get("diagnostic", {})
            report["diagnosis_for_manual_review"] = {
                "content": result.get("content"),
                "findings": result.get("data", {}).get("findings"),
                "cited_observations": [
                    {"id": oid, "observation": records.get(oid, {}).get("observation")}
                    for oid in result.get("evidence_ids", [])
                ],
                "automated_prose_accuracy_claimed": False,
            }
            check(
                report,
                "result_operation_correlation",
                result.get("operation_id") == operation.get("id")
                and result.get("batch_id") == operation.get("batch_id")
                and result.get("context_version") == operation.get("context_version")
                and result.get("target_ids") == operation.get("target_ids")
                and operation.get("terminal") is True,
            )
            required_http = ("captured_batch_id", "context_version", "target_ids")
            absent_http = [name for name in required_http if name not in diagnostic]
            check(
                report,
                "http_operation_correlation",
                not absent_http
                and diagnostic.get("captured_batch_id") == operation.get("batch_id")
                and diagnostic.get("context_version")
                == operation.get("context_version")
                and diagnostic.get("target_ids") == operation.get("target_ids"),
                missing=bool(absent_http),
                detail=absent_http,
            )
            captured = diagnostic.get("captured_observations")
            captured_ids = (
                {item.get("id") for item in captured}
                if isinstance(captured, list)
                else set()
            )
            evidence = set(result.get("evidence_ids", []))
            check(
                report,
                "evidence_ids_captured_and_retained",
                isinstance(captured, list)
                and evidence <= captured_ids
                and evidence <= set(records)
                and len(evidence) == len(result.get("evidence_ids", [])),
                missing=not isinstance(captured, list),
            )
            captured_health = {
                item.get("id")
                for item in captured or []
                if item.get("reducer_id") == "telemetry"
                and item.get("facts", {}).get("service") == "demo.worker"
                and item["facts"].get("status") in ("degraded", "down")
            }
            check(
                report,
                "diagnosis_cites_captured_source_health",
                result.get("status") == "success"
                and result.get("applicable") is True
                and bool(evidence & captured_health & set(source_health)),
            )
            check(
                report,
                "captured_facts_match_immutable_observations",
                isinstance(captured, list)
                and all(
                    item.get("id") in records
                    and item.get("facts")
                    == records[item["id"]]["observation"].get("facts")
                    and item.get("code")
                    == records[item["id"]]["observation"].get("code")
                    and item.get("reducer_id")
                    == records[item["id"]]["observation"].get("reducer_id")
                    for item in captured
                ),
                missing=not isinstance(captured, list),
            )
            check(
                report,
                "system2_no_completion_budget",
                diagnostic.get("completion_budget_present") is False,
                missing="completion_budget_present" not in diagnostic,
            )
            check(
                report,
                "system2_no_tools",
                diagnostic.get("tools_present") is False,
                missing="tools_present" not in diagnostic,
            )
            level = operation.get("thinking_level")
            check(
                report,
                "system2_thinking_matches_gate",
                level in ("none", "some", "extra")
                and http.get("thinking_level") == level
                and diagnostic.get("reasoning")
                == {
                    "effort": {"none": "none", "some": "low", "extra": "high"}.get(
                        level
                    ),
                    "exclude": True,
                },
            )
            check(
                report,
                "successful_result_reviewed_by_gate",
                result.get("id") in state.get("reviewed_result_ids", []),
            )
            check(
                report,
                "initiator_is_resolved_submitted_event",
                operation.get("initiator_id") in operation.get("target_ids", [])
                and records.get(operation.get("initiator_id"), {}).get("resolution")
                == "resolved"
                and records.get(operation.get("initiator_id"), {})
                .get("observation", {})
                .get("code")
                == "conversation.message"
                and records.get(operation.get("initiator_id"), {})
                .get("observation", {})
                .get("kind")
                == "event"
                and records.get(operation.get("initiator_id"), {})
                .get("observation", {})
                .get("finality")
                == "committed",
            )

        router = summary.get("router_manifest", {})
        check(
            report,
            "router_summary_matches_final_state",
            router == state.get("router_manifest"),
        )
        check(
            report,
            "pinned_cpu_laya_configuration",
            all(router.get(key) == value for key, value in LAYA.items()),
        )
        if baseline_router is None and all(
            router.get(key) == value for key, value in LAYA.items()
        ):
            baseline_router = router
        check(
            report,
            "router_manifest_unchanged_across_runs",
            baseline_router is not None and router == baseline_router,
            missing=baseline_router is None,
        )
    except (KeyError, TypeError, ValueError, OSError, AttributeError) as error:
        check(report, "well_formed_saved_evidence", False, detail=type(error).__name__)
    return finish_run(report), baseline_router


def finish_run(report):
    report["failed_checks"] = [
        name for name, item in report["checks"].items() if item["status"] == "fail"
    ]
    report["missing_checks"] = [
        name for name, item in report["checks"].items() if item["status"] == "missing"
    ]
    report["audit_complete"] = not report["missing_checks"]
    report["audited_success"] = (
        report.get("recorded_workflow_passed", False)
        and not report["failed_checks"]
        and not report["missing_checks"]
    )
    return report


def audit(folder: Path, expected_runs: int, manifest: Path | None):
    paths = sorted(folder.glob("run-[0-9][0-9]"))
    if not paths and (folder / "summary.json").exists():
        paths = [folder]
    runs, baseline = [], None
    for path in paths:
        item, baseline = audit_run(path, baseline)
        runs.append(item)
    required = 9 if expected_runs == 10 else expected_runs
    recorded_successes = sum(run.get("recorded_workflow_passed", False) for run in runs)
    audited_successes = sum(run["audited_success"] for run in runs)
    violations = sum(run.get("recomputed_assertion_violations", 0) for run in runs)
    sampled_violations = sum(
        run.get("sampled_pre_recovery_violations", 0) for run in runs
    )
    run_ids = [run.get("run_id") for run in runs]
    unique_runs = None not in run_ids and len(run_ids) == len(set(run_ids))
    report = {
        "schema_version": "1.0",
        "audited_at": utcnow().isoformat(),
        "input_folder": str(folder),
        "scope": "Saved scenario assertions, sampled stage lifecycle states, immutable captured context and request/result correlation. No exhaustive lifecycle or automated prose-accuracy claim.",
        "expected_runs": expected_runs,
        "found_runs": len(runs),
        "required_successes": required,
        "unique_run_identities": unique_runs,
        "recorded_successful_runs": recorded_successes,
        "fully_audited_successful_runs": audited_successes,
        "recorded_assertion_violations": violations,
        "sampled_pre_recovery_violations": sampled_violations,
        "provider_reported_cost": sum(
            run.get("provider_reported_cost", 0) for run in runs
        ),
        "actual_http_call_counts": dict(
            sum(
                (Counter(run.get("actual_http_call_counts", {})) for run in runs),
                Counter(),
            )
        ),
        "runs": runs,
        "failed_runs_preserved": True,
    }
    source_ok = True
    if manifest:
        try:
            frozen = load_object(manifest)
            checks = {
                name: Path(name).is_file()
                and hashlib.sha256(Path(name).read_bytes()).hexdigest() == digest
                for name, digest in frozen.get("source_sha256", {}).items()
            }
            source_ok = bool(checks) and all(checks.values())
            report["current_source_manifest_check"] = {
                "path": str(manifest),
                "matches": source_ok,
                "mismatched_paths": [
                    name for name, matched in checks.items() if not matched
                ],
                "scope": "Current source matches frozen record; does not independently prove which executable produced a saved run.",
            }
        except OSError, ValueError, TypeError, AttributeError:
            source_ok = False
            report["current_source_manifest_check"] = {
                "path": str(manifest),
                "matches": False,
                "error": "manifest_unavailable_or_invalid",
            }
    report["passed"] = (
        len(runs) == expected_runs
        and unique_runs
        and audited_successes >= required
        and violations == 0
        and sampled_violations == 0
        and source_ok
    )
    return report


if __name__ == "__main__":
    arguments = argparse.ArgumentParser(description=__doc__)
    arguments.add_argument("folder", type=Path)
    arguments.add_argument(
        "--expected-runs", type=int, choices=range(1, 11), default=10
    )
    arguments.add_argument("--manifest", type=Path)
    arguments.add_argument("--output", type=Path)
    try:
        args = arguments.parse_args()
        report = audit(args.folder, args.expected_runs, args.manifest)
        destination = args.output or args.folder / "audit.json"
        evidence_paths = {
            path.resolve()
            for path in args.folder.rglob("*")
            if path.name
            in ("summary.json", "final-state.json", "trace.jsonl", "qualification.json")
        }
        if destination.resolve() in evidence_paths:
            raise ValueError("audit_output_may_not_replace_run_evidence")
        write_private(destination, report)
        print(
            json.dumps(
                {
                    "audit": str(destination),
                    "passed": report["passed"],
                    "found_runs": report["found_runs"],
                    "recorded_successful_runs": report["recorded_successful_runs"],
                    "fully_audited_successful_runs": report[
                        "fully_audited_successful_runs"
                    ],
                    "provider_reported_cost": report["provider_reported_cost"],
                }
            ),
            flush=True,
        )
        code = 0 if report["passed"] else 1
    except Exception as error:
        print({"error": type(error).__name__}, file=sys.stderr)
        code = 1
    raise SystemExit(code)
