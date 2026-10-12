"""Offline evidence-integrity checks; these synthetic reports make no live claims."""

import copy
import importlib.util
import json
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "live_audit", Path(__file__).parents[1] / "scripts/audit_live_workflows.py"
)
live_audit = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(live_audit)


def saved_run(folder, *, run_id="synthetic-run"):
    def record(oid, kind, code, seq, status=None):
        telemetry = status is not None
        return {
            "observation": {
                "id": oid,
                "reducer_id": "telemetry" if telemetry else "conversation",
                "subject_id": "sensor/demo.worker" if telemetry else "chat/request",
                "subject_seq": seq,
                "kind": kind,
                "code": code,
                "finality": "committed",
                "content": "Synthetic evidence",
                "facts": {"service": "demo.worker", "status": status}
                if telemetry
                else {"submitted": True},
                "evidence_refs": [{"signal_id": "synthetic"}],
            },
            "consumption": "acknowledged",
            "resolution": "no_action_required",
        }

    healthy = record("healthy", "snapshot", "telemetry.status", 1, "ok")
    degraded = record("degraded", "snapshot", "telemetry.status", 2, "degraded")
    degraded["resolution"] = "resolved"
    recovered = record("recovered", "snapshot", "telemetry.status", 3, "ok")
    question = record("question", "event", "conversation.message", 1)
    question["resolution"] = "resolved"
    records = [healthy, degraded, recovered, question]
    operation = {
        "id": "operation",
        "action": "escalate",
        "batch_id": "batch",
        "context_version": 10,
        "target_ids": ["question"],
        "initiator_id": "question",
        "terminal": True,
        "thinking_level": "some",
    }
    result = {
        "id": "result",
        "type": "system2",
        "operation_id": "operation",
        "batch_id": "batch",
        "context_version": 10,
        "target_ids": ["question"],
        "status": "success",
        "applicable": True,
        "content": "Synthetic bounded answer; root cause unknown.",
        "data": {"findings": []},
        "evidence_ids": ["degraded"],
    }
    calls = [
        {
            "component": "reducer_telemetry",
            "model": live_audit.MODELS["reducer_telemetry"],
            "usage": {"cost": 0.001},
        },
        {
            "component": "system1",
            "model": live_audit.MODELS["system1"],
            "usage": {"cost": 0.001},
        },
        {
            "component": "system2",
            "model": live_audit.MODELS["system2"],
            "thinking_level": "some",
            "usage": {"cost": 0.01},
            "diagnostic": {
                "captured_batch_id": "batch",
                "context_version": 10,
                "target_ids": ["question"],
                "captured_observations": [
                    {
                        key: degraded["observation"][key]
                        for key in ("id", "code", "reducer_id", "facts")
                    }
                ],
                "completion_budget_present": False,
                "tools_present": False,
                "reasoning": {"effort": "low", "exclude": True},
            },
        },
    ]
    counts = {"reducer_telemetry": 1, "system1": 1, "system2": 1}
    router = dict(live_audit.LAYA)
    checks = {name: True for name in live_audit.REQUIRED}
    summary = {
        "run_id": run_id,
        "mode": "live",
        "passed": True,
        "live_scenario_passed": True,
        "checks": checks,
        "lifecycle_violations": 0,
        "lifecycle_violation_scope": "evaluated_scenario_checks",
        "lifecycle_check_coverage": {
            "checked": list(live_audit.LIFECYCLE),
            "not_checked": [],
            "complete": True,
        },
        "timeline": list(live_audit.STAGES),
        "trace_truncated": False,
        "models": counts,
        "router_manifest": router,
        "configuration_sha256": "synthetic-configuration",
        "repeat_input_pattern": "paced_one_signal_per_second",
    }
    state = {
        "run_id": run_id,
        "mode": "live",
        "configuration_sha256": "synthetic-configuration",
        "records": records,
        "operations": [operation],
        "results": [result],
        "reviewed_result_ids": ["result"],
        "model_calls": calls,
        "models": counts,
        "router_manifest": router,
        "transport": {"expired": 0},
    }
    trace = []
    for index, name in enumerate(live_audit.STAGES):
        projected = []
        for source in records:
            obs = source["observation"]
            if (
                index == 0
                and obs["id"] != "healthy"
                or index < 5
                and obs["id"] == "recovered"
            ):
                continue
            item = {
                key: obs[key]
                for key in (
                    "id",
                    "code",
                    "reducer_id",
                    "subject_id",
                    "subject_seq",
                    "finality",
                )
            }
            item.update(
                {
                    "status": obs["facts"].get("status"),
                    "consumption": source["consumption"],
                    "resolution": "unresolved"
                    if index < 5 and obs["id"] == "degraded"
                    else source["resolution"],
                }
            )
            projected.append(item)
        trace.append(
            {
                "stage": name,
                "observation_lifecycle": projected,
                "models": {"reducer_telemetry": 1, "system1": 1}
                if index == 0
                else counts,
            }
        )
    write_run(folder, summary, state, trace)
    return summary, state, trace


def write_run(folder, summary, state, trace):
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "summary.json").write_text(json.dumps(summary))
    (folder / "final-state.json").write_text(json.dumps(state))
    (folder / "trace.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in trace)
    )


def test_positive_audit_and_missing_request_evidence_are_distinct(tmp_path):
    summary, state, trace = saved_run(tmp_path)
    assert live_audit.audit(tmp_path, 1, None)["passed"]
    del state["model_calls"][-1]["diagnostic"]["tools_present"]
    write_run(tmp_path, summary, state, trace)
    report = live_audit.audit(tmp_path, 1, None)
    assert not report["passed"]
    assert "system2_no_tools" in report["runs"][0]["missing_checks"]


@pytest.mark.parametrize(
    "fault,expected",
    [
        ("premature_health", "no_sampled_pre_recovery_health_resolution"),
        ("wrong_evidence", "evidence_ids_captured_and_retained"),
        ("wrong_context", "http_operation_correlation"),
        ("wrong_model", "configured_models_preserved"),
        ("extra_attempt", "exactly_one_system2_operation_result_attempt"),
        ("budget", "system2_no_completion_budget"),
    ],
)
def test_audit_rejects_tampered_successful_reports(tmp_path, fault, expected):
    summary, state, trace = saved_run(tmp_path)
    if fault == "premature_health":
        next(
            item
            for item in trace[3]["observation_lifecycle"]
            if item["id"] == "degraded"
        )["resolution"] = "resolved"
    elif fault == "wrong_evidence":
        state["results"][0]["evidence_ids"] = ["outside-captured-context"]
    elif fault == "wrong_context":
        state["model_calls"][-1]["diagnostic"]["context_version"] = 11
    elif fault == "wrong_model":
        state["model_calls"][-1]["model"] = "unregistered/model"
    elif fault == "extra_attempt":
        state["model_calls"].append(copy.deepcopy(state["model_calls"][-1]))
    elif fault == "budget":
        state["model_calls"][-1]["diagnostic"]["completion_budget_present"] = True
    write_run(tmp_path, summary, state, trace)
    report = live_audit.audit(tmp_path, 1, None)
    assert not report["passed"]
    assert expected in report["runs"][0]["failed_checks"]


def test_nine_of_ten_rule_retains_failed_run_without_changing_it(tmp_path):
    for index in range(1, 11):
        folder = tmp_path / f"run-{index:02d}"
        summary, state, trace = saved_run(folder, run_id=f"synthetic-{index}")
        if index == 10:
            summary["checks"]["no_scheduler_errors"] = False
            summary["passed"] = summary["live_scenario_passed"] = False
            write_run(folder, summary, state, trace)
    failed = tmp_path / "run-10" / "summary.json"
    before = failed.read_bytes()
    report = live_audit.audit(tmp_path, 10, None)
    assert report["passed"]
    assert (
        report["recorded_successful_runs"]
        == report["fully_audited_successful_runs"]
        == 9
    )
    assert len(report["runs"]) == 10
    assert failed.read_bytes() == before


def test_malformed_nested_report_is_failed_without_stopping_other_audits(tmp_path):
    summary, state, trace = saved_run(tmp_path)
    summary["lifecycle_check_coverage"] = None
    write_run(tmp_path, summary, state, trace)
    report = live_audit.audit(tmp_path, 1, None)
    assert not report["passed"]
    assert "well_formed_saved_evidence" in report["runs"][0]["failed_checks"]
