"""Context packing regressions exercise the actual immutable capture path."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from live_gate_contexts import build_case

from signal_reducers.config import Config
from signal_reducers.models import canonical, public
from signal_reducers.providers import DrexGate, OpenRouterClient


@pytest.mark.parametrize(
    "name",
    [
        "integrated_diagnosis",
        "integrated_result_review",
        "integrated_recovery",
        "revision_terminal_context",
        "unsubmitted_context",
    ],
)
async def test_complete_multi_domain_groups_fit_without_source_truncation(name):
    config = Config()
    async with OpenRouterClient(config, "synthetic") as client:
        gate = DrexGate(client)
        store, named = await build_case(name, gate, config)
        batch = await store.create_batch(gate.request_size)
        before = canonical(public(batch))
        body = gate.request(batch)
        assert len(canonical(body)) <= 12_000
        captured = {key: body["state"][key] for key in public(batch)}
        captured["records"] = [
            {
                key: value
                for key, value in record.items()
                if key not in ("gate_target", "gate_flags")
            }
            for record in captured["records"]
        ]
        assert captured == public(batch)
        assert canonical(public(batch)) == before
        if name == "integrated_diagnosis":
            assert len(batch.records) == 5
            assert set(
                named[key].id
                for key in (
                    "health",
                    "health_event",
                    "request",
                    "request_snapshot",
                    "video",
                )
            ) == {r.id for r in batch.records}
            assert batch.deferred["new"] == 0 and batch.deferred["unresolved"] == 0
        if name == "integrated_result_review":
            result = named["synthetic_result"]
            assert {r.id for r in batch.records} >= set(
                result.target_ids + result.evidence_ids
            )
            assert batch.results == (result,)
        if name == "revision_terminal_context":
            assert len(batch.records) == 3
            targets = [
                record for record in body["state"]["records"] if "gate_target" in record
            ]
            assert len(targets) == 3
            for record in targets:
                if record["gate_flags"]["terminal"]:
                    index = record["gate_target"][1:]
                    assert "basis_" + index not in body["questions"]
        await store.finish_batch(batch)
