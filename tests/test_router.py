import asyncio
import time

import pytest

from signal_reducers.config import Config
from signal_reducers.models import ContractError, Delivery, SignalInput
from signal_reducers.router import (
    PINNED_LAYA_REVISION,
    QUESTIONS,
    FixtureClassifier,
    LayaClassifier,
    SignalRouter,
    evaluate_router,
)


def delivery(source="source", seq=1, payload="hello"):
    return Delivery(
        signal=SignalInput(
            modality="text", source=source, source_seq=seq, payload=payload
        ).accept()
    )


class FakeClassifier:
    def __init__(self, probabilities):
        self.probabilities = probabilities
        self.inputs = []

    def predict(self, state, questions):
        self.inputs.append(state)
        return {
            "answers": {
                rid: {
                    "type": "choice",
                    "choice": "A" if p >= 0.5 else "B",
                    "confidence": max(p, 1 - p),
                    "probabilities": {"A": p, "B": 1 - p},
                }
                for rid, p in zip(questions, self.probabilities, strict=True)
            }
        }


@pytest.mark.parametrize(
    "probabilities,expected",
    [
        ([0.1, 0.2, 0.3], ()),
        ([0.5, 0.1, 0.1], ("conversation",)),
        ([0.9, 0.9, 0.1], ("conversation", "telemetry")),
        ([1, 1, 1], ("conversation", "telemetry", "simulated_video")),
    ],
)
async def test_independent_probabilities_allow_zero_through_three(
    probabilities, expected
):
    router = SignalRouter(FakeClassifier(probabilities), Config())
    assert await router.route(delivery()) == expected
    assert len(QUESTIONS) == 3 and all(
        set(q["criteria"]) == {"A", "B"} for q in QUESTIONS.values()
    )
    await router.close()


async def test_preview_utf8_history_and_source_lru():
    classifier = FakeClassifier([0.9, 0.1, 0.1])
    router = SignalRouter(classifier, Config())
    for i in range(7):
        await router.route(delivery(seq=i + 1, payload="🙂" * 1000))
    state = classifier.inputs[-1]
    assert len(state["preview"].encode()) <= 2048
    assert state["preview_truncated"] is True
    assert state["signal"]["received_at"] and state["signal"]["id"]
    assert len(state["recent_same_source_decisions"]) == 5
    assert [h["source_seq"] for h in state["recent_same_source_decisions"]] == [
        2,
        3,
        4,
        5,
        6,
    ]
    await router.route(delivery(source=None))
    assert classifier.inputs[-1]["recent_same_source_decisions"] == []
    for i in range(130):
        await router.route(delivery(source="source" + str(i)))
    assert len(router.history) == 128 and "source" not in router.history
    await router.close()


async def test_invalid_probabilities_not_added_to_history():
    classifier = FakeClassifier([float("nan"), 0.1, 0.1])
    router = SignalRouter(classifier, Config())
    with pytest.raises(ContractError):
        await router.route(delivery())
    assert not router.history and router.healthy
    await router.close()


async def test_native_thread_timeout_stops_future_classification():
    class Slow(FakeClassifier):
        def predict(self, *args):
            time.sleep(0.05)
            return super().predict(*args)

    router = SignalRouter(Slow([1, 0, 0]), Config(router_timeout=0.005))
    with pytest.raises(ContractError, match="router_timeout"):
        await router.route(delivery())
    with pytest.raises(ContractError, match="router_unhealthy"):
        await router.route(delivery(seq=2))
    assert router.counters["calls"] == 1 and not router.history
    await asyncio.sleep(0.06)
    await router.close()


async def test_binary_payload_not_previewed_and_fixture_explicit():
    router = SignalRouter(FixtureClassifier(), Config(mode="fixture"))
    await router.prepare()
    value = Delivery(
        signal=SignalInput(
            modality="video",
            source="video",
            source_seq=1,
            mimetype="application/vnd.signal-reducers.sim-video+json",
            payload=b'{"service":"checkout","scene":"clear"}',
        ).accept()
    )
    assert router.input(value)["preview"] == ""
    assert router.input(value)["preview_truncated"] is False
    assert await router.route(value) == ("simulated_video",)
    assert router.manifest["quality_qualified"] is False
    await router.close()


async def test_family_separated_corpus_and_predicted_history_evaluation():
    result = await evaluate_router(
        Config(mode="fixture"), classifier=FixtureClassifier()
    )
    assert result["cases"] == 80 and result["quality_pass"]
    assert result["history"] == "predicted_previous_decisions"
    assert result["live_model"] is False
    assert result["input_projection_sha256"] and result["captured_inputs_sha256"]
    assert result["process_peak_rss_bytes"] > 0
    assert set(result["confusion"]) == set(QUESTIONS)


async def test_bundle_fanout_uses_explicit_fixture_only():
    router = SignalRouter(FixtureClassifier(), Config(mode="fixture"))
    value = Delivery(
        signal=SignalInput(
            source="mixed",
            source_seq=1,
            modality="mixed",
            payload={
                "type": "bundle",
                "conversation": {
                    "type": "conversation",
                    "message_id": "q",
                    "event": "replace",
                    "text": "Explain checkout.",
                },
                "telemetry": {
                    "type": "telemetry",
                    "service": "checkout",
                    "status": "degraded",
                    "metrics": {"error_rate": 0.2, "queue_depth": 40},
                },
            },
        ).accept()
    )
    assert await router.route(value) == ("conversation", "telemetry")
    await router.close()


@pytest.mark.parametrize(
    "fault,code",
    [
        ("state", "router_input_truncated"),
        ("options", "router_collapsed_options"),
        ("head", "router_question_truncated"),
    ],
)
def test_native_formatter_preflight_refuses_all_silent_cuts(monkeypatch, fault, code):
    import laya.common

    class Agent:
        tok = object()

        def _to_internal(self, question):
            return {
                "t": "choice",
                "ins": question["instructions"],
                "crit": question["criteria"],
            }

        def _encode_state(self, state, ids, internal, **kwargs):
            assert kwargs == {"max_len": 8192, "head_max_len": 256}
            return [
                {
                    "markers": [1, 3],
                    "options": {
                        "options": 2,
                        "options_distinct": 1 if fault == "options" else 2,
                    },
                    "state_stats": {
                        "truncated": fault == "state",
                        "state_tokens_dropped": int(fault == "state"),
                    },
                }
                for _ in ids
            ]

    monkeypatch.setattr(
        laya.common,
        "_encode_question_text",
        lambda tok, text, **kw: (
            list(range(300))
            if fault == "head" and text.startswith("choice question:")
            else [1]
        ),
    )
    with pytest.raises(ContractError, match=code):
        LayaClassifier(Config(), agent=Agent()).preflight({"signal": {}}, QUESTIONS)


async def test_checkpoint_load_pinned_once_on_cpu_four_threads(monkeypatch, tmp_path):
    import huggingface_hub
    import laya
    import torch

    (tmp_path / "model.safetensors").write_bytes(b"fixture weights")
    (tmp_path / "tokenizer").mkdir()
    (tmp_path / "tokenizer" / "tokenizer.json").write_text("{}")
    (tmp_path / "rl_agent_config.json").write_text("{}")
    calls = []

    class Model:
        def parameters(self):
            return [type("P", (), {"device": torch.device("cpu")})()]

    agent = type(
        "Agent", (), {"device": torch.device("cpu"), "model": Model(), "cfg": {}}
    )()

    def snapshot(repo, **kwargs):
        assert (
            repo == "convaiinnovations/laya-typed-decisions"
            and kwargs["revision"] == PINNED_LAYA_REVISION
        )
        assert "cache_dir" not in kwargs  # Honor Hugging Face's user cache/environment.
        return str(tmp_path)

    def load(path, **kwargs):
        assert kwargs == {
            "device": "cpu",
            "backend": "eager",
            "compile": False,
            "fast": False,
        }
        calls.append(path)
        return agent

    monkeypatch.setattr(huggingface_hub, "snapshot_download", snapshot)
    monkeypatch.setattr(laya, "load", load)
    monkeypatch.setattr(torch, "set_num_threads", lambda count: calls.append(count))
    classifier = LayaClassifier(Config())
    await asyncio.gather(classifier.prepare(), classifier.prepare())
    assert calls == [4, str(tmp_path)]
    assert classifier.manifest["revision"] == PINNED_LAYA_REVISION
    assert (
        classifier.manifest["device"] == "cpu"
        and classifier.manifest["checkpoint_sha256"]
    )
    assert (
        classifier.manifest["tokenizer_sha256"]
        and classifier.manifest["configuration_sha256"]
    )
    assert classifier.manifest["input_projection_sha256"]


def test_registered_descriptions_supplied_to_actual_model_questions():
    from signal_reducers.router import REGISTRY

    assert all(
        REGISTRY[key] in question["instructions"] for key, question in QUESTIONS.items()
    )
