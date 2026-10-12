"""A serial CPU decision router with independent reducer destinations."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import resource
import sys
import time
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Any, Protocol

from .config import Config
from .models import REDUCER_IDS, ContractError, Delivery, canonical, public
from .providers import ProviderError, choice_question, validate_choice

PINNED_LAYA_REVISION = "e929ae5cf69bc34259cd2f95c9e91145b818b1f0"
REGISTRY = {
    "conversation": "User message text, conversation replace/append/submit fragments, questions, or requests.",
    "telemetry": "Structured service status and numeric health metrics, including partial or changed telemetry reports.",
    "simulated_video": "Synthetic video packets identified by application/vnd.signal-reducers.sim-video+json; no real media decoding.",
}
QUESTIONS = {
    "conversation": choice_question(
        "Classify the current signal, not past signals. Which description is correct?",
        {
            "A": "User conversational message or question",
            "B": "Service health report, video packet, media data or inventory",
        },
    ),
    "telemetry": choice_question(
        "Which input is the current signal?",
        {
            "A": "Numeric error_rate and queue_depth values in a telemetry report",
            "B": "User conversation, fragment or question about service health or processing; image, audio, video, binary or inventory",
        },
    ),
    "simulated_video": choice_question(
        "Classify the current signal, not past signals. Which description is correct?",
        {
            "A": "Simulated video packet",
            "B": "Conversational message or question, service health report, opaque media data or inventory",
        },
    ),
}
for _rid, _question in QUESTIONS.items():
    _question["instructions"] += " Registered reducer: " + REGISTRY[_rid]

INPUT_PROJECTION = {
    "version": "1.0",
    "envelope": "all metadata, including received_at; omit payload values",
    "fields": [
        "signal",
        "payload_bytes",
        "preview",
        "preview_truncated",
        "recent_same_source_decisions",
    ],
    "json_preview_utf8_bytes": 2048,
    "incomplete_utf8_suffix": "omit",
    "binary_preview": "empty",
    "preview_truncated": "true only for JSON payloads exceeding the preview limit",
    "same_source_history": 5,
    "history_source_capacity": 128,
    "registry_location": "per-question instructions",
}


def input_projection_hash() -> str:
    return hashlib.sha256(
        canonical(
            {
                "projection": INPUT_PROJECTION,
                "questions": QUESTIONS,
                "registry": REGISTRY,
            }
        )
    ).hexdigest()


class Classifier(Protocol):
    def predict(self, state: dict, questions: dict) -> dict: ...


def _digest(path: Path | Traversable) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            value.update(block)
    return value.hexdigest()


class LayaClassifier:
    def __init__(self, config: Config, *, agent: Any = None):
        self.config = config
        self.agent = agent
        self.manifest: dict = {}
        self._prepare_lock = asyncio.Lock()

    async def prepare(self):
        async with self._prepare_lock:
            if self.agent is None:
                await asyncio.to_thread(self._load)

    def _load(self):
        import laya
        import torch
        from huggingface_hub import snapshot_download

        revision = self.config.laya_revision
        if revision is None:
            if self.config.laya_model != "convaiinnovations/laya-typed-decisions":
                raise ContractError("router_revision_required")
            revision = PINNED_LAYA_REVISION
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise ContractError("router_revision_must_be_commit")
        torch.set_num_threads(self.config.laya_threads)
        directory = Path(
            snapshot_download(
                self.config.laya_model,
                revision=revision,
                allow_patterns=[
                    "rl_agent_config.json",
                    "model.safetensors",
                    "encoder/*",
                    "tokenizer/*",
                ],
            )
        )
        self.agent = laya.load(
            str(directory), device="cpu", backend="eager", compile=False, fast=False
        )
        if str(self.agent.device) != "cpu" or any(
            parameter.device.type != "cpu"
            for parameter in self.agent.model.parameters()
        ):
            self.agent = None
            raise ContractError("router_not_cpu")
        files = {
            str(path.relative_to(directory)): _digest(path)
            for path in sorted(directory.rglob("*"))
            if path.is_file()
        }
        self.manifest = {
            "model": self.config.laya_model,
            "revision": revision,
            "device": "cpu",
            "threads": self.config.laya_threads,
            "max_len": self.config.laya_max_len,
            "head_max_len": self.config.laya_head_max_len,
            "artifacts": files,
            "checkpoint_sha256": files.get("model.safetensors"),
            "tokenizer_sha256": hashlib.sha256(
                canonical(
                    {k: v for k, v in files.items() if k.startswith("tokenizer/")}
                )
            ).hexdigest(),
            "configuration_sha256": hashlib.sha256(
                canonical(
                    {
                        "checkpoint": self.agent.cfg,
                        "max_len": self.config.laya_max_len,
                        "head_max_len": self.config.laya_head_max_len,
                        "device": "cpu",
                        "threads": self.config.laya_threads,
                    }
                )
            ).hexdigest(),
            "prompt_sha256": hashlib.sha256(canonical(QUESTIONS)).hexdigest(),
            "registry_sha256": hashlib.sha256(canonical(REGISTRY)).hexdigest(),
            "input_projection_sha256": input_projection_hash(),
        }

    def preflight(self, state: dict, questions: dict):
        if self.agent is None:
            raise ContractError("router_not_prepared")
        # Drive the checkpoint's actual formatter/tokenizer, including its layout.
        # This private boundary is pinned to laya==0.4.3 and covered by a real-model probe.
        internal = {
            key: self.agent._to_internal(question)
            for key, question in questions.items()
        }
        try:
            rows = self.agent._encode_state(
                state,
                list(questions),
                internal,
                max_len=self.config.laya_max_len,
                head_max_len=self.config.laya_head_max_len,
            )
        except ValueError:
            raise ContractError("router_input_capacity") from None
        from laya.common import _encode_question_text, render_options

        for (key, question), row in zip(internal.items(), rows, strict=True):
            if (
                row["state_stats"]["truncated"]
                or row["state_stats"]["state_tokens_dropped"]
            ):
                raise ContractError("router_input_truncated")
            if (
                row["options"]["options_distinct"] != row["options"]["options"]
                or len(row["markers"]) != 2
            ):
                raise ContractError("router_collapsed_options")
            # build_head also slices instruction/option text. Refuse those cuts,
            # even when the surviving options happen to remain distinguishable.
            instruction_ids = _encode_question_text(
                self.agent.tok,
                "choice question: " + question["ins"],
                add_special_tokens=False,
            )
            option_lengths = [
                len(
                    _encode_question_text(
                        self.agent.tok, " " + option, add_special_tokens=False
                    )
                )
                for option in render_options(question)
            ]
            if any(length > 48 for length in option_lengths):
                raise ContractError("router_option_truncated")
            head_room = self.config.laya_head_max_len - sum(
                length + 1 for length in option_lengths
            )
            if head_room < 16 or len(instruction_ids) > max(8, head_room):
                raise ContractError("router_question_truncated")

    def predict(self, state: dict, questions: dict) -> dict:
        self.preflight(state, questions)
        result = self.agent.system_one(
            state,
            questions,
            max_len=self.config.laya_max_len,
            head_max_len=self.config.laya_head_max_len,
        )
        usage = result.get("usage", {})
        if usage.get("truncated") or usage.get("state_tokens_dropped"):
            raise ContractError("router_input_truncated")
        if usage.get("options"):
            raise ContractError("router_collapsed_options")
        return result


class FixtureClassifier:
    """Explicit offline fixture adapter; never used as a fallback for live routing."""

    manifest = {"model": "fixture", "device": "fixture", "quality_qualified": False}

    async def prepare(self):
        return None

    def predict(self, state: dict, questions: dict) -> dict:
        signal = state["signal"]
        try:
            payload = json.loads(state["preview"]) if state["preview"] else None
        except json.JSONDecodeError:
            payload = None
        members = [payload]
        if isinstance(payload, dict) and payload.get("type") == "bundle":
            members.extend(
                value
                for key, value in payload.items()
                if key in ("conversation", "telemetry") and isinstance(value, dict)
            )
        conversation = signal["modality"] == "text" or any(
            isinstance(item, dict) and item.get("type") == "conversation"
            for item in members
        )
        telemetry = any(
            isinstance(item, dict) and item.get("type") == "telemetry"
            for item in members
        )
        video = (
            signal["modality"] == "video"
            and signal["mimetype"] == "application/vnd.signal-reducers.sim-video+json"
        )
        values = dict(zip(REDUCER_IDS, (conversation, telemetry, video), strict=True))
        return {
            "answers": {
                key: {
                    "type": "choice",
                    "choice": "A" if values[key] else "B",
                    "confidence": 1.0,
                    "probabilities": {
                        "A": float(values[key]),
                        "B": float(not values[key]),
                    },
                }
                for key in questions
            },
            "usage": {"truncated": False},
        }


class SignalRouter:
    def __init__(self, classifier: Classifier, config: Config):
        self.classifier = classifier
        self.config = config
        self.history: OrderedDict[str, list[dict]] = OrderedDict()
        self.executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="signal-router"
        )
        self._lock = asyncio.Lock()
        self.healthy = True
        self.counters: Counter = Counter()
        self.latencies: list[float] = []

    @property
    def manifest(self):
        return getattr(self.classifier, "manifest", {})

    @property
    def unhealthy(self):
        return not self.healthy

    async def prepare(self):
        method = getattr(self.classifier, "prepare", None)
        if method:
            await method()

    def input(self, delivery: Delivery) -> dict:
        signal = delivery.signal
        # Boundary truncation is intentionally limited to the documented payload
        # preview. The constructed classifier input itself must fit in full.
        preview = (
            signal.payload_bytes[:2048].decode("utf-8", errors="ignore")
            if signal.payload_encoding == "json"
            else ""
        )
        metadata = public(signal)
        metadata.pop("payload", None)
        metadata.pop("payload_base64", None)
        history = (
            [
                {
                    "source_seq": entry["source_seq"],
                    "destinations": list(entry["destinations"]),
                }
                for entry in self.history.get(signal.source, [])
            ]
            if signal.source
            else []
        )
        return {
            "signal": metadata,
            "payload_bytes": len(signal.payload_bytes),
            "preview": preview,
            "preview_truncated": signal.payload_encoding == "json"
            and len(signal.payload_bytes) > 2048,
            "recent_same_source_decisions": history,
        }

    async def route(self, delivery: Delivery) -> tuple[str, ...]:
        async with self._lock:
            if not self.healthy:
                raise ContractError("router_unhealthy")
            state = self.input(delivery)
            started = time.monotonic()
            future = asyncio.get_running_loop().run_in_executor(
                self.executor, self.classifier.predict, state, QUESTIONS
            )
            self.counters["calls"] += 1
            try:
                result = await asyncio.wait_for(
                    asyncio.shield(future), timeout=self.config.router_timeout
                )
                if (
                    not isinstance(result, dict)
                    or not isinstance(result.get("answers"), dict)
                    or set(result["answers"]) != set(REDUCER_IDS)
                ):
                    raise ProviderError("router_invalid_answers")
                destinations = []
                for rid in REDUCER_IDS:
                    answer = result["answers"][rid]
                    validate_choice(answer, QUESTIONS[rid]["criteria"])
                    if answer["probabilities"]["A"] >= 0.5:
                        destinations.append(rid)
            except TimeoutError:
                self.healthy = False
                self.counters["timeouts"] += 1
                # Python cannot stop a native inference thread. Do not pop or
                # schedule further signals; consume eventual exception silently.
                future.add_done_callback(
                    lambda f: f.exception() if not f.cancelled() else None
                )
                raise ContractError("router_timeout") from None
            except asyncio.CancelledError:
                self.healthy = False
                future.add_done_callback(
                    lambda f: f.exception() if not f.cancelled() else None
                )
                raise
            except Exception:
                self.counters["failures"] += 1
                raise
            finally:
                self.latencies.append(time.monotonic() - started)
                del self.latencies[:-1024]
            source = delivery.signal.source
            if source:
                history = self.history.setdefault(source, [])
                history.append(
                    {
                        "source_seq": delivery.signal.source_seq,
                        "destinations": destinations,
                    }
                )
                del history[:-5]
                self.history.move_to_end(source)
                while len(self.history) > 128:
                    self.history.popitem(last=False)
            self.counters["routed"] += 1
            if not destinations:
                self.counters["no_destinations"] += 1
            return tuple(destinations)

    async def close(self):
        self.healthy = False
        self.executor.shutdown(wait=False, cancel_futures=True)


async def evaluate_router(
    config: Config,
    path: str | Path | None = None,
    *,
    classifier: Classifier | None = None,
    split: str = "heldout",
) -> dict:
    dataset = (
        Path(path)
        if path
        else files("signal_reducers").joinpath("data", "routing.jsonl")
    )
    cases = [
        json.loads(line)
        for line in dataset.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    families = {
        name: {case["family"] for case in cases if case["split"] == name}
        for name in ("development", "heldout")
    }
    counts = Counter(case["split"] for case in cases)
    if (
        counts["development"] < 240
        or counts["heldout"] < 80
        or families["development"] & families["heldout"]
    ):
        raise ContractError("invalid_evaluation_split")
    if any(
        "demo" in case["family"] or "demo.worker" in canonical(case).decode()
        for case in cases
    ):
        raise ContractError("demo_contaminates_evaluation")
    from .models import SignalInput

    router = SignalRouter(classifier or LayaClassifier(config), config)
    await router.prepare()
    tp = Counter()
    fp = Counter()
    fn = Counter()
    exact = no_destination = no_destination_correct = total = 0
    failure_cases = []
    input_hash = hashlib.sha256()
    try:
        for case in cases:
            if case["split"] != split:
                continue
            signal_data = dict(case["signal"])
            if "payload_binary" in signal_data:
                signal_data["payload"] = signal_data.pop("payload_binary").encode()
            signal = SignalInput(**signal_data).accept()
            delivery = Delivery(signal=signal)
            input_hash.update(
                json.dumps(router.input(delivery), ensure_ascii=False).encode()
            )
            actual = set(await router.route(delivery))
            expected = set(case["destinations"])
            for rid in REDUCER_IDS:
                tp[rid] += int(rid in actual and rid in expected)
                fp[rid] += int(rid in actual and rid not in expected)
                fn[rid] += int(rid not in actual and rid in expected)
            total += 1
            exact += actual == expected
            if actual != expected:
                failure_cases.append(
                    {
                        "family": case["family"],
                        "signal_id": str(signal.id),
                        "source_seq": signal.source_seq,
                        "expected": sorted(expected),
                        "predicted": sorted(actual),
                    }
                )
            if not expected:
                no_destination += 1
                no_destination_correct += not actual
    finally:
        await router.close()
    recall = {rid: tp[rid] / max(1, tp[rid] + fn[rid]) for rid in REDUCER_IDS}
    precision = sum(tp.values()) / max(1, sum(tp.values()) + sum(fp.values()))
    exact_set = exact / max(1, total)
    specificity = no_destination_correct / max(1, no_destination)
    times = sorted(router.latencies)
    p95 = times[min(len(times) - 1, int(len(times) * 0.95))] if times else None
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        "split": split,
        "cases": total,
        "dataset_sha256": _digest(dataset),
        "manifest": router.manifest,
        "input_projection_sha256": input_projection_hash(),
        "captured_inputs_sha256": input_hash.hexdigest(),
        "process_peak_rss_bytes": rss if sys.platform == "darwin" else rss * 1024,
        "rss_scope": "whole process including checkpoint loading, interpreter and any test overhead",
        "process_platform": sys.platform,
        "history": "predicted_previous_decisions",
        "recall": recall,
        "micro_precision": precision,
        "confusion": {
            rid: {"tp": tp[rid], "fp": fp[rid], "fn": fn[rid]} for rid in REDUCER_IDS
        },
        "failure_cases": failure_cases,
        "exact_set_accuracy": exact_set,
        "no_destination_specificity": specificity,
        "p95_seconds": p95,
        "signals_per_second": total / max(0.000001, sum(times)),
        "quality_pass": all(r >= 0.95 for r in recall.values())
        and precision >= 0.90
        and exact_set >= 0.90
        and specificity >= 0.90,
        "performance_pass": total > 0 and total / max(0.000001, sum(times)) >= 1,
        "live_model": not isinstance(router.classifier, FixtureClassifier),
    }
