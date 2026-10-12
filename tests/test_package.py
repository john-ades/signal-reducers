"""Distribution resources must work without a source checkout or model download."""

import hashlib
import json
from importlib.metadata import version
from importlib.resources import files

from signal_reducers import __version__
from signal_reducers.config import Config
from signal_reducers.router import FixtureClassifier, evaluate_router

DATASET_SHA256 = "041ec6daef323b8cb69df13758b5eadd694d5fc82e47d383b383c924114d33e0"


def test_version_comes_from_distribution_metadata():
    assert __version__ == version("signal-reducers")


def test_bundled_dataset_preserves_original_bytes_and_splits():
    data = files("signal_reducers").joinpath("data", "routing.jsonl").read_bytes()
    assert hashlib.sha256(data).hexdigest() == DATASET_SHA256
    rows = [json.loads(line) for line in data.decode("utf-8").splitlines() if line]
    assert len(rows) == 320
    assert sum(row["split"] == "development" for row in rows) == 240
    assert sum(row["split"] == "heldout" for row in rows) == 80


async def test_default_dataset_is_independent_of_working_directory(
    monkeypatch, tmp_path
):
    monkeypatch.chdir(tmp_path)
    report = await evaluate_router(
        Config(mode="fixture"), classifier=FixtureClassifier()
    )
    assert report["dataset_sha256"] == DATASET_SHA256
    assert report["cases"] == 80


async def test_custom_dataset_override_remains_supported(tmp_path):
    path = tmp_path / "custom-routing.jsonl"
    data = files("signal_reducers").joinpath("data", "routing.jsonl").read_bytes()
    path.write_bytes(data)
    report = await evaluate_router(
        Config(mode="fixture"),
        path,
        classifier=FixtureClassifier(),
        split="development",
    )
    assert report["dataset_sha256"] == DATASET_SHA256
    assert report["cases"] == 240
