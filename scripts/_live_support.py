"""Explicit credential loading for user-authorized live verification only."""

from __future__ import annotations

import hashlib
import os
import shlex
import sys
from pathlib import Path

from signal_reducers import __version__
from signal_reducers.models import utcnow


def request_controls(body: dict) -> dict:
    """Expose control flags, never prompt text, credentials or reasoning output."""
    return {
        "reasoning": body.get("reasoning"),
        "completion_budget_present": any(
            field in body for field in ("max_tokens", "max_completion_tokens")
        )
        or any(
            field in body.get("reasoning", {})
            for field in ("max_tokens", "max_completion_tokens")
        ),
        "tools_present": "tools" in body,
    }


def source_manifest() -> dict:
    """Hash the current executable inputs without reading any credentials."""
    root = Path(__file__).resolve().parents[1]
    paths = [
        *root.glob("src/**/*.py"),
        *root.glob("scripts/*.py"),
        root / "src/signal_reducers/data/routing.jsonl",
        root / "pyproject.toml",
        root / "uv.lock",
        root / "config.example.toml",
    ]
    return {
        "schema_version": "1.0",
        "recorded_at": utcnow().isoformat(),
        "version": __version__,
        "python": sys.version.split()[0],
        "credentials_logged": False,
        "source_sha256": {
            str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(paths)
            if path.is_file()
        },
    }


def load_live_key(path: Path = Path(".env")) -> str:
    values = {}
    if path.is_file():
        for line in path.read_text().splitlines():
            name, separator, value = line.strip().removeprefix("export ").partition("=")
            if separator and name.strip() in ("OPENROUTER_KEY", "OPENROUTER_API_KEY"):
                parts = shlex.split(value, comments=True)
                if len(parts) != 1 or not parts[0]:
                    raise ValueError("invalid_dotenv_key_assignment")
                values[name.strip()] = parts[0]
    for name in ("OPENROUTER_KEY", "OPENROUTER_API_KEY"):
        if values.get(name):
            os.environ["OPENROUTER_API_KEY"] = values[name]
            return "dotenv:" + name
    if os.environ.get("OPENROUTER_API_KEY"):
        return "environment:OPENROUTER_API_KEY"
    if os.environ.get("OPENROUTER_KEY"):
        os.environ["OPENROUTER_API_KEY"] = os.environ["OPENROUTER_KEY"]
        return "environment:OPENROUTER_KEY"
    raise ValueError("openrouter_key_missing")
