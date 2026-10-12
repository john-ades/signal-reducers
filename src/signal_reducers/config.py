from __future__ import annotations

import os
import tomllib
from pathlib import Path

from pydantic import Field, model_validator

from .models import Frozen


class Config(Frozen):
    mode: str = "live"
    redis_url: str = "redis://127.0.0.1:6379/0"
    openrouter_url: str = "https://openrouter.ai/api/v1"
    reducer_model: str = "openai/gpt-4.1-mini"
    gate_model: str = "nace-ai/drex-v1.5"
    system2_model: str = "openai/gpt-5.1"
    laya_model: str = "convaiinnovations/laya-typed-decisions"
    laya_revision: str | None = None
    laya_device: str = "cpu"
    laya_threads: int = Field(default=4, ge=1)
    laya_max_len: int = Field(default=8192, ge=1024, le=8192)
    laya_head_max_len: int = Field(default=256, ge=64)
    router_timeout: float = Field(default=5, gt=0)
    reducer_timeout: float = Field(default=30, gt=0)
    gate_timeout: float = Field(default=30, gt=0)
    system2_timeout: float = Field(default=90, gt=0)
    queue_capacity: int = Field(default=1000, ge=1)
    queue_ttl: int = Field(default=600, ge=120)
    signal_max_age: float = Field(default=60, gt=0)
    mailbox_capacity: int = Field(default=100, ge=1)
    mailbox_bytes: int = Field(default=16 * 1024 * 1024, ge=200_000)
    max_subjects: int = Field(default=128, ge=1)
    state_records: int = Field(default=10_000, ge=32)
    state_bytes: int = Field(default=32 * 1024 * 1024, ge=24_000)
    batch_bytes: int = Field(default=12_000, ge=2048)
    operation_capacity: int = Field(default=128, ge=1)
    result_capacity: int = Field(default=128, ge=1)
    debounce: float = Field(default=0.1, gt=0)
    max_debounce: float = Field(default=0.5, gt=0)
    wait_followup: float = Field(default=5, gt=0)
    retry_delay: float = Field(default=1, gt=0)
    video_delay: float = Field(default=0.5, ge=0)
    video_window: float = Field(default=1, gt=0)
    draft_flush: float = Field(default=0.5, gt=0)
    draft_expiry: float = Field(default=5, gt=0)

    @model_validator(mode="after")
    def compatible(self):
        if self.mode not in ("fixture", "live") or self.laya_device != "cpu":
            raise ValueError("P0 requires fixture/live mode and a CPU router")
        if self.max_debounce < self.debounce or self.draft_expiry <= self.draft_flush:
            raise ValueError("invalid timer configuration")
        if not self.openrouter_url.startswith("https://"):
            raise ValueError("OpenRouter requires HTTPS")
        return self


def load_config(path: str | Path | None = None, **overrides) -> Config:
    values = {}
    if path:
        values = tomllib.loads(Path(path).read_text())
        if "prototype" in values:
            values = values["prototype"]
    for env, key in (
        ("REDIS_URL", "redis_url"),
        ("OPENROUTER_BASE_URL", "openrouter_url"),
    ):
        if os.environ.get(env):
            values[key] = os.environ[env]
    values.update({k: v for k, v in overrides.items() if v is not None})
    return Config(**values)
