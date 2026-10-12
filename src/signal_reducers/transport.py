"""Transient Redis admission: atomic, destructive, and explicitly best effort."""

from __future__ import annotations

import json
import struct
from datetime import datetime, timezone
from uuid import UUID, uuid4

import redis.asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from .config import Config
from .models import (
    ContractError,
    Delivery,
    Receipt,
    SignalInput,
    canonical,
    public,
    utcnow,
)

CLAIM = """
if redis.call('EXISTS',KEYS[1]) == 1 or redis.call('EXISTS',KEYS[2]) == 1 then return 0 end
redis.call('SET',KEYS[1],ARGV[1],'EX',ARGV[2]); return 1
"""
ADMIT = """
if redis.call('EXISTS',KEYS[1]) == 0 then return -1 end
if redis.call('LLEN',KEYS[2]) + #ARGV - 2 > tonumber(ARGV[1]) then return -2 end
redis.call('RPUSH',KEYS[2],unpack(ARGV,3))
redis.call('EXPIRE',KEYS[2],ARGV[2]); return #ARGV - 2
"""
RENEW = """
if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('EXPIRE',KEYS[1],ARGV[2]); return 1
"""
RELEASE = """
if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL',KEYS[1],KEYS[2]); return 1
"""


class AdmissionError(ContractError):
    def __init__(self, outcome: str, code: str):
        self.outcome = outcome
        super().__init__(code)


def encode(delivery: Delivery) -> bytes:
    signal = delivery.signal
    header = public(signal)
    header.pop("payload", None)
    header.pop("payload_base64", None)
    header["receipt_id"] = str(delivery.receipt_id)
    raw = canonical(header, maximum=8192)
    return b"SR1\0" + struct.pack(">I", len(raw)) + raw + signal.payload_bytes


def decode(raw: bytes) -> Delivery:
    try:
        if len(raw) < 8 or raw[:4] != b"SR1\0":
            raise ContractError("invalid_frame")
        length = struct.unpack(">I", raw[4:8])[0]
        if length > 8192 or len(raw) < 8 + length:
            raise ContractError("invalid_header")
        header = json.loads(raw[8 : 8 + length])
        if not isinstance(header, dict):
            raise ContractError("invalid_header")
        body = raw[8 + length :]
        if len(body) > 200_000:
            raise ContractError("payload_too_large")
        receipt = UUID(header.pop("receipt_id"))
        encoding = header.pop("payload_encoding")
        if encoding not in ("json", "binary"):
            raise ContractError("invalid_encoding")
        received = datetime.fromisoformat(header.pop("received_at"))
        if received.tzinfo is None or received.utcoffset() is None:
            raise ContractError("timezone_required")
        value = body if encoding == "binary" else json.loads(body)
        validated = SignalInput(**header, payload=value).accept()
        signal = validated.model_copy(
            update={"received_at": received.astimezone(timezone.utc)}
        )
        return Delivery(receipt_id=receipt, signal=signal)
    except (ValueError, TypeError, KeyError, RecursionError, UnicodeDecodeError) as exc:
        raise ContractError("corrupt_signal") from exc


class SignalQueue:
    def __init__(self, run_id: UUID, config: Config):
        self.run_id = run_id
        self.config = config
        self.owner_key = f"sr:{run_id}:owner"
        self.queue_key = f"sr:{run_id}:signals"
        self.owner_token: str | None = None
        # Disable redis-py's automatic retries: a submitted command may have succeeded.
        from redis.backoff import NoBackoff
        from redis.retry import Retry

        self.client = redis.from_url(
            config.redis_url,
            decode_responses=False,
            socket_connect_timeout=3,
            socket_timeout=3,
            retry=Retry(NoBackoff(), 0),
        )
        self.counters = {"admitted": 0, "popped": 0, "expired": 0, "corrupt": 0}

    async def check(self, strict: bool = True) -> dict:
        await self.client.ping()
        settings = await self.client.config_get(
            "save", "appendonly", "maxmemory", "maxmemory-policy"
        )
        decoded = {
            (k.decode() if isinstance(k, bytes) else k): (
                v.decode() if isinstance(v, bytes) else v
            )
            for k, v in settings.items()
        }
        ready = (
            decoded.get("save") == ""
            and decoded.get("appendonly") == "no"
            and decoded.get("maxmemory-policy") == "noeviction"
            and 0 < int(decoded.get("maxmemory", "0")) <= 256 * 1024 * 1024
        )
        if strict and not ready:
            raise ContractError("redis_requires_dedicated_transient_configuration")
        return {"ready": ready, "settings": decoded}

    async def claim(self):
        token = str(uuid4())
        claimed = await self.client.eval(
            CLAIM, 2, self.owner_key, self.queue_key, token, self.config.queue_ttl
        )
        if not claimed:
            raise ContractError("run_namespace_exists")
        self.owner_token = token

    async def renew(self):
        if not self.owner_token or not await self.client.eval(
            RENEW, 1, self.owner_key, self.owner_token, self.config.queue_ttl
        ):
            raise ContractError("run_lease_lost")

    async def publish(
        self, signals: list[SignalInput] | tuple[SignalInput, ...]
    ) -> tuple[Receipt, ...]:
        if not signals:
            return ()
        if len(signals) > 100:
            raise AdmissionError("rejected", "batch_capacity_exceeded")
        try:
            deliveries = tuple(Delivery(signal=s.accept()) for s in signals)
            encoded = tuple(encode(d) for d in deliveries)
            if sum(map(len, encoded)) > 4 * 1024 * 1024:
                raise ContractError("batch_bytes_exceeded")
        except (ValueError, TypeError) as exc:
            raise AdmissionError(
                "rejected", getattr(exc, "code", "invalid_signal")
            ) from exc
        try:
            # PING distinguishes a definite pre-submission failure from an unknown outcome.
            await self.client.ping()
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise AdmissionError("unavailable", "redis_unavailable") from exc
        try:
            count = await self.client.eval(
                ADMIT,
                2,
                self.owner_key,
                self.queue_key,
                self.config.queue_capacity,
                self.config.queue_ttl,
                *encoded,
            )
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise AdmissionError(
                "outcome_unknown", "admission_outcome_unknown"
            ) from exc
        except ResponseError as exc:
            raise AdmissionError("rejected", "redis_admission_rejected") from exc
        if count == -1:
            raise AdmissionError("rejected", "run_not_live")
        if count == -2:
            raise AdmissionError("rejected", "queue_capacity_exceeded")
        self.counters["admitted"] += count
        return tuple(
            Receipt(receipt_id=d.receipt_id, signal_id=d.signal.id) for d in deliveries
        )

    async def pull(self, timeout_s: int = 1) -> Delivery | None:
        if (
            not self.owner_token
            or await self.client.get(self.owner_key) != self.owner_token.encode()
        ):
            raise ContractError("run_lease_lost")
        item = await self.client.blpop(self.queue_key, timeout=timeout_s)
        if item is None:
            return None
        self.counters["popped"] += 1
        try:
            delivery = decode(item[1])
        except ContractError:
            self.counters["corrupt"] += 1
            return None
        if (
            utcnow() - delivery.signal.received_at
        ).total_seconds() > self.config.signal_max_age:
            self.counters["expired"] += 1
            return None
        return delivery

    async def close(self, release: bool = True):
        if release and self.owner_token:
            await self.client.eval(
                RELEASE, 2, self.owner_key, self.queue_key, self.owner_token
            )
            self.owner_token = None
        await self.client.aclose()


SignalIntake = SignalQueue
