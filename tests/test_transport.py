import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from signal_reducers.config import Config
from signal_reducers.models import ContractError, Delivery, SignalInput, utcnow
from signal_reducers.transport import AdmissionError, SignalQueue, decode, encode


def test_framing_immutability_and_boundary():
    data = {"nested": [{"value": 1}]}
    delivery = Delivery(
        signal=SignalInput(modality="structured", payload=data).accept()
    )
    data["nested"][0]["value"] = 2
    detached = delivery.signal.payload
    detached["nested"][0]["value"] = 3
    assert delivery.signal.payload == {"nested": [{"value": 1}]}
    assert decode(encode(delivery)) == delivery
    binary = Delivery(
        signal=SignalInput(modality="binary", payload=b"\xff\x00" * 100_000).accept()
    )
    assert decode(encode(binary)).signal.payload == binary.signal.payload
    with pytest.raises(ValueError):
        SignalInput(modality="binary", payload=b"x" * 200_001)
    for value in (float("nan"), float("inf"), {1: "bad"}):
        with pytest.raises(ValueError):
            SignalInput(modality="structured", payload=value)
    with pytest.raises(ValidationError):
        SignalInput(modality="text", payload="x", source_seq=True)
    with pytest.raises(ContractError):
        decode(b"bad")


async def test_real_redis_atomic_capacity_and_destructive_pop(redis_url):
    queue = SignalQueue(uuid4(), Config(redis_url=redis_url, queue_capacity=2))
    await queue.check()
    await queue.claim()
    try:
        signal = SignalInput(modality="binary", payload=b"\xff\x00")
        results = await asyncio.gather(
            queue.publish([signal, signal]),
            queue.publish([signal, signal]),
            return_exceptions=True,
        )
        assert sum(isinstance(x, tuple) for x in results) == 1
        failure = next(x for x in results if isinstance(x, AdmissionError))
        assert failure.outcome == "rejected"
        assert failure.code == "queue_capacity_exceeded"
        first, second = await queue.pull(), await queue.pull()
        assert first.signal.id == second.signal.id
        assert first.receipt_id != second.receipt_id
        assert first.signal.payload == b"\xff\x00"
        assert await queue.pull() is None
        with pytest.raises(ContractError):
            duplicate = SignalQueue(queue.run_id, queue.config)
            try:
                await duplicate.claim()
            finally:
                await duplicate.close()
    finally:
        await queue.close()


async def test_corrupt_expiry_lease_and_empty(redis_url):
    queue = SignalQueue(uuid4(), Config(redis_url=redis_url))
    await queue.claim()
    assert await queue.publish([]) == ()
    stale = (
        SignalInput(modality="text", payload="old")
        .accept()
        .model_copy(update={"received_at": utcnow() - timedelta(seconds=61)})
    )
    await queue.client.rpush(queue.queue_key, b"bad", encode(Delivery(signal=stale)))
    assert await queue.pull() is None
    assert await queue.pull() is None
    assert queue.counters["corrupt"] == queue.counters["expired"] == 1
    await queue.client.set(queue.owner_key, "replacement")
    with pytest.raises(ContractError):
        await queue.renew()
    await queue.close()
    # A replaced owner must survive old-owner shutdown.
    import redis.asyncio as redis

    client = redis.from_url(redis_url)
    assert await client.get(queue.owner_key) == b"replacement"
    await client.delete(queue.owner_key)
    await client.aclose()


@pytest.mark.parametrize("failure_stage", ["before_submission", "after_acceptance"])
async def test_publication_failure_distinguishes_definite_from_unknown_without_retry(
    redis_url, failure_stage
):
    from redis.exceptions import ConnectionError

    queue = SignalQueue(uuid4(), Config(redis_url=redis_url))
    await queue.claim()
    original = queue.client

    class FaultClient:
        ping_calls = 0
        eval_calls = 0

        async def ping(self):
            self.ping_calls += 1
            if failure_stage == "before_submission":
                raise ConnectionError("injected preflight connection failure")
            return await original.ping()

        async def eval(self, *args):
            self.eval_calls += 1
            # The server has committed admission, but its reply is lost. Repeating
            # the operation would duplicate a best-effort delivery.
            await original.eval(*args)
            raise ConnectionError("injected lost admission reply")

    client = FaultClient()
    queue.client = client
    signal = SignalInput(modality="text", payload="synthetic publication")
    try:
        with pytest.raises(AdmissionError) as caught:
            await queue.publish([signal])
        assert client.ping_calls == 1
        if failure_stage == "before_submission":
            assert caught.value.outcome == "unavailable"
            assert caught.value.code == "redis_unavailable"
            assert client.eval_calls == 0
            assert await original.llen(queue.queue_key) == 0
        else:
            assert caught.value.outcome == "outcome_unknown"
            assert caught.value.code == "admission_outcome_unknown"
            assert client.eval_calls == 1
            assert await original.llen(queue.queue_key) == 1
            delivery = decode(await original.lpop(queue.queue_key))
            assert delivery.signal.id == signal.id
            assert await original.llen(queue.queue_key) == 0
        assert queue.counters["admitted"] == 0
    finally:
        queue.client = original
        await queue.close()


async def test_shutdown_removes_only_owned_namespace(redis_url):
    import redis.asyncio as redis

    queue = SignalQueue(uuid4(), Config(redis_url=redis_url))
    await queue.claim()
    await queue.publish([SignalInput(modality="text", payload="owned item")])
    owner_key, queue_key = queue.owner_key, queue.queue_key
    await queue.close()
    observer = redis.from_url(redis_url)
    try:
        assert not await observer.exists(owner_key, queue_key)
        replacement = SignalQueue(uuid4(), Config(redis_url=redis_url))
        await replacement.claim()
        await replacement.client.set(replacement.owner_key, "replacement-owner")
        await replacement.client.rpush(replacement.queue_key, b"replacement-owned-item")
        await replacement.close()
        assert await observer.get(replacement.owner_key) == b"replacement-owner"
        assert await observer.lrange(replacement.queue_key, 0, -1) == [
            b"replacement-owned-item"
        ]
        await observer.delete(replacement.owner_key, replacement.queue_key)
    finally:
        await observer.aclose()
