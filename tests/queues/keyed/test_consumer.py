# ruff: noqa: F811
"""KeyedConsumer tests (FEAT-713 / TASK-4577)."""
import asyncio

from navigator_eventbus.queues.keyed.config import Outcome
from navigator_eventbus.queues.keyed.consumer import KeyedConsumer, backoff_ms

from .test_store import build, clock, dlq, fake_redis, keyed  # noqa: F401


def test_backoff_capped_exponential():
    seq = [backoff_ms(n, base_ms=30_000, max_ms=900_000, factor=2.0) for n in range(7)]
    assert seq == [30_000, 60_000, 120_000, 240_000, 480_000, 900_000, 900_000]


async def test_consumer_run_once_applies_outcome(keyed, fake_redis):
    await keyed.push("w1", {"n": 1})
    consumer = KeyedConsumer(keyed, wait_ms=0)
    seen = []

    async def handler(msg):
        seen.append(msg.body)
        return Outcome.ack()

    out = await consumer.run_once(handler)
    assert out.kind == "ack" and seen == [{"n": 1}]
    assert await fake_redis.zcard(keyed._k.key("w1")) == 0
    assert await consumer.run_once(handler) is None  # nothing left


async def test_handler_exception_becomes_retry_with_text(keyed, fake_redis):
    mid = await keyed.push("w1", {})
    consumer = KeyedConsumer(keyed, wait_ms=0)

    async def handler(msg):
        raise RuntimeError("downstream 503")

    out = await consumer.run_once(handler)
    assert out.kind == "retry" and "downstream 503" in out.reason
    raw = await fake_redis.hgetall(keyed._k.msg(mid))
    assert "downstream 503" in raw["last_error"] and raw["attempts"] == "1"
    assert await fake_redis.zscore(keyed._k.delayed, "w1") == keyed.now_ms() + 30_000


async def test_handler_timeout_becomes_retry(fake_redis, clock):
    q = build(fake_redis, clock, call_timeout_ms=20)
    await q.push("w1", {})

    async def slow(msg):
        await asyncio.sleep(1)

    out = await KeyedConsumer(q, wait_ms=0).run_once(slow)
    assert out.kind == "retry" and "timed out" in out.reason


async def test_non_outcome_return_is_retry(keyed):
    await keyed.push("w1", {})

    async def bad(msg):
        return "ack"

    out = await KeyedConsumer(keyed, wait_ms=0).run_once(bad)
    assert out.kind == "retry" and "TypeError" in out.reason


async def test_consumer_extends_lock_for_long_handler(fake_redis, clock):
    q = build(fake_redis, clock, lock_ttl_ms=300)
    await q.push("w1", {})

    async def long(msg):
        for _ in range(4):  # 800 fake ms total, > lock_ttl_ms
            clock.advance_ms(200)
            await asyncio.sleep(0.12)
        return Outcome.ack()

    out = await KeyedConsumer(q, wait_ms=0).run_once(long)
    assert out is not None and out.kind == "ack"
    assert await fake_redis.zcard(q._k.key("w1")) == 0


async def test_lease_lost_drops_outcome(keyed, fake_redis, clock):
    await keyed.push("w1", {})

    async def handler(msg):
        clock.advance_ms(11_000)  # lock expires, no extension happens in time
        fake_redis.strings[keyed._k.lock("w1")] = "other"
        return Outcome.ack()

    assert await KeyedConsumer(keyed, wait_ms=0).run_once(handler) is None
    assert await fake_redis.zcard(keyed._k.key("w1")) == 1


async def test_pause_never_receives_signals(fake_redis, clock):
    keyed = build(fake_redis, clock, pause_poll_ms=1)
    for k in ("a", "b", "c"):
        await keyed.push(k, {})
    await keyed.pause(ttl_ms=10_000_000)
    consumer = KeyedConsumer(keyed, wait_ms=0)
    fake_redis.calls.clear()
    handled = []

    async def handler(msg):
        handled.append(msg.key)
        return Outcome.ack()

    for _ in range(50):  # far beyond max_receives (5)
        clock.advance_ms(30_000)
        assert await consumer.run_once(handler) is None
    assert not [c for c in fake_redis.calls if c[0] in ("xreadgroup", "xautoclaim")]
    assert handled == []
    await keyed.resume()
    for _ in range(3):
        await consumer.run_once(handler)
    assert sorted(handled) == ["a", "b", "c"]
    assert (await keyed.describe())["messages"] == 0


async def test_run_stops_and_survives_errors(keyed):
    await keyed.push("w1", {})
    consumer = KeyedConsumer(keyed, wait_ms=0)
    stop = asyncio.Event()
    calls = {"n": 0}
    original = consumer.run_once

    async def flaky(handler):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient")
        out = await original(handler)
        stop.set()
        return out

    consumer.run_once = flaky

    async def handler(msg):
        return Outcome.ack()

    await asyncio.wait_for(consumer.run(handler, stop=stop), timeout=5)
    assert calls["n"] >= 2 and stop.is_set()
