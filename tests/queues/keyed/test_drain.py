# ruff: noqa: F811
"""drain tests (FEAT-713 / TASK-4578)."""
import asyncio

from navigator_eventbus.queues.keyed.config import Outcome

from .test_store import build, clock, dlq, fake_redis, keyed  # noqa: F401


async def _fill(q, keys, per_key=3):
    for k in keys:
        for i in range(per_key):
            await q.push(k, {"k": k, "i": i}, order=i)


async def test_drain_concurrent_across_keys_sequential_within(keyed):
    await _fill(keyed, ["a", "b", "c"])
    order: list[tuple] = []
    active, peak = {"n": 0}, {"n": 0}
    inside: set = set()

    async def handler(msg):
        assert msg.key not in inside  # never two heads of one key at once
        inside.add(msg.key)
        active["n"] += 1
        peak["n"] = max(peak["n"], active["n"])
        await asyncio.sleep(0.01)
        order.append((msg.key, msg.body["i"]))
        active["n"] -= 1
        inside.discard(msg.key)
        return Outcome.ack()

    report = await keyed.drain(handler, concurrency=2)
    assert report.acked == 9 and report.keys_seen == 3 and not report.stopped_early
    assert peak["n"] == 2
    for k in "abc":
        assert [i for kk, i in order if kk == k] == [0, 1, 2]
    assert (await keyed.describe())["messages"] == 0


async def test_drain_skips_locked_key_as_busy(keyed):
    await _fill(keyed, ["a", "b"], per_key=1)
    held = await keyed.lease_key("a")  # a live consumer holds "a"
    seen = []

    async def handler(msg):
        seen.append(msg.key)
        return Outcome.ack()

    report = await keyed.drain(handler)
    assert report.busy == 1 and report.acked == 1 and seen == ["b"]
    await keyed.apply(held, Outcome.ack())


async def test_drain_ignores_retry_delay(keyed):
    await keyed.push("a", {"n": 1})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.retry(delay_ms=900_000))
    assert await keyed.lease() is None  # normal path respects the delay

    async def handler(msg):
        return Outcome.ack()

    report = await keyed.drain(handler)
    assert report.acked == 1 and report.keys_seen == 1


async def test_drain_fail_fast(keyed):
    await _fill(keyed, [f"k{i}" for i in range(8)], per_key=2)
    calls = []

    async def handler(msg):
        calls.append(msg.key)
        raise RuntimeError("down")

    report = await keyed.drain(handler, concurrency=1, stop_after_failures=5)
    assert report.stopped_early and report.retried == 5 and report.acked == 0
    assert len(calls) == 5
    assert (await keyed.describe())["messages"] == 16  # nothing lost


async def test_fail_fast_counter_reset_by_ack(keyed):
    await _fill(keyed, ["a", "b", "c", "d"], per_key=1)

    async def handler(msg):
        if msg.key in ("a", "c"):
            raise RuntimeError("x")
        return Outcome.ack()

    report = await keyed.drain(handler, concurrency=1, stop_after_failures=2)
    assert not report.stopped_early and report.retried == 2 and report.acked == 2


async def test_drain_respects_wait_and_hold(keyed, fake_redis):
    await _fill(keyed, ["w", "h", "ok"], per_key=2)

    async def handler(msg):
        if msg.key == "w":
            return Outcome.wait()
        if msg.key == "h":
            return Outcome.hold("operator")
        return Outcome.ack()

    report = await keyed.drain(handler)
    assert (report.waited, report.held, report.acked) == (1, 1, 2)
    assert await fake_redis.zcard(keyed._k.key("w")) == 2
    assert await fake_redis.zcard(keyed._k.key("h")) == 2
    # waiting key is not re-drained (deadline respected), held key skipped by default
    again = await keyed.drain(handler)
    assert again.waited == 0 and again.held == 0 and again.keys_seen == 1  # only "w" (held skipped)


async def test_drain_include_held(keyed):
    await keyed.push("h", {})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.hold("op"))
    seen = []

    async def handler(msg):
        seen.append(msg.key)
        return Outcome.ack()

    assert (await keyed.drain(handler)).keys_seen == 0
    report = await keyed.drain(handler, include_held=True)
    assert seen == ["h"] and report.acked == 1


async def test_drain_key_prefix_scopes_keys(keyed):
    for k in ("a:1", "a:2", "b:1"):
        await keyed.push(k, {})
    seen = []

    async def handler(msg):
        seen.append(msg.key)
        return Outcome.ack()

    report = await keyed.drain(handler, key_prefix="a:")
    assert sorted(seen) == ["a:1", "a:2"] and report.keys_seen == 2
    assert await keyed.keys() == ["b:1"]


async def test_drain_paused(keyed):
    await keyed.push("a", {})
    await keyed.pause(reason="down")
    seen = []

    async def handler(msg):
        seen.append(1)
        return Outcome.ack()

    report = await keyed.drain(handler)
    assert report.stopped_early and seen == []
    report = await keyed.drain(handler, ignore_pause=True)
    assert report.acked == 1 and seen == [1]


async def test_drain_counts_expired(fake_redis, clock, dlq):
    q = build(fake_redis, clock, dlq, max_age_ms=1_000)
    await q.push("a", {"n": "old"}, order=1)
    clock.advance_ms(2_000)
    await q.push("a", {"n": "new"}, order=2)

    async def handler(msg):
        return Outcome.ack()

    report = await q.drain(handler)
    assert report.expired == 1 and report.acked == 1 and len(dlq.calls) == 1


async def test_drain_handler_timeout_is_retry(keyed):
    await keyed.push("a", {})

    async def slow(msg):
        await asyncio.sleep(1)

    report = await keyed.drain(slow, call_timeout_ms=20)
    assert report.retried == 1
