# ruff: noqa: F811
"""Regression tests for the independent FEAT-713 review (Codex + Claude, 2026-10-30)."""
import asyncio

import pytest

from navigator_eventbus.queues.keyed.config import LeaseLost, Outcome

from .test_store import DLQ, build, clock, dlq, fake_redis, keyed  # noqa: F401

# ---------------------------------------------------------------------------
# Group 1 — fences
# ---------------------------------------------------------------------------


class BlockingDLQ(DLQ):
    """on_dlq that parks until released, so another actor can run meanwhile."""

    def __init__(self) -> None:
        super().__init__()
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, envelope, **kw):
        await super().__call__(envelope, **kw)
        self.reached.set()
        await self.release.wait()


async def test_expire_head_is_fenced(fake_redis, clock):
    """A stalls in on_dlq, its lock lapses, B removes + a producer re-pushes + B
    leases the NEW message; A must not delete the new hash/indexes."""
    slow = BlockingDLQ()
    qa = build(fake_redis, clock, slow, max_age_ms=1_000)
    qb = build(fake_redis, clock, DLQ(), max_age_ms=1_000)
    await qa.push("w1", {"v": "old"}, message_id="m", order=1)
    clock.advance_ms(2_000)
    lock_a = qa._new_lock("w1")
    assert await lock_a.acquire(blocking=False)
    head = (await qa.peek("w1"))[0]
    task = asyncio.create_task(qa.expire_head("w1", head, lock=lock_a))
    await slow.reached.wait()
    clock.advance_ms(11_000)  # A's lock lapses
    assert await qb.lease_key("w1") is None  # B expires the old m, key empty
    await qb.push("w1", {"v": "new"}, message_id="m", order=1)  # producer re-push
    lease_b = await qb.lease_key("w1")
    assert lease_b is not None and lease_b.message.body == {"v": "new"}
    slow.release.set()
    with pytest.raises(LeaseLost):
        await task
    assert await fake_redis.zcard(qb._k.key("w1")) == 1
    assert (await fake_redis.hgetall(qb._k.msg("m")))["gen"] == "1"
    assert await fake_redis.zscore(qb._k.all, "m") is not None
    await qb.apply(lease_b, Outcome.ack())


async def test_lease_acquisition_write_is_fenced(fake_redis, clock, dlq):
    """A snapshots the head, stalls past its lock TTL; B leases and applies
    retry(900s). A resuming must NOT bump gen / clear due_ms from its stale view."""
    qa = build(fake_redis, clock, dlq)
    qb = build(fake_redis, clock, dlq)
    qc = build(fake_redis, clock, dlq)
    await qa.push("w1", {"n": 1})
    reached, release = asyncio.Event(), asyncio.Event()
    original = qa.expire_head

    async def stall(*a, **kw):  # runs after A's snapshot, before A's acquisition write
        reached.set()
        await release.wait()
        return await original(*a, **kw)

    qa.expire_head = stall
    task = asyncio.create_task(qa.lease_key("w1"))
    await reached.wait()
    clock.advance_ms(11_000)
    lease_b = await qb.lease_key("w1")
    assert lease_b is not None
    await qb.apply(lease_b, Outcome.retry(delay_ms=900_000))
    release.set()
    assert await task is None  # A lost the key
    raw = await fake_redis.hgetall(qb._k.msg(lease_b.message.message_id))
    assert raw["gen"] == "1" and raw["due_ms"] != "" and raw["attempts"] == "1"
    assert await qc.lease_key("w1") is None  # C still sees the 900 s delay


# ---------------------------------------------------------------------------
# Group 2 — ordering / pause / recovery
# ---------------------------------------------------------------------------


async def _drain_order(q, key, n):
    got = []
    for _ in range(n):
        lease = await q.lease_key(key)
        got.append(lease.message.body["i"])
        await q.apply(lease, Outcome.ack())
    return got


async def test_default_order_survives_a_1000_push_same_ms_burst(keyed):
    """The old `seq % 1000` tiebreak sorted push #1000 before push #999."""
    n = 1_000
    for i in range(n):
        await keyed.push("w1", {"i": i}, message_id=f"id-{n - i:05d}")  # ids reverse-lexical
    assert await _drain_order(keyed, "w1", n) == list(range(n))


async def test_default_order_across_ms_boundaries(keyed, clock):
    n = 3_000
    for i in range(n):
        await keyed.push("w1", {"i": i}, message_id=f"id-{n - i:05d}")
        if i % 100 == 99:
            clock.advance_ms(1)
    assert await _drain_order(keyed, "w1", n) == list(range(n))


async def test_default_order_scores_are_exact(keyed, fake_redis):
    for i in range(50):
        await keyed.push("w1", {"i": i})
    scores = [s for _, s in await fake_redis.zrange(keyed._k.key("w1"), 0, -1, withscores=True)]
    assert scores == sorted(scores) and len(set(scores)) == 50
    assert all((sc * 1024) == int(sc * 1024) for sc in scores)  # multiples of 2**-10: exact


async def test_pause_rechecked_inside_lease(keyed, fake_redis):
    """A pause set while receive() was in flight must not run a handler."""
    await keyed.push("w1", {})
    original = keyed._store.receive

    async def receive_then_pause(*a, **kw):
        out = await original(*a, **kw)
        await keyed.pause(reason="paused during long poll")
        return out

    keyed._store.receive = receive_then_pause
    assert await keyed.lease() is None
    keyed._store.receive = original
    assert await fake_redis.exists(keyed._k.lock("w1")) == 0  # nothing was leased
    await keyed.resume()
    lease = await keyed.lease()  # the signal was NOT consumed: redelivered
    assert lease is not None and lease.key == "w1"


async def test_ack_after_deadline_head_does_not_leave_stale_delayed(keyed, fake_redis):
    """drain-style ack of a head that had a retry deadline: next head must not wait."""
    await keyed.push("w1", {"i": 1}, order=1)
    await keyed.push("w1", {"i": 2}, order=2)
    first = await keyed.lease()
    await keyed.apply(first, Outcome.retry(delay_ms=900_000))
    forced, _ = await keyed._lease_key_detailed("w1", ignore_due=True)  # what drain does
    assert forced.message.body == {"i": 1}
    await keyed.apply(forced, Outcome.ack())
    assert await fake_redis.zscore(keyed._k.delayed, "w1") is None  # head 2 is leasable now
    fake_redis.streams[keyed._store.stream_for("punches-signal")].clear()  # lose the signal
    assert await keyed.recover(force=True) == 1
    assert (await keyed.lease()).message.body == {"i": 2}


async def test_lower_order_push_during_retry_is_not_orphaned(keyed, fake_redis):
    await keyed.push("w1", {"i": "old"}, order=5)
    holder = await keyed.lease()
    await keyed.push("w1", {"i": "new"}, order=1)  # new head; its signal is acked as busy
    assert await keyed.lease() is None
    await keyed.apply(holder, Outcome.retry(delay_ms=900_000))
    got = await keyed.lease()  # must not wait 15 minutes
    assert got is not None and got.message.body == {"i": "new"}


async def test_recover_signals_key_whose_current_head_is_due_despite_stale_delayed(keyed, fake_redis):
    await keyed.push("w1", {})
    await fake_redis.zadd(keyed._k.delayed, {"w1": 1e15})  # stale far-future entry
    fake_redis.streams[keyed._store.stream_for("punches-signal")].clear()
    assert await keyed.recover(force=True) == 1
    assert await fake_redis.zscore(keyed._k.delayed, "w1") is None


async def test_recover_is_bounded_per_pass(fake_redis, clock, dlq):
    q = build(fake_redis, clock, dlq, recover_batch=500)
    for i in range(1_200):
        await fake_redis.zadd(q._k.key(f"k{i:04d}"), {f"m{i}": 1})
        await fake_redis.hset(q._k.msg(f"m{i}"), mapping={"key": f"k{i:04d}", "body": "{}"})
        await fake_redis.sadd(q._k.keys, f"k{i:04d}")
    sent = []
    for _ in range(3):
        sent.append(await q.recover())
        clock.advance_ms(60_001)
    assert max(sent) <= 500 and sum(sent) == 1_200  # cursor persisted: full coverage in 3 passes
    assert await q.recover(force=True) == 1_200  # force = one full pass
