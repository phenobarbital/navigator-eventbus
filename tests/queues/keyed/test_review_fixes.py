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
