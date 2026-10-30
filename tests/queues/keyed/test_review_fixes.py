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


# ---------------------------------------------------------------------------
# Group 3 — robustness
# ---------------------------------------------------------------------------


@pytest.fixture
def bytes_redis(clock):
    from .._fake_streams import FakeStreamsRedis

    return FakeStreamsRedis(clock, decode_responses=False)


async def test_bytes_mode_client_full_flow(bytes_redis, clock, dlq):
    """decode_responses=False: replies are bytes; every reader must normalise."""
    assert isinstance(await bytes_redis.hgetall("nothing"), dict)
    q = build(bytes_redis, clock, dlq)
    await q.push("w1", {"n": 1}, order=1)
    await q.push("w1", {"n": 2}, order=2)
    assert await bytes_redis.hset("probe", "a", "1") == 1
    assert isinstance(await bytes_redis.hget("probe", "a"), bytes)
    lease = await q.lease()
    assert lease is not None and lease.message.body == {"n": 1}
    await q.apply(lease, Outcome.retry(delay_ms=1_000))
    clock.advance_ms(1_001)
    assert await q.promote_due() == 1
    lease = await q.lease()
    await q.apply(lease, Outcome.hold("operator"))
    assert await q.release_hold("w1") is True
    assert [m.body["n"] for m in await q.peek("w1")] == [1, 2]
    assert (await q.describe())["messages"] == 2
    assert await q.keys() == ["w1"]
    assert await q.recover(force=True) >= 0

    async def handler(msg):
        return Outcome.ack()

    report = await q.drain(handler)
    assert report.acked == 2 and report.busy == 0
    assert (await q.describe())["messages"] == 0


def test_backoff_never_overflows():
    from navigator_eventbus.queues.keyed.store import backoff_ms

    for attempts in (10, 1_023, 1_024, 10_000, 10**9):
        assert backoff_ms(attempts, base_ms=30_000, max_ms=900_000, factor=2.0) == 900_000
    assert backoff_ms(10_000, base_ms=30_000, max_ms=900_000, factor=1.0) == 30_000
    assert backoff_ms(3, base_ms=30_000, max_ms=900_000, factor=2.0) == 240_000
    assert backoff_ms(5, base_ms=1_000_000, max_ms=900_000, factor=2.0) == 900_000


async def test_drain_key_releases_lock_on_cancel(keyed, fake_redis):
    from navigator_eventbus.queues.keyed.drainer import drain_key  # noqa: F401

    await keyed.push("w1", {})
    started = asyncio.Event()

    async def hang(msg):
        started.set()
        await asyncio.sleep(60)

    task = asyncio.create_task(keyed.drain(hang))
    await started.wait()
    assert await fake_redis.exists(keyed._k.lock("w1")) == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await fake_redis.exists(keyed._k.lock("w1")) == 0


async def test_drain_key_releases_lock_when_apply_raises(keyed, fake_redis):
    await keyed.push("w1", {})

    async def handler(msg):
        return Outcome.ack()

    async def broken_apply(lease, outcome):
        raise ConnectionError("redis went away")

    keyed.apply = broken_apply
    with pytest.raises(ConnectionError):
        await keyed.drain(handler)
    assert await fake_redis.exists(keyed._k.lock("w1")) == 0


async def test_extend_rejects_more_than_lock_ttl(keyed, fake_redis):
    await keyed.push("w1", {})
    lease = await keyed.lease()
    with pytest.raises(ValueError):
        await lease.extend(additional_ms=keyed.config.lock_ttl_ms + 1)
    with pytest.raises(ValueError):
        await lease.extend(additional_ms=-5)
    await lease.extend(additional_ms=5_000)
    # rule 2: the signal lease must outlive the lock's remaining TTL
    stream = keyed._store.stream_for("punches-signal")
    entry = next(iter(fake_redis.groups[(stream, "workers")]["pending"].values()))
    remaining_signal = 20_000 - fake_redis._idle_ms(entry)
    assert remaining_signal > await fake_redis.pttl(keyed._k.lock("w1"))
    await keyed.apply(lease, Outcome.ack())


def test_drain_export_is_the_function_not_the_submodule():
    import subprocess
    import sys

    code = (
        "import navigator_eventbus.queues.keyed.drainer\n"
        "from navigator_eventbus.queues.keyed import drain\n"
        "import navigator_eventbus.queues as q\n"
        "assert callable(drain) and not hasattr(drain, '__path__') and drain.__name__ == 'drain', drain\n"
        "assert callable(q.drain) and q.drain is drain\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# Round 2
# ---------------------------------------------------------------------------


async def test_r2_1_unbounded_same_ms_burst_keeps_arrival_order(keyed, fake_redis):
    """Frozen clock, 5,000 default-order pushes: strictly increasing exact scores."""
    n = 5_000
    for i in range(n):
        await keyed.push("w1", {"i": i}, message_id=f"id-{n - i:05d}")
    scored = await fake_redis.zrange(keyed._k.key("w1"), 0, -1, withscores=True)
    scores = [sc for _, sc in scored]
    assert len(set(scores)) == n and scores == sorted(scores)
    assert all(sc * 1024 == int(sc * 1024) for sc in scores)  # exact multiples of 2**-10
    assert [(await keyed.peek("w1", limit=3))[j].body["i"] for j in range(3)] == [0, 1, 2]
    assert await _drain_order(keyed, "w1", 1_100) == list(range(1_100))


async def test_r2_1_interleaved_producers(fake_redis, clock, dlq):
    qa, qb = build(fake_redis, clock, dlq), build(fake_redis, clock, dlq)

    async def produce(q, tag, n):
        for i in range(n):
            await q.push("w1", {"tag": tag, "i": i})

    await asyncio.gather(produce(qa, "a", 700), produce(qb, "b", 700))
    heads = await qa.peek("w1", limit=1_400)
    for tag in ("a", "b"):
        seq = [m.body["i"] for m in heads if m.body["tag"] == tag]
        assert seq == list(range(700))  # each producer's own order is preserved
    scored = await fake_redis.zrange(qa._k.key("w1"), 0, -1, withscores=True)
    assert len({sc for _, sc in scored}) == 1_400  # no ties anywhere
