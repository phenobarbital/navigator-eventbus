"""KeyedQueue core tests against FakeStreamsRedis (FEAT-713 / TASK-4575)."""
import logging

import pytest

from navigator_eventbus.queues.config import QueueConfig, QueueGroupConfig, QueueRegistry
from navigator_eventbus.queues.keyed.config import KeyedQueueConfig, LeaseLost, Outcome
from navigator_eventbus.queues.keyed.store import KeyedQueue, backoff_ms
from navigator_eventbus.queues.receipts import ReceiptCodec
from navigator_eventbus.queues.store import QueueStore

from .._fake_streams import FakeClock, FakeStreamsRedis


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def fake_redis(clock) -> FakeStreamsRedis:
    return FakeStreamsRedis(clock)


class DLQ:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def __call__(self, envelope, *, attempts, error, subscriber_id):
        self.calls.append((envelope, attempts, str(error), subscriber_id))


@pytest.fixture
def dlq() -> DLQ:
    return DLQ()


def build(fake_redis, clock, dlq=None, **cfg) -> KeyedQueue:
    signal = QueueConfig(
        name="punches-signal",
        groups=[QueueGroupConfig(name="workers")],
        default_visibility_timeout_ms=20_000,
        max_visibility_timeout_ms=20_000,
    )
    store = QueueStore(QueueRegistry(queues=[signal]), fake_redis, ReceiptCodec(["k"]), clock=clock)
    cfg.setdefault("lock_ttl_ms", 10_000)
    return KeyedQueue(
        KeyedQueueConfig(name="punches", signal=signal, **cfg),
        fake_redis,
        store=store,
        on_dlq=dlq,
        clock=clock,
    )


def lose_signals(q: KeyedQueue, fake_redis) -> None:
    """Simulate lost wake-ups: the signal entries AND their liveness markers vanish
    (what an eviction / a parked signal looks like once the marker TTL lapsed)."""
    fake_redis.streams[q._store.stream_for("punches-signal")].clear()
    for name in [n for n in fake_redis.zsets if n.startswith(q._k.base + "sig:")]:
        del fake_redis.zsets[name]


def live_signal_ids(q: KeyedQueue, fake_redis, key: str) -> set[str]:
    """Entry ids the queue currently believes are live signals for *key*."""
    now = q.now_ms()
    return {
        m for m, expires in fake_redis.zsets.get(q._k.sig(key), {}).items() if expires > now
    }


def stream_ids(q: KeyedQueue, fake_redis) -> list[str]:
    return [mid for mid, _ in fake_redis.streams.get(q._store.stream_for("punches-signal"), [])]


@pytest.fixture
def keyed(fake_redis, clock, dlq) -> KeyedQueue:
    return build(fake_redis, clock, dlq)


async def test_push_orders_by_score_not_arrival(keyed):
    await keyed.push("w1", {"n": "late"}, order=1200)
    await keyed.push("w1", {"n": "early"}, order=900)
    lease = await keyed.lease()
    assert lease.message.body == {"n": "early"}


async def test_one_in_flight_per_key(keyed):
    await keyed.push("w1", {"n": 1})
    await keyed.push("w1", {"n": 2})
    first = await keyed.lease()
    assert first is not None
    assert await keyed.lease() is None  # duplicate signal acked, key locked
    assert first.message.gen == 1 and first.message.state == "leased"


async def test_other_keys_lease_in_parallel(keyed):
    await keyed.push("w1", {})
    await keyed.push("w2", {})
    a, b = await keyed.lease(), await keyed.lease()
    assert {a.key, b.key} == {"w1", "w2"}


async def test_lock_expiry_makes_head_leasable_again(keyed, clock):
    await keyed.push("w1", {"n": 1})
    stale = await keyed.lease()
    clock.advance_ms(21_000)  # lock (10s) and signal lease (20s) both lapsed
    fresh = await keyed.lease()
    assert fresh is not None and fresh.message.message_id == stale.message.message_id
    assert fresh.gen == stale.gen + 1
    with pytest.raises(LeaseLost):
        await keyed.apply(stale, Outcome.ack())
    # B's state intact
    assert await keyed._redis.zcard(keyed._k.key("w1")) == 1
    await keyed.apply(fresh, Outcome.ack())


async def test_ack_resignals_when_key_not_empty(keyed, fake_redis):
    await keyed.push("w1", {"n": 1}, order=1)
    await keyed.push("w1", {"n": 2}, order=2)
    lease = await keyed.lease()
    before = len(fake_redis.streams[keyed._store.stream_for("punches-signal")])
    await keyed.apply(lease, Outcome.ack())
    after = len(fake_redis.streams[keyed._store.stream_for("punches-signal")])
    assert after == before + 1
    assert await fake_redis.sismember(keyed._k.keys, "w1")
    nxt = await keyed.lease()
    assert nxt.message.body == {"n": 2}
    await keyed.apply(nxt, Outcome.ack())
    assert not await fake_redis.sismember(keyed._k.keys, "w1")
    assert await fake_redis.exists(keyed._k.msg(lease.message.message_id)) == 0


async def test_retry_keeps_head_and_delays_key(keyed, fake_redis, clock):
    await keyed.push("w1", {"n": 1})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.retry(delay_ms=5_000, reason="boom"))
    assert await fake_redis.zrange(keyed._k.key("w1"), 0, 0) == [lease.message.message_id]
    due = await fake_redis.zscore(keyed._k.delayed, "w1")
    assert due == keyed.now_ms() + 5_000
    msg = await fake_redis.hgetall(keyed._k.msg(lease.message.message_id))
    assert msg["attempts"] == "1" and msg["last_error"] == "boom"
    assert await keyed.lease() is None
    clock.advance_ms(5_001)
    await keyed._signal("w1")  # what promote_due does (TASK-4576)
    again = await keyed.lease()
    assert again.message.message_id == lease.message.message_id
    assert again.message.attempts == 1


async def test_retry_default_backoff(keyed, fake_redis):
    await keyed.push("w1", {})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.retry())
    assert await fake_redis.zscore(keyed._k.delayed, "w1") == keyed.now_ms() + 30_000


async def test_retry_delay_enforced_against_duplicate_signal(keyed, fake_redis):
    await keyed.push("w1", {"n": 1})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.retry(delay_ms=900_000))
    await keyed._signal("w1")
    await keyed.push("w1", {"n": 2}, order=4e12)  # sorts after; re-signals too
    assert await keyed.lease() is None
    assert await keyed.lease() is None
    assert await fake_redis.zscore(keyed._k.delayed, "w1") == keyed.now_ms() + 900_000


async def test_wait_without_until_gets_safety_deadline(keyed, fake_redis, clock):
    await keyed.push("w1", {})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.wait(reason="downstream"))
    assert await fake_redis.zscore(keyed._k.delayed, "w1") == keyed.now_ms() + 100_000
    await keyed._signal("w1")
    assert await keyed.lease() is None
    clock.advance_ms(100_001)
    await keyed._signal("w1")
    assert (await keyed.lease()).message.message_id == lease.message.message_id


async def test_wait_blocks_key_until_deadline_or_signal(keyed, fake_redis, clock):
    await keyed.push("w1", {"n": 1}, order=1)
    lease = await keyed.lease()
    until = keyed.now_ms() + 60_000
    await keyed.apply(lease, Outcome.wait(until_ms=until))
    await keyed.push("w1", {"n": 2}, order=2)  # re-signals
    assert await keyed.lease() is None  # head still waiting
    head = await fake_redis.zrange(keyed._k.key("w1"), 0, 0)
    assert head == [lease.message.message_id]
    clock.advance_ms(60_001)
    await keyed._signal("w1")
    nxt = await keyed.lease()
    assert nxt.message.message_id == lease.message.message_id


async def test_hold_blocks_key(keyed, fake_redis):
    await keyed.push("w1", {})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.hold("rejected"))
    assert await fake_redis.hget(keyed._k.held, "w1") is not None
    await keyed._signal("w1")
    assert await keyed.lease() is None
    assert await fake_redis.zcard(keyed._k.key("w1")) == 1


async def test_max_age_expires_head_to_dlq(fake_redis, clock, dlq):
    q = build(fake_redis, clock, dlq, max_age_ms=1_000)
    await q.push("w1", {"n": "old"}, order=1)
    clock.advance_ms(2_000)
    await q.push("w1", {"n": "new"}, order=2)
    lease = await q.lease()
    assert lease is not None and lease.message.body == {"n": "new"}
    assert len(dlq.calls) == 1
    envelope, attempts, error, sub = dlq.calls[0]
    assert envelope.payload == {"n": "old"} and attempts == 0
    assert "expired" in error and sub == "keyed:punches"
    assert q.expired_count == 1


async def test_expiry_logs_expiring(fake_redis, clock, dlq):
    q = build(fake_redis, clock, dlq, max_age_ms=1_000)
    records = []

    class H(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    q.logger.addHandler(H())
    await q.push("w1", {})
    clock.advance_ms(2_000)
    assert await q.lease() is None
    assert any("expiring" in m for m in records)
    assert await fake_redis.zcard(q._k.key("w1")) == 0
    assert not await fake_redis.sismember(q._k.keys, "w1")


async def test_push_is_idempotent_by_message_id(keyed, fake_redis):
    first = await keyed.push("w1", {"n": 1}, message_id="m1", order=5)
    second = await keyed.push("w1", {"n": 999}, message_id="m1", order=1)
    assert first == second == "m1"
    assert await fake_redis.zcard(keyed._k.key("w1")) == 1
    raw = await fake_redis.hgetall(keyed._k.msg("m1"))
    assert raw["body"] == '{"n": 1}' and raw["attempts"] == "0"
    assert await fake_redis.zscore(keyed._k.key("w1"), "m1") == 5.0
    assert len(fake_redis.streams[keyed._store.stream_for("punches-signal")]) == 2  # re-signalled


async def test_push_same_id_other_key_rejected(keyed):
    await keyed.push("w1", {}, message_id="m1")
    with pytest.raises(ValueError):
        await keyed.push("w2", {}, message_id="m1")


async def test_push_is_atomic(keyed, fake_redis, monkeypatch):
    pipe_cls = type(fake_redis.pipeline())
    real_execute = pipe_cls.execute

    async def failing_execute(self):
        await self.reset()
        raise RuntimeError("crash before EXEC")

    monkeypatch.setattr(pipe_cls, "execute", failing_execute)
    with pytest.raises(RuntimeError):
        await keyed.push("w1", {"n": 1}, message_id="m1")
    assert await fake_redis.exists(keyed._k.msg("m1")) == 0
    assert await fake_redis.zcard(keyed._k.key("w1")) == 0
    assert await fake_redis.scard(keyed._k.keys) == 0
    monkeypatch.setattr(pipe_cls, "execute", real_execute)
    await keyed.push("w1", {"n": 1}, message_id="m1")
    assert await fake_redis.zcard(keyed._k.key("w1")) == 1
    assert await fake_redis.hget(keyed._k.msg("m1"), "key") == "w1"
    assert await fake_redis.zscore(keyed._k.all, "m1") is not None


async def test_max_attempts_parks_to_dlq(fake_redis, clock, dlq):
    q = build(fake_redis, clock, dlq, max_attempts=3)
    await q.push("w1", {"n": 1}, order=1)
    await q.push("w1", {"n": 2}, order=2)
    for _ in range(3):
        lease = await q.lease()
        await q.apply(lease, Outcome.retry(delay_ms=0, reason="down"))
        await q._signal("w1")
    assert not dlq.calls
    lease = await q.lease()
    await q.apply(lease, Outcome.retry(delay_ms=0, reason="down"))  # 4th
    assert len(dlq.calls) == 1
    assert dlq.calls[0][1] == 4 and dlq.calls[0][0].payload == {"n": 1}
    nxt = await q.lease()  # key unblocked
    assert nxt.message.body == {"n": 2}


async def test_signal_kept_while_leased_and_recovered_after_crash(fake_redis, clock, dlq):
    a = build(fake_redis, clock, dlq)
    b = build(fake_redis, clock, dlq)
    await a.push("w1", {"n": 1}, order=1)
    leased = await a.lease()  # A "dies": no apply
    assert leased is not None
    stream = a._store.stream_for("punches-signal")
    pending = fake_redis.groups[(stream, "workers")]["pending"]
    assert len(pending) == 1  # A's signal is kept un-acked
    await a.push("w1", {"n": 2}, order=2)  # duplicate signal
    assert await b.lease() is None  # lock alive → acked as duplicate
    assert len(pending) == 1
    clock.advance_ms(21_000)  # A's lock expired AND its signal lease lapsed
    recovered = await b.lease()
    assert recovered is not None and recovered.message.message_id == leased.message.message_id


async def test_pause_blocks_lease_without_receive(keyed, fake_redis, clock):
    await keyed.push("w1", {})
    await keyed.pause(ttl_ms=100_000, reason="downstream down")
    assert await keyed.is_paused() == "downstream down"
    fake_redis.calls.clear()
    for _ in range(10):
        assert await keyed.lease() is None
    assert not [c for c in fake_redis.calls if c[0] in ("xreadgroup", "xautoclaim")]
    await keyed.resume()
    assert await keyed.is_paused() is None
    assert (await keyed.lease()).key == "w1"


async def test_pause_expires_by_ttl(keyed, clock):
    await keyed.pause(ttl_ms=1_000)
    clock.advance_ms(1_001)
    assert await keyed.is_paused() is None


async def test_stale_apply_is_fenced(keyed, fake_redis, clock):
    """A's lock expires and B leases between A's token check and EXEC."""
    await keyed.push("w1", {"n": 1})
    stale = await keyed.lease()
    pipe_cls = type(fake_redis.pipeline())
    real_multi = pipe_cls.multi
    fired = {"done": False}

    def multi_with_takeover(self):
        # Injected between A's reads and its EXEC: the lock changes owner.
        if not fired["done"]:
            fired["done"] = True
            fake_redis.strings.pop(keyed._k.lock("w1"), None)
            fake_redis.strings[keyed._k.lock("w1")] = "other-token"
            fake_redis._bump(keyed._k.lock("w1"))
        return real_multi(self)

    pipe_cls.multi = multi_with_takeover
    try:
        with pytest.raises(LeaseLost):
            await keyed.apply(stale, Outcome.ack())
    finally:
        pipe_cls.multi = real_multi
    assert await fake_redis.zcard(keyed._k.key("w1")) == 1
    assert await fake_redis.exists(keyed._k.msg(stale.message.message_id)) == 1


async def test_apply_twice_is_lease_lost(keyed):
    await keyed.push("w1", {})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.ack())
    with pytest.raises(LeaseLost):
        await keyed.apply(lease, Outcome.ack())


async def test_lease_extend_keeps_lock_and_signal(keyed, fake_redis, clock):
    await keyed.push("w1", {})
    lease = await keyed.lease()
    clock.advance_ms(8_000)
    await lease.extend()
    clock.advance_ms(8_000)  # past the original TTL
    await keyed.apply(lease, Outcome.ack())  # still ours
    assert await keyed.lease() is None


async def test_extend_after_loss_raises(keyed, clock):
    await keyed.push("w1", {})
    lease = await keyed.lease()
    clock.advance_ms(11_000)
    with pytest.raises(LeaseLost):
        await lease.extend()


def test_backoff_capped_exponential():
    seq = [backoff_ms(n, base_ms=30_000, max_ms=900_000, factor=2.0) for n in range(7)]
    assert seq == [30_000, 60_000, 120_000, 240_000, 480_000, 900_000, 900_000]


async def test_prefix_must_be_disjoint(fake_redis, clock):
    from navigator_eventbus.queues.config import DEFAULT_QUEUE_PREFIX

    with pytest.raises(Exception):
        build(fake_redis, clock, prefix=DEFAULT_QUEUE_PREFIX)


async def test_same_millisecond_pushes_keep_arrival_order(keyed):
    ids = ["z", "y", "x", "w", "v"]  # reverse-lexical ids: ties must not decide
    for i, mid in enumerate(ids):
        await keyed.push("w1", {"i": i}, message_id=mid)
    got = []
    for _ in ids:
        lease = await keyed.lease_key("w1")
        got.append(lease.message.body["i"])
        await keyed.apply(lease, Outcome.ack())
    assert got == [0, 1, 2, 3, 4]


async def test_hold_on_vanished_message_is_cleared(keyed, fake_redis):
    await keyed.push("w1", {})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.hold("op"))
    # Redis lost the message hash: the held entry must not block the key forever
    await fake_redis.delete(keyed._k.msg(lease.message.message_id))
    assert await keyed.release_hold("w1") is True  # must not resurrect a ghost hash
    assert await fake_redis.exists(keyed._k.msg(lease.message.message_id)) == 0
    await fake_redis.hset(keyed._k.held, "w1", "{}")  # orphan hold again
    assert await keyed.lease_key("w1") is None  # dangling member repaired, key empty
    assert await fake_redis.hget(keyed._k.held, "w1") is None
    assert not await fake_redis.sismember(keyed._k.keys, "w1")
