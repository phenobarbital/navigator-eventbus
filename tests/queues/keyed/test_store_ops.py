# ruff: noqa: F811
"""KeyedQueue operations: promote_due, recover, release_hold, peek, keys, describe."""
import asyncio

from navigator_eventbus.queues.keyed.config import Outcome

from .test_store import build, clock, dlq, fake_redis, keyed, lose_signals  # noqa: F401


def stream_len(q, fake_redis) -> int:
    return len(fake_redis.streams.get(q._store.stream_for("punches-signal"), []))


async def test_retry_then_promote_due_makes_head_leasable(keyed, fake_redis, clock):
    await keyed.push("w1", {"n": 1})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.retry(delay_ms=5_000))
    assert await keyed.promote_due() == 0  # not due yet
    clock.advance_ms(5_001)
    assert await keyed.promote_due() == 1
    assert await fake_redis.zscore(keyed._k.delayed, "w1") is None
    again = await keyed.lease()
    assert again.message.message_id == lease.message.message_id


async def test_promote_due_is_leaderless(keyed, fake_redis, clock):
    await keyed.push("w1", {})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.retry(delay_ms=1_000))
    clock.advance_ms(1_001)
    before = stream_len(keyed, fake_redis)
    a, b = build(fake_redis, clock), build(fake_redis, clock)
    results = await asyncio.gather(a.promote_due(), b.promote_due(), keyed.promote_due())
    assert sorted(results) == [0, 0, 1]
    assert stream_len(keyed, fake_redis) == before + 1


async def test_promote_due_respects_limit(keyed, fake_redis, clock):
    for i in range(3):
        await fake_redis.zadd(keyed._k.delayed, {f"k{i}": 1.0})
    assert await keyed.promote_due(limit=2) == 2
    assert await fake_redis.zcard(keyed._k.delayed) == 1


async def test_recover_resignals_orphan_key(keyed, fake_redis, clock):
    await keyed.push("w1", {})
    # lose the signal
    lose_signals(keyed, fake_redis)
    before = stream_len(keyed, fake_redis)
    other = build(fake_redis, clock)
    results = await asyncio.gather(keyed.recover(), other.recover())
    assert sorted(results) == [0, 1]  # throttle → one pass
    assert stream_len(keyed, fake_redis) == before + 1
    assert await keyed.recover() == 0  # still throttled
    clock.advance_ms(60_001)
    assert await keyed.recover() == 1  # marker lapsed with the signal window
    assert await keyed.recover(force=True) == 0  # fresh marker: live signal
    lose_signals(keyed, fake_redis)
    assert await keyed.recover(force=True) == 1


async def test_recover_skips_locked_held_and_genuinely_delayed(keyed, fake_redis, clock):
    for k in ("locked", "held", "delayed", "plain"):
        await keyed.push(k, {})
    lease = await keyed.lease_key("delayed")  # make "delayed"'s head genuinely not due
    await keyed.apply(lease, Outcome.retry(delay_ms=900_000))
    lose_signals(keyed, fake_redis)
    await fake_redis.set(keyed._k.lock("locked"), "tok", px=10_000)
    await fake_redis.hset(keyed._k.held, "held", "{}")
    assert await keyed.recover(force=True) == 1
    entry = fake_redis.streams[keyed._store.stream_for("punches-signal")][0][1]
    assert "plain" in str(entry)


async def test_promote_crash_between_zrem_and_send_is_recovered(keyed, fake_redis, clock):
    await keyed.push("w1", {})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.retry(delay_ms=1_000))
    clock.advance_ms(1_001)
    lose_signals(keyed, fake_redis)
    await fake_redis.zrem(keyed._k.delayed, "w1")  # ZREM done, send never happened
    assert await keyed.lease() is None  # nothing signals the key
    assert await keyed.recover(force=True) == 1
    assert (await keyed.lease()).message.message_id == lease.message.message_id


async def test_hold_blocks_key_release_hold_resumes(keyed, fake_redis):
    await keyed.push("w1", {})
    lease = await keyed.lease()
    await keyed.apply(lease, Outcome.hold("operator"))
    await keyed._signal("w1")
    assert await keyed.lease() is None
    assert await keyed.release_hold("w1") is True
    assert await keyed.release_hold("w1") is False
    head = (await keyed.peek("w1"))[0]
    assert head.state == "pending"
    again = await keyed.lease()
    assert again.message.message_id == lease.message.message_id


async def test_peek_keys_describe(keyed, fake_redis, clock):
    await keyed.push("b", {"n": 3}, order=3)
    await keyed.push("a", {"n": 2}, order=2)
    await keyed.push("a", {"n": 1}, order=1)
    clock.advance_ms(500)
    assert await keyed.keys() == ["a", "b"]
    assert [m.body["n"] for m in await keyed.peek("a")] == [1, 2]
    assert len(await keyed.peek("a", limit=1)) == 1
    assert await keyed.peek("zzz") == []
    info = await keyed.describe()
    assert info == {
        "name": "punches", "keys": 2, "messages": 3, "delayed": 0, "held": 0,
        "paused": None, "oldest_age_ms": 500,
    }
    await keyed.pause(reason="x")
    assert (await keyed.describe())["paused"] == "x"


async def test_describe_empty(keyed):
    assert (await keyed.describe())["oldest_age_ms"] is None


async def test_push_racing_drop_empty_never_loses_the_key(keyed, fake_redis):
    """gather-interleaved: a push landing while an empty key is being dropped."""
    for _ in range(5):
        await fake_redis.sadd(keyed._k.keys, "w1")  # stale entry, key:w1 empty
        await asyncio.gather(
            keyed._drop_key_if_empty("w1"),
            keyed.push("w1", {"n": 1}, message_id=f"m{_}"),
        )
        assert await fake_redis.zcard(keyed._k.key("w1")) >= 1
        assert await fake_redis.sismember(keyed._k.keys, "w1")
        await fake_redis.delete(keyed._k.key("w1"), keyed._k.msg(f"m{_}"))
