"""Tests of the keyed-queue command surface of FakeStreamsRedis (TASK-4574)."""
import pytest
from redis.asyncio.lock import Lock
from redis.exceptions import LockNotOwnedError, WatchError

from ._fake_streams import FakeClock, FakeStreamsRedis


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def r(clock) -> FakeStreamsRedis:
    return FakeStreamsRedis(clock)


async def test_set_nx_get_delete_and_ttl(r, clock):
    assert await r.set("k", "v", nx=True, px=1000) is True
    assert await r.set("k", "w", nx=True) is None
    assert await r.get("k") == "v"
    assert 0 < await r.pttl("k") <= 1000
    clock.advance_ms(1000)
    assert await r.get("k") is None
    assert await r.pttl("k") == -2
    await r.set("a", "1")
    assert await r.exists("a", "zz") == 1
    assert await r.delete("a", "zz") == 1
    assert await r.set("x", "1", xx=True) is None


async def test_pexpire(r, clock):
    await r.set("k", "v")
    assert await r.pttl("k") == -1
    assert await r.pexpire("k", 500) is True
    clock.advance_ms(500)
    assert await r.exists("k") == 0
    assert await r.pexpire("k", 5) is False


async def test_hashes(r):
    assert await r.hset("h", "a", "1") == 1
    assert await r.hset("h", mapping={"a": "2", "b": 3}) == 1
    assert await r.hsetnx("h", "a", "9") == 0
    assert await r.hsetnx("h", "c", "9") == 1
    assert await r.hget("h", "a") == "2"
    assert await r.hgetall("h") == {"a": "2", "b": "3", "c": "9"}
    assert await r.hincrby("h", "b", 2) == 5
    assert await r.hincrby("h", "n") == 1
    assert await r.hlen("h") == 4
    assert await r.hdel("h", "a", "zz") == 1
    assert await r.hget("h", "a") is None


async def test_sets(r):
    assert await r.sadd("s", "a", "b", "a") == 2
    assert await r.scard("s") == 2
    assert await r.sismember("s", "a") is True
    assert await r.smembers("s") == {"a", "b"}
    assert await r.srem("s", "a", "zz") == 1
    await r.srem("s", "b")
    assert await r.exists("s") == 0


async def test_zsets(r):
    assert await r.zadd("z", {"b": 2, "a": 2, "c": 1}) == 3
    assert await r.zadd("z", {"c": 5}, nx=True) == 0
    assert await r.zscore("z", "c") == 1.0
    assert await r.zrange("z", 0, 0) == ["c"]
    assert await r.zrange("z", 0, -1) == ["c", "a", "b"]  # ties by member
    assert await r.zrange("z", 0, 0, withscores=True) == [("c", 1.0)]
    assert await r.zrangebyscore("z", "-inf", 2) == ["c", "a", "b"]
    assert await r.zrangebyscore("z", "(1", "+inf") == ["a", "b"]
    assert await r.zrangebyscore("z", 0, 10, start=1, num=1) == ["a"]
    assert await r.zcard("z") == 3
    assert await r.zrem("z", "a", "zz") == 1
    assert await r.zscore("z", "a") is None


async def test_pipeline_multi_exec(r):
    pipe = r.pipeline(transaction=True)
    pipe.hset("h", "a", "1")
    pipe.zadd("z", {"m": 1})
    pipe.sadd("s", "x")
    assert await pipe.execute() == [1, 1, 1]
    assert await r.zcard("z") == 1


async def test_pipeline_watch_detects_every_family(r):
    for write in (
        lambda: r.set("w", "1"),
        lambda: r.hset("w", "a", "1"),
        lambda: r.sadd("w", "a"),
        lambda: r.zadd("w", {"a": 1}),
    ):
        await r.delete("w")
        async with r.pipeline() as pipe:
            await pipe.watch("w")
            assert await pipe.get("w") is None  # immediate mode
            await write()
            pipe.multi()
            pipe.set("other", "1")
            with pytest.raises(WatchError):
                await pipe.execute()
    assert await r.get("other") is None


async def test_pipeline_watch_clean_executes(r):
    async with r.pipeline() as pipe:
        await pipe.watch("w")
        pipe.multi()
        pipe.set("w", "1")
        assert await pipe.execute() == [True]


async def test_expiry_of_watched_key_does_not_abort(r, clock):
    """Modern Redis: a watched key merely expiring leaves EXEC valid."""
    await r.set("w", "1", px=100)
    async with r.pipeline() as pipe:
        await pipe.watch("w")
        clock.advance_ms(200)
        pipe.multi()
        pipe.set("z", "1")
        assert await pipe.execute() == [True]


async def test_real_lock_round_trip(r, clock):
    lock = r.lock("lk", timeout=10, blocking=False)
    assert await lock.acquire(blocking=False) is True
    other = r.lock("lk", timeout=10, blocking=False)
    assert await other.acquire(blocking=False) is False
    assert await lock.owned() is True
    assert await other.owned() is False
    await lock.extend(5)
    assert 14_000 < await r.pttl("lk") <= 15_000
    await lock.release()
    assert await r.get("lk") is None
    assert await other.acquire(blocking=False) is True

    clock.advance_ms(10_000)  # expiry
    assert await other.owned() is False
    with pytest.raises(LockNotOwnedError):
        await other.release()
    third = r.lock("lk", timeout=10, blocking=False)
    assert await third.acquire(blocking=False) is True
    await third.reacquire()


async def test_unknown_script_rejected(r):
    script = r.register_script("return redis.call('ping')")
    with pytest.raises(NotImplementedError):
        await script(keys=["k"], args=["t"], client=r)
    assert Lock.LUA_RELEASE_SCRIPT  # sanity: scripts used by the shim exist
