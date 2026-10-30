"""KeyedQueue against a real Redis (FEAT-713 / TASK-4579). Skipped without Redis.

Uses a dedicated DB (``KEYED_TEST_REDIS_DB``, default 15) and refuses to run
unless that DB is empty, so ``FLUSHDB`` can only ever remove our own keys.
"""
import asyncio
import os
import uuid

import pytest

from navigator_eventbus.queues.config import QueueConfig, QueueGroupConfig, QueueRegistry
from navigator_eventbus.queues.keyed.config import KeyedQueueConfig, Outcome
from navigator_eventbus.queues.keyed.consumer import KeyedConsumer
from navigator_eventbus.queues.keyed.store import KeyedQueue
from navigator_eventbus.queues.receipts import ReceiptCodec
from navigator_eventbus.queues.store import QueueStore

pytestmark = pytest.mark.integration

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_DB = int(os.getenv("KEYED_TEST_REDIS_DB", "15"))
aioredis = pytest.importorskip("redis.asyncio")


@pytest.fixture
async def client():
    from redis.asyncio.lock import Lock

    redis = aioredis.from_url(REDIS_URL, db=REDIS_DB, decode_responses=True)
    try:
        await redis.ping()
    except Exception:  # noqa: BLE001
        pytest.skip(f"no Redis reachable at {REDIS_URL}")
    if await redis.dbsize():
        await redis.aclose()
        pytest.skip(f"Redis DB {REDIS_DB} is not empty; refusing to use it")
    # redis-py caches Lock scripts on the class; the fake-based tests may have
    # registered theirs first.
    Lock.lua_release = Lock.lua_extend = Lock.lua_reacquire = None
    yield redis
    await redis.flushdb()  # DB verified empty above, so these are our keys
    Lock.lua_release = Lock.lua_extend = Lock.lua_reacquire = None
    await redis.aclose()


def make_queue(client, **cfg) -> KeyedQueue:
    signal = QueueConfig(
        name=f"sig-{uuid.uuid4().hex[:8]}",
        groups=[QueueGroupConfig(name="workers")],
        default_visibility_timeout_ms=20_000,
        max_visibility_timeout_ms=20_000,
    )
    store = QueueStore(QueueRegistry(queues=[signal]), client, ReceiptCodec(["k"]))
    return KeyedQueue(
        KeyedQueueConfig(name=f"it{uuid.uuid4().hex[:6]}", signal=signal, lock_ttl_ms=10_000, **cfg),
        client,
        store=store,
    )


async def test_keyed_queue_real_redis_ordering_and_lock(client):
    queue = make_queue(client)
    keys, per_key = 50, 5
    for k in range(keys):
        for i in range(per_key):
            await queue.push(f"k{k}", {"i": i}, order=i)

    in_flight: set[str] = set()
    seen: dict[str, list[int]] = {}
    violations: list[str] = []

    async def handler(msg):
        if msg.key in in_flight:
            violations.append(msg.key)
        in_flight.add(msg.key)
        await asyncio.sleep(0)
        seen.setdefault(msg.key, []).append(msg.body["i"])
        in_flight.discard(msg.key)
        return Outcome.ack()

    stop = asyncio.Event()
    consumers = [KeyedConsumer(queue, wait_ms=50) for _ in range(2)]
    tasks = [asyncio.create_task(c.run(handler, stop=stop)) for c in consumers]
    try:
        async with asyncio.timeout(60):
            while (await queue.describe())["messages"] > 0:
                await asyncio.sleep(0.05)
    finally:
        stop.set()
        await asyncio.gather(*tasks)
    assert not violations
    assert len(seen) == keys
    assert all(v == list(range(per_key)) for v in seen.values())


async def test_rebuild_after_flushdb(client):
    queue = make_queue(client)
    records = [(f"w{i % 3}", f"id-{i}", float(i)) for i in range(10)]
    for key, mid, order in records:
        await queue.push(key, {"id": mid}, message_id=mid, order=order)

    def snapshot():
        return queue.keys(), [queue.peek(k, limit=20) for k in ("w0", "w1", "w2")]

    async def state():
        ks, peeks = snapshot()
        return await ks, [[m.message_id for m in await p] for p in peeks]

    before = await state()
    await client.flushdb()  # DB checked empty at fixture start
    assert (await queue.describe())["messages"] == 0
    for key, mid, order in records:  # rebuild from the caller's record
        await queue.push(key, {"id": mid}, message_id=mid, order=order)
        await queue.push(key, {"id": mid}, message_id=mid, order=order)  # idempotent
    assert await state() == before
    assert (await queue.describe())["messages"] == 10


async def test_default_order_is_arrival_order_on_real_redis(client):
    """5000 back-to-back pushes without `order`: the old `seq % 1000` tiebreak
    put push #3999 before #3998."""
    queue = make_queue(client)
    n = 5_000
    for i in range(n):
        await queue.push("w1", {"i": i})
    heads = await queue.peek("w1", limit=n)
    assert [m.body["i"] for m in heads] == list(range(n))


async def test_recover_sweeps_all_orphans_in_one_pass_on_real_redis(client):
    """Real SSCAN/pipeline semantics: 1,500 orphan keys, one pass, no duplicates after."""
    queue = make_queue(client)
    for i in range(1_500):
        key = f"k{i:05d}"
        await client.zadd(queue._k.key(key), {f"m{i}": 1})
        await client.hset(queue._k.msg(f"m{i}"), mapping={"key": key, "body": "{}"})
        await client.sadd(queue._k.keys, key)
    assert await queue.recover() == 1_500
    assert await queue.recover(force=True) == 0  # live markers: nothing re-signalled


async def test_backlog_detection_and_per_signal_markers_on_real_redis(client):
    """Real XINFO GROUPS lag / XRANGE / ZREM semantics behind R3-1..R3-3."""
    queue = make_queue(client)
    assert await queue._has_backlog() is False  # no stream yet
    await queue.push("a", {})
    await queue.push("b", {})
    assert await queue._has_backlog() is True  # undelivered signals (group not created yet)
    assert await queue.recover(force=True) == 0  # quiet during backlog
    for _ in range(2):
        lease = await queue.lease()
        assert lease is not None
        live = await client.zrange(queue._k.sig(lease.key), 0, -1)
        assert lease.receipt.message_id in live
        await queue.apply(lease, Outcome.ack())
        assert lease.receipt.message_id not in await client.zrange(queue._k.sig(lease.key), 0, -1)
    assert await queue._has_backlog() is False
