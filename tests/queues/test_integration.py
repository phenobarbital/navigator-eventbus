"""QueueStore against a real Redis (queues, TASK-G).

Marked ``integration`` and skipped when no Redis is reachable, matching the
two existing real-Redis tests in ``tests/test_backends_streams.py``.

These are not redundant with the unit tier. Three behaviours the whole
visibility design rests on are *documented* Redis semantics that the fake
merely re-states — if the fake and the store shared a misunderstanding, the
unit tests would agree with each other and both be wrong:

1. ``XCLAIM ... JUSTID`` does not increment ``times_delivered``.
2. ``XCLAIM ... IDLE n`` sets the **absolute** idle time.
3. ``XAUTOCLAIM`` returns a 3-tuple on Redis 7.0+.

Run with::

    REDIS_URL=redis://localhost:6379 pytest tests/queues/test_integration.py -m integration
"""
import asyncio
import os
import uuid

import pytest

from navigator_eventbus.envelope import EventEnvelope
from navigator_eventbus.queues.config import (
    QueueConfig,
    QueueGroupConfig,
    QueueRegistry,
)
from navigator_eventbus.queues.receipts import ReceiptCodec
from navigator_eventbus.queues.store import QueueStore

pytestmark = pytest.mark.integration

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
aioredis = pytest.importorskip("redis.asyncio")


@pytest.fixture
async def client():
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        await redis.ping()
    except Exception:  # noqa: BLE001
        pytest.skip(f"no Redis reachable at {REDIS_URL}")
    yield redis
    await redis.aclose()


@pytest.fixture
def config() -> QueueConfig:
    return QueueConfig(
        name=f"itest-{uuid.uuid4().hex[:8]}",
        groups=[QueueGroupConfig(name="billing"), QueueGroupConfig(name="analytics")],
        default_visibility_timeout_ms=1_000,
        max_visibility_timeout_ms=5_000,
        max_receives=2,
    )


@pytest.fixture
async def store(client, config):
    registry = QueueRegistry(queues=[config])
    instance = QueueStore(registry, client, ReceiptCodec(["itest-key"]))
    await instance.ensure_queue(config)
    yield instance
    await client.delete(registry.stream_for(config.name))


def envelope(**payload) -> EventEnvelope:
    return EventEnvelope(topic="queue.itest.event", payload=payload or {"n": 1})


# ---------------------------------------------------------------------------
# The load-bearing Redis semantics
# ---------------------------------------------------------------------------


async def test_justid_does_not_inflate_the_delivery_counter(store, config, client):
    """If JUSTID incremented, max_receives accounting would silently be wrong."""
    await store.send(config, envelope())
    received = await store.receive(config, "billing", visibility_ms=2_000)
    assert len(received) == 1
    assert received[0].delivered == 1

    stream = store.stream_for(config.name)
    pending = await client.xpending_range(
        name=stream, groupname="billing", min="-", max="+", count=10
    )
    assert pending[0]["times_delivered"] == 1, (
        "the visibility XCLAIM must use JUSTID; without it the counter inflates "
        "on every receive and messages reach the DLQ far too early"
    )


async def test_idle_offset_grants_the_requested_lease(store, config, client):
    """The mechanism the whole per-message visibility design rests on."""
    await store.send(config, envelope())
    await store.receive(config, "billing", visibility_ms=1_000)

    # threshold is 5000ms and the lease 1000ms, so idle was set to ~4000ms.
    stream = store.stream_for(config.name)
    pending = await client.xpending_range(
        name=stream, groupname="billing", min="-", max="+", count=10
    )
    idle = pending[0]["time_since_delivered"]
    assert 3_500 <= idle <= 4_500, f"expected idle offset ~4000ms, got {idle}"


async def test_message_reappears_only_after_its_lease(store, config):
    await store.send(config, envelope())
    first = await store.receive(config, "billing", visibility_ms=1_000)
    assert len(first) == 1

    assert await store.receive(config, "billing") == [], "must be invisible while leased"

    await asyncio.sleep(1.3)
    again = await store.receive(config, "billing", visibility_ms=1_000)
    assert len(again) == 1
    assert again[0].message_id == first[0].message_id
    assert again[0].delivered == 2


async def test_xautoclaim_returns_a_three_tuple(store, config, client):
    """Redis 7.0+ adds the deleted-ids element the store guards for."""
    await store.send(config, envelope())
    await store.receive(config, "billing", visibility_ms=1_000)
    await asyncio.sleep(1.3)
    result = await client.xautoclaim(
        store.stream_for(config.name), "billing", "probe", min_idle_time=0, start_id="0-0"
    )
    assert len(result) == 3


# ---------------------------------------------------------------------------
# Delivery guarantees
# ---------------------------------------------------------------------------


async def test_two_groups_each_receive_every_message(store, config):
    await store.send(config, envelope(n=1))
    await store.send(config, envelope(n=2))

    billing = await store.receive(config, "billing", max_messages=10)
    analytics = await store.receive(config, "analytics", max_messages=10)

    assert sorted(m.envelope.payload["n"] for m in billing) == [1, 2]
    assert sorted(m.envelope.payload["n"] for m in analytics) == [1, 2]


async def test_two_consumers_in_one_group_are_disjoint(client, config):
    registry = QueueRegistry(queues=[config])
    codec = ReceiptCodec(["itest-key"])
    a = QueueStore(registry, client, codec, consumer_name="itest-a")
    b = QueueStore(registry, client, codec, consumer_name="itest-b")
    await a.ensure_queue(config)
    try:
        for index in range(4):
            await a.send(config, envelope(n=index))
        got_a = await a.receive(config, "billing", max_messages=2)
        got_b = await b.receive(config, "billing", max_messages=2)
        ids_a = {m.message_id for m in got_a}
        ids_b = {m.message_id for m in got_b}
        assert len(ids_a) == 2 and len(ids_b) == 2
        assert ids_a.isdisjoint(ids_b)
    finally:
        await client.delete(registry.stream_for(config.name))


async def test_delete_removes_only_from_that_group(store, config):
    await store.send(config, envelope())
    billing = (await store.receive(config, "billing"))[0]
    await store.delete(config, billing.receipt)

    assert await store.receive(config, "billing") == []
    assert len(await store.receive(config, "analytics")) == 1


# ---------------------------------------------------------------------------
# nack / DLQ / admin
# ---------------------------------------------------------------------------


async def test_change_visibility_zero_is_an_immediate_nack(store, config):
    await store.send(config, envelope())
    message = (await store.receive(config, "billing", visibility_ms=5_000))[0]
    await store.change_visibility(config, message.receipt, 0)

    again = await store.receive(config, "billing")
    assert len(again) == 1
    assert again[0].message_id == message.message_id


async def test_poison_message_reaches_the_dlq_and_stops(client, config):
    parked = []

    async def on_dlq(env, **kwargs):
        parked.append(kwargs)

    registry = QueueRegistry(queues=[config])
    store = QueueStore(
        registry, client, ReceiptCodec(["itest-key"]), on_dlq=on_dlq
    )
    await store.ensure_queue(config)
    try:
        await store.send(config, envelope(poison=True))
        for _ in range(4):
            await store.receive(config, "billing", visibility_ms=200)
            await asyncio.sleep(0.35)
        assert len(parked) == 1
        assert parked[0]["attempts"] > config.max_receives
        assert await store.receive(config, "billing") == []
    finally:
        await client.delete(registry.stream_for(config.name))


async def test_describe_reports_backlog_and_inflight(store, config):
    for _ in range(3):
        await store.send(config, envelope())
    await store.receive(config, "billing", max_messages=1)

    info = await store.describe(config, "billing")
    assert info["stream_length"] == 3
    assert info["approximate_number_of_messages_not_visible"] == 1
    assert info["approximate_number_of_messages"] == 2


async def test_purge_empties_and_leaves_the_group_usable(store, config, client):
    for _ in range(3):
        await store.send(config, envelope())
    await store.receive(config, "billing")

    await store.purge(config)

    assert await client.xlen(store.stream_for(config.name)) == 0
    assert await store.receive(config, "billing") == []
    await store.send(config, envelope(after="purge"))
    got = await store.receive(config, "billing")
    assert len(got) == 1 and got[0].envelope.payload == {"after": "purge"}


async def test_long_poll_returns_as_soon_as_a_message_arrives(store, config):
    async def produce_later():
        await asyncio.sleep(0.2)
        await store.send(config, envelope(late=True))

    task = asyncio.create_task(produce_later())
    try:
        loop = asyncio.get_running_loop()
        started = loop.time()
        got = await store.receive(config, "billing", wait_ms=3_000)
        elapsed = loop.time() - started
        assert len(got) == 1
        assert elapsed < 2.0, "long poll should wake on arrival, not time out"
    finally:
        await task
