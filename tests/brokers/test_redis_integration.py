"""Redis integration smoke tests for navigator_eventbus.brokers.redis
(TASK-1819, FEAT-316).

Marked ``@pytest.mark.redis`` — skipped gracefully when no live Redis is
reachable (deselect explicitly with ``-m "not redis"``).

Note: ``RedisConnection.ensure_group_exists()`` (ported verbatim from the
navigator source) seeds brand-new streams with an ``{'initial': 'message'}``
entry so ``XGROUP CREATE`` has something to anchor to. That entry has no
``body``/``ContentType`` fields, so it decodes as a bare ``None`` — these
tests account for it instead of assuming the first message read is the
payload they published.
"""
import asyncio
import uuid

import pytest

from navigator_eventbus.brokers.redis import RedisConnection, RedisConsumer

pytestmark = pytest.mark.redis


@pytest.fixture
async def redis_conn():
    conn = RedisConnection()
    try:
        await asyncio.wait_for(conn.connect(), timeout=2)
    except Exception:
        pytest.skip("no live Redis available")
    yield conn
    for stream in list(conn._queues.keys()):
        try:
            await conn._connection.delete(stream)
        except Exception:
            pass
    await conn.disconnect()


async def test_redis_publish_consume_roundtrip(redis_conn):
    stream = f"it_stream_{uuid.uuid4().hex[:8]}"
    redis_conn._queue_name = stream
    redis_conn._group_name = f"it_group_{uuid.uuid4().hex[:8]}"
    await redis_conn.ensure_group_exists()  # seeds the {'initial': 'message'} entry

    await redis_conn.publish_message({"hello": "world"}, queue_name=stream)

    received = []

    async def _consume_until_found():
        while not any(isinstance(m, dict) and m.get("hello") == "world" for m in received):
            response = await redis_conn._connection.xreadgroup(
                groupname=redis_conn._group_name,
                consumername=redis_conn._consumer_name,
                streams={stream: ">"},
                count=1,
                block=2000,
            )
            for _, messages in response:
                for message_id, message_data in messages:
                    processed = await redis_conn.process_message(message_data)
                    received.append(processed)
                    await redis_conn._connection.xack(
                        stream, redis_conn._group_name, message_id
                    )

    await asyncio.wait_for(_consume_until_found(), timeout=5)

    assert any(isinstance(m, dict) and m.get("hello") == "world" for m in received)

    await redis_conn._connection.delete(stream)


async def test_redis_consumer_subscribe_events(redis_conn):
    stream = f"it_stream_{uuid.uuid4().hex[:8]}"
    consumer = RedisConsumer(
        queue_name=stream,
        group_name=f"it_group_{uuid.uuid4().hex[:8]}",
    )
    try:
        await asyncio.wait_for(consumer.connect(), timeout=2)
    except Exception:
        pytest.skip("no live Redis available")

    received = []

    async def cb(message_id, body):
        received.append(body)

    try:
        await consumer.subscribe_to_events(queue_name=stream, callback=cb)
        await consumer.publish_message({"hello": "events"}, queue_name=stream)
        # give the background consumer task a chance to process both the
        # seed 'initial' entry and our real payload
        for _ in range(20):
            if any(isinstance(m, dict) and m.get("hello") == "events" for m in received):
                break
            await asyncio.sleep(0.25)
        assert any(isinstance(m, dict) and m.get("hello") == "events" for m in received)
    finally:
        await consumer.stop_consumer()
        try:
            await consumer._connection.delete(stream)
        except Exception:
            pass
        await consumer.disconnect()
