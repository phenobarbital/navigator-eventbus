"""Integration tests for CompositeBackend against a real Redis instance
(FEAT-430, TASK-1850, spec §4 Module 5).

Mirrors the ``@pytest.mark.integration`` / skip-when-unreachable pattern
already used by ``tests/test_backends_streams.py`` (``test_end_to_end_
streams_two_consumers`` / ``test_end_to_end_broadcast_two_instances``):
a probe connection pings + ``FLUSHDB``s the test database before each
test, and the test is skipped (not failed) when no Redis is reachable.

Uses a dedicated Redis logical DB (``REDIS_TEST_URL``, default
``redis://localhost:6379/15``) to avoid colliding with
``tests/test_backends_streams.py``'s own ``.../9`` and
``tests/brokers/test_redis_integration.py``'s default DB.
"""
import asyncio
import os
import time
import uuid

import pytest
import redis.asyncio as aioredis

from navigator_eventbus.backends.composite import Channel, CompositeBackend
from navigator_eventbus.envelope import EventEnvelope

pytestmark = pytest.mark.integration

REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:6379/15")


def make_envelope(topic: str = "test.composite", **kwargs) -> EventEnvelope:
    return EventEnvelope(topic=topic, payload=kwargs.pop("payload", {"k": 1}), **kwargs)


async def _noop_handler(envelope: EventEnvelope) -> None:
    pass


async def wait_until(condition, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(0.02)
    pytest.fail("condition not met within timeout")


async def _require_redis() -> None:
    """Skip the test (not fail it) when no live Redis is reachable."""
    try:
        probe = await aioredis.from_url(REDIS_TEST_URL)
        await probe.ping()
        await probe.flushdb()
        await probe.close()
    except Exception:
        pytest.skip(f"No Redis reachable at {REDIS_TEST_URL}")


# ---------------------------------------------------------------------------
# spec §4: test_composite_broadcast_plus_two_groups
# ---------------------------------------------------------------------------


async def test_composite_broadcast_plus_two_groups():
    """Publish 10 entries; broadcast sees all 10; each group sees all 10
    independently (exactly once per group)."""
    await _require_redis()

    # Unique topic-class per run so all 3 channels' internal backends
    # converge on the SAME default-sharded stream
    # (``<stream_prefix><topic-class>`` — no ``streams=``/``stream_key_fn=``
    # override here, mirroring ``test_backends_streams.py``'s live tests).
    topic_class = f"itest{uuid.uuid4().hex[:8]}"
    seed_envelope = make_envelope(f"{topic_class}.seed")

    broadcast_received: list[str] = []
    ledger_received: list[str] = []
    push_received: list[str] = []

    async def broadcast_transport_callback(envelope: EventEnvelope) -> None:
        # Broadcast's tail cursor already excludes the seed (see below) —
        # no filtering needed here.
        broadcast_received.append(envelope.event_id)

    async def ledger_consumer(envelope: EventEnvelope) -> None:
        if envelope.event_id == seed_envelope.event_id:
            return  # group cursors start at "0" — they DO see the seed.
        ledger_received.append(envelope.event_id)

    async def push_consumer(envelope: EventEnvelope) -> None:
        if envelope.event_id == seed_envelope.event_id:
            return
        push_received.append(envelope.event_id)

    channels = [
        Channel(name="broadcast", delivery="broadcast", on_envelope=_noop_handler),
        Channel(
            name="audit-ledger",
            delivery="group",
            on_envelope=ledger_consumer,
            max_deliveries=5,
        ),
        Channel(
            name="push-alerts",
            delivery="group",
            on_envelope=push_consumer,
            max_deliveries=5,
        ),
    ]

    composite = CompositeBackend(
        redis_url=REDIS_TEST_URL,
        channels=channels,
        block_ms=100,
        stream_refresh_interval=0.1,
    )

    # Broadcast mode resolves its tail cursor ONCE, at discovery time, to
    # the CURRENT last entry in the stream (real Redis, not the sentinel
    # "$" — see redis_streams.py:464 _resolve_tail_id's docstring). If the
    # stream does not exist yet when discovery runs, tail resolution
    # would instead land on whichever entry happens to be last by the
    # time discovery/publish race resolves — so seed the stream with one
    # throwaway entry FIRST (same topic-class, so the resulting stream
    # key matches), THEN start consumers, so the seed itself becomes the
    # excluded tail and every "real" entry published afterwards is seen.
    # Mirrors test_backends_streams.py's test_end_to_end_broadcast_two_
    # instances. Group channels do not need this — a fresh consumer
    # group's cursor starts at "0" (sees everything, including the seed,
    # hence the seed_envelope.event_id filter above).
    await composite.publish(seed_envelope)
    await composite.start_consumer(broadcast_transport_callback)
    await asyncio.sleep(0.3)

    envs = [make_envelope(f"{topic_class}.job{i}") for i in range(10)]
    for env in envs:
        await composite.publish(env)

    await wait_until(
        lambda: len(broadcast_received) == 10
        and len(ledger_received) == 10
        and len(push_received) == 10,
        timeout=10.0,
    )

    expected_ids = sorted(e.event_id for e in envs)
    assert sorted(broadcast_received) == expected_ids
    assert sorted(ledger_received) == expected_ids
    assert sorted(push_received) == expected_ids
    # Exactly once per group — no duplicate deliveries within a group.
    assert len(set(ledger_received)) == 10
    assert len(set(push_received)) == 10

    await composite.close()


# ---------------------------------------------------------------------------
# spec §4: test_composite_shutdown_closes_one_connection
# ---------------------------------------------------------------------------


async def test_composite_shutdown_closes_one_connection():
    """``close()`` issues exactly ONE Redis connection-close (not N=3) —
    the shared client is owned once by ``CompositeBackend`` itself; the
    N internal ``RedisStreamsBackend`` instances never own/close it
    (verified: ``redis_streams.py:384`` — an injected ``client=`` is
    skipped on that backend's own ``close()``)."""
    await _require_redis()

    channels = [
        Channel(name="broadcast", delivery="broadcast", on_envelope=_noop_handler),
        Channel(
            name="audit-ledger",
            delivery="group",
            on_envelope=_noop_handler,
            max_deliveries=5,
        ),
        Channel(
            name="push-alerts",
            delivery="group",
            on_envelope=_noop_handler,
            max_deliveries=5,
        ),
    ]

    composite = CompositeBackend(
        redis_url=REDIS_TEST_URL,
        channels=channels,
        block_ms=100,
        stream_refresh_interval=0.1,
    )
    await composite.start_consumer(_noop_handler)

    shared_client = composite._redis
    assert shared_client is not None

    close_calls: list[int] = []
    original_close = shared_client.close

    async def _spy_close(*args, **kwargs):
        close_calls.append(1)
        return await original_close(*args, **kwargs)

    shared_client.close = _spy_close

    await composite.close()

    assert len(close_calls) == 1
