"""FEAT-711 integration: outbox durability through Redis Streams (requires Redis)."""
import asyncio
import os
from collections.abc import Awaitable, Callable

import pytest
import redis.asyncio as aioredis

from navigator_eventbus import EventBus, EventEnvelope
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend

pytestmark = pytest.mark.integration
REDIS_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:6379/9")


async def _redis_or_skip() -> None:
    """Clear the test Redis database or skip when Redis is unavailable."""
    try:
        probe = await aioredis.from_url(REDIS_URL)
        await probe.ping()
        await probe.flushdb()
        await probe.close()
    except Exception:
        pytest.skip(f"No Redis reachable at {REDIS_URL}")


async def wait_until(
    predicate: Callable[[], bool], timeout: float = 30.0, step: float = 0.05
) -> None:
    """Wait until *predicate* succeeds or fail within a bounded interval."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"Condition was not met within {timeout} seconds")
        await asyncio.sleep(step)


class _OutageBackend:
    """Delegate to a real backend; raise ``ConnectionError`` while ``down``."""

    def __init__(self, inner: RedisStreamsBackend) -> None:
        self.inner = inner
        self.down = True
        self.attempted_event_ids: set[str] = set()

    async def publish(self, envelope: EventEnvelope) -> None:
        """Publish an envelope unless the simulated connection is down."""
        self.attempted_event_ids.add(envelope.event_id)
        if self.down:
            raise ConnectionError("simulated outage")
        await self.inner.publish(envelope)

    async def start_consumer(
        self, on_envelope: Callable[[EventEnvelope], Awaitable[None]]
    ) -> None:
        """Start the wrapped backend consumer."""
        await self.inner.start_consumer(on_envelope)

    async def close(self) -> None:
        """Close the wrapped backend."""
        await self.inner.close()


async def test_streams_durability_through_outage(tmp_path) -> None:
    """An outbox relays every event emitted during a Redis outage."""
    await _redis_or_skip()
    received: set[str] = set()
    consumer = RedisStreamsBackend(
        REDIS_URL,
        consumer_name="outbox-durability-consumer",
        block_ms=100,
        autoclaim_interval=999,
        stream_refresh_interval=0.1,
    )
    outage = _OutageBackend(
        RedisStreamsBackend(
            REDIS_URL,
            consumer_name="outbox-durability-producer",
            block_ms=100,
            autoclaim_interval=999,
            stream_refresh_interval=0.1,
        )
    )
    bus = EventBus(
        REDIS_URL,
        backend=outage,
        publish_only=True,
        outbox=str(tmp_path / "outbox.db"),
        relay_interval=0.05,
    )

    async def on_envelope(envelope: EventEnvelope) -> None:
        received.add(envelope.event_id)

    try:
        await consumer.start_consumer(on_envelope)
        for index in range(1_000):
            await bus.emit("outbox.integration", {"index": index})
        await wait_until(lambda: len(outage.attempted_event_ids) == 1_000)

        outage.down = False
        await wait_until(lambda: len(received) == 1_000)

        missing = outage.attempted_event_ids - received
        assert not missing
    finally:
        await bus.close()
        await consumer.close()


async def test_restart_recovers_pending_rows_real_redis(tmp_path) -> None:
    """A new bus relays rows persisted by a bus closed during an outage."""
    await _redis_or_skip()
    received: set[str] = set()
    consumer = RedisStreamsBackend(
        REDIS_URL,
        consumer_name="outbox-restart-consumer",
        block_ms=100,
        autoclaim_interval=999,
        stream_refresh_interval=0.1,
    )
    outage = _OutageBackend(
        RedisStreamsBackend(
            REDIS_URL,
            consumer_name="outbox-restart-producer",
            block_ms=100,
            autoclaim_interval=999,
            stream_refresh_interval=0.1,
        )
    )
    outbox_path = tmp_path / "outbox.db"
    first_bus = EventBus(
        REDIS_URL,
        backend=outage,
        publish_only=True,
        outbox=str(outbox_path),
        relay_interval=0.05,
    )
    restarted_bus: EventBus | None = None
    first_bus_closed = False

    async def on_envelope(envelope: EventEnvelope) -> None:
        received.add(envelope.event_id)

    try:
        await consumer.start_consumer(on_envelope)
        for index in range(100):
            await first_bus.emit("outbox.restart", {"index": index})
        await wait_until(lambda: len(outage.attempted_event_ids) == 100)
        await first_bus.close()
        first_bus_closed = True

        restarted_bus = EventBus(
            REDIS_URL,
            backend=RedisStreamsBackend(
                REDIS_URL,
                consumer_name="outbox-restarted-producer",
                block_ms=100,
                autoclaim_interval=999,
                stream_refresh_interval=0.1,
            ),
            publish_only=True,
            outbox=str(outbox_path),
            relay_interval=0.05,
        )
        await restarted_bus.connect()
        await wait_until(lambda: len(received) == 100)

        missing = outage.attempted_event_ids - received
        assert not missing
    finally:
        if not first_bus_closed:
            await first_bus.close()
        if restarted_bus is not None:
            await restarted_bus.close()
        await consumer.close()


async def test_publish_only_producer_not_in_consumer_group(tmp_path) -> None:
    """A publish-only producer never registers itself as a group consumer."""
    await _redis_or_skip()
    received: list[str] = []
    consumer = RedisStreamsBackend(
        REDIS_URL,
        consumer_name="outbox-group-consumer",
        block_ms=100,
        autoclaim_interval=999,
        stream_refresh_interval=0.1,
    )
    producer = EventBus(
        REDIS_URL,
        backend=RedisStreamsBackend(
            REDIS_URL,
            consumer_name="outbox-group-producer",
            block_ms=100,
            autoclaim_interval=999,
            stream_refresh_interval=0.1,
        ),
        publish_only=True,
        outbox=str(tmp_path / "outbox.db"),
        relay_interval=0.05,
    )

    async def on_envelope(envelope: EventEnvelope) -> None:
        received.append(envelope.event_id)

    try:
        await consumer.start_consumer(on_envelope)
        await producer.emit("outbox.group", {"kind": "producer-check"})
        await wait_until(lambda: len(received) == 1)

        probe = await aioredis.from_url(REDIS_URL)
        try:
            consumers = await probe.xinfo_consumers("evb:stream:outbox", "evb-bus")
        finally:
            await probe.close()

        names = {item["name"] for item in consumers}
        assert names == {"outbox-group-consumer"}
    finally:
        await producer.close()
        await consumer.close()
