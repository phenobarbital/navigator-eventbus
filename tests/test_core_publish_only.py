"""Tests for BusCore publish-only mode (FEAT-711)."""
import asyncio

from navigator_eventbus import EventEnvelope, SqliteOutbox
from navigator_eventbus.core import BusCore


class _SpyBackend:
    """Minimal backend spy recording consumer startup and publishes."""

    def __init__(self) -> None:
        self.consumer_started = False
        self.published: list[str] = []
        self.published_event = asyncio.Event()

    async def publish(self, envelope: EventEnvelope) -> None:
        """Record a published envelope."""
        self.published.append(envelope.event_id)
        self.published_event.set()

    async def start_consumer(self, on_envelope) -> None:
        """Record that the consumer would have been started."""
        self.consumer_started = True

    async def close(self) -> None:
        """Satisfy the transport backend lifecycle protocol."""


async def test_publish_only_never_starts_consumer() -> None:
    """A publish-only core does not start its backend consumer."""
    backend = _SpyBackend()
    core = BusCore(backend=backend, publish_only=True)

    await core.start()
    try:
        assert not backend.consumer_started
    finally:
        await core.close()


async def test_publish_only_still_publishes_and_dispatches_locally() -> None:
    """Publish-only mode preserves backend fan-out and local delivery."""
    backend = _SpyBackend()
    core = BusCore(backend=backend, publish_only=True)
    delivered = asyncio.Event()
    envelope = EventEnvelope(topic="publish.only", payload={})

    async def handler(received: EventEnvelope) -> None:
        assert received == envelope
        delivered.set()

    core.subscribe("publish.only", handler)
    await core.start()
    try:
        await core.publish(envelope)
        await asyncio.wait_for(delivered.wait(), timeout=1.0)
        await asyncio.wait_for(backend.published_event.wait(), timeout=1.0)
        assert backend.published == [envelope.event_id]
    finally:
        await core.close()


async def test_publish_only_relay_still_runs(tmp_path) -> None:
    """The outbox relay re-publishes pending rows in publish-only mode."""
    backend = _SpyBackend()
    outbox = SqliteOutbox(tmp_path / "outbox.db")
    envelope = EventEnvelope(topic="publish.only.relay", payload={})
    await outbox.append(envelope)
    core = BusCore(
        backend=backend,
        outbox=outbox,
        publish_only=True,
        relay_interval=60.0,
    )

    await core.start()
    try:
        await asyncio.wait_for(backend.published_event.wait(), timeout=1.0)
        assert backend.published == [envelope.event_id]
        assert not backend.consumer_started
    finally:
        await core.close()


async def test_default_starts_consumer() -> None:
    """Default BusCore lifecycle behavior starts the backend consumer."""
    backend = _SpyBackend()
    core = BusCore(backend=backend)

    await core.start()
    try:
        assert backend.consumer_started
    finally:
        await core.close()
