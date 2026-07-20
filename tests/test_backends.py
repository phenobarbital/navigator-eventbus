"""Unit tests for transport backends (FEAT-312, TASK-1801).

Mudado desde ``packages/ai-parrot/tests/core/events/bus/test_backends.py``
(ai-parrot@686aba1fe, FEAT-310) — imports adapted to
``navigator_eventbus``; wire-format assertions updated to the neutral
``evb:events:`` default channel prefix (FEAT-312 decoupling). New tests
added: ``channel_prefix`` default/override on ``RedisPubSubBackend``.
"""
import asyncio
import json
import time

import pytest

from navigator_eventbus import BusCore, EventEnvelope
from navigator_eventbus.backends.base import TransportBackend
from navigator_eventbus.backends.memory import MemoryBackend
from navigator_eventbus.backends.redis_pubsub import RedisPubSubBackend


def make_envelope(topic: str = "test.topic", **kwargs) -> EventEnvelope:
    return EventEnvelope(topic=topic, payload=kwargs.pop("payload", {"k": 1}), **kwargs)


async def wait_until(condition, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(0.01)
    pytest.fail("condition not met within timeout")


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakePubSub:
    """Minimal aioredis PubSub stand-in."""

    def __init__(self, owner: "FakeRedis") -> None:
        self._owner = owner
        self.patterns: list[str] = []
        self.closed = False
        self.punsubscribed = False

    async def psubscribe(self, pattern: str) -> None:
        self._owner.psubscribe_attempts += 1
        if self._owner.psubscribe_attempts <= self._owner.fail_psubscribe:
            raise ConnectionError("fake redis unavailable")
        self.patterns.append(pattern)

    async def punsubscribe(self) -> None:
        self.punsubscribed = True

    async def close(self) -> None:
        self.closed = True

    async def listen(self):
        for message in self._owner.incoming:
            yield message
        # Block like a live connection with no more traffic.
        await asyncio.Event().wait()


class FakeRedis:
    """Minimal aioredis client stand-in."""

    def __init__(self, incoming=None, fail_psubscribe: int = 0) -> None:
        self.published: list[tuple[str, str]] = []
        self.incoming = incoming or []
        self.fail_psubscribe = fail_psubscribe
        self.psubscribe_attempts = 0

    async def publish(self, channel: str, data: str) -> None:
        self.published.append((channel, data))

    def pubsub(self) -> FakePubSub:
        return FakePubSub(self)

    async def close(self) -> None:
        pass


class SlowBackend:
    """TransportBackend whose publish is deliberately slow."""

    def __init__(self, delay: float = 0.3) -> None:
        self.delay = delay
        self.published: list[EventEnvelope] = []
        self._on_envelope = None

    async def publish(self, envelope: EventEnvelope) -> None:
        await asyncio.sleep(self.delay)
        self.published.append(envelope)

    async def start_consumer(self, on_envelope) -> None:
        self._on_envelope = on_envelope

    async def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_backends_satisfy_protocol():
    assert isinstance(MemoryBackend(), TransportBackend)
    assert isinstance(
        RedisPubSubBackend(client=FakeRedis()), TransportBackend
    )
    assert isinstance(SlowBackend(), TransportBackend)


def test_pubsub_requires_url_or_client():
    with pytest.raises(ValueError):
        RedisPubSubBackend()


# ---------------------------------------------------------------------------
# MemoryBackend
# ---------------------------------------------------------------------------


async def test_memory_backend_delivers_to_consumer():
    backend = MemoryBackend()
    received: list[EventEnvelope] = []

    async def consumer(envelope):
        received.append(envelope)

    await backend.start_consumer(consumer)
    env = make_envelope("mem.topic")
    await backend.publish(env)
    assert received == [env]

    await backend.close()
    await backend.publish(make_envelope("mem.dropped"))  # no consumer
    assert len(received) == 1  # at-most-once: dropped silently


# ---------------------------------------------------------------------------
# RedisPubSubBackend
# ---------------------------------------------------------------------------


def test_pubsub_channel_prefix_default_neutral():
    backend = RedisPubSubBackend(client=FakeRedis())
    assert backend.CHANNEL_PREFIX == "evb:events:"
    assert backend.channel_prefix == "evb:events:"


def test_pubsub_channel_prefix_override():
    backend = RedisPubSubBackend(
        client=FakeRedis(), channel_prefix="parrot:events:"
    )
    assert backend.channel_prefix == "parrot:events:"


async def test_pubsub_wire_roundtrip():
    env = make_envelope("wire.topic")
    wire = {
        "type": "pmessage",
        "pattern": "evb:events:*",
        "channel": f"evb:events:{env.topic}",
        "data": json.dumps(env.to_dict()),
    }
    fake = FakeRedis(incoming=[wire])
    backend = RedisPubSubBackend(client=fake)

    # Outbound: publish → prefixed channel + JSON wire dict.
    await backend.publish(env)
    channel, data = fake.published[0]
    assert channel == f"evb:events:{env.topic}"
    assert EventEnvelope.from_dict(json.loads(data)) == env

    # Inbound: consumer decodes wire dict back to an identical envelope.
    received: list[EventEnvelope] = []

    async def consumer(envelope):
        received.append(envelope)

    await backend.start_consumer(consumer)
    await wait_until(lambda: len(received) == 1)
    assert received[0] == env
    await backend.close()


async def test_pubsub_roundtrip_with_version():
    """FEAT-319 M1: legacy (version-less) and v1 messages both consumable
    over the Redis Pub/Sub backend."""
    legacy_env = make_envelope("legacy.topic")
    legacy_wire = legacy_env.to_dict()
    del legacy_wire["schema_version"]

    v1_env = make_envelope("v1.topic")
    v1_wire = v1_env.to_dict()

    incoming = [
        {
            "type": "pmessage",
            "pattern": "evb:events:*",
            "channel": f"evb:events:{legacy_env.topic}",
            "data": json.dumps(legacy_wire),
        },
        {
            "type": "pmessage",
            "pattern": "evb:events:*",
            "channel": f"evb:events:{v1_env.topic}",
            "data": json.dumps(v1_wire),
        },
    ]
    fake = FakeRedis(incoming=incoming)
    backend = RedisPubSubBackend(client=fake)

    received: list[EventEnvelope] = []

    async def consumer(envelope):
        received.append(envelope)

    await backend.start_consumer(consumer)
    await wait_until(lambda: len(received) == 2)
    by_topic = {env.topic: env for env in received}
    assert by_topic["legacy.topic"].schema_version == 1
    assert by_topic["v1.topic"].schema_version == 1
    await backend.close()


async def test_pubsub_reconnect_backoff():
    env = make_envelope("reconnect.topic")
    wire = {
        "type": "pmessage",
        "pattern": "evb:events:*",
        "channel": f"evb:events:{env.topic}",
        "data": json.dumps(env.to_dict()),
    }
    fake = FakeRedis(incoming=[wire], fail_psubscribe=2)
    backend = RedisPubSubBackend(
        client=fake, reconnect_base_delay=0.01, reconnect_max_delay=0.05
    )
    received: list[EventEnvelope] = []

    async def consumer(envelope):
        received.append(envelope)

    await backend.start_consumer(consumer)
    # Two failures, then a successful subscribe on the third attempt.
    await wait_until(lambda: fake.psubscribe_attempts >= 3)
    await wait_until(lambda: len(received) == 1)
    assert received[0] == env
    await backend.close()


async def test_pubsub_poison_message_isolated():
    poison = {
        "type": "pmessage",
        "pattern": "evb:events:*",
        "channel": "evb:events:x",
        "data": "not-json{{{",
    }
    env = make_envelope("after.poison")
    good = {
        "type": "pmessage",
        "pattern": "evb:events:*",
        "channel": f"evb:events:{env.topic}",
        "data": json.dumps(env.to_dict()),
    }
    fake = FakeRedis(incoming=[poison, good])
    backend = RedisPubSubBackend(client=fake)
    received: list[EventEnvelope] = []

    async def consumer(envelope):
        received.append(envelope)

    await backend.start_consumer(consumer)
    await wait_until(lambda: len(received) == 1)
    assert received[0] == env
    await backend.close()


# ---------------------------------------------------------------------------
# BusCore integration
# ---------------------------------------------------------------------------


async def test_end_to_end_memory_bus():
    """spec §4 — emit → workers → subscriber, MemoryBackend."""
    backend = MemoryBackend()
    core = BusCore(workers=2, queue_size=8, backend=backend)
    await core.start()

    received: list[str] = []

    async def handler(envelope):
        received.append(envelope.topic)

    core.subscribe("e2e.*", handler)
    await backend.publish(make_envelope("e2e.event"))
    await wait_until(lambda: received == ["e2e.event"])
    await core.close()


async def test_buscore_fanout_nonblocking():
    backend = SlowBackend(delay=0.3)
    core = BusCore(workers=2, queue_size=8, backend=backend)
    await core.start()

    received: list[str] = []

    async def handler(envelope):
        received.append(envelope.topic)

    core.subscribe("fan.*", handler)

    t0 = time.monotonic()
    await core.publish(make_envelope("fan.out"))
    assert time.monotonic() - t0 < 0.1  # backend slowness invisible

    # Local dispatch completes long before the slow backend publish.
    await wait_until(lambda: len(received) == 1, timeout=0.25)
    assert not backend.published
    await wait_until(lambda: len(backend.published) == 1)
    await core.close()


async def test_buscore_memory_backend_no_double_dispatch():
    core = BusCore(workers=2, queue_size=8, backend=MemoryBackend())
    await core.start()

    received: list[str] = []

    async def handler(envelope):
        received.append(envelope.event_id)

    core.subscribe("echo.*", handler)
    await core.publish(make_envelope("echo.once"))
    await wait_until(lambda: len(received) == 1)
    await asyncio.sleep(0.1)  # give a (buggy) echo a chance to double-fire
    assert len(received) == 1
    await core.close()


async def test_buscore_transport_envelope_dispatched_locally():
    backend = MemoryBackend()
    core = BusCore(workers=2, queue_size=8, backend=backend)
    await core.start()

    received: list[str] = []

    async def handler(envelope):
        received.append(envelope.topic)

    core.subscribe("remote.*", handler)
    # Simulate a remote-origin envelope arriving via the transport.
    await backend.publish(make_envelope("remote.event"))
    await wait_until(lambda: received == ["remote.event"])
    await core.close()
