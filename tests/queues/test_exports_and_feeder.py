"""Packaging, lazy exports and the bus->queue feeder (queues, TASK-H)."""
import subprocess
import sys

import pytest

import navigator_eventbus
from navigator_eventbus.envelope import EventEnvelope
from navigator_eventbus.evb import EventBus
from navigator_eventbus.queues.config import (
    QueueConfig,
    QueueGroupConfig,
    QueueRegistry,
)
from navigator_eventbus.queues.feeder import QueueFeeder
from navigator_eventbus.queues.receipts import ReceiptCodec
from navigator_eventbus.queues.store import QueueStore

from ._fake_streams import FakeClock, FakeStreamsRedis

# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name", ["QueueAPI", "QueueConfig", "QueueGroupConfig", "QueueRegistry", "QueueStore"]
)
def test_queue_names_are_exported_from_the_root(name):
    assert name in navigator_eventbus.__all__
    assert getattr(navigator_eventbus, name) is not None


def test_unknown_root_attribute_still_raises():
    with pytest.raises(AttributeError):
        navigator_eventbus.DefinitelyNotAThing


@pytest.mark.parametrize(
    "name", ["QueueAPI", "QueueStore", "QueueFeeder", "StaleReceipt"]
)
def test_queues_package_lazy_names_resolve(name):
    import navigator_eventbus.queues as queues

    assert getattr(queues, name) is not None
    assert name in dir(queues)


def test_queues_package_unknown_attribute_raises():
    import navigator_eventbus.queues as queues

    with pytest.raises(AttributeError):
        queues.NotAQueueThing


def test_queue_engine_is_not_imported_until_used():
    """The lazy exports must actually defer the Redis-touching modules.

    Note what this does *not* claim: ``redis`` itself is a hard transitive
    dependency (``navconfig[default]`` requires ``redis~=5.2.1`` and imports
    it at module level), so it is always present regardless of the ``[redis]``
    extra. What laziness buys is not importing the queue store/API machinery —
    and its Redis client construction — for callers that never touch a queue.
    """
    script = (
        "import sys\n"
        "import navigator_eventbus\n"
        "import navigator_eventbus.queues as q\n"
        "assert 'navigator_eventbus.queues.store' not in sys.modules, 'store loaded eagerly'\n"
        "assert 'navigator_eventbus.queues.api' not in sys.modules, 'api loaded eagerly'\n"
        "_ = q.QueueStore\n"
        "assert 'navigator_eventbus.queues.store' in sys.modules, 'lazy resolution failed'\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True
    )
    assert "OK" in result.stdout, result.stderr


# ---------------------------------------------------------------------------
# QueueFeeder
# ---------------------------------------------------------------------------


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def redis(clock):
    return FakeStreamsRedis(clock)


@pytest.fixture
def config():
    return QueueConfig(name="orders", groups=[QueueGroupConfig(name="billing")])


@pytest.fixture
def store(config, redis, clock):
    return QueueStore(
        QueueRegistry(queues=[config]), redis, ReceiptCodec(["k"]), clock=clock
    )


@pytest.fixture
async def bus():
    instance = EventBus()
    await instance.connect()
    yield instance
    await instance.close()


async def wait_until(condition, timeout: float = 2.0):
    import asyncio

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return True
        await asyncio.sleep(0.01)
    return condition()


async def test_feeder_forwards_bus_events_into_the_queue(bus, store, config):
    feeder = QueueFeeder(store, config, patterns=["order.*"])
    feeder.attach(bus)
    await store.ensure_queue(config)

    await bus.emit("order.created", {"id": 7})

    assert await wait_until(lambda: feeder.stats["forwarded"] == 1)
    messages = await store.receive(config, "billing")
    assert len(messages) == 1
    assert messages[0].envelope.payload == {"id": 7}


async def test_feeder_ignores_non_matching_topics(bus, store, config):
    feeder = QueueFeeder(store, config, patterns=["order.*"])
    feeder.attach(bus)
    await store.ensure_queue(config)

    await bus.emit("payment.created", {"id": 1})
    import asyncio

    await asyncio.sleep(0.15)
    assert feeder.stats["forwarded"] == 0


async def test_feeder_excludes_bus_internal_topics_by_default(bus, store, config):
    feeder = QueueFeeder(store, config, patterns=["*"])
    feeder.attach(bus)
    await store.ensure_queue(config)

    await bus.emit("bus.something", {})
    await bus.emit("app.something", {})

    assert await wait_until(lambda: feeder.stats["forwarded"] == 1)
    import asyncio

    await asyncio.sleep(0.1)
    assert feeder.stats["forwarded"] == 1


async def test_feeder_detach_stops_forwarding(bus, store, config):
    feeder = QueueFeeder(store, config, patterns=["order.*"])
    feeder.attach(bus)
    assert feeder.detach(bus) == 1

    await bus.emit("order.created", {})
    import asyncio

    await asyncio.sleep(0.15)
    assert feeder.stats["forwarded"] == 0


async def test_feeder_raises_so_the_bus_retries_and_dlqs(store, config, monkeypatch):
    """The one place letting the exception escape is correct."""

    async def boom(*args, **kwargs):
        raise RuntimeError("redis down")

    monkeypatch.setattr(store, "send", boom)
    feeder = QueueFeeder(store, config)
    with pytest.raises(RuntimeError):
        await feeder.handle(EventEnvelope(topic="order.created", payload={}))
    assert feeder.stats["failed"] == 1
