"""RMQManager tests (FEAT-431 Module 3, TASK-4565)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiormq
import pytest
from aiohttp import web

from navigator_eventbus.brokers import BaseConnection, BrokerConsumer, BrokerProducer
from navigator_eventbus.brokers.manager import BrokerManager
from navigator_eventbus.brokers.rabbitmq import (
    RabbitMQConnection,
    RMQConsumer,
    RMQManager,
)


@pytest.fixture
def manager_kwargs():
    """Return standard RMQ manager constructor arguments."""
    return {
        "credentials": "amqp://guest:guest@localhost:5672/navigator",
        "exchange_name": "navigator",
        "queue_name": "processing_queue",
        "routing_key": "#",
    }


@pytest.fixture
def channel():
    """Return an asynchronous RabbitMQ channel double."""
    ch = MagicMock()
    for name in (
        "exchange_declare",
        "queue_declare",
        "queue_bind",
        "basic_publish",
        "basic_qos",
        "basic_ack",
        "basic_nack",
        "basic_cancel",
        "close",
    ):
        setattr(ch, name, AsyncMock())
    ch.basic_consume = AsyncMock(
        side_effect=lambda **kw: aiormq.spec.Basic.ConsumeOk(
            consumer_tag=f"ctag-{ch.basic_consume.await_count}"
        )
    )
    return ch


@pytest.fixture
def patched(monkeypatch, channel):
    """Replace network connection with a fake open connection and channel."""
    state = {"connects": 0, "events": []}

    async def fake_connect(self):
        state["connects"] += 1
        self._connection = SimpleNamespace(is_closed=False, close=AsyncMock())
        self._channel = channel

    monkeypatch.setattr(RabbitMQConnection, "connect", fake_connect)
    return state


def _delivered(tag: int = 1):
    """Return a minimal aiormq delivery double."""
    props = aiormq.spec.Basic.Properties(content_type="text/plain", headers={})
    return SimpleNamespace(
        body=b"hi",
        delivery_tag=tag,
        header=SimpleNamespace(properties=props),
        delivery=SimpleNamespace(exchange="navigator", routing_key="#"),
    )


def test_manager_mro_rabbitmq():
    """The manager preserves the specified cooperative MRO."""
    assert RMQManager.__mro__[:7] == (
        RMQManager,
        BrokerManager,
        BrokerProducer,
        RMQConsumer,
        RabbitMQConnection,
        BaseConnection,
        BrokerConsumer,
    )


def test_manager_init_chain(manager_kwargs):
    """Initialization configures producer, consumer, and tracker state."""
    manager = RMQManager(**manager_kwargs)

    assert manager._serializer is not None
    assert manager._dsn == manager_kwargs["credentials"]
    assert manager.queue_size > 0
    assert manager._exchange_name == "navigator"
    assert manager._queue_name == "processing_queue"
    assert manager._consumer_tags == []
    assert manager._name_ == "rabbitmq_manager"


async def test_manager_auto_subscribe_false(manager_kwargs, patched, channel):
    """Manual subscription mode starts workers without consuming."""
    manager = RMQManager(auto_subscribe=False, callback=AsyncMock(), **manager_kwargs)
    await manager.start(web.Application())

    assert patched["connects"] == 1
    assert len(manager._workers) == manager._num_workers
    channel.basic_consume.assert_not_awaited()

    await manager.stop(None)


async def test_rmq_manager_stop_consumers_first(manager_kwargs, patched, channel):
    """Cancellation precedes delivery settlement, drain, and disconnect."""
    release = asyncio.Event()
    order: list[str] = []

    async def slow_callback(message, body):
        await release.wait()
        order.append("callback_done")

    manager = RMQManager(callback=slow_callback, **manager_kwargs)
    await manager.start(web.Application())
    deliver = channel.basic_consume.call_args.kwargs["consumer_callback"]
    inflight = asyncio.create_task(deliver(_delivered()))
    await asyncio.sleep(0)
    channel.basic_cancel.side_effect = lambda tag: order.append(f"cancel:{tag}")
    original_disconnect = manager.disconnect

    async def tracked_disconnect():
        order.append("disconnect")
        await original_disconnect()

    manager.disconnect = tracked_disconnect
    stopping = asyncio.create_task(manager.stop(None))
    await asyncio.sleep(0)

    assert order == ["cancel:ctag-1"]
    assert not stopping.done()

    release.set()
    await inflight
    await stopping

    assert order == ["cancel:ctag-1", "callback_done", "disconnect"]
    channel.basic_ack.assert_awaited_once_with(1)


async def test_manager_stop_manual_or_no_subscription(manager_kwargs, patched, channel):
    """Shutdown tolerates no subscription and cancels manual tags."""
    manager = RMQManager(auto_subscribe=False, callback=AsyncMock(), **manager_kwargs)
    await manager.start(web.Application())
    await manager.stop(None)
    channel.basic_cancel.assert_not_awaited()

    manager = RMQManager(auto_subscribe=False, callback=AsyncMock(), **manager_kwargs)
    await manager.start(web.Application())
    await manager.subscribe_to_events("navigator", "q2", "#", AsyncMock())
    await manager.stop(None)

    channel.basic_cancel.assert_awaited_once_with("ctag-1")


async def test_manager_publishes_and_consumes(manager_kwargs, patched, channel):
    """One manager connection serves both producer and consumer roles."""
    received = []

    async def callback(message, body):
        received.append(body)

    manager = RMQManager(callback=callback, num_workers=1, **manager_kwargs)
    await manager.start(web.Application())
    manager.event_queue.put_nowait(
        {"body": "out", "queue_name": "navigator", "routing_key": "a.b"}
    )
    await manager.event_queue.join()
    await channel.basic_consume.call_args.kwargs["consumer_callback"](_delivered())

    channel.basic_publish.assert_awaited()
    assert received == ["hi"]
    assert patched["connects"] == 1

    await manager.stop(None)


def test_rmq_manager_setup_registers_name(manager_kwargs):
    """Setup registers the manager and its producer endpoint."""
    app = web.Application()
    RMQManager(**manager_kwargs).setup(app)

    assert "rabbitmq_manager" in app
    assert any(
        route.method == "POST"
        and route.resource.canonical == "/api/v1/broker/rabbitmq/publish_event"
        for route in app.router.routes()
    )


async def test_event_subscribe_consumer_cancelled_on_stop(
    manager_kwargs, patched, channel
):
    """Manual ``event_subscribe`` consumers are cancelled on shutdown."""
    manager = RMQManager(auto_subscribe=False, callback=AsyncMock(), **manager_kwargs)
    await manager.start(web.Application())

    await manager.event_subscribe("manual_queue", AsyncMock())
    assert manager._consumer_tags == ["ctag-1"]

    await manager.stop(None)

    channel.basic_cancel.assert_awaited_once_with("ctag-1")


async def test_start_failure_releases_workers_and_connection(
    manager_kwargs, patched, channel, monkeypatch
):
    """A failing subscribe during ``start`` tears down workers and connection."""
    manager = RMQManager(callback=AsyncMock(), **manager_kwargs)

    async def boom(app):
        raise RuntimeError("subscribe failed")

    monkeypatch.setattr(manager, "_subscribe", boom)
    with pytest.raises(RuntimeError, match="subscribe failed"):
        await manager.start(web.Application())

    assert not manager._workers or all(w.done() for w in manager._workers)
    assert manager._connection is None
