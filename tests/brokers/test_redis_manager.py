"""RedisManager unit tests (FEAT-431 Module 3, TASK-4566)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from navigator_eventbus.brokers import BaseConnection, BrokerConsumer, BrokerProducer
from navigator_eventbus.brokers.manager import BrokerManager
from navigator_eventbus.brokers.redis import (
    RedisConnection,
    RedisConsumer,
    RedisManager,
    RedisProducer,
)


@pytest.fixture
def patched(monkeypatch):
    """Fake connect/disconnect and a never-ending consume loop that logs its cancellation."""
    events: list[str] = []

    async def fake_connect(self):
        self._connection = SimpleNamespace(close=AsyncMock())

    async def fake_disconnect(self):
        events.append("disconnect")

    async def fake_consume(self, queue_name, callback, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append(f"consumer_cancelled:{queue_name}")
            raise

    async def fake_publish(self, body, queue_name=None, **kwargs):
        events.append("publish")

    monkeypatch.setattr(RedisConnection, "connect", fake_connect)
    monkeypatch.setattr(RedisConnection, "disconnect", fake_disconnect)
    monkeypatch.setattr(RedisConnection, "consume_messages", fake_consume)
    monkeypatch.setattr(RedisConnection, "publish_message", fake_publish)
    return events


def test_manager_mro_redis():
    assert RedisManager.__mro__[:7] == (
        RedisManager,
        BrokerManager,
        BrokerProducer,
        RedisConsumer,
        RedisConnection,
        BaseConnection,
        BrokerConsumer,
    )


def test_redis_manager_registration_key():
    """Real cooperative init keeps "redis_manager"; setup registers that literal key."""
    m = RedisManager(queue_name="s")
    assert m._name_ == "redis_manager"
    app = web.Application()
    m.setup(app)
    assert "redis_manager" in app
    assert "RedisManager" not in app
    routes = [(r.method, r.resource.canonical) for r in app.router.routes()]
    assert ("POST", "/api/v1/broker/redis/publish_event") in routes


def test_redis_producer_consumer_registration_unchanged():
    assert RedisProducer()._name_ == "RedisProducer"
    assert RedisConsumer()._name_ == "RedisConsumer"


async def test_manager_stop_consumers_first(patched):
    """Consumer tasks are cancelled and awaited before drain and disconnect; none remain."""
    m = RedisManager(queue_name="s", num_workers=1)
    await m.start(web.Application())
    await asyncio.sleep(0)
    original_join = m.event_queue.join

    async def recording_join():
        patched.append("drain")
        await original_join()

    m.event_queue.join = recording_join
    await m.stop(None)
    assert patched[-3:] == ["consumer_cancelled:s", "drain", "disconnect"]
    assert m._consumer_tasks == []
    assert m.consumer_task is None


async def test_manager_stop_manual_or_no_subscription(patched):
    cb = AsyncMock()
    # (a) no subscription
    m = RedisManager(queue_name="s", num_workers=1, auto_subscribe=False)
    await m.start(web.Application())
    await m.stop(None)
    assert not any(e.startswith("consumer_cancelled") for e in patched)
    assert m._consumer_tasks == []

    # (b) two explicit subscriptions
    patched.clear()
    m = RedisManager(queue_name="s", num_workers=1, auto_subscribe=False)
    await m.start(web.Application())
    await m.subscribe_to_events("s1", cb)
    await m.subscribe_to_events("s2", cb)
    await asyncio.sleep(0)
    await m.stop(None)
    assert {"consumer_cancelled:s1", "consumer_cancelled:s2"} <= set(patched)
    assert m._consumer_tasks == []
    assert m.consumer_task is None
