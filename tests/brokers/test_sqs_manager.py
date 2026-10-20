"""SQSManager unit tests (FEAT-431 Module 3, TASK-4567)."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from navigator_eventbus.brokers import BaseConnection, BrokerConsumer, BrokerProducer
from navigator_eventbus.brokers.manager import BrokerManager
from navigator_eventbus.brokers.sqs import SQSConnection, SQSConsumer, SQSManager

CREDS = {"region_name": "us-east-1"}


@pytest.fixture
def patched(monkeypatch):
    events: list[str] = []

    async def fake_connect(self):
        self._connection = object()

    async def fake_disconnect(self):
        events.append("disconnect")

    async def fake_consume(self, queue_name, callback, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append(f"consumer_cancelled:{queue_name}")
            raise

    monkeypatch.setattr(SQSConnection, "connect", fake_connect)
    monkeypatch.setattr(SQSConnection, "disconnect", fake_disconnect)
    monkeypatch.setattr(SQSConnection, "consume_messages", fake_consume)
    return events


def test_manager_mro_sqs():
    assert SQSManager.__mro__[:7] == (SQSManager, BrokerManager, BrokerProducer, SQSConsumer,
                                      SQSConnection, BaseConnection, BrokerConsumer)


def test_sqs_manager_setup_registers_name():
    app = web.Application()
    SQSManager(credentials=CREDS, queue_name="q").setup(app)
    assert "sqs_manager" in app
    routes = [(r.method, r.resource.canonical) for r in app.router.routes()]
    assert ("POST", "/api/v1/broker/sqs/publish_event") in routes


async def test_manager_stop_consumers_first(patched):
    m = SQSManager(credentials=CREDS, queue_name="q", num_workers=1)
    await m.start(web.Application())
    await asyncio.sleep(0)
    original_join = m.event_queue.join

    async def recording_join():
        patched.append("drain")
        await original_join()

    m.event_queue.join = recording_join
    await m.stop(None)
    assert patched[-3:] == ["consumer_cancelled:q", "drain", "disconnect"]
    assert m._consumer_tasks == []
    assert m.consumer_task is None


async def test_manager_stop_manual_or_no_subscription(patched):
    cb = AsyncMock()
    m = SQSManager(credentials=CREDS, queue_name="q", num_workers=1, auto_subscribe=False)
    await m.start(web.Application())
    await m.stop(None)
    assert not any(e.startswith("consumer_cancelled") for e in patched)
    assert m._consumer_tasks == []

    patched.clear()
    m = SQSManager(credentials=CREDS, queue_name="q", num_workers=1, auto_subscribe=False)
    await m.start(web.Application())
    await m.subscribe_to_events("q1", cb)
    await m.subscribe_to_events("q2", cb)
    await asyncio.sleep(0)
    await m.stop(None)
    assert {"consumer_cancelled:q1", "consumer_cancelled:q2"} <= set(patched)
    assert m._consumer_tasks == []
    assert m.consumer_task is None
