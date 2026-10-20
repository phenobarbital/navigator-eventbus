"""Live-broker round trips for the unified managers (FEAT-431 Module 7, TASK-4568).

Skipped gracefully when RabbitMQ / Redis are not reachable.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest
from aiohttp import web

from navigator_eventbus.brokers.rabbitmq import RMQManager
from navigator_eventbus.brokers.redis import RedisManager


def _uid() -> str:
    return uuid.uuid4().hex[:8]


async def _started(manager, counter: dict):
    """Count connect() calls, start the manager, or skip when the broker is down."""
    original = manager.connect

    async def counting_connect():
        counter["connects"] += 1
        await original()

    manager.connect = counting_connect
    try:
        await asyncio.wait_for(manager.start(web.Application()), timeout=2)
    except BaseException as exc:  # includes timeouts/connection errors
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        pytest.skip(f"no live broker for {type(manager).__name__}")
    return manager


@pytest.mark.integration
async def test_manager_roundtrip_rabbitmq():
    got = asyncio.Event()
    received = []
    exchange = f"feat431-{_uid()}"

    async def cb(message, body):
        received.append(body)
        got.set()

    counter = {"connects": 0}
    m = await _started(
        RMQManager(
            exchange_name=exchange, queue_name=exchange, routing_key="#", callback=cb
        ),
        counter,
    )
    try:
        await m.queue_event({"hello": "world"}, exchange, "feat431.test")
        await asyncio.wait_for(got.wait(), timeout=5)
        assert received == [{"hello": "world"}]
        assert counter["connects"] == 1
    finally:
        # Queue/exchange are uniquely named; no channel helper to delete them.
        await m.stop(None)


@pytest.mark.integration
async def test_exchange_binding_roundtrip():
    """queue_name != exchange_name: a message published to the exchange reaches the consumer (D1)."""
    got = asyncio.Event()
    received = []
    uid = _uid()
    exchange, queue = f"ex-{uid}", f"q-{uid}"

    async def cb(message, body):
        received.append(body)
        got.set()

    counter = {"connects": 0}
    m = await _started(
        RMQManager(
            exchange_name=exchange, queue_name=queue, routing_key="#", callback=cb
        ),
        counter,
    )
    try:
        await m.publish_event({"d1": True}, exchange, routing_key="any.key")
        await asyncio.wait_for(got.wait(), timeout=5)
        assert received == [{"d1": True}]
    finally:
        await m.stop(None)


@pytest.mark.redis
async def test_manager_roundtrip_redis():
    got = asyncio.Event()
    received = []
    stream = f"feat431-{_uid()}"

    async def cb(message, body):
        if body is None:  # seeded {'initial': 'message'} entry
            return
        received.append(body)
        got.set()

    counter = {"connects": 0}
    m = await _started(RedisManager(queue_name=stream, callback=cb), counter)
    try:
        await m.queue_event({"hello": "redis"}, stream)
        await asyncio.wait_for(got.wait(), timeout=5)
        assert received == [{"hello": "redis"}]
        assert counter["connects"] == 1
    finally:
        await m.stop(None)
        try:
            await m._connection.delete(stream)
        except Exception:  # connection already closed by stop()
            pass
