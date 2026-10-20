"""Tests for FEAT-431 Module 4: ensure_queue / reject_message / hook settlement (TASK-4563)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiormq
import pytest

from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection
from navigator_eventbus.hooks.brokers.rabbitmq import RabbitMQBrokerHook
from navigator_eventbus.hooks.models import BrokerHookConfig

DSN = "amqp://guest:guest@localhost:5672/navigator"


@pytest.fixture
def mock_amqp_channel():
    """Build an asynchronous RabbitMQ channel double."""
    ch = MagicMock()
    for name in (
        "exchange_declare",
        "queue_declare",
        "queue_bind",
        "basic_publish",
        "basic_consume",
        "basic_qos",
        "basic_ack",
        "basic_nack",
    ):
        setattr(ch, name, AsyncMock())
    return ch


@pytest.fixture
def conn(mock_amqp_channel):
    """Build a connected RabbitMQ connection with a mocked channel."""
    connection = RabbitMQConnection(credentials=DSN)
    connection._connection = SimpleNamespace(is_closed=False)
    connection._channel = mock_amqp_channel
    return connection


def _delivered(tag: int = 3):
    """Build a minimal delivered message accepted by the callback wrapper."""
    properties = aiormq.spec.Basic.Properties(content_type="text/plain", headers={})
    return SimpleNamespace(
        body=b"payload",
        delivery_tag=tag,
        header=SimpleNamespace(properties=properties),
        delivery=SimpleNamespace(exchange="navigator", routing_key="#"),
    )


def _hook(conn) -> RabbitMQBrokerHook:
    """Build a RabbitMQ hook that uses the supplied connection."""
    hook = RabbitMQBrokerHook(
        BrokerHookConfig(broker_type="rabbitmq", queue_name="q", exchange_name="ex")
    )
    hook._connection = conn
    return hook


async def test_ensure_queue_declares_and_binds(conn, mock_amqp_channel):
    """An exchange-backed queue is declared and bound using its routing key."""
    await conn.ensure_queue("q", exchange_name="ex", routing_key="a.#")
    mock_amqp_channel.queue_declare.assert_awaited_once_with(
        queue="q",
        durable=True,
        arguments=None,
    )
    mock_amqp_channel.queue_bind.assert_awaited_once_with(
        queue="q",
        exchange="ex",
        routing_key="a.#",
    )


async def test_ensure_queue_no_binding(conn, mock_amqp_channel):
    """A queue without an exchange is declared without a binding."""
    await conn.ensure_queue("q")
    mock_amqp_channel.queue_declare.assert_awaited_once_with(
        queue="q",
        durable=True,
        arguments=None,
    )
    mock_amqp_channel.queue_bind.assert_not_awaited()


@pytest.mark.parametrize("requeue", [False, True])
async def test_reject_message_nack(conn, mock_amqp_channel, requeue):
    """Rejection nacks the delivery with the requested requeue setting."""
    await conn.reject_message(_delivered(), requeue=requeue)
    mock_amqp_channel.basic_nack.assert_awaited_once_with(3, requeue=requeue)


async def test_rabbitmq_hook_connect(monkeypatch, mock_amqp_channel):
    """connect() completes after declaring and binding the configured queue."""

    async def fake_connect(self):
        self._connection = SimpleNamespace(is_closed=False)
        self._channel = mock_amqp_channel

    monkeypatch.setattr(RabbitMQConnection, "connect", fake_connect)
    hook = RabbitMQBrokerHook(
        BrokerHookConfig(broker_type="rabbitmq", queue_name="q", exchange_name="ex")
    )
    await hook.connect()
    mock_amqp_channel.queue_declare.assert_awaited_once_with(
        queue="q",
        durable=True,
        arguments=None,
    )
    mock_amqp_channel.queue_bind.assert_awaited_once_with(
        queue="q",
        exchange="ex",
        routing_key="",
    )


async def test_rabbitmq_hook_failure_settles_once(conn, mock_amqp_channel, monkeypatch):
    """A failed hook callback nacks once without acknowledgement or republish."""
    hook = _hook(conn)
    monkeypatch.setattr(
        hook, "_on_message", AsyncMock(side_effect=RuntimeError("boom"))
    )

    wrapped = conn.wrap_callback(hook._consumer_callback, requeue_on_fail=True)
    await wrapped(_delivered())

    mock_amqp_channel.basic_nack.assert_awaited_once_with(3, requeue=False)
    mock_amqp_channel.basic_ack.assert_not_awaited()
    mock_amqp_channel.basic_publish.assert_not_awaited()


async def test_rabbitmq_hook_success_settles_once(conn, mock_amqp_channel, monkeypatch):
    """A successful hook callback acknowledges once without rejection."""
    hook = _hook(conn)
    monkeypatch.setattr(hook, "_on_message", AsyncMock())

    wrapped = conn.wrap_callback(hook._consumer_callback)
    await wrapped(_delivered())

    mock_amqp_channel.basic_ack.assert_awaited_once_with(3)
    mock_amqp_channel.basic_nack.assert_not_awaited()


async def test_rabbitmq_callback_retry_preserved(conn, mock_amqp_channel):
    """Ordinary callback exceptions still nack and republish with one retry."""

    async def failing(message, body):
        raise ValueError("ordinary failure")

    await conn.wrap_callback(failing, max_retries=3)(_delivered())

    mock_amqp_channel.basic_nack.assert_awaited_once_with(3, requeue=False)
    mock_amqp_channel.basic_publish.assert_awaited_once()
    properties = mock_amqp_channel.basic_publish.await_args.kwargs["properties"]
    assert properties.headers["x-retry"] == "1"
