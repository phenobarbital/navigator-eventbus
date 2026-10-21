"""Tests for FEAT-431 Modules 2 and 5: RMQ/SQS consumer fixes (TASK-4562)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiormq
import pytest

from navigator_eventbus.brokers.rabbitmq import RMQConsumer
from navigator_eventbus.brokers.sqs import SQSConsumer

DSN = "amqp://guest:guest@localhost:5672/navigator"


@pytest.fixture
def mock_amqp_channel():
    """aiormq channel double (spec §4 fixture)."""
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
    ):
        setattr(ch, name, AsyncMock())
    ch.basic_consume = AsyncMock(
        return_value=aiormq.spec.Basic.ConsumeOk(consumer_tag="ctag-1")
    )
    return ch


def _wire(consumer: RMQConsumer, channel) -> RMQConsumer:
    """Attach a fake open connection + channel so ensure_connection() is a no-op."""
    consumer._connection = SimpleNamespace(is_closed=False)
    consumer._channel = channel
    return consumer


def _delivered(body: bytes = b"hello", tag: int = 7):
    """Minimal DeliveredMessage double accepted by RabbitMQConnection.wrap_callback."""
    props = aiormq.spec.Basic.Properties(content_type="text/plain", headers={})
    return SimpleNamespace(
        body=body,
        delivery_tag=tag,
        header=SimpleNamespace(properties=props),
        delivery=SimpleNamespace(exchange="navigator", routing_key="#"),
    )


def test_exchange_not_clobbered():
    c = RMQConsumer(
        credentials=DSN, exchange_name="navigator", queue_name="processing_queue"
    )
    assert c._exchange_name == "navigator"
    assert c._queue_name == "processing_queue"


def test_exchange_falls_back_to_queue():
    c = RMQConsumer(credentials=DSN, queue_name="processing_queue")
    assert c._exchange_name == "processing_queue"


def test_exchange_default():
    c = RMQConsumer(credentials=DSN)
    assert c._exchange_name == "navigator"


async def test_event_subscribe_single_wrap(mock_amqp_channel):
    """User callback receives (message, decoded_body); exactly one basic_ack."""
    c = _wire(RMQConsumer(credentials=DSN), mock_amqp_channel)
    received = []

    async def cb(message, body):
        received.append((message, body))

    await c.event_subscribe("processing_queue", cb)
    consumer_callback = mock_amqp_channel.basic_consume.call_args.kwargs[
        "consumer_callback"
    ]
    msg = _delivered()
    await consumer_callback(msg)
    assert received == [(msg, "hello")]
    mock_amqp_channel.basic_ack.assert_awaited_once_with(7)


async def test_subscribe_records_consumer_tag(mock_amqp_channel):
    c = _wire(RMQConsumer(credentials=DSN), mock_amqp_channel)
    await c.subscribe_to_events("navigator", "q", "#", AsyncMock())
    assert c._consumer_tags == ["ctag-1"]


def test_consumer_tags_empty_by_default():
    assert RMQConsumer(credentials=DSN)._consumer_tags == []


async def test_sqs_event_subscribe_single_wrap(monkeypatch):
    """SQSConsumer.event_subscribe hands the raw callback to consume_messages."""
    c = SQSConsumer(credentials={"region_name": "us-east-1"})
    consume = AsyncMock()
    monkeypatch.setattr(c, "consume_messages", consume)

    async def cb(message, body): ...

    await c.event_subscribe("q", cb)
    assert consume.call_args.kwargs["callback"] is cb
