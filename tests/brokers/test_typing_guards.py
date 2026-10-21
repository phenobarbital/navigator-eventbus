"""Guard tests for the FEAT-712 typing pass (issue:b12a71083ca3).

TASK-4571 covers the base contracts, the RabbitMQ family and the broker hooks;
TASK-4572 appends the Redis / SQS guards.
"""
from __future__ import annotations

import pytest

from navigator_eventbus.brokers import MessageCallback
from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection
from navigator_eventbus.hooks.brokers.rabbitmq import RabbitMQBrokerHook
from navigator_eventbus.hooks.models import BrokerHookConfig

DSN = "amqp://guest:guest@localhost:5672/"


def test_message_callback_alias_is_exported() -> None:
    assert MessageCallback is not None


def test_rabbitmq_rejects_non_str_credentials() -> None:
    with pytest.raises(TypeError):
        RabbitMQConnection(credentials={"host": "x"})


async def test_rabbitmq_publish_requires_exchange_and_routing_key() -> None:
    conn = RabbitMQConnection(credentials=DSN)
    with pytest.raises(ValueError):
        await conn.publish_message({"a": 1}, queue_name=None, routing_key="k")
    with pytest.raises(ValueError):
        await conn.publish_message({"a": 1}, queue_name="exchange", routing_key=None)


async def test_rabbitmq_hook_start_consuming_before_connect_raises() -> None:
    hook = RabbitMQBrokerHook(BrokerHookConfig(broker_type="rabbitmq", queue_name="q"))
    with pytest.raises(RuntimeError):
        await hook.start_consuming()


async def test_rabbitmq_hook_rejects_credentials_without_dsn() -> None:
    hook = RabbitMQBrokerHook(
        BrokerHookConfig(broker_type="rabbitmq", queue_name="q", credentials={"host": "x"})
    )
    with pytest.raises(TypeError):
        await hook.connect()


def test_redis_require_client_before_connect_raises() -> None:
    from navigator_eventbus.brokers.redis import RedisConnection

    with pytest.raises(RuntimeError):
        RedisConnection()._require_client()


def test_sqs_require_resource_before_connect_raises() -> None:
    from navigator_eventbus.brokers.sqs import SQSConnection

    with pytest.raises(RuntimeError):
        SQSConnection(credentials={"region_name": "us-east-1"})._require_resource()


async def test_sqs_publish_requires_queue_name() -> None:
    from navigator_eventbus.brokers.sqs import SQSConnection

    with pytest.raises(ValueError):
        await SQSConnection(credentials={"region_name": "us-east-1"}).publish_message({"a": 1})
