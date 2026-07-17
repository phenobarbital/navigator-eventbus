"""Broker hooks sub-package (FEAT-312, Module 6).

Mudado desde
``packages/ai-parrot/src/parrot/core/hooks/brokers/__init__.py``
(ai-parrot@686aba1fe, FEAT-310). Lazy-imports to ``navigator.brokers.*``
and ``gmqtt`` are preserved as-is — phase 3 (``eventbus-brokers-port``)
recables them to the internal transport layer.
"""
from navigator_eventbus.hooks.brokers.base import BaseBrokerHook
from navigator_eventbus.hooks.brokers.mqtt import MQTTBrokerHook
from navigator_eventbus.hooks.brokers.rabbitmq import RabbitMQBrokerHook
from navigator_eventbus.hooks.brokers.redis import RedisBrokerHook
from navigator_eventbus.hooks.brokers.sqs import SQSBrokerHook

__all__ = [
    "BaseBrokerHook",
    "RedisBrokerHook",
    "RabbitMQBrokerHook",
    "MQTTBrokerHook",
    "SQSBrokerHook",
]
