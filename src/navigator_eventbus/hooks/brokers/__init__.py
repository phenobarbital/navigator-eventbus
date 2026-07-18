"""Broker hooks sub-package (FEAT-312 Module 6; rewired by FEAT-316 TASK-1818).

Mudado desde
``packages/ai-parrot/src/parrot/core/hooks/brokers/__init__.py``
(ai-parrot@686aba1fe, FEAT-310). The Redis/RabbitMQ/SQS hooks' lazy-imports
now point at this package's own internal ``brokers`` port
(``navigator_eventbus.brokers.*``, delivered by FEAT-316) — the external
navigator framework is no longer a dependency for broker support. The
``gmqtt``-based MQTT hook is unaffected (no MQTT broker exists upstream).
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
