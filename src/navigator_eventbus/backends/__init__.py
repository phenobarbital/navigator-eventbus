"""Transport backends for navigator_eventbus (FEAT-312, Module 4).

Mudado desde
``packages/ai-parrot/src/parrot/core/events/bus/backends/__init__.py``
(ai-parrot@686aba1fe, FEAT-310).
"""
from navigator_eventbus.backends.base import OnEnvelope, TransportBackend
from navigator_eventbus.backends.memory import MemoryBackend
from navigator_eventbus.backends.redis_pubsub import RedisPubSubBackend
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend

__all__ = (
    "MemoryBackend",
    "OnEnvelope",
    "RedisPubSubBackend",
    "RedisStreamsBackend",
    "TransportBackend",
)
