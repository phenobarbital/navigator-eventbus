"""Redis Streams broker hook (FEAT-312, Module 6).

Mudado desde ``packages/ai-parrot/src/parrot/core/hooks/brokers/redis.py``
(ai-parrot@686aba1fe, FEAT-310) sin cambios de comportamiento. El
lazy-import a ``navigator.brokers.redis.RedisConnection`` se conserva TAL
CUAL (spec: la fase 3 recablea los hooks de brokers a la capa interna).
"""
from typing import Any

from navigator_eventbus.hooks.brokers.base import BaseBrokerHook
from navigator_eventbus.hooks.models import BrokerHookConfig, HookType


class RedisBrokerHook(BaseBrokerHook):
    """Consumes messages from a Redis Stream."""

    hook_type = HookType.BROKER_REDIS

    def __init__(self, config: BrokerHookConfig, **kwargs) -> None:
        super().__init__(config, **kwargs)
        self._stream = config.stream_name or config.queue_name or "default_stream"
        self._group = config.group_name
        self._consumer = config.consumer_name
        self._connection = None

    async def connect(self) -> None:
        from navigator.brokers.redis import RedisConnection
        self._connection = RedisConnection(credentials=self._config.credentials)
        await self._connection.connect()  # type: ignore[attr-defined]
        self.logger.info("Redis Stream connection established")

    async def disconnect(self) -> None:
        if self._connection:
            await self._connection.disconnect()
            self.logger.info("Redis Stream connection closed")

    async def start_consuming(self) -> None:
        await self._connection.consume_messages(  # type: ignore[attr-defined]
            stream_name=self._stream,
            group_name=self._group,
            consumer_name=self._consumer,
            callback=self._consumer_callback,
        )

    async def _consumer_callback(self, message_id: Any, body: Any) -> None:
        self.logger.debug(f"Redis msg {message_id}: {body}")
        await self._on_message(message_id=message_id, payload=body)
