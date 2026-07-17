"""AWS SQS broker hook (FEAT-312, Module 6).

Mudado desde ``packages/ai-parrot/src/parrot/core/hooks/brokers/sqs.py``
(ai-parrot@686aba1fe, FEAT-310) sin cambios de comportamiento. El
lazy-import a ``navigator.brokers.sqs.SQSConnection`` se conserva TAL
CUAL (spec: la fase 3 recablea los hooks de brokers a la capa interna).
"""
from typing import Any

from navigator_eventbus.hooks.brokers.base import BaseBrokerHook
from navigator_eventbus.hooks.models import BrokerHookConfig, HookType


class SQSBrokerHook(BaseBrokerHook):
    """Consumes messages from an AWS SQS queue."""

    hook_type = HookType.BROKER_SQS

    def __init__(self, config: BrokerHookConfig, **kwargs) -> None:
        super().__init__(config, **kwargs)
        self._queue_name = config.queue_name or "default_queue"
        self._max_messages = config.max_messages
        self._wait_time = config.wait_time
        self._idle_sleep = config.idle_sleep
        self._connection = None

    async def connect(self) -> None:
        from navigator.brokers.sqs import SQSConnection
        self._connection = SQSConnection(credentials=self._config.credentials)
        await self._connection.connect()  # type: ignore[attr-defined]
        self.logger.info("AWS SQS connection established")

    async def disconnect(self) -> None:
        if self._connection:
            await self._connection.disconnect()
            self.logger.info("AWS SQS connection closed")

    async def start_consuming(self) -> None:
        await self._connection.consume_messages(  # type: ignore[attr-defined]
            queue_name=self._queue_name,
            callback=self._consumer_callback,
            max_messages=self._max_messages,
            wait_time=self._wait_time,
            idle_sleep=self._idle_sleep,
        )

    async def _consumer_callback(self, message_id: Any, body: Any) -> None:
        self.logger.debug(f"SQS msg {message_id}: {body}")
        await self._on_message(message_id=message_id, payload=body)
