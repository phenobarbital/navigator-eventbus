"""RabbitMQ broker hook (FEAT-312 Module 6; rewired by FEAT-316 TASK-1818).

Mudado desde
``packages/ai-parrot/src/parrot/core/hooks/brokers/rabbitmq.py``
(ai-parrot@686aba1fe, FEAT-310) sin cambios de comportamiento. El
lazy-import ahora apunta al port interno de FEAT-316
(``navigator_eventbus.brokers.rabbitmq.RabbitMQConnection``) — el paquete
ya no depende del framework navigator externo para soporte de brokers.
FEAT-431 keeps delivery settlement in the connection callback wrapper.
"""

from typing import TYPE_CHECKING, Any, Optional

from navigator_eventbus.brokers.exceptions import RejectDelivery
from navigator_eventbus.hooks.brokers.base import BaseBrokerHook
from navigator_eventbus.hooks.models import BrokerHookConfig, HookType

if TYPE_CHECKING:
    from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection


class RabbitMQBrokerHook(BaseBrokerHook):
    """Consumes messages from a RabbitMQ queue."""

    hook_type = HookType.BROKER_RABBITMQ

    def __init__(self, config: BrokerHookConfig, **kwargs) -> None:
        super().__init__(config, **kwargs)
        self._queue_name = config.queue_name or "default_queue"
        self._routing_key = config.routing_key
        self._exchange_name = config.exchange_name
        self._exchange_type = config.exchange_type
        self._prefetch_count = config.prefetch_count
        self._connection: Optional["RabbitMQConnection"] = None

    async def connect(self) -> None:
        from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection

        # RabbitMQConnection takes a DSN string; the generic BrokerHookConfig
        # carries a dict, so the DSN travels under its "dsn" key (or is omitted
        # to use the navconfig RABBITMQ_* default).
        credentials = self._config.credentials
        dsn: Optional[str] = None
        if credentials:
            dsn = credentials.get("dsn")
            if not isinstance(dsn, str):
                raise TypeError(
                    "RabbitMQBrokerHook: BrokerHookConfig.credentials must carry a "
                    "'dsn' string (or be unset to use the configured default)"
                )
        connection = RabbitMQConnection(credentials=dsn)
        await connection.connect()
        self._connection = connection
        self.logger.info("RabbitMQ connection established")
        # Ensure exchange and queue exist
        await connection.ensure_exchange(
            exchange_name=self._exchange_name or self._queue_name,
            exchange_type=self._exchange_type,
        )
        await connection.ensure_queue(
            queue_name=self._queue_name,
            exchange_name=self._exchange_name or self._queue_name,
            routing_key=self._routing_key,
        )

    async def disconnect(self) -> None:
        if self._connection:
            await self._connection.disconnect()
            self.logger.info("RabbitMQ connection closed")

    async def start_consuming(self) -> None:
        """Consume from the configured queue.

        Raises:
            RuntimeError: If :meth:`connect` has not run.
        """
        if self._connection is None:
            raise RuntimeError("RabbitMQBrokerHook.connect() must run before start_consuming()")
        await self._connection.consume_messages(
            queue_name=self._queue_name,
            callback=self._consumer_callback,
            prefetch_count=self._prefetch_count,
        )

    async def _consumer_callback(self, message: Any, body: Any) -> None:
        message_id = getattr(message, "delivery_tag", str(message))
        self.logger.debug("RabbitMQ msg %s: %s", message_id, body)
        try:
            await self._on_message(message_id=message_id, payload=body, message=message)
        except Exception as exc:
            self.logger.error("RabbitMQ callback error: %s", exc)
            # The connection's wrap_callback owns settlement (FEAT-431 M4):
            # signal a non-requeued rejection instead of nacking here, so
            # the delivery is never both rejected and acknowledged.
            raise RejectDelivery(str(exc)) from exc
