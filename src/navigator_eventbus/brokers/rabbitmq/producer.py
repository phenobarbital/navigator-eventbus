"""RMQProducer — port of navigator.brokers.rabbitmq.producer (TASK-1816, FEAT-316).

Inherits PR navigator#393 fix #3 (keyword ``credentials`` with ``None``
default) from :class:`~navigator_eventbus.brokers.producer.BrokerProducer`.

Note the REVERSED base order vs. the Redis/SQS producers —
``RMQProducer(BrokerProducer, RabbitMQConnection)`` — ported exactly as in
the navigator source.
"""
from __future__ import annotations

from typing import Any, Optional, Union

from navconfig.logging import logging

from ..producer import BrokerProducer
from .connection import RabbitMQConnection

# Disable Debug Logging for AIORMQ
logging.getLogger("aiormq").setLevel(logging.INFO)


class RMQProducer(BrokerProducer, RabbitMQConnection):
    """RMQProducer — Producer functionality for RabbitMQ using aiormq.

    Args:
        credentials: RabbitMQ DSN.
        queue_size: Size of the asyncio Queue used to buffer outgoing
            events before a worker picks them up.
        num_workers: Number of workers processing the queue.
        timeout: Timeout for the RabbitMQ connection.
    """

    _name_: str = "rabbitmq_producer"

    def __init__(
        self,
        credentials: Union[str, dict] = None,
        queue_size: Optional[int] = None,
        num_workers: Optional[int] = 4,
        timeout: Optional[int] = 5,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            queue_size=queue_size,
            num_workers=num_workers,
            timeout=timeout,
            **kwargs,
        )
