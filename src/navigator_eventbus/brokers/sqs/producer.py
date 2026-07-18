"""SQSProducer — port of navigator.brokers.sqs.producer (TASK-1817, FEAT-316).

Inherits PR navigator#393 fix #3 (keyword ``credentials`` with ``None``
default) from :class:`~navigator_eventbus.brokers.producer.BrokerProducer`.
"""
from __future__ import annotations

from typing import Any, Optional, Union

from ..producer import BrokerProducer
from .connection import SQSConnection


class SQSProducer(SQSConnection, BrokerProducer):
    """SQSProducer — Producer functionality for AWS SQS.

    Args:
        credentials: AWS credentials.
        queue_size: Size of the asyncio Queue used to buffer outgoing
            events before a worker picks them up.
        num_workers: Number of workers processing the queue.
        timeout: Timeout for the SQS connection.
    """

    _name_: str = "sqs_producer"

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
