"""RedisProducer — port of navigator.brokers.redis.producer (TASK-1815, FEAT-316).

Inherits PR navigator#393 fix #3 (keyword ``credentials`` with ``None``
default) from :class:`~navigator_eventbus.brokers.producer.BrokerProducer`.
"""
from __future__ import annotations

from typing import Any, Optional, Union

from ..producer import BrokerProducer
from .connection import RedisConnection


class RedisProducer(RedisConnection, BrokerProducer):
    """RedisProducer — Producer functionality for Redis Streams.

    Args:
        credentials: Redis connection kwargs.
        queue_size: Size of the asyncio Queue used to buffer outgoing
            events before a worker picks them up.
        num_workers: Number of workers processing the queue.
        timeout: Timeout for the Redis connection.
    """

    _name_: str = "redis_producer"

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
