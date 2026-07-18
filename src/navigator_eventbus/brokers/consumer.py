"""BrokerConsumer — port of navigator.brokers.consumer (TASK-1814, FEAT-316).

Ported near-verbatim; no navigator-framework imports were present in the
source.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable, Optional, Union

from navconfig.logging import logging


class BrokerConsumer(ABC):
    """Broker Consumer Interface."""

    _name_: str = "broker_consumer"

    def __init__(
        self,
        callback: Optional[Union[Awaitable, Callable]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the consumer.

        Args:
            callback: Optional callback invoked for each received message;
                defaults to ``self.subscriber_callback``.
            **kwargs: Backend-specific options; ``queue_name`` is read here
                (default ``"navigator"``).
        """
        self._queue_name = kwargs.get("queue_name", "navigator")
        self.logger = logging.getLogger("Broker.Consumer")
        self._callback_ = callback if callback else self.subscriber_callback

    @abstractmethod
    async def event_subscribe(
        self,
        queue_name: str,
        callback: Union[Callable, Awaitable],
    ) -> None:
        """Subscribe to a Queue and consume messages."""

    @abstractmethod
    async def subscriber_callback(self, message: Any, body: str) -> None:
        """Default callback for event subscription."""

    @abstractmethod
    def wrap_callback(
        self,
        callback: Callable[[Any, str], Awaitable[None]],
        requeue_on_fail: bool = False,
        max_retries: int = 3,
    ) -> Callable:
        """Wrap the user-provided callback to handle decoding and ack."""
