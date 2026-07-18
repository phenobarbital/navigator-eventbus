"""SQSConsumer — port of navigator.brokers.sqs.consumer (TASK-1817, FEAT-316).

Ported near-verbatim; no desacoples required beyond the shared
``SQSConnection`` changes.
"""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional, Union

from aiohttp import web
from navconfig.logging import logging

from ..consumer import BrokerConsumer
from .connection import SQSConnection


class SQSConsumer(SQSConnection, BrokerConsumer):
    """SQSConsumer — Broker Client (Consumer) using Amazon AWS SQS."""

    _name_: str = "sqs_consumer"

    def __init__(
        self,
        credentials: Optional[Union[str, dict]] = None,
        timeout: Optional[int] = 5,
        callback: Optional[Union[Awaitable, Callable]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the SQS consumer.

        Args:
            credentials: AWS credentials; see
                :class:`~navigator_eventbus.brokers.sqs.connection.SQSConnection`.
            timeout: Connection timeout, in seconds.
            callback: Optional callback invoked for each received message.
            **kwargs: ``queue_name`` plus anything else forwarded to
                :class:`SQSConnection`.
        """
        self._queue_name = kwargs.get("queue_name", "navigator")
        super().__init__(
            credentials=credentials, timeout=timeout, callback=callback, **kwargs
        )
        self.logger = logging.getLogger("SQSConsumer")

    async def subscriber_callback(self, message: Any, body: str) -> None:
        """Default callback for event subscription."""
        try:
            self.logger.info(f"Received Message: {body}")
        except Exception as e:
            self.logger.error(f"Error in subscriber_callback: {e}")
            raise

    async def event_subscribe(
        self,
        queue_name: str,
        callback: Union[Callable, Awaitable],
    ) -> None:
        """Subscribe to a queue and consume messages via *callback*."""
        await self.consume_messages(
            queue_name=queue_name, callback=self.wrap_callback(callback)
        )

    async def subscribe_to_events(
        self,
        queue_name: str,
        callback: Union[Callable, Awaitable],
        max_messages: int = 10,
        wait_time: int = 10,
        idle_sleep: int = 5,
        **kwargs: Any,
    ) -> None:
        """Subscribe to events from a specific queue in a background task."""
        await self.ensure_connection()
        try:
            self.logger.notice(f"Starting SQS consumer for queue: {queue_name}")  # type: ignore[attr-defined]
            self.consumer_task = asyncio.create_task(
                self.consume_messages(
                    queue_name=queue_name,
                    callback=callback,
                    max_messages=max_messages,
                    wait_time=wait_time,
                    idle_sleep=idle_sleep,
                    **kwargs,
                )
            )
        except Exception as e:
            self.logger.error(f"Error subscribing to events: {e}")
            raise

    async def start(self, app: web.Application) -> None:
        """Connect to SQS and start consuming (``on_startup`` handler)."""
        await super().start(app)
        await self.subscribe_to_events(
            queue_name=self._queue_name, callback=self._callback_
        )

    async def stop(self, app: web.Application) -> None:
        """Stop consuming and disconnect (``on_shutdown`` handler)."""
        await self.stop_consumer()
        await super().stop(app)

    async def stop_consumer(self) -> None:
        """Stop the SQS consumer task gracefully."""
        if self.consumer_task:
            self.logger.info("Stopping SQS consumer...")
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                self.logger.info("SQS consumer task cancelled.")
            self.consumer_task = None
