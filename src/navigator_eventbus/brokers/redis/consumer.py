"""RedisConsumer — port of navigator.brokers.redis.consumer (TASK-1815, FEAT-316).

Applies PR navigator#393 fix #1: ``queue_name``/``group_name``/
``consumer_name`` are popped from ``kwargs`` (instead of read via
``kwargs.get()``) before re-forwarding them explicitly to
``super().__init__()`` — the source's ``.get()`` left them in ``kwargs``
too, so they were forwarded both as explicit keyword args AND inside
``**kwargs``, raising ``TypeError: got multiple values for keyword
argument``.
"""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional, Union

from aiohttp import web
from navconfig.logging import logging

from ..consumer import BrokerConsumer
from .connection import RedisConnection


class RedisConsumer(RedisConnection, BrokerConsumer):
    """RedisConsumer — Broker Client (Consumer) using Redis Streams."""

    _name_: str = "redis_consumer"

    def __init__(
        self,
        credentials: Optional[Union[str, dict]] = None,
        timeout: Optional[int] = 5,
        callback: Optional[Union[Awaitable, Callable]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the Redis consumer.

        Args:
            credentials: Redis connection kwargs; see
                :class:`~navigator_eventbus.brokers.redis.connection.RedisConnection`.
            timeout: Connection timeout, in seconds.
            callback: Optional callback invoked for each received message.
            **kwargs: ``queue_name``/``group_name``/``consumer_name``
                (fix #1: popped, not merely read, to avoid a duplicate
                keyword argument when re-forwarded) plus anything else
                forwarded to :class:`RedisConnection`.
        """
        self._queue_name = kwargs.pop("queue_name", "message_stream")
        self._group_name = kwargs.pop("group_name", "default_group")
        self._consumer_name = kwargs.pop("consumer_name", "default_consumer")
        super().__init__(
            credentials=credentials,
            timeout=timeout,
            callback=callback,
            queue_name=self._queue_name,
            group_name=self._group_name,
            consumer_name=self._consumer_name,
            **kwargs,
        )
        self.logger = logging.getLogger("RedisConsumer")
        self.consumer_task: Optional[asyncio.Task] = None
        self._callback_ = callback if callback else self.subscriber_callback

    async def subscriber_callback(self, message_id: str, body: Any) -> None:
        """Default callback for event subscription."""
        try:
            self.logger.info(f"Received Message ID: {message_id} Body: {body}")
        except Exception as e:
            self.logger.error(f"Error in subscriber_callback: {e}")
            raise

    def wrap_callback(
        self,
        callback: Callable[[Any, Any], Awaitable[None]],
    ) -> Callable[[Any, Any], Awaitable[None]]:
        """Wrap the user-provided callback for message handling."""

        async def wrapped_callback(message_id, body):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(message_id, body)
                else:
                    callback(message_id, body)
            except Exception as e:
                self.logger.error(f"Error processing message {message_id}: {e}")

        return wrapped_callback

    async def event_subscribe(
        self,
        queue_name: Optional[str],
        callback: Union[Callable, Awaitable],
        **kwargs: Any,
    ) -> None:
        """Subscribe to a stream and consume messages via *callback*."""
        await self.consume_messages(
            queue_name=queue_name,
            callback=self.wrap_callback(callback),
            **kwargs,
        )

    async def subscribe_to_events(
        self,
        queue_name: Optional[str],
        callback: Union[Callable, Awaitable],
        **kwargs: Any,
    ) -> None:
        """Subscribe to events from a specific stream in a background task."""
        # Declare the stream and ensure group exists
        await self.ensure_connection()
        try:
            self.logger.info(f"Starting Redis consumer for stream: {queue_name}")
            self.consumer_task = asyncio.create_task(
                self.consume_messages(queue_name=queue_name, callback=callback, **kwargs)
            )
        except Exception as e:
            self.logger.error(f"Error subscribing to events: {e}")
            raise

    async def stop_consumer(self) -> None:
        """Stop the Redis consumer task gracefully."""
        if self.consumer_task:
            self.logger.info("Stopping Redis consumer...")
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                self.logger.info("Redis consumer task cancelled.")
            self.consumer_task = None

    async def start(self, app: web.Application) -> None:
        """Connect to Redis and start consuming (``on_startup`` handler)."""
        await super().start(app)
        await self.subscribe_to_events(
            queue_name=self._queue_name, callback=self._callback_
        )

    async def stop(self, app: web.Application) -> None:
        """Stop consuming and disconnect (``on_shutdown`` handler)."""
        await self.stop_consumer()
        await super().stop(app)
