"""RMQManager — RabbitMQ producer + consumer over one connection (FEAT-431 Module 3).

Combines :class:`~navigator_eventbus.brokers.manager.BrokerManager` with
:class:`~navigator_eventbus.brokers.rabbitmq.consumer.RMQConsumer`; the single
AMQP connection/channel comes from the MRO. Shutdown cancels every consumer
tag and lets in-flight deliveries finish before the outgoing queue drains.
"""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from aiohttp import web
from navconfig.logging import logging

from ..manager import BrokerManager
from .consumer import RMQConsumer


class RMQManager(BrokerManager, RMQConsumer):
    """Unified RabbitMQ publisher/consumer sharing one connection and channel.

    Subscribe explicitly with :meth:`subscribe_to_events` when built with
    ``auto_subscribe=False``; those consumer tags are cancelled on shutdown.
    Consumers started via :meth:`event_subscribe` are in-flight tracked but
    record no tag; they end when the channel closes on disconnect.
    """

    _name_: str = "rabbitmq_manager"
    #: Upper bound (seconds) for in-flight deliveries to finish on shutdown.
    _inflight_timeout: float = 30.0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger("Broker.Manager.RMQManager")
        self._inflight: int = 0
        self._idle: asyncio.Event = asyncio.Event()
        self._idle.set()

    def wrap_callback(
        self,
        callback: Callable[[Any, str], Awaitable[None]],
        requeue_on_fail: bool = False,
        max_retries: int = 3,
    ) -> Callable:
        """Wrap like :class:`RabbitMQConnection` and track the delivery as in-flight."""
        inner = super().wrap_callback(
            callback, requeue_on_fail=requeue_on_fail, max_retries=max_retries
        )

        async def tracked(message: Any) -> None:
            self._inflight += 1
            self._idle.clear()
            try:
                await inner(message)
            finally:
                self._inflight -= 1
                if self._inflight == 0:
                    self._idle.set()

        return tracked

    async def _subscribe(self, app: web.Application) -> None:
        """Subscribe with the configured exchange/queue/routing key/callback."""
        await self.subscribe_to_events(
            exchange=self._exchange_name,
            queue_name=self._queue_name,
            routing_key=self._routing_key,
            callback=self._callback_,
            exchange_type=self._exchange_type,
            durable=True,
            prefetch_count=1,
            requeue_on_fail=True,
        )

    async def _stop_consuming(self) -> None:
        """Cancel all consumer tags, then wait (bounded) for in-flight deliveries."""
        tags, self._consumer_tags = list(self._consumer_tags), []
        for tag in tags:
            if self._channel is None:
                continue
            try:
                await self._channel.basic_cancel(tag)
            except Exception as err:
                self.logger.warning(f"Could not cancel consumer tag {tag}: {err}")
        try:
            await asyncio.wait_for(self._idle.wait(), timeout=self._inflight_timeout)
        except asyncio.TimeoutError:
            self.logger.warning(
                f"{self._inflight} delivery(ies) still in flight after {self._inflight_timeout}s; continuing shutdown."
            )
