"""BrokerManager — unified producer + consumer over one connection (FEAT-431 Module 3).

Extends :class:`~navigator_eventbus.brokers.producer.BrokerProducer` with a
lifecycle that performs both halves against a single broker connection.
Concrete managers combine it with a backend consumer, e.g.
``class RMQManager(BrokerManager, RMQConsumer)``. The base order is
load-bearing: ``BrokerProducer`` first, ``BrokerConsumer`` last in the MRO
(``BrokerConsumer.__init__`` terminates the cooperative ``__init__`` chain).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from aiohttp import web
from navconfig.logging import logging

from .producer import BrokerProducer


class BrokerManager(BrokerProducer, ABC):
    """Unifies producer and consumer roles over a single broker connection.

    Args:
        *args: Forwarded to :class:`BrokerProducer`.
        auto_subscribe: When ``True`` (default), :meth:`start` subscribes the
            consumer side after connecting. Set ``False`` to connect and start
            the publish workers only, and subscribe explicitly later (needed
            when the consumer callback does not exist yet at construction).
        **kwargs: Forwarded through the cooperative ``__init__`` chain
            (producer and backend consumer options).
    """

    _name_: str = "broker_manager"

    def __init__(self, *args: Any, auto_subscribe: bool = True, **kwargs: Any) -> None:
        self._auto_subscribe: bool = auto_subscribe
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(f"Broker.Manager.{type(self).__name__}")

    async def start(self, app: web.Application) -> None:
        """``on_startup`` handler — one connect, publish workers, then subscribe.

        Args:
            app: The aiohttp application.
        """
        await self.connect()
        await self.start_workers()
        if self._auto_subscribe:
            await self._subscribe(app)
        else:
            self.logger.debug("auto_subscribe=False: consumer side not subscribed.")

    async def stop(self, app: web.Application) -> None:
        """``on_shutdown`` handler — stop consuming, drain, cancel workers, disconnect.

        Args:
            app: The aiohttp application.
        """
        try:
            await self._stop_consuming()
        except Exception as err:  # pylint: disable=broad-except
            self.logger.error(f"Error stopping consumer side: {err}")
        # BrokerProducer.stop: event_queue.join() → cancel/gather workers → disconnect()
        await super().stop(app)

    @abstractmethod
    async def _subscribe(self, app: web.Application) -> None:
        """Subscribe the consumer side using the manager's configured options."""

    @abstractmethod
    async def _stop_consuming(self) -> None:
        """Stop all consumer activity (including manual subscriptions); must tolerate none."""
