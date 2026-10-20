"""RedisManager — Redis Streams producer + consumer over one connection (FEAT-431 Module 3)."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional, Union

from aiohttp import web

from ..manager import BrokerManager
from .consumer import RedisConsumer


class RedisManager(BrokerManager, RedisConsumer):
    """Unified Redis Streams publisher/consumer sharing one connection.

    Consumer tasks are cancelled mid-handler on shutdown, so an unfinished
    message stays pending and is reclaimed later (at-least-once).
    """

    _name_: str = "redis_manager"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("broker_service", "redis")
        super().__init__(*args, **kwargs)
        # RedisConnection.__init__ sets self._name_ = class name; restore the
        # declared registration key so setup() registers app["redis_manager"].
        self._name_ = type(self)._name_
        self._consumer_tasks: list[asyncio.Task] = []

    async def subscribe_to_events(
        self,
        queue_name: Optional[str],
        callback: Union[Callable, Awaitable],
        **kwargs: Any,
    ) -> None:
        """Start a consumer task (see :class:`RedisConsumer`) and track it for shutdown."""
        await super().subscribe_to_events(queue_name, callback, **kwargs)
        if self.consumer_task is not None:
            self._consumer_tasks.append(self.consumer_task)

    async def _subscribe(self, app: web.Application) -> None:
        """Subscribe to the configured stream with the configured callback."""
        await self.subscribe_to_events(
            queue_name=self._queue_name, callback=self._callback_
        )

    async def _stop_consuming(self) -> None:
        """Cancel and await every consumer task; tolerate none."""
        tasks, self._consumer_tasks = self._consumer_tasks, []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.consumer_task = None
