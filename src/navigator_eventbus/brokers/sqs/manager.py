"""SQSManager — AWS SQS producer + consumer over one connection (FEAT-431 Module 3)."""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Union

from aiohttp import web

from ..manager import BrokerManager
from .consumer import SQSConsumer


class SQSManager(BrokerManager, SQSConsumer):
    """Unified SQS publisher/consumer sharing one connection."""

    _name_: str = "sqs_manager"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("broker_service", "sqs")
        super().__init__(*args, **kwargs)
        self._consumer_tasks: list[asyncio.Task] = []

    async def subscribe_to_events(
        self,
        queue_name: str,
        callback: Union[Callable, Awaitable],
        max_messages: int = 10,
        wait_time: int = 10,
        idle_sleep: int = 5,
        **kwargs: Any,
    ) -> None:
        """Start a polling consumer task (see :class:`SQSConsumer`) and track it for shutdown."""
        await super().subscribe_to_events(
            queue_name, callback, max_messages=max_messages,
            wait_time=wait_time, idle_sleep=idle_sleep, **kwargs,
        )
        if self.consumer_task is not None:
            self._consumer_tasks.append(self.consumer_task)

    async def _subscribe(self, app: web.Application) -> None:
        """Subscribe to the configured queue with the configured callback."""
        await self.subscribe_to_events(queue_name=self._queue_name, callback=self._callback_)

    async def _stop_consuming(self) -> None:
        """Cancel and await every consumer task; tolerate none."""
        tasks, self._consumer_tasks = self._consumer_tasks, []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.consumer_task = None
