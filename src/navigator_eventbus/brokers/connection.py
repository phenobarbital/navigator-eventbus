"""BaseConnection — port of navigator.brokers.connection (TASK-1814, FEAT-316).

Decoupled from ``navigator.applications.base.BaseApplication`` (spec §7):
``setup()`` now duck-types the passed-in app object instead of doing an
``isinstance`` check, so it works with a plain ``aiohttp.web.Application``
just as well as with navigator's ``BaseApplication`` wrapper.
"""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, Optional, Union

from aiohttp import web
from navconfig.logging import logging

from .serializers import DataSerializer


class BaseConnection(ABC):
    """Manages connection and operations over Broker Services."""

    #: Key under which ``setup()`` registers this instance on the aiohttp
    #: app (``app[self._name_] = self``). Concrete subclasses override this
    #: with a specific name (e.g. ``"redis_producer"``); the base default
    #: only exists so mypy sees a concrete attribute here.
    _name_: str = "broker_connection"

    def __init__(
        self,
        *args: Any,
        credentials: Optional[Union[str, dict]] = None,
        timeout: Optional[int] = 5,
        **kwargs: Any,
    ) -> None:
        """Initialize the connection state shared by all broker backends.

        Args:
            credentials: Broker connection credentials.
            timeout: Connection timeout, in seconds.
            **kwargs: Backend-specific options; ``max_reconnect_attempts`` is
                read here (default 3), everything else is forwarded to
                ``super().__init__()`` for cooperative multiple inheritance.
        """
        self._credentials = credentials
        self._timeout: Optional[int] = timeout
        self._connection = None
        self._monitor_task: Optional[Awaitable] = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self._queues: dict = {}
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = kwargs.get("max_reconnect_attempts", 3)
        self.reconnect_delay = 1  # Initial delay in seconds
        self._lock = asyncio.Lock()
        self._serializer = DataSerializer()
        super().__init__(*args, **kwargs)

    def get_connection(self) -> Optional[Union[Callable, Awaitable]]:
        """Return the underlying broker connection object.

        Raises:
            RuntimeError: If no connection has been established yet.
        """
        if not self._connection:
            raise RuntimeError("No connection established.")
        return self._connection

    def get_serializer(self) -> DataSerializer:
        """Return the ``DataSerializer`` used to encode/decode messages."""
        return self._serializer

    async def __aenter__(self) -> "BaseConnection":
        """Connect on entering an ``async with`` block."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Disconnect on exiting an ``async with`` block."""
        await self.disconnect()

    @abstractmethod
    async def connect(self) -> None:
        """Create a connection to the Broker Service."""
        raise NotImplementedError

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the Broker Service."""
        raise NotImplementedError

    async def ensure_connection(self) -> None:
        """Ensure that the connection is active, connecting if needed."""
        if self._connection is None:
            await self.connect()

    @abstractmethod
    async def publish_message(
        self,
        body: Union[str, list, dict, Any],
        queue_name: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Publish a message to the Broker Service."""
        raise NotImplementedError

    @abstractmethod
    async def consume_messages(
        self,
        queue_name: str,
        callback: Callable,
        **kwargs: Any,
    ) -> None:
        """Consume messages from the Broker Service."""
        raise NotImplementedError

    @abstractmethod
    async def process_message(self, body: bytes, properties: Any) -> str:
        """Process a message from the Broker Service."""
        raise NotImplementedError

    async def start(self, app: web.Application) -> None:
        """``on_startup`` signal handler — connects to the broker."""
        await self.connect()

    async def stop(self, app: web.Application) -> None:
        """``on_shutdown`` signal handler — disconnects from the broker."""
        await self.disconnect()

    def setup(self, app: Any = None) -> None:
        """Wire this connection into an aiohttp application.

        Accepts either a plain ``web.Application`` or an object exposing a
        ``get_app()`` method (e.g. navigator's ``BaseApplication``) via
        duck-typing — this decouples the brokers package from the navigator
        framework while remaining compatible with it (spec §7). Typed as
        ``Any`` (rather than ``Optional[web.Application]``) precisely
        because it accepts that wider, duck-typed surface.

        Raises:
            ValueError: If *app* (or ``app.get_app()``) resolves to ``None``.
        """
        app = app.get_app() if hasattr(app, "get_app") else app
        self.app = app
        if self.app is None:
            raise ValueError("App is not defined.")
        # Initialize the Producer instance.
        self.app.on_startup.append(self.start)
        self.app.on_shutdown.append(self.stop)
        self.app[self._name_] = self
