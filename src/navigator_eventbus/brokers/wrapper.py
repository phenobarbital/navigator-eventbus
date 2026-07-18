"""BaseWrapper — port of navigator.brokers.wrapper (TASK-1814, FEAT-316).

Ported verbatim — the source has zero navigator-framework imports.
"""
from __future__ import annotations

import random
import uuid
from typing import Any


class BaseWrapper:
    """Abstract Wrapper Base. Any other wrapper extends this."""

    _queued: bool = True
    _debug: bool = False

    def __init__(self, coro: Any = None, *args: Any, **kwargs: Any) -> None:
        """Initialize the wrapper around a coroutine function.

        Args:
            coro: The coroutine function to wrap and eventually call.
            *args: Positional arguments forwarded to *coro*.
            **kwargs: Keyword arguments forwarded to *coro*; ``queued`` is
                popped here to configure :attr:`queued`.
        """
        if "queued" in kwargs:
            self._queued = kwargs["queued"]
            del kwargs["queued"]
        self._id = uuid.uuid1(node=random.getrandbits(48) | 0x010000000000)
        self.args = args
        self.kwargs = kwargs
        self.loop = None
        # retry functionality
        self.retries = 0
        # function to be handled:
        self.coro = coro

    async def call(self) -> None:
        """Call the wrapped coroutine with ``args[1:]`` and ``kwargs``."""
        await self.coro(*self.args[1:], **self.kwargs)

    async def __call__(self) -> Any:
        """Call the wrapped coroutine with ``args`` and ``kwargs``."""
        return await self.coro(*self.args, **self.kwargs)

    def add_retries(self) -> None:
        """Increment the retry counter."""
        self.retries += 1

    @property
    def queued(self) -> bool:
        """Whether this wrapper is queued for later execution."""
        return self._queued

    @queued.setter
    def queued(self, value: bool) -> None:
        self._queued = value

    @property
    def debug(self) -> bool:
        """Whether debug mode is enabled for this wrapper."""
        return self._debug

    @debug.setter
    def debug(self, debug: bool = False) -> None:
        self._debug = debug

    @property
    def id(self) -> Any:
        """The unique id assigned to this wrapper instance."""
        return self._id

    @id.setter
    def id(self, value: Any) -> None:
        self._id = value

    def set_loop(self, event_loop: Any) -> None:
        """Attach an event loop to this wrapper."""
        self.loop = event_loop
