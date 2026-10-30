"""Keyed queues (FEAT-713): per-key FIFO, one in flight per key, head-blocking outcomes.

The config/model names are eager (pure Pydantic); ``KeyedQueue``,
``KeyedLease``, ``KeyedConsumer``, ``drain`` and ``backoff_ms`` resolve lazily
so importing this package costs no engine code.
"""
from typing import TYPE_CHECKING, Any

from navigator_eventbus.queues.keyed.config import (
    DrainReport,
    KeyedMessage,
    KeyedQueueConfig,
    LeaseLost,
    Outcome,
)

if TYPE_CHECKING:  # pragma: no cover
    from navigator_eventbus.queues.keyed.consumer import KeyedConsumer
    from navigator_eventbus.queues.keyed.drain import drain
    from navigator_eventbus.queues.keyed.store import KeyedLease, KeyedQueue, backoff_ms

_LAZY_MAP = {
    "KeyedConsumer": "navigator_eventbus.queues.keyed.consumer",
    "KeyedLease": "navigator_eventbus.queues.keyed.store",
    "KeyedQueue": "navigator_eventbus.queues.keyed.store",
    "backoff_ms": "navigator_eventbus.queues.keyed.store",
    "drain": "navigator_eventbus.queues.keyed.drain",
}

__all__ = (
    "DrainReport",
    "KeyedConsumer",
    "KeyedLease",
    "KeyedMessage",
    "KeyedQueue",
    "KeyedQueueConfig",
    "LeaseLost",
    "Outcome",
    "backoff_ms",
    "drain",
)


def __getattr__(name: str) -> Any:
    """Resolve engine classes on first access."""
    module_path = _LAZY_MAP.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(module_path), name)


def __dir__() -> list[str]:
    return sorted(__all__)
