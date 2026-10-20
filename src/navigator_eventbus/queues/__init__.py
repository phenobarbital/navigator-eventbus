"""SQS-style pull queues for navigator-eventbus (FEAT-432).

A producer POSTs an
:class:`~navigator_eventbus.ingress_models.IngressEnvelope` to a queue URL;
consumers **pull** messages over HTTP with receipt handles, then delete them
or change their visibility. Consumers need nothing but HTTP, so they can be
written in any language and live outside the Redis network.

Semantics come straight from Redis Streams consumer groups, which are exactly
"queue per group, fan-out across groups": every group sees every message, and
within a group each message goes to exactly one consumer. The pending-entries
list *is* the in-flight set.

This is a **parallel plane to the bus**, not a layer on it. ``BusCore`` never
re-raises from a handler (isolation model B), so a message consumed through
``bus.subscribe()`` is acknowledged even when processing fails — the opposite
of what a lease-based queue needs. The queue plane therefore owns the lease
lifecycle end to end. Two opt-in bridges connect the planes:
``QueueConfig.mirror_to_bus`` and :class:`~navigator_eventbus.queues.feeder.QueueFeeder`.

``store`` and ``api`` are imported lazily so callers that never touch a queue
do not pay for the engine at import time. (Not an extras concern: ``navconfig
[default]`` already makes ``redis`` a hard transitive dependency.)
"""
from typing import TYPE_CHECKING, Any

from navigator_eventbus.queues.config import (
    DEFAULT_QUEUE_PREFIX,
    MAX_BATCH_ENTRIES,
    MAX_WAIT_TIME_SECONDS,
    QueueConfig,
    QueueGroupConfig,
    QueueRegistry,
    assert_prefixes_disjoint,
)
from navigator_eventbus.queues.models import (
    QueueAttributes,
    ReceivedMessage,
    ReceiveRequest,
    ReceiveResponse,
)
from navigator_eventbus.queues.receipts import Receipt, ReceiptCodec, ReceiptError

if TYPE_CHECKING:  # pragma: no cover
    from navigator_eventbus.queues.api import QueueAPI
    from navigator_eventbus.queues.feeder import QueueFeeder
    from navigator_eventbus.queues.store import QueueStore

_LAZY_MAP = {
    "QueueAPI": "navigator_eventbus.queues.api",
    "QueueFeeder": "navigator_eventbus.queues.feeder",
    "QueueStore": "navigator_eventbus.queues.store",
    "StaleReceipt": "navigator_eventbus.queues.store",
}

__all__ = (
    "DEFAULT_QUEUE_PREFIX",
    "MAX_BATCH_ENTRIES",
    "MAX_WAIT_TIME_SECONDS",
    "QueueAPI",
    "QueueAttributes",
    "QueueConfig",
    "QueueFeeder",
    "QueueGroupConfig",
    "QueueRegistry",
    "QueueStore",
    "Receipt",
    "ReceiptCodec",
    "ReceiptError",
    "ReceiveRequest",
    "ReceiveResponse",
    "ReceivedMessage",
    "StaleReceipt",
    "assert_prefixes_disjoint",
)


def __getattr__(name: str) -> Any:
    """Resolve Redis-dependent classes on first access."""
    module_path = _LAZY_MAP.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(module_path), name)


def __dir__() -> list[str]:
    return sorted(__all__)
