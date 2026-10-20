"""QueueFeeder — bridge bus events into a pull queue.

The more valuable of the two bridges between the planes: it makes events the
bus is already carrying available to external, non-Python consumers over
plain HTTP, without giving them Redis access or a Python runtime.

.. note::
   This inherits ``BusCore``'s isolation model B. A failed ``XADD`` is
   retried by the bus and then routed to the DLQ; it does **not** apply
   backpressure to the emitter and it does not fail the original publish.
   The bus, not the queue, is the source of truth on this path.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from navconfig.logging import logging

from navigator_eventbus.envelope import EventEnvelope
from navigator_eventbus.queues.config import QueueConfig

if TYPE_CHECKING:  # pragma: no cover
    from navigator_eventbus.core import BusCore
    from navigator_eventbus.queues.store import QueueStore

__all__ = ("QueueFeeder",)


class QueueFeeder:
    """Copies matching bus envelopes into a queue's stream.

    Example:
        >>> feeder = QueueFeeder(store, config, patterns=["order.*"])
        >>> feeder.attach(bus)                     # doctest: +SKIP

    Args:
        store: The queue engine to write through.
        config: Target queue.
        patterns: Glob topic patterns; one subscription per entry.
        exclude_bus_internal: Drop ``bus.*`` meta-topics. On by default —
            forwarding the bus's own error topics into a business queue is
            almost never wanted, and ``bus.queue_dlq`` would feed itself.
    """

    def __init__(
        self,
        store: "QueueStore",
        config: QueueConfig,
        *,
        patterns: Optional[list[str]] = None,
        exclude_bus_internal: bool = True,
    ) -> None:
        self._store = store
        self._config = config
        self._patterns = list(patterns or ["*"])
        self._exclude_bus_internal = exclude_bus_internal
        self._subscription_ids: list[str] = []
        self._forwarded = 0
        self._failed = 0
        self.logger = logging.getLogger(
            f"navigator_eventbus.queues.feeder.{config.name}"
        )

    def attach(self, bus: Any) -> list[str]:
        """Subscribe on *bus*.

        Args:
            bus: A ``BusCore`` or the ``EventBus`` facade (resolved via its
                ``.core`` property), matching every other subscriber here.

        Returns:
            One subscription id per pattern.
        """
        core: "BusCore" = getattr(bus, "core", bus)
        for pattern in self._patterns:
            self._subscription_ids.append(
                core.subscribe(pattern, self.handle, filter_fn=self._accepts)
            )
        self.logger.info(
            "QueueFeeder attached to %d pattern(s) -> queue %s",
            len(self._subscription_ids),
            self._config.name,
        )
        return list(self._subscription_ids)

    def detach(self, bus: Any) -> int:
        """Remove every subscription. Returns how many were removed."""
        core: "BusCore" = getattr(bus, "core", bus)
        removed = sum(1 for sid in self._subscription_ids if core.unsubscribe(sid))
        self._subscription_ids.clear()
        return removed

    @property
    def stats(self) -> dict[str, int]:
        """Forwarding counters."""
        return {"forwarded": self._forwarded, "failed": self._failed}

    def _accepts(self, envelope: EventEnvelope) -> bool:
        """Subscription filter."""
        if self._exclude_bus_internal and envelope.topic.startswith("bus."):
            return False
        return True

    async def handle(self, envelope: EventEnvelope) -> None:
        """Append *envelope* to the queue.

        Raises on failure so ``BusCore``'s retry and DLQ path sees it — the
        one place in this package where letting the exception escape is the
        correct behaviour.
        """
        try:
            await self._store.send(self._config, envelope)
        except Exception:
            self._failed += 1
            raise
        self._forwarded += 1
