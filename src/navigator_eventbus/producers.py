"""Generic producer facade in front of :class:`BusCore`.

This module contributes upstream a small compatibility shim that consuming
applications otherwise hand-roll in front of ``BusCore.publish()``:

- **Lazy bus resolution** — the producer holds a *provider callable*, not a
  bus reference, so a bus wired (or replaced) after the producer was
  constructed is picked up on the very next publish.
- **A bounded publish timeout** — ``BusCore.publish()`` is not
  unconditionally O(1): under the default ``block`` backpressure policy it
  awaits ``queue.put(envelope)``, which can block indefinitely when the
  target priority queue is full.
- **Explicit strict-vs-fail-soft error propagation** — a caller decides,
  per producer instance, whether a delivery failure should raise or be
  logged and swallowed.

See ``sdd/specs/buscore-producer-facade.spec.md`` for the full design
rationale, in particular §2 "Architectural Design" for the two-phase call
sequence and the strict-mode exception-type table.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any, Optional

from navigator_eventbus.core import BusCore
from navigator_eventbus.envelope import EventEnvelope, Severity
from navigator_eventbus.evb import EventPriority


class BusCoreProducer:
    """Generic compatibility facade in front of ``BusCore.publish()``.

    Owns lazy bus resolution, a bounded publish timeout, and explicit
    strict-vs-fail-soft error propagation. Contains NO application-specific
    routing, topic, or schema rules — ``body`` is opaque payload and
    ``queue_name`` is the raw topic string.

    Every envelope published through an instance of this class carries the
    ``source``, ``severity`` and ``priority`` configured at construction
    time — there are no per-call overrides. A caller needing per-event
    variation of those three fields should hold a second producer instance
    configured differently.

    Under a saturated queue with the default ``block`` backpressure policy,
    a fail-soft producer (``raise_on_error=False``) silently **drops**
    events once ``timeout`` elapses — this is a deliberate trade-off, not a
    delivery guarantee.

    ``BusCore.publish()`` spawns the backend fan-out task *before* the local
    enqueue, so a producer-side timeout on the local enqueue does not cancel
    a fan-out already in flight: the event may still reach the transport
    even though this facade logs (or raises) a timeout.

    Attributes:
        logger: Per-module logger, ``navigator_eventbus.producers``.
    """

    def __init__(
        self,
        bus_provider: Callable[[], Optional[BusCore]],
        *,
        timeout: float = 1.0,
        raise_on_error: bool = False,
        source: Optional[str] = None,
        severity: Severity = Severity.INFO,
        priority: EventPriority = EventPriority.NORMAL,
    ) -> None:
        """Configure the producer.

        Args:
            bus_provider: Zero-argument callable returning the current
                ``BusCore`` instance, or ``None`` if one is not yet
                available. Called fresh on every ``publish_event()`` —
                never memoized here — so a bus wired after construction is
                picked up transparently.
            timeout: Seconds to bound the ``BusCore.publish()`` await.
                Must be ``> 0``. Pass ``None`` to disable the bound (the
                publish is awaited unbounded).
            raise_on_error: When ``True``, a delivery failure (missing bus,
                ``BusCore.publish()`` raising, or timeout expiry) propagates
                to the caller. When ``False`` (default), it is logged at
                WARNING and swallowed. This flag does **not** govern request
                validation failures (e.g. a naive ``timestamp``), which
                always raise regardless of its value.
            source: Optional emitter identifier stamped on every envelope
                this producer publishes.
            severity: ``Severity`` stamped on every envelope this producer
                publishes. Defaults to ``Severity.INFO``.
            priority: ``EventPriority`` stamped on every envelope this
                producer publishes. Defaults to ``EventPriority.NORMAL``.

        Raises:
            ValueError: If ``timeout`` is not ``None`` and is ``<= 0``.
        """
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be > 0, or None to disable it")
        self._bus_provider = bus_provider
        self._timeout = timeout
        self._raise_on_error = raise_on_error
        self._source = source
        self._severity = severity
        self._priority = priority
        self.logger = logging.getLogger("navigator_eventbus.producers")

    async def publish_event(
        self,
        body: dict[str, Any],
        queue_name: str,
        **kwargs: Any,
    ) -> None:
        """Publish ``body`` under topic ``queue_name`` through ``BusCore``.

        Only the ``timestamp`` key is read from ``**kwargs``, and only when
        its value is a ``datetime`` instance. Every other key — including
        legacy names like ``routing_key`` — is accepted for call-site
        compatibility with older duck-typed producer seams and is silently
        ignored: it is *not* an error, but it also has no effect. In
        particular, ``source``, ``severity`` and ``priority`` are **not**
        readable from ``**kwargs`` here — they are per-instance constructor
        arguments (see ``__init__``); passing them to this method does
        nothing.

        Two phases, with a hard boundary between them:

        Phase 1 — request validation (always raises, in both
        ``raise_on_error`` modes): the ``EventEnvelope`` is constructed. A
        naive or non-``datetime`` ``timestamp`` raises ``ValueError`` here,
        unconditionally.

        Phase 2 — delivery (governed by ``raise_on_error``): the bus is
        resolved via ``bus_provider()`` and ``BusCore.publish()`` is
        awaited inside a bounded ``asyncio.timeout``. A missing bus raises
        ``RuntimeError``; ``BusCore.publish()`` exceptions (``BusClosedError``,
        ``BackpressureError``, or anything else) propagate as themselves;
        timeout expiry raises ``TimeoutError``. In strict mode
        (``raise_on_error=True``) these propagate to the caller; in
        fail-soft mode (``raise_on_error=False``) they are logged at
        WARNING and swallowed. ``asyncio.CancelledError`` is never
        swallowed by either mode.

        Args:
            body: Opaque JSON-safe payload, forwarded verbatim as
                ``EventEnvelope.payload``.
            queue_name: Raw topic string, forwarded verbatim as
                ``EventEnvelope.topic``. No prefixing, no validation.
            **kwargs: Legacy/duck-typed call-site compatibility keywords.
                Only ``timestamp`` (a ``datetime``) is honoured.

        Raises:
            ValueError: A naive or non-``datetime`` ``timestamp`` reached
                ``EventEnvelope``. Always raised, regardless of
                ``raise_on_error``.
            RuntimeError: ``bus_provider()`` returned ``None``, in strict
                mode.
            TimeoutError: The publish exceeded ``timeout``, in strict mode.
            BusClosedError: ``BusCore.publish()`` raised it, in strict mode.
            BackpressureError: ``BusCore.publish()`` raised it, in strict
                mode.
        """
        # ---- PHASE 1: request validation — ALWAYS raises ----
        timestamp = kwargs.get("timestamp")
        extra: dict[str, Any] = (
            {"timestamp": timestamp} if isinstance(timestamp, datetime) else {}
        )
        unrecognised = set(kwargs) - {"timestamp"}
        if unrecognised:
            self.logger.debug(
                "publish_event received unrecognised legacy kwargs %s for %s",
                sorted(unrecognised),
                queue_name,
            )
        envelope = EventEnvelope(
            topic=queue_name,
            payload=body,
            source=self._source,
            severity=self._severity,
            priority=self._priority,
            **extra,
        )

        # ---- PHASE 2: delivery — governed by raise_on_error ----
        try:
            bus = self._bus_provider()
            if bus is None:
                raise RuntimeError(
                    f"No BusCore available from bus_provider() for topic {queue_name!r}"
                )
            async with asyncio.timeout(self._timeout):
                await bus.publish(envelope)
        except Exception:
            if self._raise_on_error:
                raise
            self.logger.warning(
                "publish_event failed for topic %s", queue_name, exc_info=True
            )
