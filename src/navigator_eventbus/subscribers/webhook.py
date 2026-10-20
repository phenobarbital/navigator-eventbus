"""WebhookDeliverySubscriber — POST matching bus envelopes to an HTTP endpoint.

The outbound half of the webhook fabric, and the mirror image of
:mod:`navigator_eventbus.hooks.webhook`: the same
:class:`~navigator_eventbus.webhook_signatures.SignatureScheme` objects that
*verify* inbound deliveries *sign* outbound ones here, so a navigator-eventbus
receiver can consume a navigator-eventbus sender with no extra glue.

.. note::
   Not to be confused with
   :class:`navigator_eventbus.lifecycle.subscribers.webhook.WebhookSubscriber`,
   which is an ``EventProvider`` for the lifecycle ``EventRegistry`` and takes
   ``LifecycleEvent`` objects. This class attaches to ``BusCore`` and takes
   :class:`~navigator_eventbus.envelope.EventEnvelope` objects. They are not
   interchangeable.

**Why deliveries are queued rather than POSTed inline.** ``BusCore`` applies
``handler_timeout`` (30 s by default) to subscriber handlers. Three HTTP
attempts with backoff can occupy ~18 s of that budget *per event*, holding a
dispatch worker the whole time; under a burst of failures a small worker pool
stalls completely and every stalled handler lands on ``bus.subscriber_error``
or in the DLQ. Buffering keeps the handler O(1) and preserves the bus's
subscriber-isolation contract. On overload the **oldest** buffered delivery is
dropped and counted — the bus is never back-pressured, matching
``AuditSubscriber``.
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any, Optional
from urllib.parse import urlparse

from navconfig.logging import logging
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.json_schema import SkipJsonSchema

from navigator_eventbus._imports import resolve_callable
from navigator_eventbus.core import BusCore
from navigator_eventbus.envelope import EventEnvelope, Severity
from navigator_eventbus.subscribers._delivery import HttpDelivery
from navigator_eventbus.webhook_signatures import get_signature_scheme

__all__ = ("DELIVERY_FAILED_TOPIC", "WebhookDeliveryConfig", "WebhookDeliverySubscriber")

#: Meta-topic emitted when a delivery exhausts its retries. Lives under
#: ``bus.*`` deliberately: ``exclude_bus_internal`` defaults to True, so the
#: subscriber structurally cannot re-deliver its own failure notice.
DELIVERY_FAILED_TOPIC = "bus.webhook_delivery_failed"


class WebhookDeliveryConfig(BaseModel):
    """Configuration for :class:`WebhookDeliverySubscriber`."""

    model_config = ConfigDict(extra="forbid")

    name: str = "webhook_delivery"
    url: str = Field(..., description="Destination endpoint (http/https only).")
    patterns: list[str] = Field(
        default_factory=lambda: ["*"],
        description="Glob topic patterns; one bus subscription per entry.",
    )
    min_severity: Severity = Field(
        default=Severity.INFO, description="Drop envelopes below this severity."
    )
    exclude_bus_internal: bool = Field(
        default=True,
        description="Drop bus.* meta-topics. Also the loop guard for this subscriber's own failure events.",
    )

    # --- signing ---------------------------------------------------------
    secret: Optional[str] = Field(default=None, repr=False)
    signature_scheme: str = "generic"
    signature_header: Optional[str] = None
    headers: dict[str, str] = Field(default_factory=dict)

    # --- retry -----------------------------------------------------------
    timeout_seconds: float = Field(default=5.0, gt=0)
    max_attempts: int = Field(default=3, ge=1)
    backoff_base: float = Field(default=0.5, gt=0)
    backoff_max: float = Field(default=30.0, gt=0)
    backoff_jitter: bool = True
    retry_statuses: set[int] = Field(
        default_factory=lambda: {429},
        description="Extra retryable statuses. 5xx is always retried.",
    )

    # --- buffering -------------------------------------------------------
    queue_size: int = Field(default=1000, gt=0)
    concurrency: int = Field(default=4, ge=1)

    # --- payload shaping --------------------------------------------------
    transform: Optional[str] = Field(
        default=None, description='Import string "pkg.mod:func" mapping envelope -> dict.'
    )
    transform_fn: SkipJsonSchema[Optional[Callable[..., Any]]] = Field(
        default=None, exclude=True, repr=False
    )
    emit_failure_meta: bool = Field(
        default=True, description=f"Emit {DELIVERY_FAILED_TOPIC} when retries are exhausted."
    )

    @field_validator("url")
    @classmethod
    def _supported_scheme(cls, value: str) -> str:
        parsed = urlparse(value)
        if parsed.scheme not in ("https", "http"):
            raise ValueError(
                f"Unsupported URL scheme {parsed.scheme!r}. Only 'https' and "
                "'http' are allowed."
            )
        if not parsed.netloc:
            raise ValueError(f"URL {value!r} has no host")
        return value

    @field_validator("signature_scheme")
    @classmethod
    def _known_scheme(cls, value: str) -> str:
        try:
            get_signature_scheme(value)
        except KeyError as exc:
            raise ValueError(str(exc).strip('"')) from exc
        return value

    def model_post_init(self, __context: Any) -> None:
        """Resolve the transform import string at construction time."""
        if self.transform_fn is None and self.transform:
            try:
                self.transform_fn = resolve_callable(self.transform)
            except TypeError as exc:
                raise ValueError(str(exc)) from exc


class WebhookDeliverySubscriber:
    """Bus subscriber that POSTs matching envelopes to an HTTP endpoint.

    Example:
        >>> subscriber = WebhookDeliverySubscriber(
        ...     url="https://example.test/hook",
        ...     patterns=["order.*"],
        ...     secret="shared",
        ...     signature_scheme="github",
        ... )                                   # doctest: +SKIP
        >>> subscriber.attach(bus)              # doctest: +SKIP
        >>> await subscriber.aclose()           # doctest: +SKIP

    Args:
        config: A full configuration object.
        **kwargs: Individual :class:`WebhookDeliveryConfig` fields, used when
            *config* is omitted.
    """

    def __init__(
        self, config: Optional[WebhookDeliveryConfig] = None, **kwargs: Any
    ) -> None:
        self._config = config or WebhookDeliveryConfig(**kwargs)
        self.logger = logging.getLogger(
            f"navigator_eventbus.subscribers.{self._config.name}"
        )
        self._scheme = get_signature_scheme(self._config.signature_scheme)
        self._delivery = HttpDelivery(
            url=self._config.url,
            timeout_seconds=self._config.timeout_seconds,
            max_attempts=self._config.max_attempts,
            backoff_base=self._config.backoff_base,
            backoff_max=self._config.backoff_max,
            jitter=self._config.backoff_jitter,
            retry_statuses=self._config.retry_statuses,
            logger=self.logger,
        )
        self._queue: asyncio.Queue[Optional[EventEnvelope]] = asyncio.Queue(
            maxsize=self._config.queue_size
        )
        self._workers: list[asyncio.Task[None]] = []
        self._subscription_ids: list[str] = []
        self._core: Optional[BusCore] = None
        self._closed = False
        self._delivered = 0
        self._failed = 0
        self._dropped = 0
        self._retries = 0

    # ------------------------------------------------------------------
    # Attach / detach
    # ------------------------------------------------------------------

    def attach(self, bus: BusCore) -> list[str]:
        """Subscribe on *bus* and start the delivery workers.

        Args:
            bus: The ``BusCore`` to deliver from — or the ``EventBus`` facade
                (resolved via its ``.core`` property), matching the
                convention used by every other subscriber in this package.

        Returns:
            One subscription id per configured pattern.
        """
        core: BusCore = getattr(bus, "core", bus)
        self._core = core
        self._closed = False
        self._start_workers()
        for pattern in self._config.patterns:
            self._subscription_ids.append(
                core.subscribe(
                    pattern,
                    self._on_envelope,
                    filter_fn=self._accepts,
                    min_severity=self._config.min_severity,
                )
            )
        self.logger.info(
            "WebhookDeliverySubscriber '%s' attached to %d pattern(s) -> %s",
            self._config.name,
            len(self._subscription_ids),
            self._config.url,
        )
        return list(self._subscription_ids)

    def detach(self, bus: BusCore) -> int:
        """Remove every subscription. Workers keep draining until :meth:`aclose`."""
        core: BusCore = getattr(bus, "core", bus)
        removed = sum(
            1 for sid in self._subscription_ids if core.unsubscribe(sid)
        )
        self._subscription_ids.clear()
        return removed

    def _start_workers(self) -> None:
        """Spawn the delivery worker tasks if they are not already running."""
        if self._workers:
            return
        for index in range(self._config.concurrency):
            task = asyncio.create_task(
                self._run_worker(index), name=f"webhook-delivery-{index}"
            )
            self._workers.append(task)

    async def aclose(self) -> None:
        """Drain this subscriber's buffer, stop the workers, close the session.

        Only the subscriber's own buffer is drained — an envelope still
        waiting in a ``BusCore`` priority queue has not reached this
        subscriber yet and is invisible here. For a clean shutdown close the
        bus first, then the subscriber::

            await bus.close()
            await subscriber.aclose()

        Idempotent.
        """
        if self._closed:
            return
        self._closed = True
        if self._workers:
            try:
                await asyncio.wait_for(self._queue.join(), timeout=self._drain_budget())
            except asyncio.TimeoutError:
                self.logger.warning(
                    "WebhookDeliverySubscriber '%s' closed with %d undelivered "
                    "item(s) still queued",
                    self._config.name,
                    self._queue.qsize(),
                )
            for task in self._workers:
                task.cancel()
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers.clear()
        await self._delivery.aclose()

    def _drain_budget(self) -> float:
        """Upper bound on how long :meth:`aclose` waits for the queue."""
        return self._config.timeout_seconds * self._config.max_attempts + 1.0

    @property
    def stats(self) -> dict[str, int]:
        """Delivery counters."""
        return {
            "delivered": self._delivered,
            "failed": self._failed,
            "dropped": self._dropped,
            "retries": self._retries,
            "queued": self._queue.qsize(),
        }

    # ------------------------------------------------------------------
    # Bus handling
    # ------------------------------------------------------------------

    def _accepts(self, envelope: EventEnvelope) -> bool:
        """Subscription filter — drops ``bus.*`` internals when configured."""
        if self._config.exclude_bus_internal and envelope.topic.startswith("bus."):
            return False
        return True

    async def _on_envelope(self, envelope: EventEnvelope) -> None:
        """Buffer *envelope* for delivery. Returns immediately.

        Never awaits the network, so a slow or dead endpoint cannot occupy a
        ``BusCore`` dispatch worker.
        """
        if self._closed:
            return
        try:
            self._queue.put_nowait(envelope)
        except asyncio.QueueFull:
            # Drop the OLDEST buffered item and keep the newest: under
            # sustained overload fresh events are more useful than stale ones.
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                self._dropped += 1
            except asyncio.QueueEmpty:  # pragma: no cover — race, harmless
                pass
            try:
                self._queue.put_nowait(envelope)
            except asyncio.QueueFull:  # pragma: no cover — race, harmless
                self._dropped += 1
            if self._dropped % 100 == 1:
                self.logger.warning(
                    "Webhook delivery queue overloaded — %d envelope(s) dropped",
                    self._dropped,
                )

    async def _run_worker(self, index: int) -> None:
        """Consume the queue and deliver each envelope."""
        while True:
            envelope = await self._queue.get()
            try:
                if envelope is None:
                    return
                await self._deliver(envelope)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — a worker must not die
                self.logger.error(
                    "Webhook delivery worker %d error: %s", index, exc, exc_info=True
                )
            finally:
                self._queue.task_done()

    # ------------------------------------------------------------------
    # Delivery
    # ------------------------------------------------------------------

    def _build_request(self, envelope: EventEnvelope) -> tuple[bytes, dict[str, str]]:
        """Serialize *envelope* and build the signed request headers."""
        if self._config.transform_fn is not None:
            payload = self._config.transform_fn(envelope)
        else:
            payload = envelope.to_dict()
        body = json.dumps(payload, default=str).encode("utf-8")
        headers = {"Content-Type": "application/json", **self._config.headers}
        if self._config.secret:
            signed = self._scheme.sign(secret=self._config.secret, body=body)
            if self._config.signature_header:
                # Re-key onto the operator's chosen header.
                (value,) = signed.values()
                signed = {self._config.signature_header: value}
            headers.update(signed)
        return body, headers

    async def _deliver(self, envelope: EventEnvelope) -> bool:
        """Deliver one envelope, emitting a meta-event if it never lands."""
        body, headers = self._build_request(envelope)
        outcome = await self._delivery.post(body, headers)
        self._retries += max(0, outcome.attempts - 1)
        if outcome.delivered:
            self._delivered += 1
            return True
        self._failed += 1
        if self._config.emit_failure_meta:
            await self._emit_failure(envelope, outcome)
        return False

    async def _emit_failure(self, envelope: EventEnvelope, outcome: Any) -> None:
        """Publish ``bus.webhook_delivery_failed`` for observability.

        Failure-isolated, and safe from feedback loops: the topic is under
        ``bus.*``, which :meth:`_accepts` filters out by default.
        """
        if self._core is None:
            return
        try:
            await self._core.publish(
                EventEnvelope(
                    topic=DELIVERY_FAILED_TOPIC,
                    payload={
                        "subscriber": self._config.name,
                        "url": self._config.url,
                        "topic": envelope.topic,
                        "event_id": envelope.event_id,
                        "attempts": outcome.attempts,
                        "status": outcome.status,
                        "error": outcome.error,
                    },
                    source=self._config.name,
                    severity=Severity.ERROR,
                )
            )
        except Exception as exc:  # noqa: BLE001 — observability must not throw
            self.logger.warning(
                "Could not emit %s: %s", DELIVERY_FAILED_TOPIC, exc
            )
