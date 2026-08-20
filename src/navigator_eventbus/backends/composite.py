"""CompositeBackend — multiplexes N delivery channels over one Redis
connection (FEAT-430).

`app.py`-style compliance wiring today creates one
:class:`~navigator_eventbus.backends.redis_streams.RedisStreamsBackend` per
delivery mode (broadcast fan-out, plus one consumer-group backend per
group), each with its own :class:`~navigator_eventbus.core.BusCore`, Redis
connection, and start/stop lifecycle hook. ``RedisStreamsBackend`` fixes
``delivery=``/``group=`` at construction (one mode per instance), and
``BusCore`` accepts a single ``backend=`` — so consolidating N delivery
modes onto one connection needs a composition layer, not a change to
either of those classes.

``CompositeBackend`` is that composition layer: it owns exactly one shared
``redis.asyncio`` client and creates N internal ``RedisStreamsBackend``
instances (one per :class:`Channel`), injecting the shared client into
each via the already-supported ``client=`` dependency-injection seam
(verified: ``redis_streams.py:238-239`` — an injected client is never
closed by the owning backend's own ``close()``, see
``redis_streams.py:384``). It implements
:class:`~navigator_eventbus.backends.base.TransportBackend` itself, so
``BusCore(backend=composite)`` works with zero changes to ``BusCore``.

Delegation, not inheritance: ``CompositeBackend`` never subclasses
``RedisStreamsBackend`` — it only composes and orchestrates instances of it.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Awaitable, Callable, Literal, Optional, Union

import redis.asyncio as aioredis
from navconfig.logging import logging

from navigator_eventbus.backends.base import OnEnvelope
from navigator_eventbus.backends.redis_streams import Codec, RedisStreamsBackend
from navigator_eventbus.envelope import EventEnvelope

#: Per-group dedup prefix template (spec §7 "Per-group dedup prefix").
_GROUP_DEDUP_PREFIX_TEMPLATE = "evb:events:dedup:{group}:"


@dataclass(frozen=True)
class Channel:
    """One logical delivery channel within a :class:`CompositeBackend`.

    Attributes:
        name: Human-readable identifier (also used as the consumer group
            name when ``delivery="group"``).
        delivery: ``"broadcast"`` or ``"group"``. At most one channel in a
            given :class:`CompositeBackend` may use ``"broadcast"`` (see
            that class's constructor validation) — it is the one whose
            entries feed ``BusCore``'s local dispatch queue.
        on_envelope: Per-channel callback invoked for each delivered
            envelope for domain dispatch. Required — a channel without a
            consumer is meaningless, and this field is ALWAYS invoked
            for every entry on this channel, regardless of delivery mode.
            For the single ``delivery="broadcast"`` channel specifically,
            :meth:`CompositeBackend.start_consumer` additionally chains
            in the transport-level callback ``BusCore`` supplies (see
            that method's docstring) — both fire, in that order, for
            every entry; neither one silently replaces the other.
        streams: Optional stream subset this channel consumes. When
            ``None``, inherits the parent ``CompositeBackend``'s full
            stream set. Stored internally as an immutable ``tuple`` (see
            ``__post_init__``) so this frozen dataclass cannot have its
            effective stream subset mutated via an aliased list after
            construction.
        max_deliveries: Group-mode only — entries exceeding this PEL
            delivery count are parked to ``on_dlq``. Raises ``ValueError``
            at construction when combined with ``delivery="broadcast"``
            (there is no PEL in group-less consumption). When ``None``,
            falls back to the composite-level ``max_deliveries=`` (if any
            was passed as a ``common_backend_kwargs`` default) — see
            :meth:`CompositeBackend._build_backend`.
        on_dlq: Group-mode only — DLQ callback for this channel. When
            ``None``, falls back to a composite-level ``on_dlq=`` default
            the same way ``max_deliveries`` does.
        codec: Optional per-channel :class:`~navigator_eventbus.backends.
            redis_streams.Codec` override. Defaults to the parent
            ``CompositeBackend``'s codec when ``None``.
    """
    name: str
    delivery: Literal["broadcast", "group"]
    on_envelope: OnEnvelope
    streams: Optional[list[str]] = None
    max_deliveries: Optional[int] = None
    on_dlq: Optional[Callable[..., Union[None, Awaitable[None]]]] = None
    codec: Optional[Codec] = None

    def __post_init__(self) -> None:
        if self.delivery == "broadcast" and self.max_deliveries is not None:
            raise ValueError(
                "max_deliveries is not supported with delivery='broadcast' "
                "— there is no PEL (pending entries list) in group-less "
                "consumption to bound redeliveries against"
            )
        if self.streams is not None:
            # Defensive copy — this dataclass is frozen, but a plain list
            # attribute can still be mutated (or shared/aliased) from the
            # outside after construction. Freezing the field itself to an
            # immutable tuple closes that gap.
            object.__setattr__(self, "streams", tuple(self.streams))


class CompositeBackend:
    """Multiplexes N delivery channels over one Redis connection.

    Implements ``TransportBackend`` so ``BusCore`` can consume it as a
    single backend. Internally creates one ``RedisStreamsBackend`` per
    :class:`Channel`, injecting the shared ``redis.asyncio`` client into
    every one of them.

    ``publish()`` fans out to the ONE channel designated as the publisher
    (``publish_via=``); consumption is independent per channel.

    Known limitation — broadcast cold start:
        ``RedisStreamsBackend``'s broadcast mode resolves its ``XREAD``
        tail cursor ONCE, at stream-discovery time, to whatever is
        currently the last entry in that Redis stream (see
        ``redis_streams.py``'s ``_resolve_tail_id``/
        ``_refresh_streams_broadcast``). If a brand-new stream does not
        exist yet when the broadcast channel's consumer starts, the
        FIRST entry ever published to it can race that discovery: should
        discovery run right after that publish, the newly-created
        stream's tail already IS that first entry, so it is treated as
        "pre-existing" and silently never dispatched. Group-mode
        channels are unaffected (a fresh consumer group's cursor starts
        at ``"0"`` and always sees every entry already in the stream).

        Callers that need the very first published entry to be reliably
        delivered on a cold stream should publish one throwaway "seed"
        entry BEFORE calling :meth:`start_consumer` (group channels will
        also see this seed — filter it out downstream, e.g. by
        ``event_id``, if that matters). This is exactly the pattern
        ``tests/test_composite_integration.py`` uses. Example::

            await composite.publish(seed_envelope)   # primes the stream
            await composite.start_consumer(on_envelope)
            # ... now safe to publish real entries on this stream
    """

    def __init__(
        self,
        *,
        redis_url: Optional[str] = None,
        client: Optional[Any] = None,
        password: Optional[str] = None,
        channels: list[Channel],
        codec: Optional[Codec] = None,
        stream_key_fn: Optional[Callable[[str], str]] = None,
        streams: Optional[list[str]] = None,
        retention: Optional[timedelta] = None,
        publish_via: Optional[str] = None,
        **common_backend_kwargs: Any,
    ) -> None:
        if redis_url is None and client is None:
            raise ValueError(
                "CompositeBackend requires a redis_url or an injected client"
            )
        if not channels:
            raise ValueError("CompositeBackend requires at least one channel")
        names = [channel.name for channel in channels]
        if len(names) != len(set(names)):
            raise ValueError(
                "Channel names must be unique within a CompositeBackend"
            )
        broadcast_names = [c.name for c in channels if c.delivery == "broadcast"]
        if len(broadcast_names) > 1:
            raise ValueError(
                "CompositeBackend supports at most one delivery='broadcast' "
                f"channel (got {broadcast_names!r}) — start_consumer() wires "
                "BusCore's single transport-level callback to exactly one "
                "feeder channel, so more than one broadcast channel would "
                "have no well-defined way to each receive it independently"
            )

        self.redis_url = redis_url
        self._client = client  # injected — not owned; None means we own it
        self._redis: Optional[Any] = client
        self._password = password
        self._channels: list[Channel] = list(channels)
        self._by_name: dict[str, Channel] = {c.name: c for c in channels}
        self._codec = codec
        self._stream_key_fn = stream_key_fn
        self._streams = streams
        self._retention = retention
        self._common_backend_kwargs = common_backend_kwargs

        publisher_name = publish_via or channels[0].name
        if publisher_name not in self._by_name:
            raise ValueError(
                f"publish_via={publisher_name!r} does not match any "
                f"channel name in {names!r}"
            )
        self._publish_via = publisher_name

        self._backends: dict[str, RedisStreamsBackend] = {}
        self.logger = logging.getLogger(
            "navigator_eventbus.backends.composite"
        )

    # ------------------------------------------------------------------
    # TransportBackend protocol
    # ------------------------------------------------------------------

    async def publish(self, envelope: EventEnvelope) -> None:
        """Fan *envelope* out through the ``publish_via`` channel only.

        Args:
            envelope: The envelope to publish.
        """
        await self._ensure_backends()
        backend = self._backends[self._publish_via]
        await backend.publish(envelope)

    async def start_consumer(self, on_envelope: OnEnvelope) -> None:
        """Start all N internal backends, wiring each channel's callback.

        ``on_envelope`` is the transport-level callback ``BusCore`` uses
        for loopback echo-suppression and local dispatch-queue feeding.
        It is chained onto the FEEDER channel — the single
        ``delivery="broadcast"`` channel (at most one is allowed, see
        the constructor), falling back to the ``publish_via`` channel
        when none is configured. "Chained" means BOTH callbacks fire for
        every entry on that channel, in order: ``on_envelope`` first
        (feeding ``BusCore``'s local dispatch queue for
        ``BusCore.subscribe()`` handlers), then ``channel.on_envelope``
        (the channel's own domain callback, e.g. routing to WebSockets) —
        neither ever silently replaces the other. Every other (group-mode)
        channel uses only its own ``channel.on_envelope`` for domain
        dispatch, since group consumption is independent of ``BusCore``'s
        local queue.

        A channel whose consumer fails to start is logged and skipped —
        it does not prevent the other channels from starting (spec §5
        "Channel consumer failure is isolated"). If some (or all)
        channels fail, a single aggregated warning/error is logged after
        the loop with the failed channel names, so a fully-degraded
        transport (every channel failed) is distinguishable in the logs
        from a healthy start — this never raises, matching this
        package's "a broken transport degrades to local-only dispatch,
        it never crashes the bus" contract (``backends/base.py``).

        Args:
            on_envelope: ``BusCore``'s transport-level callback.
        """
        await self._ensure_backends()
        broadcast_name = next(
            (c.name for c in self._channels if c.delivery == "broadcast"), None
        )
        feeder_name = broadcast_name or self._publish_via
        failed: list[str] = []
        for name, backend in self._backends.items():
            channel = self._by_name[name]
            if name == feeder_name:
                callback = self._chain_feeder_callback(on_envelope, channel)
            else:
                callback = channel.on_envelope
            try:
                await backend.start_consumer(callback)
            except Exception:  # noqa: BLE001
                failed.append(name)
                self.logger.exception(
                    "CompositeBackend channel %r failed to start its "
                    "consumer — other channels continue unaffected",
                    name,
                )
        if failed:
            total = len(self._backends)
            started = total - len(failed)
            log = self.logger.error if started == 0 else self.logger.warning
            log(
                "CompositeBackend started %d/%d channel consumer(s) — "
                "failed: %s%s",
                started, total, ", ".join(failed),
                " (ALL channels failed — this transport is effectively "
                "down; the bus degrades to local-only dispatch)"
                if started == 0 else "",
            )

    @staticmethod
    def _chain_feeder_callback(
        transport_callback: OnEnvelope, channel: Channel
    ) -> OnEnvelope:
        """Chain ``BusCore``'s transport callback with the feeder
        channel's own domain callback — both fire, in order, for every
        entry (see :meth:`start_consumer`'s docstring)."""

        async def _combined(envelope: EventEnvelope) -> None:
            await transport_callback(envelope)
            await channel.on_envelope(envelope)

        return _combined

    async def close(self) -> None:
        """Stop all internal backends, then close the shared Redis client.

        Internal backends never own the shared client (it is always
        injected via ``client=``), so their own ``close()`` is a no-op
        regarding the connection — only THIS method closes it, and only
        if this ``CompositeBackend`` created it itself (i.e. ``client=``
        was not passed to its own constructor).
        """
        for name, backend in self._backends.items():
            try:
                await backend.close()
            except Exception:  # noqa: BLE001
                self.logger.exception(
                    "CompositeBackend channel %r failed to close cleanly",
                    name,
                )
        self._backends.clear()
        if self._client is None and self._redis is not None:
            try:
                await self._redis.close()
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("shared redis client close error: %s", exc)
        self._redis = None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _ensure_client(self) -> Any:
        """(Re)build the shared Redis client if it does not exist yet."""
        if self._redis is None:
            kwargs: dict[str, Any] = {"decode_responses": True}
            if self._password is not None:
                kwargs["password"] = self._password
            self._redis = await aioredis.from_url(
                self.redis_url, **kwargs
            )
        return self._redis

    def _build_backend(
        self, channel: Channel, client: Any
    ) -> RedisStreamsBackend:
        """Create the internal ``RedisStreamsBackend`` for *channel*."""
        kwargs: dict[str, Any] = dict(self._common_backend_kwargs)
        # The consumer-group name is always this channel's own name — it
        # is never caller-overridable via a composite-wide default (dedup
        # prefixing and channel bookkeeping are keyed off it), so drop
        # any stray group= a caller might have passed as a common kwarg
        # rather than silently forwarding it to every channel (including
        # broadcast ones, where it would be inert but confusing).
        kwargs.pop("group", None)
        # channel-level value wins; else fall back to a composite-wide
        # default (if any was supplied via common_backend_kwargs) — same
        # precedence pattern as codec= below.
        common_max_deliveries = kwargs.pop("max_deliveries", None)
        common_on_dlq = kwargs.pop("on_dlq", None)
        kwargs["codec"] = channel.codec or self._codec
        kwargs["stream_key_fn"] = self._stream_key_fn
        kwargs["streams"] = (
            list(channel.streams) if channel.streams else self._streams
        )
        kwargs["retention"] = self._retention
        if channel.delivery == "group":
            kwargs["group"] = channel.name
            kwargs["dedup_prefix"] = _GROUP_DEDUP_PREFIX_TEMPLATE.format(
                group=channel.name
            )
            kwargs["max_deliveries"] = (
                channel.max_deliveries
                if channel.max_deliveries is not None
                else common_max_deliveries
            )
            kwargs["on_dlq"] = channel.on_dlq if channel.on_dlq is not None else common_on_dlq
        return RedisStreamsBackend(
            client=client,
            delivery=channel.delivery,
            **kwargs,
        )

    async def _ensure_backends(self) -> None:
        """Lazily create the N internal backends over the shared client."""
        if self._backends:
            return
        client = await self._ensure_client()
        for channel in self._channels:
            self._backends[channel.name] = self._build_backend(channel, client)
