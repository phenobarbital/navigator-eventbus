"""RedisStreamsBackend — durable at-least-once transport (FEAT-312, Module 4).

Mudado desde
``packages/ai-parrot/src/parrot/core/events/bus/backends/redis_streams.py``
(ai-parrot@686aba1fe, FEAT-310). Brings consumer groups, explicit ACKs,
``XAUTOCLAIM`` pending recovery, and ``event_id`` dedup — at-least-once
delivery across instances with zero app-code change (it is just another
:class:`~navigator_eventbus.backends.base.TransportBackend`).

Design (unchanged from FEAT-310):

- **Stream per topic-class** (first topic segment):
  ``XADD <stream_prefix><topic-class>`` with the ``EventEnvelope.to_dict()``
  JSON wire format in a single ``envelope`` field.
- **Consumer group** (default ``evb-bus``, configurable) with a
  per-instance consumer name (``<hostname>-<pid>``) for XAUTOCLAIM
  bookkeeping.
- **Dedup**: TTL'd Redis key ``<dedup_prefix><event_id>`` (default 24 h)
  — checked before dispatch, set only AFTER successful dispatch
  (crash-safe: a crash mid-dispatch leaves the entry un-ACKed and
  unmarked, so it is reclaimed and reprocessed).

⚠️ **At-least-once, NOT exactly-once**: the dedup set *mitigates*
duplicates but cannot eliminate them (TTL expiry, Redis eviction, crash
between dispatch and ACK). Consumers MUST be idempotent in distributed
mode.

**FEAT-312 decoupling**: ``STREAM_PREFIX``/``DEDUP_PREFIX``/``group`` are
now neutral by default (``evb:stream:`` / ``evb:events:dedup:`` /
``evb-bus``) and configurable per constructor kwarg or navconfig
(``BUS_STREAM_PREFIX``, ``BUS_DEDUP_PREFIX``, ``BUS_GROUP``) — ai-parrot
will pin the legacy ``parrot:*`` / ``parrot-bus`` values when it migrates
(phase 4). No change to the XADD/XREADGROUP/XACK/XAUTOCLAIM logic itself.

Retention: ``XADD ... MAXLEN ~ <n>`` (approximate trim, default 100 000
entries per stream) — unchanged.

Deployment note: requires a reachable Redis instance from every process —
configuration only, no code assumptions.
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Literal, Optional, Protocol, Union

import redis.asyncio as aioredis
from navconfig import config as nav_config
from navconfig.logging import logging

from navigator_eventbus.backends.base import OnEnvelope
from navigator_eventbus.envelope import EventEnvelope

#: Neutral defaults (FEAT-312) — override via constructor kwarg or navconfig.
DEFAULT_STREAM_PREFIX = "evb:stream:"
DEFAULT_DEDUP_PREFIX = "evb:events:dedup:"
DEFAULT_GROUP = "evb-bus"


def _default_consumer_name() -> str:
    """Stable per-instance consumer name for XAUTOCLAIM bookkeeping."""
    return f"{socket.gethostname()}-{os.getpid()}"


class Codec(Protocol):
    """Wire encode/decode seam for :class:`RedisStreamsBackend` (FEAT-320
    Module 2).

    Replaces the hard-coded ``{"envelope": json.dumps(envelope.to_dict())}``
    wire shape when supplied via ``codec=`` — lets a consuming app plug in
    its own envelope wire format without forking this backend. Structural
    typing (mirrors this repo's :class:`~navigator_eventbus.backends.base.
    TransportBackend` Protocol convention), not a Pydantic model.
    """

    def encode(self, envelope: EventEnvelope) -> dict[str, Any]:
        """Encode *envelope* into ``XADD`` field/value pairs."""
        ...

    def decode(self, fields: dict[str, Any]) -> EventEnvelope:
        """Decode ``XADD`` field/value pairs back into an ``EventEnvelope``."""
        ...


class _DefaultCodec:
    """Preserves today's exact wire shape — the implicit codec used when
    ``codec=`` is not supplied."""

    def encode(self, envelope: EventEnvelope) -> dict[str, Any]:
        return {"envelope": json.dumps(envelope.to_dict())}

    def decode(self, fields: dict[str, Any]) -> EventEnvelope:
        raw = fields.get("envelope") or fields.get(b"envelope")  # type: ignore[call-overload]
        data = raw.decode() if isinstance(raw, bytes) else raw
        return EventEnvelope.from_dict(json.loads(data))


class RedisStreamsBackend:
    """Redis Streams transport: durable, replayable, at-least-once.

    Args:
        redis_url: Redis connection URL (ignored when *client* is given).
        client: Optional pre-built redis client (dependency injection for
            tests); when provided, this backend does not own/close it.
        group: Consumer-group name (FEAT-312 — default ``evb-bus``, also
            configurable via the ``BUS_GROUP`` navconfig key).
        consumer_name: Per-instance consumer name
            (default ``<hostname>-<pid>``).
        stream_prefix: Stream key prefix (FEAT-312 — default
            ``evb:stream:``, also configurable via ``BUS_STREAM_PREFIX``).
        dedup_prefix: Dedup key prefix (FEAT-312 — default
            ``evb:events:dedup:``, also configurable via
            ``BUS_DEDUP_PREFIX``).
        dedup_ttl: TTL in seconds for the ``event_id`` dedup keys
            (default 24 h).
        block_ms: ``XREADGROUP`` blocking timeout in milliseconds — keeps
            the loop cancellable on ``close()``.
        batch_count: Max messages fetched per ``XREADGROUP``/``XAUTOCLAIM``.
        min_idle_time_ms: Pending-entry idle time before the sweeper
            reclaims it (crashed-consumer recovery).
        autoclaim_interval: Seconds between sweeper passes.
        maxlen: Approximate per-stream retention (``MAXLEN ~`` — see the
            module docstring for the decision rationale).
        stream_refresh_interval: Seconds between ``SCAN``-based discovery
            of new ``<stream_prefix>*`` streams.
        reconnect_base_delay: Initial reconnect backoff in seconds.
        reconnect_max_delay: Reconnect backoff ceiling in seconds.
        delivery: ``"group"`` (default, unchanged) uses consumer-GROUP
            delivery (``XREADGROUP``/``XACK``/``XAUTOCLAIM``, one consumer
            per entry). ``"broadcast"`` (FEAT-320 Module 1) is a group-less
            free-read (``XREAD``) mode where every backend instance
            dispatches every entry — no ACK, no sweeper/``XAUTOCLAIM``
            (there is no PEL in group-less consumption). A newly discovered
            stream starts at ``"$"`` (tail) so pre-existing entries are
            never replayed on start.
        codec: Optional :class:`Codec` (FEAT-320 Module 2) — encode/decode
            seam that replaces the hard-coded ``{"envelope": ...}`` wire
            shape. Defaults to preserving today's exact format.
        stream_key_fn: Optional ``Callable[[str], str]`` (FEAT-320
            Module 2) replacing ``_stream_for``'s ``<stream_prefix>
            <topic-class>`` naming with a caller-owned scheme.
        streams: Optional explicit stream set (FEAT-320 Module 2) that
            bypasses ``SCAN``-based discovery entirely — required whenever
            ``stream_key_fn=`` produces names outside the
            ``<stream_prefix>*`` shape (a warning is logged, not raised, if
            ``stream_key_fn`` is set without ``streams``).
        retention: Optional ``timedelta`` (FEAT-320 Module 3) — alternative,
            time-based retention strategy. When set, a periodic
            ``XTRIM ... MINID <now - retention>`` task replaces the
            default count-based ``MAXLEN ~`` trimming (``publish()`` stops
            passing ``maxlen=``/``approximate=`` to ``XADD``). Mutually
            exclusive with ``maxlen`` IN PRACTICE — ``retention`` silently
            wins if both are set (no constructor error).

            **Caveat**: the trimmer task only runs while ``start_consumer()``
            has been called on this instance (it is spawned there, same as
            the sweeper/retention/broadcast tasks). A producer-only
            instance (never consumes) that sets ``retention=`` gets NO
            trimming at all — unlike ``maxlen``, which trims inline on
            every ``XADD`` regardless of whether this instance ever
            consumes. Pair ``retention=`` with at least one long-running
            consumer instance per stream, or keep ``maxlen`` (the default)
            for producer-only deployments.
        retention_trim_interval: Seconds between ``XTRIM MINID`` passes
            (only used when ``retention`` is set).
        max_deliveries: Optional int (FEAT-320 Module 4, ``delivery="group"``
            only) — an entry whose Redis PEL delivery count
            (``XPENDING ... IDLE``'s ``times_delivered``) exceeds this cap
            is routed to ``on_dlq`` and ``XACK``ed (never reclaimed again)
            instead of being redispatched forever. Raises ``ValueError`` at
            construction when combined with ``delivery="broadcast"``
            (there is no PEL in group-less consumption).
        on_dlq: Optional ``Callable[..., Union[None, Awaitable[None]]]``
            invoked as ``on_dlq(envelope, *, attempts, error, subscriber_id)``
            for an entry parked by ``max_deliveries`` — signature matches
            :meth:`navigator_eventbus.dlq.DLQHandler.on_dlq` by convention
            (duck-typed; this module does not import ``dlq.py``).

    Note:
        Exactly-one-trimmer-per-stream (whether via ``maxlen`` or
        ``retention``) is the CALLER's responsibility across backend
        instances — this class does not add distributed locking around
        trimming, unchanged from today's ``maxlen`` principle.
    """

    #: Kept for backward-reading callers that inspect class attributes
    #: directly; instances use ``self.stream_prefix``/``self.dedup_prefix``.
    STREAM_PREFIX = DEFAULT_STREAM_PREFIX
    DEDUP_PREFIX = DEFAULT_DEDUP_PREFIX

    def __init__(
        self,
        redis_url: Optional[str] = None,
        *,
        client: Optional[Any] = None,
        password: Optional[str] = None,
        username: Optional[str] = None,
        group: Optional[str] = None,
        consumer_name: Optional[str] = None,
        stream_prefix: Optional[str] = None,
        dedup_prefix: Optional[str] = None,
        dedup_ttl: int = 86_400,
        block_ms: int = 1_000,
        batch_count: int = 32,
        min_idle_time_ms: int = 60_000,
        autoclaim_interval: float = 30.0,
        maxlen: int = 100_000,
        stream_refresh_interval: float = 10.0,
        reconnect_base_delay: float = 0.5,
        reconnect_max_delay: float = 30.0,
        delivery: Literal["group", "broadcast"] = "group",
        codec: Optional["Codec"] = None,
        stream_key_fn: Optional[Callable[[str], str]] = None,
        streams: Optional[list[str]] = None,
        retention: Optional[timedelta] = None,
        retention_trim_interval: float = 60.0,
        max_deliveries: Optional[int] = None,
        on_dlq: Optional[
            Callable[..., Union[None, Awaitable[None]]]
        ] = None,
    ) -> None:
        if redis_url is None and client is None:
            raise ValueError(
                "RedisStreamsBackend requires a redis_url or an injected client"
            )
        if delivery not in ("group", "broadcast"):
            raise ValueError(
                f"delivery must be 'group' or 'broadcast', got {delivery!r}"
            )
        if delivery == "broadcast" and max_deliveries is not None:
            raise ValueError(
                "max_deliveries is not supported with delivery='broadcast' "
                "— there is no PEL (pending entries list) in group-less "
                "consumption to bound redeliveries against"
            )
        self.redis_url = redis_url
        self._password = password
        self._username = username
        self._client = client  # injected — not owned
        self._redis: Optional[Any] = None
        self.stream_prefix = stream_prefix or nav_config.get(
            "BUS_STREAM_PREFIX", fallback=DEFAULT_STREAM_PREFIX
        )
        self.dedup_prefix = dedup_prefix or nav_config.get(
            "BUS_DEDUP_PREFIX", fallback=DEFAULT_DEDUP_PREFIX
        )
        self._group = group or nav_config.get(
            "BUS_GROUP", fallback=DEFAULT_GROUP
        )
        self._consumer = consumer_name or _default_consumer_name()
        self._dedup_ttl = dedup_ttl
        self._block_ms = block_ms
        self._batch_count = batch_count
        self._min_idle_time_ms = min_idle_time_ms
        self._autoclaim_interval = autoclaim_interval
        self._maxlen = maxlen
        self._stream_refresh_interval = stream_refresh_interval
        self._reconnect_base_delay = reconnect_base_delay
        self._reconnect_max_delay = reconnect_max_delay
        self._delivery = delivery
        self._codec: "Codec" = codec or _DefaultCodec()
        self._stream_key_fn = stream_key_fn
        self._explicit_streams = streams
        self._retention = retention
        self._retention_trim_interval = retention_trim_interval
        self._max_deliveries = max_deliveries
        self._on_dlq = on_dlq

        self._on_envelope: Optional[OnEnvelope] = None
        self._consumer_task: Optional[asyncio.Task[None]] = None
        self._sweeper_task: Optional[asyncio.Task[None]] = None
        self._broadcast_task: Optional[asyncio.Task[None]] = None
        self._retention_task: Optional[asyncio.Task[None]] = None
        self._running = False
        self._streams: set[str] = set()
        self._groups_ready: set[str] = set()
        #: Per-stream last-delivered id cursor (broadcast mode only).
        self._last_ids: dict[str, str] = {}
        self.logger = logging.getLogger("navigator_eventbus.backends.redis_streams")
        if self._retention is not None:
            self.logger.debug(
                "retention=%s set — maxlen=%s is ignored (time-based "
                "XTRIM MINID trimming owns retention instead)",
                self._retention, self._maxlen,
            )

    # ------------------------------------------------------------------
    # TransportBackend protocol
    # ------------------------------------------------------------------

    async def publish(self, envelope: EventEnvelope) -> None:
        """``XADD`` *envelope* to its topic-class stream (MAXLEN ~ trim).

        Args:
            envelope: The envelope to persist on the stream.
        """
        await self._ensure_connection()
        stream = self._stream_for(envelope.topic)
        # No consumer group in broadcast mode — group-less consumption has
        # no PEL, so joining a group here would create a permanently
        # unused one (FEAT-320 Module 1: "no PEL in group-less consumption").
        if self._delivery != "broadcast":
            await self._ensure_group(stream)
        trim_kwargs: dict[str, Any] = (
            {} if self._retention is not None
            else {"maxlen": self._maxlen, "approximate": True}
        )
        await self._redis.xadd(  # type: ignore[union-attr]
            stream,
            self._codec.encode(envelope),
            **trim_kwargs,
        )

    async def start_consumer(self, on_envelope: OnEnvelope) -> None:
        """Spawn this backend's consumption task(s) for the active *delivery* mode.

        ``delivery="group"`` (default): the ``XREADGROUP`` consumer loop
        plus the ``XAUTOCLAIM`` sweeper (unchanged from today).
        ``delivery="broadcast"``: a group-less ``XREAD`` reader loop only —
        no sweeper, since there is no PEL to reclaim.

        When ``streams=`` was supplied at construction, this explicit set
        is seeded ONCE here (bypassing ``SCAN``-based discovery entirely
        for the lifetime of this instance).

        Args:
            on_envelope: Awaited for each envelope (deduped in group mode).
        """
        self._on_envelope = on_envelope
        self._running = True
        if self._stream_key_fn is not None and not self._explicit_streams:
            self.logger.warning(
                "stream_key_fn= is set without streams= — SCAN-based "
                "discovery (match=%s*) may not find custom-named streams; "
                "pass streams=[...] explicitly if your naming scheme does "
                "not share the <stream_prefix>* shape.",
                self.stream_prefix,
            )
        if self._explicit_streams:
            await self._ensure_connection()
            if self._delivery == "broadcast":
                for name in self._explicit_streams:
                    if name not in self._streams:
                        self._streams.add(name)
                        self._last_ids[name] = await self._resolve_tail_id(name)
            else:
                for name in self._explicit_streams:
                    await self._ensure_group(name)
        if self._delivery == "broadcast":
            self._broadcast_task = asyncio.create_task(
                self._run_broadcast(), name="bus-redis-streams-broadcast"
            )
        else:
            self._consumer_task = asyncio.create_task(
                self._run_consumer(), name="bus-redis-streams-consumer"
            )
            self._sweeper_task = asyncio.create_task(
                self._run_sweeper(), name="bus-redis-streams-sweeper"
            )
        if self._retention is not None:
            self._retention_task = asyncio.create_task(
                self._run_retention_trimmer(), name="bus-redis-streams-retention"
            )

    async def close(self) -> None:
        """Stop whichever task(s) are running for the active mode and release
        owned connections."""
        self._running = False
        for task in (
            self._consumer_task,
            self._sweeper_task,
            self._broadcast_task,
            self._retention_task,
        ):
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._consumer_task = None
        self._sweeper_task = None
        self._broadcast_task = None
        self._retention_task = None
        if self._redis is not None and self._client is None:
            try:
                await self._redis.close()
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("redis close error: %s", exc)
        self._redis = None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _stream_for(self, topic: str) -> str:
        """Stream key for *topic*.

        Uses ``stream_key_fn=`` (FEAT-320 Module 2) when supplied;
        otherwise the spec-fixed ``<stream_prefix><topic-class>`` sharding
        (unchanged default).
        """
        if self._stream_key_fn is not None:
            return self._stream_key_fn(topic)
        return f"{self.stream_prefix}{topic.split('.', 1)[0]}"

    async def _ensure_connection(self) -> None:
        """(Re)build the redis client if needed."""
        if self._redis is not None:
            return
        if self._client is not None:
            self._redis = self._client
            return
        kwargs: dict[str, Any] = {"decode_responses": True}
        if self._password is not None:
            kwargs["password"] = self._password
        if self._username is not None:
            kwargs["username"] = self._username
        self._redis = await aioredis.from_url(self.redis_url, **kwargs)

    async def _ensure_group(self, stream: str) -> None:
        """Idempotently create the consumer group (MKSTREAM) for *stream*."""
        if stream in self._groups_ready:
            return
        try:
            await self._redis.xgroup_create(  # type: ignore[union-attr]
                stream, self._group, id="0", mkstream=True
            )
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._groups_ready.add(stream)
        self._streams.add(stream)

    async def _refresh_streams(self) -> None:
        """Discover ``<stream_prefix>*`` streams and join their groups.

        No-op when ``streams=`` (FEAT-320 Module 2) was supplied — the
        explicit set is seeded once in :meth:`start_consumer`, bypassing
        ``SCAN``-based discovery entirely.
        """
        if self._explicit_streams is not None:
            return
        async for key in self._redis.scan_iter(match=f"{self.stream_prefix}*"):  # type: ignore[union-attr]
            name = key.decode() if isinstance(key, bytes) else key
            if name not in self._streams:
                await self._ensure_group(name)

    async def _refresh_streams_broadcast(self) -> None:
        """Discover ``<stream_prefix>*`` streams for broadcast mode.

        Group-less counterpart of :meth:`_refresh_streams`: no
        ``xgroup_create``/PEL involved. Newly discovered streams start
        their cursor at whatever :meth:`_resolve_tail_id` resolves to
        (effectively ``"$"``/tail, resolved ONCE — see that method's
        docstring for why) so pre-existing entries are never replayed.
        No-op when ``streams=`` was supplied (seeded once in
        :meth:`start_consumer`).
        """
        if self._explicit_streams is not None:
            return
        async for key in self._redis.scan_iter(match=f"{self.stream_prefix}*"):  # type: ignore[union-attr]
            name = key.decode() if isinstance(key, bytes) else key
            if name not in self._streams:
                self._streams.add(name)
                self._last_ids[name] = await self._resolve_tail_id(name)

    async def _resolve_tail_id(self, stream: str) -> str:
        """Resolve a concrete starting cursor for broadcast mode's ``XREAD``.

        Real Redis resolves the ``"$"`` sentinel FRESH at each ``XREAD``
        invocation — if this backend kept passing the literal ``"$"`` on
        every empty poll (no entries returned yet), an entry published in
        the gap between two non-matching polls would have its arrival
        "invisible": the NEXT poll's ``"$"`` would resolve past it, and it
        would be silently skipped forever (no PEL/redelivery in broadcast
        mode to fall back on). Resolving to a concrete id ONCE, right at
        discovery — via the id of the current last entry (``XREVRANGE ...
        COUNT 1``), or ``"0-0"`` for an empty stream — and then advancing
        it only as real entries are read (see :meth:`_run_broadcast`)
        closes that window: every subsequent poll targets a fixed,
        monotonically-advancing id instead of a sentinel re-evaluated
        against a moving target.
        """
        try:
            tail = await self._redis.xrevrange(stream, count=1)  # type: ignore[union-attr]
        except Exception as exc:  # noqa: BLE001 — fall back to the sentinel
            self.logger.debug(
                "XREVRANGE tail resolution failed for %s (%s) — falling "
                "back to \"$\"", stream, exc,
            )
            return "$"
        if tail:
            msg_id, _ = tail[0]
            return msg_id.decode() if isinstance(msg_id, bytes) else msg_id
        return "0-0"

    async def _run_consumer(self) -> None:
        """XREADGROUP loop with reconnect-and-backoff (degraded mode)."""
        delay = self._reconnect_base_delay
        last_refresh = 0.0
        while self._running:
            try:
                await self._ensure_connection()
                loop_now = asyncio.get_running_loop().time()
                if (
                    not self._streams
                    or loop_now - last_refresh >= self._stream_refresh_interval
                ):
                    await self._refresh_streams()
                    last_refresh = loop_now
                if not self._streams:
                    await asyncio.sleep(self._block_ms / 1000)
                    continue
                results = await self._redis.xreadgroup(  # type: ignore[union-attr]
                    self._group,
                    self._consumer,
                    {stream: ">" for stream in self._streams},
                    count=self._batch_count,
                    block=self._block_ms,
                )
                delay = self._reconnect_base_delay  # healthy — reset backoff
                for stream, messages in results or []:
                    stream_name = (
                        stream.decode() if isinstance(stream, bytes) else stream
                    )
                    for msg_id, fields in messages:
                        await self._handle_message(stream_name, msg_id, fields)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — degraded mode
                if not self._running:
                    return
                self.logger.warning(
                    "Streams consumer error (%s: %s) — reconnecting in "
                    "%.1fs; local dispatch continues",
                    type(exc).__name__, exc, delay,
                )
                if self._client is None:
                    self._redis = None
                self._groups_ready.clear()
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._reconnect_max_delay)

    async def _run_broadcast(self) -> None:
        """Group-less ``XREAD`` free-read loop (``delivery="broadcast"``).

        Mirrors :meth:`_run_consumer`'s reconnect-and-backoff shape, but
        every backend instance dispatches every entry it reads (no
        dedup-set check/mark, no ``XACK``, no sweeper — there is no PEL in
        group-less consumption).
        """
        delay = self._reconnect_base_delay
        last_refresh = 0.0
        while self._running:
            try:
                await self._ensure_connection()
                loop_now = asyncio.get_running_loop().time()
                if (
                    not self._streams
                    or loop_now - last_refresh >= self._stream_refresh_interval
                ):
                    await self._refresh_streams_broadcast()
                    last_refresh = loop_now
                if not self._streams:
                    await asyncio.sleep(self._block_ms / 1000)
                    continue
                results = await self._redis.xread(  # type: ignore[union-attr]
                    {stream: self._last_ids[stream] for stream in self._streams},
                    count=self._batch_count,
                    block=self._block_ms,
                )
                delay = self._reconnect_base_delay  # healthy — reset backoff
                for stream, messages in results or []:
                    stream_name = (
                        stream.decode() if isinstance(stream, bytes) else stream
                    )
                    for msg_id, fields in messages:
                        self._last_ids[stream_name] = msg_id
                        await self._dispatch_broadcast_entry(stream_name, msg_id, fields)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — degraded mode
                if not self._running:
                    return
                self.logger.warning(
                    "Streams broadcast reader error (%s: %s) — reconnecting "
                    "in %.1fs; local dispatch continues",
                    type(exc).__name__, exc, delay,
                )
                if self._client is None:
                    self._redis = None
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._reconnect_max_delay)

    async def _dispatch_broadcast_entry(
        self, stream: str, msg_id: Any, fields: dict[str, Any]
    ) -> None:
        """Decode-and-dispatch one broadcast-mode entry.

        No dedup-set check/mark and no ``XACK`` — broadcast means every
        instance is SUPPOSED to see every entry; dedup collapsing does not
        apply here, and there is no PEL to acknowledge against.
        """
        envelope = await self._decode_or_ack(
            stream, msg_id, fields, context="broadcast entry", ack=False
        )
        if envelope is None:
            return
        try:
            await self._on_envelope(envelope)  # type: ignore[misc]
        except Exception:  # noqa: BLE001 — isolated, no redelivery mechanism
            self.logger.exception(
                "Broadcast consumer callback failed for %s", envelope.topic,
            )

    async def _run_sweeper(self) -> None:
        """Periodic XAUTOCLAIM pass reclaiming stale pending entries.

        When ``max_deliveries`` is set (FEAT-320 Module 4), entries about
        to be reclaimed are first checked against their Redis PEL delivery
        count (``XPENDING ... IDLE``'s ``times_delivered``) — those
        exceeding the cap are parked to ``on_dlq`` and ``XACK``ed
        (terminal, never reclaimed again) instead of being redispatched.
        """
        while self._running:
            try:
                await asyncio.sleep(self._autoclaim_interval)
                await self._ensure_connection()
                for stream in list(self._streams):
                    if self._max_deliveries is not None:
                        # No consumername= filter: XAUTOCLAIM reclaims stale
                        # entries regardless of their CURRENT owner (that is
                        # the whole point — the same poison entry crashing a
                        # DIFFERENT consumer each time it is reclaimed, per
                        # spec §2). Filtering XPENDING to this instance's
                        # own consumer name would only ever match entries
                        # ALREADY owned by self._consumer, silently missing
                        # every entry still attributed to some other
                        # stale/crashed consumer at the moment of this sweep.
                        pending = await self._redis.xpending_range(  # type: ignore[union-attr]
                            name=stream, groupname=self._group,
                            min="-", max="+", count=self._batch_count,
                            idle=self._min_idle_time_ms,
                        )
                        over_threshold = {
                            p["message_id"]: p["times_delivered"]
                            for p in pending
                            if p["times_delivered"] > self._max_deliveries
                        }
                    else:
                        over_threshold = {}

                    result = await self._redis.xautoclaim(  # type: ignore[union-attr]
                        stream,
                        self._group,
                        self._consumer,
                        min_idle_time=self._min_idle_time_ms,
                        start_id="0-0",
                        count=self._batch_count,
                    )
                    # redis-py returns [next_start, messages(, deleted_ids)]
                    messages = result[1] if result and len(result) > 1 else []
                    for msg_id, fields in messages:
                        if msg_id in over_threshold:
                            await self._park_to_dlq(
                                stream, msg_id, fields, over_threshold[msg_id]
                            )
                            continue
                        self.logger.info(
                            "XAUTOCLAIM reclaimed %s from %s", msg_id, stream
                        )
                        await self._handle_message(stream, msg_id, fields)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — degraded mode
                if not self._running:
                    return
                self.logger.warning("Streams sweeper error: %s", exc)

    async def _park_to_dlq(
        self, stream: str, msg_id: Any, fields: dict[str, Any], times_delivered: int
    ) -> None:
        """Terminal handling for an entry that exceeded ``max_deliveries``.

        Never redispatched through ``on_envelope`` (excluded from the
        normal dedup-check/mark path too — it is being parked, not
        processed), always ``XACK``ed so it is never reclaimed again.
        """
        envelope = await self._decode_or_ack(
            stream, msg_id, fields, context="over-threshold entry"
        )
        if envelope is None:
            return
        if self._on_dlq is not None:
            try:
                result = self._on_dlq(
                    envelope,
                    attempts=times_delivered,
                    error=RuntimeError(
                        f"exceeded max_deliveries={self._max_deliveries} "
                        "without ack"
                    ),
                    subscriber_id=f"{stream}:{self._group}",
                )
                if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
                    await result
            except Exception:  # noqa: BLE001 — DLQ failures isolated too
                self.logger.exception("on_dlq callback failed for %s", msg_id)
        await self._ack(stream, msg_id)

    async def _run_retention_trimmer(self) -> None:
        """Periodic ``XTRIM ... MINID`` pass — alternative to inline
        ``MAXLEN`` trimming, active only when ``retention=`` is set.

        Mirrors :meth:`_run_sweeper`'s periodic-task shape (sleep, guard on
        ``self._running``, isolate errors in degraded mode). Exactly-one-
        trimmer-per-stream remains the CALLER's responsibility (unchanged
        principle from today's ``maxlen`` trimming — no distributed
        locking is added here).
        """
        while self._running:
            try:
                await asyncio.sleep(self._retention_trim_interval)
                await self._ensure_connection()
                cutoff_ms = int(
                    (datetime.now(timezone.utc) - self._retention).timestamp()
                    * 1000
                )
                minid = f"{cutoff_ms}-0"
                for stream in list(self._streams):
                    await self._redis.xtrim(  # type: ignore[union-attr]
                        stream, minid=minid, approximate=True
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — degraded mode, same as sweeper
                if not self._running:
                    return
                self.logger.warning("Retention trimmer error: %s", exc)

    async def _handle_message(
        self, stream: str, msg_id: Any, fields: dict[str, Any]
    ) -> None:
        """Dedup-check, dispatch, and ACK one stream entry.

        Ordering is crash-safe (at-least-once):

        1. ``event_id`` already in the dedup set → skip dispatch, ``XACK``.
        2. Dispatch via ``on_envelope`` — with ``BusCore`` this runs the
           subscribers INLINE (handler failures are isolated inside the
           core: retry → DLQ, so they count as processed).
        3. ONLY on successful return: mark the dedup key, then ``XACK``.

        A crash anywhere before step 3 leaves the entry un-ACKed and the
        dedup key unset, so ``XAUTOCLAIM`` redelivers it for reprocessing.
        The check→set window means a reclaimed entry can occasionally be
        processed twice across consumers — that IS at-least-once;
        consumers must be idempotent.
        """
        envelope = await self._decode_or_ack(
            stream, msg_id, fields, context="stream entry"
        )
        if envelope is None:
            return

        dedup_key = f"{self.dedup_prefix}{envelope.event_id}"
        seen = await self._redis.get(dedup_key)  # type: ignore[union-attr]
        if seen:
            self.logger.debug(
                "Duplicate event %s skipped (dedup set)", envelope.event_id
            )
            await self._ack(stream, msg_id)
            return
        try:
            await self._on_envelope(envelope)  # type: ignore[misc]
        except Exception:  # noqa: BLE001 — leave pending for redelivery
            self.logger.exception(
                "Consumer callback failed for %s — entry stays pending "
                "(un-ACKed) for reclaim", envelope.topic,
            )
            return
        # Success: mark seen FIRST, then ACK — a crash between the two
        # redelivers a message the dedup check will skip (and re-ACK).
        try:
            await self._redis.set(dedup_key, "1", ex=self._dedup_ttl)  # type: ignore[union-attr]
        except Exception:  # noqa: BLE001 — dedup is best-effort
            self.logger.warning(
                "Dedup mark failed for %s (duplicates possible)",
                envelope.event_id,
            )
        await self._ack(stream, msg_id)

    async def _decode_or_ack(
        self,
        stream: str,
        msg_id: Any,
        fields: dict[str, Any],
        *,
        context: str,
        ack: bool = True,
    ) -> Optional[EventEnvelope]:
        """Decode *fields* via the active codec; on failure, log the
        poison entry and (optionally) ``XACK`` it away — shared by every
        dispatch path (:meth:`_handle_message`, :meth:`_dispatch_broadcast_entry`,
        :meth:`_park_to_dlq`) that needs "decode, or drop-and-move-on".

        Args:
            stream: Stream key (for logging/ack).
            msg_id: Entry id (for logging/ack).
            fields: Raw ``XADD`` field/value pairs to decode.
            context: Short label for the log message (e.g. ``"stream
                entry"``, ``"broadcast entry"``, ``"over-threshold entry"``).
            ack: Whether to ``XACK`` on decode failure. Broadcast mode has
                no PEL to acknowledge against, so callers there pass
                ``False``.

        Returns:
            The decoded envelope, or ``None`` if decoding failed (already
            logged, and ACKed if requested).
        """
        try:
            return self._codec.decode(fields)
        except Exception as exc:  # noqa: BLE001 — poison entries isolated
            self.logger.error(
                "Undecodable %s %s on %s dropped: %s",
                context, msg_id, stream, exc,
            )
            if ack:
                await self._ack(stream, msg_id)
            return None

    async def _ack(self, stream: str, msg_id: Any) -> None:
        """Best-effort XACK."""
        try:
            await self._redis.xack(stream, self._group, msg_id)  # type: ignore[union-attr]
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("XACK failed for %s on %s: %s", msg_id, stream, exc)
