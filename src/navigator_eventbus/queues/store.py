"""QueueStore — request-scoped Redis Streams operations for the pull plane.

This is deliberately **not** built on :class:`~navigator_eventbus.backends.
redis_streams.RedisStreamsBackend`. That class is organised around a
background consumer loop that pushes into a callback and **auto-ACKs** once
the callback returns — precisely the semantic a lease-based queue must not
have. It also cannot help here in another way: ``BusCore`` never re-raises
from a handler (isolation model B), so the backend's "callback raised → leave
pending → reclaim" path is dead whenever the bus is the consumer, and a
business-logic failure gets ACKed anyway.

What *is* shared is the wire format: this module imports ``Codec`` and
``DefaultCodec`` from the backend rather than re-implementing them, so a
message written by ``bus.publish()`` stays readable here and vice versa.

Concurrency note: every method is request-scoped and holds no cross-request
state, so a single instance is safe to share across concurrent handlers.
"""
from __future__ import annotations

import os
import socket
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Optional

from navconfig.logging import logging

from navigator_eventbus.backends.redis_streams import Codec, DefaultCodec
from navigator_eventbus.envelope import EventEnvelope
from navigator_eventbus.queues.config import (
    DEFAULT_QUEUE_PREFIX,
    QueueConfig,
    QueueRegistry,
    assert_prefixes_disjoint,
)
from navigator_eventbus.queues.receipts import Receipt, ReceiptCodec

__all__ = ("DeliveredMessage", "QueueStore", "default_consumer_name")

#: Callback invoked when a message exceeds its queue's ``max_receives``.
#: Duck-typed to match ``DLQHandler.on_dlq``.
OnDLQ = Callable[..., Awaitable[None]]


def default_consumer_name() -> str:
    """Stable per-replica consumer name.

    ``XREADGROUP`` demands a consumer name and an HTTP caller has no stable
    identity. One name **per replica** — never per request: Redis keeps a
    consumer registry per group indefinitely, so per-request names leak memory
    without buying anything. ``XACK`` is group-scoped, so a delete still works
    from any replica regardless of which one served the receive.

    Mirrors ``redis_streams._default_consumer_name``.
    """
    return f"http-{socket.gethostname()}-{os.getpid()}"


def _as_str(value: Any) -> str:
    """Normalise a Redis reply that may be ``bytes`` under ``decode_responses=False``."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


class DeliveredMessage:
    """One message handed out by :meth:`QueueStore.receive`."""

    __slots__ = ("message_id", "envelope", "receipt", "handle", "delivered")

    def __init__(
        self,
        message_id: str,
        envelope: EventEnvelope,
        receipt: Receipt,
        handle: str,
        delivered: int,
    ) -> None:
        self.message_id = message_id
        self.envelope = envelope
        self.receipt = receipt
        self.handle = handle
        self.delivered = delivered


class QueueStore:
    """Redis Streams operations backing the pull-queue HTTP API.

    Args:
        registry: The declared queues this store serves.
        client: A ``redis.asyncio`` client. **Never closed by this class** —
            an injected client may be shared with a bus backend, matching the
            convention of ``RedisStreamsBackend`` and ``CompositeBackend``.
        receipts: Codec used to mint and verify receipt handles.
        consumer_name: Overrides :func:`default_consumer_name`.
        codec: Wire codec. Defaults to the bus's, and should stay that way.
        on_dlq: Called for messages exceeding ``max_receives``.
        bus_stream_prefix: The prefix the bus backend SCANs, checked for
            overlap. See :func:`assert_prefixes_disjoint`.
        clock: Injectable time source in **seconds** (test seam).
    """

    def __init__(
        self,
        registry: QueueRegistry,
        client: Any,
        receipts: ReceiptCodec,
        *,
        consumer_name: Optional[str] = None,
        codec: Optional[Codec] = None,
        on_dlq: Optional[OnDLQ] = None,
        bus_stream_prefix: str = "evb:stream:",
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        assert_prefixes_disjoint(registry.queue_prefix or DEFAULT_QUEUE_PREFIX, bus_stream_prefix)
        self._registry = registry
        self._redis = client
        self._receipts = receipts
        self._consumer = consumer_name or default_consumer_name()
        self._codec: Codec = codec or DefaultCodec()
        self._on_dlq = on_dlq
        self._clock = clock or time.time
        self.logger = logging.getLogger("navigator_eventbus.queues.store")
        self._warn_if_bytes_mode()

    def _warn_if_bytes_mode(self) -> None:
        """Warn when the injected client returns bytes rather than str."""
        try:
            kwargs = self._redis.connection_pool.connection_kwargs
        except AttributeError:
            return
        if not kwargs.get("decode_responses", False):
            self.logger.warning(
                "QueueStore was given a Redis client with decode_responses=False. "
                "Ids and stream keys arrive as bytes; they are normalised, but "
                "decode_responses=True is what the bus backends use and is "
                "strongly preferred."
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def consumer_name(self) -> str:
        """The consumer name this replica reads as."""
        return self._consumer

    def now_ms(self) -> int:
        """Current time in milliseconds, from this store's clock."""
        return int(self._clock() * 1000)

    def _now_ms(self) -> int:
        return self.now_ms()

    def decode_receipt(self, handle: str, *, queue: str, group: str) -> Receipt:
        """Verify a receipt handle against this store's clock.

        Receipt minting and verification must share one clock, or handles
        would be judged expired against a different timeline than the one
        that set their expiry. Keeping both behind the store is what makes
        that impossible to get wrong.

        Raises:
            ReceiptError: With a stable ``reason`` slug.
        """
        return self._receipts.decode(
            handle, queue=queue, group=group, now_ms=self.now_ms()
        )

    def stream_for(self, queue: str) -> str:
        """Redis stream key backing *queue*."""
        return self._registry.stream_for(queue)

    async def _ensure_group(self, stream: str, group: str) -> None:
        """Create the consumer group if absent, tolerating BUSYGROUP.

        ``id="0"`` (not ``$``) so a group declared after messages already
        exist still sees them. Same shape as
        ``RedisStreamsBackend._ensure_group``.
        """
        try:
            await self._redis.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception as exc:  # noqa: BLE001 — BUSYGROUP is expected
            if "BUSYGROUP" not in str(exc):
                raise

    async def ensure_queue(self, config: QueueConfig) -> None:
        """Materialise a queue's stream and every declared consumer group."""
        stream = self.stream_for(config.name)
        for group in config.groups:
            await self._ensure_group(stream, group.name)

    async def ensure_all(self) -> None:
        """Materialise every declared queue. Called at API startup."""
        for config in self._registry.queues:
            await self.ensure_queue(config)

    # ------------------------------------------------------------------
    # Producer
    # ------------------------------------------------------------------

    async def send(self, config: QueueConfig, envelope: EventEnvelope) -> str:
        """Append *envelope* to the queue's stream.

        Args:
            config: The target queue.
            envelope: The message.

        Returns:
            The Redis entry id.
        """
        stream = self.stream_for(config.name)
        message_id = await self._redis.xadd(
            stream,
            self._codec.encode(envelope),
            maxlen=config.maxlen,
            approximate=True,
        )
        return _as_str(message_id)

    # ------------------------------------------------------------------
    # Consumer
    # ------------------------------------------------------------------

    async def receive(
        self,
        config: QueueConfig,
        group: str,
        *,
        max_messages: int = 1,
        wait_ms: int = 0,
        visibility_ms: Optional[int] = None,
    ) -> list[DeliveredMessage]:
        """Lease up to *max_messages* messages for *group*.

        The stage order is load-bearing:

        1. ``XPENDING`` to find entries past ``max_receives``.
        2. ``XAUTOCLAIM`` to reclaim leases that expired; over-cap entries go
           to the DLQ and are ACKed, never handed out again.
        3. ``XREADGROUP >`` for however many are still wanted.

        Reclaim must come **first** because ``XAUTOCLAIM`` cannot block: after
        a blocking ``XREADGROUP`` it would be delayed by the whole long-poll
        window on every call. And on a busy queue, reading new entries first
        would always fill the batch, so the pending list would never be
        scanned and expired leases would live forever.
        """
        stream = self.stream_for(config.name)
        threshold = config.max_visibility_timeout_ms
        lease_ms = (
            config.default_visibility_timeout_ms
            if visibility_ms is None
            else visibility_ms
        )
        await self._ensure_group(stream, group)

        messages = await self._reclaim(config, group, stream, threshold, max_messages)

        remaining = max_messages - len(messages)
        if remaining > 0:
            messages.extend(await self._read_new(stream, group, remaining, wait_ms))

        if not messages:
            return []

        await self._apply_visibility(
            stream, group, [mid for mid, _, _ in messages], threshold, lease_ms
        )
        expires_at = self._now_ms() + lease_ms

        delivered: list[DeliveredMessage] = []
        for message_id, fields, count in messages:
            try:
                envelope = self._codec.decode(fields)
            except Exception as exc:  # noqa: BLE001 — undecodable entry
                self.logger.error(
                    "Queue %s: entry %s could not be decoded (%s) — ACKing to "
                    "stop it blocking the group",
                    config.name,
                    message_id,
                    exc,
                )
                await self._ack(stream, group, message_id)
                continue
            receipt = Receipt(
                queue=config.name,
                group=group,
                stream=stream,
                message_id=message_id,
                consumer=self._consumer,
                delivered=count,
                expires_at_ms=expires_at,
            )
            delivered.append(
                DeliveredMessage(
                    message_id=message_id,
                    envelope=envelope,
                    receipt=receipt,
                    handle=self._receipts.encode(receipt),
                    delivered=count,
                )
            )
        return delivered

    async def _reclaim(
        self,
        config: QueueConfig,
        group: str,
        stream: str,
        threshold_ms: int,
        count: int,
    ) -> list[tuple[str, dict, int]]:
        """Reclaim expired leases, parking poison messages to the DLQ.

        Returns ``(message_id, fields, times_delivered)`` triples. The
        delivery count comes from the same ``XPENDING`` scan that finds
        over-cap entries, plus one for the reclaim itself — so no extra
        round-trip is needed to report it.
        """
        cap = config.receives_cap(group)
        try:
            pending = await self._redis.xpending_range(
                name=stream,
                groupname=group,
                min="-",
                max="+",
                count=count,
                idle=threshold_ms,
            )
        except Exception as exc:  # noqa: BLE001 — reclaim is best effort
            self.logger.warning("Queue %s: XPENDING failed: %s", config.name, exc)
            return []

        seen_counts = {
            _as_str(entry["message_id"]): int(entry["times_delivered"])
            for entry in pending or []
        }
        over_cap = {mid: n for mid, n in seen_counts.items() if n > cap}

        try:
            result = await self._redis.xautoclaim(
                stream,
                group,
                self._consumer,
                min_idle_time=threshold_ms,
                start_id="0-0",
                count=count,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Queue %s: XAUTOCLAIM failed: %s", config.name, exc)
            return []

        entries = result[1] if result and len(result) > 1 else []
        usable: list[tuple[str, dict, int]] = []
        for raw_id, fields in entries or []:
            message_id = _as_str(raw_id)
            if message_id in over_cap:
                await self._park_to_dlq(
                    config, group, stream, message_id, fields, over_cap[message_id]
                )
                continue
            # XAUTOCLAIM increments the counter, hence the +1.
            usable.append((message_id, fields, seen_counts.get(message_id, 0) + 1))
        return usable

    async def _read_new(
        self, stream: str, group: str, count: int, wait_ms: int
    ) -> list[tuple[str, dict, int]]:
        """Read never-before-delivered entries, optionally long-polling.

        ``block`` is only passed when a wait was actually requested: Redis
        treats ``BLOCK 0`` as *block forever*, which would pin a pool
        connection and an aiohttp handler indefinitely.
        """
        kwargs: dict[str, Any] = {"count": count}
        if wait_ms > 0:
            kwargs["block"] = wait_ms
        results = await self._redis.xreadgroup(
            group, self._consumer, {stream: ">"}, **kwargs
        )
        out: list[tuple[str, dict, int]] = []
        for _stream_name, entries in results or []:
            for raw_id, fields in entries or []:
                # Straight off '>' means never delivered before, so the count
                # is 1 by definition — no XPENDING round-trip required.
                out.append((_as_str(raw_id), fields, 1))
        return out

    async def _apply_visibility(
        self,
        stream: str,
        group: str,
        message_ids: Sequence[str],
        threshold_ms: int,
        lease_ms: int,
    ) -> None:
        """Offset each entry's idle clock so its lease ends in *lease_ms*.

        Redis has no per-message visibility timeout: it keeps one idle clock
        per pending entry, and the threshold lives on the *reader*
        (``XAUTOCLAIM min-idle-time``). So a shorter lease is expressed by
        ageing the message's clock — ``idle = threshold - lease`` means the
        entry crosses the threshold exactly *lease_ms* from now.

        ``justid=True`` is mandatory: without it ``XCLAIM`` increments
        ``times_delivered``, which would corrupt ``max_receives`` accounting.
        (Verified against Redis 7.0: JUSTID leaves the counter untouched.)

        Skipped entirely when the lease equals the threshold — the common
        path then costs no extra round-trip.

        Failure here is safe in one direction only, and that is the safe one:
        the entry keeps ``idle=0`` and becomes reclaimable after the full
        threshold instead of after the lease. Late redelivery, never early
        double-delivery.
        """
        if not message_ids or lease_ms >= threshold_ms:
            return
        try:
            await self._redis.xclaim(
                stream,
                group,
                self._consumer,
                min_idle_time=0,
                message_ids=list(message_ids),
                idle=max(0, threshold_ms - lease_ms),
                justid=True,
            )
        except Exception as exc:  # noqa: BLE001 — degrades to the longer lease
            self.logger.warning(
                "Queue stream %s: could not set visibility (%s) — entries will "
                "be reclaimable after the full threshold instead",
                stream,
                exc,
            )

    # ------------------------------------------------------------------
    # Delete / visibility
    # ------------------------------------------------------------------

    async def delete(self, config: QueueConfig, receipt: Receipt) -> None:
        """Acknowledge a message, removing it from the group's pending list.

        Only ``XACK``. ``XDEL`` is deliberately not used unless a queue
        explicitly opts in *and* has a single group: ``XDEL`` removes the
        entry from the **stream**, so any other group that has not read it yet
        would lose it permanently — a data-loss bug under the fan-out-across-
        groups guarantee.

        Raises:
            StaleReceipt: The lease generation was superseded and the queue
                has ``strict_ack`` enabled.
        """
        if config.strict_ack:
            await self._assert_lease_current(receipt)
        await self._ack(receipt.stream, receipt.group, receipt.message_id)
        if config.delete_entries:
            try:
                await self._redis.xdel(receipt.stream, receipt.message_id)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "Queue %s: XDEL failed for %s: %s",
                    config.name,
                    receipt.message_id,
                    exc,
                )

    async def _assert_lease_current(self, receipt: Receipt) -> None:
        """Raise when the message was reclaimed since the handle was minted."""
        try:
            pending = await self._redis.xpending_range(
                name=receipt.stream,
                groupname=receipt.group,
                min=receipt.message_id,
                max=receipt.message_id,
                count=1,
            )
        except Exception:  # noqa: BLE001 — cannot check, allow the ack
            return
        for entry in pending or []:
            if int(entry["times_delivered"]) > receipt.delivered:
                raise StaleReceipt(
                    f"message {receipt.message_id} was redelivered "
                    f"({entry['times_delivered']} > {receipt.delivered}); "
                    "another consumer may hold it now"
                )

    async def change_visibility(
        self, config: QueueConfig, receipt: Receipt, visibility_ms: int
    ) -> None:
        """Extend or shorten a message's lease.

        ``visibility_ms=0`` makes the entry immediately reclaimable — a real
        negative acknowledgement. The push ``TransportBackend`` contract
        cannot express that, which is the strongest single justification for
        this subpackage existing.
        """
        threshold = config.max_visibility_timeout_ms
        idle = threshold if visibility_ms <= 0 else max(0, threshold - visibility_ms)
        await self._redis.xclaim(
            receipt.stream,
            receipt.group,
            receipt.consumer,
            min_idle_time=0,
            message_ids=[receipt.message_id],
            idle=idle,
            justid=True,
        )

    async def _ack(self, stream: str, group: str, message_id: str) -> None:
        """Best-effort ``XACK``."""
        try:
            await self._redis.xack(stream, group, message_id)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("XACK failed for %s on %s: %s", message_id, stream, exc)

    async def _park_to_dlq(
        self,
        config: QueueConfig,
        group: str,
        stream: str,
        message_id: str,
        fields: dict,
        attempts: int,
    ) -> None:
        """Hand a poison message to the DLQ, then ACK it terminally."""
        if self._on_dlq is not None:
            try:
                envelope = self._codec.decode(fields)
                await self._on_dlq(
                    envelope,
                    attempts=attempts,
                    error=f"exceeded max_receives={config.receives_cap(group)}",
                    subscriber_id=f"{stream}:{group}",
                )
            except Exception as exc:  # noqa: BLE001 — DLQ must not block reclaim
                self.logger.error(
                    "Queue %s: DLQ handoff failed for %s: %s",
                    config.name,
                    message_id,
                    exc,
                )
        self.logger.warning(
            "Queue %s group %s: message %s exceeded max_receives (%d) — parked",
            config.name,
            group,
            message_id,
            attempts,
        )
        await self._ack(stream, group, message_id)

    # ------------------------------------------------------------------
    # Introspection / admin
    # ------------------------------------------------------------------

    async def describe(
        self, config: QueueConfig, group: Optional[str] = None
    ) -> dict[str, Any]:
        """Return approximate depth and in-flight counters."""
        stream = self.stream_for(config.name)
        try:
            length = int(await self._redis.xlen(stream))
        except Exception:  # noqa: BLE001
            length = 0
        info: dict[str, Any] = {
            "queue": config.name,
            "stream": stream,
            "stream_length": length,
            "groups": [g.name for g in config.groups],
            "group": group,
            "approximate_number_of_messages": None,
            "approximate_number_of_messages_not_visible": None,
            "approximate_number_of_messages_delayed": 0,
            "default_visibility_timeout_seconds": (
                config.default_visibility_timeout_ms // 1000
            ),
            "max_receives": config.max_receives,
        }
        if group is None:
            return info
        try:
            groups = await self._redis.xinfo_groups(stream)
        except Exception:  # noqa: BLE001
            return info
        for entry in groups or []:
            if _as_str(entry.get("name")) != group:
                continue
            pending = entry.get("pending")
            info["approximate_number_of_messages_not_visible"] = (
                int(pending) if pending is not None else None
            )
            lag = entry.get("lag")
            # `lag` is Redis 7.0+ and is None when entries were trimmed while
            # still unread — fall back to the raw stream length.
            info["approximate_number_of_messages"] = (
                int(lag) if lag is not None else length
            )
            break
        return info

    async def purge(self, config: QueueConfig) -> None:
        """Discard every message and reset every group.

        Trimming alone is not enough: it would leave pending-list entries
        pointing at deleted ids. Each group is destroyed and recreated at the
        stream tail.
        """
        stream = self.stream_for(config.name)
        await self._redis.xtrim(stream, maxlen=0, approximate=False)
        for group in config.groups:
            try:
                await self._redis.xgroup_destroy(stream, group.name)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "Queue %s: XGROUP DESTROY failed for %s: %s",
                    config.name,
                    group.name,
                    exc,
                )
            try:
                await self._redis.xgroup_create(
                    stream, group.name, id="$", mkstream=True
                )
            except Exception as exc:  # noqa: BLE001
                if "BUSYGROUP" not in str(exc):
                    raise

    async def prune_consumers(
        self, config: QueueConfig, *, idle_ms: int = 86_400_000
    ) -> int:
        """Remove idle, empty consumers from every group.

        Redis keeps a consumer registry per group forever. Replicas come and
        go, so without this the registry grows without bound. Only consumers
        with **no** pending entries are removed — one still holding a lease
        must keep its registration.

        Returns:
            The number of consumers removed.
        """
        stream = self.stream_for(config.name)
        removed = 0
        for group in config.groups:
            try:
                consumers = await self._redis.xinfo_consumers(stream, group.name)
            except Exception:  # noqa: BLE001 — group may not exist yet
                continue
            for consumer in consumers or []:
                if int(consumer.get("pending", 0)) != 0:
                    continue
                if int(consumer.get("idle", 0)) < idle_ms:
                    continue
                try:
                    await self._redis.xgroup_delconsumer(
                        stream, group.name, _as_str(consumer["name"])
                    )
                    removed += 1
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("XGROUP DELCONSUMER failed: %s", exc)
        return removed


class StaleReceipt(Exception):
    """The receipt's lease generation was superseded by a redelivery."""
