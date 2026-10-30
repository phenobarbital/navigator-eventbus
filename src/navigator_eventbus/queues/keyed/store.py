"""KeyedQueue core (FEAT-713): per-key FIFO, one in flight per key, fenced outcomes.

The signal stream (a composed :class:`QueueStore`) only carries *hints* — "key
``k`` may have work". The per-key sorted set is the real order and the per-key
:class:`redis.asyncio.lock.Lock` is the real mutual exclusion. No Lua of our
own: every atomic step is a single command, a ``Lock`` call, or a
``WATCH``/``MULTI``/``EXEC`` transaction.
"""
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Optional
from uuid import uuid4

from navconfig.logging import logging
from redis.asyncio.lock import Lock
from redis.exceptions import LockError, LockNotOwnedError, WatchError

from navigator_eventbus.envelope import EventEnvelope
from navigator_eventbus.queues.config import (
    DEFAULT_QUEUE_PREFIX,
    assert_prefixes_disjoint,
)
from navigator_eventbus.queues.keyed.config import (
    KeyedMessage,
    KeyedQueueConfig,
    LeaseLost,
    Outcome,
)
from navigator_eventbus.queues.receipts import Receipt
from navigator_eventbus.queues.store import OnDLQ, QueueStore, StaleReceipt

__all__ = ("KeyedLease", "KeyedQueue", "backoff_ms")

#: How many signals one ``lease()`` call may burn through before giving up.
_MAX_SIGNALS_PER_LEASE = 16


def backoff_ms(attempts: int, *, base_ms: int, max_ms: int, factor: float) -> int:
    """Capped exponential delay; ``attempts=0`` gives ``base_ms``.

    Args:
        attempts: Failed attempts so far.
        base_ms: Delay for the first retry.
        max_ms: Upper bound.
        factor: Growth factor per attempt.

    Returns:
        The delay in milliseconds.
    """
    return int(min(max_ms, base_ms * (factor ** max(0, attempts))))


def _as_str(value: Any) -> str:
    """Normalise a Redis reply that may be ``bytes``."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _opt_int(value: Any) -> Optional[int]:
    """Hash value -> optional int (``""``/missing mean ``None``)."""
    if value is None or value == "":
        return None
    return int(float(_as_str(value)))


def _to_message(raw: dict[Any, Any], message_id: Optional[str] = None) -> KeyedMessage:
    """Rebuild a :class:`KeyedMessage` from ``HGETALL`` output (all ``str``)."""
    data = {_as_str(k): _as_str(v) for k, v in raw.items()}
    return KeyedMessage(
        message_id=message_id or data.get("message_id", ""),
        key=data["key"],
        body=json.loads(data.get("body", "{}")),
        order=float(data.get("order", "0")),
        created_ms=int(float(data.get("created_ms", "0"))),
        attempts=int(float(data.get("attempts", "0"))),
        state=data.get("state", "pending"),  # type: ignore[arg-type]
        last_error=data.get("last_error") or None,
        wait_until_ms=_opt_int(data.get("wait_until_ms")),
        due_ms=_opt_int(data.get("due_ms")),
        gen=int(float(data.get("gen", "0"))),
    )


class _Keys:
    """Every Redis key name of one keyed queue (spec §2 key table)."""

    def __init__(self, prefix: str, name: str) -> None:
        self.base = f"{prefix}{name}:"
        self.delayed = self.base + "delayed"
        self.held = self.base + "held"
        self.keys = self.base + "keys"
        self.all = self.base + "all"
        self.paused = self.base + "paused"
        self.recover = self.base + "recover"

    def key(self, k: str) -> str:
        """Sorted set of a key's pending message ids."""
        return f"{self.base}key:{k}"

    def msg(self, message_id: str) -> str:
        """Hash of one message."""
        return f"{self.base}msg:{message_id}"

    def lock(self, k: str) -> str:
        """Per-key in-flight lock."""
        return f"{self.base}lock:{k}"


@dataclass
class KeyedLease:
    """The head of one key, leased under its lock. Apply exactly one outcome."""

    key: str
    message: KeyedMessage
    lock: Lock
    gen: int
    receipt: Optional[Receipt] = None
    _queue: Optional["KeyedQueue"] = field(repr=False, default=None)

    async def extend(self, additional_ms: Optional[int] = None) -> None:
        """Extend the lock and the signal visibility together.

        Args:
            additional_ms: New TTL of the lock (replaces the remaining TTL).
                Defaults to ``lock_ttl_ms``.

        Raises:
            LeaseLost: The lock expired or changed owner.
        """
        queue = self._queue
        if queue is None:
            raise LeaseLost("lease is not bound to a queue")
        cfg = queue.config
        ms = additional_ms or cfg.lock_ttl_ms
        try:
            await self.lock.extend(ms / 1000.0, replace_ttl=True)
        except (LockNotOwnedError, LockError) as exc:
            raise LeaseLost(f"lock for key {self.key!r} lost: {exc}") from exc
        if self.receipt is not None:
            # Signal rule 2: the signal lease must outlive the lock.
            margin = cfg.signal.default_visibility_timeout_ms - cfg.lock_ttl_ms
            visibility = min(
                max(ms + margin, ms + 1), cfg.signal.max_visibility_timeout_ms
            )
            try:
                await queue._store.change_visibility(
                    cfg.signal, self.receipt, visibility
                )
            except Exception as exc:  # noqa: BLE001 — a hint, never fatal
                queue.logger.debug("signal visibility extend failed: %s", exc)


class KeyedQueue:
    """Key-ordered, head-blocking queue over Redis; see FEAT-713 §2.

    Args:
        config: Queue configuration.
        client: A ``redis.asyncio`` client (never closed here).
        store: The :class:`QueueStore` serving the signal stream.
        on_dlq: Duck-typed ``DLQHandler.on_dlq`` callback.
        consumer_name: Informational identity of this consumer.
        clock: Time source in **seconds** (test seam).
    """

    def __init__(
        self,
        config: KeyedQueueConfig,
        client: Any,
        *,
        store: QueueStore,
        on_dlq: Optional[OnDLQ] = None,
        consumer_name: Optional[str] = None,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        assert_prefixes_disjoint(config.prefix, "evb:stream:")
        assert_prefixes_disjoint(config.prefix, DEFAULT_QUEUE_PREFIX)
        self.config = config
        self._redis = client
        self._store = store
        self._on_dlq = on_dlq
        self._consumer = consumer_name or uuid4().hex
        self._clock = clock or time.time
        self._k = _Keys(config.prefix, config.name)
        self.logger = logging.getLogger("navigator_eventbus.queues.keyed")
        #: Heads expired through ``expire_head`` by this instance (drain reads it).
        self.expired_count = 0
        if config.signal.mirror_to_bus:
            self.logger.warning(
                "keyed queue %s: signal queue %s has mirror_to_bus=True; signals "
                "are internal hints and should not reach the bus",
                config.name,
                config.signal.name,
            )

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def now_ms(self) -> int:
        """Current time in milliseconds from the injected clock."""
        return int(self._clock() * 1000)

    def _new_lock(self, key: str) -> Lock:
        return Lock(
            self._redis,
            self._k.lock(key),
            timeout=self.config.lock_ttl_ms / 1000.0,
            blocking=False,
        )

    async def _signal(self, key: str) -> None:
        """Send one wake-up hint for *key*."""
        envelope = EventEnvelope(
            topic=f"keyed.{self.config.name}.signal", payload={"key": key}
        )
        await self._store.send(self.config.signal, envelope)

    async def _ack_signal(self, receipt: Optional[Receipt]) -> None:
        """Delete a signal; a stale receipt only means the pull plane reclaimed it."""
        if receipt is None:
            return
        try:
            await self._store.delete(self.config.signal, receipt)
        except StaleReceipt:
            self.logger.debug("signal receipt was stale (reclaimed) — ignoring")
        except Exception as exc:  # noqa: BLE001 — signals are hints
            self.logger.warning("could not delete signal: %s", exc)

    async def _release(self, lock: Lock) -> None:
        """Release a lock, tolerating one that already expired."""
        try:
            await lock.release()
        except (LockNotOwnedError, LockError) as exc:
            self.logger.warning("lock already gone at release: %s", exc)

    async def _handoff_dlq(self, message: KeyedMessage, error: str) -> None:
        """Best-effort ``on_dlq`` handoff; the body is logged if it fails."""
        if self._on_dlq is None:
            self.logger.error(
                "keyed queue %s: no on_dlq configured; dropping %s (%s) body=%s",
                self.config.name,
                message.message_id,
                error,
                json.dumps(message.body, default=str),
            )
            return
        envelope = EventEnvelope(
            topic=f"keyed.{self.config.name}.dead",
            payload=dict(message.body),
            metadata={
                "key": message.key,
                "message_id": message.message_id,
                "created_ms": message.created_ms,
            },
        )
        try:
            await self._on_dlq(
                envelope,
                attempts=message.attempts,
                error=RuntimeError(error),
                subscriber_id=f"keyed:{self.config.name}",
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.error(
                "keyed queue %s: on_dlq failed for %s (%s): %s body=%s",
                self.config.name,
                message.message_id,
                error,
                exc,
                json.dumps(message.body, default=str),
            )

    async def _drop_key_if_empty(self, key: str) -> bool:
        """Guarded ``SREM keys``: only while ``key:<k>`` is really empty."""
        for _ in range(2):
            pipe = self._redis.pipeline(transaction=True)
            try:
                async with pipe:
                    await pipe.watch(self._k.key(key))
                    if await pipe.zcard(self._k.key(key)) > 0:
                        return False
                    pipe.multi()
                    pipe.srem(self._k.keys, key)
                    pipe.zrem(self._k.delayed, key)
                    pipe.hdel(self._k.held, key)  # a hold on an empty key is an orphan
                    await pipe.execute()
                    return True
            except WatchError:
                continue  # a concurrent push added a member and signals itself
        return False

    # ------------------------------------------------------------------
    # push
    # ------------------------------------------------------------------

    async def push(
        self,
        key: str,
        body: dict,
        *,
        message_id: Optional[str] = None,
        order: Optional[float] = None,
    ) -> str:
        """Enqueue *body* under *key* and send a wake-up signal.

        One ``WATCH msg:<id>`` / ``MULTI`` / ``EXEC``: a crash before ``EXEC``
        writes nothing. Idempotent by ``message_id`` — an existing id changes
        nothing (but still re-signals). After an ack the hash is gone, so a
        later push of the same id creates a NEW message: callers re-pushing
        from their own record must make the handler check that record.

        Args:
            key: Ordering key.
            body: JSON-serialisable message body.
            message_id: Idempotency id (default: a fresh uuid).
            order: Sort score within the key (default: enqueue time in ms plus a
                sub-ms arrival counter, so same-ms pushes stay in order).

        Returns:
            The message id.

        Raises:
            ValueError: The id already exists under a different key.
        """
        mid = message_id or uuid4().hex
        now = self.now_ms()
        if order is not None:
            score = float(order)
        else:
            # Default order = enqueue time; a shared counter adds a sub-ms
            # fraction so same-millisecond pushes keep arrival order.
            seq = await self._redis.hincrby(self._k.base + "seq", "n", 1)
            score = float(now) + (int(seq) % 1000) / 1000.0
        msg_name = self._k.msg(mid)
        fields = {
            "message_id": mid,
            "key": key,
            "body": json.dumps(body, default=str),
            "order": repr(score),
            "created_ms": str(now),
            "attempts": "0",
            "state": "pending",
            "gen": "0",
        }
        for _ in range(8):
            pipe = self._redis.pipeline(transaction=True)
            try:
                async with pipe:
                    await pipe.watch(msg_name)
                    existing = await pipe.hget(msg_name, "key")
                    if existing is not None:
                        if _as_str(existing) != key:
                            raise ValueError(
                                f"message_id {mid!r} already exists under key "
                                f"{_as_str(existing)!r}, not {key!r}"
                            )
                    else:
                        pipe.multi()
                        pipe.hset(msg_name, mapping=fields)
                        pipe.zadd(self._k.key(key), {mid: score})
                        pipe.sadd(self._k.keys, key)
                        pipe.zadd(self._k.all, {mid: float(now)})
                        await pipe.execute()
                break
            except WatchError:
                continue
        else:  # pragma: no cover — 8 consecutive conflicts on one message id
            raise RuntimeError(f"push of {mid!r} lost 8 WATCH races")
        await self._signal(key)
        return mid

    # ------------------------------------------------------------------
    # lease
    # ------------------------------------------------------------------

    async def lease(self, *, wait_ms: int = 0) -> Optional[KeyedLease]:
        """Lease the head of one key, or ``None``.

        Never calls ``receive()`` while the queue is paused (signal rule 4),
        so a pause cannot burn ``max_receives``. The signal stays un-acked
        while the lease is held (rule 1).

        Args:
            wait_ms: Long-poll for the first signal.

        Returns:
            A :class:`KeyedLease`, or ``None`` (paused, no signal, or nothing
            leasable among the signals examined).
        """
        if await self.is_paused():
            return None
        group = self.config.signal.groups[0].name
        for attempt in range(_MAX_SIGNALS_PER_LEASE):
            delivered = await self._store.receive(
                self.config.signal,
                group,
                max_messages=1,
                wait_ms=wait_ms if attempt == 0 else 0,
            )
            if not delivered:
                return None
            item = delivered[0]
            key = item.envelope.payload.get("key")
            if not key:
                await self._ack_signal(item.receipt)
                continue
            lease = await self.lease_key(str(key), receipt=item.receipt)
            if lease is not None:
                return lease
        return None

    async def lease_key(
        self, key: str, *, receipt: Optional[Receipt] = None
    ) -> Optional[KeyedLease]:
        """Per-key half of the lease; also used by ``drain``.

        Non-blocking lock acquire (busy → the signal is a duplicate, ack it);
        then examine the head: expired → ``expire_head``; key held, head
        waiting or not yet due → arrange a ``delayed`` wake-up and skip.

        Args:
            key: The ordering key.
            receipt: The signal receipt to keep un-acked while leased.

        Returns:
            A lease, or ``None`` (the signal receipt is acked in that case).
        """
        lease, _ = await self._lease_key_detailed(key, receipt=receipt)
        return lease

    async def _lease_key_detailed(
        self,
        key: str,
        *,
        receipt: Optional[Receipt] = None,
        ignore_due: bool = False,
    ) -> tuple[Optional[KeyedLease], str]:
        """``lease_key`` plus the reason for a ``None`` (``drain`` needs it).

        Reasons: ``"leased"``, ``"busy"``, ``"empty"``, ``"held"``, ``"waiting"``.
        ``ignore_due`` skips the retry-delay (``due_ms``) check but never a
        ``wait`` deadline.
        """
        lock = self._new_lock(key)
        if not await lock.acquire(blocking=False):
            await self._ack_signal(receipt)
            return None, "busy"
        try:
            lease, reason = await self._lease_locked(key, lock, receipt, ignore_due)
        except BaseException:
            await self._release(lock)
            raise
        if lease is None:
            await self._release(lock)
            await self._ack_signal(receipt)
        return lease, reason

    async def _lease_locked(
        self,
        key: str,
        lock: Lock,
        receipt: Optional[Receipt],
        ignore_due: bool = False,
    ) -> tuple[Optional[KeyedLease], str]:
        zkey = self._k.key(key)
        while True:
            head = await self._redis.zrange(zkey, 0, 0)
            if not head:
                await self._drop_key_if_empty(key)
                return None, "empty"
            mid = _as_str(head[0])
            raw = await self._redis.hgetall(self._k.msg(mid))
            if not raw:  # dangling member (hash lost): repair and continue
                await self._redis.zrem(zkey, mid)
                await self._redis.zrem(self._k.all, mid)
                continue
            message = _to_message(raw, mid)
            now = self.now_ms()
            if await self.expire_head(key, message):
                continue
            if await self._redis.hget(self._k.held, key) is not None:
                return None, "held"
            deadline = 0
            if message.state == "waiting" and (message.wait_until_ms or 0) > now:
                deadline = message.wait_until_ms or 0
            if not ignore_due and (message.due_ms or 0) > now:
                deadline = max(deadline, message.due_ms or 0)
            if deadline:
                await self._redis.zadd(self._k.delayed, {key: float(deadline)})
                return None, "waiting"
            gen = await self._redis.hincrby(self._k.msg(mid), "gen", 1)
            await self._redis.hset(
                self._k.msg(mid),
                mapping={"state": "leased", "wait_until_ms": "", "due_ms": ""},
            )
            message = message.model_copy(
                update={
                    "gen": int(gen),
                    "state": "leased",
                    "wait_until_ms": None,
                    "due_ms": None,
                }
            )
            return (
                KeyedLease(
                    key=key, message=message, lock=lock, gen=int(gen),
                    receipt=receipt, _queue=self,
                ),
                "leased",
            )

    # ------------------------------------------------------------------
    # apply
    # ------------------------------------------------------------------

    async def apply(self, lease: KeyedLease, outcome: Outcome) -> None:
        """Apply one outcome, fenced by lock token + ``gen`` in ``WATCH``/``MULTI``.

        Args:
            lease: A lease obtained from :meth:`lease` / :meth:`lease_key`.
            outcome: ``ack`` / ``retry`` / ``wait`` / ``hold``.

        Raises:
            LeaseLost: The lock expired or changed owner, or the message
                generation moved. Nothing is changed in that case.
        """
        key = lease.key
        mid = lease.message.message_id
        lock_name = self._k.lock(key)
        msg_name = self._k.msg(mid)
        token = getattr(lease.lock.local, "token", None)
        dlq: Optional[tuple[KeyedMessage, str]] = None
        pipe = self._redis.pipeline(transaction=True)
        try:
            async with pipe:
                await pipe.watch(lock_name, msg_name)
                stored = await pipe.get(lock_name)
                if token is None or stored is None or _as_str(stored) != _as_str(token):
                    raise LeaseLost(f"lock for key {key!r} no longer owned")
                raw = await pipe.hgetall(msg_name)
                if not raw or int(float(_as_str(raw.get("gen", "0")))) != lease.gen:
                    raise LeaseLost(f"message {mid!r} generation changed")
                current = _to_message(raw, mid)
                now = self.now_ms()
                kind = outcome.kind
                attempts = current.attempts
                if kind == "retry":
                    attempts += 1
                    if (
                        self.config.max_attempts > 0
                        and attempts > self.config.max_attempts
                    ):
                        kind = "ack"
                        dlq = (
                            current.model_copy(update={"attempts": attempts}),
                            outcome.reason or "max_attempts exceeded",
                        )
                if dlq is not None:
                    await self._handoff_dlq(*dlq)
                pipe.multi()
                if kind == "ack":
                    pipe.zrem(self._k.key(key), mid)
                    pipe.delete(msg_name)
                    pipe.zrem(self._k.all, mid)
                elif kind == "retry":
                    delay = (
                        outcome.delay_ms
                        if outcome.delay_ms is not None
                        else backoff_ms(
                            current.attempts,
                            base_ms=self.config.retry_base_ms,
                            max_ms=self.config.retry_max_ms,
                            factor=self.config.retry_factor,
                        )
                    )
                    due = now + int(delay)
                    pipe.hset(
                        msg_name,
                        mapping={
                            "attempts": str(attempts),
                            "due_ms": str(due),
                            "last_error": outcome.reason or "",
                            "state": "pending",
                        },
                    )
                    pipe.zadd(self._k.delayed, {key: float(due)})
                elif kind == "wait":
                    until = (
                        outcome.until_ms
                        if outcome.until_ms is not None
                        else now + self.config.effective_wait_safety_ms
                    )
                    pipe.hset(
                        msg_name,
                        mapping={
                            "state": "waiting",
                            "wait_until_ms": str(until),
                            "last_error": outcome.reason or "",
                        },
                    )
                    pipe.zadd(self._k.delayed, {key: float(until)})
                else:  # hold
                    pipe.hset(
                        self._k.held,
                        mapping={
                            key: json.dumps(
                                {
                                    "reason": outcome.reason,
                                    "since_ms": now,
                                    "message_id": mid,
                                }
                            )
                        },
                    )
                    pipe.hset(
                        msg_name,
                        mapping={"state": "held", "last_error": outcome.reason or ""},
                    )
                await pipe.execute()
        except WatchError as exc:
            raise LeaseLost(f"lease on key {key!r} superseded during apply") from exc

        # EXEC succeeded: the state change is correct whatever happens below.
        await self._release(lease.lock)
        if kind == "ack":
            if await self._redis.zcard(self._k.key(key)) > 0:
                await self._signal(key)
            else:
                await self._drop_key_if_empty(key)
        await self._ack_signal(lease.receipt)

    # ------------------------------------------------------------------
    # expiry
    # ------------------------------------------------------------------

    async def expire_head(self, key: str, message: KeyedMessage) -> bool:
        """Expire *message* when ``created_ms + max_age_ms < now``.

        Logs ``"expiring"``, awaits ``on_dlq`` and removes the message in one
        ``MULTI``. The caller must hold the key's lock.

        Returns:
            ``True`` when the message was expired and removed.
        """
        max_age = self.config.max_age_ms
        if max_age is None:
            return False
        now = self.now_ms()
        if message.created_ms + max_age >= now:
            return False
        self.logger.warning(
            "keyed queue %s: expiring message %s of key %r (age %d ms > max_age_ms %d)",
            self.config.name,
            message.message_id,
            key,
            now - message.created_ms,
            max_age,
        )
        await self._handoff_dlq(message, "expired: max_age_ms exceeded")
        held_raw = await self._redis.hget(self._k.held, key)
        pipe = self._redis.pipeline(transaction=True)
        async with pipe:
            pipe.zrem(self._k.key(key), message.message_id)
            pipe.delete(self._k.msg(message.message_id))
            pipe.zrem(self._k.all, message.message_id)
            if held_raw is not None:
                try:
                    held_id = json.loads(_as_str(held_raw)).get("message_id")
                except ValueError:
                    held_id = None
                if held_id == message.message_id:
                    pipe.hdel(self._k.held, key)
            await pipe.execute()
        self.expired_count += 1
        return True

    # ------------------------------------------------------------------
    # pause
    # ------------------------------------------------------------------

    async def pause(self, *, ttl_ms: Optional[int] = None, reason: str = "") -> None:
        """Pause the queue for every consumer (shared through Redis).

        Args:
            ttl_ms: Pause duration (default ``pause_default_ttl_ms``).
            reason: Free-form text returned by :meth:`is_paused`.
        """
        await self._redis.set(
            self._k.paused,
            reason or "paused",
            px=ttl_ms or self.config.pause_default_ttl_ms,
        )

    async def resume(self) -> None:
        """Clear the pause."""
        await self._redis.delete(self._k.paused)

    async def is_paused(self) -> Optional[str]:
        """Return the pause reason, or ``None`` when not paused."""
        value = await self._redis.get(self._k.paused)
        return None if value is None else _as_str(value)

    # ------------------------------------------------------------------
    # operations (TASK-4576)
    # ------------------------------------------------------------------

    async def promote_due(self, *, limit: Optional[int] = None) -> int:
        """Move due keys from ``delayed`` to the signal stream.

        Safe from N consumers with no leader: ``ZREM`` is atomic per member, so
        only the caller whose ``ZREM`` returned 1 sends the signal.

        Args:
            limit: Max keys to promote (default ``promote_batch``).

        Returns:
            The number of signals this call sent.
        """
        batch = limit or self.config.promote_batch
        due = await self._redis.zrangebyscore(
            self._k.delayed, "-inf", self.now_ms(), start=0, num=batch
        )
        sent = 0
        for raw in due:
            key = _as_str(raw)
            if await self._redis.zrem(self._k.delayed, key) == 1:
                await self._signal(key)
                sent += 1
        return sent

    async def recover(self, *, force: bool = False) -> int:
        """Re-signal keys whose wake-up may have been lost (signal rule 5).

        Throttled by ``SET recover NX PX recover_interval_ms`` — a rate limit,
        not a leader: whoever wins runs one pass and owns nothing afterwards.
        A key is signalled when it is not locked, not held and has no
        ``delayed`` entry.

        Args:
            force: Skip the throttle.

        Returns:
            The number of signals sent.
        """
        if not force:
            won = await self._redis.set(
                self._k.recover,
                self._consumer,
                nx=True,
                px=self.config.recover_interval_ms,
            )
            if not won:
                return 0
        sent = 0
        for raw in sorted(await self._redis.smembers(self._k.keys)):
            key = _as_str(raw)
            if await self._redis.exists(self._k.lock(key)):
                continue
            if await self._redis.hget(self._k.held, key) is not None:
                continue
            if await self._redis.zscore(self._k.delayed, key) is not None:
                continue
            await self._signal(key)
            sent += 1
        return sent

    async def release_hold(self, key: str) -> bool:
        """Clear a hold; the head becomes pending and the key is signalled.

        Args:
            key: The held key.

        Returns:
            ``True`` when a hold existed and was released.
        """
        if await self._redis.hdel(self._k.held, key) == 0:
            return False
        head = await self._redis.zrange(self._k.key(key), 0, 0)
        if head and await self._redis.exists(self._k.msg(_as_str(head[0]))):
            await self._redis.hset(
                self._k.msg(_as_str(head[0])), mapping={"state": "pending"}
            )
        await self._signal(key)
        return True

    async def peek(self, key: str, *, limit: int = 10) -> list[KeyedMessage]:
        """Return the first *limit* messages of *key* in order (read-only)."""
        ids = await self._redis.zrange(self._k.key(key), 0, max(limit, 1) - 1)
        out: list[KeyedMessage] = []
        for raw in ids:
            mid = _as_str(raw)
            data = await self._redis.hgetall(self._k.msg(mid))
            if data:
                out.append(_to_message(data, mid))
        return out

    async def keys(self) -> list[str]:
        """Return the keys with at least one pending message, sorted."""
        return sorted(_as_str(k) for k in await self._redis.smembers(self._k.keys))

    async def describe(self) -> dict[str, Any]:
        """Operational snapshot; alarm on ``oldest_age_ms``.

        Returns:
            ``name``, ``keys``, ``messages``, ``delayed``, ``held``, ``paused``
            (reason or ``None``) and ``oldest_age_ms`` (``None`` when empty).
        """
        oldest = await self._redis.zrange(self._k.all, 0, 0, withscores=True)
        return {
            "name": self.config.name,
            "keys": await self._redis.scard(self._k.keys),
            "messages": await self._redis.zcard(self._k.all),
            "delayed": await self._redis.zcard(self._k.delayed),
            "held": await self._redis.hlen(self._k.held),
            "paused": await self.is_paused(),
            "oldest_age_ms": (
                max(0, self.now_ms() - int(oldest[0][1])) if oldest else None
            ),
        }

    async def drain(
        self,
        handler: Any,
        *,
        key_prefix: Optional[str] = None,
        concurrency: int = 10,
        call_timeout_ms: Optional[int] = None,
        stop_after_failures: int = 5,
        include_held: bool = False,
        ignore_pause: bool = False,
    ) -> Any:
        """Direct administrative flush; see :func:`navigator_eventbus.queues.keyed.drain.drain`."""
        from navigator_eventbus.queues.keyed.drain import drain as _drain

        return await _drain(
            self,
            handler,
            key_prefix=key_prefix,
            concurrency=concurrency,
            call_timeout_ms=call_timeout_ms,
            stop_after_failures=stop_after_failures,
            include_held=include_held,
            ignore_pause=ignore_pause,
        )
