# TASK-4575: KeyedQueue core — push, lease, apply, extend, expiry, pause

**Feature**: FEAT-713 — KeyedQueue (key-ordered, head-blocking work queue)
**Spec**: `sdd/specs/keyed-queue.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: L (4-8h)
**Depends-on**: TASK-4573, TASK-4574
**Assigned-to**: unassigned

---

## Context

Spec §2 (key table, **signal lifetime rules 1–5**, component diagram) and §3 Module 2, §7
"Atomicity decisions". This is the safety-critical half of `KeyedQueue`: at most one in-flight
message per key cluster-wide, outcomes that never move the head, a fenced `apply`, an atomic
`push`. Spec M2 is marked **not delegation-eligible** — the implementer must reason about each
atomic step. TASK-4576 adds the operational methods to the same file.

---

## Scope

- Create `src/navigator_eventbus/queues/keyed/store.py` with `KeyedQueue`, `KeyedLease`, `backoff_ms`.
- `KeyedQueue.__init__(config, client, *, store, on_dlq=None, consumer_name=None, clock=None)`;
  assert the keyed prefix is disjoint from `DEFAULT_QUEUE_PREFIX` and `"evb:stream:"` with
  `assert_prefixes_disjoint`.
- `push` — one `WATCH msg:<id>` / `MULTI` / `EXEC` (spec §7 "Push is one transaction"): if the hash
  is absent write every field + `ZADD key:<k>` + `SADD keys` + `ZADD all`; if present with the same
  key do nothing; present with another key → `ValueError`. Then always send one signal.
- `lease(*, wait_ms=0)` — return `None` immediately when paused (rule 4); else `QueueStore.receive`
  one signal, then `lease_key(key, receipt=...)`.
- `lease_key(key, *, receipt=None)` — the per-key half of the lease, also used by `drain` (TASK-4578):
  non-blocking `Lock` acquire (fail → ack the signal as a duplicate, rule 3, return `None`);
  head = `ZRANGE key:<k> 0 0`; empty → release, ack, `SREM keys` guarded (see notes), `None`;
  expired head (`created_ms + max_age_ms < now`, any state) → `expire_head`, retry with the next head;
  held key, waiting head before `wait_until_ms`, or head with `due_ms > now` → ensure a `delayed`
  entry at that deadline (held: none), release, ack the signal, `None`;
  otherwise `HINCRBY gen`, `state=leased`, return a `KeyedLease` that **keeps the signal receipt un-acked** (rule 1).
- `apply(lease, outcome)` — `WATCH lock:<k> msg:<id>`; lock value must equal our token and `msg.gen`
  must equal `lease.gen`, else `LeaseLost` (nothing changed). `MULTI`:
  - `ack`: `ZREM key:<k> id`, `DEL msg:<id>`, `ZREM all id`.
  - `retry`: `attempts+1`; if `max_attempts > 0` and the new count exceeds it → hand to `on_dlq` and remove like ack; else `due_ms = now + (delay_ms or backoff_ms(attempts, …))`, `ZADD delayed <k> due_ms`, `last_error = reason`, `state=pending`.
  - `wait`: `state=waiting`, `wait_until_ms = until_ms or now + effective_wait_safety_ms`, `ZADD delayed <k> wait_until_ms`.
  - `hold`: `HSET held <k> {reason, since_ms, message_id}`, `state=held`.
  After `EXEC`: release the lock, delete the holder's signal receipt (rule 1), and for `ack` re-signal if `ZCARD key:<k> > 0` else guarded `SREM keys`.
- `KeyedLease.extend(additional_ms=None)` — extend the lock (`replace_ttl=True`, default `lock_ttl_ms`) **and** the signal visibility (`QueueStore.change_visibility`) when a receipt exists; lock not owned → `LeaseLost`.
- `expire_head(key, message)` — log `"expiring"` at WARNING with key, id, age; `await on_dlq(envelope, attempts=…, error=…, subscriber_id=f"keyed:{name}")`; remove like ack in one `MULTI`. Returns `True` when expired.
- `pause(*, ttl_ms=None, reason="")`, `resume()`, `is_paused() -> str | None` (`paused` string key with `PX`).
- `backoff_ms(attempts, *, base_ms, max_ms, factor)` — capped exponential, attempts=0 → base.
- Unit tests in `tests/queues/keyed/test_store.py` for every row of spec §4 tagged M2 that this task's methods cover (list in checklist).

**NOT in scope**: `promote_due`, `recover`, `release_hold`, `peek`, `keys`, `describe` (TASK-4576);
the consumer loop (TASK-4577); `drain` (TASK-4578); package exports (TASK-4579).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/queues/keyed/store.py` | CREATE | `KeyedQueue` core, `KeyedLease`, `backoff_ms` |
| `tests/queues/keyed/test_store.py` | CREATE | core tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navconfig.logging import logging                                   # pattern: queues/store.py:27
from redis.asyncio.lock import Lock                                     # redis 5.2.1
from redis.exceptions import LockError, LockNotOwnedError, WatchError   # redis 5.2.1
from navigator_eventbus.envelope import EventEnvelope                   # envelope.py:106 (topic: str, payload: dict, event_id, ...)
from navigator_eventbus.queues.config import DEFAULT_QUEUE_PREFIX, QueueConfig, assert_prefixes_disjoint  # config.py:38, :95
from navigator_eventbus.queues.receipts import Receipt                  # receipts.py:69
from navigator_eventbus.queues.store import OnDLQ, QueueStore           # store.py:43, :87
from navigator_eventbus.queues.keyed.config import KeyedMessage, KeyedQueueConfig, LeaseLost, Outcome  # TASK-4573
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/queues/store.py
OnDLQ = Callable[..., Awaitable[None]]                      # :43 — called as on_dlq(envelope, *, attempts, error, subscriber_id)
class QueueStore:                                           # :87
    async def send(self, config: QueueConfig, envelope: EventEnvelope) -> str                     # :204
    async def receive(self, config, group, *, max_messages=1, wait_ms=0, visibility_ms=None) -> list[DeliveredMessage]  # :227
    async def delete(self, config: QueueConfig, receipt: Receipt) -> None                           # :444 — XACK; StaleReceipt if strict_ack and reclaimed
    async def change_visibility(self, config, receipt, visibility_ms: int) -> None                  # :491 — JUSTID, does not bump times_delivered
class DeliveredMessage:                                     # :67 — .message_id, .envelope, .receipt, .handle, .delivered
class StaleReceipt(Exception)                               # :666
# src/navigator_eventbus/queues/config.py
def assert_prefixes_disjoint(queue_prefix: str, bus_stream_prefix: str) -> None   # :38
# src/navigator_eventbus/dlq.py
class DLQHandler: async def on_dlq(self, envelope, *, attempts: int, error: BaseException, subscriber_id: str) -> None   # :185
# redis 5.2.1 asyncio Lock
Lock(redis, name, timeout=<seconds float>, blocking=False)  # acquire() -> bool; owned(); extend(additional_time, replace_ttl=False); release()
# Lock.local.token is bytes; with decode_responses=True GET returns str — compare decoded.
```

### Does NOT Exist
- ~~`QueueStore.nack()`~~ — use `change_visibility(..., 0)`.
- ~~`QueueStore.pause()`~~, ~~delayed delivery in the pull plane~~.
- ~~Lua scripts of our own~~ — forbidden (spec §8 resolved); only redis-py's `Lock` carries Lua.
- ~~`client.lock(...)` on the fake~~ — construct `Lock(client, name, ...)` directly.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/queues/keyed/store.py", "action": "CREATE"},
    {"path": "tests/queues/keyed/test_store.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/queues/store.py#QueueStore",
    "sym:src/navigator_eventbus/queues/store.py#QueueStore.send",
    "sym:src/navigator_eventbus/queues/store.py#QueueStore.receive",
    "sym:src/navigator_eventbus/queues/store.py#QueueStore.delete",
    "sym:src/navigator_eventbus/queues/store.py#QueueStore.change_visibility",
    "sym:src/navigator_eventbus/queues/config.py#assert_prefixes_disjoint",
    "sym:src/navigator_eventbus/envelope.py#EventEnvelope",
    "sym:src/navigator_eventbus/dlq.py#DLQHandler.on_dlq"
  ]
}
```

---

## Implementation Notes

- **Key names** live in one helper (`_Keys`) built from `prefix + name + ":"`: `key:<k>`, `msg:<id>`, `lock:<k>`, `delayed`, `held`, `keys`, `all`, `paused`, `recover`. Never format key names elsewhere.
- **Clock**: `clock()` returns seconds (same seam as `QueueStore`, store.py:120); `now_ms = int(clock() * 1000)`.
  Lock TTL in Redis is wall time; tests drive the fake's clock, which drives both.
- **Signal envelope**: `EventEnvelope(topic=f"keyed.{name}.signal", payload={"key": key})`; the signal queue must not mirror to the bus — log a WARNING at init if `config.signal.mirror_to_bus` is set.
- **Guarded `SREM keys`**: `WATCH key:<k>` → `ZCARD == 0` → `MULTI SREM EXEC`; on `WatchError` re-check once (a concurrent `push` added a member and will signal itself).
- **Releasing after EXEC**: `LockNotOwnedError` there means the lock expired after a successful fenced EXEC — log at WARNING and continue (the state change is already correct).
- **Acking a signal**: `QueueStore.delete` may raise `StaleReceipt` when the pull plane reclaimed it — log DEBUG and continue; a signal is a hint (rule 5).
- **Body**: stored as JSON text in the hash; `KeyedMessage` is rebuilt from `HGETALL` with type coercion (all hash values come back as `str`).
- Request-scoped methods, one instance shareable across tasks; never close the injected client (spec §7).

---

## Implementation Blueprint

### Steps (in order)
1. Write `_Keys`, `backoff_ms`, `KeyedLease`, `KeyedQueue.__init__` — *why*: every method below depends on them.
2. Implement `push` as one transaction — *why*: v0.2 forbids partially initialised messages (Codex finding 7).
3. Implement `lease` / `lease_key` following signal rules 1–4 literally — *why*: they are the liveness proof (spec §2).
4. Implement fenced `apply` — *why*: `owned()` then separate commands is a check-then-act race (spec §7).
5. Implement `KeyedLease.extend`, `expire_head`, `pause/resume/is_paused`.
6. Write the tests in the checklist.

```python
# src/navigator_eventbus/queues/keyed/store.py  (CREATE)
"""KeyedQueue core (FEAT-713): per-key FIFO, one in flight per key, fenced outcomes."""
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
from navigator_eventbus.queues.config import DEFAULT_QUEUE_PREFIX, assert_prefixes_disjoint
from navigator_eventbus.queues.keyed.config import KeyedMessage, KeyedQueueConfig, LeaseLost, Outcome
from navigator_eventbus.queues.receipts import Receipt
from navigator_eventbus.queues.store import OnDLQ, QueueStore

__all__ = ("KeyedLease", "KeyedQueue", "backoff_ms")


def backoff_ms(attempts: int, *, base_ms: int, max_ms: int, factor: float) -> int:
    """Capped exponential delay; attempts=0 → base_ms."""
    return int(min(max_ms, base_ms * (factor ** max(0, attempts))))


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
        return f"{self.base}key:{k}"

    def msg(self, message_id: str) -> str:
        return f"{self.base}msg:{message_id}"

    def lock(self, k: str) -> str:
        return f"{self.base}lock:{k}"


@dataclass
class KeyedLease:
    """The head of one key, leased under its lock. Apply exactly one outcome."""

    key: str
    message: KeyedMessage
    lock: Lock
    gen: int
    receipt: Optional[Receipt] = None
    _queue: "KeyedQueue" = field(repr=False, default=None)

    async def extend(self, additional_ms: Optional[int] = None) -> None:
        """Extend the lock and the signal visibility; raises LeaseLost."""
        # FILL IN: lock.extend(ms/1000, replace_ttl=True); LockNotOwnedError/LockError → LeaseLost;
        #          receipt → queue._store.change_visibility(queue.config.signal, receipt, ms + lock_ttl margin) — bounded by signal rule 2


class KeyedQueue:
    """Key-ordered, head-blocking queue over Redis; see FEAT-713 §2."""

    def __init__(self, config: KeyedQueueConfig, client: Any, *, store: QueueStore,
                 on_dlq: Optional[OnDLQ] = None, consumer_name: Optional[str] = None,
                 clock: Optional[Callable[[], float]] = None) -> None:
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
        # FILL IN: warn when config.signal.mirror_to_bus is set

    def now_ms(self) -> int:
        return int(self._clock() * 1000)

    async def push(self, key: str, body: dict, *, message_id: Optional[str] = None,
                   order: Optional[float] = None) -> str:
        """One WATCH/MULTI/EXEC; idempotent by message_id; then signal. See spec §3 M2."""
        # FILL IN — bounded by spec §7 "Push is one transaction" and AC "push with an existing message_id is a no-op apart from re-signalling"

    async def lease(self, *, wait_ms: int = 0) -> Optional[KeyedLease]:
        """Lease the head of one key, or None (paused, no signal, or nothing leasable)."""
        # FILL IN: is_paused() → None (no receive, rule 4); receive one signal from (config.signal, its only group); lease_key(key, receipt=...)

    async def lease_key(self, key: str, *, receipt: Optional[Receipt] = None) -> Optional[KeyedLease]:
        """Per-key half of the lease; also used by drain(). Rules 1–3; expiry; waiting/held/due checks."""
        # FILL IN — bounded by the v0.2 component diagram, step by step

    async def apply(self, lease: KeyedLease, outcome: Outcome) -> None:
        """Fenced by lock token + gen in WATCH/MULTI/EXEC; LeaseLost changes nothing."""
        # FILL IN — bounded by spec §7 "Outcome application is fenced"

    async def expire_head(self, key: str, message: KeyedMessage) -> bool:
        """When created_ms + max_age_ms < now: log 'expiring', await on_dlq(...), remove in one MULTI."""
        # FILL IN

    async def pause(self, *, ttl_ms: Optional[int] = None, reason: str = "") -> None:
        await self._redis.set(self._k.paused, reason or "paused", px=ttl_ms or self.config.pause_default_ttl_ms)

    async def resume(self) -> None:
        await self._redis.delete(self._k.paused)

    async def is_paused(self) -> Optional[str]:
        return await self._redis.get(self._k.paused)
```

### FILL IN checklist
- [ ] `push` transactional + idempotent + different-key `ValueError`; always signals.
- [ ] `lease` never calls `receive()` while paused.
- [ ] `lease_key`: lock-busy duplicate acked; held/waiting/not-due skip with `delayed` deadline; expiry before the state checks; `gen` bumped; receipt kept.
- [ ] `apply`: token + `gen` fence; four outcomes; `max_attempts` → DLQ; ack re-signal / guarded SREM; release + signal delete after EXEC.
- [ ] `KeyedLease.extend` extends lock and signal visibility.
- [ ] Tests (spec §4): `test_push_orders_by_score_not_arrival`, `test_one_in_flight_per_key`, `test_lock_expiry_makes_head_leasable_again`, `test_ack_resignals_when_key_not_empty`, `test_retry_keeps_head_and_delays_key` (leasable again once `due_ms` passes and a signal arrives — the `promote_due` part is TASK-4576), `test_wait_blocks_key_until_deadline_or_signal`, `test_wait_without_until_gets_safety_deadline`, `test_hold_blocks_key` (release half in TASK-4576), `test_max_age_expires_head_to_dlq_with_log`, `test_push_is_idempotent_by_message_id`, `test_max_attempts_parks_to_dlq`, `test_signal_kept_while_leased_and_recovered_after_crash`, `test_retry_delay_enforced_against_duplicate_signal`, `test_stale_apply_is_fenced`, `test_push_is_atomic`, `test_push_same_id_other_key_rejected`, `test_pause_blocks_lease_without_receive`.

---

## Acceptance Criteria

- [ ] At most one message per key is leased at a time (unit).
- [ ] A non-ack outcome never moves the head; a stale `apply` raises `LeaseLost` and changes nothing.
- [ ] `push` with an existing id changes nothing but re-signals.
- [ ] No `receive()` call while paused.
- [ ] `tests/queues/test_store.py` unchanged and green.

## Validation Commands
- `pytest tests/queues/keyed/test_store.py -q`
- `pytest tests/queues/test_store.py -q`

---

## Agent Instructions

Follow the diagram in spec §2 literally; if a step seems wrong, stop and write it in the
Completion Note instead of improvising. Use `self.logger`, never `print`.

## Completion Note

*(filled by the implementing agent)*
