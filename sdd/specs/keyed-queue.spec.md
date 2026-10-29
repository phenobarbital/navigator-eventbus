---
# SDD flow type and base branch (FEAT-145).
type: feature
base_branch: main
projects: [navigator-eventbus]
tags: [queues, redis-streams, ordering, keyed-queue, workers, flush]
---

<!-- LANGUAGE: This document MUST be written entirely in English (proper nouns keep native spelling). -->

# Feature Specification: KeyedQueue — key-ordered, head-blocking work queue

**Feature ID**: FEAT-713
**Date**: 2026-10-29
**Author**: Jesus Lara (design) · Claude (spec)
**Status**: review
**Target version**: navigator-eventbus 0.4.0 — the release that fieldsync FEAT-830
(`sdd/proposals/workday-punch-outage-resilience.proposal.md` in the fieldsync repo) depends on.

---

## 1. Motivation & Business Requirements

### Problem Statement

The pull-queue plane (FEAT-432, `navigator_eventbus.queues`) gives at-least-once
delivery with leases, reclaim, `max_receives` → DLQ and purge. It deliberately
has **no ordering key, no delayed delivery and no pause** (`sdd/specs/pull-queues.spec.md`
§9). Three gaps block the first real consumer of the plane, fieldsync's
Workday punch delivery (FEAT-830), and will block any worker that talks to an
external system with strict per-entity ordering:

1. **Per-key FIFO with one message in flight per key.** A worker's Clock-Out
   must never be delivered before, or concurrently with, its Clock-In. A
   consumer group hands entries to whichever consumer asks; two consumers can
   hold two messages of the same key at once, and a redelivery lands at the
   stream tail.
2. **Head-blocking outcomes.** After a *retry later*, an *ambiguous* result
   or a *rejection*, the message must stay at the head of its key and block
   the key — not go back to the tail, not be dropped. Today the only
   non-ACK path is "lease expires, redeliver", which cannot express
   "wait at the head".
3. **Operations without a leader.** Backoff, pause while the downstream is
   down, a logged expiry, an administrative drain and recovery after Redis
   data loss must work with N identical consumers and no leader election
   across the cluster.

The design was decided during the FEAT-830 architecture review (2026-10-29):
Redis is the transport and the work-dispatch state; the *caller's* record
(for fieldsync, Postgres `punch_log`) stays the record of truth and the
recovery source. The queue never needs to be durable beyond Redis
persistence because `push()` is idempotent and callers re-push from their
record.

### Goals

- A `KeyedQueue` in `navigator_eventbus.queues.keyed` that orders messages
  per key by a caller-supplied score, leases **the head of a key**, and
  guarantees at most one in-flight message per key cluster-wide.
- Four consumer outcomes: `ack`, `retry(delay)`, `wait` (head stays, key
  blocked until re-signalled or until a deadline), `hold(reason)` (key
  parked for an operator).
- Delayed re-delivery through a sorted set promoted by **any** consumer;
  no scheduler, no leader.
- `pause(ttl)` / `resume()` shared through Redis; consumers never lease
  while paused.
- Message `max_age`: an expired head is logged and handed to the DLQ
  callback, never silently trimmed.
- `drain()`: a direct, leaderless administrative flush — `asyncio.gather`
  across keys with a semaphore, strictly sequential inside a key, bounded by
  a per-call timeout, fail-fast on consecutive failures. It uses the same
  per-key lock as the consumers, so it can never double-deliver.
- Idempotent `push(message_id=…)` so a caller can rebuild the queue from
  its own record after Redis data loss.
- A `KeyedConsumer` loop ("read, process, apply outcome, repeat until
  stopped") so workers do not re-implement the loop.

### Non-Goals (explicitly out of scope)

- Durability beyond Redis persistence. The caller's record is the recovery
  source; the library guarantees only idempotent re-push.
- Knowledge of any downstream (Workday, Odoo, …). Outcomes are generic;
  the caller maps its own error classes onto them.
- Exactly-once delivery. A lock can expire while a handler runs; the
  library then refuses to apply the stale outcome (`LeaseLost`) and the
  caller's idempotency rules decide. Same limit as FEAT-432 §9.
- An HTTP surface. In-process API only in this feature; `QueueAPI`-style
  routes can follow later.
- Changes to `QueueStore`, `RedisStreamsBackend` or `BusCore` semantics.
- Lua scripts. Every atomic step maps to a single Redis command or to
  `redis.asyncio.lock.Lock` (see §7).

---

## 2. Architectural Design

### Overview

`KeyedQueue` composes the existing `QueueStore` for the **signal stream**
(a consumer-group stream whose entries mean "key *k* may have work") and adds
per-key structures. The signal is a wake-up hint, tolerant to duplicates
and to loss: the per-key sorted set is the actual order, and the per-key
lock is the actual mutual exclusion.

Per queue `<q>` under prefix `evq:keyed:<q>:` (disjoint from the bus prefix
`evb:stream:` and the pull-queue prefix, checked with
`assert_prefixes_disjoint`):

| Key | Type | Content |
|---|---|---|
| `signal` (via `QueueStore`) | stream + group | `{key}` wake-ups, leased with the pull-queue visibility rules |
| `key:<k>` | zset | member = `message_id`, score = caller `order` (default: enqueue time ms) |
| `msg:<id>` | hash | `body` (JSON), `key`, `order`, `created_ms`, `attempts`, `state`, `last_error`, `wait_until_ms` |
| `lock:<k>` | string | `redis.asyncio.lock.Lock` token; TTL = `lock_ttl_ms` |
| `delayed` | zset | member = key, score = due ms (retry backoff, `wait` deadlines) |
| `held` | hash | key → JSON `{reason, since_ms, message_id}` |
| `keys` | set | keys with at least one pending message (drain / describe) |
| `all` | zset | member = `message_id`, score = `created_ms` (oldest-age metric, `max_age` scan) |
| `paused` | string | reason; `PX` = pause TTL |

### Component Diagram

```
producer ── push(key, body, message_id, order) ──► msg:<id> (HSETNX) · ZADD key:<k> · SADD keys · ZADD all
                                                  └─► QueueStore.send(signal{key})            (hint)

KeyedConsumer × N (any pod, no leader)
  loop:  promote_due()            ZRANGEBYSCORE delayed ≤ now → ZREM (winner only) → send(signal{key})
         lease()                  QueueStore.receive(signal) → paused? requeue signal with visibility
                                  → Lock(lock:<k>).acquire(blocking=False) → head = ZRANGE key:<k> 0 0
                                  → head expired (max_age)? on_dlq + remove, log, next
                                  → head waiting (wait_until > now) / key held? release, ack signal, next
                                  → return KeyedLease(key, message, lock)
         outcome = handler(msg)   bounded by asyncio.wait_for(call_timeout_ms)
         apply:  ack   → ZREM head · DEL msg · ZREM all · re-signal if ZCARD>0 else SREM keys · unlock
                 retry → attempts+1 · ZADD delayed key=now+delay · unlock      (head stays)
                 wait  → state=waiting · wait_until · ZADD delayed (deadline) · unlock (head stays)
                 hold  → HSET held key · unlock                               (head stays)
         every path: QueueStore.delete(signal receipt)

drain(handler)                    gather(drain_key(k) for k in keys ∪ delayed, sem) — same Lock per key;
                                  sequential inside a key; fail-fast after N consecutive retry outcomes
```

### Integration Points

| Existing Component | Integration Type | Notes |
|---|---|---|
| `navigator_eventbus.queues.store.QueueStore` | composes | signal stream: `send`, `receive`, `delete`, `change_visibility`, reclaim and `max_receives` for signals |
| `navigator_eventbus.queues.config.QueueConfig` / `QueueRegistry` | uses | the signal queue is a regular pull queue with one group |
| `navigator_eventbus.queues.config.assert_prefixes_disjoint` | uses | keyed prefix vs bus / pull prefixes |
| `navigator_eventbus.dlq.DLQHandler.on_dlq` | duck-typed callback | `max_age` expiry and `max_attempts` hand the body to `on_dlq(envelope, attempts=…, error=…, subscriber_id=…)` |
| `navigator_eventbus.envelope.EventEnvelope` | uses | the DLQ handoff wraps the body in an envelope; the signal entry is an envelope |
| `redis.asyncio.lock.Lock` (redis-py 5.2.1) | uses | per-key lock: `acquire(blocking=False)`, `extend`, `release`, `owned` |
| `tests/queues/_fake_streams.FakeStreamsRedis` | extends | needs zset / hash / set / string commands and a lock-compatible `set(nx, px)` + `eval` shim |

### Data Models

```python
class KeyedQueueConfig(BaseModel):
    name: str
    signal: QueueConfig                      # one group; visibility = signal lease
    lock_ttl_ms: int = 60_000                # per-key in-flight lease
    call_timeout_ms: int = 30_000            # KeyedConsumer / drain handler bound
    max_attempts: int = 0                    # 0 = unlimited retries (caller's deadline rules)
    max_age_ms: Optional[int] = None         # None = never expires
    retry_base_ms: int = 30_000
    retry_max_ms: int = 900_000
    retry_factor: float = 2.0
    promote_batch: int = 100
    pause_default_ttl_ms: int = 60_000
    prefix: str = "evq:keyed:"

class KeyedMessage(BaseModel):
    message_id: str
    key: str
    body: dict[str, Any]
    order: float
    created_ms: int
    attempts: int
    state: Literal["pending", "leased", "waiting", "held"]
    last_error: Optional[str]
    wait_until_ms: Optional[int]

class Outcome(BaseModel):                   # what a handler returns
    kind: Literal["ack", "retry", "wait", "hold"]
    delay_ms: Optional[int] = None          # retry: None → backoff(attempts)
    until_ms: Optional[int] = None          # wait: re-check deadline (None → only re-signal)
    reason: Optional[str] = None            # retry / wait / hold

class DrainReport(BaseModel):
    keys_seen: int
    acked: int
    retried: int
    waited: int
    held: int
    busy: int                               # keys locked by a live consumer, skipped
    expired: int
    stopped_early: bool                     # fail-fast tripped
    elapsed_ms: int
```

### New Public Interfaces

```python
class KeyedQueue:
    async def push(self, key: str, body: dict, *, message_id: str | None = None,
                   order: float | None = None) -> str: ...
    async def lease(self, *, wait_ms: int = 0) -> KeyedLease | None: ...
    async def apply(self, lease: KeyedLease, outcome: Outcome) -> None: ...
    async def promote_due(self, *, limit: int | None = None) -> int: ...
    async def pause(self, *, ttl_ms: int | None = None, reason: str = "") -> None: ...
    async def resume(self) -> None: ...
    async def is_paused(self) -> str | None: ...
    async def release_hold(self, key: str) -> bool: ...
    async def peek(self, key: str, *, limit: int = 10) -> list[KeyedMessage]: ...
    async def keys(self) -> list[str]: ...
    async def describe(self) -> dict[str, Any]: ...
    async def drain(self, handler, *, concurrency: int = 10, call_timeout_ms: int | None = None,
                    stop_after_failures: int = 5, include_held: bool = False,
                    ignore_pause: bool = False) -> DrainReport: ...

class KeyedLease:
    key: str
    message: KeyedMessage
    async def extend(self, additional_ms: int | None = None) -> None: ...   # raises LeaseLost

class KeyedConsumer:
    async def run(self, handler, *, stop: asyncio.Event) -> None: ...
```

---

## 3. Module Breakdown

#### Delegation-eligible modules

| Module | Eligible? | Decided patterns / exact contracts | Why not (if no) |
|---|---|---|---|
| M1: config & models | yes | Pydantic, `extra="forbid"`, fields fixed in §2 | — |
| M2: KeyedQueue store | no | — | atomicity choices per method (§7) need the thinking model |
| M3: KeyedConsumer | yes | loop in §2 diagram; outcome application is M2's `apply` | — |
| M4: drain | yes | §2 diagram; same lock; fail-fast counter; `DrainReport` | — |
| M5: fake Redis + tests | yes | command surface listed in §4 | — |
| M6: exports, CHANGELOG, README | yes | lazy-map pattern already in `queues/__init__.py` | — |

### Module 1: Config and models
- **Path**: `src/navigator_eventbus/queues/keyed/config.py`
- **Responsibility**: `KeyedQueueConfig`, `KeyedMessage`, `Outcome`, `DrainReport`, `LeaseLost`, `KeyHeld`.
- **Depends on**: `navigator_eventbus.queues.config.QueueConfig`.
- **Interface Skeleton**:
  ```python
  # src/navigator_eventbus/queues/keyed/config.py  (new)
  class KeyedQueueConfig(BaseModel):
      """One keyed queue; `signal` is the embedded pull queue (exactly one group)."""
      @field_validator("signal")
      def _single_group(cls, value: QueueConfig) -> QueueConfig:
          """Raises ValueError when the signal queue declares more than one group."""
  class LeaseLost(Exception):
      """The per-key lock expired or changed owner before the outcome was applied."""
  ```

### Module 2: KeyedQueue store
- **Path**: `src/navigator_eventbus/queues/keyed/store.py`
- **Responsibility**: every Redis operation in §2 (push, lease, apply, promote_due, pause, hold, describe, expiry).
- **Depends on**: M1, `QueueStore` (`store.py:87`), `redis.asyncio.lock.Lock`.
- **Interface Skeleton**:
  ```python
  # src/navigator_eventbus/queues/keyed/store.py  (new)
  class KeyedQueue:
      """Key-ordered, head-blocking queue over Redis; see FEAT-713 §2."""
      def __init__(self, config: KeyedQueueConfig, client: Any, *, store: QueueStore,
                   on_dlq: Optional[OnDLQ] = None, consumer_name: Optional[str] = None,
                   clock: Optional[Callable[[], float]] = None) -> None: ...
      async def push(self, key: str, body: dict, *, message_id: Optional[str] = None,
                     order: Optional[float] = None) -> str:
          """HSETNX msg → ZADD key → SADD keys → ZADD all → signal. Returns the id.
          Idempotent: an existing message_id changes nothing and still re-signals."""
      async def lease(self, *, wait_ms: int = 0) -> Optional[KeyedLease]:
          """Lease the head of one key, or None. Never returns a message of a key
          that is locked, held, waiting (before its deadline) or paused."""
      async def apply(self, lease: KeyedLease, outcome: Outcome) -> None:
          """Apply ack/retry/wait/hold. Raises LeaseLost if the lock is not owned."""
      async def promote_due(self, *, limit: Optional[int] = None) -> int:
          """Move due keys from `delayed` to the signal stream. Safe to call from N consumers:
          only the consumer whose ZREM returned 1 sends the signal."""
      async def expire_head(self, key: str, message: KeyedMessage) -> bool:
          """When created_ms + max_age_ms < now: log 'expiring', on_dlq(...), remove. Returns True if expired."""
  ```

### Module 3: KeyedConsumer
- **Path**: `src/navigator_eventbus/queues/keyed/consumer.py`
- **Responsibility**: the worker loop; backoff computation; `wait_for` around the handler; lock extension for long handlers.
- **Depends on**: M1, M2.
- **Interface Skeleton**:
  ```python
  # src/navigator_eventbus/queues/keyed/consumer.py  (new)
  Handler = Callable[[KeyedMessage], Awaitable[Outcome]]
  def backoff_ms(attempts: int, *, base_ms: int, max_ms: int, factor: float) -> int:
      """Capped exponential delay; attempts=0 → base_ms."""
  class KeyedConsumer:
      """Run `handler` over leased heads until `stop` is set; exceptions become retry outcomes."""
      async def run(self, handler: Handler, *, stop: asyncio.Event) -> None: ...
      async def run_once(self, handler: Handler) -> Optional[Outcome]:
          """promote_due → lease → handler (wait_for) → apply. Returns the applied outcome or None."""
  ```

### Module 4: drain
- **Path**: `src/navigator_eventbus/queues/keyed/drain.py`
- **Responsibility**: the administrative flush — direct, concurrent across keys, sequential per key, same lock, fail-fast.
- **Depends on**: M1, M2.
- **Interface Skeleton**:
  ```python
  # src/navigator_eventbus/queues/keyed/drain.py  (new)
  async def drain(queue: KeyedQueue, handler: Handler, *, concurrency: int = 10,
                  call_timeout_ms: Optional[int] = None, stop_after_failures: int = 5,
                  include_held: bool = False, ignore_pause: bool = False) -> DrainReport:
      """gather(drain_key(k)) over keys ∪ delayed-keys under a Semaphore(concurrency).
      Ignores retry delays. A key locked by a live consumer counts as `busy` and is skipped.
      Stops launching new keys after `stop_after_failures` consecutive retry outcomes."""
  async def drain_key(queue: KeyedQueue, key: str, handler: Handler, sem: asyncio.Semaphore,
                      *, call_timeout_ms: int, counters: DrainReport) -> None:
      """Lock the key once; loop over heads until a non-ack outcome or an empty key; unlock."""
  ```

### Module 5: fake Redis extension and tests
- **Path**: `tests/queues/_fake_streams.py` (modify), `tests/queues/keyed/` (new)
- **Responsibility**: add `zadd/zrange/zrangebyscore/zrem/zcard/zscore`, `hset/hsetnx/hget/hgetall/hincrby/hdel/hlen`, `sadd/srem/smembers/scard`, `set(nx, px)/get/delete/pexpire/pttl`, and the `eval`/`register_script` shim `redis.asyncio.lock.Lock` uses for `release`/`extend` (token compare), to `FakeStreamsRedis`; the tests in §4.
- **Depends on**: M1–M4.

### Module 6: exports, changelog, README
- **Path**: `src/navigator_eventbus/queues/__init__.py` (modify `_LAZY_MAP`, `__all__`), `CHANGELOG.md` (`## [0.4.0] — Unreleased`), `README.md` (new "Keyed queues" subsection after "Pull queues (SQS-style)").
- **Depends on**: M1–M4.

---

## 4. Test Specification

### Unit Tests
| Test | Module | Description |
|---|---|---|
| `test_push_orders_by_score_not_arrival` | M2 | push order 12:00 then 09:00 for one key; `lease` returns 09:00 |
| `test_one_in_flight_per_key` | M2 | two consumers; second `lease` on the same key returns None or another key while the lock is held |
| `test_lock_expiry_makes_head_leasable_again` | M2 | advance clock past `lock_ttl_ms`; head leases again; stale `apply` raises `LeaseLost` |
| `test_ack_resignals_when_key_not_empty` | M2 | ack with a second message pending → one new signal; SREM keys when empty |
| `test_retry_keeps_head_and_delays_key` | M2 | retry → head unchanged, key in `delayed`, not leasable until `promote_due` after due |
| `test_wait_blocks_key_until_deadline_or_signal` | M2 | wait with `until_ms` → not leasable before; a new push to the key re-signals and the head is still the waiting message |
| `test_hold_blocks_key_release_hold_resumes` | M2 | hold → `lease` skips the key; `release_hold` → leasable |
| `test_pause_blocks_lease_and_requeues_signal` | M2 | paused → `lease` returns None, signal visibility extended, not consumed |
| `test_max_age_expires_head_to_dlq_with_log` | M2 | created older than `max_age_ms` → `on_dlq` called with attempts/error, message removed, log record "expiring" |
| `test_push_is_idempotent_by_message_id` | M2 | second push with same id → no duplicate member, no change to body/attempts |
| `test_promote_due_is_leaderless` | M2 | two promoters race on the same due key → exactly one signal |
| `test_max_attempts_parks_to_dlq` | M2 | `max_attempts=3` → fourth retry → `on_dlq`, head removed, key unblocked |
| `test_backoff_capped_exponential` | M3 | base 30 s, factor 2, max 900 s → 30, 60, 120 … 900 |
| `test_consumer_run_once_applies_outcome` | M3 | handler outcome applied; handler exception → retry with error text |
| `test_consumer_extends_lock_for_long_handler` | M3 | handler longer than `lock_ttl_ms` with extension → lock kept, `apply` succeeds |
| `test_drain_concurrent_across_keys_sequential_within` | M4 | 3 keys × 3 msgs, concurrency 2 → per-key order preserved, max 2 keys concurrently |
| `test_drain_skips_locked_key_as_busy` | M4 | a key locked by a consumer → `busy += 1`, never double-delivered |
| `test_drain_ignores_retry_delay` | M4 | delayed key drained immediately |
| `test_drain_fail_fast` | M4 | 5 consecutive retry outcomes → `stopped_early=True`, remaining keys untouched |
| `test_drain_respects_wait_and_hold` | M4 | a wait/hold outcome stops that key only |
| `test_exports_lazy` | M6 | `from navigator_eventbus.queues import KeyedQueue` resolves lazily, no redis import at package import |

### Integration Tests
| Test | Description |
|---|---|
| `test_keyed_queue_real_redis_ordering_and_lock` | marker `integration`, skipped without Redis (same gate as `tests/queues/test_integration.py:34-46`): two consumer tasks, 50 keys × 5 messages, assert per-key order and no concurrent lease per key |
| `test_rebuild_after_flushdb` | push 10, `FLUSHDB`, re-push the same ids from the caller's list → identical queue, no duplicates |

### Validation Contract

tests:
- tests/queues/keyed/test_store.py
- tests/queues/keyed/test_consumer.py
- tests/queues/keyed/test_drain.py
- tests/queues/keyed/test_exports.py
- tests/queues/test_store.py
- tests/queues/test_exports_and_feeder.py
markers: none

### Test Data / Fixtures
```python
@pytest.fixture
def keyed(fake_redis, clock) -> KeyedQueue:
    signal = QueueConfig(name="punches-signal", groups=[QueueGroupConfig(name="workers")],
                         default_visibility_timeout_ms=5_000, max_visibility_timeout_ms=5_000)
    store = QueueStore(QueueRegistry(queues=[signal]), fake_redis, ReceiptCodec(["k"]), clock=clock)
    return KeyedQueue(KeyedQueueConfig(name="punches", signal=signal, lock_ttl_ms=10_000),
                      fake_redis, store=store, clock=clock)
```

---

## 5. Acceptance Criteria

- [ ] All §4 unit tests pass (`pytest tests/queues -q`).
- [ ] Integration tests pass against a real Redis 7 (`pytest tests/queues -m integration`).
- [ ] At most one message per key is ever leased at a time across N consumers (unit + integration).
- [ ] A non-ack outcome never moves the head of its key; the key is blocked until the outcome's condition clears.
- [ ] `promote_due`, `pause`, `drain` and expiry work with N identical consumers and no leader.
- [ ] `push` with an existing `message_id` is a no-op apart from re-signalling.
- [ ] An expired head is logged and handed to `on_dlq`; nothing is removed silently.
- [ ] `drain` never delivers a key that a live consumer holds (`busy`), and never delivers out of order inside a key.
- [ ] No change to `QueueStore`, `RedisStreamsBackend`, `BusCore` behaviour; `tests/queues/test_store.py` unchanged and green.
- [ ] `CHANGELOG.md` 0.4.0 entry, README subsection, lazy exports.
- [ ] Release 0.4.0 tagged and published; fieldsync FEAT-830 pins `navigator-eventbus>=0.4.0`.

---

## 6. Codebase Contract

### Verified Imports
```python
from navigator_eventbus.queues.store import QueueStore, DeliveredMessage, default_consumer_name  # verified: queues/store.py:52,67,87
from navigator_eventbus.queues.config import QueueConfig, QueueGroupConfig, QueueRegistry, assert_prefixes_disjoint, DEFAULT_QUEUE_PREFIX  # verified: queues/config.py:67,95,209
from navigator_eventbus.queues.receipts import Receipt, ReceiptCodec  # verified: queues/receipts.py:69,95
from navigator_eventbus.envelope import EventEnvelope  # verified: queues/store.py:27 imports it
from navigator_eventbus.backends.redis_streams import Codec, DefaultCodec  # verified: queues/store.py:26
from redis.asyncio.lock import Lock  # verified: redis 5.2.1 in .venv; acquire/release/extend/owned/reacquire
```

### Existing Class Signatures
```python
# src/navigator_eventbus/queues/store.py
class QueueStore:
    def __init__(self, registry: QueueRegistry, client: Any, receipts: ReceiptCodec, *,
                 consumer_name=None, codec=None, on_dlq=None, bus_stream_prefix="evb:stream:", clock=None)  # line 104
    async def send(self, config: QueueConfig, envelope: EventEnvelope) -> str  # line 204
    async def receive(self, config: QueueConfig, group: str, *, max_messages=1, wait_ms=0, visibility_ms=None) -> list[DeliveredMessage]  # line 227
    async def delete(self, config: QueueConfig, receipt: Receipt) -> None  # line 444 (XACK only)
    async def change_visibility(self, config: QueueConfig, receipt: Receipt, visibility_ms: int) -> None  # line 491 (0 = immediate nack)
    async def _park_to_dlq(self, config, group, stream, message_id, fields, attempts) -> None  # line 520 (on_dlq duck-typing)
    async def describe(self, config: QueueConfig, group: Optional[str] = None) -> dict  # line 559
    async def purge(self, config: QueueConfig) -> None  # line 604
OnDLQ = Callable[..., Awaitable[None]]  # line 41; called as on_dlq(envelope, *, attempts, error, subscriber_id)

# src/navigator_eventbus/queues/config.py
class QueueConfig(BaseModel):  # line 95 — default_visibility_timeout_ms=30_000, max_visibility_timeout_ms=3_600_000, max_receives=5, maxlen=100_000, strict_ack=True

# src/navigator_eventbus/dlq.py
class DLQHandler:  # line 120
    async def on_dlq(self, envelope: EventEnvelope, *, attempts: int, error: BaseException, subscriber_id: str) -> None  # line 185
```

### Integration Points
| New Component | Connects To | Via | Verified At |
|---|---|---|---|
| `KeyedQueue.push` | `QueueStore.send()` | signal envelope `{key}` | `queues/store.py:204` |
| `KeyedQueue.lease` | `QueueStore.receive()` / `delete()` / `change_visibility()` | signal lease, ack, requeue while paused | `queues/store.py:227,444,491` |
| `KeyedQueue.expire_head` / `max_attempts` | `DLQHandler.on_dlq` | duck-typed `OnDLQ` | `dlq.py:185`, `queues/store.py:41` |
| `KeyedQueue` per-key lock | `redis.asyncio.lock.Lock` | `client.lock(name, timeout=lock_ttl_ms/1000, blocking=False)` | redis 5.2.1 |
| `navigator_eventbus.queues` package | `_LAZY_MAP` | lazy export of `KeyedQueue`, `KeyedConsumer`, `drain` | `queues/__init__.py:49` |

### Does NOT Exist (Anti-Hallucination)
- ~~`QueueStore.nack()`~~ — negative ack is `change_visibility(..., 0)` (`store.py:491`).
- ~~delayed delivery in the pull plane~~ — `approximate_number_of_messages_delayed` is always 0 (`queues/models.py:216-218`).
- ~~an ordering / partition key in `RedisStreamsBackend` or `QueueStore`~~ — none.
- ~~Lua scripts anywhere in `src/navigator_eventbus`~~ — none; this feature adds none either (the redis-py `Lock` carries its own).
- ~~`QueueStore.pause()` / `resume()`~~ — do not exist.
- ~~`FakeStreamsRedis.zadd` / `hset` / `set` / `eval`~~ — the fake has only stream, group and `xclaim` commands (`tests/queues/_fake_streams.py:94-308`); M5 adds the rest.

### Edit Sites (Blueprint Anchors)

Verified against: `279b0d8` (origin/main at spec time; ledger commit `c9c064e` on top).

| File | Action | Verbatim anchor line | Verified at | Occurrences |
|---|---|---|---|---|
| `src/navigator_eventbus/queues/keyed/__init__.py` | CREATE | — | — | — |
| `src/navigator_eventbus/queues/keyed/config.py` | CREATE | — | — | — |
| `src/navigator_eventbus/queues/keyed/store.py` | CREATE | — | — | — |
| `src/navigator_eventbus/queues/keyed/consumer.py` | CREATE | — | — | — |
| `src/navigator_eventbus/queues/keyed/drain.py` | CREATE | — | — | — |
| `src/navigator_eventbus/queues/__init__.py` | MODIFY | `_LAZY_MAP = {` | `queues/__init__.py:49` | 1 |
| `tests/queues/_fake_streams.py` | MODIFY | `class FakeStreamsRedis:` | `_fake_streams.py:64` | 1 |
| `tests/queues/keyed/test_store.py` · `test_consumer.py` · `test_drain.py` · `test_exports.py` | CREATE | — | — | — |
| `CHANGELOG.md` | MODIFY | `## [0.4.0] — Unreleased` | `CHANGELOG.md:6` | 1 |
| `README.md` | MODIFY | `### Pull queues (SQS-style)` | `README.md:104` | 1 |

---

## 7. Implementation Notes & Constraints

### Patterns to Follow
- Compose `QueueStore`; never subclass `RedisStreamsBackend` (its auto-ACK loop is the wrong semantic, `queues/store.py:1-16`).
- Request-scoped methods, no cross-request state, one instance shareable across tasks (same as `QueueStore`).
- The injected redis client is never closed by the queue.
- `navconfig.logging` logger named `navigator_eventbus.queues.keyed`.
- Pydantic models `extra="forbid"`; every public method documented with a Google-style docstring.

### Atomicity decisions (why no Lua)
- **Mutual exclusion per key**: `redis.asyncio.lock.Lock` already does `SET NX PX` with an owner token and token-checked `release`/`extend`. Never hand-roll `SET NX`.
- **Promotion is leaderless because `ZREM` is atomic per member**: every consumer may read the due keys, but only the one whose `ZREM delayed <key>` returns 1 sends the signal.
- **Idempotent push**: `HSETNX msg:<id> key <k>` decides creation; `ZADD`, `SADD`, `ZADD all` are idempotent by member, so a partially failed push is repaired by the next push of the same id.
- **Outcome application is guarded by the lock**: `apply` first checks `lock.owned()`; a stale lease raises `LeaseLost` and changes nothing. The handler's external side effect may already have happened; that is the caller's idempotency concern (FEAT-432 §9 "consumers must be idempotent" applies unchanged).
- **Signals are hints**: losing one costs at most one `promote_due`/re-push; duplicates cost one no-op lease. Never build an invariant on signal presence.

### Known Risks / Gotchas
- A handler longer than `lock_ttl_ms` without `KeyedLease.extend()` loses the key to another consumer. `KeyedConsumer` extends automatically at `lock_ttl_ms / 3`; direct `lease()` users must do it themselves. Documented loudly.
- `max_age` expiry is detected lazily (when a head is examined). A key nobody signals keeps its expired head until `drain`, `promote_due` or a new push touches it. `describe()` exposes `oldest_age_ms` from `all` so an operator can alarm on it.
- `drain` with high `concurrency` against a downstream that just recovered can overload it; `stop_after_failures` and the semaphore are the only guards. Callers choose the number.
- `pause` is global for the queue. Per-tenant pausing is a caller concern (use one queue per tenant, or hold keys).
- Redis data loss empties the queue silently. The contract is explicit: callers rebuild via idempotent `push` from their record at startup / in their flush.

### External Dependencies
| Package | Version | Reason |
|---|---|---|
| `redis` | `>=5` (already `[redis]` extra) | `redis.asyncio.lock.Lock`, zset / hash commands |

---

## 8. Open Questions

- [x] Lua or plain commands? — *Owner: Jesús* — Plain commands plus redis-py `Lock`; see §7.
- [ ] Q1 — *Owner: Jesús*: should `KeyedQueue` also expose HTTP routes (a `QueueAPI` sibling) in 0.4.0, or stay in-process until a non-Python consumer needs it? Recommendation: in-process only; fieldsync's worker is Python.
- [ ] Q2 — *Owner: Jesús*: default `max_attempts` — unlimited (0) so the caller's deadline rules apply, or a finite default like `max_receives=5`? Recommendation: 0; the pull plane's 5 is about poison messages, not about a downstream outage.
- [ ] Q3 — *Owner: Jesús*: `wait` without `until_ms` relies entirely on a later push to the same key re-signalling. Keep that, or always add a safety deadline (`lock_ttl_ms × 10`)? Recommendation: always add the safety deadline.

---

## 9. Design Research Cross-Check

> Status: completed over the accepted design (fieldsync FEAT-830 proposal review, 2026-10-29;
> Codex review transcript kept in that session's scratchpad, not in `sdd/state/`).

| # | Suggestion (kind) | Disposition | Reason | Landed in |
|---|---|---|---|---|
| S1 | Do not reimplement consumer-group bookkeeping that `QueueStore` already has (architecture) | CONFIRM | signal stream is a composed `QueueStore` | §2 Overview |
| S2 | Drop per-message "live signal" tracking and payload snapshots; use bounded retention plus due-key discovery (simplification) | CONFIRM | signals are hints; `promote_due` + `keys` set | §2, §7 |
| S3 | Use an ownership token / CAS for in-flight transitions, not status checks (risk) | CONFIRM | redis-py `Lock` token; `LeaseLost` on stale apply | §7 |
| S4 | Make flush a durable asynchronous job returning a job id (operations) | REJECT | lead decision 2026-10-29: flush is a direct drain with `gather`, no queue; async wrapping is the caller's choice | §2 drain |
| S5 | Pinger as advisory throttling, not a hard gate (operations) | CONFIRM | `pause(ttl)` with TTL; `drain(ignore_pause=True)` | §2 |
| S6 | Define a per-key safety deadline for `wait` (risk) | ESCALATE | — | §8 Q3 |

Summary: **4** confirmed · **1** rejected · **1** escalated.

---

## Revision History

| Version | Date | Author | Change |
|---|---|---|---|
| 0.1 | 2026-10-29 | Claude (for Jesus Lara) | Initial draft from the FEAT-830 architecture review |
