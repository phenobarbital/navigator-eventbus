---
# SDD flow type and base branch (FEAT-145).
type: feature
base_branch: dev
projects: [navigator-eventbus]
tags: [queues, redis-streams, ordering, keyed-queue, workers, flush]
---

<!-- LANGUAGE: This document MUST be written entirely in English (proper nouns keep native spelling). -->

# Feature Specification: KeyedQueue — key-ordered, head-blocking work queue

**Feature ID**: FEAT-713
**Date**: 2026-10-29
**Author**: Jesus Lara (design) · Claude (spec)
**Status**: approved (v0.2, 2026-10-29 — Q1–Q4 resolved; tasks TASK-4573…4579)
**Target version**: the next navigator-eventbus release (number chosen by Jesús at release time; written **0.5.0** below as a placeholder) — the release that fieldsync FEAT-830
(`sdd/proposals/workday-punch-outage-resilience.proposal.md` in the fieldsync repo) depends on.
0.4.0 is already tagged (`279b0d8`, FEAT-431 Unified Broker Manager) and published on PyPI,
so this feature cannot ship under that number.

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
| `msg:<id>` | hash | `body` (JSON), `key`, `order`, `created_ms`, `attempts`, `state`, `last_error`, `wait_until_ms`, `due_ms` (retry eligibility), `gen` (lease generation) |
| `lock:<k>` | string | `redis.asyncio.lock.Lock` token; TTL = `lock_ttl_ms` |
| `delayed` | zset | member = key, score = due ms (retry backoff, `wait` deadlines) |
| `held` | hash | key → JSON `{reason, since_ms, message_id}` |
| `keys` | set | keys with at least one pending message (drain / describe) |
| `all` | zset | member = `message_id`, score = `created_ms` (oldest-age metric, `max_age` scan) |
| `paused` | string | reason; `PX` = pause TTL |
| `recover` | string | `SET NX PX recover_interval_ms` throttle token for `recover()` — a rate limit, not a leader: whoever wins runs one pass, nobody owns anything |
| `sig:<k>` | string | liveness marker of key `k`'s wake-up signal (`SET PX` = signal visibility); set when a signal is sent, delivered or extended, deleted when acked. `recover()` re-signals only keys with no marker and no lock |
| `lastscore` | string | last default `order` handed out (read + written inside `push`'s WATCH/MULTI): default `order` = `max(now_ms, last + 2^-10)` — strictly increasing for any burst, exact in a double (multiples of 2^-10, epoch-ms < 2^43) |

**Signal lifetime rules (v0.2, load-bearing for liveness).**
1. The consumer that wins the per-key lock **keeps its signal un-acked** until `apply()`
   finishes; `apply()` deletes it. If that consumer dies, the signal is reclaimed by the pull
   plane and the next consumer finds the lock free once `lock_ttl_ms` has passed.
2. Therefore the signal lease must outlive the lock: `signal.default_visibility_timeout_ms`
   **>** `lock_ttl_ms` (validated in `KeyedQueueConfig`), and `KeyedLease.extend()` extends
   the signal visibility together with the lock.
3. A consumer that receives a signal for a key whose lock is **held by someone else** acks
   that signal (it is a duplicate: the holder still owns its own signal and re-signals on ack).
4. While the queue is paused, consumers **do not call `receive()`** at all: they sleep until
   `is_paused()` clears. A paused queue never cycles signals, so `max_receives` (pull plane,
   `ge=1`, default 5 — `queues/config.py:120`) is never consumed by a pause.
5. Signals stay hints. `recover()` is the backstop that makes a lost signal (consumer crash
   between `ZREM delayed` and `send`, a signal parked by `max_receives`, Redis eviction)
   cost at most one `recover_interval_ms` (plus, when only the `sig:<k>` marker outlived the
   signal, one signal-visibility window). One pass sweeps the whole key set; nothing is
   persisted across passes.

### Component Diagram

```
producer ── push(key, body, message_id, order) ──► msg:<id> (HSETNX) · ZADD key:<k> · SADD keys · ZADD all
                                                  └─► QueueStore.send(signal{key})            (hint)

KeyedConsumer × N (any pod, no leader)
  loop:  is_paused()?             yes → sleep min(pause PTTL, poll) · NO receive (signal rule 4)
         recover()                every recover_interval_ms, throttled by SET NX PX `recover`
                                  → for k in keys: not locked, not held, no delayed entry → send(signal{k})
         promote_due()            ZRANGEBYSCORE delayed ≤ now → ZREM (winner only) → send(signal{key})
         lease()                  QueueStore.receive(signal)
                                  → Lock(lock:<k>).acquire(blocking=False)
                                      fails → ack signal (duplicate, rule 3), next
                                  → head = ZRANGE key:<k> 0 0
                                  → head expired (max_age)? → log "expiring" · await on_dlq · remove
                                      (same MULTI as ack), next
                                  → key held / head waiting (wait_until > now) / head not due (due_ms > now)
                                      → release lock, ack signal, ensure ZADD delayed key=deadline, next
                                  → gen+1 on msg · return KeyedLease(key, message, lock, signal receipt, gen)
         outcome = handler(msg)   bounded by asyncio.wait_for(call_timeout_ms); KeyedConsumer extends
                                  lock + signal visibility every lock_ttl_ms/3
         apply (WATCH lock:<k> msg:<id> → token == ours and gen unchanged → MULTI … EXEC; else LeaseLost):
                 ack   → ZREM head · DEL msg · ZREM all · re-signal if ZCARD>0 else SREM keys · unlock
                 retry → attempts+1 · due_ms=now+delay · ZADD delayed key=due_ms · unlock     (head stays)
                 wait  → state=waiting · wait_until · ZADD delayed (deadline) · unlock      (head stays)
                 hold  → HSET held key · unlock                                             (head stays)
         after EXEC: QueueStore.delete(signal receipt)   (rule 1 — the holder's own signal)

drain(handler, key_prefix)        gather(drain_key(k) for k in (keys ∪ delayed) matching key_prefix, sem)
                                  same Lock per key; sequential inside a key; lock extended per head;
                                  fail-fast after N consecutive retry outcomes
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
    pause_poll_ms: int = 1_000               # paused consumers re-check at most this often
    recover_interval_ms: int = 60_000        # recover() throttle (SET NX PX), see signal rule 5
    wait_safety_ms: Optional[int] = None     # None → lock_ttl_ms × 10; deadline for a `wait` without until_ms (Q3)
    prefix: str = "evq:keyed:"
    # validator: signal.default_visibility_timeout_ms > lock_ttl_ms (signal rule 2)

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
    due_ms: Optional[int]                    # retry eligibility; lease() skips the head before it
    gen: int                                 # lease generation; apply() fences on it

class Outcome(BaseModel):                   # what a handler returns
    kind: Literal["ack", "retry", "wait", "hold"]
    delay_ms: Optional[int] = None          # retry: None → backoff(attempts)
    until_ms: Optional[int] = None          # wait: re-check deadline (None → now + wait_safety_ms, Q3)
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
    async def recover(self, *, force: bool = False) -> int: ...
    async def drain(self, handler, *, key_prefix: str | None = None, concurrency: int = 10,
                    call_timeout_ms: int | None = None, stop_after_failures: int = 5,
                    include_held: bool = False, ignore_pause: bool = False) -> DrainReport: ...

class KeyedLease:
    key: str
    message: KeyedMessage
    async def extend(self, additional_ms: int | None = None) -> None: ...   # lock + signal visibility; raises LeaseLost

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
          """One MULTI/EXEC: HSET msg (all fields) only if absent (WATCH msg:<id>) + ZADD key
          + SADD keys + ZADD all; then signal. Returns the id. Idempotent: an existing
          message_id changes nothing and still re-signals; an existing id with a different
          key raises ValueError. NOTE: after an ack the hash is gone, so a later push of the
          same id creates a NEW message — callers that re-push from their record must make
          their handler check that record (see §7)."""
      async def lease(self, *, wait_ms: int = 0) -> Optional[KeyedLease]:
          """Lease the head of one key, or None. Never returns a message of a key that is
          locked, held, waiting (before its deadline), not yet due (retry) or paused. Keeps
          the signal un-acked while leased (signal rule 1); acks duplicates (rule 3)."""
      async def apply(self, lease: KeyedLease, outcome: Outcome) -> None:
          """Apply ack/retry/wait/hold inside WATCH lock:<k> msg:<id> → MULTI/EXEC, after
          checking the lock token and `gen`. Raises LeaseLost and changes nothing otherwise.
          Deletes the holder's signal after EXEC."""
      async def promote_due(self, *, limit: Optional[int] = None) -> int:
          """Move due keys from `delayed` to the signal stream. Safe to call from N consumers:
          only the consumer whose ZREM returned 1 sends the signal."""
      async def recover(self, *, force: bool = False) -> int:
          """Throttled (SET NX PX `recover`) re-signal of every key in `keys` that is not
          locked, not held and has no `delayed` entry. Any consumer may win; returns signals sent."""
      async def expire_head(self, key: str, message: KeyedMessage) -> bool:
          """When created_ms + max_age_ms < now: log 'expiring', await on_dlq(...), remove in one MULTI. Returns True if expired."""
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
          """paused? → sleep · recover (throttled) · promote_due → lease → handler (wait_for, lock +
          signal extended every lock_ttl_ms/3) → apply. Returns the applied outcome or None."""
  ```

### Module 4: drain
- **Path**: `src/navigator_eventbus/queues/keyed/drainer.py` (renamed from `drain.py`: a submodule named like the exported function shadowed it)
- **Responsibility**: the administrative flush — direct, concurrent across keys, sequential per key, same lock, fail-fast.
- **Depends on**: M1, M2.
- **Interface Skeleton**:
  ```python
  # src/navigator_eventbus/queues/keyed/drain.py  (new)
  async def drain(queue: KeyedQueue, handler: Handler, *, key_prefix: Optional[str] = None,
                  concurrency: int = 10, call_timeout_ms: Optional[int] = None,
                  stop_after_failures: int = 5, include_held: bool = False,
                  ignore_pause: bool = False) -> DrainReport:
      """gather(drain_key(k)) over (keys ∪ delayed-keys) whose name starts with `key_prefix`
      (None = every key) under a Semaphore(concurrency). Ignores retry delays (`due_ms`),
      never `wait` deadlines. A key locked by a live consumer counts as `busy` and is skipped.
      Stops launching new keys after `stop_after_failures` consecutive retry outcomes."""
  async def drain_key(queue: KeyedQueue, key: str, handler: Handler, sem: asyncio.Semaphore,
                      *, call_timeout_ms: int, counters: DrainReport) -> None:
      """Lock the key once; loop over heads until a non-ack outcome or an empty key, extending
      the lock before each head and applying each outcome through the same fenced `apply`; unlock."""
  ```

### Module 5: fake Redis extension and tests
- **Path**: `tests/queues/_fake_streams.py` (modify), `tests/queues/keyed/` (new)
- **Responsibility**: add `zadd/zrange/zrangebyscore/zrem/zcard/zscore`, `hset/hsetnx/hget/hgetall/hincrby/hdel/hlen`, `sadd/srem/smembers/scard`, `set(nx, px)/get/delete/pexpire/pttl`, `watch`/`multi`/`exec` pipelines (`pipeline(transaction=True)` with `WATCH` invalidation), and the `eval`/`register_script` shim `redis.asyncio.lock.Lock` uses for `release`/`extend` (token compare), to `FakeStreamsRedis`; the tests in §4.
- **Depends on**: M1–M4.

### Module 6: exports, changelog, README
- **Path**: `src/navigator_eventbus/queues/__init__.py` (modify `_LAZY_MAP`, `__all__`), `CHANGELOG.md` (new `## [0.5.0] — Unreleased` section above 0.4.0; retitle the stale `## [0.4.0] — Unreleased` heading to the 0.4.0 release date), `README.md` (new "Keyed queues" subsection after "Pull queues (SQS-style)").
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
| `test_wait_without_until_gets_safety_deadline` | M2 | `wait` with `until_ms=None` → key in `delayed` at `now + wait_safety_ms`; leasable after it |
| `test_wait_blocks_key_until_deadline_or_signal` | M2 | wait with `until_ms` → not leasable before; a new push to the key re-signals and the head is still the waiting message |
| `test_hold_blocks_key_release_hold_resumes` | M2 | hold → `lease` skips the key; `release_hold` → leasable |
| `test_pause_never_receives_signals` | M3 | paused for longer than `max_receives` × visibility → no `receive()` call, no signal parked to the DLQ; after resume every key is leased |
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
| `test_drain_key_prefix_scopes_keys` | M4 | keys `a:1`, `a:2`, `b:1`; `drain(key_prefix="a:")` touches only `a:*` |
| `test_signal_kept_while_leased_and_recovered_after_crash` | M2 | consumer A leases and "dies" (no apply); B gets a duplicate signal while A's lock lives → acks it; after `lock_ttl_ms` A's own reclaimed signal lets B lease the head |
| `test_visibility_must_exceed_lock_ttl` | M1 | `signal.default_visibility_timeout_ms <= lock_ttl_ms` → ValueError |
| `test_recover_resignals_orphan_key` | M2 | key with a pending head, no signal, no delayed entry → `recover()` sends one signal; two concurrent `recover()` → one pass (throttle) |
| `test_promote_crash_between_zrem_and_send_is_recovered` | M2 | ZREM done, send skipped → `recover()` re-signals |
| `test_retry_delay_enforced_against_duplicate_signal` | M2 | retry(900 s) then a duplicate signal / a new push to the key → `lease` skips the head until `due_ms` |
| `test_stale_apply_is_fenced` | M2 | A's lock expires between its token check and EXEC, B leases → A's `apply` raises `LeaseLost`, B's state intact (WATCH invalidation) |
| `test_push_is_atomic` | M2 | push interrupted before EXEC → nothing written; re-push creates the full message |
| `test_push_same_id_other_key_rejected` | M2 | ValueError |
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
                         default_visibility_timeout_ms=20_000, max_visibility_timeout_ms=20_000)  # > lock_ttl_ms (rule 2)
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
- [ ] `CHANGELOG.md` 0.5.0 entry, README subsection, lazy exports.
- [ ] A pause never consumes signal deliveries; a consumer crash or a lost signal delays a key by at most `max(lock_ttl_ms, recover_interval_ms)`, never forever.
- [ ] `apply` is fenced by token + generation in one `WATCH`/`MULTI`; a stale outcome changes nothing.
- [ ] A retry delay is honoured even when the key is re-signalled early.
- [ ] `drain(key_prefix=…)` never touches keys outside the prefix.
- [ ] Release **0.5.0** tagged and published; fieldsync FEAT-830 pins `navigator-eventbus>=0.5.0`.

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
| `src/navigator_eventbus/queues/keyed/drainer.py` (renamed from `drain.py`: a submodule named like the exported function shadowed it) | CREATE | — | — | — |
| `src/navigator_eventbus/queues/__init__.py` | MODIFY | `_LAZY_MAP = {` | `queues/__init__.py:49` | 1 |
| `tests/queues/_fake_streams.py` | MODIFY | `class FakeStreamsRedis:` | `_fake_streams.py:64` | 1 |
| `tests/queues/keyed/test_store.py` · `test_consumer.py` · `test_drain.py` · `test_exports.py` | CREATE | — | — | — |
| `CHANGELOG.md` | MODIFY | `## [0.4.0] — Unreleased` (insert `## [0.5.0] — Unreleased` above it, retitle it) | `CHANGELOG.md:6` | 1 |
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
- **Outcome application is fenced, not just checked** (v0.2): `lock.owned()` followed by separate commands is a check-then-act race (the lock can expire between them and another consumer's state gets overwritten). `apply` runs `WATCH lock:<k> msg:<id>`, compares the lock value with our token and `msg.gen` with the lease's generation, then issues every mutation in one `MULTI`/`EXEC`; a `WatchError` or a mismatch raises `LeaseLost` and changes nothing. No Lua is needed. The handler's external side effect may already have happened; that is the caller's idempotency concern (FEAT-432 §9 "consumers must be idempotent" applies unchanged).
- **Push is one transaction** (v0.2): `WATCH msg:<id>` → if absent, `MULTI` HSET (all fields) + ZADD key + SADD keys + ZADD all → `EXEC`. A crash before EXEC writes nothing; a partially initialised message cannot exist.
- **A handler timeout or cancellation is reported to the handler's owner, not hidden** : `KeyedConsumer` converts exceptions into `retry`, which is only correct for handlers whose side effect is idempotent or provably not started. Handlers talking to a non-idempotent downstream must catch `asyncio.CancelledError`/`TimeoutError` themselves and return their own outcome (documented in the README; FEAT-830 does exactly this).
- **Signals are hints**: losing one costs at most one `promote_due`/re-push; duplicates cost one no-op lease. Never build an invariant on signal presence.

### Known Risks / Gotchas
- A handler longer than `lock_ttl_ms` without `KeyedLease.extend()` loses the key to another consumer. `KeyedConsumer` extends automatically at `lock_ttl_ms / 3`; direct `lease()` users must do it themselves. Documented loudly.
- `max_age` expiry is detected lazily (when a head is examined). A key nobody signals keeps its expired head until `drain`, `promote_due` or a new push touches it. `describe()` exposes `oldest_age_ms` from `all` so an operator can alarm on it.
- `drain` with high `concurrency` against a downstream that just recovered can overload it; `stop_after_failures` and the semaphore are the only guards. Callers choose the number.
- `pause` is global for the queue. Per-tenant pausing is a caller concern (use one queue per tenant, or hold keys).
- Redis data loss empties the queue silently. The contract is explicit: callers rebuild via idempotent `push` from their record at startup / in their flush. `recover()` does not help here: it re-signals keys that still exist in Redis.
- **Re-push after ack recreates the message** (the hash is deleted on ack). Callers that re-push from their record must have the handler consult that record and `ack` without side effects when the work is already done.
- **Ordering is among queued messages only.** A message pushed after a later-scored message of the same key was already acked sorts first among what is left, but cannot undo the earlier delivery. Callers needing stronger guarantees enforce them in the handler.
- **DLQ handoff is best-effort persistence**: `DLQHandler.on_dlq` persists in a background task and swallows failures (`dlq.py:195-227`), and its replay republishes to the bus, not into a `KeyedQueue`. The caller's record stays the recovery source for expired messages.

### External Dependencies
| Package | Version | Reason |
|---|---|---|
| `redis` | `>=5` (already `[redis]` extra) | `redis.asyncio.lock.Lock`, zset / hash commands |

---

## 8. Open Questions

- [x] Lua or plain commands? — *Owner: Jesús* — Plain commands plus redis-py `Lock`; see §7.
- [x] Q1 HTTP surface — *Resolved 2026-10-29 (Juan, for the FEAT-830 consumer)*: in-process only in this release; no `QueueAPI`-style routes.
- [x] Q2 default `max_attempts` — *Resolved 2026-10-29*: `0` (unlimited); a downstream outage is not a poison message, the caller's deadline rules decide.
- [x] Q3 `wait` without `until_ms` — *Resolved 2026-10-29*: always add a safety deadline, `wait_safety_ms` (default `lock_ttl_ms × 10`); a `wait` with no `until_ms` gets `until = now + wait_safety_ms`.
- [x] Q4 release number — *Resolved 2026-10-29*: Jesús picks the number when he cuts the release. "0.5.0" in this spec is a placeholder for "the first release that ships FEAT-713" (0.4.0 is taken by FEAT-431).

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
| 0.2 | 2026-10-29 | Juan (with Claude; Codex cross-check) | Spec review fixes, design decisions unchanged: target **0.5.0** (0.4.0 already published); signal lifetime rules (holder keeps its signal, visibility > lock TTL, duplicates acked, no `receive()` while paused); `recover()` backstop for lost signals; `apply` fenced by token + generation in `WATCH`/`MULTI`; atomic `push`; `due_ms` enforces retry delays; `drain(key_prefix=)`; risks documented (re-push after ack, ordering scope, DLQ durability) |
