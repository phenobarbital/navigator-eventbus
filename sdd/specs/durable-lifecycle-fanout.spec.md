---
# SDD flow type and base branch (FEAT-145).
# - type: feature  (default)  → base_branch: main (this project uses main)
type: feature
base_branch: main
---

<!-- LANGUAGE: This document MUST be written entirely in English (proper nouns keep native spelling). -->

# Feature Specification: Durable lifecycle fan-out (registry bus hook + streams facade + persistent outbox)

**Feature ID**: FEAT-711 (reserved in the shared ai-parrot ledger `sdd/tasks/.id_ledger.json`, like FEAT-430..433)
**Date**: 2026-10-20
**Author**: Jesus Lara (with Claude)
**Status**: draft
**Target version**: navigator-eventbus 0.4.0
**Downstream**: ai-parrot FEAT-710 `parrot-telemetry-event-fanout` (consumer of this release); NVP-505 BigQuery sink (consumer of the stream)

---

## 1. Motivation & Business Requirements

### Problem Statement

ai-parrot will fan out its usage lifecycle events (`AfterInvokeEvent`, `InvokeFailedEvent`, `AfterClientCallEvent`,
`ClientCallFailedEvent`) through `EventRegistry.forward_to_bus` to an external telemetry consumer that bills
per tenant. **Losing an event is not acceptable.** Today the path cannot guarantee that, for four reasons:

1. **No public way to attach a bus, and no payload control.** `EventRegistry` takes `event_bus` only in
   `__init__` (`lifecycle/registry.py:104-113`), and `get_global_registry()` builds the registry lazily without
   one. Forwarding sends `event.to_dict()` verbatim (`registry.py:280-285`), so a host cannot strip sensitive
   fields such as `error_message` (`str(exc)`).
2. **The facade cannot select the durable transport.** `EventBus.__init__` builds only `RedisPubSubBackend`
   (at-most-once) or `MemoryBackend` (`evb.py:187-191`). `RedisStreamsBackend` (at-least-once, FEAT-312) is
   reachable only through `BusCore(backend=...)`.
3. **The producer drops on transport failure.** `BusCore.publish` spawns `_backend_publish` fire-and-forget
   (`core.py:302-308`), which logs and discards the envelope when the backend raises (`core.py:347-356`).
   Streams protects what reached the stream, not what never got there (Redis down, network partition,
   process killed before `XADD`).
4. **Producers become consumers.** `BusCore.start()` always calls `backend.start_consumer(...)` (`core.py:213-214`).
   With `RedisStreamsBackend` in `group` delivery, every producer process joins the consumer group and
   ACKs entries meant for the real consumer.

### Goals

- G1. `EventRegistry.attach_bus()` / `detach_bus()` and a payload **serializer hook** (registry-level default
  plus a per-subscription override; returning `None` skips the publish).
- G2. `EventBus` accepts an explicit transport: `backend=` (any `TransportBackend`) or `transport="streams"`.
- G3. A **persistent producer outbox** in `BusCore`: every backend-bound envelope is written to a local durable
  store *before* the transport publish, deleted only after the backend confirms, and re-published by a relay
  loop after a failure or a process restart.
- G4. A **publish-only** mode: a `BusCore`/`EventBus` that never starts the backend consumer.
- G5. Delivery contract end to end: **at-least-once** from `emit` to the consumer. Consumers dedup by
  `envelope.event_id`; `RedisStreamsBackend` already dedups on the consumer side.

### Non-Goals (explicitly out of scope)

- Exactly-once delivery.
- The telemetry consumer, `TelemetryEvent` or the BigQuery sink (NVP-505).
- Changing `RedisStreamsBackend` consumer semantics (groups, ACK, XAUTOCLAIM, DLQ).
- A distributed outbox (shared DB). The outbox is per process/host, on local disk.
- Making the registry's `create_task` scheduling synchronous (see §7 residual window).

---

## 2. Architectural Design

### Overview

```
EventRegistry.emit(event)
  └─ sub.forward_to_bus and bus attached
       payload = (sub.bus_serializer or registry.bus_serializer or to_dict)(event)
       payload is None → skip
       create_task(bus.emit(f"{prefix}.{EventClass}", payload))
              ▼
EventBus.emit → BusCore.publish(envelope)
  ├─ outbox.append(envelope)   ← awaited, durable (SQLite WAL, via asyncio.to_thread)
  ├─ spawn _backend_publish(envelope)
  │     ok   → outbox.ack(event_id)
  │     fail → stays in outbox (counted)
  └─ _enqueue_local(envelope)   (unchanged)
OutboxRelay (BusCore task): every relay_interval, and once at start():
  pending = outbox.due(limit) → backend.publish → ack | backoff(attempts)
              ▼
RedisStreamsBackend.publish → XADD  (publish_only producer: no consumer started)
              ▼
consumer group (NVP-505 sink) — XREADGROUP / ACK / XAUTOCLAIM / dedup by event_id
```

**Outbox ordering rule.** `append` completes before the transport publish is spawned. A crash at any later
point leaves the row in the outbox, and the relay re-publishes it on the next start. That gives
at-least-once; a re-publish after an unconfirmed-but-successful `XADD` produces a duplicate with the same
`event_id`, which the consumer dedups.

**Bounded outbox.** `max_rows` (default 1 000 000) and `max_age` (default 7 days). On overflow the policy is
`reject_new` by default: the publish raises `OutboxFullError`, logs an error and counts `rejected`, never
silently. `drop_oldest` is opt-in. A full outbox means Redis has been unreachable for a long time and is an
operational alarm, exposed via `stats()`.

**No new dependency.** The default store is stdlib `sqlite3` in WAL mode, with every call run through
`asyncio.to_thread` on a dedicated connection (`check_same_thread=False`, guarded by a lock), so the event
loop never blocks on disk.

### Integration Points

| Piece | Location | Change |
|---|---|---|
| `_Subscription` | `lifecycle/registry.py:63-82` | + `bus_serializer` |
| `EventRegistry.__init__` | `lifecycle/registry.py:104-113` | + `bus_serializer=` |
| `EventRegistry.subscribe` | `lifecycle/registry.py:121-157` | + `bus_serializer=` kwarg |
| forward branch | `lifecycle/registry.py:276-292` | use serializer, skip on `None` |
| `EventBus.__init__` | `evb.py:164-194` | + `backend=`, `transport=`, `publish_only=`, `outbox=` |
| `BusCore.__init__` | `core.py:128-140` | + `outbox=`, `publish_only=`, relay options |
| `BusCore.start/close` | `core.py:203-216`, `:217+` | start relay; skip consumer when publish-only; flush on close |
| `BusCore.publish` / `_backend_publish` | `core.py:287-310`, `:347-356` | outbox write-ahead + ack |
| `RedisStreamsBackend.publish` | `backends/redis_streams.py:305-326` | unchanged (raises on failure, which the outbox relies on) |

### Data Models

```sql
-- outbox table (SQLite, WAL)
CREATE TABLE IF NOT EXISTS evb_outbox (
  event_id    TEXT PRIMARY KEY,
  topic       TEXT NOT NULL,
  envelope    TEXT NOT NULL,          -- EventEnvelope.to_dict() JSON
  created_at  REAL NOT NULL,
  attempts    INTEGER NOT NULL DEFAULT 0,
  next_try_at REAL NOT NULL,
  last_error  TEXT                    -- exception class name only
);
CREATE INDEX IF NOT EXISTS evb_outbox_due ON evb_outbox(next_try_at);
```

### New Public Interfaces

```python
# lifecycle/registry.py
BusSerializer = Callable[[LifecycleEvent], Optional[dict[str, Any]]]

class EventRegistry:
    def __init__(self, *, event_bus=None, bus_channel_prefix="lifecycle", forward_to_global=True,
                 bus_serializer: "Optional[BusSerializer]" = None) -> None: ...
    def subscribe(self, event_type, callback, *, where=None, forward_to_bus=False,
                  bus_serializer: "Optional[BusSerializer]" = None) -> str: ...
    def attach_bus(self, bus: "EventBus", *, channel_prefix: Optional[str] = None,
                   serializer: "Optional[BusSerializer]" = None) -> None:
        """Attach (or replace) the EventBus used by forward_to_bus subscriptions. Idempotent for the same bus."""
    def detach_bus(self) -> None:
        """Stop forwarding; subscriptions keep their flag and resume on the next attach_bus()."""
    @property
    def event_bus(self) -> "Optional[EventBus]": ...

# outbox.py (new)
class OutboxStore(Protocol):
    async def append(self, envelope: EventEnvelope) -> None: ...
    async def ack(self, event_id: str) -> None: ...
    async def nack(self, event_id: str, *, error: str, next_try_at: float) -> None: ...
    async def due(self, *, now: float, limit: int) -> list[EventEnvelope]: ...
    async def stats(self) -> dict[str, int]: ...
    async def close(self) -> None: ...

class SqliteOutbox:  # implements OutboxStore
    def __init__(self, path: str | os.PathLike, *, max_rows: int = 1_000_000,
                 max_age: timedelta = timedelta(days=7),
                 overflow: Literal["reject_new", "drop_oldest"] = "reject_new") -> None: ...

class OutboxFullError(RuntimeError): ...

# core.py
class BusCore:
    def __init__(self, *, ..., backend=None, outbox: "Optional[OutboxStore]" = None,
                 publish_only: bool = False, relay_interval: float = 5.0, relay_batch: int = 500,
                 relay_max_backoff: float = 300.0) -> None: ...
    def stats(self) -> dict[str, int]: ...   # outbox_pending, outbox_acked, outbox_retried, outbox_rejected, backend_failures

# evb.py
class EventBus:
    def __init__(self, redis_url=None, use_redis=False, *, channel_prefix=None,
                 backend: "Optional[TransportBackend]" = None,
                 transport: Literal["memory", "pubsub", "streams"] | None = None,
                 publish_only: bool = False, outbox: "Optional[OutboxStore | str]" = None,
                 **bus_options) -> None:
        """backend= wins over transport=; transport= wins over use_redis. outbox= accepts a path (SqliteOutbox)."""
```

Config (navconfig, all optional): `BUS_TRANSPORT`, `BUS_PUBLISH_ONLY`, `BUS_OUTBOX_PATH`, `BUS_OUTBOX_MAX_ROWS`,
`BUS_OUTBOX_OVERFLOW`, `BUS_OUTBOX_RELAY_INTERVAL`.

---

## 3. Module Breakdown

#### Delegation-eligible modules

| Module | Eligible | Decided patterns / contracts |
|---|---|---|
| M1 registry hook | yes | signatures above; default serializer = `event.to_dict()`; `None` → skip; serializer exceptions logged, never raised |
| M2 outbox store | yes | SQLite WAL, `asyncio.to_thread`, schema above, overflow policies |
| M3 BusCore outbox + relay | design fixed, review needed | write-ahead before spawn; relay with exponential backoff capped at `relay_max_backoff`; flush on close bounded by `drain_timeout` |
| M4 publish-only | yes | `start()` skips `backend.start_consumer` |
| M5 EventBus facade | yes | precedence `backend=` > `transport=` > `use_redis` |

### Module 1: registry bus hook
Add `bus_serializer` to `_Subscription`, `__init__` and `subscribe`, plus `attach_bus`, `detach_bus` and an
`event_bus` property. In the forward branch, resolve the serializer (subscription → registry → `to_dict`);
a raising serializer is logged with `logger.exception` and that forward is skipped. Existing behavior is
byte-for-byte unchanged when no serializer is configured.

### Module 2: `OutboxStore` + `SqliteOutbox`
New `navigator_eventbus/outbox.py`. `append` is idempotent on `event_id` (`INSERT OR IGNORE`). `due` returns rows
with `next_try_at <= now` ordered by `created_at`. `max_age` purge runs inside `due`, and purged rows are
counted as `expired` and logged at ERROR. Store only the exception class name in `last_error`.

### Module 3: BusCore write-ahead + relay
In `publish`, when `self._backend` and `self._outbox` are set, `await self._outbox.append(envelope)` before
`_spawn(self._backend_publish(...))`. `_backend_publish` calls `outbox.ack` on success and
`outbox.nack(error, next_try_at=now + backoff(attempts))` on failure. A relay task, started in `start()`, drains
`due()` once immediately (crash recovery), then every `relay_interval`. `close()` stops the relay after one final
bounded drain; rows still pending stay on disk for the next start. Without an outbox, behavior is unchanged.

### Module 4: publish-only mode
`BusCore(publish_only=True)` skips `backend.start_consumer()` in `start()`. `EventBus(publish_only=True)` forwards it.

### Module 5: EventBus facade transport selection
`backend=` uses the given backend. `transport="streams"` builds `RedisStreamsBackend(redis_url, ...)`, forwarding
`BUS_STREAM_*` config. `transport="pubsub"` is the current Redis path, and `"memory"` uses `MemoryBackend`. A
string `outbox=` builds `SqliteOutbox(path)`. The legacy pub/sub-only listener (`start_redis_listener`) stays as is.

---

## 4. Test Specification

### Unit Tests
- `test_attach_bus_after_construction_forwards`; `test_detach_bus_stops_forwarding`.
- `test_serializer_redacts_and_none_skips`; `test_subscription_serializer_overrides_registry`.
- `test_serializer_exception_logged_not_raised`.
- `test_default_payload_unchanged_without_serializer` (regression).
- `test_outbox_append_idempotent`; `test_outbox_due_order_and_backoff`; `test_outbox_overflow_reject_new_raises`;
  `test_outbox_overflow_drop_oldest_counts`; `test_outbox_max_age_purge_counted`.
- `test_publish_writes_outbox_before_backend` (backend spy observes the row already present).
- `test_backend_failure_keeps_row_and_relay_redelivers` (fake backend fails N times, then succeeds → exactly one ack).
- `test_restart_recovers_pending_rows` (new BusCore on the same path publishes leftovers on `start()`).
- `test_publish_only_never_starts_consumer`.
- `test_eventbus_transport_precedence`.

### Integration Tests
| Test | Description |
|---|---|
| streams durability | Redis Streams + outbox: stop Redis, emit 1 000 events, restart Redis → all 1 000 `event_id`s reach the consumer group (duplicates allowed, missing = 0) |
| producer/consumer split | publish-only producer + separate consumer process: the producer never appears in `XINFO CONSUMERS` |

### Validation Contract
- `tests/lifecycle/test_registry.py`
- `tests/lifecycle/test_registry_fire_and_forget.py`
- `tests/lifecycle/test_dual_emit_integration.py`
- `tests/test_core.py`
- `tests/test_backends_streams.py`
- `tests/test_evb.py`

---

## 5. Acceptance Criteria

- [ ] `attach_bus()` works on an already-built registry (including the global one); `detach_bus()` stops forwarding.
- [ ] A serializer can redact or skip the payload; with no serializer the payload is identical to today's.
- [ ] `EventBus(transport="streams")` publishes through `RedisStreamsBackend`.
- [ ] With an outbox configured, no envelope is lost when the backend fails or Redis is down, or after a
  process restart: the integration test shows 0 missing `event_id`s.
- [ ] A full outbox is never silent: `reject_new` raises `OutboxFullError`, and every drop or expiry is counted
  in `stats()` and logged at ERROR.
- [ ] A `publish_only` bus never joins a consumer group.
- [ ] `emit` latency: the `test_emit_overhead.py` budget still holds with the outbox enabled (the disk write runs
  off-loop).
- [ ] No new runtime dependency.

---

## 6. Codebase Contract

Verified against navigator-eventbus `main` @ `8e6f360` (version 0.3.0).

### Verified Imports
```python
from navigator_eventbus.lifecycle.registry import EventRegistry          # src/navigator_eventbus/lifecycle/registry.py:90
from navigator_eventbus.lifecycle.base import LifecycleEvent             # lifecycle/base.py (to_dict at :52)
from navigator_eventbus.core import BusCore, BusClosedError              # core.py:92, :61
from navigator_eventbus.envelope import EventEnvelope                    # envelope.py:106 (event_id :132, to_dict :164, from_dict :188)
from navigator_eventbus.backends.base import TransportBackend            # backends/base.py:28
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend  # backends/redis_streams.py:112
from navigator_eventbus.evb import EventBus                              # evb.py
```

### Existing Class Signatures
```python
# lifecycle/registry.py
@dataclass class _Subscription: subscription_id, event_type, callback, where, forward_to_bus, order   # :63-82
EventRegistry.__init__(self, *, event_bus=None, bus_channel_prefix="lifecycle", forward_to_global=True)  # :104
EventRegistry.subscribe(self, event_type, callback, *, where=None, forward_to_bus=False) -> str        # :121
# forward branch: create_task(self._event_bus.emit(channel, event.to_dict()))                           # :280-285
# core.py
BusCore.__init__(self, *, workers=4, queue_size=1024, handler_timeout=30.0, retry_attempts=3,
                 retry_base_delay=0.1, backpressure=None, default_backpressure=POLICY_BLOCK,
                 drain_timeout=5.0, on_dlq=None, backend=None)                                          # :128-140
BusCore.start() -> None   # starts workers, then backend.start_consumer(...)                           # :203-216
BusCore.publish(envelope) -> None   # spawn _backend_publish BEFORE _enqueue_local                      # :287-310
BusCore._backend_publish(envelope)  # logs warning and discards on exception                            # :347-356
# backends/base.py:28  TransportBackend(Protocol): publish, start_consumer, close
# backends/redis_streams.py:205  RedisStreamsBackend.__init__(redis_url=None, *, ..., delivery="group", streams=None, ...)
# backends/redis_streams.py:305  publish(envelope) -> XADD (raises on failure)
# evb.py:164  EventBus.__init__(redis_url=None, use_redis=False, *, channel_prefix=None, **bus_options)
# evb.py:187-191  backend = RedisPubSubBackend(...) if use_redis else MemoryBackend()
# evb.py:349  async def emit(event_type, payload, **kwargs) -> int
```

### Does NOT Exist (Anti-Hallucination)
- `EventRegistry.attach_bus`, `detach_bus`, `event_bus` property, any serializer hook.
- `navigator_eventbus.outbox`, `OutboxStore`, `SqliteOutbox`, `OutboxFullError`.
- `BusCore(outbox=...)`, `BusCore(publish_only=...)`, `BusCore.stats()`.
- `EventBus(backend=...)`, `EventBus(transport=...)`.

### Edit Sites (Blueprint Anchors)

| File | Anchor | Line | Count | Action |
|---|---|---|---|---|
| `src/navigator_eventbus/lifecycle/registry.py` | `    forward_to_bus: bool` (in `_Subscription`) | 82 | 1 | modify |
| `src/navigator_eventbus/lifecycle/registry.py` | `        bus_channel_prefix: str = "lifecycle",` | 108 | 1 | modify |
| `src/navigator_eventbus/lifecycle/registry.py` | `        forward_to_bus: bool = False,` | 127 | 1 | modify |
| `src/navigator_eventbus/lifecycle/registry.py` | `self._event_bus.emit(channel, event.to_dict()),` | 284 | 1 | modify |
| `src/navigator_eventbus/core.py` | `        backend: Optional[TransportBackend] = None,` | 140 | 1 | modify |
| `src/navigator_eventbus/core.py` | `            await self._backend.start_consumer(self._on_transport_envelope)` | 214 | 1 | modify |
| `src/navigator_eventbus/core.py` | `                self._backend_publish(envelope),` | 307 | 1 | modify |
| `src/navigator_eventbus/core.py` | `            await self._backend.publish(envelope)  # type: ignore[union-attr]` | 350 | 1 | modify |
| `src/navigator_eventbus/evb.py` | `            RedisPubSubBackend(redis_url, channel_prefix=self.channel_prefix)` | 188 | 1 | modify |
| `src/navigator_eventbus/outbox.py` | — | — | — | create |
| `src/navigator_eventbus/__init__.py` | — | — | — | modify (export outbox API) |

---

## 7. Implementation Notes & Constraints

### Patterns to Follow
- Optional collaborators injected via constructor kwargs, like `backend=` and `on_dlq=` in `BusCore`.
- Failure isolation: transport failures never reach the publisher or local dispatch (`core.py:347-356`), except
  the explicit `OutboxFullError` under `reject_new`.
- Config via `_bus_config()` / navconfig (`evb.py:114`).

### Known Risks / Gotchas
- **Residual window:** `EventRegistry` schedules `bus.emit` with `create_task` (`registry.py:282`). A process
  killed between the lifecycle emit and that task's `outbox.append` (one loop iteration) loses the event.
  This is accepted and documented; closing it would put a synchronous disk write on the agent request path.
- **Duplicates are expected** (relay after an unconfirmed `XADD`). Consumers must dedup by `event_id`.
- **Per-host disk:** the outbox path must be on persistent, per-instance storage (one file per process/worker,
  e.g. `{path}/{hostname}-{pid}.db`, plus an adoption sweep of orphaned files at start). Ephemeral container
  disks defeat restart recovery; the deploy note must mount a volume.
- **Multi-worker gunicorn:** each worker owns its own outbox file. Two workers must never share one SQLite file.
- **Ordering:** at-least-once with relay does not preserve global order; consumers order by event timestamp.

### External Dependencies
None new (stdlib `sqlite3`; `redis>=5` is already in the `[redis]` extra).

---

## 8. Open Questions

- [ ] Q1. Orphaned outbox files from dead PIDs: adopt them on start of any worker on the same host (file lock),
  or only via an explicit CLI `evb-outbox-drain <dir>`? — *Owner: Jesus Lara*
- [ ] Q2. Default `max_rows` / `max_age`: are 1M rows / 7 days right for the telemetry volume? — *Owner: Jesus Lara*
- [x] Durability level — *Resolved with the user (2026-10-20)*: persistent outbox; losing events is not an accepted risk.

---

## Worktree Strategy

- One feature worktree; per-task sub-worktrees.
- Module graph: M3 → M2 (uses `OutboxStore`); M5 → M3 + M4 (forwards `outbox=`/`publish_only=`). M1 has no
  edge and can run in parallel with M2.
- Shared files: `core.py` (M3 + M4), so those tasks are serialized.
- Exclusive resources: none.
- Cross-feature: ai-parrot FEAT-710 depends on the release that contains this feature.

---

## Revision History

| Version | Date | Author | Change |
|---|---|---|---|
| 0.1 | 2026-10-20 | Jesus Lara / Claude | Initial draft (from the FEAT-710 design review, codex finding S10) |
