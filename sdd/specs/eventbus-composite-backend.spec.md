---
# SDD flow type and base branch (FEAT-145).
# - type: feature  (default)  → base_branch: main (this project uses main)
# - type: hotfix              → base_branch MUST be: main
type: feature
base_branch: main
---

<!-- LANGUAGE: This document MUST be written entirely in English (proper nouns keep native spelling). -->

# Feature Specification: Composite Multi-Channel Backend for navigator-eventbus

**Feature ID**: FEAT-430
**Date**: 2026-07-28
**Author**: Jesus
**Status**: approved
**Target version**: navigator-eventbus 0.6.x (next minor)

---

## 1. Motivation & Business Requirements

### Problem Statement

`app.py`'s compliance event-bus wiring (lines ~1535–1920) creates **3
separate `RedisStreamsBackend` instances**, each with its own
`BusCore`, Redis connection, and start/stop lifecycle hook — totalling
~200 lines of boilerplate:

| Instance | `delivery` | `group` | Streams | Purpose |
|---|---|---|---|---|
| `compliance_bus` | `broadcast` | — | 4 Topic streams | WebSocket fan-out |
| `compliance_ledger_bus` | `group` | `fieldsync-audit-ledger` | `fieldsync.manager` | Audit ledger writer |
| `compliance_push_alerts_bus` | `group` | `fieldsync-push-alerts` | `fieldsync.manager`, `fieldsync.program` | FCM push notifications |

All three share the same codec (`FieldSyncWireCodec`), the same stream
naming scheme (`stream_key_fn=lambda topic: topic`), and the same Redis
server. They cannot be consolidated today because:

1. `RedisStreamsBackend` accepts a single `delivery=` mode at
   construction (line 212 of `redis_streams.py`) — broadcast and group
   are mutually exclusive.
2. `RedisStreamsBackend` accepts a single `group=` name — two
   consumer groups require two instances.
3. `BusCore` accepts a single `backend=` — one backend per bus.

The GPS bus (a 4th `BusCore`) has a different codec (`GPSWireCodec`) and
stream, so it is out of scope.

### Goals

- **G1**: Introduce a `CompositeBackend` class in `navigator_eventbus`
  that multiplexes N *channels* (each a delivery mode + optional group
  name + stream subset + callback) over a **single Redis connection**.
- **G2**: `CompositeBackend` implements the `TransportBackend` protocol
  so it can be passed to `BusCore(backend=)` without changes to
  `BusCore`.
- **G3**: Reduce `app.py`'s compliance bus wiring from ~200 lines / 3
  Redis connections / 3 BusCores / 3 start–stop hooks to ~40–50 lines /
  1 Redis connection / 1 BusCore / 1 start–stop hook.
- **G4**: Each channel retains its own `on_envelope` callback so
  dispatch semantics are unchanged (broadcast → all workers see every
  entry; group → exactly-once per group).
- **G5**: Dedup is per-group (not global) so the same event can be
  delivered to both the ledger group AND the push-alerts group AND
  broadcast, as it is today.
- **G6**: Each group channel can have its own `max_deliveries` and
  `on_dlq` callback — the DLQ pipeline is unchanged.

### Non-Goals (explicitly out of scope)

- **Changing `TransportBackend` protocol** — the protocol stays as-is
  (3 methods: `publish`, `start_consumer`, `close`).
- **Changing `BusCore`** — the core stays single-backend; composite
  behaviour lives below it.
- **Changing `RedisStreamsBackend`** — it remains the single-channel
  implementation; `CompositeBackend` delegates to it.
- **GPS bus consolidation** — different codec and stream set; out of
  scope.
- **Distributed dedup** — remains the caller's responsibility (Redis
  dedup keys, same as today).

---

## 2. Architectural Design

### Overview

`CompositeBackend` is a **composition layer** above `TransportBackend`,
not a replacement. It owns one shared Redis connection and creates N
internal `RedisStreamsBackend` instances (one per channel), injecting the
shared `client=` into each. It implements `TransportBackend` itself, so
`BusCore` sees a single backend.

The key insight: `RedisStreamsBackend` already accepts `client=` for
dependency injection and tracks `_owns_pool` semantics (it will NOT
close an injected client). This means N backends sharing one
`redis.asyncio.Redis` client is already supported — `CompositeBackend`
just formalises the pattern.

### Component Diagram

```
app.py
  │
  └── BusCore(backend=CompositeBackend)
              │
              ├── CompositeBackend
              │     │  owns: 1 Redis client
              │     │  owns: N RedisStreamsBackend instances (client= injected)
              │     │
              │     ├── Channel "broadcast"
              │     │     └── RedisStreamsBackend(delivery="broadcast", client=shared)
              │     │           → on_envelope: _route_bus_envelope_to_ws
              │     │
              │     ├── Channel "fieldsync-audit-ledger"
              │     │     └── RedisStreamsBackend(delivery="group", group=..., client=shared)
              │     │           → on_envelope: ledger_exception_handler
              │     │
              │     └── Channel "fieldsync-push-alerts"
              │           └── RedisStreamsBackend(delivery="group", group=..., client=shared)
              │                 → on_envelope: push_alert_handler
              │
              └── subscribe("fieldsync.*", handler)  ← unchanged BusCore API
```

### Integration Points

| Existing Component | Integration Type | Notes |
|---|---|---|
| `TransportBackend` (protocol) | implements | `CompositeBackend` satisfies all 3 methods |
| `RedisStreamsBackend` | delegates to | One instance per channel, shared `client=` |
| `BusCore` | used by | `BusCore(backend=composite)` — zero changes |
| `DLQHandler` | per-channel | Each group channel can wire its own `on_dlq` |
| `FieldSyncWireCodec` | shared | Passed to each internal backend via `codec=` |
| `app.py` compliance wiring | simplifies | Target consumer of the new class |

### Data Models

```python
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable, Awaitable, Literal, Optional, Union

from navigator_eventbus.backends.base import OnEnvelope
from navigator_eventbus.backends.redis_streams import Codec


@dataclass(frozen=True)
class Channel:
    """One logical delivery channel within a CompositeBackend.

    Attributes:
        name: Human-readable identifier (also used as the consumer group
            name when ``delivery="group"``).
        delivery: ``"broadcast"`` or ``"group"``.
        on_envelope: Per-channel callback invoked for each delivered
            envelope. Required — a channel without a consumer is
            meaningless.
        streams: Optional stream subset this channel consumes. When
            ``None``, inherits the parent ``CompositeBackend``'s full
            stream set.
        max_deliveries: Group-mode only — entries exceeding this PEL
            delivery count are parked to ``on_dlq``.
        on_dlq: Group-mode only — DLQ callback for this channel.
    """
    name: str
    delivery: Literal["broadcast", "group"]
    on_envelope: OnEnvelope
    streams: list[str] | None = None
    max_deliveries: int | None = None
    on_dlq: Optional[Callable[..., Union[None, Awaitable[None]]]] = None
```

### New Public Interfaces

```python
class CompositeBackend:
    """Multiplexes N delivery channels over one Redis connection.

    Implements ``TransportBackend`` so ``BusCore`` can consume it as a
    single backend. Internally creates one ``RedisStreamsBackend`` per
    channel, injecting the shared ``redis.asyncio`` client.

    ``publish()`` fans out to the ONE channel designated as the
    publisher (``publish_via=``); consumption is independent per channel.
    """

    def __init__(
        self,
        *,
        redis_url: str | None = None,
        client: Any | None = None,
        channels: list[Channel],
        codec: Codec | None = None,
        stream_key_fn: Callable[[str], str] | None = None,
        streams: list[str] | None = None,
        retention: timedelta | None = None,
        publish_via: str | None = None,
        **common_backend_kwargs: Any,
    ) -> None: ...

    # TransportBackend protocol
    async def publish(self, envelope: EventEnvelope) -> None: ...
    async def start_consumer(self, on_envelope: OnEnvelope) -> None: ...
    async def close(self) -> None: ...
```

**`publish_via`**: Names the channel whose internal backend handles
`publish()` (i.e., the one whose `stream_key_fn` / `streams` /
`retention` govern the XADD). Defaults to the first channel. Only ONE
channel publishes — the others are consume-only. This matches today's
`app.py` where only `compliance_bus` (broadcast) publishes and the two
group buses only consume.

**`start_consumer(on_envelope)`**: The `on_envelope` argument from
`BusCore` is the **transport-level** callback used for loopback
echo-suppression. Each channel has its own `channel.on_envelope` for
domain dispatch. `CompositeBackend.start_consumer()` starts all
internal backends, wiring each channel's callback. The
`BusCore`-provided `on_envelope` is wired to the broadcast channel (the
one whose entries should enter the local dispatch queue for
`BusCore.subscribe()` handlers).

---

## 3. Module Breakdown

### Module 1: `Channel` dataclass
- **Path**: `navigator_eventbus/backends/composite.py`
- **Responsibility**: Frozen dataclass describing one delivery channel
  (name, delivery mode, callback, optional stream subset, optional DLQ).
- **Depends on**: `navigator_eventbus.backends.base.OnEnvelope`

### Module 2: `CompositeBackend` class
- **Path**: `navigator_eventbus/backends/composite.py`
- **Responsibility**: Implements `TransportBackend`. Creates and manages
  N internal `RedisStreamsBackend` instances over one shared Redis client.
  Routes `publish()` to the designated publisher channel. Starts/stops
  all channels' consumers.
- **Depends on**: Module 1, `RedisStreamsBackend`, `TransportBackend`

### Module 3: Package re-exports
- **Path**: `navigator_eventbus/backends/__init__.py`,
  `navigator_eventbus/__init__.py`
- **Responsibility**: Export `CompositeBackend` and `Channel` from the
  public surface.
- **Depends on**: Module 2

### Module 4: Unit tests
- **Path**: `tests/eventbus/test_composite_backend.py` (in fieldsync,
  using the installed package)
- **Responsibility**: Unit tests with mocked Redis — channel isolation,
  per-group dedup, publish routing, start/stop lifecycle, error
  isolation.
- **Depends on**: Module 2

### Module 5: Integration test
- **Path**: `tests/eventbus/test_composite_integration.py`
- **Responsibility**: Real-Redis integration test — verifies broadcast +
  2 groups over one connection deliver correctly.
- **Depends on**: Module 2 (requires running Redis)

### Module 6: `app.py` consolidation
- **Path**: `app.py` (fieldsync)
- **Responsibility**: Replace the 3-backend / 3-BusCore / 3-Redis-client
  compliance bus wiring with a single `CompositeBackend` + `BusCore` +
  Redis client. Preserve identical runtime behaviour and shutdown
  ordering.
- **Depends on**: Module 2, Module 3 (CompositeBackend must be released
  in `navigator-eventbus` first)

---

## 4. Test Specification

### Unit Tests

| Test | Module | Description |
|---|---|---|
| `test_channel_validates_broadcast_no_max_deliveries` | 1 | `Channel(delivery="broadcast", max_deliveries=5)` raises `ValueError` |
| `test_composite_creates_internal_backends` | 2 | Composite with 3 channels creates 3 `RedisStreamsBackend` instances |
| `test_composite_shares_redis_client` | 2 | All internal backends receive the same `client=` reference |
| `test_publish_routes_to_designated_channel` | 2 | `publish()` calls `XADD` via the `publish_via` channel's backend only |
| `test_start_consumer_starts_all_channels` | 2 | `start_consumer()` starts all N internal backends |
| `test_close_closes_all_channels_then_client` | 2 | `close()` stops all backends, then closes the shared Redis client |
| `test_channel_failure_isolated` | 2 | One channel's consumer error does not crash others |
| `test_broadcast_channel_receives_all_entries` | 2 | Broadcast channel callback fires for every published entry |
| `test_group_channel_receives_each_entry_once` | 2 | Group channel callback fires exactly once per entry |
| `test_two_groups_both_receive_same_entry` | 2 | Same entry delivered to both group channels independently |
| `test_dedup_is_per_group` | 2 | Dedup keys include group name — no cross-group suppression |
| `test_channel_stream_subset` | 2 | Channel with `streams=[X]` only consumes from stream X |
| `test_on_dlq_per_channel` | 2 | Each group channel's `on_dlq` fires independently |

### Integration Tests

| Test | Description |
|---|---|
| `test_composite_broadcast_plus_two_groups` | Real Redis: publish 10 entries; broadcast channel sees all 10; each group sees all 10; entries are correctly ACKed per group |
| `test_composite_shutdown_closes_one_connection` | Real Redis: after `close()`, only 1 Redis `QUIT` is issued (not 3) |

### Test Data / Fixtures

```python
@pytest.fixture
def channels():
    """Three compliance-like channels for testing."""
    return [
        Channel(name="broadcast", delivery="broadcast",
                on_envelope=_noop_handler),
        Channel(name="audit-ledger", delivery="group",
                on_envelope=_noop_handler, max_deliveries=5),
        Channel(name="push-alerts", delivery="group",
                on_envelope=_noop_handler, max_deliveries=5),
    ]

@pytest.fixture
async def composite(channels):
    """CompositeBackend wired to a real Redis instance."""
    backend = CompositeBackend(
        redis_url=REDIS_TEST_URL,
        channels=channels,
        streams=["fieldsync.manager", "fieldsync.associate",
                 "fieldsync.program", "fieldsync.admin"],
    )
    yield backend
    await backend.close()
```

---

## 5. Acceptance Criteria

> This feature is complete when ALL of the following are true:

- [ ] `CompositeBackend` satisfies the `TransportBackend` protocol
  (`isinstance(composite, TransportBackend)` is `True`)
- [ ] A single `CompositeBackend` with 1 broadcast + 2 group channels
  over 1 Redis connection delivers identically to 3 separate
  `RedisStreamsBackend` instances over 3 connections
- [ ] Broadcast channel: every process receives every entry (no group,
  no ACK)
- [ ] Group channels: each entry processed exactly once per group
  (XREADGROUP + XACK)
- [ ] Dedup keys are scoped per-group (format:
  `evb:events:dedup:<group>:<event_id>`)
- [ ] `publish()` fans out through exactly one designated channel
- [ ] `close()` stops all internal consumers, then closes the single
  shared Redis client
- [ ] Channel consumer failure is isolated — other channels continue
- [ ] `on_dlq` per channel fires independently
- [ ] `Channel(delivery="broadcast", max_deliveries=N)` raises
  `ValueError` at construction
- [ ] All unit tests pass
- [ ] Real-Redis integration test passes
- [ ] `app.py` compliance wiring consolidation reduces to 1 BusCore +
  1 Redis connection + 1 start/stop hook
- [ ] No breaking changes to `TransportBackend`, `BusCore`, or
  `RedisStreamsBackend`

---

## 6. Codebase Contract

> **CRITICAL — Anti-Hallucination Anchor**

### Verified Imports

```python
# navigator_eventbus package (installed)
from navigator_eventbus import BusCore, DLQHandler                      # __init__.py:17-18
from navigator_eventbus.backends.base import TransportBackend, OnEnvelope  # backends/base.py:28,24
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend, Codec  # backends/redis_streams.py:101,68
from navigator_eventbus.envelope import EventEnvelope, Severity         # envelope.py (re-exported __init__.py:19)

# RedisStreamsBackend constructor (verified: backends/redis_streams.py:194-222)
RedisStreamsBackend(
    redis_url=None, *, client=None, group=None, consumer_name=None,
    stream_prefix=None, dedup_prefix=None, dedup_ttl=86400,
    block_ms=1000, batch_count=32, min_idle_time_ms=60000,
    autoclaim_interval=30.0, maxlen=100000,
    stream_refresh_interval=10.0, reconnect_base_delay=0.5,
    reconnect_max_delay=30.0, delivery="group", codec=None,
    stream_key_fn=None, streams=None, retention=None,
    retention_trim_interval=60.0, max_deliveries=None, on_dlq=None,
)

# BusCore constructor (verified: core.py:128-141)
BusCore(
    *, workers=4, queue_size=1024, handler_timeout=30.0,
    retry_attempts=3, retry_base_delay=0.1, backpressure=None,
    default_backpressure="block", drain_timeout=5.0,
    on_dlq=None, backend=None,
)

# DLQHandler constructor (verified: dlq.py:125-131)
DLQHandler(bus: BusCore, *, dsn=None, driver="pg")

# TransportBackend protocol methods (verified: backends/base.py:28-45)
#   async def publish(self, envelope: EventEnvelope) -> None
#   async def start_consumer(self, on_envelope: OnEnvelope) -> None
#   async def close(self) -> None

# fieldsync-side imports (verified in app.py)
from services.eventbus.codec import FieldSyncWireCodec    # services/eventbus/codec.py
from services.eventbus.router import route_envelope        # services/eventbus/router.py
from services.eventbus import (                            # services/eventbus/__init__.py
    ledger_exception_handler, push_alert_handler,
    stream_for, Topic, BusCorePublisherShim,
)
```

### Existing Class Signatures

```python
# navigator_eventbus/backends/base.py
OnEnvelope = Callable[[EventEnvelope], Awaitable[None]]  # line 24

@runtime_checkable
class TransportBackend(Protocol):                        # line 28
    async def publish(self, envelope: EventEnvelope) -> None: ...   # line 37
    async def start_consumer(self, on_envelope: OnEnvelope) -> None: ...  # line 41
    async def close(self) -> None: ...                   # line 45

# navigator_eventbus/backends/redis_streams.py
class Codec(Protocol):                                   # line 68
    def encode(self, envelope: EventEnvelope) -> dict[str, Any]: ...  # line 79
    def decode(self, fields: dict[str, Any]) -> EventEnvelope: ...    # line 83

class RedisStreamsBackend:                                # line 101
    # Key constructor params for CompositeBackend:
    # client: Any | None = None         → injected Redis client (not owned)
    # delivery: "group" | "broadcast"   → fixed at construction
    # group: str | None                 → consumer group name
    # codec: Codec | None               → wire encoder/decoder
    # stream_key_fn: Callable | None    → stream naming override
    # streams: list[str] | None         → explicit stream set
    # max_deliveries: int | None        → group-mode DLQ threshold
    # on_dlq: Callable | None           → DLQ callback
    #
    # When client= is provided, RedisStreamsBackend does NOT close it
    # on close() — it only closes connections it created itself.
    # (verified: redis_streams.py:384-389)
```

### Integration Points

| New Component | Connects To | Via | Verified At |
|---|---|---|---|
| `CompositeBackend.__init__` | `RedisStreamsBackend.__init__` | creates N instances with `client=shared` | `redis_streams.py:194` |
| `CompositeBackend.publish` | `RedisStreamsBackend.publish` | delegates to the `publish_via` channel's backend | `redis_streams.py:290` |
| `CompositeBackend.start_consumer` | `RedisStreamsBackend.start_consumer` | starts all N backends with per-channel callback | `redis_streams.py:313` |
| `CompositeBackend.close` | `RedisStreamsBackend.close` + `redis.aclose()` | closes backends first (they skip client close), then closes the shared client | `redis_streams.py:364` |
| `app.py` | `CompositeBackend` | replaces 3 backends + 3 BusCores with 1 + 1 | `app.py:~1535-1920` |

### Does NOT Exist (Anti-Hallucination)

- ~~`navigator_eventbus.backends.composite`~~ — does not exist yet (this spec creates it)
- ~~`navigator_eventbus.Channel`~~ — does not exist yet
- ~~`RedisStreamsBackend.add_group()`~~ — no such method; groups are fixed at construction
- ~~`BusCore(backends=[...])`~~ — BusCore accepts a single `backend=`, not a list
- ~~`TransportBackend.start_consumers()`~~ — no plural form exists
- ~~`RedisStreamsBackend._delivery` setter~~ — `_delivery` is set in `__init__` and never mutated

---

## 7. Implementation Notes & Constraints

### Patterns to Follow

- **Delegation, not inheritance**: `CompositeBackend` creates
  `RedisStreamsBackend` instances internally — it does not subclass it.
- **Shared client injection**: Use `RedisStreamsBackend(client=shared)`
  for all internal backends. This is already the supported DI path
  (verified: `redis_streams.py:238-239`). None of them will close the
  client on their `close()` (verified: `redis_streams.py:384`).
- **Per-group dedup prefix**: Override `dedup_prefix=` per group-mode
  backend to `evb:events:dedup:<group>:` so the same `event_id`
  can be processed by both groups independently. Broadcast backends
  do not use dedup (verified: `_dispatch_broadcast_entry` at line 592
  has no dedup check).
- **`start_consumer` callback routing**: `BusCore` calls
  `backend.start_consumer(on_envelope)` with its own
  `_on_transport_envelope` method. This must go to the broadcast
  channel (or whichever channel feeds BusCore's local dispatch queue).
  Group channels use their own `channel.on_envelope` and do NOT feed
  into BusCore's queue — they are consumed independently.
- **Shutdown ordering**: `close()` must stop all internal backends
  FIRST (so no consumer task touches Redis post-close), then close the
  shared client. Mirrors the current `_stop_compliance_realtime_stack`
  in `app.py`.

### Known Risks / Gotchas

- **`start_consumer` protocol mismatch**: `TransportBackend.
  start_consumer` takes a single `on_envelope` callback.
  `CompositeBackend` must reconcile this with N per-channel callbacks.
  The solution: `start_consumer(on_envelope)` wires `on_envelope` to
  the broadcast channel (or `publish_via` channel) and starts all
  others with their own callbacks. This is the ONE point where the
  protocol's single-callback assumption meets multi-channel reality —
  it works because BusCore only needs transport-level loopback
  suppression from the broadcast path, and group channels dispatch
  directly to their handlers without going through BusCore's queue.
- **Connection limits**: Sharing one `redis.asyncio.Redis` client across
  N concurrent `XREAD`/`XREADGROUP` loops is safe (redis-py uses a
  connection pool internally), but the pool size should be at least N+1
  (N readers + 1 writer). Default pool size is 2^31 so this is not a
  practical concern, but document it.
- **Codec shared by default, overridable per channel**: All 3 compliance
  channels use `FieldSyncWireCodec`. The GPS bus uses a different codec.
  `Channel` should optionally accept a `codec=` override for future
  use, but the default is the parent's codec.

### External Dependencies

| Package | Version | Reason |
|---|---|---|
| `navigator-eventbus` | current (extends) | This spec adds a new module to the package |
| `redis[hiredis]` | `>=5.0` | Shared `redis.asyncio` client (already a dependency) |

---

## 8. Open Questions

- [x] **Channel-level codec override**: Should `Channel` accept an
  optional `codec=` for cases where channels decode differently (e.g.,
  GPS-like heterogeneous codecs on the same connection)? Leaning yes
  for forward-compatibility but not required by the compliance use
  case. — *Owner: Jesus*: yes
- [x] **`publish_via` multiple channels**: Should `publish()` XADD to
  multiple channels' streams, or is single-publisher sufficient? The
  compliance case only publishes via the broadcast channel's streams
  (the group channels consume from the same streams). Single-publisher
  is sufficient today. — *Owner: Jesus*: yes
- [x] **navigator-eventbus release cadence**: This lives upstream in
  `navigator-eventbus`. Module 6 (app.py consolidation) can only land
  after a release containing Modules 1–3. Should Modules 4–5 (tests)
  live in fieldsync or in navigator-eventbus's own test suite?
  — *Owner: Jesus*: ok

---

## Worktree Strategy

- **Isolation unit**: `per-spec` — all modules are sequential.
- **Ordering**: Modules 1–3 must be implemented upstream in
  `navigator-eventbus` first. Module 4–5 (tests) can run in fieldsync
  against the installed package. Module 6 (app.py) depends on the
  released package.
- **Cross-feature dependencies**: None — this is an additive feature
  with no changes to existing classes.

---

## Revision History

| Version | Date | Author | Change |
|---|---|---|---|
| 0.1 | 2026-07-28 | Jesus | Initial draft |
