---
# SDD flow type and base branch (FEAT-145).
# - type: feature  (default)  → base_branch: dev (or any non-main branch)
# - type: hotfix              → base_branch MUST be: main
type: feature
base_branch: main
---

# Feature Specification: RedisStreamsBackend Generic Capability Extensions

**Feature ID**: FEAT-320
**Date**: 2026-07-25
**Author**: Jesús Lara
**Status**: approved
**Target version**: next release after 0.1.0 (bump `version.py`)

---

## 1. Motivation & Business Requirements

### Problem Statement

`RedisStreamsBackend` (`src/navigator_eventbus/backends/redis_streams.py`,
FEAT-312) is the durable, at-least-once `TransportBackend` behind `BusCore`.
It hard-codes: (a) consumer-GROUP delivery only (`XREADGROUP`/`XACK`/
`XAUTOCLAIM`) — there is no group-less broadcast mode; (b) ONE wire codec
(`{"envelope": json.dumps(envelope.to_dict())}`) and ONE stream-naming
scheme (`<stream_prefix><topic-class>`, discovered via `SCAN`); (c) count-
based `MAXLEN ~` retention only; (d) unbounded redelivery — a pending entry
is reclaimed by `XAUTOCLAIM` forever, with no cap and no terminal parking.

A consuming application (FieldSync, `../fieldsync`, FEAT-409) needs to run
its OWN wire envelope (`fieldsync.*` streams, its own `EventEnvelope`
shape — see `sdd/state/` cross-repo notes) over this backend instead of
forking a parallel transport implementation. The right layer for this is
navigator-eventbus itself (this repo owns the transport machinery; FieldSync
should keep only configuration + handlers) — per this project's own
`CONTEXT.md` "generic capabilities live here" principle.

### Goals

- **`delivery="broadcast"`**: a group-less `XREAD` free-read consume mode
  where every backend instance receives every entry (the WS/socket
  fan-out semantic) — `delivery="group"` (today's only mode) stays the
  default, unchanged.
- **`codec=` / `stream_key_fn=` seams**: let a consuming app supply its own
  wire encode/decode and stream-naming function, bypassing the default
  package-envelope-in-one-field format and the `<stream_prefix><topic-class>`
  layout — WITHOUT forking the backend.
- **`retention=timedelta`**: a time-based `XTRIM MINID` mode, alternative
  to today's count-based `MAXLEN ~`.
- **`max_deliveries=N`**: group-mode retry-then-park — an entry reclaimed
  more than `N` times without a successful ACK is routed to a caller-
  supplied DLQ callback and ACKed (never reclaimed again), instead of being
  retried forever.
- Register any new topic namespace(s) consuming apps need
  (`fieldsync.*` for FEAT-409) in `TOPICS.md` and cut a release so
  `navigator-eventbus` version pins can move forward.

### Non-Goals (explicitly out of scope)

- Changing the `TransportBackend` protocol (`backends/base.py`) — it stays
  three methods, one Protocol; these are `RedisStreamsBackend`-specific
  constructor capabilities, not protocol changes.
- Changing default behavior for existing callers: group mode, the package
  codec, `<stream_prefix><topic-class>` naming, and `MAXLEN ~` trimming are
  UNCHANGED when the new kwargs are not passed. Every existing test in
  `tests/test_backends_streams.py` and `tests/test_integration.py` must
  keep passing untouched.
- Changing `BusCore`'s own per-subscriber retry (`core.py::_invoke_with_retry`,
  `retry_attempts`/`on_dlq`) — that mechanism is IN-PROCESS (a handler
  raising N times, in-process, per subscriber) and is orthogonal to this
  feature. `max_deliveries` here operates on Redis's own PEL delivery-count
  bookkeeping (`XPENDING`), guarding against a DIFFERENT failure mode: a
  poison entry that keeps crashing/hanging the consuming PROCESS itself
  (so `BusCore._on_transport_envelope` never even gets a chance to run its
  own retry/DLQ logic for it) and would otherwise be reclaimed by
  `XAUTOCLAIM` forever. See §2 "Why not reuse BusCore's DLQ" below.
- Consolidating `RedisStreamsBackend` with `navigator_eventbus.brokers.redis`
  (`RedisProducer`/`RedisConsumer`/`RedisConnection`, FEAT-316). These are
  separate subsystems today — the backend owns its `redis.asyncio` client
  directly and does not compose `RedisConnection`. This feature does not
  merge them.
- Any FieldSync-repo file. The consuming codec/handlers/composition-root
  wiring is FieldSync's own follow-up work (tracked there as FEAT-409
  Modules 6-8), gated on this feature's release.

---

## 2. Architectural Design

### Overview

Four independent, additive, opt-in constructor kwargs on
`RedisStreamsBackend`, plus one new explicit-streams override that the
`stream_key_fn=` seam requires (see Module 2). Existing behavior is the
`None`/default path through every new kwarg — nothing changes for current
callers.

**Why not reuse BusCore's DLQ for `max_deliveries`.** `core.py::_dispatch`
→ `_invoke_with_retry` already retries a handler in-process
(`retry_attempts`, default 3) and — critically — **never raises**: "Handler-
level failures remain isolated (model B: retry → DLQ) and therefore count
as processed" (`core.py:374`, `_on_transport_envelope` docstring). That
means `RedisStreamsBackend._handle_message`'s `await self._on_envelope(envelope)`
call essentially never raises from ordinary handler failures — BusCore
already isolated and DLQ'd them, in-process, per subscriber, and the
backend correctly ACKs (`_handle_message`'s "count as processed" comment,
`redis_streams.py:338` region, is accurate today). The backend's own
"leave unacked → `XAUTOCLAIM` redelivers" path is therefore NOT primarily
about handler business-logic failures — it is about the CONSUMING PROCESS
itself failing before the entry is ACKed (crash, OOM, an exception type
`_invoke_with_retry` cannot isolate because it happens outside `_dispatch`
entirely, e.g. inside `_handle_message`'s own decode/dedup Redis calls, or
the process dying mid-flight). `max_deliveries` guards against exactly
that: the SAME entry crashing every consumer it lands on, forever reclaimed
via `XAUTOCLAIM`, never progressing. This is why it is implemented against
Redis's OWN per-entry delivery-count bookkeeping (`XPENDING ... IDLE`,
which reports `times_delivered` per pending id) — a signal BusCore's
in-process retry counter cannot see or replace.

### Component Diagram

```
RedisStreamsBackend(
    delivery="group" | "broadcast",       # Module 1
    codec=Codec | None,                   # Module 2
    stream_key_fn=Callable[[str], str] | None,   # Module 2
    streams=list[str] | None,             # Module 2 (explicit set, bypasses SCAN)
    retention=timedelta | None,           # Module 3 (alternative to maxlen)
    max_deliveries=int | None,            # Module 4 (group mode only)
    on_dlq=Callable[..., Awaitable[None]] | None,  # Module 4
)
        │
        ├── delivery="group" (DEFAULT, unchanged): XREADGROUP/XACK/XAUTOCLAIM
        │     └── max_deliveries set → XPENDING IDLE check before reclaim;
        │           times_delivered > N → on_dlq(...) + XACK, never reclaimed again
        │
        ├── delivery="broadcast" (NEW): XREAD (free-read, no group),
        │     per-stream last-id cursor, every instance sees every entry,
        │     no ACK / no XAUTOCLAIM (nothing pending to reclaim)
        │
        ├── codec (NEW): encode()/decode() replace the hard-coded
        │     {"envelope": json.dumps(to_dict())} field
        │
        ├── stream_key_fn / streams (NEW): replace <stream_prefix><topic-class>
        │     SCAN discovery with caller-owned naming + an explicit stream set
        │
        └── retention=timedelta (NEW): periodic XTRIM MINID(now - retention),
              mutually exclusive in practice with maxlen (caller's choice,
              "exactly one trimmer" remains the CALLER's responsibility
              across backend instances, unchanged from today)
```

### Integration Points

| Existing Component | Integration Type | Notes |
|---|---|---|
| `RedisStreamsBackend.__init__` (`redis_streams.py:104`) | modifies | new kwargs, all default `None`/unchanged behavior |
| `RedisStreamsBackend.publish` (`redis_streams.py:162`) | modifies | codec seam for encode; retention path skips `maxlen=` |
| `RedisStreamsBackend.start_consumer` (`redis_streams.py:178`) | modifies | branches on `delivery` — spawns broadcast reader OR today's consumer+sweeper pair |
| `RedisStreamsBackend._handle_message` (`redis_streams.py:328`) | modifies | codec seam for decode; broadcast path skips dedup-set + XACK |
| `RedisStreamsBackend._stream_for` / `_refresh_streams` (`redis_streams.py:216,245`) | modifies | `stream_key_fn=` / `streams=` bypass |
| `TransportBackend` Protocol (`backends/base.py:28`) | unchanged | three methods, one Protocol — no change |
| `DLQHandler.on_dlq` (`dlq.py:175`) | reference shape | `on_dlq=` callback signature matches this method's, so callers CAN pass `DLQHandler.on_dlq` directly, but this module does not import `dlq.py` (avoids a `backends → core → dlq → core` layering concern; duck-typed callback only) |
| `TOPICS.md` | modifies | register `fieldsync.*` |
| `src/navigator_eventbus/version.py` | modifies | release bump |
| `tests/test_backends_streams.py` | modifies | extend `FakeStreamsRedis` with `xread`, `xpending_range`, `xtrim`; new test classes per module |

### Data Models

No new persistent data models. New in-process protocol (structural, not a
Pydantic model — matches this repo's existing `TransportBackend`
`Protocol` style):

```python
class Codec(Protocol):
    def encode(self, envelope: EventEnvelope) -> dict[str, Any]: ...
    def decode(self, fields: dict[str, Any]) -> EventEnvelope: ...
```

### New Public Interfaces

```python
# src/navigator_eventbus/backends/redis_streams.py (extended, not new file)

class RedisStreamsBackend:
    def __init__(
        self,
        redis_url: Optional[str] = None,
        *,
        client: Optional[Any] = None,
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
        # --- NEW (this feature) ---
        delivery: Literal["group", "broadcast"] = "group",
        codec: Optional["Codec"] = None,
        stream_key_fn: Optional[Callable[[str], str]] = None,
        streams: Optional[list[str]] = None,
        retention: Optional[timedelta] = None,
        retention_trim_interval: float = 60.0,
        max_deliveries: Optional[int] = None,
        on_dlq: Optional[
            Callable[..., Union[None, Awaitable[None]]]
        ] = None,  # (envelope, *, attempts, error, subscriber_id) -> None
    ) -> None: ...
```

---

## 3. Module Breakdown

### Module 1: Broadcast delivery mode
- **Path**: `src/navigator_eventbus/backends/redis_streams.py`
- **Responsibility**: `delivery="broadcast"` — group-less `XREAD` free-read
  loop (per-stream last-id cursor, starting at `"$"` so no pre-existing
  entries replay on start — matching the WS/socket "no replay" semantic).
  Every backend instance dispatches every entry. No `XACK`, no sweeper —
  `close()` only cancels the broadcast reader task in this mode.
- **Depends on**: none.

### Module 2: Codec + stream-naming + explicit streams seams
- **Path**: `src/navigator_eventbus/backends/redis_streams.py`
- **Responsibility**: `codec=` (encode/decode seam, default preserves
  today's exact wire shape), `stream_key_fn=` (replaces `_stream_for`),
  `streams=` (explicit fixed stream set — bypasses `SCAN`-based
  `_refresh_streams` discovery, required because a custom `stream_key_fn`
  breaks the `<stream_prefix>*` SCAN pattern assumption). Composes with
  EITHER delivery mode (Module 1 is delivery-mode-orthogonal).
- **Depends on**: none (parallel with Module 1; an integration test proves
  they compose — see §4).

### Module 3: Time-based retention
- **Path**: `src/navigator_eventbus/backends/redis_streams.py`
- **Responsibility**: `retention=timedelta` — periodic `XTRIM <stream>
  MINID <id derived from now - retention>` task, alternative to the
  existing `maxlen=` count trim (when `retention` is set, `publish()`
  drops the `maxlen=`/`approximate=` XADD kwargs — time-based trimming
  owns retention instead). New `retention_trim_interval` kwarg controls
  cadence. Exactly-one-trimmer-per-stream remains the CALLER's
  responsibility (unchanged principle from today's `maxlen` trimming).
- **Depends on**: none.

### Module 4: max_deliveries retry-then-park (group mode)
- **Path**: `src/navigator_eventbus/backends/redis_streams.py`
- **Responsibility**: `max_deliveries=N` + `on_dlq=` — before redispatching
  an `XAUTOCLAIM`-reclaimed entry, query its delivery count via
  `XPENDING <stream> <group> IDLE <min_idle_time_ms> - + <count> <consumer>`
  (`times_delivered` field); entries exceeding `N` are hand to `on_dlq(
  envelope, attempts=times_delivered, error=RuntimeError(...), subscriber_id=
  "<stream>:<group>")` and `XACK`ed directly (excluded from redispatch,
  never reclaimed again). Constructing at `max_deliveries=N`
  with `delivery="broadcast"` raises `ValueError` at `__init__` (no
  PEL in broadcast mode — invalid combination, fail fast, not silently
  ignored).
- **Depends on**: none (parallel with Modules 1-3).

### Module 5: TOPICS.md registration + release
- **Path**: `TOPICS.md`, `src/navigator_eventbus/version.py`,
  `tests/test_backends_streams.py` (final regression sweep)
- **Responsibility**: Register `fieldsync.*` in `TOPICS.md` (governance —
  required before any app publishes under that namespace); bump
  `version.py`; full test suite green; tag the release; record the exact
  released kwarg signatures for downstream consumers (FieldSync FEAT-409
  Modules 6-8 pin against this).
- **Depends on**: Modules 1, 2, 3, 4.

---

## 4. Test Specification

### Unit Tests

| Test | Module | Description |
|---|---|---|
| `test_broadcast_two_instances_receive_all` | 1 | Two backend instances, same stream, broadcast mode: BOTH receive every published entry |
| `test_broadcast_no_replay_on_start` | 1 | A broadcast backend started AFTER entries already exist does not replay them (starts at `"$"`) |
| `test_broadcast_group_mode_default_unchanged` | 1 | Omitting `delivery=` behaves EXACTLY as today (existing test suite, unmodified, is this proof) |
| `test_custom_codec_roundtrip` | 2 | Custom `Codec.encode`/`decode` round-trips a non-default wire shape |
| `test_custom_stream_key_fn_with_explicit_streams` | 2 | `stream_key_fn=` + `streams=[...]` bypasses SCAN discovery and consumes the exact given streams |
| `test_stream_key_fn_and_broadcast_compose` | 1+2 | Broadcast mode + custom naming together (the FieldSync WS-path shape) |
| `test_retention_minid_trim_issued` | 3 | `XTRIM ... MINID <id>` issued at `retention_trim_interval` cadence with an id derived from `now - retention` |
| `test_retention_disables_maxlen_argument` | 3 | When `retention=` is set, `XADD` is NOT called with `maxlen=`/`approximate=` |
| `test_max_deliveries_parks_to_dlq_and_acks` | 4 | An entry redelivered > N times is routed to `on_dlq` and ACKed; NOT reclaimed again afterward |
| `test_max_deliveries_under_threshold_still_redelivers` | 4 | An entry redelivered <= N times is dispatched normally (existing behavior) |
| `test_max_deliveries_with_broadcast_raises` | 4 | `RedisStreamsBackend(..., delivery="broadcast", max_deliveries=3)` raises `ValueError` at construction |

### Integration Tests

| Test | Description |
|---|---|
| `test_end_to_end_streams_two_consumers` (existing) | Must pass UNCHANGED — group-mode parity baseline |
| `test_end_to_end_broadcast_two_instances` (new, real Redis, `integration`-marked, skips without Redis) | Real-Redis proof of broadcast fan-out |

### Test Data / Fixtures

```python
# Extend tests/test_backends_streams.py's FakeStreamsRedis (hand-rolled,
# fakeredis is not in the dependency set — matches this repo's existing
# convention) with:
async def xread(self, streams: dict, count=None, block=None): ...
    # free-read: streams={name: last_id}; supports "$" (tail) and
    # numeric ids; returns the same shape as xreadgroup's per-stream tuples
async def xpending_range(
    self, name, groupname, min="-", max="+", count=None, consumername=None,
    idle=None,
): ...
    # returns [{"message_id": ..., "consumer": ..., "time_since_delivered":
    # ..., "times_delivered": ...}, ...] — times_delivered increments each
    # time xautoclaim reclaims the same id
async def xtrim(self, name, minid=None, approximate=True): ...
```

---

## 5. Acceptance Criteria

> This feature is complete when ALL of the following are true:

- [ ] Broadcast mode: two backend instances both receive every entry
      (unit + real-Redis integration test)
- [ ] Broadcast mode does not replay pre-existing entries on start
- [ ] Codec + `stream_key_fn=` seams: custom wire format round-trips on
      custom stream names; compose correctly with broadcast mode
- [ ] `streams=` explicit override bypasses `SCAN` discovery
- [ ] `retention=` issues `XTRIM MINID` at the configured interval and
      disables the `maxlen=` `XADD` argument
- [ ] `max_deliveries` parks to `on_dlq` after N deliveries and ACKs;
      below N it redelivers as today; combined with `delivery="broadcast"`
      raises `ValueError`
- [ ] Existing `tests/test_backends_streams.py` and
      `tests/test_integration.py` suites green, UNCHANGED (proves default
      behavior parity)
- [ ] `fieldsync.*` registered in `TOPICS.md`
- [ ] `src/navigator_eventbus/version.py` bumped; release tagged
- [ ] Full suite green: `source .venv/bin/activate && pytest -x -q`
- [ ] `ruff check src/navigator_eventbus/backends/redis_streams.py` clean

---

## 6. Codebase Contract

> **CRITICAL — Anti-Hallucination Anchor**
> Verified 2026-07-25 against this repo's `main`@`8ef73b3` (tag `0.1.0`).
> Corrects an initial cross-repo assumption (FieldSync FEAT-409 TASK-701's
> Codebase Contract) that `RedisStreamsBackend` is built on
> `navigator_eventbus.brokers.redis.RedisConnection`'s free-read mode
> (`consumer_group=False`, `c2cf660`) — it is NOT. `RedisStreamsBackend`
> owns its `redis.asyncio` client directly (`_ensure_connection`,
> `redis_streams.py:220`) and is architecturally independent of
> `brokers/redis/*` (a separate subsystem, FEAT-316). Broadcast mode
> (Module 1) is implemented with plain `XREAD` on the backend's own
> client — no dependency on `RedisConnection` needed.

### Verified Imports

```python
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend
    # verified: src/navigator_eventbus/backends/redis_streams.py:67
from navigator_eventbus.backends.base import TransportBackend, OnEnvelope
    # verified: src/navigator_eventbus/backends/base.py:28,24
from navigator_eventbus.envelope import EventEnvelope
    # verified: src/navigator_eventbus/envelope.py (imported at redis_streams.py:54)
import redis.asyncio as aioredis
    # verified: redis_streams.py:49 (the backend's OWN client, not brokers/redis)
```

### Existing Class Signatures

```python
# src/navigator_eventbus/backends/redis_streams.py
class RedisStreamsBackend:                                       # line 67
    STREAM_PREFIX = DEFAULT_STREAM_PREFIX                          # line 101
    DEDUP_PREFIX = DEFAULT_DEDUP_PREFIX                            # line 102
    def __init__(self, redis_url=None, *, client=None, group=None,
                 consumer_name=None, stream_prefix=None,
                 dedup_prefix=None, dedup_ttl=86_400, block_ms=1_000,
                 batch_count=32, min_idle_time_ms=60_000,
                 autoclaim_interval=30.0, maxlen=100_000,
                 stream_refresh_interval=10.0, reconnect_base_delay=0.5,
                 reconnect_max_delay=30.0) -> None: ...             # line 104
    async def publish(self, envelope: EventEnvelope) -> None: ...  # line 162
        # XADD stream, {"envelope": json.dumps(envelope.to_dict())},
        # maxlen=self._maxlen, approximate=True
    async def start_consumer(self, on_envelope: OnEnvelope) -> None: ...  # line 178
        # spawns self._run_consumer() + self._run_sweeper() as asyncio.Task
    async def close(self) -> None: ...                             # line 193
    def _stream_for(self, topic: str) -> str: ...                  # line 216
        # f"{self.stream_prefix}{topic.split('.', 1)[0]}" — topic-CLASS sharding
    async def _ensure_connection(self) -> None: ...                 # line 220
        # builds self._redis = aioredis.from_url(...) OR uses injected self._client
    async def _ensure_group(self, stream: str) -> None: ...         # line 231
        # xgroup_create(stream, self._group, id="0", mkstream=True); BUSYGROUP swallowed
    async def _refresh_streams(self) -> None: ...                   # line 245
        # SCAN match=f"{self.stream_prefix}*"; joins group for each newly found stream
    async def _run_consumer(self) -> None: ...                      # line 252
        # XREADGROUP loop; degraded-mode reconnect+backoff on exception
    async def _run_sweeper(self) -> None: ...                       # line 299
        # periodic XAUTOCLAIM per self._streams; redispatches reclaimed entries
    async def _handle_message(self, stream, msg_id, fields) -> None: ...  # line 328
        # decode {"envelope": ...} JSON; dedup-set check/mark; on_envelope();
        # XACK on success OR on decode failure (poison-drop); left pending on
        # on_envelope() exception (see §2 "Why not reuse BusCore's DLQ")
    async def _ack(self, stream, msg_id) -> None: ...                # line 386

# src/navigator_eventbus/backends/base.py
class TransportBackend(Protocol):                                  # line 28
    async def publish(self, envelope: EventEnvelope) -> None: ...  # line 37
    async def start_consumer(self, on_envelope: OnEnvelope) -> None: ...  # line 41
    async def close(self) -> None: ...                              # line 45
OnEnvelope = Callable[[EventEnvelope], Awaitable[None]]              # line 24

# src/navigator_eventbus/core.py (reference only — NOT modified by this feature)
class BusCore:                                                      # line 92
    async def _on_transport_envelope(self, envelope: EventEnvelope) -> None: ...  # line 358
        # "Handler-level failures remain isolated (model B: retry -> DLQ)
        #  and therefore count as processed." (docstring, line 374)
    async def _dispatch(self, envelope: EventEnvelope) -> None: ...  # line 511
    async def _invoke_with_retry(self, sub, envelope) -> None: ...   # line 549
        # NEVER raises — retries in-process, then _invoke_dlq(), then returns

# src/navigator_eventbus/dlq.py (reference shape for on_dlq= callback)
class DLQHandler:                                                   # line 110
    async def on_dlq(self, envelope: EventEnvelope, *, attempts: int,
                      error: BaseException, subscriber_id: str) -> None: ...  # line 175
```

### Integration Points

| New Component | Connects To | Via | Verified At |
|---|---|---|---|
| `delivery="broadcast"` reader loop | backend's own `self._redis` client | plain `XREAD` (no group) | `redis_streams.py:220` (`_ensure_connection`) |
| `codec.encode/decode` | `publish()` / `_handle_message()` | replaces the hard-coded JSON-in-`envelope`-field shape | `redis_streams.py:162,171,347-350` |
| `stream_key_fn` / `streams=` | `_stream_for()` / `_refresh_streams()` | replaces topic-class sharding + SCAN discovery | `redis_streams.py:216,245` |
| `retention=` trimmer task | `publish()`'s XADD kwargs | disables `maxlen=`/`approximate=`, runs periodic `XTRIM MINID` instead | `redis_streams.py:171-176` |
| `max_deliveries` / `on_dlq` | `_run_sweeper()` | `XPENDING IDLE` delivery-count check before `XAUTOCLAIM` redispatch | `redis_streams.py:299-326` |

### Does NOT Exist (Anti-Hallucination)

- ~~`RedisStreamsBackend` composes `navigator_eventbus.brokers.redis.RedisConnection`~~ — it does NOT; it owns `redis.asyncio` directly (see header note above). This corrects FieldSync FEAT-409 TASK-701's initial assumption.
- ~~`delivery=`, `codec=`, `stream_key_fn=`, `streams=`, `retention=`, `retention_trim_interval=`, `max_deliveries=`, `on_dlq=` kwargs~~ — none exist yet; this feature creates all of them.
- ~~Delivery-count tracking anywhere in the current sweeper~~ — `_run_sweeper` (`redis_streams.py:299`) redispatches every reclaimed entry unconditionally, forever; no `XPENDING` call exists today.
- ~~`fakeredis` as an installed test dependency~~ — `tests/test_backends_streams.py` uses a hand-rolled `FakeStreamsRedis` (see its own docstring); extend that class, do not add a `fakeredis` dependency.
- ~~`BusCore`'s `on_dlq`/`retry_attempts` mechanism reused by this feature~~ — see §2 "Why not reuse BusCore's DLQ"; this feature's `on_dlq=` is a SEPARATE, backend-level, Redis-PEL-delivery-count-driven callback, not a call into `core.py`.

---

## 7. Implementation Notes & Constraints

### Patterns to Follow

- Keep `RedisStreamsBackend` self-contained (own `redis.asyncio` client) —
  do NOT introduce a dependency on `navigator_eventbus.brokers.redis.*`.
- Every new kwarg defaults to preserving TODAY's exact behavior — write
  the "old" code path test FIRST for each module, confirm it still passes
  unmodified, THEN add the new path.
- Degraded-mode reconnect+backoff (`_run_consumer`'s existing pattern,
  `redis_streams.py:252-297`) is the template for the NEW broadcast reader
  loop's own reconnect handling.
- `self.logger` (already `logging.getLogger("navigator_eventbus.backends.
  redis_streams")`, `redis_streams.py:156`) for all new log lines.
- Google-style docstrings + strict type hints (repo standard).

### Known Risks / Gotchas

- **Broadcast mode has no PEL** — `max_deliveries` + `delivery="broadcast"`
  MUST raise at construction, not silently no-op (Module 4).
- **`streams=` + custom `stream_key_fn=` invalidates SCAN discovery** — if
  a caller sets `stream_key_fn=` WITHOUT `streams=`, discovery has nothing
  to match against (custom names rarely share the `<stream_prefix>*`
  shape); document this as a required pairing in the module docstring
  (not enforced by construction — a caller COULD intentionally keep
  `stream_prefix`-shaped custom naming, so do not hard-raise, just log a
  warning if `stream_key_fn` is set and `streams` is empty when consumption
  starts).
- **Exactly-one-trimmer** is unchanged from today's `maxlen` principle —
  document it for `retention=` too; this feature does not add distributed
  locking around trimming.
- **`XPENDING` extra round-trip cost**: only queried right before an
  `XAUTOCLAIM` sweep when `max_deliveries` is set (zero overhead for
  everyone else).
- **Backward compatibility is the acceptance gate**: every existing test
  in `tests/test_backends_streams.py` must pass BYTE-FOR-BYTE unmodified.

### External Dependencies

| Package | Version | Reason |
|---|---|---|
| `redis` (redis.asyncio) | already present | same client the backend already owns |

---

## 8. Open Questions

- [x] Does `RedisStreamsBackend` compose `RedisConnection`'s free-read
  mode? — *Resolved by this repo's verified code*: NO, it owns its own
  client; broadcast mode uses plain `XREAD` directly, no cross-module
  dependency.
- [x] Should `max_deliveries` reuse `BusCore`'s existing retry/DLQ? —
  *Resolved*: NO — different failure mode, different layer (see §2).
- [ ] Exact FEAT-409 (FieldSync) consumption shape (codec content, stream
  names) is FieldSync's own concern — not designed here, only the SEAMS
  are. — *Owner: Jesús Lara (cross-repo, tracked in FieldSync's own spec)*.

---

## Revision History

| Version | Date | Author | Change |
|---|---|---|---|
| 0.1 | 2026-07-25 | Jesús Lara | Initial draft — decomposed from FieldSync FEAT-409 TASK-701 (cross-repo), corrected against this repo's verified `RedisStreamsBackend`/`BusCore` architecture (self-contained client; DLQ via Redis PEL delivery count, not BusCore's in-process retry) |
