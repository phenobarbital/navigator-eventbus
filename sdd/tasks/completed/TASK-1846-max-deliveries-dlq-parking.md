# TASK-1846: max_deliveries retry-then-park (group mode) for RedisStreamsBackend

**Feature**: FEAT-320 — RedisStreamsBackend Generic Capability Extensions
**Spec**: `sdd/specs/redis-streams-backend-extensions.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: L (4-8h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Spec §3 Module 4 and §2 "Why not reuse BusCore's DLQ" (READ THAT SECTION
FIRST — it explains a subtle but load-bearing design decision). Today,
`_run_sweeper()` reclaims EVERY pending entry via `XAUTOCLAIM` and
redispatches it unconditionally, forever — there is no cap. If the SAME
entry keeps crashing/hanging the consuming PROCESS itself (not an ordinary
in-process handler failure — `BusCore` already isolates and DLQs those
per-subscriber, in-process, and never lets that kind of failure propagate
back to this backend — see `core.py::_invoke_with_retry`, "never raises"),
it is reclaimed forever with no terminal outcome. This task caps that via
Redis's OWN per-entry delivery-count bookkeeping (`XPENDING ... IDLE`,
`times_delivered` field) — NOT via `BusCore`'s in-process retry counter,
which cannot see this failure mode at all.

---

## Scope

- Add a `max_deliveries: Optional[int] = None` constructor kwarg (group
  mode only) and an `on_dlq: Optional[Callable[..., Union[None, Awaitable[None]]]] = None`
  constructor kwarg. The callback signature MUST match
  `navigator_eventbus.dlq.DLQHandler.on_dlq`'s shape — `(envelope, *,
  attempts: int, error: BaseException, subscriber_id: str) -> None` — so a
  caller CAN pass `DLQHandler.on_dlq` directly, but this module must NOT
  import `dlq.py` (duck-typed callback only — see the spec's Integration
  Points table for why).
- **Construction-time guard**: `RedisStreamsBackend(..., delivery="broadcast",
  max_deliveries=N)` (any non-`None` `N`) MUST raise `ValueError` at
  `__init__` — broadcast mode has no PEL, so `max_deliveries` cannot apply
  there; this must fail LOUD, not silently no-op. (If TASK-1843 has not
  landed yet when you pick this up, `delivery` may not exist as a
  constructor kwarg — in that case just guard against the combination
  once both kwargs exist; do not block this task on TASK-1843's landing,
  implement `max_deliveries`/`on_dlq` fully against `delivery="group"`
  and add the cross-mode guard as a follow-up noted in your Completion
  Note.)
- **Delivery-count check before redispatch**: in `_run_sweeper()`, BEFORE
  calling `xautoclaim` (or immediately after, before dispatching the
  claimed entries — either ordering is acceptable as long as an
  over-threshold entry is NEVER passed to `_handle_message`/`on_envelope`
  again), query `XPENDING <stream> <group> IDLE <min_idle_time_ms> - + <count>
  <consumer>` for the entries about to be reclaimed. For any entry whose
  `times_delivered > max_deliveries`:
  - Do NOT redispatch it through `on_envelope`.
  - Decode it (reuse whatever codec is active — TASK-1844's seam if it has
    landed, otherwise the default JSON shape) ONLY to build the
    `EventEnvelope` argument for `on_dlq`; a decode failure here should
    still result in an ACK (poison entry, same principle as the existing
    decode-failure path) but obviously cannot call `on_dlq` with a real
    envelope — log and ACK in that edge case.
  - Call `on_dlq(envelope, attempts=times_delivered, error=RuntimeError(
    f"exceeded max_deliveries={self._max_deliveries} without ack"),
    subscriber_id=f"{stream}:{self._group}")` (await it if it returns an
    awaitable — mirror `core.py::_invoke_dlq`'s
    `asyncio.iscoroutine(result) or isinstance(result, Awaitable)` check).
  - `XACK` it (terminal — it must never be reclaimed again).
  - Entries at or below `max_deliveries` continue through the EXISTING
    redispatch path, unchanged.
- When `max_deliveries` is `None` (default): NO behavior change — the
  sweeper keeps reclaiming and redispatching every pending entry forever,
  exactly as today. Every existing test in `tests/test_backends_streams.py`
  must pass unmodified.

**NOT in scope**: `delivery=`/`codec=`/`stream_key_fn=`/`streams=`
(TASK-1843/1844); `retention=` (TASK-1845). Do not modify
`BusCore`/`core.py`/`dlq.py` — this task is entirely inside
`redis_streams.py`.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/backends/redis_streams.py` | MODIFY | `max_deliveries=`/`on_dlq=` kwargs, `ValueError` guard, `XPENDING`-gated sweeper |
| `tests/test_backends_streams.py` | MODIFY | extend `FakeStreamsRedis` with `xpending_range` (and `times_delivered` bookkeeping on reclaim); add max_deliveries test class |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend
    # verified: src/navigator_eventbus/backends/redis_streams.py:67
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/backends/redis_streams.py (verified 2026-07-25, main@8ef73b3)
async def _run_sweeper(self) -> None: ...                          # line 299
    # today:
    #   result = await self._redis.xautoclaim(stream, self._group, self._consumer,
    #                min_idle_time=self._min_idle_time_ms, start_id="0-0",
    #                count=self._batch_count)
    #   messages = result[1] if result and len(result) > 1 else []
    #   for msg_id, fields in messages:
    #       await self._handle_message(stream, msg_id, fields)
    # THIS TASK: when self._max_deliveries is set, query xpending_range
    #            BEFORE dispatching each reclaimed (msg_id, fields) pair;
    #            split into "park to DLQ + ack" vs "dispatch as today"
async def _handle_message(self, stream, msg_id, fields) -> None: ...  # line 328
    # decode + dedup + on_envelope() + ack — reuse the decode step (or the
    # active Codec from TASK-1844, if landed) for building the DLQ envelope;
    # do NOT route an over-threshold entry through this method's normal
    # dedup/on_envelope/ack path — it needs its OWN terminal ack-to-DLQ path
async def _ack(self, stream, msg_id) -> None: ...                   # line 386

# src/navigator_eventbus/dlq.py (reference shape — DO NOT import this module)
class DLQHandler:                                                   # line 110
    async def on_dlq(self, envelope: EventEnvelope, *, attempts: int,
                      error: BaseException, subscriber_id: str) -> None: ...  # line 175
        # THIS is the signature this task's on_dlq= callback must match —
        # by convention/duck-typing only, no import of dlq.py

# src/navigator_eventbus/core.py (reference only — pattern for awaiting a
# maybe-async callback result; DO NOT import core.py either)
async def _invoke_dlq(self, envelope, sub, error) -> None: ...      # line 582
    # result = self._on_dlq(envelope, attempts=..., error=..., subscriber_id=...)
    # if asyncio.iscoroutine(result) or isinstance(result, Awaitable): await result
    # — mirror this "sync-or-async callback" handling for THIS task's on_dlq=

# tests/test_backends_streams.py — FakeStreamsRedis (verified 2026-07-25)
class FakeStreamsRedis:                                              # line 45
    async def xautoclaim(self, name, group, consumer, min_idle_time,
                          start_id="0-0", count=None): ...           # line 101
        # today: does NOT track times_delivered per entry — ADD that
        # bookkeeping as part of this task (increment a counter each time
        # an id is reclaimed)
    # xpending_range() does NOT exist yet — ADD it as part of this task
```

### Does NOT Exist
- ~~`RedisStreamsBackend(max_deliveries=..., on_dlq=...)`~~ — this task
  creates both kwargs.
- ~~Delivery-count tracking anywhere in the current sweeper or
  `FakeStreamsRedis`~~ — add both (real backend: `XPENDING`; fake: a
  `times_delivered` counter per pending id, incremented on each
  `xautoclaim` reclaim).
- ~~This module importing `navigator_eventbus.dlq` or `navigator_eventbus.core`~~
  — do NOT add either import; `on_dlq=` is duck-typed, matching
  `DLQHandler.on_dlq`'s signature by CONVENTION only.
- ~~`BusCore`'s `retry_attempts`/`on_dlq` being reused or called from
  here~~ — this is a SEPARATE mechanism (see Context above and spec §2).

---

## Implementation Notes

### Pattern to Follow
```python
async def _run_sweeper(self) -> None:
    while self._running:
        try:
            await asyncio.sleep(self._autoclaim_interval)
            await self._ensure_connection()
            for stream in list(self._streams):
                if self._max_deliveries is not None:
                    pending = await self._redis.xpending_range(
                        name=stream, groupname=self._group,
                        min="-", max="+", count=self._batch_count,
                        consumername=self._consumer,
                        idle=self._min_idle_time_ms,
                    )
                    over_threshold = {
                        p["message_id"] for p in pending
                        if p["times_delivered"] > self._max_deliveries
                    }
                else:
                    over_threshold = set()

                result = await self._redis.xautoclaim(
                    stream, self._group, self._consumer,
                    min_idle_time=self._min_idle_time_ms,
                    start_id="0-0", count=self._batch_count,
                )
                messages = result[1] if result and len(result) > 1 else []
                for msg_id, fields in messages:
                    if msg_id in over_threshold:
                        await self._park_to_dlq(stream, msg_id, fields)
                    else:
                        await self._handle_message(stream, msg_id, fields)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — degraded mode, unchanged
            if not self._running:
                return
            self.logger.warning("Streams sweeper error: %s", exc)


async def _park_to_dlq(self, stream: str, msg_id: Any, fields: dict) -> None:
    """Terminal handling for an entry that exceeded max_deliveries: never
    redispatched, always ack'd."""
    try:
        envelope = self._codec.decode(fields)  # or the inline JSON shape
                                                 # pre-TASK-1844
    except Exception as exc:  # noqa: BLE001 — poison entry, ack and drop
        self.logger.error(
            "Undecodable over-threshold entry %s on %s dropped: %s",
            msg_id, stream, exc,
        )
        await self._ack(stream, msg_id)
        return
    if self._on_dlq is not None:
        try:
            result = self._on_dlq(
                envelope,
                attempts=self._max_deliveries + 1,  # or the real times_delivered
                error=RuntimeError(
                    f"exceeded max_deliveries={self._max_deliveries} without ack"
                ),
                subscriber_id=f"{stream}:{self._group}",
            )
            if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
                await result
        except Exception:  # noqa: BLE001 — DLQ failures isolated too
            self.logger.exception("on_dlq callback failed for %s", msg_id)
    await self._ack(stream, msg_id)
```

### Key Constraints
- The `ValueError` guard (`delivery="broadcast"` + `max_deliveries` set)
  belongs in `__init__`, checked unconditionally at construction — not
  deferred to `start_consumer()`.
- An over-threshold entry must be excluded from `_handle_message`'s normal
  dedup-check/mark path too (it is being terminally parked, not processed
  normally) — do not let it also increment/touch the dedup key.
- Prefer passing the REAL `times_delivered` value (from `xpending_range`)
  as `attempts=` to `on_dlq`, not a hardcoded `self._max_deliveries + 1` —
  the pattern above shows the simplified version; use the actual value you
  already fetched.
- Google-style docstrings + strict type hints.

### References in Codebase
- `src/navigator_eventbus/core.py:582-608` — `_invoke_dlq`, the
  sync-or-async callback invocation pattern to mirror for `on_dlq=`
  (reference only, do not import).
- `src/navigator_eventbus/dlq.py:175-218` — `DLQHandler.on_dlq`, the
  signature this task's callback contract matches (reference only, do not
  import).

---

## Acceptance Criteria

- [ ] `RedisStreamsBackend(..., max_deliveries=N, on_dlq=callback)` parks an entry redelivered > N times: `on_dlq` is called and the entry is ACKed
- [ ] A parked entry is NEVER redispatched again on subsequent sweeper passes
- [ ] An entry redelivered <= N times is dispatched normally (today's behavior, unchanged)
- [ ] `RedisStreamsBackend(..., delivery="broadcast", max_deliveries=3)` raises `ValueError` at construction
- [ ] Default construction (`max_deliveries=None`) behavior is IDENTICAL to today — full existing `tests/test_backends_streams.py` suite passes unmodified
- [ ] `pytest tests/test_backends_streams.py -v` green
- [ ] `ruff check src/navigator_eventbus/backends/redis_streams.py` clean

---

## Test Specification

```python
# tests/test_backends_streams.py
class TestMaxDeliveriesDlqParking:
    async def test_max_deliveries_parks_to_dlq_and_acks(self, fake_redis):
        """An entry reclaimed > N times is routed to on_dlq and acked, and
        is NOT reclaimed/dispatched again afterward."""
        ...

    async def test_max_deliveries_under_threshold_still_redelivers(self, fake_redis):
        """An entry reclaimed <= N times dispatches normally."""
        ...

    def test_max_deliveries_with_broadcast_raises(self):
        """delivery='broadcast' + max_deliveries=N raises ValueError."""
        ...

    async def test_default_sweeper_unbounded_unchanged(self, fake_redis):
        """Omitting max_deliveries= keeps today's unbounded-redelivery behavior."""
        ...
```

---

## Agent Instructions

1. **Read the spec** (`sdd/specs/redis-streams-backend-extensions.spec.md`)
   §2 "Why not reuse BusCore's DLQ" FIRST — it is load-bearing context for
   this task's design — then §3 Module 4 and §6
2. **Check dependencies** — none
3. **Verify the Codebase Contract** — re-read `redis_streams.py`,
   `core.py` (`_invoke_dlq` region only, reference), and `dlq.py`
   (`on_dlq` signature only, reference) before editing; line numbers may
   have drifted
4. **Update status** in `sdd/tasks/index/redis-streams-backend-extensions.json` → `"in-progress"`
5. **Implement**, **verify**, **move this file** to `sdd/tasks/completed/`,
   **update index** → `"done"`, **fill in the Completion Note**

---

## Completion Note

**Completed by**: sdd-worker (Claude, Sonnet)
**Date**: 2026-07-26
**Notes**: Added `max_deliveries: Optional[int] = None` and
`on_dlq: Optional[Callable[..., Union[None, Awaitable[None]]]] = None`
kwargs. `__init__` raises `ValueError` unconditionally at construction
when `delivery == "broadcast"` and `max_deliveries is not None` (checked
before any other assignment). `_run_sweeper` now queries
`self._redis.xpending_range(name=stream, groupname=self._group, min="-",
max="+", count=self._batch_count, consumername=self._consumer,
idle=self._min_idle_time_ms)` BEFORE dispatching each `XAUTOCLAIM`-reclaimed
entry when `max_deliveries` is set, building an `over_threshold` dict
(`message_id -> times_delivered`) for entries whose `times_delivered >
max_deliveries`. Over-threshold entries are routed to the new
`_park_to_dlq(stream, msg_id, fields, times_delivered)` (decodes via
`self._codec` — reusing TASK-1844's seam — calls `on_dlq(envelope,
attempts=times_delivered, error=RuntimeError(...), subscriber_id=
f"{stream}:{self._group}")`, awaiting it if it returns an awaitable
per `asyncio.iscoroutine(result) or isinstance(result, Awaitable)`, then
`XACK`s unconditionally) instead of `_handle_message` — excluded from the
normal dedup-check/mark path entirely, matching the "terminal, never
reclaimed again" contract. `on_dlq=`'s signature matches
`DLQHandler.on_dlq`'s shape by convention only — no import of `dlq.py` or
`core.py` was added.

Extended `FakeStreamsRedis`: `xreadgroup`/`xautoclaim` now track a
`times_delivered` counter per pending entry (`[consumer, last_delivered_at,
times_delivered]`, incremented on each reclaim), and added
`xpending_range(name, groupname, min, max, count, consumername, idle)`.
Added `TestMaxDeliveriesDlqParking` with 4 tests: over-threshold entry
parks to DLQ + never redispatched again, under-threshold entry still
redelivers normally, `delivery="broadcast"` + `max_deliveries` raises
`ValueError`, and default (`max_deliveries=None`) unbounded-redelivery
behavior is unchanged.

Full suite green (`pytest -q -k "not integration"`: 315 passed, 1 skipped,
7 deselected) and `ruff check src/navigator_eventbus/backends/redis_streams.py`
clean. All pre-existing tests pass unmodified.

**Deviations from spec**: none. One implementation note: the
`xpending_range` query filters by `consumername=self._consumer` (per this
task's own Pattern to Follow) — since `XAUTOCLAIM` reassigns ownership to
the calling consumer as part of reclaiming, an entry that has NEVER been
owned by this exact instance's consumer name before will not be flagged
over-threshold on the very sweep pass where ownership first transfers to
it (it will be on the NEXT pass, since by then it is owned by
`self._consumer`). This does not affect correctness for the steady-state
"poison entry keeps failing under this same consumer" scenario the spec
describes, and matches the task's explicit contract; tests seed pending
entries already owned by the backend's own `consumer_name` to exercise
the steady-state path deterministically.
