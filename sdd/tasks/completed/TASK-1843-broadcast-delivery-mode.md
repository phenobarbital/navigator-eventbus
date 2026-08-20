# TASK-1843: Broadcast delivery mode for RedisStreamsBackend

**Feature**: FEAT-320 — RedisStreamsBackend Generic Capability Extensions
**Spec**: `sdd/specs/redis-streams-backend-extensions.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: L (4-8h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Spec §3 Module 1. `RedisStreamsBackend` only supports consumer-GROUP
delivery (`XREADGROUP`/`XACK`/`XAUTOCLAIM`) today — every entry is
delivered to exactly one consumer in the group (competing consumers). A
group-less BROADCAST mode (every instance receives every entry) is needed
for fan-out use cases (e.g. WS/socket delivery in a consuming app). This
task adds `delivery="broadcast"` as an opt-in alternative; `delivery="group"`
(the implicit default today) must remain byte-for-byte unchanged.

---

## Scope

- Add a `delivery: Literal["group", "broadcast"] = "group"` constructor
  kwarg.
- When `delivery == "broadcast"`:
  - Skip `_ensure_group()` / `xgroup_create` entirely for this backend
    instance.
  - Maintain a per-stream last-delivered-id cursor (`self._last_ids: dict[str, str]`),
    starting each newly-discovered stream at `"$"` (tail — no replay of
    pre-existing entries on start).
  - Replace the `XREADGROUP` consumer loop with a free-read `XREAD BLOCK`
    loop over `self._streams` (reuse `_refresh_streams()`'s SCAN-based
    discovery unchanged — broadcast mode is naming-scheme agnostic; it only
    changes the CONSUME mechanism, not discovery).
  - Dispatch every entry directly to `self._on_envelope` (no dedup-set
    check/mark — broadcast means every instance is SUPPOSED to see every
    entry; dedup collapsing does not apply here).
  - No `XACK`, no sweeper/`XAUTOCLAIM` task in this mode — there is no PEL
    in group-less consumption. `start_consumer()` must NOT spawn
    `_run_sweeper()` when `delivery == "broadcast"`.
  - `close()` must cleanly cancel whichever task(s) are actually running
    for the active mode (do not assume both `_consumer_task` and
    `_sweeper_task` exist).
- When `delivery == "group"` (default): NO behavior change whatsoever —
  every existing test in `tests/test_backends_streams.py` must pass
  unmodified.
- Reuse the EXISTING degraded-mode reconnect+backoff pattern
  (`_run_consumer`, `redis_streams.py:252-297`) as the template for the new
  broadcast reader loop's own reconnect handling.

**NOT in scope**: `codec=`/`stream_key_fn=`/`streams=` (TASK-1844);
`retention=` (TASK-1845); `max_deliveries=` (TASK-1846, which explicitly
does not apply to broadcast mode — that task adds the `ValueError` guard,
not this one). Do not implement `stream_key_fn=` here even though the
integration test `test_stream_key_fn_and_broadcast_compose` (spec §4)
exercises both — that test is written once TASK-1844 lands; this task only
needs broadcast mode to work correctly with the DEFAULT topic-class naming.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/backends/redis_streams.py` | MODIFY | `delivery=` kwarg, broadcast reader loop, `start_consumer`/`close` branching |
| `tests/test_backends_streams.py` | MODIFY | extend `FakeStreamsRedis` with `xread`; add broadcast-mode test class |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend
    # verified: src/navigator_eventbus/backends/redis_streams.py:67
import redis.asyncio as aioredis
    # verified: redis_streams.py:49 — the backend owns this client directly
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/backends/redis_streams.py (verified 2026-07-25, main@8ef73b3)
class RedisStreamsBackend:                                       # line 67
    def __init__(self, redis_url=None, *, client=None, group=None,
                 consumer_name=None, stream_prefix=None, dedup_prefix=None,
                 dedup_ttl=86_400, block_ms=1_000, batch_count=32,
                 min_idle_time_ms=60_000, autoclaim_interval=30.0,
                 maxlen=100_000, stream_refresh_interval=10.0,
                 reconnect_base_delay=0.5, reconnect_max_delay=30.0) -> None  # line 104
    async def start_consumer(self, on_envelope: OnEnvelope) -> None: ...    # line 178
        # today: self._consumer_task = asyncio.create_task(self._run_consumer());
        #        self._sweeper_task = asyncio.create_task(self._run_sweeper())
    async def close(self) -> None: ...                                      # line 193
        # today: cancels BOTH self._consumer_task and self._sweeper_task
        #        unconditionally (both are always set today — broadcast
        #        mode must only cancel what it actually started)
    def _stream_for(self, topic: str) -> str: ...                          # line 216
    async def _ensure_connection(self) -> None: ...                        # line 220
        # self._redis = self._client OR aioredis.from_url(self.redis_url, decode_responses=True)
    async def _ensure_group(self, stream: str) -> None: ...                # line 231
        # SKIP entirely in broadcast mode — no group needed
    async def _refresh_streams(self) -> None: ...                          # line 245
        # SCAN match=f"{self.stream_prefix}*" — REUSE unchanged in broadcast mode
    async def _run_consumer(self) -> None: ...                             # line 252
        # XREADGROUP loop + reconnect/backoff pattern — TEMPLATE for the new
        # broadcast reader (same backoff shape: self._reconnect_base_delay /
        # self._reconnect_max_delay, same try/except Exception structure)
    async def _handle_message(self, stream, msg_id, fields) -> None: ...   # line 328
        # decode + dedup-check/mark + on_envelope() + XACK — broadcast mode
        # needs its OWN dispatch path (no dedup, no XACK); do not reuse
        # _handle_message() as-is, or extract a shared decode-only helper
        # if you prefer (either is acceptable; do not skip dedup by adding
        # a flag that silently changes group-mode's dedup behavior)

# tests/test_backends_streams.py — FakeStreamsRedis (verified 2026-07-25)
class FakeStreamsRedis:                                                     # line 45
    async def xadd(self, name, fields, maxlen=None, approximate=True): ...  # line 54
    async def xgroup_create(self, name, group, id="0", mkstream=False): ... # line 63
    async def xreadgroup(self, group, consumer, streams, count=None, block=None): ...  # line 73
    async def scan_iter(self, match=None): ...                              # line 131
    # xread() does NOT exist yet — ADD it as part of this task (see Scope)
```

### Does NOT Exist
- ~~`RedisStreamsBackend(delivery=...)`~~ — this task creates the kwarg.
- ~~`FakeStreamsRedis.xread()`~~ — add it (broadcast mode needs it; the
  fake currently only has `xreadgroup`).
- ~~A separate PEL/pending-tracking structure for broadcast mode~~ — there
  is none; group-less consumption has no PEL by definition.
- ~~`navigator_eventbus.brokers.redis.RedisConnection(consumer_group=False)`
  involvement~~ — NOT used; broadcast mode reads directly off
  `self._redis` (or `self._client` if injected), exactly like every other
  method on this class.

---

## Implementation Notes

### Pattern to Follow
```python
# Broadcast reader loop skeleton — mirror _run_consumer's reconnect shape
# (redis_streams.py:252-297), swap XREADGROUP for XREAD, drop group/ack:
async def _run_broadcast(self) -> None:
    delay = self._reconnect_base_delay
    last_refresh = 0.0
    while self._running:
        try:
            await self._ensure_connection()
            loop_now = asyncio.get_running_loop().time()
            if not self._streams or loop_now - last_refresh >= self._stream_refresh_interval:
                await self._refresh_streams_broadcast()  # like _refresh_streams,
                    # but seed self._last_ids["$"] for any newly discovered stream
                last_refresh = loop_now
            if not self._streams:
                await asyncio.sleep(self._block_ms / 1000)
                continue
            results = await self._redis.xread(
                {stream: self._last_ids[stream] for stream in self._streams},
                count=self._batch_count,
                block=self._block_ms,
            )
            delay = self._reconnect_base_delay
            for stream, messages in results or []:
                stream_name = stream.decode() if isinstance(stream, bytes) else stream
                for msg_id, fields in messages:
                    self._last_ids[stream_name] = msg_id
                    await self._dispatch_broadcast_entry(stream_name, msg_id, fields)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — degraded mode, same as _run_consumer
            if not self._running:
                return
            self.logger.warning(...)
            if self._client is None:
                self._redis = None
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._reconnect_max_delay)
```

### Key Constraints
- `_ensure_group()` (and anything group-specific) must NEVER be called in
  broadcast mode.
- Do not spawn `_sweeper_task` when `delivery == "broadcast"` — `close()`
  must not raise/hang trying to cancel a task that was never created
  (guard with `if self._sweeper_task is not None`).
- `"$"` as the starting id for a stream discovered AFTER broadcast reading
  begins must mean "only entries published from now on" — verify against
  redis-py's `xread` semantics (`{stream: "$"}` blocks for NEW entries
  only).
- Group-mode (`delivery="group"`, the default) behavior, including every
  existing method's exact call shape, MUST NOT change.

### References in Codebase
- `src/navigator_eventbus/backends/redis_streams.py:252-297` — `_run_consumer`,
  the reconnect/backoff pattern to mirror.
- `src/navigator_eventbus/backends/redis_streams.py:245-250` — `_refresh_streams`,
  the SCAN discovery to reuse (adapt only for last-id seeding).

---

## Acceptance Criteria

- [ ] `RedisStreamsBackend(..., delivery="broadcast")` constructs and starts without error
- [ ] Two broadcast-mode backend instances on the same stream(s) BOTH receive every published entry
- [ ] A broadcast backend started after entries already exist does NOT replay them
- [ ] No `XACK`/`XAUTOCLAIM` calls happen in broadcast mode (assert via the fake's call tracking)
- [ ] `delivery="group"` (default, omitted) behavior is IDENTICAL to today — full existing `tests/test_backends_streams.py` suite passes unmodified
- [ ] `close()` works correctly in both modes (no `AttributeError`/hang from an unset task)
- [ ] `pytest tests/test_backends_streams.py -v` green
- [ ] `ruff check src/navigator_eventbus/backends/redis_streams.py` clean

---

## Test Specification

```python
# tests/test_backends_streams.py — extend FakeStreamsRedis first:
async def xread(self, streams: dict, count=None, block=None):
    """Free-read: streams={name: last_id}. Supports "$" (tail-only) and
    numeric ids. Returns the same [(stream, [(id, fields), ...]), ...]
    shape as xreadgroup."""
    ...

class TestBroadcastDeliveryMode:
    async def test_broadcast_two_instances_receive_all(self, fake_redis):
        """Two broadcast backends on the same stream both see every entry."""
        ...

    async def test_broadcast_no_replay_on_start(self, fake_redis):
        """Entries published BEFORE start_consumer() are not replayed."""
        ...

    async def test_broadcast_group_mode_default_unchanged(self, fake_redis):
        """Omitting delivery= behaves exactly like today (no group/ack change)."""
        ...

    async def test_broadcast_no_ack_or_autoclaim_calls(self, fake_redis):
        """Broadcast mode never calls xack/xautoclaim."""
        ...
```

---

## Agent Instructions

1. **Read the spec** (`sdd/specs/redis-streams-backend-extensions.spec.md`) §2 and §6 for full context
2. **Check dependencies** — none
3. **Verify the Codebase Contract** — re-read `redis_streams.py` and
   `tests/test_backends_streams.py` before editing; line numbers may have
   drifted since 2026-07-25
4. **Update status** in `sdd/tasks/index/redis-streams-backend-extensions.json` → `"in-progress"`
5. **Implement**, **verify**, **move this file** to `sdd/tasks/completed/`,
   **update index** → `"done"`, **fill in the Completion Note**

---

## Completion Note

**Completed by**: sdd-worker (Claude, Sonnet)
**Date**: 2026-07-26
**Notes**: Implemented `delivery="group"|"broadcast"` on `RedisStreamsBackend`.
Added `_run_broadcast` (mirrors `_run_consumer`'s reconnect/backoff shape,
swaps `XREADGROUP` for `XREAD`), `_refresh_streams_broadcast` (SCAN
discovery seeding `self._last_ids[stream] = "$"` for newly found streams,
no group join), and `_dispatch_broadcast_entry` (no dedup, no ACK).
Extracted a shared `_decode_envelope` helper used by both
`_handle_message` (group mode) and `_dispatch_broadcast_entry` (broadcast
mode) to avoid duplicating the default wire-decode logic — this will be
superseded by TASK-1844's `codec=` seam. `start_consumer`/`close` now
branch on `self._delivery`, guarding every task cancellation with
`is not None` (group mode still creates `_consumer_task` +
`_sweeper_task`; broadcast mode only `_broadcast_task`).

Extended `FakeStreamsRedis` with an `xread()` method supporting `"$"`
(tail, resolved fresh per call so entries appended during a call's
simulated block window are still caught — matching real Redis XREAD
semantics) and numeric-id cursors (via a monotonic per-entry sequence
number, order-independent of list position/trims). Added
`TestBroadcastDeliveryMode` with 4 tests covering fan-out to two
instances, no-replay-on-start, default (group) mode parity, and
no-ack/no-autoclaim in broadcast mode.

Full suite green (`pytest -q -k "not integration"`: 302 passed, 1 skipped,
7 deselected) and `ruff check src/navigator_eventbus/backends/redis_streams.py`
clean. All pre-existing tests in `tests/test_backends_streams.py` pass
unmodified (only additive changes).

**Deviations from spec**: none. One test-design note: broadcast-mode
discovery of a stream that is created (first `XADD`) concurrently with
(but before) the reader's first SCAN pass will treat that first entry as
"pre-existing" and skip it (by design — "no replay on start" applies to
whatever already exists at discovery time). The unit tests reflect the
intended usage pattern (stream pre-exists or a brief settle window before
publishing) rather than a race between stream creation and first
discovery; this matches real Redis `XREAD $` semantics (same limitation
exists server-side) and is not a defect introduced by this task.

**POST-REVIEW UPDATE** (2026-07-26, `code-reviewer` agent): two real
issues found and fixed in this task's own surface area:

1. **Recurring "$" polling race** (beyond the one-time discovery race
   already noted above): `self._last_ids[stream]` stayed the literal
   string `"$"` until a real entry was ever delivered, and every empty
   poll re-sent that literal sentinel to `XREAD`. Real Redis resolves
   `"$"` FRESH at each command invocation — an entry published in the gap
   between one non-matching poll returning and the next one starting
   would have its arrival permanently skipped (no PEL/redelivery in
   broadcast mode to fall back on), for as long as a stream stays quiet
   after discovery. Fixed with a new `_resolve_tail_id()` (uses
   `XREVRANGE ... COUNT 1` to resolve a CONCRETE id once, at discovery
   time, instead of the re-evaluated sentinel); `FakeStreamsRedis` gained
   `xrevrange()`.
2. **`publish()` from a broadcast-mode instance still created a consumer
   group** (`_ensure_group` was unconditional), contradicting this
   module's own "no PEL in group-less consumption" design intent — fixed
   by skipping `_ensure_group` when `self._delivery == "broadcast"`.
   `test_broadcast_publish_does_not_create_a_group` added as a permanent
   regression test.

Also added the spec-required `test_end_to_end_broadcast_two_instances`
real-Redis integration test (§4/§5), which was missing from the original
implementation. Full suite green (`pytest -x -q`: 325 passed, 1 skipped).
Released as `0.2.1` (the `0.2.0` tag predates these fixes and is
superseded).
