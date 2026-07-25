# TASK-1845: Time-based (XTRIM MINID) retention for RedisStreamsBackend

**Feature**: FEAT-320 — RedisStreamsBackend Generic Capability Extensions
**Spec**: `sdd/specs/redis-streams-backend-extensions.spec.md`
**Status**: pending
**Priority**: medium
**Estimated effort**: M (2-4h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Spec §3 Module 3. `RedisStreamsBackend.publish()` only trims via count-
based `XADD ... MAXLEN ~ <maxlen>` (default 100,000 entries). A consuming
app may instead want TIME-based retention (e.g. "keep 7 days"), independent
of entry volume. This task adds `retention=timedelta` as an alternative
trim strategy, driven by a periodic `XTRIM ... MINID <id>` task rather than
`XADD`'s inline `MAXLEN`.

---

## Scope

- Add a `retention: Optional[timedelta] = None` constructor kwarg and a
  `retention_trim_interval: float = 60.0` kwarg (seconds between trim
  passes).
- When `retention` is set:
  - `publish()` must NOT pass `maxlen=`/`approximate=` to `XADD` anymore
    (time-based trimming owns retention instead — the two strategies are
    mutually exclusive in practice, per spec §3 Module 3).
  - `start_consumer()` spawns a new periodic task (e.g. `_run_retention_trimmer`)
    that, every `retention_trim_interval` seconds, computes
    `minid = f"{int((datetime.now(timezone.utc) - self._retention).timestamp() * 1000)}-0"`
    and issues `XTRIM <stream> MINID <minid>` for every stream in
    `self._streams` (reuse whatever discovery mechanism is active —
    `_refresh_streams()`'s SCAN result or the explicit `streams=` set from
    TASK-1844, whichever this backend instance is configured with).
  - `close()` must cancel this new task too (alongside whichever
    consumer/sweeper/broadcast tasks are running).
- When `retention` is `None` (default): NO behavior change — `publish()`
  keeps passing `maxlen=self._maxlen, approximate=True` exactly as today,
  and no new task is spawned. Every existing test in
  `tests/test_backends_streams.py` must pass unmodified.
- Document in the class docstring: "exactly one trimmer per stream" is the
  CALLER's responsibility (same principle as today's `maxlen` trimming —
  this feature does not add distributed locking).

**NOT in scope**: `delivery=`/`codec=`/`stream_key_fn=`/`streams=`
(TASK-1843/1844); `max_deliveries=` (TASK-1846). If `streams=` (TASK-1844)
has not landed yet when you pick this up, trim only over
`self._streams` as populated by the EXISTING `_refresh_streams()` SCAN
mechanism — do not implement the `streams=` kwarg yourself.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/backends/redis_streams.py` | MODIFY | `retention=`/`retention_trim_interval=` kwargs, trimmer task, `publish()` maxlen skip |
| `tests/test_backends_streams.py` | MODIFY | extend `FakeStreamsRedis` with `xtrim`; add retention test class |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend
    # verified: src/navigator_eventbus/backends/redis_streams.py:67
from datetime import datetime, timedelta, timezone
    # stdlib — not currently imported in redis_streams.py; add it
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/backends/redis_streams.py (verified 2026-07-25, main@8ef73b3)
async def publish(self, envelope: EventEnvelope) -> None: ...     # line 162
    # today: await self._redis.xadd(stream, {"envelope": ...},
    #                                maxlen=self._maxlen, approximate=True)
    # THIS TASK: if self._retention is not None, omit maxlen=/approximate=
    #            from this call entirely
async def start_consumer(self, on_envelope: OnEnvelope) -> None: ...  # line 178
    # today: spawns self._consumer_task + self._sweeper_task
    # THIS TASK: ALSO spawn self._retention_task when self._retention is set
async def close(self) -> None: ...                                # line 193
    # today: cancels self._consumer_task, self._sweeper_task
    # THIS TASK: ALSO cancel self._retention_task if it was created
async def _run_sweeper(self) -> None: ...                         # line 299
    # the closest existing periodic-task pattern to mirror (asyncio.sleep(interval)
    # loop, try/except Exception, self._running guard) for the new trimmer

# tests/test_backends_streams.py — FakeStreamsRedis (verified 2026-07-25)
class FakeStreamsRedis:                                            # line 45
    async def xadd(self, name, fields, maxlen=None, approximate=True): ...  # line 54
    # xtrim() does NOT exist yet — ADD it as part of this task
```

### Does NOT Exist
- ~~`RedisStreamsBackend(retention=..., retention_trim_interval=...)`~~ —
  this task creates both kwargs.
- ~~`FakeStreamsRedis.xtrim()`~~ — add it.
- ~~A `minid`-based trim call anywhere in the current codebase~~ — only
  count-based `MAXLEN ~` exists today (`publish()`'s inline `XADD` kwargs).

---

## Implementation Notes

### Pattern to Follow
```python
async def _run_retention_trimmer(self) -> None:
    """Periodic XTRIM MINID pass — alternative to inline MAXLEN trimming."""
    while self._running:
        try:
            await asyncio.sleep(self._retention_trim_interval)
            await self._ensure_connection()
            cutoff_ms = int(
                (datetime.now(timezone.utc) - self._retention).timestamp() * 1000
            )
            minid = f"{cutoff_ms}-0"
            for stream in list(self._streams):
                await self._redis.xtrim(stream, minid=minid, approximate=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — degraded mode, same as _run_sweeper
            if not self._running:
                return
            self.logger.warning("Retention trimmer error: %s", exc)
```

### Key Constraints
- `retention` and `maxlen` are mutually exclusive IN PRACTICE (per spec) —
  when `retention` is set, do not also pass `maxlen=` to `XADD`; the
  constructor should NOT raise if both are given (a caller might pass the
  default `maxlen=100_000` unknowingly) — just make `retention` win
  silently (log at DEBUG that maxlen is being ignored).
- `close()` must guard with `if self._retention_task is not None` before
  cancelling (mirror the existing pattern for `_consumer_task`/`_sweeper_task`).
- Google-style docstrings + strict type hints.

### References in Codebase
- `src/navigator_eventbus/backends/redis_streams.py:299-326` — `_run_sweeper`,
  the periodic-task pattern to mirror.

---

## Acceptance Criteria

- [ ] `RedisStreamsBackend(..., retention=timedelta(days=7))` issues `XTRIM ... MINID <id>` at `retention_trim_interval` cadence
- [ ] The `MINID` value is derived correctly from `now - retention`
- [ ] When `retention` is set, `XADD` is called WITHOUT `maxlen=`/`approximate=`
- [ ] Default construction (`retention=None`) behavior is IDENTICAL to today — full existing `tests/test_backends_streams.py` suite passes unmodified
- [ ] `close()` cleanly cancels the retention task when one was created
- [ ] `pytest tests/test_backends_streams.py -v` green
- [ ] `ruff check src/navigator_eventbus/backends/redis_streams.py` clean

---

## Test Specification

```python
# tests/test_backends_streams.py
class TestTimeBasedRetention:
    async def test_retention_minid_trim_issued(self, fake_redis):
        """XTRIM MINID is issued at the configured interval with a
        correctly-derived cutoff id."""
        ...

    async def test_retention_disables_maxlen_argument(self, fake_redis):
        """publish() does not pass maxlen=/approximate= when retention= is set."""
        ...

    async def test_default_maxlen_trim_unchanged(self, fake_redis):
        """Omitting retention= keeps today's maxlen=/approximate= XADD kwargs."""
        ...
```

---

## Agent Instructions

1. **Read the spec** (`sdd/specs/redis-streams-backend-extensions.spec.md`) §2 and §6 for full context
2. **Check dependencies** — none
3. **Verify the Codebase Contract** — re-read `redis_streams.py` before editing; line numbers may have drifted
4. **Update status** in `sdd/tasks/index/redis-streams-backend-extensions.json` → `"in-progress"`
5. **Implement**, **verify**, **move this file** to `sdd/tasks/completed/`,
   **update index** → `"done"`, **fill in the Completion Note**

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**:
**Date**:
**Notes**:

**Deviations from spec**: none
