# TASK-4556: BusCore write-ahead outbox + relay loop + stats()

**Feature**: FEAT-711 — Durable lifecycle fan-out
**Spec**: `sdd/specs/durable-lifecycle-fanout.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: L (4-8h)
**Depends-on**: TASK-4555
**Assigned-to**: unassigned

---

## Context

Spec Goals G3/G5, Module 3 (marked "design fixed, review needed" — get a code review before closing).
Today `BusCore._backend_publish` logs and **discards** an envelope when the transport fails
(`core.py:347-356`), so a Redis outage silently loses events. This task makes `BusCore` write every
backend-bound envelope to an injected `OutboxStore` (TASK-4555) BEFORE spawning the transport
publish, ack it on success, nack it with backoff on failure, and run a relay task that re-publishes
due rows — once at `start()` (crash recovery) and every `relay_interval` after.

---

## Scope

- Add `outbox=`, `relay_interval=5.0`, `relay_batch=500`, `relay_max_backoff=300.0` kwargs to `BusCore.__init__` (after `backend=`).
- Add a sync `stats() -> dict[str, int]` with keys `outbox_pending`, `outbox_acked`, `outbox_retried`, `outbox_rejected`, `backend_failures`.
- `publish()`: when `self._backend` and `self._outbox` are set, `await self._outbox.append(envelope)` BEFORE `_spawn(self._backend_publish(...))`. On `OutboxFullError`: count `outbox_rejected`, log ERROR, re-raise (nothing spawned, nothing enqueued locally).
- `_backend_publish()`: on success → `outbox.ack`, count `outbox_acked`; on failure → count `backend_failures`, `outbox.nack(error=type(exc).__name__, next_try_at=now + backoff(attempts))`. Outbox I/O failures are logged, never raised.
- Relay: `_relay_loop` task started in `start()` when backend+outbox are set; runs `_relay_once()` immediately, then every `relay_interval`. `_relay_once` skips event_ids currently in flight.
- `close()`: stop the relay, wait (bounded) for in-flight backend publishes, run ONE final bounded `_relay_once()`, then `outbox.close()` — all before `backend.close()`. Pending rows stay on disk.
- Without an outbox, behavior is unchanged (existing tests pass untouched).
- Unit tests in a NEW file `tests/test_core_outbox.py`.

**NOT in scope**: `publish_only` (TASK-4557 — same file, runs after this task); `EventBus` kwargs/config (TASK-4558); real-Redis tests (TASK-4559); persisting `attempts` across restarts (in-memory only — after a restart backoff restarts at attempt 1, accepted).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/core.py` | MODIFY | outbox kwargs, stats, write-ahead, ack/nack, relay, close flush |
| `tests/test_core_outbox.py` | CREATE | unit tests with a fake backend + real `SqliteOutbox(tmp_path)` |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.core import BusCore, BusClosedError          # verified: core.py:92, :61
from navigator_eventbus.envelope import EventEnvelope               # verified: envelope.py:106
from navigator_eventbus.backends.base import TransportBackend, OnEnvelope  # verified: backends/base.py:24-28
from navigator_eventbus.outbox import OutboxStore, OutboxFullError, SqliteOutbox  # created by TASK-4555
```
Already imported in `core.py` (lines 24-38): `asyncio`, `contextvars`, `fnmatch`, `uuid`, `OrderedDict`,
`dataclass, field`, `Any, Awaitable, Callable, Optional`, `logging` (navconfig), `TransportBackend`,
`EventEnvelope, Severity`, `EventPriority`. `time` is NOT imported yet — add `import time`.

### Existing Signatures to Use
```python
# src/navigator_eventbus/core.py
class BusCore:                                                        # :92
    def __init__(self, *, workers=4, queue_size=1024, handler_timeout=30.0, retry_attempts=3,
                 retry_base_delay=0.1, backpressure=None, default_backpressure=POLICY_BLOCK,
                 drain_timeout=5.0, on_dlq=None,
                 backend: Optional[TransportBackend] = None,          # :140 (anchor)
                 ) -> None:                                            # :128-141
        self._drain_timeout = drain_timeout
        self._backend = backend
        self._fanned_out: OrderedDict[str, None]                      # echo suppression
        self._started / self._closing / self._stopping: bool
        self._background_tasks: set[asyncio.Task[None]]               # strong refs for _spawn
        self.logger = logging.getLogger("navigator_eventbus.core")    # last line of __init__ (:190)
    def _spawn(self, coro, name: str) -> asyncio.Task[None]           # :192
    async def start(self) -> None                                     # :203-216
        #   if self._backend is not None:                                    (:213)
        #       await self._backend.start_consumer(self._on_transport_envelope)  (:214)
    async def close(self, drain_timeout: Optional[float] = None)      # :217-273
        #   finally: if self._backend is not None: await self._backend.close()   (:264-270)
    async def publish(self, envelope: EventEnvelope) -> None          # :287-310
        #   if self._backend is not None:
        #       self._remember_fanout(envelope.event_id)
        #       self._spawn(self._backend_publish(envelope),             (:307 anchor)
        #                   name=f"bus-backend-publish.{envelope.topic}")
        #   await self._enqueue_local(envelope)
    async def _backend_publish(self, envelope) -> None                # :347-356
        #   await self._backend.publish(envelope)  # type: ignore[union-attr]   (:350 anchor)
        #   except Exception as exc: self.logger.warning(...)
    def _remember_fanout(self, event_id: str) -> None                 # :391

# src/navigator_eventbus/backends/base.py:28 — TransportBackend(Protocol): publish, start_consumer, close
# src/navigator_eventbus/backends/memory.py:15 — MemoryBackend (publish/start_consumer/close)
```

### Does NOT Exist
- ~~`BusCore(outbox=...)`~~, ~~`BusCore.stats()`~~, ~~`BusCore._relay_loop`~~ — added by this task.
- ~~`BusCore(publish_only=...)`~~ — TASK-4557, do NOT add it here.
- ~~`OutboxStore.attempts(event_id)`~~ — the store does not expose attempts; keep `self._outbox_attempts: dict[str, int]` in memory.
- ~~`BusCore.metrics`~~ / ~~`BusCore.counters`~~ — the public surface is `stats()` only.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/core.py", "action": "MODIFY"},
    {"path": "tests/test_core_outbox.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/core.py#BusCore",
    "sym:src/navigator_eventbus/backends/base.py#TransportBackend",
    "sym:src/navigator_eventbus/envelope.py#EventEnvelope"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- **Ordering rule (spec §2):** `append` completes before `_spawn(...)`. A crash after that leaves the row for the relay.
- **No double publish in one process:** add `event_id` to `self._outbox_inflight` before spawning and `discard` it in `_backend_publish`'s `finally`; `_relay_once` skips in-flight ids — because `append` makes a row immediately due, so without this the relay would race the first publish.
- **Echo suppression:** relay publishes also call `self._remember_fanout(env.event_id)`.
- **Failure isolation (spec §7):** transport and outbox failures never reach the publisher, except `OutboxFullError`.
- **Backoff:** `min(self._relay_max_backoff, self._relay_interval * 2 ** (attempts - 1))` — exponential on the relay interval, capped (spec M3).
- `outbox_pending` in the sync `stats()` is the value cached from `await self._outbox.stats()` at the end of every `_relay_once()` — because `stats()` is sync and the store's `stats()` is async.
- If `outbox` is given without a `backend`, log a WARNING in `__init__` and ignore it.
- Run `/sdd-codereview` (or ask for a review) before closing — spec marks M3 "review needed".

---

## Implementation Blueprint

### Steps (in order)
1. Add `import time` and a `TYPE_CHECKING` import of `OutboxStore`; import `OutboxFullError` at module level — *why*: `outbox.py` imports only `envelope`, so no cycle.
2. Add the four kwargs after `backend=` and the state in `__init__` — *why*: spec §2 fixes the names/defaults.
3. Add `stats()`, `_backoff()`, `_outbox_ack()`, `_outbox_nack()`, `_relay_once()`, `_relay_loop()` — *why*: keep `publish` short.
4. Change `publish`, `_backend_publish`, `start`, `close` — *why*: the write-ahead/ack/relay/flush contract.
5. Write tests; run the Validation Commands (the existing `tests/test_core.py` must stay green).

### `src/navigator_eventbus/core.py` (MODIFY) — imports
```python
# occurrences: 1 (verified: grep -c 'from navigator_eventbus.backends.base import TransportBackend' src/navigator_eventbus/core.py)
# AFTER — insert below `from navigator_eventbus.backends.base import TransportBackend` (verified: core.py:36)
from navigator_eventbus.outbox import OutboxFullError, OutboxStore
# and add `import time` next to `import uuid` (core.py:29)
```

### `src/navigator_eventbus/core.py` (MODIFY) — `__init__`
```python
# occurrences: 1 (verified: grep -c '        backend: Optional[TransportBackend] = None,' src/navigator_eventbus/core.py)
# AFTER — insert below `        backend: Optional[TransportBackend] = None,` (verified: core.py:140)
        outbox: Optional[OutboxStore] = None,
        relay_interval: float = 5.0,
        relay_batch: int = 500,
        relay_max_backoff: float = 300.0,
# In the body, AFTER `self._backend = backend`:
        if outbox is not None and backend is None:
            # FILL IN: self.logger is created at the END of __init__ — use logging.getLogger("navigator_eventbus.core").warning(...) here
            outbox = None
        self._outbox = outbox
        self._relay_interval = relay_interval
        self._relay_batch = max(1, relay_batch)
        self._relay_max_backoff = relay_max_backoff
        self._relay_task: Optional[asyncio.Task[None]] = None
        self._outbox_inflight: set[str] = set()
        self._outbox_attempts: dict[str, int] = {}
        self._stats: dict[str, int] = {
            "outbox_pending": 0, "outbox_acked": 0, "outbox_retried": 0,
            "outbox_rejected": 0, "backend_failures": 0,
        }
```
Document all four kwargs in the class docstring Args.

### `src/navigator_eventbus/core.py` (MODIFY) — `publish` write-ahead
```python
# occurrences: 1 (verified: grep -c '                self._backend_publish(envelope),' src/navigator_eventbus/core.py) → :307
# Replace the `if self._backend is not None:` block in publish() (core.py:304-309) with:
        if self._backend is not None:
            if self._outbox is not None:
                try:
                    await self._outbox.append(envelope)
                except OutboxFullError:
                    self._stats["outbox_rejected"] += 1
                    self.logger.error(
                        "Outbox full — publish rejected for %s (%s)",
                        envelope.topic, envelope.event_id,
                    )
                    raise
                # FILL IN: any OTHER exception from append — log ERROR and continue WITHOUT the outbox for this envelope
                #          (degraded mode, like a transport failure) — bounded by spec §7 failure isolation
                self._outbox_inflight.add(envelope.event_id)
            self._remember_fanout(envelope.event_id)
            self._spawn(
                self._backend_publish(envelope),
                name=f"bus-backend-publish.{envelope.topic}",
            )
```
Update the `publish` docstring `Raises:` with `OutboxFullError`.

### `src/navigator_eventbus/core.py` (MODIFY) — `_backend_publish` + helpers
```python
# occurrences: 1 (verified: grep -c '            await self._backend.publish(envelope)  # type: ignore\[union-attr\]' core.py) → :350
# Replace the body of _backend_publish (core.py:347-356) with:
    async def _backend_publish(self, envelope: EventEnvelope) -> None:
        """Fan *envelope* out to the transport backend; ack/nack the outbox row."""
        try:
            await self._backend.publish(envelope)  # type: ignore[union-attr]
        except Exception as exc:  # noqa: BLE001 — degraded mode, local OK
            self._stats["backend_failures"] += 1
            self.logger.warning(
                "Transport backend publish failed for %s (%s): %s — "
                "local dispatch unaffected",
                envelope.topic, envelope.event_id, exc,
            )
            await self._outbox_nack(envelope, exc)
        else:
            await self._outbox_ack(envelope)
        finally:
            self._outbox_inflight.discard(envelope.event_id)

    def _backoff(self, attempts: int) -> float:
        """Exponential retry delay (seconds), capped at ``relay_max_backoff``."""
        return min(self._relay_max_backoff, self._relay_interval * 2 ** (attempts - 1))

    async def _outbox_ack(self, envelope: EventEnvelope) -> None:
        # FILL IN: no-op without outbox; await outbox.ack(event_id); pop _outbox_attempts; outbox_acked += 1;
        #          any exception → logger.exception, never raise
        ...

    async def _outbox_nack(self, envelope: EventEnvelope, exc: BaseException) -> None:
        # FILL IN: no-op without outbox; attempts = _outbox_attempts.get(id, 0) + 1 (store back);
        #          await outbox.nack(id, error=type(exc).__name__, next_try_at=time.time() + self._backoff(attempts));
        #          any exception → logger.exception, never raise
        ...
```

### `src/navigator_eventbus/core.py` (MODIFY) — relay + stats (insert after the helpers above)
```python
    async def _relay_once(self) -> None:
        """Re-publish due outbox rows (skips envelopes already in flight)."""
        # FILL IN: rows = await self._outbox.due(now=time.time(), limit=self._relay_batch)
        #          for env not in self._outbox_inflight: inflight.add; _remember_fanout; outbox_retried += 1;
        #          await self._backend_publish(env)   (sequential — keeps relay pressure bounded)
        #          finally: self._stats["outbox_pending"] = (await self._outbox.stats())["pending"]
        #          any exception → logger.exception, never raise
        ...

    async def _relay_loop(self) -> None:
        """Drain once immediately (crash recovery), then every ``relay_interval``."""
        await self._relay_once()
        while not self._stopping:
            await asyncio.sleep(self._relay_interval)
            await self._relay_once()

    def stats(self) -> dict[str, int]:
        """Outbox / transport counters (``outbox_pending`` refreshed by each relay pass)."""
        return dict(self._stats)
```

### `src/navigator_eventbus/core.py` (MODIFY) — `start` and `close`
```python
# start(): occurrences: 1 (verified: grep -c '            await self._backend.start_consumer(self._on_transport_envelope)' core.py) → :214
# AFTER — insert below that line (still inside start(), dedented to method level):
        if self._backend is not None and self._outbox is not None:
            self._relay_task = asyncio.create_task(
                self._relay_loop(), name="bus-core-outbox-relay"
            )
# close(): insert at the START of close()'s `finally:` (core.py:266 — `finally:` occurs 4 times in core.py; use the one inside close()), BEFORE `if self._backend is not None:`
            await self._stop_relay(deadline)
# and add the method:
    async def _stop_relay(self, deadline: float) -> None:
        """Cancel the relay, wait for in-flight publishes, run one final bounded drain, close the outbox."""
        # FILL IN: no-op without outbox; cancel + await self._relay_task (suppress CancelledError);
        #          await asyncio.wait_for(gather(in-flight bus-backend-publish tasks from self._background_tasks), deadline)
        #          then await asyncio.wait_for(self._relay_once(), deadline); then await self._outbox.close();
        #          every timeout/exception logged, never raised — rows still pending stay on disk (spec M3)
        ...
```
**Why**: the relay starts after the consumer so a crash-recovered envelope can be echo-suppressed; the flush runs before `backend.close()` because the relay needs a live transport.

### FILL IN checklist
- [ ] `__init__` warning when `outbox` without `backend`.
- [ ] non-`OutboxFullError` append failure → degraded mode, not raised.
- [ ] `_outbox_ack` / `_outbox_nack` bodies; bounded by failure isolation.
- [ ] `_relay_once` body incl. `outbox_pending` refresh.
- [ ] `_stop_relay` body; bounded by `drain_timeout`.
- [ ] tests below.

---

## Acceptance Criteria

- [ ] With an outbox, the row exists before `backend.publish` is called (spy backend observes it).
- [ ] A backend failing N times then succeeding → exactly one ack, row gone, `backend_failures == N`.
- [ ] A new `BusCore` on the same outbox path publishes leftover rows on `start()`.
- [ ] `reject_new` overflow → `publish` raises `OutboxFullError`, `stats()["outbox_rejected"]` increments.
- [ ] Without an outbox, `tests/test_core.py` passes unchanged.
- [ ] `ruff check src/navigator_eventbus/core.py` is clean.

---

## Validation Commands

- `pytest tests/test_core_outbox.py -q`
- `pytest tests/test_core.py -q`
- `pytest tests/test_backends.py -q`
- `pytest tests/test_producers.py -q`

---

## Test Specification

```python
# tests/test_core_outbox.py
import asyncio

import pytest

from navigator_eventbus import EventEnvelope, OutboxFullError, SqliteOutbox
from navigator_eventbus.core import BusCore


class _FlakyBackend:
    """TransportBackend fake: fails the first `fail` publishes, records the rest."""

    def __init__(self, fail: int = 0, outbox=None) -> None:
        self.fail, self.outbox = fail, outbox
        self.published: list[str] = []
        self.seen_in_outbox: list[bool] = []
        self.consumer_started = False

    async def publish(self, envelope): ...      # FILL IN: record whether the row is already in outbox (await outbox.due(now=far_future, limit=...)), fail first N
    async def start_consumer(self, on_envelope): self.consumer_started = True
    async def close(self): ...


async def test_publish_writes_outbox_before_backend(tmp_path): ...
async def test_backend_failure_keeps_row_and_relay_redelivers(tmp_path): ...  # relay_interval=0.05
async def test_restart_recovers_pending_rows(tmp_path): ...
async def test_overflow_reject_new_raises_from_publish(tmp_path): ...
async def test_no_outbox_behavior_unchanged(): ...
async def test_stats_counters(tmp_path): ...
```

---

## Agent Instructions

1. **Work in the feature worktree** (`python -m scripts.sdd.ensure_worktree --slug durable-lifecycle-fanout --feature-id FEAT-711`)
2. **Read the spec** for full context
3. **Check dependencies** — TASK-4555 must be `"done"` in the per-spec index
4. **Verify the Codebase Contract** before writing code
5. **Update status** → `"in-progress"` and commit only the index file
6. **Implement** from the blueprint; complete every `# FILL IN:` marker
7. **Verify** — run the Validation Commands; request a code review (spec M3 "review needed")
8. **Commit the code** — only the listed files
9. **Close the task** with `scripts/sdd/close_task.sh TASK-4556 durable-lifecycle-fanout verified`
10. **Fill in the Completion Note**, then commit the staged SDD state

---

## Completion Note

Implemented by gpt-5.6-terra (codex, 1 attempt); merged by sdd-worker.

**Completed by**: gpt-5.6-terra (codex, 1 attempt)
**Date**: 2026-10-20
**Notes**: Validated manually with PYTHONPATH=src (the validation tool imports the primary checkout's editable src): test_core_outbox, test_core, test_backends, test_producers -> 55 passed.

**Deviations from spec**: none | describe if any
