# TASK-4555: OutboxStore protocol + SqliteOutbox

**Feature**: FEAT-711 — Durable lifecycle fan-out
**Spec**: `sdd/specs/durable-lifecycle-fanout.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Spec Goal G3 / Module 2. The persistent producer outbox needs a local durable store: every
backend-bound envelope is written here before the transport publish and deleted only after the
backend confirms. This task builds the store only (a new module, stdlib `sqlite3` in WAL mode,
every call off-loop via `asyncio.to_thread`). `BusCore` wiring is TASK-4556.

---

## Scope

- Create `src/navigator_eventbus/outbox.py` with `OutboxStore` (a `runtime_checkable` `Protocol`), `OutboxFullError(RuntimeError)` and `SqliteOutbox`.
- `SqliteOutbox` implements the spec §2 schema (`evb_outbox` + `evb_outbox_due` index), WAL mode, lazy connect, one connection guarded by a `threading.Lock`, every DB call via `asyncio.to_thread`.
- `append` is idempotent on `event_id` (`INSERT OR IGNORE`) and enforces `max_rows` with the `overflow` policy.
- `due` purges rows older than `max_age` (counted as `expired`, logged at ERROR) and returns rows with `next_try_at <= now`, ordered by `created_at`, limited.
- `stats()` returns `{"pending", "expired", "dropped", "rejected"}`.
- Export `OutboxStore`, `SqliteOutbox`, `OutboxFullError` from `navigator_eventbus/__init__.py`.
- Unit tests in a NEW file `tests/test_outbox.py`.

**NOT in scope**: `BusCore` write-ahead / relay / backoff computation (TASK-4556 — the store only stores the `next_try_at` it is given); per-PID file naming and orphan adoption (spec Open Question Q1 — unresolved, not part of this feature's tasks); a non-SQLite store.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/outbox.py` | CREATE | `OutboxStore`, `SqliteOutbox`, `OutboxFullError` |
| `src/navigator_eventbus/__init__.py` | MODIFY | export the three names |
| `tests/test_outbox.py` | CREATE | unit tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.envelope import EventEnvelope   # verified: src/navigator_eventbus/envelope.py:106
from navconfig.logging import logging                  # verified: used by core.py:34 / evb.py:40
# stdlib only otherwise: asyncio, json, os, sqlite3, threading, time,
# datetime.timedelta, pathlib.Path, typing (Literal, Optional, Protocol, runtime_checkable)
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/envelope.py
@dataclass(frozen=True, slots=True)  # frozen — never mutate an envelope
class EventEnvelope:                                    # :106
    topic: str
    event_id: str  # UUID4 string; dedup key            # :132
    def to_dict(self) -> dict[str, Any]: ...            # :164 — every value JSON-serializable (timestamp ISO string, enums as int)
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EventEnvelope": ...  # :188 — raises UnsupportedSchemaVersion on a newer schema

# src/navigator_eventbus/__init__.py
from navigator_eventbus.dlq import DLQHandler           # :19
__all__ = [ ..., "DLQHandler", ... ]                    # :47 — alphabetical-ish list of names
```

### Does NOT Exist
- ~~`navigator_eventbus.outbox`~~, ~~`OutboxStore`~~, ~~`SqliteOutbox`~~, ~~`OutboxFullError`~~ — created by this task.
- ~~`aiosqlite`~~ — NOT a dependency; spec AC: "No new runtime dependency". Use stdlib `sqlite3` + `asyncio.to_thread`.
- ~~`EventEnvelope.to_json`~~ / ~~`EventEnvelope.from_json`~~ — use `json.dumps(env.to_dict())` / `EventEnvelope.from_dict(json.loads(...))`.
- ~~`OutboxStore.get`~~, ~~`OutboxStore.attempts`~~ — the protocol has exactly `append, ack, nack, due, stats, close`.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/outbox.py", "action": "CREATE"},
    {"path": "src/navigator_eventbus/__init__.py", "action": "MODIFY"},
    {"path": "tests/test_outbox.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/envelope.py#EventEnvelope"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- **Never block the loop**: every sqlite call goes through `asyncio.to_thread(self._sync_xxx, ...)`; the `_sync_*` methods take `self._lock`.
- **Pending count without `COUNT(*)` per append**: `SELECT COUNT(*)` scans the table (1M rows). Read it once at connect, then keep `self._pending` in memory, adjusting by `cursor.rowcount` on insert/delete.
- `synchronous=NORMAL` with WAL — do this because WAL+NORMAL survives a process crash (the spec's failure model) without an fsync on every append; power loss of the last commits is accepted.
- Store only the exception **class name** in `last_error` (spec M2) — the caller passes it; do not add tracebacks.
- A full outbox is never silent: every reject/drop/expiry increments its counter AND logs at ERROR.
- `append` sets `next_try_at = created_at` (immediately due). Duplicate in-flight publishes are prevented by `BusCore` (TASK-4556), not by the store.
- Create the parent directory of `path` if missing (`Path(path).parent.mkdir(parents=True, exist_ok=True)`).

---

## Implementation Blueprint

### Steps (in order)
1. Create `outbox.py` from the block below — *why*: signatures are fixed by spec §2.
2. Fill in each `_sync_*` body — *why*: the SQL is the judgement part; the async wrappers are mechanical.
3. Add exports to `__init__.py` — *why*: spec §6 Edit Sites lists `__init__.py` (export outbox API).
4. Write `tests/test_outbox.py` using `tmp_path` — *why*: real SQLite files, no mocks.

### `src/navigator_eventbus/outbox.py` (CREATE)
```python
"""Persistent producer outbox for backend-bound envelopes (FEAT-711).

Write-ahead store: an envelope is appended before the transport publish and
deleted only after the backend confirms it. Stdlib ``sqlite3`` in WAL mode;
every call runs off-loop through ``asyncio.to_thread``.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Literal, Optional, Protocol, runtime_checkable

from navconfig.logging import logging

from navigator_eventbus.envelope import EventEnvelope

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS evb_outbox ("
    " event_id TEXT PRIMARY KEY, topic TEXT NOT NULL, envelope TEXT NOT NULL,"
    " created_at REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,"
    " next_try_at REAL NOT NULL, last_error TEXT)",
    "CREATE INDEX IF NOT EXISTS evb_outbox_due ON evb_outbox(next_try_at)",
)


class OutboxFullError(RuntimeError):
    """Raised by ``append`` when the outbox is full and ``overflow='reject_new'``."""


@runtime_checkable
class OutboxStore(Protocol):
    """Durable write-ahead store for envelopes awaiting backend confirmation."""

    async def append(self, envelope: EventEnvelope) -> None: ...
    async def ack(self, event_id: str) -> None: ...
    async def nack(self, event_id: str, *, error: str, next_try_at: float) -> None: ...
    async def due(self, *, now: float, limit: int) -> list[EventEnvelope]: ...
    async def stats(self) -> dict[str, int]: ...
    async def close(self) -> None: ...


class SqliteOutbox:
    """SQLite (WAL) implementation of :class:`OutboxStore`.

    Args:
        path: Database file. One file per process — never share it between workers.
        max_rows: Row cap; on overflow the ``overflow`` policy applies.
        max_age: Rows older than this are purged in :meth:`due` (counted as ``expired``).
        overflow: ``"reject_new"`` raises :class:`OutboxFullError`;
            ``"drop_oldest"`` deletes the oldest row (counted as ``dropped``).
    """

    def __init__(
        self,
        path: str | os.PathLike,
        *,
        max_rows: int = 1_000_000,
        max_age: timedelta = timedelta(days=7),
        overflow: Literal["reject_new", "drop_oldest"] = "reject_new",
    ) -> None:
        if overflow not in ("reject_new", "drop_oldest"):
            raise ValueError(f"Unknown outbox overflow policy {overflow!r}")
        self._path = Path(path)
        self._max_rows = max_rows
        self._max_age = max_age.total_seconds()
        self._overflow = overflow
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()
        self._pending = 0
        self._counters = {"expired": 0, "dropped": 0, "rejected": 0}
        self.logger = logging.getLogger("navigator_eventbus.outbox")

    # -- sync helpers (run in a worker thread, always under self._lock) --
    def _ensure_conn(self) -> sqlite3.Connection:
        """Open the connection lazily; create schema; load the pending count."""
        # FILL IN: mkdir parent; sqlite3.connect(self._path, check_same_thread=False, isolation_level=None);
        #          PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; run _SCHEMA; self._pending = COUNT(*) once.
        raise NotImplementedError

    def _sync_append(self, envelope: EventEnvelope) -> None:
        # FILL IN: overflow check (self._pending >= self._max_rows) → reject_new: counters["rejected"]+=1,
        #          logger.error, raise OutboxFullError | drop_oldest: DELETE oldest by created_at, counters["dropped"]+=1, logger.error;
        #          then INSERT OR IGNORE (event_id, topic, json.dumps(envelope.to_dict()), now, 0, now, NULL); self._pending += rowcount.
        raise NotImplementedError

    def _sync_ack(self, event_id: str) -> None:
        # FILL IN: DELETE WHERE event_id=?; self._pending -= rowcount
        raise NotImplementedError

    def _sync_nack(self, event_id: str, error: str, next_try_at: float) -> None:
        # FILL IN: UPDATE attempts = attempts + 1, last_error = ?, next_try_at = ? WHERE event_id = ?
        raise NotImplementedError

    def _sync_due(self, now: float, limit: int) -> list[EventEnvelope]:
        # FILL IN: purge created_at < now - self._max_age (counters["expired"] += n; logger.error when n);
        #          SELECT envelope WHERE next_try_at <= ? ORDER BY created_at LIMIT ?;
        #          EventEnvelope.from_dict(json.loads(row)) — a row that fails to decode is logged and deleted (counted as expired)
        raise NotImplementedError

    def _sync_close(self) -> None:
        # FILL IN: close and drop self._conn if open
        raise NotImplementedError

    # -- OutboxStore API --
    async def append(self, envelope: EventEnvelope) -> None:
        """Persist *envelope* (idempotent on ``event_id``).

        Raises:
            OutboxFullError: The outbox is full and ``overflow='reject_new'``.
        """
        await asyncio.to_thread(self._locked, self._sync_append, envelope)

    async def ack(self, event_id: str) -> None:
        """Delete the row — the backend confirmed the publish."""
        await asyncio.to_thread(self._locked, self._sync_ack, event_id)

    async def nack(self, event_id: str, *, error: str, next_try_at: float) -> None:
        """Record a failed attempt and when to retry."""
        await asyncio.to_thread(self._locked, self._sync_nack, event_id, error, next_try_at)

    async def due(self, *, now: float, limit: int) -> list[EventEnvelope]:
        """Purge expired rows, then return up to *limit* rows due at *now*, oldest first."""
        return await asyncio.to_thread(self._locked, self._sync_due, now, limit)

    async def stats(self) -> dict[str, int]:
        """Return ``pending``, ``expired``, ``dropped`` and ``rejected`` counts."""
        return {"pending": self._pending, **self._counters}

    async def close(self) -> None:
        """Close the database connection (rows stay on disk)."""
        await asyncio.to_thread(self._locked, self._sync_close)

    def _locked(self, fn, *args):  # type: ignore[no-untyped-def]
        """Run *fn* under the connection lock (called in a worker thread)."""
        with self._lock:
            self._ensure_conn()
            return fn(*args)
```
**Why this shape**: the protocol and constructor are fixed by spec §2; `_locked` funnels every DB call through one lock so the single `check_same_thread=False` connection is never used concurrently. `time` is imported for `time.time()` in `_sync_append`. `_sync_close` must not reopen a connection — FILL IN: have `close()` bypass `_ensure_conn` (e.g. call `_sync_close` under `self._lock` directly).

### `src/navigator_eventbus/__init__.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c 'from navigator_eventbus.dlq import DLQHandler' src/navigator_eventbus/__init__.py)
# AFTER — insert below `from navigator_eventbus.dlq import DLQHandler` (verified: __init__.py:19)
from navigator_eventbus.outbox import OutboxFullError, OutboxStore, SqliteOutbox
# and add "OutboxFullError", "OutboxStore", "SqliteOutbox" to __all__ (:47) next to "IngressEnvelope".
```
**Why**: stdlib-only module, so an eager import is safe (unlike the lazy `queues` names which need `redis`).

### FILL IN checklist
- [ ] `_ensure_conn` — schema + WAL + pending count; bounded by "no blocking on loop".
- [ ] `_sync_append` — overflow policy; bounded by spec AC-5 (never silent).
- [ ] `_sync_due` — purge + ordered select; bounded by spec M2.
- [ ] `close()` must not reopen a closed connection.
- [ ] tests below.

---

## Acceptance Criteria

- [ ] `append` twice with the same `event_id` stores one row.
- [ ] `due` returns only rows with `next_try_at <= now`, ordered by `created_at`, at most `limit`.
- [ ] `reject_new` overflow raises `OutboxFullError` and counts `rejected`; `drop_oldest` deletes the oldest and counts `dropped`; both log at ERROR (spec AC-5).
- [ ] Rows older than `max_age` are purged in `due` and counted as `expired`.
- [ ] Rows survive `close()` + a new `SqliteOutbox` on the same path.
- [ ] `from navigator_eventbus import SqliteOutbox, OutboxStore, OutboxFullError` works; `isinstance(SqliteOutbox(p), OutboxStore)` is `True`.
- [ ] No new runtime dependency (spec AC-8).

---

## Validation Commands

- `pytest tests/test_outbox.py -q`
- `pytest tests/test_package.py -q`

---

## Test Specification

```python
# tests/test_outbox.py
import time
from datetime import timedelta

import pytest

from navigator_eventbus import EventEnvelope, OutboxFullError, OutboxStore, SqliteOutbox


def _env(topic: str = "t.x") -> EventEnvelope:
    return EventEnvelope(topic=topic, payload={"k": 1})  # FILL IN: confirm required ctor fields (envelope.py:106)


async def test_outbox_append_idempotent(tmp_path): ...
async def test_outbox_due_order_and_backoff(tmp_path): ...       # nack(next_try_at=now+10) hides the row until then
async def test_outbox_overflow_reject_new_raises(tmp_path): ...
async def test_outbox_overflow_drop_oldest_counts(tmp_path): ...
async def test_outbox_max_age_purge_counted(tmp_path): ...       # max_age=timedelta(seconds=0), due(now=time.time()+1)
async def test_outbox_survives_reopen(tmp_path): ...
async def test_outbox_satisfies_protocol(tmp_path): ...
```

---

## Agent Instructions

1. **Work in the feature worktree** (`python -m scripts.sdd.ensure_worktree --slug durable-lifecycle-fanout --feature-id FEAT-711`)
2. **Read the spec** for full context
3. **Check dependencies** — none for this task
4. **Verify the Codebase Contract** before writing code
5. **Update status** in `sdd/tasks/index/durable-lifecycle-fanout.json` → `"in-progress"` and commit only that file
6. **Implement** from the blueprint; complete every `# FILL IN:` marker
7. **Verify** — run the Validation Commands
8. **Commit the code** — only the listed files
9. **Close the task** with `scripts/sdd/close_task.sh TASK-4555 durable-lifecycle-fanout verified`
10. **Fill in the Completion Note**, then commit the staged SDD state

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**: <session or agent ID>
**Date**: YYYY-MM-DD
**Notes**:

**Deviations from spec**: none | describe if any
