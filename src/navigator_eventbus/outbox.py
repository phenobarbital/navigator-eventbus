"""Persistent producer outbox for backend-bound envelopes (FEAT-711).

Write-ahead store: an envelope is appended before the transport publish and
deleted only after the backend confirms it. Stdlib ``sqlite3`` in WAL mode;
every call runs off-loop on a dedicated worker thread.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, Literal, Optional, Protocol, runtime_checkable

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


class OutboxLockedError(RuntimeError):
    """Raised when another live ``SqliteOutbox`` already owns the database file."""


@runtime_checkable
class OutboxStore(Protocol):
    """Durable write-ahead store for envelopes awaiting backend confirmation."""

    async def append(self, envelope: EventEnvelope) -> None:
        """Persist *envelope* (idempotent on ``event_id``)."""
        ...

    async def ack(self, event_id: str) -> None:
        """Delete the row once the backend confirmed the publish."""
        ...

    async def nack(self, event_id: str, *, error: str, next_try_at: float) -> None:
        """Record a failed publish attempt and when to retry."""
        ...

    async def due(self, *, now: float, limit: int) -> list[EventEnvelope]:
        """Return up to *limit* envelopes due at *now*, oldest first."""
        ...

    async def stats(self) -> dict[str, int]:
        """Return pending and loss counters."""
        ...

    async def close(self) -> None:
        """Release the store's resources (rows stay persisted)."""
        ...


class SqliteOutbox:
    """SQLite (WAL) implementation of :class:`OutboxStore`.

    Args:
        path: Database file. One file per live store: an exclusive ``flock`` on
            ``<path>.lock`` is taken at construction, so a second store on the same
            file (another bus or worker) raises :class:`OutboxLockedError` instead of
            silently double-relaying rows. Give each worker its own file.
        max_rows: Row cap; on overflow the ``overflow`` policy applies.
        max_age: Rows older than this are purged in :meth:`due` (counted as ``expired``).
        overflow: ``"reject_new"`` raises :class:`OutboxFullError`;
            ``"drop_oldest"`` deletes the oldest row (counted as ``dropped``).

    Note:
        The ``expired``/``dropped``/``rejected``/``corrupt`` counters are in-memory
        for the lifetime of this store; they reset on restart (every occurrence is
        also logged at ERROR). ``pending`` is always read from the database.

    Raises:
        OutboxLockedError: Another live store already owns *path*.
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
        self._counters = {"expired": 0, "dropped": 0, "rejected": 0, "corrupt": 0}
        self.logger = logging.getLogger(__name__)
        # A dedicated single worker keeps SQLite off the loop without competing
        # with the default executor, and serialises access to the connection.
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="evb-outbox"
        )
        self._lock_file = self._acquire_file_lock()

    def _acquire_file_lock(self):
        """Take the exclusive, non-blocking ``flock`` guarding the database file."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(f"{self._path}.lock", "a")  # noqa: SIM115 - held until close()
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            self._executor.shutdown(wait=False)
            raise OutboxLockedError(
                f"Outbox {self._path} is already owned by another live store; "
                "use one outbox file per bus/worker"
            ) from exc
        return handle

    # -- sync helpers (run in a worker thread, always under self._lock) --
    def _ensure_conn(self) -> sqlite3.Connection:
        """Open the connection lazily; create schema; load the pending count."""
        if self._conn is not None:
            return self._conn
        self._path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            str(self._path), check_same_thread=False, isolation_level=None
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        for stmt in _SCHEMA:
            conn.execute(stmt)
        self._pending = conn.execute("SELECT COUNT(*) FROM evb_outbox").fetchone()[0]
        self._conn = conn
        return conn

    def _sync_append(self, envelope: EventEnvelope) -> None:
        conn = self._ensure_conn()
        if self._pending >= self._max_rows:
            if self._overflow == "reject_new":
                self._counters["rejected"] += 1
                self.logger.error(
                    "Outbox full (%d rows): rejecting envelope %s",
                    self._pending,
                    envelope.event_id,
                )
                raise OutboxFullError(
                    f"Outbox at {self._path} is full ({self._max_rows} rows)"
                )
            cur = conn.execute(
                "DELETE FROM evb_outbox WHERE event_id = ("
                "SELECT event_id FROM evb_outbox ORDER BY created_at LIMIT 1)"
            )
            self._pending -= cur.rowcount
            self._counters["dropped"] += cur.rowcount
            self.logger.error(
                "Outbox full (%d rows): dropped oldest row to admit %s",
                self._max_rows,
                envelope.event_id,
            )
        now = time.time()
        cur = conn.execute(
            "INSERT OR IGNORE INTO evb_outbox"
            " (event_id, topic, envelope, created_at, attempts, next_try_at, last_error)"
            " VALUES (?, ?, ?, ?, 0, ?, NULL)",
            (
                envelope.event_id,
                envelope.topic,
                json.dumps(envelope.to_dict()),
                now,
                now,
            ),
        )
        self._pending += cur.rowcount

    def _sync_ack(self, event_id: str) -> None:
        conn = self._ensure_conn()
        cur = conn.execute("DELETE FROM evb_outbox WHERE event_id = ?", (event_id,))
        self._pending -= cur.rowcount

    def _sync_nack(self, event_id: str, error: str, next_try_at: float) -> None:
        conn = self._ensure_conn()
        conn.execute(
            "UPDATE evb_outbox SET attempts = attempts + 1, last_error = ?,"
            " next_try_at = ? WHERE event_id = ?",
            (error, next_try_at, event_id),
        )

    def _sync_due(self, now: float, limit: int) -> list[EventEnvelope]:
        conn = self._ensure_conn()
        cur = conn.execute(
            "DELETE FROM evb_outbox WHERE created_at < ?", (now - self._max_age,)
        )
        if cur.rowcount > 0:
            self._pending -= cur.rowcount
            self._counters["expired"] += cur.rowcount
            self.logger.error(
                "Outbox purged %d row(s) older than max_age", cur.rowcount
            )
        rows = conn.execute(
            "SELECT event_id, envelope FROM evb_outbox WHERE next_try_at <= ?"
            " ORDER BY created_at LIMIT ?",
            (now, limit),
        ).fetchall()
        result: list[EventEnvelope] = []
        for event_id, raw in rows:
            try:
                result.append(EventEnvelope.from_dict(json.loads(raw)))
            except Exception as exc:  # noqa: BLE001 - undecodable row is dropped
                d = conn.execute(
                    "DELETE FROM evb_outbox WHERE event_id = ?", (event_id,)
                )
                self._pending -= d.rowcount
                self._counters["corrupt"] += d.rowcount
                self.logger.error(
                    "Outbox row %s undecodable (%s); deleted",
                    event_id,
                    type(exc).__name__,
                )
        return result

    def _sync_stats(self) -> dict[str, int]:
        return {"pending": self._pending, **self._counters}

    def _sync_close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    # -- OutboxStore API --
    async def append(self, envelope: EventEnvelope) -> None:
        """Persist *envelope* (idempotent on ``event_id``).

        Raises:
            OutboxFullError: The outbox is full and ``overflow='reject_new'``.
        """
        await self._run(self._locked, self._sync_append, envelope)

    async def ack(self, event_id: str) -> None:
        """Delete the row — the backend confirmed the publish."""
        await self._run(self._locked, self._sync_ack, event_id)

    async def nack(self, event_id: str, *, error: str, next_try_at: float) -> None:
        """Record a failed attempt and when to retry."""
        await self._run(self._locked, self._sync_nack, event_id, error, next_try_at)

    async def due(self, *, now: float, limit: int) -> list[EventEnvelope]:
        """Purge expired rows, then return up to *limit* rows due at *now*, oldest first."""
        return await self._run(self._locked, self._sync_due, now, limit)

    async def stats(self) -> dict[str, int]:
        """Return ``pending``, ``expired``, ``dropped``, ``rejected`` and ``corrupt`` counts."""
        return await self._run(self._locked, self._sync_stats)

    async def close(self) -> None:
        """Close the database connection (rows stay on disk)."""
        try:
            await self._run(self._locked_close)
        finally:
            self._executor.shutdown(wait=False)
            try:
                self._lock_file.close()  # releases the flock
            except OSError:  # pragma: no cover - defensive
                pass

    async def _run(self, fn: Callable[..., Any], *args: Any) -> Any:
        """Run *fn* on the dedicated outbox worker thread."""
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, fn, *args
        )

    def _locked_close(self) -> None:
        """Close under the lock without reopening a connection."""
        with self._lock:
            self._sync_close()

    def _locked(self, fn: Callable[..., Any], *args: Any) -> Any:
        """Run *fn* under the connection lock (called in a worker thread)."""
        with self._lock:
            self._ensure_conn()
            return fn(*args)
