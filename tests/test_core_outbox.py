"""Durable outbox behavior for :class:`navigator_eventbus.core.BusCore`."""
import asyncio
import time
from collections.abc import Callable

import pytest

from navigator_eventbus import EventEnvelope, OutboxFullError, SqliteOutbox
from navigator_eventbus.backends.base import OnEnvelope
from navigator_eventbus.core import BusCore


class _FlakyBackend:
    """Transport fake that fails a configurable number of publishes."""

    def __init__(self, fail: int = 0, outbox: SqliteOutbox | None = None) -> None:
        self.fail = fail
        self.outbox = outbox
        self.published: list[str] = []
        self.seen_in_outbox: list[bool] = []
        self.consumer_started = False

    async def publish(self, envelope: EventEnvelope) -> None:
        """Record the call and fail until the configured count is exhausted."""
        if self.outbox is not None:
            due = await self.outbox.due(now=time.time() + 60, limit=100)
            self.seen_in_outbox.append(
                any(row.event_id == envelope.event_id for row in due)
            )
        self.published.append(envelope.event_id)
        if self.fail > 0:
            self.fail -= 1
            raise RuntimeError("backend unavailable")

    async def start_consumer(self, on_envelope: OnEnvelope) -> None:
        """Record consumer startup."""
        self.consumer_started = True

    async def close(self) -> None:
        """Implement the transport teardown protocol."""


async def _wait_until(condition: Callable[[], bool], timeout: float = 1.0) -> None:
    """Wait for a background condition or fail the current test."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(0.005)
    pytest.fail("condition was not met before timeout")


async def test_publish_writes_outbox_before_backend(tmp_path) -> None:
    """The backend sees a write-ahead row before its first publish call."""
    outbox = SqliteOutbox(tmp_path / "outbox.db")
    backend = _FlakyBackend(outbox=outbox)
    core = BusCore(backend=backend, outbox=outbox)
    await core.start()

    await core.publish(EventEnvelope(topic="outbox.before", payload={}))
    await _wait_until(lambda: len(backend.published) == 1)

    assert backend.seen_in_outbox == [True]
    await core.close()


async def test_backend_failure_keeps_row_and_relay_redelivers(tmp_path) -> None:
    """Failed publishes remain durable and are acked only after recovery."""
    outbox = SqliteOutbox(tmp_path / "outbox.db")
    backend = _FlakyBackend(fail=2, outbox=outbox)
    core = BusCore(
        backend=backend,
        outbox=outbox,
        relay_interval=0.01,
    )
    await core.start()
    envelope = EventEnvelope(topic="outbox.retry", payload={})

    await core.publish(envelope)
    await _wait_until(lambda: len(backend.published) == 3)
    await _wait_until(lambda: core.stats()["outbox_acked"] == 1)

    assert core.stats()["backend_failures"] == 2
    assert (await outbox.stats())["pending"] == 0
    await core.close()


async def test_restart_recovers_pending_rows(tmp_path) -> None:
    """A new core relays durable rows left by an earlier failed core."""
    path = tmp_path / "outbox.db"
    first_outbox = SqliteOutbox(path)
    first_backend = _FlakyBackend(fail=100, outbox=first_outbox)
    first_core = BusCore(
        backend=first_backend,
        outbox=first_outbox,
        relay_interval=0.05,
    )
    await first_core.start()

    await first_core.publish(EventEnvelope(topic="outbox.restart", payload={}))
    await _wait_until(lambda: first_core.stats()["backend_failures"] == 1)
    await first_core.close()

    second_outbox = SqliteOutbox(path)
    second_backend = _FlakyBackend(outbox=second_outbox)
    second_core = BusCore(
        backend=second_backend,
        outbox=second_outbox,
        relay_interval=0.01,
    )
    await second_core.start()
    await _wait_until(lambda: len(second_backend.published) == 1)

    assert (await second_outbox.stats())["pending"] == 0
    await second_core.close()


async def test_overflow_reject_new_raises_from_publish(tmp_path) -> None:
    """A reject-new outbox rejects before spawning a backend publish."""
    outbox = SqliteOutbox(tmp_path / "outbox.db", max_rows=0)
    backend = _FlakyBackend(outbox=outbox)
    core = BusCore(backend=backend, outbox=outbox)

    with pytest.raises(OutboxFullError):
        await core.publish(EventEnvelope(topic="outbox.full", payload={}))

    assert core.stats()["outbox_rejected"] == 1
    assert backend.published == []
    await core.close()


async def test_no_outbox_behavior_unchanged() -> None:
    """A core without an outbox still fans out to its backend."""
    backend = _FlakyBackend()
    core = BusCore(backend=backend)
    await core.start()

    await core.publish(EventEnvelope(topic="outbox.none", payload={}))
    await _wait_until(lambda: len(backend.published) == 1)

    assert core.stats() == {
        "outbox_pending": 0,
        "outbox_acked": 0,
        "outbox_retried": 0,
        "outbox_rejected": 0,
        "backend_failures": 0,
    }
    await core.close()


async def test_stats_counters(tmp_path) -> None:
    """Stats report successful acknowledgements and cached pending rows."""
    outbox = SqliteOutbox(tmp_path / "outbox.db")
    backend = _FlakyBackend(outbox=outbox)
    core = BusCore(backend=backend, outbox=outbox)
    await core.start()

    await core.publish(EventEnvelope(topic="outbox.stats", payload={}))
    await _wait_until(lambda: core.stats()["outbox_acked"] == 1)
    await core._relay_once()

    assert core.stats()["outbox_pending"] == 0
    assert core.stats()["outbox_acked"] == 1
    await core.close()
