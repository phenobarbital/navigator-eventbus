"""Hardening of the FEAT-711 outbox: file ownership, circuit-breaker, counters."""

import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass

import pytest

from navigator_eventbus import EventBus, EventEnvelope, SqliteOutbox
from navigator_eventbus.backends.memory import MemoryBackend
from navigator_eventbus.core import BusCore
from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.registry import EventRegistry
from navigator_eventbus.lifecycle.trace import TraceContext
from navigator_eventbus.outbox import OutboxFullError, OutboxLockedError


def _env(topic: str = "t.x") -> EventEnvelope:
    return EventEnvelope(topic=topic, payload={"k": 1})


async def test_second_store_on_same_file_is_rejected(tmp_path) -> None:
    """Two live stores must not share one database file."""
    first = SqliteOutbox(tmp_path / "o.db")
    with pytest.raises(OutboxLockedError):
        SqliteOutbox(tmp_path / "o.db")
    await first.close()
    reopened = SqliteOutbox(tmp_path / "o.db")  # released on close
    await reopened.close()


async def test_corrupt_row_counted_separately(tmp_path) -> None:
    """An undecodable row is deleted and reported as ``corrupt``, not ``expired``."""
    path = tmp_path / "o.db"
    ob = SqliteOutbox(path)
    await ob.append(_env())
    conn = sqlite3.connect(str(path))
    conn.execute("UPDATE evb_outbox SET envelope = ?", (json.dumps({"bad": 1}),))
    conn.commit()
    conn.close()

    assert await ob.due(now=time.time() + 1, limit=10) == []
    stats = await ob.stats()
    assert stats["corrupt"] == 1
    assert stats["expired"] == 0
    assert stats["pending"] == 0
    await ob.close()


class _DownBackend:
    """Backend that always fails and counts publish attempts."""

    def __init__(self) -> None:
        self.calls = 0

    async def publish(self, envelope: EventEnvelope) -> None:
        self.calls += 1
        raise RuntimeError("down")

    async def start_consumer(self, on_envelope) -> None:
        return None

    async def close(self) -> None:
        return None


async def test_relay_pass_stops_after_first_backend_failure(tmp_path) -> None:
    """A down backend costs one attempt per pass, not one per due row."""
    outbox = SqliteOutbox(tmp_path / "o.db")
    for i in range(5):
        await outbox.append(_env(f"t.{i}"))
    backend = _DownBackend()
    core = BusCore(backend=backend, outbox=outbox, relay_interval=3600)

    await core._relay_once()

    assert backend.calls == 1
    assert (await outbox.stats())["pending"] == 5
    await outbox.close()


class _BrokenOutbox:
    """Outbox whose append fails with a non-full error."""

    async def append(self, envelope) -> None:
        raise sqlite3.OperationalError("disk full")

    async def ack(self, event_id) -> None:
        return None

    async def nack(self, event_id, *, error, next_try_at) -> None:
        return None

    async def due(self, *, now, limit):
        return []

    async def stats(self):
        return {"pending": 0}

    async def close(self) -> None:
        return None


async def test_append_failure_is_counted_and_publish_continues() -> None:
    """A failing outbox degrades durability visibly via stats()."""
    backend = MemoryBackend()
    core = BusCore(backend=backend, outbox=_BrokenOutbox(), relay_interval=3600)
    await core.start()
    await core.publish(_env())
    assert core.stats()["outbox_append_failed"] == 1
    await core.close()


@pytest.mark.parametrize("name", ["Memory", " MEMORY "])
def test_transport_name_is_normalised(name) -> None:
    """``BUS_TRANSPORT``-style values are case/whitespace tolerant."""
    bus = EventBus(transport=name)
    assert isinstance(bus.core._backend, MemoryBackend)


def test_invalid_overflow_config_names_the_key(tmp_path, monkeypatch) -> None:
    """A bad ``BUS_OUTBOX_OVERFLOW`` value is reported with its config key."""
    from navigator_eventbus import evb

    real = evb.nav_config.get

    def fake(key, *a, **kw):
        return "bogus" if key == "BUS_OUTBOX_OVERFLOW" else real(key, *a, **kw)

    monkeypatch.setattr(evb.nav_config, "get", fake)
    with pytest.raises(ValueError, match="BUS_OUTBOX_OVERFLOW"):
        EventBus(backend=MemoryBackend(), outbox=str(tmp_path / "o.db"))


@dataclass(frozen=True)
class _Ev(LifecycleEvent):
    detail: str = ""


async def test_forward_failure_is_logged(caplog) -> None:
    """A failing bus emit is logged by the done-callback, deterministically."""

    class _FullBus:
        async def emit(self, channel, payload) -> None:
            raise OutboxFullError("full")

    async def handler(_: LifecycleEvent) -> None:
        return None

    registry = EventRegistry(forward_to_global=False)
    registry.attach_bus(_FullBus())
    registry.subscribe(_Ev, handler, forward_to_bus=True)
    event = _Ev(
        trace_context=TraceContext.new_root(), source_type="t", source_name="n"
    )
    with caplog.at_level("ERROR"):
        await registry.emit(event)
        await asyncio.sleep(0.05)
    assert any("OutboxFullError" in r.getMessage() for r in caplog.records)
