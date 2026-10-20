"""Tests for the SqliteOutbox store (FEAT-711)."""
import logging
import time
from datetime import timedelta

import pytest

from navigator_eventbus import EventEnvelope, OutboxFullError, OutboxStore, SqliteOutbox


def _env(topic: str = "t.x") -> EventEnvelope:
    return EventEnvelope(topic=topic, payload={"k": 1})


async def test_outbox_append_idempotent(tmp_path):
    ob = SqliteOutbox(tmp_path / "o.db")
    e = _env()
    await ob.append(e)
    await ob.append(e)
    assert (await ob.stats())["pending"] == 1
    await ob.close()


async def test_outbox_due_order_and_backoff(tmp_path):
    ob = SqliteOutbox(tmp_path / "o.db")
    a, b, c = _env("a"), _env("b"), _env("c")
    for e in (a, b, c):
        await ob.append(e)
        time.sleep(0.01)
    now = time.time() + 1
    got = await ob.due(now=now, limit=10)
    assert [e.event_id for e in got] == [a.event_id, b.event_id, c.event_id]
    assert len(await ob.due(now=now, limit=2)) == 2
    await ob.nack(a.event_id, error="Boom", next_try_at=now + 10)
    got = await ob.due(now=now, limit=10)
    assert [e.event_id for e in got] == [b.event_id, c.event_id]
    got = await ob.due(now=now + 11, limit=10)
    assert len(got) == 3
    await ob.ack(a.event_id)
    assert (await ob.stats())["pending"] == 2
    await ob.close()


async def test_outbox_overflow_reject_new_raises(tmp_path, caplog):
    ob = SqliteOutbox(tmp_path / "o.db", max_rows=1)
    await ob.append(_env())
    with caplog.at_level(logging.ERROR):
        with pytest.raises(OutboxFullError):
            await ob.append(_env())
    st = await ob.stats()
    assert st["rejected"] == 1 and st["pending"] == 1
    await ob.close()


async def test_outbox_overflow_drop_oldest_counts(tmp_path):
    ob = SqliteOutbox(tmp_path / "o.db", max_rows=1, overflow="drop_oldest")
    first, second = _env("a"), _env("b")
    await ob.append(first)
    time.sleep(0.01)
    await ob.append(second)
    st = await ob.stats()
    assert st["dropped"] == 1 and st["pending"] == 1
    got = await ob.due(now=time.time() + 1, limit=10)
    assert [e.event_id for e in got] == [second.event_id]
    await ob.close()


async def test_outbox_max_age_purge_counted(tmp_path):
    ob = SqliteOutbox(tmp_path / "o.db", max_age=timedelta(seconds=0))
    await ob.append(_env())
    got = await ob.due(now=time.time() + 1, limit=10)
    assert got == []
    st = await ob.stats()
    assert st["expired"] == 1 and st["pending"] == 0
    await ob.close()


async def test_outbox_survives_reopen(tmp_path):
    p = tmp_path / "sub" / "o.db"
    ob = SqliteOutbox(p)
    e = _env()
    await ob.append(e)
    await ob.close()
    ob2 = SqliteOutbox(p)
    got = await ob2.due(now=time.time() + 1, limit=10)
    assert [x.event_id for x in got] == [e.event_id]
    assert (await ob2.stats())["pending"] == 1
    await ob2.close()


async def test_outbox_satisfies_protocol(tmp_path):
    assert isinstance(SqliteOutbox(tmp_path / "o.db"), OutboxStore)
