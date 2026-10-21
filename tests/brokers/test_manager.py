"""Generic BrokerManager lifecycle tests (FEAT-431 Module 3, TASK-4564)."""

from __future__ import annotations

import pytest
from aiohttp import web

from navigator_eventbus.brokers import BrokerManager


class DummyManager(BrokerManager):
    _name_ = "dummy_manager"

    def __init__(self, *args, **kwargs):
        self.calls: list[str] = []
        super().__init__(*args, **kwargs)

    async def connect(self):
        self.calls.append("connect")

    async def disconnect(self):
        self.calls.append("disconnect")

    async def publish_message(self, body, queue_name=None, **kwargs):
        self.calls.append("publish")

    async def consume_messages(self, queue_name, callback, **kwargs): ...

    async def process_message(self, body, properties=None): ...

    async def _subscribe(self, app):
        self.calls.append("subscribe")

    async def _stop_consuming(self):
        self.calls.append("stop_consuming")


def test_abstract_methods_enforced():
    class Incomplete(BrokerManager):
        async def connect(self): ...
        async def disconnect(self): ...
        async def publish_message(self, body, queue_name=None, **kw): ...
        async def consume_messages(self, queue_name, callback, **kw): ...

    with pytest.raises(TypeError):
        Incomplete()


async def test_manager_single_connection():
    m = DummyManager(num_workers=2)
    await m.start(web.Application())
    assert m.calls.count("connect") == 1
    await m.stop(None)


async def test_manager_start_runs_both_halves():
    m = DummyManager(num_workers=3)
    await m.start(web.Application())
    assert len(m._workers) == 3
    assert "subscribe" in m.calls
    await m.stop(None)


async def test_manager_auto_subscribe_false():
    """Regression for D2: the flag, not _queue_name/_callback_, decides.

    DummyManager has no BrokerConsumer base, so consumer kwargs (queue_name,
    callback) would reach object.__init__ — the real-class D2 regression with
    those kwargs lives in TASK-4565 (test_rmq_manager.py).
    """
    m = DummyManager(auto_subscribe=False, num_workers=2)
    await m.start(web.Application())
    assert "connect" in m.calls
    assert len(m._workers) == 2
    assert "subscribe" not in m.calls
    await m.stop(None)


async def test_manager_auto_subscribe_default_true():
    m = DummyManager()
    await m.start(web.Application())
    assert m.calls.count("subscribe") == 1
    await m.stop(None)


def test_manager_setup_registers_name():
    app = web.Application()
    m = DummyManager()
    m.setup(app)
    assert app["dummy_manager"] is m
    assert m.start in app.on_startup
    assert m.stop in app.on_shutdown


async def test_manager_stop_drains_and_disconnects():
    """Consumers stop first, then the queue drains, workers end, disconnect once."""
    m = DummyManager(num_workers=1)
    await m.start(web.Application())
    m.event_queue.put_nowait({"body": "b", "queue_name": "q", "routing_key": "r"})
    await m.stop(None)
    assert m.calls[-3:] == ["stop_consuming", "publish", "disconnect"]
    assert all(t.done() for t in m._workers)
    assert m.calls.count("disconnect") == 1
    assert m.event_queue.empty()


async def test_manager_stop_continues_when_stop_consuming_fails():
    m = DummyManager(num_workers=1)

    async def boom():
        raise RuntimeError("boom")

    m._stop_consuming = boom
    await m.start(web.Application())
    await m.stop(None)
    assert m.calls.count("disconnect") == 1
    assert all(t.done() for t in m._workers)


async def test_manager_start_cleans_up_when_subscribe_fails():
    """Regression for issue:545bb0fa9305 (TASK-4569).

    aiohttp may not run ``on_shutdown`` after a failed startup, so a
    ``_subscribe`` failure inside ``start()`` must stop the workers and
    disconnect before the error propagates.
    """
    m = DummyManager(num_workers=2)

    async def boom(app):
        m.calls.append("subscribe")
        raise RuntimeError("subscribe failed")

    m._subscribe = boom
    with pytest.raises(RuntimeError, match="subscribe failed"):
        await m.start(web.Application())
    assert m.calls.count("connect") == 1
    assert "stop_consuming" in m.calls
    assert m.calls.count("disconnect") == 1
    assert m.calls[-1] == "disconnect"
    assert len(m._workers) == 2
    assert all(t.done() for t in m._workers)
