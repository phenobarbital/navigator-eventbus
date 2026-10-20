"""Tests for TASK-1841 — HookManager tri-state route_to_bus.

``route_to_bus: Optional[bool] = None`` auto-routes iff a bus is attached
via :meth:`HookManager.set_event_bus`; explicit ``True``/``False`` always
win. See ``tests/test_hooks_manager.py`` for the pre-existing legacy
dual-emit suite (some of those tests now pin ``route_to_bus=False``
explicitly to keep testing the legacy wire shape under the new
tri-state default).
"""
from unittest.mock import AsyncMock

from navigator_eventbus.hooks.manager import HookManager
from navigator_eventbus.hooks.models import HookEvent, HookType


def make_event(hook_type=HookType.SCHEDULER, event_type="tick") -> HookEvent:
    return HookEvent(
        hook_id="test-hook",
        hook_type=hook_type,
        event_type=event_type,
        payload={"value": 42},
    )


class FakeBus:
    def __init__(self):
        self.emitted = []

    async def emit(self, topic, payload=None, **kw):
        self.emitted.append((topic, payload, kw))


async def test_route_to_bus_auto_with_bus():
    mgr = HookManager()  # route_to_bus omitted → None → auto
    bus = FakeBus()
    mgr.set_event_bus(bus)
    assert mgr.route_to_bus is True  # effective value

    dispatch = mgr._build_dispatch()
    await dispatch(make_event())
    assert len(bus.emitted) == 1


async def test_route_to_bus_auto_without_bus():
    mgr = HookManager()
    assert mgr.route_to_bus is False  # no bus → auto-off, no error


async def test_route_to_bus_explicit_false_overrides_bus():
    mgr = HookManager(route_to_bus=False)
    mgr.set_event_bus(FakeBus())
    assert mgr.route_to_bus is False


async def test_route_to_bus_explicit_true_preserved():
    mgr = HookManager(route_to_bus=True)
    assert mgr.route_to_bus is True
    bus = FakeBus()
    mgr.set_event_bus(bus)
    assert mgr.route_to_bus is True

    dispatch = mgr._build_dispatch()
    await dispatch(make_event())
    assert len(bus.emitted) == 1


async def test_callback_still_fires_when_routed():
    mgr = HookManager()  # auto
    cb = AsyncMock()
    bus = FakeBus()
    mgr.set_event_callback(cb)
    mgr.set_event_bus(bus)

    dispatch = mgr._build_dispatch()
    event = make_event()
    await dispatch(event)

    cb.assert_awaited_once_with(event)
    assert len(bus.emitted) == 1


def test_auto_activation_logs_once(caplog):
    mgr = HookManager()
    with caplog.at_level("INFO", logger="navigator_eventbus.hooks.manager"):
        mgr.set_event_bus(FakeBus())
        mgr.set_event_bus(FakeBus())  # replace → flag reset → logs again
    assert sum("auto-enabled" in r.message for r in caplog.records) == 2


def test_auto_activation_not_logged_for_explicit_route_to_bus(caplog):
    mgr = HookManager(route_to_bus=True)
    with caplog.at_level("INFO", logger="navigator_eventbus.hooks.manager"):
        mgr.set_event_bus(FakeBus())
    assert sum("auto-enabled" in r.message for r in caplog.records) == 0
