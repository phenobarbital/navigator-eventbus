"""Unit tests for HookManager (FEAT-312, TASK-1803).

Mudado desde
``packages/ai-parrot/tests/core/hooks/{test_hookmanager_route_to_bus,
test_hookmanager_eventbus}.py`` (ai-parrot@686aba1fe, FEAT-310) — imports
adapted to ``navigator_eventbus``. ``HookType.JIRA_WEBHOOK`` is
registered locally by the one test that needs a non-generic topic
(simulating ai-parrot's own phase-4 registration).
"""
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from navigator_eventbus import Event, EventBus
from navigator_eventbus.envelope import Severity
from navigator_eventbus.hooks.manager import HookManager
from navigator_eventbus.hooks.models import HOOK_TYPES, HookEvent, HookType


def make_event(hook_type=HookType.SCHEDULER, event_type="tick") -> HookEvent:
    return HookEvent(
        hook_id="test-hook",
        hook_type=hook_type,
        event_type=event_type,
        payload={"value": 42},
        metadata={"m": 1},
        target_type="agent",
        target_id="my-agent",
    )


async def wait_until(condition, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(0.01)
    pytest.fail("condition not met within timeout")


@pytest.fixture
def jira_webhook_registered():
    """Simulates ai-parrot registering its own hook type at import time."""
    HOOK_TYPES.register(HookType.JIRA_WEBHOOK)
    yield HookType.JIRA_WEBHOOK
    HOOK_TYPES.unregister(HookType.JIRA_WEBHOOK)


async def test_route_to_bus_publishes_envelope(jira_webhook_registered):
    """route_to_bus=True → first-class hooks.<type>.<event> publication."""
    bus = EventBus()
    received: list[Event] = []

    async def observer(event):
        received.append(event)

    bus.subscribe("hooks.*", observer)

    mgr = HookManager(route_to_bus=True)
    assert mgr.route_to_bus is True
    cb = AsyncMock()
    mgr.set_event_callback(cb)
    mgr.set_event_bus(bus)

    dispatch = mgr._build_dispatch()
    hook_event = make_event(jira_webhook_registered, "issue_created")
    await dispatch(hook_event)

    cb.assert_awaited_once_with(hook_event)  # callback untouched
    await wait_until(lambda: len(received) == 1)
    event = received[0]
    assert event.event_type == "hooks.jira_webhook.issue_created"
    assert event.payload == {"value": 42}  # hook payload, not model_dump
    assert event.source == "test-hook"
    assert event.metadata["m"] == 1
    assert event.metadata["target_type"] == "agent"
    assert event.metadata["target_id"] == "my-agent"
    await bus.close()


async def test_route_to_bus_severity_from_metadata():
    bus = EventBus()
    envelopes = []
    bus.core.subscribe("hooks.*", lambda env: envelopes.append(env))

    mgr = HookManager(route_to_bus=True)
    mgr.set_event_bus(bus)
    dispatch = mgr._build_dispatch()

    critical = make_event()
    critical.metadata["severity"] = "critical"
    await dispatch(critical)
    plain = make_event(event_type="plain")
    await dispatch(plain)

    await wait_until(lambda: len(envelopes) == 2)
    by_topic = {env.topic: env for env in envelopes}
    assert by_topic["hooks.scheduler.tick"].severity == Severity.CRITICAL
    assert "severity" not in by_topic["hooks.scheduler.tick"].metadata
    assert by_topic["hooks.scheduler.plain"].severity == Severity.INFO
    await bus.close()


async def test_route_to_bus_default_off_legacy_dual_emit():
    """Default OFF → byte-identical legacy dual-emit wire shape."""
    mgr = HookManager()
    assert mgr.route_to_bus is False
    cb = AsyncMock()
    bus = MagicMock()
    bus.emit = AsyncMock(return_value=1)
    mgr._callback = cb
    mgr._event_bus = bus

    dispatch = mgr._build_dispatch()
    event = make_event(HookType.SCHEDULER, "tick")
    await dispatch(event)

    cb.assert_awaited_once_with(event)
    bus.emit.assert_awaited_once_with(
        "hooks.scheduler.tick",
        event.model_dump(),
    )


async def test_orchestrator_callback_still_fires():
    """Callback path is invoked in BOTH modes (never replaced)."""
    for route in (False, True):
        mgr = HookManager(route_to_bus=route)
        cb = AsyncMock()
        bus = MagicMock()
        bus.emit = AsyncMock(return_value=1)
        mgr.set_event_callback(cb)
        mgr.set_event_bus(bus)

        dispatch = mgr._build_dispatch()
        event = make_event()
        await dispatch(event)
        cb.assert_awaited_once_with(event)
        bus.emit.assert_awaited_once()


async def test_route_to_bus_setter_reinjects_hooks():
    mgr = HookManager()
    hook = MagicMock()
    hook.hook_id = "h1"
    hook.enabled = True
    mgr._hooks["h1"] = hook
    mgr._event_bus = MagicMock()

    mgr.route_to_bus = True
    assert mgr.route_to_bus is True
    hook.set_callback.assert_called()


async def test_bus_emit_failure_isolated_in_route_mode():
    mgr = HookManager(route_to_bus=True)
    cb = AsyncMock()
    bus = MagicMock()
    bus.emit = AsyncMock(side_effect=RuntimeError("bus down"))
    mgr._callback = cb
    mgr._event_bus = bus

    dispatch = mgr._build_dispatch()
    event = make_event()
    await dispatch(event)  # must not raise
    cb.assert_awaited_once_with(event)


# ---------------------------------------------------------------------------
# Legacy dual-emit (TASK-272 origin)
# ---------------------------------------------------------------------------


def _make_hook(hook_id="h1"):
    hook = MagicMock()
    hook.hook_id = hook_id
    hook.enabled = True
    hook.hook_type = HookType.SCHEDULER
    hook.name = hook_id
    hook._callback = None

    def set_cb(cb):
        hook._callback = cb

    hook.set_callback.side_effect = set_cb
    return hook


class TestSetEventBus:
    def test_set_event_bus_stores_bus(self):
        mgr = HookManager()
        bus = MagicMock()
        mgr.set_event_bus(bus)
        assert mgr._event_bus is bus

    def test_set_event_bus_updates_existing_hooks(self):
        mgr = HookManager()
        hook = _make_hook()
        mgr._hooks["h1"] = hook
        bus = MagicMock()
        mgr.set_event_bus(bus)
        hook.set_callback.assert_called()

    def test_without_bus_build_dispatch_returns_callback(self):
        mgr = HookManager()
        cb = AsyncMock()
        mgr._callback = cb
        dispatch = mgr._build_dispatch()
        assert dispatch is cb

    def test_without_callback_or_bus_build_dispatch_returns_none(self):
        mgr = HookManager()
        assert mgr._build_dispatch() is None


class TestDualEmit:
    async def test_dual_emit_calls_callback_and_bus(self):
        mgr = HookManager()
        cb = AsyncMock()
        bus = MagicMock()
        bus.emit = AsyncMock(return_value=1)
        mgr._callback = cb
        mgr._event_bus = bus

        dispatch = mgr._build_dispatch()
        event = make_event(HookType.SCHEDULER, "tick")
        await dispatch(event)

        cb.assert_awaited_once_with(event)
        bus.emit.assert_awaited_once_with(
            "hooks.scheduler.tick",
            event.model_dump(),
        )

    async def test_dual_emit_channel_uses_hook_type_and_event_type(self):
        mgr = HookManager()
        mgr._callback = AsyncMock()
        bus = MagicMock()
        bus.emit = AsyncMock(return_value=1)
        mgr._event_bus = bus

        dispatch = mgr._build_dispatch()
        event = make_event(HookType.POSTGRES_LISTEN, "row_inserted")
        await dispatch(event)

        bus.emit.assert_awaited_once_with(
            "hooks.postgres_listen.row_inserted",
            event.model_dump(),
        )

    async def test_no_bus_only_callback_called(self):
        mgr = HookManager()
        cb = AsyncMock()
        mgr._callback = cb

        dispatch = mgr._build_dispatch()
        event = make_event()
        await dispatch(event)

        cb.assert_awaited_once_with(event)

    async def test_bus_only_no_callback(self):
        mgr = HookManager()
        bus = MagicMock()
        bus.emit = AsyncMock(return_value=1)
        mgr._event_bus = bus

        dispatch = mgr._build_dispatch()
        event = make_event()
        await dispatch(event)

        bus.emit.assert_awaited_once()

    async def test_bus_emit_failure_does_not_raise(self):
        mgr = HookManager()
        cb = AsyncMock()
        mgr._callback = cb
        bus = MagicMock()
        bus.emit = AsyncMock(side_effect=RuntimeError("redis down"))
        mgr._event_bus = bus

        dispatch = mgr._build_dispatch()
        event = make_event()
        await dispatch(event)

        cb.assert_awaited_once_with(event)


class TestRegisterWithBus:
    def test_new_hook_registered_after_bus_set_gets_dual_emit(self):
        mgr = HookManager()
        cb = AsyncMock()
        bus = MagicMock()
        mgr.set_event_callback(cb)
        mgr.set_event_bus(bus)

        hook = _make_hook("h2")
        mgr.register(hook)

        hook.set_callback.assert_called()
        injected = hook._callback
        assert injected is not cb

    def test_set_event_callback_after_bus_uses_dual_emit(self):
        mgr = HookManager()
        bus = MagicMock()
        bus.emit = AsyncMock()
        mgr.set_event_bus(bus)

        cb = AsyncMock()
        mgr.set_event_callback(cb)

        dispatch = mgr._build_dispatch()
        assert dispatch is not cb

    async def test_stale_closure_hook_sees_callback_set_after_registration(self):
        """Hooks registered between set_event_bus and set_event_callback still
        invoke the callback because _dual_emit reads self._callback at dispatch
        time rather than capturing it at closure-creation time."""
        mgr = HookManager()
        bus = MagicMock()
        bus.emit = AsyncMock(return_value=1)
        mgr.set_event_bus(bus)

        # Register hook BEFORE the callback is set — this is the hazard window.
        hook = _make_hook("stale-window")
        mgr.register(hook)

        # Now set the callback (after registration).
        cb = AsyncMock()
        mgr.set_event_callback(cb)

        # Fire an event through the dispatch that was injected at register time.
        injected_dispatch = hook._callback
        assert injected_dispatch is not None
        event = make_event()
        await injected_dispatch(event)

        # Dynamic self._callback reference ensures cb is still called.
        cb.assert_awaited_once_with(event)
        bus.emit.assert_awaited_once()


class TestSyncCallback:
    async def test_sync_callback_called_without_error(self):
        """A plain synchronous callable registered as the callback should be
        invoked correctly without raising (iscoroutinefunction guard)."""
        mgr = HookManager()
        bus = MagicMock()
        bus.emit = AsyncMock(return_value=1)
        mgr._event_bus = bus

        calls = []

        def sync_cb(event):
            calls.append(event)

        mgr._callback = sync_cb

        dispatch = mgr._build_dispatch()
        event = make_event()
        await dispatch(event)

        assert len(calls) == 1
        assert calls[0] is event
        bus.emit.assert_awaited_once()

    async def test_async_callback_still_awaited(self):
        """Async callbacks continue to be awaited correctly after the guard."""
        mgr = HookManager()
        bus = MagicMock()
        bus.emit = AsyncMock(return_value=1)
        mgr._event_bus = bus

        cb = AsyncMock()
        mgr._callback = cb

        dispatch = mgr._build_dispatch()
        event = make_event()
        await dispatch(event)

        cb.assert_awaited_once_with(event)
