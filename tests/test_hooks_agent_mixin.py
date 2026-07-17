"""Tests for the HookableAgent mixin (FEAT-312, TASK-1803).

Mudado desde
``packages/ai-parrot/tests/core/hooks/{test_hookable_agent,
test_hookable_cleanup}.py`` (ai-parrot@686aba1fe, FEAT-310) — imports
adapted to ``navigator_eventbus``.
"""
from unittest.mock import AsyncMock

import pytest

from navigator_eventbus.hooks import BaseHook, HookableAgent, HookEvent, HookManager
from navigator_eventbus.hooks.models import HookType

# -- Fixtures / helpers -------------------------------------------------------

class StubHook(BaseHook):
    """Minimal concrete hook for testing."""

    hook_type = HookType.SCHEDULER

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


class MyAgent(HookableAgent):
    """Simulates an integration handler using the mixin."""

    def __init__(self):
        self._init_hooks()


class MyAgentCustomHandler(HookableAgent):
    """Agent that overrides handle_hook_event."""

    def __init__(self):
        self._init_hooks()
        self.received_events: list[HookEvent] = []

    async def handle_hook_event(self, event: HookEvent) -> None:
        self.received_events.append(event)


# -- Tests --------------------------------------------------------------------


class TestHookableAgentInit:
    """Tests for mixin initialization."""

    def test_init_creates_hook_manager(self):
        agent = MyAgent()
        assert isinstance(agent.hook_manager, HookManager)

    def test_hook_manager_without_init_raises(self):
        raw = HookableAgent()
        with pytest.raises(RuntimeError, match="call _init_hooks"):
            _ = raw.hook_manager

    def test_init_sets_callback(self):
        agent = MyAgent()
        assert agent.hook_manager._callback is not None


class TestAttachHook:
    """Tests for attach_hook()."""

    def test_attach_returns_hook_id(self):
        agent = MyAgent()
        hook = StubHook(name="test_hook")
        hook_id = agent.attach_hook(hook)
        assert hook_id == hook.hook_id

    def test_attach_registers_in_manager(self):
        agent = MyAgent()
        hook = StubHook(name="test_hook")
        agent.attach_hook(hook)
        assert agent.hook_manager.get_hook(hook.hook_id) is hook

    def test_attach_multiple_hooks(self):
        agent = MyAgent()
        h1 = StubHook(name="hook_a")
        h2 = StubHook(name="hook_b")
        agent.attach_hook(h1)
        agent.attach_hook(h2)
        assert len(agent.hook_manager.hooks) == 2


class TestLifecycle:
    """Tests for start_hooks / stop_hooks."""

    async def test_start_hooks(self):
        agent = MyAgent()
        hook = StubHook(name="start_test")
        hook.start = AsyncMock()
        agent.attach_hook(hook)

        await agent.start_hooks()
        hook.start.assert_awaited_once()

    async def test_stop_hooks(self):
        agent = MyAgent()
        hook = StubHook(name="stop_test")
        hook.stop = AsyncMock()
        agent.attach_hook(hook)

        await agent.stop_hooks()
        hook.stop.assert_awaited_once()

    async def test_start_stop_full_cycle(self):
        agent = MyAgent()
        hook = StubHook(name="lifecycle")
        hook.start = AsyncMock()
        hook.stop = AsyncMock()
        agent.attach_hook(hook)

        await agent.start_hooks()
        await agent.stop_hooks()
        hook.start.assert_awaited_once()
        hook.stop.assert_awaited_once()

    async def test_disabled_hook_not_started(self):
        agent = MyAgent()
        hook = StubHook(name="disabled_hook", enabled=False)
        hook.start = AsyncMock()
        agent.attach_hook(hook)

        await agent.start_hooks()
        hook.start.assert_not_awaited()


class TestHandleHookEvent:
    """Tests for event handling."""

    async def test_default_handler_does_not_raise(self):
        agent = MyAgent()
        event = HookEvent(
            hook_id="test123",
            hook_type=HookType.SCHEDULER,
            event_type="tick",
            payload={"msg": "hello"},
        )
        # Default handler logs — should not raise
        await agent.handle_hook_event(event)

    async def test_custom_handler_receives_event(self):
        agent = MyAgentCustomHandler()
        event = HookEvent(
            hook_id="test456",
            hook_type=HookType.FILE_WATCHDOG,
            event_type="file_created",
            payload={"path": "/tmp/test.csv"},
        )
        await agent.handle_hook_event(event)
        assert len(agent.received_events) == 1
        assert agent.received_events[0].event_type == "file_created"

    async def test_hook_fires_to_agent_handler(self):
        """End-to-end: hook fires event → mixin callback → agent handler."""
        agent = MyAgentCustomHandler()
        hook = StubHook(name="e2e_hook")
        agent.attach_hook(hook)

        # Simulate hook firing an event
        event = hook._make_event("triggered", {"data": "test"})
        await hook.on_event(event)

        assert len(agent.received_events) == 1
        assert agent.received_events[0].event_type == "triggered"
        assert agent.received_events[0].payload == {"data": "test"}


class TestMixinWithArbitraryClass:
    """Verify mixin works on any host class, not just bots."""

    def test_plain_class_with_mixin(self):

        class DataProcessor(HookableAgent):
            def __init__(self, name: str):
                self.name = name
                self._init_hooks()

        processor = DataProcessor("csv_processor")
        assert processor.name == "csv_processor"
        assert isinstance(processor.hook_manager, HookManager)

        hook = StubHook(name="file_watch")
        hook_id = processor.attach_hook(hook)
        assert hook_id is not None

    async def test_multiple_inheritance(self):

        class BaseThing:
            def __init__(self):
                self.thing_ready = True

        class HookableThing(BaseThing, HookableAgent):
            def __init__(self):
                super().__init__()
                self._init_hooks()

        obj = HookableThing()
        assert obj.thing_ready is True
        assert isinstance(obj.hook_manager, HookManager)

        hook = StubHook(name="multi_inherit")
        hook.start = AsyncMock()
        obj.attach_hook(hook)
        await obj.start_hooks()
        hook.start.assert_awaited_once()


# ---------------------------------------------------------------------------
# cleanup() — FEAT-114 bot-cleanup-lifecycle contract (origin TASK-815)
# ---------------------------------------------------------------------------


class _BaseWithCleanup:
    """Minimal synthetic 'bot base' that records super().cleanup() calls."""

    def __init__(self) -> None:
        self.super_cleanup_called = False

    async def cleanup(self) -> None:
        self.super_cleanup_called = True


class _BaseNoCleanup:
    """Minimal synthetic base with NO cleanup() — exercises the super() guard."""

    def __init__(self) -> None:
        pass


class _HookableWithBase(HookableAgent, _BaseWithCleanup):
    """Mixin declared BEFORE the base — correct MRO ordering."""

    def __init__(self, init_hooks: bool = True) -> None:
        _BaseWithCleanup.__init__(self)
        if init_hooks:
            self._init_hooks()


class _HookableNoBase(HookableAgent, _BaseNoCleanup):
    """Mixin with a base that has no cleanup() — exercises the super() guard."""

    def __init__(self, init_hooks: bool = True) -> None:
        _BaseNoCleanup.__init__(self)
        if init_hooks:
            self._init_hooks()


async def test_hookable_cleanup_calls_stop_hooks() -> None:
    """cleanup() must call stop_hooks() exactly once when _hook_manager exists."""
    bot = _HookableWithBase(init_hooks=True)
    bot.stop_hooks = AsyncMock()

    await bot.cleanup()

    bot.stop_hooks.assert_awaited_once()


async def test_hookable_cleanup_no_hooks_initialized() -> None:
    """cleanup() must not raise when _init_hooks() was never called."""
    bot = _HookableWithBase(init_hooks=False)
    # _hook_manager does not exist; the guard should make this a no-op
    await bot.cleanup()
    # super().cleanup() still runs even without hooks
    assert bot.super_cleanup_called is True


async def test_hookable_cleanup_chains_super() -> None:
    """cleanup() must invoke super().cleanup() (i.e. _BaseWithCleanup.cleanup)."""
    bot = _HookableWithBase(init_hooks=True)
    bot.stop_hooks = AsyncMock()

    await bot.cleanup()

    assert bot.super_cleanup_called is True


async def test_hookable_cleanup_swallows_stop_hooks_error() -> None:
    """If stop_hooks() raises, the exception must be swallowed and
    super().cleanup() must still run."""
    bot = _HookableWithBase(init_hooks=True)
    bot.stop_hooks = AsyncMock(side_effect=RuntimeError("boom"))

    # Must not propagate
    await bot.cleanup()

    # super().cleanup() still reached despite the error
    assert bot.super_cleanup_called is True
