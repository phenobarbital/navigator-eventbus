"""Unit tests for the injectable event-name table + wire_events (FEAT-313 TASK-1823)."""
from dataclasses import dataclass

import pytest

from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.registry import EventRegistry
from navigator_eventbus.lifecycle.trace import TraceContext
from navigator_eventbus.lifecycle.yaml_loader import (
    EVENT_CLASSES,
    _wire_handler,
    _wire_provider,
    register_event_names,
    wire_events,
)


@dataclass(frozen=True)
class _CustomEvent(LifecycleEvent):
    detail: str = ""


class TestRegisterEventNames:
    def setup_method(self):
        # Reset to baseline — only LifecycleEvent pre-registered
        EVENT_CLASSES.clear()
        EVENT_CLASSES[LifecycleEvent.__name__] = LifecycleEvent

    def teardown_method(self):
        EVENT_CLASSES.clear()
        EVENT_CLASSES[LifecycleEvent.__name__] = LifecycleEvent

    def test_register_adds_event(self):
        register_event_names({"_CustomEvent": _CustomEvent})
        assert "_CustomEvent" in EVENT_CLASSES

    def test_register_is_additive(self):
        register_event_names({"A": _CustomEvent})
        register_event_names({"B": _CustomEvent})
        assert "A" in EVENT_CLASSES
        assert "B" in EVENT_CLASSES

    def test_register_overrides_same_key(self):
        @dataclass(frozen=True)
        class _Other(LifecycleEvent):
            pass

        register_event_names({"X": _CustomEvent})
        register_event_names({"X": _Other})
        assert EVENT_CLASSES["X"] is _Other

    def test_lifecycle_event_always_preregistered(self):
        assert "LifecycleEvent" in EVENT_CLASSES


class TestUnknownEventName:
    def setup_method(self):
        EVENT_CLASSES.clear()
        EVENT_CLASSES[LifecycleEvent.__name__] = LifecycleEvent

    def teardown_method(self):
        EVENT_CLASSES.clear()
        EVENT_CLASSES[LifecycleEvent.__name__] = LifecycleEvent

    def test_unknown_name_raises_value_error(self):
        """Unregistered event name → clear error, not ImportError."""
        registry = EventRegistry(forward_to_global=False)
        with pytest.raises(ValueError, match="register_event_names"):
            _wire_handler(registry, {
                "events": ["NonExistentEventName"],
                "handler": f"{__name__}:_dummy_handler",
            })


class TestWireEvents:
    def setup_method(self):
        EVENT_CLASSES.clear()
        EVENT_CLASSES[LifecycleEvent.__name__] = LifecycleEvent
        register_event_names({"_CustomEvent": _CustomEvent})

    def teardown_method(self):
        EVENT_CLASSES.clear()
        EVENT_CLASSES[LifecycleEvent.__name__] = LifecycleEvent

    def test_wire_events_noop_on_empty_block(self):
        class _Bot:
            events = EventRegistry(forward_to_global=False)

        wire_events(_Bot(), None)
        wire_events(_Bot(), {})

    def test_wire_events_noop_when_bot_lacks_events(self):
        class _Bot:
            pass

        wire_events(_Bot(), {"subscribers": [{"handler": f"{__name__}:_dummy_handler"}]})

    def test_wire_handler_subscribes(self):
        class _Bot:
            def __init__(self):
                self.events = EventRegistry(forward_to_global=False)

        bot = _Bot()
        wire_events(bot, {
            "subscribers": [
                {"handler": f"{__name__}:_dummy_handler", "events": ["_CustomEvent"]},
            ],
        })
        assert bot.events.has_subscribers(_CustomEvent)

    def test_wire_handler_no_events_subscribes_wildcard(self):
        class _Bot:
            def __init__(self):
                self.events = EventRegistry(forward_to_global=False)

        bot = _Bot()
        wire_events(bot, {
            "subscribers": [{"handler": f"{__name__}:_dummy_handler"}],
        })
        assert bot.events.has_subscribers(LifecycleEvent)

    def test_wire_provider(self):
        registry = EventRegistry(forward_to_global=False)
        _wire_provider(registry, {"provider": f"{__name__}:_DummyProvider"})
        assert registry.has_subscribers(LifecycleEvent)

    def test_subscriber_entry_without_handler_or_provider_raises(self):
        class _Bot:
            def __init__(self):
                self.events = EventRegistry(forward_to_global=False)

        with pytest.raises(ValueError):
            wire_events(_Bot(), {"subscribers": [{"foo": "bar"}]})

    @pytest.mark.asyncio
    async def test_wired_handler_receives_matching_event(self):
        received = []

        async def handler(e):
            received.append(e)

        class _Bot:
            def __init__(self):
                self.events = EventRegistry(forward_to_global=False)

        import sys
        this_module = sys.modules[__name__]
        this_module._recording_handler = handler

        bot = _Bot()
        wire_events(bot, {
            "subscribers": [
                {"handler": f"{__name__}:_recording_handler", "events": ["_CustomEvent"]},
            ],
        })
        evt = _CustomEvent(trace_context=TraceContext.new_root(), source_type="test", source_name="unit")
        await bot.events.emit(evt)
        assert len(received) == 1


async def _dummy_handler(event):
    pass


class _DummyProvider:
    def register(self, registry):
        registry.subscribe(LifecycleEvent, self._on_event)

    async def _on_event(self, event):
        pass
