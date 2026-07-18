"""Unit tests for EventRegistry (FEAT-313 TASK-1821)."""
from dataclasses import dataclass

import pytest

from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.meta import SubscriberErrorEvent
from navigator_eventbus.lifecycle.registry import EventRegistry
from navigator_eventbus.lifecycle.trace import TraceContext


@dataclass(frozen=True)
class _TestEvent(LifecycleEvent):
    detail: str = ""


@dataclass(frozen=True)
class AfterTestEvent(LifecycleEvent):
    """Name intentionally starts with 'After' — exercises reverse dispatch order."""
    detail: str = ""


@pytest.fixture
def registry():
    return EventRegistry(forward_to_global=False)


def _make_event(cls=_TestEvent, **kwargs):
    kwargs.setdefault("trace_context", TraceContext.new_root())
    kwargs.setdefault("source_type", "test")
    kwargs.setdefault("source_name", "unit")
    return cls(**kwargs)


class TestEventRegistry:
    @pytest.mark.asyncio
    async def test_emit_calls_subscriber(self, registry):
        received = []

        async def handler(e):
            received.append(e)

        registry.subscribe(_TestEvent, handler)
        evt = _make_event()
        await registry.emit(evt)
        assert len(received) == 1
        assert received[0] is evt

    @pytest.mark.asyncio
    async def test_emit_never_raises_on_subscriber_error(self, registry):
        async def bad_handler(e):
            raise RuntimeError("boom")

        registry.subscribe(_TestEvent, bad_handler)
        evt = _make_event()
        await registry.emit(evt)  # must not raise

    @pytest.mark.asyncio
    async def test_subscriber_error_emits_meta_event(self):
        """SubscriberErrorEvent is always routed to the GLOBAL registry
        (_emit_subscriber_error uses get_global_registry()), never to the
        emitting registry itself — so the error subscriber must be attached
        via scope()."""
        import asyncio

        from navigator_eventbus.lifecycle.global_registry import scope

        errors = []

        async def bad_handler(e):
            raise RuntimeError("boom")

        async def error_handler(e):
            errors.append(e)

        with scope() as global_reg:
            global_reg.subscribe(SubscriberErrorEvent, error_handler)

            local_registry = EventRegistry(forward_to_global=False)
            local_registry.subscribe(_TestEvent, bad_handler)
            evt = _make_event()
            await local_registry.emit(evt)
            await asyncio.sleep(0)  # let the scheduled meta-emit task run

        assert len(errors) == 1
        assert "boom" in errors[0].error_message

    @pytest.mark.asyncio
    async def test_subscribe_returns_unique_id(self, registry):
        async def handler(e):
            pass

        id1 = registry.subscribe(_TestEvent, handler)
        id2 = registry.subscribe(_TestEvent, handler)
        assert id1 != id2

    @pytest.mark.asyncio
    async def test_unsubscribe_removes_subscription(self, registry):
        received = []

        async def handler(e):
            received.append(e)

        sub_id = registry.subscribe(_TestEvent, handler)
        assert registry.unsubscribe(sub_id) is True
        await registry.emit(_make_event())
        assert received == []

    def test_unsubscribe_unknown_id_returns_false(self, registry):
        assert registry.unsubscribe("nonexistent") is False

    @pytest.mark.asyncio
    async def test_where_predicate_filters(self, registry):
        received = []

        async def handler(e):
            received.append(e)

        registry.subscribe(_TestEvent, handler, where=lambda e: e.detail == "match")
        await registry.emit(_make_event(detail="no-match"))
        await registry.emit(_make_event(detail="match"))
        assert len(received) == 1
        assert received[0].detail == "match"

    @pytest.mark.asyncio
    async def test_before_events_forward_order(self, registry):
        order = []

        async def h1(e):
            order.append("h1")

        async def h2(e):
            order.append("h2")

        registry.subscribe(_TestEvent, h1)
        registry.subscribe(_TestEvent, h2)
        await registry.emit(_make_event())
        assert order == ["h1", "h2"]

    @pytest.mark.asyncio
    async def test_after_events_reverse_order(self, registry):
        order = []

        async def h1(e):
            order.append("h1")

        async def h2(e):
            order.append("h2")

        registry.subscribe(AfterTestEvent, h1)
        registry.subscribe(AfterTestEvent, h2)
        await registry.emit(_make_event(cls=AfterTestEvent))
        assert order == ["h2", "h1"]

    def test_has_subscribers(self, registry):
        assert registry.has_subscribers(_TestEvent) is False

        async def handler(e):
            pass

        registry.subscribe(_TestEvent, handler)
        assert registry.has_subscribers(_TestEvent) is True

    def test_add_provider_registers_subscriptions(self, registry):
        class _Provider:
            def register(self, reg):
                reg.subscribe(_TestEvent, self._on_event)

            async def _on_event(self, e):
                pass

        ids = registry.add_provider(_Provider())
        assert len(ids) == 1

    def test_add_provider_rejects_non_conforming(self, registry):
        with pytest.raises(TypeError):
            registry.add_provider(object())

    @pytest.mark.asyncio
    async def test_emit_nowait_schedules_emit(self, registry):
        received = []

        async def handler(e):
            received.append(e)

        registry.subscribe(_TestEvent, handler)
        registry.emit_nowait(_make_event())
        import asyncio
        await asyncio.sleep(0)
        assert len(received) == 1

    def test_emit_nowait_without_loop_drops_silently(self):
        # No running loop in this sync test — must not raise.
        reg = EventRegistry(forward_to_global=False)
        reg.emit_nowait(_make_event())
