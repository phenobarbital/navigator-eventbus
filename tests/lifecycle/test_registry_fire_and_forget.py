"""Dual-emit fire-and-forget tests for EventRegistry (FEAT-313 TASK-1821)."""
import asyncio
from dataclasses import dataclass

import pytest

from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.registry import EventRegistry
from navigator_eventbus.lifecycle.trace import TraceContext


@dataclass(frozen=True)
class _TestEvent(LifecycleEvent):
    detail: str = ""


class _FakeBus:
    """Duck-typed stand-in for EventBus.emit(channel, payload)."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    async def emit(self, channel: str, payload: dict) -> int:
        self.calls.append((channel, payload))
        return 1


class TestRegistryFireAndForget:
    @pytest.mark.asyncio
    async def test_dual_emit_forwards_to_bus_when_flag_set(self):
        bus = _FakeBus()
        registry = EventRegistry(event_bus=bus, forward_to_global=False)

        async def handler(e):
            pass

        registry.subscribe(_TestEvent, handler, forward_to_bus=True)
        evt = _TestEvent(
            trace_context=TraceContext.new_root(),
            source_type="test", source_name="unit", detail="hi",
        )
        await registry.emit(evt)
        await asyncio.sleep(0.05)  # let the fire-and-forget task complete

        assert len(bus.calls) == 1
        channel, payload = bus.calls[0]
        assert channel == "lifecycle._TestEvent"
        assert payload["detail"] == "hi"

    @pytest.mark.asyncio
    async def test_no_dual_emit_when_flag_unset(self):
        bus = _FakeBus()
        registry = EventRegistry(event_bus=bus, forward_to_global=False)

        async def handler(e):
            pass

        registry.subscribe(_TestEvent, handler)  # forward_to_bus defaults False
        evt = _TestEvent(
            trace_context=TraceContext.new_root(),
            source_type="test", source_name="unit",
        )
        await registry.emit(evt)
        await asyncio.sleep(0.05)

        assert bus.calls == []

    @pytest.mark.asyncio
    async def test_dual_emit_never_blocks_emitter(self):
        """The emitter's await returns before the bus call is awaited."""

        class _SlowBus:
            async def emit(self, channel, payload):
                await asyncio.sleep(1)
                return 1

        registry = EventRegistry(event_bus=_SlowBus(), forward_to_global=False)

        async def handler(e):
            pass

        registry.subscribe(_TestEvent, handler, forward_to_bus=True)
        evt = _TestEvent(
            trace_context=TraceContext.new_root(),
            source_type="test", source_name="unit",
        )
        # Should complete quickly — dual-emit is fire-and-forget.
        await asyncio.wait_for(registry.emit(evt), timeout=0.5)
