"""Integration test: EventRegistry dual-emit → phase-1 EventBus facade
(FEAT-313 TASK-1825)."""
import asyncio
from dataclasses import dataclass

import pytest

from navigator_eventbus.evb import EventBus
from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.registry import EventRegistry
from navigator_eventbus.lifecycle.trace import TraceContext


@dataclass(frozen=True)
class _IntegrationEvent(LifecycleEvent):
    detail: str = ""


class TestDualEmitIntegration:
    @pytest.mark.asyncio
    async def test_emit_forwards_to_bus(self):
        bus = EventBus()
        received = []
        bus.on("lifecycle.*")(lambda envelope: received.append(envelope))
        await bus.connect()

        registry = EventRegistry(event_bus=bus, forward_to_global=False)

        async def handler(e):
            pass

        registry.subscribe(_IntegrationEvent, handler, forward_to_bus=True)

        evt = _IntegrationEvent(
            trace_context=TraceContext.new_root(),
            source_type="test", source_name="integration", detail="hello",
        )
        await registry.emit(evt)
        await asyncio.sleep(0.1)  # let fire-and-forget dual-emit task complete

        await bus.close()

        assert len(received) >= 1
        arrived = received[0]
        assert arrived.event_type == "lifecycle._IntegrationEvent"
        assert arrived.payload["detail"] == "hello"
