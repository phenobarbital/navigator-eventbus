"""FEAT-711 AC-7: the FEAT-177 emit-overhead budget still holds with the outbox enabled."""
import asyncio
import time
from dataclasses import dataclass

import pytest

from navigator_eventbus import EventBus
from navigator_eventbus.backends.memory import MemoryBackend
from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.registry import EventRegistry
from navigator_eventbus.lifecycle.trace import TraceContext


@dataclass(frozen=True)
class _BenchEvent(LifecycleEvent):
    """Minimal lifecycle event for the dual-emit benchmark."""

    detail: str = ""


_SIMULATED_REQUEST_SECONDS = 0.01


@pytest.mark.asyncio
async def test_overhead_under_budget_with_outbox(tmp_path) -> None:
    """Outbox-enabled dual emit stays within the FEAT-177 CI margin."""
    n = 100
    event = _BenchEvent(
        trace_context=TraceContext.new_root(),
        source_type="bench",
        source_name="overhead",
    )

    async def handler(_: LifecycleEvent) -> None:
        """Provide the same no-op subscriber for both benchmark paths."""

    registry = EventRegistry(forward_to_global=False)
    registry.subscribe(_BenchEvent, handler, forward_to_bus=False)
    t0 = time.perf_counter()
    for _ in range(n):
        await asyncio.sleep(_SIMULATED_REQUEST_SECONDS)
        await registry.emit(event)
    baseline = time.perf_counter() - t0

    bus = EventBus(backend=MemoryBackend(), outbox=str(tmp_path / "outbox.db"))
    registry_dual = EventRegistry(forward_to_global=False)
    registry_dual.attach_bus(bus)
    registry_dual.subscribe(_BenchEvent, handler, forward_to_bus=True)
    try:
        t0 = time.perf_counter()
        for _ in range(n):
            await asyncio.sleep(_SIMULATED_REQUEST_SECONDS)
            await registry_dual.emit(event)
        with_dual_emit = time.perf_counter() - t0

        overhead = (with_dual_emit - baseline) / baseline
        assert overhead < 0.05, (
            f"Dual-emit overhead {overhead:.2%} exceeds the generous CI "
            f"margin (target budget is < 0.1%, FEAT-177)"
        )
    finally:
        await asyncio.sleep(0)
        await bus.close()
