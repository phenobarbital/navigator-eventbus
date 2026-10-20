import asyncio
from dataclasses import dataclass

import pytest

from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.registry import EventRegistry
from navigator_eventbus.lifecycle.trace import TraceContext


@dataclass(frozen=True)
class _Evt(LifecycleEvent):
    secret: str = "s3cr3t"


class _SpyBus:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def emit(self, channel: str, payload: dict) -> int:
        self.calls.append((channel, payload))
        return 1


def _evt() -> _Evt:
    return _Evt(
        trace_context=TraceContext.new_root(),
        source_type="test",
        source_name="registry-bus-hook",
    )


async def _flush() -> None:
    await asyncio.sleep(0)


async def _handler(event: LifecycleEvent) -> None:
    del event


async def test_attach_bus_after_construction_forwards(fresh_global_registry) -> None:
    bus = _SpyBus()
    registry = fresh_global_registry
    registry.attach_bus(bus)
    registry.subscribe(_Evt, _handler, forward_to_bus=True)

    await registry.emit(_evt())
    await _flush()

    assert registry.event_bus is bus
    assert bus.calls[0][0] == "lifecycle._Evt"


async def test_detach_bus_stops_forwarding() -> None:
    bus = _SpyBus()
    registry = EventRegistry(event_bus=bus, forward_to_global=False)
    registry.subscribe(_Evt, _handler, forward_to_bus=True)

    registry.detach_bus()
    await registry.emit(_evt())
    await _flush()

    assert registry.event_bus is None
    assert bus.calls == []


async def test_serializer_redacts_and_none_skips() -> None:
    bus = _SpyBus()
    registry = EventRegistry(
        event_bus=bus,
        forward_to_global=False,
        bus_serializer=lambda event: {"event_class": type(event).__name__},
    )
    registry.subscribe(_Evt, _handler, forward_to_bus=True)

    await registry.emit(_evt())
    await _flush()

    assert bus.calls[0][1] == {"event_class": "_Evt"}

    registry.attach_bus(bus, serializer=lambda event: None)
    await registry.emit(_evt())
    await _flush()

    assert len(bus.calls) == 1


async def test_subscription_serializer_overrides_registry() -> None:
    bus = _SpyBus()
    registry = EventRegistry(
        event_bus=bus,
        forward_to_global=False,
        bus_serializer=lambda event: {"serializer": "registry"},
    )
    registry.subscribe(
        _Evt,
        _handler,
        forward_to_bus=True,
        bus_serializer=lambda event: {"serializer": "subscription"},
    )

    await registry.emit(_evt())
    await _flush()

    assert bus.calls[0][1] == {"serializer": "subscription"}


async def test_serializer_exception_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    def raising_serializer(event: LifecycleEvent) -> dict:
        del event
        raise ValueError("serializer failed")

    bus = _SpyBus()
    registry = EventRegistry(
        event_bus=bus,
        forward_to_global=False,
        bus_serializer=raising_serializer,
    )
    registry.subscribe(_Evt, _handler, forward_to_bus=True)

    await registry.emit(_evt())
    await _flush()

    assert "Bus serializer raised for _Evt; forward skipped" in caplog.text
    assert bus.calls == []


async def test_default_payload_unchanged_without_serializer() -> None:
    bus = _SpyBus()
    registry = EventRegistry(event_bus=bus, forward_to_global=False)
    registry.subscribe(_Evt, _handler, forward_to_bus=True)
    event = _evt()

    await registry.emit(event)
    await _flush()

    assert bus.calls[0][1] == event.to_dict()


async def test_attach_bus_idempotent_and_prefix_override() -> None:
    bus = _SpyBus()
    registry = EventRegistry(forward_to_global=False)
    registry.subscribe(_Evt, _handler, forward_to_bus=True)

    registry.attach_bus(bus)
    registry.attach_bus(bus, channel_prefix="telemetry")
    await registry.emit(_evt())
    await _flush()

    assert registry.event_bus is bus
    assert bus.calls[0][0] == "telemetry._Evt"
