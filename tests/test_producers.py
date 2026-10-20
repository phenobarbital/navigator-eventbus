"""Unit tests for :class:`navigator_eventbus.producers.BusCoreProducer`.

FEAT-433, TASK-1863. ``asyncio_mode = "auto"`` is set in ``pyproject.toml``,
so plain ``async def test_*`` functions run without a decorator.
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from navigator_eventbus.core import BackpressureError, BusClosedError, BusCore
from navigator_eventbus.envelope import EventEnvelope, Severity
from navigator_eventbus.evb import EventPriority
from navigator_eventbus.producers import BusCoreProducer


class FakeBus:
    """Minimal BusCore stand-in that records envelopes."""

    def __init__(self, *, raises=None, delay: float = 0.0) -> None:
        self.published: list[EventEnvelope] = []
        self._raises = raises
        self._delay = delay

    async def publish(self, envelope: EventEnvelope) -> None:
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._raises is not None:
            raise self._raises
        self.published.append(envelope)


def provider_of(*returns):
    """bus_provider yielding a scripted sequence, then repeating the last."""
    seq = list(returns)

    def _p():
        return seq.pop(0) if len(seq) > 1 else seq[0]

    return _p


# --------------------------------------------------------------------------
# 1. Envelope wrapping
# --------------------------------------------------------------------------


async def test_publish_event_wraps_body_and_topic():
    bus = FakeBus()
    producer = BusCoreProducer(provider_of(bus))
    body = {"foo": "bar"}
    await producer.publish_event(body, "some.topic")
    assert len(bus.published) == 1
    envelope = bus.published[0]
    assert envelope.topic == "some.topic"
    assert envelope.payload == body


# --------------------------------------------------------------------------
# 2. Lazy bus resolution
# --------------------------------------------------------------------------


async def test_bus_provider_is_called_lazily_per_publish():
    bus = FakeBus()
    provider = provider_of(None, bus)
    producer = BusCoreProducer(provider)

    # Call 1: provider returns None -> fail-soft no-op.
    await producer.publish_event({}, "t")
    assert bus.published == []

    # Call 2: provider now returns the bus -> publishes.
    await producer.publish_event({}, "t")
    assert len(bus.published) == 1


# --------------------------------------------------------------------------
# 3-5. Missing bus
# --------------------------------------------------------------------------


async def test_missing_bus_fail_soft():
    producer = BusCoreProducer(provider_of(None), raise_on_error=False)
    await producer.publish_event({}, "t")  # must not raise


async def test_missing_bus_strict_raises():
    producer = BusCoreProducer(provider_of(None), raise_on_error=True)
    with pytest.raises(Exception):
        await producer.publish_event({}, "t")


async def test_missing_bus_raises_runtime_error():
    producer = BusCoreProducer(provider_of(None), raise_on_error=True)
    with pytest.raises(RuntimeError):
        await producer.publish_event({}, "t")


# --------------------------------------------------------------------------
# 6-8. Publish exceptions
# --------------------------------------------------------------------------


async def test_publish_exception_fail_soft():
    bus = FakeBus(raises=BusClosedError("closed"))
    producer = BusCoreProducer(provider_of(bus), raise_on_error=False)
    await producer.publish_event({}, "t")  # must not raise


async def test_publish_exception_strict_raises():
    bus = FakeBus(raises=BusClosedError("closed"))
    producer = BusCoreProducer(provider_of(bus), raise_on_error=True)
    with pytest.raises(BusClosedError):
        await producer.publish_event({}, "t")


async def test_backpressure_error_propagates_strict():
    bus = FakeBus(raises=BackpressureError("full"))
    producer = BusCoreProducer(provider_of(bus), raise_on_error=True)
    with pytest.raises(BackpressureError):
        await producer.publish_event({}, "t")


# --------------------------------------------------------------------------
# 9-10. Timeout
# --------------------------------------------------------------------------


async def test_timeout_fail_soft():
    bus = FakeBus(delay=1.0)
    producer = BusCoreProducer(
        provider_of(bus), timeout=0.05, raise_on_error=False
    )
    await producer.publish_event({}, "t")  # must not raise, must not hang


async def test_timeout_strict_raises():
    bus = FakeBus(delay=1.0)
    producer = BusCoreProducer(
        provider_of(bus), timeout=0.05, raise_on_error=True
    )
    with pytest.raises(TimeoutError):
        await producer.publish_event({}, "t")


# --------------------------------------------------------------------------
# 11. Legacy kwargs
# --------------------------------------------------------------------------


async def test_legacy_kwargs_accepted():
    bus = FakeBus()
    producer = BusCoreProducer(provider_of(bus))
    body = {"foo": "bar"}
    await producer.publish_event(body, "t", routing_key="x", whatever=1)
    envelope = bus.published[0]
    assert envelope.topic == "t"
    assert envelope.payload == body


# --------------------------------------------------------------------------
# 12-16. Timestamp handling
# --------------------------------------------------------------------------


async def test_explicit_timestamp_kwarg_is_used():
    bus = FakeBus()
    producer = BusCoreProducer(provider_of(bus))
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await producer.publish_event({}, "t", timestamp=ts)
    assert bus.published[0].timestamp == ts


async def test_default_timestamp_is_now():
    bus = FakeBus()
    producer = BusCoreProducer(provider_of(bus))
    before = datetime.now(timezone.utc)
    await producer.publish_event({}, "t")
    after = datetime.now(timezone.utc)
    envelope_ts = bus.published[0].timestamp
    assert envelope_ts.tzinfo is not None
    assert before - timedelta(seconds=1) <= envelope_ts <= after + timedelta(seconds=1)


async def test_ts_field_in_body_is_not_special_cased():
    bus = FakeBus()
    producer = BusCoreProducer(provider_of(bus))
    body = {"ts": "2020-01-01T00:00:00Z"}
    before = datetime.now(timezone.utc)
    await producer.publish_event(body, "t")
    after = datetime.now(timezone.utc)
    envelope = bus.published[0]
    assert envelope.payload["ts"] == "2020-01-01T00:00:00Z"
    assert before - timedelta(seconds=1) <= envelope.timestamp <= after + timedelta(
        seconds=1
    )


@pytest.mark.parametrize("raise_on_error", [True, False])
async def test_naive_timestamp_always_raises(raise_on_error):
    bus = FakeBus()
    producer = BusCoreProducer(provider_of(bus), raise_on_error=raise_on_error)
    naive = datetime(2026, 1, 1)  # no tzinfo
    with pytest.raises(ValueError):
        await producer.publish_event({}, "t", timestamp=naive)


async def test_bad_timestamp_type_is_ignored():
    bus = FakeBus()
    producer = BusCoreProducer(provider_of(bus))
    before = datetime.now(timezone.utc)
    await producer.publish_event({}, "t", timestamp="2026-01-01")
    after = datetime.now(timezone.utc)
    envelope_ts = bus.published[0].timestamp
    assert before - timedelta(seconds=1) <= envelope_ts <= after + timedelta(seconds=1)


# --------------------------------------------------------------------------
# 17. Cancellation
# --------------------------------------------------------------------------


async def test_cancellation_is_not_swallowed():
    bus = FakeBus(delay=1.0)
    producer = BusCoreProducer(provider_of(bus), timeout=None, raise_on_error=False)
    task = asyncio.create_task(producer.publish_event({}, "t"))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# --------------------------------------------------------------------------
# 18. Invalid timeout
# --------------------------------------------------------------------------


def test_invalid_timeout_rejected_at_init():
    with pytest.raises(ValueError):
        BusCoreProducer(provider_of(None), timeout=0)
    with pytest.raises(ValueError):
        BusCoreProducer(provider_of(None), timeout=-1)


# --------------------------------------------------------------------------
# 19-20. Constructor defaults: source / severity / priority
# --------------------------------------------------------------------------


async def test_constructor_source_severity_priority_stamped():
    bus = FakeBus()
    default_producer = BusCoreProducer(provider_of(bus))
    await default_producer.publish_event({}, "t")
    envelope = bus.published[0]
    assert envelope.source is None
    assert envelope.severity == Severity.INFO
    assert envelope.priority == EventPriority.NORMAL

    bus2 = FakeBus()
    custom_producer = BusCoreProducer(
        provider_of(bus2),
        source="svc",
        severity=Severity.ERROR,
        priority=EventPriority.HIGH,
    )
    await custom_producer.publish_event({}, "t")
    envelope2 = bus2.published[0]
    assert envelope2.source == "svc"
    assert envelope2.severity == Severity.ERROR
    assert envelope2.priority == EventPriority.HIGH


async def test_severity_kwarg_is_ignored_not_honoured():
    bus = FakeBus()
    producer = BusCoreProducer(provider_of(bus), severity=Severity.INFO)
    await producer.publish_event({}, "t", severity=Severity.CRITICAL)
    envelope = bus.published[0]
    assert envelope.severity == Severity.INFO


# --------------------------------------------------------------------------
# Source-level neutrality guard
# --------------------------------------------------------------------------


def test_producers_module_is_application_neutral():
    """Modeled on tests/test_neutrality.py — source scan, no import needed."""
    src = Path(__file__).parent.parent / "src" / "navigator_eventbus" / "producers.py"
    text = src.read_text()
    assert "fieldsync" not in text.lower()
    assert '"ts"' not in text and "'ts'" not in text


# --------------------------------------------------------------------------
# Root export (TASK-1864)
# --------------------------------------------------------------------------


def test_producer_exported_from_root():
    """BusCoreProducer is an eager, first-class root export."""
    import navigator_eventbus
    from navigator_eventbus import BusCoreProducer
    from navigator_eventbus.producers import BusCoreProducer as Direct

    assert BusCoreProducer is Direct
    assert "BusCoreProducer" in navigator_eventbus.__all__
    # Eager, not __getattr__-resolved: a lazily-mapped name is absent from dir().
    assert "BusCoreProducer" in dir(navigator_eventbus)
    assert "BusCoreProducer" not in navigator_eventbus._QUEUE_EXPORTS


# --------------------------------------------------------------------------
# Integration tests against a real BusCore (TASK-1865)
# --------------------------------------------------------------------------


async def wait_until(condition, timeout: float = 3.0) -> None:
    """Copied from tests/test_integration.py:22 — dispatch is asynchronous."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(0.01)
    pytest.fail("condition not met within timeout")


class TestBusCoreProducerIntegration:
    """Exercises BusCoreProducer against a real, started, in-memory BusCore."""

    async def test_publish_through_real_buscore(self):
        """Envelope reaches a subscriber with topic, payload and source intact."""
        bus = BusCore(workers=2)
        await bus.start()
        try:
            received: list[EventEnvelope] = []
            bus.subscribe("test.topic", lambda env: received.append(env))

            producer = BusCoreProducer(lambda: bus, source="test-svc")
            await producer.publish_event({"k": "v"}, "test.topic")

            await wait_until(lambda: received)
            assert received[0].topic == "test.topic"
            assert received[0].payload == {"k": "v"}
            assert received[0].source == "test-svc"
        finally:
            await bus.close()

    async def test_high_priority_routes_through_priority_queue(self):
        """A HIGH-priority producer's envelope arrives with .priority == HIGH."""
        bus = BusCore(workers=2)
        await bus.start()
        try:
            received: list[EventEnvelope] = []
            bus.subscribe("test.priority", lambda env: received.append(env))

            producer = BusCoreProducer(
                lambda: bus, source="test-svc", priority=EventPriority.HIGH
            )
            await producer.publish_event({"k": "v"}, "test.priority")

            await wait_until(lambda: received)
            assert received[0].priority is EventPriority.HIGH
        finally:
            await bus.close()
