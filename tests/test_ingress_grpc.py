"""Tests for the gRPC ingress adapter (FEAT-312, TASK-1804).

Mudado desde
``packages/ai-parrot/tests/core/events/bus/test_ingress.py``
(ai-parrot@686aba1fe, FEAT-310) — gRPC portion, imports adapted to
``navigator_eventbus``. Skips entirely when ``grpc`` / the generated
modules are unavailable (extra ``[grpc]``).
"""
import asyncio
import json
import time

import pytest

from navigator_eventbus import Event, EventBus, Severity
from navigator_eventbus.hooks.base import BaseHook

grpc = pytest.importorskip("grpc")


async def wait_until(condition, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(0.01)
    pytest.fail("condition not met within timeout")


@pytest.fixture
async def bus():
    b = EventBus()
    yield b
    await b.close()


TOKEN = "sekret-token"


def _import_grpc_ingress():
    from navigator_eventbus.ingress.grpc import (
        GrpcIngress,
        validate_publish_request,
    )
    return GrpcIngress, validate_publish_request


def test_grpc_ingress_lazy_export():
    from navigator_eventbus import ingress as ingress_pkg
    GrpcIngress, _ = _import_grpc_ingress()
    assert ingress_pkg.GrpcIngress is GrpcIngress
    assert issubclass(GrpcIngress, BaseHook)


def test_grpc_validate_publish_request_boundary():
    _, validate_publish_request = _import_grpc_ingress()

    envelope = validate_publish_request(
        {
            "topic": "orders.created",
            "payload_json": json.dumps({"order_id": 7}),
            "severity": Severity.ERROR.value,
            "source": "grpc-client",
        }
    )
    assert envelope.topic == "orders.created"
    assert envelope.payload == {"order_id": 7}
    assert envelope.severity == Severity.ERROR

    with pytest.raises(ValueError):  # malformed JSON payload
        validate_publish_request({"topic": "a.b", "payload_json": "{{{"})
    with pytest.raises(ValueError):  # payload not an object
        validate_publish_request({"topic": "a.b", "payload_json": "[1,2]"})
    with pytest.raises(ValueError):  # missing topic
        validate_publish_request({"payload_json": "{}"})
    with pytest.raises(ValueError):  # bogus severity value
        validate_publish_request({"topic": "a.b", "severity": 999})


def test_grpc_priority_zero_is_low_not_default():
    """Explicit priority=0 (LOW) must survive; absent → NORMAL."""
    from navigator_eventbus.evb import EventPriority
    _, validate_publish_request = _import_grpc_ingress()

    low = validate_publish_request({"topic": "a.b", "priority": 0})
    assert low.priority == EventPriority.LOW

    unset = validate_publish_request({"topic": "a.b", "priority": None})
    assert unset.priority == EventPriority.NORMAL

    # Proto-level presence: unset optional field maps to None server-side.
    from navigator_eventbus.ingress.proto import events_pb2
    explicit = events_pb2.PublishRequest(version="1.0", topic="a.b", priority=0)
    absent = events_pb2.PublishRequest(version="1.0", topic="a.b")
    assert explicit.HasField("priority") is True
    assert absent.HasField("priority") is False


async def test_grpc_ingress_publish_end_to_end(bus):
    """In-process grpc.aio server round-trip with auth + validation."""
    GrpcIngress, _ = _import_grpc_ingress()
    from navigator_eventbus.ingress.proto import (
        events_pb2,
        events_pb2_grpc,
    )

    received: list[Event] = []
    bus.subscribe("grpc.*", lambda e: received.append(e))

    ingress = GrpcIngress(bus, address="127.0.0.1:50961", auth_token=TOKEN)
    await ingress.start()
    try:
        async with grpc.aio.insecure_channel("127.0.0.1:50961") as channel:
            stub = events_pb2_grpc.EventBusIngressStub(channel)

            # Unauthenticated → UNAUTHENTICATED.
            with pytest.raises(grpc.aio.AioRpcError) as excinfo:
                await stub.Publish(
                    events_pb2.PublishRequest(version="1.0", topic="grpc.x")
                )
            assert excinfo.value.code() == grpc.StatusCode.UNAUTHENTICATED

            metadata = (("authorization", f"Bearer {TOKEN}"),)

            # Valid publish → accepted + arrives on the bus.
            resp = await stub.Publish(
                events_pb2.PublishRequest(
                    version="1.0",
                    topic="grpc.ping",
                    payload_json=json.dumps({"n": 1}),
                ),
                metadata=metadata,
            )
            assert resp.status == "accepted"
            assert resp.event_id
            await wait_until(lambda: len(received) == 1)
            assert received[0].event_type == "grpc.ping"

            # Malformed → rejected at the IngressEnvelope boundary.
            resp = await stub.Publish(
                events_pb2.PublishRequest(
                    version="1.0", topic="", payload_json="{}"
                ),
                metadata=metadata,
            )
            assert resp.status == "rejected"
    finally:
        await ingress.stop()
