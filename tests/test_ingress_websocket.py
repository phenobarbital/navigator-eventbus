"""Tests for the WebSocket ingress adapter (FEAT-312, TASK-1804).

Mudado desde
``packages/ai-parrot/tests/core/events/bus/test_ingress.py``
(ai-parrot@686aba1fe, FEAT-310) — WebSocket portion, imports adapted to
``navigator_eventbus``. The gRPC portion lives in
``tests/test_ingress_grpc.py``.
"""
import asyncio
import time

import pytest
from aiohttp import web

from navigator_eventbus import Event, EventBus, Severity
from navigator_eventbus.hooks.base import BaseHook
from navigator_eventbus.ingress import WebSocketIngress

TOKEN = "sekret-token"


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


@pytest.fixture
def ingress(bus):
    return WebSocketIngress(bus, auth_token=TOKEN)


@pytest.fixture
def app(ingress):
    application = web.Application()
    ingress.setup_routes(application)
    return application


def test_ws_ingress_is_a_base_hook(ingress):
    assert isinstance(ingress, BaseHook)


async def test_ws_ingress_valid_event_reaches_bus(aiohttp_client, bus, app):
    received: list[Event] = []

    async def observer(event):
        received.append(event)

    bus.subscribe("orders.*", observer)

    client = await aiohttp_client(app)
    ws = await client.ws_connect(f"/api/v1/events/ws?token={TOKEN}")
    await ws.send_json(
        {
            "topic": "orders.created",
            "payload": {"order_id": 7},
            "severity": Severity.WARNING.value,
            "source": "external-erp",
        }
    )
    ack = await ws.receive_json()
    assert ack["status"] == "accepted"
    assert ack["event_id"]

    await wait_until(lambda: len(received) == 1)
    event = received[0]
    assert event.event_type == "orders.created"
    assert event.payload == {"order_id": 7}
    assert event.source == "external-erp"
    assert event.timestamp.tzinfo is not None
    await ws.close()


async def test_ws_ingress_malformed_payload_rejected(aiohttp_client, bus, app):
    received: list[Event] = []
    bus.subscribe("*", lambda e: received.append(e))

    client = await aiohttp_client(app)
    ws = await client.ws_connect(f"/api/v1/events/ws?token={TOKEN}")

    # Not JSON at all.
    await ws.send_str("this is not json{{{")
    err = await ws.receive_json()
    assert err["status"] == "rejected"

    # Extra field — forbidden by IngressEnvelope (extra="forbid").
    await ws.send_json({"topic": "a.b", "nope": True})
    err = await ws.receive_json()
    assert err["status"] == "rejected"
    assert "nope" in err["error"]

    # Missing topic.
    await ws.send_json({"payload": {}})
    err = await ws.receive_json()
    assert err["status"] == "rejected"

    # The connection SURVIVES — a valid event still goes through.
    await ws.send_json({"topic": "a.b", "payload": {"ok": 1}})
    ack = await ws.receive_json()
    assert ack["status"] == "accepted"
    await wait_until(lambda: any(e.event_type == "a.b" for e in received))
    assert len([e for e in received if e.event_type == "a.b"]) == 1
    await ws.close()


async def test_ws_ingress_requires_auth(aiohttp_client, app):
    client = await aiohttp_client(app)
    # No token → 401 before the upgrade.
    resp = await client.get("/api/v1/events/ws")
    assert resp.status == 401
    # Wrong token → 401.
    resp = await client.get("/api/v1/events/ws?token=wrong")
    assert resp.status == 401
    # Bearer header works.
    ws = await client.ws_connect(
        "/api/v1/events/ws",
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    await ws.close()


async def test_ws_ingress_no_token_configured_refuses_all(aiohttp_client, bus):
    ingress = WebSocketIngress(bus, auth_token="")
    application = web.Application()
    ingress.setup_routes(application)
    client = await aiohttp_client(application)
    resp = await client.get(f"{ingress.url}?token=anything")
    assert resp.status == 401  # auth required by default


async def test_ws_ingress_stop_closes_connections(aiohttp_client, bus, ingress, app):
    client = await aiohttp_client(app)
    ws = await client.ws_connect(f"/api/v1/events/ws?token={TOKEN}")
    await wait_until(lambda: len(ingress._websockets) == 1)
    await ingress.stop()
    assert ingress._websockets == set()
    msg = await ws.receive()
    assert msg.type.name in ("CLOSE", "CLOSING", "CLOSED")
