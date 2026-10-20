"""End-to-end webhook test: inbound HTTP -> bus -> outbound HTTP (TASK-F).

Proves the two halves of the fabric compose, and that they agree on the wire
format: the signature this package *emits* on the outbound side verifies with
the same scheme object the inbound side uses to *check* deliveries.
"""
import asyncio
import hashlib
import hmac
import json

import pytest
from aiohttp import web

from navigator_eventbus.envelope import Severity
from navigator_eventbus.evb import EventBus
from navigator_eventbus.hooks.manager import HookManager
from navigator_eventbus.hooks.webhook.listener import WebhookListenerHook
from navigator_eventbus.subscribers.webhook import WebhookDeliverySubscriber
from navigator_eventbus.webhook_signatures import get_signature_scheme

INBOUND_SECRET = "inbound-secret"
OUTBOUND_SECRET = "outbound-secret"


async def wait_until(condition, timeout: float = 3.0, interval: float = 0.01) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return True
        await asyncio.sleep(interval)
    return condition()


class Receiver:
    """Records outbound deliveries."""

    def __init__(self):
        self.requests: list[tuple[bytes, dict]] = []

    async def handle(self, request: web.Request) -> web.Response:
        self.requests.append((await request.read(), dict(request.headers)))
        return web.Response(status=200)

    def app(self) -> web.Application:
        application = web.Application()
        application.router.add_post("/downstream", self.handle)
        return application


@pytest.fixture
async def bus():
    b = EventBus()
    await b.connect()
    yield b
    await b.close()


async def test_inbound_post_travels_to_outbound_endpoint(
    aiohttp_client, aiohttp_server, bus
):
    """Signed POST -> listener -> hooks.webhook.* -> delivery subscriber -> receiver."""
    # --- outbound half -------------------------------------------------
    receiver = Receiver()
    downstream = await aiohttp_server(receiver.app())
    subscriber = WebhookDeliverySubscriber(
        url=str(downstream.make_url("/downstream")),
        patterns=["hooks.webhook.*"],
        secret=OUTBOUND_SECRET,
        signature_scheme="github",
        backoff_base=0.01,
        backoff_jitter=False,
    )
    subscriber.attach(bus)

    # --- inbound half ---------------------------------------------------
    manager = HookManager(route_to_bus=True)
    manager.set_event_bus(bus)
    listener = WebhookListenerHook()
    listener.register_endpoint(
        "/orders",
        secret=INBOUND_SECRET,
        event_type_prefix="orders",
        event_type_header="X-Order-Event",
        preprocessor="_webhook_preprocessors:to_upper",
    )
    manager.register(listener)
    await manager.start_all()

    application = web.Application()
    listener.setup_routes(application)
    client = await aiohttp_client(application)

    try:
        body = json.dumps({"status": "paid"}).encode()
        signature = hmac.new(
            INBOUND_SECRET.encode(), body, hashlib.sha256
        ).hexdigest()
        resp = await client.post(
            "/api/v1/hooks/webhook/orders",
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Signature": signature,
                "X-Order-Event": "created",
            },
        )
        assert resp.status == 202

        assert await wait_until(
            lambda: receiver.requests
        ), "the inbound delivery never reached the downstream endpoint"

        out_body, out_headers = receiver.requests[0]
        envelope = json.loads(out_body)

        # Topic governance: hooks.<hook_type>.<event_type>, prefix applied.
        assert envelope["topic"] == "hooks.webhook.orders.created"
        # The preprocessor ran on the way through.
        assert envelope["payload"] == {"status": "PAID"}
        assert envelope["metadata"]["webhook_path"] == "/orders"

        # The outbound signature verifies with the same scheme object the
        # inbound side would use — the two halves cannot drift apart.
        check = get_signature_scheme("github").verify(
            secret=OUTBOUND_SECRET, body=out_body, headers=out_headers
        )
        assert check.ok, f"outbound signature did not verify: {check}"

        assert subscriber.stats["delivered"] == 1
        assert listener.stats["totals"]["accepted"] == 1
    finally:
        await manager.stop_all()
        await subscriber.aclose()


async def test_unauthenticated_inbound_never_reaches_outbound(
    aiohttp_client, aiohttp_server, bus
):
    """A rejected delivery must produce no downstream traffic at all."""
    receiver = Receiver()
    downstream = await aiohttp_server(receiver.app())
    subscriber = WebhookDeliverySubscriber(
        url=str(downstream.make_url("/downstream")),
        patterns=["hooks.webhook.*"],
        backoff_base=0.01,
    )
    subscriber.attach(bus)

    manager = HookManager(route_to_bus=True)
    manager.set_event_bus(bus)
    listener = WebhookListenerHook()
    listener.register_endpoint("/orders", secret=INBOUND_SECRET)
    manager.register(listener)

    application = web.Application()
    listener.setup_routes(application)
    client = await aiohttp_client(application)

    try:
        resp = await client.post(
            "/api/v1/hooks/webhook/orders", json={"forged": True}
        )
        assert resp.status == 401
        await asyncio.sleep(0.2)
        assert not receiver.requests
        assert subscriber.stats["delivered"] == 0
    finally:
        await manager.stop_all()
        await subscriber.aclose()


async def test_severity_from_preprocessor_metadata_reaches_the_bus(
    aiohttp_client, bus
):
    """HookManager maps metadata['severity'] onto the envelope severity.

    Subscribes on ``bus.core`` rather than the facade: ``EventBus.subscribe``
    hands handlers the facade ``Event`` dataclass, which carries no severity —
    severity is an ``EventEnvelope`` concern.
    """
    from navigator_eventbus.hooks.webhook.preprocess import PreprocessResult

    seen = []
    bus.core.subscribe("hooks.webhook.*", lambda envelope: seen.append(envelope))

    manager = HookManager(route_to_bus=True)
    manager.set_event_bus(bus)
    listener = WebhookListenerHook()
    listener.register_endpoint(
        "/alerts",
        require_signature=False,
        preprocessor_fn=lambda payload: PreprocessResult(
            payload=payload, metadata={"severity": "error"}
        ),
    )
    manager.register(listener)

    application = web.Application()
    listener.setup_routes(application)
    client = await aiohttp_client(application)

    try:
        resp = await client.post("/api/v1/hooks/webhook/alerts", json={"a": 1})
        assert resp.status == 202
        assert await wait_until(lambda: seen)
        assert seen[0].severity == Severity.ERROR
    finally:
        await manager.stop_all()
