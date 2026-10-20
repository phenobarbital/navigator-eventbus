"""Tests for the outbound WebhookDeliverySubscriber (webhook-support, TASK-E)."""
import asyncio
import json

import pytest
from aiohttp import web
from pydantic import ValidationError

from navigator_eventbus.envelope import EventEnvelope, Severity
from navigator_eventbus.evb import EventBus
from navigator_eventbus.subscribers.webhook import (
    DELIVERY_FAILED_TOPIC,
    WebhookDeliveryConfig,
    WebhookDeliverySubscriber,
)
from navigator_eventbus.webhook_signatures import get_signature_scheme

SECRET = "outbound-secret"


async def wait_until(condition, timeout: float = 3.0, interval: float = 0.01) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return True
        await asyncio.sleep(interval)
    return condition()


class Receiver:
    """A recording HTTP endpoint with a scriptable status sequence."""

    def __init__(self, statuses=None, delay: float = 0.0):
        self.requests: list[tuple[bytes, dict]] = []
        self.statuses = list(statuses or [])
        self.delay = delay

    async def handle(self, request: web.Request) -> web.Response:
        body = await request.read()
        self.requests.append((body, dict(request.headers)))
        if self.delay:
            await asyncio.sleep(self.delay)
        status = self.statuses.pop(0) if self.statuses else 200
        return web.Response(status=status, text="ok")

    def app(self) -> web.Application:
        application = web.Application()
        application.router.add_post("/hook", self.handle)
        return application


@pytest.fixture
async def bus():
    b = EventBus()
    await b.connect()
    yield b
    await b.close()


async def make_subscriber(aiohttp_server, receiver: Receiver, **kwargs):
    server = await aiohttp_server(receiver.app())
    url = str(server.make_url("/hook"))
    kwargs.setdefault("max_attempts", 3)
    kwargs.setdefault("backoff_base", 0.01)
    kwargs.setdefault("backoff_jitter", False)
    return WebhookDeliverySubscriber(url=url, **kwargs)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def test_rejects_bad_url_scheme():
    with pytest.raises(ValidationError, match="Unsupported URL scheme"):
        WebhookDeliveryConfig(url="ftp://example.test/hook")


def test_rejects_url_without_host():
    with pytest.raises(ValidationError, match="no host"):
        WebhookDeliveryConfig(url="https:///nohost")


def test_rejects_unknown_signature_scheme():
    with pytest.raises(ValidationError):
        WebhookDeliveryConfig(url="https://x.test/h", signature_scheme="nope")


def test_config_is_json_serializable_with_live_transform():
    cfg = WebhookDeliveryConfig(
        url="https://x.test/h", transform_fn=lambda envelope: {"x": 1}
    )
    dumped = cfg.model_dump(mode="json")
    assert "transform_fn" not in dumped
    json.dumps(dumped)


def test_secret_absent_from_repr():
    cfg = WebhookDeliveryConfig(url="https://x.test/h", secret="TOP-SECRET")
    assert "TOP-SECRET" not in repr(cfg)


# ---------------------------------------------------------------------------
# Attach / detach
# ---------------------------------------------------------------------------


async def test_attach_subscribes_every_configured_pattern(aiohttp_server, bus):
    receiver = Receiver()
    sub = await make_subscriber(aiohttp_server, receiver, patterns=["a.*", "b.*"])
    try:
        ids = sub.attach(bus)
        assert len(ids) == 2
        assert len(set(ids)) == 2
    finally:
        await sub.aclose()


async def test_detach_stops_receiving(aiohttp_server, bus):
    receiver = Receiver()
    sub = await make_subscriber(aiohttp_server, receiver, patterns=["order.*"])
    try:
        sub.attach(bus)
        assert sub.detach(bus) == 1
        await bus.emit("order.created", {"id": 1})
        await asyncio.sleep(0.15)
        assert not receiver.requests
    finally:
        await sub.aclose()


async def test_aclose_is_idempotent(aiohttp_server, bus):
    receiver = Receiver()
    sub = await make_subscriber(aiohttp_server, receiver, patterns=["order.*"])
    sub.attach(bus)
    await bus.emit("order.created", {"id": 1})
    assert await wait_until(lambda: receiver.requests)
    await sub.aclose()
    await sub.aclose()
    assert len(receiver.requests) == 1


async def test_aclose_drains_its_own_buffer(aiohttp_server, bus):
    """aclose() waits for everything already buffered in the subscriber.

    It cannot drain the *bus* queue — an envelope still awaiting BusCore
    dispatch has not reached this subscriber yet. Correct shutdown order is
    `await bus.close()` first, then `await subscriber.aclose()`.
    """
    receiver = Receiver(delay=0.05)
    sub = await make_subscriber(
        aiohttp_server, receiver, patterns=["order.*"], concurrency=1
    )
    sub.attach(bus)
    for index in range(5):
        await bus.emit("order.created", {"i": index})
    # Let BusCore hand every envelope to the subscriber's buffer first.
    assert await wait_until(lambda: sub.stats["queued"] + sub.stats["delivered"] >= 5)
    await sub.aclose()
    assert len(receiver.requests) == 5
    assert sub.stats["delivered"] == 5


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------


async def test_delivers_envelope_as_json(aiohttp_server, bus):
    receiver = Receiver()
    sub = await make_subscriber(aiohttp_server, receiver, patterns=["order.*"])
    try:
        sub.attach(bus)
        await bus.emit("order.created", {"id": 7})
        assert await wait_until(lambda: receiver.requests)
        body, headers = receiver.requests[0]
        payload = json.loads(body)
        assert payload["topic"] == "order.created"
        assert payload["payload"] == {"id": 7}
        assert headers["Content-Type"] == "application/json"
        assert sub.stats["delivered"] == 1
    finally:
        await sub.aclose()


async def test_signature_verifies_with_the_matching_scheme(aiohttp_server, bus):
    """The same scheme object signs here and verifies on the inbound side."""
    receiver = Receiver()
    sub = await make_subscriber(
        aiohttp_server,
        receiver,
        patterns=["order.*"],
        secret=SECRET,
        signature_scheme="github",
    )
    try:
        sub.attach(bus)
        await bus.emit("order.created", {"id": 1})
        assert await wait_until(lambda: receiver.requests)
        body, headers = receiver.requests[0]
        check = get_signature_scheme("github").verify(
            secret=SECRET, body=body, headers=headers
        )
        assert check.ok
    finally:
        await sub.aclose()


async def test_custom_signature_header_is_used(aiohttp_server, bus):
    receiver = Receiver()
    sub = await make_subscriber(
        aiohttp_server,
        receiver,
        patterns=["order.*"],
        secret=SECRET,
        signature_header="X-My-Sig",
    )
    try:
        sub.attach(bus)
        await bus.emit("order.created", {})
        assert await wait_until(lambda: receiver.requests)
        _, headers = receiver.requests[0]
        assert "X-My-Sig" in headers
    finally:
        await sub.aclose()


async def test_static_headers_are_sent(aiohttp_server, bus):
    receiver = Receiver()
    sub = await make_subscriber(
        aiohttp_server, receiver, patterns=["order.*"], headers={"X-Tenant": "acme"}
    )
    try:
        sub.attach(bus)
        await bus.emit("order.created", {})
        assert await wait_until(lambda: receiver.requests)
        assert receiver.requests[0][1]["X-Tenant"] == "acme"
    finally:
        await sub.aclose()


async def test_transform_import_string_applied(aiohttp_server, bus):
    receiver = Receiver()
    sub = await make_subscriber(
        aiohttp_server,
        receiver,
        patterns=["order.*"],
        transform_fn=lambda envelope: {"only": envelope.topic},
    )
    try:
        sub.attach(bus)
        await bus.emit("order.created", {"id": 1})
        assert await wait_until(lambda: receiver.requests)
        assert json.loads(receiver.requests[0][0]) == {"only": "order.created"}
    finally:
        await sub.aclose()


# ---------------------------------------------------------------------------
# Retry semantics
# ---------------------------------------------------------------------------


async def test_retries_on_500_then_succeeds(aiohttp_server, bus):
    receiver = Receiver(statuses=[500, 200])
    sub = await make_subscriber(aiohttp_server, receiver, patterns=["order.*"])
    try:
        sub.attach(bus)
        await bus.emit("order.created", {})
        assert await wait_until(lambda: sub.stats["delivered"] == 1)
        assert len(receiver.requests) == 2
        assert sub.stats["retries"] == 1
    finally:
        await sub.aclose()


async def test_retries_on_429(aiohttp_server, bus):
    receiver = Receiver(statuses=[429, 200])
    sub = await make_subscriber(aiohttp_server, receiver, patterns=["order.*"])
    try:
        sub.attach(bus)
        await bus.emit("order.created", {})
        assert await wait_until(lambda: sub.stats["delivered"] == 1)
        assert len(receiver.requests) == 2
    finally:
        await sub.aclose()


async def test_gives_up_on_400_without_retry(aiohttp_server, bus):
    receiver = Receiver(statuses=[400, 200])
    sub = await make_subscriber(aiohttp_server, receiver, patterns=["order.*"])
    try:
        sub.attach(bus)
        await bus.emit("order.created", {})
        assert await wait_until(lambda: sub.stats["failed"] == 1)
        assert len(receiver.requests) == 1
    finally:
        await sub.aclose()


async def test_exhausted_retries_count_as_failed(aiohttp_server, bus):
    receiver = Receiver(statuses=[500, 500, 500])
    sub = await make_subscriber(aiohttp_server, receiver, patterns=["order.*"])
    try:
        sub.attach(bus)
        await bus.emit("order.created", {})
        assert await wait_until(lambda: sub.stats["failed"] == 1)
        assert len(receiver.requests) == 3
    finally:
        await sub.aclose()


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


async def test_bus_internal_topics_excluded_by_default(aiohttp_server, bus):
    receiver = Receiver()
    sub = await make_subscriber(aiohttp_server, receiver, patterns=["*"])
    try:
        sub.attach(bus)
        await bus.emit("bus.something", {"internal": True})
        await bus.emit("app.something", {"internal": False})
        assert await wait_until(lambda: receiver.requests)
        await asyncio.sleep(0.1)
        topics = [json.loads(body)["topic"] for body, _ in receiver.requests]
        assert topics == ["app.something"]
    finally:
        await sub.aclose()


async def test_bus_internal_topics_included_when_opted_in(aiohttp_server, bus):
    receiver = Receiver()
    sub = await make_subscriber(
        aiohttp_server, receiver, patterns=["bus.*"], exclude_bus_internal=False
    )
    try:
        sub.attach(bus)
        await bus.emit("bus.something", {})
        assert await wait_until(lambda: receiver.requests)
    finally:
        await sub.aclose()


async def test_min_severity_filter(aiohttp_server, bus):
    receiver = Receiver()
    sub = await make_subscriber(
        aiohttp_server, receiver, patterns=["app.*"], min_severity=Severity.ERROR
    )
    try:
        sub.attach(bus)
        await bus.emit("app.info", {}, severity=Severity.INFO)
        await bus.emit("app.bad", {}, severity=Severity.ERROR)
        assert await wait_until(lambda: receiver.requests)
        await asyncio.sleep(0.1)
        topics = [json.loads(body)["topic"] for body, _ in receiver.requests]
        assert topics == ["app.bad"]
    finally:
        await sub.aclose()


# ---------------------------------------------------------------------------
# Isolation and overload
# ---------------------------------------------------------------------------


async def test_emit_returns_promptly_while_endpoint_hangs(aiohttp_server, bus):
    """The isolation guarantee: a slow receiver must not delay the bus."""
    receiver = Receiver(delay=1.5)
    sub = await make_subscriber(aiohttp_server, receiver, patterns=["order.*"])
    try:
        sub.attach(bus)
        loop = asyncio.get_running_loop()
        started = loop.time()
        for index in range(5):
            await bus.emit("order.created", {"i": index})
        elapsed = loop.time() - started
        assert elapsed < 0.5, f"emit was blocked by the slow endpoint ({elapsed:.2f}s)"
    finally:
        await sub.aclose()


async def test_queue_overflow_drops_oldest_and_counts(aiohttp_server, bus):
    receiver = Receiver(delay=0.5)
    sub = await make_subscriber(
        aiohttp_server, receiver, patterns=["order.*"], queue_size=2, concurrency=1
    )
    try:
        sub.attach(bus)
        for index in range(25):
            await bus.emit("order.created", {"i": index})
        assert await wait_until(lambda: sub.stats["dropped"] > 0)
    finally:
        await sub.aclose()


async def test_exhausted_retries_emit_bus_delivery_failed_meta_event(
    aiohttp_server, bus
):
    receiver = Receiver(statuses=[500, 500, 500])
    seen = []
    bus.subscribe(DELIVERY_FAILED_TOPIC, lambda event: seen.append(event))
    sub = await make_subscriber(aiohttp_server, receiver, patterns=["order.*"])
    try:
        sub.attach(bus)
        await bus.emit("order.created", {"id": 3})
        assert await wait_until(lambda: seen), "no failure meta-event emitted"
        payload = seen[0].payload
        assert payload["topic"] == "order.created"
        assert payload["attempts"] == 3
        assert payload["status"] == 500
    finally:
        await sub.aclose()


async def test_delivery_failure_meta_event_does_not_loop(aiohttp_server, bus):
    """The meta-event lives under bus.*, which the default filter drops."""
    receiver = Receiver(statuses=[500, 500, 500])
    sub = await make_subscriber(aiohttp_server, receiver, patterns=["*"])
    try:
        sub.attach(bus)
        await bus.emit("order.created", {})
        assert await wait_until(lambda: sub.stats["failed"] == 1)
        await asyncio.sleep(0.2)
        # Exactly one delivery attempt sequence: 3 requests, no re-entry.
        assert len(receiver.requests) == 3
        assert sub.stats["failed"] == 1
    finally:
        await sub.aclose()


async def test_failure_meta_can_be_disabled(aiohttp_server, bus):
    receiver = Receiver(statuses=[500, 500, 500])
    seen = []
    bus.subscribe(DELIVERY_FAILED_TOPIC, lambda event: seen.append(event))
    sub = await make_subscriber(
        aiohttp_server, receiver, patterns=["order.*"], emit_failure_meta=False
    )
    try:
        sub.attach(bus)
        await bus.emit("order.created", {})
        assert await wait_until(lambda: sub.stats["failed"] == 1)
        await asyncio.sleep(0.15)
        assert not seen
    finally:
        await sub.aclose()


async def test_stats_shape(aiohttp_server, bus):
    receiver = Receiver()
    sub = await make_subscriber(aiohttp_server, receiver)
    try:
        assert set(sub.stats) == {
            "delivered",
            "failed",
            "dropped",
            "retries",
            "queued",
        }
    finally:
        await sub.aclose()


async def test_unreachable_endpoint_counts_failure(bus):
    """A connection error is retried, then counted — never raised at the bus."""
    sub = WebhookDeliverySubscriber(
        url="http://127.0.0.1:1/hook",
        patterns=["order.*"],
        max_attempts=2,
        backoff_base=0.01,
        backoff_jitter=False,
        timeout_seconds=0.2,
    )
    try:
        sub.attach(bus)
        await bus.emit("order.created", {})
        assert await wait_until(lambda: sub.stats["failed"] == 1)
    finally:
        await sub.aclose()


def test_envelope_to_dict_is_the_default_body_shape():
    envelope = EventEnvelope(topic="a.b", payload={"x": 1})
    assert set(envelope.to_dict()) >= {"topic", "payload", "event_id", "timestamp"}
