"""Tests for WebhookListenerHook and the shared ingest pipeline (TASK-C)."""
import asyncio
import hashlib
import hmac
import json

import pytest
from aiohttp import web

from navigator_eventbus.evb import EventBus
from navigator_eventbus.hooks.base import BaseHook
from navigator_eventbus.hooks.manager import HookManager
from navigator_eventbus.hooks.webhook.listener import WebhookListenerHook
from navigator_eventbus.hooks.webhook.models import WebhookHookConfig

SECRET = "s3cr3t"


def sign(body: bytes, secret: str = SECRET) -> str:
    """Signature value for the default ``generic`` scheme."""
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def signed(payload: dict, secret: str = SECRET) -> tuple[bytes, dict[str, str]]:
    body = json.dumps(payload).encode()
    return body, {
        "Content-Type": "application/json",
        "X-Webhook-Signature": sign(body, secret),
    }


async def wait_until(condition, timeout: float = 2.0, interval: float = 0.01) -> bool:
    """Poll *condition* until it is truthy or *timeout* elapses."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return True
        await asyncio.sleep(interval)
    return condition()


@pytest.fixture
async def bus():
    b = EventBus()
    yield b
    await b.close()


@pytest.fixture
def received():
    return []


@pytest.fixture
def manager(received):
    m = HookManager()

    async def capture(event):
        received.append(event)

    m.set_event_callback(capture)
    return m


@pytest.fixture
def listener(manager):
    hook = WebhookListenerHook()
    hook.register_endpoint("/demo", secret=SECRET)
    manager.register(hook)
    return hook


@pytest.fixture
def app(listener):
    application = web.Application()
    listener.setup_routes(application)
    return application


# ---------------------------------------------------------------------------
# Contract
# ---------------------------------------------------------------------------


def test_listener_is_a_base_hook(listener):
    assert isinstance(listener, BaseHook)
    assert listener.hook_type == "webhook"


def test_listener_exposes_public_base_path(listener):
    """Operators need this to exclude the route from auth middleware."""
    assert listener.base_path == "/api/v1/hooks/webhook"


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_signed_post_emits_hook_event(aiohttp_client, app, listener, received):
    client = await aiohttp_client(app)
    body, headers = signed({"hello": "world"})
    resp = await client.post("/api/v1/hooks/webhook/demo", data=body, headers=headers)
    assert resp.status == 202
    assert (await resp.json())["status"] == "accepted"
    assert await wait_until(lambda: received)
    event = received[0]
    assert event.hook_type == "webhook"
    assert event.event_type == "received"
    assert event.payload == {"hello": "world"}
    assert event.metadata["webhook_path"] == "/demo"


async def test_event_reaches_bus_as_hooks_webhook_topic(
    aiohttp_client, app, listener, manager, bus
):
    seen = []
    bus.subscribe("hooks.webhook.*", lambda event: seen.append(event))
    manager.route_to_bus = True
    manager.set_event_bus(bus)

    client = await aiohttp_client(app)
    body, headers = signed({"n": 1})
    resp = await client.post("/api/v1/hooks/webhook/demo", data=body, headers=headers)
    assert resp.status == 202
    assert await wait_until(lambda: seen), "event never reached the bus"
    assert seen[0].event_type == "hooks.webhook.received"
    assert seen[0].payload == {"n": 1}


async def test_post_to_bare_base_path_resolves(aiohttp_client, manager):
    """`{webhook_id:.*}` needs the separating slash; the bare path needs its own route."""
    hook = WebhookListenerHook()
    hook.register_endpoint("/", secret=SECRET)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    body, headers = signed({"root": True})
    resp = await client.post("/api/v1/hooks/webhook", data=body, headers=headers)
    assert resp.status == 202


async def test_endpoint_lookup_is_prefix_independent(aiohttp_client, manager, received):
    """Keying on match-info (not request.path) survives a mounted prefix."""
    hook = WebhookListenerHook(base_path="/hooks")
    hook.register_endpoint("/demo", secret=SECRET)
    manager.register(hook)

    inner = web.Application()
    hook.setup_routes(inner)
    outer = web.Application()
    outer.add_subapp("/mounted", inner)
    client = await aiohttp_client(outer)

    body, headers = signed({"prefixed": True})
    resp = await client.post("/mounted/hooks/demo", data=body, headers=headers)
    assert resp.status == 202
    assert await wait_until(lambda: received)


# ---------------------------------------------------------------------------
# Routing failures
# ---------------------------------------------------------------------------


async def test_unknown_endpoint_returns_404(aiohttp_client, app):
    client = await aiohttp_client(app)
    body, headers = signed({})
    resp = await client.post("/api/v1/hooks/webhook/nope", data=body, headers=headers)
    assert resp.status == 404
    assert (await resp.json())["status"] == "unknown_endpoint"


async def test_disabled_endpoint_returns_503_with_retry_after(
    aiohttp_client, manager
):
    hook = WebhookListenerHook()
    hook.register_endpoint("/off", secret=SECRET, enabled=False)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    body, headers = signed({})
    resp = await client.post("/api/v1/hooks/webhook/off", data=body, headers=headers)
    assert resp.status == 503
    assert resp.headers["Retry-After"] == "60"


# ---------------------------------------------------------------------------
# Signature enforcement
# ---------------------------------------------------------------------------


async def test_missing_signature_returns_401(aiohttp_client, app):
    client = await aiohttp_client(app)
    resp = await client.post("/api/v1/hooks/webhook/demo", json={"a": 1})
    assert resp.status == 401
    assert (await resp.json())["reason"] == "missing_signature"


async def test_bad_signature_returns_401(aiohttp_client, app):
    client = await aiohttp_client(app)
    body, headers = signed({"a": 1}, secret="wrong-secret")
    resp = await client.post("/api/v1/hooks/webhook/demo", data=body, headers=headers)
    assert resp.status == 401
    assert (await resp.json())["status"] == "unauthorized"


async def test_malformed_signature_returns_400(aiohttp_client, app):
    client = await aiohttp_client(app)
    body = json.dumps({"a": 1}).encode()
    resp = await client.post(
        "/api/v1/hooks/webhook/demo",
        data=body,
        headers={"Content-Type": "application/json", "X-Webhook-Signature": "nothex!!"},
    )
    assert resp.status == 400
    assert (await resp.json())["status"] == "bad_signature_format"


async def test_tampered_body_returns_401(aiohttp_client, app, received):
    client = await aiohttp_client(app)
    body, headers = signed({"a": 1})
    resp = await client.post(
        "/api/v1/hooks/webhook/demo", data=body + b" ", headers=headers
    )
    assert resp.status == 401
    assert not received


async def test_unsigned_endpoint_accepts_when_require_signature_false(
    aiohttp_client, manager, received
):
    hook = WebhookListenerHook()
    hook.register_endpoint("/open", require_signature=False)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post("/api/v1/hooks/webhook/open", json={"free": True})
    assert resp.status == 202
    assert await wait_until(lambda: received)


async def test_github_scheme_endpoint(aiohttp_client, manager, received):
    hook = WebhookListenerHook()
    hook.register_endpoint(
        "/gh",
        secret=SECRET,
        signature_scheme="github",
        event_type_header="X-GitHub-Event",
        event_type_prefix="github",
    )
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    body = json.dumps({"action": "opened"}).encode()
    resp = await client.post(
        "/api/v1/hooks/webhook/gh",
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": f"sha256={sign(body)}",
            "X-GitHub-Event": "pull_request",
        },
    )
    assert resp.status == 202
    assert await wait_until(lambda: received)
    assert received[0].event_type == "github.pull_request"


# ---------------------------------------------------------------------------
# IP allowlist
# ---------------------------------------------------------------------------


async def test_ip_not_allowed_returns_403(aiohttp_client, manager):
    hook = WebhookListenerHook()
    hook.register_endpoint("/fenced", secret=SECRET, allowed_ips=["203.0.113.0/24"])
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    body, headers = signed({})
    resp = await client.post("/api/v1/hooks/webhook/fenced", data=body, headers=headers)
    assert resp.status == 403


async def test_cidr_allowlist_permits_loopback(aiohttp_client, manager, received):
    hook = WebhookListenerHook()
    hook.register_endpoint("/local", secret=SECRET, allowed_ips=["127.0.0.0/8", "::1"])
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    body, headers = signed({})
    resp = await client.post("/api/v1/hooks/webhook/local", data=body, headers=headers)
    assert resp.status == 202


async def test_trust_forwarded_for_uses_header_when_enabled(
    aiohttp_client, manager, received
):
    hook = WebhookListenerHook()
    hook.register_endpoint(
        "/fwd",
        secret=SECRET,
        allowed_ips=["203.0.113.0/24"],
        trust_forwarded_for=True,
    )
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    body, headers = signed({})
    headers["X-Forwarded-For"] = "203.0.113.7, 10.0.0.1"
    resp = await client.post("/api/v1/hooks/webhook/fwd", data=body, headers=headers)
    assert resp.status == 202


async def test_forwarded_for_ignored_when_not_trusted(aiohttp_client, manager):
    hook = WebhookListenerHook()
    hook.register_endpoint("/nofwd", secret=SECRET, allowed_ips=["203.0.113.0/24"])
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    body, headers = signed({})
    headers["X-Forwarded-For"] = "203.0.113.7"
    resp = await client.post("/api/v1/hooks/webhook/nofwd", data=body, headers=headers)
    assert resp.status == 403


# ---------------------------------------------------------------------------
# Body size cap
# ---------------------------------------------------------------------------


async def test_body_over_cap_returns_413_via_content_length(aiohttp_client, manager):
    hook = WebhookListenerHook(WebhookHookConfig(max_body_bytes=64))
    hook.register_endpoint("/small", require_signature=False)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post("/api/v1/hooks/webhook/small", data=b"x" * 500)
    assert resp.status == 413
    assert (await resp.json())["limit"] == 64


async def test_body_over_cap_returns_413_when_chunked(aiohttp_client, manager):
    """Chunked transfer sends no Content-Length — the streaming cap must catch it."""
    hook = WebhookListenerHook(WebhookHookConfig(max_body_bytes=64))
    hook.register_endpoint("/small", require_signature=False)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    async def gen():
        for _ in range(10):
            yield b"x" * 100

    resp = await client.post("/api/v1/hooks/webhook/small", data=gen())
    assert resp.status == 413


async def test_per_endpoint_cap_overrides_listener_default(aiohttp_client, manager):
    hook = WebhookListenerHook(WebhookHookConfig(max_body_bytes=10_000))
    hook.register_endpoint("/tiny", require_signature=False, max_body_bytes=32)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post("/api/v1/hooks/webhook/tiny", data=b"y" * 200)
    assert resp.status == 413


# ---------------------------------------------------------------------------
# Body parsing
# ---------------------------------------------------------------------------


async def test_non_json_body_wrapped_as_raw(aiohttp_client, manager, received):
    hook = WebhookListenerHook()
    hook.register_endpoint("/text", require_signature=False)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post(
        "/api/v1/hooks/webhook/text",
        data=b"plain text",
        headers={"Content-Type": "text/plain"},
    )
    assert resp.status == 202
    assert await wait_until(lambda: received)
    assert received[0].payload["raw"] == "plain text"


async def test_non_json_rejected_when_disallowed(aiohttp_client, manager):
    hook = WebhookListenerHook()
    hook.register_endpoint("/strict", require_signature=False, allow_non_json=False)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post(
        "/api/v1/hooks/webhook/strict",
        data=b"not json",
        headers={"Content-Type": "text/plain"},
    )
    assert resp.status == 400
    assert (await resp.json())["status"] == "invalid_payload"


async def test_form_encoded_body_parsed_to_dict(aiohttp_client, manager, received):
    hook = WebhookListenerHook()
    hook.register_endpoint("/form", require_signature=False)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post("/api/v1/hooks/webhook/form", data={"a": "1", "b": "2"})
    assert resp.status == 202
    assert await wait_until(lambda: received)
    assert received[0].payload == {"a": "1", "b": "2"}


async def test_json_array_body_wrapped_under_data(aiohttp_client, manager, received):
    hook = WebhookListenerHook()
    hook.register_endpoint("/arr", require_signature=False)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post("/api/v1/hooks/webhook/arr", json=[1, 2, 3])
    assert resp.status == 202
    assert await wait_until(lambda: received)
    assert received[0].payload == {"data": [1, 2, 3]}


# ---------------------------------------------------------------------------
# Preprocessing
# ---------------------------------------------------------------------------


async def test_preprocessor_transforms_payload(aiohttp_client, manager, received):
    hook = WebhookListenerHook()
    hook.register_endpoint(
        "/pre",
        require_signature=False,
        preprocessor="_webhook_preprocessors:to_upper",
    )
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post("/api/v1/hooks/webhook/pre", json={"a": "x"})
    assert resp.status == 202
    assert await wait_until(lambda: received)
    assert received[0].payload == {"a": "X"}


async def test_preprocessor_error_still_emits_raw_payload_and_returns_202(
    aiohttp_client, manager, received
):
    hook = WebhookListenerHook()
    hook.register_endpoint(
        "/boom",
        require_signature=False,
        preprocessor="_webhook_preprocessors:always_raises",
    )
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post("/api/v1/hooks/webhook/boom", json={"keep": "me"})
    assert resp.status == 202
    payload = await resp.json()
    assert payload["preprocessor"] == "failed"
    assert await wait_until(lambda: received)
    assert received[0].payload == {"keep": "me"}
    assert "preprocessor_error" in received[0].metadata
    assert hook.stats["totals"]["preprocessor_errors"] == 1


async def test_preprocessor_can_ignore_delivery(aiohttp_client, manager, received):
    from navigator_eventbus.hooks.webhook.preprocess import PreprocessResult

    hook = WebhookListenerHook()
    hook.register_endpoint(
        "/skip",
        require_signature=False,
        preprocessor_fn=lambda p: PreprocessResult(payload={}, ignore=True),
    )
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post("/api/v1/hooks/webhook/skip", json={"a": 1})
    assert resp.status == 200
    assert (await resp.json())["status"] == "ignored"
    await asyncio.sleep(0.05)
    assert not received


async def test_preprocessor_event_type_bypasses_prefix(
    aiohttp_client, manager, received
):
    hook = WebhookListenerHook()
    hook.register_endpoint(
        "/tup",
        require_signature=False,
        event_type_prefix="ignored",
        preprocessor="_webhook_preprocessors:returns_tuple",
    )
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post("/api/v1/hooks/webhook/tup", json={"a": 1})
    assert resp.status == 202
    assert await wait_until(lambda: received)
    assert received[0].event_type == "custom.event"


# ---------------------------------------------------------------------------
# Event typing
# ---------------------------------------------------------------------------


async def test_event_type_lifted_from_header(aiohttp_client, manager, received):
    hook = WebhookListenerHook()
    hook.register_endpoint(
        "/hdr", require_signature=False, event_type_header="X-Event-Name"
    )
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post(
        "/api/v1/hooks/webhook/hdr", json={}, headers={"X-Event-Name": "thing.made"}
    )
    assert resp.status == 202
    assert await wait_until(lambda: received)
    assert received[0].event_type == "thing.made"


async def test_ignored_classification_returns_200_and_emits_nothing(
    aiohttp_client, manager, received
):
    class Quiet(WebhookListenerHook):
        def _classify_event(self, request, endpoint, payload):
            return None

    hook = Quiet()
    hook.register_endpoint("/quiet", require_signature=False)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post("/api/v1/hooks/webhook/quiet", json={"a": 1})
    assert resp.status == 200
    assert (await resp.json())["status"] == "ignored"
    await asyncio.sleep(0.05)
    assert not received
    assert hook.stats["totals"]["ignored"] == 1


async def test_routing_hints_reach_the_event(aiohttp_client, manager, received):
    hook = WebhookListenerHook()
    hook.register_endpoint(
        "/routed", require_signature=False, target_type="agent", target_id="Reviewer"
    )
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    await client.post("/api/v1/hooks/webhook/routed", json={})
    assert await wait_until(lambda: received)
    assert received[0].target_type == "agent"
    assert received[0].target_id == "Reviewer"


# ---------------------------------------------------------------------------
# De-duplication
# ---------------------------------------------------------------------------


async def test_duplicate_delivery_id_returns_200_and_emits_once(
    aiohttp_client, manager, received
):
    hook = WebhookListenerHook(WebhookHookConfig(dedup_ttl_seconds=60))
    hook.register_endpoint(
        "/dedup", require_signature=False, dedup_header="X-Delivery-Id"
    )
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    headers = {"X-Delivery-Id": "abc-123"}
    first = await client.post("/api/v1/hooks/webhook/dedup", json={}, headers=headers)
    second = await client.post("/api/v1/hooks/webhook/dedup", json={}, headers=headers)
    assert first.status == 202
    assert second.status == 200
    assert (await second.json())["status"] == "duplicate"
    assert await wait_until(lambda: len(received) == 1)
    await asyncio.sleep(0.05)
    assert len(received) == 1
    assert hook.stats["totals"]["duplicates"] == 1


async def test_dedup_disabled_by_default(aiohttp_client, manager, received):
    hook = WebhookListenerHook()
    hook.register_endpoint("/nodedup", require_signature=False, dedup_header="X-Id")
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    headers = {"X-Id": "same"}
    await client.post("/api/v1/hooks/webhook/nodedup", json={}, headers=headers)
    await client.post("/api/v1/hooks/webhook/nodedup", json={}, headers=headers)
    assert await wait_until(lambda: len(received) == 2)


async def test_delivery_id_recorded_in_metadata(aiohttp_client, manager, received):
    hook = WebhookListenerHook()
    hook.register_endpoint("/did", require_signature=False, dedup_header="X-Delivery")
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    await client.post(
        "/api/v1/hooks/webhook/did", json={}, headers={"X-Delivery": "d-1"}
    )
    assert await wait_until(lambda: received)
    assert received[0].metadata["delivery_id"] == "d-1"


# ---------------------------------------------------------------------------
# Runtime registration
# ---------------------------------------------------------------------------


async def test_register_endpoint_at_runtime_is_live_without_route_rebuild(
    aiohttp_client, app, listener, received
):
    client = await aiohttp_client(app)
    body, headers = signed({})
    assert (
        await client.post("/api/v1/hooks/webhook/late", data=body, headers=headers)
    ).status == 404

    listener.register_endpoint("/late", secret=SECRET)

    resp = await client.post("/api/v1/hooks/webhook/late", data=body, headers=headers)
    assert resp.status == 202
    assert await wait_until(lambda: received)


async def test_unregister_endpoint_then_404(aiohttp_client, app, listener):
    client = await aiohttp_client(app)
    body, headers = signed({})
    assert (
        await client.post("/api/v1/hooks/webhook/demo", data=body, headers=headers)
    ).status == 202

    removed = listener.unregister_endpoint("/demo")
    assert removed is not None

    assert (
        await client.post("/api/v1/hooks/webhook/demo", data=body, headers=headers)
    ).status == 404


def test_get_and_list_endpoints(listener):
    assert listener.get_endpoint("/demo") is not None
    assert listener.get_endpoint("demo/") is not None  # normalized lookup
    assert listener.get_endpoint("/missing") is None
    assert set(listener.endpoints) == {"/demo"}


def test_register_endpoint_applies_listener_defaults():
    hook = WebhookListenerHook(
        WebhookHookConfig(
            default_signature_scheme="github", default_require_signature=False
        )
    )
    endpoint = hook.register_endpoint("/x")
    assert endpoint.signature_scheme == "github"
    assert endpoint.require_signature is False


def test_register_endpoint_rejects_signed_endpoint_without_secret():
    from pydantic import ValidationError

    hook = WebhookListenerHook()
    with pytest.raises(ValidationError):
        hook.register_endpoint("/oops")


def test_endpoints_from_config_are_installed():
    from navigator_eventbus.hooks.webhook.models import WebhookEndpointConfig

    cfg = WebhookHookConfig(
        endpoints=[WebhookEndpointConfig(path="/a", require_signature=False)]
    )
    hook = WebhookListenerHook(cfg)
    assert hook.get_endpoint("/a") is not None


# ---------------------------------------------------------------------------
# _list route
# ---------------------------------------------------------------------------


async def test_list_route_absent_by_default(aiohttp_client, app):
    client = await aiohttp_client(app)
    resp = await client.get("/api/v1/hooks/webhook/_list")
    assert resp.status == 405  # POST-only catch-all matched, GET not allowed


async def test_list_route_requires_token_when_enabled(aiohttp_client, manager):
    hook = WebhookListenerHook(
        WebhookHookConfig(expose_list_route=True, list_route_token="tok")
    )
    hook.register_endpoint("/a", secret=SECRET)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    assert (await client.get("/api/v1/hooks/webhook/_list")).status == 401
    resp = await client.get(
        "/api/v1/hooks/webhook/_list", headers={"X-API-Key": "tok"}
    )
    assert resp.status == 200
    body = await resp.json()
    assert body["endpoints"][0]["path"] == "/a"
    assert body["endpoints"][0]["signed"] is True
    assert "secret" not in json.dumps(body).lower() or SECRET not in json.dumps(body)


async def test_list_route_accepts_bearer_token(aiohttp_client, manager):
    hook = WebhookListenerHook(
        WebhookHookConfig(expose_list_route=True, list_route_token="tok")
    )
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.get(
        "/api/v1/hooks/webhook/_list", headers={"Authorization": "Bearer tok"}
    )
    assert resp.status == 200


# ---------------------------------------------------------------------------
# Stats and failure isolation
# ---------------------------------------------------------------------------


async def test_stats_counters_increment_per_outcome(aiohttp_client, app, listener):
    client = await aiohttp_client(app)
    body, headers = signed({})
    await client.post("/api/v1/hooks/webhook/demo", data=body, headers=headers)
    await client.post("/api/v1/hooks/webhook/demo", json={"unsigned": True})

    stats = listener.stats
    assert stats["endpoints"] == 1
    assert stats["totals"]["accepted"] == 1
    assert stats["totals"]["rejected"] == 1
    assert stats["totals"]["call_count"] == 2
    assert stats["per_endpoint"]["/demo"]["last_called"] is not None


async def test_no_callback_registered_logs_error_and_counts_drop(
    aiohttp_client, caplog
):
    """A listener nobody registered must not silently swallow deliveries."""
    hook = WebhookListenerHook()  # deliberately NOT registered with a manager
    hook.register_endpoint("/orphan", require_signature=False)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    caplog.set_level("ERROR")
    resp = await client.post("/api/v1/hooks/webhook/orphan", json={})
    assert resp.status == 202
    assert hook.stats["totals"]["dropped_no_callback"] == 1
    assert any("NO callback registered" in rec.message for rec in caplog.records)


async def test_start_warns_when_no_callback(listener, caplog):
    orphan = WebhookListenerHook()
    caplog.set_level("WARNING")
    await orphan.start()
    assert any("NO callback registered" in rec.message for rec in caplog.records)


async def test_unexpected_handler_error_returns_500(aiohttp_client, manager, caplog):
    class Broken(WebhookListenerHook):
        def _classify_event(self, request, endpoint, payload):
            raise RuntimeError("kaboom")

    hook = Broken()
    hook.register_endpoint("/broken", require_signature=False)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    caplog.set_level("ERROR")
    resp = await client.post("/api/v1/hooks/webhook/broken", json={})
    assert resp.status == 500
    assert (await resp.json())["status"] == "error"


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


async def test_background_dispatch_tracks_task_strongly(aiohttp_client, manager):
    """A bare create_task can be GC'd mid-flight; the task set prevents that."""
    gate = asyncio.Event()
    seen = []

    async def slow(event):
        seen.append(event)
        await gate.wait()

    manager.set_event_callback(slow)
    hook = WebhookListenerHook(WebhookHookConfig(dispatch_mode="background"))
    hook.register_endpoint("/bg", require_signature=False)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post("/api/v1/hooks/webhook/bg", json={})
    assert resp.status == 202
    assert await wait_until(lambda: seen)
    assert hook._background_tasks, "background task must be strongly referenced"
    gate.set()
    assert await wait_until(lambda: not hook._background_tasks)


async def test_stop_cancels_inflight_background_tasks(aiohttp_client, manager):
    gate = asyncio.Event()

    async def never(event):
        await gate.wait()

    manager.set_event_callback(never)
    hook = WebhookListenerHook(WebhookHookConfig(dispatch_mode="background"))
    hook.register_endpoint("/bg", require_signature=False)
    manager.register(hook)
    application = web.Application()
    hook.setup_routes(application)
    client = await aiohttp_client(application)

    await client.post("/api/v1/hooks/webhook/bg", json={})
    assert await wait_until(lambda: hook._background_tasks)
    await hook.stop()
    assert not hook._background_tasks


async def test_max_inflight_zero_disables_semaphore():
    hook = WebhookListenerHook(WebhookHookConfig(max_inflight=0))
    assert hook._semaphore is None
