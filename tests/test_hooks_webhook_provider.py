"""Tests for ProviderWebhookHook, the static per-provider base (TASK-D)."""
import asyncio
import hashlib
import hmac
import json

import pytest
from aiohttp import web

from navigator_eventbus.hooks.base import BaseHook
from navigator_eventbus.hooks.manager import HookManager
from navigator_eventbus.hooks.webhook.listener import WebhookListenerHook
from navigator_eventbus.hooks.webhook.models import WebhookEndpointConfig
from navigator_eventbus.hooks.webhook.provider import ProviderWebhookHook

SECRET = "s3cr3t"


def gh_sign(body: bytes, secret: str = SECRET) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


async def wait_until(condition, timeout: float = 2.0, interval: float = 0.01) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return True
        await asyncio.sleep(interval)
    return condition()


class _FakeProviderHook(ProviderWebhookHook):
    """A stand-in provider integration exercising every override point.

    Uses the pre-registered ``github_webhook`` hook type so the test does not
    have to mutate the global HOOK_TYPES registry.
    """

    hook_type = "github_webhook"
    default_signature_scheme = "github"
    default_event_type_header = "X-GitHub-Event"

    _INTERESTING = {"opened", "reopened"}

    def _classify_event(self, request, endpoint, payload):
        action = (payload.get("action") or "").lower()
        if action not in self._INTERESTING:
            return None
        return f"pr_{action}"

    def _normalize_payload(self, request, endpoint, payload, event_type):
        pull_request = payload.get("pull_request") or {}
        return {
            "action": payload.get("action"),
            "pr_number": pull_request.get("number"),
            "title": pull_request.get("title"),
        }

    def _build_task(self, endpoint, event_type, payload):
        return f"GitHub {event_type}: #{payload.get('pr_number')}"


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
def hook(manager):
    h = _FakeProviderHook(
        _FakeProviderHook.build_config("/api/v1/hooks/github", secret=SECRET)
    )
    manager.register(h)
    return h


@pytest.fixture
def app(hook):
    application = web.Application()
    hook.setup_routes(application)
    return application


def post_body(action="opened", number=7):
    return json.dumps(
        {"action": action, "pull_request": {"number": number, "title": "Fix"}}
    ).encode()


# ---------------------------------------------------------------------------
# Contract
# ---------------------------------------------------------------------------


def test_provider_hook_is_a_base_hook(hook):
    assert isinstance(hook, BaseHook)


def test_provider_hook_registers_static_route_and_exposes_url(hook, app):
    assert hook.url == "/api/v1/hooks/github"
    paths = {getattr(r.resource, "canonical", None) for r in app.router.routes()}
    assert "/api/v1/hooks/github" in paths


def test_build_config_applies_provider_defaults():
    cfg = _FakeProviderHook.build_config("/x", secret=SECRET)
    assert cfg.signature_scheme == "github"
    assert cfg.event_type_header == "X-GitHub-Event"


def test_build_config_respects_explicit_override():
    cfg = _FakeProviderHook.build_config("/x", secret=SECRET, signature_scheme="jira")
    assert cfg.signature_scheme == "jira"


def test_config_property_exposes_endpoint(hook):
    assert hook.config.path == "/api/v1/hooks/github"


def test_provider_hook_rejects_unregistered_hook_type():
    """Fail at construction, not on the first live delivery."""

    class Unregistered(ProviderWebhookHook):
        hook_type = "definitely_not_registered_type"

    with pytest.raises(ValueError, match="not registered"):
        Unregistered(WebhookEndpointConfig(path="/x", require_signature=False))


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------


async def test_provider_hook_verifies_github_signature(aiohttp_client, app, received):
    client = await aiohttp_client(app)
    body = post_body()
    resp = await client.post(
        "/api/v1/hooks/github",
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": gh_sign(body),
            "X-GitHub-Event": "pull_request",
        },
    )
    assert resp.status == 202
    assert await wait_until(lambda: received)


async def test_provider_hook_rejects_bad_signature(aiohttp_client, app, received):
    client = await aiohttp_client(app)
    body = post_body()
    resp = await client.post(
        "/api/v1/hooks/github",
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": gh_sign(body, "wrong"),
        },
    )
    assert resp.status == 401
    await asyncio.sleep(0.05)
    assert not received


async def test_provider_hook_emits_normalized_payload_and_task(
    aiohttp_client, app, received
):
    client = await aiohttp_client(app)
    body = post_body(number=42)
    await client.post(
        "/api/v1/hooks/github",
        data=body,
        headers={"Content-Type": "application/json", "X-Hub-Signature-256": gh_sign(body)},
    )
    assert await wait_until(lambda: received)
    event = received[0]
    assert event.hook_type == "github_webhook"
    assert event.event_type == "pr_opened"
    assert event.payload == {"action": "opened", "pr_number": 42, "title": "Fix"}
    assert event.task == "GitHub pr_opened: #42"


async def test_provider_hook_ignores_uninteresting_action_with_200(
    aiohttp_client, app, received
):
    """Non-2xx would make the provider retry and eventually disable the hook."""
    client = await aiohttp_client(app)
    body = post_body(action="labeled")
    resp = await client.post(
        "/api/v1/hooks/github",
        data=body,
        headers={"Content-Type": "application/json", "X-Hub-Signature-256": gh_sign(body)},
    )
    assert resp.status == 200
    assert (await resp.json())["status"] == "ignored"
    await asyncio.sleep(0.05)
    assert not received
    assert hook_stats_ignored(app) == 1


def hook_stats_ignored(app) -> int:
    for route in app.router.routes():
        handler = route.handler
        bound = getattr(handler, "__self__", None)
        if isinstance(bound, ProviderWebhookHook):
            return bound.stats["ignored"]
    raise AssertionError("provider hook not found on the app")


async def test_provider_hook_stats(aiohttp_client, app, hook):
    client = await aiohttp_client(app)
    body = post_body()
    await client.post(
        "/api/v1/hooks/github",
        data=body,
        headers={"Content-Type": "application/json", "X-Hub-Signature-256": gh_sign(body)},
    )
    await client.post("/api/v1/hooks/github", data=body)  # unsigned -> 401
    stats = hook.stats
    assert stats["url"] == "/api/v1/hooks/github"
    assert stats["accepted"] == 1
    assert stats["rejected"] == 1


async def test_two_provider_hooks_on_one_app_have_independent_routes(
    aiohttp_client, manager, received
):
    first = _FakeProviderHook(_FakeProviderHook.build_config("/hooks/a", secret=SECRET))
    second = _FakeProviderHook(_FakeProviderHook.build_config("/hooks/b", secret="other"))
    manager.register(first)
    manager.register(second)
    application = web.Application()
    first.setup_routes(application)
    second.setup_routes(application)
    client = await aiohttp_client(application)

    body = post_body()
    assert (
        await client.post(
            "/hooks/a",
            data=body,
            headers={"Content-Type": "application/json", "X-Hub-Signature-256": gh_sign(body)},
        )
    ).status == 202
    # The 'a' secret must not authenticate 'b'.
    assert (
        await client.post(
            "/hooks/b",
            data=body,
            headers={"Content-Type": "application/json", "X-Hub-Signature-256": gh_sign(body)},
        )
    ).status == 401


async def test_provider_hook_start_warns_without_callback(caplog):
    orphan = _FakeProviderHook(_FakeProviderHook.build_config("/x", secret=SECRET))
    caplog.set_level("WARNING")
    await orphan.start()
    assert any("NO callback registered" in rec.message for rec in caplog.records)


async def test_provider_hook_start_warns_without_secret(caplog):
    open_hook = _FakeProviderHook(
        _FakeProviderHook.build_config("/x", require_signature=False)
    )
    caplog.set_level("WARNING")
    await open_hook.start()
    assert any("unauthenticated" in rec.message for rec in caplog.records)


async def test_provider_hook_unexpected_error_returns_500(aiohttp_client, manager):
    class Broken(_FakeProviderHook):
        def _normalize_payload(self, request, endpoint, payload, event_type):
            raise RuntimeError("kaboom")

    broken = Broken(Broken.build_config("/broken", require_signature=False))
    manager.register(broken)
    application = web.Application()
    broken.setup_routes(application)
    client = await aiohttp_client(application)

    resp = await client.post("/broken", data=post_body(), headers={"Content-Type": "application/json"})
    assert resp.status == 500


async def test_provider_hook_stop_is_safe(hook):
    await hook.stop()


# ---------------------------------------------------------------------------
# Pipeline parity with the listener
# ---------------------------------------------------------------------------


@pytest.fixture(params=["listener", "provider"])
async def parity_client(request, aiohttp_client, manager):
    """Serve the same endpoint config through both hook kinds."""
    application = web.Application()
    if request.param == "listener":
        hook = WebhookListenerHook(base_path="/api/v1/hooks/webhook")
        hook.register_endpoint(
            "/p", secret=SECRET, signature_scheme="github", allowed_ips=["203.0.113.0/24"]
        )
        url = "/api/v1/hooks/webhook/p"
    else:
        hook = _FakeProviderHook(
            _FakeProviderHook.build_config(
                "/api/v1/hooks/webhook/p", secret=SECRET, allowed_ips=["203.0.113.0/24"]
            )
        )
        url = "/api/v1/hooks/webhook/p"
    manager.register(hook)
    hook.setup_routes(application)
    client = await aiohttp_client(application)
    return client, url


async def test_pipeline_parity_rejects_disallowed_ip(parity_client):
    client, url = parity_client
    body = post_body()
    resp = await client.post(
        url,
        data=body,
        headers={"Content-Type": "application/json", "X-Hub-Signature-256": gh_sign(body)},
    )
    assert resp.status == 403


async def test_pipeline_parity_rejects_missing_signature(aiohttp_client, manager):
    for build in ("listener", "provider"):
        application = web.Application()
        if build == "listener":
            hook = WebhookListenerHook()
            hook.register_endpoint("/p", secret=SECRET, signature_scheme="github")
            url = "/api/v1/hooks/webhook/p"
        else:
            hook = _FakeProviderHook(_FakeProviderHook.build_config("/p", secret=SECRET))
            url = "/p"
        manager.register(hook)
        hook.setup_routes(application)
        client = await aiohttp_client(application)
        resp = await client.post(url, data=post_body(), headers={"Content-Type": "application/json"})
        assert resp.status == 401, f"{build} did not return 401"
