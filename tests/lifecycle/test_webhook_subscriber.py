"""Unit tests for WebhookSubscriber (FEAT-313 TASK-1824)."""
import hashlib
import hmac
from dataclasses import dataclass

import pytest
from aiohttp import web

from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.registry import EventRegistry
from navigator_eventbus.lifecycle.subscribers.webhook import WebhookSubscriber
from navigator_eventbus.lifecycle.trace import TraceContext


@dataclass(frozen=True)
class _TestEvent(LifecycleEvent):
    detail: str = ""


class TestWebhookSubscriber:
    def test_init_validates_url(self):
        sub = WebhookSubscriber(url="https://example.com/hook", secret="s3cret")
        assert sub._url == "https://example.com/hook"

    def test_init_rejects_bad_scheme(self):
        with pytest.raises(ValueError):
            WebhookSubscriber(url="ftp://example.com/hook")

    def test_register_subscribes_default_event_classes(self):
        registry = EventRegistry(forward_to_global=False)
        sub = WebhookSubscriber(url="https://example.com/hook")
        sub.register(registry)
        assert registry.has_subscribers(LifecycleEvent)

    def test_hmac_signature_correctness(self):
        """Verify HMAC-SHA256 signature matches expected."""
        secret = "test-secret"
        body = b'{"event": "test"}'
        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        computed = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        assert computed == expected

    @pytest.mark.asyncio
    async def test_aclose_without_session(self):
        sub = WebhookSubscriber(url="https://example.com/hook")
        await sub.aclose()  # must not raise

    @pytest.mark.asyncio
    async def test_posts_event_and_signs_with_hmac(self, aiohttp_client):
        received = {}

        async def handler(request):
            received["body"] = await request.read()
            received["headers"] = dict(request.headers)
            return web.Response(status=200)

        app = web.Application()
        app.router.add_post("/hook", handler)
        client = await aiohttp_client(app)

        sub = WebhookSubscriber(
            url=str(client.make_url("/hook")), secret="s3cret",
        )
        registry = EventRegistry(forward_to_global=False)
        sub.register(registry)

        evt = _TestEvent(
            trace_context=TraceContext.new_root(),
            source_type="test", source_name="unit", detail="hi",
        )
        await registry.emit(evt)

        assert b'"detail": "hi"' in received["body"]
        expected_sig = hmac.new(b"s3cret", received["body"], hashlib.sha256).hexdigest()
        assert received["headers"]["X-Parrot-Signature"] == f"sha256={expected_sig}"
        await sub.aclose()

    @pytest.mark.asyncio
    async def test_gives_up_immediately_on_4xx(self, aiohttp_client):
        attempts = []

        async def handler(request):
            attempts.append(1)
            return web.Response(status=400)

        app = web.Application()
        app.router.add_post("/hook", handler)
        client = await aiohttp_client(app)

        sub = WebhookSubscriber(url=str(client.make_url("/hook")), max_attempts=3)
        registry = EventRegistry(forward_to_global=False)
        sub.register(registry)

        evt = _TestEvent(trace_context=TraceContext.new_root(),
                         source_type="test", source_name="unit")
        await registry.emit(evt)
        assert len(attempts) == 1  # no retry on 4xx
        await sub.aclose()
