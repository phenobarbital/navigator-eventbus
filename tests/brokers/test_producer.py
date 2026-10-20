"""Unit tests for navigator_eventbus.brokers.producer (TASK-1814, FEAT-316)."""

import time

import pytest
from aiohttp import web

from navigator_eventbus.brokers.producer import BrokerProducer


class DummyProducer(BrokerProducer):
    _name_ = "dummy_producer"

    async def connect(self): ...

    async def disconnect(self): ...

    async def publish_message(self, body, queue_name=None, **kwargs): ...

    async def consume_messages(self, queue_name, callback, **kwargs): ...

    async def process_message(self, body, properties=None): ...


@pytest.fixture
def auth_callable():
    async def _auth(request):
        return {"user_id": 42}

    return _auth


def test_broker_producer_credentials_keyword():
    """PR #393 fix #3: credentials is a keyword with a None default."""
    p = DummyProducer()  # no positional credentials → OK
    assert p is not None
    p2 = DummyProducer(credentials={"host": "x"})
    assert p2 is not None


async def test_broker_producer_auth_callable(auth_callable):
    p = DummyProducer(auth_callable=auth_callable)
    assert p._auth_callable is auth_callable

    @BrokerProducer.service_auth
    async def protected(self, request):
        return web.json_response({"ok": True})

    class FakeRequest: ...

    response = await protected(p, FakeRequest())
    assert response.status == 200
    assert p._userid == 42


async def test_broker_producer_no_auth_raises_401():
    p = DummyProducer()

    @BrokerProducer.service_auth
    async def protected(self, request):
        return web.json_response({"ok": True})

    class FakeRequest: ...

    with pytest.raises(web.HTTPUnauthorized):
        await protected(p, FakeRequest())


def _post_paths(app: web.Application) -> set[str]:
    """Return the canonical paths of all POST routes on *app*."""
    return {
        route.resource.canonical
        for route in app.router.routes()
        if route.method == "POST"
    }


def test_endpoint_path_default():
    """Without the kwarg, the historical route is registered (FEAT-431 M1)."""
    app = web.Application()
    DummyProducer().setup(app)
    assert "/api/v1/broker/rabbitmq/publish_event" in _post_paths(app)


def test_endpoint_path_custom():
    """A custom path is registered and the default is not."""
    app = web.Application()
    DummyProducer(endpoint_path="/api/v1/rewards/events").setup(app)
    paths = _post_paths(app)
    assert "/api/v1/rewards/events" in paths
    assert "/api/v1/broker/rabbitmq/publish_event" not in paths


async def test_queue_event_no_sleep():
    """queue_event() returns without the 100 ms delay (FEAT-431 M6)."""
    producer = DummyProducer()
    start = time.perf_counter()
    await producer.queue_event({"a": 1}, "navigator", "x.y")
    assert time.perf_counter() - start < 0.05
    assert producer.event_queue.qsize() == 1
