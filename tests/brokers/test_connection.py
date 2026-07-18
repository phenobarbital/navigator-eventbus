"""Unit tests for navigator_eventbus.brokers.connection (TASK-1814, FEAT-316)."""
from aiohttp import web

from navigator_eventbus.brokers.producer import BrokerProducer


class DummyProducer(BrokerProducer):
    _name_ = "dummy_producer"

    async def connect(self):
        ...

    async def disconnect(self):
        ...

    async def publish_message(self, body, queue_name=None, **kwargs):
        ...

    async def consume_messages(self, queue_name, callback, **kwargs):
        ...

    async def process_message(self, body, properties=None):
        ...


def test_base_connection_setup_raw_app():
    """setup() must accept a plain web.Application (no BaseApplication)."""
    app = web.Application()
    p = DummyProducer()
    p.setup(app)  # must not require BaseApplication
    assert p.app is app


def test_base_connection_setup_get_app_duck_type():
    """setup() must also accept an object exposing get_app()."""

    class FakeBaseApplication:
        def __init__(self, app):
            self._app = app

        def get_app(self):
            return self._app

    app = web.Application()
    wrapper = FakeBaseApplication(app)
    p = DummyProducer()
    p.setup(wrapper)
    assert p.app is app


def test_base_connection_setup_none_raises():
    p = DummyProducer()
    try:
        p.setup(None)
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
