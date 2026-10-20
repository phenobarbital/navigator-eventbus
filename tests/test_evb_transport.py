"""Unit tests for EventBus transport and outbox facade options."""
import pytest

from navigator_eventbus import EventBus, SqliteOutbox
from navigator_eventbus.backends.memory import MemoryBackend


def test_eventbus_transport_precedence():
    """Explicit backends beat transports, which beat legacy Redis selection."""
    custom_backend = MemoryBackend()
    bus = EventBus(
        redis_url="redis://localhost:6379/0",
        use_redis=True,
        transport="streams",
        backend=custom_backend,
    )
    assert bus.core._backend is custom_backend

    transport_bus = EventBus(
        redis_url="redis://localhost:6379/0", use_redis=True, transport="memory"
    )
    assert isinstance(transport_bus.core._backend, MemoryBackend)


def test_transport_streams_builds_redis_streams_backend():
    """Streams selection constructs the durable backend without connecting."""
    pytest.importorskip("redis")
    from navigator_eventbus.backends.redis_streams import RedisStreamsBackend

    bus = EventBus("redis://localhost:6379/0", transport="streams")
    assert isinstance(bus.core._backend, RedisStreamsBackend)


@pytest.mark.parametrize("transport", ["pubsub", "streams"])
def test_transport_requires_redis_url(transport):
    """Redis transports reject construction without a Redis URL."""
    with pytest.raises(ValueError, match="requires redis_url"):
        EventBus(transport=transport)


def test_unknown_transport_raises():
    """Unsupported transport names are rejected explicitly."""
    with pytest.raises(ValueError, match="Unknown transport"):
        EventBus(transport="unknown")


def test_outbox_path_builds_sqlite_outbox(tmp_path):
    """A path builds the default SQLite outbox store."""
    bus = EventBus(outbox=tmp_path / "outbox.db")
    assert isinstance(bus.core._outbox, SqliteOutbox)


def test_outbox_store_passed_through(tmp_path):
    """An explicitly constructed outbox is preserved unchanged."""
    outbox = SqliteOutbox(tmp_path / "outbox.db")
    bus = EventBus(outbox=outbox)
    assert bus.core._outbox is outbox


async def test_publish_only_forwarded():
    """Publish-only buses never register a backend consumer."""
    backend = MemoryBackend()
    bus = EventBus(backend=backend, publish_only=True)
    await bus.connect()
    assert bus.core._publish_only is True
    assert backend._on_envelope is None
    await bus.close()


def test_navconfig_keys_honored(monkeypatch, tmp_path):
    """Configured facade and outbox options flow into the BusCore."""
    import navigator_eventbus.evb as evb_module

    values = {
        "BUS_TRANSPORT": "memory",
        "BUS_PUBLISH_ONLY": "yes",
        "BUS_OUTBOX_PATH": str(tmp_path / "configured.db"),
        "BUS_OUTBOX_MAX_ROWS": "17",
        "BUS_OUTBOX_OVERFLOW": "drop_oldest",
        "BUS_OUTBOX_RELAY_INTERVAL": "2.5",
    }
    monkeypatch.setattr(
        evb_module.nav_config,
        "get",
        lambda key, fallback=None: values.get(key, fallback),
    )

    bus = EventBus()
    assert isinstance(bus.core._backend, MemoryBackend)
    assert bus.core._publish_only is True
    assert isinstance(bus.core._outbox, SqliteOutbox)
    assert bus.core._outbox._max_rows == 17
    assert bus.core._outbox._overflow == "drop_oldest"
    assert bus.core._relay_interval == 2.5


def test_default_construction_unchanged():
    """The default facade still uses the in-process memory backend."""
    bus = EventBus()
    assert isinstance(bus.core._backend, MemoryBackend)
