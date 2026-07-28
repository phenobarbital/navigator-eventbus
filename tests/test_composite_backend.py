"""Tests for CompositeBackend (FEAT-430, TASK-1849).

Unit tier — mocked Redis only, no live Redis required. The internal
``RedisStreamsBackend`` instances that ``CompositeBackend`` creates are
replaced with :class:`FakeInternalBackend` fakes (recording constructor
kwargs, with ``AsyncMock`` protocol methods) so these tests exercise ONLY
``CompositeBackend``'s own orchestration logic — channel isolation,
publish routing, per-group dedup-prefix computation, start/stop
lifecycle, and error isolation. ``RedisStreamsBackend``'s own Redis
Streams semantics (XADD/XREADGROUP/XACK/dedup) are already covered by
``tests/test_backends_streams.py``.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from navigator_eventbus.backends.base import TransportBackend
from navigator_eventbus.backends.composite import Channel, CompositeBackend
from navigator_eventbus.envelope import EventEnvelope


def make_envelope(topic: str = "fieldsync.manager", **kwargs) -> EventEnvelope:
    return EventEnvelope(topic=topic, payload=kwargs.pop("payload", {"k": 1}), **kwargs)


async def _noop_handler(envelope: EventEnvelope) -> None:
    pass


class FakeInternalBackend:
    """Stand-in for ``RedisStreamsBackend`` — records constructor kwargs,
    exposes ``TransportBackend``-shaped ``AsyncMock`` methods."""

    def __init__(self, **kwargs) -> None:
        self.init_kwargs = kwargs
        self.publish = AsyncMock()
        self.start_consumer = AsyncMock()
        self.close = AsyncMock()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def created_backends() -> list[FakeInternalBackend]:
    """Populated, in channel-construction order, by ``patched_redis_streams``."""
    return []


@pytest.fixture
def patched_redis_streams(created_backends):
    """Patches ``composite.RedisStreamsBackend`` with a fake factory."""

    def _factory(**kwargs) -> FakeInternalBackend:
        backend = FakeInternalBackend(**kwargs)
        created_backends.append(backend)
        return backend

    with patch(
        "navigator_eventbus.backends.composite.RedisStreamsBackend",
        MagicMock(side_effect=_factory),
    ) as mock_cls:
        yield mock_cls


@pytest.fixture
def shared_client() -> MagicMock:
    client = MagicMock(name="shared_redis_client")
    client.close = AsyncMock()
    return client


@pytest.fixture
def channels() -> list[Channel]:
    """Three compliance-like channels for testing (spec §4)."""
    return [
        Channel(name="broadcast", delivery="broadcast", on_envelope=_noop_handler),
        Channel(
            name="audit-ledger",
            delivery="group",
            on_envelope=_noop_handler,
            max_deliveries=5,
        ),
        Channel(
            name="push-alerts",
            delivery="group",
            on_envelope=_noop_handler,
            max_deliveries=5,
        ),
    ]


@pytest.fixture
def composite(channels, shared_client, patched_redis_streams):
    return CompositeBackend(client=shared_client, channels=channels)


# ---------------------------------------------------------------------------
# Channel validation
# ---------------------------------------------------------------------------


class TestChannel:
    def test_channel_validates_broadcast_no_max_deliveries(self) -> None:
        with pytest.raises(ValueError):
            Channel(
                name="bad",
                delivery="broadcast",
                on_envelope=_noop_handler,
                max_deliveries=5,
            )


# ---------------------------------------------------------------------------
# CompositeBackend orchestration
# ---------------------------------------------------------------------------


class TestCompositeBackend:
    def test_composite_backend_satisfies_transport_backend_protocol(
        self, composite
    ) -> None:
        assert isinstance(composite, TransportBackend)

    async def test_composite_creates_internal_backends(
        self, composite, created_backends
    ) -> None:
        await composite.start_consumer(AsyncMock())
        assert len(created_backends) == 3

    async def test_composite_shares_redis_client(
        self, composite, created_backends, shared_client
    ) -> None:
        await composite.start_consumer(AsyncMock())
        assert len(created_backends) == 3
        for backend in created_backends:
            assert backend.init_kwargs["client"] is shared_client

    async def test_publish_routes_to_designated_channel(
        self, composite, created_backends
    ) -> None:
        envelope = make_envelope()
        await composite.publish(envelope)

        assert len(created_backends) == 3
        broadcast_backend, ledger_backend, push_backend = created_backends
        broadcast_backend.publish.assert_awaited_once_with(envelope)
        ledger_backend.publish.assert_not_called()
        push_backend.publish.assert_not_called()

    async def test_start_consumer_starts_all_channels(
        self, composite, created_backends
    ) -> None:
        callback = AsyncMock()
        await composite.start_consumer(callback)

        assert len(created_backends) == 3
        for backend in created_backends:
            backend.start_consumer.assert_awaited_once()

    async def test_close_closes_all_channels_then_client(
        self, channels, patched_redis_streams, created_backends
    ) -> None:
        fake_client = MagicMock(name="owned_redis_client")
        fake_client.close = AsyncMock()

        with patch(
            "navigator_eventbus.backends.composite.aioredis.from_url",
            AsyncMock(return_value=fake_client),
        ):
            composite = CompositeBackend(redis_url="redis://test", channels=channels)
            await composite.start_consumer(AsyncMock())

            call_order: list[object] = []
            for backend in created_backends:
                backend.close.side_effect = (
                    lambda b=backend: call_order.append(b) or None
                )
            fake_client.close.side_effect = lambda: call_order.append(fake_client)

            await composite.close()

        assert call_order[:-1] == created_backends
        assert call_order[-1] is fake_client

    async def test_channel_failure_isolated(
        self, composite, created_backends
    ) -> None:
        # Trigger backend creation without starting consumers yet.
        await composite.publish(make_envelope())
        assert len(created_backends) == 3

        created_backends[1].start_consumer.side_effect = RuntimeError("boom")

        # Must not raise — one channel's failure is isolated from others.
        await composite.start_consumer(AsyncMock())

        for backend in created_backends:
            backend.start_consumer.assert_awaited_once()

    async def test_broadcast_channel_receives_all_entries(
        self, composite, created_backends
    ) -> None:
        transport_callback = AsyncMock()
        await composite.start_consumer(transport_callback)

        broadcast_backend = created_backends[0]
        broadcast_backend.start_consumer.assert_awaited_once_with(transport_callback)
        wired_callback = broadcast_backend.start_consumer.call_args.args[0]

        envelope_1, envelope_2 = make_envelope(), make_envelope()
        await wired_callback(envelope_1)
        await wired_callback(envelope_2)

        assert transport_callback.await_count == 2
        transport_callback.assert_any_await(envelope_1)
        transport_callback.assert_any_await(envelope_2)

    async def test_group_channel_receives_each_entry_once(self) -> None:
        ledger_callback = AsyncMock()
        channels = [
            Channel(name="broadcast", delivery="broadcast", on_envelope=_noop_handler),
            Channel(
                name="audit-ledger",
                delivery="group",
                on_envelope=ledger_callback,
                max_deliveries=5,
            ),
        ]
        with patch(
            "navigator_eventbus.backends.composite.RedisStreamsBackend",
            MagicMock(side_effect=lambda **kw: FakeInternalBackend(**kw)),
        ):
            composite = CompositeBackend(client=MagicMock(), channels=channels)
            await composite.start_consumer(AsyncMock())
            ledger_backend = composite._backends["audit-ledger"]

        wired_callback = ledger_backend.start_consumer.call_args.args[0]
        assert wired_callback is ledger_callback

        envelope = make_envelope()
        await wired_callback(envelope)

        ledger_callback.assert_awaited_once_with(envelope)

    async def test_two_groups_both_receive_same_entry(self) -> None:
        ledger_callback = AsyncMock()
        push_callback = AsyncMock()
        channels = [
            Channel(name="broadcast", delivery="broadcast", on_envelope=_noop_handler),
            Channel(
                name="audit-ledger",
                delivery="group",
                on_envelope=ledger_callback,
                max_deliveries=5,
            ),
            Channel(
                name="push-alerts",
                delivery="group",
                on_envelope=push_callback,
                max_deliveries=5,
            ),
        ]
        with patch(
            "navigator_eventbus.backends.composite.RedisStreamsBackend",
            MagicMock(side_effect=lambda **kw: FakeInternalBackend(**kw)),
        ):
            composite = CompositeBackend(client=MagicMock(), channels=channels)
            await composite.start_consumer(AsyncMock())
            ledger_backend = composite._backends["audit-ledger"]
            push_backend = composite._backends["push-alerts"]

        ledger_wired = ledger_backend.start_consumer.call_args.args[0]
        push_wired = push_backend.start_consumer.call_args.args[0]

        envelope = make_envelope()
        await ledger_wired(envelope)
        await push_wired(envelope)

        ledger_callback.assert_awaited_once_with(envelope)
        push_callback.assert_awaited_once_with(envelope)

    async def test_dedup_is_per_group(self, composite, created_backends) -> None:
        await composite.start_consumer(AsyncMock())

        _broadcast_backend, ledger_backend, push_backend = created_backends
        assert ledger_backend.init_kwargs["dedup_prefix"] == (
            "evb:events:dedup:audit-ledger:"
        )
        assert push_backend.init_kwargs["dedup_prefix"] == (
            "evb:events:dedup:push-alerts:"
        )
        assert (
            ledger_backend.init_kwargs["dedup_prefix"]
            != push_backend.init_kwargs["dedup_prefix"]
        )

    async def test_channel_stream_subset(
        self, shared_client, patched_redis_streams, created_backends
    ) -> None:
        channels = [
            Channel(
                name="broadcast",
                delivery="broadcast",
                on_envelope=_noop_handler,
                streams=["fieldsync.manager"],
            ),
            Channel(
                name="audit-ledger",
                delivery="group",
                on_envelope=_noop_handler,
                max_deliveries=5,
            ),
        ]
        composite = CompositeBackend(
            client=shared_client,
            channels=channels,
            streams=["fieldsync.manager", "fieldsync.program"],
        )
        await composite.start_consumer(AsyncMock())

        broadcast_backend, ledger_backend = created_backends
        assert broadcast_backend.init_kwargs["streams"] == ["fieldsync.manager"]
        # No channel-level override — inherits the parent's full stream set.
        assert ledger_backend.init_kwargs["streams"] == [
            "fieldsync.manager",
            "fieldsync.program",
        ]

    async def test_on_dlq_per_channel(
        self, shared_client, patched_redis_streams, created_backends
    ) -> None:
        ledger_dlq = AsyncMock()
        push_dlq = AsyncMock()
        channels = [
            Channel(name="broadcast", delivery="broadcast", on_envelope=_noop_handler),
            Channel(
                name="audit-ledger",
                delivery="group",
                on_envelope=_noop_handler,
                max_deliveries=5,
                on_dlq=ledger_dlq,
            ),
            Channel(
                name="push-alerts",
                delivery="group",
                on_envelope=_noop_handler,
                max_deliveries=5,
                on_dlq=push_dlq,
            ),
        ]
        composite = CompositeBackend(client=shared_client, channels=channels)
        await composite.start_consumer(AsyncMock())

        _broadcast_backend, ledger_backend, push_backend = created_backends
        assert ledger_backend.init_kwargs["on_dlq"] is ledger_dlq
        assert push_backend.init_kwargs["on_dlq"] is push_dlq
        assert ledger_backend.init_kwargs["on_dlq"] is not push_backend.init_kwargs[
            "on_dlq"
        ]
