"""Tests for QueueStore — the Redis Streams lease engine (queues, TASK-C)."""
import pytest

from navigator_eventbus.backends.redis_streams import DefaultCodec
from navigator_eventbus.envelope import EventEnvelope
from navigator_eventbus.queues.config import (
    QueueConfig,
    QueueGroupConfig,
    QueueRegistry,
)
from navigator_eventbus.queues.receipts import ReceiptCodec
from navigator_eventbus.queues.store import QueueStore, StaleReceipt, default_consumer_name

from ._fake_streams import FakeClock, FakeStreamsRedis

KEY = "receipt-key"


def make_queue(**kwargs) -> QueueConfig:
    kwargs.setdefault("name", "orders")
    kwargs.setdefault("groups", [QueueGroupConfig(name="billing")])
    kwargs.setdefault("max_visibility_timeout_ms", 60_000)
    # The default lease can never exceed the reclaim threshold, so derive it
    # whenever a test lowers the threshold.
    kwargs.setdefault(
        "default_visibility_timeout_ms", min(30_000, kwargs["max_visibility_timeout_ms"])
    )
    return QueueConfig(**kwargs)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def redis(clock) -> FakeStreamsRedis:
    return FakeStreamsRedis(clock)


@pytest.fixture
def config() -> QueueConfig:
    return make_queue()


def build_store(redis, clock, config, **kwargs) -> QueueStore:
    registry = QueueRegistry(queues=[config])
    return QueueStore(
        registry,
        redis,
        ReceiptCodec([KEY]),
        clock=clock,
        **kwargs,
    )


@pytest.fixture
def store(redis, clock, config) -> QueueStore:
    return build_store(redis, clock, config)


def envelope(topic="queue.orders.created", **payload) -> EventEnvelope:
    return EventEnvelope(topic=topic, payload=payload or {"n": 1})


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_rejects_overlapping_prefixes(redis, clock, config):
    registry = QueueRegistry(queues=[config], queue_prefix="evb:")
    with pytest.raises(ValueError, match="overlaps the bus stream prefix"):
        QueueStore(registry, redis, ReceiptCodec([KEY]), bus_stream_prefix="evb:stream:")


def test_consumer_name_is_per_replica_not_per_request(store):
    assert store.consumer_name == default_consumer_name()
    assert store.consumer_name.startswith("http-")


def test_warns_when_client_returns_bytes(clock, config, caplog):
    redis = FakeStreamsRedis(clock)
    redis.connection_pool.connection_kwargs = {"decode_responses": False}
    caplog.set_level("WARNING")
    build_store(redis, clock, config)
    assert any("decode_responses=False" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


async def test_send_receive_delete_round_trip(store, config, redis):
    await store.ensure_queue(config)
    await store.send(config, envelope(n=7))

    messages = await store.receive(config, "billing")
    assert len(messages) == 1
    assert messages[0].envelope.payload == {"n": 7}
    assert messages[0].delivered == 1

    await store.delete(config, messages[0].receipt)
    assert await store.receive(config, "billing") == []


async def test_receive_on_empty_queue_returns_empty(store, config):
    await store.ensure_queue(config)
    assert await store.receive(config, "billing") == []


async def test_ensure_queue_is_idempotent(store, config):
    await store.ensure_queue(config)
    await store.ensure_queue(config)
    await store.ensure_all()


async def test_receive_auto_creates_the_group(store, config):
    """A receive before ensure_queue must not explode."""
    await store.send(config, envelope())
    assert len(await store.receive(config, "billing")) == 1


async def test_delete_is_idempotent(store, config):
    await store.ensure_queue(config)
    await store.send(config, envelope())
    message = (await store.receive(config, "billing"))[0]
    await store.delete(config, message.receipt)
    await store.delete(config, message.receipt)


async def test_max_messages_is_respected(store, config):
    await store.ensure_queue(config)
    for _ in range(5):
        await store.send(config, envelope())
    assert len(await store.receive(config, "billing", max_messages=3)) == 3


# ---------------------------------------------------------------------------
# The two delivery guarantees
# ---------------------------------------------------------------------------


async def test_two_consumers_in_one_group_get_disjoint_messages(
    redis, clock, config
):
    await build_store(redis, clock, config).ensure_queue(config)
    producer = build_store(redis, clock, config)
    for _ in range(4):
        await producer.send(config, envelope())

    a = build_store(redis, clock, config, consumer_name="node-a")
    b = build_store(redis, clock, config, consumer_name="node-b")
    got_a = await a.receive(config, "billing", max_messages=2)
    got_b = await b.receive(config, "billing", max_messages=2)

    ids_a = {m.message_id for m in got_a}
    ids_b = {m.message_id for m in got_b}
    assert len(ids_a) == 2 and len(ids_b) == 2
    assert ids_a.isdisjoint(ids_b), "competing consumers must not overlap"


async def test_two_groups_each_receive_every_message(redis, clock):
    """Fan-out across groups — the requirement that forbids XDEL."""
    config = make_queue(
        groups=[QueueGroupConfig(name="billing"), QueueGroupConfig(name="analytics")]
    )
    store = build_store(redis, clock, config)
    await store.ensure_queue(config)
    await store.send(config, envelope(n=1))
    await store.send(config, envelope(n=2))

    billing = await store.receive(config, "billing", max_messages=10)
    analytics = await store.receive(config, "analytics", max_messages=10)

    assert [m.envelope.payload for m in billing] == [{"n": 1}, {"n": 2}]
    assert [m.envelope.payload for m in analytics] == [{"n": 1}, {"n": 2}]


async def test_deleting_in_one_group_leaves_the_other_pending(redis, clock):
    config = make_queue(
        groups=[QueueGroupConfig(name="billing"), QueueGroupConfig(name="analytics")]
    )
    store = build_store(redis, clock, config)
    await store.ensure_queue(config)
    await store.send(config, envelope())

    billing = (await store.receive(config, "billing"))[0]
    await store.delete(config, billing.receipt)

    assert len(await store.receive(config, "analytics")) == 1


# ---------------------------------------------------------------------------
# Visibility timeout — the IDLE-offset mechanism
# ---------------------------------------------------------------------------


async def test_message_is_invisible_while_the_lease_holds(store, config, clock):
    await store.ensure_queue(config)
    await store.send(config, envelope())
    assert len(await store.receive(config, "billing", visibility_ms=10_000)) == 1

    clock.advance_ms(9_000)
    assert await store.receive(config, "billing") == []


async def test_message_is_redelivered_after_the_lease_expires(store, config, clock):
    await store.ensure_queue(config)
    await store.send(config, envelope())
    first = await store.receive(config, "billing", visibility_ms=10_000)
    assert len(first) == 1

    clock.advance_ms(11_000)
    again = await store.receive(config, "billing")
    assert len(again) == 1
    assert again[0].message_id == first[0].message_id
    assert again[0].delivered == 2, "redelivery must increment the receive count"


async def test_default_visibility_is_used_when_unspecified(store, config, clock):
    await store.ensure_queue(config)
    await store.send(config, envelope())
    await store.receive(config, "billing")  # default 30s

    clock.advance_ms(29_000)
    assert await store.receive(config, "billing") == []
    clock.advance_ms(2_000)
    assert len(await store.receive(config, "billing")) == 1


async def test_no_xclaim_when_lease_equals_the_threshold(redis, clock):
    """The common-path optimisation: no extra round-trip."""
    config = make_queue(
        default_visibility_timeout_ms=60_000, max_visibility_timeout_ms=60_000
    )
    store = build_store(redis, clock, config)
    await store.ensure_queue(config)
    await store.send(config, envelope())
    await store.receive(config, "billing")
    assert not [c for c in redis.calls if c[0] == "xclaim"]


async def test_xclaim_uses_justid_so_the_counter_is_not_inflated(store, config, redis):
    await store.ensure_queue(config)
    await store.send(config, envelope())
    messages = await store.receive(config, "billing", visibility_ms=5_000)
    assert messages[0].delivered == 1
    claims = [c for c in redis.calls if c[0] == "xclaim"]
    assert claims and claims[0][2]["justid"] is True


async def test_visibility_failure_degrades_to_the_longer_lease(
    store, config, clock, monkeypatch, redis, caplog
):
    """Failure direction must be late redelivery, never early double-delivery."""

    async def boom(*args, **kwargs):
        raise RuntimeError("xclaim down")

    monkeypatch.setattr(redis, "xclaim", boom)
    await store.ensure_queue(config)
    await store.send(config, envelope())
    caplog.set_level("WARNING")
    assert len(await store.receive(config, "billing", visibility_ms=1_000)) == 1

    clock.advance_ms(2_000)
    assert await store.receive(config, "billing") == [], "must NOT be visible early"
    clock.advance_ms(60_000)
    assert len(await store.receive(config, "billing")) == 1


# ---------------------------------------------------------------------------
# change_visibility, including nack
# ---------------------------------------------------------------------------


async def test_change_visibility_to_zero_is_a_nack(store, config):
    await store.ensure_queue(config)
    await store.send(config, envelope())
    message = (await store.receive(config, "billing", visibility_ms=30_000))[0]

    await store.change_visibility(config, message.receipt, 0)

    again = await store.receive(config, "billing")
    assert len(again) == 1, "visibility 0 must release the message immediately"
    assert again[0].message_id == message.message_id


async def test_change_visibility_extends_a_lease(store, config, clock):
    await store.ensure_queue(config)
    await store.send(config, envelope())
    message = (await store.receive(config, "billing", visibility_ms=5_000))[0]

    await store.change_visibility(config, message.receipt, 40_000)

    clock.advance_ms(10_000)
    assert await store.receive(config, "billing") == [], "lease should be extended"


async def test_change_visibility_shortens_a_lease(store, config, clock):
    await store.ensure_queue(config)
    await store.send(config, envelope())
    message = (await store.receive(config, "billing", visibility_ms=50_000))[0]

    await store.change_visibility(config, message.receipt, 1_000)

    clock.advance_ms(2_000)
    assert len(await store.receive(config, "billing")) == 1


# ---------------------------------------------------------------------------
# Poison messages -> DLQ
# ---------------------------------------------------------------------------


async def test_message_over_max_receives_is_parked_to_the_dlq(redis, clock):
    parked = []

    async def on_dlq(env, **kwargs):
        parked.append((env, kwargs))

    config = make_queue(max_receives=2, max_visibility_timeout_ms=10_000)
    store = build_store(redis, clock, config, on_dlq=on_dlq)
    await store.ensure_queue(config)
    await store.send(config, envelope(poison=True))

    for _ in range(4):
        await store.receive(config, "billing")
        clock.advance_ms(11_000)

    assert len(parked) == 1, f"expected exactly one DLQ handoff, got {len(parked)}"
    env, kwargs = parked[0]
    assert env.payload == {"poison": True}
    assert kwargs["attempts"] > 2
    assert kwargs["subscriber_id"] == "evb:queue:orders:billing"
    assert await store.receive(config, "billing") == [], "must never be redelivered"


async def test_group_level_max_receives_override(redis, clock):
    parked = []

    async def on_dlq(env, **kwargs):
        parked.append(env)

    config = make_queue(
        max_receives=99,
        max_visibility_timeout_ms=10_000,
        groups=[QueueGroupConfig(name="billing", max_receives=1)],
    )
    store = build_store(redis, clock, config, on_dlq=on_dlq)
    await store.ensure_queue(config)
    await store.send(config, envelope())
    for _ in range(3):
        await store.receive(config, "billing")
        clock.advance_ms(11_000)
    assert parked


async def test_dlq_failure_does_not_block_reclaim(redis, clock, caplog):
    async def broken(*args, **kwargs):
        raise RuntimeError("dlq down")

    config = make_queue(max_receives=1, max_visibility_timeout_ms=10_000)
    store = build_store(redis, clock, config, on_dlq=broken)
    await store.ensure_queue(config)
    await store.send(config, envelope())
    caplog.set_level("ERROR")
    for _ in range(3):
        await store.receive(config, "billing")
        clock.advance_ms(11_000)
    assert await store.receive(config, "billing") == []


# ---------------------------------------------------------------------------
# strict_ack / lease generation
# ---------------------------------------------------------------------------


async def test_stale_receipt_is_rejected_when_strict(store, config, clock):
    await store.ensure_queue(config)
    await store.send(config, envelope())
    first = (await store.receive(config, "billing", visibility_ms=1_000))[0]

    clock.advance_ms(61_000)
    await store.receive(config, "billing")  # someone else reclaimed it

    with pytest.raises(StaleReceipt):
        await store.delete(config, first.receipt)


async def test_stale_receipt_allowed_when_strict_ack_is_off(redis, clock):
    config = make_queue(strict_ack=False, max_visibility_timeout_ms=10_000)
    store = build_store(redis, clock, config)
    await store.ensure_queue(config)
    await store.send(config, envelope())
    first = (await store.receive(config, "billing"))[0]
    clock.advance_ms(11_000)
    await store.receive(config, "billing")
    await store.delete(config, first.receipt)


# ---------------------------------------------------------------------------
# Reclaim ordering
# ---------------------------------------------------------------------------


async def test_reclaimed_messages_are_returned_before_fresh_ones(store, config, clock):
    """Otherwise a busy queue starves retries forever."""
    await store.ensure_queue(config)
    await store.send(config, envelope(order=1))
    stale = (await store.receive(config, "billing", visibility_ms=1_000))[0]
    clock.advance_ms(61_000)

    await store.send(config, envelope(order=2))
    got = await store.receive(config, "billing", max_messages=2)
    assert got[0].message_id == stale.message_id


# ---------------------------------------------------------------------------
# XDEL policy
# ---------------------------------------------------------------------------


async def test_delete_does_not_xdel_by_default(store, config, redis):
    await store.ensure_queue(config)
    await store.send(config, envelope())
    message = (await store.receive(config, "billing"))[0]
    await store.delete(config, message.receipt)
    assert not [c for c in redis.calls if c[0] == "xdel"]
    assert await redis.xlen("evb:queue:orders") == 1


async def test_delete_entries_opt_in_does_xdel(redis, clock):
    config = make_queue(delete_entries=True)
    store = build_store(redis, clock, config)
    await store.ensure_queue(config)
    await store.send(config, envelope())
    message = (await store.receive(config, "billing"))[0]
    await store.delete(config, message.receipt)
    assert [c for c in redis.calls if c[0] == "xdel"]
    assert await redis.xlen("evb:queue:orders") == 0


# ---------------------------------------------------------------------------
# Codec identity — keeps the two planes' wire formats from drifting
# ---------------------------------------------------------------------------


async def test_queue_decodes_an_envelope_written_by_the_bus_codec(store, config, redis):
    """A bus-published entry must be readable by the queue plane."""
    await store.ensure_queue(config)
    original = EventEnvelope(topic="app.job", payload={"from": "bus"})
    await redis.xadd("evb:queue:orders", DefaultCodec().encode(original))

    message = (await store.receive(config, "billing"))[0]
    assert message.envelope.topic == "app.job"
    assert message.envelope.payload == {"from": "bus"}
    assert message.envelope.event_id == original.event_id


async def test_bus_codec_decodes_an_envelope_written_by_the_queue(store, config, redis):
    await store.ensure_queue(config)
    sent = envelope(topic="app.job", direction="out")
    await store.send(config, sent)
    _mid, fields = redis.streams["evb:queue:orders"][0]
    assert DefaultCodec().decode(fields).payload == sent.payload


async def test_undecodable_entry_is_acked_and_skipped(store, config, redis, caplog):
    await store.ensure_queue(config)
    await redis.xadd("evb:queue:orders", {"envelope": "{not json"})
    caplog.set_level("ERROR")
    assert await store.receive(config, "billing") == []
    assert redis.acked, "a poison-format entry must be ACKed, not left blocking"


# ---------------------------------------------------------------------------
# describe / purge / prune
# ---------------------------------------------------------------------------


async def test_describe_without_group(store, config):
    await store.ensure_queue(config)
    await store.send(config, envelope())
    info = await store.describe(config)
    assert info["queue"] == "orders"
    assert info["stream"] == "evb:queue:orders"
    assert info["stream_length"] == 1
    assert info["groups"] == ["billing"]
    assert info["approximate_number_of_messages_delayed"] == 0


async def test_describe_with_group_reports_backlog_and_inflight(store, config):
    await store.ensure_queue(config)
    for _ in range(3):
        await store.send(config, envelope())
    await store.receive(config, "billing", max_messages=1)

    info = await store.describe(config, "billing")
    assert info["approximate_number_of_messages"] == 2
    assert info["approximate_number_of_messages_not_visible"] == 1


async def test_purge_empties_the_stream_and_keeps_groups_usable(store, config, redis):
    await store.ensure_queue(config)
    for _ in range(3):
        await store.send(config, envelope())
    await store.receive(config, "billing")

    await store.purge(config)

    assert await redis.xlen("evb:queue:orders") == 0
    assert await store.receive(config, "billing") == []
    await store.send(config, envelope(after="purge"))
    got = await store.receive(config, "billing")
    assert len(got) == 1 and got[0].envelope.payload == {"after": "purge"}


async def test_prune_consumers_removes_only_idle_empty_ones(redis, clock, config):
    store = build_store(redis, clock, config, consumer_name="worker-1")
    await store.ensure_queue(config)
    await store.send(config, envelope())
    message = (await store.receive(config, "billing"))[0]

    clock.advance(90_000)
    assert await store.prune_consumers(config) == 0, "holds a lease — must stay"

    await store.delete(config, message.receipt)
    assert await store.prune_consumers(config) == 1


async def test_prune_consumers_keeps_recent_ones(redis, clock, config):
    store = build_store(redis, clock, config, consumer_name="worker-1")
    await store.ensure_queue(config)
    await store.send(config, envelope())
    message = (await store.receive(config, "billing"))[0]
    await store.delete(config, message.receipt)
    assert await store.prune_consumers(config) == 0


# ---------------------------------------------------------------------------
# Long polling
# ---------------------------------------------------------------------------


async def test_block_is_not_passed_when_no_wait_requested(store, config, redis):
    """BLOCK 0 means block forever in Redis — it must never be sent."""
    await store.ensure_queue(config)
    await store.receive(config, "billing", wait_ms=0)
    reads = [c for c in redis.calls if c[0] == "xreadgroup"]
    assert reads and reads[-1][2]["block"] is None


async def test_block_is_passed_when_waiting(store, config, redis):
    await store.ensure_queue(config)
    await store.receive(config, "billing", wait_ms=5_000)
    reads = [c for c in redis.calls if c[0] == "xreadgroup"]
    assert reads[-1][2]["block"] == 5_000
