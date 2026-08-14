"""Tests for the QueueAPI HTTP surface (queues, TASK-D/E/F)."""
import pytest
from aiohttp import web

from navigator_eventbus.queues.api import QueueAPI
from navigator_eventbus.queues.config import (
    QueueConfig,
    QueueGroupConfig,
    QueueRegistry,
)
from navigator_eventbus.queues.receipts import ReceiptCodec
from navigator_eventbus.queues.store import QueueStore

from ._fake_streams import FakeClock, FakeStreamsRedis

PRODUCER = "producer-token"
CONSUMER = "consumer-token"
ADMIN = "admin-token"
BASE = "/api/v1/queues"


def producer_headers():
    return {"X-API-Key": PRODUCER}


def consumer_headers():
    return {"X-API-Key": CONSUMER}


def admin_headers():
    return {"X-API-Key": ADMIN}


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def redis(clock):
    return FakeStreamsRedis(clock)


@pytest.fixture
def config():
    return QueueConfig(
        name="orders",
        groups=[QueueGroupConfig(name="billing"), QueueGroupConfig(name="analytics")],
        default_visibility_timeout_ms=30_000,
        max_visibility_timeout_ms=60_000,
        producer_tokens=(PRODUCER,),
        consumer_tokens=(CONSUMER,),
        admin_tokens=(ADMIN,),
    )


@pytest.fixture
def registry(config):
    return QueueRegistry(queues=[config])


@pytest.fixture
def store(registry, redis, clock):
    return QueueStore(registry, redis, ReceiptCodec(["api-key"]), clock=clock)


@pytest.fixture
def api(registry, store):
    return QueueAPI(registry, store, expose_admin_routes=True)


@pytest.fixture
async def client(aiohttp_client, api):
    app = web.Application()
    api.setup_routes(app)
    await api.start()
    return await aiohttp_client(app)


async def send(client, payload=None, headers=None):
    return await client.post(
        f"{BASE}/orders/messages",
        json=payload or {"topic": "queue.orders.created", "payload": {"n": 1}},
        headers=headers or producer_headers(),
    )


async def receive(client, **body):
    body.setdefault("group", "billing")
    return await client.post(
        f"{BASE}/orders/messages/receive", json=body, headers=consumer_headers()
    )


# ---------------------------------------------------------------------------
# Send
# ---------------------------------------------------------------------------


async def test_send_returns_201_with_ids(client):
    resp = await send(client)
    assert resp.status == 201
    body = await resp.json()
    assert body["message_id"]
    assert body["event_id"]


async def test_send_rejects_unknown_envelope_field(client):
    resp = await send(client, {"topic": "t", "payload": {}, "bogus": 1})
    assert resp.status == 400
    body = await resp.json()
    assert body["status"] == "invalid_payload"
    assert body["errors"]


async def test_send_rejects_missing_topic(client):
    resp = await send(client, {"payload": {}})
    assert resp.status == 400


async def test_send_rejects_non_json_body(client):
    resp = await client.post(
        f"{BASE}/orders/messages", data=b"not json", headers=producer_headers()
    )
    assert resp.status == 400
    assert (await resp.json())["status"] == "invalid_payload"


async def test_send_rejects_oversized_body(aiohttp_client, registry, store):
    registry.queues[0].max_body_bytes = 64
    api = QueueAPI(registry, store)
    app = web.Application()
    api.setup_routes(app)
    client = await aiohttp_client(app)
    resp = await client.post(
        f"{BASE}/orders/messages", data=b"x" * 500, headers=producer_headers()
    )
    assert resp.status == 413
    assert (await resp.json())["limit"] == 64


async def test_unknown_queue_returns_404(client):
    resp = await client.post(
        f"{BASE}/nope/messages", json={"topic": "t", "payload": {}},
        headers=producer_headers(),
    )
    assert resp.status == 404
    assert (await resp.json())["status"] == "unknown_queue"


# ---------------------------------------------------------------------------
# Auth matrix
# ---------------------------------------------------------------------------


async def test_no_token_is_401(client):
    resp = await client.post(f"{BASE}/orders/messages", json={"topic": "t", "payload": {}})
    assert resp.status == 401


async def test_wrong_token_is_401(client):
    resp = await send(client, headers={"X-API-Key": "nonsense"})
    assert resp.status == 401


async def test_producer_token_on_receive_is_403(client):
    resp = await client.post(
        f"{BASE}/orders/messages/receive",
        json={"group": "billing"},
        headers=producer_headers(),
    )
    assert resp.status == 403
    assert (await resp.json())["required_role"] == "consumer"


async def test_consumer_token_on_send_is_403(client):
    resp = await send(client, headers=consumer_headers())
    assert resp.status == 403


async def test_consumer_token_on_purge_is_403(client):
    resp = await client.post(f"{BASE}/orders/purge", headers=consumer_headers())
    assert resp.status == 403


async def test_bearer_header_is_accepted(client):
    resp = await send(client, headers={"Authorization": f"Bearer {PRODUCER}"})
    assert resp.status == 201


async def test_query_token_is_accepted(client):
    resp = await client.post(
        f"{BASE}/orders/messages?token={PRODUCER}",
        json={"topic": "t", "payload": {}},
    )
    assert resp.status == 201


async def test_shared_token_used_when_no_per_queue_tokens(aiohttp_client, redis, clock):
    config = QueueConfig(name="open", groups=[QueueGroupConfig(name="g")])
    registry = QueueRegistry(queues=[config])
    store = QueueStore(registry, redis, ReceiptCodec(["k"]), clock=clock)
    api = QueueAPI(registry, store, auth_token="shared")
    app = web.Application()
    api.setup_routes(app)
    client = await aiohttp_client(app)

    resp = await client.post(
        f"{BASE}/open/messages",
        json={"topic": "t", "payload": {}},
        headers={"X-API-Key": "shared"},
    )
    assert resp.status == 201


async def test_start_warns_when_nothing_is_configured(aiohttp_client, redis, clock, caplog):
    config = QueueConfig(name="open", groups=[QueueGroupConfig(name="g")])
    registry = QueueRegistry(queues=[config])
    store = QueueStore(registry, redis, ReceiptCodec(["k"]), clock=clock)
    api = QueueAPI(registry, store, auth_token="")
    caplog.set_level("WARNING")
    await api.start()
    assert any("NO tokens configured" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Receive
# ---------------------------------------------------------------------------


async def test_receive_returns_a_message_with_a_handle(client):
    await send(client)
    resp = await receive(client)
    assert resp.status == 200
    messages = (await resp.json())["messages"]
    assert len(messages) == 1
    assert messages[0]["receipt_handle"]
    assert messages[0]["body"]["payload"] == {"n": 1}
    assert messages[0]["attributes"]["approximate_receive_count"] == 1
    assert messages[0]["attributes"]["visibility_expires_at"]


async def test_empty_receive_is_200_with_an_empty_list(client):
    resp = await receive(client)
    assert resp.status == 200
    assert (await resp.json())["messages"] == []


async def test_receive_rejects_unknown_group(client):
    resp = await receive(client, group="nope")
    assert resp.status == 400
    assert (await resp.json())["detail"] == "unknown group"


@pytest.mark.parametrize("value", [0, 11, 99])
async def test_receive_rejects_out_of_range_max_messages(client, value):
    resp = await receive(client, max_messages=value)
    assert resp.status == 400


async def test_receive_rejects_wait_time_over_the_cap(client):
    """The model caps it at 20 — SQS's limit — so 999 is a 400 at the boundary."""
    resp = await receive(client, wait_time_seconds=999)
    assert resp.status == 400


async def test_receive_rejects_visibility_over_the_queue_maximum(client):
    resp = await receive(client, visibility_timeout_seconds=999)
    assert resp.status == 400
    assert (await resp.json())["maximum"] == 60


async def test_two_groups_each_receive_the_message(client):
    await send(client)
    billing = (await (await receive(client, group="billing")).json())["messages"]
    analytics = (await (await receive(client, group="analytics")).json())["messages"]
    assert len(billing) == 1 and len(analytics) == 1


async def test_receive_returns_429_when_saturated(aiohttp_client, registry, store):
    api = QueueAPI(registry, store, max_inflight_receives=1)
    app = web.Application()
    api.setup_routes(app)
    client = await aiohttp_client(app)
    await api._semaphore.acquire()
    resp = await client.post(
        f"{BASE}/orders/messages/receive",
        json={"group": "billing"},
        headers=consumer_headers(),
    )
    assert resp.status == 429
    assert resp.headers["Retry-After"] == "1"


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


async def test_delete_acknowledges_the_message(client):
    await send(client)
    handle = (await (await receive(client)).json())["messages"][0]["receipt_handle"]

    resp = await client.post(
        f"{BASE}/orders/messages/delete",
        json={"group": "billing", "entries": [{"id": "1", "receipt_handle": handle}]},
        headers=consumer_headers(),
    )
    assert resp.status == 200
    assert (await resp.json())["failed"] == []
    assert (await (await receive(client)).json())["messages"] == []


async def test_delete_with_a_forged_handle_is_reported_per_entry(client):
    resp = await client.post(
        f"{BASE}/orders/messages/delete",
        json={"group": "billing", "entries": [{"id": "1", "receipt_handle": "q1.aaa.bbb"}]},
        headers=consumer_headers(),
    )
    assert resp.status == 207
    failed = (await resp.json())["failed"]
    assert failed[0]["code"] == "invalid_receipt_handle"


async def test_handle_from_another_group_is_rejected(client):
    """The check that makes cross-group fan-out safe."""
    await send(client)
    handle = (await (await receive(client, group="billing")).json())["messages"][0][
        "receipt_handle"
    ]
    resp = await client.post(
        f"{BASE}/orders/messages/delete",
        json={"group": "analytics", "entries": [{"id": "1", "receipt_handle": handle}]},
        headers=consumer_headers(),
    )
    assert resp.status == 207
    assert (await resp.json())["failed"][0]["code"] == "invalid_receipt_handle"


async def test_expired_handle_reports_its_own_code(client, clock):
    await send(client)
    handle = (await (await receive(client)).json())["messages"][0]["receipt_handle"]
    clock.advance(120)
    resp = await client.post(
        f"{BASE}/orders/messages/delete",
        json={"group": "billing", "entries": [{"id": "1", "receipt_handle": handle}]},
        headers=consumer_headers(),
    )
    assert resp.status == 207
    assert (await resp.json())["failed"][0]["code"] == "expired_receipt_handle"


async def test_delete_batch_over_ten_is_rejected(client):
    entries = [{"id": str(i), "receipt_handle": "h"} for i in range(11)]
    resp = await client.post(
        f"{BASE}/orders/messages/delete",
        json={"group": "billing", "entries": entries},
        headers=consumer_headers(),
    )
    assert resp.status == 400


async def test_partial_batch_returns_207(client):
    await send(client)
    good = (await (await receive(client)).json())["messages"][0]["receipt_handle"]
    resp = await client.post(
        f"{BASE}/orders/messages/delete",
        json={
            "group": "billing",
            "entries": [
                {"id": "ok", "receipt_handle": good},
                {"id": "bad", "receipt_handle": "garbage"},
            ],
        },
        headers=consumer_headers(),
    )
    assert resp.status == 207
    body = await resp.json()
    assert [item["id"] for item in body["successful"]] == ["ok"]
    assert [item["id"] for item in body["failed"]] == ["bad"]


# ---------------------------------------------------------------------------
# Batch send
# ---------------------------------------------------------------------------


async def test_send_batch(client):
    resp = await client.post(
        f"{BASE}/orders/messages/batch",
        json={
            "entries": [
                {"id": "a", "message": {"topic": "t", "payload": {"i": 1}}},
                {"id": "b", "message": {"topic": "t", "payload": {"i": 2}}},
            ]
        },
        headers=producer_headers(),
    )
    assert resp.status == 200
    assert len((await resp.json())["successful"]) == 2


async def test_send_batch_over_ten_is_rejected(client):
    entries = [
        {"id": str(i), "message": {"topic": "t", "payload": {}}} for i in range(11)
    ]
    resp = await client.post(
        f"{BASE}/orders/messages/batch", json={"entries": entries},
        headers=producer_headers(),
    )
    assert resp.status == 400


# ---------------------------------------------------------------------------
# Visibility, including nack
# ---------------------------------------------------------------------------


async def test_change_visibility_to_zero_releases_the_message(client):
    await send(client)
    handle = (await (await receive(client)).json())["messages"][0]["receipt_handle"]

    resp = await client.post(
        f"{BASE}/orders/messages/visibility",
        json={
            "group": "billing",
            "entries": [
                {"id": "1", "receipt_handle": handle, "visibility_timeout_seconds": 0}
            ],
        },
        headers=consumer_headers(),
    )
    assert resp.status == 200
    again = (await (await receive(client)).json())["messages"]
    assert len(again) == 1, "visibility 0 must be a real nack"


async def test_change_visibility_over_the_maximum_fails_that_entry(client):
    await send(client)
    handle = (await (await receive(client)).json())["messages"][0]["receipt_handle"]
    resp = await client.post(
        f"{BASE}/orders/messages/visibility",
        json={
            "group": "billing",
            "entries": [
                {"id": "1", "receipt_handle": handle, "visibility_timeout_seconds": 999}
            ],
        },
        headers=consumer_headers(),
    )
    assert resp.status == 207
    assert (await resp.json())["failed"][0]["code"] == "invalid_parameter"


# ---------------------------------------------------------------------------
# Describe / admin
# ---------------------------------------------------------------------------


async def test_describe_without_group(client):
    await send(client)
    resp = await client.get(f"{BASE}/orders", headers=consumer_headers())
    assert resp.status == 200
    body = await resp.json()
    assert body["queue"] == "orders"
    assert body["stream_length"] == 1
    assert sorted(body["groups"]) == ["analytics", "billing"]


async def test_describe_with_group(client):
    await send(client)
    await receive(client)
    resp = await client.get(f"{BASE}/orders?group=billing", headers=consumer_headers())
    body = await resp.json()
    assert body["approximate_number_of_messages_not_visible"] == 1


async def test_describe_rejects_unknown_group(client):
    resp = await client.get(f"{BASE}/orders?group=nope", headers=consumer_headers())
    assert resp.status == 400


async def test_list_queues_requires_admin(client):
    assert (await client.get(BASE, headers=consumer_headers())).status == 401
    resp = await client.get(BASE, headers=admin_headers())
    assert resp.status == 200
    assert (await resp.json())["queues"][0]["name"] == "orders"


async def test_purge_empties_the_queue(client):
    await send(client)
    resp = await client.post(f"{BASE}/orders/purge", headers=admin_headers())
    assert resp.status == 200
    assert (await (await receive(client)).json())["messages"] == []


async def test_dlq_route_is_501_without_a_handler(client):
    resp = await client.get(f"{BASE}/orders/dlq", headers=admin_headers())
    assert resp.status == 501
    assert (await resp.json())["status"] == "not_implemented"


async def test_admin_routes_absent_by_default(aiohttp_client, registry, store):
    api = QueueAPI(registry, store)  # expose_admin_routes defaults to False
    app = web.Application()
    api.setup_routes(app)
    client = await aiohttp_client(app)
    assert (await client.post(f"{BASE}/orders/purge", headers=admin_headers())).status == 404


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------


def test_api_is_a_base_hook_with_its_own_hook_type(api):
    from navigator_eventbus.hooks.base import BaseHook

    assert isinstance(api, BaseHook)
    assert api.hook_type == "queue"


async def test_mirror_to_bus_emits_under_the_governed_prefix(
    aiohttp_client, redis, clock
):
    from navigator_eventbus.evb import EventBus

    config = QueueConfig(
        name="orders",
        groups=[QueueGroupConfig(name="billing")],
        producer_tokens=(PRODUCER,),
        mirror_to_bus=True,
    )
    registry = QueueRegistry(queues=[config])
    store = QueueStore(registry, redis, ReceiptCodec(["k"]), clock=clock)
    bus = EventBus()
    seen = []
    bus.subscribe("queue.orders.*", lambda event: seen.append(event))
    api = QueueAPI(registry, store, bus=bus)
    app = web.Application()
    api.setup_routes(app)
    client = await aiohttp_client(app)

    try:
        resp = await client.post(
            f"{BASE}/orders/messages",
            json={"topic": "created", "payload": {"n": 1}},
            headers=producer_headers(),
        )
        assert resp.status == 201
        import asyncio

        for _ in range(100):
            if seen:
                break
            await asyncio.sleep(0.01)
        assert seen, "mirror_to_bus should have emitted"
        assert seen[0].event_type == "queue.orders.created"
        assert seen[0].metadata["queue"] == "orders"
    finally:
        await bus.close()


async def test_mirror_failure_does_not_fail_the_send(aiohttp_client, redis, clock, caplog):
    class BrokenBus:
        async def emit(self, *args, **kwargs):
            raise RuntimeError("bus down")

    config = QueueConfig(
        name="orders",
        groups=[QueueGroupConfig(name="billing")],
        producer_tokens=(PRODUCER,),
        mirror_to_bus=True,
    )
    registry = QueueRegistry(queues=[config])
    store = QueueStore(registry, redis, ReceiptCodec(["k"]), clock=clock)
    api = QueueAPI(registry, store, bus=BrokenBus())
    app = web.Application()
    api.setup_routes(app)
    client = await aiohttp_client(app)

    caplog.set_level("WARNING")
    resp = await client.post(
        f"{BASE}/orders/messages",
        json={"topic": "created", "payload": {}},
        headers=producer_headers(),
    )
    assert resp.status == 201, "the queue stream is the source of truth"
    assert any("mirror_to_bus failed" in r.message for r in caplog.records)
