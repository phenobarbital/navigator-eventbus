"""Tests for RedisStreamsBackend (FEAT-312, TASK-1801).

Mudado desde
``packages/ai-parrot/tests/core/events/bus/test_redis_streams.py``
(ai-parrot@686aba1fe, FEAT-310) — imports adapted to
``navigator_eventbus``; wire-format assertions updated to the neutral
defaults (``evb:stream:``, ``evb:events:dedup:``, group ``evb-bus`` —
FEAT-312 decoupling). New tests added: prefix/group default + override
(constructor and navconfig).

Unit tier uses a hand-rolled fake streams client (fakeredis is not in the
dependency set); the two-consumer end-to-end test is ``integration``-marked
and skips when no Redis is reachable.
"""
import asyncio
import json
import os
import time

import pytest

from navigator_eventbus.backends.base import TransportBackend
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend
from navigator_eventbus.envelope import EventEnvelope


def make_envelope(topic: str = "app.job", **kwargs) -> EventEnvelope:
    return EventEnvelope(topic=topic, payload=kwargs.pop("payload", {"k": 1}), **kwargs)


async def wait_until(condition, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(0.01)
    pytest.fail("condition not met within timeout")


# ---------------------------------------------------------------------------
# Fake Redis with minimal Streams semantics
# ---------------------------------------------------------------------------


class FakeStreamsRedis:
    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict]]] = {}
        # (stream, group) -> {"delivered": int, "pending": {id: [consumer, ts]}}
        self.groups: dict[tuple[str, str], dict] = {}
        self.kv: dict[str, str] = {}
        self.acked: list[tuple[str, str, str]] = []
        self._seq = 0

    async def xadd(self, name, fields, maxlen=None, approximate=True):
        self._seq += 1
        msg_id = f"{self._seq}-0"
        entries = self.streams.setdefault(name, [])
        entries.append((msg_id, dict(fields)))
        if maxlen is not None and len(entries) > maxlen:
            del entries[: len(entries) - maxlen]
        return msg_id

    async def xgroup_create(self, name, group, id="0", mkstream=False):
        if name not in self.streams:
            if not mkstream:
                raise Exception("NOGROUP no such stream")
            self.streams[name] = []
        key = (name, group)
        if key in self.groups:
            raise FakeResponseError("BUSYGROUP Consumer Group name already exists")
        self.groups[key] = {"delivered": 0, "pending": {}}

    async def xreadgroup(self, group, consumer, streams, count=None, block=None):
        results = []
        for stream in streams:
            g = self.groups.get((stream, group))
            if g is None:
                continue
            entries = self.streams.get(stream, [])
            new = entries[g["delivered"]:]
            if count:
                new = new[:count]
            if new:
                now = time.monotonic()
                for msg_id, _ in new:
                    g["pending"][msg_id] = [consumer, now]
                g["delivered"] += len(new)
                results.append((stream, list(new)))
        if not results and block:
            await asyncio.sleep(min(block / 1000, 0.02))
        return results

    async def xack(self, stream, group, msg_id):
        g = self.groups.get((stream, group))
        if g and msg_id in g["pending"]:
            del g["pending"][msg_id]
            self.acked.append((stream, group, msg_id))
            return 1
        return 0

    async def xautoclaim(
        self, name, group, consumer, min_idle_time, start_id="0-0", count=None
    ):
        g = self.groups.get((name, group))
        if g is None:
            return ["0-0", [], []]
        now = time.monotonic()
        claimed = []
        by_id = dict(self.streams.get(name, []))
        for msg_id, meta in list(g["pending"].items()):
            idle_ms = (now - meta[1]) * 1000
            if idle_ms >= min_idle_time and msg_id in by_id:
                g["pending"][msg_id] = [consumer, now]
                claimed.append((msg_id, by_id[msg_id]))
                if count and len(claimed) >= count:
                    break
        return ["0-0", claimed, []]

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    async def get(self, key):
        return self.kv.get(key)

    async def delete(self, key):
        return 1 if self.kv.pop(key, None) is not None else 0

    async def scan_iter(self, match=None):
        prefix = (match or "*").rstrip("*")
        for name in list(self.streams):
            if name.startswith(prefix):
                yield name

    async def close(self):
        pass


class FakeResponseError(Exception):
    pass


@pytest.fixture(autouse=True)
def _patch_response_error(monkeypatch):
    """Make the backend's BUSYGROUP check catch the fake's error type."""
    import navigator_eventbus.backends.redis_streams as mod
    monkeypatch.setattr(
        mod.aioredis, "ResponseError", FakeResponseError, raising=False
    )


@pytest.fixture
def fake_redis():
    return FakeStreamsRedis()


def make_backend(fake_redis, **overrides) -> RedisStreamsBackend:
    defaults = dict(
        client=fake_redis,
        consumer_name="test-consumer",
        block_ms=20,
        autoclaim_interval=0.05,
        min_idle_time_ms=50,
        stream_refresh_interval=0.01,
        dedup_ttl=60,
    )
    defaults.update(overrides)
    return RedisStreamsBackend(**defaults)


# ---------------------------------------------------------------------------
# FEAT-312 — neutral prefix/group defaults + override (new)
# ---------------------------------------------------------------------------


def test_streams_prefixes_default_neutral(fake_redis):
    backend = make_backend(fake_redis)
    assert backend.stream_prefix == "evb:stream:"
    assert backend.dedup_prefix == "evb:events:dedup:"
    assert backend._group == "evb-bus"


def test_streams_prefixes_override(fake_redis):
    backend = make_backend(
        fake_redis,
        stream_prefix="parrot:stream:",
        dedup_prefix="parrot:events:dedup:",
        group="parrot-bus",
    )
    assert backend.stream_prefix == "parrot:stream:"
    assert backend.dedup_prefix == "parrot:events:dedup:"
    assert backend._group == "parrot-bus"


def test_streams_prefixes_override_via_navconfig(fake_redis, monkeypatch):
    import navigator_eventbus.backends.redis_streams as mod

    overrides = {
        "BUS_STREAM_PREFIX": "nav:stream:",
        "BUS_DEDUP_PREFIX": "nav:events:dedup:",
        "BUS_GROUP": "nav-bus",
    }
    monkeypatch.setattr(
        mod.nav_config, "get", lambda key, fallback=None: overrides.get(key, fallback)
    )
    backend = make_backend(fake_redis)
    assert backend.stream_prefix == "nav:stream:"
    assert backend.dedup_prefix == "nav:events:dedup:"
    assert backend._group == "nav-bus"


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


def test_streams_backend_satisfies_protocol(fake_redis):
    assert isinstance(make_backend(fake_redis), TransportBackend)


def test_requires_url_or_client():
    with pytest.raises(ValueError):
        RedisStreamsBackend()


async def test_streams_publish_consume_ack(fake_redis):
    backend = make_backend(fake_redis)
    received: list[EventEnvelope] = []

    async def consumer(envelope):
        received.append(envelope)

    env = make_envelope("app.job")
    await backend.publish(env)

    # stream-per-topic-class with the JSON wire format
    assert "evb:stream:app" in fake_redis.streams
    _, fields = fake_redis.streams["evb:stream:app"][0]
    assert EventEnvelope.from_dict(json.loads(fields["envelope"])) == env

    await backend.start_consumer(consumer)
    await wait_until(lambda: len(received) == 1)
    assert received[0] == env
    # ACKed exactly once in the happy path
    await wait_until(lambda: len(fake_redis.acked) == 1)
    stream, group, _ = fake_redis.acked[0]
    assert (stream, group) == ("evb:stream:app", "evb-bus")
    pending = fake_redis.groups[("evb:stream:app", "evb-bus")]["pending"]
    assert pending == {}
    await backend.close()


async def test_streams_autoclaim_reclaims_pending(fake_redis):
    env = make_envelope("app.crashed")
    # Seed: entry delivered to a consumer that died before ACK.
    await fake_redis.xadd(
        "evb:stream:app", {"envelope": json.dumps(env.to_dict())}
    )
    await fake_redis.xgroup_create("evb:stream:app", "evb-bus", id="0")
    g = fake_redis.groups[("evb:stream:app", "evb-bus")]
    g["delivered"] = 1
    g["pending"]["1-0"] = ["dead-consumer", time.monotonic() - 10]  # stale

    backend = make_backend(fake_redis)
    received: list[EventEnvelope] = []

    async def consumer(envelope):
        received.append(envelope)

    await backend.start_consumer(consumer)
    # The sweeper reclaims + reprocesses + ACKs.
    await wait_until(lambda: len(received) == 1)
    assert received[0] == env
    await wait_until(lambda: ("evb:stream:app", "evb-bus", "1-0") in fake_redis.acked)
    assert g["pending"] == {}
    await backend.close()


async def test_streams_event_id_dedup(fake_redis):
    backend = make_backend(fake_redis)
    received: list[EventEnvelope] = []

    async def consumer(envelope):
        received.append(envelope)

    env = make_envelope("app.dup")
    # The same envelope lands on the stream twice (redelivery scenario).
    await backend.publish(env)
    await backend.publish(env)

    await backend.start_consumer(consumer)
    await wait_until(lambda: len(fake_redis.acked) == 2)  # both ACKed
    await asyncio.sleep(0.05)
    assert len(received) == 1  # processed once — dedup SET honored
    assert f"evb:events:dedup:{env.event_id}" in fake_redis.kv
    await backend.close()


async def test_streams_roundtrip_with_version(fake_redis):
    """FEAT-319 M1: legacy (version-less) and v1 messages coexist in one
    stream and both are consumable via ``from_dict``."""
    backend = make_backend(fake_redis)
    received: list[EventEnvelope] = []

    async def consumer(envelope):
        received.append(envelope)

    # Legacy entry: wire dict with no "schema_version" key at all —
    # simulates a message produced before this spec landed.
    legacy_env = make_envelope("app.legacy")
    legacy_wire = legacy_env.to_dict()
    del legacy_wire["schema_version"]
    await fake_redis.xadd(
        "evb:stream:app", {"envelope": json.dumps(legacy_wire)}
    )

    # v1 entry via the normal publish path.
    v1_env = make_envelope("app.v1")
    await backend.publish(v1_env)

    await backend.start_consumer(consumer)
    await wait_until(lambda: len(received) == 2)
    by_topic = {env.topic: env for env in received}
    assert by_topic["app.legacy"].schema_version == 1
    assert by_topic["app.v1"].schema_version == 1
    await backend.close()


async def test_streams_failure_keeps_pending_and_unmarked(fake_redis):
    backend = make_backend(fake_redis, autoclaim_interval=999)  # sweeper idle
    calls: list[str] = []

    async def failing_consumer(envelope):
        calls.append(envelope.event_id)
        raise RuntimeError("handler boom")

    env = make_envelope("app.fail")
    await backend.publish(env)
    await backend.start_consumer(failing_consumer)
    await wait_until(lambda: len(calls) == 1)
    await asyncio.sleep(0.05)
    # No ACK → stays pending for reclaim; dedup key NEVER set on failure
    # (set-after-success ordering), so redelivery reprocesses it.
    assert fake_redis.acked == []
    pending = fake_redis.groups[("evb:stream:app", "evb-bus")]["pending"]
    assert "1-0" in pending
    assert f"evb:events:dedup:{env.event_id}" not in fake_redis.kv
    await backend.close()


async def test_streams_ack_only_after_buscore_dispatch(fake_redis, monkeypatch):
    """The Critical-fix contract: with real BusCore wiring, XACK fires only
    AFTER subscribers have fully run (not after a mere local enqueue)."""
    from navigator_eventbus import BusCore

    backend = make_backend(fake_redis, autoclaim_interval=999)
    core = BusCore(workers=2, queue_size=16, backend=backend)

    order: list[str] = []
    release = asyncio.Event()

    async def slow_handler(envelope):
        order.append("handler-start")
        await release.wait()
        order.append("handler-end")

    core.subscribe("remote.*", slow_handler)

    real_ack = backend._ack

    async def spying_ack(stream, msg_id):
        order.append("ack")
        await real_ack(stream, msg_id)

    monkeypatch.setattr(backend, "_ack", spying_ack)

    # Simulate a remote-origin entry: XADD directly (no local fan-out).
    env = make_envelope("remote.job")
    await fake_redis.xadd(
        "evb:stream:remote", {"envelope": json.dumps(env.to_dict())}
    )
    await core.start()  # starts the backend consumer

    await wait_until(lambda: "handler-start" in order)
    await asyncio.sleep(0.05)
    assert "ack" not in order  # handler still running → NOT acked yet
    release.set()
    await wait_until(lambda: "ack" in order)
    assert order == ["handler-start", "handler-end", "ack"]
    # Dedup key marked only after success.
    assert f"evb:events:dedup:{env.event_id}" in fake_redis.kv
    await core.close()


async def test_streams_poison_entry_acked_and_dropped(fake_redis):
    await fake_redis.xadd("evb:stream:app", {"envelope": "not-json{{{"})
    backend = make_backend(fake_redis)
    received: list[EventEnvelope] = []

    async def consumer(envelope):
        received.append(envelope)

    await backend.start_consumer(consumer)
    await wait_until(lambda: len(fake_redis.acked) == 1)  # poison ACKed away
    assert received == []
    await backend.close()


# ---------------------------------------------------------------------------
# Integration (real Redis) — spec §4 test_end_to_end_streams_mode
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_end_to_end_streams_two_consumers():
    """Two consumers in one group: at-least-once, no double-processing."""
    import redis.asyncio as aioredis

    redis_url = os.environ.get("REDIS_TEST_URL", "redis://localhost:6379/9")
    try:
        probe = await aioredis.from_url(redis_url)
        await probe.ping()
        await probe.flushdb()
        await probe.close()
    except Exception:
        pytest.skip(f"No Redis reachable at {redis_url}")

    received_a: list[str] = []
    received_b: list[str] = []

    backend_a = RedisStreamsBackend(
        redis_url, consumer_name="itest-a", block_ms=100,
        autoclaim_interval=999, stream_refresh_interval=0.1,
    )
    backend_b = RedisStreamsBackend(
        redis_url, consumer_name="itest-b", block_ms=100,
        autoclaim_interval=999, stream_refresh_interval=0.1,
    )

    async def consumer_a(env):
        received_a.append(env.event_id)

    async def consumer_b(env):
        received_b.append(env.event_id)

    envs = [make_envelope(f"itest.job{i}") for i in range(20)]
    for env in envs:
        await backend_a.publish(env)

    await backend_a.start_consumer(consumer_a)
    await backend_b.start_consumer(consumer_b)
    await wait_until(
        lambda: len(received_a) + len(received_b) == 20, timeout=10.0
    )
    await asyncio.sleep(0.3)

    processed = received_a + received_b
    assert sorted(processed) == sorted(e.event_id for e in envs)
    assert len(set(processed)) == 20  # each processed exactly once (dedup)
    await backend_a.close()
    await backend_b.close()
