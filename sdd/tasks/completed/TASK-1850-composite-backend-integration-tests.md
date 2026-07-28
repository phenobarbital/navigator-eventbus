# TASK-1850: Integration tests for CompositeBackend (real Redis)

**Feature**: FEAT-430 — Composite Multi-Channel Backend for navigator-eventbus
**Spec**: `sdd/specs/eventbus-composite-backend.spec.md`
**Status**: pending
**Priority**: medium
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-1848
**Assigned-to**: unassigned

---

## Context

> Implements spec Module 5: integration tests against a real Redis instance.
> Verifies that broadcast + 2 group channels over one shared connection deliver
> correctly end-to-end, and that `close()` issues only 1 Redis `QUIT` (not 3).
>
> These tests require a running Redis server and should be marked with
> `@pytest.mark.integration` (or skipped if Redis is unavailable).

---

## Scope

- Write 2 integration tests from spec §4 against a real Redis instance
- Verify correct broadcast + group delivery semantics end-to-end
- Verify single-connection shutdown behaviour

**NOT in scope**:
- Unit tests with mocked Redis (TASK-1849)
- Implementation changes to `CompositeBackend` (TASK-1848)

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `tests/test_composite_integration.py` | CREATE | Real-Redis integration tests |

---

## Codebase Contract (Anti-Hallucination)

> **CRITICAL**: This section contains VERIFIED code references from the actual codebase.

### Verified Imports
```python
# TASK-1848 creates these — verify they exist before writing tests
from navigator_eventbus.backends.composite import Channel, CompositeBackend

# Already exist:
from navigator_eventbus.envelope import EventEnvelope, Severity  # envelope.py
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend  # redis_streams.py:101

# Redis client:
import redis.asyncio as aioredis  # already a project dependency

# Test dependencies:
import pytest
import asyncio
```

### Existing Signatures to Use

```python
# CompositeBackend (created by TASK-1848, spec §2):
class CompositeBackend:
    def __init__(self, *, redis_url=None, client=None, channels, codec=None,
                 stream_key_fn=None, streams=None, retention=None,
                 publish_via=None, **common_backend_kwargs): ...
    async def publish(self, envelope: EventEnvelope) -> None: ...
    async def start_consumer(self, on_envelope: OnEnvelope) -> None: ...
    async def close(self) -> None: ...

# Channel (created by TASK-1848, spec §2):
@dataclass(frozen=True)
class Channel:
    name: str
    delivery: Literal["broadcast", "group"]
    on_envelope: OnEnvelope
    streams: list[str] | None = None
    max_deliveries: int | None = None
    on_dlq: Optional[Callable[..., Union[None, Awaitable[None]]]] = None

# EventEnvelope — used to create test events
# Verify actual constructor by reading envelope.py before writing tests
```

### Does NOT Exist
- ~~`CompositeBackend.connection_count`~~ — no such attribute; verify shutdown via Redis MONITOR or client pool inspection

---

## Implementation Notes

### Test Cases (from spec §4)

1. `test_composite_broadcast_plus_two_groups` — Real Redis: publish 10 entries;
   broadcast channel sees all 10; each group sees all 10; entries are correctly
   ACKed per group.
2. `test_composite_shutdown_closes_one_connection` — Real Redis: after `close()`,
   only 1 Redis `QUIT` is issued (not 3).

### Key Constraints
- Use `pytest-asyncio` for async test functions
- Mark tests with `@pytest.mark.skipif` when Redis is unavailable
- Use a test-specific Redis URL (e.g., `redis://localhost:6379/15`)
- Clean up streams/groups after each test to avoid pollution
- Use `asyncio.Event` or `asyncio.Queue` in callbacks to collect delivered entries

### Test Fixture (from spec §4)
```python
REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:6379/15")

@pytest.fixture
def channels():
    """Three compliance-like channels for testing."""
    return [
        Channel(name="broadcast", delivery="broadcast",
                on_envelope=_noop_handler),
        Channel(name="audit-ledger", delivery="group",
                on_envelope=_noop_handler, max_deliveries=5),
        Channel(name="push-alerts", delivery="group",
                on_envelope=_noop_handler, max_deliveries=5),
    ]

@pytest.fixture
async def composite(channels):
    """CompositeBackend wired to a real Redis instance."""
    backend = CompositeBackend(
        redis_url=REDIS_TEST_URL,
        channels=channels,
        streams=["fieldsync.manager", "fieldsync.associate",
                 "fieldsync.program", "fieldsync.admin"],
    )
    yield backend
    await backend.close()
```

### References in Codebase
- `tests/test_integration.py` — existing integration test patterns
- `tests/test_backends_streams.py` — existing Redis backend tests

---

## Acceptance Criteria

- [ ] Both integration test cases from spec §4 are implemented
- [ ] Tests pass against a real Redis instance: `pytest tests/test_composite_integration.py -v`
- [ ] Tests skip gracefully when Redis is unavailable
- [ ] No linting errors: `ruff check tests/test_composite_integration.py`
- [ ] Broadcast channel receives all published entries
- [ ] Each group channel receives all entries independently (exactly once per group)
- [ ] `close()` closes only 1 Redis connection (not N)

---

## Test Specification

```python
import asyncio
import os
import pytest
from navigator_eventbus.backends.composite import Channel, CompositeBackend
from navigator_eventbus.envelope import EventEnvelope

REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:6379/15")


@pytest.mark.asyncio
async def test_composite_broadcast_plus_two_groups():
    """Publish 10 entries; broadcast sees all 10; each group sees all 10."""
    broadcast_received = []
    ledger_received = []
    push_received = []

    channels = [
        Channel(name="broadcast", delivery="broadcast",
                on_envelope=lambda e: broadcast_received.append(e)),
        Channel(name="audit-ledger", delivery="group",
                on_envelope=lambda e: ledger_received.append(e),
                max_deliveries=5),
        Channel(name="push-alerts", delivery="group",
                on_envelope=lambda e: push_received.append(e),
                max_deliveries=5),
    ]

    composite = CompositeBackend(
        redis_url=REDIS_TEST_URL,
        channels=channels,
        streams=["test.composite"],
    )
    # ... publish 10 entries, start consumer, wait, assert counts
    await composite.close()


@pytest.mark.asyncio
async def test_composite_shutdown_closes_one_connection():
    """After close(), only 1 Redis QUIT is issued (not 3)."""
    ...
```

---

## Agent Instructions

When you pick up this task:

1. **Read the spec** at `sdd/specs/eventbus-composite-backend.spec.md` for full context
2. **Check dependencies** — verify TASK-1848 is in `sdd/tasks/completed/`
3. **Verify the Codebase Contract** — read `src/navigator_eventbus/backends/composite.py`
   to learn the actual API surface TASK-1848 created
4. **Read `tests/test_integration.py`** for existing integration test patterns
5. **Read `envelope.py`** to learn how to construct test `EventEnvelope` instances
6. **Update status** in `sdd/tasks/index/eventbus-composite-backend.json` → `"in-progress"`
7. **Implement** both integration test cases
8. **Run**: `pytest tests/test_composite_integration.py -v` (requires Redis)
9. **Move this file** to `sdd/tasks/completed/TASK-1850-composite-backend-integration-tests.md`
10. **Update index** → `"done"`
11. **Fill in the Completion Note** below

---

## Completion Note

**Completed by**: sdd-worker (Claude Sonnet)
**Date**: 2026-07-29
**Notes**: Implemented both spec §4 integration tests in
`tests/test_composite_integration.py`, marked `pytestmark =
pytest.mark.integration` (registered marker, matches
`tests/test_backends_streams.py`'s live-Redis tests), skipping
gracefully via a `probe.ping()`/`flushdb()` helper when no Redis is
reachable at `REDIS_TEST_URL` (default `redis://localhost:6379/15`) —
verified both by running against a live local Redis (both PASS) and by
pointing `REDIS_TEST_URL` at an unreachable port (both SKIP, 0 failures).

`test_composite_broadcast_plus_two_groups`: publishes 10 entries through
a broadcast + 2 group channels sharing ONE Redis connection; asserts the
broadcast channel sees all 10, and each group sees all 10 independently
exactly once. Deliberately does **not** pass `streams=`/`stream_key_fn=`
to `CompositeBackend` — all 3 channels converge on the same
default-sharded stream (`<stream_prefix><topic-class>`) via SCAN
discovery, mirroring `tests/test_backends_streams.py`'s own live tests.
Discovered mid-implementation (see Deviations) that broadcast mode needs
a seed entry published on the SAME stream *before* `start_consumer()` —
otherwise tail-resolution (`_resolve_tail_id`, `redis_streams.py:464`)
races the first real batch and swallows it entirely as "pre-existing".
Group channels' fresh consumer-group cursors start at `"0"` and DO see
that seed, so the group callbacks filter it out by `event_id`.

`test_composite_shutdown_closes_one_connection`: white-box spy on the
ONE shared `redis.asyncio.Redis` client's `close()` (real client,
real Redis) proves `CompositeBackend.close()` issues exactly one
connection-close regardless of channel count — the N internal
`RedisStreamsBackend` instances never own/close the injected client
(verified `redis_streams.py:384`). Chosen over literal Redis `MONITOR`-based
`QUIT` sniffing (mentioned in the spec) as a more deterministic,
less flaky signal for the same underlying guarantee.

Full suite: `pytest tests/` — 342 passed, 0 regressions.
`ruff check tests/test_composite_integration.py` — clean.

**Deviations from spec**: the spec's own Test Fixture code block (§4)
passes `streams=["fieldsync.manager", ...]` to `CompositeBackend` without
a paired `stream_key_fn=` override — reproducing it literally causes
`publish()` (which always targets `_stream_for(topic)`,
i.e. `<stream_prefix><topic-class>` by default) to write to a DIFFERENT
stream than the one the internal backends are told to *consume* via
`streams=`, so nothing is ever delivered. That fixture is explicitly a
scaffold (the Test Specification body is `...`), not runnable code, so
this task drops the explicit `streams=` override and relies on
`CompositeBackend`'s default SCAN-based discovery instead — verified
working end-to-end against a real Redis instance. No production
`CompositeBackend` code changed to accommodate this; it is purely a test
construction choice.
