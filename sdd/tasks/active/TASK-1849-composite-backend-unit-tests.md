# TASK-1849: Unit tests for CompositeBackend (mocked Redis)

**Feature**: FEAT-430 — Composite Multi-Channel Backend for navigator-eventbus
**Spec**: `sdd/specs/eventbus-composite-backend.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-1848
**Assigned-to**: unassigned

---

## Context

> Implements spec Module 4: unit tests with mocked Redis. Covers channel
> isolation, per-group dedup, publish routing, start/stop lifecycle, error
> isolation, and `Channel` validation. These tests must run without a live
> Redis instance.

---

## Scope

- Write unit tests for `Channel` dataclass validation
- Write unit tests for `CompositeBackend` with mocked Redis
- Cover all 13 unit test cases from spec §4

**NOT in scope**:
- Integration tests with real Redis (TASK-1850)
- Implementation changes to `CompositeBackend` (TASK-1848)

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `tests/test_composite_backend.py` | CREATE | Unit tests with mocked Redis |

---

## Codebase Contract (Anti-Hallucination)

> **CRITICAL**: This section contains VERIFIED code references from the actual codebase.

### Verified Imports
```python
# TASK-1848 creates these — verify they exist before writing tests
from navigator_eventbus.backends.composite import Channel, CompositeBackend

# Already exist:
from navigator_eventbus.backends.base import OnEnvelope, TransportBackend  # base.py:24,28
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend   # redis_streams.py:101
from navigator_eventbus.envelope import EventEnvelope                      # envelope.py

# Test dependencies:
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
```

### Existing Signatures to Use

```python
# src/navigator_eventbus/backends/base.py:24
OnEnvelope = Callable[[EventEnvelope], Awaitable[None]]

# src/navigator_eventbus/backends/base.py:28-46
@runtime_checkable
class TransportBackend(Protocol):
    async def publish(self, envelope: EventEnvelope) -> None: ...
    async def start_consumer(self, on_envelope: OnEnvelope) -> None: ...
    async def close(self) -> None: ...

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
```

### Does NOT Exist
- ~~`CompositeBackend.channels`~~ — verify actual attribute name; may be `_channels`
- ~~`Channel.codec`~~ — may or may not exist; verify after TASK-1848 completes

---

## Implementation Notes

### Test Cases (from spec §4)

1. `test_channel_validates_broadcast_no_max_deliveries` — `Channel(delivery="broadcast", max_deliveries=5)` raises `ValueError`
2. `test_composite_creates_internal_backends` — Composite with 3 channels creates 3 `RedisStreamsBackend` instances
3. `test_composite_shares_redis_client` — All internal backends receive the same `client=` reference
4. `test_publish_routes_to_designated_channel` — `publish()` calls `XADD` via the `publish_via` channel's backend only
5. `test_start_consumer_starts_all_channels` — `start_consumer()` starts all N internal backends
6. `test_close_closes_all_channels_then_client` — `close()` stops all backends, then closes the shared Redis client
7. `test_channel_failure_isolated` — One channel's consumer error does not crash others
8. `test_broadcast_channel_receives_all_entries` — Broadcast channel callback fires for every published entry
9. `test_group_channel_receives_each_entry_once` — Group channel callback fires exactly once per entry
10. `test_two_groups_both_receive_same_entry` — Same entry delivered to both group channels independently
11. `test_dedup_is_per_group` — Dedup keys include group name — no cross-group suppression
12. `test_channel_stream_subset` — Channel with `streams=[X]` only consumes from stream X
13. `test_on_dlq_per_channel` — Each group channel's `on_dlq` fires independently

### Key Constraints
- Use `unittest.mock.AsyncMock` for Redis client mocking
- Use `pytest-asyncio` for async test functions
- Follow the existing test pattern in `tests/test_backends_streams.py`

### Test Fixture (from spec §4)
```python
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
```

### References in Codebase
- `tests/test_backends_streams.py` — existing RedisStreamsBackend tests (pattern reference)
- `tests/conftest.py` — shared fixtures

---

## Acceptance Criteria

- [ ] All 13 unit test cases from spec §4 are implemented
- [ ] Tests pass with mocked Redis (no live Redis required): `pytest tests/test_composite_backend.py -v`
- [ ] No linting errors: `ruff check tests/test_composite_backend.py`
- [ ] `isinstance(composite, TransportBackend)` verified in tests
- [ ] Channel validation (broadcast + max_deliveries → ValueError) tested

---

## Test Specification

```python
import pytest
from unittest.mock import AsyncMock
from navigator_eventbus.backends.composite import Channel, CompositeBackend


async def _noop_handler(envelope):
    pass


class TestChannel:
    def test_channel_validates_broadcast_no_max_deliveries(self):
        with pytest.raises(ValueError):
            Channel(name="bad", delivery="broadcast",
                    on_envelope=_noop_handler, max_deliveries=5)


class TestCompositeBackend:
    async def test_composite_creates_internal_backends(self, channels):
        ...

    async def test_composite_shares_redis_client(self, channels):
        ...

    async def test_publish_routes_to_designated_channel(self, channels):
        ...

    async def test_start_consumer_starts_all_channels(self, channels):
        ...

    async def test_close_closes_all_channels_then_client(self, channels):
        ...

    async def test_channel_failure_isolated(self, channels):
        ...

    async def test_broadcast_channel_receives_all_entries(self, channels):
        ...

    async def test_group_channel_receives_each_entry_once(self, channels):
        ...

    async def test_two_groups_both_receive_same_entry(self, channels):
        ...

    async def test_dedup_is_per_group(self, channels):
        ...

    async def test_channel_stream_subset(self, channels):
        ...

    async def test_on_dlq_per_channel(self, channels):
        ...
```

---

## Agent Instructions

When you pick up this task:

1. **Read the spec** at `sdd/specs/eventbus-composite-backend.spec.md` for full context
2. **Check dependencies** — verify TASK-1848 is in `sdd/tasks/completed/`
3. **Verify the Codebase Contract** — read `src/navigator_eventbus/backends/composite.py`
   to learn the actual API surface TASK-1848 created
4. **Update status** in `sdd/tasks/index/eventbus-composite-backend.json` → `"in-progress"`
5. **Implement** all 13 test cases
6. **Run**: `pytest tests/test_composite_backend.py -v`
7. **Move this file** to `sdd/tasks/completed/TASK-1849-composite-backend-unit-tests.md`
8. **Update index** → `"done"`
9. **Fill in the Completion Note** below

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**: <session or agent ID>
**Date**: YYYY-MM-DD
**Notes**: What was implemented, any deviations from scope, issues encountered.

**Deviations from spec**: none | describe if any
