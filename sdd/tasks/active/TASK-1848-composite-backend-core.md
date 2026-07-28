# TASK-1848: Channel dataclass + CompositeBackend + package re-exports

**Feature**: FEAT-430 — Composite Multi-Channel Backend for navigator-eventbus
**Spec**: `sdd/specs/eventbus-composite-backend.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: L (4-8h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

> Implements spec Modules 1–3: the `Channel` dataclass, the `CompositeBackend`
> class, and the package re-exports. This is the foundation task — everything
> else (tests, downstream app.py consolidation) depends on it.
>
> `CompositeBackend` multiplexes N delivery channels over a single Redis
> connection by creating N internal `RedisStreamsBackend` instances with a
> shared `client=`. It implements `TransportBackend` so `BusCore` consumes
> it without changes.

---

## Scope

- Implement the `Channel` frozen dataclass (spec §2 Data Models)
- Implement `CompositeBackend` class satisfying `TransportBackend` (spec §2 New Public Interfaces)
- Add re-exports in `backends/__init__.py` and `__init__.py`

**NOT in scope**:
- Tests (TASK-1849, TASK-1850)
- Modifications to `RedisStreamsBackend`, `BusCore`, or `TransportBackend`
- fieldsync `app.py` consolidation (different repo)

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/backends/composite.py` | CREATE | `Channel` dataclass + `CompositeBackend` class |
| `src/navigator_eventbus/backends/__init__.py` | MODIFY | Add `CompositeBackend`, `Channel` to imports and `__all__` |
| `src/navigator_eventbus/__init__.py` | MODIFY | Add `CompositeBackend` to top-level re-exports |

---

## Codebase Contract (Anti-Hallucination)

> **CRITICAL**: This section contains VERIFIED code references from the actual codebase.
> The implementing agent MUST use these exact imports, class names, and method signatures.
> **DO NOT** invent, guess, or assume any import, attribute, or method not listed here.

### Verified Imports
```python
# src/navigator_eventbus/backends/base.py:24
from navigator_eventbus.backends.base import OnEnvelope, TransportBackend

# src/navigator_eventbus/backends/redis_streams.py:68,101
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend, Codec

# src/navigator_eventbus/envelope.py (re-exported via __init__.py:19)
from navigator_eventbus.envelope import EventEnvelope
```

### Existing Signatures to Use

```python
# src/navigator_eventbus/backends/base.py:24
OnEnvelope = Callable[[EventEnvelope], Awaitable[None]]

# src/navigator_eventbus/backends/base.py:28-46
@runtime_checkable
class TransportBackend(Protocol):
    async def publish(self, envelope: EventEnvelope) -> None: ...   # line 37
    async def start_consumer(self, on_envelope: OnEnvelope) -> None: ...  # line 41
    async def close(self) -> None: ...  # line 45

# src/navigator_eventbus/backends/redis_streams.py:68-85
class Codec(Protocol):
    def encode(self, envelope: EventEnvelope) -> dict[str, Any]: ...  # line 79
    def decode(self, fields: dict[str, Any]) -> EventEnvelope: ...    # line 83

# src/navigator_eventbus/backends/redis_streams.py:194-222
# Key constructor params for CompositeBackend delegation:
RedisStreamsBackend(
    redis_url=None, *, client=None, group=None, consumer_name=None,
    stream_prefix=None, dedup_prefix=None, dedup_ttl=86400,
    block_ms=1000, batch_count=32, min_idle_time_ms=60000,
    autoclaim_interval=30.0, maxlen=100000,
    stream_refresh_interval=10.0, reconnect_base_delay=0.5,
    reconnect_max_delay=30.0, delivery="group", codec=None,
    stream_key_fn=None, streams=None, retention=None,
    retention_trim_interval=60.0, max_deliveries=None, on_dlq=None,
)

# Client ownership: redis_streams.py:384
# When self._client is not None (injected), close() does NOT close the redis
# connection — it only closes connections it created itself.

# src/navigator_eventbus/backends/__init__.py — current __all__:
__all__ = (
    "MemoryBackend", "OnEnvelope", "RedisPubSubBackend",
    "RedisStreamsBackend", "TransportBackend",
)

# src/navigator_eventbus/__init__.py — current __all__ (line 32-46):
# Does NOT currently export backend classes directly.
# CompositeBackend should be added here.
```

### Does NOT Exist
- ~~`navigator_eventbus.backends.composite`~~ — does not exist yet (this task creates it)
- ~~`navigator_eventbus.Channel`~~ — does not exist yet
- ~~`RedisStreamsBackend.add_group()`~~ — no such method; groups are fixed at construction
- ~~`BusCore(backends=[...])`~~ — BusCore accepts a single `backend=`, not a list
- ~~`TransportBackend.start_consumers()`~~ — no plural form exists
- ~~`RedisStreamsBackend._delivery` setter~~ — `_delivery` is set in `__init__` and never mutated

---

## Implementation Notes

### Pattern to Follow

Use **delegation, not inheritance**: `CompositeBackend` creates `RedisStreamsBackend`
instances internally — it does not subclass it.

```python
# Shared client injection pattern (verified: redis_streams.py:238-239, 410-411)
# When client= is provided, RedisStreamsBackend stores it as self._client
# and uses it directly instead of creating its own connection.
# On close(), it only closes connections it created itself (line 384).

# CompositeBackend creates one Redis client and injects it into all internal backends:
self._redis = await aioredis.from_url(redis_url, decode_responses=True)
backend = RedisStreamsBackend(client=self._redis, delivery=channel.delivery, ...)
```

### Key Constraints

1. **Channel dataclass** (frozen=True):
   - Validate at `__post_init__`: `delivery="broadcast"` + `max_deliveries` → `ValueError`
   - `on_envelope` is required (a channel without a consumer is meaningless)
   - Optional `codec` override per channel for forward-compatibility

2. **CompositeBackend**:
   - `publish()`: delegates to the ONE channel designated by `publish_via` (default: first channel)
   - `start_consumer(on_envelope)`: the `on_envelope` from BusCore goes to the broadcast
     channel; group channels use their own `channel.on_envelope`
   - `close()`: stop all internal backends FIRST, then close the shared Redis client
   - Per-group dedup: override `dedup_prefix=` per group-mode backend to
     `evb:events:dedup:<group>:` so the same `event_id` can be processed by both groups

3. **Re-exports**: Add `CompositeBackend` and `Channel` to `backends/__init__.py` `__all__`
   and to `__init__.py` `__all__`.

### References in Codebase
- `src/navigator_eventbus/backends/redis_streams.py` — the delegate backend
- `src/navigator_eventbus/backends/base.py` — TransportBackend protocol to satisfy
- `src/navigator_eventbus/backends/__init__.py` — re-export target
- `src/navigator_eventbus/__init__.py` — top-level re-export target

---

## Acceptance Criteria

- [ ] `Channel` dataclass with `__post_init__` validation (broadcast + max_deliveries → ValueError)
- [ ] `CompositeBackend` satisfies `TransportBackend` protocol (`isinstance(composite, TransportBackend)` is `True`)
- [ ] `CompositeBackend.__init__` creates N `RedisStreamsBackend` instances with shared `client=`
- [ ] `publish()` delegates to the `publish_via` channel's backend only
- [ ] `start_consumer(on_envelope)` starts all N internal backends, wiring BusCore's callback to broadcast channel
- [ ] `close()` stops all internal backends, then closes the shared Redis client
- [ ] Per-group dedup prefix: `evb:events:dedup:<group>:<event_id>`
- [ ] Re-exports work: `from navigator_eventbus.backends import CompositeBackend, Channel`
- [ ] Re-exports work: `from navigator_eventbus import CompositeBackend`
- [ ] No linting errors: `ruff check src/navigator_eventbus/backends/composite.py`
- [ ] No changes to `TransportBackend`, `BusCore`, or `RedisStreamsBackend`

---

## Test Specification

> Tests are covered by TASK-1849 and TASK-1850. This task should verify
> basic import correctness only:

```python
# Smoke test — verify imports work
from navigator_eventbus.backends.composite import Channel, CompositeBackend
from navigator_eventbus.backends import CompositeBackend, Channel
from navigator_eventbus import CompositeBackend

assert isinstance(CompositeBackend, type)
```

---

## Agent Instructions

When you pick up this task:

1. **Read the spec** at `sdd/specs/eventbus-composite-backend.spec.md` for full context
2. **Check dependencies** — this task has none
3. **Verify the Codebase Contract** — before writing ANY code:
   - Confirm every import in "Verified Imports" still exists (`grep` or `read` the source)
   - Confirm every class/method in "Existing Signatures" still has the listed attributes
   - If anything has changed, update the contract FIRST, then implement
   - **NEVER** reference an import, attribute, or method not in the contract without verifying it exists
4. **Update status** in `sdd/tasks/index/eventbus-composite-backend.json` → `"in-progress"`
5. **Implement** following the scope, codebase contract, and notes above
6. **Verify** all acceptance criteria are met
7. **Move this file** to `sdd/tasks/completed/TASK-1848-composite-backend-core.md`
8. **Update index** → `"done"`
9. **Fill in the Completion Note** below

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**: <session or agent ID>
**Date**: YYYY-MM-DD
**Notes**: What was implemented, any deviations from scope, issues encountered.

**Deviations from spec**: none | describe if any
