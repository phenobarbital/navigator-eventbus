# TASK-4566: `RedisManager` — Redis Streams producer + consumer over one connection

**Feature**: FEAT-431 — Unified Broker Manager + brokers hardening
**Spec**: `sdd/specs/brokers-unified-manager.spec.md`
**Status**: pending
**Priority**: medium
**Estimated effort**: S (< 2h)
**Depends-on**: TASK-4564
**Assigned-to**: unassigned

---

## Context

Implements the Redis concrete manager of spec §3 **Module 3**. Q2 is resolved:
all three managers ship this cycle. `RedisManager(BrokerManager, RedisConsumer)`
linearizes the same way as the RabbitMQ manager (spec §2, verified by execution).

There are two Redis-specific contracts (spec §2 "Shutdown and registration contracts"):

1. `RedisConnection.__init__` assigns `self._name_ = self.__class__.__name__`
   (`redis/connection.py:64`), so without a fix the manager registers as `app["RedisManager"]`.
   `RedisManager` must **restore `"redis_manager"`** after cooperative init. Existing
   `RedisProducer`/`RedisConsumer` registration behavior stays unchanged.
2. `BrokerProducer.stop()` does not chain to `RedisConsumer.stop()`, so the manager's
   `_stop_consuming()` must cancel and await **every** consumer task, including
   subscriptions made explicitly after startup with `auto_subscribe=False`.
   `RedisConsumer.subscribe_to_events` overwrites the single `self.consumer_task`, so
   the manager keeps its own list.

---

## Scope

- Create `src/navigator_eventbus/brokers/redis/manager.py` with `RedisManager(BrokerManager, RedisConsumer)`, `_name_ = "redis_manager"`.
- In `__init__`: `kwargs.setdefault("broker_service", "redis")` before `super().__init__`, then
  restore `self._name_ = type(self)._name_` and init `self._consumer_tasks: list[asyncio.Task] = []`.
- Override `subscribe_to_events(...)` to call the parent, then record `self.consumer_task` in `_consumer_tasks`.
- `_subscribe(app)`: `subscribe_to_events(queue_name=self._queue_name, callback=self._callback_)` (same as `RedisConsumer.start`, `redis/consumer.py:133-135`).
- `_stop_consuming()`: cancel all recorded tasks, await them (swallow `CancelledError`), clear the list, set `self.consumer_task = None`.
- Re-export from `navigator_eventbus.brokers.redis`.
- Write `tests/brokers/test_redis_manager.py` (unit; no live Redis).

**NOT in scope**: `RedisConnection`/`RedisConsumer`/`RedisProducer` changes; live-Redis round trip (TASK-4568).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/brokers/redis/manager.py` | CREATE | `RedisManager` |
| `src/navigator_eventbus/brokers/redis/__init__.py` | MODIFY | Re-export `RedisManager` |
| `tests/brokers/test_redis_manager.py` | CREATE | MRO, registration key, teardown-order tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.brokers.manager import BrokerManager              # created by TASK-4564
from navigator_eventbus.brokers.redis.consumer import RedisConsumer       # verified: brokers/redis/consumer.py:23
from navigator_eventbus.brokers.redis import RedisConnection, RedisConsumer  # verified: brokers/redis/__init__.py:2-3
from navigator_eventbus.brokers import BaseConnection, BrokerConsumer, BrokerProducer  # verified: brokers/__init__.py:14-16
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/brokers/redis/connection.py
class RedisConnection(BaseConnection):
    def __init__(self, credentials=None, timeout=5, consumer_group=True, **kwargs)  # line 44
    #   self._name_ = self.__class__.__name__                        # line 64  ← clobbers the registration key
    async def connect(self) -> None                                  # line 80
    async def ensure_connection(self) -> None                        # line 101 — connects only when _connection is None
    async def disconnect(self) -> None                               # line 106
    async def consume_messages(self, queue_name, callback, count=1, block=1000, **kwargs)  # line 233

# src/navigator_eventbus/brokers/redis/consumer.py
class RedisConsumer(RedisConnection, BrokerConsumer):                # line 23
    #   pops queue_name/group_name/consumer_name from kwargs and re-forwards them  # lines 47-58
    #   self.consumer_task: Optional[asyncio.Task] = None            # line 60
    async def subscribe_to_events(self, queue_name, callback, **kwargs)  # line 101 — ensure_connection(); self.consumer_task = create_task(consume_messages(...))
    async def stop_consumer(self) -> None                            # line 119 — single task only
    async def start(self, app) / stop(self, app)                     # lines 130 / 137 — NOT reached in the manager MRO

# src/navigator_eventbus/brokers/manager.py (TASK-4564)
class BrokerManager(BrokerProducer, ABC): _subscribe(app) / _stop_consuming() abstract; start()/stop() fixed.
# src/navigator_eventbus/brokers/producer.py
    #   self._broker_service = kwargs.get("broker_service", "rabbitmq")   # line 65 — hence setdefault("redis")
```

### Does NOT Exist
- ~~`RedisManager`~~ — created here.
- ~~`RedisConsumer` tracking multiple consumer tasks~~ — it keeps one `consumer_task`; the manager adds `_consumer_tasks`.
- ~~`RedisConnection` respecting a subclass `_name_`~~ — it overwrites it (line 64); do not "fix" `RedisConnection` here.
- ~~Re-exporting `RedisManager` from `navigator_eventbus.brokers`~~ — forbidden (lazy optional deps, spec §7).

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/brokers/redis/manager.py", "action": "CREATE"},
    {"path": "src/navigator_eventbus/brokers/redis/__init__.py", "action": "MODIFY"},
    {"path": "tests/brokers/test_redis_manager.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/brokers/redis/consumer.py#RedisConsumer",
    "sym:src/navigator_eventbus/brokers/redis/consumer.py#RedisConsumer.subscribe_to_events",
    "sym:src/navigator_eventbus/brokers/redis/connection.py#RedisConnection"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- Restore `_name_` **after** `super().__init__()`, because `RedisConnection.__init__` runs inside that call.
- `type(self)._name_` reads the class attribute, ignoring the instance attribute, so subclasses of
  `RedisManager` with their own `_name_` keep working.
- `setdefault("broker_service", "redis")` makes the default publish route
  `/api/v1/broker/redis/publish_event` instead of colliding with a RabbitMQ route on the same app.
  An explicit `broker_service` still wins.

---

## Implementation Blueprint

### Steps (in order)
1. Create `redis/manager.py` — *why*: Module 3 concrete manager.
2. Re-export it — *why*: AC "re-exported from their subpackages".
3. Write the tests; run them.

### `src/navigator_eventbus/brokers/redis/manager.py` (CREATE)
```python
"""RedisManager — Redis Streams producer + consumer over one connection (FEAT-431 Module 3)."""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional, Union

from aiohttp import web

from ..manager import BrokerManager
from .consumer import RedisConsumer


class RedisManager(BrokerManager, RedisConsumer):
    """Unified Redis Streams publisher/consumer sharing one connection."""

    _name_: str = "redis_manager"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("broker_service", "redis")
        super().__init__(*args, **kwargs)
        # RedisConnection.__init__ sets self._name_ = class name; restore the
        # declared registration key so setup() registers app["redis_manager"].
        self._name_ = type(self)._name_
        self._consumer_tasks: list[asyncio.Task] = []

    async def subscribe_to_events(
        self,
        queue_name: Optional[str],
        callback: Union[Callable, Awaitable],
        **kwargs: Any,
    ) -> None:
        """Start a consumer task (see :class:`RedisConsumer`) and track it for shutdown."""
        await super().subscribe_to_events(queue_name, callback, **kwargs)
        if self.consumer_task is not None:
            self._consumer_tasks.append(self.consumer_task)

    async def _subscribe(self, app: web.Application) -> None:
        """Subscribe to the configured stream with the configured callback."""
        await self.subscribe_to_events(queue_name=self._queue_name, callback=self._callback_)

    async def _stop_consuming(self) -> None:
        """Cancel and await every consumer task; tolerate none."""
        tasks, self._consumer_tasks = self._consumer_tasks, []
        # FILL IN: cancel each task, then `await asyncio.gather(*tasks, return_exceptions=True)`;
        #          set self.consumer_task = None — bounded by AC "no consumer tasks remain"
```

### `src/navigator_eventbus/brokers/redis/__init__.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c 'from .producer import RedisProducer' src/navigator_eventbus/brokers/redis/__init__.py)
# AFTER — insert below `from .producer import RedisProducer` (verified: redis/__init__.py:4)
from .manager import RedisManager
# and change line 6 to: __all__ = ["RedisConnection", "RedisConsumer", "RedisProducer", "RedisManager"]
```

### `tests/brokers/test_redis_manager.py` (CREATE)
```python
"""RedisManager unit tests (FEAT-431 Module 3, TASK-4566)."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from navigator_eventbus.brokers import BaseConnection, BrokerConsumer, BrokerProducer
from navigator_eventbus.brokers.manager import BrokerManager
from navigator_eventbus.brokers.redis import RedisConnection, RedisConsumer, RedisManager


@pytest.fixture
def patched(monkeypatch):
    """Fake connect/disconnect and a never-ending consume loop that logs its cancellation."""
    events: list[str] = []

    async def fake_connect(self):
        self._connection = SimpleNamespace(close=AsyncMock())

    async def fake_disconnect(self):
        events.append("disconnect")

    async def fake_consume(self, queue_name, callback, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append(f"consumer_cancelled:{queue_name}")
            raise

    async def fake_publish(self, body, queue_name=None, **kwargs):
        events.append("publish")

    monkeypatch.setattr(RedisConnection, "connect", fake_connect)
    monkeypatch.setattr(RedisConnection, "disconnect", fake_disconnect)
    monkeypatch.setattr(RedisConnection, "consume_messages", fake_consume)
    monkeypatch.setattr(RedisConnection, "publish_message", fake_publish)
    return events


def test_manager_mro_redis():
    assert RedisManager.__mro__[:7] == (RedisManager, BrokerManager, BrokerProducer, RedisConsumer,
                                        RedisConnection, BaseConnection, BrokerConsumer)


def test_redis_manager_registration_key():
    """Real cooperative init keeps "redis_manager"; setup registers that literal key."""
    m = RedisManager(queue_name="s")
    assert m._name_ == "redis_manager"
    app = web.Application()
    m.setup(app)
    # FILL IN: "redis_manager" in app and "RedisManager" not in app;
    #          POST /api/v1/broker/redis/publish_event registered


def test_redis_producer_consumer_registration_unchanged():
    # FILL IN: RedisProducer()._name_ / RedisConsumer()._name_ equal their class names (existing behaviour)
    ...


async def test_manager_stop_consumers_first(patched):
    """Consumer tasks are cancelled and awaited before drain and disconnect; none remain."""
    m = RedisManager(queue_name="s", num_workers=1)
    await m.start(web.Application())
    await asyncio.sleep(0)
    # Record the moment the outgoing drain starts — deterministic, unlike racing a worker publish
    # against the consumer-task cancellation (both are scheduled on the same loop tick).
    original_join = m.event_queue.join

    async def recording_join():
        patched.append("drain")
        await original_join()

    m.event_queue.join = recording_join
    await m.stop(None)
    assert patched[-3:] == ["consumer_cancelled:s", "drain", "disconnect"]
    # FILL IN: m._consumer_tasks == [] and m.consumer_task is None


async def test_manager_stop_manual_or_no_subscription(patched):
    # FILL IN: (a) auto_subscribe=False, no subscription → stop() ok, no "consumer_cancelled";
    #          (b) auto_subscribe=False + two explicit subscribe_to_events("s1"/"s2", cb) → both cancelled
    ...
```

### FILL IN checklist
- [ ] `_stop_consuming` cancel + gather.
- [ ] Each `# FILL IN` test body — spec §4 rows `test_manager_mro_redis`, `test_redis_manager_registration_key`,
      `test_manager_stop_consumers_first`, `test_manager_stop_manual_or_no_subscription`.

---

## Acceptance Criteria

- [ ] `from navigator_eventbus.brokers.redis import RedisManager` works.
- [ ] Redis manager setup registers exactly `app["redis_manager"]` after real backend initialization.
- [ ] Shutdown cancels and awaits every consumer task (auto and manual) before draining and disconnecting; none remain.
- [ ] `RedisProducer`/`RedisConsumer` registration is unchanged.
- [ ] Existing `tests/brokers/test_redis_consumer.py` passes.

---

## Validation Commands
- `pytest tests/brokers/test_redis_manager.py -q`
- `pytest tests/brokers/test_redis_consumer.py -q`

---

## Test Specification

See the blueprint test file.

---

## Agent Instructions

1. **Work in the feature worktree** — never on `dev`
   (`python -m scripts.sdd.ensure_worktree --slug brokers-unified-manager --feature-id FEAT-431`).
2. **Check dependencies**: TASK-4564 must be `done`.
3. **Environment**: `source .venv/bin/activate`; if `pytest` is missing run
   `uv pip install -e ".[dev,rabbitmq,sqs,redis]"` (no manifest change).
4. Verify the Codebase Contract; set status `in-progress`, commit only the index.
5. Implement from the blueprint; complete every `# FILL IN:`; run the Validation Commands; stage only listed files.
6. Close with `scripts/sdd/close_task.sh TASK-4566 brokers-unified-manager verified`, fill the Completion Note, commit.

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**:
**Date**:
**Notes**:

**Deviations from spec**: none
