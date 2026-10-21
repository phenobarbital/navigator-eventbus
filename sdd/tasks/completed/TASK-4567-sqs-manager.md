# TASK-4567: `SQSManager` — AWS SQS producer + consumer over one connection

**Feature**: FEAT-431 — Unified Broker Manager + brokers hardening
**Spec**: `sdd/specs/brokers-unified-manager.spec.md`
**Status**: pending
**Priority**: medium
**Estimated effort**: S (< 2h)
**Depends-on**: TASK-4564
**Assigned-to**: unassigned

---

## Context

Implements the SQS concrete manager of spec §3 **Module 3**. Q2 is resolved:
all three managers ship this cycle. `SQSManager(BrokerManager, SQSConsumer)`
linearizes like the other managers (verified by execution while writing this task):

```
SQSManager → BrokerManager → BrokerProducer → SQSConsumer → SQSConnection
           → BaseConnection → BrokerConsumer → ABC → object
```

Unlike Redis, `SQSConnection` does not overwrite `_name_`, so the declared
`"sqs_manager"` survives init (checked). Teardown follows the same contract as Redis.
`BrokerProducer.stop()` bypasses `SQSConsumer.stop()`, and `SQSConsumer.subscribe_to_events`
keeps only one `self.consumer_task`, so the manager tracks every consumer task itself.

---

## Scope

- Create `src/navigator_eventbus/brokers/sqs/manager.py` with `SQSManager(BrokerManager, SQSConsumer)`, `_name_ = "sqs_manager"`.
- In `__init__`: `kwargs.setdefault("broker_service", "sqs")`, then init `self._consumer_tasks: list[asyncio.Task] = []` after `super().__init__`.
- Override `subscribe_to_events(...)` (same signature as `SQSConsumer.subscribe_to_events`) to record `self.consumer_task`.
- `_subscribe(app)`: `subscribe_to_events(queue_name=self._queue_name, callback=self._callback_)` (as `SQSConsumer.start`, `sqs/consumer.py:94-96`).
- `_stop_consuming()`: cancel and await every recorded task, clear, set `self.consumer_task = None`.
- Re-export from `navigator_eventbus.brokers.sqs`.
- Write `tests/brokers/test_sqs_manager.py` (unit; no AWS).

**NOT in scope**: `SQSConnection`/`SQSConsumer`/`SQSProducer` changes (the SQS single-wrap fix is TASK-4562); live AWS tests.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/brokers/sqs/manager.py` | CREATE | `SQSManager` |
| `src/navigator_eventbus/brokers/sqs/__init__.py` | MODIFY | Re-export `SQSManager` |
| `tests/brokers/test_sqs_manager.py` | CREATE | MRO, registration, teardown-order tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.brokers.manager import BrokerManager          # created by TASK-4564
from navigator_eventbus.brokers.sqs.consumer import SQSConsumer       # verified: brokers/sqs/consumer.py:18
from navigator_eventbus.brokers.sqs import SQSConnection, SQSConsumer # verified: brokers/sqs/__init__.py:2-3
from navigator_eventbus.brokers import BaseConnection, BrokerConsumer, BrokerProducer  # verified: brokers/__init__.py:14-16
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/brokers/sqs/connection.py
class SQSConnection(BaseConnection):
    def __init__(self, credentials=None, timeout=5, **kwargs)        # line 35 — defaults credentials from navconfig AWS_*; self.consumer_task = None (line 60)
    async def connect(self) -> None                                  # line 62 — aioboto3; patch in tests
    async def disconnect(self) -> None                               # line 78
    async def consume_messages(self, queue_name, callback, max_messages=10, wait_time=10, idle_sleep=5)  # line 254 — infinite poll loop

# src/navigator_eventbus/brokers/sqs/consumer.py
class SQSConsumer(SQSConnection, BrokerConsumer):                    # line 18
    _name_: str = "sqs_consumer"                                     # line 21
    async def subscribe_to_events(self, queue_name, callback, max_messages=10,
                                  wait_time=10, idle_sleep=5, **kwargs)  # line 64 — ensure_connection(); self.consumer_task = create_task(consume_messages(...))
    async def stop_consumer(self) -> None                            # line 103 — single task only

# src/navigator_eventbus/brokers/manager.py (TASK-4564)
class BrokerManager(BrokerProducer, ABC): _subscribe(app) / _stop_consuming() abstract.
```
`SQSConnection.ensure_connection` is the inherited `BaseConnection.ensure_connection` (`brokers/connection.py:92`).
Re-verify that it calls `connect()` before relying on the patch in tests.

### Does NOT Exist
- ~~`SQSManager`~~ — created here.
- ~~`SQSConsumer` tracking multiple consumer tasks~~ — single `consumer_task`; the manager adds `_consumer_tasks`.
- ~~Re-exporting `SQSManager` from `navigator_eventbus.brokers`~~ — forbidden (`aioboto3` is optional).

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/brokers/sqs/manager.py", "action": "CREATE"},
    {"path": "src/navigator_eventbus/brokers/sqs/__init__.py", "action": "MODIFY"},
    {"path": "tests/brokers/test_sqs_manager.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/brokers/sqs/consumer.py#SQSConsumer",
    "sym:src/navigator_eventbus/brokers/sqs/consumer.py#SQSConsumer.subscribe_to_events",
    "sym:src/navigator_eventbus/brokers/sqs/connection.py#SQSConnection"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- Mirror TASK-4566 (`RedisManager`) structurally. The two run in parallel and share no files, so copy
  the pattern from this blueprint, not from the other worktree.
- No `_name_` restoration is needed (`SQSConnection` does not clobber it). Still assert the literal key in tests.
- `setdefault("broker_service", "sqs")` → default route `/api/v1/broker/sqs/publish_event`.

---

## Implementation Blueprint

### Steps (in order)
1. Create `sqs/manager.py` — *why*: Module 3 concrete manager.
2. Re-export it — *why*: AC "re-exported from their subpackages".
3. Write the tests; run them.

### `src/navigator_eventbus/brokers/sqs/manager.py` (CREATE)
```python
"""SQSManager — AWS SQS producer + consumer over one connection (FEAT-431 Module 3)."""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Union

from aiohttp import web

from ..manager import BrokerManager
from .consumer import SQSConsumer


class SQSManager(BrokerManager, SQSConsumer):
    """Unified SQS publisher/consumer sharing one connection."""

    _name_: str = "sqs_manager"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("broker_service", "sqs")
        super().__init__(*args, **kwargs)
        self._consumer_tasks: list[asyncio.Task] = []

    async def subscribe_to_events(
        self,
        queue_name: str,
        callback: Union[Callable, Awaitable],
        max_messages: int = 10,
        wait_time: int = 10,
        idle_sleep: int = 5,
        **kwargs: Any,
    ) -> None:
        """Start a polling consumer task (see :class:`SQSConsumer`) and track it for shutdown."""
        await super().subscribe_to_events(
            queue_name, callback, max_messages=max_messages,
            wait_time=wait_time, idle_sleep=idle_sleep, **kwargs,
        )
        if self.consumer_task is not None:
            self._consumer_tasks.append(self.consumer_task)

    async def _subscribe(self, app: web.Application) -> None:
        """Subscribe to the configured queue with the configured callback."""
        await self.subscribe_to_events(queue_name=self._queue_name, callback=self._callback_)

    async def _stop_consuming(self) -> None:
        """Cancel and await every consumer task; tolerate none."""
        tasks, self._consumer_tasks = self._consumer_tasks, []
        # FILL IN: cancel each task, `await asyncio.gather(*tasks, return_exceptions=True)`,
        #          set self.consumer_task = None — bounded by AC "no consumer tasks remain"
```

### `src/navigator_eventbus/brokers/sqs/__init__.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c 'from .producer import SQSProducer' src/navigator_eventbus/brokers/sqs/__init__.py)
# AFTER — insert below `from .producer import SQSProducer` (verified: sqs/__init__.py:4)
from .manager import SQSManager
# and change line 6 to: __all__ = ["SQSConnection", "SQSConsumer", "SQSProducer", "SQSManager"]
```

### `tests/brokers/test_sqs_manager.py` (CREATE)
```python
"""SQSManager unit tests (FEAT-431 Module 3, TASK-4567)."""
from __future__ import annotations

import asyncio

import pytest
from aiohttp import web

from navigator_eventbus.brokers import BaseConnection, BrokerConsumer, BrokerProducer
from navigator_eventbus.brokers.manager import BrokerManager
from navigator_eventbus.brokers.sqs import SQSConnection, SQSConsumer, SQSManager

CREDS = {"region_name": "us-east-1"}


@pytest.fixture
def patched(monkeypatch):
    events: list[str] = []

    async def fake_connect(self):
        self._connection = object()

    async def fake_disconnect(self):
        events.append("disconnect")

    async def fake_consume(self, queue_name, callback, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append(f"consumer_cancelled:{queue_name}")
            raise

    monkeypatch.setattr(SQSConnection, "connect", fake_connect)
    monkeypatch.setattr(SQSConnection, "disconnect", fake_disconnect)
    monkeypatch.setattr(SQSConnection, "consume_messages", fake_consume)
    return events


def test_manager_mro_sqs():
    assert SQSManager.__mro__[:7] == (SQSManager, BrokerManager, BrokerProducer, SQSConsumer,
                                      SQSConnection, BaseConnection, BrokerConsumer)


def test_sqs_manager_setup_registers_name():
    app = web.Application()
    SQSManager(credentials=CREDS, queue_name="q").setup(app)
    # FILL IN: "sqs_manager" in app; POST /api/v1/broker/sqs/publish_event registered


async def test_manager_stop_consumers_first(patched):
    m = SQSManager(credentials=CREDS, queue_name="q", num_workers=1)
    await m.start(web.Application())
    await asyncio.sleep(0)
    original_join = m.event_queue.join

    async def recording_join():
        patched.append("drain")
        await original_join()

    m.event_queue.join = recording_join
    await m.stop(None)
    assert patched[-3:] == ["consumer_cancelled:q", "drain", "disconnect"]
    # FILL IN: m._consumer_tasks == [] and m.consumer_task is None


async def test_manager_stop_manual_or_no_subscription(patched):
    # FILL IN: (a) auto_subscribe=False, no subscription → clean stop, no "consumer_cancelled";
    #          (b) auto_subscribe=False + explicit subscribe_to_events("q1"/"q2", cb) → both cancelled
    ...
```

### FILL IN checklist
- [ ] `_stop_consuming` cancel + gather.
- [ ] Each `# FILL IN` test body — spec §4 rows `test_manager_mro_sqs`, `test_manager_stop_consumers_first`,
      `test_manager_stop_manual_or_no_subscription`.

---

## Acceptance Criteria

- [ ] `from navigator_eventbus.brokers.sqs import SQSManager` works.
- [ ] MRO is exactly `SQSManager → BrokerManager → BrokerProducer → SQSConsumer → SQSConnection → BaseConnection → BrokerConsumer`.
- [ ] Shutdown cancels and awaits every consumer task (auto and manual) before draining and disconnecting.
- [ ] `setup()` registers `app["sqs_manager"]`.
- [ ] Existing `tests/brokers/test_sqs.py` passes.

---

## Validation Commands
- `pytest tests/brokers/test_sqs_manager.py -q`
- `pytest tests/brokers/test_sqs.py -q`

---

## Test Specification

See the blueprint test file.

---

## Agent Instructions

1. **Work in the feature worktree** — never on `dev`
   (`python -m scripts.sdd.ensure_worktree --slug brokers-unified-manager --feature-id FEAT-431`).
2. **Check dependencies**: TASK-4564 must be `done`.
3. **Environment**: `source .venv/bin/activate`; if `pytest`/`aioboto3` are missing run
   `uv pip install -e ".[dev,rabbitmq,sqs,redis]"` (no manifest change).
4. Verify the Codebase Contract; set status `in-progress`, commit only the index.
5. Implement from the blueprint; complete every `# FILL IN:`; run the Validation Commands; stage only listed files.
6. Close with `scripts/sdd/close_task.sh TASK-4567 brokers-unified-manager verified`, fill the Completion Note, commit.

---

## Completion Note

**Completed by**: sonnet (native)
**Date**: 2026-10-21
**Notes**: Merged. Verified locally with PYTHONPATH=worktree/src: only the 2 pre-existing navconfig-env failures in tests/brokers.

**Deviations from spec**: none
