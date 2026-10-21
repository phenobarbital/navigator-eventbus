# TASK-4564: `BrokerManager` — generic unified producer/consumer mixin

**Feature**: FEAT-431 — Unified Broker Manager + brokers hardening
**Spec**: `sdd/specs/brokers-unified-manager.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Implements the generic half of spec §3 **Module 3**. `BrokerManager` is a thin
mixin over `BrokerProducer` that overrides the aiohttp `on_startup`/`on_shutdown`
handlers so that **one** connection serves both roles:

- `start()`: connect once, start the producer's queue workers, then (unless
  `auto_subscribe=False`) call the abstract `_subscribe(app)`.
- `stop()`: await the abstract `_stop_consuming()` **first**, then delegate to
  `BrokerProducer.stop()`, which drains the outgoing queue, cancels and awaits
  the workers, and disconnects once.

Concrete managers combine it with a backend Consumer: `(BrokerManager, XConsumer)`
in TASK-4565/4566/4567. The base order is load-bearing (spec §2): `BrokerProducer`
first, `BrokerConsumer` last.

`auto_subscribe` must be an explicit flag. `_queue_name` and `_callback_` cannot
act as sentinels because `BrokerConsumer.__init__` runs last and always assigns
them (D2, spec §2).

---

## Scope

- Create `src/navigator_eventbus/brokers/manager.py` with `BrokerManager(BrokerProducer, ABC)`.
- Re-export `BrokerManager` from `navigator_eventbus.brokers` (top level; this keeps the
  concrete subpackages lazy).
- Write `tests/brokers/test_manager.py` with generic tests using an in-test dummy manager.

**NOT in scope**: `RMQManager` / `RedisManager` / `SQSManager` (TASK-4565/4566/4567);
exact-MRO tests per backend (those tasks); any change to `BrokerProducer`.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/brokers/manager.py` | CREATE | `BrokerManager` mixin |
| `src/navigator_eventbus/brokers/__init__.py` | MODIFY | Re-export `BrokerManager` |
| `tests/brokers/test_manager.py` | CREATE | Generic lifecycle tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.brokers.producer import BrokerProducer   # verified: brokers/producer.py:30
from aiohttp import web                                            # used by producer.py:23
from navconfig.logging import logging                              # verified: brokers/producer.py:24
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/brokers/producer.py
class BrokerProducer(BaseConnection, ABC):                         # line 30
    def __init__(self, credentials=None, queue_size=None, num_workers=4, timeout=5,
                 *, auth_callable=None, **kwargs)                  # line 48 — forwards **kwargs via super().__init__(credentials=, timeout=, **kwargs) (line 76)
    #   self.event_queue: asyncio.Queue                            # line 62
    #   self._workers: list                                        # line 64
    def setup(self, app=None) -> None                              # line 78 — appends self.start / self.stop to on_startup / on_shutdown; app[self._name_] = self
    async def start_workers(self) -> None                          # line 103
    async def start(self, app) -> None                             # line 109 — connect() + start_workers(); does NOT call super()
    async def stop(self, app) -> None                              # line 118 — event_queue.join(); cancel+gather workers; disconnect(); does NOT call super()
    async def _event_broker(self, worker_id) -> None               # line 261 — worker: publish_message(body=, queue_name=, routing_key=, **event)

# src/navigator_eventbus/brokers/connection.py
class BaseConnection(ABC):                                         # line 21
    #   abstract: connect (83), disconnect (88), publish_message (98), consume_messages (108)
    #   process_message raises NotImplementedError (118)

# src/navigator_eventbus/brokers/__init__.py
from .producer import BrokerProducer                               # line 16
__all__ = ["BaseConnection", "BrokerConsumer", "BrokerProducer", "BaseWrapper", "DataSerializer"]  # lines 20-26
```
The test pattern to copy is `tests/brokers/test_producer.py:8` (`DummyProducer`, with no-op
`connect`/`disconnect`/`publish_message`/`consume_messages`/`process_message`).

### Does NOT Exist
- ~~`BrokerManager`~~ — created here.
- ~~`BrokerProducer` calling `super().start()` / `super().stop()`~~ — it does not; that is why the manager must call `_stop_consuming()` itself.
- ~~`_queue_name` / `_callback_` as "was it supplied?" sentinels~~ — unusable (D2). Use `self._auto_subscribe`.
- ~~`BrokerConsumer` calling `super().__init__()`~~ — it does not, by design.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/brokers/manager.py", "action": "CREATE"},
    {"path": "src/navigator_eventbus/brokers/__init__.py", "action": "MODIFY"},
    {"path": "tests/brokers/test_manager.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/brokers/producer.py#BrokerProducer",
    "sym:src/navigator_eventbus/brokers/producer.py#BrokerProducer.start_workers",
    "sym:src/navigator_eventbus/brokers/producer.py#BrokerProducer.stop",
    "sym:src/navigator_eventbus/brokers/producer.py#BrokerProducer.setup"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- `start()` must **not** call `super().start(app)`. In concrete managers that would resolve
  to `BrokerProducer.start` (fine), but a future reorder could reach a Consumer's `start()`,
  which auto-subscribes regardless of the flag. Call `connect()` and `start_workers()` explicitly.
- `stop()` must run producer teardown even if `_stop_consuming()` raises. Log the error with
  `self.logger.error(...)` and continue, because leaking the connection and worker tasks is
  worse than a noisy shutdown.
- `auto_subscribe` is keyword-only and consumed **before** `super().__init__`, so it never
  reaches the backend `__init__` chain.
- `from __future__ import annotations`; Google-style docstrings; the module docstring names FEAT-431.

---

## Implementation Blueprint

### Steps (in order)
1. Create `manager.py` from the block below — *why*: fixes the public interface from spec §2.
2. Add the re-export in `brokers/__init__.py` — *why*: AC "BrokerManager is re-exported from `navigator_eventbus.brokers`"; `manager.py` imports only base modules, so laziness is preserved.
3. Write the generic tests with an in-file `DummyManager`, then run them.

### `src/navigator_eventbus/brokers/manager.py` (CREATE)
```python
"""BrokerManager — unified producer + consumer over one connection (FEAT-431 Module 3).

Extends :class:`~navigator_eventbus.brokers.producer.BrokerProducer` with a
lifecycle that performs both halves against a single broker connection.
Concrete managers combine it with a backend consumer, e.g.
``class RMQManager(BrokerManager, RMQConsumer)``. The base order is
load-bearing: ``BrokerProducer`` first, ``BrokerConsumer`` last in the MRO
(``BrokerConsumer.__init__`` terminates the cooperative ``__init__`` chain).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from aiohttp import web
from navconfig.logging import logging

from .producer import BrokerProducer


class BrokerManager(BrokerProducer, ABC):
    """Unifies producer and consumer roles over a single broker connection.

    Args:
        *args: Forwarded to :class:`BrokerProducer`.
        auto_subscribe: When ``True`` (default), :meth:`start` subscribes the
            consumer side after connecting. Set ``False`` to connect and start
            the publish workers only, and subscribe explicitly later (needed
            when the consumer callback does not exist yet at construction).
        **kwargs: Forwarded through the cooperative ``__init__`` chain
            (producer and backend consumer options).
    """

    _name_: str = "broker_manager"

    def __init__(self, *args: Any, auto_subscribe: bool = True, **kwargs: Any) -> None:
        self._auto_subscribe: bool = auto_subscribe
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(f"Broker.Manager.{type(self).__name__}")

    async def start(self, app: web.Application) -> None:
        """``on_startup`` handler — one connect, publish workers, then subscribe.

        Args:
            app: The aiohttp application.
        """
        await self.connect()
        await self.start_workers()
        if self._auto_subscribe:
            await self._subscribe(app)
        else:
            self.logger.debug("auto_subscribe=False: consumer side not subscribed.")

    async def stop(self, app: web.Application) -> None:
        """``on_shutdown`` handler — stop consuming, drain, cancel workers, disconnect.

        Args:
            app: The aiohttp application.
        """
        try:
            await self._stop_consuming()
        except Exception as err:  # pylint: disable=broad-except
            self.logger.error(f"Error stopping consumer side: {err}")
        # BrokerProducer.stop: event_queue.join() → cancel/gather workers → disconnect()
        await super().stop(app)

    @abstractmethod
    async def _subscribe(self, app: web.Application) -> None:
        """Subscribe the consumer side using the manager's configured options."""

    @abstractmethod
    async def _stop_consuming(self) -> None:
        """Stop all consumer activity (including manual subscriptions); must tolerate none."""
```
**Why this shape**: the method set and signatures are fixed by spec §2 "New Public Interfaces".
Calling `super().stop(app)` resolves to `BrokerProducer.stop` in every concrete MRO, which is
exactly the drain → cancel → disconnect sequence.

### `src/navigator_eventbus/brokers/__init__.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c 'from .producer import BrokerProducer' src/navigator_eventbus/brokers/__init__.py)
# AFTER — insert below `from .producer import BrokerProducer` (verified: __init__.py:16)
from .manager import BrokerManager
# and add "BrokerManager", to __all__ (after "BrokerProducer", line 23)
```
Also mention `BrokerManager` in the module docstring's list of base abstractions.

### `tests/brokers/test_manager.py` (CREATE)
```python
"""Generic BrokerManager lifecycle tests (FEAT-431 Module 3, TASK-4564)."""
from __future__ import annotations

import pytest
from aiohttp import web

from navigator_eventbus.brokers import BrokerManager


class DummyManager(BrokerManager):
    _name_ = "dummy_manager"

    def __init__(self, *args, **kwargs):
        self.calls: list[str] = []
        super().__init__(*args, **kwargs)

    async def connect(self):
        self.calls.append("connect")

    async def disconnect(self):
        self.calls.append("disconnect")

    async def publish_message(self, body, queue_name=None, **kwargs):
        self.calls.append("publish")

    async def consume_messages(self, queue_name, callback, **kwargs):
        ...

    async def process_message(self, body, properties=None):
        ...

    async def _subscribe(self, app):
        self.calls.append("subscribe")

    async def _stop_consuming(self):
        self.calls.append("stop_consuming")


def test_abstract_methods_enforced():
    class Incomplete(BrokerManager):
        async def connect(self): ...
        async def disconnect(self): ...
        async def publish_message(self, body, queue_name=None, **kw): ...
        async def consume_messages(self, queue_name, callback, **kw): ...

    with pytest.raises(TypeError):
        Incomplete()


async def test_manager_single_connection():
    m = DummyManager(num_workers=2)
    await m.start(web.Application())
    assert m.calls.count("connect") == 1
    await m.stop(None)


async def test_manager_start_runs_both_halves():
    # FILL IN: after start(), len(m._workers) == num_workers and "subscribe" in calls; stop() afterwards
    ...


async def test_manager_auto_subscribe_false():
    """Regression for D2: the flag, not _queue_name/_callback_, decides.

    DummyManager has no BrokerConsumer base, so consumer kwargs (queue_name,
    callback) would reach object.__init__ — the real-class D2 regression with
    those kwargs lives in TASK-4565 (test_rmq_manager.py).
    """
    m = DummyManager(auto_subscribe=False)
    await m.start(web.Application())
    # FILL IN: "connect" in calls, workers started, "subscribe" NOT in calls; stop()
    ...


async def test_manager_auto_subscribe_default_true():
    # FILL IN: default instance subscribes exactly once
    ...


def test_manager_setup_registers_name():
    app = web.Application()
    m = DummyManager()
    m.setup(app)
    assert app["dummy_manager"] is m
    # FILL IN: m.start in app.on_startup, m.stop in app.on_shutdown


async def test_manager_stop_drains_and_disconnects():
    """Consumers stop first, then the queue drains, workers end, disconnect once."""
    m = DummyManager(num_workers=1)
    await m.start(web.Application())
    m.event_queue.put_nowait({"body": "b", "queue_name": "q", "routing_key": "r"})
    await m.stop(None)
    assert m.calls[-3:] == ["stop_consuming", "publish", "disconnect"]
    # FILL IN: all m._workers done; calls.count("disconnect") == 1; event_queue.empty()


async def test_manager_stop_continues_when_stop_consuming_fails():
    # FILL IN: _stop_consuming raises RuntimeError → stop() still disconnects exactly once
    ...
```

### FILL IN checklist
- [ ] Each `# FILL IN` test body above; bounded by spec §4 Module 3 rows and AC "exactly one connection", "auto_subscribe=False reliably suppresses", "shutdown stops consumer activity before draining".

---

## Acceptance Criteria

- [ ] `from navigator_eventbus.brokers import BrokerManager` works without `aiormq`/`aioboto3`.
- [ ] `start()` connects once, starts workers, and subscribes only when `auto_subscribe` is true.
- [ ] `stop()` calls `_stop_consuming()` before draining, cancels workers, and disconnects once.
- [ ] `setup()` registers `app[_name_]` and wires `on_startup`/`on_shutdown`.
- [ ] `ruff check src/navigator_eventbus/brokers/manager.py tests/brokers/test_manager.py` clean.

---

## Validation Commands
- `pytest tests/brokers/test_manager.py -q`
- `pytest tests/brokers/test_producer.py -q`

---

## Test Specification

See the blueprint test file. It covers spec §4 rows `test_manager_single_connection`,
`test_manager_start_runs_both_halves`, `test_manager_auto_subscribe_false`,
`test_manager_auto_subscribe_default_true`, `test_manager_setup_registers_name` and
`test_manager_stop_drains_and_disconnects`.

---

## Agent Instructions

1. **Work in the feature worktree** — never on `dev`
   (`python -m scripts.sdd.ensure_worktree --slug brokers-unified-manager --feature-id FEAT-431`).
2. **Environment**: `source .venv/bin/activate`; if `pytest` is missing run
   `uv pip install -e ".[dev,rabbitmq,sqs,redis]"` (no manifest change).
3. Read the spec; verify the Codebase Contract (re-grep anchors; line numbers may drift).
4. Set status `in-progress` in `sdd/tasks/index/brokers-unified-manager.json`, commit only that file.
5. Implement from the blueprint; complete every `# FILL IN:`; never change fixed signatures.
6. Run the Validation Commands; stage only the files listed above.
7. Close with `scripts/sdd/close_task.sh TASK-4564 brokers-unified-manager verified`, fill the Completion Note, commit.

---

## Completion Note

**Completed by**: sonnet (native)
**Date**: 2026-10-21
**Notes**: Merged. Verified locally with PYTHONPATH=worktree/src: tests/brokers 53 passed; only 2 pre-existing navconfig-env failures (fail on baseline).

**Deviations from spec**: none
