# TASK-4565: `RMQManager` — RabbitMQ producer + consumer over one connection

**Feature**: FEAT-431 — Unified Broker Manager + brokers hardening
**Spec**: `sdd/specs/brokers-unified-manager.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-4562, TASK-4564
**Assigned-to**: unassigned

---

## Context

Implements the RabbitMQ concrete manager of spec §3 **Module 3**. This is the class
the downstream `nav-rewards` migration (FEAT-002) needs.
`RMQManager(BrokerManager, RMQConsumer)` gets one AMQP connection and channel from
the MRO (spec §2, verified):

```
RMQManager → BrokerManager → BrokerProducer → RMQConsumer → RabbitMQConnection
           → BaseConnection → BrokerConsumer → ABC → object
```

Shutdown contract (spec §2): **cancel every consumer tag**, let **in-flight callbacks
finish**, then drain outgoing work and disconnect. The drain and disconnect come from
`BrokerManager.stop` via `BrokerProducer.stop`. aiormq 7.2.2 runs each delivery's
`consumer_callback` as its own task, so the manager tracks in-flight deliveries itself.
It does this by overriding `wrap_callback`, which every RabbitMQ subscription path
(`subscribe_to_events`, `consume_messages`) calls through `self.wrap_callback(...)`.

---

## Scope

- Create `src/navigator_eventbus/brokers/rabbitmq/manager.py` with `RMQManager(BrokerManager, RMQConsumer)`, `_name_ = "rabbitmq_manager"`.
- Implement `_subscribe(app)` using `subscribe_to_events(...)` with the instance's exchange/queue/routing/callback (the same arguments `RMQConsumer.start` uses, `rabbitmq/consumer.py:126-138`).
- Override `wrap_callback(...)` to count in-flight deliveries around the parent wrapper.
- Implement `_stop_consuming()`: `basic_cancel` every tag in `self._consumer_tags` (from TASK-4562),
  clear the list, then wait (bounded) until no delivery is in flight. Tolerate no subscription and a missing channel.
- Re-export `RMQManager` from `navigator_eventbus.brokers.rabbitmq` (**not** from `navigator_eventbus.brokers`).
- Write `tests/brokers/test_rmq_manager.py`.

**NOT in scope**: `BrokerManager` itself (TASK-4564); D1 / consumer tags (TASK-4562);
`ensure_queue`/settlement (TASK-4563); live-broker integration tests (TASK-4568).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/brokers/rabbitmq/manager.py` | CREATE | `RMQManager` |
| `src/navigator_eventbus/brokers/rabbitmq/__init__.py` | MODIFY | Re-export `RMQManager` |
| `tests/brokers/test_rmq_manager.py` | CREATE | MRO, init chain, lifecycle and teardown-order tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.brokers.manager import BrokerManager          # created by TASK-4564 (brokers/manager.py)
from navigator_eventbus.brokers.rabbitmq.consumer import RMQConsumer  # verified: brokers/rabbitmq/consumer.py:22
from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection    # verified: brokers/rabbitmq/__init__.py:3
from navigator_eventbus.brokers import BaseConnection, BrokerConsumer, BrokerProducer  # verified: brokers/__init__.py:14-16
from navconfig.logging import logging
import aiormq                                                         # 7.2.2 — aiormq.spec.Basic.ConsumeOk(consumer_tag=...)
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/brokers/manager.py  (TASK-4564)
class BrokerManager(BrokerProducer, ABC):
    def __init__(self, *args, auto_subscribe: bool = True, **kwargs) -> None
    async def start(self, app) -> None      # connect() once → start_workers() → _subscribe(app) if auto_subscribe
    async def stop(self, app) -> None       # _stop_consuming() → BrokerProducer.stop (drain, cancel workers, disconnect)
    @abstractmethod async def _subscribe(self, app) -> None
    @abstractmethod async def _stop_consuming(self) -> None

# src/navigator_eventbus/brokers/rabbitmq/consumer.py (after TASK-4562)
class RMQConsumer(RabbitMQConnection, BrokerConsumer):               # line 22
    #   self._routing_key, self._exchange_type, self._exchange_name, self._queue_name   # lines 45-50
    #   self._consumer_tags: list[str]                                  # added by TASK-4562; filled by subscribe_to_events
    async def subscribe_to_events(self, exchange, queue_name, routing_key, callback,
        exchange_type="topic", durable=True, prefetch_count=1,
        requeue_on_fail=True, max_retries=3, **kwargs)                  # line 78 — calls self.wrap_callback(callback, requeue_on_fail=, max_retries=)
    async def start(self, app) -> None                                  # line 126 — reference argument set for _subscribe (lines 129-138)

# src/navigator_eventbus/brokers/rabbitmq/connection.py
    #   self._channel: Optional[AbstractChannel]                        # line 48
    def wrap_callback(self, callback, requeue_on_fail=False, max_retries=3) -> Callable  # line 236 — returns async wrapped_callback(message)
    async def consume_messages(self, queue_name, callback, prefetch_count=1)            # line 313 — calls self.wrap_callback(callback)
    async def disconnect(self) -> None                                  # line 100 — closes channel + connection, cancels monitor
# src/navigator_eventbus/brokers/consumer.py
    #   BrokerConsumer.__init__ sets self._callback_ = callback or self.subscriber_callback  # line 34

# aiormq 7.2.2 Channel
basic_cancel(consumer_tag: str, *, nowait: bool = False, timeout=None) -> CancelOk | None
```

### Does NOT Exist
- ~~`RMQManager`~~ — created here.
- ~~`RabbitMQConnection.cancel_consumers()`~~ / ~~`RMQConsumer.unsubscribe()`~~ — do not exist; call `self._channel.basic_cancel(tag)` directly.
- ~~aiormq awaiting consumer callbacks inline~~ — it does not; each delivery runs as a separate task. Hence the in-flight tracker.
- ~~`_queue_name` / `_callback_` as sentinels~~ — unusable (D2); `BrokerManager` exposes `auto_subscribe`.
- ~~Re-exporting `RMQManager` from `navigator_eventbus.brokers`~~ — forbidden; it would eagerly import `aiormq` (spec §7).

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/brokers/rabbitmq/manager.py", "action": "CREATE"},
    {"path": "src/navigator_eventbus/brokers/rabbitmq/__init__.py", "action": "MODIFY"},
    {"path": "tests/brokers/test_rmq_manager.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/brokers/rabbitmq/consumer.py#RMQConsumer",
    "sym:src/navigator_eventbus/brokers/rabbitmq/consumer.py#RMQConsumer.subscribe_to_events",
    "sym:src/navigator_eventbus/brokers/rabbitmq/connection.py#RabbitMQConnection.wrap_callback",
    "sym:src/navigator_eventbus/brokers/rabbitmq/connection.py#RabbitMQConnection.disconnect"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- Base order `(BrokerManager, RMQConsumer)` is fixed; the MRO test asserts it exactly.
- `_stop_consuming()` must cancel tags **before** waiting for in-flight deliveries. Otherwise new
  deliveries keep arriving while we wait.
- Bound the in-flight wait with the class attribute `_inflight_timeout: float = 30.0`. On timeout,
  log a warning and continue, so shutdown never hangs forever on a stuck callback.
- Initialize the tracker state (`_inflight`, `_idle`) **after** `super().__init__`, as plain instance
  attributes. `asyncio.Event()` needs no running loop on Python ≥ 3.10.
- Subscriptions made via `event_subscribe()` go through `consume_messages`, which does not record a
  consumer tag. They are still in-flight-tracked, and the channel close in `disconnect()` ends them.
  Mention this in the class docstring. `subscribe_to_events()` is the documented way to subscribe
  explicitly with `auto_subscribe=False`.

---

## Implementation Blueprint

### Steps (in order)
1. Create `rabbitmq/manager.py` from the block — *why*: fixes class, `_name_` and hooks per spec §2.
2. Re-export from `rabbitmq/__init__.py` — *why*: AC "re-exported from their subpackages".
3. Write the tests; run them with the TASK-4562/4564 suites.

### `src/navigator_eventbus/brokers/rabbitmq/manager.py` (CREATE)
```python
"""RMQManager — RabbitMQ producer + consumer over one connection (FEAT-431 Module 3).

Combines :class:`~navigator_eventbus.brokers.manager.BrokerManager` with
:class:`~navigator_eventbus.brokers.rabbitmq.consumer.RMQConsumer`; the single
AMQP connection/channel comes from the MRO. Shutdown cancels every consumer
tag and lets in-flight deliveries finish before the outgoing queue drains.
"""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from aiohttp import web
from navconfig.logging import logging

from ..manager import BrokerManager
from .consumer import RMQConsumer


class RMQManager(BrokerManager, RMQConsumer):
    """Unified RabbitMQ publisher/consumer sharing one connection and channel.

    Subscribe explicitly with :meth:`subscribe_to_events` when built with
    ``auto_subscribe=False``; those consumer tags are cancelled on shutdown.
    Consumers started via :meth:`event_subscribe` are in-flight tracked but
    record no tag; they end when the channel closes on disconnect.
    """

    _name_: str = "rabbitmq_manager"
    #: Upper bound (seconds) for in-flight deliveries to finish on shutdown.
    _inflight_timeout: float = 30.0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger("Broker.Manager.RMQManager")
        self._inflight: int = 0
        self._idle: asyncio.Event = asyncio.Event()
        self._idle.set()

    def wrap_callback(
        self,
        callback: Callable[[Any, str], Awaitable[None]],
        requeue_on_fail: bool = False,
        max_retries: int = 3,
    ) -> Callable:
        """Wrap like :class:`RabbitMQConnection` and track the delivery as in-flight."""
        inner = super().wrap_callback(
            callback, requeue_on_fail=requeue_on_fail, max_retries=max_retries
        )

        async def tracked(message: Any) -> None:
            # FILL IN: increment self._inflight and clear self._idle; await inner(message);
            #          in `finally` decrement and set self._idle when it reaches 0 — bounded by AC
            #          "in-flight callbacks finish before outgoing queue drain"
            ...

        return tracked

    async def _subscribe(self, app: web.Application) -> None:
        """Subscribe with the configured exchange/queue/routing key/callback."""
        await self.subscribe_to_events(
            exchange=self._exchange_name,
            queue_name=self._queue_name,
            routing_key=self._routing_key,
            callback=self._callback_,
            exchange_type=self._exchange_type,
            durable=True,
            prefetch_count=1,
            requeue_on_fail=True,
        )

    async def _stop_consuming(self) -> None:
        """Cancel all consumer tags, then wait (bounded) for in-flight deliveries."""
        tags, self._consumer_tags = list(self._consumer_tags), []
        for tag in tags:
            # FILL IN: if self._channel is not None, await self._channel.basic_cancel(tag);
            #          log (warning) and continue on any exception — bounded by "teardown tolerates no subscription"
            ...
        try:
            await asyncio.wait_for(self._idle.wait(), timeout=self._inflight_timeout)
        except asyncio.TimeoutError:
            self.logger.warning(
                f"{self._inflight} delivery(ies) still in flight after {self._inflight_timeout}s; continuing shutdown."
            )
```
**Why this shape**: `wrap_callback` resolves through the MRO to `RabbitMQConnection.wrap_callback`,
so the tracker wraps the whole delivery, including ack/nack. "Callback finished" therefore means
"delivery settled".

### `src/navigator_eventbus/brokers/rabbitmq/__init__.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c 'from .producer import RMQProducer' src/navigator_eventbus/brokers/rabbitmq/__init__.py)
# AFTER — insert below `from .producer import RMQProducer` (verified: rabbitmq/__init__.py:6)
from .manager import RMQManager
# and add "RMQManager", to __all__ (after "RMQProducer", line 11)
```

### `tests/brokers/test_rmq_manager.py` (CREATE)
```python
"""RMQManager tests (FEAT-431 Module 3, TASK-4565)."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiormq
import pytest
from aiohttp import web

from navigator_eventbus.brokers import BaseConnection, BrokerConsumer, BrokerProducer
from navigator_eventbus.brokers.manager import BrokerManager
from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection, RMQConsumer, RMQManager


@pytest.fixture
def manager_kwargs():
    return {
        "credentials": "amqp://guest:guest@localhost:5672/navigator",
        "exchange_name": "navigator",
        "queue_name": "processing_queue",   # deliberately != exchange_name
        "routing_key": "#",
    }


@pytest.fixture
def channel():
    ch = MagicMock()
    for name in ("exchange_declare", "queue_declare", "queue_bind", "basic_publish", "basic_qos",
                 "basic_ack", "basic_nack", "basic_cancel", "close"):
        setattr(ch, name, AsyncMock())
    ch.basic_consume = AsyncMock(side_effect=lambda **kw: aiormq.spec.Basic.ConsumeOk(
        consumer_tag=f"ctag-{ch.basic_consume.await_count}"))
    return ch


@pytest.fixture
def patched(monkeypatch, channel):
    """Replace the network connect with a fake open connection + channel; count connects."""
    state = {"connects": 0, "events": []}

    async def fake_connect(self):
        state["connects"] += 1
        self._connection = SimpleNamespace(is_closed=False, close=AsyncMock())
        self._channel = channel

    monkeypatch.setattr(RabbitMQConnection, "connect", fake_connect)
    return state


def _delivered(tag: int = 1):
    props = aiormq.spec.Basic.Properties(content_type="text/plain", headers={})
    return SimpleNamespace(body=b"hi", delivery_tag=tag, header=SimpleNamespace(properties=props),
                           delivery=SimpleNamespace(exchange="navigator", routing_key="#"))


def test_manager_mro_rabbitmq():
    assert RMQManager.__mro__[:7] == (RMQManager, BrokerManager, BrokerProducer, RMQConsumer,
                                      RabbitMQConnection, BaseConnection, BrokerConsumer)


def test_manager_init_chain(manager_kwargs):
    m = RMQManager(**manager_kwargs)
    # FILL IN: _serializer, _dsn == credentials, queue_size, _exchange_name == "navigator",
    #          _queue_name == "processing_queue", _consumer_tags == [], _name_ == "rabbitmq_manager"


async def test_manager_auto_subscribe_false(manager_kwargs, patched, channel):
    """D2 regression on the real class: queue_name + callback supplied, still no subscribe."""
    m = RMQManager(auto_subscribe=False, callback=AsyncMock(), **manager_kwargs)
    await m.start(web.Application())
    # FILL IN: patched["connects"] == 1, workers started, channel.basic_consume not awaited; await m.stop(None)


async def test_rmq_manager_stop_consumers_first(manager_kwargs, patched, channel):
    """Tags cancelled and the in-flight delivery settles before drain + disconnect."""
    release = asyncio.Event()
    order: list[str] = []

    async def slow_cb(message, body):
        await release.wait()
        order.append("callback_done")

    m = RMQManager(callback=slow_cb, **manager_kwargs)
    await m.start(web.Application())
    deliver = channel.basic_consume.call_args.kwargs["consumer_callback"]
    inflight = asyncio.create_task(deliver(_delivered()))
    await asyncio.sleep(0)
    channel.basic_cancel.side_effect = lambda tag: order.append(f"cancel:{tag}")
    # FILL IN: wrap m.disconnect to append "disconnect"; start stop task; assert "cancel:ctag-1" first and
    #          stop not done while callback blocked; release.set(); await both;
    #          assert order == ["cancel:ctag-1", "callback_done", "disconnect"]; basic_ack awaited once


async def test_manager_stop_manual_or_no_subscription(manager_kwargs, patched, channel):
    # FILL IN: (a) auto_subscribe=False, no subscription → stop() ok, basic_cancel not awaited;
    #          (b) auto_subscribe=False then await m.subscribe_to_events("navigator", "q2", "#", AsyncMock())
    #              → stop() cancels exactly that tag
    ...


async def test_manager_publishes_and_consumes(manager_kwargs, patched, channel):
    received = []

    async def cb(message, body):
        received.append(body)

    m = RMQManager(callback=cb, num_workers=1, **manager_kwargs)
    await m.start(web.Application())
    m.event_queue.put_nowait({"body": "out", "queue_name": "navigator", "routing_key": "a.b"})
    await m.event_queue.join()
    await channel.basic_consume.call_args.kwargs["consumer_callback"](_delivered())
    # FILL IN: basic_publish awaited, received == ["hi"], patched["connects"] == 1; await m.stop(None)


def test_rmq_manager_setup_registers_name(manager_kwargs):
    app = web.Application()
    RMQManager(**manager_kwargs).setup(app)
    # FILL IN: "rabbitmq_manager" in app; POST /api/v1/broker/rabbitmq/publish_event registered
```

### FILL IN checklist
- [ ] `RMQManager.wrap_callback.tracked` body — in-flight accounting.
- [ ] `_stop_consuming` cancel loop — tolerate a missing channel and cancel errors.
- [ ] Every `# FILL IN` in the test file — spec §4 rows `test_manager_mro_rabbitmq`, `test_manager_init_chain`,
      `test_manager_auto_subscribe_false`, `test_rmq_manager_stop_consumers_first`,
      `test_manager_stop_manual_or_no_subscription`, `test_manager_publishes_and_consumes`.

---

## Acceptance Criteria

- [ ] `from navigator_eventbus.brokers.rabbitmq import RMQManager` works, and `navigator_eventbus.brokers` does not import it.
- [ ] MRO is exactly `RMQManager → BrokerManager → BrokerProducer → RMQConsumer → RabbitMQConnection → BaseConnection → BrokerConsumer`.
- [ ] One `connect()` serves publishing and consuming.
- [ ] `auto_subscribe=False` suppresses subscription even with `queue_name` and `callback` supplied.
- [ ] Shutdown cancels consumer tags (auto and manual), waits for in-flight deliveries, then drains and disconnects.
- [ ] `ruff check` clean on the touched files.

---

## Validation Commands
- `pytest tests/brokers/test_rmq_manager.py -q`

---

## Test Specification

See the blueprint test file.

---

## Agent Instructions

1. **Work in the feature worktree** — never on `dev`
   (`python -m scripts.sdd.ensure_worktree --slug brokers-unified-manager --feature-id FEAT-431`).
2. **Check dependencies**: TASK-4562 and TASK-4564 must be `done` in `sdd/tasks/index/brokers-unified-manager.json`.
3. **Environment**: `source .venv/bin/activate`; if `pytest`/`aiormq` are missing run
   `uv pip install -e ".[dev,rabbitmq,sqs,redis]"` (no manifest change).
4. Verify the Codebase Contract (re-grep; confirm `_consumer_tags` exists from TASK-4562).
5. Set status `in-progress`, commit only the index; implement from the blueprint; complete every `# FILL IN:`.
6. Run the Validation Commands; stage only the files listed above.
7. Close with `scripts/sdd/close_task.sh TASK-4565 brokers-unified-manager verified`, fill the Completion Note, commit.

---

## Completion Note

**Completed by**: gpt-5.6-terra (codex)
**Date**: 2026-10-21
**Notes**: Merged. Verified locally with PYTHONPATH=worktree/src: tests/brokers only the 2 pre-existing navconfig-env failures.

**Deviations from spec**: none
