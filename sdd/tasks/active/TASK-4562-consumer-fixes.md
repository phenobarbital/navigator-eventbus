# TASK-4562: Consumer fixes — exchange clobber (D1), single callback wrap, consumer-tag tracking

**Feature**: FEAT-431 — Unified Broker Manager + brokers hardening
**Spec**: `sdd/specs/brokers-unified-manager.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Implements spec §3 **Module 2** (D1: `queue_name` clobbers an explicit
`exchange_name`) and **Module 5** (double callback wrapping in
`event_subscribe()` for RabbitMQ and SQS). It also records the AMQP
consumer tag returned by `basic_consume()` in `RMQConsumer.subscribe_to_events()`.
`RMQManager` (TASK-4565) needs those tags to cancel consumers before draining
outgoing work (spec §2 "Shutdown and registration contracts").

Module 3 depends on this task. A manager built on the unfixed consumer binds
to the wrong exchange (spec §7 Known Risks).

---

## Scope

- **D1**: in `RMQConsumer.__init__`, apply the queue-name fallback for the
  exchange **only** when no `exchange_name` was supplied; default to
  `"navigator"` when neither is supplied.
- **Module 5 (RabbitMQ)**: `RMQConsumer.event_subscribe()` passes the raw
  callback to `consume_messages()` (which already wraps it, `rabbitmq/connection.py:331`).
- **Module 5 (SQS)**: same for `SQSConsumer.event_subscribe()`. `SQSConnection.consume_messages()`
  already wraps each message (`sqs/connection.py:271`).
- **Consumer tags**: initialize `self._consumer_tags: list[str] = []` in
  `RMQConsumer.__init__` and append the `consumer_tag` of the `ConsumeOk`
  returned by `basic_consume()` in `subscribe_to_events()`.
- Write `tests/brokers/test_consumer_fixes.py`.

**NOT in scope**: cancelling consumers (TASK-4565); any change to
`rabbitmq/connection.py` (TASK-4563 owns that file); Redis consumer;
`BrokerConsumer`.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/brokers/rabbitmq/consumer.py` | MODIFY | D1 fix, single wrap, consumer-tag tracking |
| `src/navigator_eventbus/brokers/sqs/consumer.py` | MODIFY | Single wrap in `event_subscribe()` |
| `tests/brokers/test_consumer_fixes.py` | CREATE | Unit tests for D1, single wrap, tag tracking |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.brokers.rabbitmq import RMQConsumer   # verified: brokers/rabbitmq/__init__.py:4
from navigator_eventbus.brokers.sqs import SQSConsumer        # verified: brokers/sqs/__init__.py:3
import aiormq                                                 # aiormq 7.2.2 — rabbitmq extra; aiormq.spec.Basic.Properties / ConsumeOk available
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/brokers/rabbitmq/consumer.py
class RMQConsumer(RabbitMQConnection, BrokerConsumer):           # line 22
    def __init__(self, credentials=None, timeout=5, callback=None, **kwargs)  # line 27
    #   self._routing_key   = kwargs.get("routing_key", "*")     # line 45
    #   self._exchange_type = kwargs.get("exchange_type", "topic")  # line 46
    #   self._exchange_name = kwargs.get("exchange_name", "navigator")  # line 47
    #   self._queue_name    = kwargs.get("queue_name", None)     # line 48
    #   if self._queue_name:                                     # line 49  ← D1
    #       self._exchange_name = self._queue_name               # line 50  ← D1
    #   super().__init__(credentials=..., timeout=..., callback=..., **kwargs)  # line 51-53
    #   self.logger = logging.getLogger("RMQConsumer")           # line 54
    async def event_subscribe(self, queue_name, callback)        # line 68
    #   await self.consume_messages(queue_name=queue_name, callback=self.wrap_callback(callback))  # line 74-76 ← double wrap
    async def subscribe_to_events(self, exchange, queue_name, routing_key, callback,
        exchange_type="topic", durable=True, prefetch_count=1,
        requeue_on_fail=True, max_retries=3, **kwargs)           # line 78
    #   await self._channel.basic_consume(queue=..., consumer_callback=self.wrap_callback(...), **kwargs)  # line 109-117

# src/navigator_eventbus/brokers/rabbitmq/connection.py  (read-only for this task)
    async def ensure_connection(self) -> None                    # line 122 — connects only if _connection is None or is_closed
    def wrap_callback(self, callback, requeue_on_fail=False, max_retries=3)  # line 236 — decodes, calls callback(message, body), basic_ack
    async def consume_messages(self, queue_name, callback, prefetch_count=1)  # line 313 — wraps via self.wrap_callback(callback) at line 331

# src/navigator_eventbus/brokers/sqs/consumer.py
class SQSConsumer(SQSConnection, BrokerConsumer):                # line 18
    async def event_subscribe(self, queue_name, callback)        # line 54
    #   await self.consume_messages(queue_name=queue_name, callback=self.wrap_callback(callback))  # line 60-62 ← double wrap
# src/navigator_eventbus/brokers/sqs/connection.py
    async def consume_messages(self, queue_name, callback, max_messages=10, wait_time=10, idle_sleep=5)  # line 254 — wraps per message at line 271

# aiormq 7.2.2 (installed)
Channel.basic_consume(queue, consumer_callback, *, no_ack=False, exclusive=False,
                      arguments=None, consumer_tag=None, timeout=None) -> pamqp.commands.Basic.ConsumeOk  # has .consumer_tag
```
`BrokerConsumer.__init__` (`brokers/consumer.py:32`) runs **last** and resets
`self._queue_name = kwargs.get("queue_name", "navigator")`. This does not affect
`_exchange_name`, which is resolved in `RMQConsumer.__init__` before `super()`.

### Does NOT Exist
- ~~`RMQConsumer._consumer_tags`~~ — created by this task.
- ~~`RabbitMQConnection.cancel_consumers()`~~ / ~~`RMQConsumer.unsubscribe()`~~ — do not exist and are not added here.
- ~~`BrokerConsumer` calling `super().__init__()`~~ — it does not, by design. Do not "fix" it.
- ~~`message.delivery.delivery_tag` being required~~ — `DeliveredMessage.delivery_tag` is a property; `wrap_callback` is correct.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/brokers/rabbitmq/consumer.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/sqs/consumer.py", "action": "MODIFY"},
    {"path": "tests/brokers/test_consumer_fixes.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/brokers/rabbitmq/consumer.py#RMQConsumer",
    "sym:src/navigator_eventbus/brokers/rabbitmq/consumer.py#RMQConsumer.event_subscribe",
    "sym:src/navigator_eventbus/brokers/rabbitmq/consumer.py#RMQConsumer.subscribe_to_events",
    "sym:src/navigator_eventbus/brokers/sqs/consumer.py#SQSConsumer.event_subscribe",
    "sym:src/navigator_eventbus/brokers/rabbitmq/connection.py#RabbitMQConnection.wrap_callback",
    "sym:src/navigator_eventbus/brokers/rabbitmq/connection.py#RabbitMQConnection.consume_messages"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- Preserve the D1 fallback: only `queue_name` supplied → the exchange mirrors it (spec §7 risk 1).
- Use `"exchange_name" in kwargs`-style presence semantics through a falsy check
  (`kwargs.get("exchange_name")`). An explicit `None`/`""` counts as "not supplied".
- Do not touch `rabbitmq/connection.py`; TASK-4563 edits it concurrently.
- Release-notes impact (spec §7): Module 5 changes what `event_subscribe()` callbacks
  receive, from a raw `DeliveredMessage` to `(message, decoded_body)`. TASK-4568 records this.

---

## Implementation Blueprint

### Steps (in order)
1. Replace the D1 block in `RMQConsumer.__init__` — *why*: Module 2; explicit `exchange_name` must survive.
2. Initialize `self._consumer_tags` after the `super().__init__` call — *why*: it must exist even if the instance never subscribes, so TASK-4565 can iterate it safely.
3. Capture `basic_consume()`'s return in `subscribe_to_events()` and record the tag — *why*: the manager cancels these before draining.
4. Pass the raw callback in both `event_subscribe()` methods — *why*: Module 5; `consume_messages()` already wraps.
5. Write the tests; run them.

### `src/navigator_eventbus/brokers/rabbitmq/consumer.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c '        if self._queue_name:' src/navigator_eventbus/brokers/rabbitmq/consumer.py)
# REPLACE lines 47-50 (from `self._exchange_name = kwargs.get("exchange_name", "navigator")`
#                      through `self._exchange_name = self._queue_name`) with:
        self._queue_name = kwargs.get("queue_name", None)
        # D1 fix (FEAT-431 Module 2): the queue name only stands in for the
        # exchange when no explicit exchange_name was supplied.
        self._exchange_name = (
            kwargs.get("exchange_name") or self._queue_name or "navigator"
        )
```
```python
# occurrences: 1 (verified: grep -c '        self.logger = logging.getLogger("RMQConsumer")' src/navigator_eventbus/brokers/rabbitmq/consumer.py)
# AFTER — insert below `        self.logger = logging.getLogger("RMQConsumer")` (verified: consumer.py:54)
        #: AMQP consumer tags returned by ``basic_consume`` in
        #: :meth:`subscribe_to_events` (FEAT-431) — used by RMQManager to
        #: cancel consumers before draining outgoing work.
        self._consumer_tags: list[str] = []
```
```python
# occurrences: 1 (verified: grep -c '            queue_name=queue_name, callback=self.wrap_callback(callback)' src/navigator_eventbus/brokers/rabbitmq/consumer.py)
# REPLACE line 75 with (consume_messages already wraps — FEAT-431 Module 5):
            queue_name=queue_name, callback=callback
```
```python
# occurrences: 1 (verified: grep -c '            await self._channel.basic_consume(' src/navigator_eventbus/brokers/rabbitmq/consumer.py)
# REPLACE line 109 `            await self._channel.basic_consume(` with:
            consume_ok = await self._channel.basic_consume(
# and AFTER the closing `)` of that call (line 117), insert:
            consumer_tag = getattr(consume_ok, "consumer_tag", None)
            if consumer_tag:
                self._consumer_tags.append(consumer_tag)
```

### `src/navigator_eventbus/brokers/sqs/consumer.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c '            queue_name=queue_name, callback=self.wrap_callback(callback)' src/navigator_eventbus/brokers/sqs/consumer.py)
# REPLACE line 61 with (consume_messages already wraps per message — FEAT-431 Module 5):
            queue_name=queue_name, callback=callback
```

### `tests/brokers/test_consumer_fixes.py` (CREATE)
```python
"""Tests for FEAT-431 Modules 2 and 5: RMQ/SQS consumer fixes (TASK-4562)."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiormq
import pytest

from navigator_eventbus.brokers.rabbitmq import RMQConsumer
from navigator_eventbus.brokers.sqs import SQSConsumer

DSN = "amqp://guest:guest@localhost:5672/navigator"


@pytest.fixture
def mock_amqp_channel():
    """aiormq channel double (spec §4 fixture)."""
    ch = MagicMock()
    for name in ("exchange_declare", "queue_declare", "queue_bind", "basic_publish",
                 "basic_qos", "basic_ack", "basic_nack", "basic_cancel"):
        setattr(ch, name, AsyncMock())
    ch.basic_consume = AsyncMock(return_value=aiormq.spec.Basic.ConsumeOk(consumer_tag="ctag-1"))
    return ch


def _wire(consumer: RMQConsumer, channel) -> RMQConsumer:
    """Attach a fake open connection + channel so ensure_connection() is a no-op."""
    consumer._connection = SimpleNamespace(is_closed=False)
    consumer._channel = channel
    return consumer


def _delivered(body: bytes = b"hello", tag: int = 7):
    """Minimal DeliveredMessage double accepted by RabbitMQConnection.wrap_callback."""
    props = aiormq.spec.Basic.Properties(content_type="text/plain", headers={})
    return SimpleNamespace(body=body, delivery_tag=tag, header=SimpleNamespace(properties=props),
                           delivery=SimpleNamespace(exchange="navigator", routing_key="#"))


def test_exchange_not_clobbered():
    c = RMQConsumer(credentials=DSN, exchange_name="navigator", queue_name="processing_queue")
    assert c._exchange_name == "navigator"
    assert c._queue_name == "processing_queue"


def test_exchange_falls_back_to_queue():
    # FILL IN: only queue_name="processing_queue" → _exchange_name == "processing_queue" — bounded by spec §7 risk 1
    ...


def test_exchange_default():
    # FILL IN: neither supplied → _exchange_name == "navigator" — bounded by spec §4 test_exchange_default
    ...


async def test_event_subscribe_single_wrap(mock_amqp_channel):
    """User callback receives (message, decoded_body); exactly one basic_ack."""
    c = _wire(RMQConsumer(credentials=DSN), mock_amqp_channel)
    received = []

    async def cb(message, body):
        received.append((message, body))

    await c.event_subscribe("processing_queue", cb)
    consumer_callback = mock_amqp_channel.basic_consume.call_args.kwargs["consumer_callback"]
    msg = _delivered()
    await consumer_callback(msg)
    # FILL IN: assert received == [(msg, "hello")] and basic_ack awaited exactly once with 7 — bounded by AC "wraps the callback exactly once"


async def test_subscribe_records_consumer_tag(mock_amqp_channel):
    c = _wire(RMQConsumer(credentials=DSN), mock_amqp_channel)
    # FILL IN: await c.subscribe_to_events("navigator", "q", "#", AsyncMock()); assert c._consumer_tags == ["ctag-1"]
    ...


def test_consumer_tags_empty_by_default():
    assert RMQConsumer(credentials=DSN)._consumer_tags == []


async def test_sqs_event_subscribe_single_wrap(monkeypatch):
    """SQSConsumer.event_subscribe hands the raw callback to consume_messages."""
    c = SQSConsumer(credentials={"region_name": "us-east-1"})
    consume = AsyncMock()
    monkeypatch.setattr(c, "consume_messages", consume)

    async def cb(message, body):
        ...

    await c.event_subscribe("q", cb)
    # FILL IN: assert consume.call_args.kwargs["callback"] is cb — bounded by Module 5 (SQS counterpart)
```
**Why this shape**: the RabbitMQ single-wrap test runs the real `consume_messages`
and `wrap_callback` against a channel double. A second wrap would pass the
already wrapped coroutine function as the user callback, and the assertions on
`received` would catch that.

### FILL IN checklist
- [ ] `test_exchange_falls_back_to_queue`, `test_exchange_default` bodies — spec §4 Module 2 rows.
- [ ] `test_event_subscribe_single_wrap` assertions — AC "event_subscribe() wraps the callback exactly once".
- [ ] `test_subscribe_records_consumer_tag` body.
- [ ] `test_sqs_event_subscribe_single_wrap` assertion.

---

## Acceptance Criteria

- [ ] `exchange_name` and `queue_name` are independent (D1); the fallback is preserved when only `queue_name` is given.
- [ ] `event_subscribe()` wraps the callback exactly once (RabbitMQ and SQS).
- [ ] `subscribe_to_events()` records every consumer tag in `self._consumer_tags`.
- [ ] Existing `tests/brokers/test_rabbitmq.py` and `tests/brokers/test_sqs.py` still pass.
- [ ] `ruff check` clean on the touched files.

---

## Validation Commands
- `pytest tests/brokers/test_consumer_fixes.py -q`
- `pytest tests/brokers/test_rabbitmq.py -q`
- `pytest tests/brokers/test_sqs.py -q`

---

## Test Specification

See the blueprint test file. It covers spec §4 rows `test_exchange_not_clobbered`,
`test_exchange_falls_back_to_queue`, `test_exchange_default` and
`test_event_subscribe_single_wrap`, plus consumer-tag tracking.

---

## Agent Instructions

1. **Work in the feature worktree** — never on `dev`
   (`python -m scripts.sdd.ensure_worktree --slug brokers-unified-manager --feature-id FEAT-431`).
2. **Environment**: `source .venv/bin/activate`; if `pytest`/`aiormq`/`aioboto3` are missing run
   `uv pip install -e ".[dev,rabbitmq,sqs,redis]"` (no manifest change).
3. Read the spec; verify the Codebase Contract (re-grep anchors; line numbers may drift).
4. Set status `in-progress` in `sdd/tasks/index/brokers-unified-manager.json`, commit only that file.
5. Implement from the blueprint; complete every `# FILL IN:`; never change fixed signatures.
6. Run the Validation Commands; stage only the files listed above.
7. Close with `scripts/sdd/close_task.sh TASK-4562 brokers-unified-manager verified`, fill the Completion Note, commit.

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**:
**Date**:
**Notes**:

**Deviations from spec**: none
