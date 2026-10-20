# TASK-4563: RabbitMQConnection `ensure_queue` / `reject_message` + single-owner hook settlement

**Feature**: FEAT-431 — Unified Broker Manager + brokers hardening
**Spec**: `sdd/specs/brokers-unified-manager.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Implements spec §3 **Module 4** and §2 "RabbitMQ delivery settlement ownership".
`RabbitMQBrokerHook` calls `self._connection.ensure_queue(...)` (`hooks/brokers/rabbitmq.py:40`)
and `self._connection.reject_message(...)` (`:68`). Neither method exists on
`RabbitMQConnection`, so `connect()` raises `AttributeError` today.

Adding `reject_message()` alone is not enough. The hook catches the error,
rejects, and returns normally, so `wrap_callback()` then acks the same delivery.
The wrapper must become the **sole owner** of settlement. The hook raises a
dedicated internal exception, and the wrapper catches it before its generic
retry handler, nacks once with `requeue=False`, and returns.

---

## Scope

- Define an internal exception `RejectDelivery(Exception)` in `brokers/rabbitmq/connection.py`.
- Add `RabbitMQConnection.ensure_queue(queue_name, exchange_name=None, routing_key=None, durable=True, **kwargs)`.
- Add `RabbitMQConnection.reject_message(message, requeue=False)`.
- In `wrap_callback()`'s inner coroutine, add an `except RejectDelivery` clause **before**
  `except Exception`. It calls `reject_message(message, requeue=False)` once and returns,
  with no ack and no republish.
- In `RabbitMQBrokerHook._consumer_callback()`, replace the direct `reject_message` call with
  a lazily imported `raise RejectDelivery(...) from exc`.
- Write `tests/brokers/test_rabbitmq_settlement.py`.

**NOT in scope**: `rabbitmq/consumer.py` (TASK-4562); manager classes; changing the
retry/backoff policy for ordinary callback exceptions; `ack_message()` (it is not added).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/brokers/rabbitmq/connection.py` | MODIFY | `RejectDelivery`, `ensure_queue`, `reject_message`, wrapper clause |
| `src/navigator_eventbus/hooks/brokers/rabbitmq.py` | MODIFY | Raise `RejectDelivery` instead of settling directly |
| `tests/brokers/test_rabbitmq_settlement.py` | CREATE | Unit tests (spec §4 Module 4 rows) |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection          # verified: brokers/rabbitmq/__init__.py:3
from navigator_eventbus.hooks.brokers.rabbitmq import RabbitMQBrokerHook    # verified: hooks/brokers/rabbitmq.py:16
from navigator_eventbus.hooks.models import BrokerHookConfig                # verified: hooks/models.py:343
import aiormq                                                               # aiormq 7.2.2 (rabbitmq extra)
# created by this task (import it in the hook ONLY lazily, inside the except block):
from navigator_eventbus.brokers.rabbitmq.connection import RejectDelivery
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/brokers/rabbitmq/connection.py
from ..wrapper import BaseWrapper                                       # line 25 (last import)
class RabbitMQConnection(BaseConnection):                               # line 28
    #   self._channel: Optional[AbstractChannel] = None                 # line 48
    async def ensure_connection(self) -> None                           # line 122
    async def ensure_exchange(self, exchange_name, exchange_type="topic", **kwargs)  # line 150
    async def publish_message(self, body, queue_name, routing_key, **kwargs)         # line 159
    def wrap_callback(self, callback, requeue_on_fail=False, max_retries=3)          # line 236
    #   inner: decode → await callback(message, body) → basic_ack(message.delivery_tag)  # lines 245-256
    #   except Exception as e:  → retry (basic_nack + basic_publish) or discard (basic_nack)  # line 257-309
    #   NOTE: `            except Exception as e:` occurs TWICE in this file (lines 84, 257)

# src/navigator_eventbus/hooks/brokers/rabbitmq.py
class RabbitMQBrokerHook(BaseBrokerHook):                               # line 16
    def __init__(self, config: BrokerHookConfig, **kwargs)              # line 21
    async def connect(self) -> None                                     # line 30 — builds RabbitMQConnection(credentials=config.credentials), connect(), ensure_exchange(), ensure_queue()
    async def start_consuming(self) -> None                             # line 51 — consume_messages(queue_name, callback=self._consumer_callback, prefetch_count)
    async def _consumer_callback(self, message, body) -> None           # line 58
    #   except Exception as exc:                                        # line 65
    #       self.logger.error(f"RabbitMQ callback error: {exc}")        # line 66
    #       if self._connection:                                        # line 67
    #           await self._connection.reject_message(message, requeue=False)  # line 68

# src/navigator_eventbus/hooks/brokers/base.py
    async def _on_message(self, message_id, payload, message=None) -> None  # line 69 — tests monkeypatch this

# src/navigator_eventbus/hooks/models.py — BrokerHookConfig (line 343)
#   queue_name: Optional[str] = None; routing_key: str = ""; exchange_name: str = ""
#   exchange_type: str = "topic"; prefetch_count: int = 1; credentials: Optional[Dict[str, Any]] = None

# aiormq 7.2.2 Channel
queue_declare(queue='', *, passive=False, durable=False, exclusive=False, auto_delete=False, nowait=False, arguments=None, timeout=None)
queue_bind(queue, exchange, routing_key='', nowait=False, arguments=None, timeout=None)
basic_nack(delivery_tag, multiple=False, requeue=True, wait=True)
```

### Does NOT Exist
- ~~`RabbitMQConnection.ensure_queue()`~~ — created here (only `brokers/sqs/connection.py:106` has one; do not copy its signature).
- ~~`RabbitMQConnection.reject_message()`~~ — created here.
- ~~`RabbitMQConnection.ack_message()`~~ — does not exist and is NOT added; acking stays inside `wrap_callback`.
- ~~`RejectDelivery`~~ — created here; it is internal. Do not re-export it from `brokers/rabbitmq/__init__.py`.
- ~~`message.delivery.delivery_tag` being required~~ — `DeliveredMessage.delivery_tag` is a property; do not "fix" `wrap_callback`.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/brokers/rabbitmq/connection.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/hooks/brokers/rabbitmq.py", "action": "MODIFY"},
    {"path": "tests/brokers/test_rabbitmq_settlement.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/brokers/rabbitmq/connection.py#RabbitMQConnection",
    "sym:src/navigator_eventbus/brokers/rabbitmq/connection.py#RabbitMQConnection.wrap_callback",
    "sym:src/navigator_eventbus/brokers/rabbitmq/connection.py#RabbitMQConnection.ensure_exchange",
    "sym:src/navigator_eventbus/hooks/brokers/rabbitmq.py#RabbitMQBrokerHook.connect",
    "sym:src/navigator_eventbus/hooks/brokers/rabbitmq.py#RabbitMQBrokerHook._consumer_callback"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- Lazy import in the hook: `hooks/brokers/rabbitmq.py` must stay importable without `aiormq`
  (`tests/brokers/test_hook_rewire.py` constructs the hook). Import `RejectDelivery` inside the `except` block only.
- `test_hook_rewire.py::test_hook_rewire` asserts `"navigator_eventbus.brokers"` appears in the
  source of `RabbitMQBrokerHook.connect`. Keep the existing lazy import in `connect()`.
- `ensure_queue` binds only when `exchange_name` is truthy. Pass `routing_key or ""` (aiormq's own
  default), because the hook passes `BrokerHookConfig.routing_key`, which defaults to `""`.
- Forward `**kwargs` to `queue_declare(arguments=kwargs or None)`, matching how `create_exchange`
  forwards extra kwargs as `arguments` (`connection.py:144`).
- Ordinary callback exceptions keep the existing retry path unchanged (AC "ordinary callback retry behavior is preserved").

---

## Implementation Blueprint

### Steps (in order)
1. Add `RejectDelivery` after the imports — *why*: the hook and wrapper share one signal type defined beside the wrapper.
2. Add `ensure_queue` and `reject_message` right before `publish_message` — *why*: they belong with the other declaration helpers (`create_exchange`/`ensure_exchange`).
3. Add the `except RejectDelivery` clause before the generic handler in `wrap_callback` — *why*: Python matches `except` clauses in order, and the generic one would otherwise treat a rejection as a retryable failure.
4. Rewrite the hook failure path to raise — *why*: the wrapper owns settlement; the hook must never ack or nack.
5. Write the tests; run them plus the existing rewire and RabbitMQ tests.

### `src/navigator_eventbus/brokers/rabbitmq/connection.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c 'from ..wrapper import BaseWrapper' src/navigator_eventbus/brokers/rabbitmq/connection.py)
# AFTER — insert below `from ..wrapper import BaseWrapper` (verified: connection.py:25)


class RejectDelivery(Exception):
    """Internal signal: settle the current delivery with a non-requeued nack.

    Raised by a consumer callback (e.g. ``RabbitMQBrokerHook``) to tell
    :meth:`RabbitMQConnection.wrap_callback` — the sole owner of delivery
    settlement — to reject the message once, without acknowledging or
    republishing it (FEAT-431 Module 4).
    """
```
```python
# occurrences: 1 (verified: grep -c '    async def publish_message(' src/navigator_eventbus/brokers/rabbitmq/connection.py)
# BEFORE — insert above `    async def publish_message(` (verified: connection.py:159)
    async def ensure_queue(
        self,
        queue_name: str,
        exchange_name: Optional[str] = None,
        routing_key: Optional[str] = None,
        durable: bool = True,
        **kwargs: Any,
    ) -> None:
        """Declare *queue_name* and, when *exchange_name* is given, bind it.

        Args:
            queue_name: Queue to declare.
            exchange_name: Exchange to bind the queue to; when falsy the queue
                is only declared.
            routing_key: Binding key; ``None`` binds with ``""``.
            durable: Whether the queue survives a broker restart.
            **kwargs: Extra queue arguments (``x-dead-letter-exchange`` …).
        """
        await self.ensure_connection()
        await self._channel.queue_declare(
            queue=queue_name, durable=durable, arguments=kwargs or None
        )
        if exchange_name:
            await self._channel.queue_bind(
                queue=queue_name, exchange=exchange_name, routing_key=routing_key or ""
            )
        self.logger.info(f"Queue '{queue_name}' ensured (exchange={exchange_name!r}).")

    async def reject_message(self, message: Any, requeue: bool = False) -> None:
        """Negatively acknowledge *message* (``basic_nack``).

        Args:
            message: The ``aiormq.abc.DeliveredMessage`` to reject.
            requeue: Whether the broker should requeue the message.
        """
        # FILL IN: await self._channel.basic_nack(message.delivery_tag, requeue=requeue) and log at debug — bounded by spec §4 test_reject_message_nack
```
```python
# occurrences: 2 for `            except Exception as e:` (lines 84, 257) → disambiguate with the preceding 2 lines (unique: grep -c 'self.logger.debug(f"Message acknowledged: {message.delivery_tag}")' = 1)
# AFTER — insert between these lines (verified: connection.py:256-257):
#                 self.logger.debug(f"Message acknowledged: {message.delivery_tag}")
#             except Exception as e:
            except RejectDelivery as reject:
                # The callback asked for a non-requeued rejection: settle once
                # and return — no ack, no retry/republish (FEAT-431 Module 4).
                self.logger.warning(f"Message {message.delivery_tag} rejected: {reject}")
                await self.reject_message(message, requeue=False)
```

### `src/navigator_eventbus/hooks/brokers/rabbitmq.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c '                await self._connection.reject_message(message, requeue=False)' src/navigator_eventbus/hooks/brokers/rabbitmq.py)
# REPLACE lines 65-68 (`        except Exception as exc:` … `reject_message(message, requeue=False)`) with:
        except Exception as exc:
            self.logger.error(f"RabbitMQ callback error: {exc}")
            # The connection's wrap_callback owns settlement (FEAT-431 M4):
            # signal a non-requeued rejection instead of nacking here, so
            # the delivery is never both rejected and acknowledged.
            from navigator_eventbus.brokers.rabbitmq.connection import RejectDelivery

            raise RejectDelivery(str(exc)) from exc
```
Also update the module docstring with one line noting the FEAT-431 settlement change.

### `tests/brokers/test_rabbitmq_settlement.py` (CREATE)
```python
"""Tests for FEAT-431 Module 4: ensure_queue / reject_message / hook settlement (TASK-4563)."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiormq
import pytest

from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection
from navigator_eventbus.hooks.brokers.rabbitmq import RabbitMQBrokerHook
from navigator_eventbus.hooks.models import BrokerHookConfig

DSN = "amqp://guest:guest@localhost:5672/navigator"


@pytest.fixture
def mock_amqp_channel():
    ch = MagicMock()
    for name in ("exchange_declare", "queue_declare", "queue_bind", "basic_publish",
                 "basic_consume", "basic_qos", "basic_ack", "basic_nack"):
        setattr(ch, name, AsyncMock())
    return ch


@pytest.fixture
def conn(mock_amqp_channel):
    c = RabbitMQConnection(credentials=DSN)
    c._connection = SimpleNamespace(is_closed=False)
    c._channel = mock_amqp_channel
    return c


def _delivered(tag: int = 3):
    props = aiormq.spec.Basic.Properties(content_type="text/plain", headers={})
    return SimpleNamespace(body=b"payload", delivery_tag=tag, header=SimpleNamespace(properties=props),
                           delivery=SimpleNamespace(exchange="navigator", routing_key="#"))


def _hook(conn) -> RabbitMQBrokerHook:
    hook = RabbitMQBrokerHook(BrokerHookConfig(broker_type="rabbitmq", queue_name="q", exchange_name="ex"))
    hook._connection = conn
    return hook


async def test_ensure_queue_declares_and_binds(conn, mock_amqp_channel):
    await conn.ensure_queue("q", exchange_name="ex", routing_key="a.#")
    mock_amqp_channel.queue_declare.assert_awaited_once()
    mock_amqp_channel.queue_bind.assert_awaited_once_with(queue="q", exchange="ex", routing_key="a.#")


async def test_ensure_queue_no_binding(conn, mock_amqp_channel):
    # FILL IN: ensure_queue("q") → declare awaited once, queue_bind not awaited — spec §4 row
    ...


@pytest.mark.parametrize("requeue", [False, True])
async def test_reject_message_nack(conn, mock_amqp_channel, requeue):
    # FILL IN: reject_message(_delivered(), requeue=requeue) → basic_nack(3, requeue=requeue) once
    ...


async def test_rabbitmq_hook_connect(monkeypatch, mock_amqp_channel):
    """connect() completes (was AttributeError on ensure_queue)."""
    async def fake_connect(self):
        self._connection = SimpleNamespace(is_closed=False)
        self._channel = mock_amqp_channel

    monkeypatch.setattr(RabbitMQConnection, "connect", fake_connect)
    hook = RabbitMQBrokerHook(BrokerHookConfig(broker_type="rabbitmq", queue_name="q", exchange_name="ex"))
    await hook.connect()
    # FILL IN: assert queue_declare and queue_bind(queue="q", exchange="ex", ...) awaited


async def test_rabbitmq_hook_failure_settles_once(conn, mock_amqp_channel, monkeypatch):
    hook = _hook(conn)
    monkeypatch.setattr(hook, "_on_message", AsyncMock(side_effect=RuntimeError("boom")))
    wrapped = conn.wrap_callback(hook._consumer_callback, requeue_on_fail=True)
    await wrapped(_delivered())
    mock_amqp_channel.basic_nack.assert_awaited_once_with(3, requeue=False)
    # FILL IN: assert basic_ack and basic_publish NOT awaited — AC "settle exactly once … no ack or republish"


async def test_rabbitmq_hook_success_settles_once(conn, mock_amqp_channel, monkeypatch):
    # FILL IN: _on_message = AsyncMock(); run through the real wrapper; basic_ack once, basic_nack never
    ...


async def test_rabbitmq_callback_retry_preserved(conn, mock_amqp_channel):
    """Ordinary callback exceptions still nack + republish with x-retry incremented."""
    async def failing(message, body):
        raise ValueError("ordinary failure")

    await conn.wrap_callback(failing, max_retries=3)(_delivered())
    # FILL IN: basic_nack(3, requeue=False) once AND basic_publish once with headers["x-retry"] == "1"
```

### FILL IN checklist
- [ ] `reject_message` body — spec §4 `test_reject_message_nack`.
- [ ] Every `# FILL IN` test body/assertion above — spec §4 Module 4 rows; AC "Hook failures settle exactly once …".

---

## Acceptance Criteria

- [ ] `RabbitMQBrokerHook.connect()` and its reject path no longer raise `AttributeError`.
- [ ] Hook failures settle exactly once with a non-requeued nack, with no ack and no republish.
- [ ] Successful hook deliveries ack exactly once.
- [ ] Ordinary callback exceptions keep the existing retry behaviour.
- [ ] `hooks/brokers/rabbitmq.py` imports no RabbitMQ module at module level.
- [ ] Existing `tests/brokers/test_hook_rewire.py` and `tests/brokers/test_rabbitmq.py` pass.

---

## Validation Commands
- `pytest tests/brokers/test_rabbitmq_settlement.py -q`
- `pytest tests/brokers/test_hook_rewire.py -q`
- `pytest tests/brokers/test_rabbitmq.py -q`

---

## Test Specification

See the blueprint test file. It covers spec §4 rows `test_ensure_queue_declares_and_binds`,
`test_ensure_queue_no_binding`, `test_reject_message_nack`, `test_rabbitmq_hook_connect`,
`test_rabbitmq_hook_failure_settles_once`, `test_rabbitmq_hook_success_settles_once` and
`test_rabbitmq_callback_retry_preserved`.

---

## Agent Instructions

1. **Work in the feature worktree** — never on `dev`
   (`python -m scripts.sdd.ensure_worktree --slug brokers-unified-manager --feature-id FEAT-431`).
2. **Environment**: `source .venv/bin/activate`; if `pytest`/`aiormq` are missing run
   `uv pip install -e ".[dev,rabbitmq,sqs,redis]"` (no manifest change).
3. Read the spec; verify the Codebase Contract (re-grep anchors; line numbers may drift).
4. Set status `in-progress` in `sdd/tasks/index/brokers-unified-manager.json`, commit only that file.
5. Implement from the blueprint; complete every `# FILL IN:`; never change fixed signatures.
6. Run the Validation Commands; stage only the files listed above.
7. Close with `scripts/sdd/close_task.sh TASK-4563 brokers-unified-manager verified`, fill the Completion Note, commit.

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**:
**Date**:
**Notes**:

**Deviations from spec**: none
