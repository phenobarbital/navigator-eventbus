# TASK-4571: Brokers typing pass — base contracts, RabbitMQ family, broker hooks

**Feature**: FEAT-712 — Brokers typing pass (mypy-clean `brokers/` + `hooks/brokers/`)
**Spec**: `sdd/specs/fixgroup-17c252023ef4.spec.md`
**Status**: pending
**Priority**: medium
**Estimated effort**: M (2-4h)
**Depends-on**: none
**Assigned-to**: unassigned
**Discovered-from**: `issue:b12a71083ca3`

---

## Context

Spec §3 Modules 1 and 2. `uv run mypy src/` is a CI step but reports 77 errors
in the brokers layer (`issue:b12a71083ca3`). This task fixes the **base
contracts first** (so every backend override becomes LSP-valid) and then the
RabbitMQ family plus the three `hooks/brokers/*.py` hooks. TASK-4572 does Redis,
SQS and the pytest mypy gate on top of this task's `MessageCallback`.

---

## Scope

- Add `MessageCallback = Callable[..., Union[Awaitable[None], None]]` to
  `brokers/connection.py`; re-export it from `brokers/__init__.py`.
- Narrow `BaseConnection`: `_connection: Any`, `_monitor_task: Optional[asyncio.Task[None]]`,
  `get_connection() -> Any`, `consume_messages(..., callback: MessageCallback, **kwargs)`,
  `process_message(self, body: Any) -> Any`.
- `BrokerConsumer`: `callback: Optional[MessageCallback]` in `__init__`,
  `event_subscribe(callback: MessageCallback)`, `wrap_callback(callback: MessageCallback, ...) -> Callable[..., Awaitable[None]]`.
- `BrokerProducer.queue_event` / `publish_event`: `body: Any`.
- `rabbitmq/connection.py`: `_dsn: str` + `TypeError` guard in `__init__`; `connect()` through a
  local `connection`; `_require_delivery_tag()` module helper; `_require_channel()` in
  `publish_message`, `wrap_callback`, `consume_messages`; `publish_message(queue_name=None, routing_key=None)` → `ValueError`;
  `process_message(body: bytes, properties: Optional[Basic.Properties] = None) -> Any`;
  narrowed `x-retry` parsing; `getattr(message.delivery, "exchange"/"routing_key")`;
  `consume_messages(..., prefetch_count=1, **kwargs)` forwarding kwargs to `basic_consume`.
- `rabbitmq/consumer.py`: `MessageCallback` params; `channel = self._require_channel()` in `subscribe_to_events`.
- `rabbitmq/manager.py`: `wrap_callback(callback: MessageCallback, ...)`.
- `rabbitmq/downlink.py:58` and `rabbitmq/bridge.py:75`: explicit `Optional[...] = None`.
- `hooks/brokers/{rabbitmq,redis,sqs}.py`: `_connection: Optional["<X>Connection"]` via `TYPE_CHECKING`
  import; `RuntimeError` guard in `start_consuming()`; remove the `# type: ignore[attr-defined]` lines.
- Create `tests/brokers/test_typing_guards.py` with the RabbitMQ `TypeError` test and the hook guard test.

**NOT in scope**: `brokers/redis/*`, `brokers/sqs/*` (TASK-4572); the pytest mypy gate (TASK-4572);
any functional fix to the Redis hook's `consume_messages(stream_name=…)` call (spec §8).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/brokers/connection.py` | MODIFY | `MessageCallback`, typed attrs, narrowed abstract contract |
| `src/navigator_eventbus/brokers/consumer.py` | MODIFY | callback typing |
| `src/navigator_eventbus/brokers/producer.py` | MODIFY | `queue_event`/`publish_event` body `Any` |
| `src/navigator_eventbus/brokers/__init__.py` | MODIFY | re-export `MessageCallback` |
| `src/navigator_eventbus/brokers/rabbitmq/connection.py` | MODIFY | guards, helpers, LSP-valid overrides |
| `src/navigator_eventbus/brokers/rabbitmq/consumer.py` | MODIFY | `MessageCallback`, `_require_channel()` |
| `src/navigator_eventbus/brokers/rabbitmq/manager.py` | MODIFY | `wrap_callback` param type |
| `src/navigator_eventbus/brokers/rabbitmq/downlink.py` | MODIFY | explicit Optional |
| `src/navigator_eventbus/brokers/rabbitmq/bridge.py` | MODIFY | explicit Optional |
| `src/navigator_eventbus/hooks/brokers/rabbitmq.py` | MODIFY | typed `_connection`, guard |
| `src/navigator_eventbus/hooks/brokers/redis.py` | MODIFY | typed `_connection`, guard |
| `src/navigator_eventbus/hooks/brokers/sqs.py` | MODIFY | typed `_connection`, guard |
| `tests/brokers/test_typing_guards.py` | CREATE | guard tests (RabbitMQ creds TypeError, hook RuntimeError) |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.brokers.connection import BaseConnection   # verified: src/navigator_eventbus/brokers/connection.py:22
from navigator_eventbus.brokers.consumer import BrokerConsumer     # verified: src/navigator_eventbus/brokers/consumer.py:14
from navigator_eventbus.brokers.producer import BrokerProducer     # verified: src/navigator_eventbus/brokers/producer.py:30
from navigator_eventbus.brokers.exceptions import RejectDelivery   # verified: src/navigator_eventbus/brokers/rabbitmq/connection.py:26
from aiormq.abc import AbstractChannel, AbstractConnection         # verified: src/navigator_eventbus/brokers/rabbitmq/connection.py:19
from navigator_eventbus.hooks.brokers.base import BaseBrokerHook   # verified: src/navigator_eventbus/hooks/brokers/rabbitmq.py:15
from navigator_eventbus.hooks.models import BrokerHookConfig, HookType  # verified: src/navigator_eventbus/hooks/brokers/rabbitmq.py:16
from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection # verified: tests/brokers/test_rabbitmq.py:6
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/brokers/connection.py
class BaseConnection(ABC):
    self._connection = None                                 # :47
    self._monitor_task: Optional[Awaitable] = None          # :49
    def get_connection(self) -> Optional[Union[Callable, Awaitable]]   # :59
    async def consume_messages(self, queue_name: str, callback: Callable, **kwargs: Any) -> None  # :108
    async def process_message(self, body: bytes, properties: Any) -> str                          # :118

# src/navigator_eventbus/brokers/consumer.py
class BrokerConsumer(ABC):
    def __init__(self, callback: Optional[Union[Awaitable, Callable]] = None, **kwargs: Any)   # :19-22
    async def event_subscribe(self, queue_name: str, callback: Union[Callable, Awaitable])    # :37-41
    def wrap_callback(self, callback: Callable[[Any, str], Awaitable[None]], requeue_on_fail: bool = False, max_retries: int = 3) -> Callable  # :49-55

# src/navigator_eventbus/brokers/producer.py
    async def queue_event(self, body: str, queue_name: str, routing_key: Optional[str] = None, **kwargs: Any) -> None  # :143-149
    async def publish_event(self, body: str, queue_name: str, **kwargs: Any) -> None                                     # :168-173

# src/navigator_eventbus/brokers/rabbitmq/connection.py
    def _require_channel(self) -> AbstractChannel            # :163-171  (model)
    async def publish_message(self, body, queue_name: str, routing_key: str, **kwargs) -> None   # :216-222
    async def process_message(self, body: bytes, properties: "aiormq.spec.Basic.Properties") -> str  # :258-262
    def wrap_callback(self, callback: Callable[["aiormq.abc.DeliveredMessage", str], Awaitable[None]], requeue_on_fail=False, max_retries=3) -> Callable  # :293-298
    async def consume_messages(self, queue_name: str, callback: Callable[[...], Awaitable[None]], prefetch_count: int = 1) -> None  # :372-377

# src/navigator_eventbus/hooks/brokers/rabbitmq.py
class RabbitMQBrokerHook(BaseBrokerHook):
    self._connection = None                                  # :31
    async def connect(self) -> None                          # :33  (lazy import at :34)
    async def start_consuming(self) -> None                  # :55
# hooks/brokers/redis.py:25 and hooks/brokers/sqs.py:26 — same `self._connection = None` line.

# aiormq.abc.DeliveredMessage (site-packages, py.typed)
    delivery: spec.Basic.Deliver | spec.Basic.Return | GetResultType
    delivery_tag -> int | None
```

### Does NOT Exist
- ~~`navigator_eventbus.brokers.types`~~ — the alias lives in `brokers/connection.py`.
- ~~`BaseConnection._require_connection()`~~ — not in the base.
- ~~`RabbitMQConnection._exchange_name`~~ — only `RMQConsumer` has `_exchange_name`.
- ~~`aiormq.__version__`~~ — no such attribute.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/brokers/connection.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/consumer.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/producer.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/__init__.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/rabbitmq/connection.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/rabbitmq/consumer.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/rabbitmq/manager.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/rabbitmq/downlink.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/rabbitmq/bridge.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/hooks/brokers/rabbitmq.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/hooks/brokers/redis.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/hooks/brokers/sqs.py", "action": "MODIFY"},
    {"path": "tests/brokers/test_typing_guards.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/brokers/connection.py#BaseConnection",
    "sym:src/navigator_eventbus/brokers/connection.py#BaseConnection.consume_messages",
    "sym:src/navigator_eventbus/brokers/connection.py#BaseConnection.process_message",
    "sym:src/navigator_eventbus/brokers/consumer.py#BrokerConsumer",
    "sym:src/navigator_eventbus/brokers/consumer.py#BrokerConsumer.wrap_callback",
    "sym:src/navigator_eventbus/brokers/producer.py#BrokerProducer.queue_event",
    "sym:src/navigator_eventbus/brokers/rabbitmq/connection.py#RabbitMQConnection",
    "sym:src/navigator_eventbus/brokers/rabbitmq/connection.py#RabbitMQConnection._require_channel",
    "sym:src/navigator_eventbus/hooks/brokers/rabbitmq.py#RabbitMQBrokerHook"
  ]
}
```

---

## Implementation Notes

### Pattern to Follow
```python
# src/navigator_eventbus/brokers/rabbitmq/connection.py:163-171 — the guard to copy
def _require_channel(self) -> AbstractChannel:
    if self._channel is None:
        raise RuntimeError("RabbitMQ channel is not open")
    return self._channel
```

### Key Constraints
- Base contracts first: run mypy after Module 1 edits; the override errors in the
  RabbitMQ files must disappear *because* the base changed, not because of ignores.
- Never add `# type: ignore[override]` / `[union-attr]` (spec AC-5).
- Assign untyped factory results to a local before storing on `self`
  (`connection = await asyncio.wait_for(aiormq.connect(self._dsn), ...)`).
- The hooks keep their **lazy runtime import** inside `connect()`; the typed
  import is `TYPE_CHECKING`-only so optional deps are not required at import time.
- Keep every pre-existing `# type: ignore[attr-defined]` on `self.logger.notice(...)` only.

### References in Codebase
- `src/navigator_eventbus/brokers/rabbitmq/connection.py:163` — `_require_channel()`
- `src/navigator_eventbus/brokers/rabbitmq/manager.py:41-62` — `wrap_callback` override that must stay LSP-valid

---

## Implementation Blueprint

### Steps (in order)
1. Edit `brokers/connection.py` (alias + attrs + abstract signatures) — *why*: every backend error of kind `override`/`arg-type` is measured against this contract.
2. Edit `brokers/consumer.py`, `brokers/producer.py`, `brokers/__init__.py` — *why*: consumers/producers must speak the same `MessageCallback`/`Any` body before the backends are typed.
3. Run `mypy src/navigator_eventbus/brokers/rabbitmq` and fix `rabbitmq/connection.py` top-down (init → connect → publish → process → wrap → consume) — *why*: the guard helper and `routing_key`/`queue_name` ValueError decisions are fixed by the spec (§7).
4. Fix `rabbitmq/consumer.py`, `manager.py`, `downlink.py`, `bridge.py` — *why*: they inherit the new signatures; only parameter types and the `_require_channel()` local change.
5. Type the three hooks — *why*: same pattern thrice; remove the ignores.
6. Write `tests/brokers/test_typing_guards.py`; run the Validation Commands.

### `src/navigator_eventbus/brokers/connection.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c '        self._monitor_task: Optional\[Awaitable\] = None' src/navigator_eventbus/brokers/connection.py)
# REPLACE line 47 `self._connection = None` and line 49 (verified: connection.py:47,49) with:
        self._connection: Any = None
        self._monitor_task: Optional["asyncio.Task[None]"] = None

# ADD after the imports (verified anchor: `from .serializers import DataSerializer`, connection.py:19):
MessageCallback = Callable[..., Union[Awaitable[None], None]]
"""A sync or async message handler invoked as ``callback(message, body)``."""

# REPLACE signatures (verified: connection.py:59, :108-115, :118):
    def get_connection(self) -> Any:
    async def consume_messages(self, queue_name: str, callback: MessageCallback, **kwargs: Any) -> None:
    async def process_message(self, body: Any) -> Any:
        """Decode one raw broker payload. Backends add their own optional metadata parameter."""
```
**Why this shape**: `Any` for `_connection` is honest (three unrelated client types); `process_message(body)` is the narrowest common call so each backend's `(body, properties=None)` widening is LSP-valid.

### `src/navigator_eventbus/brokers/consumer.py` (MODIFY)
```python
# occurrences: 1 each (verified: consumer.py:21, :40, :54)
from .connection import MessageCallback
    def __init__(self, callback: Optional[MessageCallback] = None, **kwargs: Any) -> None:
    async def event_subscribe(self, queue_name: str, callback: MessageCallback) -> None:
    def wrap_callback(self, callback: MessageCallback, requeue_on_fail: bool = False,
                      max_retries: int = 3) -> Callable[..., Awaitable[None]]:
```
**Why**: an `Awaitable` is never a callback; the runtime already branches on `iscoroutinefunction`.

### `src/navigator_eventbus/brokers/producer.py` (MODIFY)
```python
# occurrences: 2 (verified: grep -c '        body: str,' src/navigator_eventbus/brokers/producer.py) → lines 145 (queue_event) and 170 (publish_event)
# FILL IN: disambiguate — both become `        body: Any,` (the surrounding `async def queue_event(` / `async def publish_event(` lines identify them)
```
**Why**: `publish_message` already accepts `Union[str, list, dict, Any]`; `downlink.py:98` passes a dict.

### `src/navigator_eventbus/brokers/rabbitmq/connection.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c '    def _require_channel(self) -> AbstractChannel:' …/rabbitmq/connection.py)
# ADD module-level helper before `class RabbitMQConnection` (verified: connection.py:32):
def _require_delivery_tag(message: "aiormq.abc.DeliveredMessage") -> int:
    """Return the delivery tag of *message*; raise RuntimeError when absent."""
    tag = message.delivery_tag
    if tag is None:
        raise RuntimeError("Delivered message carries no delivery_tag")
    return tag

# __init__ (verified: connection.py:49): keep the DSN default, add the guard
        if credentials is not None and not isinstance(credentials, str):
            raise TypeError("RabbitMQConnection expects a DSN string as credentials")
        self._dsn: str = credentials if credentials is not None else default_rabbitmq_dsn

# connect() (verified: connection.py:63-67): go through a local
                connection = await asyncio.wait_for(aiormq.connect(self._dsn), timeout=self._timeout)
                self._connection = connection
                self.reconnect_attempts = 0
                self._channel = await connection.channel()

# publish_message (verified: connection.py:216): LSP-valid signature + guard
    async def publish_message(self, body: Union[str, list, dict, Any], queue_name: Optional[str] = None,
                              routing_key: Optional[str] = None, **kwargs: Any) -> None:
        if queue_name is None or routing_key is None:
            raise ValueError("publish_message() requires queue_name (exchange) and routing_key")
        ...
        channel = self._require_channel()
        try:
            await channel.basic_publish(...)

# process_message (verified: connection.py:258): properties optional
    async def process_message(self, body: bytes, properties: Optional["aiormq.spec.Basic.Properties"] = None) -> Any:
        props = properties or aiormq.spec.Basic.Properties()

# wrap_callback (verified: connection.py:293): MessageCallback; channel local; typed tag; getattr on delivery
    def wrap_callback(self, callback: MessageCallback, requeue_on_fail: bool = False, max_retries: int = 3,
                      ) -> Callable[["aiormq.abc.DeliveredMessage"], Awaitable[None]]:
        async def wrapped_callback(message: "aiormq.abc.DeliveredMessage") -> None:
            channel = self._require_channel()
            delivery_tag = _require_delivery_tag(message)
            ...  # FILL IN: replace every `message.delivery_tag` / `self._channel` in the body — bounded by AC-5 (no ignores)
            raw_retry = headers.get("x-retry", 0)
            if isinstance(raw_retry, bytes):
                retry_count = int(raw_retry.decode())
            elif isinstance(raw_retry, (str, int, float)):
                retry_count = int(raw_retry)
            else:
                retry_count = 0
            ...
            exchange = getattr(message.delivery, "exchange", "") or ""
            routing_key = getattr(message.delivery, "routing_key", "") or ""

# consume_messages (verified: connection.py:372): **kwargs forwarded
    async def consume_messages(self, queue_name: str, callback: MessageCallback, prefetch_count: int = 1,
                               **kwargs: Any) -> None:
        channel = self._require_channel()
        ... basic_consume(queue=queue_name, consumer_callback=self.wrap_callback(callback), **kwargs)
```
**Why this shape**: the spec fixes the three behaviour tightenings (§7); everything else is a pure type narrowing with identical runtime paths.

### `src/navigator_eventbus/hooks/brokers/rabbitmq.py` (MODIFY) — same for `redis.py`, `sqs.py`
```python
# occurrences: 1 (verified: grep -c '        self._connection = None' src/navigator_eventbus/hooks/brokers/rabbitmq.py)
from typing import TYPE_CHECKING, Any, Optional
if TYPE_CHECKING:
    from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection
...
        self._connection: Optional["RabbitMQConnection"] = None
...
    async def start_consuming(self) -> None:
        if self._connection is None:
            raise RuntimeError("RabbitMQBrokerHook.connect() must run before start_consuming()")
        await self._connection.consume_messages(...)   # no type: ignore
```
**Why**: the lazy import in `connect()` stays (optional deps); the typed name exists only for the checker.

### `tests/brokers/test_typing_guards.py` (CREATE)
```python
"""Guard tests for the FEAT-712 typing pass (TASK-4571; TASK-4572 appends Redis/SQS)."""
import pytest

from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection
from navigator_eventbus.hooks.brokers.rabbitmq import RabbitMQBrokerHook
from navigator_eventbus.hooks.models import BrokerHookConfig


def test_rabbitmq_rejects_non_str_credentials() -> None:
    with pytest.raises(TypeError):
        RabbitMQConnection(credentials={"host": "x"})


async def test_rabbitmq_hook_start_consuming_before_connect_raises() -> None:
    hook = RabbitMQBrokerHook(BrokerHookConfig(name="h", broker_type="rabbitmq"))  # FILL IN: required BrokerHookConfig fields — bounded by hooks/models.py
    with pytest.raises(RuntimeError):
        await hook.start_consuming()
```

### FILL IN checklist
- [ ] `producer.py` — both `body: str,` → `body: Any,` (lines 145 and 170); bounded by AC-6.
- [ ] `rabbitmq/connection.py::wrap_callback` — replace all `self._channel` / `message.delivery_tag` uses in the body with the locals; bounded by AC-5.
- [ ] `test_typing_guards.py` — the exact `BrokerHookConfig(...)` constructor arguments; bounded by `src/navigator_eventbus/hooks/models.py`.

---

## Acceptance Criteria

- [ ] `mypy src/navigator_eventbus/brokers src/navigator_eventbus/hooks/brokers 2>&1 | grep -E 'brokers/(connection|consumer|producer|__init__|rabbitmq/)|hooks/brokers/' | grep -c error` → `0`
- [ ] No new `# type: ignore` (AC-5).
- [ ] All Validation Commands pass.
- [ ] `ruff check src/navigator_eventbus/brokers src/navigator_eventbus/hooks/brokers tests/brokers` passes.
- [ ] `python -c "from navigator_eventbus.brokers import MessageCallback"` works (AC-7).

## Validation Commands

- `pytest tests/brokers/test_typing_guards.py -q`
- `pytest tests/brokers/test_rabbitmq.py -q`
- `pytest tests/brokers/test_rabbitmq_settlement.py -q`
- `pytest tests/brokers/test_consumer_fixes.py -q`
- `pytest tests/brokers/test_rmq_manager.py -q`
- `pytest tests/brokers/test_producer.py -q`
- `pytest tests/brokers/test_connection.py -q`
- `pytest tests/brokers/test_manager.py -q`
- `pytest tests/brokers/test_hook_rewire.py -q`
- `pytest tests/test_hooks_imports.py -q`

---

## Test Specification

See the `tests/brokers/test_typing_guards.py` block above. Existing RabbitMQ tests
(`test_rabbitmq_settlement.py`, `test_consumer_fixes.py`, `test_rmq_manager.py`) are
the behavioural regression net and must pass unchanged.

---

## Agent Instructions

1. **Work in the feature worktree** (`python -m scripts.sdd.ensure_worktree --slug fixgroup-17c252023ef4 --feature-id FEAT-712`).
2. Read the spec §3 M1/M2, §6 and §7.
3. Verify the Codebase Contract anchors (`grep -n`) before editing.
4. Mark `in-progress` in `sdd/tasks/index/fixgroup-17c252023ef4.json`.
5. Implement in the Steps order; run mypy after step 2 and after step 4.
6. Run the Validation Commands; stage only the listed files.
7. Close with `scripts/sdd/close_task.sh TASK-4571 fixgroup-17c252023ef4 verified` and fill the Completion Note.

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**:
**Date**:
**Notes**:

**Deviations from spec**: none
