---
# SDD flow type and base branch (FEAT-145).
type: feature
base_branch: dev
projects: [navigator-eventbus]
tags: [brokers, typing, mypy, tech-debt, ledger-fix]
---

<!-- LANGUAGE: This document MUST be written entirely in English (proper nouns keep native spelling). -->

# Feature Specification: Brokers typing pass — mypy-clean `brokers/` + `hooks/brokers/`

**Feature ID**: FEAT-712
**Date**: 2026-10-21
**Author**: Jesus Lara (via `/sdd-fix issue:b12a71083ca3`)
**Status**: approved
**Target version**: patch after the current `version.py` (no public-API break)

> **Origin**: ledger issue `issue:b12a71083ca3` (tech_debt, low) discovered
> from `task:TASK-4570` (FEAT-431 review cleanups). Fix group
> `fixgroup:17c252023ef4`, SDD lane (no file scope), no open parent spec.

---

## 1. Motivation & Business Requirements

### Problem Statement

`uv run mypy src/` is already a CI step (`.github/workflows/ci.yml`, "Type check
(mypy)"), but at `61f5f97` it reports **81 errors in 14 files**, 77 of them in
the `brokers` subpackage ported from `navigator.brokers` (FEAT-316) plus
`hooks/brokers/rabbitmq.py`. The three sibling hook files share the same
pattern (`hooks/brokers` alone: 3 errors). CI's type gate is therefore red and
cannot catch regressions in the brokers layer, which FEAT-431 just extended
(`BrokerManager`, `RMQManager`, `RedisManager`, `SQSManager`).

The 77 errors fall into six patterns:

1. **Nullable client used without a guard** (`union-attr`): `RedisConnection._connection`
   (`Optional[aioredis.Redis]`), `RabbitMQConnection._channel` / `._connection`,
   `SQSConnection._connection` / `._session` (typed as `None` by inference).
   `rabbitmq/connection.py` already has `_require_channel()` as the model.
2. **Loose callback typing**: `BrokerConsumer.event_subscribe` /
   `subscribe_to_events` accept `Union[Callable, Awaitable]` — an `Awaitable` is
   not callable, so passing it to `consume_messages` / `wrap_callback`
   (`Callable[[msg, str], Awaitable[None]]`) is an `arg-type` error. Every
   backend already supports sync *and* async callables at runtime
   (`asyncio.iscoroutinefunction` branches).
3. **LSP-incompatible overrides** of `BaseConnection.consume_messages`,
   `publish_message` and `process_message` (narrower callback, required params
   where the base has defaults, different arity).
4. **`SQSManager(BrokerManager, SQSConsumer)` MRO clash**: `SQSConnection.wrap_callback`
   and `RedisConsumer.wrap_callback` drop the `requeue_on_fail` / `max_retries`
   parameters declared by `BrokerConsumer.wrap_callback`.
5. **Implicit `Optional` defaults** (`rabbitmq/downlink.py:58`, `rabbitmq/bridge.py:75`)
   and `BrokerProducer.queue_event(body: str)` called with a `dict` (`downlink.py:99`).
6. **Hook attribute declared `None`** (`hooks/brokers/{rabbitmq,redis,sqs}.py`):
   `self._connection = None` then assigned a connection object, with
   `# type: ignore[attr-defined]` scattered over every use.

Plus library-typing mismatches in `redis/connection.py` (`xadd` field map,
`xread` stream map — `dict` is invariant), `aiormq` nullable `delivery_tag`,
`GetEmpty` lacking `exchange`/`routing_key`, and AMQP header values typed as
the full `FieldValue` union.

### Goals

- `mypy src/navigator_eventbus/brokers src/navigator_eventbus/hooks/brokers`
  reports **0 errors** under the repo's `[tool.mypy]` config.
- One shared, honest callback type for the whole layer (sync or async
  callable) replacing the `Union[Callable, Awaitable]` lie.
- Every nullable client/channel access goes through a `_require_*()` guard
  that raises `RuntimeError` with a clear message instead of `AttributeError`
  on `None`.
- Base-class contracts (`BaseConnection`, `BrokerConsumer`, `BrokerProducer`)
  become the narrowest common contract so backend overrides are LSP-valid
  without `# type: ignore[override]`.
- A pytest-level regression gate keeps the two packages mypy-clean even while
  the rest of `src/` is still red.

### Non-Goals (explicitly out of scope)

- The 4 remaining non-brokers mypy errors (`evb.py:304`, `backends/redis_streams.py:741`,
  …) — tracked by other ledger issues (`issue:9fcac7941c4f` lists `evb.py`).
- Any runtime behaviour change beyond what a type-driven guard implies (see §7
  for the three deliberate, documented tightenings).
- Fixing pre-existing functional bugs noticed while typing (e.g. the Redis hook
  calling `consume_messages(stream_name=…)` with no `queue_name`; the msgpack
  body path on SQS/Redis). These are noted in §8 for separate issues.
- Enabling `strict = true` or `warn_unused_ignores` in `[tool.mypy]`.

---

## 2. Architectural Design

### Overview

A single typing pass across the brokers layer, applied **base first, backends
second**:

1. `brokers/connection.py` gains a module-level alias
   `MessageCallback = Callable[..., Union[Awaitable[None], None]]` and the base
   contract is narrowed to what every backend can honour: `consume_messages(queue_name, callback: MessageCallback, **kwargs)`,
   `process_message(body: Any) -> Any`, `_connection: Any`, `_monitor_task: Optional[asyncio.Task[None]]`.
2. `brokers/consumer.py` and `brokers/producer.py` adopt the alias
   (`BrokerConsumer.__init__`, `event_subscribe`, `wrap_callback`) and widen
   `queue_event`/`publish_event` bodies to `Any` (the body is serialized by
   `publish_message`, which already accepts `Union[str, list, dict, Any]`).
3. Each backend (`rabbitmq`, `redis`, `sqs`) gets a `_require_<client>()` guard
   mirroring the existing `RabbitMQConnection._require_channel()`, adopts
   `MessageCallback`, and makes its overrides LSP-valid by (a) accepting
   `**kwargs: Any` where the base does, (b) giving `queue_name`/`routing_key`
   `Optional[...] = None` defaults that raise `ValueError` when missing, and
   (c) accepting the interface-parity `requeue_on_fail`/`max_retries`
   parameters on `wrap_callback` even where the backend has no retry/republish.
4. The three `hooks/brokers/*.py` hooks declare
   `self._connection: Optional[<Backend>Connection]` under `TYPE_CHECKING`
   (the runtime import stays lazy so optional deps are not required), guard
   `start_consuming()` with a `RuntimeError`, and drop the `type: ignore`s.
5. `tests/brokers/test_mypy_clean.py` runs mypy over the two packages as a
   pytest test (skipped when mypy is not importable), so the gate rides the
   existing `uv run pytest tests/` CI step even while `uv run mypy src/`
   stays red for unrelated files.

### Component Diagram
```
BaseConnection (MessageCallback, _connection: Any, process_message(body))
   │
   ├─ BrokerProducer (queue_event/publish_event body: Any)
   │     └─ BrokerManager
   ├─ BrokerConsumer (callback: MessageCallback; wrap_callback(cb, requeue_on_fail, max_retries))
   │
   ├─ RabbitMQConnection ── _require_channel() ── RMQConsumer ── RMQManager
   │        │                                          └── EmployeeEventsBridge
   │        └── MQTTDownlinkPublisher (via RMQProducer)
   ├─ RedisConnection ──── _require_client()  ── RedisConsumer ── RedisManager
   └─ SQSConnection ────── _require_resource() ── SQSConsumer ── SQSManager

hooks/brokers/{rabbitmq,redis,sqs}.py : _connection: Optional[<X>Connection]  (TYPE_CHECKING import)
tests/brokers/test_mypy_clean.py      : subprocess mypy gate over both packages
```

### Integration Points

| Existing Component | Integration Type | Notes |
|---|---|---|
| `BaseConnection` (`brokers/connection.py`) | modifies | alias + narrowed abstract contract |
| `BrokerConsumer` (`brokers/consumer.py`) | modifies | callback typing |
| `BrokerProducer` (`brokers/producer.py`) | modifies | `queue_event`/`publish_event` body `Any` |
| `RabbitMQConnection`, `RMQConsumer`, `RMQManager` | modifies | guards + typing |
| `MQTTDownlinkPublisher`, `EmployeeEventsBridge` | modifies | explicit `Optional` defaults |
| `RedisConnection`, `RedisConsumer`, `RedisManager` | modifies | `_require_client()`, redis.typing maps |
| `SQSConnection`, `SQSConsumer`, `SQSManager` | modifies | `_require_resource()`, `Optional[Session]` |
| `RabbitMQBrokerHook`, `RedisBrokerHook`, `SQSBrokerHook` | modifies | typed `_connection`, guard |
| `.github/workflows/ci.yml` | unchanged | `uv run mypy src/` already present; the pytest gate is the new enforcement |

### Data Models

No new Pydantic models. One type alias:

```python
# src/navigator_eventbus/brokers/connection.py
MessageCallback = Callable[..., Union[Awaitable[None], None]]
"""A sync or async message handler. Backends call it as ``callback(message, body)``
(RabbitMQ/SQS) or ``callback(message_id, body)`` (Redis) and await the result
when it is a coroutine."""
```

### New Public Interfaces

- `navigator_eventbus.brokers.connection.MessageCallback` (re-exported from
  `navigator_eventbus.brokers`).
- `RedisConnection._require_client() -> aioredis.Redis`,
  `SQSConnection._require_resource() -> Any` (private, mirror of
  `RabbitMQConnection._require_channel()`).

---

## 3. Module Breakdown

#### Delegation-eligible modules

| Module | Eligible? | Decided patterns / exact contracts | Why not (if no) |
|---|---|---|---|
| M1: base contracts | no | — | edits span three files and every signature below must be applied verbatim; the type-check feedback loop needs the thinking model |
| M2: RabbitMQ family + hooks | no | — | 22 errors across `wrap_callback`'s retry path; AMQP typing judgement calls |
| M3: Redis + SQS families + gate | no | — | redis.typing invariance; aioboto3 untyped resources |

### Module 1: Base contracts
- **Path**: `src/navigator_eventbus/brokers/connection.py`, `src/navigator_eventbus/brokers/consumer.py`, `src/navigator_eventbus/brokers/producer.py`, `src/navigator_eventbus/brokers/__init__.py`
- **Responsibility**: define `MessageCallback`; narrow the abstract contract so backend overrides are LSP-valid.
- **Depends on**: nothing.
- **Interface Skeleton**:
  ```python
  # src/navigator_eventbus/brokers/connection.py  (modifies :49, :59, :108-115, :118)
  MessageCallback = Callable[..., Union[Awaitable[None], None]]

  class BaseConnection(ABC):
      _connection: Any            # was inferred as None (line 47 assigns None)
      _monitor_task: Optional["asyncio.Task[None]"]   # was Optional[Awaitable]

      def get_connection(self) -> Any:
          """Return the underlying broker connection object (raises RuntimeError when unset)."""

      @abstractmethod
      async def consume_messages(self, queue_name: str, callback: MessageCallback, **kwargs: Any) -> None:
          """Consume messages from the Broker Service."""

      @abstractmethod
      async def process_message(self, body: Any) -> Any:
          """Decode one raw broker payload; backends add their own optional metadata parameter."""

  # src/navigator_eventbus/brokers/consumer.py  (modifies :21, :40, :49-55)
  class BrokerConsumer(ABC):
      def __init__(self, callback: Optional[MessageCallback] = None, **kwargs: Any) -> None: ...
      @abstractmethod
      async def event_subscribe(self, queue_name: str, callback: MessageCallback) -> None: ...
      @abstractmethod
      def wrap_callback(self, callback: MessageCallback, requeue_on_fail: bool = False,
                        max_retries: int = 3) -> Callable[..., Awaitable[None]]: ...

  # src/navigator_eventbus/brokers/producer.py  (modifies :145, :170)
  class BrokerProducer(BaseConnection, ABC):
      async def queue_event(self, body: Any, queue_name: str, routing_key: Optional[str] = None, **kwargs: Any) -> None: ...
      async def publish_event(self, body: Any, queue_name: str, **kwargs: Any) -> None: ...
  ```

### Module 2: RabbitMQ family + broker hooks
- **Path**: `src/navigator_eventbus/brokers/rabbitmq/{connection,consumer,manager,downlink,bridge}.py`, `src/navigator_eventbus/hooks/brokers/{rabbitmq,redis,sqs}.py`
- **Responsibility**: None-guards via `_require_channel()`, typed AMQP helpers, LSP-valid overrides, explicit Optionals, typed hook attribute.
- **Depends on**: Module 1 (`MessageCallback`).
- **Interface Skeleton**:
  ```python
  # src/navigator_eventbus/brokers/rabbitmq/connection.py
  def _require_delivery_tag(message: "aiormq.abc.DeliveredMessage") -> int:
      """Return the delivery tag, raising RuntimeError when the broker sent none."""

  class RabbitMQConnection(BaseConnection):
      _dsn: str   # __init__ raises TypeError when credentials is not a str
      async def connect(self) -> None: ...            # local `connection` var, no Optional access
      async def publish_message(self, body: Union[str, list, dict, Any], queue_name: Optional[str] = None,
                                routing_key: Optional[str] = None, **kwargs: Any) -> None:
          """Raises ValueError when queue_name (exchange) or routing_key is None."""
      async def process_message(self, body: bytes,
                                properties: Optional["aiormq.spec.Basic.Properties"] = None) -> Any: ...
      def wrap_callback(self, callback: MessageCallback, requeue_on_fail: bool = False,
                        max_retries: int = 3) -> Callable[["aiormq.abc.DeliveredMessage"], Awaitable[None]]: ...
      async def consume_messages(self, queue_name: str, callback: MessageCallback,
                                 prefetch_count: int = 1, **kwargs: Any) -> None:
          """Extra kwargs are forwarded to ``basic_consume`` (same as subscribe_to_events)."""

  # src/navigator_eventbus/brokers/rabbitmq/consumer.py — callback params become MessageCallback;
  #   subscribe_to_events uses `channel = self._require_channel()`.
  # src/navigator_eventbus/brokers/rabbitmq/manager.py — wrap_callback(callback: MessageCallback, ...)
  # src/navigator_eventbus/brokers/rabbitmq/downlink.py:58 — credentials: Optional[Union[str, dict]] = None
  # src/navigator_eventbus/brokers/rabbitmq/bridge.py:75 — accepted_schema_versions: Optional[set] = None

  # src/navigator_eventbus/hooks/brokers/rabbitmq.py (same shape for redis.py / sqs.py)
  if TYPE_CHECKING:
      from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection
  class RabbitMQBrokerHook(BaseBrokerHook):
      _connection: Optional["RabbitMQConnection"]
      async def start_consuming(self) -> None:
          """Raises RuntimeError when connect() has not run."""
  ```

### Module 3: Redis + SQS families + regression gate
- **Path**: `src/navigator_eventbus/brokers/redis/{connection,consumer,manager}.py`, `src/navigator_eventbus/brokers/sqs/{connection,consumer,manager}.py`, `tests/brokers/test_mypy_clean.py`
- **Responsibility**: `_require_client()` / `_require_resource()` guards, redis.typing-correct maps, interface-parity `wrap_callback`, `**kwargs` on `consume_messages`, pytest mypy gate.
- **Depends on**: Module 1 (`MessageCallback`).
- **Interface Skeleton**:
  ```python
  # src/navigator_eventbus/brokers/redis/connection.py
  class RedisConnection(BaseConnection):
      def _require_client(self) -> aioredis.Redis:
          """Return the open client; raise RuntimeError when connect() has not run."""
      async def connect(self) -> None:
          """Raises TypeError when credentials is not a dict (Redis kwargs)."""
      async def publish_message(...)  # fields: Dict[FieldT, EncodableT]; packed_body: Union[bytes, str]
      async def process_message(self, message_data: Dict[Any, Any]) -> Any:
          """Returns None immediately when the entry has no ``body`` field."""
      async def consume_messages(self, queue_name: Optional[str], callback: MessageCallback,
                                 count: int = 1, block: int = 1000, **kwargs: Any) -> None: ...
      async def reclaim_pending_messages(self, queue_name: str, callback: MessageCallback, *, ...) -> int: ...
      async def xread(self, streams: Dict[str, str], ...) -> list:  # converted to Dict[KeyT, StreamIdT]

  # src/navigator_eventbus/brokers/redis/consumer.py
  class RedisConsumer(RedisConnection, BrokerConsumer):
      def wrap_callback(self, callback: MessageCallback, requeue_on_fail: bool = False,
                        max_retries: int = 3) -> Callable[[Any, Any], Awaitable[None]]:
          """requeue_on_fail/max_retries are accepted for interface parity and ignored (no republish on Streams)."""

  # src/navigator_eventbus/brokers/sqs/connection.py
  class SQSConnection(BaseConnection):
      _session: Optional["aioboto3.Session"]
      _connection: Any     # aioboto3 resources are dynamic
      def _require_resource(self) -> Any:
          """Return the SQS resource; raise RuntimeError when connect() has not run."""
      async def publish_message(self, body: Union[str, list, dict, Any], queue_name: Optional[str] = None,
                                **kwargs: Any) -> None:
          """Raises ValueError when queue_name is None."""
      async def process_message(self, body: str, attributes: Optional[dict] = None) -> Any: ...
      def wrap_callback(self, callback: MessageCallback, requeue_on_fail: bool = False,
                        max_retries: int = 3) -> Callable[[Any], Awaitable[None]]: ...
      async def consume_messages(self, queue_name: str, callback: MessageCallback, max_messages: int = 10,
                                 wait_time: int = 10, idle_sleep: int = 5, **kwargs: Any) -> None: ...
      async def consume_message(self, queue_name: str, callback: Optional[MessageCallback] = None,
                                wait_time: int = 5) -> Optional[dict]: ...

  # tests/brokers/test_mypy_clean.py
  def test_brokers_packages_are_mypy_clean() -> None:
      """Runs `python -m mypy src/navigator_eventbus/brokers src/navigator_eventbus/hooks/brokers`
      and asserts exit code 0; skipped when mypy is not importable."""
  ```

---

## 4. Test Specification

### Unit Tests
| Test | Module | Description |
|---|---|---|
| `test_brokers_packages_are_mypy_clean` | M3 | subprocess mypy over both packages exits 0 |
| `test_require_client_raises_before_connect` | M3 | `RedisConnection()._require_client()` → `RuntimeError` |
| `test_require_resource_raises_before_connect` | M3 | `SQSConnection(credentials={...})._require_resource()` → `RuntimeError` |
| `test_rabbitmq_rejects_non_str_credentials` | M2 | `RabbitMQConnection(credentials={"host": "x"})` → `TypeError` |
| `test_hook_start_consuming_before_connect_raises` | M2 | `RabbitMQBrokerHook.start_consuming()` without `connect()` → `RuntimeError` |
| existing `tests/brokers/*` | all | unchanged behaviour (settlement, consumer fixes, managers, producers) |

### Integration Tests
| Test | Description |
|---|---|
| `tests/brokers/test_manager_integration.py`, `test_redis_integration.py` | unchanged; run when a live broker is available |

### Validation Contract

tests:
- tests/brokers/test_mypy_clean.py
- tests/brokers/test_typing_guards.py
- tests/brokers/test_connection.py
- tests/brokers/test_consumer_fixes.py
- tests/brokers/test_hook_rewire.py
- tests/brokers/test_manager.py
- tests/brokers/test_manager_integration.py
- tests/brokers/test_producer.py
- tests/brokers/test_rabbitmq.py
- tests/brokers/test_rabbitmq_settlement.py
- tests/brokers/test_redis_consumer.py
- tests/brokers/test_redis_integration.py
- tests/brokers/test_redis_manager.py
- tests/brokers/test_redis_reclaim.py
- tests/brokers/test_rmq_manager.py
- tests/brokers/test_serializers.py
- tests/brokers/test_sqs_manager.py
- tests/brokers/test_sqs.py
- tests/test_hooks_imports.py
markers: none

### Test Data / Fixtures

Existing fixtures in `tests/brokers/` (`mock_amqp_channel`, `DSN`, `CREDS`) are
reused; no new fixtures.

---

## 5. Acceptance Criteria

- [ ] AC-1 `mypy src/navigator_eventbus/brokers src/navigator_eventbus/hooks/brokers` → `Success: no issues found`.
- [ ] AC-2 `mypy src/` error count drops from 81 to ≤ 4, and none of the remaining errors is under `brokers/` or `hooks/brokers/`.
- [ ] AC-3 `pytest tests/brokers tests/test_hooks_imports.py -q -m "not redis"` passes.
- [ ] AC-4 `ruff check src/navigator_eventbus/brokers src/navigator_eventbus/hooks/brokers tests/brokers` passes.
- [ ] AC-5 No `# type: ignore[override]` or `# type: ignore[union-attr]` is introduced; the only surviving ignores are the pre-existing `logger.notice` `attr-defined` ones.
- [ ] AC-6 Public call sites keep working unchanged: `queue_event(dict, …)`, `publish_event(dict, …)`, `RMQConsumer(credentials=DSN, …)`, `SQSConsumer(credentials={…})`, `RedisConsumer(queue_name=…)`.
- [ ] AC-7 `navigator_eventbus.brokers.MessageCallback` is importable.

---

## 6. Codebase Contract

> Verified against: `61f5f97` (dev).

### Verified Imports
```python
from navigator_eventbus.brokers.connection import BaseConnection          # verified: src/navigator_eventbus/brokers/connection.py:22
from navigator_eventbus.brokers.consumer import BrokerConsumer            # verified: src/navigator_eventbus/brokers/consumer.py:14
from navigator_eventbus.brokers.producer import BrokerProducer            # verified: src/navigator_eventbus/brokers/producer.py:30
from navigator_eventbus.brokers.manager import BrokerManager              # verified: src/navigator_eventbus/brokers/manager.py:22
from navigator_eventbus.brokers.serializers import DataSerializer         # verified: src/navigator_eventbus/brokers/serializers.py:69
from navigator_eventbus.brokers.exceptions import RejectDelivery          # verified: src/navigator_eventbus/brokers/rabbitmq/connection.py:26
from navigator_eventbus.brokers.wrapper import BaseWrapper                # verified: src/navigator_eventbus/brokers/rabbitmq/connection.py:27
from aiormq.abc import AbstractChannel, AbstractConnection                # verified: src/navigator_eventbus/brokers/rabbitmq/connection.py:19
from redis import asyncio as aioredis                                     # verified: src/navigator_eventbus/brokers/redis/connection.py:19
from redis.typing import EncodableT, FieldT, KeyT, StreamIdT              # verified: python -c import (redis ships py.typed)
import aioboto3                                                           # verified: src/navigator_eventbus/brokers/sqs/connection.py:16
from navigator_eventbus.hooks.brokers.base import BaseBrokerHook          # verified: src/navigator_eventbus/hooks/brokers/rabbitmq.py:15
```

### Existing Class Signatures
```python
# src/navigator_eventbus/brokers/connection.py
class BaseConnection(ABC):
    _name_: str = "broker_connection"                                     # line 29
    def __init__(self, *args, credentials: Optional[Union[str, dict]] = None, timeout: Optional[int] = 5, **kwargs) -> None  # line 31
        self._connection = None                                           # line 47
        self._monitor_task: Optional[Awaitable] = None                    # line 49
        self._serializer = DataSerializer()                               # line 55
    def get_connection(self) -> Optional[Union[Callable, Awaitable]]      # line 59
    async def publish_message(self, body, queue_name: Optional[str] = None, **kwargs) -> None   # line 98 (abstract)
    async def consume_messages(self, queue_name: str, callback: Callable, **kwargs) -> None       # line 108 (abstract)
    async def process_message(self, body: bytes, properties: Any) -> str                          # line 118 (abstract)

# src/navigator_eventbus/brokers/consumer.py
class BrokerConsumer(ABC):
    def __init__(self, callback: Optional[Union[Awaitable, Callable]] = None, **kwargs) -> None   # line 19
    async def event_subscribe(self, queue_name: str, callback: Union[Callable, Awaitable]) -> None # line 37 (abstract)
    def wrap_callback(self, callback: Callable[[Any, str], Awaitable[None]], requeue_on_fail: bool = False, max_retries: int = 3) -> Callable  # line 49 (abstract)

# src/navigator_eventbus/brokers/producer.py
class BrokerProducer(BaseConnection, ABC):
    async def queue_event(self, body: str, queue_name: str, routing_key: Optional[str] = None, **kwargs) -> None  # line 143
    async def publish_event(self, body: str, queue_name: str, **kwargs) -> None                                     # line 168

# src/navigator_eventbus/brokers/rabbitmq/connection.py
class RabbitMQConnection(BaseConnection):
    def _require_channel(self) -> AbstractChannel                         # line 163  (model to copy)
    async def reject_message(self, message: Any, requeue: bool = False) -> None   # line 206
    async def publish_message(self, body, queue_name: str, routing_key: str, **kwargs) -> None  # line 216
    async def process_message(self, body: bytes, properties: "aiormq.spec.Basic.Properties") -> str  # line 258
    def wrap_callback(self, callback: Callable[[DeliveredMessage, str], Awaitable[None]], requeue_on_fail=False, max_retries=3) -> Callable  # line 293
    async def consume_messages(self, queue_name: str, callback: Callable[[DeliveredMessage, str], Awaitable[None]], prefetch_count: int = 1) -> None  # line 372

# src/navigator_eventbus/brokers/serializers.py
class DataSerializer:
    def pack(self, data: Any) -> bytes      # line 216
    def unpack(self, data: bytes) -> Any    # line 241
    def serialize(self, data: Any) -> str   # line 164
    def decode(self, data: str) -> Any      # line 116

# aiormq.abc (site-packages, py.typed)
class DeliveredMessage:
    delivery: spec.Basic.Deliver | spec.Basic.Return | GetResultType
    @property
    def delivery_tag(self) -> int | None
```

### Integration Points
| New Component | Connects To | Via | Verified At |
|---|---|---|---|
| `MessageCallback` | `BrokerConsumer.event_subscribe` | param type | `brokers/consumer.py:40` |
| `RedisConnection._require_client()` | every `self._connection.<cmd>` in redis/connection.py | call | lines 121-142, 195, 257, 279, 302, 334, 383, 407, 431, 450, 463 |
| `SQSConnection._require_resource()` | `create_queue`, `ensure_queue` | call | `sqs/connection.py:96, 115, 119` |
| `RabbitMQConnection._require_channel()` | `publish_message`, `wrap_callback`, `consume_messages`, `RMQConsumer.subscribe_to_events` | call | `rabbitmq/connection.py:248, 310, 337, 356, 368, 382-388`; `rabbitmq/consumer.py:103-114` |
| `BrokerProducer.queue_event(body: Any)` | `MQTTDownlinkPublisher.publish_to_employee` | call with dict | `rabbitmq/downlink.py:98` |

### Does NOT Exist (Anti-Hallucination)
- ~~`navigator_eventbus.brokers.types`~~ — no such module; the alias lives in `brokers/connection.py`.
- ~~`BaseConnection._require_connection()`~~ — not defined in the base; each backend owns its guard.
- ~~`RedisConnection._client`~~ — the attribute is `_connection`.
- ~~`SQSConnection._resource`~~ — the attribute is `_connection`; `_session` is the `aioboto3.Session`.
- ~~`aiormq.__version__`~~ — aiormq exposes no version attribute.
- ~~`docs/`~~ — the repo has no docs directory; nothing to update there.

### Edit Sites (Blueprint Anchors)

Verified against: `61f5f97`

| File | Action | Verbatim anchor line | Verified at | Occurrences |
|---|---|---|---|---|
| `src/navigator_eventbus/brokers/connection.py` | MODIFY | `        self._monitor_task: Optional[Awaitable] = None` | `connection.py:49` | 1 |
| `src/navigator_eventbus/brokers/connection.py` | MODIFY | `    def get_connection(self) -> Optional[Union[Callable, Awaitable]]:` | `connection.py:59` | 1 |
| `src/navigator_eventbus/brokers/connection.py` | MODIFY | `        callback: Callable,` | `connection.py:111` | 1 |
| `src/navigator_eventbus/brokers/connection.py` | MODIFY | `    async def process_message(self, body: bytes, properties: Any) -> str:` | `connection.py:118` | 1 |
| `src/navigator_eventbus/brokers/consumer.py` | MODIFY | `        callback: Optional[Union[Awaitable, Callable]] = None,` | `consumer.py:21` | 1 |
| `src/navigator_eventbus/brokers/consumer.py` | MODIFY | `        callback: Union[Callable, Awaitable],` | `consumer.py:40` | 1 |
| `src/navigator_eventbus/brokers/consumer.py` | MODIFY | `    ) -> Callable:` | `consumer.py:54` | 1 |
| `src/navigator_eventbus/brokers/producer.py` | MODIFY | `        body: str,` (inside `queue_event` and `publish_event`) | `producer.py:145, 170` | 2 |
| `src/navigator_eventbus/brokers/__init__.py` | MODIFY | `from .connection import BaseConnection` | `__init__.py:15` | 1 |
| `src/navigator_eventbus/brokers/rabbitmq/connection.py` | MODIFY | `    def _require_channel(self) -> AbstractChannel:` | `connection.py:163` | 1 |
| `src/navigator_eventbus/brokers/rabbitmq/connection.py` | MODIFY | `    async def publish_message(` | `connection.py:216` | 1 |
| `src/navigator_eventbus/brokers/rabbitmq/connection.py` | MODIFY | `    def wrap_callback(` | `connection.py:293` | 1 |
| `src/navigator_eventbus/brokers/rabbitmq/connection.py` | MODIFY | `    async def consume_messages(` | `connection.py:372` | 1 |
| `src/navigator_eventbus/brokers/rabbitmq/consumer.py` | MODIFY | `    async def subscribe_to_events(` | `consumer.py:83` | 1 |
| `src/navigator_eventbus/brokers/rabbitmq/manager.py` | MODIFY | `    def wrap_callback(` | `manager.py:41` | 1 |
| `src/navigator_eventbus/brokers/rabbitmq/downlink.py` | MODIFY | `        credentials: Union[str, dict] = None,` | `downlink.py:58` | 1 |
| `src/navigator_eventbus/brokers/rabbitmq/bridge.py` | MODIFY | `        accepted_schema_versions: set = None,` | `bridge.py:75` | 1 |
| `src/navigator_eventbus/hooks/brokers/rabbitmq.py` | MODIFY | `        self._connection = None` | `rabbitmq.py:31` | 1 |
| `src/navigator_eventbus/hooks/brokers/redis.py` | MODIFY | `        self._connection = None` | `redis.py:25` | 1 |
| `src/navigator_eventbus/hooks/brokers/sqs.py` | MODIFY | `        self._connection = None` | `sqs.py:26` | 1 |
| `src/navigator_eventbus/brokers/redis/connection.py` | MODIFY | `    async def ensure_group_exists(self) -> None:` | `connection.py:117` | 1 |
| `src/navigator_eventbus/brokers/redis/connection.py` | MODIFY | `    async def process_message(self, message_data: Dict[Any, Any]) -> Any:` | `connection.py:201` | 1 |
| `src/navigator_eventbus/brokers/redis/consumer.py` | MODIFY | `    def wrap_callback(` | `consumer.py:71` | 1 |
| `src/navigator_eventbus/brokers/redis/manager.py` | MODIFY | `        callback: Union[Callable, Awaitable],` | `manager.py:34` | 1 |
| `src/navigator_eventbus/brokers/sqs/connection.py` | MODIFY | `        self._session = None` (the `__init__` one, line 59; line 87 is `disconnect`) | `connection.py:59` | 2 |
| `src/navigator_eventbus/brokers/sqs/connection.py` | MODIFY | `    async def process_message(self, body: str, attributes: dict) -> Any:` | `connection.py:192` | 1 |
| `src/navigator_eventbus/brokers/sqs/connection.py` | MODIFY | `    def wrap_callback(` | `connection.py:225` | 1 |
| `src/navigator_eventbus/brokers/sqs/connection.py` | MODIFY | `        callback: Union[Callable, Awaitable[None]] = None,` | `connection.py:289` | 1 |
| `src/navigator_eventbus/brokers/sqs/consumer.py` | MODIFY | `        callback: Union[Callable, Awaitable],` (`event_subscribe` :58 and `subscribe_to_events` :66) | `consumer.py:58, 66` | 2 |
| `src/navigator_eventbus/brokers/sqs/manager.py` | MODIFY | `        callback: Union[Callable, Awaitable],` | `manager.py:31` | 1 |
| `tests/brokers/test_mypy_clean.py` | CREATE | — | — | — |

---

## 7. Implementation Notes & Constraints

### Patterns to Follow
- Copy `RabbitMQConnection._require_channel()` (`rabbitmq/connection.py:163-171`) for
  `RedisConnection._require_client()` and `SQSConnection._require_resource()`:
  `if self._connection is None: raise RuntimeError("<Backend> … connect() first")`.
- Bind the guard result to a local (`channel = self._require_channel()`) once per
  method rather than calling the guard on every line.
- Assign library objects through a local before storing them on `self` when the
  factory is untyped (`aiormq.connect`, `aioboto3.Session().resource`) — mypy does
  not narrow an attribute assigned an `Any`.
- For `redis-py` maps (`xadd` fields, `xread` streams) build the dict **with a
  declared target type** (`fields: Dict[FieldT, EncodableT] = {...}`) — `dict` is
  invariant, so `Dict[str, str]` is never accepted where `Dict[KeyT, StreamIdT]` is expected.
- Google-style docstrings and `self.logger` throughout; no `print`.

### Deliberate behaviour tightenings (documented, type-driven)
1. `RabbitMQConnection.__init__` raises `TypeError` when `credentials` is neither
   `None` nor `str` (previously failed inside `connect()` and looped on reconnect).
2. `RabbitMQConnection.publish_message` / `SQSConnection.publish_message` raise
   `ValueError` when `queue_name` (or `routing_key` for RabbitMQ) is `None`
   (previously an opaque library error swallowed/logged inside the method).
3. `RedisConnection.connect` / `SQSConnection.connect` raise `TypeError` when
   `credentials` is not a `dict` (previously `TypeError: argument after ** must be a mapping`).

### Known Risks / Gotchas
- **MRO**: `SQSManager(BrokerManager, SQSConsumer)` → `SQSConsumer(SQSConnection, BrokerConsumer)`.
  `SQSConnection.wrap_callback` wins the MRO over `BrokerConsumer.wrap_callback`, so its
  signature must be a valid override of the abstract one (same for `RedisConsumer`).
- **Redis hook** passes `stream_name=…` and no `queue_name` to `consume_messages` —
  a pre-existing runtime bug; this feature only types it (see §8).
- `SQSConsumer.subscribe_to_events` forwards `**kwargs` into `consume_messages`;
  adding `**kwargs: Any` there removes a latent `TypeError` but must not change
  polling behaviour — unknown kwargs are logged at debug and ignored.
- `redis` marker: `tests/brokers/test_redis_integration.py` and
  `test_manager_integration.py` need live services; CI runs `-m "not redis"`.

### External Dependencies
| Package | Version | Reason |
|---|---|---|
| `mypy` | `>=1.10` (dev extra, already declared) | type gate; 2.4.0 in the shared `.venv` |
| `redis` | installed (py.typed) | `redis.typing` aliases |
| `aiormq` | installed (py.typed) | `aiormq.abc` types |

---

## 8. Open Questions

- [x] Should `process_message` keep a two-parameter abstract signature? — *Resolved in this spec*: no; the base narrows to `(body: Any) -> Any` and each backend widens with its own optional metadata parameter (`properties`/`attributes`), which is the LSP-valid direction.
- [ ] `RedisBrokerHook.start_consuming()` calls `consume_messages(stream_name=…)` without `queue_name` → `TypeError` at runtime. File as a separate ledger issue (bug). — *Owner: maintainer*
- [ ] msgpack bodies (`pack()` → bytes) cannot travel as SQS `MessageBody`/Redis `decode_responses=True` strings; the `application/msgpack` branch is effectively dead on both backends. File as a separate ledger issue. — *Owner: maintainer*

---

## 9. Design Research Cross-Check

Status: skipped (ledger fix lane — no exploration document; `/sdd-fix` authors the spec directly from the issue).

| # | Suggestion (kind) | Disposition | Reason | Landed in |
|---|---|---|---|---|

Summary: **0** confirmed · **0** rejected · **0** escalated.

---

## Revision History

| Version | Date | Author | Change |
|---|---|---|---|
| 0.1 | 2026-10-21 | Jesus Lara / sdd-fix | Initial draft from issue:b12a71083ca3 |
