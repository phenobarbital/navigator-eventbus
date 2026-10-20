---
# SDD flow type and base branch (FEAT-145).
# - type: feature  (default)  → base_branch: main (this project uses main)
# - type: hotfix              → base_branch MUST be: main
type: feature
base_branch: dev
---

<!-- LANGUAGE: This document MUST be written entirely in English (proper nouns keep native spelling). -->

# Feature Specification: Unified Broker Manager + brokers hardening

**Feature ID**: FEAT-431
**Date**: 2026-08-06
**Author**: Jesus Lara
**Status**: approved
**Target version**: next minor after the current `version.py` — 0.4.0, or 0.5.0 if FEAT-711's 0.4.0 release (TASK-4560) lands first (bump `src/navigator_eventbus/version.py`)

> **Downstream driver**: this feature unblocks the `nav-rewards` migration
> (`nav-rewards@sdd/specs/eventbus-migration.spec.md`, FEAT-002), which cannot
> start until Modules 1–3 of this spec are released. Modules 4–6 are
> pre-existing `brokers` debt bundled into the same cycle.

---

## 1. Motivation & Business Requirements

### Problem Statement

The `brokers` subpackage (ported from the deprecated `navigator.brokers` in
FEAT-316) models producers and consumers as **separate classes with separate
connections**: `RMQProducer` and `RMQConsumer`, each opening its own AMQP
connection and channel.

Applications that need to both publish and consume — the common case for an
event-driven service — currently have no supported way to do so over a single
connection. They must instantiate two objects, register two keys on the aiohttp
app, and reason about two independent reconnection state machines.

`nav-rewards` is the concrete driver: it carries a hand-rolled 418-line
`EventManager` built directly on `aiormq` precisely because `brokers` offers no
unified abstraction. Migrating it requires that abstraction to exist upstream.

Auditing the subpackage while designing that abstraction surfaced four
pre-existing defects, one of which silently breaks any consumer that binds a
queue to an exchange with different names.

### Goals

- Provide a **generic, backend-agnostic `BrokerManager`** that unifies producer
  and consumer roles over a **single connection and channel**, with concrete
  managers for RabbitMQ, Redis and SQS.
- Make the producer's HTTP publish endpoint path **configurable**, so consuming
  applications can preserve their own public API contracts.
- Fix the `queue_name` / `exchange_name` clobber that makes exchange-vs-queue
  binding impossible.
- Clear three lower-severity defects in the same cycle: the broken RabbitMQ
  hook, the double callback wrapping, and the unconditional publish latency.
- Keep **100 % backward compatibility**: every change is additive or a
  bug fix; no existing public signature changes meaning.

### Non-Goals (explicitly out of scope)

- A RabbitMQ transport backend for `BusCore` — brokers and the event bus stay
  separate subsystems.
- Changing the reversed base-class order between `RMQProducer` and
  `RedisProducer`/`SQSProducer` (documented as intentional in
  `brokers/rabbitmq/producer.py`).
- Reworking `DataSerializer`, the retry/backoff policies, or the DLQ.
- Any change inside `nav-rewards` — that is FEAT-002 in the downstream repo.
- Deprecating `RMQProducer` / `RMQConsumer`; they remain first-class.

---

## 2. Architectural Design

### Overview

`BrokerManager` is a thin **mixin over `BrokerProducer`** that overrides the
aiohttp `on_startup` handler to perform both halves of the lifecycle — connect,
start the producer's queue workers, then subscribe the consumer — against one
connection. Concrete managers combine it with the backend's existing Consumer
class, so the shared connection comes from the MRO rather than from composition.

Composition (holding a producer instance and a consumer instance) was rejected:
it yields two connections, which is exactly the property the feature exists to
remove.

### Component Diagram

```
        BaseConnection (ABC)
               ▲
               │
        BrokerProducer ─────────┐
               ▲                │
               │                │  (mixin: unified start/stop)
        BrokerManager ──────────┘
               ▲
               │        ┌──────────────────────────────┐
               ├────────┤ RMQManager(BrokerManager,    │──→ RabbitMQConnection
               │        │            RMQConsumer)      │
               │        └──────────────────────────────┘
               ├────────  RedisManager(BrokerManager, RedisConsumer) ──→ RedisConnection
               └────────  SQSManager(BrokerManager, SQSConsumer)     ──→ SQSConnection

  MRO (verified):
  RMQManager → BrokerManager → BrokerProducer → RMQConsumer
             → RabbitMQConnection → BaseConnection → BrokerConsumer → ABC → object
                                                     ▲
                                    terminates the cooperative __init__ chain
```

### Base-class order is load-bearing, not stylistic

`BrokerConsumer.__init__` (`brokers/consumer.py:19-34`) **does not call
`super().__init__()`**. That is deliberate: it absorbs the leftover `**kwargs`
that would otherwise reach `object.__init__()` and raise `TypeError` — the
hazard `brokers/producer.py:66-75` already documents.

Consequence: in every manager, `BrokerConsumer` must be **last** in the MRO and
`BrokerProducer` **first**. The order `(BrokerManager, XConsumer)` satisfies
this for all three backends.

### Verified by execution

The hierarchy was reproduced with stubs faithful to the real `__init__`
signatures and `super()` calls and executed. Results:

| Check | Result |
|---|---|
| C3 linearization | ✅ Valid for `(BrokerProducer, RMQConsumer)` and for `(BrokerManager, RMQConsumer)` |
| Cooperative `__init__` chain | ✅ Traverses all five classes, terminates cleanly at `BrokerConsumer`, no `TypeError` |
| State initialization | ✅ `_dsn`, `_credentials`, `queue_size`, `_num_workers`, `_serializer`, `_routing_key` all set |
| Method resolution | `setup`→`BrokerProducer`, inherited `stop`→`BrokerProducer`, `subscribe_to_events`→`RMQConsumer`, `wrap_callback`/`publish_message`→`RabbitMQConnection`; managers must override `stop` to perform consumer teardown first |
| Single connection | ✅ Unified `start()` runs both halves against one `connect()` |
| Genericity | ✅ `RedisManager` linearizes identically and instantiates |

The same run surfaced defects **D1** and **D2** below.

### Two initialization defects the design must account for

**D1 — `queue_name` clobbers `exchange_name`.**
`brokers/rabbitmq/consumer.py:48-50`:

```python
self._queue_name = kwargs.get("queue_name", None)
if self._queue_name:
    self._exchange_name = self._queue_name    # destroys an explicit exchange_name
```

With `exchange_name="navigator"` + `queue_name="reward_processing_queue"` the
instance ends up with `_exchange_name == "reward_processing_queue"`, so
`start()` declares and binds against the **wrong exchange**. Any consumer whose
queue name differs from its exchange name silently receives nothing.

**D2 — `_queue_name` and `_callback_` cannot act as sentinels.**
`BrokerConsumer.__init__` runs **last** in the MRO and unconditionally
reassigns both (`consumer.py:32,34`):

```python
self._queue_name = kwargs.get("queue_name", "navigator")
self._callback_  = callback if callback else self.subscriber_callback
```

It therefore overwrites whatever `RMQConsumer.__init__` set moments earlier.
An instance built with **no** `queue_name` still ends up with
`_queue_name == "navigator"`, and `_callback_` is never falsy.

This was confirmed empirically: a conditional auto-subscribe guarded by
`if self._queue_name and self._callback_` **subscribed anyway**. `BrokerManager`
must therefore expose an **explicit `auto_subscribe` flag**, not infer intent
from those attributes.

`auto_subscribe` matters downstream: `nav-rewards` builds its manager in
`app.py` before the engine callback exists, so it needs to opt out of
auto-subscription and subscribe explicitly later.

### Shutdown and registration contracts

`BrokerProducer.stop()` does not call `super().stop()`. In the proposed MRO,
it bypasses `RedisConsumer.stop()` and `SQSConsumer.stop()`, leaving their
background tasks running. `BrokerManager.stop()` must first await an abstract
`_stop_consuming()` backend hook, then invoke `BrokerProducer.stop()` to drain
outgoing events, cancel and await producer workers, and disconnect once.
Redis and SQS implement the hook using `stop_consumer()`. RabbitMQ must retain
consumer tags and cancel its subscriptions before draining outgoing work;
callbacks already running must finish before producer workers are stopped.
Subscription tracking must include explicit subscriptions made after startup
with `auto_subscribe=False`. Teardown must also tolerate no subscription.
Tests must verify ordering and that no consumer tasks remain after shutdown.

`RedisConnection.__init__()` assigns `self._name_ = self.__class__.__name__`.
`RedisManager` must restore its declared registration key after cooperative
initialization, so `setup()` registers `app["redis_manager"]`. Keep existing
Redis producer and consumer registration behavior unchanged.

### RabbitMQ delivery settlement ownership

The callback wrapper is the sole owner of delivery settlement. Currently the
hook catches an error, calls `reject_message()`, and returns normally; adding
that method alone would make the wrapper acknowledge an already rejected
delivery. Module 4 must also change the hook error path: raise a dedicated
internal rejection exception instead of settling the delivery directly.
Define that exception in `brokers/rabbitmq/connection.py` and import it lazily
inside the hook failure path to preserve optional dependency behavior.
`wrap_callback()` catches it before its general retry handler, calls
`reject_message(message, requeue=False)` exactly once, and returns without
acknowledging or republishing. Ordinary callback exceptions retain the existing
retry behavior; successful hook callbacks receive exactly one acknowledgement.

### Integration Points

| Existing Component | Integration Type | Notes |
|---|---|---|
| `BrokerProducer` | extends + additive kwarg | `BrokerManager` subclasses it; `endpoint_path` added to `__init__`/`setup()` |
| `RMQConsumer` / `RedisConsumer` / `SQSConsumer` | combines | Second base of each concrete manager |
| `RabbitMQConnection` | fixes | Gains `ensure_queue()` and `reject_message()` |
| `RabbitMQBrokerHook` | unblocks + fixes settlement | Missing queue declaration support; replace direct rejection with an internal rejection signal handled by the wrapper |
| `BaseConnection.setup()` | unchanged | Managers inherit `BrokerProducer.setup()`, which wins in the MRO |
| `DataSerializer` | unchanged | Reachable from managers via `BaseConnection._serializer` |

### New Public Interfaces

```python
# src/navigator_eventbus/brokers/manager.py
class BrokerManager(BrokerProducer, ABC):
    """Unifies producer and consumer roles over a single broker connection."""
    _name_: str = "broker_manager"

    def __init__(self, *args, auto_subscribe: bool = True, **kwargs) -> None: ...
    async def start(self, app: web.Application) -> None: ...
    async def stop(self, app: web.Application) -> None: ...
    @abstractmethod
    async def _subscribe(self, app: web.Application) -> None: ...
    @abstractmethod
    async def _stop_consuming(self) -> None: ...

# src/navigator_eventbus/brokers/rabbitmq/manager.py
class RMQManager(BrokerManager, RMQConsumer):
    _name_: str = "rabbitmq_manager"

# src/navigator_eventbus/brokers/redis/manager.py
class RedisManager(BrokerManager, RedisConsumer):
    _name_: str = "redis_manager"

# src/navigator_eventbus/brokers/sqs/manager.py
class SQSManager(BrokerManager, SQSConsumer):
    _name_: str = "sqs_manager"

# src/navigator_eventbus/brokers/producer.py  — additive
class BrokerProducer(BaseConnection, ABC):
    def __init__(self, ..., endpoint_path: Optional[str] = None, **kwargs) -> None: ...

# src/navigator_eventbus/brokers/rabbitmq/connection.py  — new methods
class RabbitMQConnection(BaseConnection):
    async def ensure_queue(self, queue_name: str, exchange_name: Optional[str] = None,
                           routing_key: Optional[str] = None, durable: bool = True,
                           **kwargs: Any) -> None: ...
    async def reject_message(self, message: Any, requeue: bool = False) -> None: ...
```

---

## 3. Module Breakdown

### Module 1: Configurable producer endpoint path
- **Path**: `src/navigator_eventbus/brokers/producer.py`
- **Responsibility**: Add `endpoint_path` keyword to `__init__`, defaulting to
  the current `f"/api/v1/broker/{self._broker_service}/publish_event"`, and use
  it in `setup()`. Purely additive.
- **Depends on**: none

### Module 2: Fix `queue_name` clobbering `exchange_name` (D1)
- **Path**: `src/navigator_eventbus/brokers/rabbitmq/consumer.py`
- **Responsibility**: Apply the queue-name fallback only when no explicit
  `exchange_name` was supplied.
- **Depends on**: none

### Module 3: `BrokerManager` + concrete managers
- **Path**: `src/navigator_eventbus/brokers/manager.py`,
  `brokers/rabbitmq/manager.py`, `brokers/redis/manager.py`,
  `brokers/sqs/manager.py`, plus re-exports in the respective `__init__.py`
- **Responsibility**: The generic mixin (unified `start()`/`stop()`,
  `auto_subscribe` flag, abstract `_subscribe()` / `_stop_consuming()`) and one
  concrete manager per backend implementing subscription and teardown.
  Preserve the Redis manager registration key after backend initialization.
- **Depends on**: Module 2 (a manager built on the unfixed consumer binds to the
  wrong exchange)

### Module 4: `ensure_queue` + `reject_message` on `RabbitMQConnection`
- **Path**: `src/navigator_eventbus/brokers/rabbitmq/connection.py`,
  `src/navigator_eventbus/hooks/brokers/rabbitmq.py`
- **Responsibility**: Implement the two methods `RabbitMQBrokerHook` already
  references, and fix hook failure settlement using the internal rejection
  exception described above. The wrapper performs the nack; the hook must no
  longer nack directly and then return to the wrapper's success path.
- **Depends on**: none

### Module 5: Fix double callback wrapping
- **Path**: `src/navigator_eventbus/brokers/rabbitmq/consumer.py` (and the SQS
  counterpart, which has the same shape)
- **Responsibility**: Pass the raw callback to `consume_messages()`, which
  already wraps it.
- **Depends on**: none

### Module 6: Remove unconditional publish latency
- **Path**: `src/navigator_eventbus/brokers/producer.py`
- **Responsibility**: Drop the `await asyncio.sleep(0.1)` in `queue_event()`.
- **Depends on**: none

### Module 7: Tests
- **Path**: `tests/brokers/test_manager.py` (new),
  `tests/brokers/test_rabbitmq.py`, `tests/brokers/test_producer.py`
- **Responsibility**: Coverage per §4.
- **Depends on**: Modules 1–6

---

## 4. Test Specification

### Unit Tests

| Test | Module | Description |
|---|---|---|
| `test_endpoint_path_default` | 1 | Without the kwarg, the historical route is registered |
| `test_endpoint_path_custom` | 1 | Custom path is registered and the default is not |
| `test_exchange_not_clobbered` | 2 | `exchange_name` + `queue_name` both explicit → each keeps its value |
| `test_exchange_falls_back_to_queue` | 2 | Only `queue_name` → exchange mirrors it (current behaviour preserved) |
| `test_exchange_default` | 2 | Neither supplied → `"navigator"` |
| `test_manager_mro_rabbitmq` | 3 | MRO is exactly Manager → BrokerManager → BrokerProducer → RMQConsumer → RabbitMQConnection → BaseConnection → BrokerConsumer |
| `test_manager_mro_redis` | 3 | Same shape for Redis |
| `test_manager_mro_sqs` | 3 | Same shape for SQS |
| `test_manager_init_chain` | 3 | Every base initializes; `_serializer`, `_dsn`, `queue_size` present; no `TypeError` |
| `test_manager_single_connection` | 3 | `start()` calls `connect()` exactly once for both roles |
| `test_manager_start_runs_both_halves` | 3 | `start()` starts queue workers **and** subscribes |
| `test_manager_auto_subscribe_false` | 3 | `auto_subscribe=False` → connects and starts workers, does **not** subscribe (regression for D2) |
| `test_manager_auto_subscribe_default_true` | 3 | Default subscribes |
| `test_manager_setup_registers_name` | 3 | `setup()` registers under `_name_` and wires `on_startup`/`on_shutdown` |
| `test_redis_manager_registration_key` | 3 | Real cooperative initialization preserves `"redis_manager"`; setup registers that literal key, not `"RedisManager"` |
| `test_manager_stop_drains_and_disconnects` | 3 | `stop()` drains the queue, cancels workers, disconnects once |
| `test_manager_stop_consumers_first` | 3 | Redis/SQS consumer tasks are cancelled and awaited before outgoing queue drain and disconnect; no tasks remain |
| `test_rmq_manager_stop_consumers_first` | 3 | Consumer tags are cancelled and in-flight callbacks finish before outgoing queue drain and disconnect |
| `test_manager_stop_manual_or_no_subscription` | 3 | Teardown handles both an explicit subscription after `auto_subscribe=False` and no subscription |
| `test_manager_publishes_and_consumes` | 3 | Same instance can publish and consume |
| `test_ensure_queue_declares_and_binds` | 4 | Declares the queue and binds it when exchange/routing key are given |
| `test_ensure_queue_no_binding` | 4 | Without exchange, declares only |
| `test_reject_message_nack` | 4 | Issues `basic_nack` with the requested `requeue` |
| `test_rabbitmq_hook_connect` | 4 | `RabbitMQBrokerHook.connect()` completes (currently `AttributeError`) |
| `test_rabbitmq_hook_failure_settles_once` | 4 | Invoke the real wrapper around the hook callback; failing `_on_message()` produces one nack with `requeue=False`, no ack, and no republish |
| `test_rabbitmq_hook_success_settles_once` | 4 | Successful hook callback through the real wrapper produces one ack and no nack |
| `test_rabbitmq_callback_retry_preserved` | 4 | Ordinary callback exceptions still follow the existing retry policy |
| `test_event_subscribe_single_wrap` | 5 | User callback receives `(message, decoded_body)`; exactly one `basic_ack` |
| `test_queue_event_no_sleep` | 6 | `queue_event()` returns without the 100 ms delay |

### Integration Tests

| Test | Description |
|---|---|
| `test_manager_roundtrip_rabbitmq` | Publish via the manager, consume via the same manager, single connection (marked `integration`) |
| `test_manager_roundtrip_redis` | Same for Redis Streams (marked `redis`) |
| `test_exchange_binding_roundtrip` | Queue name ≠ exchange name; message published to the exchange reaches the consumer — end-to-end regression for D1 |

### Validation Contract
tests:
- tests/brokers/test_producer.py
- tests/brokers/test_consumer_fixes.py
- tests/brokers/test_rabbitmq_settlement.py
- tests/brokers/test_manager.py
- tests/brokers/test_rmq_manager.py
- tests/brokers/test_redis_manager.py
- tests/brokers/test_sqs_manager.py
- tests/brokers/test_rabbitmq.py
- tests/brokers/test_hook_rewire.py
- tests/brokers/test_sqs.py
- tests/brokers/test_redis_consumer.py
- tests/brokers/test_manager_integration.py
markers: none

### Test Data / Fixtures

```python
@pytest.fixture
def mock_amqp_channel():
    """aiormq channel double: exchange_declare / queue_declare / queue_bind /
    basic_publish / basic_consume / basic_qos / basic_ack / basic_nack."""

@pytest.fixture
def manager_kwargs():
    return {
        "credentials": "amqp://guest:guest@localhost:5672/navigator",
        "exchange_name": "navigator",
        "queue_name": "processing_queue",   # deliberately != exchange_name
        "routing_key": "#",
    }
```

---

## 5. Acceptance Criteria

- [ ] `BrokerManager` exists and is re-exported from `navigator_eventbus.brokers`
- [ ] `RMQManager`, `RedisManager`, `SQSManager` exist and are re-exported from their subpackages
- [ ] A manager opens **exactly one** connection for publishing and consuming
- [ ] `auto_subscribe=False` reliably suppresses auto-subscription (D2)
- [ ] Shutdown stops consumer activity before draining outgoing work and disconnecting; no consumer tasks remain, including manually started subscriptions
- [ ] Redis manager setup registers exactly `app["redis_manager"]` after real backend initialization
- [ ] `exchange_name` and `queue_name` are independent (D1)
- [ ] `endpoint_path` is configurable; omitting it preserves the historical route
- [ ] `RabbitMQBrokerHook.connect()` and its reject path no longer raise `AttributeError`
- [ ] Hook failures settle exactly once with a non-requeued nack, no ack or republish; successful deliveries ack once and ordinary callback retry behavior is preserved
- [ ] `event_subscribe()` wraps the callback exactly once
- [ ] `queue_event()` has no unconditional sleep
- [ ] **No breaking changes**: existing `RMQProducer` / `RMQConsumer` / `RedisProducer` / `SQSProducer` construction and behaviour unchanged
- [ ] `uv run pytest tests/ -v` passes
- [ ] `uv run ruff check src/ tests/` and `uv run mypy src/` pass
- [ ] `version.py` bumped and released, so FEAT-002 downstream can pin it

---

## 6. Codebase Contract

> **CRITICAL — Anti-Hallucination Anchor**
> Verified by direct reading on 2026-08-06. Implementation agents MUST NOT
> reference imports, attributes or methods absent from this section without
> verifying them first.

### Verified Imports

```python
from navigator_eventbus.brokers import (
    BaseConnection, BrokerConsumer, BrokerProducer, BaseWrapper, DataSerializer,
)                                          # verified: brokers/__init__.py:16-20
from navigator_eventbus.brokers.rabbitmq import (
    RabbitMQConnection, RMQConsumer, RMQProducer,
    EmployeeEventsBridge, MQTTDownlinkPublisher,
)                                          # verified: brokers/rabbitmq/__init__.py:2-6
from navigator_eventbus.brokers.redis import RedisConnection, RedisConsumer, RedisProducer
from navigator_eventbus.brokers.sqs import SQSConnection, SQSConsumer, SQSProducer
from navigator_eventbus.serialization import dumps, loads
from navigator_eventbus.brokers._conf import BROKER_MANAGER_QUEUE_SIZE, rabbitmq_dsn
```

### Existing Class Signatures

```python
# src/navigator_eventbus/brokers/connection.py
class BaseConnection(ABC):                                            # line 21
    _name_: str = "broker_connection"
    def __init__(self, *args, credentials=None, timeout=5, **kwargs)  # line 30
    #   self._serializer = DataSerializer()                           # line 56
    #   calls super().__init__(*args, **kwargs)  — cooperative
    def get_serializer(self) -> DataSerializer                        # line 69
    async def start(self, app) -> None                                # line 122
    async def stop(self, app) -> None                                 # line 126
    def setup(self, app=None) -> None                                 # line 130
    #   duck-types app via hasattr(app, "get_app"); app[self._name_] = self

# src/navigator_eventbus/brokers/consumer.py
class BrokerConsumer(ABC):                                            # line 14
    def __init__(self, callback=None, **kwargs)                       # line 19
    #   self._queue_name = kwargs.get("queue_name", "navigator")      # line 32
    #   self._callback_  = callback or self.subscriber_callback       # line 34
    #   DOES NOT call super().__init__()  — terminates the chain

# src/navigator_eventbus/brokers/producer.py
class BrokerProducer(BaseConnection, ABC):                            # line 30
    _name_: str = "broker_producer"
    def __init__(self, credentials=None, queue_size=None, num_workers=4,
                 timeout=5, *, auth_callable=None, **kwargs)          # line 48
    #   self._broker_service = kwargs.get("broker_service", "rabbitmq")  # line 65
    def setup(self, app=None) -> None                                 # line 78
    #   add_post(f"/api/v1/broker/{self._broker_service}/publish_event")  # lines 97-98
    async def queue_event(self, body, queue_name, routing_key=None, **kwargs)  # line 136
    #   self.event_queue.put_nowait(...)                              # line 149
    #   await asyncio.sleep(0.1)   ← to remove (Module 6)             # line 157
    async def publish_event(self, body, queue_name, **kwargs)         # line 162 — publishes directly
    async def get_userid(self, session: dict, idx="user_id") -> int   # line 171
    def service_auth(fn)   # staticmethod                             # line 190
    async def event_publisher(self, request) -> web.Response          # ~line 210
    async def _event_broker(self, worker_id: int) -> None             # line 261

# src/navigator_eventbus/brokers/rabbitmq/connection.py
class RabbitMQConnection(BaseConnection):
    def __init__(self, credentials=None, timeout=5, **kwargs)         # line 31
    async def connect(self) -> None                                   # line 54
    async def connection_monitor(self) -> None                        # line 77
    async def schedule_reconnect(self) -> None                        # line 88 — raises RuntimeError when exhausted
    async def disconnect(self) -> None                                # line 100
    async def ensure_connection(self) -> None                         # line 122
    async def create_exchange(self, exchange_name, exchange_type="topic",
                              durable=True, **kwargs)                 # line 127
    async def ensure_exchange(self, exchange_name, exchange_type="topic", **kwargs)  # line 150
    async def publish_message(self, body, queue_name, routing_key, **kwargs)         # line 159
    async def process_message(self, body, properties) -> str          # line 201
    def wrap_callback(self, callback, requeue_on_fail=False, max_retries=3)          # line 236
    async def consume_messages(self, queue_name, callback, prefetch_count=1)         # line 313
    #   already calls self.wrap_callback(callback) internally  ← cause of Module 5

# src/navigator_eventbus/brokers/rabbitmq/consumer.py
class RMQConsumer(RabbitMQConnection, BrokerConsumer):                # line 22
    def __init__(self, credentials=None, timeout=5, callback=None, **kwargs)  # line 27
    #   self._exchange_name = kwargs.get("exchange_name", "navigator")  # line 47
    #   self._queue_name    = kwargs.get("queue_name", None)            # line 48
    #   if self._queue_name: self._exchange_name = self._queue_name  ← D1  # lines 49-50
    async def event_subscribe(self, queue_name, callback)             # line 68 — double-wraps (Module 5)
    async def subscribe_to_events(self, exchange, queue_name, routing_key,
        callback, exchange_type="topic", durable=True, prefetch_count=1,
        requeue_on_fail=True, max_retries=3, **kwargs)                # line 78
    async def start(self, app) -> None                                # line 126 — auto-subscribes

# src/navigator_eventbus/brokers/rabbitmq/producer.py
class RMQProducer(BrokerProducer, RabbitMQConnection)   # NOTE reversed vs Redis/SQS — intentional

# src/navigator_eventbus/brokers/redis/consumer.py
class RedisConsumer(RedisConnection, BrokerConsumer)
    #   pops queue_name/group_name/consumer_name from kwargs (PR navigator#393 fix #1)
# src/navigator_eventbus/brokers/sqs/consumer.py
class SQSConsumer(SQSConnection, BrokerConsumer)
# src/navigator_eventbus/brokers/redis/producer.py
class RedisProducer(RedisConnection, BrokerProducer)
# src/navigator_eventbus/brokers/sqs/producer.py
class SQSProducer(SQSConnection, BrokerProducer)

# src/navigator_eventbus/brokers/sqs/connection.py
async def ensure_queue(...)                                           # line 106 — the ONLY existing ensure_queue

# src/navigator_eventbus/hooks/brokers/rabbitmq.py
await self._connection.ensure_queue(...)                              # line 40 — target does not exist
await self._connection.reject_message(message, requeue=False)         # line 68 — target does not exist
```

### Integration Points

| New Component | Connects To | Via | Verified At |
|---|---|---|---|
| `BrokerManager` | `BrokerProducer` | inheritance | `brokers/producer.py:30` |
| `BrokerManager.start()` | `BaseConnection.connect()` + `BrokerProducer.start_workers()` | override | `producer.py:118`, `connection.py:122` |
| `RMQManager._subscribe()` | `RMQConsumer.subscribe_to_events()` | method call | `rabbitmq/consumer.py:78` |
| `RMQManager` | `app[self._name_]` | `BrokerProducer.setup()` | `producer.py:78` |
| `ensure_queue()` | `RabbitMQBrokerHook.connect()` | method call | `hooks/brokers/rabbitmq.py:40` |
| `reject_message()` | `RabbitMQConnection.wrap_callback()` | handles the hook's internal rejection exception | Replaces direct hook settlement at `hooks/brokers/rabbitmq.py:68` |

### Does NOT Exist (Anti-Hallucination)

- ~~`RabbitMQConnection.ensure_queue()`~~ — does not exist. Only `brokers/sqs/connection.py:106` has one. Created by Module 4.
- ~~`RabbitMQConnection.reject_message()`~~ — does not exist. Created by Module 4.
- ~~`RabbitMQConnection.ack_message()`~~ — does not exist; acking happens inside `wrap_callback`.
- ~~`BrokerProducer.endpoint_path`~~ — does not exist yet; added by Module 1.
- ~~`BrokerManager` / `RMQManager` / `RedisManager` / `SQSManager`~~ — do not exist yet; created by Module 3.
- ~~`BrokerConsumer` calling `super().__init__()`~~ — it does **not**, by design. Do not "fix" this: it is what keeps leftover kwargs from reaching `object.__init__()`.
- ~~`_queue_name` / `_callback_` as "was it supplied?" sentinels~~ — unusable. `BrokerConsumer.__init__` runs last and unconditionally defaults them (`consumer.py:32,34`). Hence the explicit `auto_subscribe` flag.
- ~~A RabbitMQ backend for `BusCore`~~ — does not exist. `backends/` holds only `memory`, `composite`, `redis_pubsub`, `redis_streams`.
- ~~`message.delivery.delivery_tag` being required~~ — `aiormq.abc.DeliveredMessage` exposes `delivery_tag` as a `@property` (`aiormq/abc.py:98`). The existing `wrap_callback` is correct; do not "fix" it.

---

## 7. Implementation Notes & Constraints

### Patterns to Follow

- `uv` exclusively; activate `.venv` before any Python command.
- `from __future__ import annotations` at the top of every broker module.
- Google-style docstrings with `Args:` / `Returns:` / `Raises:`, matching the
  FEAT-316 port style.
- Every module docstring states what it ports/extends and which spec drove it.
- `navconfig.logging.logging` for loggers; never `print`.
- Concrete backend subpackages stay lazily importable: `brokers/__init__.py`
  must not eagerly import `rabbitmq`/`redis`/`sqs` (optional dependencies).
  Re-export `BrokerManager` at the top level, but each concrete manager only
  from its own subpackage.

### Known Risks / Gotchas

| Risk | Mitigation |
|---|---|
| **D1 fix changes behaviour for callers that relied on the clobber** — someone passing both names today silently gets queue-name-as-exchange | Preserve the fallback when `exchange_name` is absent; only stop overriding an explicit value. Covered by `test_exchange_falls_back_to_queue` |
| **MRO regressions** when adding the mixin | Assert the exact MRO per backend in tests; it is the load-bearing property of the design |
| **Module 3 built on the unfixed consumer** binds to the wrong exchange | Hard dependency Module 2 → Module 3 |
| **Module 5 changes what the callback receives** for anyone calling `event_subscribe()` today (raw `DeliveredMessage` → decoded body) | This is the documented intent of the API; call it out in the release notes as a bug fix |
| `_name_` collisions if a manager and a producer are both registered | Managers use distinct `_name_` defaults (`rabbitmq_manager`, …) |
| Redis initialization overwrites the manager's `_name_` | Restore the declared manager key after initialization; assert the literal app key |
| Producer shutdown bypasses consumer shutdown in the manager MRO | Explicit `_stop_consuming()` before queue drain; verify consumer cancellation and completion |
| Hook rejects a delivery then the wrapper acknowledges it | Wrapper owns settlement; dedicated internal rejection exception bypasses success ack and generic retry |
| Redis/SQS managers are unexercised by the downstream driver | Ship them, but keep integration coverage behind the existing `redis` / `integration` pytest markers |

### External Dependencies

No new runtime dependencies. Existing optional extras (`rabbitmq` → `aiormq`,
`redis`, `sqs` → `aioboto3`) already cover the concrete managers.

---

## 8. Open Questions

- [ ] **Q1 — Should `BrokerManager` also be exposed as a `BrokerBridge`-style
      helper** that wires publish-side and consume-side to different
      exchanges/queues, or is a single subscription per manager enough for now?
      — *Owner: Jesus Lara*
- [x] **Q2 — Do Redis and SQS managers ship in this cycle or follow later?**
      Only the RabbitMQ one is required by FEAT-002. Shipping all three proves
      the abstraction is genuinely generic but widens the test surface.
      — *Owner: Jesus Lara* — **Resolved 2026-10-21: all three ship in this cycle.**
- [ ] **Q3 — Is Module 5 a patch or a minor bump?** It changes what
      `event_subscribe()` hands to user callbacks. Semantically a bug fix, but
      observable. — *Owner: Jesus Lara*
- [ ] **Q4 — Target version number**: `version.py` reads `0.2.3` while
      FEAT-430's spec targeted "0.6.x". Confirm the intended series before
      releasing, since FEAT-002 will pin it. — *Owner: Jesus Lara*

---

## Revision History

| Version | Date | Author | Change |
|---|---|---|---|
| 0.1 | 2026-08-06 | Jesus Lara | Initial draft. Derived from the `nav-rewards` migration proposal (`nav-rewards@sdd/proposals/eventbus-migration.md`, Plan A) and from an MRO/init-chain audit of the `brokers` subpackage |
| 0.2 | 2026-10-21 | Codex | Specify consumer teardown, single-owner hook settlement, and preservation of the Redis manager registration key, with regression tests and acceptance criteria |
| 0.3 | 2026-10-21 | Jesus Lara | Approved. Q2 resolved (all three managers ship); target version = next minor after current `version.py` (0.3.0; FEAT-711 also targets 0.4.0); §4 Validation Contract added |
