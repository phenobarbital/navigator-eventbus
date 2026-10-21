# TASK-4572: Brokers typing pass — Redis + SQS families and the pytest mypy gate

**Feature**: FEAT-712 — Brokers typing pass (mypy-clean `brokers/` + `hooks/brokers/`)
**Spec**: `sdd/specs/fixgroup-17c252023ef4.spec.md`
**Status**: pending
**Priority**: medium
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-4571
**Assigned-to**: unassigned
**Discovered-from**: `issue:b12a71083ca3`

---

## Context

Spec §3 Module 3. With the base contracts and `MessageCallback` landed by
TASK-4571, this task types the Redis Streams and AWS SQS families (the
`SQSManager` MRO clash and the `redis-py` invariant maps live here) and adds
`tests/brokers/test_mypy_clean.py`, the pytest-level gate that keeps both
packages clean while the rest of `src/` is still red for unrelated issues.

---

## Scope

- `redis/connection.py`: `_require_client() -> aioredis.Redis` and use it for every
  `self._connection.<cmd>`; `connect()` raises `TypeError` unless `credentials` is a dict;
  `packed_body: Union[bytes, str]`; `fields: Dict[FieldT, EncodableT]` for `xadd`;
  `process_message` returns `None` when the entry has no `body`; `consume_messages` /
  `reclaim_pending_messages` take `MessageCallback`; `xread` converts to `Dict[KeyT, StreamIdT]`.
- `redis/consumer.py`: `wrap_callback(callback: MessageCallback, requeue_on_fail=False, max_retries=3)
  -> Callable[[Any, Any], Awaitable[None]]` (parity params ignored, docstring says so); typed inner
  `wrapped_callback(message_id: Any, body: Any) -> None`; `MessageCallback` on `event_subscribe` /
  `subscribe_to_events`; `callback: Optional[MessageCallback]` in `__init__`.
- `redis/manager.py:34`: `callback: MessageCallback`.
- `sqs/connection.py`: `_session: Optional[aioboto3.Session]`, `_connection: Any`, `_require_resource() -> Any`;
  `connect()` through a local session + dict check; `publish_message(queue_name: Optional[str] = None)` → `ValueError`;
  `process_message(body: str, attributes: Optional[dict] = None) -> Any` with `unpack(body.encode("utf-8"))` on the
  msgpack branch; `wrap_callback(callback: MessageCallback, requeue_on_fail=False, max_retries=3)`;
  `consume_messages(..., **kwargs: Any)` (unknown kwargs logged at debug); `consume_message(callback: Optional[MessageCallback] = None)`.
- `sqs/consumer.py:58,66` and `sqs/manager.py:31`: `callback: MessageCallback`; `__init__` callback `Optional[MessageCallback]`.
- Create `tests/brokers/test_mypy_clean.py`; append Redis/SQS guard tests to `tests/brokers/test_typing_guards.py`.

**NOT in scope**: anything under `brokers/rabbitmq/`, the base files, or `hooks/brokers/` (TASK-4571);
functional fixes to the msgpack path or the Redis hook (spec §8).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/brokers/redis/connection.py` | MODIFY | `_require_client()`, typed maps, callback types |
| `src/navigator_eventbus/brokers/redis/consumer.py` | MODIFY | LSP-valid `wrap_callback`, callback types |
| `src/navigator_eventbus/brokers/redis/manager.py` | MODIFY | callback type |
| `src/navigator_eventbus/brokers/sqs/connection.py` | MODIFY | `_require_resource()`, Optional session, overrides |
| `src/navigator_eventbus/brokers/sqs/consumer.py` | MODIFY | callback types |
| `src/navigator_eventbus/brokers/sqs/manager.py` | MODIFY | callback type |
| `tests/brokers/test_mypy_clean.py` | CREATE | subprocess mypy gate |
| `tests/brokers/test_typing_guards.py` | MODIFY | add `_require_client` / `_require_resource` tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.brokers.connection import BaseConnection, MessageCallback  # MessageCallback created by TASK-4571 (connection.py)
from redis import asyncio as aioredis                               # verified: src/navigator_eventbus/brokers/redis/connection.py:19
from redis.typing import EncodableT, FieldT, KeyT, StreamIdT        # verified: `python -c` import OK (redis ships py.typed)
import aioboto3                                                     # verified: src/navigator_eventbus/brokers/sqs/connection.py:16
from navigator_eventbus.brokers.redis import RedisConnection, RedisConsumer  # verified: tests/brokers/test_redis_consumer.py:2
from navigator_eventbus.brokers.sqs import SQSConnection            # verified: tests/brokers/test_sqs.py:6
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/brokers/redis/connection.py
class RedisConnection(BaseConnection):
    self._connection: Optional[aioredis.Redis] = None                         # :73
    async def ensure_group_exists(self) -> None                               # :117
    async def publish_message(self, body, queue_name: Optional[str] = None, **kwargs) -> None   # :152-157  (message_data dict :161, xadd :195)
    async def process_message(self, message_data: Dict[Any, Any]) -> Any      # :201
    async def consume_messages(self, queue_name: Optional[str], callback: Callable[[Dict[str, Any], str], Awaitable[None]], count=1, block=1000, **kwargs) -> None  # :233-240
    async def reclaim_pending_messages(self, queue_name: str, callback: Callable, *, min_idle_time=30_000, count=10) -> int  # :307-314
    async def xread(self, streams: Dict[str, str], count: Optional[int] = 100, block: Optional[int] = None) -> list  # :363-368 (call :383)

# src/navigator_eventbus/brokers/redis/consumer.py
class RedisConsumer(RedisConnection, BrokerConsumer):
    def wrap_callback(self, callback: Callable[[Any, Any], Awaitable[None]]) -> Callable[[Any, Any], Awaitable[None]]   # :71-74
    async def event_subscribe(self, queue_name: Optional[str], callback: Union[Callable, Awaitable], **kwargs) -> None  # :88-93 (calls wrap_callback :97)
    async def subscribe_to_events(self, queue_name: Optional[str], callback: Union[Callable, Awaitable], **kwargs) -> None  # :101-106

# src/navigator_eventbus/brokers/sqs/connection.py
class SQSConnection(BaseConnection):
    self._connection = None; self._session = None                              # :58-59
    async def connect(self) -> None                                            # :62  (Session :68, resource :69-72)
    async def publish_message(self, body, queue_name: str, **kwargs) -> None   # :128-133
    async def process_message(self, body: str, attributes: dict) -> Any        # :192  (unpack :205)
    def wrap_callback(self, callback: Callable[[dict, str], Awaitable[None]]) -> Callable[[Any], Awaitable[None]]  # :225-228
    async def consume_messages(self, queue_name: str, callback, max_messages=10, wait_time=10, idle_sleep=5) -> None  # :254-261
    async def consume_message(self, queue_name: str, callback: Union[Callable, Awaitable[None]] = None, wait_time: int = 5) -> Optional[dict]  # :286-291

# src/navigator_eventbus/brokers/sqs/manager.py
class SQSManager(BrokerManager, SQSConsumer):                                  # :14
    async def subscribe_to_events(self, queue_name: str, callback: Union[Callable, Awaitable], ...)  # :28-36
# src/navigator_eventbus/brokers/redis/manager.py
class RedisManager(BrokerManager, RedisConsumer):                              # :14
    async def subscribe_to_events(self, queue_name: Optional[str], callback: Union[Callable, Awaitable], **kwargs)  # :31-36

# src/navigator_eventbus/brokers/serializers.py
    def pack(self, data: Any) -> bytes     # :216
    def unpack(self, data: bytes) -> Any   # :241
```

### Does NOT Exist
- ~~`RedisConnection._client`~~ — the attribute is `_connection`.
- ~~`SQSConnection._resource`~~ — the attribute is `_connection`; `_session` holds the `aioboto3.Session`.
- ~~`aioboto3` type stubs for resources~~ — resource objects are dynamic; keep `_connection: Any`.
- ~~`redis.typing.StreamT`~~ — use `KeyT` / `StreamIdT` / `FieldT` / `EncodableT`.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/brokers/redis/connection.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/redis/consumer.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/redis/manager.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/sqs/connection.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/sqs/consumer.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/brokers/sqs/manager.py", "action": "MODIFY"},
    {"path": "tests/brokers/test_mypy_clean.py", "action": "CREATE"},
    {"path": "tests/brokers/test_typing_guards.py", "action": "MODIFY"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/brokers/redis/connection.py#RedisConnection",
    "sym:src/navigator_eventbus/brokers/redis/consumer.py#RedisConsumer",
    "sym:src/navigator_eventbus/brokers/redis/consumer.py#RedisConsumer.wrap_callback",
    "sym:src/navigator_eventbus/brokers/sqs/connection.py#SQSConnection",
    "sym:src/navigator_eventbus/brokers/sqs/connection.py#SQSConnection.wrap_callback",
    "sym:src/navigator_eventbus/brokers/sqs/manager.py#SQSManager",
    "sym:src/navigator_eventbus/brokers/serializers.py#DataSerializer.unpack"
  ]
}
```

---

## Implementation Notes

### Pattern to Follow
```python
# RabbitMQConnection._require_channel() (rabbitmq/connection.py:163) — copy for Redis/SQS
def _require_client(self) -> aioredis.Redis:
    if self._connection is None:
        raise RuntimeError("Redis client is not connected; call connect() first")
    return self._connection
```

### Key Constraints
- `dict` is invariant: build redis maps **with a declared target type**
  (`fields: Dict[FieldT, EncodableT] = {"body": packed_body, "ContentType": content_type}`,
  `streams_arg: Dict[KeyT, StreamIdT] = {k: v for k, v in streams.items()}`). Never `cast`.
- `aioboto3` resources are untyped: `self._connection: Any` is correct; the guard exists for the
  `None` case, not for attribute typing.
- `wrap_callback` on Redis/SQS must accept `requeue_on_fail`/`max_retries` so the
  `SQSManager(BrokerManager, SQSConsumer)` / `RedisManager` MROs are consistent (`misc` errors).
- No new `# type: ignore` (spec AC-5).

### References in Codebase
- `src/navigator_eventbus/brokers/rabbitmq/connection.py:163-171` — guard pattern
- `tests/brokers/test_sqs_manager.py`, `tests/brokers/test_redis_manager.py` — manager behaviour that must not change

---

## Implementation Blueprint

### Steps (in order)
1. `redis/connection.py`: add `_require_client()` right after `disconnect()`; replace every
   `self._connection.<cmd>` with a `client = self._require_client()` local — *why*: 11 `union-attr` errors share one root cause.
2. `redis/connection.py`: typed maps + `packed_body` annotation + `process_message` None guard + callback types — *why*: remaining `arg-type`/`assignment` errors.
3. `redis/consumer.py`, `redis/manager.py` — *why*: MRO-consistent `wrap_callback`, `MessageCallback` everywhere.
4. `sqs/connection.py` top-down (`__init__` → `connect` → `_require_resource` → `create_queue`/`ensure_queue` → `publish_message` → `process_message` → `wrap_callback` → `consume_messages` → `consume_message`).
5. `sqs/consumer.py`, `sqs/manager.py`.
6. Create `tests/brokers/test_mypy_clean.py`; append guard tests; run the Validation Commands.

### `src/navigator_eventbus/brokers/redis/connection.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c '    async def ensure_group_exists(self) -> None:' src/navigator_eventbus/brokers/redis/connection.py)
# BEFORE `    async def ensure_group_exists(self) -> None:` (verified: connection.py:117) insert:
    def _require_client(self) -> aioredis.Redis:
        """Return the connected client.

        Raises:
            RuntimeError: When :meth:`connect` has not been called.
        """
        if self._connection is None:
            raise RuntimeError("Redis client is not connected; call connect() first")
        return self._connection

# connect() (verified: connection.py:91): dict guard before the constructor
            if not isinstance(self._credentials, dict):
                raise TypeError("RedisConnection expects a credentials dict (host/port/password/db)")
            self._connection = aioredis.Redis(**self._credentials, decode_responses=True, encoding="utf-8")

# publish_message (verified: connection.py:161, :195)
            packed_body: Union[bytes, str]
            ...
            fields: Dict[FieldT, EncodableT] = {"body": packed_body, "ContentType": content_type}
            await self._require_client().xadd(stream, fields, nomkstream=False)

# process_message (verified: connection.py:207)
        body = message_data.get("body")
        if body is None:
            return None

# xread (verified: connection.py:383)
        streams_arg: Dict[KeyT, StreamIdT] = {key: last_id for key, last_id in streams.items()}
        return await self._require_client().xread(streams=streams_arg, count=count, block=block) or []
```
**Why this shape**: identical runtime path for every success case; only the `None`-client case now raises a clear `RuntimeError` instead of `AttributeError`.

### `src/navigator_eventbus/brokers/redis/consumer.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c '    def wrap_callback(' src/navigator_eventbus/brokers/redis/consumer.py)
    def wrap_callback(
        self,
        callback: MessageCallback,
        requeue_on_fail: bool = False,
        max_retries: int = 3,
    ) -> Callable[[Any, Any], Awaitable[None]]:
        """Wrap *callback* for message handling.

        ``requeue_on_fail`` and ``max_retries`` are accepted for interface parity
        with :class:`BrokerConsumer` and ignored: Redis Streams has no republish path.
        """
        async def wrapped_callback(message_id: Any, body: Any) -> None:
```
**Why**: the MRO `RedisManager → BrokerManager → … → RedisConsumer → BrokerConsumer` needs one compatible signature.

### `src/navigator_eventbus/brokers/sqs/connection.py` (MODIFY)
```python
# occurrences: 2 (verified: grep -c '        self._session = None' …/sqs/connection.py) — the __init__ one is line 59 (line 87 is disconnect(), unchanged)
# FILL IN: disambiguate — edit the one directly after `super().__init__(...)` in __init__
        self._connection: Any = None
        self._session: Optional[aioboto3.Session] = None

    def _require_resource(self) -> Any:
        """Return the SQS resource; raise RuntimeError before :meth:`connect`."""
        if self._connection is None:
            raise RuntimeError("SQS resource is not connected; call connect() first")
        return self._connection

# connect() (verified: connection.py:68-72)
            if not isinstance(self._credentials, dict):
                raise TypeError("SQSConnection expects a credentials dict")
            session = aioboto3.Session()
            self._session = session
            async with session.resource("sqs", **self._credentials) as sqs_resource:
                self._connection = sqs_resource

# publish_message (verified: connection.py:128)
    async def publish_message(self, body: Union[str, list, dict, Any], queue_name: Optional[str] = None, **kwargs: Any) -> None:
        if queue_name is None:
            raise ValueError("publish_message() requires queue_name")

# process_message (verified: connection.py:192, :205)
    async def process_message(self, body: str, attributes: Optional[dict] = None) -> Any:
        attrs = attributes or {}
        ...
            elif content_type == "application/msgpack":
                return self._serializer.unpack(body.encode("utf-8"))

# wrap_callback / consume_messages / consume_message (verified: :225, :254, :289)
    def wrap_callback(self, callback: MessageCallback, requeue_on_fail: bool = False, max_retries: int = 3) -> Callable[[Any], Awaitable[None]]:
    async def consume_messages(self, queue_name: str, callback: MessageCallback, max_messages: int = 10,
                               wait_time: int = 10, idle_sleep: int = 5, **kwargs: Any) -> None:
        if kwargs:
            self.logger.debug("consume_messages: ignoring unsupported kwargs %s", sorted(kwargs))
    async def consume_message(self, queue_name: str, callback: Optional[MessageCallback] = None, wait_time: int = 5) -> Optional[dict]:
```
**Why**: `SQSConsumer.subscribe_to_events` already forwards `**kwargs` into `consume_messages` (consumer.py:83); accepting them removes a latent `TypeError` without changing polling.

### `tests/brokers/test_mypy_clean.py` (CREATE)
```python
"""FEAT-712 regression gate: the brokers layer stays mypy-clean (issue:b12a71083ca3)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TARGETS = ("src/navigator_eventbus/brokers", "src/navigator_eventbus/hooks/brokers")


def test_brokers_packages_are_mypy_clean() -> None:
    """``mypy`` over both packages exits 0 under the repo's ``[tool.mypy]`` config."""
    pytest.importorskip("mypy")
    result = subprocess.run(
        [sys.executable, "-m", "mypy", *TARGETS],
        cwd=REPO_ROOT, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
```

### `tests/brokers/test_typing_guards.py` (MODIFY)
```python
# occurrences: 1 (verified after TASK-4571: grep -c 'def test_rabbitmq_rejects_non_str_credentials' tests/brokers/test_typing_guards.py)
# APPEND at end of file:
def test_redis_require_client_before_connect_raises() -> None:
    from navigator_eventbus.brokers.redis import RedisConnection
    with pytest.raises(RuntimeError):
        RedisConnection()._require_client()


def test_sqs_require_resource_before_connect_raises() -> None:
    from navigator_eventbus.brokers.sqs import SQSConnection
    with pytest.raises(RuntimeError):
        SQSConnection(credentials={"region_name": "us-east-1"})._require_resource()
```

### FILL IN checklist
- [ ] `sqs/connection.py` — pick the `__init__` `self._session = None` (line 59), not the `disconnect()` one; bounded by the Edit Sites row.
- [ ] `redis/connection.py` — every `self._connection.<cmd>` site (lines 121-142, 195, 257, 279, 302, 334, 383, 407, 431, 450, 463) goes through the guard; bounded by AC-1.

---

## Acceptance Criteria

- [ ] `mypy src/navigator_eventbus/brokers src/navigator_eventbus/hooks/brokers` → `Success: no issues found` (spec AC-1).
- [ ] `mypy src/` reports ≤ 4 errors, none under `brokers/` or `hooks/brokers/` (spec AC-2).
- [ ] All Validation Commands pass; `ruff check src/navigator_eventbus/brokers tests/brokers` passes.
- [ ] No new `# type: ignore` (spec AC-5).

## Validation Commands

- `pytest tests/brokers/test_mypy_clean.py -q`
- `pytest tests/brokers/test_typing_guards.py -q`
- `pytest tests/brokers/test_redis_consumer.py -q`
- `pytest tests/brokers/test_redis_manager.py -q`
- `pytest tests/brokers/test_redis_reclaim.py -q`
- `pytest tests/brokers/test_sqs.py -q`
- `pytest tests/brokers/test_sqs_manager.py -q`
- `pytest tests/brokers/test_serializers.py -q`

---

## Test Specification

See the two test blocks above. Existing Redis/SQS tests are the behavioural
regression net and must pass unchanged.

---

## Agent Instructions

1. **Work in the feature worktree** (`python -m scripts.sdd.ensure_worktree --slug fixgroup-17c252023ef4 --feature-id FEAT-712`).
2. Confirm TASK-4571 is `done` in `sdd/tasks/index/fixgroup-17c252023ef4.json` and `MessageCallback` exists.
3. Verify the Codebase Contract anchors before editing.
4. Mark `in-progress` in the index; implement in the Steps order.
5. Run the Validation Commands; stage only the listed files.
6. Close with `scripts/sdd/close_task.sh TASK-4572 fixgroup-17c252023ef4 verified` and fill the Completion Note.

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**:
**Date**:
**Notes**:

**Deviations from spec**: none
