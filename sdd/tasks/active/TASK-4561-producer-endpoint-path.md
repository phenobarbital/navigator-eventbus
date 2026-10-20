# TASK-4561: Configurable producer endpoint path + remove publish latency

**Feature**: FEAT-431 — Unified Broker Manager + brokers hardening
**Spec**: `sdd/specs/brokers-unified-manager.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: S (< 2h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Implements spec §3 **Module 1** (configurable HTTP publish endpoint path) and
**Module 6** (drop the unconditional `await asyncio.sleep(0.1)` in
`queue_event()`). Both edits live in `brokers/producer.py`, so they are one task.
`nav-rewards` (downstream FEAT-002) must keep its own public publish route, which
is why the path must be configurable.

---

## Scope

- Add a keyword-only `endpoint_path: Optional[str] = None` parameter to
  `BrokerProducer.__init__` and store it as `self._endpoint_path`.
- In `setup()`, register the POST route at `self._endpoint_path` when given,
  else at the historical `f"/api/v1/broker/{self._broker_service}/publish_event"`.
- Remove `await asyncio.sleep(0.1)` from `queue_event()`.
- Add unit tests to `tests/brokers/test_producer.py`.

**NOT in scope**: `BrokerManager` (TASK-4564); any change to `RMQProducer`,
`RedisProducer`, `SQSProducer`; fixing the pre-existing default
`broker_service="rabbitmq"` of Redis/SQS producers.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/brokers/producer.py` | MODIFY | `endpoint_path` kwarg + use in `setup()`; remove sleep |
| `tests/brokers/test_producer.py` | MODIFY | Add endpoint-path and no-sleep tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.brokers.producer import BrokerProducer  # verified: src/navigator_eventbus/brokers/producer.py:30
from aiohttp import web                                          # verified: producer.py:23
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/brokers/producer.py
class BrokerProducer(BaseConnection, ABC):                         # line 30
    _name_: str = "broker_producer"                                # line 46
    def __init__(self, credentials=None, queue_size=None, num_workers=4,
                 timeout=5, *, auth_callable=None, **kwargs)       # line 48-57
    #   self._broker_service: str = kwargs.get("broker_service", "rabbitmq")   # line 65
    #   self._auth_callable = auth_callable                        # line 66
    #   super().__init__(credentials=credentials, timeout=timeout, **kwargs)   # line 76
    def setup(self, app: Any = None) -> None                       # line 78
    #   self.app.router.add_post(                                  # line 97
    #       f"/api/v1/broker/{self._broker_service}/publish_event",  # line 98
    #       self.event_publisher)                                  # line 99
    async def queue_event(self, body, queue_name, routing_key=None, **kwargs)  # line 136
    #   self.event_queue.put_nowait({...})                         # line 149
    #   await asyncio.sleep(0.1)                                   # line 157  ← remove
```
`tests/brokers/test_producer.py:8` already defines `DummyProducer(BrokerProducer)`
with no-op `connect`/`disconnect`/`publish_message`/`consume_messages`/`process_message` — reuse it.

### Does NOT Exist
- ~~`BrokerProducer.endpoint_path`~~ / ~~`BrokerProducer._endpoint_path`~~ — created by this task.
- ~~A public `endpoint_path` property~~ — do not add one; the private attribute is enough.
- ~~`BaseConnection.setup()` registering routes~~ — it does not; only `BrokerProducer.setup()` adds the POST route.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/brokers/producer.py", "action": "MODIFY"},
    {"path": "tests/brokers/test_producer.py", "action": "MODIFY"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/brokers/producer.py#BrokerProducer",
    "sym:src/navigator_eventbus/brokers/producer.py#BrokerProducer.setup",
    "sym:src/navigator_eventbus/brokers/producer.py#BrokerProducer.queue_event"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- 100 % backward compatible: omitting `endpoint_path` must register exactly the
  historical route.
- Resolve the default path **inside `setup()`**, not in `__init__` — because
  subclasses (and the managers in TASK-4566/4567) may set `broker_service` via
  kwargs, and resolving late keeps the route in sync with `self._broker_service`.
- `endpoint_path` is keyword-only (after `*`), next to `auth_callable`.
- Keep `import asyncio` — it is still used (`asyncio.Queue`, workers).

---

## Implementation Blueprint

### Steps (in order)
1. Add the `endpoint_path` keyword-only parameter — *why*: Module 1 public interface (spec §2 "New Public Interfaces").
2. Store it before `super().__init__` — *why*: every other producer attribute is set there; keeps the init order uniform.
3. Use it in `setup()` with the historical fallback — *why*: AC "omitting it preserves the historical route".
4. Delete the sleep line — *why*: Module 6; `put_nowait` is already non-blocking.
5. Document `endpoint_path` in the class docstring `Args:` — *why*: Google-style docstring rule.
6. Write the three tests and run them.

### `src/navigator_eventbus/brokers/producer.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c '        auth_callable: Optional\[Callable\[\[web.Request\], Awaitable\[Any\]\]\] = None,' src/navigator_eventbus/brokers/producer.py)
# AFTER — insert below `        auth_callable: Optional[Callable[[web.Request], Awaitable[Any]]] = None,` (verified: producer.py:55)
        endpoint_path: Optional[str] = None,
```
```python
# occurrences: 1 (verified: grep -c '        self._auth_callable = auth_callable' src/navigator_eventbus/brokers/producer.py)
# AFTER — insert below `        self._auth_callable = auth_callable` (verified: producer.py:66)
        self._endpoint_path: Optional[str] = endpoint_path
```
```python
# occurrences: 1 (verified: grep -c '            f"/api/v1/broker/{self._broker_service}/publish_event",' src/navigator_eventbus/brokers/producer.py)
# REPLACE lines 96-100 (`# Generic Event Subscription:` … closing `)`) with:
        # Generic Event Subscription (path configurable — FEAT-431 Module 1):
        path = self._endpoint_path or f"/api/v1/broker/{self._broker_service}/publish_event"
        self.app.router.add_post(path, self.event_publisher)
```
```python
# occurrences: 1 (verified: grep -c '            await asyncio.sleep(0.1)' src/navigator_eventbus/brokers/producer.py)
# DELETE line 157: `            await asyncio.sleep(0.1)`
```
Also add to the class docstring `Args:` (after `auth_callable`):
```python
        endpoint_path: Optional path for the HTTP publish endpoint. When
            ``None`` (default), ``/api/v1/broker/<broker_service>/publish_event``
            is used (FEAT-431).
```
**Why**: purely additive kwarg; the fallback expression is the exact historical
f-string, so existing apps see the same route.

### `tests/brokers/test_producer.py` (MODIFY)
```python
# AFTER — append at end of file (after `test_broker_producer_no_auth_raises_401`, verified: tests/brokers/test_producer.py:59)
import time  # FILL IN: move to the top-level imports block


def _post_paths(app: web.Application) -> set:
    """Return the canonical paths of all POST routes on *app*."""
    return {
        r.resource.canonical for r in app.router.routes() if r.method == "POST"
    }


def test_endpoint_path_default():
    """Without the kwarg, the historical route is registered (FEAT-431 M1)."""
    app = web.Application()
    DummyProducer().setup(app)
    assert "/api/v1/broker/rabbitmq/publish_event" in _post_paths(app)


def test_endpoint_path_custom():
    """A custom path is registered and the default is not."""
    # FILL IN: build DummyProducer(endpoint_path="/api/v1/rewards/events"), setup on a new app,
    #          assert the custom path is present and the default path absent — bounded by AC "endpoint_path is configurable"


async def test_queue_event_no_sleep():
    """queue_event() returns without the 100 ms delay (FEAT-431 M6)."""
    p = DummyProducer()
    start = time.perf_counter()
    await p.queue_event({"a": 1}, "navigator", "x.y")
    # FILL IN: assert elapsed < 0.05 s and p.event_queue.qsize() == 1 — bounded by AC "queue_event() has no unconditional sleep"
```

### FILL IN checklist
- [ ] `test_producer.py` — move `import time` to the imports block.
- [ ] `test_endpoint_path_custom` body; bounded by AC "endpoint_path is configurable".
- [ ] `test_queue_event_no_sleep` assertions; bounded by AC "no unconditional sleep".

---

## Acceptance Criteria

- [ ] `BrokerProducer(endpoint_path="/x")` registers POST `/x` and not the default.
- [ ] Omitting `endpoint_path` registers `/api/v1/broker/<broker_service>/publish_event`.
- [ ] `queue_event()` contains no `asyncio.sleep`.
- [ ] Existing producer tests still pass.
- [ ] `ruff check src/navigator_eventbus/brokers/producer.py tests/brokers/test_producer.py` clean.

---

## Validation Commands
- `pytest tests/brokers/test_producer.py -q`

---

## Test Specification

See the blueprint: `test_endpoint_path_default`, `test_endpoint_path_custom`,
`test_queue_event_no_sleep` (spec §4, Modules 1 and 6).

---

## Agent Instructions

1. **Work in the feature worktree** — never on `dev`
   (`python -m scripts.sdd.ensure_worktree --slug brokers-unified-manager --feature-id FEAT-431`).
2. **Environment**: `source .venv/bin/activate`; if `pytest` is missing from the venv run
   `uv pip install -e ".[dev,rabbitmq,sqs,redis]"` (no manifest change).
3. Read the spec; verify the Codebase Contract (line numbers may drift — re-grep anchors).
4. Set status `in-progress` in `sdd/tasks/index/brokers-unified-manager.json`, commit only that file.
5. Implement from the blueprint; complete every `# FILL IN:`; never change fixed signatures.
6. Run the Validation Commands; stage only the files listed above.
7. Close with `scripts/sdd/close_task.sh TASK-4561 brokers-unified-manager verified`, fill the Completion Note, commit.

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**:
**Date**:
**Notes**:

**Deviations from spec**: none
