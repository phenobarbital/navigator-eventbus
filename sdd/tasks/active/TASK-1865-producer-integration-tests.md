# TASK-1865: Integration tests against a real BusCore

**Feature**: FEAT-433 — BusCore Producer Facade
**Spec**: `sdd/specs/buscore-producer-facade.spec.md`
**Status**: pending
**Priority**: medium
**Estimated effort**: S (< 2h)
**Depends-on**: TASK-1863
**Assigned-to**: unassigned

---

## Context

Implements the integration half of spec **Module 3**. TASK-1863's unit tests
all run against a `FakeBus`, which proves the facade's own logic but proves
nothing about whether its envelopes are actually *accepted and dispatched* by
the real engine.

That gap matters most for the constructor defaults resolved in spec §8 Q3: a
`priority=EventPriority.HIGH` producer is only useful if `HIGH` genuinely
selects the high-priority queue in `BusCore`. A fake bus that just appends to a
list can never catch a regression there.

---

## Scope

- Add an integration test class to `tests/test_producers.py` exercising
  `BusCoreProducer` against a real, started `BusCore`.
- Cover: end-to-end topic/payload/source delivery, and priority routing.

**NOT in scope**:
- Any change to `producers.py` → TASK-1863.
- Any change to `__init__.py` → TASK-1864.
- Redis, any `TransportBackend`, DLQ, or `EventBus` (the facade) — memory only.
- The version bump → TASK-1866.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `tests/test_producers.py` | MODIFY | Append `TestBusCoreProducerIntegration` |

---

## Codebase Contract (Anti-Hallucination)

> Verified by reading source on 2026-09-12 at commit `2ba82b1`.

### Verified Imports

```python
from navigator_eventbus.core import BusCore            # verified: src/navigator_eventbus/core.py:92
from navigator_eventbus.envelope import EventEnvelope  # verified: src/navigator_eventbus/envelope.py:106
from navigator_eventbus.envelope import Severity       # verified: src/navigator_eventbus/envelope.py:89
from navigator_eventbus.evb import EventPriority       # verified: src/navigator_eventbus/evb.py:51
from navigator_eventbus.producers import BusCoreProducer   # created by TASK-1863
```

### Existing Signatures to Use

```python
# src/navigator_eventbus/core.py:92
class BusCore:
    def __init__(                                              # line 128
        self, *, workers: int = 4, queue_size: int = 1024,
        handler_timeout: float = 30.0, retry_attempts: int = 3,
        retry_base_delay: float = 0.1,
        backpressure: Optional[dict[str, str]] = None,
        default_backpressure: str = POLICY_BLOCK,
        drain_timeout: float = 5.0,
        on_dlq: Optional[DLQCallback] = None,
        backend: Optional[TransportBackend] = None,
    ) -> None: ...

    async def start(self) -> None:                             # line 203  — "Start the worker pool. Idempotent."
    async def close(self, drain_timeout: Optional[float] = None) -> None:   # line 217
    async def publish(self, envelope: EventEnvelope) -> None:  # line 287

    def subscribe(                                             # line 397
        self,
        pattern: str,                                          # exact topic OR glob ("order.*")
        handler: Callable[[EventEnvelope], Any],               # sync OR async; receives the envelope
        *,
        priority: int = 0,                                     # handler ordering — NOT EventPriority
        filter_fn: Optional[Callable[[EventEnvelope], bool]] = None,
        min_severity: Optional[Severity] = None,
    ) -> str: ...                                              # returns subscriber_id
```

> **`subscribe(priority=...)` is an `int` controlling handler execution order
> among matching handlers. It is NOT `EventPriority` and has nothing to do with
> queue scheduling.** Do not pass an `EventPriority` to it.

### Does NOT Exist

- ~~`BusCore.publish_event()`~~ — the method is `publish()`.
- ~~`BusCore.emit()`~~ — that is on `EventBus` (`evb.py:349`).
- ~~`BusCore.wait_for_idle()`~~ / ~~`BusCore.drain()`~~ / ~~`BusCore.join()`~~ —
  no public "wait until dispatched" method exists. `_drain()` (line 275) is
  private; do not call it. Poll instead (see Implementation Notes).
- ~~a shared `bus` fixture in `tests/conftest.py`~~ — that file has NO fixtures,
  only a docstring. Build the bus locally.
- ~~`wait_until` importable from a helpers module~~ — it is defined inline at
  the top of `tests/test_integration.py:22`. Copy the pattern; do not import it
  across test modules.

---

## Implementation Notes

### The one gotcha that will bite you

**`BusCore.publish()` only ENQUEUES.** It is an O(1) put into a per-priority
`asyncio.Queue`; dispatch to handlers happens later, on a worker task
(`core.py:287` docstring: *"never awaits a handler"*). So this is WRONG and
will fail intermittently:

```python
await producer.publish_event({"a": 1}, "t.topic")
assert received          # ← FLAKY: the worker may not have run yet
```

Poll for the condition instead, following the `wait_until` helper defined
inline at `tests/test_integration.py:22`:

```python
async def wait_until(condition, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(0.01)
    pytest.fail("condition not met within timeout")
```

### Pattern to Follow

```python
class TestBusCoreProducerIntegration:
    async def test_publish_through_real_buscore(self):
        bus = BusCore(workers=2)
        await bus.start()
        try:
            received: list[EventEnvelope] = []
            bus.subscribe("test.topic", lambda env: received.append(env))

            producer = BusCoreProducer(lambda: bus, source="test-svc")
            await producer.publish_event({"k": "v"}, "test.topic")

            await wait_until(lambda: received)
            assert received[0].topic == "test.topic"
            assert received[0].payload == {"k": "v"}
            assert received[0].source == "test-svc"
        finally:
            await bus.close()
```

### Key Constraints

- `asyncio_mode = "auto"` (`pyproject.toml:108`) — plain `async def test_*`, no
  `@pytest.mark.asyncio`.
- **Always `await bus.close()` in a `finally`** (or a fixture teardown).
  A leaked `BusCore` leaves worker tasks running and pollutes later tests.
- Memory only: construct `BusCore()` with **no** `backend=` argument.
- Keep timeouts short (`wait_until(..., timeout=3.0)`) so a genuine failure
  fails fast rather than hanging the suite.
- The producer's own `timeout` default (1.0s) is ample for an in-memory bus;
  do not lower it here or the test becomes timing-sensitive.

### References in Codebase

- `tests/test_integration.py:1-40` — the `wait_until` helper and the
  import/setup conventions for real-`BusCore` tests.
- `tests/test_core.py` — existing `BusCore` start/subscribe/close patterns.

---

## Acceptance Criteria

- [ ] `test_publish_through_real_buscore` passes: a real started `BusCore`
      dispatches the producer's envelope to a subscribed handler with the
      correct `topic`, `payload`, and configured `source`.
- [ ] `test_high_priority_routes_through_priority_queue` passes: a producer
      built with `priority=EventPriority.HIGH` yields a received envelope whose
      `.priority is EventPriority.HIGH`, proving the constructor default
      survives the dispatch path.
- [ ] No test asserts on handler side effects without `wait_until` — no
      race-dependent assertions.
- [ ] Every test closes its `BusCore` (no leaked worker tasks); the suite
      produces no "Task was destroyed but it is pending" warnings.
- [ ] No Redis, no backend, no network.
- [ ] `pytest tests/test_producers.py -v` passes in full.
- [ ] Full suite still green: `pytest -q`

---

## Test Specification

```python
# appended to tests/test_producers.py
import time


async def wait_until(condition, timeout: float = 3.0) -> None:
    """Copied from tests/test_integration.py:22 — dispatch is asynchronous."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(0.01)
    pytest.fail("condition not met within timeout")


class TestBusCoreProducerIntegration:
    async def test_publish_through_real_buscore(self):
        """Envelope reaches a subscriber with topic, payload and source intact."""

    async def test_high_priority_routes_through_priority_queue(self):
        """A HIGH-priority producer's envelope arrives with .priority == HIGH."""
```

---

## Agent Instructions

1. **Check the dependency**: `TASK-1863` must be in `sdd/tasks/completed/`.
2. **Verify the Codebase Contract** — confirm `BusCore.subscribe`'s signature
   and that no public drain/wait method has appeared.
3. Update `sdd/tasks/index/buscore-producer-facade.json` → `"in-progress"`.
4. Implement, verify every acceptance criterion.
5. Move this file to `sdd/tasks/completed/`, update the index → `"done"`.
6. Fill in the Completion Note.

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**:
**Date**:
**Notes**:

**Deviations from spec**: none | describe if any
