# TASK-1863: Implement BusCoreProducer with unit tests

**Feature**: FEAT-433 — BusCore Producer Facade
**Spec**: `sdd/specs/buscore-producer-facade.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: L (4-8h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Implements spec **Module 1** and the unit-test half of **Module 3**. This is
the heart of FEAT-433: a generic producer facade contributed upstream from
FieldSync's local `BusCorePublisherShim`, so any app on navigator-eventbus gets
lazy bus resolution, a bounded publish timeout, and explicit
strict-vs-fail-soft error propagation without re-implementing them.

Implementation and unit tests live in ONE task deliberately — `CLAUDE.md`
mandates a TDD approach per task, and splitting them would leave the
implementer without a specification to drive against.

Read spec **§2 Architectural Design** (the two-phase call sequence and the
strict-mode exception-type table) before writing any code. The phase boundary
is the single most important design decision in this feature.

---

## Scope

- Create `src/navigator_eventbus/producers.py` defining `BusCoreProducer`.
- Implement the two-phase `publish_event()` described in spec §2:
  - **Phase 1 (always raises)**: build the `EventEnvelope`. Runs OUTSIDE the
    error-policy `try`.
  - **Phase 2 (governed by `raise_on_error`)**: resolve the bus, publish it
    inside `asyncio.timeout`.
- Implement keyword-only constructor parameters `timeout`, `raise_on_error`,
  `source`, `severity`, `priority`, validating `timeout`.
- Create `tests/test_producers.py` with the 20 unit tests listed below.
- Google-style docstrings and strict type hints on every public method.

**NOT in scope**:
- Exporting from `src/navigator_eventbus/__init__.py` → TASK-1864.
- Integration tests against a real `BusCore` → TASK-1865.
- The version bump → TASK-1866.
- Any change to `BusCore`, `EventEnvelope`, or any backend.
- Per-call `source`/`severity`/`priority` overrides (spec §8 Q3 deferred them).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/producers.py` | CREATE | `BusCoreProducer` facade |
| `tests/test_producers.py` | CREATE | 20 unit tests (see Test Specification) |

---

## Codebase Contract (Anti-Hallucination)

> Verified by reading source on 2026-09-12 at commit `2ba82b1`.

### Verified Imports

```python
from navigator_eventbus.core import BusCore              # verified: src/navigator_eventbus/core.py:92
from navigator_eventbus.core import BusClosedError       # verified: src/navigator_eventbus/core.py:61
from navigator_eventbus.core import BackpressureError    # verified: src/navigator_eventbus/core.py:65
from navigator_eventbus.envelope import EventEnvelope    # verified: src/navigator_eventbus/envelope.py:106
from navigator_eventbus.envelope import Severity         # verified: src/navigator_eventbus/envelope.py:89
from navigator_eventbus.evb import EventPriority         # verified: src/navigator_eventbus/evb.py:51
```

In `producers.py` import from these **module paths**, not from the package
root — importing `navigator_eventbus` from inside a submodule risks a cycle.
In `tests/test_producers.py` either path works.

### Existing Signatures to Use

```python
# src/navigator_eventbus/core.py:61
class BusClosedError(RuntimeError): ...      # "Raised when publishing after close() has begun."
# src/navigator_eventbus/core.py:65
class BackpressureError(RuntimeError): ...   # "Raised to the emitter when the reject policy is active and full."

# src/navigator_eventbus/core.py:92
class BusCore:
    async def publish(self, envelope: EventEnvelope) -> None:   # line 287
        # Raises BusClosedError (line 291) / BackpressureError (line 318, reject policy only).
        # Under the DEFAULT block policy it awaits queue.put() at line 343 and can
        # block indefinitely — this is why the bounded timeout is load-bearing.
    self.logger = logging.getLogger("navigator_eventbus.core")   # line 190

# src/navigator_eventbus/envelope.py:105-106
@dataclass(frozen=True, slots=True)
class EventEnvelope:
    topic: str                                    # line 130  REQUIRED
    payload: dict[str, Any]                       # line 131  REQUIRED
    event_id: str = <uuid4 factory>               # line 132
    timestamp: datetime = <now(utc) factory>      # line 133-135
    source: Optional[str] = None                  # line 136
    severity: Severity = Severity.INFO            # line 137
    priority: EventPriority = EventPriority.NORMAL # line 138
    correlation_id: Optional[str] = None          # line 139
    trace_context: Optional[dict] = None          # line 140
    metadata: dict[str, Any] = field(default_factory=dict)  # line 141
    schema_version: int = ENVELOPE_SCHEMA_VERSION # line 142

    def __post_init__(self) -> None:              # line 144
        # raises ValueError if timestamp is not a datetime      (lines 152-156)
        # raises ValueError if timestamp is NAIVE               (lines 157-162)

# src/navigator_eventbus/evb.py:51
class EventPriority(Enum):
    LOW = 0; NORMAL = 5; HIGH = 10; CRITICAL = 15   # lines 53-56

# src/navigator_eventbus/envelope.py:89
class Severity(IntEnum):
    DEBUG = 10; INFO = 20; WARNING = 30; ERROR = 40; CRITICAL = 50   # lines 98-102
```

**Only `topic` and `payload` are required.** Everything else has a default —
this is what makes the two-argument `publish_event` signature work.

### Does NOT Exist

- ~~`src/navigator_eventbus/producers.py`~~ — you are creating it.
- ~~`tests/test_producers.py`~~ — you are creating it.
- ~~`BusCore.publish_event()`~~ — the method is `publish()`.
- ~~`BusCore.emit()`~~ — `emit()` is on `EventBus` (`evb.py:349`), a different class.
- ~~`EventEnvelope.ts`~~ / ~~`EventEnvelope.routing_key`~~ / ~~`EventEnvelope.queue_name`~~ — not fields.
- ~~`navigator_eventbus.producers.BaseProducer`~~ / ~~`AbstractProducer`~~ / ~~`Producer`~~ — no producer base class exists. `BusCoreProducer` inherits from nothing.
- ~~`src/navigator_eventbus/base/`~~ — this directory does NOT exist. Ignore any "AbstractBase pattern" hint from the templates.
- ~~`BusCorePublisherShim`~~ — FieldSync-owned, in another repo. **Do NOT import or vendor it.**
- ~~`Topic`~~ / ~~`TargetScope`~~ — FieldSync concepts, absent here.
- ~~fixtures in `tests/conftest.py`~~ — it contains only a docstring, no fixtures. Define your own locally.

---

## Implementation Notes

### Pattern to Follow

```python
# Shape (NOT literal code — write it properly with docstrings and type hints):
async def publish_event(self, body, queue_name, **kwargs) -> None:
    # ---- PHASE 1: request validation — ALWAYS raises ----
    ts = kwargs.get("timestamp")
    extra = {"timestamp": ts} if isinstance(ts, datetime) else {}
    envelope = EventEnvelope(
        topic=queue_name,
        payload=body,
        source=self._source,
        severity=self._severity,
        priority=self._priority,
        **extra,
    )
    # ---- PHASE 2: delivery — governed by raise_on_error ----
    try:
        bus = self._bus_provider()
        if bus is None:
            raise RuntimeError("No BusCore available from bus_provider()")
        async with asyncio.timeout(self._timeout):
            await bus.publish(envelope)
    except Exception:
        if self._raise_on_error:
            raise
        self.logger.warning("publish_event failed for %s", queue_name, exc_info=True)
```

### Key Constraints

- **The `try` must NOT wrap envelope construction.** That is the whole point of
  spec §8 Q1. A naive `datetime` must raise `ValueError` in BOTH modes.
- **Bounded await**: `async with asyncio.timeout(...)`, the house idiom
  (`core.py:560`, `hooks/webhook/preprocess.py:164`,
  `subscribers/notification.py:516`). `requires-python = ">=3.11"`
  (`pyproject.toml:14`), so it is available. Do NOT use `asyncio.wait_for`.
- **`timeout=None` disables the bound.** `asyncio.timeout(None)` is valid and
  means "no deadline" — no branch needed.
- **Validate `timeout` in `__init__`**: a non-`None` value `<= 0` raises
  `ValueError` there, never from `publish_event`.
- **`bus_provider()` is called inside `publish_event`, every call.** Never
  memoize it in `__init__` — the bus often does not exist at construction time.
- **`except Exception` never catches `CancelledError`** (it derives from
  `BaseException` on 3.8+), so external cancellation propagates for free. Do
  NOT add `except BaseException`.
- **Only `timestamp` is read from `**kwargs`**, and only when it
  `isinstance(..., datetime)`. A `str` is left alone and the envelope falls
  back to its own "now". Never parse dates here.
- **The module must not contain the literal `"ts"` anywhere** — not in code,
  not in a comment. A guard test asserts this.
- Logger: `self.logger = logging.getLogger("navigator_eventbus.producers")` in
  `__init__`, matching `core.py:190` / `dlq.py:145` / `evb.py:200`. Never `print`.
- Optionally log unrecognised `**kwargs` keys at **DEBUG** — not WARNING;
  `routing_key` arrives on every legacy call and must not generate noise.
- No Pydantic. `EventEnvelope` is a deliberately non-Pydantic frozen slotted
  dataclass for hot-path speed (`envelope.py:10-13`).

### Docstring Requirements

`publish_event`'s docstring MUST state plainly that only `timestamp` is read
from `**kwargs`, and that `source` / `severity` / `priority` are per-instance
constructor arguments. Without this, `publish_event(body, t, severity=...)`
silently doing nothing is an unmarked trap (spec §7).

Also document in the class docstring:
- Under a saturated queue with the default `block` policy, a fail-soft caller
  **drops** events after `timeout`. This is not a delivery guarantee.
- `BusCore.publish()` spawns the backend fan-out BEFORE the local enqueue
  (`core.py:297-303`), so a producer-side timeout does not cancel a fan-out
  already in flight — an event may reach the transport while this logs a timeout.

### References in Codebase

- `src/navigator_eventbus/subscribers/webhook.py` — a comparable small,
  configurable component with a per-module logger.
- `tests/test_neutrality.py` — the source-scanning guard pattern to copy for
  `test_producers_module_is_application_neutral`.
- `pyproject.toml:107-109` — `asyncio_mode = "auto"`, so **no
  `@pytest.mark.asyncio` decorators are needed**; a plain `async def test_*`
  runs.

---

## Acceptance Criteria

- [ ] `src/navigator_eventbus/producers.py` exists and defines `BusCoreProducer`.
- [ ] Constructor signature is exactly:
      `(bus_provider, *, timeout=1.0, raise_on_error=False, source=None, severity=Severity.INFO, priority=EventPriority.NORMAL)`.
- [ ] `publish_event(self, body, queue_name, **kwargs) -> None` — signature
      unchanged from the spec; no extra positional or keyword-only parameters.
- [ ] `body` becomes `payload` verbatim; `queue_name` becomes `topic` verbatim.
- [ ] `source`, `severity`, `priority` from the instance reach every envelope.
- [ ] Request-validation failures raise in BOTH modes: naive `timestamp` →
      `ValueError`; `timeout <= 0` → `ValueError` from `__init__`.
- [ ] Delivery failures follow `raise_on_error`, with strict-mode types matching
      spec §2: `RuntimeError` (missing bus), `TimeoutError` (timeout),
      `BusClosedError` / `BackpressureError` propagated **unwrapped**.
- [ ] `bus_provider` is called on every `publish_event`, never memoized.
- [ ] `asyncio.CancelledError` propagates even with `raise_on_error=False`.
- [ ] Legacy kwargs (`routing_key`, anything unknown) are accepted without
      `TypeError` and influence nothing.
- [ ] The module contains no `fieldsync` reference and no `"ts"` literal.
- [ ] All 20 unit tests pass: `pytest tests/test_producers.py -v`
- [ ] No linting errors: `ruff check src/navigator_eventbus/producers.py`
- [ ] Types clean: `mypy src/navigator_eventbus/producers.py`

---

## Test Specification

All 20 tests go in `tests/test_producers.py`. `asyncio_mode = "auto"` is set in
`pyproject.toml:108` — write plain `async def` tests, no decorator.

```python
# tests/test_producers.py
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from navigator_eventbus.core import BackpressureError, BusClosedError
from navigator_eventbus.envelope import EventEnvelope, Severity
from navigator_eventbus.evb import EventPriority
from navigator_eventbus.producers import BusCoreProducer


class FakeBus:
    """Minimal BusCore stand-in that records envelopes."""

    def __init__(self, *, raises=None, delay=0.0):
        self.published: list[EventEnvelope] = []
        self._raises = raises
        self._delay = delay

    async def publish(self, envelope: EventEnvelope) -> None:
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._raises is not None:
            raise self._raises
        self.published.append(envelope)


def provider_of(*returns):
    """bus_provider yielding a scripted sequence, then repeating the last."""
    seq = list(returns)
    def _p():
        return seq.pop(0) if len(seq) > 1 else seq[0]
    return _p
```

Required tests:

| # | Test | Asserts |
|---|---|---|
| 1 | `test_publish_event_wraps_body_and_topic` | `envelope.topic == queue_name`, `envelope.payload == body` |
| 2 | `test_bus_provider_is_called_lazily_per_publish` | provider returns `None` then a bus; call 1 no-ops (fail-soft), call 2 publishes. Proves no memoization. |
| 3 | `test_missing_bus_fail_soft` | returns normally, nothing published |
| 4 | `test_missing_bus_strict_raises` | propagates |
| 5 | `test_missing_bus_raises_runtime_error` | strict mode raises `RuntimeError` specifically |
| 6 | `test_publish_exception_fail_soft` | `BusClosedError` swallowed |
| 7 | `test_publish_exception_strict_raises` | original exception reaches caller |
| 8 | `test_backpressure_error_propagates_strict` | `BackpressureError` arrives **as itself**, not wrapped |
| 9 | `test_timeout_fail_soft` | bus sleeps past `timeout`; returns normally within the bound |
| 10 | `test_timeout_strict_raises` | raises `TimeoutError` |
| 11 | `test_legacy_kwargs_accepted` | `routing_key="x", whatever=1` → no raise, no effect on topic/payload |
| 12 | `test_explicit_timestamp_kwarg_is_used` | tz-aware datetime lands on `envelope.timestamp` |
| 13 | `test_default_timestamp_is_now` | tz-aware and close to now |
| 14 | `test_ts_field_in_body_is_not_special_cased` | `body={"ts": ...}` does NOT influence `envelope.timestamp`; `ts` survives untouched in `payload` |
| 15 | `test_naive_timestamp_always_raises` | **parametrize `raise_on_error=[True, False]`** — `ValueError` in both |
| 16 | `test_bad_timestamp_type_is_ignored` | `timestamp="2026-01-01"` (a `str`) is ignored as a legacy key; envelope takes default "now"; does **not** raise |
| 17 | `test_cancellation_is_not_swallowed` | cancelling the calling task mid-publish propagates `CancelledError` even fail-soft |
| 18 | `test_invalid_timeout_rejected_at_init` | `timeout=0` and `timeout=-1` raise `ValueError` from `__init__` |
| 19 | `test_constructor_source_severity_priority_stamped` | non-defaults reach the envelope; defaults are `None` / `INFO` / `NORMAL` |
| 20 | `test_severity_kwarg_is_ignored_not_honoured` | `publish_event(..., severity=Severity.CRITICAL)` does NOT override the instance default |

Plus the source-level guard (counts within the 20 as a single test):

```python
def test_producers_module_is_application_neutral():
    """Modeled on tests/test_neutrality.py — source scan, no import needed."""
    src = Path(__file__).parent.parent / "src" / "navigator_eventbus" / "producers.py"
    text = src.read_text()
    assert "fieldsync" not in text.lower()
    assert '"ts"' not in text and "'ts'" not in text
```

**Gotcha for test 17**: cancel the *task* awaiting `publish_event`, not the
inner publish — e.g. `task = asyncio.create_task(producer.publish_event(...))`,
`await asyncio.sleep(0)`, `task.cancel()`, then
`with pytest.raises(asyncio.CancelledError): await task`.

**Gotcha for test 9/10**: give `FakeBus` a `delay` comfortably larger than
`timeout` (e.g. `timeout=0.05`, `delay=1.0`) so the test is fast and not flaky.

---

## Agent Instructions

1. **Read the spec** at `sdd/specs/buscore-producer-facade.spec.md`, especially
   §2 (two-phase design + exception table) and §7 (gotchas).
2. **Verify the Codebase Contract** above with `grep`/`read` before writing code.
3. Write `tests/test_producers.py` FIRST (TDD), then `producers.py`.
4. Update `sdd/tasks/index/buscore-producer-facade.json` → `"in-progress"`.
5. Implement, verify every acceptance criterion.
6. Move this file to `sdd/tasks/completed/`, update the index → `"done"`.
7. Fill in the Completion Note.

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**:
**Date**:
**Notes**:

**Deviations from spec**: none | describe if any
