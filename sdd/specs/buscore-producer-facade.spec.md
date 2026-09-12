---
# SDD flow type and base branch (FEAT-145).
# - type: feature  (default)  → base_branch: main (this project uses main)
# - type: hotfix              → base_branch MUST be: main
type: feature
base_branch: main
---

<!-- LANGUAGE: This document MUST be written entirely in English (proper nouns keep native spelling). -->

# Feature Specification: BusCore Producer Facade

**Feature ID**: FEAT-433
**Date**: 2026-09-12
**Author**: Jesus Lara
**Status**: approved
**Target version**: 0.3.0

> **Input document**: `sdd/specs/buscore-producer-facade.brief.md` (upstream
> brief, originating from FieldSync's FEAT-577). No `.brainstorm.md` or
> `.proposal.md` exists for this feature — the brief is the authoritative
> input and was carried forward per the `/sdd-spec` §2 mapping.

---

## 1. Motivation & Business Requirements

### Problem Statement

Consuming applications currently have to hand-roll a small compatibility shim
in front of `BusCore`'s publish path whenever they need:

1. **Lazy resolution** of a `BusCore` instance that may not exist yet at call
   time (the bus is typically wired during application startup, while
   producer objects are constructed earlier).
2. **A bounded publish timeout** — `BusCore.publish()` is *not* unconditionally
   O(1): under the default `block` backpressure policy it performs
   `await queue.put(envelope)` (`src/navigator_eventbus/core.py:343`), which
   waits indefinitely when the target priority queue is full.
3. **A configurable choice between raising on publish failure** (strict
   callers, e.g. an admin HTTP endpoint that must report the failure to its
   client) **versus swallowing it** (fail-soft callers, e.g. ordinary
   background publishers that must never break their caller).

FieldSync's `services/eventbus/publisher.py` implements exactly this as a
local, FieldSync-owned class (`BusCorePublisherShim`) with zero
FieldSync-specific logic inside it — it is generic producer plumbing that
happens to live in the wrong repo.

This feature contributes that plumbing upstream as `BusCoreProducer` so any
application on navigator-eventbus can reuse it without re-inventing it, and so
FieldSync can delete its local copy once this is released and pinned.

### Goals

- Ship a generic, application-neutral producer facade at
  `src/navigator_eventbus/producers.py`.
- Own lazy bus resolution, a bounded publish timeout, and explicit
  strict-vs-fail-soft error propagation — the three concerns every consuming
  app currently re-implements.
- Accept legacy/duck-typed keyword arguments (e.g. `routing_key`) for call-site
  compatibility with older producer seams, **without** encoding an
  application-specific rule for any of them.
- Preserve behavioral parity with the shim it replaces, **except** for the
  `body["ts"]` dependency, which is deliberately not carried forward (see §7
  Known Risks).
- Let a producer instance stamp a fixed `source`, `severity` and `priority` on
  everything it emits, so events published through the facade are attributable
  on the bus and are not locked to `INFO`/`NORMAL`.
- Export `BusCoreProducer` from the package root, mirroring the existing eager
  export convention used for `BusCore` and `CompositeBackend`.
- Bump the package version so a consuming app can pin the release.

### Non-Goals (explicitly out of scope)

- Anything FieldSync-specific: no `Topic` / `TargetScope` / tenant-routing
  concept, no import of FieldSync code. The brief's reference to
  `BusCorePublisherShim` is **behavioral context, not a dependency**.
- Any change to `BusCore.publish()` itself.
- Any change to any backend (`RedisStreamsBackend`, `CompositeBackend`,
  `RedisPubSubBackend`).
- A consumer-side counterpart facade. This is producer-only.
- Topic derivation or governance — `queue_name` is the raw topic string; the
  facade adds no prefixing, no `TOPICS.md` validation, and no routing rules of
  its own.
- **Per-call** `source` / `severity` / `priority` overrides. These are settable
  **per producer instance** through the constructor (§8 Q3 resolved), but
  `publish_event` keeps the brief's exact signature and takes all three from
  the instance. A caller needing per-event variation holds a second producer.
- Special-casing any payload field name (notably `ts`) to derive a timestamp.

---

## 2. Architectural Design

### Overview

`BusCoreProducer` is a thin, stateless-except-for-config adapter class. It
holds no bus reference: it holds a **provider callable** and invokes it on
every `publish_event()` call, so a bus that is created (or replaced) after the
producer was constructed is picked up on the next publish without any
re-wiring.

Each `publish_event()` call proceeds in two phases, and **the phase boundary is
the error-policy boundary**:

**Phase 1 — request validation (always raises).** Build an `EventEnvelope` with
`topic=queue_name`, `payload=body`, and `source` / `severity` / `priority` taken
from the producer instance's configured defaults. `timestamp` comes from a
`timestamp: datetime` keyword when supplied, and otherwise is left to
`EventEnvelope`'s own `datetime.now(timezone.utc)` default factory
(`envelope.py:133-135`) — the facade does not compute "now" itself, so there is
exactly one definition of it. This phase happens **outside** the error-policy
`try`, so a malformed request propagates regardless of `raise_on_error`.

**Phase 2 — delivery (governed by `raise_on_error`).** Call `bus_provider()`;
a `None` return is a failure, not a silent no-op. Then await
`BusCore.publish(envelope)` inside `async with asyncio.timeout(...)` — the
house idiom for bounded awaits in this package (`core.py:560`,
`hooks/webhook/preprocess.py:164`, `subscribers/notification.py:516`). On any
failure here — missing bus, `BusCore.publish()` raising (`BusClosedError`,
`BackpressureError`, or anything else), or timeout expiry:

- `raise_on_error=True` → propagate to the caller.
- `raise_on_error=False` → log at WARNING with `exc_info` and return normally.

**Why the boundary sits there (design decision, §8 Q1 resolved).** The three
delivery failures are *operational* — transient, environment-dependent, and
plausibly resolved on the next call (the bus gets wired, the queue drains).
A rejected envelope is a *caller bug*: `EventEnvelope.__post_init__` raises
`ValueError` on a naive `timestamp` (`envelope.py:157-162`) deterministically,
on every call, forever. Swallowing that is not degraded delivery — it is 100%
silent event loss on a code path whose defining property is that nobody is
watching it.

Two considerations settled this:

1. **Asymmetry of recovery.** If the facade raises, a caller wanting total
   safety wraps the call in `try/except` — recoverable. If the facade swallows,
   a caller wanting to see the bug has no way to opt in — unrecoverable. The
   recoverable default wins.
2. **Internal consistency.** `timeout <= 0` already raises `ValueError` from
   `__init__` unconditionally. Argument validation that always raises is
   therefore already this class's rule; routing envelope construction through
   `raise_on_error` would contradict it.

**Exception types in strict mode** (pinned so tests are deterministic):

| Failure | Raises | Governed by `raise_on_error`? |
|---|---|---|
| Naive / non-`datetime` `timestamp` | `ValueError` (from `EventEnvelope`) | **No — always raises** |
| `timeout <= 0` at construction | `ValueError` (from `__init__`) | **No — always raises** |
| `bus_provider()` returned `None` | `RuntimeError` | yes |
| Bus closing | `BusClosedError` | yes |
| Queue full under `reject` policy | `BackpressureError` | yes |
| Publish exceeded `timeout` | `TimeoutError` | yes |

Bus exception types are **never flattened** — `BusClosedError` and
`BackpressureError` reach a strict caller as themselves, not wrapped.

**Cancellation is never swallowed.** `asyncio.CancelledError` derives from
`BaseException`, so the `except Exception` used by the fail-soft path does not
catch it; external cancellation of the calling task propagates unchanged.

### Component Diagram

```
caller ──publish_event(body, queue_name, **kwargs)──→ BusCoreProducer
                                                           │
  PHASE 1 — request validation (always raises)             │
                                                           ▼
                                                    EventEnvelope(
                                                      topic=queue_name,
                                                      payload=body,
                                                      timestamp=kwargs|default,
                                                      source=self._source,
                                                      severity=self._severity,
                                                      priority=self._priority)
                                                           │
                                        ValueError ────────┤ (escapes: naive ts)
  ─────────────────────────────────────────────────────────┼──────────────────
  PHASE 2 — delivery (raise_on_error governs)              ▼
                                                    bus_provider()
                                                           │
                                                   BusCore | None
                                                           │
                                        async with asyncio.timeout(timeout)
                                                           │
                                              await BusCore.publish(envelope)
                                                           │
                                        ┌──────────────────┴──────────────────┐
                                    success                                failure
                                        │                                      │
                                     return                 raise_on_error ? raise : log+return
```

### Integration Points

| Existing Component | Integration Type | Notes |
|---|---|---|
| `BusCore.publish()` | calls | The only bus method this facade touches. Verified `core.py:287`. |
| `EventEnvelope` | constructs | `topic` / `payload` / optional `timestamp`; all other fields take their dataclass defaults. Verified `envelope.py:105-142`. |
| `BusClosedError`, `BackpressureError` | catches | The two documented `publish()` exceptions; caught as part of a broad `except Exception`, not enumerated in a narrow handler. Verified `core.py:61,65`. |
| `navigator_eventbus/__init__.py` | extends | Adds `BusCoreProducer` to the eager imports and to `__all__`, mirroring `BusCore`. |

### Data Models

No new Pydantic models and no new dataclasses. `BusCoreProducer` is a plain
class holding three configuration values; the wire contract is the existing
frozen `EventEnvelope`.

### New Public Interfaces

```python
# src/navigator_eventbus/producers.py  (NEW MODULE)
from collections.abc import Callable
from typing import Any, Optional

from navigator_eventbus.core import BusCore
from navigator_eventbus.envelope import Severity
from navigator_eventbus.evb import EventPriority


class BusCoreProducer:
    """Generic compatibility facade in front of ``BusCore.publish()``.

    Owns lazy bus resolution, a bounded publish timeout, and explicit
    strict-vs-fail-soft error propagation. Contains NO application-specific
    routing, topic, or schema rules — ``body`` is opaque payload and
    ``queue_name`` is the raw topic string.
    """

    def __init__(
        self,
        bus_provider: Callable[[], Optional[BusCore]],
        *,
        timeout: float = 1.0,
        raise_on_error: bool = False,
        source: Optional[str] = None,
        severity: Severity = Severity.INFO,
        priority: EventPriority = EventPriority.NORMAL,
    ) -> None: ...

    async def publish_event(
        self,
        body: dict[str, Any],
        queue_name: str,
        **kwargs: Any,
    ) -> None: ...
```

**Constructor envelope defaults (§8 Q3 resolved).** `source`, `severity` and
`priority` configure the producer *instance*; every envelope it emits carries
them. `source` is the load-bearing one — without it every event this facade
publishes is unattributable on the bus, and `EventEnvelope.source` exists
precisely to answer "who emitted this". `severity` and `priority` remove the
ceiling that would otherwise force any caller needing a non-`NORMAL` publish to
bypass the facade entirely.

This does **not** violate the brief's `**kwargs` rule. That rule forbids
encoding *application-specific* meaning into undocumented legacy keys (the `ts`
problem). `source` / `severity` / `priority` are core navigator-eventbus
concepts exposed as explicit, documented, keyword-only constructor parameters —
and `publish_event`'s signature stays exactly as the brief specifies.

**`publish_event` contract:**

- `body` becomes `EventEnvelope.payload` verbatim (no copy, no mutation, no
  key inspection).
- `queue_name` becomes `EventEnvelope.topic` verbatim (no prefixing, no
  validation against `TOPICS.md`).
- `source`, `severity` and `priority` come from the producer instance's
  configured defaults on **every** envelope. They are not readable from
  `kwargs` — a caller passing `severity=...` to `publish_event` is ignored like
  any other legacy key (see §7 gotchas).
- `kwargs` is accepted for call-site compatibility with older duck-typed
  producer seams. **Exactly one key is honoured**: `timestamp`, when its value
  is a `datetime`, populates `EventEnvelope.timestamp`. Every other key —
  including `routing_key` — is accepted and ignored without raising
  `TypeError`. No other key is parsed or special-cased; in particular the
  module contains no reference to a field named `ts`.
- `timeout` is in seconds and must be `> 0`; `None` disables the bound
  (the publish is awaited unbounded). A non-positive `float` raises
  `ValueError` from `__init__`, not from `publish_event`.
- `self.logger = logging.getLogger("navigator_eventbus.producers")`, matching
  the per-module logger convention (`core.py:190`, `dlq.py:145`,
  `evb.py:200`).

---

## 3. Module Breakdown

### Module 1: `BusCoreProducer`
- **Path**: `src/navigator_eventbus/producers.py` *(new file)*
- **Responsibility**: The facade class described in §2 — lazy bus resolution,
  envelope construction, bounded publish, error policy, logging.
- **Depends on**: `navigator_eventbus.core.BusCore` (type only),
  `navigator_eventbus.envelope.EventEnvelope` (constructed),
  `navigator_eventbus.envelope.Severity` and `navigator_eventbus.evb.EventPriority`
  (constructor default values). No new external dependency.
- **Import-cycle note**: `envelope.py` already imports `EventPriority` from
  `evb.py` (`envelope.py:21`), and nothing imports `producers.py`, so this new
  leaf module introduces no cycle.

### Module 2: Public export
- **Path**: `src/navigator_eventbus/__init__.py` *(modified)*
- **Responsibility**: Add `from navigator_eventbus.producers import BusCoreProducer`
  to the **eager** import block and `"BusCoreProducer"` to `__all__`.
  `producers.py` imports nothing optional, so it must **not** go through the
  `_QUEUE_EXPORTS` lazy `__getattr__` map (`__init__.py:88-106`) — that map
  exists only to defer the queue machinery.
- **Depends on**: Module 1

### Module 3: Unit tests
- **Path**: `tests/test_producers.py` *(new file)*
- **Responsibility**: The full matrix in §4, using a fake `bus_provider` and a
  fake `BusCore` recorder. No Redis, no real event loop machinery beyond
  `pytest-asyncio`.
- **Depends on**: Module 1

### Module 4: Version bump
- **Path**: `src/navigator_eventbus/version.py` *(modified)*
- **Responsibility**: `0.2.4` → `0.3.0` (new public interface, backwards
  compatible). `pyproject.toml` reads the version from this module at build
  time (`dynamic = ["version"]`, `pyproject.toml:7`) — do **not** hard-code a
  version anywhere else. `tests/test_package.py::test_package_imports`
  compares the root re-export against `version.py` rather than a literal, so
  it does not need editing.
- **Depends on**: Modules 1–3 complete

---

## 4. Test Specification

### Unit Tests

| Test | Module | Description |
|---|---|---|
| `test_publish_event_wraps_body_and_topic` | 1 | Fake bus records the envelope; assert `envelope.topic == queue_name` and `envelope.payload == body` (identity of contents, exactly). |
| `test_bus_provider_is_called_lazily_per_publish` | 1 | Provider returns `None` on call 1 and a fake bus on call 2; assert call 1 is a no-op (fail-soft) and call 2 publishes. Proves no memoization at `__init__`. |
| `test_missing_bus_fail_soft` | 1 | `bus_provider()` → `None`, `raise_on_error=False`: returns normally, nothing published. |
| `test_missing_bus_strict_raises` | 1 | Same with `raise_on_error=True`: propagates. |
| `test_publish_exception_fail_soft` | 1 | Fake bus raises `BusClosedError`, `raise_on_error=False`: swallowed. |
| `test_publish_exception_strict_raises` | 1 | Same with `raise_on_error=True`: the original exception reaches the caller. |
| `test_backpressure_error_propagates_strict` | 1 | Fake bus raises `BackpressureError`; strict mode propagates it **as itself** (guards that the facade does not flatten or wrap bus exception types). |
| `test_missing_bus_raises_runtime_error` | 1 | Strict mode with `bus_provider()` → `None` raises `RuntimeError` specifically, per the §2 exception-type table. |
| `test_timeout_fail_soft` | 1 | Fake bus `publish` sleeps past `timeout`, `raise_on_error=False`: returns normally within the bound. |
| `test_timeout_strict_raises` | 1 | Same with `raise_on_error=True`: raises `TimeoutError`. |
| `test_legacy_kwargs_accepted` | 1 | `publish_event(body, topic, routing_key="x", whatever=1)` does not raise, and neither key influences `topic` or `payload`. |
| `test_explicit_timestamp_kwarg_is_used` | 1 | A tz-aware `datetime` passed as `timestamp=` lands on `envelope.timestamp`. |
| `test_default_timestamp_is_now` | 1 | With no `timestamp=`, `envelope.timestamp` is tz-aware and close to now. |
| `test_ts_field_in_body_is_not_special_cased` | 1 | `body={"ts": <some datetime/str>}` does **not** influence `envelope.timestamp`; `ts` survives untouched inside `payload`. Directly guards the §7 risk. |
| `test_naive_timestamp_always_raises` | 1 | Naive `datetime` passed as `timestamp=` raises `ValueError` in **both** modes — parametrize over `raise_on_error=[True, False]`. Pins the §2 phase boundary. |
| `test_bad_timestamp_type_is_ignored` | 1 | `timestamp="2026-01-01"` (a `str`, not a `datetime`) is ignored as a legacy key and the envelope takes the default "now" — it does **not** reach `EventEnvelope` and does **not** raise. Guards the `isinstance` check. |
| `test_constructor_source_stamped_on_envelope` | 1 | `BusCoreProducer(..., source="svc")` → `envelope.source == "svc"`; default is `None`. |
| `test_constructor_severity_and_priority_stamped` | 1 | Non-default `Severity.ERROR` / `EventPriority.HIGH` reach the envelope; defaults are `INFO` / `NORMAL`. |
| `test_severity_kwarg_is_ignored_not_honoured` | 1 | `publish_event(body, topic, severity=Severity.CRITICAL)` does **not** override the instance default — proves `kwargs` is inert and the only path is the constructor. |
| `test_cancellation_is_not_swallowed` | 1 | Cancelling the calling task while `publish` is in flight propagates `CancelledError` even with `raise_on_error=False`. |
| `test_invalid_timeout_rejected_at_init` | 1 | `timeout=0` and `timeout=-1` raise `ValueError` from `__init__`. |
| `test_producer_exported_from_root` | 2 | `from navigator_eventbus import BusCoreProducer` works and the name is in `__all__`. |
| `test_producers_module_is_application_neutral` | 1 | Source-level guard: `producers.py` contains no `fieldsync` reference and no `"ts"` literal. Mirrors the `tests/test_neutrality.py` source-scanning pattern. |

### Integration Tests

| Test | Description |
|---|---|
| `test_publish_through_real_buscore` | Construct a real `BusCore`, `start()` it, subscribe a recording handler, publish via `BusCoreProducer`, assert the handler receives the topic, payload **and the configured `source`**. Memory only — no backend, no Redis. |
| `test_high_priority_routes_through_priority_queue` | A producer built with `priority=EventPriority.HIGH` publishes to a real `BusCore`; assert the received envelope's `.priority` is `HIGH`, proving the constructor default reaches the dispatch path and is not dropped. |

### Test Data / Fixtures

```python
# tests/test_producers.py
@pytest.fixture
def fake_bus():
    """Minimal BusCore stand-in that records envelopes."""
    class _FakeBus:
        def __init__(self) -> None:
            self.published: list[EventEnvelope] = []
        async def publish(self, envelope: EventEnvelope) -> None:
            self.published.append(envelope)
    return _FakeBus()


@pytest.fixture
def provider_for():
    """Build a bus_provider that yields a scripted sequence of returns."""
    def _build(*returns):
        it = iter(returns)
        return lambda: next(it)
    return _build
```

`tests/conftest.py` already exists (228 B) — check it before adding
`asyncio_mode` configuration; do not duplicate a setting that is already there
or in `pyproject.toml`.

---

## 5. Acceptance Criteria

> This feature is complete when ALL of the following are true:

- [ ] `src/navigator_eventbus/producers.py` exists and defines
      `BusCoreProducer` with the §2 signature.
- [ ] The facade contains **no** application-specific import, topic, or schema
      rule — no `fieldsync` reference anywhere, and no `"ts"` literal.
- [ ] `publish_event` wraps the supplied `body` as the envelope's `payload`
      and calls `BusCore.publish()` with `queue_name` as the envelope's
      `topic`.
- [ ] `bus_provider` is called lazily on **every** `publish_event` call, not
      memoized at `__init__` — proven by the provider returning `None` first
      and a working bus later, with the later call succeeding.
- [ ] Strict (`raise_on_error=True`) vs fail-soft (`raise_on_error=False`)
      behavior is unit tested for all three **delivery** failure sources:
      missing bus, `BusCore.publish()` raising, and timeout expiry.
- [ ] **Request-validation failures always raise**, in both modes: a naive or
      non-`datetime` value reaching `EventEnvelope` raises `ValueError`, and
      `timeout <= 0` raises `ValueError` from `__init__`. Neither is governed
      by `raise_on_error`.
- [ ] Strict-mode exception types match the §2 table exactly: `RuntimeError`
      for a missing bus, `TimeoutError` for timeout expiry, and
      `BusClosedError` / `BackpressureError` propagated unwrapped.
- [ ] `asyncio.CancelledError` propagates in fail-soft mode.
- [ ] Legacy keyword arguments (e.g. `routing_key`) are accepted without
      raising `TypeError` and without influencing `topic` or `payload`.
- [ ] A `timestamp: datetime` keyword populates `EventEnvelope.timestamp`;
      with no such keyword the envelope's own default factory supplies it.
- [ ] A `ts` key inside `body` has no effect on `envelope.timestamp` and is
      passed through untouched in `payload`.
- [ ] `source`, `severity` and `priority` are keyword-only constructor
      parameters defaulting to `None` / `Severity.INFO` /
      `EventPriority.NORMAL`, and each reaches every published envelope.
- [ ] Passing `severity=` / `priority=` / `source=` to `publish_event` does
      **not** override the instance defaults — `kwargs` remains inert.
- [ ] `from navigator_eventbus import BusCoreProducer` works; the name is in
      `__all__`; it is an **eager** export, not routed through the lazy
      `__getattr__` queue map.
- [ ] No breaking change to any existing public API (`BusCore`, `EventBus`,
      `EventEnvelope` untouched).
- [ ] `pytest tests/test_producers.py -v` passes.
- [ ] The full suite still passes: `pytest -q`.
- [ ] Lint and types clean on the changed files: `ruff check` and `mypy` on
      `src/navigator_eventbus/producers.py`.
- [ ] `src/navigator_eventbus/version.py` bumped to `0.3.0` — the only place
      the version is written (`pyproject.toml` reads it dynamically).
- [ ] Google-style docstrings and strict type hints on every public method
      (project standard, `CLAUDE.md` §Code Standards).

---

## 6. Codebase Contract

> **CRITICAL — Anti-Hallucination Anchor**
> Every entry below was verified by reading the source at the stated path on
> 2026-09-12 against `main` @ `273078d`.

### Verified Imports

```python
from navigator_eventbus.core import BusCore                # verified: src/navigator_eventbus/core.py:92
from navigator_eventbus.core import BusClosedError         # verified: src/navigator_eventbus/core.py:61
from navigator_eventbus.core import BackpressureError      # verified: src/navigator_eventbus/core.py:65
from navigator_eventbus.envelope import EventEnvelope      # verified: src/navigator_eventbus/envelope.py:105
from navigator_eventbus.envelope import Severity           # verified: src/navigator_eventbus/envelope.py:89
from navigator_eventbus.evb import EventPriority           # verified: src/navigator_eventbus/evb.py:51
```

All six are already re-exported from the package root
(`src/navigator_eventbus/__init__.py:18-27`), so tests may import them from
either path.

### Existing Class Signatures

```python
# src/navigator_eventbus/core.py
class BusClosedError(RuntimeError):        # line 61 — "Raised when publishing after close() has begun."
class BackpressureError(RuntimeError):     # line 65 — "Raised to the emitter when the reject policy is active and full."

class BusCore:                             # line 92
    async def publish(self, envelope: EventEnvelope) -> None:   # line 287
        # Raises: BusClosedError (line 291), BackpressureError (line 318, reject policy only)
        # NOTE: under POLICY_BLOCK (the default) this awaits `queue.put(envelope)`
        #       at line 343 and can block indefinitely — this is precisely why
        #       BusCoreProducer's bounded timeout is load-bearing.
    def __init__(                                               # line 128
        self, *, workers: int = 4, queue_size: int = 1024,
        handler_timeout: float = 30.0, retry_attempts: int = 3,
        retry_base_delay: float = 0.1,
        backpressure: Optional[dict[str, str]] = None,
        default_backpressure: str = POLICY_BLOCK,
        drain_timeout: float = 5.0,
        on_dlq: Optional[DLQCallback] = None,
        backend: Optional[TransportBackend] = None,
    ) -> None: ...
    async def start(self) -> None:                              # line 203
    async def close(self, drain_timeout: Optional[float] = None) -> None:  # line 217
    self.logger = logging.getLogger("navigator_eventbus.core")  # line 190


# src/navigator_eventbus/envelope.py
@dataclass(frozen=True, slots=True)
class EventEnvelope:                                            # line 105
    topic: str                                                  # line 130  (required)
    payload: dict[str, Any]                                     # line 131  (required)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))        # line 132
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))     # line 133-135
    source: Optional[str] = None                                # line 136
    severity: Severity = Severity.INFO                          # line 137
    priority: EventPriority = EventPriority.NORMAL              # line 138
    correlation_id: Optional[str] = None                        # line 139
    trace_context: Optional[dict] = None                        # line 140
    metadata: dict[str, Any] = field(default_factory=dict)      # line 141
    schema_version: int = ENVELOPE_SCHEMA_VERSION               # line 142

    def __post_init__(self) -> None:                            # line 144
        # Raises ValueError if timestamp is not a datetime (line 152-156)
        # Raises ValueError if timestamp is NAIVE (line 157-162)  ← governs §8 Q1


# src/navigator_eventbus/evb.py
class EventPriority(Enum):                                      # line 51
    LOW = 0; NORMAL = 5; HIGH = 10; CRITICAL = 15               # lines 53-56
```

**Correction to the brief.** The brief's `EventEnvelope` sketch lists
`event_id`, `timestamp`, `source`, `severity`, `priority` as if they were
required fields. They all carry defaults (lines 132-142). Only `topic` and
`payload` are required, which is what makes the facade's two-argument
`publish_event` signature sufficient.

### Integration Points

| New Component | Connects To | Via | Verified At |
|---|---|---|---|
| `BusCoreProducer.publish_event()` | `BusCore.publish()` | `await` inside `asyncio.timeout` | `src/navigator_eventbus/core.py:287` |
| `BusCoreProducer.publish_event()` | `EventEnvelope(...)` | constructor, `topic=` + `payload=` (+ optional `timestamp=`) | `src/navigator_eventbus/envelope.py:105-142` |
| `BusCoreProducer` | `navigator_eventbus.__all__` | eager re-export | `src/navigator_eventbus/__init__.py:18-27, 45-82` |
| `version.py` | `pyproject.toml` | `dynamic = ["version"]` | `pyproject.toml:7` |

### Does NOT Exist (Anti-Hallucination)

- ~~`src/navigator_eventbus/producers.py`~~ — **new file**, confirmed absent.
- ~~`tests/test_producers.py`~~ — **new file**, confirmed absent.
- ~~`navigator_eventbus.BusCoreProducer`~~ — not currently exported.
- ~~`BusCorePublisherShim`~~ — FieldSync-owned, lives in
  `fieldsync/services/eventbus/publisher.py`. **Do NOT import it. Do NOT
  vendor it.** It is behavioral reference only.
- ~~`BusCore.publish_event()`~~ — not a method. `BusCore` exposes `publish()`.
- ~~`BusCore.emit()`~~ — not a method on `BusCore`. `emit()` exists on
  `EventBus` (`evb.py:349`), a different class.
- ~~`EventEnvelope.ts`~~ / ~~`EventEnvelope.routing_key`~~ — not fields.
- ~~`navigator_eventbus.producers.Producer`~~ / ~~`BaseProducer`~~ /
  ~~`AbstractProducer`~~ — no producer base class exists in this package;
  `BusCoreProducer` inherits from nothing.
- ~~`src/navigator_eventbus/base/`~~ — the spec template's "AbstractBase
  pattern from `src/navigator_eventbus/base/`" refers to a directory that does
  **not** exist in this repo. Ignore that template hint.
- ~~`Topic`~~ / ~~`TargetScope`~~ — FieldSync concepts, absent here and out of
  scope.

---

## 7. Implementation Notes & Constraints

### Patterns to Follow

- **Bounded awaits**: `async with asyncio.timeout(...)`, the idiom already used
  at `core.py:560`, `hooks/webhook/preprocess.py:164`, and
  `subscribers/notification.py:516`. `requires-python = ">=3.11"`
  (`pyproject.toml:14`), so it is available. Prefer it over
  `asyncio.wait_for()` for the new code.
- **Per-module logger**: `self.logger = logging.getLogger("navigator_eventbus.producers")`
  in `__init__`, matching `core.py:190` / `dlq.py:145` / `evb.py:200`. Never
  `print`.
- **Catch `(asyncio.TimeoutError, TimeoutError)`** when handling timeout
  explicitly — the codebase pairs both spellings (`core.py:230,253`;
  `subscribers/audit.py:253`) even though they are the same object on 3.11+.
- **No Pydantic here.** `EventEnvelope` is a deliberately non-Pydantic frozen
  slotted dataclass for hot-path speed (`envelope.py:10-13`). The project rule
  "Pydantic models for all data structures" applies at ingress boundaries, not
  on the publish path. `BusCoreProducer` adds no model.
- **Source-scanning neutrality test**: follow the shape of
  `tests/test_neutrality.py` (regex over `Path.rglob("*.py")`) for the
  application-neutrality guard.
- Google-style docstrings and strict type hints throughout.

### Known Risks / Gotchas

- **Timestamp ownership (carried from the brief).** The shim being replaced
  reads a FieldSync-specific `body["ts"]` field to populate the timestamp.
  This facade must **not** acquire that undocumented, application-specific
  dependency. The generic `timestamp: datetime` keyword is the sanctioned
  escape hatch; any caller that stores its time under `ts` extracts it itself
  and passes it explicitly. *Mitigation*: acceptance criterion + the dedicated
  `test_ts_field_in_body_is_not_special_cased` test + the source-level guard
  asserting no `"ts"` literal in the module.
- **Behavioral divergence from the shim.** Because of the point above,
  FieldSync's cut-over is **not** a drop-in swap: its call sites that relied on
  `body["ts"]` must start passing `timestamp=` explicitly, or they will
  silently begin stamping "now". This must be called out in the release notes
  — it is the one place where parity is intentionally broken.
- **`**kwargs` swallows typos, and now also swallows plausible-looking envelope
  fields.** Accepting arbitrary keywords means `publish_event(body, topic,
  timestmap=x)` is silently ignored rather than a `TypeError`. With §8 Q3
  resolved toward constructor defaults, this extends to a sharper trap:
  `publish_event(body, topic, severity=Severity.CRITICAL)` looks like it should
  work and silently does nothing, because `severity` is configured per instance.
  That is the explicit price of call-site compatibility with the duck-typed
  seams this replaces. *Mitigation*: state it plainly in the `publish_event`
  docstring ("only `timestamp` is read from `**kwargs`; `source`, `severity`
  and `priority` are per-instance constructor arguments"); emit a `DEBUG`-level
  log of unrecognised keys (DEBUG, not WARNING — legacy keys like
  `routing_key` are expected and must not generate noise); and pin the
  behaviour with `test_severity_kwarg_is_ignored_not_honoured`.
- **`timestamp` is type-checked, not coerced.** Only an `isinstance(...,
  datetime)` value is forwarded; a `str` or `float` is treated as an
  unrecognised legacy key and the envelope falls back to "now". This keeps the
  facade out of the date-parsing business, but means a caller passing an ISO
  string gets a silently wrong timestamp rather than an error. Documented in
  the docstring and pinned by `test_bad_timestamp_type_is_ignored`.
- **The default `timeout=1.0` interacts with the `block` backpressure policy.**
  Under a saturated queue with the default policy, `publish()` waits for space;
  the producer will convert that into a timeout after 1 s. A fail-soft caller
  therefore *drops* events under sustained backpressure. This is the intended
  trade-off, but it should be explicit in the class docstring so operators
  reading a quiet log do not mistake it for a delivery guarantee.
- **Fire-and-forget backend fan-out is unaffected.** `BusCore.publish()` spawns
  the backend publish as a background task *before* the local enqueue
  (`core.py:297-303`), so a producer-side timeout on the local enqueue does not
  cancel a fan-out already in flight. The event may reach the transport while
  the producer logs a timeout. Worth a docstring note; not a defect to fix
  here.
- **No topic governance.** `queue_name` is passed through verbatim, so this
  facade can emit under any namespace, including reserved ones in `TOPICS.md`.
  That is consistent with `BusCore.publish()` itself (which also does not
  govern topics) and out of scope for this feature.

### External Dependencies

| Package | Version | Reason |
|---|---|---|
| *(none)* | — | `producers.py` uses only the stdlib plus two in-package modules. No `pyproject.toml` dependency change. |
| `pytest-asyncio` | existing | Already a dev dependency — used by the whole async suite. |

---

## 8. Open Questions

- [x] Where does the facade live and what is it called? — *Resolved in brief*:
      `BusCoreProducer` in `src/navigator_eventbus/producers.py`.
- [x] How is the timestamp sourced? — *Resolved in brief*: a generic
      `timestamp: datetime` keyword, else `EventEnvelope`'s own "now" default.
      **Never** by special-casing a `ts`-named field.
- [x] Should legacy kwargs like `routing_key` raise? — *Resolved in brief*:
      accepted without raising, and without influencing `topic`/`payload`.
- [x] Is `bus_provider` memoized? — *Resolved in brief*: no, resolved lazily
      once per `publish_event` call.
- [x] Eager or lazy package export? — *Resolved by codebase research*: eager,
      mirroring `BusCore`. The `_QUEUE_EXPORTS` lazy map exists only to defer
      the optional queue machinery, which `producers.py` does not touch.
- [x] **Q1 — Does `raise_on_error` govern envelope *construction* failure, or
      only *delivery* failure?** — *Resolved 2026-09-12, Jesus Lara*:
      **only delivery failure.** Request validation always raises, in both
      modes; `raise_on_error` governs the three operational failures (missing
      bus, `publish()` raising, timeout). Rationale, the strict-mode exception
      types, and the phase boundary are in §2 "Why the boundary sits there".
      This reverses the draft's original "uniform" default — the deciding
      arguments were the asymmetry of recovery (a raising facade is
      recoverable by the caller; a swallowing one is not) and internal
      consistency with `timeout <= 0` already raising from `__init__`.
- [x] **Q2 — Version bump: `0.3.0` or `0.2.5`?** — *Resolved 2026-09-12,
      Jesus Lara*: **`0.3.0`**. Current is `0.2.4` (`version.py:7`). Repo
      history shows both patterns (0.2.2 and 0.2.3 shipped features as patch
      bumps), but the one minor bump — `0.2.0`, commit `8889f91` — marked new
      public surface, which is what this is: a new top-level name in
      `__all__`. Adding a parameter to an existing class and adding a class
      are different events.
- [x] **Q3 — Should `priority` / `severity` / `source` be settable?** —
      *Resolved 2026-09-12, Jesus Lara*: **yes, as keyword-only constructor
      arguments** (`source=None`, `severity=Severity.INFO`,
      `priority=EventPriority.NORMAL`). `publish_event`'s signature is
      unchanged from the brief, and `**kwargs` stays inert. The decisive
      factor was `source`: leaving it `None` makes every event from this
      facade unattributable on the bus, which is an observability hole rather
      than a scope reduction. Per-call overrides were considered and deferred
      — they can be added later as additive keyword-only parameters without
      breaking this contract.

> No open questions remain. The spec is ready for `/sdd-task`.

---

## Worktree Strategy

- **Default isolation unit**: `per-spec` — all tasks run sequentially in one
  worktree.
- **Rationale**: Module 2 (export), Module 3 (tests) and Module 4 (version
  bump) all depend on Module 1 existing, and Modules 2 and 4 touch shared files
  (`__init__.py`, `version.py`). Parallelising them would only manufacture
  conflicts. The whole feature is one small new module plus two one-line edits.
- **Cross-feature dependencies**: none. This feature touches no file owned by
  an in-flight spec (`eventbus-composite-backend`,
  `redis-streams-backend-extensions`, `pull-queues`, `webhook-support`), and
  adds no backend or queue behavior.
- **Suggested worktree**:
  ```bash
  git worktree add -b feat-FEAT-433-buscore-producer-facade \
    .claude/worktrees/feat-FEAT-433-buscore-producer-facade HEAD
  ```
  Given the size, working directly on a short-lived feature branch is also
  defensible — `CLAUDE.md` §"When NOT to Use Worktrees" exempts single-task
  features.

---

## Revision History

| Version | Date | Author | Change |
|---|---|---|---|
| 0.1 | 2026-09-12 | Jesus Lara | Initial draft from `buscore-producer-facade.brief.md` (FieldSync FEAT-577); codebase contract verified against `main` @ 273078d |
| 0.2 | 2026-09-12 | Jesus Lara | Resolved Q1/Q2/Q3. Q1: request validation always raises, `raise_on_error` governs delivery only (reverses the draft default) — §2 rewritten into two phases with a strict-mode exception-type table. Q2: target `0.3.0`. Q3: `source`/`severity`/`priority` added as keyword-only constructor defaults. §1, §3, §4, §5, §7 updated to match. Status → approved. |
