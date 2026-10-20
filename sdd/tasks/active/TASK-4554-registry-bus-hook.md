# TASK-4554: Registry bus hook — attach_bus / detach_bus + payload serializer

**Feature**: FEAT-711 — Durable lifecycle fan-out
**Spec**: `sdd/specs/durable-lifecycle-fanout.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Spec §1 problem 1-2 / Goal G1 / Module 1. Today an `EventRegistry` can only get an `EventBus` at
construction (`event_bus=`), so the process-wide global registry — built lazily by
`get_global_registry()` — can never forward to a bus, and the forwarded payload is always
`event.to_dict()` with no way to redact or skip it. This task adds a late-binding bus hook and a
serializer hook. It is independent of the outbox work (TASK-4555..4558).

---

## Scope

- Add a module-level type alias `BusSerializer = Callable[[LifecycleEvent], Optional[dict[str, Any]]]`.
- Add `bus_serializer: Optional[BusSerializer] = None` to `_Subscription` (as the LAST field — it has a default).
- Add `bus_serializer=` kwarg to `EventRegistry.__init__` (registry-level default) and to `EventRegistry.subscribe` (per-subscription override).
- Add `attach_bus(bus, *, channel_prefix=None, serializer=None)`, `detach_bus()` and a read-only `event_bus` property.
- In the dual-emit forward branch, resolve the payload: subscription serializer → registry serializer → `event.to_dict()`. A `None` result skips the publish. A raising serializer is logged with `logger.exception` and that forward is skipped (never raised).
- Write unit tests in a NEW file `tests/lifecycle/test_registry_bus_hook.py`.

**NOT in scope**: the outbox, `BusCore`, `EventBus` changes (TASK-4555..4558); making `create_task` scheduling synchronous (spec §7 residual window — accepted); changing `forward_to_global` behavior; editing existing test files.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/lifecycle/registry.py` | MODIFY | serializer alias, `_Subscription` field, kwargs, attach/detach/property, forward branch |
| `tests/lifecycle/test_registry_bus_hook.py` | CREATE | unit tests for the new API |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.lifecycle.registry import EventRegistry   # verified: src/navigator_eventbus/lifecycle/registry.py:90
from navigator_eventbus.lifecycle.base import LifecycleEvent       # verified: src/navigator_eventbus/lifecycle/base.py:21 (to_dict at :52)
from navigator_eventbus.lifecycle.global_registry import get_global_registry, scope  # verified: global_registry.py:36, :58
from navigator_eventbus.lifecycle.trace import TraceContext        # used by tests/lifecycle/test_emit_overhead.py
```
Already imported at the top of `registry.py` (lines 26-40): `asyncio`, `logging`, `uuid`, `dataclass`,
`TYPE_CHECKING, Any, Awaitable, Callable, Optional, Type, TypeVar`, `LifecycleEvent`, and
`EventBus` under `TYPE_CHECKING` only. Module logger: `logger = logging.getLogger("navigator_eventbus.lifecycle.registry")` (:56).

### Existing Signatures to Use
```python
# src/navigator_eventbus/lifecycle/registry.py
AsyncSubscriber = Callable[[LifecycleEvent], Awaitable[None]]          # :46
E = TypeVar("E", bound=LifecycleEvent)                                 # :47

@dataclass
class _Subscription:                                                   # :63-83
    subscription_id: str
    event_type: Type[LifecycleEvent]
    callback: AsyncSubscriber
    where: Optional[Callable[[Any], bool]]
    forward_to_bus: bool                                               # :82
    order: int                                                         # :83

class EventRegistry:                                                   # :90
    def __init__(self, *, event_bus: "Optional[EventBus]" = None,
                 bus_channel_prefix: str = "lifecycle",                # :108
                 forward_to_global: bool = True) -> None:              # :104-116
        # body sets self._subscriptions, self._event_bus, self._bus_channel_prefix,
        #           self._forward_to_global, self._insertion_counter
    def subscribe(self, event_type: Type[E], callback: AsyncSubscriber, *,
                  where=None, forward_to_bus: bool = False) -> str:    # :121-157
        # builds _Subscription(..., forward_to_bus=forward_to_bus, order=...)  # :152
    def unsubscribe(self, subscription_id: str) -> bool:               # :159

# forward branch inside emit():                                        # :276-292
            if sub.forward_to_bus and self._event_bus is not None:
                channel = f"{self._bus_channel_prefix}.{cls_name}"
                try:
                    asyncio.create_task(
                        self._event_bus.emit(channel, event.to_dict()),  # :284
                        name=f"lifecycle.bus.{cls_name}",
                    )
                except RuntimeError: ...

# src/navigator_eventbus/evb.py:349
async def EventBus.emit(self, event_type: str, payload: dict, **kwargs) -> int
```

### Does NOT Exist
- ~~`EventRegistry.attach_bus`~~, ~~`EventRegistry.detach_bus`~~, ~~`EventRegistry.event_bus`~~ (property) — added by this task.
- ~~`BusSerializer`~~, ~~`_Subscription.bus_serializer`~~, ~~`EventRegistry._bus_serializer`~~ — added by this task.
- ~~`EventRegistry.set_event_bus`~~ — that name exists only on `hooks.manager.HookManager` (`hooks/manager.py:98`); do NOT add it to the registry.
- ~~`LifecycleEvent.to_payload`~~ / ~~`LifecycleEvent.serialize`~~ — the only serializer is `to_dict()`.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/lifecycle/registry.py", "action": "MODIFY"},
    {"path": "tests/lifecycle/test_registry_bus_hook.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/lifecycle/registry.py#EventRegistry",
    "sym:src/navigator_eventbus/lifecycle/registry.py#_Subscription",
    "sym:src/navigator_eventbus/lifecycle/base.py#LifecycleEvent"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- **Byte-for-byte regression**: with no serializer configured, the forwarded payload MUST still be exactly `event.to_dict()` — `tests/lifecycle/test_registry.py` and `test_dual_emit_integration.py` must pass unchanged.
- The serializer runs synchronously on the emit path, BEFORE `create_task` — keep the resolution cheap (no I/O).
- `attach_bus()` with the same bus object is a no-op apart from applying the optional prefix/serializer (idempotent).
- `detach_bus()` only clears `self._event_bus`; subscriptions keep `forward_to_bus=True` and resume on the next `attach_bus()`.
- Google-style docstrings + type hints on every new method.

---

## Implementation Blueprint

### Steps (in order)
1. Add the `BusSerializer` alias right after `E = TypeVar(...)` (:47) — *why*: `_Subscription` and the public signatures reference it.
2. Add the `bus_serializer` field after `order: int` (:83) — *why*: dataclass fields with defaults must come after fields without defaults; `forward_to_bus: bool` occurs twice in the file so it is not a safe anchor.
3. Extend `__init__` and `subscribe` with the `bus_serializer=` kwarg and pass it into `_Subscription` — *why*: spec §2 New Public Interfaces fixes these signatures.
4. Add `attach_bus` / `detach_bus` / `event_bus` after `subscribe` (before `unsubscribe`, :159) — *why*: they are public API and belong in the "Public API" block.
5. Add the private helper `_bus_payload` and rewrite the forward branch to use it — *why*: keeps the try/except for the serializer out of the hot loop's body.
6. Write the tests, then run the Validation Commands.

### `src/navigator_eventbus/lifecycle/registry.py` (MODIFY) — alias
```python
# occurrences: 1 (verified: grep -c 'E = TypeVar("E", bound=LifecycleEvent)' src/navigator_eventbus/lifecycle/registry.py)
# AFTER — insert below `E = TypeVar("E", bound=LifecycleEvent)` (verified: registry.py:47)

#: Turns a lifecycle event into the bus payload; returning ``None`` skips the publish.
BusSerializer = Callable[[LifecycleEvent], Optional[dict[str, Any]]]
```

### `src/navigator_eventbus/lifecycle/registry.py` (MODIFY) — `_Subscription`
```python
# occurrences: 1 (verified: grep -c '    order: int' src/navigator_eventbus/lifecycle/registry.py)
# AFTER — insert below `    order: int` (verified: registry.py:83)
    bus_serializer: "Optional[BusSerializer]" = None
```
Also add a `bus_serializer:` entry to the class docstring's Attributes list.

### `src/navigator_eventbus/lifecycle/registry.py` (MODIFY) — `__init__`
```python
# occurrences: 1 (verified: grep -c '        forward_to_global: bool = True,' registry.py) → line 109
# AFTER — insert below `        forward_to_global: bool = True,` (verified: registry.py:109)
        bus_serializer: "Optional[BusSerializer]" = None,
# and in the body, AFTER `self._forward_to_global = forward_to_global`:
        self._bus_serializer = bus_serializer
```
Document `bus_serializer` in the class docstring Args.

### `src/navigator_eventbus/lifecycle/registry.py` (MODIFY) — `subscribe`
```python
# occurrences: 1 (verified: grep -c '        forward_to_bus: bool = False,' registry.py) → line 127
# AFTER — insert below `        forward_to_bus: bool = False,` (verified: registry.py:127)
        bus_serializer: "Optional[BusSerializer]" = None,
# occurrences: 1 (verified: grep -c '                forward_to_bus=forward_to_bus,' registry.py) → line 152
# AFTER — insert below `                forward_to_bus=forward_to_bus,` (verified: registry.py:152)
                bus_serializer=bus_serializer,
```
NOTE: insert `bus_serializer=` before `order=` is fine because it is a keyword argument.

### `src/navigator_eventbus/lifecycle/registry.py` (MODIFY) — new public methods
```python
# BEFORE — insert above `    def unsubscribe(self, subscription_id: str) -> bool:` (verified: registry.py:159, occurrences: 1)
    def attach_bus(
        self,
        bus: "EventBus",
        *,
        channel_prefix: Optional[str] = None,
        serializer: "Optional[BusSerializer]" = None,
    ) -> None:
        """Attach (or replace) the EventBus used by ``forward_to_bus`` subscriptions.

        Idempotent for the same bus. Works on an already-built registry,
        including the process-wide global one.

        Args:
            bus: The ``EventBus`` to forward events to.
            channel_prefix: Optional new channel prefix; ``None`` keeps the current one.
            serializer: Optional new registry-level serializer; ``None`` keeps the current one.
        """
        self._event_bus = bus
        if channel_prefix is not None:
            self._bus_channel_prefix = channel_prefix
        if serializer is not None:
            self._bus_serializer = serializer

    def detach_bus(self) -> None:
        """Stop forwarding; subscriptions keep their flag and resume on the next ``attach_bus()``."""
        self._event_bus = None

    @property
    def event_bus(self) -> "Optional[EventBus]":
        """The currently attached ``EventBus`` (``None`` when detached)."""
        return self._event_bus

    def _bus_payload(
        self, sub: _Subscription, event: LifecycleEvent
    ) -> Optional[dict[str, Any]]:
        """Resolve the bus payload for *event* (subscription → registry → ``to_dict``).

        Returns:
            The payload, or ``None`` when the serializer skips or raises.
        """
        serializer = sub.bus_serializer or self._bus_serializer
        if serializer is None:
            return event.to_dict()
        try:
            return serializer(event)
        except Exception:  # noqa: BLE001 — never break the emit path
            logger.exception(
                "Bus serializer raised for %s; forward skipped",
                type(event).__name__,
            )
            return None
```
**Why**: spec §2 fixes `attach_bus`/`detach_bus`/`event_bus`; the helper keeps the forward loop readable and guarantees the "logged, never raised" rule (spec §3 M1 table).

### `src/navigator_eventbus/lifecycle/registry.py` (MODIFY) — forward branch
```python
# FILL IN: disambiguate — the anchor `self._event_bus.emit(channel, event.to_dict()),` (count 1, :284)
# lives inside this block (verified: registry.py:280-292). Replace the block with:
            if sub.forward_to_bus and self._event_bus is not None:
                payload = self._bus_payload(sub, event)
                if payload is None:
                    continue  # FILL IN: confirm nothing else in the loop body follows this block (it is the last statement, verified :280-292)
                channel = f"{self._bus_channel_prefix}.{cls_name}"
                try:
                    asyncio.create_task(
                        self._event_bus.emit(channel, payload),
                        name=f"lifecycle.bus.{cls_name}",
                    )
                except RuntimeError:
                    logger.exception(
                        "Dual-emit scheduling failed for channel %s", channel
                    )
```
**Why**: `None` → skip is the spec's redaction contract; keeping the existing `create_task` + `RuntimeError` handling and comment block unchanged preserves FEAT-177 fire-and-forget semantics.

### `tests/lifecycle/test_registry_bus_hook.py` (CREATE)
See Test Specification below — copy it and fill in the bodies.

### FILL IN checklist
- [ ] forward branch: `continue` vs. `if payload is not None:` wrapping — either is fine as long as no statement after the block in the `for sub` loop is skipped.
- [ ] test bodies for all 7 tests below — bounded by AC-1/AC-2 of the spec.

---

## Acceptance Criteria

- [ ] `attach_bus()` works on an already-built registry, including the global one (`scope()` registry); `detach_bus()` stops forwarding (spec AC-1).
- [ ] A serializer can redact or skip (returns `None`) the payload; a per-subscription serializer wins over the registry one (spec AC-2).
- [ ] A raising serializer is logged and does not raise out of `emit()`.
- [ ] With no serializer, the forwarded payload equals `event.to_dict()` (regression).
- [ ] Existing lifecycle tests pass unchanged.
- [ ] `ruff check src/navigator_eventbus/lifecycle/registry.py` is clean.

---

## Validation Commands

- `pytest tests/lifecycle/test_registry_bus_hook.py -q`
- `pytest tests/lifecycle/test_registry.py -q`
- `pytest tests/lifecycle/test_registry_fire_and_forget.py -q`
- `pytest tests/lifecycle/test_dual_emit_integration.py -q`
- `pytest tests/lifecycle/test_emit_overhead.py -q`

---

## Test Specification

```python
# tests/lifecycle/test_registry_bus_hook.py
import asyncio
from dataclasses import dataclass

import pytest

from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.registry import EventRegistry
from navigator_eventbus.lifecycle.trace import TraceContext


@dataclass
class _Evt(LifecycleEvent):
    secret: str = "s3cr3t"


class _SpyBus:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def emit(self, channel: str, payload: dict) -> int:
        self.calls.append((channel, payload))
        return 1


def _evt() -> _Evt:
    # FILL IN: build like tests/lifecycle/test_emit_overhead.py:_BenchEvent (trace_context=TraceContext.new_root(), source_type=..., source_name=...)
    ...


async def _flush() -> None:
    await asyncio.sleep(0)  # let create_task'd emits run


async def test_attach_bus_after_construction_forwards(): ...        # also via fresh_global_registry fixture
async def test_detach_bus_stops_forwarding(): ...
async def test_serializer_redacts_and_none_skips(): ...
async def test_subscription_serializer_overrides_registry(): ...
async def test_serializer_exception_logged_not_raised(caplog): ...
async def test_default_payload_unchanged_without_serializer(): ...  # payload == evt.to_dict()
async def test_attach_bus_idempotent_and_prefix_override(): ...
```

---

## Agent Instructions

1. **Work in the feature worktree** — never on `dev`/`main`
   (`python -m scripts.sdd.ensure_worktree --slug durable-lifecycle-fanout --feature-id FEAT-711`)
2. **Read the spec** at the path listed above for full context
3. **Check dependencies** — none for this task
4. **Verify the Codebase Contract** before writing any code; update it first if anything moved
5. **Update status** in `sdd/tasks/index/durable-lifecycle-fanout.json` → `"in-progress"` (set `started_at`) and commit only that index file
6. **Implement** from the Implementation Blueprint; complete every `# FILL IN:` marker
7. **Verify** all acceptance criteria — run the Validation Commands
8. **Commit the code** — stage only the files listed above
9. **Close the task** with `scripts/sdd/close_task.sh TASK-4554 durable-lifecycle-fanout verified`
10. **Fill in the Completion Note** below, then commit the staged SDD state

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**: <session or agent ID>
**Date**: YYYY-MM-DD
**Notes**:

**Deviations from spec**: none | describe if any
