# TASK-1864: Export BusCoreProducer from the package root

**Feature**: FEAT-433 — BusCore Producer Facade
**Spec**: `sdd/specs/buscore-producer-facade.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: S (< 2h)
**Depends-on**: TASK-1863
**Assigned-to**: unassigned

---

## Context

Implements spec **Module 2**. `BusCoreProducer` is useless to a consuming app
until it is importable from the package root — FieldSync's whole reason for
this feature is `from navigator_eventbus import BusCoreProducer` against a
pinned release.

The one real decision here is **eager vs. lazy**. This package has both
conventions and picking the wrong one is a silent correctness bug, so read the
contract below before touching `__init__.py`.

---

## Scope

- Add `BusCoreProducer` to the **eager** import block of
  `src/navigator_eventbus/__init__.py`.
- Add `"BusCoreProducer"` to `__all__`.
- Add an export test.

**NOT in scope**:
- Any change to `producers.py` itself → TASK-1863.
- Routing the export through the lazy `__getattr__` map — see contract.
- The version bump → TASK-1866.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/__init__.py` | MODIFY | Eager import + `__all__` entry |
| `tests/test_producers.py` | MODIFY | Append `test_producer_exported_from_root` |

---

## Codebase Contract (Anti-Hallucination)

> Verified by reading source on 2026-09-12 at commit `2ba82b1`.

### Verified Imports

```python
from navigator_eventbus.producers import BusCoreProducer  # created by TASK-1863
```

### Existing Structure to Modify

`src/navigator_eventbus/__init__.py` has THREE distinct regions. Put the new
import in the first one.

```python
# REGION 1 — eager imports, lines 16-44. Alphabetical by module path.
from navigator_eventbus import lifecycle                                  # line 16
from navigator_eventbus.backends.composite import CompositeBackend        # line 17
from navigator_eventbus.core import BackpressureError, BusClosedError, BusCore   # line 18
from navigator_eventbus.dlq import DLQHandler                             # line 19
from navigator_eventbus.envelope import (...)                             # line 20
from navigator_eventbus.evb import Event, EventBus, EventPriority, EventSubscription   # line 26
from navigator_eventbus.ingress_models import IngressEnvelope             # line 27
from navigator_eventbus.version import (...)                              # line 28
from navigator_eventbus.webhook_signatures import (...)                   # line 37
#   ↑ `producers` sorts between `ingress_models` and `version` — insert there.

# REGION 2 — __all__, lines 46-82. Metadata dunders first, then names.
__all__ = [ "__author__", ..., "BackpressureError", "BusClosedError", "BusCore",
            "CompositeBackend", ... ]

# REGION 3 — LAZY exports, lines 88-108. DO NOT TOUCH.
_QUEUE_EXPORTS = {
    "QueueAPI": "navigator_eventbus.queues.api",
    ...
}
def __getattr__(name: str):
    module_path = _QUEUE_EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib
    return getattr(importlib.import_module(module_path), name)
```

### Why eager, not lazy

`_QUEUE_EXPORTS` exists for ONE reason, stated at `__init__.py:84-87`: the
queue machinery is heavy, and callers who never touch a queue should not pay
for it at import time. `producers.py` imports only `asyncio`, `logging`,
`datetime`, plus `core` / `envelope` / `evb` — all three of which
`__init__.py` **already imports eagerly** at lines 18, 20 and 26.

Deferring it would therefore buy nothing and cost correctness: a name in
`__all__` that is resolved only by `__getattr__` is invisible to `dir()`,
to static analysers, and to `from navigator_eventbus import *`.

**Add it to the eager block. Do not add it to `_QUEUE_EXPORTS`.**

### Does NOT Exist

- ~~`navigator_eventbus.BusCoreProducer`~~ — not exported yet; that is this task.
- ~~a `_PRODUCER_EXPORTS` map~~ — do not create one.
- ~~`navigator_eventbus.producers.__all__`~~ — the submodule has no `__all__`
  and does not need one.
- ~~`tests/test_exports.py`~~ — no such file. Export assertions live in
  `tests/test_package.py` (existing) and `tests/test_producers.py` (TASK-1863).

---

## Implementation Notes

### Pattern to Follow

```python
# src/navigator_eventbus/__init__.py — insert after line 27 (ingress_models):
from navigator_eventbus.producers import BusCoreProducer

# and in __all__, alongside the other bus names:
    "BusCore",
    "BusCoreProducer",
    "CompositeBackend",
```

### Key Constraints

- Keep the eager import block's existing alphabetical-by-module-path ordering —
  `producers` sits between `ingress_models` (line 27) and `version` (line 28).
- `__all__` groups metadata dunders first, then class names; insert
  `"BusCoreProducer"` next to `"BusCore"`.
- Do not reorder or reformat anything else in the file; the diff should be two
  added lines.

### References in Codebase

- `tests/test_package.py` — the existing export-assertion patterns, including
  `test_all_exports_superset_of_legacy_parrot_core_events`, which uses
  `issubset` against `__all__`.
- `tests/test_package.py:test_package_imports` docstring explains that the
  module-level `__getattr__` is exactly the kind of wiring that breaks
  attribute resolution silently — which is why this task ships a test.

---

## Acceptance Criteria

- [ ] `from navigator_eventbus import BusCoreProducer` works.
- [ ] `"BusCoreProducer" in navigator_eventbus.__all__`.
- [ ] The import is **eager** — present in the import block at the top of
      `__init__.py`, NOT in `_QUEUE_EXPORTS`.
- [ ] `"BusCoreProducer" in dir(navigator_eventbus)` (proves it is a real
      attribute, not `__getattr__`-resolved).
- [ ] `navigator_eventbus.BusCoreProducer is navigator_eventbus.producers.BusCoreProducer`.
- [ ] Existing exports still resolve — `pytest tests/test_package.py -v` passes.
- [ ] No linting errors: `ruff check src/navigator_eventbus/__init__.py`
- [ ] Full suite still green: `pytest -q`

---

## Test Specification

Append to `tests/test_producers.py`:

```python
def test_producer_exported_from_root():
    """BusCoreProducer is an eager, first-class root export."""
    import navigator_eventbus
    from navigator_eventbus import BusCoreProducer
    from navigator_eventbus.producers import BusCoreProducer as Direct

    assert BusCoreProducer is Direct
    assert "BusCoreProducer" in navigator_eventbus.__all__
    # Eager, not __getattr__-resolved: a lazily-mapped name is absent from dir().
    assert "BusCoreProducer" in dir(navigator_eventbus)
    assert "BusCoreProducer" not in navigator_eventbus._QUEUE_EXPORTS
```

---

## Agent Instructions

1. **Check the dependency**: `TASK-1863` must be in `sdd/tasks/completed/`.
   `producers.py` must exist before this import can resolve.
2. **Verify the Codebase Contract** — re-read `__init__.py` and confirm the
   three regions are still where the contract says.
3. Make the two-line change, add the test.
4. Update `sdd/tasks/index/buscore-producer-facade.json` → `"in-progress"`.
5. Verify every acceptance criterion.
6. Move this file to `sdd/tasks/completed/`, update the index → `"done"`.
7. Fill in the Completion Note.

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**:
**Date**:
**Notes**:

**Deviations from spec**: none | describe if any
