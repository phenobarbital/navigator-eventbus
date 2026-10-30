# TASK-4579: Exports, CHANGELOG, README and real-Redis integration tests

**Feature**: FEAT-713 — KeyedQueue (key-ordered, head-blocking work queue)
**Spec**: `sdd/specs/keyed-queue.spec.md`
**Status**: pending
**Priority**: medium
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-4577, TASK-4578
**Assigned-to**: unassigned

---

## Context

Spec §3 Module 6, §4 Integration Tests, §5 Acceptance Criteria. Makes the feature importable
from `navigator_eventbus.queues` (lazily — no Redis import at package import), documents it,
and proves the per-key guarantees against a real Redis 7.

---

## Scope

- `src/navigator_eventbus/queues/__init__.py`: add `KeyedQueue`, `KeyedConsumer`, `KeyedLease`, `drain` to `_LAZY_MAP` and `__all__`; add `KeyedQueueConfig`, `KeyedMessage`, `Outcome`, `DrainReport`, `LeaseLost` as eager imports from `queues.keyed.config` (pure Pydantic, no Redis).
- `src/navigator_eventbus/queues/keyed/__init__.py`: add the same lazy pattern for `KeyedQueue`, `KeyedConsumer`, `KeyedLease`, `drain`, `backoff_ms`.
- `CHANGELOG.md`: new `## [0.5.0] — Unreleased` section above the current `## [0.4.0] — Unreleased` heading with an "Added — FEAT-713 KeyedQueue" list; retitle that 0.4.0 heading `## [0.4.0] — 2026-10-21` (tag `279b0d8`). The release number is the lead's call (spec §8 Q4) — keep `0.5.0` as the placeholder.
- `README.md`: a "Keyed queues" subsection after `### Pull queues (SQS-style)` — what it guarantees, the four outcomes, the signal rules in two lines, `recover`, `drain(key_prefix=)`, and the idempotency warning (re-push after ack, handlers with non-idempotent side effects must catch cancellation).
- `tests/queues/keyed/test_exports.py`: `test_exports_lazy` (spec §4).
- `tests/queues/keyed/test_integration.py` (marker `integration`, skipped without Redis, same gate as `tests/queues/test_integration.py:34-46`): `test_keyed_queue_real_redis_ordering_and_lock` (two consumer tasks, 50 keys × 5 messages, per-key order and no concurrent lease per key) and `test_rebuild_after_flushdb` (use a dedicated Redis DB number, never DB 0 data you did not create — `FLUSHDB` only on that DB).

**NOT in scope**: any behaviour change in `store.py`, `consumer.py`, `drain.py`; tagging/publishing a release.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/queues/__init__.py` | MODIFY | exports |
| `src/navigator_eventbus/queues/keyed/__init__.py` | MODIFY | lazy exports |
| `CHANGELOG.md` | MODIFY | 0.5.0 section, retitle 0.4.0 |
| `README.md` | MODIFY | Keyed queues subsection |
| `tests/queues/keyed/test_exports.py` | CREATE | lazy export test |
| `tests/queues/keyed/test_integration.py` | CREATE | real-Redis tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
import importlib, os, sys
import pytest
from navigator_eventbus.queues.config import QueueConfig, QueueGroupConfig, QueueRegistry   # config.py:95,67,209
from navigator_eventbus.queues.receipts import ReceiptCodec                                # receipts.py:95
from navigator_eventbus.queues.store import QueueStore                                     # store.py:87
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/queues/__init__.py
_LAZY_MAP = {                                     # :49 (verify line at task time)
    "QueueAPI": "navigator_eventbus.queues.api", "QueueFeeder": ..., "QueueStore": ..., "StaleReceipt": ...,
}
__all__ = (...)                                    # tuple, sorted
def __getattr__(name: str) -> Any                  # lazy resolver
# tests/queues/test_integration.py:34-46 — REDIS_URL env (default redis://localhost:6379), ping or pytest.skip
# CHANGELOG.md:6 — "## [0.4.0] — Unreleased"
# README.md:104 — "### Pull queues (SQS-style)"
```

### Does NOT Exist
- ~~a published 0.5.0~~ — do not tag or publish.
- ~~`conftest.py` Redis fixtures~~ — copy the client fixture pattern into the new test file.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/queues/__init__.py", "action": "MODIFY"},
    {"path": "src/navigator_eventbus/queues/keyed/__init__.py", "action": "MODIFY"},
    {"path": "CHANGELOG.md", "action": "MODIFY"},
    {"path": "README.md", "action": "MODIFY"},
    {"path": "tests/queues/keyed/test_exports.py", "action": "CREATE"},
    {"path": "tests/queues/keyed/test_integration.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/queues/store.py#QueueStore",
    "sym:src/navigator_eventbus/queues/receipts.py#ReceiptCodec"
  ]
}
```

---

## Implementation Notes

- `test_exports_lazy`: in a subprocess (`sys.executable -c ...`) import `navigator_eventbus.queues` and assert `"navigator_eventbus.queues.keyed.store"` is not in `sys.modules`, then access `KeyedQueue` and assert it is — a subprocess avoids pollution from other tests.
- redis-py caches `Lock` scripts on the class; the fake's script delegates to a real client (TASK-4574), so fake and real tests can share one process.
- Integration signal queue: `default_visibility_timeout_ms` must exceed `lock_ttl_ms` (validator).

---

## Implementation Blueprint

### Steps (in order)
1. Exports in both `__init__.py` — *why*: callers (fieldsync FEAT-830) import from `navigator_eventbus.queues.keyed`.
2. CHANGELOG and README.
3. Export test, then integration tests; run them against a local Redis 7 if available and record ran/skipped.

```python
# src/navigator_eventbus/queues/__init__.py  (MODIFY)
# AFTER — extend the dict that starts at `_LAZY_MAP = {`
# occurrences: 1 (verify: grep -c '_LAZY_MAP = {' src/navigator_eventbus/queues/__init__.py)
    "KeyedConsumer": "navigator_eventbus.queues.keyed.consumer",
    "KeyedLease": "navigator_eventbus.queues.keyed.store",
    "KeyedQueue": "navigator_eventbus.queues.keyed.store",
    "drain": "navigator_eventbus.queues.keyed.drain",
# FILL IN: eager import of the five config names from navigator_eventbus.queues.keyed.config; extend __all__ (keep it sorted)
```

### FILL IN checklist
- [ ] Both `__init__.py` lazy maps; `__all__` sorted.
- [ ] CHANGELOG 0.5.0 section + 0.4.0 retitle.
- [ ] README subsection.
- [ ] `test_exports_lazy`.
- [ ] Two integration tests; record in the Completion Note whether they ran or skipped.

---

## Acceptance Criteria

- [ ] `from navigator_eventbus.queues import KeyedQueue` resolves lazily; no Redis import at package import.
- [ ] Integration tests pass against a real Redis 7 (or are skipped with the gate's reason when none is reachable — say which in the Completion Note).
- [ ] `pytest tests/queues -q` green (existing tests unchanged).

## Validation Commands
- `pytest tests/queues/keyed/test_exports.py -q`
- `pytest tests/queues/keyed/test_integration.py -q`
- `pytest tests/queues/test_exports_and_feeder.py -q`

---

## Agent Instructions

Never run `FLUSHDB` on a Redis database you did not select for the test.

## Completion Note

Exports (queues/__init__.py lazy map + eager config names, `__all__` sorted; keyed/__init__.py lazy), CHANGELOG 0.5.0 (placeholder number) + 0.4.0 retitled 2026-10-21, README subsection, test_exports.py, test_integration.py. Integration tests RAN (not skipped) against local Redis (redis-cli reports a v8 server, not 7) on dedicated DB 15 (env KEYED_TEST_REDIS_DB); the fixture refuses to run unless that DB is empty, and FLUSHDB runs only there. The fixture also resets redis-py's class-level Lock script cache so fake-based and real tests can run in any order. `tests/queues`: 261 passed.
