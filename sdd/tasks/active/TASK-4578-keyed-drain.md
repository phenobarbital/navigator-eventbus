# TASK-4578: drain — direct administrative flush

**Feature**: FEAT-713 — KeyedQueue (key-ordered, head-blocking work queue)
**Spec**: `sdd/specs/keyed-queue.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-4576, TASK-4577
**Assigned-to**: unassigned

---

## Context

Spec §2 "drain(handler, key_prefix)" and §3 Module 4; lead decision (FEAT-830 proposal §0b-3):
flush is a direct drain, not a re-enqueue — concurrent across keys under a semaphore, sequential
inside a key, bounded per call, fail-fast, using the **same per-key lock** as the consumers so it
can never double-deliver. v0.2 added `key_prefix` so a caller can drain one tenant's keys.

---

## Scope

- Create `src/navigator_eventbus/queues/keyed/drain.py` with `drain` and `drain_key`.
- `drain(queue, handler, *, key_prefix=None, concurrency=10, call_timeout_ms=None, stop_after_failures=5, include_held=False, ignore_pause=False) -> DrainReport`:
  paused and not `ignore_pause` → return an empty report with `stopped_early=True`;
  candidate keys = `keys()` ∪ members of `delayed`, filtered by `key_prefix`; held keys skipped unless `include_held` (then `release_hold` first);
  `asyncio.gather` of `drain_key` under `Semaphore(concurrency)`; stop launching new keys once `stop_after_failures` consecutive `retry` outcomes were seen (shared counter, reset by any `ack`).
- `drain_key(...)`: loop: `queue.lease_key(key)` (no receipt); `None` because the lock is held → `busy += 1`, stop this key; `None` because empty/waiting → stop; run the handler under `asyncio.wait_for`; handler exception/timeout → `Outcome.retry(reason=...)`; `apply`; count by kind; continue only on `ack`. **Retry delays are ignored** (drain leases even when `due_ms > now` — pass a flag to `lease_key` or clear `due_ms`, see notes); `wait` deadlines are respected.
- Add `KeyedQueue.drain(...)` in `store.py` as a thin delegate with a function-scope import of `drain.drain` (avoids an import cycle).
- Tests in `tests/queues/keyed/test_drain.py`.

**NOT in scope**: HTTP surface (spec Q1 resolved: none); exports (TASK-4579).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/queues/keyed/drain.py` | CREATE | `drain`, `drain_key` |
| `src/navigator_eventbus/queues/keyed/store.py` | MODIFY | `KeyedQueue.drain` delegate; `lease_key(..., ignore_due=False)` |
| `tests/queues/keyed/test_drain.py` | CREATE | tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
import asyncio
import time
from navigator_eventbus.queues.keyed.config import DrainReport, LeaseLost, Outcome          # TASK-4573
from navigator_eventbus.queues.keyed.consumer import Handler                               # TASK-4577 (dependency)
from navigator_eventbus.queues.keyed.store import KeyedQueue                               # TASK-4575
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/queues/keyed/store.py (TASK-4575/4576)
async def lease_key(self, key: str, *, receipt: Optional[Receipt] = None) -> Optional[KeyedLease]
async def apply(self, lease: KeyedLease, outcome: Outcome) -> None
async def keys(self) -> list[str]
async def release_hold(self, key: str) -> bool
async def is_paused(self) -> Optional[str]
_k.delayed  # zset of keys
```

### Does NOT Exist
- ~~a re-enqueue path~~ — drain never pushes.
- ~~a way for `lease_key` to report why it returned `None`~~ — add one in this task (see notes) rather than guessing from side effects.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/queues/keyed/drain.py", "action": "CREATE"},
    {"path": "src/navigator_eventbus/queues/keyed/store.py", "action": "MODIFY"},
    {"path": "tests/queues/keyed/test_drain.py", "action": "CREATE"}
  ],
  "contract_symbols": []
}
```

---

## Implementation Notes

- `drain_key` needs to know *why* `lease_key` returned `None` (busy vs empty vs waiting). Add an
  internal `_lease_key_detailed(key, *, receipt=None, ignore_due=False) -> tuple[Optional[KeyedLease], str]`
  returning a reason (`"busy"`, `"empty"`, `"waiting"`, `"held"`, `"expired"`), and make `lease_key`
  a wrapper — do not change `lease_key`'s public signature or behaviour.
- The lock is taken per head (lease → apply releases). Spec M4 says "lock the key once"; per-head
  locking keeps the same guarantee (never two deliveries of one key at once, order preserved) — a
  consumer that grabs the key between two heads simply makes the drain count it `busy` and stop.
  Record this in the Completion Note.
- `elapsed_ms` from `time.monotonic()`.

---

## Implementation Blueprint

### Steps (in order)
1. Add `_lease_key_detailed` + `ignore_due` in `store.py`, keep `lease_key` as a wrapper — *why*: drain must distinguish busy from empty.
2. Write `drain.py`.
3. Add the `KeyedQueue.drain` delegate.
4. Tests.

```python
# src/navigator_eventbus/queues/keyed/drain.py  (CREATE)
"""Direct administrative drain for keyed queues (FEAT-713 M4)."""
import asyncio
import time
from typing import Optional

from navigator_eventbus.queues.keyed.config import DrainReport, LeaseLost, Outcome
from navigator_eventbus.queues.keyed.consumer import Handler
from navigator_eventbus.queues.keyed.store import KeyedQueue

__all__ = ("drain", "drain_key")


async def drain(queue: KeyedQueue, handler: Handler, *, key_prefix: Optional[str] = None,
                concurrency: int = 10, call_timeout_ms: Optional[int] = None,
                stop_after_failures: int = 5, include_held: bool = False,
                ignore_pause: bool = False) -> DrainReport:
    """gather(drain_key(k)) over (keys ∪ delayed) matching key_prefix under a Semaphore."""
    # FILL IN — bounded by spec §3 M4 docstring and AC "drain never delivers a key that a live consumer holds"


async def drain_key(queue: KeyedQueue, key: str, handler: Handler, sem: asyncio.Semaphore,
                    *, call_timeout_ms: int, counters: DrainReport) -> None:
    """Lease heads of one key in order until a non-ack outcome, busy, or empty."""
    # FILL IN
```

```python
# src/navigator_eventbus/queues/keyed/store.py  (MODIFY)
# AFTER — insert below the `describe` method added by TASK-4576
# occurrences: 1 (verify: grep -c 'async def describe' src/navigator_eventbus/queues/keyed/store.py)
    async def drain(self, handler, *, key_prefix: Optional[str] = None, concurrency: int = 10,
                    call_timeout_ms: Optional[int] = None, stop_after_failures: int = 5,
                    include_held: bool = False, ignore_pause: bool = False):
        """Direct administrative flush; see drain.drain."""
        from navigator_eventbus.queues.keyed.drain import drain as _drain
        return await _drain(self, handler, key_prefix=key_prefix, concurrency=concurrency,
                            call_timeout_ms=call_timeout_ms, stop_after_failures=stop_after_failures,
                            include_held=include_held, ignore_pause=ignore_pause)
```

### FILL IN checklist
- [ ] `test_drain_concurrent_across_keys_sequential_within` (3 keys × 3, concurrency 2).
- [ ] `test_drain_skips_locked_key_as_busy`.
- [ ] `test_drain_ignores_retry_delay`.
- [ ] `test_drain_fail_fast` (5 consecutive retries → `stopped_early`, remaining keys untouched).
- [ ] `test_drain_respects_wait_and_hold`.
- [ ] `test_drain_key_prefix_scopes_keys`.
- [ ] paused without `ignore_pause` → no handler call.

---

## Acceptance Criteria

- [ ] `drain` never delivers a key a live consumer holds and never out of order inside a key.
- [ ] `drain(key_prefix=…)` never touches keys outside the prefix.

## Validation Commands
- `pytest tests/queues/keyed/test_drain.py -q`

---

## Agent Instructions

`lease_key`'s public behaviour must stay identical: also run `pytest tests/queues/keyed/test_store.py -q` (TASK-4575's tests) before committing.

## Completion Note

*(filled by the implementing agent)*
