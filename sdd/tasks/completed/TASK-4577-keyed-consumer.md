# TASK-4577: KeyedConsumer loop

**Feature**: FEAT-713 — KeyedQueue (key-ordered, head-blocking work queue)
**Spec**: `sdd/specs/keyed-queue.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-4576
**Assigned-to**: unassigned

---

## Context

Spec §2 component diagram ("KeyedConsumer × N") and §3 Module 3. The loop every worker runs so
callers never re-implement it: pause-aware sleep, throttled `recover`, `promote_due`, lease,
handler under `asyncio.wait_for`, automatic lock + signal extension, fenced `apply`.

---

## Scope

- Create `src/navigator_eventbus/queues/keyed/consumer.py` with `Handler`, `KeyedConsumer`; re-export `backoff_ms` from `store.py` (spec M3 lists it here).
- `KeyedConsumer(queue: KeyedQueue, *, wait_ms: int = 1_000)`.
- `run_once(handler) -> Outcome | None`: paused → sleep `min(pause PTTL, pause_poll_ms)` and return `None` (no receive, rule 4); else `recover()` (throttled inside), `promote_due()`, `lease(wait_ms=...)`; `None` → return `None`; run the handler under `asyncio.wait_for(call_timeout_ms)` while a side task calls `lease.extend()` every `lock_ttl_ms / 3`; handler exception or timeout → `Outcome.retry(reason=<exception text>)` (spec: "exceptions become retry outcomes"); `apply`; `LeaseLost` → log WARNING and return `None`.
- `run(handler, *, stop: asyncio.Event)`: loop `run_once` until `stop` is set; an unexpected error in one iteration is logged and the loop continues after `pause_poll_ms`.
- README-level docstring warning (spec §7): handlers with non-idempotent side effects must catch `CancelledError`/`TimeoutError` themselves and return their own outcome.
- Tests in `tests/queues/keyed/test_consumer.py`.

**NOT in scope**: `drain` (TASK-4578), exports (TASK-4579).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/queues/keyed/consumer.py` | CREATE | loop |
| `tests/queues/keyed/test_consumer.py` | CREATE | tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
import asyncio
from navconfig.logging import logging
from navigator_eventbus.queues.keyed.config import KeyedMessage, LeaseLost, Outcome   # TASK-4573
from navigator_eventbus.queues.keyed.store import KeyedLease, KeyedQueue, backoff_ms  # TASK-4575/4576
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/queues/keyed/store.py (TASK-4575/4576)
class KeyedQueue:
    config: KeyedQueueConfig     # lock_ttl_ms, call_timeout_ms, pause_poll_ms
    async def is_paused(self) -> Optional[str]
    async def recover(self, *, force: bool = False) -> int
    async def promote_due(self, *, limit: Optional[int] = None) -> int
    async def lease(self, *, wait_ms: int = 0) -> Optional[KeyedLease]
    async def apply(self, lease: KeyedLease, outcome: Outcome) -> None   # raises LeaseLost
class KeyedLease:
    async def extend(self, additional_ms: Optional[int] = None) -> None  # raises LeaseLost
# the fake: FakeStreamsRedis.pttl(name) (TASK-4574) — used to size the pause sleep
```

### Does NOT Exist
- ~~`KeyedQueue.run`~~ — the loop is this class, not the queue.
- ~~automatic retry of a handler inside the consumer~~ — one handler call per lease.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/queues/keyed/consumer.py", "action": "CREATE"},
    {"path": "tests/queues/keyed/test_consumer.py", "action": "CREATE"}
  ],
  "contract_symbols": []
}
```

---

## Implementation Notes

- The extension side task must be cancelled in a `finally`, before `apply`.
- Use `asyncio.sleep` with the injected clock only for durations the test fakes; tests should call `run_once` directly rather than `run` wherever possible.
- `call_timeout_ms` is the library bound; callers like FEAT-830 set a shorter bound of their own.

---

## Implementation Blueprint

### Steps (in order)
1. Write `consumer.py` from the block — *why*: the loop order in spec §2 is load-bearing (pause before receive).
2. Fill `run_once`, then `run`.
3. Write tests.

```python
# src/navigator_eventbus/queues/keyed/consumer.py  (CREATE)
"""KeyedConsumer (FEAT-713): the worker loop over a KeyedQueue."""
import asyncio
from collections.abc import Awaitable, Callable
from typing import Optional

from navconfig.logging import logging

from navigator_eventbus.queues.keyed.config import KeyedMessage, LeaseLost, Outcome
from navigator_eventbus.queues.keyed.store import KeyedLease, KeyedQueue, backoff_ms

__all__ = ("Handler", "KeyedConsumer", "backoff_ms")

Handler = Callable[[KeyedMessage], Awaitable[Outcome]]


class KeyedConsumer:
    """Run `handler` over leased heads until `stop` is set; exceptions become retry outcomes.

    Handlers whose side effect is not idempotent must catch ``asyncio.CancelledError`` and
    ``TimeoutError`` themselves and return their own outcome: the generic retry is only safe
    when the side effect provably did not start.
    """

    def __init__(self, queue: KeyedQueue, *, wait_ms: int = 1_000) -> None:
        self.queue = queue
        self.wait_ms = wait_ms
        self.logger = logging.getLogger("navigator_eventbus.queues.keyed")

    async def run_once(self, handler: Handler) -> Optional[Outcome]:
        """paused? → sleep · recover · promote_due → lease → handler (wait_for, extension) → apply."""
        # FILL IN — bounded by spec §2 diagram and signal rule 4

    async def _keep_extended(self, lease: KeyedLease) -> None:
        """Extend lock + signal every lock_ttl_ms / 3 until cancelled."""
        # FILL IN

    async def run(self, handler: Handler, *, stop: asyncio.Event) -> None:
        # FILL IN
```

### FILL IN checklist
- [ ] `test_backoff_capped_exponential` (30, 60, 120 … 900).
- [ ] `test_consumer_run_once_applies_outcome`; handler exception → retry with the error text.
- [ ] `test_consumer_extends_lock_for_long_handler`.
- [ ] `test_pause_never_receives_signals` (paused longer than `max_receives` × visibility → no `receive` call recorded, no signal parked; after resume every key leased).
- [ ] `run` stops when `stop` is set; survives one iteration raising.

---

## Acceptance Criteria

- [ ] A pause never consumes signal deliveries.
- [ ] Long handlers keep their lock and signal.

## Validation Commands
- `pytest tests/queues/keyed/test_consumer.py -q`

---

## Agent Instructions

Implement exactly the files above.

## Completion Note

consumer.py + test_consumer.py (9 tests). A non-`Outcome` handler return becomes a retry (TypeError text). Pause test uses `pause_poll_ms=1` so the loop doesn't really sleep 1s x 50. `tests/queues`: passes.
