# TASK-4576: KeyedQueue operations — promote_due, recover, release_hold, peek, keys, describe

**Feature**: FEAT-713 — KeyedQueue (key-ordered, head-blocking work queue)
**Spec**: `sdd/specs/keyed-queue.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-4575
**Assigned-to**: unassigned

---

## Context

Spec §2 (signal rule 5, `recover` row of the key table, component diagram "promote_due" and
"recover") and §3 Module 2. These are the leaderless backstops: `promote_due` turns deadlines into
signals, `recover` re-signals keys that lost theirs. Same file as TASK-4575 (serialized).

---

## Scope

Add to `KeyedQueue` in `store.py`:
- `promote_due(*, limit=None) -> int` — `ZRANGEBYSCORE delayed -inf now` (limit `promote_batch`); for each key only the caller whose `ZREM delayed <k>` returns 1 sends a signal. Returns signals sent.
- `recover(*, force=False) -> int` — unless `force`, `SET recover <consumer> NX PX recover_interval_ms` must succeed (a rate limit, not a leader); then for every member of `keys`: not locked (`EXISTS lock:<k>` false), not in `held`, no `delayed` score → send one signal. Returns signals sent.
- `release_hold(key) -> bool` — `HDEL held <k>`; when it existed, set the head's `state=pending` and send a signal. Returns whether a hold was released.
- `peek(key, *, limit=10) -> list[KeyedMessage]` — heads in order, read-only.
- `keys() -> list[str]` — members of `keys`, sorted.
- `describe() -> dict` — `{"name", "keys", "messages" (ZCARD all), "delayed" (ZCARD delayed), "held" (HLEN held), "paused": reason|None, "oldest_age_ms": now - lowest score of all | None}`.
- Tests in `tests/queues/keyed/test_store_ops.py`.

**NOT in scope**: anything in TASK-4575's methods except calling them; the consumer loop (TASK-4577).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/queues/keyed/store.py` | MODIFY | add the six methods |
| `tests/queues/keyed/test_store_ops.py` | CREATE | tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
# all already imported by store.py (TASK-4575); add nothing new
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/queues/keyed/store.py (TASK-4575)
class _Keys: delayed, held, keys, all, paused, recover; key(k), msg(id), lock(k)
class KeyedQueue:
    config: KeyedQueueConfig; _redis; _store: QueueStore; _k: _Keys; _consumer: str
    def now_ms(self) -> int
    async def is_paused(self) -> Optional[str]
    # FILL IN at task time: the private helper TASK-4575 wrote to send a signal for a key (re-verify its name with grep before use)
# src/navigator_eventbus/queues/store.py:204
async def send(self, config: QueueConfig, envelope: EventEnvelope) -> str
```

### Does NOT Exist
- ~~a scheduler or leader election in navigator-eventbus queues~~ — `recover`'s `SET NX PX` is a throttle only; never treat the winner as an owner of anything beyond one pass.
- ~~`KeyedQueue.drain`~~ — TASK-4578.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/queues/keyed/store.py", "action": "MODIFY"},
    {"path": "tests/queues/keyed/test_store_ops.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/queues/store.py#QueueStore.send"
  ]
}
```

---

## Implementation Notes

- `promote_due` is safe from N consumers because `ZREM` is atomic per member (spec §7).
- `recover` must not signal a key whose head is waiting/due later: those keys have a `delayed` entry, which is exactly the skip condition.
- `describe()` is what an operator alarms on (`oldest_age_ms`, spec §7 Known Risks).

---

## Implementation Blueprint

### Steps (in order)
1. Re-read `store.py` from TASK-4575 and find its signal helper — *why*: reuse it, do not format a second envelope.
2. Add the six methods after `is_paused` — *why*: keeps the core block intact for review.
3. Write the tests.

```python
# src/navigator_eventbus/queues/keyed/store.py  (MODIFY)
# AFTER — insert below `async def is_paused(self) -> Optional[str]:` and its return line
# occurrences: 1 (verify: grep -c 'async def is_paused' src/navigator_eventbus/queues/keyed/store.py)
    async def promote_due(self, *, limit: Optional[int] = None) -> int:
        """Move due keys from `delayed` to the signal stream; only the ZREM winner signals."""
        # FILL IN

    async def recover(self, *, force: bool = False) -> int:
        """Throttled re-signal of keys that are not locked, not held and have no delayed entry."""
        # FILL IN — bounded by signal rule 5

    async def release_hold(self, key: str) -> bool:
        """Clear a hold; the head becomes pending and the key is signalled."""
        # FILL IN

    async def peek(self, key: str, *, limit: int = 10) -> list[KeyedMessage]:
        # FILL IN

    async def keys(self) -> list[str]:
        # FILL IN

    async def describe(self) -> dict[str, Any]:
        # FILL IN
```

### FILL IN checklist
- [ ] `test_promote_due_is_leaderless` — two promoters, one signal.
- [ ] `test_recover_resignals_orphan_key` — and two concurrent `recover()` run one pass.
- [ ] `test_promote_crash_between_zrem_and_send_is_recovered`.
- [ ] `test_hold_blocks_key_release_hold_resumes` (release half).
- [ ] `test_retry_keeps_head_and_delays_key` — `promote_due` half.
- [ ] `describe()` fields incl. `oldest_age_ms`; `peek` order; `keys` sorted.

---

## Acceptance Criteria

- [ ] `promote_due` and `recover` work from N identical consumers with no leader.
- [ ] A lost signal is recovered within `recover_interval_ms`.

## Validation Commands
- `pytest tests/queues/keyed/test_store_ops.py -q`

---

## Agent Instructions

Also run `pytest tests/queues/keyed/test_store.py -q` (TASK-4575's tests) before committing. Do not change TASK-4575's method bodies; if one is wrong, stop and note it in the Completion Note.

## Completion Note

*(filled by the implementing agent)*
