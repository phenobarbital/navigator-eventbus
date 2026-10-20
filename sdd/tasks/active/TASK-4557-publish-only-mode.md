# TASK-4557: Publish-only mode for BusCore

**Feature**: FEAT-711 — Durable lifecycle fan-out
**Spec**: `sdd/specs/durable-lifecycle-fanout.spec.md`
**Status**: pending
**Priority**: medium
**Estimated effort**: S (< 2h)
**Depends-on**: TASK-4556
**Assigned-to**: unassigned

---

## Context

Spec §1 problem 4 / Goal G4 / Module 4. `BusCore.start()` always calls
`backend.start_consumer(...)` (`core.py:213-214`). With `RedisStreamsBackend` in `group` delivery,
every producer process therefore joins the consumer group and ACKs entries meant for the real
consumer (the NVP-505 sink). A `publish_only=True` bus must never start the backend consumer.

---

## Scope

- Add `publish_only: bool = False` to `BusCore.__init__` (after `relay_max_backoff=` added by TASK-4556), store it, document it.
- In `start()`, skip `backend.start_consumer(...)` when `publish_only` is set. Publishing, the outbox relay and local dispatch are unaffected.
- Unit tests in a NEW file `tests/test_core_publish_only.py`.

**NOT in scope**: `EventBus(publish_only=...)` forwarding and the `BUS_PUBLISH_ONLY` config key (TASK-4558); the real-Redis `XINFO CONSUMERS` check (TASK-4559).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/core.py` | MODIFY | `publish_only` kwarg + `start()` guard |
| `tests/test_core_publish_only.py` | CREATE | unit tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.core import BusCore                     # verified: core.py:92
from navigator_eventbus.envelope import EventEnvelope           # verified: envelope.py:106
from navigator_eventbus.outbox import SqliteOutbox              # created by TASK-4555
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/core.py (state AFTER TASK-4556 — re-verify line numbers, they moved)
class BusCore:
    def __init__(self, *, ..., backend: Optional[TransportBackend] = None,
                 outbox: Optional[OutboxStore] = None, relay_interval: float = 5.0,
                 relay_batch: int = 500, relay_max_backoff: float = 300.0) -> None
    async def start(self) -> None:
        ...
        if self._backend is not None:                                        # was core.py:213
            await self._backend.start_consumer(self._on_transport_envelope)  # was core.py:214 (count 1)
        if self._backend is not None and self._outbox is not None:           # added by TASK-4556
            self._relay_task = asyncio.create_task(self._relay_loop(), ...)
```

### Does NOT Exist
- ~~`BusCore(publish_only=...)`~~ — added by this task.
- ~~`TransportBackend.start_producer`~~ / ~~`TransportBackend.publish_only`~~ — the backend protocol is unchanged (publish, start_consumer, close); the switch lives in `BusCore` only.
- ~~`RedisStreamsBackend(publish_only=...)`~~ — do not touch the backend.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/core.py", "action": "MODIFY"},
    {"path": "tests/test_core_publish_only.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/core.py#BusCore"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- Only the consumer start is skipped. The relay task (TASK-4556) must still start in publish-only mode — it is how a producer recovers after a Redis outage.
- Log at DEBUG in `start()` that the consumer was skipped (publish-only).

---

## Implementation Blueprint

### Steps (in order)
1. Re-verify the anchors (TASK-4556 shifted line numbers) — *why*: the blueprint quotes the pre-4556 lines.
2. Add the kwarg and `self._publish_only` — *why*: spec §2 `BusCore(publish_only=...)`.
3. Guard `start_consumer` — *why*: spec M4.
4. Write the tests.

### `src/navigator_eventbus/core.py` (MODIFY) — `__init__`
```python
# occurrences: 1 (verify: grep -c '        relay_max_backoff: float = 300.0,' src/navigator_eventbus/core.py)
# AFTER — insert below `        relay_max_backoff: float = 300.0,` (added by TASK-4556)
        publish_only: bool = False,
# body, AFTER `self._relay_max_backoff = relay_max_backoff`:
        self._publish_only = publish_only
```
Docstring Args: `publish_only: When True, start() never starts the backend consumer — this process only publishes (it never joins a consumer group).`

### `src/navigator_eventbus/core.py` (MODIFY) — `start()`
```python
# occurrences: 1 (verify: grep -c '            await self._backend.start_consumer(self._on_transport_envelope)' src/navigator_eventbus/core.py)
# FILL IN: disambiguate — `if self._backend is not None:` occurs several times in core.py; quote the 2 lines
#          (`if self._backend is not None:` + the start_consumer line) inside start() and replace them with:
        if self._backend is not None and not self._publish_only:
            await self._backend.start_consumer(self._on_transport_envelope)
        elif self._backend is not None:
            self.logger.debug("BusCore publish-only: backend consumer not started")
```
**Why**: the narrowest change that keeps relay start and worker start untouched.

### FILL IN checklist
- [ ] anchor disambiguation in `start()`.
- [ ] test bodies below.

---

## Acceptance Criteria

- [ ] `BusCore(backend=spy, publish_only=True).start()` never calls `spy.start_consumer` (spec AC-6, unit level).
- [ ] Publishing still reaches `spy.publish` and local subscribers.
- [ ] With an outbox, the relay still runs in publish-only mode.
- [ ] Default (`publish_only=False`) behavior unchanged.

---

## Validation Commands

- `pytest tests/test_core_publish_only.py -q`
- `pytest tests/test_core.py -q`

---

## Test Specification

```python
# tests/test_core_publish_only.py
from navigator_eventbus import EventEnvelope, SqliteOutbox
from navigator_eventbus.core import BusCore


class _SpyBackend:
    def __init__(self) -> None:
        self.consumer_started = False
        self.published: list[str] = []

    async def publish(self, envelope): self.published.append(envelope.event_id)
    async def start_consumer(self, on_envelope): self.consumer_started = True
    async def close(self): pass


async def test_publish_only_never_starts_consumer(): ...
async def test_publish_only_still_publishes_and_dispatches_locally(): ...
async def test_publish_only_relay_still_runs(tmp_path): ...
async def test_default_starts_consumer(): ...
```

---

## Agent Instructions

1. **Work in the feature worktree** (`python -m scripts.sdd.ensure_worktree --slug durable-lifecycle-fanout --feature-id FEAT-711`)
2. **Read the spec** for full context
3. **Check dependencies** — TASK-4556 must be `"done"`
4. **Verify the Codebase Contract** (line numbers moved after TASK-4556)
5. **Update status** → `"in-progress"` and commit only the index file
6. **Implement** from the blueprint; complete every `# FILL IN:` marker
7. **Verify** — run the Validation Commands
8. **Commit the code** — only the listed files
9. **Close the task** with `scripts/sdd/close_task.sh TASK-4557 durable-lifecycle-fanout verified`
10. **Fill in the Completion Note**, then commit the staged SDD state

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**: <session or agent ID>
**Date**: YYYY-MM-DD
**Notes**:

**Deviations from spec**: none | describe if any
