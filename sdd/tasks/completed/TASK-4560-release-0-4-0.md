# TASK-4560: Release 0.4.0 — version bump, CHANGELOG, README config + deploy note

**Feature**: FEAT-711 — Durable lifecycle fan-out
**Spec**: `sdd/specs/durable-lifecycle-fanout.spec.md`
**Status**: pending
**Priority**: medium
**Estimated effort**: S (< 2h)
**Depends-on**: TASK-4559
**Assigned-to**: unassigned

---

## Context

Spec header "Target version: navigator-eventbus 0.4.0" and §7 Known Risks (per-host disk,
multi-worker gunicorn, residual window, duplicates). ai-parrot FEAT-710 consumes this release. This
task bumps the version and documents the new knobs and the deployment rules operators must follow
for the outbox to actually survive restarts.

---

## Scope

- Bump `__version__` to `"0.4.0"` in `src/navigator_eventbus/version.py` (`pyproject.toml` reads it dynamically — do NOT edit `pyproject.toml`).
- Add a `## [0.4.0]` entry at the top of `CHANGELOG.md` (above `## [0.1.0]`) listing: registry `attach_bus`/`detach_bus`/serializer hook; `EventBus(backend=, transport=, publish_only=, outbox=)`; `OutboxStore`/`SqliteOutbox`/`OutboxFullError`; `BusCore(outbox=, publish_only=, relay_*)` + `BusCore.stats()`; delivery contract "at-least-once, consumers dedup by `event_id`".
- README "Configuration knobs" → BusCore/EventBus table: add rows for `BUS_TRANSPORT`, `BUS_PUBLISH_ONLY`, `BUS_OUTBOX_PATH`, `BUS_OUTBOX_MAX_ROWS`, `BUS_OUTBOX_OVERFLOW`, `BUS_OUTBOX_RELAY_INTERVAL`.
- README: add a short "Durable producer outbox" subsection with the §7 deploy rules: persistent per-instance volume, one file per process/worker (never shared across gunicorn workers), residual `create_task` window, duplicates expected, no global ordering.

**NOT in scope**: tagging / publishing to PyPI (manual, after the PR merges); `TOPICS.md` (no new topics); code changes.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/version.py` | MODIFY | `0.3.0` → `0.4.0` |
| `CHANGELOG.md` | MODIFY | `[0.4.0]` entry |
| `README.md` | MODIFY | config rows + outbox deploy note |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus import __version__   # verified: __init__.py:29 re-exports version.py
```

### Existing Signatures to Use
```text
src/navigator_eventbus/version.py:7   __version__ = "0.3.0"          (count 1)
pyproject.toml:7                       dynamic = ["version"]          (do not edit)
CHANGELOG.md:6                         ## [0.1.0] — Unreleased (pending tag + PyPI publish)   — newest entry today; 0.2.0/0.3.0 have no entries
README.md:162                          ## Configuration knobs
README.md:179                          ### BusCore / EventBus facade (`evb.py`, `core.py`)
README.md:189 (count 1)                | `BUS_DRAIN_TIMEOUT` | `drain_timeout=` | `5.0` (s) | Deadline for graceful `close()` drain |
README.md:192                          ### Redis Streams backend (`backends/redis_streams.py`)
```

### Does NOT Exist
- ~~`docs/`~~ directory — documentation lives in `README.md` only.
- ~~`CHANGELOG.md` entries for 0.2.0 / 0.3.0~~ — do not back-fill them in this task.
- ~~`navigator_eventbus.__version_info__`~~ — only `__version__`.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/version.py", "action": "MODIFY"},
    {"path": "CHANGELOG.md", "action": "MODIFY"},
    {"path": "README.md", "action": "MODIFY"}
  ],
  "contract_symbols": []
}
```

---

## Implementation Notes

### Key Constraints
- Defaults in the README must match the code merged by TASK-4555..4558 — re-read them, do not copy from the spec blindly.
- English only.

---

## Implementation Blueprint

### Steps (in order)
1. Bump the version — *why*: release target 0.4.0.
2. Add the CHANGELOG entry — *why*: downstream ai-parrot FEAT-710 pins this release.
3. Add the README rows and the deploy subsection — *why*: spec §7 says the deploy note must require a mounted volume.

### `src/navigator_eventbus/version.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c '__version__ = "0.3.0"' src/navigator_eventbus/version.py) → :7
__version__ = "0.4.0"
```

### `CHANGELOG.md` (MODIFY)
```markdown
<!-- occurrences: 1 (verified: grep -c '## \[0.1.0\] — Unreleased' CHANGELOG.md) → :6 -->
<!-- BEFORE — insert above `## [0.1.0] — Unreleased (pending tag + PyPI publish)` -->
## [0.4.0] — Unreleased

Ships FEAT-711 (durable lifecycle fan-out).

### Added

- FILL IN: one bullet per public addition listed in Scope (names exactly as merged).

### Delivery contract

- With an outbox configured, delivery from `emit` to a Streams consumer is **at-least-once**;
  consumers dedup by `envelope.event_id`.

```

### `README.md` (MODIFY) — config rows
```markdown
<!-- occurrences: 1 (verified: grep -c '| `BUS_DRAIN_TIMEOUT` |' README.md) → :189 -->
<!-- AFTER — insert below the `BUS_DRAIN_TIMEOUT` row -->
| `BUS_TRANSPORT` | `transport=` (on `EventBus`) | unset | `memory` \| `pubsub` \| `streams`; `backend=` wins, then `transport=`, then `use_redis` |
| `BUS_PUBLISH_ONLY` | `publish_only=` | `false` | Never start the backend consumer (producer processes) |
| `BUS_OUTBOX_PATH` | `outbox=` (on `EventBus`) | unset | SQLite outbox file — enables the durable producer outbox |
| `BUS_OUTBOX_MAX_ROWS` | `max_rows=` (on `SqliteOutbox`) | `1000000` | Row cap |
| `BUS_OUTBOX_OVERFLOW` | `overflow=` (on `SqliteOutbox`) | `"reject_new"` | `reject_new` \| `drop_oldest` |
| `BUS_OUTBOX_RELAY_INTERVAL` | `relay_interval=` | `5.0` (s) | Relay retry loop period |
```

### `README.md` (MODIFY) — deploy note
```markdown
<!-- BEFORE — insert above `### Redis Streams backend (`backends/redis_streams.py`)` (README.md:192, count 1) -->
#### Durable producer outbox

FILL IN: 5-8 lines covering spec §7 — persistent per-instance volume (ephemeral container disk defeats
restart recovery); one outbox file per process/worker, never shared; the residual window between a
lifecycle emit and the scheduled `bus.emit` task; duplicates expected (dedup by `event_id`); no global
ordering (order by event timestamp); a full outbox is an alarm, exposed via `BusCore.stats()`.
```

### FILL IN checklist
- [ ] CHANGELOG `Added` bullets.
- [ ] README deploy-note prose.
- [ ] defaults double-checked against merged code.

---

## Acceptance Criteria

- [ ] `python -c "import navigator_eventbus as n; print(n.__version__)"` prints `0.4.0`.
- [ ] CHANGELOG has a `[0.4.0]` entry above `[0.1.0]`.
- [ ] README lists all six new navconfig keys with defaults matching the code, plus the deploy note.

---

## Validation Commands

- `pytest tests/test_package.py -q`

---

## Test Specification

No new tests — `tests/test_package.py` covers the package import and version metadata.

---

## Agent Instructions

1. **Work in the feature worktree** (`python -m scripts.sdd.ensure_worktree --slug durable-lifecycle-fanout --feature-id FEAT-711`)
2. **Read the spec** for full context
3. **Check dependencies** — TASK-4559 must be `"done"`
4. **Verify the Codebase Contract** before editing
5. **Update status** → `"in-progress"` and commit only the index file
6. **Implement** from the blueprint; complete every `FILL IN` marker
7. **Verify** — run the Validation Commands
8. **Commit** — only the listed files
9. **Close the task** with `scripts/sdd/close_task.sh TASK-4560 durable-lifecycle-fanout verified`
10. **Fill in the Completion Note**, then commit the staged SDD state

---

## Completion Note

Implemented by gpt-5.6-luna (codex, 1 attempt); merged by sdd-worker.

**Completed by**: gpt-5.6-luna (codex, 1 attempt)
**Date**: 2026-10-20
**Notes**: Validated manually with PYTHONPATH=src (the validation tool imports the primary checkout's editable src): test_package passed.

**Deviations from spec**: none | describe if any
