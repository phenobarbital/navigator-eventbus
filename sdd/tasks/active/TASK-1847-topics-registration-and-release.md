# TASK-1847: TOPICS.md registration + release cut

**Feature**: FEAT-320 — RedisStreamsBackend Generic Capability Extensions
**Spec**: `sdd/specs/redis-streams-backend-extensions.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: S (< 2h)
**Depends-on**: TASK-1843, TASK-1844, TASK-1845, TASK-1846
**Assigned-to**: unassigned

---

## Context

Spec §3 Module 5 (closes the feature). Downstream consumers (FieldSync
FEAT-409, `../fieldsync`) pin `navigator-eventbus` by released version and
need `fieldsync.*` registered in `TOPICS.md` before they may publish under
that namespace (repo governance). This task registers the namespace, bumps
the package version, runs the full regression sweep, and cuts the release
that FieldSync's Module 6-8 tasks will pin against.

---

## Scope

- Register a `fieldsync.*` row in `TOPICS.md`, following the file's
  existing format/columns exactly (read the file first — do not invent a
  new column layout).
- Bump `src/navigator_eventbus/version.py` to the next version after
  `0.1.0` (read the file first to confirm its exact current value and
  format before choosing the new one — this repo uses semantic versioning;
  a MINOR bump is appropriate for additive, backward-compatible new
  capabilities, per semver).
- Run the FULL test suite (`pytest -x -q`) and `ruff check src/` — both
  must be clean. In particular, confirm TASK-1843/1844/1845/1846's new
  tests AND every pre-existing test in `tests/test_backends_streams.py`
  and `tests/test_integration.py` are green together (not just
  individually per-task).
- Tag the release (`git tag <version>`) — follow this repo's existing
  tagging convention (check `git tag` output for the `0.1.0`/`0.1.0rc1`/
  `0.1.0rc2` naming pattern already in use).
- In the Completion Note, record the FINAL released version AND the exact,
  final kwarg signatures of all four new capabilities (copy them straight
  from the actual `RedisStreamsBackend.__init__` signature after all four
  tasks have landed) — FieldSync's FEAT-409 Modules 6-8 verify against
  this recorded signature before they start.

**NOT in scope**: any new capability/behavior — this task is registration,
version bump, regression, and release ONLY. If any of TASK-1843/1844/1845/
1846's acceptance criteria are not met, STOP and report which task(s) are
incomplete rather than proceeding with the release.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `TOPICS.md` | MODIFY | register `fieldsync.*` namespace |
| `src/navigator_eventbus/version.py` | MODIFY | version bump |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
# No new imports for this task — registration/version/release only.
```

### Existing Signatures to Use
```text
# TOPICS.md — read the file first; it has an existing column/row format
# that MUST be matched exactly (do not invent new columns or a different
# markdown table shape).

# src/navigator_eventbus/version.py — read the file first to confirm the
# EXACT current value/format before bumping (e.g. a bare string constant,
# a tuple, or a __version__ = "x.y.z" assignment — verify, do not assume).
```

### Does NOT Exist
- ~~A `fieldsync` row already in `TOPICS.md`~~ — must be added (verified
  absent as of the spec's writing; re-verify before editing in case
  another task landed one in parallel).
- ~~A version beyond `0.1.0` tagged on `main` as of this task's start~~ —
  re-verify with `git tag` before choosing the new version number (another
  release may have landed in parallel; if so, bump from THAT version, not
  from a stale `0.1.0` assumption).

---

## Implementation Notes

### Key Constraints
- Read `TOPICS.md` and `version.py` fully before editing either — this
  task's whole job is to match existing conventions precisely, not invent
  new ones.
- Do not skip the "full suite together" regression check — per-task green
  runs do not guarantee the four modules compose without interference
  (e.g. two tasks adding tasks to `start_consumer()`/`close()` must all
  coexist correctly).
- If `ruff` or `pytest` surface issues introduced by any of the four
  upstream tasks, fix them here (small, obviously-correct fixes only) or
  clearly flag which task needs a follow-up — do not silently paper over a
  real regression.

### References in Codebase
- `TOPICS.md` (repo root) — existing topic registrations to match format against.
- `src/navigator_eventbus/version.py` — current version constant.

---

## Acceptance Criteria

- [ ] `fieldsync.*` registered in `TOPICS.md`, matching the file's existing format
- [ ] `src/navigator_eventbus/version.py` bumped to the next version after the current tagged release
- [ ] `source .venv/bin/activate && pytest -x -q` fully green (all four modules' tests + full pre-existing suite together)
- [ ] `ruff check src/` clean
- [ ] Release tagged (`git tag <version>`)
- [ ] Completion Note records: final released version + the exact final `RedisStreamsBackend.__init__` signature (all new kwargs, verbatim)

---

## Test Specification

```bash
# No new tests — this task is registration/release only. The gate is the
# full existing + new suite passing TOGETHER:
source .venv/bin/activate
pytest -x -q
ruff check src/
```

---

## Agent Instructions

1. **Read the spec** (`sdd/specs/redis-streams-backend-extensions.spec.md`) §3 Module 5 and §5 (acceptance criteria) for full context
2. **Check dependencies** — TASK-1843, TASK-1844, TASK-1845, TASK-1846 must ALL be in `sdd/tasks/completed/`
3. **Verify the Codebase Contract** — read `TOPICS.md` and `version.py` before editing either
4. **Update status** in `sdd/tasks/index/redis-streams-backend-extensions.json` → `"in-progress"`
5. **Implement**, **verify**, **move this file** to `sdd/tasks/completed/`,
   **update index** → `"done"`, **fill in the Completion Note** (with the
   final version + signatures — FieldSync's FEAT-409 depends on this)

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**:
**Date**:
**Notes**: (MUST include: released version, exact final kwarg signatures of delivery/codec/stream_key_fn/streams/retention/retention_trim_interval/max_deliveries/on_dlq)

**Deviations from spec**: none
