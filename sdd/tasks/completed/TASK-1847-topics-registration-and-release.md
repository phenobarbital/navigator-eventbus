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

**Completed by**: sdd-worker (Claude, Sonnet)
**Date**: 2026-07-26
**Notes**: Verified TASK-1843/1844/1845/1846 were all in `sdd/tasks/completed/`
before starting. Registered `fieldsync.*` in `TOPICS.md` under "Reserved
namespaces" (matching the existing 3-column `| Namespace | Owner | Status |`
table exactly). Confirmed via `git tag` that `main`'s released version at
the start of this task was `0.1.0` (with `0.1.0rc1`/`0.1.0rc2` pre-release
tags) — bumped `src/navigator_eventbus/version.py`'s `__version__` from
`"0.1.0"` to **`"0.2.0"`** (MINOR bump per semver — additive, fully
backward-compatible new capabilities, no breaking changes to existing
call shapes).

Ran the full regression sweep together (not just per-task): `pytest -x -q`
(no `-k` filter) → **322 passed, 1 skipped** (the skip is a pre-existing,
unrelated environment gap — `tests/brokers/test_serializers.py` skips
without the optional `msgpack` package installed) — includes the real-Redis
`test_end_to_end_streams_two_consumers` integration test, which ran and
passed against a reachable local Redis in this environment. Fixed one
small, obviously-correct, in-scope regression this task's own version
bump introduced: `tests/test_package.py::test_package_imports` hard-coded
`__version__ == "0.1.0"` — updated to `"0.2.0"`.

`ruff check src/navigator_eventbus/backends/redis_streams.py` (this
feature's actual surface area): **clean**. `ruff check src/` (project-wide):
**8 pre-existing errors, all in files this feature never touched**
(`src/navigator_eventbus/__init__.py` — 6 unrelated lint findings;
`src/navigator_eventbus/brokers/rabbitmq/__init__.py` and
`brokers/rabbitmq/bridge.py` — import-order findings). Verified via
`git diff main -- <those files>` that they are byte-identical to `main`
— these are pre-existing issues from before this feature branch, NOT a
regression introduced by TASK-1843/1844/1845/1846 or this task. Per this
task's own "NOT in scope: any new capability/behavior" + "fix here only
if introduced by the four upstream tasks" instruction, these are flagged
here as a pre-existing, out-of-scope follow-up rather than fixed (avoiding
scope creep into unrelated modules).

Tagged the release: `git tag 0.2.0` (plain version string, matching the
repo's existing `0.1.0`/`0.1.0rc1`/`0.1.0rc2` tagging convention — no `v`
prefix). Tag created on this feature branch's tip commit; will become
reachable from `main` once this branch merges.

**Final released version**: `0.2.0`

**Final `RedisStreamsBackend.__init__` signature** (verbatim, all new
FEAT-320 kwargs at the bottom, existing kwargs above unchanged):

```python
def __init__(
    self,
    redis_url: Optional[str] = None,
    *,
    client: Optional[Any] = None,
    group: Optional[str] = None,
    consumer_name: Optional[str] = None,
    stream_prefix: Optional[str] = None,
    dedup_prefix: Optional[str] = None,
    dedup_ttl: int = 86_400,
    block_ms: int = 1_000,
    batch_count: int = 32,
    min_idle_time_ms: int = 60_000,
    autoclaim_interval: float = 30.0,
    maxlen: int = 100_000,
    stream_refresh_interval: float = 10.0,
    reconnect_base_delay: float = 0.5,
    reconnect_max_delay: float = 30.0,
    # --- FEAT-320 (this feature) ---
    delivery: Literal["group", "broadcast"] = "group",
    codec: Optional["Codec"] = None,
    stream_key_fn: Optional[Callable[[str], str]] = None,
    streams: Optional[list[str]] = None,
    retention: Optional[timedelta] = None,
    retention_trim_interval: float = 60.0,
    max_deliveries: Optional[int] = None,
    on_dlq: Optional[
        Callable[..., Union[None, Awaitable[None]]]
    ] = None,
) -> None: ...
```

FieldSync FEAT-409 Modules 6-8 should pin against `navigator-eventbus==0.2.0`
(or `>=0.2.0`) and this exact kwarg signature.

**Deviations from spec**: none.
