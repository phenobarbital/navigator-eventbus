# TASK-1866: Bump to 0.3.0 and prepare the release

**Feature**: FEAT-433 — BusCore Producer Facade
**Spec**: `sdd/specs/buscore-producer-facade.spec.md`
**Status**: pending
**Priority**: medium
**Estimated effort**: S (< 2h)
**Depends-on**: TASK-1863, TASK-1864, TASK-1865
**Assigned-to**: unassigned

---

## Context

Implements spec **Module 4**. FieldSync cannot delete its local
`BusCorePublisherShim` until it can pin a released navigator-eventbus version
that contains `BusCoreProducer` — the version bump is the deliverable that
unblocks the whole point of the feature.

This task also carries the **two migration notes** that make FieldSync's
cut-over safe. Both are behavioral divergences from the shim being replaced,
and both fail silently rather than loudly, so they must be written down where a
consumer will read them.

---

## Scope

- Bump `__version__` in `src/navigator_eventbus/version.py` from `0.2.4` to `0.3.0`.
- Verify the whole suite, lint and types are green across the feature.
- Record the two consumer-facing migration notes (see below) in the release
  notes / PR description.

**NOT in scope**:
- Any code change to `producers.py` or `__init__.py` — if something is broken,
  fix it in its own task rather than here.
- Publishing to PyPI or tagging — that is a human release step.
- Any edit to FieldSync.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/version.py` | MODIFY | `__version__ = "0.3.0"` |

---

## Codebase Contract (Anti-Hallucination)

> Verified by reading source on 2026-09-12 at commit `2ba82b1`.

### Existing Signatures to Use

```python
# src/navigator_eventbus/version.py — the ONLY place the version is written
__title__ = "navigator-eventbus"          # line 3
__description__ = (...)                    # line 4
__version__ = "0.2.4"                      # line 7   ← change to "0.3.0"
__author__ = "Jesus Lara"                  # line 8
__author_email__ = "jesuslara@phenobarbital.info"   # line 9
__license__ = "MIT"                        # line 10
__copyright__ = "Copyright (c) 2020-2026 Jesus Lara"  # line 11
```

```toml
# pyproject.toml
dynamic = ["version"]        # line 7  — the version is READ from version.py at build time
requires-python = ">=3.11"   # line 14
```

### Does NOT Exist

- ~~a hard-coded version in `pyproject.toml`~~ — it is `dynamic` (line 7). Do
  NOT add a `version = "0.3.0"` key; that would conflict with the dynamic
  declaration and break the build.
- ~~`CHANGELOG.md`~~ — confirm before creating one; the repo has not kept one,
  and inventing a changelog file is out of scope for this task.
- ~~a version literal in `tests/test_package.py`~~ — that test compares the root
  re-export against `version.py` deliberately, precisely so it does NOT go
  stale on a bump. It needs **no** edit. Its docstring says so explicitly.
- ~~`src/navigator_eventbus/__init__.py` version literal~~ — the root re-exports
  from `version.py` (line 28); nothing to change there.

---

## Implementation Notes

### Why 0.3.0 and not 0.2.5

Spec §8 Q2, resolved. Repo history shows both patterns — `0.2.2` and `0.2.3`
shipped features as patch bumps — but the single minor bump, `0.2.0`
(commit `8889f91`), marked new public surface. FEAT-433 adds a new top-level
name to `__all__`, which is that kind of event: adding a parameter to an
existing class and adding a whole class are different things.

### The two migration notes (REQUIRED in the release notes / PR body)

These are the deliverable, not decoration. Both are silent-failure modes for a
consumer migrating off a hand-rolled shim.

1. **`body["ts"]` is no longer read.** The shim this replaces populated the
   timestamp from a FieldSync-specific `body["ts"]` field. `BusCoreProducer`
   deliberately does not (spec §7 Known Risks — it must not acquire an
   undocumented application dependency). Call sites that relied on it will
   **silently start stamping "now"** instead of erroring. They must pass
   `timestamp=<tz-aware datetime>` explicitly to `publish_event`.

2. **`source` / `severity` / `priority` are constructor-only.** They are
   per-producer-instance arguments (spec §8 Q3). Passing them to
   `publish_event` is silently ignored, because `**kwargs` is inert apart from
   `timestamp`. A migrating call site should construct
   `BusCoreProducer(get_bus, source="<service-name>")` rather than leaving
   `source=None` — otherwise every event it emits is unattributable on the bus.

### Key Constraints

- Change exactly one line. The bump touches `version.py` and nothing else.
- Do not tag, do not publish, do not touch `uv.lock`.

### References in Codebase

- `tests/test_package.py:test_package_imports` — asserts the root re-export
  matches `version.py`; it will exercise the bump automatically.
- `tests/test_package.py:test_version_is_a_sane_release_string` — regex-guards
  the version shape; `0.3.0` satisfies it.

---

## Acceptance Criteria

- [ ] `src/navigator_eventbus/version.py` has `__version__ = "0.3.0"`.
- [ ] `pyproject.toml` still declares `dynamic = ["version"]` and contains no
      hard-coded version key.
- [ ] `python -c "import navigator_eventbus; print(navigator_eventbus.__version__)"`
      prints `0.3.0`.
- [ ] `pytest tests/test_package.py -v` passes without edits to that file.
- [ ] **Full suite green**: `pytest -q` — no failures, no errors.
- [ ] Lint clean on every file the feature touched:
      `ruff check src/navigator_eventbus/producers.py src/navigator_eventbus/__init__.py src/navigator_eventbus/version.py`
- [ ] Types clean: `mypy src/navigator_eventbus/producers.py`
- [ ] All of spec §5's acceptance criteria are satisfied across TASK-1863..1866
      (re-read §5 and tick each one).
- [ ] Both migration notes above appear verbatim-in-substance in the PR body.

---

## Test Specification

No new tests. This task is verification plus a one-line change; the existing
`tests/test_package.py` covers the bump:

```python
# tests/test_package.py — EXISTING, do not modify
def test_package_imports():
    assert navigator_eventbus.__version__ == version_module.__version__

def test_version_is_a_sane_release_string():
    assert re.fullmatch(r"\d+\.\d+(\.\d+)?([abrc]\d*|\.dev\d*|[-+.].+)?",
                        navigator_eventbus.__version__)
```

Verification commands to run and record:

```bash
source .venv/bin/activate
pytest -q
ruff check src/navigator_eventbus/
mypy src/navigator_eventbus/producers.py
python -c "import navigator_eventbus as n; print(n.__version__, n.BusCoreProducer)"
```

---

## Agent Instructions

1. **Check dependencies**: TASK-1863, TASK-1864 and TASK-1865 must ALL be in
   `sdd/tasks/completed/`. This task gates the release — do not run it early.
2. **Verify the Codebase Contract** — confirm `version.py:7` still reads
   `0.2.4` and `pyproject.toml:7` still says `dynamic`.
3. Update `sdd/tasks/index/buscore-producer-facade.json` → `"in-progress"`.
4. Make the one-line change; run every verification command above and save the
   output to `artifacts/logs/` per `CLAUDE.md`.
5. Verify every acceptance criterion, including re-reading spec §5.
6. Move this file to `sdd/tasks/completed/`, update the index → `"done"` and
   set the index's `completed_at`.
7. Fill in the Completion Note, including both migration notes for the PR body.

---

## Completion Note

**Completed by**: sdd-worker (Claude Sonnet 5)
**Date**: 2026-09-12
**Notes**: Bumped `__version__` from `0.2.4` to `0.3.0` in
`src/navigator_eventbus/version.py` — the only line changed. Verification
logs saved to `artifacts/logs/TASK-1866-{pytest,ruff,mypy,version-check}.log`:
`pytest -q` → 799 passed / 18 skipped; `ruff check src/navigator_eventbus/`
→ all checks passed; `mypy src/navigator_eventbus/producers.py` → no
issues; `python -c "import navigator_eventbus as n; print(n.__version__,
n.BusCoreProducer)"` → `0.3.0 <class
'navigator_eventbus.producers.BusCoreProducer'>`. `tests/test_package.py`
passes unmodified. `pyproject.toml` still declares `dynamic = ["version"]`
with no hard-coded version key. All of spec §5's acceptance criteria are
satisfied across TASK-1863..1866.

**Migration notes for the PR body (required per spec §7 / task instructions):**

1. **`body["ts"]` is no longer read.** The FieldSync shim being replaced
   populated the timestamp from a FieldSync-specific `body["ts"]` field.
   `BusCoreProducer` deliberately does not. Call sites that relied on it
   will **silently start stamping "now"** instead of erroring — they must
   pass `timestamp=<tz-aware datetime>` explicitly to `publish_event`.
2. **`source` / `severity` / `priority` are constructor-only.** They are
   per-producer-instance arguments. Passing them to `publish_event` is
   silently ignored (`**kwargs` is inert apart from `timestamp`). A
   migrating call site should construct
   `BusCoreProducer(get_bus, source="<service-name>")` rather than leaving
   `source=None`, or every event it emits is unattributable on the bus.

**Deviations from spec**: none
