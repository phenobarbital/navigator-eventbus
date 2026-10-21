# TASK-4569: Regression test — `BrokerManager.start()` cleans up when subscribe fails

**Feature**: FEAT-431 — Unified Broker Manager + brokers hardening
**Spec**: `sdd/specs/brokers-unified-manager.spec.md`
**Discovered-from**: `issue:545bb0fa9305`
**Status**: pending
**Priority**: medium
**Estimated effort**: S (<1h)
**Depends-on**: TASK-4564
**Assigned-to**: agent:sdd-fix

---

## Context

Ledger issue `issue:545bb0fa9305` (minor bug): in `BrokerManager.start()`, if
`_subscribe(app)` raised after `connect()` and `start_workers()`, the publish
workers and the broker connection stayed up. aiohttp may not run `on_shutdown`
after a failed startup, so nothing released them.

The code fix already landed in commit `d2a8d58` ("FEAT-431 review follow-ups"):
the subscribe step is wrapped in `try/except BaseException`, which logs, awaits
`self.stop(app)`, and re-raises. No test covered that path.

## Scope

- Add `test_manager_start_cleans_up_when_subscribe_fails` to
  `tests/brokers/test_manager.py`: a `_subscribe` that raises must propagate
  the error, and by then `_stop_consuming` ran, every worker is done, and
  `disconnect` ran exactly once.

**NOT in scope**: any change to `src/navigator_eventbus/brokers/manager.py`
(already fixed); the other broker cleanups in `issue:abe4a96fc840`.

## Files to Create / Modify

- `tests/brokers/test_manager.py` — MODIFY (add one test)

## Codebase Contract

### Existing Signatures
- `BrokerManager.start(self, app: web.Application) -> None` — `src/navigator_eventbus/brokers/manager.py:42`
- `BrokerManager.stop(self, app: web.Application) -> None` — `src/navigator_eventbus/brokers/manager.py:62`
- `DummyManager` test double — `tests/brokers/test_manager.py:11`

## Implementation Blueprint

Override `m._subscribe` on a `DummyManager(num_workers=2)` with an async
function that raises `RuntimeError`; assert `pytest.raises` on `m.start(...)`,
then assert the call log and `m._workers` state.

## Acceptance Criteria

- [x] New test fails against the pre-fix `manager.py` (`d2a8d58^`).
- [x] New test passes against the current `manager.py`.
- [x] `tests/brokers/` passes in full.

## Validation Commands

```bash
source .venv/bin/activate
PYTHONPATH=src pytest tests/brokers/test_manager.py -q
PYTHONPATH=src pytest tests/brokers/ -q
```

## Completion Note

Added the regression test. Verified it fails against `d2a8d58^:src/navigator_eventbus/brokers/manager.py`
(1 failed) and passes against the current code; `tests/brokers/`: 76 passed, 3 skipped.
Resolves `issue:545bb0fa9305` together with the fix in `d2a8d58`.
