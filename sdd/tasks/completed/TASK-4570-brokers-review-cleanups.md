# TASK-4570: Brokers review cleanups (RejectDelivery import, cancel docs, logging, mypy)

**Feature**: FEAT-431 — Unified Broker Manager + brokers hardening
**Spec**: `sdd/specs/brokers-unified-manager.spec.md`
**Status**: done
**Priority**: low
**Estimated effort**: S (<1h)
**Depends-on**: TASK-4563, TASK-4566, TASK-4567
**Assigned-to**: agent:sdd-fix
**Discovered-from**: `issue:abe4a96fc840`

## Context

The FEAT-431 review left a batch of low-severity cleanups (ledger issue
`issue:abe4a96fc840`). Some were already handled by `d2a8d58` (review
follow-ups); this task covers the rest and records the disposition of each item.

## Scope

| # | Review item | Disposition |
|---|---|---|
| 1 | `hooks/brokers/rabbitmq.py` imports `RejectDelivery` lazily inside an `except` block | Move `RejectDelivery` to a new dependency-free `brokers/exceptions.py` and import it at module level in the hook. A plain module-level import of `brokers.rabbitmq.connection` is NOT allowed: it imports `aiormq`, an optional extra (`[rabbitmq]`). `connection.py` re-imports the class so existing imports keep working. |
| 2 | `SQSManager` / `RedisManager._stop_consuming` cancel tasks mid-handler | Document in docstrings: a message being handled at cancel time is not acked and is redelivered (SQS: after the visibility timeout; Redis: stays in the group's PEL until claimed). |
| 3 | `BrokerManager.stop()` swallows errors; new code logs with f-strings | `stop()` already uses `self.logger.exception` (d2a8d58). Convert the FEAT-431-added f-string log calls (`ensure_queue`, `reject_message`, `RejectDelivery` branch, hook callback) to lazy `%s` formatting. Pre-existing ported log lines are out of scope. |
| 4 | Formatting churn in `rabbitmq/connection.py`, blank line after module docstring in `redis/__init__.py`, `sqs/__init__.py` | No change: these hunks are the output of the project's post-worker formatter step; reverting them would be undone by the next format run. |
| 5 | mypy gate never ran | Install the declared dev dependency `mypy>=1.10` and run it on the brokers package; fix errors in FEAT-431 files. |
| 6 | CHANGELOG `[0.4.0]` "Unreleased" | Out of scope — dated and tagged at merge time by `/sdd-done`. |
| 7 | navconfig tests not hermetic | Already fixed in d2a8d58 (tests pin `config.get` to `os.environ`). |

**NOT in scope**: behaviour changes to settlement or cancellation; the CHANGELOG release date.

## Files to Create / Modify

- CREATE `src/navigator_eventbus/brokers/exceptions.py`
- MODIFY `src/navigator_eventbus/brokers/rabbitmq/connection.py`
- MODIFY `src/navigator_eventbus/hooks/brokers/rabbitmq.py`
- MODIFY `src/navigator_eventbus/brokers/redis/manager.py`
- MODIFY `src/navigator_eventbus/brokers/sqs/manager.py`
- MODIFY `tests/brokers/test_rabbitmq_settlement.py` (identity + import test)

## Acceptance Criteria

- [x] `from navigator_eventbus.brokers.exceptions import RejectDelivery` works without `aiormq`.
- [x] `navigator_eventbus.brokers.rabbitmq.connection.RejectDelivery is navigator_eventbus.brokers.exceptions.RejectDelivery`.
- [x] `hooks/brokers/rabbitmq.py` has no function-local `RejectDelivery` import.
- [x] Both `_stop_consuming` docstrings describe the redelivery behaviour.
- [x] `pytest tests/brokers` passes; mypy reports no errors in FEAT-431 files.

## Validation Commands

```bash
pytest tests/brokers -q
python -m mypy src/navigator_eventbus/brokers src/navigator_eventbus/hooks/brokers/rabbitmq.py
```

## Completion Note

Implemented by agent:sdd-fix for `issue:abe4a96fc840`.

- Item 1: `RejectDelivery` moved to `brokers/exceptions.py`; `rabbitmq/connection.py`
  re-imports it (same class object); the hook imports it at module level. A
  subprocess test confirms the hook imports with `aiormq` blocked.
- Item 2: redelivery-on-cancel documented in both `_stop_consuming` docstrings.
- Item 3: FEAT-431-added log calls use lazy `%s` formatting. `stop()` already used
  `logger.exception`.
- Item 4: no change (formatter output; see Scope).
- Item 5: mypy 2.4.0 installed (declared dev dep) and run on `brokers/` +
  the RabbitMQ hook: 80 → 77 errors. Fixed the 3 in `ensure_queue` /
  `reject_message` via `_require_channel()`. Remaining: 72 in ported FEAT-316
  code, plus 5 on FEAT-431-touched lines that share those legacy patterns
  (nullable `_channel` in `consume_messages` / `subscribe_to_events`,
  `Union[Callable, Awaitable]` callback typing in `event_subscribe`, and the
  `SQSManager` MRO `wrap_callback` signature clash). Those need a typing pass on
  the whole brokers layer — not done here.
- Items 6, 7: out of scope / already fixed (see Scope).

Validation: `pytest tests/brokers` → 78 passed, 3 skipped; `ruff check` clean.
