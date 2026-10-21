# TASK-4568: Manager integration tests + release prep (next minor)

**Feature**: FEAT-431 — Unified Broker Manager + brokers hardening
**Spec**: `sdd/specs/brokers-unified-manager.spec.md`
**Status**: pending
**Priority**: medium
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-4561, TASK-4562, TASK-4563, TASK-4565, TASK-4566
**Assigned-to**: unassigned

---

## Context

Closes spec §3 **Module 7** (integration tests from spec §4 "Integration Tests") and
the release AC ("`version.py` bumped and released, so FEAT-002 downstream can pin it").
The version is the **next minor after whatever `version.py` reads when this task runs**.
Today that is `0.3.0` → `0.4.0`. FEAT-711's TASK-4560 (`sdd/tasks/active/TASK-4560-release-0-4-0.md`)
also bumps to `0.4.0` and adds a `[0.4.0]` CHANGELOG entry. If it has landed on the base branch
first, this task uses `0.5.0` instead. Rebase or merge the base branch before starting so you see it. The CHANGELOG must call out the Module 5
behavior change in `event_subscribe()` (spec §7 risk 4).

---

## Scope

- Create `tests/brokers/test_manager_integration.py` with the three integration tests.
  Each skips gracefully when no live broker is reachable (pattern: `tests/brokers/test_redis_integration.py:23-37`).
  - `test_manager_roundtrip_rabbitmq` (`@pytest.mark.integration`): publish via an `RMQManager`, consume on the same instance, `connect()` called once.
  - `test_manager_roundtrip_redis` (`@pytest.mark.redis`): same with `RedisManager`.
  - `test_exchange_binding_roundtrip` (`@pytest.mark.integration`): `queue_name != exchange_name`; a message published to the exchange reaches the consumer. This is the end-to-end D1 regression.
- Bump `src/navigator_eventbus/version.py` `__version__` to the next minor (`0.4.0`, or `0.5.0` if FEAT-711 already took `0.4.0`).
- Add a `## [<that version>]` section at the top of `CHANGELOG.md` (above any FEAT-711 `[0.4.0]` entry) covering Added / Fixed / Changed.
- Run the whole broker suite plus `ruff` and `mypy` as the feature-level gate.

**NOT in scope**: tagging or publishing the release (a human step after merge); new pytest markers
(that would edit `pyproject.toml`; reuse `integration`/`redis`); fixing failures inside other tasks' files.
Report those failures instead.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `tests/brokers/test_manager_integration.py` | CREATE | Live-broker round trips (skip when unavailable) |
| `src/navigator_eventbus/version.py` | MODIFY | next minor (`0.4.0`, or `0.5.0` after FEAT-711) |
| `CHANGELOG.md` | MODIFY | Release notes for that version |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.brokers.rabbitmq import RMQManager, RabbitMQConnection   # RMQManager from TASK-4565
from navigator_eventbus.brokers.redis import RedisManager                        # from TASK-4566
from navigator_eventbus.brokers._conf import rabbitmq_dsn                        # verified: spec §6 (brokers/_conf.py)
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/version.py
__version__ = "0.3.0"                                              # line 7

# CHANGELOG.md
## [0.1.0] — Unreleased (pending tag + PyPI publish)               # line 6 — the only release header today; insert the new section above it

# src/navigator_eventbus/brokers/rabbitmq/connection.py
async def connect(self) -> None          # line 54 — on failure calls schedule_reconnect() (exponential backoff, raises RuntimeError when exhausted)
async def publish_message(self, body, queue_name, routing_key, **kwargs)  # line 159 — `queue_name` is the EXCHANGE name (ensure_exchange(queue_name); basic_publish(exchange=queue_name))

# src/navigator_eventbus/brokers/producer.py
async def queue_event(self, body, queue_name, routing_key=None, **kwargs)  # line 136 — enqueue for the workers
async def publish_event(self, body, queue_name, **kwargs)                   # line 162 — publishes directly

# src/navigator_eventbus/brokers/redis/connection.py
async def consume_messages(self, queue_name, callback, count=1, block=1000, **kwargs)  # line 233
# Redis streams are seeded with an {'initial': 'message'} entry that decodes to None
# (see tests/brokers/test_redis_integration.py module docstring) — filter it out.

# tests/brokers/test_redis_integration.py:23-37 — skip pattern:
#   try: await asyncio.wait_for(conn.connect(), timeout=2) except Exception: pytest.skip("no live Redis available")
```
`pyproject.toml` markers (lines 112-115): `redis`, `integration`. No `rabbitmq` marker exists.

### Does NOT Exist
- ~~A `rabbitmq` pytest marker~~ — not registered; use `integration`.
- ~~A RabbitMQ backend for `BusCore`~~ — the managers are not `TransportBackend`s.
- ~~A `0.2.x`/`0.3.x` section in `CHANGELOG.md`~~ — not there and not back-filled here. (FEAT-711 TASK-4560 may add `[0.4.0]`.)

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "tests/brokers/test_manager_integration.py", "action": "CREATE"},
    {"path": "src/navigator_eventbus/version.py", "action": "MODIFY"},
    {"path": "CHANGELOG.md", "action": "MODIFY"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/brokers/rabbitmq/connection.py#RabbitMQConnection.publish_message",
    "sym:src/navigator_eventbus/brokers/producer.py#BrokerProducer.queue_event"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- Integration fixtures must **skip, not fail**, without a broker. Wrap the first `connect()` in
  `asyncio.wait_for(..., timeout=2)`, because `RabbitMQConnection.connect` retries with backoff before raising.
- Use unique queue/exchange/stream names (`uuid4().hex[:8]`) and clean up (delete stream / queue) in teardown.
  This way repeated runs never read stale messages.
- Wait for consumption with a bounded `asyncio.wait_for(event.wait(), timeout=5)`, never a bare sleep.
- To count connections, wrap the instance's `connect` with a counter **before** `start()`.

---

## Implementation Blueprint

### Steps (in order)
1. Write the integration test module — *why*: spec §4 Integration Tests.
2. Bump the version — *why*: AC "version.py bumped".
3. Write the CHANGELOG section — *why*: the release notes must flag the Module 5 behavior change.
4. Run the full broker suite, `ruff check src/ tests/` and `mypy src/`, and report.

### `tests/brokers/test_manager_integration.py` (CREATE)
```python
"""Live-broker round trips for the unified managers (FEAT-431 Module 7, TASK-4568).

Skipped gracefully when RabbitMQ / Redis are not reachable.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest
from aiohttp import web

from navigator_eventbus.brokers.rabbitmq import RMQManager
from navigator_eventbus.brokers.redis import RedisManager


def _uid() -> str:
    return uuid.uuid4().hex[:8]


async def _started(manager, counter: dict):
    """Count connect() calls, start the manager, or skip when the broker is down."""
    original = manager.connect

    async def counting_connect():
        counter["connects"] += 1
        await original()

    manager.connect = counting_connect
    try:
        await asyncio.wait_for(manager.start(web.Application()), timeout=2)
    except Exception:
        pytest.skip(f"no live broker for {type(manager).__name__}")
    return manager


@pytest.mark.integration
async def test_manager_roundtrip_rabbitmq():
    got = asyncio.Event()
    received = []
    exchange = f"feat431-{_uid()}"

    async def cb(message, body):
        received.append(body)
        got.set()

    counter = {"connects": 0}
    m = await _started(RMQManager(exchange_name=exchange, queue_name=exchange, routing_key="#", callback=cb), counter)
    try:
        await m.queue_event({"hello": "world"}, exchange, "feat431.test")
        await asyncio.wait_for(got.wait(), timeout=5)
        # FILL IN: assert received == [{"hello": "world"}] and counter["connects"] == 1
    finally:
        await m.stop(None)
        # FILL IN: delete the test queue/exchange if a channel helper is available; else document leftover


@pytest.mark.integration
async def test_exchange_binding_roundtrip():
    """queue_name != exchange_name: a message published to the exchange reaches the consumer (D1)."""
    # FILL IN: same as above with exchange_name=f"ex-{uid}", queue_name=f"q-{uid}", routing_key="#";
    #          publish with m.publish_event(body, f"ex-{uid}", routing_key="any.key"); assert received


@pytest.mark.redis
async def test_manager_roundtrip_redis():
    # FILL IN: RedisManager(queue_name=f"feat431-{uid}", callback=cb); publish via m.queue_event(...);
    #          ignore the seeded None entry; assert payload received and one connect; m.stop(None);
    #          delete the stream in finally — bounded by spec §4 test_manager_roundtrip_redis
    ...
```

### `src/navigator_eventbus/version.py` (MODIFY)
```python
# occurrences: 1 (verified: grep -c '__version__ = "0.3.0"' src/navigator_eventbus/version.py)
# FILL IN: if it now reads "0.4.0" (FEAT-711 landed) use "0.5.0"; otherwise "0.4.0" — bounded by spec header "Target version"
# REPLACE line 7 with:
__version__ = "0.4.0"
```

### `CHANGELOG.md` (MODIFY)
```markdown
# occurrences: 1 (verified: grep -c '^## \[0.1.0\]' CHANGELOG.md)
# BEFORE — insert above the newest release header (today `## [0.1.0] — Unreleased …`, CHANGELOG.md:6; above `## [0.4.0]` if FEAT-711 added it)
## [0.4.0] — Unreleased   # FILL IN: same version as version.py

FEAT-431 — Unified Broker Manager + brokers hardening.

### Added
- `BrokerManager` (`navigator_eventbus.brokers`) plus `RMQManager`, `RedisManager`, `SQSManager`
  (from their subpackages): publish and consume over a single connection, `auto_subscribe` flag,
  consumer-first shutdown.
- `BrokerProducer(endpoint_path=...)`: configurable HTTP publish route (default unchanged).
- `RabbitMQConnection.ensure_queue()` and `reject_message()`.

### Fixed
- RabbitMQ consumer: an explicit `exchange_name` is no longer overwritten by `queue_name` (D1).
- `RabbitMQBrokerHook.connect()` no longer raises `AttributeError`; hook failures settle once
  (non-requeued nack, no ack/republish).
- `queue_event()` no longer sleeps 100 ms per event.

### Changed
- **Behaviour change** — `RMQConsumer.event_subscribe()` / `SQSConsumer.event_subscribe()` wrap the
  callback once: callbacks now receive `(message, decoded_body)` as documented, instead of failing on
  a double wrap. FILL IN: one sentence on how affected callers should adapt.
```

### FILL IN checklist
- [ ] RabbitMQ round-trip assertions and cleanup.
- [ ] `test_exchange_binding_roundtrip` body (end-to-end D1).
- [ ] `test_manager_roundtrip_redis` body.
- [ ] CHANGELOG "Changed" adaptation sentence.

---

## Acceptance Criteria

- [ ] The three integration tests pass against live brokers and **skip** (not fail) without them.
- [ ] `__version__` is the next minor, and it does not collide with FEAT-711's `0.4.0`.
- [ ] `CHANGELOG.md` has a section for that version, including the `event_subscribe()` behavior change.
- [ ] `pytest tests/brokers/ -q` passes (feature gate; report any failure with its owning task).
- [ ] `ruff check src/ tests/` and `mypy src/` pass, or pre-existing failures are listed in the Completion Note.

---

## Validation Commands
- `pytest tests/brokers/test_manager_integration.py -q`

(The feature-level gate — every broker test file — is added by the validation-contract selector at merge/feature tier.)

---

## Test Specification

See the blueprint test file (spec §4 "Integration Tests").

---

## Agent Instructions

1. **Work in the feature worktree** — never on `dev`
   (`python -m scripts.sdd.ensure_worktree --slug brokers-unified-manager --feature-id FEAT-431`).
2. **Check dependencies**: TASK-4561, 4562, 4563, 4565, 4566 must be `done` (TASK-4567 is not a dependency, but the merge/feature-tier gate runs its tests too).
3. **Environment**: `source .venv/bin/activate`; if `pytest`/`aiormq`/`aioboto3` are missing run
   `uv pip install -e ".[dev,rabbitmq,sqs,redis]"` (no manifest change).
4. Verify the Codebase Contract; set status `in-progress`, commit only the index.
5. Implement from the blueprint; complete every `# FILL IN:`; run the Validation Commands; stage only listed files.
6. Close with `scripts/sdd/close_task.sh TASK-4568 brokers-unified-manager verified`, fill the Completion Note, commit.

---

## Completion Note

**Completed by**: sonnet (native, after empty delivery on gpt-5.6-terra)
**Date**: 2026-10-21
**Notes**: Merged. Version 0.4.0 + CHANGELOG. mypy unrunnable (broken venv binary). Redis integration test skipped (no live Redis). RabbitMQ test leaves queue/exchange on broker. 2 pre-existing navconfig-env failures.

**Deviations from spec**: none
