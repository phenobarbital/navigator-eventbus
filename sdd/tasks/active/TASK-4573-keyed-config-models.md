# TASK-4573: KeyedQueue config and models

**Feature**: FEAT-713 — KeyedQueue (key-ordered, head-blocking work queue)
**Spec**: `sdd/specs/keyed-queue.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: S (< 2h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Spec §2 "Data Models" and §3 Module 1. Every other FEAT-713 task imports these
models; they carry no Redis logic. v0.2 of the spec added `due_ms`, `gen`,
`pause_poll_ms`, `recover_interval_ms`, `wait_safety_ms` and the
visibility > lock-TTL validator (signal rule 2).

---

## Scope

- Create the `navigator_eventbus.queues.keyed` package (`__init__.py`) and `config.py`.
- Implement `KeyedQueueConfig`, `KeyedMessage`, `Outcome`, `DrainReport`, `LeaseLost`
  exactly as in spec §2, all `extra="forbid"`.
- `KeyedQueueConfig` validators: the embedded `signal` queue declares **exactly one** group;
  `signal.default_visibility_timeout_ms > lock_ttl_ms`; `retry_base_ms <= retry_max_ms`.
- `KeyedQueueConfig.effective_wait_safety_ms` property: `wait_safety_ms` or `lock_ttl_ms * 10` (Q3).
- `Outcome` class-method constructors: `Outcome.ack()`, `Outcome.retry(delay_ms=None, reason=None)`,
  `Outcome.wait(until_ms=None, reason=None)`, `Outcome.hold(reason)` — FEAT-830 calls them by these names.
- Unit tests for every validator and constructor.

**NOT in scope**: anything touching Redis (TASK-4575/4576), exports from
`navigator_eventbus.queues` (TASK-4579). The spec's M1 list mentions `KeyHeld`; no method
in the spec raises it — do **not** create it.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/queues/keyed/__init__.py` | CREATE | package docstring + re-export of the config names only |
| `src/navigator_eventbus/queues/keyed/config.py` | CREATE | models and exceptions |
| `tests/queues/keyed/__init__.py` | CREATE | empty (test package, mirrors `tests/queues/__init__.py`) |
| `tests/queues/keyed/test_config.py` | CREATE | validator / constructor tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator  # pydantic v2 in .venv
from navigator_eventbus.queues.config import QueueConfig, QueueGroupConfig  # verified: queues/config.py:95, :67
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/queues/config.py:95
class QueueConfig(BaseModel):
    name: str
    groups: list[QueueGroupConfig]
    default_visibility_timeout_ms: int = Field(default=30_000, ge=0)   # :118
    max_visibility_timeout_ms: int = Field(default=3_600_000, ge=1)    # :119
    max_receives: int = Field(default=5, ge=1)                          # :120 — never 0/unlimited
```

### Does NOT Exist
- ~~`navigator_eventbus.queues.keyed`~~ — created by this task.
- ~~`KeyHeld`~~ — listed in spec M1 prose but never raised; do not create.
- ~~`QueueConfig.ordering_key` / `delay`~~ — the pull plane has neither.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/queues/keyed/__init__.py", "action": "CREATE"},
    {"path": "src/navigator_eventbus/queues/keyed/config.py", "action": "CREATE"},
    {"path": "tests/queues/keyed/__init__.py", "action": "CREATE"},
    {"path": "tests/queues/keyed/test_config.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/queues/config.py#QueueConfig",
    "sym:src/navigator_eventbus/queues/config.py#QueueGroupConfig"
  ]
}
```

---

## Implementation Notes

- Pydantic v2, `model_config = ConfigDict(extra="forbid")` on every model (spec §7).
- Google-style docstrings on every public class.
- Keep `keyed/__init__.py` free of Redis imports: it re-exports only names from `config.py` now;
  TASK-4579 adds lazy exports for `KeyedQueue`, `KeyedConsumer`, `drain`.

---

## Implementation Blueprint

### Steps (in order)
1. Create `config.py` from the block below — *why*: every later task imports these names verbatim.
2. Fill the validators — *why*: signal rule 2 (visibility > lock TTL) is a liveness invariant, enforce it at construction.
3. Create the test package and `test_config.py` — *why*: each validator must be shown to reject.

```python
# src/navigator_eventbus/queues/keyed/config.py  (CREATE)
"""Models for keyed queues (FEAT-713): config, message, outcome, drain report."""
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from navigator_eventbus.queues.config import QueueConfig

__all__ = ("DrainReport", "KeyedMessage", "KeyedQueueConfig", "LeaseLost", "Outcome")


class LeaseLost(Exception):
    """The per-key lock expired or changed owner before the outcome was applied."""


class KeyedQueueConfig(BaseModel):
    """One keyed queue; ``signal`` is the embedded pull queue (exactly one group)."""

    model_config = ConfigDict(extra="forbid")

    name: str
    signal: QueueConfig
    lock_ttl_ms: int = Field(default=60_000, gt=0)
    call_timeout_ms: int = Field(default=30_000, gt=0)
    max_attempts: int = Field(default=0, ge=0)          # 0 = unlimited (Q2)
    max_age_ms: Optional[int] = Field(default=None, gt=0)
    retry_base_ms: int = Field(default=30_000, gt=0)
    retry_max_ms: int = Field(default=900_000, gt=0)
    retry_factor: float = Field(default=2.0, ge=1.0)
    promote_batch: int = Field(default=100, gt=0)
    pause_default_ttl_ms: int = Field(default=60_000, gt=0)
    pause_poll_ms: int = Field(default=1_000, gt=0)
    recover_interval_ms: int = Field(default=60_000, gt=0)
    wait_safety_ms: Optional[int] = Field(default=None, gt=0)
    prefix: str = "evq:keyed:"

    @field_validator("signal")
    @classmethod
    def _single_group(cls, value: QueueConfig) -> QueueConfig:
        """Raises ValueError when the signal queue declares more than one group."""
        # FILL IN: len(value.groups) != 1 → ValueError naming the queue — bounded by spec §3 M1

    @model_validator(mode="after")
    def _check_timings(self) -> "KeyedQueueConfig":
        """Signal lease must outlive the lock (signal rule 2); retry base ≤ max."""
        # FILL IN: signal.default_visibility_timeout_ms <= lock_ttl_ms → ValueError; retry_base_ms > retry_max_ms → ValueError
        return self

    @property
    def effective_wait_safety_ms(self) -> int:
        """Deadline applied to a ``wait`` without ``until_ms`` (spec §8 Q3)."""
        return self.wait_safety_ms or self.lock_ttl_ms * 10


class KeyedMessage(BaseModel):
    """A message as stored in ``msg:<id>``."""

    model_config = ConfigDict(extra="forbid")

    message_id: str
    key: str
    body: dict[str, Any]
    order: float
    created_ms: int
    attempts: int = 0
    state: Literal["pending", "leased", "waiting", "held"] = "pending"
    last_error: Optional[str] = None
    wait_until_ms: Optional[int] = None
    due_ms: Optional[int] = None
    gen: int = 0


class Outcome(BaseModel):
    """What a handler returns; applied by ``KeyedQueue.apply``."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["ack", "retry", "wait", "hold"]
    delay_ms: Optional[int] = Field(default=None, ge=0)
    until_ms: Optional[int] = None
    reason: Optional[str] = None

    @classmethod
    def ack(cls) -> "Outcome":
        return cls(kind="ack")

    # FILL IN: retry(delay_ms=None, reason=None), wait(until_ms=None, reason=None), hold(reason) — same pattern;
    #          hold requires a non-empty reason (ValueError otherwise) — bounded by spec §2 Outcome


class DrainReport(BaseModel):
    """Counters returned by ``drain``."""

    model_config = ConfigDict(extra="forbid")

    keys_seen: int = 0
    acked: int = 0
    retried: int = 0
    waited: int = 0
    held: int = 0
    busy: int = 0
    expired: int = 0
    stopped_early: bool = False
    elapsed_ms: int = 0
```

```python
# src/navigator_eventbus/queues/keyed/__init__.py  (CREATE)
"""Keyed queues (FEAT-713): per-key FIFO, one in flight per key, head-blocking outcomes."""
from navigator_eventbus.queues.keyed.config import (
    DrainReport,
    KeyedMessage,
    KeyedQueueConfig,
    LeaseLost,
    Outcome,
)

__all__ = ("DrainReport", "KeyedMessage", "KeyedQueueConfig", "LeaseLost", "Outcome")
```

### FILL IN checklist
- [ ] `_single_group` rejects 0 and 2+ groups.
- [ ] `_check_timings` rejects visibility ≤ lock TTL and base > max.
- [ ] `Outcome.retry/wait/hold` constructors; `hold("")` rejected.
- [ ] Tests: `test_visibility_must_exceed_lock_ttl` (spec §4), single-group, retry bounds, `effective_wait_safety_ms` default and override, `extra="forbid"` rejects an unknown field, each `Outcome` constructor.

---

## Acceptance Criteria

- [ ] All models import from `navigator_eventbus.queues.keyed` without importing `redis`.
- [ ] Every validator has a rejecting test; `test_visibility_must_exceed_lock_ttl` passes.
- [ ] `ruff check` clean on the new files.

## Validation Commands
- `pytest tests/queues/keyed/test_config.py -q`
- `pytest tests/queues/test_config_models.py -q`

---

## Agent Instructions

Implement exactly the files above. Do not touch `navigator_eventbus/queues/__init__.py`.

## Completion Note

*(filled by the implementing agent)*
