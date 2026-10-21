# TASK-4558: EventBus facade — backend= / transport= / publish_only= / outbox=

**Feature**: FEAT-711 — Durable lifecycle fan-out
**Spec**: `sdd/specs/durable-lifecycle-fanout.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-4555, TASK-4557
**Assigned-to**: unassigned

---

## Context

Spec §1 problem 3 / Goal G2 / Module 5. `EventBus.__init__` can only build `RedisPubSubBackend`
(when `use_redis`) or `MemoryBackend` (`evb.py:187-191`), so a consumer of the facade cannot select
`RedisStreamsBackend`, inject a custom backend, run publish-only, or enable the outbox. This task
exposes everything TASK-4555..4557 built through the facade and navconfig.

---

## Scope

- Add keyword-only kwargs to `EventBus.__init__`: `backend: Optional[TransportBackend] = None`, `transport: Literal["memory", "pubsub", "streams"] | None = None`, `publish_only: bool = False`, `outbox: Optional[OutboxStore | str | os.PathLike] = None`.
- Backend precedence: `backend=` > `transport=` > `use_redis`. `transport` falls back to navconfig `BUS_TRANSPORT` when not passed.
  - `"memory"` → `MemoryBackend()`; `"pubsub"` → current `RedisPubSubBackend(redis_url, channel_prefix=...)`; `"streams"` → `RedisStreamsBackend(redis_url)` (it reads `BUS_STREAM_PREFIX` / `BUS_DEDUP_PREFIX` / `BUS_GROUP` from navconfig itself, `redis_streams.py:256`).
  - `"pubsub"`/`"streams"` without `redis_url` → `ValueError`; an unknown transport → `ValueError`.
- `outbox=`: a `str`/`PathLike` builds `SqliteOutbox(path, max_rows=BUS_OUTBOX_MAX_ROWS, overflow=BUS_OUTBOX_OVERFLOW)`; an `OutboxStore` instance is used as-is; `None` falls back to `BUS_OUTBOX_PATH` when set.
- `publish_only`: `True` if the kwarg is `True` or navconfig `BUS_PUBLISH_ONLY` is truthy.
- Add `relay_interval` (`BUS_OUTBOX_RELAY_INTERVAL`, default `5.0`) to `_bus_config()`.
- Forward `outbox=` and `publish_only=` to `BusCore`. `**bus_options` keeps overriding navconfig.
- Keep `self.use_redis` / `start_redis_listener()` behavior as is.
- Unit tests in a NEW file `tests/test_evb_transport.py`.

**NOT in scope**: per-PID outbox file naming / orphan adoption (spec Open Question Q1 — a path string is used verbatim); README config table (TASK-4560); real-Redis tests (TASK-4559).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/evb.py` | MODIFY | new kwargs, transport selection, config keys |
| `tests/test_evb_transport.py` | CREATE | unit tests |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.evb import EventBus                          # verified: evb.py:138
from navigator_eventbus.backends.memory import MemoryBackend          # verified: backends/memory.py:15
from navigator_eventbus.backends.redis_pubsub import RedisPubSubBackend  # verified: imported lazily in evb.py __init__
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend  # verified: backends/redis_streams.py:112 — needs the [redis] extra
from navigator_eventbus.backends.base import TransportBackend         # verified: backends/base.py:28
from navigator_eventbus.outbox import OutboxStore, SqliteOutbox       # created by TASK-4555
```
`evb.py` top-level imports (lines 31-40): `asyncio`, `uuid`, `deque`, `dataclass, field`, `datetime, timezone`,
`Enum`, `TYPE_CHECKING, Any, Callable, Dict, Optional`, `nav_config`, `logging`.
**Circular-import rule (evb.py:178-180):** `envelope`/`converters` import `evb`, and `outbox` imports
`envelope` — so `MemoryBackend`, `RedisPubSubBackend`, `RedisStreamsBackend`, `BusCore`, `SqliteOutbox`
and `OutboxStore` MUST be imported lazily inside `__init__` (or under `TYPE_CHECKING` for annotations).

### Existing Signatures to Use
```python
# src/navigator_eventbus/evb.py
def _bus_config() -> Dict[str, Any]:                                  # :114 — returns BusCore kwargs from BUS_* keys
    def _get(key: str, fallback: Any, cast: Callable[[Any], Any]) -> Any  # :120
    return {"workers": ..., ..., "drain_timeout": _get("BUS_DRAIN_TIMEOUT", 5.0, float)}  # last entry

class EventBus:                                                       # :138
    def __init__(self, redis_url: Optional[str] = None, use_redis: bool = False, *,
                 channel_prefix: Optional[str] = None, **bus_options: Any):   # :164-170
        self.use_redis = use_redis and redis_url is not None
        self.redis_url = redis_url
        self.channel_prefix = channel_prefix or nav_config.get("BUS_CHANNEL_PREFIX", ...)
        backend = (
            RedisPubSubBackend(redis_url, channel_prefix=self.channel_prefix)   # :188 (anchor, count 1)
            if self.use_redis
            else MemoryBackend()
        )
        core_opts = _bus_config()
        core_opts.update(bus_options)
        self._core: "BusCore" = BusCore(backend=backend, **core_opts)
    @property
    def core(self) -> "BusCore"                                       # :205
    async def emit(self, event_type: str, payload: dict, **kwargs) -> int   # :349

# src/navigator_eventbus/backends/redis_streams.py:205
RedisStreamsBackend.__init__(self, redis_url: Optional[str] = None, *, client=None, ..., delivery="group", ...)
# BusCore (after TASK-4556/4557): outbox=, relay_interval=, relay_batch=, relay_max_backoff=, publish_only=
```

### Does NOT Exist
- ~~`EventBus(backend=...)`~~, ~~`EventBus(transport=...)`~~, ~~`EventBus(publish_only=...)`~~, ~~`EventBus(outbox=...)`~~ — added by this task.
- ~~`EventBus.backend`~~ property — reach the backend via `bus.core._backend` in tests only; do not add new public accessors.
- ~~`navigator_eventbus.backends.get_backend`~~ / a transport factory registry — build backends inline.
- ~~`BUS_STREAM_URL`~~ — the Redis URL is the `redis_url` argument.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "src/navigator_eventbus/evb.py", "action": "MODIFY"},
    {"path": "tests/test_evb_transport.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:src/navigator_eventbus/evb.py#EventBus",
    "sym:src/navigator_eventbus/evb.py#_bus_config",
    "sym:src/navigator_eventbus/backends/redis_streams.py#RedisStreamsBackend",
    "sym:src/navigator_eventbus/backends/memory.py#MemoryBackend"
  ]
}
```

---

## Implementation Notes

### Key Constraints
- Default construction (`EventBus()`, `EventBus(url, use_redis=True)`) must produce exactly the same backend as today — `tests/test_evb.py` must pass unchanged.
- `RedisStreamsBackend` imports `redis` at module level; import it only inside the `"streams"` branch so a memory-only install keeps working.
- Validate `BUS_OUTBOX_OVERFLOW` against `("reject_new", "drop_oldest")` via `SqliteOutbox` (it raises `ValueError`).
- Explicit kwargs beat navconfig; `bus_options` beat `_bus_config()`.

---

## Implementation Blueprint

### Steps (in order)
1. Add `relay_interval` to `_bus_config()` — *why*: BusCore accepts it after TASK-4556, so it can flow through `core_opts`.
2. Add the kwargs to `EventBus.__init__` and document them — *why*: spec §2 fixes the signature.
3. Replace the backend-selection expression with `_select_backend()` — *why*: keeps the precedence logic testable.
4. Resolve outbox + publish_only and pass them to `BusCore` — *why*: spec M5.
5. Write tests (monkeypatch `nav_config.get` for config cases).

### `src/navigator_eventbus/evb.py` (MODIFY) — `_bus_config`
```python
# occurrences: 1 (verified: grep -c '"drain_timeout": _get("BUS_DRAIN_TIMEOUT", 5.0, float),' src/navigator_eventbus/evb.py)
# AFTER — insert below `        "drain_timeout": _get("BUS_DRAIN_TIMEOUT", 5.0, float),`
        "relay_interval": _get("BUS_OUTBOX_RELAY_INTERVAL", 5.0, float),
```

### `src/navigator_eventbus/evb.py` (MODIFY) — `__init__` signature
```python
# occurrences: 1 (verify: grep -c '        channel_prefix: Optional\[str\] = None,' src/navigator_eventbus/evb.py)
# AFTER — insert below `        channel_prefix: Optional[str] = None,` (in EventBus.__init__, evb.py:169)
        backend: "Optional[TransportBackend]" = None,
        transport: "Optional[Literal['memory', 'pubsub', 'streams']]" = None,
        publish_only: bool = False,
        outbox: "Optional[OutboxStore | str | os.PathLike]" = None,
# Top of module: add `import os`, add `Literal` to the typing import, and under TYPE_CHECKING:
#   from navigator_eventbus.backends.base import TransportBackend
#   from navigator_eventbus.outbox import OutboxStore
# evb.py already has an `if TYPE_CHECKING:` block at :42 — extend it, do not add a second one
```

### `src/navigator_eventbus/evb.py` (MODIFY) — backend selection + core construction
```python
# occurrences: 1 (verified: grep -c '            RedisPubSubBackend(redis_url, channel_prefix=self.channel_prefix)' src/navigator_eventbus/evb.py) → :188
# Replace the whole `backend = ( ... )` expression (evb.py:187-191) and the BusCore(...) line with:
        backend = self._select_backend(backend, transport)
        core_opts = _bus_config()
        core_opts.update(bus_options)
        core_opts.setdefault("outbox", self._resolve_outbox(outbox))
        core_opts.setdefault(
            "publish_only",
            publish_only or _truthy(nav_config.get("BUS_PUBLISH_ONLY", fallback=False)),
        )
        self._core: "BusCore" = BusCore(backend=backend, **core_opts)
```
Note: `core_opts.update(bus_options)` already exists — keep a single update call.

### `src/navigator_eventbus/evb.py` (MODIFY) — helpers (methods on EventBus + one module function)
```python
def _truthy(value: Any) -> bool:
    """Interpret a navconfig value as a boolean (``"1"``, ``"true"``, ``"yes"``, ``True``)."""
    # FILL IN: str(value).strip().lower() in {"1", "true", "yes", "on"}
    ...

    def _select_backend(
        self, backend: "Optional[TransportBackend]", transport: Optional[str]
    ) -> "TransportBackend":
        """Pick the transport: ``backend=`` > ``transport=`` (or ``BUS_TRANSPORT``) > ``use_redis``.

        Raises:
            ValueError: Unknown transport, or a Redis transport without ``redis_url``.
        """
        from navigator_eventbus.backends.memory import MemoryBackend
        from navigator_eventbus.backends.redis_pubsub import RedisPubSubBackend

        if backend is not None:
            return backend
        transport = transport or nav_config.get("BUS_TRANSPORT", fallback=None)
        if transport is None:
            return (
                RedisPubSubBackend(self.redis_url, channel_prefix=self.channel_prefix)
                if self.use_redis
                else MemoryBackend()
            )
        # FILL IN: "memory" → MemoryBackend(); "pubsub"/"streams" require self.redis_url (ValueError otherwise);
        #          "pubsub" → RedisPubSubBackend(...) as above; "streams" → lazy import RedisStreamsBackend(self.redis_url);
        #          anything else → ValueError listing the valid names
        ...

    def _resolve_outbox(self, outbox: "Optional[OutboxStore | str | os.PathLike]") -> "Optional[OutboxStore]":
        """Build a ``SqliteOutbox`` from a path (kwarg or ``BUS_OUTBOX_PATH``); pass a store through."""
        from navigator_eventbus.outbox import SqliteOutbox

        # FILL IN: None → nav_config.get("BUS_OUTBOX_PATH", fallback=None); still None → return None;
        #          str/PathLike → SqliteOutbox(path, max_rows=int(BUS_OUTBOX_MAX_ROWS or 1_000_000),
        #                                       overflow=BUS_OUTBOX_OVERFLOW or "reject_new");
        #          otherwise return the given store unchanged
        ...
```
**Why**: `self.redis_url`, `self.use_redis` and `self.channel_prefix` are set before backend selection (evb.py:182-186), so the helpers can read them; setting `use_redis=True` with `transport="streams"` is resolved by "transport wins".

### FILL IN checklist
- [ ] (none for imports — extend the TYPE_CHECKING block at evb.py:42)
- [ ] `_truthy` body.
- [ ] `_select_backend` transport branches; bounded by spec M5 precedence.
- [ ] `_resolve_outbox` body.
- [ ] tests below.

---

## Acceptance Criteria

- [ ] `EventBus(redis_url, transport="streams").core._backend` is a `RedisStreamsBackend` (spec AC-3; construction only — no Redis needed).
- [ ] Precedence: `backend=` > `transport=` > `use_redis` (`test_eventbus_transport_precedence`).
- [ ] `EventBus(outbox=str(tmp_path / "o.db"))` gives a `BusCore` with a `SqliteOutbox`.
- [ ] `EventBus(publish_only=True)` → `BusCore` never starts the backend consumer.
- [ ] `BUS_TRANSPORT`, `BUS_PUBLISH_ONLY`, `BUS_OUTBOX_PATH`, `BUS_OUTBOX_MAX_ROWS`, `BUS_OUTBOX_OVERFLOW`, `BUS_OUTBOX_RELAY_INTERVAL` are honored.
- [ ] `tests/test_evb.py` passes unchanged.

---

## Validation Commands

- `pytest tests/test_evb_transport.py -q`
- `pytest tests/test_evb.py -q`
- `pytest tests/test_integration.py -q`

---

## Test Specification

```python
# tests/test_evb_transport.py
import pytest

from navigator_eventbus import EventBus, SqliteOutbox
from navigator_eventbus.backends.memory import MemoryBackend


def test_eventbus_transport_precedence(): ...          # backend= beats transport= beats use_redis
def test_transport_streams_builds_redis_streams_backend(): ...   # pytest.importorskip("redis")
def test_transport_requires_redis_url(): ...
def test_unknown_transport_raises(): ...
def test_outbox_path_builds_sqlite_outbox(tmp_path): ...
def test_outbox_store_passed_through(tmp_path): ...
async def test_publish_only_forwarded(): ...
def test_navconfig_keys_honored(monkeypatch, tmp_path): ...   # FILL IN: monkeypatch navigator_eventbus.evb.nav_config.get
def test_default_construction_unchanged(): ...
```

---

## Agent Instructions

1. **Work in the feature worktree** (`python -m scripts.sdd.ensure_worktree --slug durable-lifecycle-fanout --feature-id FEAT-711`)
2. **Read the spec** for full context
3. **Check dependencies** — TASK-4555 and TASK-4557 must be `"done"`
4. **Verify the Codebase Contract** before writing code
5. **Update status** → `"in-progress"` and commit only the index file
6. **Implement** from the blueprint; complete every `# FILL IN:` marker
7. **Verify** — run the Validation Commands
8. **Commit the code** — only the listed files
9. **Close the task** with `scripts/sdd/close_task.sh TASK-4558 durable-lifecycle-fanout verified`
10. **Fill in the Completion Note**, then commit the staged SDD state

---

## Completion Note

Implemented by gpt-5.6-terra (codex, 1 attempt); merged by sdd-worker.

**Completed by**: gpt-5.6-terra (codex, 1 attempt)
**Date**: 2026-10-20
**Notes**: Validated manually with PYTHONPATH=src (the validation tool imports the primary checkout's editable src): test_evb_transport, test_evb, test_integration -> 25 passed.

**Deviations from spec**: none | describe if any
