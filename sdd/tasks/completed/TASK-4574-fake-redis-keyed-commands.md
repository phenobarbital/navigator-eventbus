# TASK-4574: Extend FakeStreamsRedis for keyed queues

**Feature**: FEAT-713 — KeyedQueue (key-ordered, head-blocking work queue)
**Spec**: `sdd/specs/keyed-queue.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Spec §3 Module 5 and §4. Every `KeyedQueue` unit test runs against the in-repo fake
(`tests/queues/_fake_streams.py`), never a `Mock`. The fake today has only stream, group and
`xclaim` commands. `KeyedQueue` additionally needs strings with TTL, hashes, sets, sorted
sets, `WATCH`/`MULTI`/`EXEC` pipelines (v0.2 fenced `apply` and atomic `push`) and what
`redis.asyncio.lock.Lock` calls: `set(nx, px)`, `get`, `register_script` (release/extend Lua),
and `connection_pool.get_encoder()`.

---

## Scope

- Add to `FakeStreamsRedis` (keep every existing method and behaviour unchanged):
  - strings with expiry driven by `self.clock`: `set(name, value, nx=False, xx=False, px=None, ex=None)`, `get`, `delete(*names)`, `exists(*names)`, `pexpire`, `pttl`;
  - hashes: `hset(name, key=None, value=None, mapping=None)`, `hsetnx`, `hget`, `hgetall`, `hincrby`, `hdel`, `hlen`;
  - sets: `sadd`, `srem`, `smembers`, `scard`, `sismember`;
  - sorted sets: `zadd(name, mapping, nx=False)`, `zrange(name, start, end, withscores=False)`, `zrangebyscore(name, min, max, start=None, num=None, withscores=False)`, `zrem`, `zcard`, `zscore`;
  - `pipeline(transaction=True)` returning a fake pipeline: `watch(*keys)` (immediate mode: commands awaited directly until `multi()`), `multi()`, buffered commands, `execute()` raising `redis.exceptions.WatchError` when a watched key was written after `watch`, `reset()`, async context manager;
  - `register_script(script)` → a script object whose `__call__(keys, args, client=None)` emulates redis-py's `Lock.LUA_RELEASE_SCRIPT` and `Lock.LUA_EXTEND_SCRIPT` when `client` is a `FakeStreamsRedis`, and otherwise delegates to `client.eval(script, len(keys), *keys, *args)` (redis-py caches scripts on the `Lock` **class**, so a script registered by the fake can later be called with a real client in the same process);
  - `connection_pool.get_encoder()` returning an object whose `encode(value)` returns `bytes`.
- Every write bumps a per-key version so `WATCH` can detect it; every write goes through `_record`.
- Tests for the new surface in `tests/queues/test_fake_redis_keyed.py`, including driving a real `redis.asyncio.lock.Lock` against the fake (acquire non-blocking, second acquire fails, `owned`, `extend`, `release`, expiry by `clock.advance`).

**NOT in scope**: any `KeyedQueue` code. Lua emulation beyond the two lock scripts used.

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `tests/queues/_fake_streams.py` | MODIFY | new command families, pipeline, script shim, encoder |
| `tests/queues/test_fake_redis_keyed.py` | CREATE | tests of the fake itself |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from tests.queues._fake_streams import FakeClock, FakeStreamsRedis   # verified: _fake_streams.py:28, :64 (tests import it relatively: `from .._fake_streams import ...`)
from redis.asyncio.lock import Lock          # redis 5.2.1 in .venv
from redis.exceptions import WatchError      # redis 5.2.1
```

### Existing Signatures to Use
```python
# tests/queues/_fake_streams.py
class FakeClock:                      # :28 — now (seconds), __call__, advance(seconds), advance_ms(ms)
class FakeStreamsRedis:               # :64
    def __init__(self, clock: Optional[FakeClock] = None)   # :67
    self.connection_pool = type("_Pool", (), {"connection_kwargs": {"decode_responses": True}})()  # :74 — add get_encoder here
    def _record(self, name, *args, **kwargs)                # :80
    async def xadd / xreadgroup / xack / xpending_range / xautoclaim / xclaim / ...  # :94-308, unchanged

# .venv/.../redis/asyncio/lock.py (5.2.1)
class Lock:
    def register_scripts(self)  # :149 — cls.lua_release = client.register_script(cls.LUA_RELEASE_SCRIPT), same for extend/reacquire
    async def do_acquire(self, token)   # :216 — await self.redis.set(self.name, token, nx=True, px=timeout)
    async def owned(self)               # :232 — await self.redis.get(name); encodes via connection_pool.get_encoder()
    async def do_release(self, expected_token)  # :256 — await self.lua_release(keys=[name], args=[expected_token], client=self.redis)
    async def do_extend(self, additional_time, replace_ttl)  # :283 — lua_extend(keys=[name], args=[token, additional_time, "1"|"0"], client=self.redis)
```

### Does NOT Exist
- ~~`FakeStreamsRedis.zadd` / `hset` / `set` / `eval` / `pipeline`~~ — added here.
- ~~a general Lua interpreter~~ — only the two lock scripts are emulated (match by `script == Lock.LUA_RELEASE_SCRIPT` / `Lock.LUA_EXTEND_SCRIPT`); any other script raises `NotImplementedError` with the first line of the script.
- ~~`conftest.py` under `tests/queues/`~~ — none; fixtures live in each test module.

---

## Complexity Contract

```json
{
  "schema_version": 1,
  "targets": [
    {"path": "tests/queues/_fake_streams.py", "action": "MODIFY"},
    {"path": "tests/queues/test_fake_redis_keyed.py", "action": "CREATE"}
  ],
  "contract_symbols": [
    "sym:tests/queues/_fake_streams.py#FakeStreamsRedis",
    "sym:tests/queues/_fake_streams.py#FakeClock"
  ]
}
```

---

## Implementation Notes

- Tokens: `Lock` passes `bytes` tokens; with `decode_responses=True` the fake stores and returns `str`.
  Normalise in `set`/script comparisons (`value.decode() if isinstance(value, bytes) else value`).
- Expiry is lazy: every read of a key first drops it when `clock.now*1000 >= expires_at_ms`.
- `zrangebyscore` accepts `"-inf"`, `"+inf"` and `"(x"` exclusive bounds; scores are floats; ties ordered by member (Redis semantics).
- Pipeline in `WATCH` mode: before `multi()` each call is awaited immediately against the fake; after `multi()` calls are buffered and `execute()` runs them in order and returns the list of results.
- The test lives in `tests/queues/` (not `tests/queues/keyed/`, which TASK-4573 creates) so this task has no dependency.

---

## Implementation Blueprint

### Steps (in order)
1. Add storage dicts and a `_versions: dict[str, int]` + `_bump(key)` / `_expired(key)` helpers to `FakeStreamsRedis.__init__` — *why*: WATCH and TTL need one place that knows every write.
2. Add the string, hash, set and zset families — *why*: `KeyedQueue` uses all four (spec §2 key table).
3. Add `_FakePipeline` and `pipeline()` — *why*: fenced `apply` and atomic `push` are `WATCH`/`MULTI`/`EXEC`.
4. Add `_FakeScript`, `register_script`, `eval` (delegating only), and `get_encoder` on the pool — *why*: `redis.asyncio.lock.Lock` must run unmodified against the fake.
5. Write `test_fake_redis.py` — *why*: a wrong fake would make every later test lie (spec §4 relies on it).

```python
# tests/queues/_fake_streams.py  (MODIFY)
# AFTER — insert inside FakeStreamsRedis.__init__, below `self._seq = 0` (verified: _fake_streams.py:73)
# occurrences: 1 (verified: grep -c 'self._seq = 0' tests/queues/_fake_streams.py)
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.sets: dict[str, set[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self._expires_ms: dict[str, float] = {}
        self._versions: dict[str, int] = {}
        # FILL IN: give self.connection_pool a get_encoder() returning an object with encode(value) -> bytes

# NEW methods on FakeStreamsRedis (place after the stream ops, before aclose):
    def _bump(self, key: str) -> None:
        self._versions[key] = self._versions.get(key, 0) + 1

    def _expire_if_due(self, key: str) -> None:
        # FILL IN: drop key from strings/hashes/sets/zsets and _expires_ms when due; bump on drop

    async def set(self, name, value, nx=False, xx=False, px=None, ex=None):
        # FILL IN: Redis SET semantics; returns True or None (redis-py returns None when NX/XX fails)

    # FILL IN: get, delete, exists, pexpire, pttl, hset, hsetnx, hget, hgetall, hincrby, hdel, hlen,
    #          sadd, srem, smembers, scard, sismember, zadd, zrange, zrangebyscore, zrem, zcard, zscore
    #          — each records via _record, writes call _bump

    def pipeline(self, transaction: bool = True) -> "_FakePipeline":
        return _FakePipeline(self)

    def register_script(self, script: str) -> "_FakeScript":
        return _FakeScript(script)


class _FakePipeline:
    """WATCH/MULTI/EXEC over FakeStreamsRedis (immediate mode until multi())."""

    def __init__(self, redis: FakeStreamsRedis) -> None:
        self._redis = redis
        self._watched: dict[str, int] = {}
        self._queue: list[tuple[str, tuple, dict]] = []
        self._buffering = False

    async def __aenter__(self) -> "_FakePipeline":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.reset()

    async def watch(self, *names: str) -> None:
        # FILL IN: record current version of each name
    def multi(self) -> None:
        self._buffering = True

    def __getattr__(self, name: str):
        # FILL IN: immediate mode → return the fake's coroutine function; buffering → return a recorder that appends (name, args, kwargs) and returns self
    async def execute(self) -> list:
        # FILL IN: any watched version changed → raise WatchError and clear; else run queued commands in order, return results
    async def reset(self) -> None:
        self._watched.clear(); self._queue.clear(); self._buffering = False


class _FakeScript:
    """Emulates the two redis-py Lock scripts; delegates to a real client otherwise."""

    def __init__(self, script: str) -> None:
        self.script = script

    async def __call__(self, keys=(), args=(), client=None):
        # FILL IN: client is a FakeStreamsRedis → emulate Lock.LUA_RELEASE_SCRIPT (token match → delete, return 1 else 0)
        #          and Lock.LUA_EXTEND_SCRIPT (token match → new TTL = (pttl + additional) or additional if replace_ttl=="1", return 1 else 0);
        #          any other script on the fake → NotImplementedError; a non-fake client → await client.eval(self.script, len(keys), *keys, *args)
```

### FILL IN checklist
- [ ] TTL expiry through `clock.advance`, including keys set with `px`.
- [ ] `set(nx=True)` returns `None` when the key exists (redis-py contract) — `Lock.do_acquire` relies on falsy.
- [ ] `WatchError` raised when a watched key is written between `watch` and `execute`, by any command family.
- [ ] Lock round trip test: acquire, second Lock same name fails, owned, extend, release, expiry after `clock.advance_ms(ttl)`.
- [ ] Existing `tests/queues/*` still green (no behaviour change for stream commands).

---

## Acceptance Criteria

- [ ] `redis.asyncio.lock.Lock` works unmodified against the fake.
- [ ] `tests/queues/test_store.py` and `tests/queues/test_api.py` unchanged and green.
- [ ] New tests cover every added command family and the pipeline's `WatchError`.

## Validation Commands
- `pytest tests/queues/test_fake_redis_keyed.py -q`
- `pytest tests/queues/test_store.py -q`
- `pytest tests/queues/test_api.py -q`

---

## Agent Instructions

Touch only `tests/queues/_fake_streams.py` and the new test file. Never mock `Lock`; drive the real class.

## Completion Note

Implemented in tests/queues/_fake_streams.py + tests/queues/test_fake_redis_keyed.py. Additions beyond the blueprint (same files): `FakeStreamsRedis.lock()` (returns a real redis `Lock` bound to the fake, mirrors `Redis.lock`), emulation of `LUA_REACQUIRE_SCRIPT` too, expiry of a watched key does NOT abort EXEC (matches Redis >= 6.0.9), container keys vanish when emptied. Test file name per Files table (`test_fake_redis_keyed.py`). `tests/queues`: 202 passed.
