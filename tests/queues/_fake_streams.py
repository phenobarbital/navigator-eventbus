"""A hand-rolled Redis Streams double for the queue tests.

The repo has no fakeredis; ``tests/test_backends_streams.py`` hand-rolls its
own fake and this follows that precedent. It is deliberately **separate**
rather than imported from there: that module carries an ``autouse`` fixture
that would not travel, and cross-test-module imports are fragile. A test
double is cheaper duplicated than coupled.

Two things this one adds over the backend's fake, both required here:

- ``xclaim`` with ``idle=``/``justid=``, modelled the way Redis actually does
  it — ``IDLE`` sets the *absolute* idle time by back-dating the delivery
  instant, and ``JUSTID`` leaves ``times_delivered`` untouched. (Both verified
  against a real Redis 7.0 before this was written.)
- an **injectable clock**, so visibility-timeout tests advance time instead of
  sleeping. The backend's fake relies on real sleeps, which is exactly what
  makes lease semantics untestable at unit tier.
"""
from __future__ import annotations

import asyncio
import functools
from typing import Any, Optional

from redis.exceptions import ResponseError


class FakeResponseError(Exception):
    """Stands in for ``redis.exceptions.ResponseError``."""


class FakeClock:
    """A manually advanced clock, in seconds."""

    def __init__(self, now: float = 1_700_000_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        """Move time forward."""
        self.now += seconds

    def advance_ms(self, milliseconds: float) -> None:
        """Move time forward by milliseconds."""
        self.now += milliseconds / 1000.0


class _Encoder:
    """Stands in for ``redis.connection.Encoder`` (what ``Lock`` needs)."""

    def encode(self, value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        return str(value).encode()


def _encode(value: Any) -> Any:
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, dict):
        return {_encode(k): _encode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_encode(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_encode(v) for v in value)
    if isinstance(value, set):
        return {_encode(v) for v in value}
    return value


def _s(value: Any) -> str:
    """Normalise a command argument to ``str`` (``decode_responses=True``)."""
    if isinstance(value, bytes):
        return value.decode()
    return value if isinstance(value, str) else str(value)


def _bound(raw: Any, default: float) -> tuple[float, bool]:
    """Parse a ZRANGEBYSCORE bound -> (value, exclusive)."""
    if raw is None:
        return default, False
    text = _s(raw)
    exclusive = text.startswith("(")
    if exclusive:
        text = text[1:]
    if text in ("-inf", "+inf", "inf"):
        return (float("-inf") if text == "-inf" else float("inf")), exclusive
    return float(text), exclusive


class _PendingEntry:
    __slots__ = ("consumer", "delivered_at", "times_delivered")

    def __init__(self, consumer: str, delivered_at: float) -> None:
        self.consumer = consumer
        self.delivered_at = delivered_at
        self.times_delivered = 1


def _id_key(message_id: str) -> tuple[int, int]:
    """Sort key for a Redis entry id — numeric, not lexical."""
    left, _, right = message_id.partition("-")
    try:
        return (int(left), int(right or 0))
    except ValueError:
        return (0, 0)


class FakeStreamsRedis:
    """Minimal Redis Streams semantics, enough to drive ``QueueStore``."""

    #: Commands whose replies carry key/member/value text (the stream commands
    #: are left as ``str`` in bytes mode: ``QueueStore`` is not under test there).
    _BYTES_REPLY = frozenset(
        {"get", "hget", "hgetall", "smembers", "zrange", "zrangebyscore", "sscan"}
    )

    def __init__(
        self, clock: Optional[FakeClock] = None, decode_responses: bool = True
    ) -> None:
        self.decode_responses = decode_responses
        self.clock = clock or FakeClock()
        self.streams: dict[str, list[tuple[str, dict]]] = {}
        self.groups: dict[tuple[str, str], dict[str, Any]] = {}
        self.acked: list[tuple[str, str, str]] = []
        self.calls: list[tuple[str, tuple, dict]] = []
        self._seq = 0
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.sets: dict[str, set[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self._expires_ms: dict[str, float] = {}
        self._versions: dict[str, int] = {}
        self.connection_pool = type(
            "_Pool",
            (),
            {
                "connection_kwargs": {"decode_responses": decode_responses},
                "get_encoder": staticmethod(lambda: _Encoder()),
            },
        )()

    # -- helpers -------------------------------------------------------

    def _out(self, command: str, value: Any) -> Any:
        """Encode a reply as ``bytes`` when emulating ``decode_responses=False``."""
        if self.decode_responses or command not in self._BYTES_REPLY:
            return value
        return _encode(value)

    def _record(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append((name, args, kwargs))

    def _idle_ms(self, entry: _PendingEntry) -> float:
        return (self.clock.now - entry.delivered_at) * 1000.0

    def _group(self, stream: str, group: str) -> dict[str, Any]:
        key = (stream, group)
        if key not in self.groups:
            raise FakeResponseError("NOGROUP No such consumer group")
        return self.groups[key]

    # -- stream ops ----------------------------------------------------

    async def xadd(self, name, fields, maxlen=None, approximate=True):
        self._record("xadd", name, maxlen=maxlen)
        self._check(name, "stream")
        self._seq += 1
        message_id = f"{self._seq}-0"
        entries = self.streams.setdefault(name, [])
        entries.append((message_id, dict(fields)))
        if maxlen is not None and maxlen >= 0 and len(entries) > maxlen:
            del entries[: len(entries) - maxlen]
        return message_id

    async def xlen(self, name):
        self._check(name, "stream")
        return len(self.streams.get(name, []))

    async def xdel(self, name, *ids):
        self._record("xdel", name, ids=ids)
        entries = self.streams.get(name, [])
        targets = set(ids)
        self.streams[name] = [e for e in entries if e[0] not in targets]
        return len(entries) - len(self.streams[name])

    async def xtrim(self, name, maxlen=None, approximate=True, minid=None):
        self._record("xtrim", name, maxlen=maxlen, minid=minid)
        entries = self.streams.get(name, [])
        if maxlen is not None:
            if maxlen == 0:
                self.streams[name] = []
            elif len(entries) > maxlen:
                self.streams[name] = entries[len(entries) - maxlen :]
        return 0

    # -- group ops -----------------------------------------------------

    async def xgroup_create(self, name, groupname, id="0", mkstream=False):
        self._record("xgroup_create", name, groupname, id=id)
        if mkstream:
            self.streams.setdefault(name, [])
        key = (name, groupname)
        if key in self.groups:
            raise FakeResponseError("BUSYGROUP Consumer Group name already exists")
        if id == "$":
            entries = self.streams.get(name, [])
            last = entries[-1][0] if entries else "0-0"
        else:
            last = id
        self.groups[key] = {"last_id": last, "pending": {}, "consumers": {}}
        return True

    async def xgroup_destroy(self, name, groupname):
        self._record("xgroup_destroy", name, groupname)
        return 1 if self.groups.pop((name, groupname), None) is not None else 0

    async def xgroup_delconsumer(self, name, groupname, consumername):
        state = self._group(name, groupname)
        state["consumers"].pop(consumername, None)
        return 0

    async def xinfo_groups(self, name):
        out = []
        entries = self.streams.get(name, [])
        for (stream, group), state in self.groups.items():
            if stream != name:
                continue
            unread = [e for e in entries if _id_key(e[0]) > _id_key(state["last_id"])]
            out.append(
                {
                    "name": group,
                    "consumers": len(state["consumers"]),
                    "pending": len(state["pending"]),
                    "last-delivered-id": state["last_id"],
                    "lag": len(unread),
                }
            )
        return out

    async def xinfo_consumers(self, name, groupname):
        state = self._group(name, groupname)
        out = []
        for consumer, last_seen in state["consumers"].items():
            pending = sum(
                1 for e in state["pending"].values() if e.consumer == consumer
            )
            # Redis reports a consumer's idle time since its last *interaction*
            # with the group, independent of whether it holds pending entries.
            idle = (self.clock.now - last_seen) * 1000.0
            out.append({"name": consumer, "pending": pending, "idle": int(idle)})
        return out

    # -- consumption ---------------------------------------------------

    async def xreadgroup(self, groupname, consumername, streams, count=None, block=None):
        self._record("xreadgroup", groupname, consumername, block=block, count=count)
        result = []
        for stream, cursor in streams.items():
            state = self._group(stream, groupname)
            state["consumers"][consumername] = self.clock.now
            if cursor != ">":
                continue
            entries = self.streams.get(stream, [])
            fresh = [
                e for e in entries if _id_key(e[0]) > _id_key(state["last_id"])
            ]
            if count:
                fresh = fresh[:count]
            if not fresh:
                continue
            for message_id, _fields in fresh:
                state["pending"][message_id] = _PendingEntry(
                    consumername, self.clock.now
                )
                state["last_id"] = message_id
            result.append((stream, fresh))
        return result

    async def xack(self, name, groupname, *ids):
        state = self._group(name, groupname)
        removed = 0
        for message_id in ids:
            if state["pending"].pop(message_id, None) is not None:
                removed += 1
                self.acked.append((name, groupname, message_id))
        return removed

    async def xpending_range(
        self, name, groupname, min="-", max="+", count=10, idle=None, consumername=None
    ):
        state = self._group(name, groupname)
        out = []
        for message_id, entry in sorted(
            state["pending"].items(), key=lambda kv: _id_key(kv[0])
        ):
            if min != "-" and _id_key(message_id) < _id_key(min):
                continue
            if max != "+" and _id_key(message_id) > _id_key(max):
                continue
            entry_idle = self._idle_ms(entry)
            if idle is not None and entry_idle < idle:
                continue
            out.append(
                {
                    "message_id": message_id,
                    "consumer": entry.consumer,
                    "time_since_delivered": int(entry_idle),
                    "times_delivered": entry.times_delivered,
                }
            )
            if count and len(out) >= count:
                break
        return out

    async def xautoclaim(
        self, name, groupname, consumername, min_idle_time, start_id="0-0", count=10
    ):
        self._record("xautoclaim", name, groupname, min_idle_time=min_idle_time)
        state = self._group(name, groupname)
        state["consumers"].setdefault(consumername, True)
        fields_by_id = dict(self.streams.get(name, []))
        claimed, deleted = [], []
        for message_id, entry in sorted(
            state["pending"].items(), key=lambda kv: _id_key(kv[0])
        ):
            if _id_key(message_id) < _id_key(start_id):
                continue
            if self._idle_ms(entry) < min_idle_time:
                continue
            if message_id not in fields_by_id:
                deleted.append(message_id)
                del state["pending"][message_id]
                continue
            entry.consumer = consumername
            entry.delivered_at = self.clock.now
            entry.times_delivered += 1
            claimed.append((message_id, fields_by_id[message_id]))
            if count and len(claimed) >= count:
                break
        # Redis 7.0+ returns a 3-tuple; the third element is deleted ids.
        return ("0-0", claimed, deleted)

    async def xclaim(
        self,
        name,
        groupname,
        consumername,
        min_idle_time,
        message_ids,
        idle=None,
        justid=False,
    ):
        self._record(
            "xclaim", name, groupname, consumername, idle=idle, justid=justid
        )
        state = self._group(name, groupname)
        state["consumers"].setdefault(consumername, True)
        fields_by_id = dict(self.streams.get(name, []))
        out = []
        for message_id in message_ids:
            entry = state["pending"].get(message_id)
            if entry is None or self._idle_ms(entry) < min_idle_time:
                continue
            entry.consumer = consumername
            # IDLE sets the ABSOLUTE idle time — model it by back-dating the
            # delivery instant, which is what Redis does internally.
            entry.delivered_at = (
                self.clock.now - (idle / 1000.0) if idle is not None else self.clock.now
            )
            if not justid:
                entry.times_delivered += 1
            out.append(
                message_id if justid else (message_id, fields_by_id.get(message_id, {}))
            )
        return out

    # -- keyed-queue command families (FEAT-713 / TASK-4574) ------------

    def _bump(self, key: str) -> None:
        self._versions[key] = self._versions.get(key, 0) + 1

    def _stores(self) -> tuple[tuple[str, dict], ...]:
        return (
            ("string", self.strings),
            ("hash", self.hashes),
            ("set", self.sets),
            ("zset", self.zsets),
            ("stream", self.streams),
        )

    def _expire_if_due(self, key: str) -> None:
        """Lazy expiry. Dropping an expired key counts as a write for WATCH
        (Redis >= 6.0.9: a watched key that existed at WATCH time and expired
        aborts EXEC)."""
        due = self._expires_ms.get(key)
        if due is not None and self.clock.now * 1000.0 >= due:
            for _kind, store in self._stores():
                store.pop(key, None)
            self._expires_ms.pop(key, None)
            self._bump(key)

    def _kind(self, key: str) -> Optional[str]:
        self._expire_if_due(key)
        for kind, store in self._stores():
            if key in store:
                return kind
        return None

    def _check(self, key: str, kind: str) -> None:
        """One key name = one type, like Redis."""
        actual = self._kind(key)
        if actual not in (None, kind):
            raise ResponseError(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )

    def _exists(self, key: str) -> bool:
        return self._kind(key) is not None

    def _cleanup(self, key: str) -> None:
        """Redis drops a container key once it becomes empty."""
        for store in (self.hashes, self.sets, self.zsets):
            if key in store and not store[key]:
                del store[key]
                self._expires_ms.pop(key, None)

    def lock(self, name, timeout=None, sleep=0.1, blocking=True,
             blocking_timeout=None, lock_class=None, thread_local=True):
        """Return a real ``redis.asyncio.lock.Lock`` bound to this fake."""
        from redis.asyncio.lock import Lock

        return (lock_class or Lock)(
            self, name, timeout=timeout, sleep=sleep, blocking=blocking,
            blocking_timeout=blocking_timeout, thread_local=thread_local,
        )

    # strings

    async def set(self, name, value, nx=False, xx=False, px=None, ex=None):
        self._record("set", name, nx=nx, xx=xx, px=px, ex=ex)
        name = _s(name)
        present = self._exists(name)
        if (nx and present) or (xx and not present):
            return None
        for _kind, store in self._stores():
            store.pop(name, None)
        self.strings[name] = _s(value)
        self._expires_ms.pop(name, None)
        if px is not None:
            self._expires_ms[name] = self.clock.now * 1000.0 + int(px)
        elif ex is not None:
            self._expires_ms[name] = self.clock.now * 1000.0 + int(ex) * 1000
        self._bump(name)
        return True

    async def get(self, name):
        self._record("get", name)
        name = _s(name)
        self._check(name, "string")
        self._expire_if_due(name)
        return self.strings.get(name)

    async def delete(self, *names):
        self._record("delete", *names)
        removed = 0
        for raw in names:
            name = _s(raw)
            if self._exists(name):
                for _kind, store in self._stores():
                    store.pop(name, None)
                self._expires_ms.pop(name, None)
                self._bump(name)
                removed += 1
        return removed

    async def exists(self, *names):
        self._record("exists", *names)
        return sum(1 for n in names if self._exists(_s(n)))

    async def pexpire(self, name, milliseconds):
        self._record("pexpire", name, milliseconds)
        name = _s(name)
        if not self._exists(name):
            return False
        self._expires_ms[name] = self.clock.now * 1000.0 + int(milliseconds)
        self._bump(name)
        return True

    async def pttl(self, name):
        self._record("pttl", name)
        name = _s(name)
        if not self._exists(name):
            return -2
        due = self._expires_ms.get(name)
        if due is None:
            return -1
        return max(0, int(due - self.clock.now * 1000.0))

    # hashes

    async def hset(self, name, key=None, value=None, mapping=None):
        self._record("hset", name)
        name = _s(name)
        self._check(name, "hash")
        self._exists(name)
        data = self.hashes.setdefault(name, {})
        items = dict(mapping or {})
        if key is not None:
            items[key] = value
        added = 0
        for field, val in items.items():
            if _s(field) not in data:
                added += 1
            data[_s(field)] = _s(val)
        self._bump(name)
        return added

    async def hsetnx(self, name, key, value):
        self._record("hsetnx", name, key)
        name, key = _s(name), _s(key)
        self._check(name, "hash")
        self._exists(name)
        data = self.hashes.setdefault(name, {})
        if key in data:
            return 0
        data[key] = _s(value)
        self._bump(name)
        return 1

    async def hget(self, name, key):
        self._record("hget", name, key)
        name = _s(name)
        self._check(name, "hash")
        self._expire_if_due(name)
        return self.hashes.get(name, {}).get(_s(key))

    async def hgetall(self, name):
        self._record("hgetall", name)
        name = _s(name)
        self._check(name, "hash")
        self._expire_if_due(name)
        return dict(self.hashes.get(name, {}))

    async def hincrby(self, name, key, amount=1):
        self._record("hincrby", name, key, amount)
        name, key = _s(name), _s(key)
        self._check(name, "hash")
        self._exists(name)
        data = self.hashes.setdefault(name, {})
        data[key] = str(int(data.get(key, "0")) + int(amount))
        self._bump(name)
        return int(data[key])

    async def hdel(self, name, *keys):
        self._record("hdel", name, *keys)
        name = _s(name)
        self._check(name, "hash")
        self._expire_if_due(name)
        data = self.hashes.get(name, {})
        removed = sum(1 for k in keys if data.pop(_s(k), None) is not None)
        if removed:
            self._bump(name)
        self._cleanup(name)
        return removed

    async def hlen(self, name):
        self._record("hlen", name)
        name = _s(name)
        self._check(name, "hash")
        self._expire_if_due(name)
        return len(self.hashes.get(name, {}))

    # sets

    async def sadd(self, name, *members):
        self._record("sadd", name, *members)
        name = _s(name)
        self._check(name, "set")
        self._exists(name)
        data = self.sets.setdefault(name, set())
        before = len(data)
        data.update(_s(m) for m in members)
        self._bump(name)
        return len(data) - before

    async def srem(self, name, *members):
        self._record("srem", name, *members)
        name = _s(name)
        self._check(name, "set")
        self._expire_if_due(name)
        data = self.sets.get(name, set())
        removed = 0
        for member in members:
            if _s(member) in data:
                data.discard(_s(member))
                removed += 1
        if removed:
            self._bump(name)
        self._cleanup(name)
        return removed

    async def smembers(self, name):
        self._record("smembers", name)
        name = _s(name)
        self._check(name, "set")
        self._expire_if_due(name)
        return set(self.sets.get(name, set()))

    async def scard(self, name):
        self._record("scard", name)
        name = _s(name)
        self._check(name, "set")
        self._expire_if_due(name)
        return len(self.sets.get(name, set()))

    async def sscan(self, name, cursor=0, match=None, count=None):
        """Cursor = offset into the sorted members; 0 means the scan is complete."""
        self._record("sscan", name, cursor, count=count)
        name = _s(name)
        self._check(name, "set")
        members = sorted(self.sets.get(name, set()))
        step = int(count or 10)
        start = int(cursor)
        chunk = members[start: start + step]
        nxt = start + step
        return (0 if nxt >= len(members) else nxt, chunk)

    async def sismember(self, name, member):
        self._record("sismember", name, member)
        name = _s(name)
        self._check(name, "set")
        self._expire_if_due(name)
        return _s(member) in self.sets.get(name, set())

    # sorted sets

    def _zsorted(self, name: str) -> list[tuple[str, float]]:
        self._check(name, "zset")
        return sorted(self.zsets.get(name, {}).items(), key=lambda kv: (kv[1], kv[0]))

    async def zadd(self, name, mapping, nx=False):
        self._record("zadd", name, nx=nx)
        name = _s(name)
        self._check(name, "zset")
        self._exists(name)
        data = self.zsets.setdefault(name, {})
        added = 0
        for member, score in mapping.items():
            member = _s(member)
            if member in data:
                if nx:
                    continue
                data[member] = float(score)
            else:
                data[member] = float(score)
                added += 1
        self._bump(name)
        return added

    async def zrange(self, name, start, end, withscores=False):
        self._record("zrange", name, start, end)
        items = self._zsorted(_s(name))
        n = len(items)
        start = start + n if start < 0 else start
        end = end + n if end < 0 else end
        picked = items[max(start, 0): end + 1]
        return list(picked) if withscores else [m for m, _ in picked]

    async def zrangebyscore(self, name, min, max, start=None, num=None,
                            withscores=False):
        self._record("zrangebyscore", name, min, max)
        lo, lo_x = _bound(min, float("-inf"))
        hi, hi_x = _bound(max, float("inf"))
        picked = [
            (m, sc)
            for m, sc in self._zsorted(_s(name))
            if (sc > lo if lo_x else sc >= lo) and (sc < hi if hi_x else sc <= hi)
        ]
        if start is not None:
            picked = picked[start:] if num is None else picked[start: start + num]
        return list(picked) if withscores else [m for m, _ in picked]

    async def zrem(self, name, *members):
        self._record("zrem", name, *members)
        name = _s(name)
        self._check(name, "zset")
        self._expire_if_due(name)
        data = self.zsets.get(name, {})
        removed = sum(1 for m in members if data.pop(_s(m), None) is not None)
        if removed:
            self._bump(name)
        self._cleanup(name)
        return removed

    async def zcard(self, name):
        self._record("zcard", name)
        name = _s(name)
        self._check(name, "zset")
        self._expire_if_due(name)
        return len(self.zsets.get(name, {}))

    async def zscore(self, name, member):
        self._record("zscore", name, member)
        name = _s(name)
        self._check(name, "zset")
        self._expire_if_due(name)
        return self.zsets.get(name, {}).get(_s(member))

    # pipelines / scripts

    def pipeline(self, transaction: bool = True) -> "_FakePipeline":
        return _FakePipeline(self)

    def register_script(self, script: str) -> "_FakeScript":
        return _FakeScript(script)

    async def eval(self, script, numkeys, *keys_and_args):
        raise NotImplementedError(
            f"FakeStreamsRedis.eval: unsupported script {script.strip().splitlines()[0]!r}"
        )

    async def aclose(self):
        return None

    async def close(self):
        return None


class _FakePipeline:
    """WATCH/MULTI/EXEC over FakeStreamsRedis.

    Mirrors redis-py: after ``watch()`` commands run immediately until
    ``multi()``; without ``watch()`` they are buffered from the start.
    """

    def __init__(self, redis: FakeStreamsRedis) -> None:
        self._redis = redis
        self._watched: dict[str, int] = {}
        self._queue: list[tuple[str, tuple, dict]] = []
        self._buffering = True

    async def __aenter__(self) -> "_FakePipeline":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.reset()

    async def watch(self, *names: str) -> bool:
        await asyncio.sleep(0)
        self._buffering = False
        for raw in names:
            name = _s(raw)
            self._redis._expire_if_due(name)
            self._watched[name] = self._redis._versions.get(name, 0)
        return True

    def multi(self) -> None:
        self._buffering = True

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        target = getattr(self._redis, name)
        if not self._buffering:
            return target

        def _record(*args: Any, **kwargs: Any) -> "_FakePipeline":
            self._queue.append((name, args, kwargs))
            return self

        return _record

    async def execute(self) -> list:
        """EXEC: one network round trip, then atomic (raw, non-yielding commands).

        Like Redis, every queued command runs and runtime errors are returned in
        place; redis-py then raises the first one (``raise_on_error=True``).
        """
        from redis.exceptions import WatchError

        await asyncio.sleep(0)
        try:
            for name, version in self._watched.items():
                self._redis._expire_if_due(name)
                if self._redis._versions.get(name, 0) != version:
                    raise WatchError("Watched variable changed.")
            results: list = []
            for name, args, kwargs in self._queue:
                try:
                    results.append(
                        self._redis._out(
                            name, await _raw(self._redis, name)(*args, **kwargs)
                        )
                    )
                except Exception as exc:  # noqa: BLE001 — returned in place
                    results.append(exc)
            for result in results:
                if isinstance(result, Exception):
                    raise result
            return results
        finally:
            await self.reset()

    async def reset(self) -> None:
        self._watched.clear()
        self._queue.clear()
        self._buffering = True

    async def aclose(self) -> None:
        await self.reset()


class _FakeScript:
    """Emulates the redis-py Lock scripts; delegates to a real client otherwise."""

    def __init__(self, script: str) -> None:
        self.script = script

    async def __call__(self, keys=(), args=(), client=None):
        from redis.asyncio.lock import Lock

        if not isinstance(client, FakeStreamsRedis):
            return await client.eval(self.script, len(keys), *keys, *args)
        name = _s(keys[0])
        stored = await _raw(client, "get")(name)
        token = _s(args[0])
        if self.script == Lock.LUA_RELEASE_SCRIPT:
            if stored is None or stored != token:
                return 0
            await _raw(client, "delete")(name)
            return 1
        if self.script == Lock.LUA_EXTEND_SCRIPT:
            if stored is None or stored != token:
                return 0
            ttl = await _raw(client, "pttl")(name)
            if ttl < 0:
                return 0
            additional = int(_s(args[1]))
            new_ttl = additional if _s(args[2]) == "1" else ttl + additional
            await _raw(client, "pexpire")(name, new_ttl)
            return 1
        if self.script == Lock.LUA_REACQUIRE_SCRIPT:
            if stored is None or stored != token:
                return 0
            await _raw(client, "pexpire")(name, int(_s(args[1])))
            return 1
        raise NotImplementedError(
            f"FakeStreamsRedis script not emulated: {self.script.strip().splitlines()[0]!r}"
        )


def _raw(client: "FakeStreamsRedis", name: str):
    """The non-yielding implementation of a command (for atomic EXEC/scripts)."""
    return functools.partial(getattr(type(client), name).__wrapped__, client)


def _yielding(fn):
    @functools.wraps(fn)
    async def wrapper(self, *args, **kwargs):
        await asyncio.sleep(0)  # a network round trip is an await point
        return self._out(fn.__name__, await fn(self, *args, **kwargs))

    return wrapper


for _name, _fn in list(vars(FakeStreamsRedis).items()):
    if (
        asyncio.iscoroutinefunction(_fn)
        and not _name.startswith("_")
        and _name not in {"aclose", "close"}
    ):
        setattr(FakeStreamsRedis, _name, _yielding(_fn))
