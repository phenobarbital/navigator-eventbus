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

from typing import Any, Optional


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

    def __init__(self, clock: Optional[FakeClock] = None) -> None:
        self.clock = clock or FakeClock()
        self.streams: dict[str, list[tuple[str, dict]]] = {}
        self.groups: dict[tuple[str, str], dict[str, Any]] = {}
        self.acked: list[tuple[str, str, str]] = []
        self.calls: list[tuple[str, tuple, dict]] = []
        self._seq = 0
        self.connection_pool = type(
            "_Pool", (), {"connection_kwargs": {"decode_responses": True}}
        )()

    # -- helpers -------------------------------------------------------

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
        self._seq += 1
        message_id = f"{self._seq}-0"
        entries = self.streams.setdefault(name, [])
        entries.append((message_id, dict(fields)))
        if maxlen is not None and maxlen >= 0 and len(entries) > maxlen:
            del entries[: len(entries) - maxlen]
        return message_id

    async def xlen(self, name):
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

    async def aclose(self):
        return None

    async def close(self):
        return None
