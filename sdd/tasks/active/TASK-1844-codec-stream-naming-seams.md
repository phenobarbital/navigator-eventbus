# TASK-1844: Codec + stream-naming + explicit streams seams for RedisStreamsBackend

**Feature**: FEAT-320 — RedisStreamsBackend Generic Capability Extensions
**Spec**: `sdd/specs/redis-streams-backend-extensions.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Spec §3 Module 2. `RedisStreamsBackend` hard-codes ONE wire shape
(`{"envelope": json.dumps(envelope.to_dict())}`) and ONE stream-naming
scheme (`<stream_prefix><topic-class>`, discovered via `SCAN`). A consuming
app that already owns its own wire envelope and stream names (e.g.
FieldSync's `fieldsync.*` streams) needs to plug those in without forking
this backend. `stream_key_fn=` breaks the `SCAN <stream_prefix>*` discovery
assumption whenever names don't share that prefix shape, so this task also
adds an explicit `streams=` override that bypasses discovery entirely.

---

## Scope

- Add a `codec: Optional[Codec] = None` constructor kwarg, where `Codec` is
  a structural `Protocol` (not a Pydantic model — matches this repo's
  `TransportBackend` style):
  ```python
  class Codec(Protocol):
      def encode(self, envelope: EventEnvelope) -> dict[str, Any]: ...
      def decode(self, fields: dict[str, Any]) -> EventEnvelope: ...
  ```
  When `codec is None` (default), `publish()`/`_handle_message()` behave
  EXACTLY as today (i.e. today's shape becomes the built-in default codec —
  implement it as a small internal default object/functions, do not
  special-case `if codec is None` scattered through the method bodies;
  prefer `self._codec = codec or _DefaultCodec()`).
- Add a `stream_key_fn: Optional[Callable[[str], str]] = None` constructor
  kwarg. When set, `_stream_for(topic)` calls `self._stream_key_fn(topic)`
  instead of the hard-coded `f"{self.stream_prefix}{topic.split('.', 1)[0]}"`.
- Add a `streams: Optional[list[str]] = None` constructor kwarg. When set:
  - `_refresh_streams()`'s `SCAN`-based discovery is bypassed entirely —
    `self._streams` is seeded once from `streams` (join group / broadcast-
    subscribe to exactly these, no auto-discovery of new ones).
  - If `stream_key_fn` is set but `streams` is NOT, log a `logger.warning`
    once (at `start_consumer()` time) noting that SCAN discovery may not
    find custom-named streams — do not raise (a caller could intentionally
    keep `stream_prefix`-shaped names with a custom encode-only codec).
- Default (`codec=None`, `stream_key_fn=None`, `streams=None`): NO behavior
  change — every existing test in `tests/test_backends_streams.py` must
  pass unmodified.

**NOT in scope**: `delivery="broadcast"` itself (TASK-1843 — but write the
`test_stream_key_fn_and_broadcast_compose` integration test here anyway,
since this task lands the `stream_key_fn=`/`streams=` seam that test
depends on; if TASK-1843 has not yet landed when you pick this task up,
implement this task's OWN seam and unit tests fully, and add the broadcast-
compose test as a follow-up note in the Completion Note instead of
blocking on it — do NOT implement broadcast mode yourself here);
`retention=` (TASK-1845); `max_deliveries=` (TASK-1846).

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `src/navigator_eventbus/backends/redis_streams.py` | MODIFY | `codec=`, `stream_key_fn=`, `streams=` kwargs; `Codec` Protocol; default-codec extraction |
| `tests/test_backends_streams.py` | MODIFY | codec round-trip test, custom-naming + explicit-streams test |

---

## Codebase Contract (Anti-Hallucination)

### Verified Imports
```python
from navigator_eventbus.backends.redis_streams import RedisStreamsBackend
    # verified: src/navigator_eventbus/backends/redis_streams.py:67
from navigator_eventbus.envelope import EventEnvelope
    # verified: imported at redis_streams.py:54
```

### Existing Signatures to Use
```python
# src/navigator_eventbus/backends/redis_streams.py (verified 2026-07-25, main@8ef73b3)
async def publish(self, envelope: EventEnvelope) -> None: ...      # line 162
    # today: stream = self._stream_for(envelope.topic); await self._ensure_group(stream);
    #        await self._redis.xadd(stream, {"envelope": json.dumps(envelope.to_dict())},
    #                                maxlen=self._maxlen, approximate=True)
    # THIS TASK: replace {"envelope": json.dumps(envelope.to_dict())} with
    #            self._codec.encode(envelope)
def _stream_for(self, topic: str) -> str: ...                     # line 216
    # today: f"{self.stream_prefix}{topic.split('.', 1)[0]}"
    # THIS TASK: if self._stream_key_fn is not None, return
    #            self._stream_key_fn(topic) instead
async def _refresh_streams(self) -> None: ...                     # line 245
    # today: SCAN match=f"{self.stream_prefix}*", joins group per new stream
    # THIS TASK: skip this method's SCAN body entirely when self._explicit_streams
    #            (the streams= kwarg) is set — seed self._streams from it once
async def _handle_message(self, stream, msg_id, fields) -> None: ...  # line 328
    # today:
    #   raw = fields.get("envelope") or fields.get(b"envelope")
    #   envelope = EventEnvelope.from_dict(json.loads(raw.decode() if bytes else raw))
    # THIS TASK: replace the two lines above with
    #            envelope = self._codec.decode(fields)
    #            (decode failure handling — the existing try/except around
    #            this block, redis_streams.py:348-357 — stays, just wraps
    #            the codec call instead of the inline json.loads)

# src/navigator_eventbus/envelope.py (reference — DO NOT modify)
class EventEnvelope:
    def to_dict(self) -> dict[str, Any]: ...
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EventEnvelope": ...
```

### Does NOT Exist
- ~~`RedisStreamsBackend(codec=..., stream_key_fn=..., streams=...)`~~ —
  this task creates all three kwargs.
- ~~A `Codec` class/Protocol anywhere in this repo~~ — define it in
  `redis_streams.py` (or a small dedicated module if you prefer, but keep
  it colocated — do not scatter it into `backends/base.py`, which is the
  `TransportBackend` protocol, a DIFFERENT concern).
- ~~Any change to `EventEnvelope.to_dict()`/`from_dict()`~~ — those stay
  exactly as they are; the codec seam sits AROUND them, not inside them.

---

## Implementation Notes

### Pattern to Follow
```python
class Codec(Protocol):
    def encode(self, envelope: EventEnvelope) -> dict[str, Any]: ...
    def decode(self, fields: dict[str, Any]) -> EventEnvelope: ...


class _DefaultCodec:
    """Preserves today's exact wire shape — the implicit codec when none
    is supplied."""

    def encode(self, envelope: EventEnvelope) -> dict[str, Any]:
        return {"envelope": json.dumps(envelope.to_dict())}

    def decode(self, fields: dict[str, Any]) -> EventEnvelope:
        raw = fields.get("envelope") or fields.get(b"envelope")
        data = raw.decode() if isinstance(raw, bytes) else raw
        return EventEnvelope.from_dict(json.loads(data))
```

### Key Constraints
- `self._codec = codec or _DefaultCodec()` in `__init__` — do not branch on
  `codec is None` anywhere else in the class body.
- `streams=` seeding must happen ONCE (at `start_consumer()`/first use),
  not re-derived every `_refresh_streams()` tick — an explicit set is
  static by definition.
- Preserve the EXACT existing decode-failure handling shape (poison entry
  → logged + acked + dropped) — only the encode/decode CALL changes, not
  the surrounding error handling.
- Google-style docstrings + strict type hints.

### References in Codebase
- `src/navigator_eventbus/backends/base.py:28` — `TransportBackend`
  Protocol, the existing structural-typing convention to mirror for `Codec`.

---

## Acceptance Criteria

- [ ] `RedisStreamsBackend(codec=CustomCodec())` round-trips a non-default wire shape correctly
- [ ] `RedisStreamsBackend(stream_key_fn=..., streams=[...])` consumes exactly the given streams, bypassing SCAN
- [ ] Setting `stream_key_fn=` without `streams=` logs a warning, does not raise
- [ ] Default construction (no new kwargs) behavior is IDENTICAL to today — full existing `tests/test_backends_streams.py` suite passes unmodified
- [ ] `pytest tests/test_backends_streams.py -v` green
- [ ] `ruff check src/navigator_eventbus/backends/redis_streams.py` clean

---

## Test Specification

```python
# tests/test_backends_streams.py
class TestCodecAndStreamNamingSeams:
    async def test_custom_codec_roundtrip(self, fake_redis):
        """Custom Codec.encode/decode round-trips a non-default wire shape."""
        ...

    async def test_custom_stream_key_fn_with_explicit_streams(self, fake_redis):
        """stream_key_fn= + streams=[...] bypasses SCAN discovery."""
        ...

    def test_stream_key_fn_without_streams_warns(self, fake_redis, caplog):
        """stream_key_fn without streams= logs a warning, does not raise."""
        ...

    async def test_default_codec_and_naming_unchanged(self, fake_redis):
        """Omitting codec=/stream_key_fn=/streams= behaves exactly like today."""
        ...
```

---

## Agent Instructions

1. **Read the spec** (`sdd/specs/redis-streams-backend-extensions.spec.md`) §2 and §6 for full context
2. **Check dependencies** — none
3. **Verify the Codebase Contract** — re-read `redis_streams.py` before editing; line numbers may have drifted
4. **Update status** in `sdd/tasks/index/redis-streams-backend-extensions.json` → `"in-progress"`
5. **Implement**, **verify**, **move this file** to `sdd/tasks/completed/`,
   **update index** → `"done"`, **fill in the Completion Note** — note
   whether TASK-1843 (broadcast mode) had already landed so the
   `test_stream_key_fn_and_broadcast_compose` integration test could be
   written, or whether it is deferred

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**:
**Date**:
**Notes**:

**Deviations from spec**: none
