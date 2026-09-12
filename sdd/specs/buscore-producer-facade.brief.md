# Brief for navigator-eventbus /sdd-spec buscore-producer-facade

## Motivation

Consuming applications currently have to hand-roll a small compatibility
shim in front of BusCore's publish path whenever they need: (1) lazy
resolution of a BusCore instance that may not exist yet at call time,
(2) a bounded publish timeout, and (3) a configurable choice between
raising on publish failure (strict callers, e.g. an admin HTTP endpoint)
versus swallowing it (fail-soft callers, e.g. ordinary background
publishers). FieldSync's `services/eventbus/publisher.py` currently
implements exactly this as a local, FieldSync-owned class
(`BusCorePublisherShim`) with zero FieldSync-specific logic inside it —
it is generic producer plumbing that happens to live in the wrong repo.
This feature contributes that plumbing upstream as `BusCoreProducer` so
any application on navigator-eventbus can reuse it without re-inventing
it, and so FieldSync can delete its local copy once this is released and
pinned.

## New Public Interface

New module: `src/navigator_eventbus/producers.py`

```python
from collections.abc import Callable
from datetime import datetime
from typing import Any

from navigator_eventbus.core import BusCore
from navigator_eventbus.envelope import EventEnvelope


class BusCoreProducer:
    """Generic compatibility facade in front of BusCore.publish().

    Owns lazy bus resolution, a bounded publish timeout, and explicit
    strict-vs-fail-soft error propagation. Contains NO application-specific
    routing, topic, or schema rules — `body` is opaque payload and
    `queue_name` is the raw topic string.
    """

    def __init__(
        self,
        bus_provider: Callable[[], BusCore | None],
        *,
        timeout: float = 1.0,
        raise_on_error: bool = False,
    ) -> None: ...

    async def publish_event(
        self,
        body: dict[str, Any],
        queue_name: str,
        **kwargs: Any,
    ) -> None:
        """Wrap `body` as `EventEnvelope.payload`, `queue_name` as `.topic`,
        and call `BusCore.publish()` within `timeout` seconds.

        `kwargs` accepts legacy keyword arguments (e.g. `routing_key`) for
        call-site compatibility with older duck-typed producer seams —
        these must be accepted without ever encoding an application-specific
        rule for them. If `kwargs` includes a `timestamp: datetime` value,
        use it to populate `EventEnvelope.timestamp`; otherwise default to
        "now" the same way `EventEnvelope` already does. Do NOT parse or
        special-case any other key (e.g. do not know about a field named
        `ts` — that is an application's job, not this facade's).

        Behavior when `bus_provider()` returns None, or when `BusCore.publish()`
        raises, or when the timeout elapses:
          - `raise_on_error=True`  -> propagate the failure to the caller.
          - `raise_on_error=False` -> log and swallow; return normally.
        """
```

## Reference: the shim this replaces (for behavioral parity only —
## do NOT import FieldSync code; this is context, not a dependency)

```python
# fieldsync's services/eventbus/publisher.py:258-302 (current, to be deleted
# there once this facade is released and pinned)
class BusCorePublisherShim:
    def __init__(self, bus_provider: Callable[[], Any | None], *, timeout: float = 1.0, raise_on_error: bool = False) -> None: ...
    async def publish_event(self, body: dict[str, Any], queue_name: str, **kwargs: Any) -> None: ...
```

## Scope

- Add `BusCoreProducer` in `src/navigator_eventbus/producers.py`.
- Lazy `BusCore | None` resolution via `bus_provider()`, called once per
  `publish_event` call (bus may not exist at construction time).
- Bounded publish timeout (default 1.0s), generic body-as-payload
  publication, explicit `raise_on_error` (default `False`).
- Accept legacy keyword arguments such as `routing_key` without acting on
  them beyond acceptance — no application rule embedded in this module.
- Export `BusCoreProducer` from the package's public surface
  (`src/navigator_eventbus/__init__.py`) if that matches this repo's
  existing export convention (check how `BusCore`, `CompositeBackend`, etc.
  are currently exported and mirror it).
- Add unit tests in `tests/test_producers.py` using a fake `bus_provider`
  and a fake/mock `BusCore` — no real Redis needed for this module.
- Bump the package version sufficiently for a consuming app to pin the
  release once merged.

**NOT in scope:** anything FieldSync-specific, anything about `Topic`/
`TargetScope`/tenant routing, changes to `BusCore.publish()` itself, or
changes to any backend (`RedisStreamsBackend`, `CompositeBackend`).

## Existing signatures to build on (verify these against this repo's
## actual current code before writing anything — do not assume drift-free)

```python
# src/navigator_eventbus/core.py (verify current line numbers)
class BusCore:
    async def publish(self, envelope: EventEnvelope) -> None: ...

# src/navigator_eventbus/envelope.py (verify current line numbers)
@dataclass(frozen=True, slots=True)
class EventEnvelope:
    topic: str
    payload: dict[str, Any]
    event_id: str
    timestamp: datetime
    source: str | None
    severity: Severity
    priority: EventPriority
    # correlation, trace, metadata, schema_version — upstream-owned, unrelated to this task
```

## Acceptance Criteria

- [ ] The facade contains no application-specific (e.g. FieldSync) import,
      topic, or schema rule.
- [ ] `publish_event` wraps the supplied `body` as the envelope's `payload`
      and calls `BusCore.publish()` with `queue_name` as the envelope's
      `topic`.
- [ ] Timeout behavior and strict (`raise_on_error=True`) vs fail-soft
      (`raise_on_error=False`) error behavior are unit tested for: missing
      bus (`bus_provider()` returns `None`), a `BusCore.publish()` exception,
      and a timeout.
- [ ] `bus_provider` is called lazily (not memoized at `__init__` time) —
      tested by having the fake provider return `None` on the first call and
      a real fake bus on a later call, then asserting the later call
      succeeds.
- [ ] Legacy keyword arguments (e.g. `routing_key`) are accepted without
      raising `TypeError`, and without influencing `topic`/`payload`.
- [ ] The package exposes `BusCoreProducer` through a stable, documented
      import path.
- [ ] Tests and lint pass; version bumped and ready to release.

## Test Specification

Add tests for: successful publication (assert the exact `EventEnvelope.topic`
== `queue_name` and `.payload` == `body` received by the fake bus), lazy
provider resolution (see above), missing bus with both `raise_on_error`
values, `BusCore.publish()` raising with both `raise_on_error` values,
timeout expiry with both `raise_on_error` values, and a legacy-kwarg
pass-through call that doesn't raise.

## Known Risk (carry this into the spec's Risks section)

**Timestamp ownership**: the shim this replaces currently reads a
FieldSync-specific `body["ts"]` field to populate the timestamp. This
facade must NOT acquire that same undocumented, application-specific
dependency. Either support a generic `timestamp: datetime` keyword (see
interface above) that ANY caller can pass explicitly, or default to "now"
and leave timestamp extraction entirely to the calling application. Do not
special-case a `ts`-named field anywhere in this module.
