# navigator-eventbus

Standalone async event bus + generic hooks fabric for aiohttp-based servers
(Navigator ecosystem: ai-parrot, Flowtask, QuerySource, navigator-auth, ...).

Extracted from ai-parrot's EventBus v2 (FEAT-310) — same design (per-priority
`asyncio.Queue` workers, backpressure, DLQ, meta-events, glob+severity
subscription matching, memory/redis-pubsub/redis-streams transports,
WebSocket/gRPC ingress, generic hooks with an open `HookTypeRegistry`) with
the `parrot.*` coupling removed. See `TOPICS.md` for the topic-namespace
registry shared across consuming apps.

> Phase 1 of a 5-phase extraction plan
> (`navigator-eventbus-extraction` brainstorm, ai-parrot repo). This phase
> ships the bus core + generic hooks. Lifecycle events, the brokers port,
> and the ai-parrot migration itself land in later phases.

## Install

```bash
uv pip install -e .            # core only
uv pip install -e ".[redis]"   # + redis pub/sub & streams backends
uv pip install -e ".[grpc]"    # + gRPC ingress
uv pip install -e ".[notify]"  # + async-notify default sender
uv pip install -e ".[scheduler]"  # + APScheduler-backed hook
uv pip install -e ".[watchdog]"   # + filesystem watchdog hook
uv pip install -e ".[mqtt]"       # + gmqtt-backed broker hook
uv pip install -e ".[all]"        # everything above
uv pip install -e ".[dev]"        # pytest, ruff, mypy
```

## Usage

```python
from navigator_eventbus import EventBus, Event, EventPriority

bus = EventBus()
await bus.emit("bus.example", {"hello": "world"})
```

## Configuration knobs

Redis channel/stream prefixes and the consumer group default to neutral
values (`evb:events:`, `evb:stream:`, `evb:events:dedup:`, `evb-bus`) —
**do not** point these at an existing deployment's `parrot:*` streams
without explicitly overriding them (constructor kwarg or the `BUS_*`
navconfig keys: `BUS_CHANNEL_PREFIX`, `BUS_STREAM_PREFIX`,
`BUS_DEDUP_PREFIX`, `BUS_GROUP`).

## Development

```bash
uv sync --extra all --extra dev
uv run pytest tests/ -v
uv run ruff check src/ tests/
uv run mypy src/
```

