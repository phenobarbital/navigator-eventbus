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

### Receiving webhooks

Mount one catch-all route and register endpoints — including at runtime,
without touching the aiohttp router. Each delivery is authenticated with the
endpoint's own HMAC scheme, optionally transformed, and emitted on
`hooks.webhook.<event_type>`.

```python
from aiohttp import web
from navigator_eventbus import EventBus
from navigator_eventbus.hooks import HookManager, WebhookListenerHook

bus, app = EventBus(), web.Application()
manager = HookManager(route_to_bus=True)
manager.set_event_bus(bus)

listener = WebhookListenerHook()
listener.register_endpoint(
    "/github",
    secret=GITHUB_WEBHOOK_SECRET,      # from the environment, never inline
    signature_scheme="github",         # X-Hub-Signature-256
    event_type_header="X-GitHub-Event",
    event_type_prefix="github",
    dedup_header="X-GitHub-Delivery",  # with dedup_ttl_seconds > 0
    preprocessor="myapp.hooks:summarize_pr",   # or preprocessor_fn=<callable>
)
manager.register(listener)
listener.setup_routes(app)             # POST /api/v1/hooks/webhook/github
```

The preprocessor may be sync or async, and receives an optional
`WebhookContext` second argument. If it raises, the raw payload is emitted
anyway and the sender still gets a `202` — a transform bug never costs you a
delivery.

Exclude `listener.base_path` from any auth or body-rewriting middleware: HMAC
verification needs the exact bytes on the wire.

For a single provider with real classification logic, subclass
`ProviderWebhookHook` instead — it serves one fixed route and gives you
`_classify_event` / `_normalize_payload` override points.

### Sending webhooks

```python
from navigator_eventbus.subscribers import WebhookDeliverySubscriber

delivery = WebhookDeliverySubscriber(
    url="https://partner.example/hooks/orders",
    patterns=["order.*"],
    secret=PARTNER_SECRET,
    signature_scheme="github",
)
delivery.attach(bus)
...
await bus.close()
await delivery.aclose()
```

Deliveries are queued and retried on 5xx/429 with capped, jittered backoff, so
a slow endpoint never occupies a bus dispatch worker. Exhausted retries emit
`bus.webhook_delivery_failed`.

## Configuration knobs

> ⚠️ **Neutral defaults vs. legacy `parrot:*` deployments.** Every prefix
> below defaults to a neutral `evb:*` value (this package has no
> knowledge of ai-parrot). If you are migrating an EXISTING ai-parrot
> deployment that already has data on `parrot:events:*` / `parrot:stream:*`
> Redis keys, you **must** override these to the legacy `parrot:*` values
> (constructor kwarg or the matching navconfig key below) — otherwise the
> migrated process will read/write a *different* set of keys than the
> deployed one, silently losing continuity. ai-parrot itself pins the
> legacy values when it migrates (phase 4 of the extraction plan); until
> then it keeps using its own in-tree copy of the bus.

All keys are read via `navconfig` (flattened env/TOML keys); every key
below has a documented in-code default and can also be overridden per
constructor kwarg where noted.

### BusCore / EventBus facade (`evb.py`, `core.py`)

| navconfig key | Constructor kwarg | Default | Notes |
|---|---|---|---|
| `BUS_WORKERS` | `workers=` | `4` | Dispatch worker-pool size |
| `BUS_QUEUE_SIZE` | `queue_size=` | `1024` | Max size of EACH per-priority queue |
| `BUS_HANDLER_TIMEOUT` | `handler_timeout=` | `30.0` (s) | Per-handler `asyncio.timeout` |
| `BUS_RETRY_ATTEMPTS` | `retry_attempts=` | `3` | Total delivery attempts per handler |
| `BUS_RETRY_BASE_DELAY` | `retry_base_delay=` | `0.1` (s) | Backoff base delay |
| `BUS_DEFAULT_BACKPRESSURE` | `default_backpressure=` | `"block"` | `block` \| `drop_oldest` \| `reject` |
| `BUS_DRAIN_TIMEOUT` | `drain_timeout=` | `5.0` (s) | Deadline for graceful `close()` drain |
| `BUS_CHANNEL_PREFIX` | `channel_prefix=` (on `EventBus`) | `"evb:events:"` | Redis pub/sub channel prefix — **see warning above** |

### Redis Streams backend (`backends/redis_streams.py`)

| navconfig key | Constructor kwarg | Default | Notes |
|---|---|---|---|
| `BUS_STREAM_PREFIX` | `stream_prefix=` | `"evb:stream:"` | Stream key prefix (per topic-class) — **see warning above** |
| `BUS_DEDUP_PREFIX` | `dedup_prefix=` | `"evb:events:dedup:"` | Dedup key prefix — **see warning above** |
| `BUS_GROUP` | `group=` | `"evb-bus"` | Consumer-group name — **see warning above** |

### DLQ / Audit persistence (`dlq.py`, `subscribers/audit.py`)

| navconfig key | Constructor kwarg | Default | Notes |
|---|---|---|---|
| `EVB_DSN` | `dsn=` | *(none — disables persistence)* | Postgres DSN for `navigator.evb_dlq` / `navigator.evb_audit`. Falls back to deriving a DSN from `DBUSER`/`DBPWD`/`DBHOST`/`DBPORT`/`DBNAME` navconfig keys (same derivation ai-parrot's `parrot.conf.default_dsn` used — but read directly, zero `parrot.*` coupling) |

### Notification alerting (`subscribers/notification.py`)

| navconfig key | Notes |
|---|---|
| `BUS_ALERTS_DEDUP_WINDOW` | Default `300.0` s |
| `BUS_ALERTS_CHANNEL_THROTTLE` | Default `10` (per channel) |
| `BUS_ALERTS_THROTTLE_WINDOW` | Default `60.0` s |
| `BUS_ALERTS_STORM_THRESHOLD` | Default `25` (ERROR+ events) |
| `BUS_ALERTS_STORM_WINDOW` | Default `30.0` s |

### Ingress auth (`ingress/websocket.py`, `ingress/grpc.py`)

| navconfig key | Constructor kwarg | Default | Notes |
|---|---|---|---|
| `BUS_INGRESS_TOKEN` | `auth_token=` | *(none — refuses all connections)* | Shared bearer token for WS/gRPC ingress |

## Development

```bash
uv sync --extra all --extra dev
uv run pytest tests/ -v
uv run ruff check src/ tests/
uv run mypy src/
```

