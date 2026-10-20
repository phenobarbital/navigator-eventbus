# PYTHON

- **Package Manager**: project use **`uv`** for package management. Commands like `uv pip`, `uv run`, and `uv add` are required.
- **Virtual Environment**: Work must always be performed within a `.venv` virtual environment.
  - **CRITICAL**: You MUST NEVER run `uv`, `python`, or `pip` commands WITHOUT first enabling the virtual environment.
  - **ALWAYS** run `source .venv/bin/activate` before any python-related command.
- **Concurrency**: Prefer non-blocking code using **`asyncio`** over blocking synchronous code.
- **Web Server**: Use **`aiohttp`** as the default web server/client library.

# Project Architecture

navigator-eventbus is a standalone async event bus + generic hooks fabric,
extracted from ai-parrot's EventBus v2 (FEAT-310). It provides:

- **Per-priority `asyncio.Queue` workers** with backpressure control
- **Dead Letter Queue (DLQ)** for failed event handling
- **Meta-events** (`bus.*` topics) for observability
- **Glob + severity subscription matching**
- **Multiple transports**: memory, redis-pubsub, redis-streams
- **Ingress adapters**: WebSocket, gRPC
- **Generic hooks fabric** with open `HookTypeRegistry`

## Source Layout

```
src/navigator_eventbus/
├── __init__.py          # Public API re-exports
├── core.py              # BusCore — the engine (queues, workers, dispatch)
├── evb.py               # EventBus — high-level facade
├── envelope.py          # EventEnvelope, Severity
├── dlq.py               # DLQHandler
├── converters.py        # Serialization converters
├── serialization.py     # Event serialization
├── _imports.py          # Lazy import helpers + resolve_callable()
├── ingress_models.py    # IngressEnvelope
├── webhook_signatures.py # Pluggable HMAC schemes (verify inbound / sign outbound)
├── backends/            # Transport backends (memory, redis)
├── hooks/               # Generic hooks fabric
│   ├── models.py        # HookEvent, HookTypeRegistry
│   ├── brokers/         # Hook broker implementations
│   └── webhook/         # Inbound webhooks (listener + provider base)
├── ingress/             # WebSocket/gRPC ingress
│   └── proto/           # gRPC protocol definitions
├── queues/              # SQS-style pull queues (store + HTTP API + receipts)
└── subscribers/         # Subscriber implementations (incl. outbound webhook)
```

## Ingress vs. hooks vs. queues — where does a new adapter go?

All three accept traffic from outside and all three are `BaseHook`
subclasses, so "is a BaseHook" does not discriminate. **Topic ownership and
delivery direction do:**

| | `ingress/` | `hooks/` | `queues/` |
|---|---|---|---|
| Input shape | already bus-shaped (`IngressEnvelope`, `extra="forbid"`) | foreign/vendor-shaped, unknown schema | bus-shaped (`IngressEnvelope`) |
| Topic | the **caller** supplies it | the **package** derives it (`hooks.<type>.<event>`) | caller supplies, governed by `topic_prefix` |
| Delivery | push, fan-out to every matching subscriber | push, via `HookManager` | **pull**, competing consumers per group |
| Destination | `bus.emit(...)` directly | `self.on_event(HookEvent)` → `HookManager` | a Redis stream; `BusCore` is bypassed |
| Ack | none — dispatch is fire-and-forget | none | explicit, with lease + redelivery |
| Auth | one shared bearer token for the adapter | per-endpoint HMAC over the raw body | per-queue tokens split by role |

A webhook receiver is in the `hooks/` column on every row, which is why
`hooks/webhook/` — not `ingress/http.py` — is where it lives.

The `queues/` column exists because a lease-based pull API cannot be
expressed by the `TransportBackend` protocol (three methods, and
`OnEnvelope` returns `Awaitable[None]` — no ack handle), and because
`BusCore` deliberately never re-raises from a handler (isolation model B),
so a message consumed through `bus.subscribe()` is acknowledged even when
processing fails. That is the opposite of queue semantics, so the queue
plane owns its lease lifecycle end to end and treats the bus as an optional,
opt-in bridge in either direction.

## Key Abstractions

| Abstraction | Location | Purpose |
|---|---|---|
| `BusCore` | `core.py` | Event dispatch engine with per-priority queues |
| `EventBus` | `evb.py` | High-level facade for emit/subscribe |
| `EventEnvelope` | `envelope.py` | Typed event container with metadata |
| `Event` / `EventPriority` | `evb.py` | Event model and priority enum |
| `EventSubscription` | `evb.py` | Subscription with glob pattern matching |
| `DLQHandler` | `dlq.py` | Dead Letter Queue for failed events |
| `Severity` | `envelope.py` | Event severity levels |
| `HookTypeRegistry` | `hooks/models.py` | Registry for hook type namespaces |
| `IngressEnvelope` | `ingress_models.py` | Envelope for ingress adapters |
| `SignatureScheme` | `webhook_signatures.py` | Pluggable HMAC scheme (github/jira/generic/stripe); signs and verifies |
| `WebhookListenerHook` | `hooks/webhook/listener.py` | Catch-all route fronting N runtime-registered webhook endpoints |
| `ProviderWebhookHook` | `hooks/webhook/provider.py` | Base class for a fixed, single-route provider webhook |
| `WebhookDeliverySubscriber` | `subscribers/webhook.py` | Outbound: POSTs matching bus envelopes to an HTTP endpoint |
| `QueueStore` | `queues/store.py` | Redis Streams lease engine: receive/delete/visibility, DLQ, purge |
| `QueueAPI` | `queues/api.py` | SQS-style HTTP surface — producers POST, consumers pull |
| `ReceiptCodec` | `queues/receipts.py` | Stateless HMAC-signed receipt handles (replica-safe) |
| `QueueFeeder` | `queues/feeder.py` | Bridge: bus events become pullable over HTTP |

## Dependencies

- `navconfig` — configuration management
- `asyncdb` — async database utilities
- `aiohttp` — HTTP server/client (core)
- `redis` — Redis backend (optional)
- `grpcio` — gRPC ingress (optional)
- `async-notify` — notification sender (optional)
- `apscheduler` — scheduler hook (optional)
- `watchdog` — filesystem hook (optional)
- `gmqtt` — MQTT broker hook (optional)

## Topic Namespace Convention

See `TOPICS.md` for the full topic registry. Key meta-topics:
- `bus.subscriber_error` — subscriber handler raised
- `bus.backpressure` — queue size limit hit
- `bus.shutdown_incomplete` — graceful shutdown timed out
- `bus.dlq` — event routed to DLQ
- `bus.webhook_delivery_failed` — outbound webhook exhausted its retries
- `bus.queue_dlq` — queued message exceeded `max_receives`
- `hooks.<hook_type>.<event>` — hook events via `HookManager`
- `hooks.webhook.<event_type>` — inbound HTTP webhook accepted
- `queue.<name>.<topic>` — message mirrored from an HTTP pull queue
