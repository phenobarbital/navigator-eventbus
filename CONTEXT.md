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
├── _imports.py          # Lazy import helpers
├── ingress_models.py    # IngressEnvelope
├── backends/            # Transport backends (memory, redis)
├── hooks/               # Generic hooks fabric
│   ├── models.py        # HookEvent, HookTypeRegistry
│   └── brokers/         # Hook broker implementations
├── ingress/             # WebSocket/gRPC ingress
│   └── proto/           # gRPC protocol definitions
└── subscribers/         # Subscriber implementations
```

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
- `hooks.<hook_type>.<event>` — hook events via `HookManager`
