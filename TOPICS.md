# TOPICS.md — Topic Namespace Registry

`navigator-eventbus` routes everything through glob-matched topic strings
(`EventBus.emit(event_type, ...)` / `BusCore.subscribe(pattern, ...)`). This
document is the **governance registry** for topic namespaces: which prefix
belongs to which app/module, so multiple consumers (ai-parrot, Flowtask,
QuerySource, navigator-auth, ...) sharing one bus never collide.

## Convention

- A namespace is the first dot-separated segment (or two, for `hooks.*`).
- Namespaces are **reserved by registration in this file** — add a row
  before you start emitting under a new prefix.
- Meta-topics (bus lifecycle/error signals) are owned by the core package
  itself and MUST NOT be reused by app code.

## Core meta-topics (owned by `navigator_eventbus.core.BusCore`)

| Topic | Emitted when |
|---|---|
| `bus.subscriber_error` | a subscriber handler raises (isolation model B — never interrupts the emitter) |
| `bus.backpressure` | a priority queue hits its configured size limit |
| `bus.shutdown_incomplete` | graceful shutdown timed out with in-flight events |
| `bus.dlq` | an event is routed to the Dead Letter Queue |
| `bus.dlq_error` | persisting to the DLQ itself fails |
| `bus.webhook_delivery_failed` | `WebhookDeliverySubscriber` exhausted its retries for one envelope |
| `bus.queue_dlq` | a queued message exceeded its queue's `max_receives` and was parked to the DLQ |
| `bus.queue_purged` | an operator purged a queue through the admin route |

`bus.webhook_delivery_failed` sits under `bus.*` on purpose:
`WebhookDeliverySubscriber` defaults to `exclude_bus_internal=True`, so it
structurally cannot re-deliver its own failure notice. Putting the topic
anywhere else would reintroduce the feedback loop.

## Hooks ingress (owned by `navigator_eventbus.hooks.manager.HookManager`)

| Topic pattern | Emitted when |
|---|---|
| `hooks.<hook_type>.<event>` | `HookManager.route_to_bus` forwards a `HookEvent` for a registered `hook_type` (see `HookTypeRegistry`) |
| `hooks.webhook.<event_type>` | `WebhookListenerHook` or `ProviderWebhookHook` accepted an inbound HTTP delivery |

`<hook_type>` must be registered against `navigator_eventbus.hooks.models.HOOK_TYPES`
before events under its namespace are accepted (`HookEvent.hook_type` validator).

For `hooks.webhook.*`, the `<event_type>` segment comes from — in precedence
order — the endpoint's preprocessor return value, the header named by
`event_type_header`, or the endpoint's configured `event_type` (with
`event_type_prefix` prepended when set). A `ProviderWebhookHook` subclass that
overrides `hook_type` emits under `hooks.<that_type>.*` instead, and must
register that type with `HOOK_TYPES.register(...)` and add a row here first —
the hook's constructor rejects an unregistered type.

## Reserved namespaces (future phases / consuming apps)

| Namespace | Owner | Status |
|---|---|---|
| `lifecycle.*` | `navigator_eventbus` (Phase 2 — `eventbus-lifecycle-extraction`) | reserved, not yet implemented in this package |
| `agent.*` | ai-parrot (`parrot.core.events.lifecycle`) | reserved |
| `task.*` / `flow.*` | Flowtask | reserved |
| `auth.*` | navigator-auth | reserved |
| `fieldsync.*` | FieldSync (`../fieldsync`, FEAT-409) | reserved — consumes `RedisStreamsBackend` via its own `codec=`/`stream_key_fn=`/`streams=` seams (FEAT-320) rather than a parallel transport |
| `saas.*` | ai-parrot SaaS control plane (`packages/ai-parrot-saas`) | reserved — run/tenant/usage/webhook lifecycle for the multi-tenant Flows service; consumes `RedisStreamsBackend` + `RedisPubSubBackend` via `CompositeBackend` |
| `queue.*` | `navigator_eventbus.queues` (FEAT-432) | active — default prefix for topics mirrored from an HTTP pull queue onto the bus |

### Governance note on `queue.*`

A queue producer supplies its own `IngressEnvelope.topic`, so with
`mirror_to_bus=True` it could otherwise emit under **any** namespace.
`QueueConfig.topic_prefix` (default `queue.<name>`) forces mirrored topics
under a governed prefix. Setting `topic_prefix=None` explicitly opts into
"the producer owns the topic", and is only legitimate when that producer
already owns a namespace registered in this file.

## Registering a new namespace

1. Pick a short, singular, lower-case namespace segment (e.g. `auth`, not
   `authentication` or `Auth`).
2. Open a PR against this file adding a row under **Reserved namespaces**
   (or a dedicated section if the namespace has significant internal
   structure, as `hooks.*` does here).
3. For hook-type namespaces specifically, also register the `hook_type`
   string with `HOOK_TYPES.register("<name>")` at import time in your
   app (see `navigator_eventbus.hooks.models.HookTypeRegistry`).
4. Do not emit under a namespace until it is registered — this file is the
   single source of truth used to catch collisions across consumers.
