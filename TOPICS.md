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

## Hooks ingress (owned by `navigator_eventbus.hooks.manager.HookManager`)

| Topic pattern | Emitted when |
|---|---|
| `hooks.<hook_type>.<event>` | `HookManager.route_to_bus` forwards a `HookEvent` for a registered `hook_type` (see `HookTypeRegistry`) |

`<hook_type>` must be registered against `navigator_eventbus.hooks.models.HOOK_TYPES`
before events under its namespace are accepted (`HookEvent.hook_type` validator).

## Reserved namespaces (future phases / consuming apps)

| Namespace | Owner | Status |
|---|---|---|
| `lifecycle.*` | `navigator_eventbus` (Phase 2 — `eventbus-lifecycle-extraction`) | reserved, not yet implemented in this package |
| `agent.*` | ai-parrot (`parrot.core.events.lifecycle`) | reserved |
| `task.*` / `flow.*` | Flowtask | reserved |
| `auth.*` | navigator-auth | reserved |

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
