# Changelog

All notable changes to `navigator-eventbus` are documented in this file.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [0.5.0] — Unreleased

FEAT-713 — KeyedQueue. (The release number is chosen at release time; 0.5.0 is a placeholder.)

### Added — FEAT-713 KeyedQueue
- `navigator_eventbus.queues.keyed`: `KeyedQueue`, `KeyedConsumer`, `KeyedLease`, `drain`,
  `KeyedQueueConfig`, `KeyedMessage`, `Outcome`, `DrainReport`, `LeaseLost` (also lazily exported
  from `navigator_eventbus.queues`).
- Per-key FIFO ordered by a caller-supplied score, at most one message in flight per key
  cluster-wide (per-key `redis.asyncio` lock), fenced `apply` (lock token + generation in one
  `WATCH`/`MULTI`), atomic idempotent `push(message_id=...)`.
- Head-blocking outcomes: `ack`, `retry(delay)`, `wait(until)` (always with a safety deadline),
  `hold(reason)` + `release_hold`.
- Leaderless operations: `promote_due`, `recover` (throttled re-signal of lost wake-ups),
  `pause(ttl)` / `resume`, `max_age_ms` expiry and `max_attempts` handed to `on_dlq`.
- `drain(handler, key_prefix=...)`: direct administrative flush — concurrent across keys,
  sequential inside a key, same lock as the consumers, fail-fast.
- No Lua of our own; no change to `QueueStore`, `RedisStreamsBackend` or `BusCore`.

## [0.4.0] — 2026-10-21

FEAT-431 — Unified Broker Manager + brokers hardening.

### Added
- `BrokerManager` (`navigator_eventbus.brokers`) plus `RMQManager`, `RedisManager`, `SQSManager`
  (from their subpackages): publish and consume over a single connection, `auto_subscribe` flag,
  consumer-first shutdown.
- `BrokerProducer(endpoint_path=...)`: configurable HTTP publish route (default unchanged).
- `RabbitMQConnection.ensure_queue()` and `reject_message()`.

### Fixed
- RabbitMQ consumer: an explicit `exchange_name` is no longer overwritten by `queue_name` (D1).
- `RabbitMQBrokerHook.connect()` no longer raises `AttributeError`; hook failures settle once
  (non-requeued nack, no ack/republish).
- `queue_event()` no longer sleeps 100 ms per event.

### Changed
- **Behaviour change** — `RMQConsumer.event_subscribe()` / `SQSConsumer.event_subscribe()` wrap the
  callback once: callbacks now receive `(message, decoded_body)` as documented, instead of failing on
  a double wrap. Callers that compensated for the double wrap (e.g. unpacking an extra layer) should
  drop that workaround and accept `(message, body)` directly.

## [0.1.0] — Unreleased (pending tag + PyPI publish)

Supersedes `0.1.0rc2`. Ships the FEAT-319 (EventBus Consolidation) M1+M2
scope: envelope schema versioning and default-capable hooks routing.

### Added

- **Envelope `schema_version`** (M1): `EventEnvelope` now carries a
  trailing `schema_version: int = 1` field (frozen/slots preserved;
  positional-argument compatibility with pre-0.1.0 10-arg construction
  unaffected). New package-root exports: `ENVELOPE_SCHEMA_VERSION` and
  `UnsupportedSchemaVersion`.
  - Deserialization is lenient backwards (a missing `schema_version` key
    is treated as legacy version `1`) and strict forwards (`from_dict`
    raises `UnsupportedSchemaVersion` for a version greater than
    `ENVELOPE_SCHEMA_VERSION`, never silently downgrades). Non-int and
    out-of-range (`< 1`) values also raise `UnsupportedSchemaVersion`
    cleanly rather than a raw `TypeError`.
  - `evb_dlq` (Postgres DLQ table) gained a `schema_version` column
    (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, so existing deployments
    are retrofitted automatically on the next `ensure_table()` call) — the
    envelope's real version is now persisted on write and read back
    unchanged on replay, with the same legacy→1 tolerance for rows
    persisted before this column existed. `DLQHandler.replay()` isolates
    per-row failures (e.g. a stored version this reader doesn't support):
    the offending row is logged and left un-replayed rather than aborting
    the whole batch or being silently downgraded.
  - The `IngressEnvelope` boundary model gained a `schema_version` field
    (shape validation only) — required so post-0.1.0 clients sending the
    key are not rejected by `extra="forbid"`; all three in-process
    converters (`from_legacy_event`, `from_lifecycle_dict`,
    `from_hook_event`) emit `schema_version=1`.
  - `redis_streams`/`redis_pubsub` backends log `UnsupportedSchemaVersion`
    distinctly from generic undecodable-message drops (both still
    drop+ACK the entry — there is no DLQ hook at the backend layer — but
    operators can now tell rolling-upgrade version skew apart from
    corrupt/poison data in the logs).

### Changed

- **`HookManager` tri-state `route_to_bus`** (M2): the constructor
  signature is now `route_to_bus: Optional[bool] = None`. `None` (the
  new default) auto-routes hook events to the bus wire format iff a bus
  is attached via `set_event_bus`; explicit `True`/`False` behave exactly
  as before. The `route_to_bus` property now returns the *effective*
  resolved value rather than the raw flag.
  - **Behavior-change note for consumers**: previously the default was a
    strict `route_to_bus=False`. Any consumer that calls `set_event_bus`
    and relied on the implicit default will now emit hook events in the
    first-class envelope wire format (rather than the legacy
    `bus.emit(topic, event.model_dump())` shape) unless it explicitly
    passes `route_to_bus=False` to restore the old behavior. The direct
    callback path is unaffected in all cases (dual-emit is additive, never
    replaces the callback).
  - A one-time `INFO` log fires on the first auto-activation per bus
    attachment (`"route_to_bus auto-enabled: bus attached"`); the flag
    resets on every `set_event_bus` call (there is no separate "detach"
    method — calling it again with a new bus is a replace), so
    re-attachment logs again.

### Notes

- No new required dependencies. No changes to `EventBus.emit/subscribe/
  on/publish` signatures.
