# Changelog

All notable changes to `navigator-eventbus` are documented in this file.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

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
    `ENVELOPE_SCHEMA_VERSION`, never silently downgrades).
  - The same legacy→1 tolerance applies to DLQ Postgres row replay
    (`DLQHandler._row_to_envelope`), the `IngressEnvelope` boundary model
    (`schema_version` field added — required so post-0.1.0 clients
    sending the key are not rejected by `extra="forbid"`), and all three
    in-process converters (`from_legacy_event`, `from_lifecycle_dict`,
    `from_hook_event`), which emit `schema_version=1`.

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
    resets on `set_event_bus` (detach/replace), so re-attachment logs
    again.

### Notes

- No new required dependencies. No changes to `EventBus.emit/subscribe/
  on/publish` signatures.
