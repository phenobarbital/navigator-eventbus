---
# SDD flow type and base branch (FEAT-145).
# - type: feature  (default)  → base_branch: main (this project uses main)
# - type: hotfix              → base_branch MUST be: main
type: feature
base_branch: main
---

<!-- LANGUAGE: This document MUST be written entirely in English (proper nouns keep native spelling). -->

# Feature Specification: Inbound and Outbound Webhook Support

**Feature ID**: FEAT-431
**Date**: 2026-08-10
**Author**: Jesus
**Status**: implemented
**Target version**: navigator-eventbus 0.3.x

---

## 1. Motivation

Before this feature, `navigator-eventbus` had **no inbound webhook receiver**.
`ingress/` shipped only `websocket.py` and `grpc.py`; the only webhook-shaped
code was `lifecycle/subscribers/webhook.py`, which is **outbound** and bound to
the `lifecycle` `EventRegistry` rather than to `BusCore`.

Two artefacts showed the gap was anticipated but unfilled:

- `HOOK_TYPES` pre-registered the hook type `"webhook"` (`hooks/models.py:93`)
  and **no class claimed it**.
- `JiraWebhookConfig` and `GitHubWebhookConfig` existed as data models whose
  docstrings state the hook itself is implemented by the consuming application.

Meanwhile ai-parrot carried a working generic receiver
(`parrot.autonomous.webhooks.WebhookListener`) and a per-provider one
(`parrot/core/hooks/github_webhook.py`). This feature ports that design into
the package, corrects its known defects, and adds the missing bus-attached
outbound half.

## 2. Scope

**In scope**

1. Pluggable HMAC signature schemes usable in both directions.
2. A dynamic multi-endpoint listener (`WebhookListenerHook`) on a catch-all route.
3. A reusable per-provider base (`ProviderWebhookHook`) on a fixed route.
4. Payload preprocessing configured by in-process callable **or** import string.
5. An outbound `WebhookDeliverySubscriber` attached to `BusCore`.

**Out of scope**

- Concrete provider hooks (`GitHubWebhookHook`, `JiraWebhookHook`). The
  FEAT-312 extraction deliberately left integration logic in ai-parrot; this
  package ships the base class and its contract only.
- Distributed rate limiting — see §6.
- Consolidating the duplicated retry logic in `lifecycle/subscribers/webhook.py`
  onto `subscribers/_delivery.py` (follow-up; touching `lifecycle/` pulls
  FEAT-313's tests into scope).

## 3. Design decisions

### 3.1 Inbound lives in `hooks/`, not `ingress/`

Both packages accept external traffic and both are `BaseHook` subclasses, so
"is a BaseHook" does not discriminate. **Topic ownership does:** `ingress/`
receives bus-shaped input and the *caller* chooses the topic; `hooks/` receives
foreign-shaped input and the *package* derives the topic
(`hooks.<type>.<event>`). A webhook is in the second column on every axis
(input shape, topic ownership, dispatch target, auth model). Recorded as a
comparison table in `CONTEXT.md`.

### 3.2 `webhook_signatures.py` is top-level

Both directions need it (`verify` inbound, `sign` outbound). A neutral
top-level module keeps `subscribers/` from importing out of `hooks/`, matches
existing convention (`ingress_models.py`, `converters.py`), and guarantees
signing and verification cannot drift — proven by a round-trip test
parametrized over every registered scheme.

### 3.3 Webhook configs live outside `hooks/models.py`

**Deviation from convention, deliberate.** Every other hook config lives in
`hooks/models.py`, which is intentionally data-only. The webhook configs carry
behaviour (a `Callable` field, a validator that invokes `importlib`) and must
import `webhook_signatures` to validate scheme names. They live in
`hooks/webhook/models.py` to preserve `hooks/models.py`'s zero-behaviour,
narrow-import property. Do not "fix" this.

### 3.4 Serializable config with a live callable

A serializable `preprocessor: str` field plus a runtime `preprocessor_fn`
field marked `SkipJsonSchema[...]`, `exclude=True`. Not a
`Union[str, Callable]`: a union puts the function object into `model_dump()`
and breaks the YAML/JSON round trip the feature exists to support.
`validate_assignment` stays off so the `mode="after"` validator can assign to
`self` without re-entering validation.

### 3.5 Fail fast on config, fail soft at runtime

An unresolvable import string raises at construction, where an operator sees
it. A preprocessor that raises or times out at request time degrades to the
raw payload, records `preprocessor_error` in the event metadata, and still
returns `202` — a transform bug never costs a delivery.

### 3.6 No return-value heuristics

A returned `dict` is **always** the payload; it is never sniffed for
envelope-shaped keys. A real GitHub `check_run` body legitimately nests a
`payload` key, and any heuristic eventually misfires on live traffic. Richer
returns require the explicit `PreprocessResult` type.

### 3.7 Outbound queues rather than POSTing inline

`BusCore` applies `handler_timeout` (30 s default) to subscriber handlers.
Three HTTP attempts with backoff can hold a dispatch worker ~18 s per event;
under a burst of failures a small worker pool stalls entirely and every stalled
handler lands on `bus.subscriber_error` or in the DLQ. Buffering keeps the
handler O(1) and preserves isolation model B. Overload drops the **oldest**
buffered delivery and counts it, matching `AuditSubscriber`.

## 4. Corrections to the ported ai-parrot design

| # | Defect in the original | Correction |
|---|---|---|
| 1 | Endpoints keyed by `request.path` — breaks under a mounted prefix or a rewriting proxy | Key by the relative match-info id |
| 2 | Bare `asyncio.create_task` — GC may collect the task mid-flight | Tracked `_background_tasks` set + `add_done_callback(discard)`, matching `core.py:196`, `notification.py:478`, `dlq.py:218` |
| 3 | Flat `webhook.*` topics bypassing `TOPICS.md` governance | `hooks.webhook.*` via `HookManager` |
| 4 | `_list` route always exposed | Opt-in and token-guarded |
| 5 | Signature header probing across four candidates — a downgrade vector | One endpoint declares exactly one scheme |
| 6 | Stripe verification hashed the body alone and accepted a single `v1` | Signs `f"{t}." + body`; accepts **any** `v1` (key rotation); freshness checked before the HMAC |
| 7 | `POST <base_path>` (no trailing segment) silently unmatched | Explicit second route |
| 8 | Bare `web.Response(text=...)` bodies | Uniform `web.json_response` everywhere |

## 5. HTTP response contract

| Situation | Status | Body |
|---|---|---|
| Unknown endpoint | 404 | `{"status":"unknown_endpoint"}` |
| Endpoint disabled | 503 + `Retry-After: 60` | `{"status":"disabled"}` |
| Listener at capacity | 503 + `Retry-After: 1` | `{"status":"busy"}` |
| IP not allowed | 403 | `{"status":"forbidden"}` |
| Signature missing | 401 | `{"status":"unauthorized","reason":"missing_signature"}` |
| Signature malformed | 400 | `{"status":"bad_signature_format"}` |
| Signature mismatch | 401 | `{"status":"unauthorized"}` |
| Timestamp stale | 401 | `{"status":"unauthorized","reason":"stale"}` |
| Body over cap | 413 | `{"status":"payload_too_large","limit":N}` |
| Unparseable, non-JSON disallowed | 400 | `{"status":"invalid_payload"}` |
| Duplicate delivery id | 200 | `{"status":"duplicate"}` |
| Preprocessor raised | 202 | `{"status":"accepted","preprocessor":"failed"}` |
| Classified uninteresting | 200 | `{"status":"ignored"}` |
| Accepted | 202 | `{"status":"accepted","event_type":…,"hook_id":…}` |
| Internal error | 500 | `{"status":"error"}` |

**Why 200 on ignored.** GitHub, Jira and Stripe treat any non-2xx as a *failed
delivery*, retry it, and eventually disable the webhook. An ignored event was
genuinely delivered — received, authenticated, and deliberately not acted on.
Returning 4xx would misreport a delivery failure. 200 (no action) versus 202
(work queued) keeps access logs unambiguous without parsing bodies. The
converse holds: 401/403/413 are never softened, because they are real
misconfigurations an operator needs to see in the provider's dashboard.

## 6. Explicitly deferred: rate limiting

An in-process token bucket yields a per-worker limit that means nothing behind
a load balancer, and is worse than no limit because it looks like protection.
Rate limiting belongs at the reverse proxy or auth middleware. No dead config
field is shipped. What *is* shipped is the in-process-correct piece: a
`max_inflight` semaphore bounding concurrent deliveries (503 + `Retry-After: 1`
when exhausted).

## 7. Security notes

- HMAC is verified over the **raw** body, read once via `read_capped()`.
  Any middleware that reads, re-encodes or normalizes the body breaks
  verification; any middleware that already consumed `request.content` makes
  the streaming read return empty. `listener.base_path` and
  `provider_hook.url` are public precisely so operators can exclude them.
- A plain body HMAC (GitHub, Jira, `generic`) does **not** prevent replay — the
  signature never expires. Pair it with `dedup_header` + `dedup_ttl_seconds`.
  Dedup is per-process and best-effort, not a distributed guarantee.
- Stripe freshness uses `time.time()` (wall clock). It must never be changed to
  `loop.time()`, whose epoch is arbitrary. Hosts need a synchronised clock.
- A preprocessor import string is **arbitrary code execution by
  configuration**. Webhook config is trusted-operator input; never resolve an
  import string taken from a request or a user-writable store.
- Comparisons are constant-time; no response or log ever contains the expected
  digest, the received digest, or the secret.

## 8. Migration notes

`require_signature` defaults to **True**, so an endpoint with no secret is a
`ValidationError` rather than a silently unauthenticated route. This is
stricter than both predecessors (`GitHubWebhookConfig.secret_token` was
optional and simply skipped verification; ai-parrot's `WebhookListener`
allowed unauthenticated endpoints). Porting an existing config may now raise —
set `require_signature=False` to opt out deliberately.

Endpoint keying changed from absolute path to relative match-info id:
registration code that passed absolute paths must pass paths relative to
`base_path`.

## 9. Known divergences

- `inspect.isawaitable(result)` is used instead of
  `asyncio.iscoroutinefunction(fn)` (correct for `functools.partial` of a
  coroutine and for objects with an async `__call__`).
  `HookManager._build_dispatch` (`manager.py:124`) still uses the latter — a
  pre-existing wart, out of scope here.

## 10. Acceptance criteria

- [x] `pytest` — 232 new tests pass; no regression in the existing suite.
- [x] `ruff check src/ tests/` clean.
- [x] `mypy` clean on every new module.
- [x] A config built with a live callable still `model_dump(mode="json")`s and
      JSON-encodes.
- [x] `model_json_schema()` does not raise.
- [x] Sign/verify round trip passes for every registered scheme.
- [x] End-to-end: signed inbound POST → `hooks.webhook.*` → outbound delivery,
      with the outbound signature verifying under the same scheme.
- [x] `TOPICS.md` registers `hooks.webhook.*` and `bus.webhook_delivery_failed`.
