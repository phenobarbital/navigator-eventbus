---
# SDD flow type and base branch (FEAT-145).
type: feature
base_branch: main
---

<!-- LANGUAGE: This document MUST be written entirely in English (proper nouns keep native spelling). -->

# Feature Specification: SQS-style HTTP Pull Queues

**Feature ID**: FEAT-432
**Date**: 2026-08-14
**Author**: Jesus
**Status**: implemented
**Target version**: navigator-eventbus 0.3.x

---

## 1. Motivation

The request: expose a URL, have a producer POST a structured payload, and let
**other systems fetch it** — possibly written in other languages, with no
Redis access and no Python runtime. That is Amazon SQS semantics.

What the package already had, verified in code:

- **The queue engine.** `backends/redis_streams.py` issues real consumer-group
  commands (`XGROUP CREATE`, `XREADGROUP`, `XACK`, `XPENDING`, `XAUTOCLAIM`).
  Redis Streams consumer groups *are* "queue per group, fan-out across
  groups", and the pending-entries list *is* SQS's in-flight set.
- **The wire contract.** `IngressEnvelope` is `extra="forbid"`, `frozen=True`.

What it did not have: **any pull API at all**. All six routes in the package
were inbound; nothing returned events to a client.

## 2. The architectural constraint

`BusCore._invoke_with_retry` (`core.py:549-580`) **never re-raises** —
isolation model B retries, routes to the DLQ, and returns normally. So the
"callback raised → entry stays pending → `XAUTOCLAIM` redelivers" path in
`redis_streams.py` is **dead** whenever `BusCore` is the consumer: a
business-logic failure is ACKed anyway. FEAT-320's spec says this outright.

`TransportBackend` cannot express the alternative either: three methods, and
`OnEnvelope` returns `Awaitable[None]` — no return value, no ack handle.
Changing it is a declared non-goal in both FEAT-320 and FEAT-430.

**Therefore the queue plane owns its lease lifecycle end to end and never
routes through `BusCore`.** Queues are a plane parallel to the bus, joined by
two opt-in, unidirectional bridges.

## 3. Redis semantics — verified, not assumed

Four load-bearing behaviours were probed against a live Redis 7.0.15 **before**
the store was written, because a wrong assumption here would have been
invisible: the unit-tier fake and the implementation would have shared the
misunderstanding and agreed with each other.

| Assumption | Result |
|---|---|
| `XCLAIM ... JUSTID` does not increment `times_delivered` | **Confirmed** — stays 1; without `JUSTID` it goes to 2 |
| `XCLAIM ... IDLE n` sets the *absolute* idle time | **Confirmed** — idle read back as exactly 5000/9000ms |
| `XAUTOCLAIM` returns a 3-tuple on 7.0+ | **Confirmed** |
| `XINFO GROUPS` exposes `lag` and `pending` | **Confirmed** |

The offset mechanism was then verified end to end: with `idle=9000`,
`XAUTOCLAIM` at `min_idle_time=10000` claims nothing (lease alive) and at
`8000` claims it (lease expired). `tests/queues/test_integration.py` keeps
all four assertions permanently.

## 4. Design decisions

### 4.1 Visibility timeout: the `IDLE`-offset trick

Redis has **no per-message visibility timeout**. It has one monotonic idle
clock per pending entry, and the threshold lives on the *reader*. So a
per-receive lease is expressed by moving the message's clock, not the
threshold: fix a queue-wide reclaim threshold `T`, then `XCLAIM ... IDLE
(T - v) JUSTID` makes the entry cross `T` exactly `v` ms from now.

Two properties make this safe:

- **Failure direction.** If the `XCLAIM` fails, the entry keeps `idle=0` and
  is reclaimed after the full `T` instead of after `v` — late redelivery,
  never early double-delivery.
- **Free nack.** `v=0` → `idle=T` → immediately reclaimable. A real negative
  acknowledgement, which the push `TransportBackend` structurally cannot
  express. This is the single strongest justification for the subpackage.

The `XCLAIM` is skipped entirely when `v == T`, so a queue that never varies
its lease pays no extra round-trip.

### 4.2 Receive order: reclaim first, then read new

`XPENDING` → `XAUTOCLAIM` → *then* `XREADGROUP >`. `XAUTOCLAIM` cannot block,
so after a blocking read it would be delayed by the whole long-poll window;
and on a busy queue, reading new entries first would always fill the batch
and the pending list would never be scanned — expired leases would live
forever. The `max_receives` cap must also be evaluated *before* a poison
message is handed out again.

### 4.3 `XACK` only — never `XDEL`

`XDEL` removes the entry from the **stream**, so a group that has not read it
yet loses it permanently. Under the fan-out-across-groups requirement that is
a data-loss bug. `delete_entries=True` exists but is validated to require
exactly one group.

### 4.4 Stateless, signed receipt handles

A server-side token map dies with the second replica (receive on A, delete on
B). The handle is HMAC-signed and carries `queue/group/stream/id/consumer/
delivered/expiry`.

- The MAC is verified **before** the claims JSON is parsed — the same ordering
  rule as `hooks/webhook/receiver.py`.
- All keys are checked with no early exit, so timing reveals nothing.
- `decode()` requires the queue and group **from the route/request** and
  compares them. Acking another group's copy would silently discard a message
  that group never saw; this check is what makes fan-out safe.
- Expiry is enforced strictly (user's decision): with a shared pending list,
  honouring an expired handle is exactly the ack that would delete a message
  another consumer is processing *right now*. `receipt_grace_seconds` is the
  escape hatch.
- `BUS_QUEUE_RECEIPT_KEYS` is **required** — never a per-process random key,
  which would break every multi-replica deployment and every restart with a
  near-undiagnosable symptom.

`webhook_signatures.py` is deliberately **not** reused: its `sign()` returns
HTTP headers and `verify()` takes headers plus a raw body. Its four security
conventions are followed.

### 4.5 Consumer naming

One consumer name **per replica**, never per request: Redis keeps a consumer
registry per group indefinitely, so per-request names leak memory with no
upside. `XACK` is group-scoped, so a delete works from any replica.
`prune_consumers()` removes idle, pending-free registrations.

### 4.6 Prefix-collision guard

`RedisStreamsBackend._refresh_streams` SCANs `f"{stream_prefix}*"` and joins a
group on **anything it finds**. If an operator set `BUS_STREAM_PREFIX="evb:"`,
the bus would discover every queue stream, consume it and **auto-ACK** it
behind the HTTP consumers' backs. `assert_prefixes_disjoint` makes that a
startup `ValueError`.

## 5. Corrections made during implementation

Two real bugs were found by the work itself and are worth recording:

1. **String-compared stream ids.** An early `_delivery_counts` used
   `min()`/`max()` over entry ids as strings, where `"10-0" < "9-0"`
   lexically. Removed entirely: fresh reads are `times_delivered == 1` by
   definition, and reclaims already know their count from the `XPENDING`
   scan — so the round-trip disappeared along with the bug.
2. **Two clocks.** `QueueStore` had an injectable clock but `api.py` called
   `time.time()` directly, so handles minted on one timeline were judged
   against another. `QueueStore.decode_receipt()` now owns both minting and
   verification, making divergence impossible.

## 6. Correction to the original plan

The plan claimed a "packaging trap": that `import navigator_eventbus` would
break without the `[redis]` extra. **That is false.** `navconfig[default]` — a
*core* dependency — requires `redis~=5.2.1` and imports it at module level, so
redis is always present. The lazy exports were kept because they do deliver a
real, smaller benefit (the store/API machinery is not imported until a queue
is touched), and the docstrings now state the true reason.

## 7. HTTP surface

`POST {base}/{queue}/messages` (and `/batch`), `POST .../messages/receive`,
`.../delete`, `.../visibility`, `GET {base}/{queue}` (describe), plus opt-in
admin routes `GET {base}` (list), `POST .../purge`, `GET .../dlq`.

Status codes: 201 send, 200 receive/delete/visibility/describe, 207 partial
batch, 400 invalid payload/parameter/receipt, 401 unauthorized, 403 wrong
role, 404 unknown queue, 409 stale receipt, 413 too large, 429 busy, 501 no
DLQ handler, 503 Redis unavailable.

Note the deliberate divergence from `hooks/webhook`, which answers 200 to
ignored deliveries so a third-party provider does not disable the webhook.
A queue client is our own SDK-style caller, so real errors get real codes.

**Queue creation is deliberately not exposed.** A queue's identity includes
its groups, threshold and retention; creating them over HTTP is an unbounded
stream-creation path guarded only by a bearer token.

## 8. Auth

Per-queue tokens split by role (producer / consumer / admin), falling back to
a shared token, then `BUS_QUEUE_TOKEN`, then `BUS_INGRESS_TOKEN`. Nothing
configured means refuse everything — the same posture as `WebSocketIngress`.
Roles are asymmetric in blast radius: producer injects, consumer *drains and
deletes*, admin purges irreversibly.

## 9. Known limits

- **At-least-once, duplicates are possible by design.** If a process dies (or
  a client disconnects) after `XREADGROUP` returns but before the HTTP
  response is written, the message sits in the pending list unseen and is
  redelivered after the lease. HTTP response delivery is not transactional
  with the Redis read; no design fixes this. **Consumers must be idempotent.**
- **`mirror_to_bus` is a non-atomic dual write.** The queue stream is the
  source of truth; a mirror failure is logged, not fatal.
- **No delayed delivery.** Redis Streams has none; faking it needs a sorted-set
  scheduler. `approximate_number_of_messages_delayed` is always 0.
- **Rate limiting is out of scope**, deliberately: an in-process token bucket
  gives a per-worker limit that is meaningless behind a load balancer and
  worse than nothing because it looks like protection. `max_inflight_receives`
  bounds concurrency (429), which is the honest in-process guarantee.
- **Reclaim cost at scale is unmeasured** — every receive runs `XPENDING` +
  `XAUTOCLAIM` bounded by `count=max_messages`. Alarm on pending-list size.

## 10. Acceptance criteria

- [x] 183 new tests pass; full suite 760 passed, 1 skipped.
- [x] `ruff` and `mypy` clean on every new module.
- [x] 12 integration tests pass against a live Redis 7.0.15.
- [x] The four Redis assumptions are asserted, not assumed.
- [x] Two groups each receive every message; two consumers in one group are disjoint.
- [x] `visibility=0` produces an immediate redelivery (real nack).
- [x] An envelope written by `RedisStreamsBackend.publish` decodes through
      `QueueStore` and vice versa (wire-format anti-drift regression).
- [x] `TOPICS.md` registers `queue.*`, `bus.queue_dlq`, `bus.queue_purged`.
