"""Direct administrative drain for keyed queues (FEAT-713 M4).

Leaderless: it takes the same per-key lock as the consumers, so it can never
double-deliver. Concurrent across keys (semaphore), strictly sequential
inside a key, bounded per handler call, fail-fast on consecutive retries.
The lock is taken per head (lease → apply releases it); a consumer grabbing
the key between two heads makes the drain count it ``busy`` and stop that key.
"""
import asyncio
import contextlib
import time
from dataclasses import dataclass
from typing import Optional

from navconfig.logging import logging

from navigator_eventbus.queues.keyed.config import DrainReport, LeaseLost, Outcome
from navigator_eventbus.queues.keyed.consumer import Handler
from navigator_eventbus.queues.keyed.store import KeyedLease, KeyedQueue

__all__ = ("drain", "drain_key")

_logger = logging.getLogger("navigator_eventbus.queues.keyed")


@dataclass
class _State:
    """Shared fail-fast state of one ``drain`` call."""

    stop_after: int
    consecutive_retries: int = 0


async def drain(
    queue: KeyedQueue,
    handler: Handler,
    *,
    key_prefix: Optional[str] = None,
    concurrency: int = 10,
    call_timeout_ms: Optional[int] = None,
    stop_after_failures: int = 5,
    include_held: bool = False,
    ignore_pause: bool = False,
) -> DrainReport:
    """Flush the queue directly through ``handler``.

    ``gather(drain_key(k))`` over (keys ∪ delayed-keys) whose name starts with
    ``key_prefix`` (``None`` = every key) under a ``Semaphore(concurrency)``.
    Ignores retry delays (``due_ms``), never ``wait`` deadlines. A key locked
    by a live consumer counts as ``busy`` and is skipped. Stops launching new
    keys after ``stop_after_failures`` consecutive retry outcomes.

    Args:
        queue: The queue to drain.
        handler: Same contract as a consumer handler.
        key_prefix: Only drain keys starting with this prefix.
        concurrency: Max keys in flight.
        call_timeout_ms: Per handler call bound (default ``call_timeout_ms``).
        stop_after_failures: Consecutive ``retry`` outcomes that trip fail-fast.
        include_held: Release holds first and drain those keys too.
        ignore_pause: Drain even while the queue is paused.

    Returns:
        A :class:`DrainReport`.
    """
    started = time.monotonic()
    report = DrainReport()
    if not ignore_pause and await queue.is_paused():
        report.stopped_early = True
        return report
    timeout = call_timeout_ms or queue.config.call_timeout_ms
    names = set(await queue.keys())
    names.update(
        str(k) if not isinstance(k, bytes) else k.decode()
        for k in await queue._redis.zrange(queue._k.delayed, 0, -1)
    )
    if key_prefix:
        names = {n for n in names if n.startswith(key_prefix)}
    candidates = sorted(names)
    held_keys = {
        (k.decode() if isinstance(k, bytes) else k)
        for k in (await queue._redis.hgetall(queue._k.held))
    }
    if include_held:
        for key in candidates:
            if key in held_keys:
                await queue.release_hold(key)
    else:
        candidates = [k for k in candidates if k not in held_keys]
    report.keys_seen = len(candidates)
    state = _State(stop_after=stop_after_failures)
    sem = asyncio.Semaphore(max(1, concurrency))
    expired_before = queue.expired_count
    await asyncio.gather(
        *(
            drain_key(
                queue, key, handler, sem,
                call_timeout_ms=timeout, counters=report, state=state,
            )
            for key in candidates
        )
    )
    report.expired = queue.expired_count - expired_before
    report.elapsed_ms = int((time.monotonic() - started) * 1000)
    return report


async def _run_handler(
    queue: KeyedQueue, handler: Handler, lease: KeyedLease, timeout_ms: int
) -> Outcome:
    """Handler under ``wait_for`` with the lock kept extended."""

    async def keep() -> None:
        while True:
            await asyncio.sleep(queue.config.lock_ttl_ms / 3000.0)
            try:
                await lease.extend()
            except LeaseLost:
                return
            except Exception as exc:  # noqa: BLE001 — transient Redis error: retry next tick
                _logger.warning("drain: extend failed for %r (%r); retrying", lease.key, exc)

    keeper = asyncio.ensure_future(keep())
    try:
        outcome = await asyncio.wait_for(handler(lease.message), timeout=timeout_ms / 1000.0)
        if not isinstance(outcome, Outcome):
            raise TypeError(f"handler returned {type(outcome).__name__}, not Outcome")
        return outcome
    except asyncio.TimeoutError:
        return Outcome.retry(reason=f"handler timed out after {timeout_ms} ms")
    except Exception as exc:  # noqa: BLE001
        return Outcome.retry(reason=f"{type(exc).__name__}: {exc}")
    finally:
        keeper.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await keeper


async def drain_key(
    queue: KeyedQueue,
    key: str,
    handler: Handler,
    sem: asyncio.Semaphore,
    *,
    call_timeout_ms: int,
    counters: DrainReport,
    state: Optional[_State] = None,
) -> None:
    """Lease heads of one key in order until a non-ack outcome, busy, or empty.

    Args:
        queue: The queue.
        key: The key to drain.
        handler: Handler for each head.
        sem: Concurrency limiter shared by the whole drain.
        call_timeout_ms: Per handler call bound.
        counters: The shared report, updated in place.
        state: Shared fail-fast state (``None`` disables fail-fast).
    """
    async with sem:
        while True:
            if counters.stopped_early:
                return
            lease, reason = await queue._lease_key_detailed(key, ignore_due=True)
            if lease is None:
                if reason == "busy":
                    counters.busy += 1
                return
            outcome = await _run_handler(queue, handler, lease, call_timeout_ms)
            try:
                await queue.apply(lease, outcome)
            except LeaseLost:
                _logger.warning("drain: lease on key %r lost before apply", key)
                counters.busy += 1
                return
            if outcome.kind == "ack":
                counters.acked += 1
                if state is not None:
                    state.consecutive_retries = 0
                continue
            if outcome.kind == "retry":
                counters.retried += 1
                if state is not None:
                    state.consecutive_retries += 1
                    if state.consecutive_retries >= state.stop_after:
                        counters.stopped_early = True
            elif outcome.kind == "wait":
                counters.waited += 1
            else:
                counters.held += 1
            return
