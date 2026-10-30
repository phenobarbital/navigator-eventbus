"""KeyedConsumer (FEAT-713): the worker loop over a KeyedQueue."""
import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Optional

from navconfig.logging import logging

from navigator_eventbus.queues.keyed.config import KeyedMessage, LeaseLost, Outcome
from navigator_eventbus.queues.keyed.store import KeyedLease, KeyedQueue, backoff_ms

__all__ = ("Handler", "KeyedConsumer", "backoff_ms")

Handler = Callable[[KeyedMessage], Awaitable[Outcome]]


class KeyedConsumer:
    """Run ``handler`` over leased heads until ``stop`` is set.

    Handler exceptions and timeouts become ``retry`` outcomes. That is only
    correct when the handler's side effect is idempotent or provably did not
    start: handlers talking to a non-idempotent downstream must catch
    ``asyncio.CancelledError`` / ``TimeoutError`` themselves and return their
    own outcome.

    Args:
        queue: The queue to consume.
        wait_ms: Long-poll window of each ``lease()``.
    """

    def __init__(self, queue: KeyedQueue, *, wait_ms: int = 1_000) -> None:
        self.queue = queue
        self.wait_ms = wait_ms
        self.logger = logging.getLogger("navigator_eventbus.queues.keyed")

    async def _sleep_paused(self) -> None:
        """Sleep ``min(pause PTTL, pause_poll_ms)`` — never touches the signal stream."""
        cfg = self.queue.config
        ttl = await self.queue._redis.pttl(self.queue._k.paused)
        delay = cfg.pause_poll_ms if ttl is None or ttl < 0 else min(ttl, cfg.pause_poll_ms)
        await asyncio.sleep(max(delay, 1) / 1000.0)

    async def run_once(self, handler: Handler) -> Optional[Outcome]:
        """One iteration: paused? sleep · recover · promote_due · lease · handle · apply.

        Returns:
            The applied outcome, or ``None`` (paused, nothing leasable, or the
            lease was lost before the outcome could be applied).
        """
        queue = self.queue
        if await queue.is_paused():
            await self._sleep_paused()
            return None
        await queue.recover()  # throttled inside (SET NX PX)
        await queue.promote_due()
        lease = await queue.lease(wait_ms=self.wait_ms)
        if lease is None:
            return None
        outcome = await self._handle(handler, lease)
        try:
            await queue.apply(lease, outcome)
        except LeaseLost as exc:
            self.logger.warning(
                "keyed queue %s: lease on key %r lost before apply (%s); outcome %s dropped",
                queue.config.name, lease.key, exc, outcome.kind,
            )
            return None
        return outcome

    async def _handle(self, handler: Handler, lease: KeyedLease) -> Outcome:
        """Run the handler under ``call_timeout_ms`` with the lock kept extended."""
        cfg = self.queue.config
        keeper = asyncio.ensure_future(self._keep_extended(lease))
        try:
            outcome = await asyncio.wait_for(
                handler(lease.message), timeout=cfg.call_timeout_ms / 1000.0
            )
            if not isinstance(outcome, Outcome):
                raise TypeError(f"handler returned {type(outcome).__name__}, not Outcome")
            return outcome
        except asyncio.TimeoutError:
            return Outcome.retry(reason=f"handler timed out after {cfg.call_timeout_ms} ms")
        except Exception as exc:  # noqa: BLE001 — exceptions become retry outcomes
            self.logger.warning(
                "keyed queue %s: handler failed for %s: %r",
                cfg.name, lease.message.message_id, exc,
            )
            return Outcome.retry(reason=f"{type(exc).__name__}: {exc}")
        finally:
            keeper.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await keeper

    async def _keep_extended(self, lease: KeyedLease) -> None:
        """Extend lock + signal every ``lock_ttl_ms / 3`` until cancelled."""
        interval = self.queue.config.lock_ttl_ms / 3000.0
        while True:
            await asyncio.sleep(interval)
            try:
                await lease.extend()
            except LeaseLost as exc:
                self.logger.warning("lease on key %r could not be extended: %s", lease.key, exc)
                return

    async def run(self, handler: Handler, *, stop: asyncio.Event) -> None:
        """Loop :meth:`run_once` until ``stop`` is set.

        An unexpected error in one iteration is logged and the loop resumes
        after ``pause_poll_ms``.
        """
        while not stop.is_set():
            try:
                await self.run_once(handler)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                self.logger.exception("keyed consumer iteration failed")
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        stop.wait(), timeout=self.queue.config.pause_poll_ms / 1000.0
                    )
