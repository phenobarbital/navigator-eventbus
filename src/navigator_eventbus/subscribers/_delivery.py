"""HTTP delivery primitive shared by outbound webhook subscribers.

Extracted from the retry logic in
:mod:`navigator_eventbus.lifecycle.subscribers.webhook` with three upgrades:

- ``429 Too Many Requests`` is retryable (the original treated every 4xx as
  permanent, so a rate-limited endpoint silently lost events).
- Backoff is **capped** and uses **full jitter**, so a fleet of subscribers
  recovering from the same outage does not retry in lockstep.
- The retryable-status set is configurable rather than hard-coded.
"""
from __future__ import annotations

import asyncio
import random
from collections.abc import Iterable
from typing import Optional

import aiohttp
from navconfig.logging import logging

__all__ = ("DeliveryOutcome", "HttpDelivery")


class DeliveryOutcome:
    """Result of one delivery attempt sequence."""

    __slots__ = ("delivered", "attempts", "status", "error")

    def __init__(
        self,
        delivered: bool,
        attempts: int,
        status: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        self.delivered = delivered
        self.attempts = attempts
        self.status = status
        self.error = error

    def __repr__(self) -> str:
        return (
            f"<DeliveryOutcome delivered={self.delivered} "
            f"attempts={self.attempts} status={self.status}>"
        )


class HttpDelivery:
    """Bounded-retry HTTP POST with a reusable ``aiohttp`` session.

    Args:
        url: Destination endpoint.
        timeout_seconds: Per-request timeout.
        max_attempts: Total attempts, including the first.
        backoff_base: Base delay in seconds for the exponential schedule.
        backoff_max: Ceiling for a single backoff sleep.
        jitter: Apply full jitter — sleep a uniform random duration in
            ``[0, delay]`` rather than exactly ``delay``.
        retry_statuses: Non-5xx status codes that should still be retried.
            5xx is always retried; every other 4xx is permanent.
        logger: Logger to report failures on.
    """

    def __init__(
        self,
        *,
        url: str,
        timeout_seconds: float = 5.0,
        max_attempts: int = 3,
        backoff_base: float = 0.5,
        backoff_max: float = 30.0,
        jitter: bool = True,
        retry_statuses: Optional[Iterable[int]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._url = url
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._max_attempts = max(1, max_attempts)
        self._backoff_base = backoff_base
        self._backoff_max = backoff_max
        self._jitter = jitter
        self._retry_statuses = set(retry_statuses or (429,))
        self._session: Optional[aiohttp.ClientSession] = None
        self.logger = logger or logging.getLogger(
            "navigator_eventbus.subscribers.delivery"
        )

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Lazily create or reuse the client session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def aclose(self) -> None:
        """Close the underlying session. Safe to call more than once."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    def _should_retry(self, status: int) -> bool:
        """Whether *status* warrants another attempt."""
        return status >= 500 or status in self._retry_statuses

    def _delay_for(self, attempt: int) -> float:
        """Backoff duration before the attempt after *attempt*."""
        delay = min(self._backoff_max, self._backoff_base * (2 ** (attempt - 1)))
        return random.uniform(0, delay) if self._jitter else delay

    async def post(self, body: bytes, headers: dict[str, str]) -> DeliveryOutcome:
        """POST *body*, retrying transient failures with capped backoff.

        Args:
            body: The encoded request body.
            headers: Request headers, including any signature.

        Returns:
            The :class:`DeliveryOutcome`. Never raises for a transport or
            HTTP failure — outbound delivery must not disturb the caller.
        """
        session = await self._ensure_session()
        last_status: Optional[int] = None
        last_error: Optional[str] = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                async with session.post(
                    self._url, data=body, headers=headers
                ) as response:
                    last_status = response.status
                    if 200 <= response.status < 300:
                        return DeliveryOutcome(True, attempt, response.status)
                    if not self._should_retry(response.status):
                        self.logger.warning(
                            "Webhook delivery to %s returned %d — not retrying",
                            self._url,
                            response.status,
                        )
                        return DeliveryOutcome(
                            False, attempt, response.status, "permanent"
                        )
                    self.logger.warning(
                        "Webhook delivery to %s returned %d (attempt %d/%d)",
                        self._url,
                        response.status,
                        attempt,
                        self._max_attempts,
                    )
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                self.logger.warning(
                    "Webhook delivery to %s failed (attempt %d/%d): %s",
                    self._url,
                    attempt,
                    self._max_attempts,
                    exc,
                )

            if attempt < self._max_attempts:
                await asyncio.sleep(self._delay_for(attempt))

        self.logger.error(
            "Webhook delivery to %s exhausted %d attempts",
            self._url,
            self._max_attempts,
        )
        return DeliveryOutcome(
            False, self._max_attempts, last_status, last_error or "exhausted"
        )
