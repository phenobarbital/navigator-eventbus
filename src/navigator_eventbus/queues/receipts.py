"""Stateless, HMAC-signed receipt handles for the pull-queue API.

A *receipt handle* is the capability a consumer presents to delete a message
it received, or to change that message's visibility. It is the queue plane's
equivalent of an SQS ``ReceiptHandle``.

**Why signed rather than server-side state.** A token table kept in the API
process dies the moment there are two replicas behind a load balancer: a
consumer that received from replica A and deletes against replica B would be
rejected. A signed handle carries its own claims, so any replica holding the
same key can verify it — no shared store, no TTL eviction, no stickiness
requirement.

The handle format is ``q1.<b64url(claims)>.<b64url(mac)>`` where the MAC
covers ``"q1.<b64url(claims)>"``.

This module is deliberately dependency-free — no redis, no aiohttp — so the
whole security surface is unit-testable in isolation.

.. warning::
   A receipt handle is a **bearer capability**: anyone holding it can delete
   the message. Serve the queue API over TLS, and keep requiring the
   consumer auth token on delete — the handle authorizes *which message*,
   never *who* is asking.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

__all__ = ("HANDLE_VERSION", "Receipt", "ReceiptCodec", "ReceiptError")

#: Version prefix, so the format can change without silently mis-parsing.
HANDLE_VERSION = "q1"


def _b64(raw: bytes) -> str:
    """URL-safe base64 without padding."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _unb64(value: str) -> bytes:
    """Inverse of :func:`_b64`, restoring the stripped padding."""
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


class ReceiptError(Exception):
    """A receipt handle could not be accepted.

    Attributes:
        reason: A stable, non-sensitive slug — one of ``malformed``,
            ``bad_version``, ``bad_signature``, ``wrong_queue``, ``expired``.
            Safe to log. The HTTP layer deliberately collapses the first four
            into a single client-facing status so a probing caller cannot
            learn *why* a handle was rejected.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class Receipt:
    """The claims carried inside a receipt handle.

    Attributes:
        queue: Queue name the message was received from.
        group: Consumer group the message was delivered to.
        stream: Redis stream key backing the queue.
        message_id: Redis Streams entry id.
        consumer: Consumer name that owns the pending entry. Needed because
            ``XCLAIM`` must re-assign to the *same* consumer when only the
            visibility is being changed.
        delivered: ``times_delivered`` at the moment the handle was minted —
            the lease *generation*. Lets ``delete`` detect that the message
            was reclaimed by someone else since.
        expires_at_ms: POSIX time in milliseconds when the lease ends.
    """

    queue: str
    group: str
    stream: str
    message_id: str
    consumer: str
    delivered: int
    expires_at_ms: int


class ReceiptCodec:
    """Mints and verifies receipt handles.

    Several keys may be supplied to support rotation: the **first** key signs
    every new handle, and **all** of them are accepted on verification. To
    rotate, prepend the new key, deploy everywhere, then drop the old one
    once every outstanding lease has expired. This mirrors the reason Stripe
    sends several ``v1=`` signatures at once, documented in
    :mod:`navigator_eventbus.webhook_signatures`.

    Args:
        keys: Signing keys, most-preferred first. Must be non-empty and
            contain no empty string.
        grace_ms: Extra tolerance beyond ``expires_at_ms`` before a handle is
            rejected as expired. Defaults to 0 (strict).

    Raises:
        ValueError: No keys, or any key is empty.
    """

    def __init__(self, keys: Sequence[str], *, grace_ms: int = 0) -> None:
        if not keys or not all(keys):
            raise ValueError(
                "ReceiptCodec requires at least one non-empty signing key. "
                "Configure BUS_QUEUE_RECEIPT_KEYS — never fall back to a "
                "per-process random key, which breaks every multi-replica "
                "deployment and every restart."
            )
        if grace_ms < 0:
            raise ValueError("grace_ms must not be negative")
        self._keys = tuple(key.encode("utf-8") for key in keys)
        self._grace_ms = grace_ms

    def encode(self, receipt: Receipt) -> str:
        """Mint a signed handle for *receipt*.

        Args:
            receipt: The claims to embed.

        Returns:
            The opaque handle string.
        """
        claims = {
            "q": receipt.queue,
            "g": receipt.group,
            "s": receipt.stream,
            "i": receipt.message_id,
            "c": receipt.consumer,
            "d": receipt.delivered,
            "e": receipt.expires_at_ms,
        }
        body = _b64(
            json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("utf-8")
        )
        mac = hmac.new(self._keys[0], self._signed_part(body), hashlib.sha256).digest()
        return f"{HANDLE_VERSION}.{body}.{_b64(mac)}"

    def decode(
        self, handle: str, *, queue: str, group: str, now_ms: int
    ) -> Receipt:
        """Verify *handle* and return its claims.

        The order here is load-bearing and mirrors the rule stated in
        :mod:`navigator_eventbus.hooks.webhook.receiver`: **the MAC is
        verified before the claims are parsed**, so attacker-controlled JSON
        never reaches the parser on an unauthenticated request.

        Args:
            handle: The handle presented by the consumer.
            queue: Queue taken from the request route — *not* from the
                handle. Compared against the signed claim.
            group: Consumer group taken from the request body, likewise
                compared.
            now_ms: Current POSIX time in milliseconds.

        Returns:
            The verified :class:`Receipt`.

        Raises:
            ReceiptError: With a stable ``reason`` slug.
        """
        if not isinstance(handle, str):
            raise ReceiptError("malformed")
        parts = handle.split(".")
        if len(parts) != 3:
            raise ReceiptError("malformed")
        version, body, signature = parts
        if version != HANDLE_VERSION:
            raise ReceiptError("bad_version")

        try:
            received = _unb64(signature)
        except Exception:  # noqa: BLE001 — any decode failure is malformed
            raise ReceiptError("malformed") from None

        # Check EVERY key with no early exit, so elapsed time does not reveal
        # which key matched (or that none did).
        signed = self._signed_part(body)
        matched = False
        for key in self._keys:
            expected = hmac.new(key, signed, hashlib.sha256).digest()
            matched = hmac.compare_digest(expected, received) or matched
        if not matched:
            raise ReceiptError("bad_signature")

        receipt = self._parse_claims(body)

        # Bind the handle to the route. Without this, a consumer could ack a
        # message belonging to a DIFFERENT group — silently discarding a
        # message that group never saw. This check is what makes the
        # fan-out-across-groups guarantee safe.
        if receipt.queue != queue or receipt.group != group:
            raise ReceiptError("wrong_queue")

        if now_ms > receipt.expires_at_ms + self._grace_ms:
            # Stricter than real SQS on purpose: with a shared pending-entries
            # list, honouring an expired handle is exactly the ack that would
            # delete a message another consumer is processing right now.
            raise ReceiptError("expired")
        return receipt

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _signed_part(body: str) -> bytes:
        """Return the exact byte string the MAC is computed over."""
        return f"{HANDLE_VERSION}.{body}".encode("ascii")

    @staticmethod
    def _parse_claims(body: str) -> Receipt:
        """Decode verified claims into a :class:`Receipt`."""
        try:
            claims: dict[str, Any] = json.loads(_unb64(body))
            return Receipt(
                queue=str(claims["q"]),
                group=str(claims["g"]),
                stream=str(claims["s"]),
                message_id=str(claims["i"]),
                consumer=str(claims["c"]),
                delivered=int(claims["d"]),
                expires_at_ms=int(claims["e"]),
            )
        except Exception:  # noqa: BLE001 — bad shape on a verified body
            raise ReceiptError("malformed") from None
