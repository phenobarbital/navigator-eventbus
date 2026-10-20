"""Pluggable HMAC signature schemes for inbound and outbound webhooks.

Every provider signs webhook deliveries differently: GitHub uses
``X-Hub-Signature-256: sha256=<hex>``, Jira uses ``X-Hub-Signature`` (with
either ``sha1=`` or ``sha256=``), Stripe uses a timestamped
``Stripe-Signature: t=<unix>,v1=<hex>`` scheme. This module reduces those
differences to one strategy interface so both directions of the webhook
fabric share a single implementation:

- **Inbound** (:mod:`navigator_eventbus.hooks.webhook`) calls :meth:`~SignatureScheme.verify`.
- **Outbound** (:mod:`navigator_eventbus.subscribers.webhook`) calls :meth:`~SignatureScheme.sign`.

Because signing and verification live on the same object they cannot drift
apart — see the round-trip test in ``tests/test_webhook_signatures.py``.

Security invariants held by every scheme in this module:

- Comparison is **always** constant-time (:func:`hmac.compare_digest`) over
  case-folded bytes. Never ``==``, never a length check first.
- Verification operates on the **raw request body**. A parsed-then-reserialized
  body will not produce the same digest.
- No scheme probes multiple candidate headers. Header probing lets an attacker
  choose the weakest scheme on offer, so an endpoint declares exactly one.
- Neither the expected digest, the received digest, nor the secret is ever
  placed in a :class:`SignatureCheck` — ``detail`` carries a stable category
  string only, safe to log and to return to the caller.

.. warning::
   A plain HMAC over the body (GitHub, Jira, ``generic``) does **not** protect
   against replay: the signature stays valid forever. Only timestamped schemes
   (Stripe) enforce a freshness window. For the others, pair the scheme with
   delivery-id de-duplication (``dedup_header`` on the endpoint config).
"""
from __future__ import annotations

import hashlib
import hmac
import time
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Optional

__all__ = (
    "HexDigestScheme",
    "SignatureCheck",
    "SignatureScheme",
    "SignatureVerdict",
    "StripeScheme",
    "available_signature_schemes",
    "get_signature_scheme",
    "register_signature_scheme",
)

#: Digest algorithms a scheme may name. Keys are the wire-format prefixes.
_ALGORITHMS = {
    "sha1": hashlib.sha1,
    "sha256": hashlib.sha256,
    "sha512": hashlib.sha512,
}


class SignatureVerdict(str, Enum):
    """Outcome of a signature verification.

    The distinction between :attr:`MALFORMED` and :attr:`MISMATCH` is what
    lets the HTTP layer answer ``400`` for a syntactically broken header and
    ``401`` for a well-formed but wrong signature.
    """

    OK = "ok"
    MISSING = "missing"
    MALFORMED = "malformed"
    MISMATCH = "mismatch"
    STALE = "stale"


@dataclass(frozen=True)
class SignatureCheck:
    """Result of verifying one delivery's signature.

    Attributes:
        verdict: The outcome category.
        detail: A stable, non-sensitive category string (e.g.
            ``"unsupported_alg"``, ``"not_hex"``). Never contains a digest,
            a secret, or any attacker-supplied value.
    """

    verdict: SignatureVerdict
    detail: Optional[str] = None

    @property
    def ok(self) -> bool:
        """Whether the signature verified successfully."""
        return self.verdict is SignatureVerdict.OK


class SignatureScheme(ABC):
    """Strategy for one provider's webhook signing convention.

    Subclasses implement a matched :meth:`verify` / :meth:`sign` pair so the
    same object can guard an inbound route and sign an outbound delivery.

    Attributes:
        name: Registry key, e.g. ``"github"``.
        header: Default HTTP header carrying the signature.
    """

    name: str = ""
    header: str = ""

    @abstractmethod
    def verify(
        self,
        *,
        secret: str,
        body: bytes,
        headers: Mapping[str, str],
        header_override: Optional[str] = None,
        tolerance_seconds: int = 300,
        now: Optional[float] = None,
    ) -> SignatureCheck:
        """Verify the signature on an inbound delivery.

        Args:
            secret: The shared secret configured for the endpoint.
            body: The **raw** request body bytes, exactly as received.
            headers: Request headers (case-insensitive mapping expected;
                aiohttp's ``CIMultiDict`` satisfies this).
            header_override: Read the signature from this header instead of
                :attr:`header`.
            tolerance_seconds: Freshness window for timestamped schemes.
                Ignored by schemes that carry no timestamp.
            now: Current POSIX time, injectable for tests. Defaults to
                :func:`time.time`.

        Returns:
            A :class:`SignatureCheck` describing the outcome.
        """

    @abstractmethod
    def sign(
        self,
        *,
        secret: str,
        body: bytes,
        now: Optional[float] = None,
    ) -> dict[str, str]:
        """Produce the headers that authenticate an outbound delivery.

        The exact inverse of :meth:`verify`: feeding this output back into
        ``verify`` with the same secret and body must yield
        :attr:`SignatureVerdict.OK`.

        Args:
            secret: The shared secret.
            body: The raw body bytes about to be sent.
            now: Current POSIX time, injectable for tests.

        Returns:
            A mapping of header name to value.
        """

    # ------------------------------------------------------------------
    # Shared primitives
    # ------------------------------------------------------------------

    @staticmethod
    def _hmac_hex(secret: str, message: bytes, algo: str) -> str:
        """Return the lower-case hex HMAC of *message* under *algo*."""
        return hmac.new(
            secret.encode("utf-8"), message, _ALGORITHMS[algo]
        ).hexdigest()

    @staticmethod
    def _equal(a: str, b: str) -> bool:
        """Constant-time, case-insensitive comparison of two hex digests."""
        return hmac.compare_digest(
            a.strip().lower().encode("utf-8"), b.strip().lower().encode("utf-8")
        )

    @staticmethod
    def _is_hex(value: str) -> bool:
        """Whether *value* is a non-empty, purely hexadecimal string."""
        if not value:
            return False
        try:
            int(value, 16)
        except ValueError:
            return False
        return True

    def _read_header(
        self, headers: Mapping[str, str], header_override: Optional[str]
    ) -> tuple[str, Optional[str]]:
        """Return ``(header_name, raw_value_or_None)`` for this scheme."""
        name = header_override or self.header
        value = headers.get(name)
        if value is None:
            # Fall back to a case-insensitive scan for plain dict headers.
            lowered = name.lower()
            for key, candidate in headers.items():
                if key.lower() == lowered:
                    value = candidate
                    break
        return name, value

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self.name!r} header={self.header!r}>"


class HexDigestScheme(SignatureScheme):
    """A plain ``HMAC(secret, body)`` hex digest carried in one header.

    Covers GitHub, Jira and the package's own ``generic`` scheme, which differ
    only in header name, permitted algorithms, and whether the value is
    prefixed with ``<alg>=``.

    Args:
        name: Registry key.
        header: Default signature header.
        algorithms: Permitted digest algorithms, **most preferred first**.
            The first entry is used for :meth:`sign`.
        allow_bare: Accept a value with no ``<alg>=`` prefix, interpreting the
            whole string as a hex digest under ``algorithms[0]``.
        emit_prefix: Whether :meth:`sign` emits ``<alg>=<hex>`` rather than a
            bare hex digest.
    """

    def __init__(
        self,
        *,
        name: str,
        header: str,
        algorithms: Sequence[str] = ("sha256",),
        allow_bare: bool = False,
        emit_prefix: bool = True,
    ) -> None:
        unknown = [algo for algo in algorithms if algo not in _ALGORITHMS]
        if unknown:
            raise ValueError(
                f"HexDigestScheme {name!r}: unknown algorithm(s) {unknown!r}. "
                f"Supported: {sorted(_ALGORITHMS)}"
            )
        if not algorithms:
            raise ValueError(f"HexDigestScheme {name!r}: at least one algorithm required")
        self.name = name
        self.header = header
        self.algorithms = tuple(algorithms)
        self.allow_bare = allow_bare
        self.emit_prefix = emit_prefix

    def verify(
        self,
        *,
        secret: str,
        body: bytes,
        headers: Mapping[str, str],
        header_override: Optional[str] = None,
        tolerance_seconds: int = 300,
        now: Optional[float] = None,
    ) -> SignatureCheck:
        """Verify a ``<alg>=<hex>`` (or bare hex) signature over the raw body."""
        _, raw = self._read_header(headers, header_override)
        if not raw:
            return SignatureCheck(SignatureVerdict.MISSING)

        raw = raw.strip()
        if "=" in raw:
            algo, _, digest = raw.partition("=")
            algo = algo.strip().lower()
            if algo not in self.algorithms:
                return SignatureCheck(SignatureVerdict.MALFORMED, "unsupported_alg")
        elif self.allow_bare:
            algo, digest = self.algorithms[0], raw
        else:
            return SignatureCheck(SignatureVerdict.MALFORMED, "missing_alg_prefix")

        digest = digest.strip()
        if not self._is_hex(digest):
            return SignatureCheck(SignatureVerdict.MALFORMED, "not_hex")

        expected = self._hmac_hex(secret, body, algo)
        if self._equal(expected, digest):
            return SignatureCheck(SignatureVerdict.OK)
        return SignatureCheck(SignatureVerdict.MISMATCH)

    def sign(
        self,
        *,
        secret: str,
        body: bytes,
        now: Optional[float] = None,
    ) -> dict[str, str]:
        """Sign *body* using the scheme's preferred algorithm."""
        algo = self.algorithms[0]
        digest = self._hmac_hex(secret, body, algo)
        value = f"{algo}={digest}" if self.emit_prefix else digest
        return {self.header: value}


class StripeScheme(SignatureScheme):
    """Stripe's timestamped signature scheme.

    The header looks like ``t=1700000000,v1=<hex>[,v1=<hex>][,v0=<hex>]`` and
    the signed payload is ``f"{t}."`` concatenated with the **raw body bytes**.

    Two behaviours matter and are easy to get wrong:

    - **Multiple ``v1`` values are legal** and are how Stripe rolls secrets.
      Verification succeeds if *any* of them matches, so a rotation window
      does not reject live traffic.
    - The timestamp is checked **before** the HMAC, so a stale replay costs no
      cryptographic work. Timestamps too far in the future are rejected too,
      which closes the clock-skew variant of the same attack.

    .. note::
       Freshness is measured against :func:`time.time` (wall clock). It must
       never be changed to ``loop.time()``, whose epoch is arbitrary. Hosts
       verifying Stripe deliveries need a reasonably synchronised clock.
    """

    def __init__(
        self,
        *,
        name: str = "stripe",
        header: str = "Stripe-Signature",
        algorithm: str = "sha256",
    ) -> None:
        if algorithm not in _ALGORITHMS:
            raise ValueError(
                f"StripeScheme: unknown algorithm {algorithm!r}. "
                f"Supported: {sorted(_ALGORITHMS)}"
            )
        self.name = name
        self.header = header
        self.algorithm = algorithm

    @staticmethod
    def _parse(raw: str) -> tuple[Optional[str], list[str]]:
        """Split the header into ``(timestamp, [v1 digests])``."""
        timestamp: Optional[str] = None
        signatures: list[str] = []
        for part in raw.split(","):
            key, sep, value = part.strip().partition("=")
            if not sep:
                continue
            key = key.strip()
            if key == "t" and timestamp is None:
                timestamp = value.strip()
            elif key == "v1":
                signatures.append(value.strip())
        return timestamp, signatures

    def verify(
        self,
        *,
        secret: str,
        body: bytes,
        headers: Mapping[str, str],
        header_override: Optional[str] = None,
        tolerance_seconds: int = 300,
        now: Optional[float] = None,
    ) -> SignatureCheck:
        """Verify a Stripe signature, checking freshness before the HMAC."""
        _, raw = self._read_header(headers, header_override)
        if not raw:
            return SignatureCheck(SignatureVerdict.MISSING)

        timestamp, signatures = self._parse(raw)
        if timestamp is None:
            return SignatureCheck(SignatureVerdict.MALFORMED, "missing_timestamp")
        if not signatures:
            return SignatureCheck(SignatureVerdict.MALFORMED, "missing_v1")
        try:
            issued_at = int(timestamp)
        except ValueError:
            return SignatureCheck(SignatureVerdict.MALFORMED, "bad_timestamp")
        if any(not self._is_hex(sig) for sig in signatures):
            return SignatureCheck(SignatureVerdict.MALFORMED, "not_hex")

        # Freshness first — a stale replay must not cost an HMAC computation.
        if tolerance_seconds > 0:
            current = time.time() if now is None else now
            if abs(current - issued_at) > tolerance_seconds:
                detail = "future" if issued_at > current else "expired"
                return SignatureCheck(SignatureVerdict.STALE, detail)

        expected = self._hmac_hex(
            secret, f"{issued_at}.".encode("utf-8") + body, self.algorithm
        )
        # Compare against every v1 — Stripe sends one per active secret.
        if any(self._equal(expected, sig) for sig in signatures):
            return SignatureCheck(SignatureVerdict.OK)
        return SignatureCheck(SignatureVerdict.MISMATCH)

    def sign(
        self,
        *,
        secret: str,
        body: bytes,
        now: Optional[float] = None,
    ) -> dict[str, str]:
        """Sign *body* with the current timestamp in Stripe's format."""
        issued_at = int(time.time() if now is None else now)
        digest = self._hmac_hex(
            secret, f"{issued_at}.".encode("utf-8") + body, self.algorithm
        )
        return {self.header: f"t={issued_at},v1={digest}"}


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_SCHEMES: dict[str, SignatureScheme] = {}


def register_signature_scheme(
    scheme: SignatureScheme, *, replace: bool = False
) -> SignatureScheme:
    """Register *scheme* under its :attr:`~SignatureScheme.name`.

    Consuming applications call this at import time to teach the webhook
    fabric a provider this package does not ship, mirroring how
    ``HOOK_TYPES.register()`` opens the hook-type namespace::

        register_signature_scheme(
            HexDigestScheme(name="shopify", header="X-Shopify-Hmac-Sha256")
        )

    Args:
        scheme: The scheme instance to register.
        replace: Permit overwriting an existing registration. Off by default
            so a name collision between two packages is loud, not silent.

    Returns:
        The registered scheme, for convenient chaining.

    Raises:
        ValueError: If the scheme has no name, or the name is taken and
            ``replace`` is False.
    """
    if not scheme.name:
        raise ValueError("Signature scheme must declare a non-empty 'name'")
    if scheme.name in _SCHEMES and not replace:
        raise ValueError(
            f"Signature scheme {scheme.name!r} is already registered. "
            "Pass replace=True to override it deliberately."
        )
    _SCHEMES[scheme.name] = scheme
    return scheme


def get_signature_scheme(name: str) -> SignatureScheme:
    """Look up a registered scheme by name.

    Args:
        name: The scheme's registry key, e.g. ``"github"``.

    Returns:
        The registered :class:`SignatureScheme`.

    Raises:
        KeyError: If *name* is not registered. The message lists every
            available name, so a typo in a config file is self-diagnosing.
    """
    try:
        return _SCHEMES[name]
    except KeyError:
        raise KeyError(
            f"Unknown signature scheme {name!r}. "
            f"Available: {available_signature_schemes()}"
        ) from None


def available_signature_schemes() -> list[str]:
    """Return every registered scheme name, sorted."""
    return sorted(_SCHEMES)


#: GitHub: ``X-Hub-Signature-256: sha256=<hex>`` over the raw body.
register_signature_scheme(
    HexDigestScheme(
        name="github",
        header="X-Hub-Signature-256",
        algorithms=("sha256",),
    )
)

#: Jira: ``X-Hub-Signature``. Cloud sends sha256; older Data Center sends sha1.
register_signature_scheme(
    HexDigestScheme(
        name="jira",
        header="X-Hub-Signature",
        algorithms=("sha256", "sha1"),
    )
)

#: This package's own default: ``X-Webhook-Signature``, prefixed or bare hex.
register_signature_scheme(
    HexDigestScheme(
        name="generic",
        header="X-Webhook-Signature",
        algorithms=("sha256",),
        allow_bare=True,
    )
)

#: Stripe: timestamped ``t=<unix>,v1=<hex>``.
register_signature_scheme(StripeScheme())
