"""Configuration models for the inbound webhook hooks.

.. note::
   Every *other* hook config in this package lives in
   :mod:`navigator_eventbus.hooks.models`, which is deliberately data-only.
   The webhook configs live here instead because they carry behaviour — a
   non-serializable ``Callable`` field and a validator that invokes
   ``importlib`` — and because they must import
   :mod:`navigator_eventbus.webhook_signatures` to validate scheme names.
   Keeping them out of ``hooks/models.py`` preserves that module's
   zero-behaviour, narrow-import property. This is an intentional deviation,
   not an oversight.

**Serializability.** These models must survive a YAML/JSON round trip — that
is the entire point of supporting an import-string preprocessor. A live
callable can still be supplied in-process, but it lives in a field that is
excluded from ``model_dump()`` and from the JSON schema, so a config built
with a lambda still serializes cleanly.
"""
from __future__ import annotations

import ipaddress
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.json_schema import SkipJsonSchema

from navigator_eventbus._imports import accepts_two_positionals, resolve_callable
from navigator_eventbus.webhook_signatures import get_signature_scheme

__all__ = (
    "DEFAULT_WEBHOOK_BASE_PATH",
    "WebhookEndpointConfig",
    "WebhookEndpointState",
    "WebhookHookConfig",
    "normalize_path",
)

#: Default mount point for the dynamic listener, matching the
#: ``/api/v1/hooks/<provider>`` convention used by every other HTTP hook.
DEFAULT_WEBHOOK_BASE_PATH = "/api/v1/hooks/webhook"

_IpNetwork = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]


def normalize_path(value: str) -> str:
    """Normalize a route path to a canonical, traversal-free form.

    Forces exactly one leading slash, removes trailing slashes, and rejects
    parent-directory segments and empty interior segments. Normalizing at the
    config boundary is what lets endpoint lookup be a plain dict hit rather
    than a fuzzy match.

    Args:
        value: The raw configured path.

    Returns:
        The normalized path, e.g. ``"/github"``. A bare root normalizes to
        ``"/"``.

    Raises:
        ValueError: The path is not a string, or contains ``..`` or an empty
            interior segment.
    """
    if not isinstance(value, str):
        raise ValueError(f"path must be a string, got {type(value).__name__}")
    candidate = value.strip()
    if not candidate:
        raise ValueError("path must not be empty")
    if "\\" in candidate:
        raise ValueError(f"path must not contain backslashes: {value!r}")
    candidate = "/" + candidate.strip("/")
    if candidate == "/":
        return "/"
    segments = candidate.split("/")[1:]
    for segment in segments:
        if segment in ("", ".", ".."):
            raise ValueError(
                f"path must not contain empty or relative segments: {value!r}"
            )
    return candidate


@dataclass
class WebhookEndpointState:
    """Mutable per-endpoint counters.

    Deliberately separate from :class:`WebhookEndpointConfig`. ai-parrot's
    original ``WebhookEndpoint`` dataclass mixed traffic counters into the
    configuration object, which makes the config non-idempotent to serialize —
    dumping it after live traffic produces a different document than dumping
    it at startup.
    """

    call_count: int = 0
    accepted: int = 0
    ignored: int = 0
    rejected: int = 0
    duplicates: int = 0
    preprocessor_errors: int = 0
    dropped_no_callback: int = 0
    last_called: Optional[datetime] = None

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-friendly snapshot of the counters."""
        return {
            "call_count": self.call_count,
            "accepted": self.accepted,
            "ignored": self.ignored,
            "rejected": self.rejected,
            "duplicates": self.duplicates,
            "preprocessor_errors": self.preprocessor_errors,
            "dropped_no_callback": self.dropped_no_callback,
            "last_called": self.last_called.isoformat() if self.last_called else None,
        }


class WebhookEndpointConfig(BaseModel):
    """One webhook endpoint: its route, its authentication, its transform.

    Used in two places with one difference in how ``path`` is read:

    - :class:`~navigator_eventbus.hooks.webhook.listener.WebhookListenerHook`
      treats ``path`` as **relative** to the listener's ``base_path``.
    - :class:`~navigator_eventbus.hooks.webhook.provider.ProviderWebhookHook`
      treats ``path`` as the **absolute** route it mounts.

    Sharing one model is what lets both hooks run the identical ingest
    pipeline.

    Security defaults are strict: ``require_signature`` is True, so an
    endpoint without a secret is a configuration error rather than a silently
    unauthenticated route. Set ``require_signature=False`` to opt out
    deliberately.
    """

    model_config = ConfigDict(extra="forbid")

    path: str = Field(..., description="Route path. Relative for the listener, absolute for provider hooks.")
    name: Optional[str] = Field(default=None, description="Human-readable label; defaults to the path.")
    enabled: bool = True

    # --- authentication -------------------------------------------------
    secret: Optional[str] = Field(default=None, repr=False, description="Shared HMAC secret.")
    signature_scheme: str = Field(default="generic", description="Registered signature scheme name.")
    signature_header: Optional[str] = Field(default=None, description="Overrides the scheme's default header.")
    require_signature: bool = Field(default=True, description="Reject unsigned deliveries.")
    tolerance_seconds: int = Field(default=300, ge=0, description="Freshness window for timestamped schemes.")
    allowed_ips: list[str] = Field(default_factory=list, description="IP/CIDR allowlist. Empty means no restriction.")
    trust_forwarded_for: bool = Field(default=False, description="Honour the left-most X-Forwarded-For entry.")

    # --- event shaping --------------------------------------------------
    event_type: str = Field(default="received", min_length=1, description="Default HookEvent event_type.")
    event_type_header: Optional[str] = Field(default=None, description="Lift the event type from this header.")
    event_type_prefix: Optional[str] = Field(default=None, description="Prefix prepended to the event type.")

    # --- preprocessing --------------------------------------------------
    preprocessor: Optional[str] = Field(
        default=None,
        description='Import string "pkg.mod:func" resolved at construction. YAML-safe.',
    )
    preprocessor_fn: SkipJsonSchema[Optional[Callable[..., Any]]] = Field(
        default=None,
        exclude=True,
        repr=False,
        description="In-process callable. Takes precedence over `preprocessor`. Never serialized.",
    )
    preprocessor_accepts_ctx: SkipJsonSchema[bool] = Field(
        default=False,
        exclude=True,
        repr=False,
        description="Cached arity check — whether the callable takes a WebhookContext.",
    )
    preprocess_timeout: float = Field(default=5.0, gt=0, description="Seconds before the preprocessor is abandoned.")

    # --- body handling --------------------------------------------------
    allow_non_json: bool = Field(default=True, description='Wrap unparseable bodies as {"raw": ...} instead of 400.')
    max_body_bytes: Optional[int] = Field(
        default=None, gt=0, description="Per-endpoint cap; falls back to the listener's."
    )
    dedup_header: Optional[str] = Field(default=None, description='Delivery-id header, e.g. "X-GitHub-Delivery".')

    # --- routing hints --------------------------------------------------
    target_type: Optional[str] = None
    target_id: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    # Parsed allowlist, populated by the validator. Excluded from dumps.
    parsed_networks: SkipJsonSchema[list[Any]] = Field(
        default_factory=list, exclude=True, repr=False
    )

    @field_validator("path")
    @classmethod
    def _normalize_path(cls, value: str) -> str:
        return normalize_path(value)

    @field_validator("signature_scheme")
    @classmethod
    def _known_scheme(cls, value: str) -> str:
        # get_signature_scheme raises KeyError, which pydantic does NOT
        # convert into a ValidationError — only ValueError/AssertionError are.
        try:
            get_signature_scheme(value)
        except KeyError as exc:
            raise ValueError(str(exc).strip('"')) from exc
        return value

    @field_validator("allowed_ips")
    @classmethod
    def _parseable_ips(cls, value: list[str]) -> list[str]:
        for entry in value:
            try:
                ipaddress.ip_network(entry, strict=False)
            except ValueError as exc:
                raise ValueError(f"Invalid IP or CIDR {entry!r}: {exc}") from exc
        return value

    @model_validator(mode="after")
    def _finalize(self) -> "WebhookEndpointConfig":
        """Resolve the preprocessor, cache its arity, and parse the allowlist.

        Resolution failure here is a *configuration* error and fails fast, so
        a typo in YAML explodes at startup where an operator will see it.
        Invocation failure at request time is handled separately and fails
        soft — see
        :func:`navigator_eventbus.hooks.webhook.preprocess.run_preprocessor`.
        """
        if self.require_signature and not self.secret:
            raise ValueError(
                f"Endpoint {self.path!r} sets require_signature=True but has no "
                "secret. Provide `secret`, or set require_signature=False to "
                "accept unauthenticated deliveries deliberately."
            )
        if self.preprocessor_fn is None and self.preprocessor:
            try:
                self.preprocessor_fn = resolve_callable(self.preprocessor)
            except TypeError as exc:
                # resolve_callable raises TypeError for a non-callable target;
                # surface it as a ValidationError like every other config error.
                raise ValueError(str(exc)) from exc
        if self.preprocessor_fn is not None:
            self.preprocessor_accepts_ctx = accepts_two_positionals(self.preprocessor_fn)
        self.parsed_networks = [
            ipaddress.ip_network(entry, strict=False) for entry in self.allowed_ips
        ]
        if self.name is None:
            self.name = self.path
        return self

    @property
    def scheme(self):
        """The resolved :class:`~navigator_eventbus.webhook_signatures.SignatureScheme`."""
        return get_signature_scheme(self.signature_scheme)


class WebhookHookConfig(BaseModel):
    """Configuration for the dynamic multi-endpoint webhook listener."""

    model_config = ConfigDict(extra="forbid")

    name: str = "webhook"
    enabled: bool = True
    base_path: str = Field(default=DEFAULT_WEBHOOK_BASE_PATH, description="Mount point for the catch-all route.")
    endpoints: list[WebhookEndpointConfig] = Field(default_factory=list)

    # --- limits ---------------------------------------------------------
    max_body_bytes: int = Field(default=1_048_576, gt=0, description="Default per-request body cap in bytes.")
    max_inflight: int = Field(
        default=64,
        ge=0,
        description="Concurrent in-flight deliveries; 0 disables the limit. Not a rate limiter.",
    )
    dispatch_mode: Literal["await", "background"] = Field(
        default="await",
        description="'await' dispatches inline (default); 'background' uses a tracked task.",
    )

    # --- de-duplication -------------------------------------------------
    dedup_ttl_seconds: int = Field(default=0, ge=0, description="0 disables delivery-id de-duplication.")
    dedup_cache_size: int = Field(default=1024, gt=0)

    # --- introspection route --------------------------------------------
    expose_list_route: bool = Field(default=False, description="Expose GET <base_path>/_list.")
    list_route_token: Optional[str] = Field(default=None, repr=False)

    # --- defaults applied by register_endpoint() -------------------------
    default_signature_scheme: str = "generic"
    default_require_signature: bool = True

    # --- routing hints ---------------------------------------------------
    target_type: Optional[str] = None
    target_id: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("base_path")
    @classmethod
    def _normalize_base_path(cls, value: str) -> str:
        normalized = normalize_path(value)
        if normalized == "/":
            raise ValueError("base_path must not be the site root '/'")
        return normalized

    @field_validator("default_signature_scheme")
    @classmethod
    def _known_default_scheme(cls, value: str) -> str:
        try:
            get_signature_scheme(value)
        except KeyError as exc:
            raise ValueError(str(exc).strip('"')) from exc
        return value

    @model_validator(mode="after")
    def _validate_endpoints(self) -> "WebhookHookConfig":
        """Reject duplicate endpoint paths and an unguarded introspection route."""
        seen: set[str] = set()
        for endpoint in self.endpoints:
            if endpoint.path in seen:
                raise ValueError(
                    f"Duplicate webhook endpoint path {endpoint.path!r} — "
                    "paths are normalized, so '/x' and '/x/' collide."
                )
            seen.add(endpoint.path)
        if self.expose_list_route and not self.list_route_token:
            raise ValueError(
                "expose_list_route=True requires list_route_token: the listing "
                "enumerates every registered path and its traffic stats."
            )
        return self
