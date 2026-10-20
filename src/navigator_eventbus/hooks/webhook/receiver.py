"""Shared inbound pipeline for every webhook hook in this package.

Two layers, split by testability:

- **Module-level pure helpers** (:func:`read_capped`, :func:`client_ip`,
  :func:`ip_allowed`, :func:`parse_body`, :func:`json_error`) — no ``self``,
  unit-testable without an aiohttp application.
- **:class:`WebhookIngestMixin`** — the orchestration that needs
  ``self.logger`` / ``self._make_event`` / ``self.on_event`` plus the
  subclass override points.

The pipeline order in :meth:`WebhookIngestMixin._ingest` is load-bearing.
Signature verification runs **before** body parsing so that a malformed body
from an unauthenticated caller never reaches the JSON parser, and it runs
against the **raw bytes** because any re-serialization changes the digest.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import OrderedDict
from collections.abc import Callable, Coroutine, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import parse_qsl

from aiohttp import web

from navigator_eventbus.hooks.models import HookEvent
from navigator_eventbus.hooks.webhook.models import (
    WebhookEndpointConfig,
    WebhookEndpointState,
)
from navigator_eventbus.hooks.webhook.preprocess import (
    WebhookContext,
    run_preprocessor,
)
from navigator_eventbus.webhook_signatures import SignatureCheck, SignatureVerdict

__all__ = (
    "BodyTooLarge",
    "WebhookIngestMixin",
    "client_ip",
    "ip_allowed",
    "json_error",
    "parse_body",
    "read_capped",
)

_JSON_TYPES = ("application/json", "+json", "text/json")
_FORM_TYPE = "application/x-www-form-urlencoded"


class BodyTooLarge(Exception):
    """Raised by :func:`read_capped` when a request body exceeds its cap."""

    def __init__(self, size: int, limit: int) -> None:
        super().__init__(f"body of {size} bytes exceeds the {limit}-byte limit")
        self.size = size
        self.limit = limit


async def read_capped(request: web.Request, limit: int) -> bytes:
    """Read the raw request body, aborting once *limit* bytes are exceeded.

    Two layers, because either alone is insufficient: a ``Content-Length``
    pre-check rejects an oversized declared body without reading it, and a
    streaming read catches chunked transfer encoding, which carries no
    ``Content-Length`` at all.

    .. warning::
       This consumes ``request.content`` directly, bypassing the cache that
       ``request.read()`` populates. Callers must parse the returned bytes;
       calling ``request.json()`` or ``request.post()`` afterwards on the same
       request will see an exhausted stream.

    Args:
        request: The inbound aiohttp request.
        limit: Maximum permitted body size in bytes.

    Returns:
        The raw body bytes.

    Raises:
        BodyTooLarge: The body exceeds *limit*.
    """
    declared = request.content_length
    if declared is not None and declared > limit:
        raise BodyTooLarge(declared, limit)
    buffer = bytearray()
    async for chunk in request.content.iter_chunked(65_536):
        buffer.extend(chunk)
        if len(buffer) > limit:
            raise BodyTooLarge(len(buffer), limit)
    return bytes(buffer)


def client_ip(
    request: web.Request, *, trust_forwarded_for: bool = False
) -> Optional[str]:
    """Resolve the caller's IP address.

    ``X-Forwarded-For`` is only consulted when *trust_forwarded_for* is set,
    because the header is caller-supplied and trivially spoofed unless a
    trusted reverse proxy is known to overwrite it.

    Args:
        request: The inbound request.
        trust_forwarded_for: Honour the left-most ``X-Forwarded-For`` entry.

    Returns:
        The client IP as a string, or ``None`` when it cannot be determined.
    """
    if trust_forwarded_for:
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            first = forwarded.split(",")[0].strip()
            if first:
                return first
    return request.remote


def ip_allowed(ip: Optional[str], networks: Sequence[Any]) -> bool:
    """Whether *ip* is permitted by the parsed allowlist.

    Args:
        ip: The client IP, or ``None`` when unknown.
        networks: Parsed ``ip_network`` objects. Empty means no restriction.

    Returns:
        True when the allowlist is empty or *ip* falls inside one entry.
        An unknown IP is rejected whenever an allowlist is configured.
    """
    if not networks:
        return True
    if not ip:
        return False
    import ipaddress

    try:
        address = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(address in network for network in networks)


def parse_body(
    body: bytes, content_type: str, *, allow_non_json: bool = True
) -> dict[str, Any]:
    """Parse a raw body into a dict, degrading gracefully.

    Tries JSON, then form encoding, then — when *allow_non_json* is set — the
    ``{"raw": ...}`` fallback, so an unexpected content type still reaches the
    bus instead of being dropped. JSON that decodes to a non-object (a list,
    a bare string) is wrapped under a ``data`` key so the payload is always a
    mapping.

    Args:
        body: Raw request body bytes.
        content_type: The request ``Content-Type``, possibly with parameters.
        allow_non_json: Permit the raw-text fallback.

    Returns:
        The parsed payload as a dict.

    Raises:
        ValueError: The body could not be parsed and *allow_non_json* is
            False.
    """
    base_type = content_type.split(";")[0].strip().lower()
    text: Optional[str]
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        text = None

    if text is not None:
        looks_json = any(token in base_type for token in _JSON_TYPES)
        if looks_json or not base_type:
            try:
                parsed = json.loads(text)
            except (json.JSONDecodeError, ValueError):
                parsed = None
            if parsed is not None:
                return parsed if isinstance(parsed, dict) else {"data": parsed}
        elif base_type == _FORM_TYPE:
            try:
                return dict(parse_qsl(text, keep_blank_values=True))
            except ValueError:
                pass
        else:
            # Unknown content type — still try JSON before giving up.
            try:
                parsed = json.loads(text)
            except (json.JSONDecodeError, ValueError):
                parsed = None
            if parsed is not None:
                return parsed if isinstance(parsed, dict) else {"data": parsed}

    if not allow_non_json:
        raise ValueError(f"Unparseable body for content-type {content_type!r}")
    return {
        "raw": text if text is not None else body.hex(),
        "content_type": content_type,
        "encoding": "utf-8" if text is not None else "hex",
    }


def json_error(
    status: int,
    slug: str,
    *,
    headers: Optional[dict[str, str]] = None,
    **extra: Any,
) -> web.Response:
    """Build a uniform JSON error response.

    Bodies are machine-readable and deliberately terse: they never echo a
    signature, a digest, a secret, or any other attacker-supplied value.

    Args:
        status: HTTP status code.
        slug: Stable machine-readable value for the body's ``status`` field.
            Named ``slug`` rather than ``reason`` so callers can pass a
            ``reason=`` body field through ``**extra`` without colliding.
        headers: Extra response headers, e.g. ``Retry-After``.
        **extra: Additional JSON body fields.

    Returns:
        The aiohttp response.
    """
    payload: dict[str, Any] = {"status": slug}
    payload.update(extra)
    return web.json_response(payload, status=status, headers=headers)


#: Maps a failed signature verdict onto its HTTP response. A malformed header
#: is the caller sending garbage (400); a mismatch or a stale timestamp is a
#: credential failure (401).
_VERDICT_RESPONSES = {
    SignatureVerdict.MISSING: (401, "unauthorized", "missing_signature"),
    SignatureVerdict.MALFORMED: (400, "bad_signature_format", None),
    SignatureVerdict.MISMATCH: (401, "unauthorized", None),
    SignatureVerdict.STALE: (401, "unauthorized", "stale"),
}


class WebhookIngestMixin:
    """The shared request pipeline for webhook hooks.

    Mixed in **ahead of** :class:`~navigator_eventbus.hooks.base.BaseHook` so
    that the pipeline can reach ``self.logger``, ``self._make_event`` and
    ``self.on_event`` while subclasses supply the provider-specific parts.

    Subclasses call :meth:`_init_ingest` from their ``__init__`` and then
    delegate their aiohttp handler to :meth:`_ingest`.

    Override points, in the order the pipeline calls them:

    - :meth:`_classify_event` — map the delivery to an event type, or ``None``
      to acknowledge it without emitting.
    - :meth:`_normalize_payload` — reshape a vendor payload.
    - :meth:`_build_task` — supply an optional ``HookEvent.task``.
    """

    if TYPE_CHECKING:
        # Supplied by BaseHook, which every concrete user of this mixin also
        # inherits. Declared here (type-check time only) so the mixin can be
        # checked in isolation without inheriting from BaseHook, which would
        # force an MRO the subclasses must not have.
        logger: logging.Logger
        hook_id: str
        hook_type: str
        name: str
        metadata: dict[str, Any]
        target_type: Optional[str]
        target_id: Optional[str]
        _callback: Optional[Callable[[HookEvent], Coroutine[Any, Any, None]]]

        async def on_event(self, event_data: HookEvent) -> None: ...

    # Defaults so a subclass that forgets _init_ingest still behaves sanely.
    _default_max_body_bytes: int = 1_048_576
    _dispatch_mode: str = "await"
    _dedup_ttl: int = 0
    _dedup_cache_size: int = 1024

    def _init_ingest(
        self,
        *,
        max_body_bytes: int = 1_048_576,
        dispatch_mode: str = "await",
        dedup_ttl_seconds: int = 0,
        dedup_cache_size: int = 1024,
    ) -> None:
        """Initialise ingest state. Call from the subclass ``__init__``."""
        self._default_max_body_bytes = max_body_bytes
        self._dispatch_mode = dispatch_mode
        self._dedup_ttl = dedup_ttl_seconds
        self._dedup_cache_size = dedup_cache_size
        self._dedup: "OrderedDict[str, float]" = OrderedDict()
        self._background_tasks: set[asyncio.Task[None]] = set()

    # ------------------------------------------------------------------
    # Override points
    # ------------------------------------------------------------------

    def _classify_event(
        self,
        request: web.Request,
        endpoint: WebhookEndpointConfig,
        payload: dict[str, Any],
    ) -> Optional[str]:
        """Return the event type for this delivery, or ``None`` to ignore it.

        The default lifts the value of ``endpoint.event_type_header`` when
        configured and present, and otherwise uses ``endpoint.event_type``.

        Returning ``None`` produces a ``200`` — the delivery was received and
        authenticated, but is not interesting. See :meth:`_ingest` for why
        that is a success code.

        Args:
            request: The inbound request.
            endpoint: The matched endpoint configuration.
            payload: The parsed (and preprocessed) payload.

        Returns:
            The event type, without the configured prefix (the pipeline
            applies that), or ``None`` to ignore the delivery.
        """
        if endpoint.event_type_header:
            header_value = request.headers.get(endpoint.event_type_header)
            if header_value:
                return header_value.strip()
        return endpoint.event_type

    def _normalize_payload(
        self,
        request: web.Request,
        endpoint: WebhookEndpointConfig,
        payload: dict[str, Any],
        event_type: str,
    ) -> dict[str, Any]:
        """Reshape a vendor payload into the form emitted on the bus.

        The default is the identity. Provider subclasses override this to flatten
        a nested vendor body into a stable, provider-agnostic dict.
        """
        return payload

    def _build_task(
        self,
        endpoint: WebhookEndpointConfig,
        event_type: str,
        payload: dict[str, Any],
    ) -> Optional[str]:
        """Return an optional ``HookEvent.task`` prompt override."""
        return None

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def _verify_signature(
        self,
        request: web.Request,
        endpoint: WebhookEndpointConfig,
        body: bytes,
    ) -> Optional[SignatureCheck]:
        """Verify the delivery signature.

        Verification runs whenever a ``secret`` is configured.
        ``require_signature`` is a *configuration-time* guard that forces a
        secret to exist; it is not re-evaluated per request.

        Returns:
            ``None`` when no secret is configured (nothing to verify), else
            the :class:`SignatureCheck`.
        """
        if not endpoint.secret:
            return None
        return endpoint.scheme.verify(
            secret=endpoint.secret,
            body=body,
            headers=request.headers,
            header_override=endpoint.signature_header,
            tolerance_seconds=endpoint.tolerance_seconds,
        )

    def _delivery_id(
        self, request: web.Request, endpoint: WebhookEndpointConfig
    ) -> Optional[str]:
        """Return the endpoint's configured delivery id header value."""
        if not endpoint.dedup_header:
            return None
        return request.headers.get(endpoint.dedup_header)

    def _is_duplicate(
        self, request: web.Request, endpoint: WebhookEndpointConfig
    ) -> bool:
        """Whether this delivery id has already been seen within the TTL.

        A bounded LRU with lazy TTL pruning from the front. This is
        **per-process and best-effort** — it is not a distributed idempotency
        guarantee. Its purpose is bounding the replay window for schemes whose
        signature never expires (GitHub, Jira), and suppressing provider
        retries of a delivery already accepted.
        """
        if self._dedup_ttl <= 0:
            return False
        key = self._delivery_id(request, endpoint)
        if not key:
            return False
        now = time.monotonic()
        cache = self._dedup
        while cache:
            oldest_key = next(iter(cache))
            if now - cache[oldest_key] <= self._dedup_ttl:
                break
            cache.popitem(last=False)
        full_key = f"{endpoint.path}:{key}"
        if full_key in cache:
            return True
        cache[full_key] = now
        while len(cache) > self._dedup_cache_size:
            cache.popitem(last=False)
        return False

    def _spawn(self, coro) -> None:
        """Run *coro* as a tracked background task.

        A bare ``asyncio.create_task`` keeps only a weak reference from the
        event loop, so CPython may garbage-collect the task mid-flight. Same
        strong-reference pattern as ``BusCore``, ``DLQHandler`` and
        ``NotificationSubscriber``.
        """
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _drain_tasks(self) -> None:
        """Cancel and await every tracked background task."""
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

    async def _dispatch(
        self, event: HookEvent, state: Optional[WebhookEndpointState]
    ) -> None:
        """Hand the event to ``HookManager`` via ``BaseHook.on_event``."""
        if getattr(self, "_callback", None) is None:
            # BaseHook.on_event would log a warning and silently drop this.
            # Count it and log loudly: a listener nobody registered accepts
            # deliveries and discards them while telling the sender 202.
            if state is not None:
                state.dropped_no_callback += 1
            self.logger.error(
                "Webhook hook %r received an event but has NO callback registered "
                "— the delivery was DROPPED. Register the hook with a HookManager "
                "(manager.register(hook)) before serving traffic.",
                getattr(self, "name", self.__class__.__name__),
            )
            return
        await self.on_event(event)

    # ------------------------------------------------------------------
    # The pipeline
    # ------------------------------------------------------------------

    async def _ingest(
        self,
        request: web.Request,
        endpoint: WebhookEndpointConfig,
        state: Optional[WebhookEndpointState] = None,
    ) -> web.Response:
        """Run one inbound delivery through the full pipeline.

        Stage order — enabled, IP, raw body, signature, de-duplication, parse,
        preprocess, classify, build, dispatch — is deliberate; see the module
        docstring.

        On the response codes: an *ignored* delivery returns ``200``, not a
        4xx. GitHub, Jira and Stripe all treat a non-2xx as a failed delivery,
        retry it, and eventually disable the webhook. From the sender's point
        of view an ignored event really was delivered — it was received,
        authenticated, and deliberately not acted on. ``202`` means the same
        plus "work was queued", which keeps access logs unambiguous without
        parsing bodies. Genuine failures (401/403/413) are never softened this
        way: they are misconfigurations an operator needs to see in the
        provider's delivery dashboard.

        Args:
            request: The inbound aiohttp request.
            endpoint: The matched endpoint configuration.
            state: Optional per-endpoint counters to update.

        Returns:
            The HTTP response for the sender.
        """
        if state is not None:
            state.call_count += 1
            state.last_called = datetime.now(timezone.utc)

        if not endpoint.enabled:
            if state is not None:
                state.rejected += 1
            return json_error(503, "disabled", headers={"Retry-After": "60"})

        # --- source IP -------------------------------------------------
        if endpoint.parsed_networks:
            remote = client_ip(
                request, trust_forwarded_for=endpoint.trust_forwarded_for
            )
            if not ip_allowed(remote, endpoint.parsed_networks):
                if state is not None:
                    state.rejected += 1
                self.logger.warning(
                    "Webhook %s rejected delivery from %s (not in allowlist)",
                    endpoint.path,
                    remote,
                )
                return json_error(403, "forbidden")

        # --- raw body (capped) -----------------------------------------
        limit = endpoint.max_body_bytes or self._default_max_body_bytes
        try:
            body = await read_capped(request, limit)
        except BodyTooLarge as exc:
            if state is not None:
                state.rejected += 1
            self.logger.warning(
                "Webhook %s rejected oversized body (%s > %s bytes)",
                endpoint.path,
                exc.size,
                exc.limit,
            )
            return json_error(413, "payload_too_large", limit=limit)

        # --- signature, over the RAW bytes, before any parsing ---------
        check = self._verify_signature(request, endpoint, body)
        if check is not None and not check.ok:
            if state is not None:
                state.rejected += 1
            status, slug, detail = _VERDICT_RESPONSES[check.verdict]
            self.logger.warning(
                "Webhook %s signature rejected: %s (%s)",
                endpoint.path,
                check.verdict.value,
                check.detail or "-",
            )
            if detail:
                return json_error(status, slug, reason=detail)
            return json_error(status, slug)

        # --- de-duplication --------------------------------------------
        if self._is_duplicate(request, endpoint):
            if state is not None:
                state.duplicates += 1
            self.logger.info(
                "Webhook %s ignoring duplicate delivery %s",
                endpoint.path,
                self._delivery_id(request, endpoint),
            )
            return web.json_response({"status": "duplicate"}, status=200)

        # --- parse -------------------------------------------------------
        content_type = request.headers.get("Content-Type", "")
        try:
            payload = parse_body(
                body, content_type, allow_non_json=endpoint.allow_non_json
            )
        except ValueError:
            if state is not None:
                state.rejected += 1
            return json_error(400, "invalid_payload")

        # --- preprocess (fail-soft) ---------------------------------------
        context = WebhookContext(
            headers=dict(request.headers),
            path=endpoint.path,
            remote=client_ip(
                request, trust_forwarded_for=endpoint.trust_forwarded_for
            ),
            content_type=content_type,
            endpoint_name=endpoint.name or endpoint.path,
            method=request.method,
        )
        pre = await run_preprocessor(endpoint, payload, context, logger=self.logger)
        preprocessor_failed = "preprocessor_error" in pre.metadata
        if preprocessor_failed and state is not None:
            state.preprocessor_errors += 1

        if pre.ignore:
            if state is not None:
                state.ignored += 1
            return web.json_response({"status": "ignored"}, status=200)

        # --- classify ------------------------------------------------------
        # An explicit event_type from the preprocessor is used verbatim; the
        # configured prefix applies only to the classifier's own result.
        if pre.event_type:
            event_type = pre.event_type
        else:
            classified = self._classify_event(request, endpoint, pre.payload)
            if classified is None:
                if state is not None:
                    state.ignored += 1
                return web.json_response({"status": "ignored"}, status=200)
            event_type = (
                f"{endpoint.event_type_prefix}.{classified}"
                if endpoint.event_type_prefix
                else classified
            )

        # --- build ----------------------------------------------------------
        final_payload = self._normalize_payload(
            request, endpoint, pre.payload, event_type
        )
        metadata: dict[str, Any] = {**(self.metadata or {}), **endpoint.metadata}
        metadata.update(pre.metadata)
        metadata.setdefault("webhook_path", endpoint.path)
        delivery_id = self._delivery_id(request, endpoint)
        if delivery_id:
            metadata.setdefault("delivery_id", delivery_id)

        try:
            event = HookEvent(
                hook_id=self.hook_id,
                hook_type=self.hook_type,
                event_type=event_type,
                payload=final_payload,
                metadata=metadata,
                target_type=endpoint.target_type or self.target_type,
                target_id=endpoint.target_id or self.target_id,
                task=pre.task
                or self._build_task(endpoint, event_type, final_payload),
            )
        except Exception as exc:  # noqa: BLE001 — invalid hook_type, bad payload
            self.logger.error(
                "Webhook %s could not build a HookEvent: %s", endpoint.path, exc
            )
            return json_error(500, "error")

        # --- dispatch ---------------------------------------------------------
        if self._dispatch_mode == "background":
            self._spawn(self._dispatch(event, state))
        else:
            await self._dispatch(event, state)

        if state is not None:
            state.accepted += 1
        body_out: dict[str, Any] = {
            "status": "accepted",
            "event_type": event_type,
            "hook_id": self.hook_id,
        }
        if preprocessor_failed:
            body_out["preprocessor"] = "failed"
        return web.json_response(body_out, status=202)
