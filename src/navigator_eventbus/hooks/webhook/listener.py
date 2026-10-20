"""WebhookListenerHook — one catch-all route fronting N dynamic endpoints.

Ported from ai-parrot's ``parrot.autonomous.webhooks.WebhookListener`` with
four corrections:

1. **Endpoints are keyed by the relative match-info id**, not ``request.path``.
   The original broke whenever the app was mounted under a prefix or sat
   behind a path-rewriting reverse proxy.
2. **Background tasks are tracked.** The original's bare
   ``asyncio.create_task`` could be garbage-collected mid-flight.
3. **Topics follow the ``hooks.<type>.<event>`` convention** instead of a flat
   ``webhook.*`` namespace that bypassed ``TOPICS.md`` governance.
4. **The ``_list`` introspection route is opt-in and token-guarded.** It
   enumerates every registered path, which endpoints carry a secret, and their
   traffic counters.
"""
from __future__ import annotations

import asyncio
import hmac
from typing import Any, Optional

from aiohttp import web

from navigator_eventbus.hooks.base import BaseHook
from navigator_eventbus.hooks.models import HookType
from navigator_eventbus.hooks.webhook.models import (
    WebhookEndpointConfig,
    WebhookEndpointState,
    WebhookHookConfig,
    normalize_path,
)
from navigator_eventbus.hooks.webhook.receiver import WebhookIngestMixin, json_error

__all__ = ("WebhookListenerHook",)


class WebhookListenerHook(WebhookIngestMixin, BaseHook):
    """Accept webhooks from many external systems on one aiohttp route.

    A single ``POST <base_path>/{webhook_id:.*}`` route fronts an in-memory
    registry of endpoints, so endpoints can be added and removed **after the
    application is running** without mutating the aiohttp router.

    Each delivery is authenticated with the endpoint's own HMAC scheme,
    optionally transformed by a preprocessor, and emitted as a
    :class:`~navigator_eventbus.hooks.models.HookEvent` on
    ``hooks.webhook.<event_type>``.

    Example:
        >>> listener = WebhookListenerHook()
        >>> listener.register_endpoint(
        ...     "/github",
        ...     secret="whsec_xxx",
        ...     signature_scheme="github",
        ...     event_type_header="X-GitHub-Event",
        ...     event_type_prefix="github",
        ...     dedup_header="X-GitHub-Delivery",
        ...     preprocessor="myapp.hooks:summarize_pr",
        ... )                                          # doctest: +SKIP
        >>> manager.register(listener)                 # doctest: +SKIP
        >>> listener.setup_routes(app)                 # doctest: +SKIP

    Args:
        config: Full listener configuration. Built from defaults when omitted.
        base_path: Convenience override for ``config.base_path``.
        **kwargs: Forwarded to :class:`~navigator_eventbus.hooks.base.BaseHook`.
    """

    hook_type: str = HookType.WEBHOOK

    def __init__(
        self,
        config: Optional[WebhookHookConfig] = None,
        *,
        base_path: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        cfg = config or WebhookHookConfig()
        if base_path is not None:
            cfg = cfg.model_copy(update={"base_path": normalize_path(base_path)})
        super().__init__(
            name=cfg.name,
            enabled=cfg.enabled,
            target_type=cfg.target_type,
            target_id=cfg.target_id,
            metadata=cfg.metadata,
            **kwargs,
        )
        self._config = cfg
        #: Public so operators can exclude the route from auth / body-rewriting
        #: middleware — HMAC verification needs the exact bytes on the wire.
        self.base_path: str = cfg.base_path
        self._endpoints: dict[str, WebhookEndpointConfig] = {}
        self._states: dict[str, WebhookEndpointState] = {}
        self._init_ingest(
            max_body_bytes=cfg.max_body_bytes,
            dispatch_mode=cfg.dispatch_mode,
            dedup_ttl_seconds=cfg.dedup_ttl_seconds,
            dedup_cache_size=cfg.dedup_cache_size,
        )
        # Safe to build outside a running loop: since Python 3.10 an
        # asyncio.Semaphore no longer binds to a loop at construction time.
        self._semaphore: Optional[asyncio.Semaphore] = (
            asyncio.Semaphore(cfg.max_inflight) if cfg.max_inflight > 0 else None
        )
        for endpoint in cfg.endpoints:
            self._install(endpoint)

    # ------------------------------------------------------------------
    # Endpoint registry
    #
    # No locking: registration and request handling both run on the event
    # loop, and dict get/set is atomic under the GIL. The only await inside
    # _ingest that could interleave with a mutation is the body read, and the
    # endpoint object is captured by reference before it — so unregistering
    # mid-flight completes the in-flight delivery, which is what we want.
    # ------------------------------------------------------------------

    def _install(self, endpoint: WebhookEndpointConfig) -> WebhookEndpointConfig:
        """Add *endpoint* to the registry, replacing any same-path entry."""
        if endpoint.path in self._endpoints:
            self.logger.warning(
                "Webhook endpoint %s already registered — replacing", endpoint.path
            )
        self._endpoints[endpoint.path] = endpoint
        self._states.setdefault(endpoint.path, WebhookEndpointState())
        return endpoint

    def register_endpoint(
        self, path: str, **kwargs: Any
    ) -> WebhookEndpointConfig:
        """Register an endpoint at runtime.

        The listener's ``default_signature_scheme`` and
        ``default_require_signature`` fill in whatever the caller omits, so a
        deployment can enforce a house policy without repeating it per call.

        Args:
            path: Path relative to :attr:`base_path`, e.g. ``"/github"``.
            **kwargs: Any :class:`WebhookEndpointConfig` field.

        Returns:
            The validated endpoint configuration.

        Raises:
            pydantic.ValidationError: The configuration is invalid — for
                instance a signed endpoint with no secret, or an unresolvable
                preprocessor import string.
        """
        kwargs.setdefault("signature_scheme", self._config.default_signature_scheme)
        kwargs.setdefault("require_signature", self._config.default_require_signature)
        return self._install(WebhookEndpointConfig(path=path, **kwargs))

    def unregister_endpoint(self, path: str) -> Optional[WebhookEndpointConfig]:
        """Remove an endpoint. Returns the removed config, or ``None``."""
        key = normalize_path(path)
        endpoint = self._endpoints.pop(key, None)
        self._states.pop(key, None)
        if endpoint is not None:
            self.logger.info("Unregistered webhook endpoint %s", key)
        return endpoint

    def get_endpoint(self, path: str) -> Optional[WebhookEndpointConfig]:
        """Look up an endpoint by (normalized) relative path."""
        return self._endpoints.get(normalize_path(path))

    @property
    def endpoints(self) -> dict[str, WebhookEndpointConfig]:
        """A snapshot of the endpoint registry, keyed by relative path."""
        return dict(self._endpoints)

    @property
    def stats(self) -> dict[str, Any]:
        """Aggregate and per-endpoint traffic counters."""
        per_endpoint = {
            path: state.as_dict() for path, state in self._states.items()
        }
        totals: dict[str, int] = {}
        for snapshot in per_endpoint.values():
            for key, value in snapshot.items():
                if isinstance(value, int):
                    totals[key] = totals.get(key, 0) + value
        return {
            "base_path": self.base_path,
            "endpoints": len(self._endpoints),
            "totals": totals,
            "per_endpoint": per_endpoint,
        }

    # ------------------------------------------------------------------
    # BaseHook contract
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Mark the listener ready; routes attach via :meth:`setup_routes`."""
        if self._callback is None:
            self.logger.warning(
                "WebhookListenerHook '%s' started with NO callback registered. "
                "Deliveries will be acknowledged and DROPPED until it is "
                "registered with a HookManager.",
                self.name,
            )
        unsigned = [
            path for path, ep in self._endpoints.items() if not ep.secret
        ]
        if unsigned:
            self.logger.warning(
                "WebhookListenerHook '%s' has unauthenticated endpoints: %s",
                self.name,
                ", ".join(sorted(unsigned)),
            )
        self.logger.info(
            "WebhookListenerHook '%s' ready at %s with %d endpoint(s)",
            self.name,
            self.base_path,
            len(self._endpoints),
        )

    async def stop(self) -> None:
        """Cancel any in-flight background dispatches."""
        await self._drain_tasks()
        self.logger.info("WebhookListenerHook '%s' stopped", self.name)

    def setup_routes(self, app: Any) -> None:
        """Register the catch-all POST route (and optionally ``_list``).

        ``_list`` is added first so the literal path is matched before the
        ``.*`` pattern; they also differ by method, so this is belt and
        braces. The extra bare-``base_path`` POST route matters because
        ``{webhook_id:.*}`` requires the separating slash and would not match
        ``POST <base_path>`` on its own — a case the ai-parrot original
        dropped silently.
        """
        if self._config.expose_list_route:
            app.router.add_get(f"{self.base_path}/_list", self._list_endpoints)
        app.router.add_post(
            f"{self.base_path}/{{webhook_id:.*}}", self._handle_webhook
        )
        app.router.add_post(self.base_path, self._handle_webhook)
        self.logger.info(
            "Webhook listener routes registered: POST %s/{webhook_id}",
            self.base_path,
        )

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    async def _handle_webhook(self, request: web.Request) -> web.Response:
        """Resolve the endpoint from match-info and run the ingest pipeline."""
        raw_id = request.match_info.get("webhook_id", "")
        key = "/" + raw_id.strip("/") if raw_id.strip("/") else "/"
        endpoint = self._endpoints.get(key)
        if endpoint is None:
            return json_error(404, "unknown_endpoint", path=key)

        state = self._states.setdefault(key, WebhookEndpointState())

        if self._semaphore is None:
            return await self._guarded_ingest(request, endpoint, state)
        if self._semaphore.locked():
            state.rejected += 1
            self.logger.warning(
                "Webhook listener at capacity (max_inflight=%d) — rejecting %s",
                self._config.max_inflight,
                key,
            )
            return json_error(503, "busy", headers={"Retry-After": "1"})
        async with self._semaphore:
            return await self._guarded_ingest(request, endpoint, state)

    async def _guarded_ingest(
        self,
        request: web.Request,
        endpoint: WebhookEndpointConfig,
        state: WebhookEndpointState,
    ) -> web.Response:
        """Run :meth:`_ingest`, converting an unexpected crash into a 500."""
        try:
            return await self._ingest(request, endpoint, state)
        except Exception as exc:  # noqa: BLE001 — never leak a traceback
            self.logger.exception(
                "Webhook %s failed with an unexpected error: %s", endpoint.path, exc
            )
            return json_error(500, "error")

    async def _list_endpoints(self, request: web.Request) -> web.Response:
        """Return a token-guarded introspection view of every endpoint."""
        token = self._config.list_route_token or ""
        supplied = request.headers.get("X-API-Key") or request.query.get("token") or ""
        authorization = request.headers.get("Authorization", "")
        if authorization.startswith("Bearer "):
            supplied = authorization[7:]
        if not hmac.compare_digest(supplied.encode(), token.encode()):
            return json_error(401, "unauthorized")
        return web.json_response(
            {
                "base_path": self.base_path,
                "endpoints": [
                    {
                        "path": path,
                        "name": endpoint.name,
                        "enabled": endpoint.enabled,
                        "signed": bool(endpoint.secret),
                        "signature_scheme": endpoint.signature_scheme,
                        "event_type": endpoint.event_type,
                        **self._states.get(path, WebhookEndpointState()).as_dict(),
                    }
                    for path, endpoint in sorted(self._endpoints.items())
                ],
            }
        )
