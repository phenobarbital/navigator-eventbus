"""ProviderWebhookHook — reusable base for a fixed, single-route webhook.

Where :class:`~navigator_eventbus.hooks.webhook.listener.WebhookListenerHook`
serves many endpoints from one catch-all route, this class serves exactly one
provider on its own fixed route. Use it when the integration needs real
classification and normalization logic — turning a vendor's nested payload
into a stable, provider-agnostic dict — rather than a generic passthrough.

**This package intentionally ships no concrete provider hook.** The FEAT-312
extraction left integration logic in the consuming application (see the
``navigator_eventbus.hooks`` package docstring); shipping a ``GitHubWebhookHook``
here would re-import the boundary that extraction drew. What ships is the
base class and its contract.
"""
from __future__ import annotations

from typing import Any, Optional

from aiohttp import web

from navigator_eventbus.hooks.base import BaseHook
from navigator_eventbus.hooks.models import HOOK_TYPES, HookType
from navigator_eventbus.hooks.webhook.models import (
    WebhookEndpointConfig,
    WebhookEndpointState,
)
from navigator_eventbus.hooks.webhook.receiver import WebhookIngestMixin, json_error

__all__ = ("ProviderWebhookHook",)


class ProviderWebhookHook(WebhookIngestMixin, BaseHook):
    """Base class for a single-provider webhook receiver on a fixed route.

    Subclasses set :attr:`hook_type` (which **must** already be registered
    with ``HOOK_TYPES``) and override :meth:`_classify_event` and
    :meth:`_normalize_payload`.

    Example:
        >>> from navigator_eventbus.hooks.models import HOOK_TYPES
        >>> HOOK_TYPES.register("acme_webhook")            # doctest: +SKIP
        >>> class AcmeWebhookHook(ProviderWebhookHook):    # doctest: +SKIP
        ...     hook_type = "acme_webhook"
        ...     default_signature_scheme = "github"
        ...     default_event_type_header = "X-Acme-Event"
        ...
        ...     def _classify_event(self, request, endpoint, payload):
        ...         if payload.get("action") not in {"opened", "closed"}:
        ...             return None          # -> HTTP 200, nothing emitted
        ...         return f"acme.{payload['action']}"
        ...
        ...     def _normalize_payload(self, request, endpoint, payload, event_type):
        ...         return {"id": payload.get("id"), "action": payload.get("action")}

    Args:
        config: The endpoint configuration. Here ``path`` is the **absolute**
            route to mount, not a path relative to a listener's base path.
        **kwargs: Forwarded to :class:`~navigator_eventbus.hooks.base.BaseHook`.

    Raises:
        ValueError: :attr:`hook_type` is not registered with ``HOOK_TYPES``.
    """

    hook_type: str = HookType.WEBHOOK

    #: Applied by :meth:`build_config` when the caller does not specify one.
    default_signature_scheme: str = "generic"
    #: Applied by :meth:`build_config` when the caller does not specify one.
    default_event_type_header: Optional[str] = None

    def __init__(self, config: WebhookEndpointConfig, **kwargs: Any) -> None:
        # Fail at construction rather than on the first live delivery: the
        # HookEvent validator rejects unregistered types, and that failure
        # would otherwise surface as a 500 in production traffic.
        if not HOOK_TYPES.is_registered(self.hook_type):
            raise ValueError(
                f"{type(self).__name__}.hook_type={self.hook_type!r} is not "
                "registered. Call HOOK_TYPES.register(...) at import time "
                "(navigator_eventbus.hooks.models) before instantiating the hook, "
                "and add the namespace to TOPICS.md."
            )
        super().__init__(
            name=config.name or type(self).__name__,
            enabled=config.enabled,
            target_type=config.target_type,
            target_id=config.target_id,
            metadata=config.metadata,
            **kwargs,
        )
        self._config = config
        self._state = WebhookEndpointState()
        self._init_ingest(
            max_body_bytes=config.max_body_bytes or 1_048_576,
            dispatch_mode="await",
            dedup_ttl_seconds=0,
        )

    @classmethod
    def build_config(cls, path: str, **kwargs: Any) -> WebhookEndpointConfig:
        """Build an endpoint config pre-filled with this provider's defaults.

        Args:
            path: The absolute route to mount.
            **kwargs: Any :class:`WebhookEndpointConfig` field.

        Returns:
            The validated configuration.
        """
        kwargs.setdefault("signature_scheme", cls.default_signature_scheme)
        if cls.default_event_type_header is not None:
            kwargs.setdefault("event_type_header", cls.default_event_type_header)
        return WebhookEndpointConfig(path=path, **kwargs)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def url(self) -> str:
        """The absolute route this hook serves.

        Public on purpose. HMAC verification needs the exact bytes on the
        wire, so operators must exclude this path from auth middleware and
        from anything that reads or rewrites the request body (e.g.
        ``navigator-auth``'s ``add_exclude_list(hook.url)``).
        """
        return self._config.path

    @property
    def config(self) -> WebhookEndpointConfig:
        """The endpoint configuration this hook serves."""
        return self._config

    @property
    def stats(self) -> dict[str, Any]:
        """Traffic counters for this hook's single endpoint."""
        return {"url": self.url, **self._state.as_dict()}

    # ------------------------------------------------------------------
    # BaseHook contract
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Mark the hook ready; the route attaches via :meth:`setup_routes`."""
        if self._callback is None:
            self.logger.warning(
                "%s '%s' started with NO callback registered. Deliveries will "
                "be acknowledged and DROPPED until it is registered with a "
                "HookManager.",
                type(self).__name__,
                self.name,
            )
        if not self._config.secret:
            self.logger.warning(
                "%s '%s' has no secret — deliveries at %s are unauthenticated.",
                type(self).__name__,
                self.name,
                self.url,
            )
        self.logger.info(
            "%s '%s' ready (route via setup_routes: POST %s)",
            type(self).__name__,
            self.name,
            self.url,
        )

    async def stop(self) -> None:
        """Cancel any in-flight background dispatches."""
        await self._drain_tasks()
        self.logger.info("%s '%s' stopped", type(self).__name__, self.name)

    def setup_routes(self, app: Any) -> None:
        """Register ``POST <url>`` on the aiohttp application."""
        app.router.add_post(self.url, self._handle_post)
        self.logger.info(
            "%s route registered: POST %s", type(self).__name__, self.url
        )

    async def _handle_post(self, request: web.Request) -> web.Response:
        """Run the shared ingest pipeline, converting a crash into a 500."""
        try:
            return await self._ingest(request, self._config, self._state)
        except Exception as exc:  # noqa: BLE001 — never leak a traceback
            self.logger.exception(
                "%s failed with an unexpected error: %s", type(self).__name__, exc
            )
            return json_error(500, "error")
