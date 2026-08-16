"""Inbound webhook fabric for navigator-eventbus.

Two hooks share one ingest pipeline:

- :class:`~navigator_eventbus.hooks.webhook.listener.WebhookListenerHook` —
  a single catch-all aiohttp route fronting N endpoints that can be
  registered and removed at runtime.
- :class:`~navigator_eventbus.hooks.webhook.provider.ProviderWebhookHook` —
  a reusable base for a fixed, single-route provider integration.

Both are :class:`~navigator_eventbus.hooks.base.BaseHook` subclasses, so they
mount via ``HookManager.setup_routes(app)`` and emit through
``self.on_event()`` onto ``hooks.<hook_type>.<event_type>`` — the package
owns the topic, unlike ``ingress/`` where the caller supplies it.

Hook classes are imported lazily so that importing the configuration models
does not pull in aiohttp request machinery.
"""
from typing import TYPE_CHECKING, Any

from navigator_eventbus.hooks.webhook.models import (
    DEFAULT_WEBHOOK_BASE_PATH,
    WebhookEndpointConfig,
    WebhookEndpointState,
    WebhookHookConfig,
    normalize_path,
)
from navigator_eventbus.hooks.webhook.preprocess import (
    PreprocessResult,
    WebhookContext,
    run_preprocessor,
)

if TYPE_CHECKING:  # pragma: no cover
    from navigator_eventbus.hooks.webhook.listener import WebhookListenerHook
    from navigator_eventbus.hooks.webhook.provider import ProviderWebhookHook
    from navigator_eventbus.hooks.webhook.receiver import WebhookIngestMixin

_LAZY_MAP = {
    "ProviderWebhookHook": "navigator_eventbus.hooks.webhook.provider",
    "WebhookIngestMixin": "navigator_eventbus.hooks.webhook.receiver",
    "WebhookListenerHook": "navigator_eventbus.hooks.webhook.listener",
}

__all__ = (
    "DEFAULT_WEBHOOK_BASE_PATH",
    "PreprocessResult",
    "ProviderWebhookHook",
    "WebhookContext",
    "WebhookEndpointConfig",
    "WebhookEndpointState",
    "WebhookHookConfig",
    "WebhookIngestMixin",
    "WebhookListenerHook",
    "normalize_path",
    "run_preprocessor",
)


def __getattr__(name: str) -> Any:
    """Resolve hook classes lazily on first attribute access."""
    module_path = _LAZY_MAP.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(module_path), name)


def __dir__() -> list[str]:
    return sorted(__all__)
