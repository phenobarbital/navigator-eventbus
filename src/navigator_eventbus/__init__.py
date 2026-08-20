"""navigator_eventbus — standalone async event bus + generic hooks fabric.

Extracted from ai-parrot's `parrot.core.events` / `parrot.core.hooks`
(FEAT-310) as part of the `navigator-eventbus` extraction plan (see
`sdd/proposals/navigator-eventbus-extraction.brainstorm.md` in ai-parrot).

This is Phase 1 (FEAT-312). Public API re-exports the same surface as
FEAT-310's `parrot.core.events` under the new import root — see
`sdd/specs/eventbus-core-extraction.spec.md` §2 "New Public Interfaces".

Phase 2 (FEAT-313) adds the `lifecycle` subpackage — typed, frozen
lifecycle event machinery (`EventRegistry`, `EventEmitterMixin`, generic
subscribers, etc.), independent of the bus core above. See
`sdd/specs/eventbus-lifecycle-extraction.spec.md`.
"""
from navigator_eventbus import lifecycle
from navigator_eventbus.backends.composite import CompositeBackend
from navigator_eventbus.core import BackpressureError, BusClosedError, BusCore
from navigator_eventbus.dlq import DLQHandler
from navigator_eventbus.envelope import (
    ENVELOPE_SCHEMA_VERSION,
    EventEnvelope,
    Severity,
    UnsupportedSchemaVersion,
)
from navigator_eventbus.evb import Event, EventBus, EventPriority, EventSubscription
from navigator_eventbus.ingress_models import IngressEnvelope
from navigator_eventbus.version import (
    __author__,
    __author_email__,
    __copyright__,
    __description__,
    __license__,
    __title__,
    __version__,
)
from navigator_eventbus.webhook_signatures import (
    SignatureCheck,
    SignatureScheme,
    SignatureVerdict,
    available_signature_schemes,
    get_signature_scheme,
    register_signature_scheme,
)

__all__ = [
    "__author__",
    "__author_email__",
    "__copyright__",
    "__description__",
    "__license__",
    "__title__",
    "__version__",
    "BackpressureError",
    "BusClosedError",
    "BusCore",
    "CompositeBackend",
    "DLQHandler",
    "ENVELOPE_SCHEMA_VERSION",
    "Event",
    "EventBus",
    "EventEnvelope",
    "EventPriority",
    "EventSubscription",
    "IngressEnvelope",
    "Severity",
    "UnsupportedSchemaVersion",
    # Webhook signature registry — the extension point an integrator reaches
    # for from the root when teaching the fabric a new provider.
    "SignatureCheck",
    "SignatureScheme",
    "SignatureVerdict",
    "available_signature_schemes",
    "get_signature_scheme",
    "register_signature_scheme",
    # SQS-style pull queues (FEAT-432). Resolved lazily — see __getattr__.
    "QueueAPI",
    "QueueConfig",
    "QueueGroupConfig",
    "QueueRegistry",
    "QueueStore",
    "lifecycle",
]


#: Queue names resolved on demand, so callers that never touch a queue do not
#: pay for the store/API machinery at import time.
#:
#: Note this is NOT about the ``[redis]`` extra: ``navconfig[default]`` — a
#: core dependency — requires ``redis~=5.2.1`` and imports it at module level,
#: so the redis package is always present regardless of extras.
_QUEUE_EXPORTS = {
    "QueueAPI": "navigator_eventbus.queues.api",
    "QueueConfig": "navigator_eventbus.queues.config",
    "QueueGroupConfig": "navigator_eventbus.queues.config",
    "QueueRegistry": "navigator_eventbus.queues.config",
    "QueueStore": "navigator_eventbus.queues.store",
}


def __getattr__(name: str):
    """Resolve the lazily-exported queue names on first access."""
    module_path = _QUEUE_EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(module_path), name)
