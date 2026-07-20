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

__all__ = [
    "__version__",
    "BackpressureError",
    "BusClosedError",
    "BusCore",
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
    "lifecycle",
]
