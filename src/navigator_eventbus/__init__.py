"""navigator_eventbus — standalone async event bus + generic hooks fabric.

Extracted from ai-parrot's `parrot.core.events` / `parrot.core.hooks`
(FEAT-310) as part of the `navigator-eventbus` extraction plan (see
`sdd/proposals/navigator-eventbus-extraction.brainstorm.md` in ai-parrot).

This is Phase 1 (FEAT-312). Public API re-exports the same surface as
FEAT-310's `parrot.core.events` under the new import root — see
`sdd/specs/eventbus-core-extraction.spec.md` §2 "New Public Interfaces".
"""
from navigator_eventbus.core import BackpressureError, BusClosedError, BusCore
from navigator_eventbus.dlq import DLQHandler
from navigator_eventbus.envelope import EventEnvelope, Severity
from navigator_eventbus.evb import Event, EventBus, EventPriority, EventSubscription
from navigator_eventbus.ingress_models import IngressEnvelope

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "BackpressureError",
    "BusClosedError",
    "BusCore",
    "DLQHandler",
    "Event",
    "EventBus",
    "EventEnvelope",
    "EventPriority",
    "EventSubscription",
    "IngressEnvelope",
    "Severity",
]
