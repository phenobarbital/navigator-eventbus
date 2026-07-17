"""navigator_eventbus — standalone async event bus + generic hooks fabric.

Extracted from ai-parrot's `parrot.core.events` / `parrot.core.hooks`
(FEAT-310) as part of the `navigator-eventbus` extraction plan (see
`sdd/proposals/navigator-eventbus-extraction.brainstorm.md` in ai-parrot).

This is Phase 1 (FEAT-312) — package scaffold. Real re-exports (EventBus,
Event, EventPriority, EventSubscription, EventEnvelope, Severity, BusCore)
are added by TASK-1799/TASK-1800.
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
