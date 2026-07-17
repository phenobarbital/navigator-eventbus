"""Smoke + top-level import tests for the navigator_eventbus package.

``test_package_imports`` is the original TASK-1798 scaffold smoke test.
The ``TestPackageImports`` class below is adapted from
``packages/ai-parrot/tests/core/events/test_eventbus_imports.py``
(ai-parrot@686aba1fe, FEAT-310, TASK-274) — ``test_all_exports`` checks
containment rather than equality since this package's ``__all__`` is a
superset of ``parrot.core.events.__all__`` (it also re-exports BusCore,
DLQHandler, IngressEnvelope, EventEnvelope, Severity, etc. — see spec §2
"New Public Interfaces").
"""
import navigator_eventbus


def test_package_imports():
    assert navigator_eventbus.__version__ == "0.1.0"


class TestPackageImports:
    def test_eventbus_import(self):
        from navigator_eventbus import Event, EventBus, EventPriority  # noqa: F401

        assert EventBus is not None
        assert Event is not None
        assert EventPriority is not None

    def test_event_subscription_import(self):
        from navigator_eventbus import EventSubscription  # noqa: F401

        assert EventSubscription is not None

    def test_all_exports_superset_of_legacy_parrot_core_events(self):
        assert {
            "EventBus",
            "Event",
            "EventPriority",
            "EventSubscription",
        }.issubset(set(navigator_eventbus.__all__))

    def test_event_model_fields(self):
        from navigator_eventbus import Event, EventPriority

        evt = Event(
            event_type="test.event",
            payload={"key": "value"},
            priority=EventPriority.NORMAL,
        )
        assert evt.event_type == "test.event"
        assert evt.payload == {"key": "value"}

    def test_event_priority_values(self):
        from navigator_eventbus import EventPriority

        assert EventPriority.LOW is not None
        assert EventPriority.NORMAL is not None
        assert EventPriority.HIGH is not None
