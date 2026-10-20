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
import re

import navigator_eventbus
from navigator_eventbus import version as version_module


def test_package_imports():
    """The root must re-export the metadata declared in ``version.py``.

    Deliberately compared against ``version.py`` rather than a hard-coded
    literal. That module is the single source of truth — ``pyproject.toml``
    reads the version from it at build time — so duplicating the number here
    would only guarantee this test goes stale on the next release, which is
    exactly what had happened (it pinned ``0.2.1`` against a ``0.2.3``
    package).

    This is not a tautology: it exercises the root package's re-export
    wiring, which now includes a module-level ``__getattr__`` for the lazy
    queue exports — precisely the kind of change that can break attribute
    resolution silently.
    """
    assert navigator_eventbus.__version__ == version_module.__version__
    assert navigator_eventbus.__title__ == version_module.__title__
    assert navigator_eventbus.__author__ == version_module.__author__
    assert navigator_eventbus.__license__ == version_module.__license__


def test_version_is_a_sane_release_string():
    """Guard the shape, so an empty or malformed version cannot slip through."""
    assert re.fullmatch(
        r"\d+\.\d+(\.\d+)?([abrc]\d*|\.dev\d*|[-+.].+)?",
        navigator_eventbus.__version__,
    ), f"unexpected version string: {navigator_eventbus.__version__!r}"


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
