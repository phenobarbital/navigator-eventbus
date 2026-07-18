"""Unit tests for the EventProvider Protocol (FEAT-313 TASK-1821)."""
from navigator_eventbus.lifecycle.provider import EventProvider
from navigator_eventbus.lifecycle.registry import EventRegistry


class _ConformingProvider:
    def register(self, registry: "EventRegistry") -> None:
        pass


class _NonConformingProvider:
    """Missing register(registry) method entirely."""


class TestEventProvider:
    def test_runtime_checkable_conforming_instance(self):
        assert isinstance(_ConformingProvider(), EventProvider)

    def test_runtime_checkable_non_conforming_instance(self):
        assert isinstance(_NonConformingProvider(), EventProvider) is False

    def test_register_is_synchronous(self):
        import inspect

        assert not inspect.iscoroutinefunction(_ConformingProvider().register)
