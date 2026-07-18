"""Unit tests for EventEmitterMixin + set_bootstrap_hook() (FEAT-313 TASK-1822)."""

from navigator_eventbus.lifecycle.mixin import EventEmitterMixin, set_bootstrap_hook
from navigator_eventbus.lifecycle.registry import EventRegistry


class _Host(EventEmitterMixin):
    def __init__(self, **kwargs):
        self._init_events(**kwargs)


class TestEventEmitterMixin:
    def setup_method(self):
        set_bootstrap_hook(None)  # reset between tests

    def teardown_method(self):
        set_bootstrap_hook(None)

    def test_init_events_creates_registry(self):
        host = _Host()
        assert host.events is not None
        assert isinstance(host.events, EventRegistry)

    def test_bootstrap_hook_invoked(self):
        calls = []
        set_bootstrap_hook(lambda: calls.append(1))
        _Host()
        assert len(calls) == 1

    def test_bootstrap_hook_called_per_init(self):
        calls = []
        set_bootstrap_hook(lambda: calls.append(1))
        _Host()
        _Host()
        assert len(calls) == 2

    def test_bootstrap_hook_failure_swallowed(self):
        def bad_hook():
            raise RuntimeError("hook exploded")

        set_bootstrap_hook(bad_hook)
        host = _Host()  # must not raise
        assert host.events is not None

    def test_no_hook_noop(self):
        set_bootstrap_hook(None)
        host = _Host()  # must not raise
        assert host.events is not None

    def test_events_property_without_init_lazily_creates_registry(self):
        """NOTE: deviation from the task's example Test Specification, which
        asserted this raises AttributeError. The verified Codebase Contract
        (and the original ai-parrot mixin.py:83-94 source) is explicit that
        accessing `events` without `_init_events()` lazily creates a default,
        globally-forwarding registry instead of raising — preserved here
        verbatim per the spec's "preserve API signatures exactly" AC.
        """
        mixin = EventEmitterMixin()
        registry = mixin.events
        assert isinstance(registry, EventRegistry)
        # Second access returns the same lazily-created instance.
        assert mixin.events is registry

    def test_init_events_forward_to_global_false(self):
        host = _Host(forward_to_global=False)
        assert host.events._forward_to_global is False

    def test_init_events_passes_event_bus(self):
        sentinel = object()
        host = _Host(event_bus=sentinel)
        assert host.events._event_bus is sentinel
