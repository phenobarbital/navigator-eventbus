"""Unit tests for SubscriberErrorEvent (FEAT-313 TASK-1820)."""
from dataclasses import FrozenInstanceError

import pytest

from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.meta import SubscriberErrorEvent
from navigator_eventbus.lifecycle.trace import TraceContext


class TestSubscriberErrorEvent:
    def test_inherits_lifecycle_event(self):
        evt = SubscriberErrorEvent(trace_context=TraceContext.new_root())
        assert isinstance(evt, LifecycleEvent)

    def test_frozen(self):
        evt = SubscriberErrorEvent(trace_context=TraceContext.new_root())
        with pytest.raises(FrozenInstanceError):
            evt.error_message = "mutated"

    def test_to_dict_truncates_long_traceback(self):
        long_tb = "\n".join(f"line {i}" for i in range(50))
        evt = SubscriberErrorEvent(
            trace_context=TraceContext.new_root(),
            traceback=long_tb,
        )
        d = evt.to_dict()
        assert d["traceback"].count("\n") == 19  # 20 lines → 19 newlines
        assert "line 49" in d["traceback"]
        assert "line 0" not in d["traceback"]

    def test_to_dict_preserves_short_traceback(self):
        short_tb = "line 1\nline 2"
        evt = SubscriberErrorEvent(
            trace_context=TraceContext.new_root(),
            traceback=short_tb,
        )
        d = evt.to_dict()
        assert d["traceback"] == short_tb
