"""Unit tests for LifecycleEvent (FEAT-313 TASK-1820)."""
import json
from dataclasses import FrozenInstanceError, dataclass

import pytest

from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.trace import TraceContext


@dataclass(frozen=True)
class _SampleEvent(LifecycleEvent):
    detail: str = ""


class TestLifecycleEvent:
    def test_frozen_instance(self):
        evt = _SampleEvent(
            trace_context=TraceContext.new_root(),
            source_type="test", source_name="unit",
        )
        with pytest.raises(FrozenInstanceError):
            evt.source_type = "mutated"

    def test_to_dict_json_serializable(self):
        evt = _SampleEvent(
            trace_context=TraceContext.new_root(),
            source_type="test", source_name="unit", detail="hello",
        )
        d = evt.to_dict()
        json.dumps(d)  # must not raise
        assert d["event_class"] == "_SampleEvent"
        assert d["detail"] == "hello"

    def test_to_dict_serializes_trace_context_and_timestamp(self):
        evt = _SampleEvent(
            trace_context=TraceContext.new_root(),
            source_type="test", source_name="unit",
        )
        d = evt.to_dict()
        assert isinstance(d["trace_context"], dict)
        assert isinstance(d["timestamp"], str)

    def test_event_id_auto_generated(self):
        evt = _SampleEvent(trace_context=TraceContext.new_root())
        assert evt.event_id
