"""Unit tests for TraceContext (FEAT-313 TASK-1820)."""
import pytest

from navigator_eventbus.lifecycle.trace import TraceContext


class TestTraceContext:
    def test_new_root_creates_valid_trace(self):
        tc = TraceContext.new_root()
        assert len(tc.trace_id) == 32
        assert len(tc.span_id) == 16
        assert tc.parent_span_id is None
        assert tc.trace_flags == 1

    def test_child_preserves_trace_id(self):
        root = TraceContext.new_root()
        child = root.child()
        assert child.trace_id == root.trace_id
        assert child.parent_span_id == root.span_id
        assert child.span_id != root.span_id

    def test_traceparent_header_roundtrip(self):
        tc = TraceContext.new_root()
        header = tc.to_traceparent_header()
        assert header.startswith("00-")
        restored = TraceContext.from_traceparent_header(header)
        assert restored.trace_id == tc.trace_id
        assert restored.span_id == tc.span_id
        assert restored.trace_flags == tc.trace_flags

    def test_from_traceparent_header_rejects_malformed(self):
        with pytest.raises(ValueError):
            TraceContext.from_traceparent_header("not-a-valid-header")

    def test_from_traceparent_header_rejects_empty(self):
        with pytest.raises(ValueError):
            TraceContext.from_traceparent_header("")

    def test_to_dict_from_dict_roundtrip(self):
        tc = TraceContext.new_root()
        d = tc.to_dict()
        restored = TraceContext.from_dict(d)
        assert restored == tc

    def test_frozen(self):
        tc = TraceContext.new_root()
        with pytest.raises(AttributeError):
            tc.trace_id = "mutated"
