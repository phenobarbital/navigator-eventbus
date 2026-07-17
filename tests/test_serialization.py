"""Unit tests for the serialization helper (FEAT-312, TASK-1799)."""
from datetime import datetime, timezone

import pytest

from navigator_eventbus import EventEnvelope, Severity
from navigator_eventbus.serialization import dumps, loads


def test_serialization_jsoncontent_roundtrip():
    env = EventEnvelope(topic="test.topic", payload={"a": 1}, severity=Severity.INFO)
    data = dumps(env.to_dict())
    assert isinstance(data, (bytes, str))
    restored = EventEnvelope.from_dict(loads(data))
    assert restored.topic == env.topic
    assert restored.payload == {"a": 1}


def test_serialization_roundtrip_preserves_all_fields():
    env = EventEnvelope(
        topic="order.created",
        payload={"k": 1},
        timestamp=datetime.now(timezone.utc),
        source="unit-test",
        severity=Severity.WARNING,
        correlation_id="corr-1",
        trace_context={"traceparent": "00-abc-def-01"},
        metadata={"m": "v"},
    )
    restored = EventEnvelope.from_dict(loads(dumps(env.to_dict())))
    assert restored == env


def test_pickle_helpers_raise_actionable_error_when_uninstalled(monkeypatch):
    import builtins

    from navigator_eventbus import serialization

    real_import = builtins.__import__

    def failing_import(name, *args, **kwargs):
        if name == "cloudpickle":
            raise ImportError("no cloudpickle")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", failing_import)
    with pytest.raises(RuntimeError, match="navigator-eventbus\\[pickle\\]"):
        serialization.dumps_pickle({"a": 1})
    with pytest.raises(RuntimeError, match="navigator-eventbus\\[pickle\\]"):
        serialization.loads_pickle(b"")
