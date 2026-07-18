"""Unit tests for navigator_eventbus.brokers.serializers (TASK-1813, FEAT-316)."""
import json

import pytest

from navigator_eventbus.brokers.serializers import DataSerializer

try:
    from datamodel import BaseModel

    class SampleModel(BaseModel):
        """Module-level Model subclass — jsonpickle's ``loadclass`` cannot
        resolve locally-scoped classes, so this must live at module scope."""

        name: str = "sample"

except ImportError:  # pragma: no cover - datamodel is a core dependency
    SampleModel = None


@pytest.fixture
def serializer():
    return DataSerializer()


class TestDataSerializerJSON:
    def test_json_roundtrip_default(self, serializer):
        payload = {"event": "test", "n": 42, "nested": {"a": [1, 2]}}
        encoded = serializer.encode(payload)
        assert serializer.decode(encoded) == payload

    def test_json_is_default_format(self, serializer):
        # encoded output of a plain dict must be valid JSON, not pickle bytes
        encoded = serializer.encode({"k": "v"})
        assert json.loads(encoded) == {"k": "v"}


class TestOptionalBackends:
    def test_cloudpickle_optional(self, serializer, monkeypatch):
        # simulate missing cloudpickle → actionable error
        import builtins

        real_import = builtins.__import__

        def fake(name, *a, **kw):
            if name == "cloudpickle":
                raise ImportError(name)
            return real_import(name, *a, **kw)

        monkeypatch.setattr(builtins, "__import__", fake)
        with pytest.raises(RuntimeError, match=r"\[pickle\]|cloudpickle"):
            serializer.serialize(object())

    def test_cloudpickle_roundtrip(self, serializer):
        pytest.importorskip("cloudpickle")

        class Dummy:
            def __init__(self, value):
                self.value = value

        obj = Dummy(42)
        encoded = serializer.serialize(obj)
        restored = serializer.unserialize(encoded)
        assert restored.value == 42

    def test_msgpack_optional(self, serializer, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def fake(name, *a, **kw):
            if name == "msgpack":
                raise ImportError(name)
            return real_import(name, *a, **kw)

        monkeypatch.setattr(builtins, "__import__", fake)
        with pytest.raises(RuntimeError, match=r"\[serializer\]|msgpack"):
            serializer.pack({"k": "v"})

    def test_msgpack_roundtrip(self, serializer):
        pytest.importorskip("msgpack")
        payload = {"k": "v", "n": 1}
        packed = serializer.pack(payload)
        assert serializer.unpack(packed) == payload

    def test_jsonpickle_model_handler(self, serializer):
        pytest.importorskip("jsonpickle")
        obj = SampleModel()
        js = DataSerializer(use_jsonpickle=True)
        encoded = js.encode(obj)
        restored = js.decode(encoded)
        assert restored.name == "sample"
