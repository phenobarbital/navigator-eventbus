"""DataSerializer — port of navigator.brokers.pickle (TASK-1813, FEAT-316).

JSON (orjson, via :mod:`navigator_eventbus.serialization`) is the default wire
format for :meth:`DataSerializer.encode`/:meth:`DataSerializer.decode`.
``jsonpickle`` (arbitrary object-graph encoding, with ``ModelHandler`` support
for ``datamodel.Model``/``datamodel.BaseModel`` instances), ``cloudpickle``
(binary pickle serialization), and ``msgpack`` (binary packing) are all lazy,
opt-in dependencies: none of them is imported at module load time, so this
module — and anything that only uses the JSON path — imports cleanly without
any of the three installed.
"""
from __future__ import annotations

import base64
from typing import Any

from navigator_eventbus.serialization import dumps, loads

_model_handler_registered = False


def _register_model_handler() -> None:
    """Register ``ModelHandler`` with jsonpickle's handler registry.

    Lazily imports ``jsonpickle`` and ``datamodel`` so this module has zero
    jsonpickle dependency until jsonpickle-based encode/decode is actually
    used. Idempotent — safe to call multiple times.

    Raises:
        RuntimeError: If ``jsonpickle`` is not installed.
    """
    global _model_handler_registered
    if _model_handler_registered:
        return

    try:
        import jsonpickle
        from jsonpickle.handlers import BaseHandler
        from jsonpickle.unpickler import loadclass
    except ImportError as exc:
        raise RuntimeError(
            "jsonpickle is required for object-graph serialization. "
            "Install it with: pip install navigator-eventbus[serializer]"
        ) from exc

    from datamodel import BaseModel, Model

    class ModelHandler(BaseHandler):
        """jsonpickle handler for ``datamodel`` Model/BaseModel instances."""

        def flatten(self, obj: Any, data: dict) -> dict:
            """Flatten a Model/BaseModel instance into a jsonpickle-safe dict."""
            data["__dict__"] = self.context.flatten(obj.__dict__, reset=False)
            return data

        def restore(self, obj: dict) -> Any:
            """Restore a Model/BaseModel instance from its flattened dict."""
            module_and_type = obj["py/object"]
            mdl = loadclass(module_and_type)
            cls = mdl.__new__(mdl)  # Create a new instance without calling __init__
            cls.__dict__.update(self.context.restore(obj["__dict__"], reset=False))
            return cls

    jsonpickle.handlers.registry.register(BaseModel, ModelHandler, base=True)
    jsonpickle.handlers.registry.register(Model, ModelHandler, base=True)
    _model_handler_registered = True


class DataSerializer:
    """Encode/decode/serialize/pack arbitrary Python objects.

    ``encode``/``decode`` default to JSON (orjson) via
    :mod:`navigator_eventbus.serialization`; ``serialize``/``unserialize`` use
    ``cloudpickle`` (opt-in, ``[pickle]`` extra); ``pack``/``unpack`` use
    ``msgpack`` (opt-in, ``[serializer]`` extra).

    Attributes:
        use_jsonpickle: When ``True``, ``encode``/``decode`` always go
            through ``jsonpickle`` (arbitrary object graphs via
            ``ModelHandler``) instead of trying plain JSON first.
    """

    def __init__(self, *, use_jsonpickle: bool = False) -> None:
        """Initialize the serializer.

        Args:
            use_jsonpickle: Force jsonpickle-based encode/decode instead of
                the JSON-first default.
        """
        self.use_jsonpickle = use_jsonpickle

    def encode(self, data: Any) -> str:
        """Serialize *data* to a string.

        Tries plain JSON (orjson) first, unless ``use_jsonpickle`` was set at
        construction time; falls back to ``jsonpickle`` when the payload is
        not JSON-safe.

        Args:
            data: The object to encode.

        Returns:
            The encoded string.

        Raises:
            RuntimeError: If the fallback jsonpickle path is required but
                ``jsonpickle`` is not installed, or encoding otherwise fails.
        """
        if not self.use_jsonpickle:
            try:
                return dumps(data)
            except Exception:
                pass  # not JSON-safe — fall through to jsonpickle
        return self._encode_jsonpickle(data)

    def decode(self, data: str) -> Any:
        """Deserialize a string produced by :meth:`encode`.

        Args:
            data: The encoded string.

        Returns:
            The decoded Python object.

        Raises:
            RuntimeError: If the fallback jsonpickle path is required but
                ``jsonpickle`` is not installed, or decoding otherwise fails.
        """
        if not self.use_jsonpickle:
            try:
                return loads(data)
            except Exception:
                pass  # not valid JSON — fall through to jsonpickle
        return self._decode_jsonpickle(data)

    def _encode_jsonpickle(self, data: Any) -> str:
        try:
            import jsonpickle
        except ImportError as exc:
            raise RuntimeError(
                "jsonpickle is required for object-graph serialization. "
                "Install it with: pip install navigator-eventbus[serializer]"
            ) from exc
        _register_model_handler()
        try:
            return jsonpickle.encode(data)
        except Exception as err:
            raise RuntimeError(err) from err

    def _decode_jsonpickle(self, data: str) -> Any:
        try:
            import jsonpickle
        except ImportError as exc:
            raise RuntimeError(
                "jsonpickle is required for object-graph serialization. "
                "Install it with: pip install navigator-eventbus[serializer]"
            ) from exc
        _register_model_handler()
        try:
            return jsonpickle.decode(data)
        except Exception as err:
            raise RuntimeError(err) from err

    def serialize(self, data: Any) -> str:
        """Serialize *data* with ``cloudpickle`` + base64 (opt-in).

        Args:
            data: Any picklable Python object.

        Returns:
            A base64-encoded string of the cloudpickle byte-stream.

        Raises:
            RuntimeError: If ``cloudpickle`` is not installed, or
                serialization otherwise fails.
        """
        try:
            import cloudpickle
        except ImportError as exc:
            raise RuntimeError(
                "cloudpickle is required for pickle serialization. "
                "Install it with: pip install navigator-eventbus[pickle]"
            ) from exc
        try:
            serialized_data = cloudpickle.dumps(data)
            return base64.b64encode(serialized_data).decode("utf-8")
        except Exception as err:
            raise RuntimeError(err) from err

    def unserialize(self, data: Any) -> Any:
        """Deserialize a payload produced by :meth:`serialize`.

        Args:
            data: A base64-encoded cloudpickle byte-stream.

        Returns:
            The decoded Python object.

        Raises:
            RuntimeError: If ``cloudpickle`` is not installed, or
                deserialization otherwise fails.
        """
        try:
            import cloudpickle
        except ImportError as exc:
            raise RuntimeError(
                "cloudpickle is required for pickle serialization. "
                "Install it with: pip install navigator-eventbus[pickle]"
            ) from exc
        try:
            decoded_data = base64.b64decode(data)
            return cloudpickle.loads(decoded_data)
        except Exception as err:
            raise RuntimeError(err) from err

    def pack(self, data: Any) -> bytes:
        """Pack *data* with ``msgpack`` (opt-in).

        Args:
            data: Any msgpack-serializable object.

        Returns:
            The packed bytes.

        Raises:
            RuntimeError: If ``msgpack`` is not installed, or packing
                otherwise fails.
        """
        try:
            import msgpack
        except ImportError as exc:
            raise RuntimeError(
                "msgpack is required for binary packing. "
                "Install it with: pip install navigator-eventbus[serializer]"
            ) from exc
        try:
            return msgpack.packb(data)
        except Exception as err:
            raise RuntimeError(err) from err

    def unpack(self, data: bytes) -> Any:
        """Unpack bytes produced by :meth:`pack`.

        Args:
            data: The packed bytes.

        Returns:
            The unpacked Python object.

        Raises:
            RuntimeError: If ``msgpack`` is not installed, or unpacking
                otherwise fails.
        """
        try:
            import msgpack
        except ImportError as exc:
            raise RuntimeError(
                "msgpack is required for binary packing. "
                "Install it with: pip install navigator-eventbus[serializer]"
            ) from exc
        try:
            return msgpack.unpackb(data)
        except Exception as err:
            raise RuntimeError(err) from err
