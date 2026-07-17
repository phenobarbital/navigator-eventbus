"""Serialization helper for navigator-eventbus (FEAT-312, Module 2).

JSON via ``datamodel.parsers.json.JSONContent`` (orjson-backed) is the
default, baseline wire format for ``EventEnvelope.to_dict()`` payloads
(consistent with the Redis pub/sub and Streams backends). ``cloudpickle``
is available as an explicit opt-in for callers that need to round-trip
non-JSON-safe Python objects — it is lazy-imported and raises a clear,
actionable error if the ``[pickle]`` extra is not installed (same
lazy-guarded pattern as ``dlq.py``'s DSN fallback in ai-parrot).
"""
from typing import Any

from datamodel.parsers.json import JSONContent

_json = JSONContent()


def dumps(obj: Any) -> str:
    """Serialize *obj* to a JSON string via ``JSONContent`` (orjson).

    Args:
        obj: A JSON-safe object — typically ``EventEnvelope.to_dict()``.

    Returns:
        The JSON encoding of *obj*.
    """
    return _json.dumps(obj)


def loads(data: bytes | str) -> Any:
    """Deserialize JSON *data* via ``JSONContent`` (orjson).

    Args:
        data: JSON bytes or string produced by :func:`dumps` (or any
            JSON-compatible source).

    Returns:
        The decoded Python object.
    """
    return _json.loads(data)


def dumps_pickle(obj: Any) -> bytes:
    """Serialize *obj* with ``cloudpickle`` (optional, non-JSON payloads).

    Args:
        obj: Any picklable Python object.

    Returns:
        The cloudpickle byte-stream.

    Raises:
        RuntimeError: If ``cloudpickle`` is not installed — install with
            ``pip install navigator-eventbus[pickle]``.
    """
    try:
        import cloudpickle
    except ImportError as exc:
        raise RuntimeError(
            "cloudpickle is required for pickle serialization. "
            "Install it with: pip install navigator-eventbus[pickle]"
        ) from exc
    return cloudpickle.dumps(obj)


def loads_pickle(data: bytes) -> Any:
    """Deserialize a ``cloudpickle`` byte-stream produced by :func:`dumps_pickle`.

    Args:
        data: Bytes produced by :func:`dumps_pickle`.

    Returns:
        The decoded Python object.

    Raises:
        RuntimeError: If ``cloudpickle`` is not installed — install with
            ``pip install navigator-eventbus[pickle]``.
    """
    try:
        import cloudpickle
    except ImportError as exc:
        raise RuntimeError(
            "cloudpickle is required for pickle serialization. "
            "Install it with: pip install navigator-eventbus[pickle]"
        ) from exc
    return cloudpickle.loads(data)
