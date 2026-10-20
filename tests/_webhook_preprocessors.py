"""Importable preprocessor fixtures for the webhook tests.

``tests/`` has no ``__init__.py``, so pytest's default ``prepend`` import mode
puts the directory itself on ``sys.path``. That makes this module importable
as the top-level name ``_webhook_preprocessors``, which in turn makes the
import string ``"_webhook_preprocessors:to_upper"`` resolvable — exactly the
YAML-configured shape the real feature supports.
"""
import asyncio
from typing import Any

CALL_LOG: list[str] = []


def to_upper(payload: dict[str, Any]) -> dict[str, Any]:
    """Sync preprocessor: upper-case every string value."""
    return {k: (v.upper() if isinstance(v, str) else v) for k, v in payload.items()}


async def async_tag(payload: dict[str, Any]) -> dict[str, Any]:
    """Async preprocessor: add a marker key."""
    await asyncio.sleep(0)
    return {**payload, "async": True}


def with_context(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Two-argument preprocessor: record where the delivery arrived."""
    return {**payload, "ctx_path": ctx.path, "ctx_endpoint": ctx.endpoint_name}


def always_raises(payload: dict[str, Any]) -> dict[str, Any]:
    """Preprocessor that blows up, to exercise the fail-soft path."""
    raise RuntimeError("boom")


async def too_slow(payload: dict[str, Any]) -> dict[str, Any]:
    """Preprocessor that outlives any sane timeout."""
    await asyncio.sleep(30)
    return payload


def returns_tuple(payload: dict[str, Any]):
    """Preprocessor returning the ``(payload, event_type)`` shape."""
    return {**payload, "tupled": True}, "custom.event"


def returns_bad_type(payload: dict[str, Any]):
    """Preprocessor returning something unsupported."""
    return 12345


def returns_none(payload: dict[str, Any]) -> None:
    """Preprocessor that opts out of transforming anything."""
    return None


#: A module attribute that is deliberately NOT callable, so
#: ``resolve_callable`` has something concrete to reject.
NOT_CALLABLE = "just a string"
