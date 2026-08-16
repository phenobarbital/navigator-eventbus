"""Preprocessing seam for inbound webhook payloads.

An external system's payload rarely has the shape the consuming application
wants on the bus. A **preprocessor** is an operator-supplied callable that
transforms the parsed payload before it becomes a
:class:`~navigator_eventbus.hooks.models.HookEvent`.

Two properties define the contract:

- **Fail-soft at runtime.** If the preprocessor raises or times out, the
  delivery is *not* lost: the raw payload is emitted anyway, an error string
  is recorded in the event metadata, and the sender still receives a ``202``.
  (Resolution of a bad import string, by contrast, fails *fast* at
  configuration time — see
  :func:`navigator_eventbus._imports.resolve_callable`.)
- **No return-value heuristics.** A returned ``dict`` is *always* the payload.
  Sniffing it for envelope-shaped keys would eventually misfire on real
  provider traffic — a GitHub ``check_run`` body legitimately nests keys like
  ``payload``. Anything richer than a payload requires the explicit
  :class:`PreprocessResult` type.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:  # pragma: no cover
    from navigator_eventbus.hooks.webhook.models import WebhookEndpointConfig

__all__ = ("PreprocessResult", "WebhookContext", "run_preprocessor")


@dataclass(frozen=True)
class WebhookContext:
    """Read-only request context handed to a two-argument preprocessor.

    Deliberately *not* the aiohttp ``Request``: handing over the request would
    let a preprocessor consume the body stream out from under the ingest
    pipeline, and would couple operator-supplied code to aiohttp internals.

    Attributes:
        headers: The inbound request headers.
        path: The matched endpoint path.
        remote: Resolved client IP, or ``None`` when unavailable.
        content_type: The request ``Content-Type``, or an empty string.
        endpoint_name: Configured name of the endpoint that matched.
        method: The HTTP method of the delivery.
    """

    headers: Mapping[str, str] = field(default_factory=dict)
    path: str = ""
    remote: Optional[str] = None
    content_type: str = ""
    endpoint_name: str = ""
    method: str = "POST"


class PreprocessResult(BaseModel):
    """Structured return value from a webhook preprocessor.

    Returning this type (rather than a bare ``dict``) is the only way to
    influence anything beyond the payload.

    Attributes:
        payload: The payload to emit on the bus.
        event_type: Overrides the endpoint's configured event type.
        task: Optional prompt/task override carried on the ``HookEvent``.
        metadata: Extra metadata merged into the ``HookEvent`` metadata.
        ignore: When True the delivery is acknowledged with ``200`` and
            nothing is emitted — the "authenticated but not interesting"
            outcome.
    """

    model_config = ConfigDict(extra="forbid")

    payload: dict[str, Any] = Field(default_factory=dict)
    event_type: Optional[str] = None
    task: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    ignore: bool = False


def _coerce(result: Any, raw_payload: dict[str, Any]) -> PreprocessResult:
    """Normalize a preprocessor's return value into a :class:`PreprocessResult`.

    Args:
        result: Whatever the preprocessor returned.
        raw_payload: The payload passed in, used when the preprocessor
            returned ``None``.

    Returns:
        The normalized result.

    Raises:
        TypeError: The return value is not one of the supported shapes. The
            caller turns this into the fail-soft path.
    """
    if result is None:
        return PreprocessResult(payload=raw_payload)
    if isinstance(result, PreprocessResult):
        return result
    if isinstance(result, dict):
        # A dict is ALWAYS the payload — never sniffed for envelope keys.
        return PreprocessResult(payload=result)
    if isinstance(result, (tuple, list)) and len(result) == 2:
        payload, event_type = result
        if not isinstance(payload, dict):
            raise TypeError(
                "Preprocessor 2-tuple must be (dict, str | None); "
                f"first element was {type(payload).__name__}"
            )
        if event_type is not None and not isinstance(event_type, str):
            raise TypeError(
                "Preprocessor 2-tuple must be (dict, str | None); "
                f"second element was {type(event_type).__name__}"
            )
        return PreprocessResult(payload=payload, event_type=event_type)
    raise TypeError(
        "Preprocessor must return None, a dict, a (dict, str | None) tuple, "
        f"or a PreprocessResult; got {type(result).__name__}"
    )


async def run_preprocessor(
    endpoint: "WebhookEndpointConfig",
    payload: dict[str, Any],
    ctx: WebhookContext,
    *,
    logger: logging.Logger,
) -> PreprocessResult:
    """Run the endpoint's preprocessor, degrading to the raw payload on failure.

    Sync and async callables are both supported. Awaitability is decided from
    the *returned object* via :func:`inspect.isawaitable`, not from the
    function via ``asyncio.iscoroutinefunction``: the latter misses
    ``functools.partial`` wrapping a coroutine function and objects with an
    async ``__call__``.

    Whether the callable receives the ``ctx`` argument was decided once, at
    config-validation time, and cached on
    ``endpoint.preprocessor_accepts_ctx``.

    Args:
        endpoint: The endpoint configuration carrying the resolved callable.
        payload: The parsed request payload.
        ctx: Read-only request context for two-argument preprocessors.
        logger: Logger used to report a failing preprocessor.

    Returns:
        The preprocessor's result, or a fail-soft result wrapping the raw
        payload plus a ``preprocessor_error`` metadata entry.
    """
    fn = endpoint.preprocessor_fn
    if fn is None:
        return PreprocessResult(payload=payload)

    try:
        async with asyncio.timeout(endpoint.preprocess_timeout):
            result = fn(payload, ctx) if endpoint.preprocessor_accepts_ctx else fn(payload)
            if inspect.isawaitable(result):
                result = await result
        return _coerce(result, payload)
    except Exception as exc:  # noqa: BLE001 — fail soft, never drop a delivery
        logger.warning(
            "Webhook preprocessor %s failed for endpoint %s: %s",
            endpoint.preprocessor or getattr(fn, "__qualname__", repr(fn)),
            endpoint.path,
            exc,
            exc_info=True,
        )
        return PreprocessResult(
            payload=payload,
            metadata={"preprocessor_error": f"{type(exc).__name__}: {exc}"},
        )
