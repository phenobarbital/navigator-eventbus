"""Tests for webhook payload preprocessing (webhook-support, TASK-B)."""
import functools
import logging

import pytest

from navigator_eventbus.hooks.webhook.models import WebhookEndpointConfig
from navigator_eventbus.hooks.webhook.preprocess import (
    PreprocessResult,
    WebhookContext,
    run_preprocessor,
)

LOGGER = logging.getLogger("test.webhook.preprocess")
PAYLOAD = {"action": "opened", "name": "demo"}


def endpoint(**kwargs) -> WebhookEndpointConfig:
    kwargs.setdefault("path", "/hook")
    kwargs.setdefault("require_signature", False)
    return WebhookEndpointConfig(**kwargs)


def ctx() -> WebhookContext:
    return WebhookContext(path="/hook", endpoint_name="demo", content_type="application/json")


async def run(cfg, payload=None):
    return await run_preprocessor(cfg, payload or dict(PAYLOAD), ctx(), logger=LOGGER)


# ---------------------------------------------------------------------------
# No preprocessor
# ---------------------------------------------------------------------------


async def test_no_preprocessor_returns_payload_unchanged():
    result = await run(endpoint())
    assert result.payload == PAYLOAD
    assert result.event_type is None
    assert result.ignore is False


# ---------------------------------------------------------------------------
# Sync / async / exotic callables
# ---------------------------------------------------------------------------


async def test_sync_preprocessor_returns_dict_payload():
    result = await run(endpoint(preprocessor="_webhook_preprocessors:to_upper"))
    assert result.payload == {"action": "OPENED", "name": "DEMO"}


async def test_async_preprocessor_is_awaited():
    result = await run(endpoint(preprocessor="_webhook_preprocessors:async_tag"))
    assert result.payload["async"] is True


async def test_partial_of_coroutine_is_awaited():
    """asyncio.iscoroutinefunction() would miss this; inspect.isawaitable does not."""

    async def tag(payload, marker):
        return {**payload, "marker": marker}

    cfg = endpoint(preprocessor_fn=functools.partial(tag, marker="p"))
    result = await run(cfg)
    assert result.payload["marker"] == "p"


async def test_callable_object_with_async_dunder_call_is_awaited():
    class AsyncCallable:
        async def __call__(self, payload):
            return {**payload, "via": "dunder"}

    result = await run(endpoint(preprocessor_fn=AsyncCallable()))
    assert result.payload["via"] == "dunder"


async def test_two_arg_preprocessor_receives_context():
    result = await run(endpoint(preprocessor="_webhook_preprocessors:with_context"))
    assert result.payload["ctx_path"] == "/hook"
    assert result.payload["ctx_endpoint"] == "demo"


# ---------------------------------------------------------------------------
# Return-value coercion
# ---------------------------------------------------------------------------


async def test_returning_none_keeps_raw_payload():
    result = await run(endpoint(preprocessor="_webhook_preprocessors:returns_none"))
    assert result.payload == PAYLOAD


async def test_returning_tuple_sets_event_type():
    result = await run(endpoint(preprocessor="_webhook_preprocessors:returns_tuple"))
    assert result.payload["tupled"] is True
    assert result.event_type == "custom.event"


async def test_returning_result_model_sets_task_and_metadata():
    def build(payload):
        return PreprocessResult(
            payload={"normalized": True},
            event_type="pr.opened",
            task="Review PR",
            metadata={"severity": "warning"},
        )

    result = await run(endpoint(preprocessor_fn=build))
    assert result.payload == {"normalized": True}
    assert result.event_type == "pr.opened"
    assert result.task == "Review PR"
    assert result.metadata == {"severity": "warning"}


async def test_returning_result_model_can_request_ignore():
    result = await run(endpoint(preprocessor_fn=lambda p: PreprocessResult(payload={}, ignore=True)))
    assert result.ignore is True


async def test_returning_dict_is_always_the_payload_never_an_envelope():
    """No key-sniffing: a dict with a 'payload' key is still just the payload."""
    tricky = {"payload": {"inner": 1}, "event_type": "should.not.be.lifted"}
    result = await run(endpoint(preprocessor_fn=lambda p: tricky))
    assert result.payload == tricky
    assert result.event_type is None


@pytest.mark.parametrize(
    "bad",
    [
        12345,
        "a string",
        ["only-one-element"],
        (1, 2, 3),
        ("not-a-dict", "evt"),
        ({"ok": 1}, 99),
    ],
)
async def test_returning_bad_type_falls_back_to_raw(bad):
    result = await run(endpoint(preprocessor_fn=lambda p: bad))
    assert result.payload == PAYLOAD
    assert "preprocessor_error" in result.metadata
    assert result.metadata["preprocessor_error"].startswith("TypeError")


# ---------------------------------------------------------------------------
# Fail-soft
# ---------------------------------------------------------------------------


async def test_exception_falls_back_to_raw_and_logs(caplog):
    caplog.set_level(logging.WARNING, logger="test.webhook.preprocess")
    result = await run(endpoint(preprocessor="_webhook_preprocessors:always_raises"))
    assert result.payload == PAYLOAD
    assert result.metadata["preprocessor_error"] == "RuntimeError: boom"
    assert any("failed for endpoint" in rec.message for rec in caplog.records)


async def test_timeout_falls_back_to_raw():
    cfg = endpoint(preprocessor="_webhook_preprocessors:too_slow", preprocess_timeout=0.01)
    result = await run(cfg)
    assert result.payload == PAYLOAD
    assert "preprocessor_error" in result.metadata
    assert "TimeoutError" in result.metadata["preprocessor_error"]


async def test_failure_does_not_raise_to_caller():
    """The whole point of fail-soft: the ingest pipeline keeps going."""
    result = await run(endpoint(preprocessor="_webhook_preprocessors:always_raises"))
    assert isinstance(result, PreprocessResult)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


def test_preprocess_result_forbids_extra_fields():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        PreprocessResult(payload={}, unexpected=1)


def test_webhook_context_is_frozen():
    import dataclasses

    context = ctx()
    with pytest.raises(dataclasses.FrozenInstanceError):
        context.path = "/other"  # type: ignore[misc]


def test_webhook_context_defaults():
    context = WebhookContext()
    assert context.method == "POST"
    assert context.remote is None
    assert context.headers == {}
