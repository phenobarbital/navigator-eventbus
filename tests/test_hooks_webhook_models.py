"""Tests for the webhook configuration models (webhook-support, TASK-B)."""
import json

import pytest
from pydantic import ValidationError

from navigator_eventbus.hooks.webhook.models import (
    WebhookEndpointConfig,
    WebhookEndpointState,
    WebhookHookConfig,
    normalize_path,
)


def endpoint(**kwargs) -> WebhookEndpointConfig:
    """Build an endpoint config, defaulting to the unsigned opt-out."""
    kwargs.setdefault("path", "/hook")
    kwargs.setdefault("require_signature", False)
    return WebhookEndpointConfig(**kwargs)


# ---------------------------------------------------------------------------
# Path normalization
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("github", "/github"),
        ("/github", "/github"),
        ("/github/", "/github"),
        ("  /github/  ", "/github"),
        ("a/b/c", "/a/b/c"),
        ("/", "/"),
    ],
)
def test_normalize_path(raw, expected):
    assert normalize_path(raw) == expected


@pytest.mark.parametrize("raw", ["", "   ", "/a//b", "/a/../b", "/a/./b", "a\\b"])
def test_normalize_path_rejects_bad_input(raw):
    with pytest.raises(ValueError):
        normalize_path(raw)


def test_endpoint_config_normalizes_path():
    assert endpoint(path="github/").path == "/github"


def test_endpoint_config_rejects_traversal_path():
    with pytest.raises(ValidationError):
        endpoint(path="/hooks/../admin")


def test_endpoint_name_defaults_to_path():
    assert endpoint(path="/gh").name == "/gh"


# ---------------------------------------------------------------------------
# Signature configuration
# ---------------------------------------------------------------------------


def test_endpoint_config_rejects_missing_secret_when_signature_required():
    with pytest.raises(ValidationError, match="require_signature=True"):
        WebhookEndpointConfig(path="/gh")


def test_endpoint_config_allows_unsigned_when_explicitly_opted_out():
    cfg = WebhookEndpointConfig(path="/gh", require_signature=False)
    assert cfg.secret is None
    assert cfg.require_signature is False


def test_endpoint_config_accepts_secret_with_default_strict_setting():
    cfg = WebhookEndpointConfig(path="/gh", secret="s3cr3t")
    assert cfg.require_signature is True


def test_endpoint_config_rejects_unknown_signature_scheme():
    with pytest.raises(ValidationError):
        endpoint(signature_scheme="not-a-real-scheme")


def test_endpoint_scheme_property_resolves():
    assert endpoint(signature_scheme="github").scheme.header == "X-Hub-Signature-256"


def test_endpoint_config_rejects_negative_tolerance():
    with pytest.raises(ValidationError):
        endpoint(tolerance_seconds=-1)


# ---------------------------------------------------------------------------
# IP allowlist
# ---------------------------------------------------------------------------


def test_endpoint_config_parses_cidr_allowed_ips():
    cfg = endpoint(allowed_ips=["127.0.0.1", "10.0.0.0/8", "::1"])
    assert len(cfg.parsed_networks) == 3


def test_endpoint_config_rejects_garbage_ip():
    with pytest.raises(ValidationError, match="Invalid IP or CIDR"):
        endpoint(allowed_ips=["not-an-ip"])


def test_parsed_networks_excluded_from_dump():
    cfg = endpoint(allowed_ips=["10.0.0.0/8"])
    assert "parsed_networks" not in cfg.model_dump()


# ---------------------------------------------------------------------------
# Preprocessor field modeling
# ---------------------------------------------------------------------------


def test_preprocessor_import_string_resolved_at_construction():
    cfg = endpoint(preprocessor="_webhook_preprocessors:to_upper")
    assert callable(cfg.preprocessor_fn)
    assert cfg.preprocessor_fn({"a": "x"}) == {"a": "X"}


def test_preprocessor_bad_import_string_raises_validation_error():
    with pytest.raises(ValidationError):
        endpoint(preprocessor="_webhook_preprocessors:does_not_exist")


def test_preprocessor_non_callable_import_string_raises_validation_error():
    with pytest.raises(ValidationError):
        endpoint(preprocessor="_webhook_preprocessors:NOT_CALLABLE")


def test_preprocessor_callable_wins_over_string():
    sentinel = lambda payload: {"from": "callable"}  # noqa: E731
    cfg = endpoint(
        preprocessor="_webhook_preprocessors:to_upper", preprocessor_fn=sentinel
    )
    assert cfg.preprocessor_fn is sentinel


def test_preprocessor_arity_cached_for_one_arg():
    cfg = endpoint(preprocessor="_webhook_preprocessors:to_upper")
    assert cfg.preprocessor_accepts_ctx is False


def test_preprocessor_arity_cached_for_two_args():
    cfg = endpoint(preprocessor="_webhook_preprocessors:with_context")
    assert cfg.preprocessor_accepts_ctx is True


def test_model_dump_json_round_trip_with_callable_set():
    """The key modeling guarantee: a live callable must not break serialization."""
    cfg = endpoint(path="/gh", preprocessor_fn=lambda payload: payload)
    dumped = cfg.model_dump(mode="json")
    assert "preprocessor_fn" not in dumped
    assert "preprocessor_accepts_ctx" not in dumped
    # Must be genuinely JSON-encodable, not merely dict-shaped.
    encoded = json.dumps(dumped)
    restored = WebhookEndpointConfig(**json.loads(encoded))
    assert restored.path == "/gh"
    assert restored.preprocessor_fn is None


def test_model_dump_round_trip_preserves_import_string():
    cfg = endpoint(preprocessor="_webhook_preprocessors:to_upper")
    restored = WebhookEndpointConfig(**json.loads(json.dumps(cfg.model_dump(mode="json"))))
    assert restored.preprocessor == "_webhook_preprocessors:to_upper"
    assert callable(restored.preprocessor_fn)


def test_model_json_schema_does_not_raise():
    """Guards the SkipJsonSchema annotation on the Callable field."""
    schema = WebhookEndpointConfig.model_json_schema()
    assert "preprocessor_fn" not in schema["properties"]
    assert "preprocessor" in schema["properties"]


def test_secret_absent_from_repr():
    cfg = WebhookEndpointConfig(path="/gh", secret="TOP-SECRET-VALUE")
    assert "TOP-SECRET-VALUE" not in repr(cfg)


def test_endpoint_config_forbids_extra_fields():
    with pytest.raises(ValidationError):
        endpoint(nonexistent_field=1)


# ---------------------------------------------------------------------------
# Listener config
# ---------------------------------------------------------------------------


def test_listener_config_defaults():
    cfg = WebhookHookConfig()
    assert cfg.base_path == "/api/v1/hooks/webhook"
    assert cfg.dispatch_mode == "await"
    assert cfg.expose_list_route is False
    assert cfg.dedup_ttl_seconds == 0


def test_listener_config_normalizes_base_path():
    assert WebhookHookConfig(base_path="hooks/in/").base_path == "/hooks/in"


def test_listener_config_rejects_root_base_path():
    with pytest.raises(ValidationError, match="site root"):
        WebhookHookConfig(base_path="/")


def test_listener_config_rejects_duplicate_endpoint_paths():
    with pytest.raises(ValidationError, match="Duplicate webhook endpoint path"):
        WebhookHookConfig(endpoints=[endpoint(path="/x"), endpoint(path="/x/")])


def test_listener_config_requires_token_when_list_route_enabled():
    with pytest.raises(ValidationError, match="requires list_route_token"):
        WebhookHookConfig(expose_list_route=True)


def test_listener_config_allows_list_route_with_token():
    cfg = WebhookHookConfig(expose_list_route=True, list_route_token="tok")
    assert cfg.expose_list_route is True


def test_listener_config_rejects_unknown_default_scheme():
    with pytest.raises(ValidationError):
        WebhookHookConfig(default_signature_scheme="nope")


def test_listener_config_round_trips_to_json():
    cfg = WebhookHookConfig(endpoints=[endpoint(path="/a"), endpoint(path="/b")])
    restored = WebhookHookConfig(**json.loads(json.dumps(cfg.model_dump(mode="json"))))
    assert [e.path for e in restored.endpoints] == ["/a", "/b"]


def test_listener_token_absent_from_repr():
    cfg = WebhookHookConfig(expose_list_route=True, list_route_token="SUPER-TOKEN")
    assert "SUPER-TOKEN" not in repr(cfg)


# ---------------------------------------------------------------------------
# Endpoint state
# ---------------------------------------------------------------------------


def test_endpoint_state_starts_zeroed_and_snapshots():
    state = WebhookEndpointState()
    snapshot = state.as_dict()
    assert snapshot["call_count"] == 0
    assert snapshot["last_called"] is None
    state.call_count += 1
    assert state.as_dict()["call_count"] == 1


def test_endpoint_state_is_not_part_of_the_config():
    assert "call_count" not in WebhookEndpointConfig.model_fields
