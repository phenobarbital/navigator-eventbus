"""Tests for HookTypeRegistry / HOOK_TYPES / HookEvent open hook_type (FEAT-312, TASK-1803).

Per the spec's closed decision #2 (§2, "no re-abrir"): this package
pre-registers ONLY the generic hook types in :data:`HOOK_TYPES`; the
ai-parrot-specific constants exposed on the :class:`HookType` compat
shim (``JIRA_WEBHOOK``, ``GITHUB_WEBHOOK``, ``SHAREPOINT``, ``TELEGRAM``,
``WHATSAPP``, ``MSTEAMS``, ``WHATSAPP_REDIS``, ``MATRIX``) are NOT
pre-registered — a consuming app must register them at its own import
time before constructing a matching ``HookEvent``.
"""
import pytest
from pydantic import ValidationError

from navigator_eventbus.hooks.models import HOOK_TYPES, HookEvent, HookType


@pytest.fixture
def custom_hook_type():
    """Register a test hook type and clean it up afterwards."""
    name = HOOK_TYPES.register("test_custom_hook")
    yield name
    HOOK_TYPES.unregister(name)


def test_generics_prepopulated():
    assert HOOK_TYPES.is_registered("scheduler")
    assert HOOK_TYPES.is_registered("broker_redis")
    assert HOOK_TYPES.is_registered("file_watchdog")
    assert HOOK_TYPES.is_registered("postgres_listen")
    assert HOOK_TYPES.is_registered("imap_watchdog")
    assert HOOK_TYPES.is_registered("file_upload")
    assert HOOK_TYPES.is_registered("broker_rabbitmq")
    assert HOOK_TYPES.is_registered("broker_mqtt")
    assert HOOK_TYPES.is_registered("broker_sqs")
    assert HOOK_TYPES.is_registered("filesystem")
    assert HOOK_TYPES.is_registered("webhook")


def test_app_specific_types_not_prepopulated():
    """Ai-parrot-specific types are NOT pre-registered by the package."""
    for name in (
        "jira_webhook", "github_webhook", "sharepoint", "telegram",
        "whatsapp", "msteams", "whatsapp_redis", "matrix",
    ):
        assert not HOOK_TYPES.is_registered(name)


def test_hook_type_compat_constants_match_generic_registered_values():
    """HookType.X constants are plain strings — readable, unaffected by
    registry membership (BaseHook's default uses HookType.SCHEDULER)."""
    assert HookType.SCHEDULER == "scheduler"
    assert HookType.FILE_WATCHDOG == "file_watchdog"
    assert HookType.JIRA_WEBHOOK == "jira_webhook"  # not registered, but readable
    assert HookType.MATRIX == "matrix"


def test_register_custom(custom_hook_type):
    ev = HookEvent(
        hook_id="h1", hook_type=custom_hook_type,
        event_type="ping", payload={}, metadata={},
    )
    assert ev.hook_type == custom_hook_type


def test_register_is_idempotent():
    HOOK_TYPES.register("idempotent_hook")
    HOOK_TYPES.register("idempotent_hook")
    assert HOOK_TYPES.is_registered("idempotent_hook")
    HOOK_TYPES.unregister("idempotent_hook")


def test_register_rejects_non_str():
    with pytest.raises(ValueError):
        HOOK_TYPES.register(123)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        HOOK_TYPES.register("")


def test_all_returns_frozenset_snapshot():
    snapshot = HOOK_TYPES.all()
    assert isinstance(snapshot, frozenset)
    assert "scheduler" in snapshot


def test_rejects_unregistered():
    with pytest.raises(ValidationError):
        HookEvent(
            hook_id="h1", hook_type="nope_never_registered",
            event_type="ping", payload={}, metadata={},
        )


def test_registering_app_specific_type_enables_it(custom_hook_type):
    """Simulates ai-parrot's phase-4 registration of its own hook types."""
    HOOK_TYPES.register(HookType.JIRA_WEBHOOK)
    try:
        ev = HookEvent(
            hook_id="h1", hook_type=HookType.JIRA_WEBHOOK,
            event_type="issue_created", payload={},
        )
        assert ev.hook_type == "jira_webhook"
    finally:
        HOOK_TYPES.unregister(HookType.JIRA_WEBHOOK)
