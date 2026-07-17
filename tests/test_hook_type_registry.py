"""Tests for HookTypeRegistry / HOOK_TYPES / HookEvent open hook_type (FEAT-312, TASK-1803).

Per the amended spec decision #2 (§2, see Revision History): this package
pre-registers, at import time, ALL 18 hook types from the pre-FEAT-312
closed enum — the 10 original generics plus the 8 ai-parrot-specific
integration types exposed on the :class:`HookType` compat shim
(``JIRA_WEBHOOK``, ``GITHUB_WEBHOOK``, ``SHAREPOINT``, ``TELEGRAM``,
``WHATSAPP``, ``MSTEAMS``, ``WHATSAPP_REDIS``, ``MATRIX``) — plus the new
generic ``"webhook"`` type. Full backward compatibility with FEAT-310; a
consuming app is still free to register additional NEW custom hook types
dynamically via ``HOOK_TYPES.register(...)``.
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


def test_app_specific_types_prepopulated():
    """Ai-parrot-specific (legacy) types ARE pre-registered by the package,
    for full FEAT-310 backward compatibility (amended spec decision #2)."""
    for name in (
        "jira_webhook", "github_webhook", "sharepoint", "telegram",
        "whatsapp", "msteams", "whatsapp_redis", "matrix",
    ):
        assert HOOK_TYPES.is_registered(name)


def test_hook_type_compat_constants_match_registered_values():
    """HookType.X constants are plain strings, and all 18 are registered."""
    assert HookType.SCHEDULER == "scheduler"
    assert HookType.FILE_WATCHDOG == "file_watchdog"
    assert HookType.JIRA_WEBHOOK == "jira_webhook"
    assert HookType.MATRIX == "matrix"
    for name, value in vars(HookType).items():
        if name.isupper() and isinstance(value, str):
            assert HOOK_TYPES.is_registered(value)


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


def test_app_specific_type_usable_out_of_the_box():
    """Legacy app-specific types build a HookEvent with zero registration
    wiring — they're pre-registered at import time (amended decision #2)."""
    ev = HookEvent(
        hook_id="h1", hook_type=HookType.JIRA_WEBHOOK,
        event_type="issue_created", payload={},
    )
    assert ev.hook_type == "jira_webhook"


def test_new_custom_app_specific_type_still_requires_registration():
    """A brand-new (not one of the legacy 18) hook type still must be
    registered explicitly — only the legacy 18 + "webhook" ship
    pre-registered; this is not a fully-open free-for-all."""
    with pytest.raises(ValidationError):
        HookEvent(
            hook_id="h1", hook_type="brand_new_never_registered_type",
            event_type="ping", payload={},
        )
