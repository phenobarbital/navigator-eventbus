"""Import and lazy-loading tests for navigator_eventbus.hooks (FEAT-312, TASK-1803).

Mudado desde ``packages/ai-parrot/tests/core/hooks/test_imports.py``
(ai-parrot@686aba1fe, FEAT-310) — adapted to the hooks actually migrated
(scheduler, file_watchdog, brokers). Integration-specific hooks (jira,
github, sharepoint, messaging, whatsapp_redis, matrix, postgres, imap,
file_upload) are NOT migrated (spec Non-Goal) — their config MODELS
still travel (data-only, spec decision #3) but the classes themselves
are not asserted as lazy-importable here (they don't exist in this
package).
"""
import sys


class TestHooksImport:
    def test_hooks_import(self):
        from navigator_eventbus.hooks import BaseHook, HookEvent, HookManager, HookType  # noqa: F401

        assert BaseHook is not None
        assert HookManager is not None
        assert HookEvent is not None
        assert HookType is not None

    def test_hookable_agent_import(self):
        from navigator_eventbus.hooks import HookableAgent  # noqa: F401

        assert HookableAgent is not None

    def test_config_imports(self):
        from navigator_eventbus.hooks import (  # noqa: F401
            BrokerHookConfig,
            FileUploadHookConfig,
            FileWatchdogHookConfig,
            IMAPHookConfig,
            JiraWebhookConfig,
            MatrixHookConfig,
            MessagingHookConfig,
            PostgresHookConfig,
            SchedulerHookConfig,
            SharePointHookConfig,
            WhatsAppRedisHookConfig,
        )

    def test_factory_helpers_import(self):
        from navigator_eventbus.hooks import (  # noqa: F401
            create_crew_whatsapp_hook,
            create_multi_agent_whatsapp_hook,
            create_simple_whatsapp_hook,
        )

    def test_lazy_hook_import_scheduler(self):
        from navigator_eventbus.hooks import SchedulerHook  # noqa: F401

        assert SchedulerHook is not None

    def test_lazy_hook_import_file_watchdog(self):
        from navigator_eventbus.hooks import FileWatchdogHook  # noqa: F401

        assert FileWatchdogHook is not None

    def test_lazy_hook_import_brokers(self):
        from navigator_eventbus.hooks import (  # noqa: F401
            BaseBrokerHook,
            MQTTBrokerHook,
            RabbitMQBrokerHook,
            RedisBrokerHook,
            SQSBrokerHook,
        )

    def test_lazy_loading_no_watchdog(self):
        """Package-level import of navigator_eventbus.hooks must NOT pull in watchdog."""
        before = set(sys.modules.keys())
        import navigator_eventbus.hooks  # noqa: F401
        new = set(sys.modules.keys()) - before
        assert not any(m.startswith("watchdog") for m in new), (
            f"watchdog was newly imported by navigator_eventbus.hooks: {new}"
        )

    def test_lazy_loading_no_apscheduler(self):
        """Package-level import of navigator_eventbus.hooks must NOT pull in apscheduler."""
        before = set(sys.modules.keys())
        import navigator_eventbus.hooks  # noqa: F401
        new = set(sys.modules.keys()) - before
        assert not any(m.startswith("apscheduler") for m in new), (
            f"apscheduler was newly imported by navigator_eventbus.hooks: {new}"
        )

    def test_lazy_loading_no_gmqtt(self):
        """Package-level import of navigator_eventbus.hooks must NOT pull in gmqtt."""
        before = set(sys.modules.keys())
        import navigator_eventbus.hooks  # noqa: F401
        new = set(sys.modules.keys()) - before
        assert not any(m.startswith("gmqtt") for m in new), (
            f"gmqtt was newly imported by navigator_eventbus.hooks: {new}"
        )

    def test_all_exports_contains_core_symbols(self):
        import navigator_eventbus.hooks as hooks_pkg

        expected = {
            "BaseHook",
            "HookManager",
            "HookableAgent",
            "HookEvent",
            "HookType",
            "HookTypeRegistry",
            "HOOK_TYPES",
            "SchedulerHook",
            "FileWatchdogHook",
        }
        assert expected.issubset(set(hooks_pkg.__all__))
