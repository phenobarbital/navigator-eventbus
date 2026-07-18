"""Generic hooks fabric for navigator_eventbus (FEAT-312, Module 6).

Mudado desde ``packages/ai-parrot/src/parrot/core/hooks/__init__.py``
(ai-parrot@686aba1fe, FEAT-310) — SOLO los hooks genéricos y sus configs
viajan al paquete; los hooks de integración parrot (jira, github,
sharepoint, telegram/whatsapp/msteams messaging, whatsapp_redis, matrix,
postgres, imap, file_upload) NO se mudan (quedan en ai-parrot, fase 4 los
recablea). Sus config models SÍ viajan (spec decisión #3: modelos de
datos, no lógica de integración) y se re-exportan aquí igual que en el
origen.

All concrete hook imports are lazy to avoid pulling in heavy transitive
dependencies (watchdog, apscheduler, gmqtt, etc.) at package import time.
"""
import importlib

from navigator_eventbus.hooks.base import BaseHook, HookRegistry, MessagingHook
from navigator_eventbus.hooks.manager import HookManager
from navigator_eventbus.hooks.mixins import HookableAgent
from navigator_eventbus.hooks.models import (
    HOOK_TYPES,
    BrokerHookConfig,
    FilesystemHookConfig,
    FileUploadHookConfig,
    FileWatchdogHookConfig,
    GitHubWebhookConfig,
    HookEvent,
    HookType,
    HookTypeRegistry,
    IMAPHookConfig,
    JiraWebhookConfig,
    MatrixHookConfig,
    MessagingHookConfig,
    PostgresHookConfig,
    SchedulerHookConfig,
    SharePointHookConfig,
    TransitionAction,
    TransitionActionType,
    WhatsAppRedisHookConfig,
    create_crew_whatsapp_hook,
    create_multi_agent_whatsapp_hook,
    create_simple_whatsapp_hook,
)


def __getattr__(name: str):
    """Lazy-import concrete hook classes on first access."""
    _lazy_map = {
        # Generic hooks (migrated)
        "SchedulerHook": ".scheduler",
        "FileWatchdogHook": ".file_watchdog",
        # Broker hooks (migrated; Redis/RabbitMQ/SQS rewired to the internal
        # brokers port by FEAT-316 TASK-1818; gmqtt lazy-import intact)
        "BaseBrokerHook": ".brokers.base",
        "RedisBrokerHook": ".brokers.redis",
        "RabbitMQBrokerHook": ".brokers.rabbitmq",
        "MQTTBrokerHook": ".brokers.mqtt",
        "SQSBrokerHook": ".brokers.sqs",
    }
    if name in _lazy_map:
        module = importlib.import_module(_lazy_map[name], package=__name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # Core
    "BaseHook",
    "HookRegistry",
    "MessagingHook",
    "HookManager",
    "HookableAgent",
    "HookEvent",
    "HookType",
    "HookTypeRegistry",
    "HOOK_TYPES",
    # Hooks (lazy, migrated)
    "SchedulerHook",
    "FileWatchdogHook",
    # Brokers (lazy, migrated)
    "BaseBrokerHook",
    "RedisBrokerHook",
    "RabbitMQBrokerHook",
    "MQTTBrokerHook",
    "SQSBrokerHook",
    # Configs (eagerly imported — lightweight Pydantic models; includes
    # ai-parrot integration configs per spec decision #3, data-only).
    "SchedulerHookConfig",
    "FileWatchdogHookConfig",
    "PostgresHookConfig",
    "IMAPHookConfig",
    "JiraWebhookConfig",
    "GitHubWebhookConfig",
    "FileUploadHookConfig",
    "BrokerHookConfig",
    "SharePointHookConfig",
    "MessagingHookConfig",
    "WhatsAppRedisHookConfig",
    "MatrixHookConfig",
    "FilesystemHookConfig",
    # Transition action models
    "TransitionAction",
    "TransitionActionType",
    # Factory helpers
    "create_simple_whatsapp_hook",
    "create_multi_agent_whatsapp_hook",
    "create_crew_whatsapp_hook",
]
