"""Pydantic models and configuration for the hooks system (FEAT-312, Module 6).

Mudado desde ``packages/ai-parrot/src/parrot/core/hooks/models.py``
(ai-parrot@686aba1fe, FEAT-310) — modelos genéricos Y configs de
integraciones de terceros (Jira/GitHub/SharePoint/WhatsApp/Matrix) viajan
juntos al paquete (son modelos de datos, no lógica de integración).

**Cambio de contrato (FEAT-312, único de esta fase)**: ``HookType`` deja de
ser un ``Enum`` cerrado y pasa a un tipo ABIERTO — un ``str`` validado
contra un registro dinámico (:class:`HookTypeRegistry`). Per the spec §2
decision #2 amendment (post-implementation revision, see Revision History):
the package pre-registers, at import time, ALL 18 hook types from the
pre-FEAT-312 closed enum (10 generics + the 8 ai-parrot-specific
integration types: jira_webhook, github_webhook, sharepoint, telegram,
whatsapp, msteams, whatsapp_redis, matrix) PLUS the new generic
``"webhook"`` type introduced by this package — full backward
compatibility, zero registration wiring required to reproduce FEAT-310
behavior. Any NEW consuming app can still register additional custom hook
types dynamically via ``HOOK_TYPES.register(...)`` at its own import time.
"""
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class HookTypeRegistry:
    """Dynamic registry of hook types (replaces the closed ``HookType`` enum).

    The package pre-registers, at import time, all 18 hook types from the
    pre-FEAT-312 closed enum plus the new generic ``"webhook"`` type (full
    backward compatibility — see module docstring). Any consuming
    application can still register additional, new custom hook types at
    ITS own import time — no static enum to extend, no coupling back to
    this package's source.
    """

    def __init__(self) -> None:
        self._types: set[str] = set()

    def register(self, name: str) -> str:
        """Register (or re-register, idempotently) a hook type.

        Args:
            name: Hook type slug, e.g. ``"scheduler"``, ``"jira_webhook"``.
                Must be a non-empty string.

        Returns:
            The registered name (for chaining, e.g.
            ``HOOK_TYPES.register("jira_webhook")``).

        Raises:
            ValueError: If ``name`` is empty or not a string.
        """
        if not isinstance(name, str) or not name:
            raise ValueError(
                f"HookTypeRegistry.register() requires a non-empty str; got {name!r}"
            )
        self._types.add(name)
        return name

    def unregister(self, name: str) -> None:
        """Remove *name* from the registry (mainly useful for test cleanup)."""
        self._types.discard(name)

    def is_registered(self, name: str) -> bool:
        """Return whether *name* is a registered hook type."""
        return name in self._types

    def all(self) -> frozenset[str]:
        """Return a snapshot of every registered hook type."""
        return frozenset(self._types)


#: Module-level singleton — pre-populated below with the generic hook types
#: this package ships. Consuming applications import this object and call
#: ``HOOK_TYPES.register("my_hook_type")`` at their own import time.
HOOK_TYPES = HookTypeRegistry()

#: Generic hook types shipped by navigator-eventbus itself.
_GENERIC_HOOK_TYPES = (
    "scheduler",
    "file_watchdog",
    "postgres_listen",
    "imap_watchdog",
    "file_upload",
    "broker_redis",
    "broker_rabbitmq",
    "broker_mqtt",
    "broker_sqs",
    "filesystem",
    "webhook",
)

#: The eight ai-parrot-specific integration hook types from the pre-FEAT-312
#: closed ``HookType(str, Enum)``. Pre-registered for full backward
#: compatibility per the amended spec §2 decision #2 (see Revision History
#: in ``eventbus-core-extraction.spec.md``) — a fresh consuming app is free
#: to register its OWN new custom hook types the same way, this list only
#: covers the legacy 8 that existed before the extraction.
_LEGACY_APP_SPECIFIC_HOOK_TYPES = (
    "jira_webhook",
    "github_webhook",
    "sharepoint",
    "telegram",
    "whatsapp",
    "msteams",
    "whatsapp_redis",
    "matrix",
)

for _name in (*_GENERIC_HOOK_TYPES, *_LEGACY_APP_SPECIFIC_HOOK_TYPES):
    HOOK_TYPES.register(_name)


class HookType:
    """Backward-compat string constants for the 18 pre-FEAT-312 hook types.

    Plain string constants (``HookType.SCHEDULER == "scheduler"``) provided
    so attribute-style access (e.g.
    ``BaseHook.hook_type: str = HookType.SCHEDULER``, or existing call
    sites doing ``HookType.X``) keeps working with a minimal diff after the
    closed ``HookType(str, Enum)`` was replaced by the open
    :class:`HookTypeRegistry`.

    All 18 names below (10 original generics + the 8 ai-parrot-specific
    integration types) are pre-registered in :data:`HOOK_TYPES` at import
    time, per the amended spec §2 decision #2 — full backward compatibility
    with FEAT-310, no per-app registration wiring required to reproduce it.
    """

    SCHEDULER = "scheduler"
    FILE_WATCHDOG = "file_watchdog"
    POSTGRES_LISTEN = "postgres_listen"
    IMAP_WATCHDOG = "imap_watchdog"
    FILE_UPLOAD = "file_upload"
    BROKER_REDIS = "broker_redis"
    BROKER_RABBITMQ = "broker_rabbitmq"
    BROKER_MQTT = "broker_mqtt"
    BROKER_SQS = "broker_sqs"
    FILESYSTEM = "filesystem"
    # Ai-parrot-specific — pre-registered in HOOK_TYPES (see docstring).
    JIRA_WEBHOOK = "jira_webhook"
    GITHUB_WEBHOOK = "github_webhook"
    SHAREPOINT = "sharepoint"
    TELEGRAM = "telegram"
    WHATSAPP = "whatsapp"
    MSTEAMS = "msteams"
    WHATSAPP_REDIS = "whatsapp_redis"
    MATRIX = "matrix"


class HookEvent(BaseModel):
    """Unified event emitted by any hook into the consuming application."""
    hook_id: str
    hook_type: str
    event_type: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.now)

    # Optional routing hints for the consumer.
    target_type: Optional[str] = None   # "agent" or "crew"
    target_id: Optional[str] = None     # agent/crew name
    task: Optional[str] = None          # prompt override

    @field_validator("hook_type")
    @classmethod
    def _validate_hook_type(cls, value: str) -> str:
        """Reject hook types that have not been registered against HOOK_TYPES."""
        if not HOOK_TYPES.is_registered(value):
            raise ValueError(
                f"Unregistered hook_type {value!r} — register it first via "
                "HOOK_TYPES.register(...) (navigator_eventbus.hooks.models)."
            )
        return value


# ---------------------------------------------------------------------------
# Per-hook configuration models
# ---------------------------------------------------------------------------

class SchedulerHookConfig(BaseModel):
    """Configuration for the APScheduler-based hook."""
    name: str = "scheduler"
    enabled: bool = True
    cron_expression: Optional[str] = None
    interval_seconds: Optional[int] = None
    prompt_template: str = "Perform your scheduled check."
    target_type: str = "agent"
    target_id: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class FileWatchdogHookConfig(BaseModel):
    """Configuration for file-system watchdog hook."""
    name: str = "file_watchdog"
    enabled: bool = True
    directory: str
    patterns: List[str] = Field(default_factory=lambda: ["*"])
    events: List[str] = Field(
        default_factory=lambda: ["created", "modified", "deleted", "moved"]
    )
    recursive: bool = True
    not_empty: bool = False
    target_type: str = "agent"
    target_id: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PostgresHookConfig(BaseModel):
    """Configuration for PostgreSQL LISTEN/NOTIFY hook."""
    name: str = "postgres_listen"
    enabled: bool = True
    dsn: Optional[str] = None
    channel: str = "notifications"
    target_type: str = "agent"
    target_id: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class IMAPHookConfig(BaseModel):
    """Configuration for IMAP mailbox monitoring hook."""
    name: str = "imap_watchdog"
    enabled: bool = True
    host: str
    port: int = 993
    user: str
    password: str
    mailbox: str = "INBOX"
    use_ssl: bool = True
    interval: int = 60
    authmech: Optional[str] = None
    search: Dict[str, Optional[str]] = Field(default_factory=lambda: {"UNSEEN": None})  # type: ignore[arg-type]
    # Optional tagged email filtering
    tag: Optional[str] = None
    alias_address: Optional[str] = None
    target_type: str = "agent"
    target_id: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class TransitionActionType(str, Enum):
    """Supported action types for Jira transition handlers."""

    NOTIFY_CHANNEL = "notify_channel"
    TRIGGER_AGENT = "trigger_agent"
    CALL_HANDLER = "call_handler"
    LOG = "log"


class TransitionAction(BaseModel):
    """A single transition-to-action mapping.

    Matches when the ticket's from_status and to_status match the
    configured patterns. Use ``"*"`` as a wildcard for either field.
    """

    from_status: str = Field(
        default="*",
        description="Source status to match (case-insensitive), or '*' for any",
    )
    to_status: str = Field(
        ...,
        description="Target status to match (case-insensitive), or '*' for any",
    )
    action_type: TransitionActionType
    action_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Action-specific configuration",
    )
    project_key: Optional[str] = Field(
        default=None,
        description="Restrict to a specific Jira project (None = all projects)",
    )
    enabled: bool = True

    @model_validator(mode="after")
    def validate_not_both_wildcards(self) -> "TransitionAction":
        """Reject configurations where both statuses are wildcards."""
        if self.from_status == "*" and self.to_status == "*":
            raise ValueError(
                "At least one of from_status or to_status must be non-wildcard"
            )
        return self


class JiraWebhookConfig(BaseModel):
    """Configuration for Jira webhook receiver."""

    name: str = "jira_webhook"
    enabled: bool = True
    url: str = "/api/v1/hooks/jira"
    secret_token: Optional[str] = None
    target_type: str = "agent"
    target_id: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)
    transition_actions: List[TransitionAction] = Field(
        default_factory=list,
        description="Registry of transition-to-action mappings for jira.transitioned events",
    )


class GitHubWebhookConfig(BaseModel):
    """Configuration for GitHub webhook receiver.

    Used by a ``GitHubWebhookHook`` implementation (consuming application)
    to register an aiohttp route that accepts ``pull_request`` deliveries
    from GitHub. ``secret_token`` enables HMAC-SHA256 signature
    verification on the ``X-Hub-Signature-256`` header.
    """
    name: str = "github_webhook"
    enabled: bool = True
    url: str = "/api/v1/hooks/github"
    secret_token: Optional[str] = None
    target_type: str = "agent"
    target_id: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class FileUploadHookConfig(BaseModel):
    """Configuration for HTTP file upload hook."""
    name: str = "file_upload"
    enabled: bool = True
    url: str = "/api/v1/hooks/upload"
    methods: List[str] = Field(default_factory=lambda: ["POST", "PUT"])
    allowed_mime_types: Optional[List[str]] = None
    allowed_file_names: Optional[List[str]] = None
    upload_dir: Optional[str] = None
    target_type: str = "agent"
    target_id: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BrokerHookConfig(BaseModel):
    """Configuration for message broker hooks (Redis, RabbitMQ, MQTT, SQS)."""
    name: str = "broker"
    enabled: bool = True
    broker_type: str = "redis"  # redis, rabbitmq, mqtt, sqs

    # Redis Streams
    stream_name: Optional[str] = None
    group_name: str = "default_group"
    consumer_name: str = "default_consumer"

    # RabbitMQ
    queue_name: Optional[str] = None
    routing_key: str = ""
    exchange_name: str = ""
    exchange_type: str = "topic"
    prefetch_count: int = 1

    # MQTT
    broker_url: Optional[str] = None
    topics: List[str] = Field(default_factory=list)

    # SQS
    max_messages: int = 10
    wait_time: int = 10
    idle_sleep: int = 5

    # Connection credentials
    credentials: Optional[Dict[str, Any]] = None

    target_type: str = "agent"
    target_id: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SharePointHookConfig(BaseModel):
    """Configuration for SharePoint webhook hook."""
    name: str = "sharepoint"
    enabled: bool = True
    url: str = "/api/v1/hooks/sharepoint"
    webhook_url: str = ""  # Public URL Microsoft Graph will POST to
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    tenant_name: Optional[str] = None
    site_name: Optional[str] = None
    host: Optional[str] = None
    folder_path: Optional[str] = None
    resource: Optional[str] = None
    client_state: str = "parrot_state"
    changetype: str = "updated"
    renewal_interval: int = 86400  # 24 hours
    target_type: str = "agent"
    target_id: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class MessagingHookConfig(BaseModel):
    """Configuration for messaging platform hooks (Telegram, WhatsApp, MS Teams)."""
    name: str
    enabled: bool = True
    platform: str  # "telegram", "whatsapp", "msteams"
    url: str = "/api/v1/hooks/messaging"  # webhook endpoint

    # Keyword / command filters (only trigger on matching messages)
    trigger_keywords: Optional[List[str]] = None
    trigger_commands: Optional[List[str]] = None
    trigger_pattern: Optional[str] = None  # regex pattern

    target_type: str = "agent"
    target_id: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WhatsAppRedisHookConfig(BaseModel):
    """Configuration for WhatsApp Redis Bridge hook."""

    # Basic hook config
    name: str = "whatsapp_hook"
    enabled: bool = True
    target_type: Optional[str] = "agent"
    target_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Redis connection
    redis_url: str = "redis://localhost:6379"
    channel: str = "whatsapp:messages"

    # WhatsApp Bridge
    bridge_url: str = "http://localhost:8765"
    auto_reply: bool = True

    # Message filtering
    command_prefix: str = ""
    allowed_phones: Optional[List[str]] = None
    allowed_groups: Optional[List[str]] = None

    # Advanced routing
    routes: Optional[List[Dict[str, Any]]] = None

    @model_validator(mode="after")
    def _normalize(self) -> "WhatsAppRedisHookConfig":
        """Normalize phone numbers and route keywords."""
        if self.allowed_phones:
            self.allowed_phones = [p.strip() for p in self.allowed_phones]
        if self.routes:
            for route in self.routes:
                if "phones" in route:
                    route["phones"] = [p.strip() for p in route["phones"]]
                if "keywords" in route:
                    route["keywords"] = [k.strip().lower() for k in route["keywords"]]
        return self


class MatrixHookConfig(BaseModel):
    """Configuration for Matrix protocol hook."""

    # Basic hook config
    name: str = "matrix_hook"
    enabled: bool = True
    target_type: Optional[str] = "agent"
    target_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Matrix connection
    homeserver: str = "http://localhost:8008"
    bot_mxid: str = ""
    access_token: str = ""
    device_id: str = "PARROT"

    # Message filtering
    command_prefix: str = "!ask"
    allowed_users: Optional[List[str]] = None  # MXIDs (e.g. @user:server)

    # Room routing: room_id → agent/crew name
    room_routing: Optional[Dict[str, str]] = None

    # Auto-reply
    auto_reply: bool = True

    @model_validator(mode="after")
    def _normalize(self) -> "MatrixHookConfig":
        """Normalize MXIDs."""
        if self.allowed_users:
            self.allowed_users = [u.strip() for u in self.allowed_users]
        return self


class FilesystemHookConfig(BaseModel):
    """Configuration for FilesystemTransport hook."""

    # Basic hook config
    name: str = "filesystem_hook"
    enabled: bool = True
    target_type: Optional[str] = "agent"
    target_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Transport config (accepts dict or FilesystemTransportConfig)
    transport: Dict[str, Any] = Field(default_factory=dict)

    # Message filtering
    command_prefix: str = ""
    allowed_agents: Optional[List[str]] = None


# ---------------------------------------------------------------------------
# WhatsApp Redis Hook — factory helpers
# ---------------------------------------------------------------------------


def create_simple_whatsapp_hook(
    agent_name: str,
    allowed_phones: Optional[List[str]] = None,
    command_prefix: str = "",
) -> WhatsAppRedisHookConfig:
    """Create a simple WhatsApp hook that routes all messages to one agent."""
    return WhatsAppRedisHookConfig(
        name=f"whatsapp_{agent_name}",
        target_type="agent",
        target_id=agent_name,
        allowed_phones=allowed_phones,
        command_prefix=command_prefix,
        auto_reply=True,
    )


def create_multi_agent_whatsapp_hook(
    default_agent: str,
    routes: List[Dict[str, Any]],
    command_prefix: str = "",
) -> WhatsAppRedisHookConfig:
    """Create a multi-agent WhatsApp hook with keyword/phone routing."""
    return WhatsAppRedisHookConfig(
        name="whatsapp_router",
        target_type="agent",
        target_id=default_agent,
        routes=routes,
        command_prefix=command_prefix,
        auto_reply=True,
    )


def create_crew_whatsapp_hook(
    crew_id: str,
    allowed_phones: Optional[List[str]] = None,
    command_prefix: str = "!",
) -> WhatsAppRedisHookConfig:
    """Create a WhatsApp hook that routes messages to an AgentCrew."""
    return WhatsAppRedisHookConfig(
        name=f"whatsapp_crew_{crew_id}",
        target_type="crew",
        target_id=crew_id,
        allowed_phones=allowed_phones,
        command_prefix=command_prefix,
        auto_reply=True,
    )
