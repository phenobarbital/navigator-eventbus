"""Core event envelope contract for navigator-eventbus (FEAT-312, Module 2).

Mudado sin cambios de comportamiento desde
``packages/ai-parrot/src/parrot/core/events/bus/envelope.py``
(ai-parrot@686aba1fe, FEAT-310). Defines the :class:`Severity` enum
(log-level semantics, orthogonal to ``EventPriority`` scheduling) and the
frozen :class:`EventEnvelope` dataclass — the single closed contract for
"an event on the bus".

The envelope is a frozen, slotted dataclass (NOT Pydantic) following the
FEAT-176 rationale: ~5x faster instantiation than Pydantic on hot paths.
Pydantic validation happens ONLY at ingress boundaries (see
``ingress_models.py``).
"""
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum
from typing import Any, Optional

from navigator_eventbus.evb import EventPriority

ENVELOPE_SCHEMA_VERSION: int = 1
"""Current wire schema version for :class:`EventEnvelope`.

Bump this constant (and add explicit migration handling in
:meth:`EventEnvelope.from_dict`) whenever the envelope's wire shape changes
in a way that requires reader awareness.
"""


class UnsupportedSchemaVersion(ValueError):
    """Raised by :meth:`EventEnvelope.from_dict` for an unknown schema version.

    Deserialization is lenient backwards (a missing ``schema_version`` key
    is treated as legacy version ``1``) and strict forwards (a version
    greater than :data:`ENVELOPE_SCHEMA_VERSION` raises this exception,
    never silently downgrades).
    """


class Severity(IntEnum):
    """Log-level severity of an event — orthogonal to ``EventPriority``.

    ``EventPriority`` controls dispatch *scheduling* (which queue an
    envelope is drained from); ``Severity`` controls *filtering and
    alerting* (e.g. ``min_severity`` subscriptions, notification rules).
    Values mirror the stdlib ``logging`` levels.
    """

    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """Immutable, single wire-format envelope for every bus event.

    Direct construction REJECTS naive datetimes (``ValueError``) —
    converters from legacy shapes coerce naive timestamps to UTC instead
    (see ``converters.py``).

    Attributes:
        topic: Hierarchical topic string, e.g. ``"order.created"``,
            ``"hooks.webhook.jira"``, ``"bus.dlq"``.
        payload: JSON-safe event payload.
        event_id: UUID4 string; dedup key in at-least-once mode.
        timestamp: Timezone-aware creation time (naive → ``ValueError``).
        source: Optional emitter identifier.
        severity: Filtering/alerting severity (default ``INFO``).
        priority: Dispatch scheduling priority (existing enum, reused).
        correlation_id: Optional chain-tracking identifier.
        trace_context: Optional dict form of a lifecycle ``TraceContext``.
        metadata: Free-form JSON-safe metadata.
        schema_version: Wire schema version (default
            :data:`ENVELOPE_SCHEMA_VERSION`). MUST stay the last field for
            positional-argument compatibility.
    """

    topic: str
    payload: dict[str, Any]
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    source: Optional[str] = None
    severity: Severity = Severity.INFO
    priority: EventPriority = EventPriority.NORMAL
    correlation_id: Optional[str] = None
    trace_context: Optional[dict] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: int = ENVELOPE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        """Validate the timestamp is present and timezone-aware.

        Raises:
            ValueError: If ``timestamp`` is missing, not a ``datetime``,
                or naive (``tzinfo`` absent/unusable).
        """
        ts = self.timestamp
        if not isinstance(ts, datetime):
            raise ValueError(
                "EventEnvelope.timestamp must be a tz-aware datetime; "
                f"got {ts!r}"
            )
        if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
            raise ValueError(
                "EventEnvelope.timestamp must be tz-aware; naive datetimes "
                "are rejected on direct construction (converters coerce "
                "legacy naive timestamps to UTC)."
            )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-safe dict for transport backends.

        Enums are emitted as their integer ``.value``; the timestamp as an
        ISO 8601 string.

        Returns:
            A dict where every value is JSON-serializable.
        """
        return {
            "topic": self.topic,
            "payload": self.payload,
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "severity": self.severity.value,
            "priority": self.priority.value,
            "correlation_id": self.correlation_id,
            "trace_context": self.trace_context,
            "metadata": self.metadata,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EventEnvelope":
        """Deserialize an envelope produced by :meth:`to_dict`.

        Version tolerance is lenient backwards and strict forwards: a
        missing ``schema_version`` key is treated as legacy version ``1``;
        a version greater than :data:`ENVELOPE_SCHEMA_VERSION` raises
        :class:`UnsupportedSchemaVersion` (never silently downgraded).

        Args:
            data: Dict with at least ``topic``, ``payload`` and an
                ISO 8601 ``timestamp``.

        Returns:
            The reconstructed :class:`EventEnvelope`.

        Raises:
            ValueError: If the parsed timestamp is naive.
            KeyError: If required keys are missing.
            UnsupportedSchemaVersion: If ``schema_version`` is greater than
                :data:`ENVELOPE_SCHEMA_VERSION`.
        """
        schema_version = data.get("schema_version", 1)
        if schema_version > ENVELOPE_SCHEMA_VERSION:
            raise UnsupportedSchemaVersion(
                f"Unsupported envelope schema_version {schema_version} "
                f"(supported <= {ENVELOPE_SCHEMA_VERSION}) for "
                f"topic={data.get('topic')!r} "
                f"event_id={data.get('event_id')!r}"
            )
        return cls(
            topic=data["topic"],
            payload=data.get("payload", {}),
            event_id=data.get("event_id", str(uuid.uuid4())),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            source=data.get("source"),
            severity=Severity(data.get("severity", Severity.INFO.value)),
            priority=EventPriority(
                data.get("priority", EventPriority.NORMAL.value)
            ),
            correlation_id=data.get("correlation_id"),
            trace_context=data.get("trace_context"),
            metadata=data.get("metadata", {}),
            schema_version=schema_version,
        )
