"""Declarative configuration for the pull-queue plane.

Queues are **configuration, not runtime state**. A queue's identity includes
its consumer groups, its reclaim threshold and its retention, so creating one
over HTTP would mean an unbounded stream-creation path guarded only by a
bearer token. Queues are therefore declared in a :class:`QueueRegistry` and
materialized (``XGROUP CREATE ... MKSTREAM``) when the API starts.
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

__all__ = (
    "DEFAULT_QUEUE_PREFIX",
    "MAX_BATCH_ENTRIES",
    "MAX_WAIT_TIME_SECONDS",
    "QueueConfig",
    "QueueGroupConfig",
    "QueueRegistry",
    "assert_prefixes_disjoint",
)

#: Redis key prefix for queue streams. Deliberately distinct from the bus's
#: ``evb:stream:`` — see :func:`assert_prefixes_disjoint`.
DEFAULT_QUEUE_PREFIX = "evb:queue:"

#: SQS caps a batch at 10 entries; matching it keeps client SDKs portable and
#: stops one caller sweeping a queue in a single call.
MAX_BATCH_ENTRIES = 10

#: SQS caps long polling at 20 seconds. Also keeps a request comfortably
#: inside a typical reverse-proxy read timeout (nginx defaults to 60s).
MAX_WAIT_TIME_SECONDS = 20


def assert_prefixes_disjoint(queue_prefix: str, bus_stream_prefix: str) -> None:
    """Reject a queue prefix that overlaps the bus's stream prefix.

    ``RedisStreamsBackend._refresh_streams`` discovers streams with
    ``scan_iter(match=f"{stream_prefix}*")`` and joins a consumer group on
    **anything it finds**. If the bus prefix were a prefix of the queue one
    (say an operator sets ``BUS_STREAM_PREFIX="evb:"``), the bus would
    silently discover every queue stream, consume it, and **auto-ACK** it —
    draining queues that no HTTP consumer ever saw.

    Args:
        queue_prefix: Prefix for queue streams.
        bus_stream_prefix: Prefix the bus backend scans.

    Raises:
        ValueError: Either prefix is a prefix of the other.
    """
    if queue_prefix.startswith(bus_stream_prefix) or bus_stream_prefix.startswith(
        queue_prefix
    ):
        raise ValueError(
            f"Queue prefix {queue_prefix!r} overlaps the bus stream prefix "
            f"{bus_stream_prefix!r}. RedisStreamsBackend SCANs "
            f"'{bus_stream_prefix}*' and joins a consumer group on every match, "
            "so it would consume and auto-ACK queue streams behind the HTTP "
            "consumers' backs. Choose disjoint prefixes."
        )


class QueueGroupConfig(BaseModel):
    """One consumer group on a queue.

    Each group receives **every** message on the queue (fan-out across
    groups); within a group, each message goes to exactly one consumer
    (competing consumers).
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, description="Consumer group name.")
    max_receives: Optional[int] = Field(
        default=None,
        ge=1,
        description="Overrides the queue's max_receives for this group.",
    )
    consumer_tokens: tuple[str, ...] = Field(
        default=(), repr=False, description="Tokens allowed to consume this group."
    )

    @field_validator("name")
    @classmethod
    def _no_whitespace(cls, value: str) -> str:
        if value.strip() != value or not value.strip():
            raise ValueError("group name must not have leading/trailing whitespace")
        return value


class QueueConfig(BaseModel):
    """A single pull queue.

    On the two timeout fields, which are easy to confuse:

    - ``default_visibility_timeout_ms`` is the lease a ``receive`` grants when
      the caller does not ask for one.
    - ``max_visibility_timeout_ms`` (``T``) is the queue-wide reclaim
      threshold. Redis has no per-message visibility timeout — it has one
      idle clock per pending entry and the threshold lives on the reader — so
      a shorter per-receive lease is expressed by *offsetting the message's
      idle clock* against this fixed ``T``. See ``QueueStore.receive``.

    When every receive uses the default, set ``max_visibility_timeout_ms ==
    default_visibility_timeout_ms``: the offsetting ``XCLAIM`` is then skipped
    entirely and the common path costs one round-trip less.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1)
    groups: list[QueueGroupConfig] = Field(..., min_length=1)

    default_visibility_timeout_ms: int = Field(default=30_000, ge=0)
    max_visibility_timeout_ms: int = Field(default=3_600_000, ge=1)
    max_receives: int = Field(
        default=5, ge=1, description="Deliveries before a message is parked to the DLQ."
    )

    maxlen: int = Field(
        default=100_000, ge=1, description="Approximate stream retention (MAXLEN ~)."
    )
    max_body_bytes: int = Field(default=262_144, gt=0)
    strict_ack: bool = Field(
        default=True,
        description="Reject a delete whose lease generation was superseded (409).",
    )
    delete_entries: bool = Field(
        default=False,
        description="Also XDEL on delete. Only legal with exactly one group.",
    )

    mirror_to_bus: bool = Field(
        default=False,
        description="Also emit onto the EventBus. NOT atomic with the XADD.",
    )
    topic_prefix: Optional[str] = Field(
        default=None,
        description=(
            "Namespace forced onto mirrored topics. Defaults to 'queue.<name>'. "
            "Set to None explicitly only when the producer owns a registered "
            "namespace of its own."
        ),
    )

    producer_tokens: tuple[str, ...] = Field(default=(), repr=False)
    consumer_tokens: tuple[str, ...] = Field(default=(), repr=False)
    admin_tokens: tuple[str, ...] = Field(default=(), repr=False)

    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _safe_name(cls, value: str) -> str:
        if value.strip() != value or not value.strip():
            raise ValueError("queue name must not have leading/trailing whitespace")
        if "/" in value or "*" in value:
            raise ValueError(
                f"queue name {value!r} must not contain '/' or '*' — it becomes "
                "part of a Redis key and a URL path segment"
            )
        return value

    @model_validator(mode="after")
    def _coherent(self) -> "QueueConfig":
        seen: set[str] = set()
        for group in self.groups:
            if group.name in seen:
                raise ValueError(f"duplicate consumer group {group.name!r}")
            seen.add(group.name)
        if self.default_visibility_timeout_ms > self.max_visibility_timeout_ms:
            raise ValueError(
                "default_visibility_timeout_ms must not exceed "
                "max_visibility_timeout_ms (the reclaim threshold)"
            )
        if self.delete_entries and len(self.groups) > 1:
            raise ValueError(
                "delete_entries=True is only legal with exactly one group: XDEL "
                "removes the entry from the STREAM, so any group that has not "
                "read it yet would lose it permanently."
            )
        return self

    def group(self, name: str) -> Optional[QueueGroupConfig]:
        """Return the group config named *name*, or ``None``."""
        for group in self.groups:
            if group.name == name:
                return group
        return None

    def receives_cap(self, group_name: str) -> int:
        """Effective ``max_receives`` for *group_name*."""
        group = self.group(group_name)
        if group is not None and group.max_receives is not None:
            return group.max_receives
        return self.max_receives

    def effective_topic_prefix(self) -> Optional[str]:
        """Namespace applied to mirrored topics, or ``None`` to leave them alone."""
        if "topic_prefix" in self.model_fields_set:
            return self.topic_prefix
        return f"queue.{self.name}"


class QueueRegistry(BaseModel):
    """The set of declared queues an API instance serves."""

    model_config = ConfigDict(extra="forbid")

    queues: list[QueueConfig] = Field(default_factory=list)
    queue_prefix: str = Field(default=DEFAULT_QUEUE_PREFIX, min_length=1)

    @model_validator(mode="after")
    def _unique_names(self) -> "QueueRegistry":
        seen: set[str] = set()
        for queue in self.queues:
            if queue.name in seen:
                raise ValueError(f"duplicate queue {queue.name!r}")
            seen.add(queue.name)
        return self

    def get(self, name: str) -> Optional[QueueConfig]:
        """Return the queue named *name*, or ``None`` when not declared."""
        for queue in self.queues:
            if queue.name == name:
                return queue
        return None

    def stream_for(self, name: str) -> str:
        """Redis stream key backing queue *name* — one stream per queue."""
        return f"{self.queue_prefix}{name}"

    @property
    def names(self) -> list[str]:
        """Declared queue names, sorted."""
        return sorted(queue.name for queue in self.queues)
