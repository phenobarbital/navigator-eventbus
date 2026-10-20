"""HTTP wire models for the pull-queue API.

Every request model is ``extra="forbid"``, matching
:class:`~navigator_eventbus.ingress_models.IngressEnvelope` — a producer's
typo becomes a 400 rather than a silently ignored field.
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from navigator_eventbus.ingress_models import IngressEnvelope
from navigator_eventbus.queues.config import MAX_BATCH_ENTRIES, MAX_WAIT_TIME_SECONDS

__all__ = (
    "BatchFailure",
    "BatchSendEntry",
    "BatchSuccess",
    "ChangeVisibilityEntry",
    "ChangeVisibilityRequest",
    "DeleteEntry",
    "DeleteRequest",
    "MessageAttributes",
    "QueueAttributes",
    "ReceiveRequest",
    "ReceiveResponse",
    "ReceivedMessage",
    "SendBatchRequest",
    "SendResponse",
)


class ReceiveRequest(BaseModel):
    """Body of a ``receive`` call."""

    model_config = ConfigDict(extra="forbid")

    group: str = Field(..., min_length=1, description="Consumer group to read as.")
    max_messages: int = Field(default=1, ge=1, le=MAX_BATCH_ENTRIES)
    wait_time_seconds: int = Field(
        default=0,
        ge=0,
        le=MAX_WAIT_TIME_SECONDS,
        description="Long-poll duration. Clamped by the server, never rejected.",
    )
    visibility_timeout_seconds: Optional[int] = Field(
        default=None,
        ge=0,
        description="Lease for the returned messages. Defaults to the queue's.",
    )


class MessageAttributes(BaseModel):
    """Per-message delivery metadata, mirroring SQS's attribute names."""

    model_config = ConfigDict(extra="forbid")

    approximate_receive_count: int = Field(
        ..., description="Deliveries so far, from the pending-entries list."
    )
    sent_at: Optional[str] = Field(
        default=None, description="ISO-8601 timestamp carried on the envelope."
    )
    visibility_expires_at: Optional[str] = Field(
        default=None, description="ISO-8601 instant when this lease ends."
    )


class ReceivedMessage(BaseModel):
    """One message handed to a consumer."""

    model_config = ConfigDict(extra="forbid")

    message_id: str
    receipt_handle: str
    body: dict[str, Any] = Field(
        ..., description="The envelope as sent, in IngressEnvelope shape."
    )
    attributes: MessageAttributes


class ReceiveResponse(BaseModel):
    """Result of a ``receive``. An empty list is a 200, never a 204."""

    model_config = ConfigDict(extra="forbid")

    messages: list[ReceivedMessage] = Field(default_factory=list)


class SendResponse(BaseModel):
    """Result of a single send."""

    model_config = ConfigDict(extra="forbid")

    message_id: str
    event_id: str


class BatchSendEntry(BaseModel):
    """One entry in a batch send. ``id`` correlates the per-entry result."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., min_length=1)
    message: IngressEnvelope


class SendBatchRequest(BaseModel):
    """Body of a batch send."""

    model_config = ConfigDict(extra="forbid")

    entries: list[BatchSendEntry] = Field(..., min_length=1, max_length=MAX_BATCH_ENTRIES)

    @model_validator(mode="after")
    def _unique_ids(self) -> "SendBatchRequest":
        _reject_duplicate_ids(entry.id for entry in self.entries)
        return self


class DeleteEntry(BaseModel):
    """One receipt handle to delete."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., min_length=1)
    receipt_handle: str = Field(..., min_length=1)


class DeleteRequest(BaseModel):
    """Body of a delete call — one or more handles for a single group."""

    model_config = ConfigDict(extra="forbid")

    group: str = Field(..., min_length=1)
    entries: list[DeleteEntry] = Field(..., min_length=1, max_length=MAX_BATCH_ENTRIES)

    @model_validator(mode="after")
    def _unique_ids(self) -> "DeleteRequest":
        _reject_duplicate_ids(entry.id for entry in self.entries)
        return self


class ChangeVisibilityEntry(BaseModel):
    """One visibility change.

    ``visibility_timeout_seconds=0`` releases the message immediately — a real
    negative acknowledgement, which the push transport cannot express.
    """

    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., min_length=1)
    receipt_handle: str = Field(..., min_length=1)
    visibility_timeout_seconds: int = Field(..., ge=0)


class ChangeVisibilityRequest(BaseModel):
    """Body of a change-visibility call."""

    model_config = ConfigDict(extra="forbid")

    group: str = Field(..., min_length=1)
    entries: list[ChangeVisibilityEntry] = Field(
        ..., min_length=1, max_length=MAX_BATCH_ENTRIES
    )

    @model_validator(mode="after")
    def _unique_ids(self) -> "ChangeVisibilityRequest":
        _reject_duplicate_ids(entry.id for entry in self.entries)
        return self


class BatchSuccess(BaseModel):
    """A batch entry that succeeded."""

    model_config = ConfigDict(extra="forbid")

    id: str
    message_id: Optional[str] = None


class BatchFailure(BaseModel):
    """A batch entry that failed, with a stable machine-readable code."""

    model_config = ConfigDict(extra="forbid")

    id: str
    code: str
    message: str


class QueueAttributes(BaseModel):
    """Result of ``describe``.

    Every count is approximate. ``approximate_number_of_messages`` comes from
    the consumer group's ``lag`` (Redis 7.0+) and falls back to ``XLEN`` when
    the group's lag is unknowable — which happens after the stream is trimmed
    while entries are still unread.
    """

    model_config = ConfigDict(extra="forbid")

    queue: str
    stream: str
    stream_length: int = Field(
        ..., description="Retained entries, shared across ALL groups — not a backlog."
    )
    groups: list[str]
    group: Optional[str] = None
    approximate_number_of_messages: Optional[int] = None
    approximate_number_of_messages_not_visible: Optional[int] = Field(
        default=None, description="Pending-entries-list size — the in-flight set."
    )
    approximate_number_of_messages_delayed: int = Field(
        default=0,
        description="Always 0: delayed delivery is not implemented (Redis Streams has none).",
    )
    default_visibility_timeout_seconds: int = 0
    max_receives: int = 0


def _reject_duplicate_ids(ids) -> None:
    """Raise when a batch reuses an entry id, which would alias its result."""
    seen: set[str] = set()
    for entry_id in ids:
        if entry_id in seen:
            raise ValueError(f"duplicate batch entry id {entry_id!r}")
        seen.add(entry_id)
