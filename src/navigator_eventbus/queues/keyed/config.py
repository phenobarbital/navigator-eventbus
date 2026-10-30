"""Models for keyed queues (FEAT-713): config, message, outcome, drain report."""
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from navigator_eventbus.queues.config import QueueConfig

__all__ = ("DrainReport", "KeyedMessage", "KeyedQueueConfig", "LeaseLost", "Outcome")


class LeaseLost(Exception):
    """The per-key lock expired or changed owner before the outcome was applied."""


class KeyedQueueConfig(BaseModel):
    """One keyed queue; ``signal`` is the embedded pull queue (exactly one group)."""

    model_config = ConfigDict(extra="forbid")

    name: str
    signal: QueueConfig
    lock_ttl_ms: int = Field(default=60_000, gt=0)
    call_timeout_ms: int = Field(default=30_000, gt=0)
    max_attempts: int = Field(default=0, ge=0)  # 0 = unlimited (Q2)
    max_age_ms: Optional[int] = Field(default=None, gt=0)
    retry_base_ms: int = Field(default=30_000, gt=0)
    retry_max_ms: int = Field(default=900_000, gt=0)
    retry_factor: float = Field(default=2.0, ge=1.0)
    promote_batch: int = Field(default=100, gt=0)
    pause_default_ttl_ms: int = Field(default=60_000, gt=0)
    pause_poll_ms: int = Field(default=1_000, gt=0)
    recover_interval_ms: int = Field(default=60_000, gt=0)
    wait_safety_ms: Optional[int] = Field(default=None, gt=0)
    prefix: str = "evq:keyed:"

    @field_validator("signal")
    @classmethod
    def _single_group(cls, value: QueueConfig) -> QueueConfig:
        """Raises ValueError when the signal queue declares more than one group."""
        if len(value.groups) != 1:
            raise ValueError(
                f"signal queue {value.name!r} must declare exactly one group, "
                f"got {len(value.groups)}"
            )
        return value

    @model_validator(mode="after")
    def _check_timings(self) -> "KeyedQueueConfig":
        """Signal lease must outlive the lock (signal rule 2); retry base <= max."""
        if self.signal.default_visibility_timeout_ms <= self.lock_ttl_ms:
            raise ValueError(
                "signal.default_visibility_timeout_ms "
                f"({self.signal.default_visibility_timeout_ms}) must be greater than "
                f"lock_ttl_ms ({self.lock_ttl_ms})"
            )
        if self.retry_base_ms > self.retry_max_ms:
            raise ValueError("retry_base_ms must be <= retry_max_ms")
        return self

    @property
    def effective_wait_safety_ms(self) -> int:
        """Deadline applied to a ``wait`` without ``until_ms`` (spec §8 Q3)."""
        return self.wait_safety_ms or self.lock_ttl_ms * 10


class KeyedMessage(BaseModel):
    """A message as stored in ``msg:<id>``."""

    model_config = ConfigDict(extra="forbid")

    message_id: str
    key: str
    body: dict[str, Any]
    order: float
    created_ms: int
    attempts: int = 0
    state: Literal["pending", "leased", "waiting", "held"] = "pending"
    last_error: Optional[str] = None
    wait_until_ms: Optional[int] = None
    due_ms: Optional[int] = None
    gen: int = 0


class Outcome(BaseModel):
    """What a handler returns; applied by ``KeyedQueue.apply``."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["ack", "retry", "wait", "hold"]
    delay_ms: Optional[int] = Field(default=None, ge=0)
    until_ms: Optional[int] = None
    reason: Optional[str] = None

    @classmethod
    def ack(cls) -> "Outcome":
        """Work done: remove the head."""
        return cls(kind="ack")

    @classmethod
    def retry(
        cls, delay_ms: Optional[int] = None, reason: Optional[str] = None
    ) -> "Outcome":
        """Keep the head, retry after ``delay_ms`` (None -> backoff)."""
        return cls(kind="retry", delay_ms=delay_ms, reason=reason)

    @classmethod
    def wait(
        cls, until_ms: Optional[int] = None, reason: Optional[str] = None
    ) -> "Outcome":
        """Keep the head and block the key until ``until_ms`` or a re-signal."""
        return cls(kind="wait", until_ms=until_ms, reason=reason)

    @classmethod
    def hold(cls, reason: str) -> "Outcome":
        """Park the key for an operator; ``reason`` is mandatory."""
        if not reason:
            raise ValueError("hold requires a non-empty reason")
        return cls(kind="hold", reason=reason)


class DrainReport(BaseModel):
    """Counters returned by ``drain``."""

    model_config = ConfigDict(extra="forbid")

    keys_seen: int = 0
    acked: int = 0
    retried: int = 0
    waited: int = 0
    held: int = 0
    busy: int = 0
    expired: int = 0
    stopped_early: bool = False
    elapsed_ms: int = 0
