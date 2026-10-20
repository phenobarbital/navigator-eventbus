"""Tests for queue configuration and HTTP wire models (queues, TASK-B)."""
import json

import pytest
from pydantic import ValidationError

from navigator_eventbus.queues.config import (
    DEFAULT_QUEUE_PREFIX,
    QueueConfig,
    QueueGroupConfig,
    QueueRegistry,
    assert_prefixes_disjoint,
)
from navigator_eventbus.queues.models import (
    ChangeVisibilityRequest,
    DeleteRequest,
    ReceiveRequest,
    SendBatchRequest,
)


def queue(**kwargs) -> QueueConfig:
    kwargs.setdefault("name", "orders")
    kwargs.setdefault("groups", [QueueGroupConfig(name="billing")])
    return QueueConfig(**kwargs)


# ---------------------------------------------------------------------------
# Prefix collision — the silent-drain hazard
# ---------------------------------------------------------------------------


def test_disjoint_default_prefixes_are_accepted():
    assert_prefixes_disjoint(DEFAULT_QUEUE_PREFIX, "evb:stream:")


@pytest.mark.parametrize(
    "queue_prefix, bus_prefix",
    [("evb:queue:", "evb:"), ("evb:", "evb:stream:"), ("evb:q:", "evb:q:")],
)
def test_overlapping_prefixes_are_rejected(queue_prefix, bus_prefix):
    with pytest.raises(ValueError, match="overlaps the bus stream prefix"):
        assert_prefixes_disjoint(queue_prefix, bus_prefix)


# ---------------------------------------------------------------------------
# QueueConfig
# ---------------------------------------------------------------------------


def test_queue_requires_at_least_one_group():
    with pytest.raises(ValidationError):
        QueueConfig(name="orders", groups=[])


def test_duplicate_group_names_rejected():
    with pytest.raises(ValidationError, match="duplicate consumer group"):
        queue(groups=[QueueGroupConfig(name="g"), QueueGroupConfig(name="g")])


@pytest.mark.parametrize("name", ["with/slash", "star*", " leading", "trailing ", ""])
def test_unsafe_queue_names_rejected(name):
    with pytest.raises(ValidationError):
        queue(name=name)


def test_default_visibility_must_not_exceed_the_reclaim_threshold():
    with pytest.raises(ValidationError, match="must not exceed"):
        queue(default_visibility_timeout_ms=10_000, max_visibility_timeout_ms=5_000)


def test_delete_entries_rejected_with_multiple_groups():
    """XDEL removes from the STREAM — a second group would lose the message."""
    with pytest.raises(ValidationError, match="only legal with exactly one group"):
        queue(
            groups=[QueueGroupConfig(name="a"), QueueGroupConfig(name="b")],
            delete_entries=True,
        )


def test_delete_entries_allowed_with_a_single_group():
    assert queue(delete_entries=True).delete_entries is True


def test_group_lookup():
    cfg = queue(groups=[QueueGroupConfig(name="a"), QueueGroupConfig(name="b")])
    assert cfg.group("a").name == "a"
    assert cfg.group("nope") is None


def test_receives_cap_prefers_the_group_override():
    cfg = queue(
        max_receives=5,
        groups=[QueueGroupConfig(name="a", max_receives=2), QueueGroupConfig(name="b")],
    )
    assert cfg.receives_cap("a") == 2
    assert cfg.receives_cap("b") == 5
    assert cfg.receives_cap("unknown") == 5


def test_topic_prefix_defaults_to_governed_namespace():
    assert queue().effective_topic_prefix() == "queue.orders"


def test_topic_prefix_explicit_none_opts_out():
    assert queue(topic_prefix=None).effective_topic_prefix() is None


def test_topic_prefix_can_be_overridden():
    assert queue(topic_prefix="custom").effective_topic_prefix() == "custom"


def test_tokens_absent_from_repr():
    cfg = queue(producer_tokens=("PRODUCER-SECRET",), admin_tokens=("ADMIN-SECRET",))
    assert "PRODUCER-SECRET" not in repr(cfg)
    assert "ADMIN-SECRET" not in repr(cfg)


def test_queue_config_forbids_extra_fields():
    with pytest.raises(ValidationError):
        queue(nonexistent=1)


def test_queue_config_round_trips_to_json():
    cfg = queue(max_receives=3)
    restored = QueueConfig(**json.loads(json.dumps(cfg.model_dump(mode="json"))))
    assert restored.max_receives == 3
    assert restored.groups[0].name == "billing"


# ---------------------------------------------------------------------------
# QueueRegistry
# ---------------------------------------------------------------------------


def test_registry_rejects_duplicate_queue_names():
    with pytest.raises(ValidationError, match="duplicate queue"):
        QueueRegistry(queues=[queue(), queue()])


def test_registry_stream_key_is_one_stream_per_queue():
    registry = QueueRegistry(queues=[queue()])
    assert registry.stream_for("orders") == "evb:queue:orders"


def test_registry_get_and_names():
    registry = QueueRegistry(queues=[queue(name="b"), queue(name="a")])
    assert registry.names == ["a", "b"]
    assert registry.get("a").name == "a"
    assert registry.get("zzz") is None


# ---------------------------------------------------------------------------
# Wire models
# ---------------------------------------------------------------------------


def test_receive_defaults():
    request = ReceiveRequest(group="billing")
    assert request.max_messages == 1
    assert request.wait_time_seconds == 0
    assert request.visibility_timeout_seconds is None


@pytest.mark.parametrize("value", [0, 11, 99])
def test_receive_rejects_out_of_range_max_messages(value):
    with pytest.raises(ValidationError):
        ReceiveRequest(group="billing", max_messages=value)


def test_receive_rejects_wait_time_over_the_cap():
    with pytest.raises(ValidationError):
        ReceiveRequest(group="billing", wait_time_seconds=21)


def test_receive_forbids_extra_fields():
    with pytest.raises(ValidationError):
        ReceiveRequest(group="billing", VisibilityTimeout=30)


def test_send_batch_rejects_more_than_ten():
    entries = [
        {"id": str(i), "message": {"topic": "t", "payload": {}}} for i in range(11)
    ]
    with pytest.raises(ValidationError):
        SendBatchRequest(entries=entries)


def test_send_batch_rejects_duplicate_ids():
    entries = [
        {"id": "same", "message": {"topic": "t", "payload": {}}},
        {"id": "same", "message": {"topic": "t", "payload": {}}},
    ]
    with pytest.raises(ValidationError, match="duplicate batch entry id"):
        SendBatchRequest(entries=entries)


def test_send_batch_validates_the_inner_envelope():
    """The envelope keeps its extra='forbid' contract inside a batch."""
    with pytest.raises(ValidationError):
        SendBatchRequest(
            entries=[{"id": "1", "message": {"topic": "t", "bogus_field": 1}}]
        )


def test_delete_rejects_duplicate_ids():
    with pytest.raises(ValidationError, match="duplicate batch entry id"):
        DeleteRequest(
            group="g",
            entries=[
                {"id": "1", "receipt_handle": "a"},
                {"id": "1", "receipt_handle": "b"},
            ],
        )


def test_change_visibility_accepts_zero_as_nack():
    request = ChangeVisibilityRequest(
        group="g",
        entries=[{"id": "1", "receipt_handle": "h", "visibility_timeout_seconds": 0}],
    )
    assert request.entries[0].visibility_timeout_seconds == 0


def test_change_visibility_rejects_negative():
    with pytest.raises(ValidationError):
        ChangeVisibilityRequest(
            group="g",
            entries=[
                {"id": "1", "receipt_handle": "h", "visibility_timeout_seconds": -1}
            ],
        )


def test_models_expose_a_json_schema():
    for model in (ReceiveRequest, DeleteRequest, ChangeVisibilityRequest, SendBatchRequest):
        assert "properties" in model.model_json_schema()
