"""Tests for TASK-1839 — envelope schema_version core field.

Covers ``ENVELOPE_SCHEMA_VERSION`` default, ``to_dict``/``from_dict``
version tolerance (lenient backwards, strict forwards), and that adding
the trailing ``schema_version`` field preserved the frozen/slots contract
and pre-spec positional-arg compatibility.
"""
import pytest

from navigator_eventbus import ENVELOPE_SCHEMA_VERSION, UnsupportedSchemaVersion
from navigator_eventbus.envelope import EventEnvelope


@pytest.fixture
def legacy_envelope_dict():
    """Wire dict as produced BEFORE this spec (no schema_version key)."""
    return {
        "topic": "order.created", "payload": {"id": 1},
        "event_id": "e-1", "timestamp": "2026-07-01T10:00:00+00:00",
        "source": "test", "severity": 20, "priority": 5,
        "correlation_id": None, "trace_context": None, "metadata": {},
    }


def test_envelope_schema_version_default():
    env = EventEnvelope(topic="t", payload={})
    assert env.schema_version == ENVELOPE_SCHEMA_VERSION == 1
    assert env.to_dict()["schema_version"] == 1


def test_from_dict_missing_version_is_legacy_v1(legacy_envelope_dict):
    env = EventEnvelope.from_dict(legacy_envelope_dict)
    assert env.schema_version == 1


def test_from_dict_unknown_version_raises(legacy_envelope_dict):
    data = {**legacy_envelope_dict, "schema_version": 99}
    with pytest.raises(UnsupportedSchemaVersion, match="order.created"):
        EventEnvelope.from_dict(data)


def test_from_dict_non_int_version_raises_cleanly(legacy_envelope_dict):
    """A loosely-typed source (e.g. JSON/JSONB) sending schema_version as a
    string must raise UnsupportedSchemaVersion, not a raw TypeError from
    the ``>`` comparison."""
    data = {**legacy_envelope_dict, "schema_version": "2"}
    with pytest.raises(UnsupportedSchemaVersion, match="order.created"):
        EventEnvelope.from_dict(data)


def test_from_dict_negative_version_raises(legacy_envelope_dict):
    data = {**legacy_envelope_dict, "schema_version": -1}
    with pytest.raises(UnsupportedSchemaVersion):
        EventEnvelope.from_dict(data)


def test_from_dict_zero_version_raises(legacy_envelope_dict):
    data = {**legacy_envelope_dict, "schema_version": 0}
    with pytest.raises(UnsupportedSchemaVersion):
        EventEnvelope.from_dict(data)


def test_from_dict_bool_version_raises(legacy_envelope_dict):
    """``bool`` is a Python ``int`` subtype but never a valid version."""
    data = {**legacy_envelope_dict, "schema_version": True}
    with pytest.raises(UnsupportedSchemaVersion):
        EventEnvelope.from_dict(data)


def test_frozen_slots_preserved_after_field_add():
    env = EventEnvelope(topic="t", payload={})
    with pytest.raises(Exception):  # FrozenInstanceError
        env.schema_version = 2
    assert not hasattr(env, "__dict__")  # slots


def test_roundtrip_preserves_version():
    env = EventEnvelope(topic="t", payload={"a": 1})
    assert EventEnvelope.from_dict(env.to_dict()).schema_version == 1


def test_positional_construction_pre_spec_arity_still_valid():
    """Pre-spec 10-positional-arg construction must remain valid; the new
    ``schema_version`` field (11th) is last and defaults."""
    from datetime import datetime, timezone

    from navigator_eventbus.envelope import Severity
    from navigator_eventbus.evb import EventPriority

    env = EventEnvelope(
        "order.created",           # topic
        {"id": 1},                 # payload
        "e-1",                     # event_id
        datetime.now(timezone.utc),  # timestamp
        "test",                    # source
        Severity.INFO,             # severity
        EventPriority.NORMAL,      # priority
        None,                      # correlation_id
        None,                      # trace_context
        {},                        # metadata
    )
    assert env.schema_version == 1
