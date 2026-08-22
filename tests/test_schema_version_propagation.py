"""Tests for TASK-1840 — schema_version propagation.

Closes the remaining envelope-producing paths beyond ``from_dict``
(covered by TASK-1839): DLQ Postgres row reconstruction, the
``IngressEnvelope`` boundary model, and the three in-process converters.
"""
from datetime import datetime, timezone

from navigator_eventbus.converters import (
    from_hook_event,
    from_legacy_event,
    from_lifecycle_dict,
)
from navigator_eventbus.dlq import DLQHandler
from navigator_eventbus.evb import Event
from navigator_eventbus.hooks.models import HookEvent
from navigator_eventbus.ingress_models import IngressEnvelope


def legacy_dlq_row():
    """Postgres evb_dlq row persisted BEFORE this spec (no schema_version)."""
    return {
        "topic": "order.created", "payload": '{"id": 1}',
        "event_id": "e-1", "failed_at": "2026-07-01T10:00:00",
        "source": "test", "severity": 20, "priority": 5,
        "correlation_id": None, "trace_context": None, "metadata": "{}",
        "attempts": 3, "error": "boom", "subscriber_id": "sub-1",
    }


def test_dlq_row_to_envelope_legacy_v1():
    env = DLQHandler._row_to_envelope(legacy_dlq_row())
    assert env.schema_version == 1


def test_dlq_row_to_envelope_passes_through_stored_version():
    row = {**legacy_dlq_row(), "schema_version": 1}
    env = DLQHandler._row_to_envelope(row)
    assert env.schema_version == 1


def test_ingress_envelope_schema_version_default_and_passthrough():
    ing = IngressEnvelope(
        topic="t", payload={}, timestamp="2026-07-01T10:00:00+00:00"
    )
    assert ing.schema_version == 1
    assert ing.to_envelope().schema_version == 1
    # explicit key must not trip extra="forbid"
    ing2 = IngressEnvelope(
        topic="t", payload={}, timestamp="2026-07-01T10:00:00+00:00",
        schema_version=1,
    )
    assert ing2.schema_version == 1


def test_converters_emit_schema_version():
    lifecycle_env = from_lifecycle_dict({"event_class": "X", "data": {}})
    assert lifecycle_env.schema_version == 1

    legacy_env = from_legacy_event(
        Event(event_type="order.created", payload={"id": 1})
    )
    assert legacy_env.schema_version == 1

    hook_env = from_hook_event(
        HookEvent(
            hook_id="hook-1",
            hook_type="webhook",
            event_type="received",
            payload={"id": 1},
            timestamp=datetime.now(timezone.utc),
        )
    )
    assert hook_env.schema_version == 1
