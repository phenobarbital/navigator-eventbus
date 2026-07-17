"""Converter tests specific to the FEAT-312 open ``hook_type`` decoupling.

The bulk of the converter test coverage (legacy Event / lifecycle-dict /
HookEvent → EventEnvelope) is mudada into ``test_envelope.py`` (mirroring
the origin layout at
``packages/ai-parrot/tests/core/events/bus/test_envelope.py``). This file
covers the one FEAT-312-specific behavior change: ``from_hook_event``
reads ``event.hook_type`` directly (an open, registry-validated ``str``)
instead of a closed ``HookType`` enum's ``.value``.
"""
from navigator_eventbus.converters import from_hook_event
from navigator_eventbus.hooks.models import HOOK_TYPES, HookEvent


def test_from_hook_event_uses_open_str_hook_type():
    HOOK_TYPES.register("test_converter_hook")
    hook = HookEvent(
        hook_id="h1",
        hook_type="test_converter_hook",
        event_type="fired",
        payload={"a": 1},
    )
    env = from_hook_event(hook)
    assert env.topic == "hooks.test_converter_hook.fired"
    assert env.payload == {"a": 1}
    HOOK_TYPES.unregister("test_converter_hook")


def test_from_hook_event_metadata_routing_hints():
    hook = HookEvent(
        hook_id="h1",
        hook_type="scheduler",
        event_type="tick",
        payload={},
        target_type="agent",
        target_id="my-agent",
        task="do the thing",
    )
    env = from_hook_event(hook)
    assert env.metadata["hook_id"] == "h1"
    assert env.metadata["target_type"] == "agent"
    assert env.metadata["target_id"] == "my-agent"
    assert env.metadata["task"] == "do the thing"
