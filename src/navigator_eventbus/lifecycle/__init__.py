"""Lifecycle Events Machinery — typed, frozen, observability-first events.

Extracted from ai-parrot's ``parrot.core.events.lifecycle`` (FEAT-313).
This package contains ONLY the machinery (registry, mixin, providers,
generic subscribers). Typed agent events (``BeforeInvokeEvent``, etc.)
remain in ai-parrot and subclass ``LifecycleEvent`` from this package.

Usage::

    from navigator_eventbus.lifecycle import (
        EventRegistry, EventEmitterMixin, TraceContext,
        get_global_registry, scope,
        set_bootstrap_hook, wire_events, register_event_names,
        LoggingSubscriber, WebhookSubscriber,
    )
"""
from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.global_registry import get_global_registry, scope
from navigator_eventbus.lifecycle.meta import SubscriberErrorEvent
from navigator_eventbus.lifecycle.mixin import EventEmitterMixin, set_bootstrap_hook
from navigator_eventbus.lifecycle.provider import EventProvider
from navigator_eventbus.lifecycle.registry import AsyncSubscriber, EventRegistry
from navigator_eventbus.lifecycle.subscribers import LoggingSubscriber, WebhookSubscriber
from navigator_eventbus.lifecycle.trace import TraceContext
from navigator_eventbus.lifecycle.yaml_loader import register_event_names, wire_events

__all__ = [
    # Trace
    "TraceContext",
    # Base + meta
    "LifecycleEvent",
    "SubscriberErrorEvent",
    # Registry + dispatch
    "EventRegistry",
    "AsyncSubscriber",
    "get_global_registry",
    "scope",
    # Provider + mixin
    "EventProvider",
    "EventEmitterMixin",
    "set_bootstrap_hook",
    # yaml_loader wiring engine
    "wire_events",
    "register_event_names",
    # Built-in subscribers
    "LoggingSubscriber",
    "WebhookSubscriber",
]
