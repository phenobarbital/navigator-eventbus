"""Ingress adapters for navigator_eventbus (FEAT-312, Module 7).

Mudado desde
``packages/ai-parrot/src/parrot/core/events/bus/ingress/__init__.py``
(ai-parrot@686aba1fe, FEAT-310). ``WebSocketIngress`` is always available
(aiohttp is a core dependency); ``GrpcIngress`` is exposed lazily so the
core package never imports grpc — install the optional extra with
``pip install navigator-eventbus[grpc]``.
"""
from typing import Any

from navigator_eventbus.ingress.websocket import WebSocketIngress

__all__ = (
    "GrpcIngress",
    "WebSocketIngress",
)


def __getattr__(name: str) -> Any:
    """Lazily resolve grpc-dependent exports."""
    if name == "GrpcIngress":
        from navigator_eventbus.ingress.grpc import GrpcIngress
        return GrpcIngress
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
