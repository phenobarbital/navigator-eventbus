"""Unit tests for LoggingSubscriber (FEAT-313 TASK-1824)."""
import logging
from dataclasses import dataclass

import pytest

from navigator_eventbus.lifecycle.base import LifecycleEvent
from navigator_eventbus.lifecycle.registry import EventRegistry
from navigator_eventbus.lifecycle.subscribers.logging import LoggingSubscriber
from navigator_eventbus.lifecycle.trace import TraceContext


@dataclass(frozen=True)
class _TestEvent(LifecycleEvent):
    detail: str = ""


class TestLoggingSubscriber:
    def test_register_subscribes(self):
        registry = EventRegistry(forward_to_global=False)
        sub = LoggingSubscriber()
        sub.register(registry)
        assert registry.has_subscribers(_TestEvent)

    def test_default_logger_name(self):
        sub = LoggingSubscriber()
        assert sub._logger.name == "parrot.lifecycle"

    def test_custom_logger_name(self):
        sub = LoggingSubscriber(logger_name="my.custom.logger")
        assert sub._logger.name == "my.custom.logger"

    @pytest.mark.asyncio
    async def test_logs_event(self, caplog):
        registry = EventRegistry(forward_to_global=False)
        sub = LoggingSubscriber(level=logging.INFO)
        sub.register(registry)
        evt = _TestEvent(trace_context=TraceContext.new_root(),
                         source_type="test", source_name="unit")
        with caplog.at_level(logging.INFO, logger="parrot.lifecycle"):
            await registry.emit(evt)
        assert any("_TestEvent" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_at_configured_level(self, caplog):
        registry = EventRegistry(forward_to_global=False)
        sub = LoggingSubscriber(level=logging.DEBUG)
        sub.register(registry)
        evt = _TestEvent(trace_context=TraceContext.new_root(),
                         source_type="test", source_name="unit")
        with caplog.at_level(logging.DEBUG, logger="parrot.lifecycle"):
            await registry.emit(evt)
        assert any(r.levelno == logging.DEBUG for r in caplog.records)
