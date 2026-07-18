"""Unit tests for get_global_registry() / scope() (FEAT-313 TASK-1821)."""
import pytest

from navigator_eventbus.lifecycle.global_registry import get_global_registry, scope
from navigator_eventbus.lifecycle.registry import EventRegistry


class TestGlobalRegistry:
    def test_get_global_registry_returns_same_instance(self):
        with scope():
            reg1 = get_global_registry()
            reg2 = get_global_registry()
            assert reg1 is reg2

    def test_global_registry_forward_to_global_is_false(self):
        with scope() as reg:
            assert reg._forward_to_global is False

    def test_scope_isolation(self):
        with scope() as reg1:
            with scope() as reg2:
                assert reg1 is not reg2
            # Back in the outer scope — reg1 should be restored.
            assert get_global_registry() is reg1

    def test_scope_restores_on_exception(self):
        outer = get_global_registry()
        with pytest.raises(RuntimeError):
            with scope() as inner:
                assert inner is not outer
                raise RuntimeError("boom")
        assert get_global_registry() is outer

    def test_scope_yields_event_registry_instance(self):
        with scope() as reg:
            assert isinstance(reg, EventRegistry)
