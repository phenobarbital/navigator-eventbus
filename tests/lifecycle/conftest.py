"""Shared pytest fixtures for the navigator_eventbus.lifecycle test suite.

FEAT-313 — EventBus Lifecycle Extraction (navigator-eventbus phase 2).

Note: ``scope()`` is imported lazily inside the fixture body (rather than at
module level) so this conftest can be collected starting from Module 1
(TASK-1820), before ``global_registry.py`` exists (added in Module 2 /
TASK-1821).
"""
import pytest


@pytest.fixture
def fresh_global_registry():
    """Isolate the global registry per test via scope()."""
    from navigator_eventbus.lifecycle.global_registry import scope

    with scope() as reg:
        yield reg
