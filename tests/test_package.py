"""Smoke test for the navigator_eventbus package scaffold (TASK-1798)."""
import navigator_eventbus


def test_package_imports():
    assert navigator_eventbus.__version__ == "0.1.0"
