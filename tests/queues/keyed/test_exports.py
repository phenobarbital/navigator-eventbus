"""Lazy export test (FEAT-713 / TASK-4579)."""
import subprocess
import sys


def test_exports_lazy():
    code = (
        "import sys\n"
        "import navigator_eventbus.queues as q\n"
        "assert 'navigator_eventbus.queues.keyed.store' not in sys.modules\n"
        "assert 'navigator_eventbus.queues.keyed.consumer' not in sys.modules\n"
        "assert q.Outcome.ack().kind == 'ack'\n"
        "assert 'navigator_eventbus.queues.keyed.store' not in sys.modules\n"
        "from navigator_eventbus.queues import KeyedQueue, KeyedConsumer, KeyedLease, drain\n"
        "assert 'navigator_eventbus.queues.keyed.store' in sys.modules\n"
        "import navigator_eventbus.queues.keyed as k\n"
        "assert k.KeyedQueue is KeyedQueue and callable(k.backoff_ms) and callable(drain)\n"
        "assert all(hasattr(q, n) for n in q.__all__), [n for n in q.__all__ if not hasattr(q, n)]\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_all_sorted_and_unknown_attribute():
    import pytest

    import navigator_eventbus.queues as q

    assert list(q.__all__) == sorted(q.__all__)
    with pytest.raises(AttributeError):
        q.NopeNotThere  # noqa: B018
