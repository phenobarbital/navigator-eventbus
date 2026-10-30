"""Validators and constructors of the keyed-queue models (FEAT-713 / TASK-4573)."""
import pytest
from pydantic import ValidationError

from navigator_eventbus.queues.config import QueueConfig, QueueGroupConfig
from navigator_eventbus.queues.keyed import KeyedMessage, KeyedQueueConfig, Outcome


def _signal(visibility: int = 20_000, groups: int = 1) -> QueueConfig:
    return QueueConfig(
        name="sig",
        groups=[QueueGroupConfig(name=f"g{i}") for i in range(groups)],
        default_visibility_timeout_ms=visibility,
        max_visibility_timeout_ms=max(visibility, 20_000),
    )


def _cfg(**kw) -> KeyedQueueConfig:
    kw.setdefault("signal", _signal())
    kw.setdefault("lock_ttl_ms", 10_000)
    return KeyedQueueConfig(name="q", **kw)


def test_visibility_must_exceed_lock_ttl():
    with pytest.raises(ValidationError):
        _cfg(signal=_signal(10_000), lock_ttl_ms=10_000)
    with pytest.raises(ValidationError):
        _cfg(signal=_signal(5_000), lock_ttl_ms=10_000)
    assert _cfg().lock_ttl_ms == 10_000


@pytest.mark.parametrize("groups", [0, 2])
def test_signal_requires_exactly_one_group(groups):
    with pytest.raises(ValidationError):
        _cfg(signal=_signal(groups=groups))


def test_retry_bounds():
    with pytest.raises(ValidationError):
        _cfg(retry_base_ms=1000, retry_max_ms=500)


def test_effective_wait_safety():
    assert _cfg().effective_wait_safety_ms == 100_000
    assert _cfg(wait_safety_ms=5_000).effective_wait_safety_ms == 5_000


def test_extra_forbidden():
    with pytest.raises(ValidationError):
        _cfg(bogus=1)
    with pytest.raises(ValidationError):
        KeyedMessage(message_id="a", key="k", body={}, order=1, created_ms=1, nope=1)
    with pytest.raises(ValidationError):
        Outcome(kind="ack", nope=1)


def test_outcome_constructors():
    assert Outcome.ack().kind == "ack"
    r = Outcome.retry(delay_ms=5, reason="x")
    assert (r.kind, r.delay_ms, r.reason) == ("retry", 5, "x")
    assert Outcome.retry().delay_ms is None
    w = Outcome.wait(until_ms=9, reason="y")
    assert (w.kind, w.until_ms) == ("wait", 9)
    assert Outcome.hold("ops").reason == "ops"
    with pytest.raises(ValueError):
        Outcome.hold("")


def test_config_modules_do_not_import_redis():
    """The package itself stays Redis-free (``redis`` is loaded by the base package anyway)."""
    import pathlib

    import navigator_eventbus.queues.keyed as pkg

    for path in pathlib.Path(pkg.__file__).parent.glob("*.py"):
        if path.name in {"__init__.py", "config.py"}:
            assert "import redis" not in path.read_text()
            assert "from redis" not in path.read_text()
