"""Rewire verification tests for hooks/brokers/{redis,rabbitmq,sqs} (TASK-1818, FEAT-316)."""
import importlib
import inspect

import pytest

from navigator_eventbus.hooks.models import BrokerHookConfig


@pytest.fixture
def broker_config():
    return BrokerHookConfig(
        queue_name="test_stream",
        group_name="test_group",
        consumer_name="test_consumer",
    )


@pytest.mark.parametrize(
    "mod,klass",
    [
        ("navigator_eventbus.hooks.brokers.redis", "RedisBrokerHook"),
        ("navigator_eventbus.hooks.brokers.rabbitmq", "RabbitMQBrokerHook"),
        ("navigator_eventbus.hooks.brokers.sqs", "SQSBrokerHook"),
    ],
)
def test_hook_rewire(mod, klass):
    """connect() sources its Connection from navigator_eventbus.brokers.*"""
    m = importlib.import_module(mod)
    src = inspect.getsource(getattr(m, klass).connect)
    assert "navigator_eventbus.brokers" in src
    assert "from navigator.brokers" not in src


def test_hooks_brokers_package_no_navigator_brokers_reference():
    """No stray `navigator.brokers` reference anywhere under hooks/brokers/."""
    import navigator_eventbus.hooks.brokers as pkg

    pkg_dir = pkg.__path__[0]  # type: ignore[attr-defined]
    import pathlib

    for path in pathlib.Path(pkg_dir).glob("*.py"):
        text = path.read_text()
        assert "navigator.brokers" not in text, f"{path} still references navigator.brokers"


def test_hooks_brokers_construct_with_config(broker_config):
    """Sanity check: the three rewired hooks still construct with a BrokerHookConfig."""
    from navigator_eventbus.hooks.brokers.rabbitmq import RabbitMQBrokerHook
    from navigator_eventbus.hooks.brokers.redis import RedisBrokerHook
    from navigator_eventbus.hooks.brokers.sqs import SQSBrokerHook

    assert RedisBrokerHook(broker_config) is not None
    assert RabbitMQBrokerHook(broker_config) is not None
    assert SQSBrokerHook(broker_config) is not None
