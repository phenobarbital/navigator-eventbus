"""Unit tests for navigator_eventbus.brokers.rabbitmq (TASK-1816, FEAT-316)."""
import inspect

from navigator_eventbus.brokers.rabbitmq import RabbitMQConnection


def test_rabbitmq_connection_dsn_navconfig(monkeypatch):
    """DSN comes from navconfig env vars, not navigator.conf."""
    monkeypatch.setenv("RABBITMQ_HOST", "mq.example.com")
    monkeypatch.setenv("RABBITMQ_USER", "svc")
    # Re-evaluate the DSN builder directly (module-level constants are
    # computed once at import time; the builder itself reads live env vars).
    import importlib

    import navigator_eventbus.brokers._conf as conf_module

    importlib.reload(conf_module)
    assert "mq.example.com" in conf_module.rabbitmq_dsn
    assert "svc" in conf_module.rabbitmq_dsn


def test_rabbitmq_connection_dsn_default():
    conn = RabbitMQConnection()
    assert conn._dsn.startswith("amqp://")


def test_rabbitmq_connection_dsn_explicit_credentials():
    conn = RabbitMQConnection(credentials="amqp://user:pass@myhost:5672/vhost")
    assert conn._dsn == "amqp://user:pass@myhost:5672/vhost"


def test_rabbitmq_no_navigator_imports():
    import navigator_eventbus.brokers.rabbitmq.connection as m

    src = inspect.getsource(m)
    assert "from navigator." not in src and "import navigator." not in src
