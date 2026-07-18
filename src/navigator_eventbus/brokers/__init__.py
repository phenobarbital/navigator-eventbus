"""navigator_eventbus.brokers — internal port of navigator.brokers (FEAT-316).

Base abstractions (``BaseConnection``, ``BrokerConsumer``, ``BrokerProducer``,
``BaseWrapper``), the ``DataSerializer`` utility, and the concrete Redis /
RabbitMQ / SQS broker implementations live under this package.

Only the base abstractions are re-exported at the top level; the concrete
``redis``/``rabbitmq``/``sqs`` subpackages are NOT imported eagerly here so
that `import navigator_eventbus.brokers` succeeds without their respective
optional dependencies installed (``aiormq``, ``aioboto3``, ...) — import the
subpackages directly, e.g. ``from navigator_eventbus.brokers.redis import
RedisConnection``.
"""
from .connection import BaseConnection
from .consumer import BrokerConsumer
from .producer import BrokerProducer
from .serializers import DataSerializer
from .wrapper import BaseWrapper

__all__ = [
    "BaseConnection",
    "BrokerConsumer",
    "BrokerProducer",
    "BaseWrapper",
    "DataSerializer",
]
