"""navigator_eventbus.brokers — internal port of navigator.brokers (FEAT-316).

Base abstractions (``BaseConnection``, ``BrokerConsumer``, ``BrokerProducer``,
``BaseWrapper``), the ``DataSerializer`` utility, and the concrete Redis /
RabbitMQ / SQS broker implementations live under this package. Public
re-exports are added by TASK-1814 once the base abstractions land.
"""
