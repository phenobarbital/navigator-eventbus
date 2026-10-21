"""Broker exceptions with no optional-dependency imports.

Kept apart from the concrete broker subpackages so callers (e.g. the
RabbitMQ broker hook) can import them at module level without pulling in
``aiormq`` / ``aioboto3`` / ``redis``.
"""


class RejectDelivery(Exception):
    """Internal signal: settle the current delivery with a non-requeued nack.

    Raised by a consumer callback (e.g. ``RabbitMQBrokerHook``) to tell
    :meth:`RabbitMQConnection.wrap_callback` — the sole owner of delivery
    settlement — to reject the message once, without acknowledging or
    republishing it (FEAT-431 Module 4).
    """


__all__ = ["RejectDelivery"]
