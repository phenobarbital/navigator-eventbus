"""Built-in lifecycle event subscribers.

FEAT-176 — Lifecycle Events System. FEAT-313 — moved to navigator-eventbus.

Available subscribers:

- :class:`~navigator_eventbus.lifecycle.subscribers.logging.LoggingSubscriber`
  — logs every lifecycle event via the standard logging framework.
- :class:`~navigator_eventbus.lifecycle.subscribers.webhook.WebhookSubscriber`
  — HTTP POSTs event payloads to a configured endpoint.

Note: ``OpenTelemetrySubscriber`` stays in ai-parrot (it depends on the
typed agent event taxonomy, which is NOT part of this package).
"""

from navigator_eventbus.lifecycle.subscribers.logging import LoggingSubscriber
from navigator_eventbus.lifecycle.subscribers.webhook import WebhookSubscriber

__all__ = [
    "LoggingSubscriber",
    "WebhookSubscriber",
]
