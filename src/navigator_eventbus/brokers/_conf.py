"""Local navconfig reads for navigator_eventbus.brokers shared constants.

Ported from ``navigator.conf`` (TASK-1814, FEAT-316) — decouples the brokers
package from the navigator framework's config module (spec §7 desacople
pattern: read broker-related env vars via ``navconfig.config`` directly
instead of importing from ``navigator.conf``). Redis/RabbitMQ constants are
added here by TASK-1815/1816.
"""
from navconfig import config

#: Max size of the asyncio.Queue used by BrokerProducer to buffer outgoing
#: events before a worker picks them up. Source (navigator.conf) currently
#: falls back to 4; the port raises the default to 1000 (per task design).
BROKER_MANAGER_QUEUE_SIZE: int = config.getint(
    "BROKER_MANAGER_QUEUE_SIZE", fallback=1000
)
