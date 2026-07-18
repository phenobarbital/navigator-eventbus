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

# ---------------------------------------------------------------------------
# Redis broker (TASK-1815)
# ---------------------------------------------------------------------------
REDIS_BROKER_HOST: str = config.get("REDIS_BROKER_HOST", fallback="localhost")
REDIS_BROKER_PORT: int = config.getint("REDIS_BROKER_PORT", fallback=6379)
REDIS_BROKER_PASSWORD = config.get("REDIS_BROKER_PASSWORD", fallback=None)
REDIS_BROKER_DB: int = config.getint("REDIS_BROKER_DB", fallback=0)
REDIS_BROKER_URL: str = (
    f"redis://{REDIS_BROKER_HOST}:{REDIS_BROKER_PORT}/{REDIS_BROKER_DB}"
)

# ---------------------------------------------------------------------------
# RabbitMQ broker (TASK-1816)
# ---------------------------------------------------------------------------
RABBITMQ_HOST: str = config.get("RABBITMQ_HOST", fallback="localhost")
RABBITMQ_PORT: int = config.getint("RABBITMQ_PORT", fallback=5672)
RABBITMQ_USER: str = config.get("RABBITMQ_USER", fallback="guest")
RABBITMQ_PASS: str = config.get("RABBITMQ_PASS", fallback="guest")
RABBITMQ_VHOST: str = config.get("RABBITMQ_VHOST", fallback="navigator")
#: RabbitMQ DSN, built from the constants above (navconfig-native — replaces
#: ``navigator.conf.rabbitmq_dsn``).
rabbitmq_dsn: str = (
    f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/{RABBITMQ_VHOST}"
)
