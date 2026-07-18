"""Unit tests for navigator_eventbus.brokers.redis.consumer (TASK-1815, FEAT-316)."""
from navigator_eventbus.brokers.redis import RedisConsumer


def test_redis_consumer_kwargs_pop():
    """PR #393 fix #1: explicit stream kwargs must not raise TypeError."""
    c = RedisConsumer(
        queue_name="test_stream",
        group_name="test_group",
        consumer_name="test_consumer",
    )
    assert c._queue_name == "test_stream"
    assert c._group_name == "test_group"
    assert c._consumer_name == "test_consumer"


def test_redis_consumer_default_kwargs():
    c = RedisConsumer()
    assert c._queue_name == "message_stream"
    assert c._group_name == "default_group"
    assert c._consumer_name == "default_consumer"
