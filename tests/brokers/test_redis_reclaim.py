"""Unit tests for RedisConnection.reclaim_pending_messages (TASK-1815, FEAT-316)."""
from unittest.mock import AsyncMock

import pytest

from navigator_eventbus.brokers.redis import RedisConnection


@pytest.fixture
def mock_redis():
    """Mock aioredis.Redis for unit tests."""
    return AsyncMock()


async def test_reclaim_pending_messages(mock_redis):
    conn = RedisConnection()
    conn._connection = mock_redis
    mock_redis.xautoclaim = AsyncMock(
        return_value=(b"0-0", [(b"1-0", {b"data": b"{}"})], [])
    )
    seen = []

    async def cb(mid, body):
        seen.append(mid)

    n = await conn.reclaim_pending_messages("test_stream", cb)
    assert n == 1 and len(seen) == 1


async def test_reclaim_pending_empty_pel(mock_redis):
    conn = RedisConnection()
    conn._connection = mock_redis
    mock_redis.xautoclaim = AsyncMock(return_value=(b"0-0", [], []))
    n = await conn.reclaim_pending_messages("test_stream", lambda *a: None)
    assert n == 0
