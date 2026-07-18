"""RedisConnection — port of navigator.brokers.redis.connection (TASK-1815, FEAT-316).

Applies PR navigator#393 fix #2 (XAUTOCLAIM-based PEL redelivery) plus the
navconfig / serialization / ValidationError desacoples: credentials are read
from :mod:`navigator_eventbus.brokers._conf` instead of ``navigator.conf``,
JSON encode/decode go through :mod:`navigator_eventbus.serialization`, and
message-decoding failures are handled as a generic ``Exception`` (logged as
a warning) instead of a navigator-specific ``ValidationError``.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import is_dataclass
from typing import Any, Awaitable, Callable, Dict, Optional, Union

from datamodel import BaseModel, Model
from navconfig.logging import logging
from redis import asyncio as aioredis

from navigator_eventbus.serialization import dumps, loads

from .._conf import (
    REDIS_BROKER_DB,
    REDIS_BROKER_HOST,
    REDIS_BROKER_PASSWORD,
    REDIS_BROKER_PORT,
)
from ..connection import BaseConnection
from ..wrapper import BaseWrapper


class RedisConnection(BaseConnection):
    """Manages connection and operations with Redis using Redis Streams."""

    def __init__(
        self,
        credentials: Union[str, dict] = None,
        timeout: Optional[int] = 5,
        **kwargs: Any,
    ) -> None:
        """Initialize the Redis connection.

        Args:
            credentials: Redis connection kwargs (``host``/``port``/
                ``password``/``db``); defaults to the local
                ``REDIS_BROKER_*`` navconfig values when omitted.
            timeout: Connection timeout, in seconds.
            **kwargs: ``group_name``/``consumer_name``/``queue_name`` and
                anything else forwarded to :class:`BaseConnection`.
        """
        self._name_ = self.__class__.__name__
        if not credentials:
            credentials = {
                "host": REDIS_BROKER_HOST,
                "port": REDIS_BROKER_PORT,
                "password": REDIS_BROKER_PASSWORD,
                "db": REDIS_BROKER_DB,
            }
        super().__init__(credentials=credentials, timeout=timeout, **kwargs)
        self._connection: Optional[aioredis.Redis] = None
        self.logger = logging.getLogger("RedisConnection")
        self._group_name = kwargs.get("group_name", "default_group")
        self._consumer_name = kwargs.get("consumer_name", "default_consumer")
        self._queue_name = kwargs.get("queue_name", "message_stream")

    async def connect(self) -> None:
        """Establish the connection with Redis and ensure the group exists."""
        if self._connection:
            return
        try:
            self.logger.info("Connecting to Redis...")
            self._connection = aioredis.Redis(
                **self._credentials, decode_responses=True, encoding="utf-8"
            )
            # Ensure that the group exists
            await self.ensure_group_exists()
            self.logger.info("Connected to Redis.")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def ensure_connection(self) -> None:
        """Ensure that the connection is active."""
        if self._connection is None:
            await self.connect()

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        self.logger.info("Disconnecting from Redis...")
        if self._connection:
            try:
                await self._connection.close()
            except Exception as e:
                self.logger.error(f"Error closing Redis connection: {e}")
        self._connection = None
        self.logger.info("Disconnected from Redis.")

    async def ensure_group_exists(self) -> None:
        """Ensure the consumer group (and consumer) exist for the stream."""
        try:
            # Create the stream if it doesn't exist
            stream_exists = await self._connection.exists(self._queue_name)
            if not stream_exists:
                await self._connection.xadd(self._queue_name, {"initial": "message"})
            # Try to create the group. This will fail if the group already exists.
            await self._connection.xgroup_create(
                name=self._queue_name,
                groupname=self._group_name,
                id="0",
                mkstream=True,
            )
            self.logger.info(
                f"Group '{self._group_name}' created on stream '{self._queue_name}'."
            )
        except aioredis.ResponseError as e:
            if "BUSYGROUP Consumer Group name already exists" in str(e):
                self.logger.info(f"Group '{self._group_name}' already exists.")
            else:
                self.logger.error(f"Error creating group '{self._group_name}': {e}")
                raise
        try:
            # create the consumer:
            await self._connection.xgroup_createconsumer(
                self._queue_name, self._group_name, self._consumer_name
            )
            self.logger.debug(
                f":: Creating Consumer {self._consumer_name} on Stream {self._queue_name}"
            )
        except Exception as exc:
            self.logger.exception(exc, stack_info=True)
            raise

    async def publish_message(
        self,
        body: Union[str, list, dict, Any],
        queue_name: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Publish a message to the specified Redis Stream."""
        stream = queue_name or self._queue_name
        try:
            message_data: Dict[str, Any] = {}
            # Determine serialization method based on the type of 'body'
            if isinstance(body, (int, float, bool, type(None))):
                # Use msgpack for primitives
                packed_body = self._serializer.pack(body)
                content_type = "application/msgpack"
            elif isinstance(body, bytes):
                # Use msgpack for raw bytes
                packed_body = self._serializer.pack(body)
                content_type = "application/msgpack"
            elif isinstance(body, (dict, list)):
                # Use JSON (orjson) for dictionaries/lists
                packed_body = dumps(body)
                content_type = "application/json"
            elif is_dataclass(body) or isinstance(body, (Model, BaseModel)):
                # cloudpickle serialization for dataclasses or BaseModel
                packed_body = self._serializer.serialize(body)
                content_type = "application/cloudpickle"
            elif isinstance(body, BaseWrapper):
                # cloudpickle serialization for BaseWrapper
                packed_body = self._serializer.serialize(body)
                content_type = "application/cloudpickle"
            elif hasattr(body, "__class__") and not isinstance(body, (str, bytes)):
                # cloudpickle serialization for other custom objects
                packed_body = self._serializer.serialize(body)
                content_type = "application/cloudpickle"
            else:
                # Fallback to plain text for str and other simple types
                packed_body = str(body)
                content_type = "text/plain"

            message_data["body"] = packed_body
            message_data["ContentType"] = content_type

            await self._connection.xadd(stream, message_data, nomkstream=False)
            self.logger.info(f"Message published to stream '{stream}'.")
        except Exception as e:
            self.logger.error(f"Failed to publish message to stream '{stream}': {e}")
            raise

    async def process_message(self, message_data: Dict[Any, Any]) -> Any:
        """Process (decode) the message received by the consumer.

        Decode failures are logged as a warning and the raw body is
        returned — no navigator-specific ``ValidationError`` handling.
        """
        body = message_data.get("body")
        content_type = message_data.get("ContentType", "text/plain")
        try:
            if content_type == "application/json":
                return loads(body)
            elif content_type == "application/msgpack":
                return self._serializer.unpack(body)
            elif content_type == "application/jsonpickle":
                try:
                    return self._serializer.decode(body)
                except Exception as e:
                    self.logger.error(f"Error decoding JSONPickle message: {e}")
                    return body
            elif content_type == "application/cloudpickle":
                return self._serializer.unserialize(body)
            elif content_type == "text/plain":
                return body
            else:
                self.logger.warning(
                    f"Unknown content type: {content_type}. Returning raw body."
                )
                return body
        except Exception as e:
            self.logger.warning(f"Error decoding message: {e}")
            return body

    async def consume_messages(
        self,
        queue_name: Optional[str],
        callback: Callable[[Dict[str, Any], str], Awaitable[None]],
        count: int = 1,
        block: int = 1000,
        **kwargs: Any,
    ) -> None:
        """Consume messages from the specified Redis Stream via the callback."""
        stream = queue_name or self._queue_name
        consumer_name = kwargs.get("consumer_name", self._consumer_name)
        try:
            # Clean up old messages before starting
            await self.cleanup_old_messages(stream)
            while True:
                response = await self._connection.xreadgroup(
                    groupname=self._group_name,
                    consumername=consumer_name,
                    streams={stream: ">"},
                    count=count,
                    block=block,
                )
                if not response:
                    await asyncio.sleep(1)
                    continue
                for _, messages in response:
                    for message_id, message_data in messages:
                        try:
                            processed_message = await self.process_message(
                                message_data
                            )
                            data = {"message_id": message_id, "data": message_data}
                            if asyncio.iscoroutinefunction(callback):
                                await callback(data, processed_message)
                            else:
                                callback(data, processed_message)
                            # Acknowledge the message
                            await self._connection.xack(
                                stream, self._group_name, message_id
                            )
                            self.logger.info(f"Message {message_id} acknowledged.")
                        except Exception as e:
                            self.logger.error(
                                f"Error processing message {message_id}: {e}"
                            )
        except (asyncio.CancelledError, KeyboardInterrupt):
            self.logger.info("Message consumption cancelled. Cleaning up...")
            raise
        except Exception as e:
            self.logger.error(f"Error consuming messages from stream '{stream}': {e}")
            raise

    async def cleanup_old_messages(self, stream: str) -> None:
        """Remove messages older than 7 days from the stream."""
        try:
            # Calculate the timestamp for 7 days ago
            seven_days_ago = int((time.time() - 7 * 24 * 60 * 60) * 1000)
            # Convert it to a Redis Stream ID format (timestamp-part-sequence)
            seven_days_ago_id = f"{seven_days_ago}-0"
            # Use XTRIM with minid to remove messages older than the calculated timestamp
            await self._connection.xtrim(stream, minid=seven_days_ago_id)
            self.logger.info(f"Cleaned up old messages from stream {stream}")
        except Exception as e:
            self.logger.error(f"Error cleaning up old messages: {e}")

    async def reclaim_pending_messages(
        self,
        queue_name: str,
        callback: Callable,
        *,
        min_idle_time: int = 30_000,
        count: int = 10,
    ) -> int:
        """FIX #2: XAUTOCLAIM-based redelivery of stuck PEL entries.

        Opt-in — callers are responsible for scheduling this sweep (e.g. on
        a periodic timer); it is not invoked automatically by
        :meth:`consume_messages`. Gracefully returns ``0`` when the Redis
        server does not support ``XAUTOCLAIM`` (Redis < 6.2).

        Args:
            queue_name: The stream whose Pending Entries List is swept.
            callback: Invoked as ``callback(message_id, body)`` for each
                claimed (redelivered) message.
            min_idle_time: Minimum idle time, in milliseconds, before a PEL
                entry becomes eligible for reclaim.
            count: Maximum number of entries to claim in this sweep.

        Returns:
            The number of messages claimed and redelivered.
        """
        try:
            _, claimed_messages, _ = await self._connection.xautoclaim(
                queue_name,
                self._group_name,
                self._consumer_name,
                min_idle_time,
                start_id="0-0",
                count=count,
            )
        except aioredis.ResponseError as exc:
            self.logger.warning(
                "XAUTOCLAIM not supported on this Redis server "
                f"(requires Redis >= 6.2): {exc}"
            )
            return 0
        for message_id, message_data in claimed_messages:
            try:
                body = await self.process_message(message_data)
            except Exception:
                body = message_data
            if asyncio.iscoroutinefunction(callback):
                await callback(message_id, body)
            else:
                callback(message_id, body)
        return len(claimed_messages)
