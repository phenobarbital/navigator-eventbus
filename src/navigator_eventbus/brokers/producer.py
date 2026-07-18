"""BrokerProducer — port of navigator.brokers.producer (TASK-1814, FEAT-316).

Applies three desacoples plus PR navigator#393 fix #3:

- **Fix #3**: ``credentials`` is now a keyword argument with a ``None``
  default (was positional-required in the source, breaking construction
  when credentials come from config rather than the caller).
- **auth desacople**: ``service_auth`` no longer hard-imports
  ``navigator_session``/``navigator_auth``; callers inject an
  ``auth_callable`` at construction time instead.
- **BaseApplication desacople**: ``setup()`` duck-types the app object
  (see :mod:`navigator_eventbus.brokers.connection`).
- **navigator.conf desacople**: ``BROKER_MANAGER_QUEUE_SIZE`` is read from
  the local :mod:`navigator_eventbus.brokers._conf` module.
"""
from __future__ import annotations

import asyncio
from abc import ABC
from functools import wraps
from typing import Any, Awaitable, Callable, Optional, Union

from aiohttp import web
from navconfig.logging import logging

from ._conf import BROKER_MANAGER_QUEUE_SIZE
from .connection import BaseConnection


class BrokerProducer(BaseConnection, ABC):
    """Broker Producer Interface.

    Args:
        credentials: Message Queue credentials (keyword, defaults to
            ``None`` — PR #393 fix #3).
        queue_size: Size of the asyncio Queue used to buffer outgoing
            events before a worker picks them up.
        num_workers: Number of workers processing the queue.
        timeout: Timeout for the MQ connection.
        auth_callable: Optional resolver invoked by :meth:`service_auth` to
            authenticate the incoming ``aiohttp`` request and return a
            session dict. When ``None``, protected endpoints reject the
            request with 401 — the producer never proceeds unauthenticated.
    """

    _name_: str = "broker_producer"

    def __init__(
        self,
        credentials: Optional[Union[str, dict]] = None,
        queue_size: Optional[int] = None,
        num_workers: Optional[int] = 4,
        timeout: Optional[int] = 5,
        *,
        auth_callable: Optional[Callable[[web.Request], Awaitable[Any]]] = None,
        **kwargs: Any,
    ) -> None:
        self.queue_size: int = queue_size if queue_size else BROKER_MANAGER_QUEUE_SIZE
        self.app: Optional[web.Application] = None
        self.timeout: Optional[int] = timeout
        self.logger = logging.getLogger("Broker.Producer")
        self.event_queue: asyncio.Queue = asyncio.Queue(maxsize=self.queue_size)
        self._num_workers: int = num_workers if num_workers else 4
        self._workers: list = []
        self._broker_service: str = kwargs.get("broker_service", "rabbitmq")
        self._auth_callable = auth_callable
        self._userid: Optional[int] = None
        # NOTE: passed as keywords (not positionally, as the navigator source
        # did) so BaseConnection.__init__ actually receives and stores them
        # via its own `credentials`/`timeout` parameters instead of
        # swallowing them into `*args` and forwarding them, un-consumed,
        # all the way down the cooperative super() chain to
        # `object.__init__()` — which raises TypeError on any leftover
        # positional argument. Required for fix #3 (credentials keyword) to
        # actually propagate `self._credentials`.
        super().__init__(credentials=credentials, timeout=timeout, **kwargs)

    def setup(self, app: Any = None) -> None:
        """Wire this producer into an aiohttp application.

        Same duck-typing desacople as :meth:`BaseConnection.setup`, plus
        registration of the ``event_publisher`` HTTP endpoint. Typed as
        ``Any`` — see :meth:`BaseConnection.setup` for why.

        Raises:
            ValueError: If *app* (or ``app.get_app()``) resolves to ``None``.
        """
        app = app.get_app() if hasattr(app, "get_app") else app
        self.app = app
        if self.app is None:
            raise ValueError("App is not defined.")
        # Initialize the Producer instance.
        self.app.on_startup.append(self.start)
        self.app.on_shutdown.append(self.stop)
        self.app[self._name_] = self
        # Generic Event Subscription:
        self.app.router.add_post(
            f"/api/v1/broker/{self._broker_service}/publish_event",
            self.event_publisher,
        )
        self.logger.notice(":: Starting Message Queue Producer ::")  # type: ignore[attr-defined]

    async def start_workers(self) -> None:
        """Start the queue worker tasks."""
        for i in range(self._num_workers):
            task = asyncio.create_task(self._event_broker(i))
            self._workers.append(task)

    async def start(self, app: web.Application) -> None:
        """``on_startup`` signal handler.

        Connects to the Message Queue and starts the queue workers.
        """
        await self.connect()
        # Start the Queue workers
        await self.start_workers()

    async def stop(self, app: web.Application) -> None:
        """``on_shutdown`` signal handler — drains the queue and disconnects."""
        # Wait for all events to be processed
        await self.event_queue.join()

        # Cancel worker tasks
        for task in self._workers:
            try:
                task.cancel()
            except asyncio.CancelledError:
                pass

        # Wait for worker tasks to finish
        await asyncio.gather(*self._workers, return_exceptions=True)

        # then, close the Message Queue connection
        await self.disconnect()

    async def queue_event(
        self,
        body: str,
        queue_name: str,
        routing_key: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Put an event onto the queue to be produced later.

        Raises:
            asyncio.QueueFull: If the event queue is full.
        """
        try:
            self.event_queue.put_nowait(
                {
                    "body": body,
                    "queue_name": queue_name,
                    "routing_key": routing_key,
                    **kwargs,
                }
            )
            await asyncio.sleep(0.1)
        except asyncio.QueueFull:
            self.logger.error("Event queue is full. Event will not published.")
            raise

    async def publish_event(
        self,
        body: str,
        queue_name: str,
        **kwargs: Any,
    ) -> None:
        """Publish an event on a Message Queue exchange."""
        await self.publish_message(body=body, queue_name=queue_name, **kwargs)

    async def get_userid(self, session: dict, idx: str = "user_id") -> int:
        """Extract the user id from a resolved session dict.

        Args:
            session: The session dict returned by ``auth_callable``.
            idx: The key used to look up the user id within *session*.

        Returns:
            The resolved user id.

        Raises:
            RuntimeError: If the user id cannot be found in the session.
        """
        try:
            return session[idx]
        except KeyError:
            raise RuntimeError("User ID is not found in the session.") from None

    @staticmethod
    def service_auth(
        fn: Callable[..., Awaitable[Any]]
    ) -> Callable[..., Awaitable[Any]]:
        """Decorate an endpoint to require an injected ``auth_callable``.

        When no ``auth_callable`` was provided at construction, the request
        is rejected with 401 — the producer never proceeds unauthenticated
        (spec §7).
        """

        @wraps(fn)
        async def _wrap(self, request: web.Request, *args: Any, **kwargs: Any) -> Any:
            if self._auth_callable is None:
                raise web.HTTPUnauthorized(reason="No authentication configured")
            try:
                session = await self._auth_callable(request)
            except (ValueError, RuntimeError) as err:
                raise web.HTTPUnauthorized(reason=str(err)) from err
            if session:
                self._userid = await self.get_userid(session)
            # Perform your session and user ID checks here
            if not self._userid:
                raise web.HTTPUnauthorized(reason="User ID not found in session")
            # TODO: Checking User Permissions:
            return await fn(self, request, *args, **kwargs)

        return _wrap

    @service_auth
    async def event_publisher(self, request: web.Request) -> web.Response:
        """Event Publisher.

        REST API endpoint used to send events to the broker.
        """
        data = await request.json()
        qs = data.pop("queue_name", "navigator")
        routing_key = data.pop("routing_key", None)
        if not routing_key:
            return web.json_response(
                {
                    "status": "error",
                    "message": "routing_key is required.",
                },
                status=422,
            )
        body = data.pop("body")
        if not body:
            return web.json_response(
                {
                    "status": "error",
                    "message": "Message Body for Broker is required.",
                },
                status=422,
            )
        try:
            await self.queue_event(body, qs, routing_key, **data)
            return web.json_response(
                {
                    "status": "success",
                    "message": f"Event {qs}.{routing_key} Published Successfully.",
                }
            )
        except asyncio.QueueFull:
            return web.json_response(
                {
                    "status": "error",
                    "message": "Event queue is full. Please try again later.",
                },
                status=429,
            )

    async def _event_broker(self, worker_id: int) -> None:
        """Worker coroutine that publishes queued events to the broker.

        Implements backpressure handling by retrying failed publishes with
        exponential backoff.
        """
        while True:
            # Wait for an event to be available in the queue
            event = await self.event_queue.get()
            try:
                routing = event.pop("routing_key")
                queue_name = event.pop("queue_name")
                body = event.pop("body")
                max_retries = event.pop("max_retries", 5)
                retry_count = 0
                retry_delay = 1
                while True:
                    try:
                        await self.publish_message(
                            body=body,
                            queue_name=queue_name,
                            routing_key=routing,
                            **event,
                        )
                        self.logger.info(
                            f"Worker {worker_id} published event: {routing}"
                        )
                        break  # Exit the retry loop on success
                    except Exception as e:  # pylint: disable=broad-except
                        retry_count += 1
                        if retry_count >= max_retries:
                            self.logger.error(
                                f"Worker {worker_id} failed to publish event: {e}"
                            )
                            break  # Exit the retry loop on max retries
                        self.logger.warning(
                            f"Worker {worker_id} failed to publish event: {e}. "
                            f"Retrying in {retry_delay} seconds..."
                        )
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
            except Exception as e:  # pylint: disable=broad-except
                self.logger.error(f"Error publishing event: {e}")
            finally:
                self.event_queue.task_done()
