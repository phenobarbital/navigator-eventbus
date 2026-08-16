"""QueueAPI — the SQS-style HTTP surface over :class:`QueueStore`.

Producers ``POST`` an :class:`~navigator_eventbus.ingress_models.IngressEnvelope`;
consumers ``receive`` messages with receipt handles, then ``delete`` or change
their visibility. Consumers need nothing but HTTP, so they can live in any
language and outside the Redis network.

Everything is ``POST`` except ``describe`` and ``list``: receive, delete and
visibility all mutate the pending-entries list, so they are neither safe nor
idempotent, and a ``GET`` with a body is not viable.

On status codes, one deliberate divergence from
:mod:`navigator_eventbus.hooks.webhook`: that module answers ``200`` to
deliveries it ignores, because a third-party provider treats non-2xx as a
failed delivery and eventually disables the webhook. A queue client is our own
SDK-style caller, so genuine errors here get real status codes.
"""
from __future__ import annotations

import asyncio
import hmac
import json
from typing import TYPE_CHECKING, Any, Optional

from aiohttp import web
from navconfig import config as nav_config
from pydantic import ValidationError

from navigator_eventbus.hooks.base import BaseHook
from navigator_eventbus.hooks.models import HookType
from navigator_eventbus.hooks.webhook.receiver import BodyTooLarge, json_error, read_capped
from navigator_eventbus.ingress_models import IngressEnvelope
from navigator_eventbus.queues.config import (
    MAX_WAIT_TIME_SECONDS,
    QueueConfig,
    QueueRegistry,
)
from navigator_eventbus.queues.models import (
    BatchFailure,
    BatchSuccess,
    ChangeVisibilityRequest,
    DeleteRequest,
    ReceiveRequest,
    SendBatchRequest,
)
from navigator_eventbus.queues.receipts import Receipt, ReceiptError
from navigator_eventbus.queues.store import QueueStore, StaleReceipt

if TYPE_CHECKING:  # pragma: no cover
    from navigator_eventbus.evb import EventBus

__all__ = ("QueueAPI",)

#: Roles a caller can hold. Blast radius differs sharply: a leaked producer
#: token lets an attacker inject; a consumer token lets them drain and delete;
#: an admin token lets them purge irreversibly.
_ROLES = ("producer", "consumer", "admin")


class QueueAPI(BaseHook):
    """aiohttp routes for the pull-queue plane.

    Args:
        registry: Declared queues.
        store: The Redis Streams engine.
        base_path: Mount point. Public so operators can exclude it from
            middleware if needed.
        auth_token: Shared fallback token for every queue and role.
        bus: Optional bus, used only by queues with ``mirror_to_bus``.
        dlq: Optional ``DLQHandler``, enabling the DLQ inspection route.
        max_inflight_receives: Concurrent long-polls before returning 429.
            A blocking ``XREADGROUP`` holds a pool connection for its whole
            duration, so this bounds socket usage.
        expose_admin_routes: Enable purge / list / DLQ.
        **kwargs: Forwarded to :class:`~navigator_eventbus.hooks.base.BaseHook`.
    """

    hook_type: str = HookType.QUEUE

    def __init__(
        self,
        registry: QueueRegistry,
        store: QueueStore,
        *,
        base_path: str = "/api/v1/queues",
        auth_token: Optional[str] = None,
        bus: Optional["EventBus"] = None,
        dlq: Any = None,
        max_inflight_receives: int = 64,
        expose_admin_routes: bool = False,
        name: str = "queues",
        **kwargs: Any,
    ) -> None:
        super().__init__(name=name, **kwargs)
        self._registry = registry
        self._store = store
        self.base_path = "/" + base_path.strip("/")
        self._bus = bus
        self._dlq = dlq
        self._expose_admin = expose_admin_routes
        self._auth_token = (
            auth_token
            if auth_token is not None
            else nav_config.get("BUS_QUEUE_TOKEN", fallback=None)
            or nav_config.get("BUS_INGRESS_TOKEN", fallback=None)
        )
        self._semaphore: Optional[asyncio.Semaphore] = (
            asyncio.Semaphore(max_inflight_receives)
            if max_inflight_receives > 0
            else None
        )

    # ------------------------------------------------------------------
    # BaseHook contract
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Materialise every declared queue and warn about weak configuration."""
        if not self._auth_token and not any(
            queue.producer_tokens or queue.consumer_tokens or queue.admin_tokens
            for queue in self._registry.queues
        ):
            self.logger.warning(
                "QueueAPI '%s' has NO tokens configured (BUS_QUEUE_TOKEN / "
                "BUS_INGRESS_TOKEN / per-queue tokens) — every request will be "
                "refused.",
                self.name,
            )
        await self._store.ensure_all()
        self.logger.info(
            "QueueAPI '%s' ready at %s with %d queue(s): %s",
            self.name,
            self.base_path,
            len(self._registry.queues),
            ", ".join(self._registry.names) or "-",
        )

    async def stop(self) -> None:
        """Nothing to release — the Redis client is owned by the caller."""
        self.logger.info("QueueAPI '%s' stopped", self.name)

    def setup_routes(self, app: Any) -> None:
        """Register every queue route.

        Literal and more-specific paths are registered before the catch-all
        ``{queue}`` resources so aiohttp's in-order dynamic matching resolves
        them correctly.
        """
        base = self.base_path
        router = app.router
        router.add_post(f"{base}/{{queue}}/messages/batch", self._handle_send_batch)
        router.add_post(f"{base}/{{queue}}/messages/receive", self._handle_receive)
        router.add_post(f"{base}/{{queue}}/messages/delete", self._handle_delete)
        router.add_post(f"{base}/{{queue}}/messages/visibility", self._handle_visibility)
        router.add_post(f"{base}/{{queue}}/messages", self._handle_send)
        if self._expose_admin:
            router.add_post(f"{base}/{{queue}}/purge", self._handle_purge)
            router.add_get(f"{base}/{{queue}}/dlq", self._handle_dlq)
            router.add_get(base, self._handle_list)
        router.add_get(f"{base}/{{queue}}", self._handle_describe)
        self.logger.info("Queue API routes registered under %s", base)

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _presented_token(self, request: web.Request) -> Optional[str]:
        """Extract the bearer token, same three carriers as WebSocketIngress."""
        authorization = request.headers.get("Authorization", "")
        if authorization.startswith("Bearer "):
            return authorization[7:]
        return request.headers.get("X-API-Key") or request.query.get("token")

    def _authorized(
        self, request: web.Request, config: QueueConfig, role: str
    ) -> bool:
        """Whether the caller may act in *role* on *config*.

        Resolution is most-specific-first: the queue's own tokens for that
        role, then the shared token. **Nothing configured means refuse** —
        the same posture as ``WebSocketIngress``, so operators carry one
        mental model across the package.
        """
        presented = self._presented_token(request)
        if not presented:
            return False
        candidates: list[str] = list(getattr(config, f"{role}_tokens", ()) or ())
        if self._auth_token:
            candidates.append(self._auth_token)
        return any(
            hmac.compare_digest(presented.encode(), candidate.encode())
            for candidate in candidates
            if candidate
        )

    def _resolve(
        self, request: web.Request, role: str
    ) -> tuple[Optional[QueueConfig], Optional[web.Response]]:
        """Look up the queue and authorize, or return the error response."""
        name = request.match_info.get("queue", "")
        config = self._registry.get(name)
        if config is None:
            return None, json_error(404, "unknown_queue", queue=name)
        if not self._presented_token(request):
            return None, json_error(401, "unauthorized")
        if not self._authorized(request, config, role):
            # A valid token in the wrong role is 403; no token at all is 401.
            if any(
                self._authorized(request, config, other)
                for other in _ROLES
                if other != role
            ):
                return None, json_error(403, "forbidden", required_role=role)
            return None, json_error(401, "unauthorized")
        return config, None

    # ------------------------------------------------------------------
    # Body helpers
    # ------------------------------------------------------------------

    async def _read_json(
        self, request: web.Request, config: QueueConfig
    ) -> tuple[Optional[Any], Optional[web.Response]]:
        """Read and parse a capped JSON body."""
        try:
            raw = await read_capped(request, config.max_body_bytes)
        except BodyTooLarge:
            return None, json_error(
                413, "payload_too_large", limit=config.max_body_bytes
            )
        try:
            return json.loads(raw.decode("utf-8")), None
        except Exception:  # noqa: BLE001
            return None, json_error(400, "invalid_payload", detail="body is not JSON")

    @staticmethod
    def _validation_error(exc: ValidationError) -> web.Response:
        """Turn a pydantic failure into a 400 without echoing the body."""
        errors = [
            {"field": ".".join(str(p) for p in err["loc"]), "error": err["msg"]}
            for err in exc.errors()[:5]
        ]
        return json_error(400, "invalid_payload", errors=errors)

    def _decode_handle(
        self, handle: str, config: QueueConfig, group: str
    ) -> tuple[Optional[Receipt], Optional[str]]:
        """Verify a receipt handle, returning ``(receipt, error_slug)``.

        Delegated to the store so minting and verification always share one
        clock — an injected test clock would otherwise judge every handle
        expired against wall time.
        """
        try:
            receipt = self._store.decode_receipt(
                handle, queue=config.name, group=group
            )
        except ReceiptError as exc:
            # Every non-expiry failure collapses to one slug so a probing
            # caller cannot learn WHY a handle was rejected.
            if exc.reason == "expired":
                return None, "expired_receipt_handle"
            return None, "invalid_receipt_handle"
        return receipt, None

    # ------------------------------------------------------------------
    # Producer routes
    # ------------------------------------------------------------------

    async def _handle_send(self, request: web.Request) -> web.Response:
        """Append one message. The body **is** an ``IngressEnvelope``."""
        config, error = self._resolve(request, "producer")
        if error is not None:
            return error
        assert config is not None
        data, error = await self._read_json(request, config)
        if error is not None:
            return error
        try:
            ingress = IngressEnvelope.model_validate(data)
        except ValidationError as exc:
            return self._validation_error(exc)

        try:
            message_id = await self._store.send(config, ingress.to_envelope())
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Queue %s: send failed: %s", config.name, exc)
            return json_error(503, "unavailable", headers={"Retry-After": "5"})

        await self._mirror(config, ingress, message_id)
        return web.json_response(
            {"message_id": message_id, "event_id": ingress.event_id}, status=201
        )

    async def _handle_send_batch(self, request: web.Request) -> web.Response:
        """Append up to ten messages, reporting per-entry outcomes."""
        config, error = self._resolve(request, "producer")
        if error is not None:
            return error
        assert config is not None
        data, error = await self._read_json(request, config)
        if error is not None:
            return error
        try:
            batch = SendBatchRequest.model_validate(data)
        except ValidationError as exc:
            return self._validation_error(exc)

        successful: list[BatchSuccess] = []
        failed: list[BatchFailure] = []
        for entry in batch.entries:
            try:
                message_id = await self._store.send(
                    config, entry.message.to_envelope()
                )
                successful.append(BatchSuccess(id=entry.id, message_id=message_id))
                await self._mirror(config, entry.message, message_id)
            except Exception as exc:  # noqa: BLE001 — one bad entry, not the batch
                failed.append(
                    BatchFailure(id=entry.id, code="unavailable", message=str(exc))
                )
        return self._batch_response(successful, failed)

    async def _mirror(
        self, config: QueueConfig, ingress: IngressEnvelope, message_id: str
    ) -> None:
        """Optionally also emit onto the bus.

        This is a **dual write and is not atomic**: the queue stream is the
        source of truth, and a mirror failure is logged rather than failing
        the send. A crash between the two leaves the message queued but not
        on the bus.
        """
        if not config.mirror_to_bus or self._bus is None:
            return
        prefix = config.effective_topic_prefix()
        topic = f"{prefix}.{ingress.topic}" if prefix else ingress.topic
        try:
            await self._bus.emit(
                topic,
                ingress.payload,
                event_id=ingress.event_id,
                timestamp=ingress.timestamp,
                source=ingress.source or f"queue:{config.name}",
                priority=ingress.priority,
                correlation_id=ingress.correlation_id,
                metadata={
                    **ingress.metadata,
                    "queue": config.name,
                    "message_id": message_id,
                },
                severity=ingress.severity,
            )
        except Exception as exc:  # noqa: BLE001 — never fail the send
            self.logger.warning(
                "Queue %s: mirror_to_bus failed for %s: %s",
                config.name,
                message_id,
                exc,
            )

    # ------------------------------------------------------------------
    # Consumer routes
    # ------------------------------------------------------------------

    async def _handle_receive(self, request: web.Request) -> web.Response:
        """Lease messages. An empty result is a 200, never a 204."""
        config, error = self._resolve(request, "consumer")
        if error is not None:
            return error
        assert config is not None
        data, error = await self._read_json(request, config)
        if error is not None:
            return error
        try:
            options = ReceiveRequest.model_validate(data)
        except ValidationError as exc:
            return self._validation_error(exc)
        if config.group(options.group) is None:
            return json_error(400, "invalid_parameter", detail="unknown group")

        visibility_ms: Optional[int] = None
        if options.visibility_timeout_seconds is not None:
            visibility_ms = options.visibility_timeout_seconds * 1000
            if visibility_ms > config.max_visibility_timeout_ms:
                return json_error(
                    400,
                    "invalid_parameter",
                    detail="visibility_timeout_seconds exceeds the queue maximum",
                    maximum=config.max_visibility_timeout_ms // 1000,
                )

        # Clamp rather than reject, matching SQS.
        wait_ms = min(options.wait_time_seconds, MAX_WAIT_TIME_SECONDS) * 1000

        if self._semaphore is not None and self._semaphore.locked():
            return json_error(429, "busy", headers={"Retry-After": "1"})
        try:
            if self._semaphore is None:
                messages = await self._receive(config, options, wait_ms, visibility_ms)
            else:
                async with self._semaphore:
                    messages = await self._receive(
                        config, options, wait_ms, visibility_ms
                    )
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Queue %s: receive failed: %s", config.name, exc)
            return json_error(503, "unavailable", headers={"Retry-After": "5"})
        return web.json_response({"messages": messages}, status=200)

    async def _receive(
        self,
        config: QueueConfig,
        options: ReceiveRequest,
        wait_ms: int,
        visibility_ms: Optional[int],
    ) -> list[dict[str, Any]]:
        """Run the store receive and shape the wire response."""
        delivered = await self._store.receive(
            config,
            options.group,
            max_messages=options.max_messages,
            wait_ms=wait_ms,
            visibility_ms=visibility_ms,
        )
        return [
            {
                "message_id": message.message_id,
                "receipt_handle": message.handle,
                "body": message.envelope.to_dict(),
                "attributes": {
                    "approximate_receive_count": message.delivered,
                    "sent_at": message.envelope.timestamp.isoformat(),
                    "visibility_expires_at": _iso_from_ms(
                        message.receipt.expires_at_ms
                    ),
                },
            }
            for message in delivered
        ]

    async def _handle_delete(self, request: web.Request) -> web.Response:
        """Acknowledge one or more leased messages."""
        config, error = self._resolve(request, "consumer")
        if error is not None:
            return error
        assert config is not None
        data, error = await self._read_json(request, config)
        if error is not None:
            return error
        try:
            batch = DeleteRequest.model_validate(data)
        except ValidationError as exc:
            return self._validation_error(exc)

        successful: list[BatchSuccess] = []
        failed: list[BatchFailure] = []
        for entry in batch.entries:
            receipt, slug = self._decode_handle(
                entry.receipt_handle, config, batch.group
            )
            if receipt is None:
                failed.append(
                    BatchFailure(id=entry.id, code=slug or "invalid_receipt_handle",
                                 message="receipt handle rejected")
                )
                continue
            try:
                await self._store.delete(config, receipt)
                successful.append(BatchSuccess(id=entry.id, message_id=receipt.message_id))
            except StaleReceipt as exc:
                failed.append(
                    BatchFailure(id=entry.id, code="stale_receipt_handle", message=str(exc))
                )
            except Exception as exc:  # noqa: BLE001
                failed.append(
                    BatchFailure(id=entry.id, code="unavailable", message=str(exc))
                )
        return self._batch_response(successful, failed, single_ok_status=200)

    async def _handle_visibility(self, request: web.Request) -> web.Response:
        """Extend, shorten, or release (nack with ``0``) a message's lease."""
        config, error = self._resolve(request, "consumer")
        if error is not None:
            return error
        assert config is not None
        data, error = await self._read_json(request, config)
        if error is not None:
            return error
        try:
            batch = ChangeVisibilityRequest.model_validate(data)
        except ValidationError as exc:
            return self._validation_error(exc)

        successful: list[BatchSuccess] = []
        failed: list[BatchFailure] = []
        for entry in batch.entries:
            visibility_ms = entry.visibility_timeout_seconds * 1000
            if visibility_ms > config.max_visibility_timeout_ms:
                failed.append(
                    BatchFailure(
                        id=entry.id,
                        code="invalid_parameter",
                        message="visibility_timeout_seconds exceeds the queue maximum",
                    )
                )
                continue
            receipt, slug = self._decode_handle(
                entry.receipt_handle, config, batch.group
            )
            if receipt is None:
                failed.append(
                    BatchFailure(id=entry.id, code=slug or "invalid_receipt_handle",
                                 message="receipt handle rejected")
                )
                continue
            try:
                await self._store.change_visibility(config, receipt, visibility_ms)
                successful.append(BatchSuccess(id=entry.id, message_id=receipt.message_id))
            except Exception as exc:  # noqa: BLE001
                failed.append(
                    BatchFailure(id=entry.id, code="unavailable", message=str(exc))
                )
        return self._batch_response(successful, failed, single_ok_status=200)

    def _batch_response(
        self,
        successful: list[BatchSuccess],
        failed: list[BatchFailure],
        *,
        single_ok_status: int = 200,
    ) -> web.Response:
        """200 when everything worked, 207 when some entries failed."""
        body: dict[str, Any] = {
            "successful": [item.model_dump() for item in successful],
            "failed": [item.model_dump() for item in failed],
        }
        if failed:
            body["status"] = "partial"
            return web.json_response(body, status=207)
        return web.json_response(body, status=single_ok_status)

    # ------------------------------------------------------------------
    # Introspection and admin
    # ------------------------------------------------------------------

    async def _handle_describe(self, request: web.Request) -> web.Response:
        """Report approximate depth and in-flight counters."""
        config, error = self._resolve(request, "consumer")
        if error is not None:
            return error
        assert config is not None
        group = request.query.get("group")
        if group is not None and config.group(group) is None:
            return json_error(400, "invalid_parameter", detail="unknown group")
        try:
            return web.json_response(await self._store.describe(config, group))
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Queue %s: describe failed: %s", config.name, exc)
            return json_error(503, "unavailable", headers={"Retry-After": "5"})

    async def _handle_list(self, request: web.Request) -> web.Response:
        """List declared queues. Admin only; never lists tokens."""
        for config in self._registry.queues:
            if self._authorized(request, config, "admin"):
                break
        else:
            return json_error(401, "unauthorized")
        return web.json_response(
            {
                "queues": [
                    {
                        "name": config.name,
                        "stream": self._registry.stream_for(config.name),
                        "groups": [group.name for group in config.groups],
                        "max_receives": config.max_receives,
                    }
                    for config in self._registry.queues
                ]
            }
        )

    async def _handle_purge(self, request: web.Request) -> web.Response:
        """Discard every message. Irreversible, hence a separate admin role."""
        config, error = self._resolve(request, "admin")
        if error is not None:
            return error
        assert config is not None
        try:
            await self._store.purge(config)
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Queue %s: purge failed: %s", config.name, exc)
            return json_error(503, "unavailable", headers={"Retry-After": "5"})
        if self._bus is not None:
            try:
                await self._bus.emit(
                    "bus.queue_purged", {"queue": config.name}, source=self.name
                )
            except Exception:  # noqa: BLE001 — observability must not throw
                pass
        return web.json_response({"status": "purged", "queue": config.name})

    async def _handle_dlq(self, request: web.Request) -> web.Response:
        """Read-only projection of DLQ rows for this queue.

        Replay is deliberately **not** exposed: it is an operator action with
        no idempotency guard.
        """
        config, error = self._resolve(request, "admin")
        if error is not None:
            return error
        assert config is not None
        if self._dlq is None:
            return json_error(
                501, "not_implemented", detail="no DLQHandler is wired"
            )
        try:
            rows = await self._dlq_rows(config)
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Queue %s: DLQ read failed: %s", config.name, exc)
            return json_error(503, "unavailable", headers={"Retry-After": "5"})
        return web.json_response({"queue": config.name, "messages": rows})

    async def _dlq_rows(self, config: QueueConfig) -> list[dict[str, Any]]:
        """Fetch DLQ rows tagged with this queue, if the handler allows it."""
        fetch = getattr(self._dlq, "list_for_queue", None)
        if fetch is None:
            return []
        return list(await fetch(config.name))


def _iso_from_ms(epoch_ms: int) -> str:
    """Render an epoch-milliseconds instant as ISO-8601 UTC."""
    from datetime import datetime, timezone

    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()
