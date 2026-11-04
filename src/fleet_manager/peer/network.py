"""Direct peer RPC and origin forwarding. Workers remain the only lease authority."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from typing import Literal

import httpx
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from starlette.requests import ClientDisconnect

from fleet_manager.models.request import normalize_model_name
from fleet_manager.peer.discovery import MAX_BODY, Exchange
from fleet_manager.peer.identity import IdentityError
from fleet_manager.peer.models import AcquireRequest, AdmissionError, Lease

INFERENCE_PATHS = {
    "/v1/chat/completions",
    "/v1/completions",
    "/v1/embeddings",
    "/v1/responses",
    "/v1/messages",
    "/api/chat",
    "/api/generate",
    "/api/embed",
    "/api/embeddings",
}
MAX_INFERENCE_BODY = 2 * 1024 * 1024


class Execution(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: Literal[
        "/v1/chat/completions",
        "/v1/completions",
        "/v1/embeddings",
        "/v1/responses",
        "/v1/messages",
        "/api/chat",
        "/api/generate",
        "/api/embed",
        "/api/embeddings",
    ]
    token: str = Field(min_length=1, max_length=256)
    payload: dict


async def read_body(receive, limit: int) -> bytes:
    body = bytearray()
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            raise ClientDisconnect()
        body.extend(message.get("body", b""))
        if len(body) > limit:
            raise AdmissionError("request_too_large", 413)
        if not message.get("more_body", False):
            return bytes(body)


def body_receiver(body, receive):
    delivered = False

    async def replacement():
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": body, "more_body": False}
        return await receive()

    return replacement


def peer_request_id(principal: str, request_id: str) -> str:
    return f"peer/{principal}/" + hashlib.sha256(request_id.encode()).hexdigest()


async def trace_failure(local, reason: str, *, node_id: str = "", request_id: str = ""):
    await local.state.trace_store.record_trace(
        request_id=request_id or str(uuid.uuid4()),
        model="",
        original_model="",
        node_id=node_id or local.state.ledger.worker_id,
        status="failed",
        error_message=reason,
        original_format="peer",
    )


class PeerRPC:
    """A separate listener with an explicit allowlist; never exposes local owner routes."""

    def __init__(self, local):
        self.local = local

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                else:
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        if scope["type"] != "http":
            return
        started = False

        async def tracked(message):
            nonlocal started
            started |= message["type"] == "http.response.start"
            await send(message)

        try:
            await self._handle(scope, receive, tracked)
        except (AdmissionError, IdentityError, ValidationError, ValueError, sqlite3.Error) as exc:
            if isinstance(exc, AdmissionError):
                reason = exc.reason
            elif isinstance(exc, sqlite3.Error):
                reason = "peer_storage_unavailable"
            elif isinstance(exc, ValidationError):
                # Pydantic's rendered errors can contain the submitted prompt.
                reason = "invalid_peer_request"
            else:
                reason = str(exc)
            await trace_failure(self.local, reason)
            if started:
                raise
            status = (
                exc.status
                if isinstance(exc, AdmissionError)
                else (
                    503
                    if isinstance(exc, sqlite3.Error)
                    else 403
                    if isinstance(exc, IdentityError)
                    else 422
                )
            )
            content = json.dumps({"error": {"type": reason}}).encode()
            signed = {}
            if scope.get("idle_herd_verified_nonce"):
                signed = self.local.state.identity.sign_response(
                    scope["idle_herd_verified_nonce"], status, content
                )
            await Response(
                status_code=status, content=content, media_type="application/json", headers=signed
            )(scope, receive, send)

    async def _handle(self, scope, receive, send):
        path = scope["path"]
        if (
            scope["method"] != "POST"
            or path
            not in {
                "/peer/v1/exchange",
                "/peer/v1/acquire",
                "/peer/v1/execute",
                "/peer/v1/release",
                "/peer/v1/inspect",
                "/peer/v1/capacity",
                "/peer/v1/status",
                "/peer/v1/jobs/store",
                "/peer/v1/jobs/fetch",
                "/peer/v1/models/inventory",
                "/peer/v1/models/manifest",
                "/peer/v1/models/chunk",
            }
            or scope.get("query_string")
        ):
            raise AdmissionError("unknown_peer_operation", 404)
        identity = self.local.state.identity
        if identity is None or self.local.state.directory is None:
            raise AdmissionError("peer_transport_disabled", 503)
        identity.check_local()
        headers = {k.decode().lower(): v.decode("latin1") for k, v in scope["headers"]}
        if "origin" in headers:
            raise AdmissionError("browser_peer_request", 403)
        body = await read_body(
            receive, MAX_BODY if path.endswith("exchange") else MAX_INFERENCE_BODY
        )
        principal = identity.verify_request("POST", path, body, headers)
        await self.local.state.ledger.accept_peer_nonce(
            principal, headers["x-idle-nonce"], int(headers["x-idle-time"])
        )
        nonce = headers["x-idle-nonce"]
        scope["idle_herd_verified_nonce"] = nonce
        if not await self.local.state.ledger.cluster_participating() and path not in {
            "/peer/v1/inspect",
            "/peer/v1/release",
        }:
            # Existing streams finish, and their owners can inspect/release.
            # No exchange, replica admission, capacity, or new execution offline.
            raise AdmissionError("manual_offline", 503)
        if path.startswith("/peer/v1/models/"):
            from fleet_manager.peer.model_sharing import ChunkRequest
            from fleet_manager.peer.model_transport import (
                MAX_INVENTORY,
                InventoryRequest,
                ManifestRequest,
                ModelInventory,
            )

            if (await self.local.state.ledger.mode_state()).mode in {"offline", "paused"}:
                raise AdmissionError("manual_offline", 503)
            sharing = self.local.state.model_sharing
            if path.endswith("inventory"):
                inventory_request = InventoryRequest.model_validate_json(body)
                models = (
                    await sharing.inventory(principal, include_packages=inventory_request.packages)
                )[:MAX_INVENTORY]
                result = ModelInventory(models=models)
            elif path.endswith("manifest"):
                request = ManifestRequest.model_validate_json(body)
                result = await sharing.source_manifest(principal, request.digest)
            else:
                result = await sharing.source_chunk(
                    principal, ChunkRequest.model_validate_json(body)
                )
        elif path in {"/peer/v1/jobs/store", "/peer/v1/jobs/fetch"}:
            from fleet_manager.peer.replication import FetchRequest, StoreRequest

            replication = getattr(self.local.state, "job_replication", None)
            if replication is None:
                raise AdmissionError("job_replication_disabled", 503)
            if path.endswith("store"):
                result = await replication.store(principal, StoreRequest.model_validate_json(body))
            else:
                result = await replication.fetch(principal, FetchRequest.model_validate_json(body))
        elif path.endswith("exchange"):
            result = await self.local.state.directory.exchange(
                principal, Exchange.model_validate_json(body)
            )
        elif path.endswith("status"):
            from fleet_manager.peer.diagnostics import StatusQuery, local_observation

            StatusQuery.model_validate_json(body)
            result = await local_observation(self.local)
        elif path.endswith("capacity"):
            from fleet_manager.peer.capacity import CapacityQuery

            query = CapacityQuery.model_validate_json(body)
            result = await self.local.state.ledger.capacity(
                normalize_model_name(query.model), "background", query.idle_only, peer=True
            )
        elif path.endswith("acquire"):
            spec = AcquireRequest.model_validate_json(body)
            if spec.wait_ms != 0:
                # Public waiting belongs to the origin, never a remote worker queue.
                spec = spec.model_copy(update={"wait_ms": 0})
            spec = spec.model_copy(
                update={
                    "request_id": peer_request_id(principal, spec.request_id),
                    "workload": "background",
                }
            )
            result = await self.local.state.ledger.acquire(spec, peer=True)
        else:
            spec = Execution.model_validate_json(body)
            lease = await self.local.state.ledger.inspect(spec.token)
            if not lease.request_id.startswith(f"peer/{principal}/"):
                raise AdmissionError("reservation_principal_mismatch", 403)
            if path.endswith("release"):
                result = await self.local.state.ledger.release(spec.token)
            elif path.endswith("inspect"):
                result = lease
            else:
                # Local translators consume the worker-issued token through the
                # very same fenced backend transport as localhost inference.
                inner_body = json.dumps(spec.payload).encode()
                inner_scope = dict(scope)
                inner_scope.update(
                    path=spec.path,
                    raw_path=spec.path.encode(),
                    root_path="",
                    query_string=b"",
                    client=("127.0.0.1", 0),
                    server=("127.0.0.1", 11435),
                    idle_herd_remote=True,
                    headers=[
                        (b"host", b"localhost"),
                        (b"content-type", b"application/json"),
                        (b"x-idle-herd-reservation", spec.token.encode()),
                    ],
                )
                if spec.path == "/v1/messages":
                    # Peer credentials authorize execution independently of a
                    # worker's localhost client key. Never forward origin secrets.
                    key = self.local.state.settings.anthropic_api_key
                    if key:
                        inner_scope["headers"].append((b"x-api-key", key.encode()))

                async def authenticated_send(message):
                    if message["type"] == "http.response.start":
                        signed = identity.sign_response(nonce, message["status"], b"")
                        message = dict(message)
                        message["headers"] = list(message.get("headers", [])) + [
                            (k.encode(), v.encode()) for k, v in signed.items()
                        ]
                    await send(message)

                await self.local(
                    inner_scope, body_receiver(inner_body, receive), authenticated_send
                )
                return
        encoded = result.model_dump_json(
            exclude={"hostname"}
            if path == "/peer/v1/status" and headers.get("x-idle-status-hostname") != "1"
            else None
        ).encode()
        signed = identity.sign_response(nonce, 200, encoded)
        await Response(content=encoded, media_type="application/json", headers=signed)(
            scope, receive, send
        )


class OriginDispatch:
    """Use local admission first; dispatch remotely only after a direct worker grant.

    No retry occurs after execution dispatch: a broken response has unknown outcome.
    Worker policy is authoritative for background and idle-only requests.
    Explicit public reservations are resolved through the origin's durable claims.
    """

    def __init__(self, app, local):
        self.app, self.local = app, local

    async def __call__(self, scope, receive, send):
        directory = getattr(self.local.state, "directory", None)
        if (
            scope["type"] != "http"
            or scope.get("idle_herd_remote")
            or scope["method"] != "POST"
            or scope["path"] not in INFERENCE_PATHS
        ):
            return await self.app(scope, receive, send)
        token = next(
            (
                v.decode("latin1")
                for k, v in scope["headers"]
                if k.lower() == b"x-idle-herd-reservation"
            ),
            None,
        )
        if directory is None and token is None:
            return await self.app(scope, receive, send)
        started = False
        downstream_send = send

        async def tracked_send(message):
            nonlocal started
            started |= message["type"] == "http.response.start"
            await downstream_send(message)

        send = tracked_send
        try:
            if scope["path"] == "/v1/messages":
                from fleet_manager.server.routes.anthropic_compat import _check_auth

                headers = dict(scope["headers"])
                key = headers.get(b"x-api-key", b"").decode("latin1")
                denied = _check_auth(self.local.state.settings, key)
                if denied is not None:
                    await trace_failure(self.local, "authentication_error")
                    return await denied(scope, receive, send)
            if token:
                try:
                    peer, lease = await self.local.state.waiters.resolve_token(token)
                except AdmissionError:
                    # Foreign or forged capabilities are rejected by local admission.
                    return await self.app(scope, receive, send)
                if peer is None:
                    return await self.app(scope, receive, send)
                await self.local.state.ledger.require_dispatch()
                body = await read_body(receive, MAX_INFERENCE_BODY - 4096)
                try:
                    payload = json.loads(body)
                except ValueError as exc:
                    raise AdmissionError("invalid_inference_request", 422) from exc
                if not isinstance(payload, dict) or not isinstance(payload.get("model"), str):
                    raise AdmissionError("invalid_inference_request", 422)
                if normalize_model_name(payload["model"]) != lease.model:
                    raise AdmissionError("reservation_model_mismatch")
                await self._execute(peer, lease, scope, payload, receive, send, lease.request_id)
                return
            if directory is None:
                return await self.app(scope, receive, send)
            if (await self.local.state.ledger.mode_state()).mode in {"offline", "paused"}:
                return await self.app(scope, receive, send)
            body = await read_body(receive, MAX_INFERENCE_BODY - 4096)
            replacement = body_receiver(body, receive)
            try:
                payload = json.loads(body)
            except ValueError:
                return await self.app(scope, replacement, send)
            model = payload.get("model") if isinstance(payload, dict) else None
            if not isinstance(model, str) or not model or model == "auto":
                return await self.app(scope, replacement, send)
            candidates = directory.candidates(model)
            if not candidates:
                return await self.app(scope, replacement, send)
            request_id = str(uuid.uuid4())
            from fleet_manager.peer.backend import RequestBinding, binding

            current = binding.get() or RequestBinding()
            spec = AcquireRequest(
                model=model,
                request_id=request_id,
                workload=current.workload,
                idle_only=current.idle_only,
            )
            try:
                lease = await self.local.state.ledger.acquire(spec)
            except AdmissionError as local_error:
                error = local_error
            else:
                inner_scope = dict(scope)
                inner_scope["headers"] = list(scope["headers"]) + [
                    (b"x-idle-herd-reservation", lease.token.encode())
                ]
                # LocalBoundary already ran, so install the captured binding here.
                marker = binding.set(current.model_copy(update={"token": lease.token}))
                try:
                    await self.app(inner_scope, replacement, send)
                finally:
                    binding.reset(marker)
                    await self.local.state.ledger.abandon_unconsumed(lease.token)
                return
            for candidate in candidates:
                try:
                    lease = await self._acquire(candidate, spec)
                except (httpx.HTTPError, IdentityError, ValueError) as exc:
                    directory.unreachable(candidate.endpoint, type(exc).__name__)
                    continue
                except AdmissionError as exc:
                    error = exc
                    continue
                await self._execute(candidate, lease, scope, payload, receive, send, request_id)
                return
            raise error
        except (AdmissionError, httpx.HTTPError, IdentityError) as exc:
            if started:
                raise RuntimeError("Inference stream failed after response headers") from exc
            await trace_failure(self.local, str(exc))
            status = exc.status if isinstance(exc, AdmissionError) else 502
            reason = exc.reason if isinstance(exc, AdmissionError) else "peer_unavailable"
            await JSONResponse(status_code=status, content={"error": {"type": reason}})(
                scope, receive, send
            )

    async def _acquire(self, peer, spec):
        await self.local.state.ledger.require_dispatch()
        path = "/peer/v1/acquire"
        identity = self.local.state.identity
        body = spec.model_dump_json().encode()
        headers = identity.sign_request("POST", path, body)
        client = self.local.state.peer_client
        async with client.stream(
            "POST", peer.endpoint + path, content=body, headers=headers, timeout=5
        ) as resp:
            content = await read_response(resp, MAX_BODY)
            identity.verify_response(
                headers["x-idle-nonce"], resp.status_code, content, resp.headers, peer.worker_id
            )
            # Only an authenticated decline proves the worker did not grant.
            # An unsigned/lost response leaves the origin pinned to this worker.
            if resp.status_code != 200:
                raise AdmissionError("remote_admission_declined", resp.status_code)
            lease = Lease.model_validate_json(content)
            if (
                lease.worker_id != peer.worker_id
                or lease.cluster_id != peer.cluster_id
                or lease.worker_incarnation != peer.incarnation
                or lease.model != spec.model
                or lease.workload != "background"
                or lease.idle_only != spec.idle_only
                or lease.state != "reserved"
                or lease.request_id != peer_request_id(identity.worker_id, spec.request_id)
                or lease.expires_at <= time.time()
            ):
                raise IdentityError("worker lease binding mismatch")
            return lease

    async def _execute(self, peer, lease, scope, payload, receive, send, request_id):
        await self.local.state.ledger.require_dispatch()
        identity = self.local.state.identity
        path = "/peer/v1/execute"
        body = (
            Execution(path=scope["path"], token=lease.token, payload=payload)
            .model_dump_json()
            .encode()
        )
        headers = identity.sign_request("POST", path, body)
        started = False
        complete = False
        status = 502
        # Presentation only: an outgoing execution under an authenticated grant
        # is in flight even before response headers (non-streaming inference may
        # send none until it finishes). This never grants or releases capacity.
        self.local.state.outbound_running += 1
        observed_running = True
        try:
            async with self.local.state.peer_client.stream(
                "POST", peer.endpoint + path, content=body, headers=headers
            ) as resp:
                identity.verify_stream_response(headers["x-idle-nonce"], resp, peer.worker_id)
                status = resp.status_code
                if not 200 <= status < 300:
                    self.local.state.outbound_running -= 1
                    observed_running = False
                forwarded = [
                    (k.encode(), v.encode())
                    for k, v in resp.headers.items()
                    if k.lower() in {"content-type", "retry-after"}
                ]
                await send({"type": "http.response.start", "status": status, "headers": forwarded})
                started = True
                async for chunk in resp.aiter_raw():
                    await send({"type": "http.response.body", "body": chunk, "more_body": True})
                await send({"type": "http.response.body", "body": b"", "more_body": False})
                complete = True
        except (httpx.HTTPError, IdentityError):
            if not started:
                raise
            # Closing an incomplete stream conveys failure without fabricating a
            # protocol terminal or retrying an execution of uncertain outcome.
            raise RuntimeError("Remote stream ended with an unknown execution outcome") from None
        finally:
            if observed_running:
                self.local.state.outbound_running -= 1
            await self.local.state.trace_store.record_trace(
                request_id=request_id,
                model=normalize_model_name(payload["model"]),
                original_model=payload["model"],
                node_id=peer.worker_id,
                status="completed" if complete and status < 400 else "failed",
                error_message=None if complete and status < 400 else "remote_execution_incomplete",
                original_format="peer",
            )


async def read_response(response, limit):
    content = bytearray()
    async for chunk in response.aiter_bytes():
        content.extend(chunk)
        if len(content) > limit:
            raise IdentityError("peer response too large")
    return bytes(content)
