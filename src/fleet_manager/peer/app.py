"""Native local ingress and durable worker authority, with opt-in authenticated peers."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit

import aiosqlite
import httpx
import psutil
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from fleet_manager.models.config import ServerSettings
from fleet_manager.models.node import (
    CpuMetrics,
    HeartbeatPayload,
    LoadedModel,
    MemoryMetrics,
    ModelTagMeta,
    OllamaMetrics,
)
from fleet_manager.models.request import RequestFormat, normalize_model_name
from fleet_manager.peer.backend import AdmissionTransport, OllamaBackend, RequestBinding, binding
from fleet_manager.peer.capacity import ClusterCapacity, cluster_capacity
from fleet_manager.peer.local_ledger import LocalLedger
from fleet_manager.peer.models import AcquireRequest, AdmissionError, Lease, PeerSettings, Workload
from fleet_manager.peer.network import (
    OriginDispatch,
    PeerRPC,
    body_receiver,
    read_body,
    trace_failure,
)
from fleet_manager.peer.policy import IdlePolicy
from fleet_manager.peer.sensors import Sensor, native_sensor
from fleet_manager.peer.stores import PeerLatencyStore, PeerTraceStore
from fleet_manager.peer.waiters import OriginWaiters
from fleet_manager.server.queue_manager import QueueManager
from fleet_manager.server.registry import NodeRegistry
from fleet_manager.server.scorer import ScoringEngine
from fleet_manager.server.session_affinity import SessionAffinityTracker
from fleet_manager.server.streaming import StreamingProxy, openai_stream_error

logger = logging.getLogger(__name__)


class LocalStreamingProxy(StreamingProxy):
    def __init__(self, *args, ledger, inner_transport=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.ledger = ledger
        self.inner_transport = inner_transport

    def _get_client(self, node_id):
        if node_id != self.ledger.worker_id:
            raise ValueError("Worker execution must use its own local backend")
        if node_id not in self._clients:
            self._clients[node_id] = httpx.AsyncClient(
                base_url=self.ledger.settings.backend_url,
                transport=AdmissionTransport(self.ledger, self.inner_transport),
                trust_env=False,
                timeout=httpx.Timeout(600, connect=5),
            )
        return self._clients[node_id]

    def make_process_fn(self, *args, **kwargs):
        process = super().make_process_fn(*args, **kwargs)
        # Queue workers are reused across requests. Capture the originating
        # request here; a task's inherited ContextVar may belong to an old caller.
        captured = binding.get()

        async def scoped(entry):
            marker = binding.set(captured)
            try:
                async for chunk in process(entry):
                    yield chunk
            except httpx.HTTPError as exc:
                if not entry.request.stream:
                    raise
                # HTTP headers have already been sent for streaming routes.
                # Preserve the wire protocol's in-band failure convention.
                detail = str(exc)
                if entry.request.original_format == RequestFormat.OPENAI:
                    yield openai_stream_error(detail)
                else:
                    yield json.dumps({"error": detail}) + "\n"
            finally:
                binding.reset(marker)

        return scoped


async def refresh_local(app: FastAPI):
    backend = app.state.backend
    ledger = app.state.ledger
    try:
        tags, running = await backend.inventory()
    except AdmissionError:
        app.state.registry.handle_drain(ledger.worker_id)
        return
    mem = await asyncio.to_thread(psutil.virtual_memory)
    payload = HeartbeatPayload(
        node_id=ledger.worker_id,
        ollama_host=backend.settings.backend_url,
        cpu=CpuMetrics(cores_physical=1, utilization_pct=0),
        memory=MemoryMetrics(
            total_gb=mem.total / 1e9,
            used_gb=mem.used / 1e9,
            available_gb=mem.available / 1e9,
        ),
        ollama=OllamaMetrics(
            models_available=[normalize_model_name(m.name) for m in tags.models],
            models_available_sizes={
                normalize_model_name(m.name): m.size / 1e9 for m in tags.models
            },
            models_available_meta={
                normalize_model_name(m.name): ModelTagMeta(
                    modified_at=m.modified_at,
                    digest=m.digest,
                    capabilities=m.capabilities,
                    **{
                        k: v
                        for k, v in m.details.items()
                        if k in ModelTagMeta.model_fields
                        and k not in {"modified_at", "digest", "capabilities"}
                        and v is not None
                    },
                )
                for m in tags.models
            },
            models_loaded=[
                LoadedModel(
                    name=normalize_model_name(m.name),
                    size_gb=m.size / 1e9,
                    context_length=m.context_length,
                    digest=m.digest,
                )
                for m in running.models
            ],
        ),
    )
    node = await app.state.registry.update_from_heartbeat(payload)
    # The legacy registry may rewrite localhost using a heartbeat's LAN address.
    # This peer never accepts remote heartbeats and always uses its local adapter.
    node.ollama_base_url = backend.settings.backend_url


class LocalBoundary:
    """Loopback and browser-origin boundary for local inference and scoped controls."""

    def __init__(self, app, local):
        self.app = app
        self.local = local

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        headers = {k.lower(): v.decode("latin1") for k, v in scope["headers"]}
        host = urlsplit("http://" + headers.get(b"host", "")).hostname
        client = scope.get("client")
        if (
            host not in {"localhost", "127.0.0.1", "::1"}
            or (client and client[0] not in {"127.0.0.1", "::1"})
            or b"origin" in headers
            or headers.get(b"sec-fetch-site") == "cross-site"
        ):
            return await JSONResponse(
                status_code=403, content={"error": {"type": "local_clients_only"}}
            )(scope, receive, send)
        if (
            (scope["path"] == "/v1/jobs" and scope["method"] == "POST")
            or (scope["path"].startswith("/v1/models/") and scope["method"] in {"POST", "PUT"})
            or (scope["path"] == "/v1/node/coordination/metadata" and scope["method"] == "PUT")
        ):
            try:
                limit = 4096 if scope["path"] == "/v1/node/coordination/metadata" else 256 * 1024
                body = await read_body(receive, limit)
            except AdmissionError:
                if scope["path"] == "/v1/jobs":
                    reason = "job_request_too_large"
                elif scope["path"] == "/v1/node/coordination/metadata":
                    reason = "admin_request_too_large"
                else:
                    reason = "model_request_too_large"
                await trace_failure(self.local, reason)
                return await JSONResponse(status_code=413, content={"error": {"type": reason}})(
                    scope, receive, send
                )
            receive = body_receiver(body, receive)
        token = headers.get(b"x-idle-herd-reservation")
        workload = headers.get(b"x-idle-herd-workload", "interactive")
        idle_only = headers.get(b"x-idle-herd-idle-only", "false")
        if workload not in {"interactive", "background"} or idle_only not in {"true", "false"}:
            await trace_failure(self.local, "invalid_workload_policy")
            return await JSONResponse(
                status_code=422, content={"error": {"type": "invalid_workload_policy"}}
            )(scope, receive, send)
        marker = binding.set(
            RequestBinding(token=token, workload=workload, idle_only=idle_only == "true")
        )

        async def private_send(message):
            if message["type"] == "http.response.start":
                message = dict(message)
                message["headers"] = [
                    (key, value)
                    for key, value in message.get("headers", [])
                    if key.lower() != b"cache-control"
                ] + [(b"cache-control", b"no-store")]
            await send(message)

        try:
            await self.app(
                scope,
                receive,
                private_send if scope["path"].startswith(("/v1/node/", "/v1/models/")) else send,
            )
        finally:
            binding.reset(marker)


def create_peer_app(
    settings: PeerSettings | None = None,
    *,
    backend_transport: httpx.AsyncBaseTransport | None = None,
    sensor: Sensor | None = None,
) -> FastAPI:
    settings = settings or PeerSettings()

    @asynccontextmanager
    async def lifespan(app):
        async with AsyncExitStack() as stack:
            backend = OllamaBackend(settings, backend_transport)
            stack.push_async_callback(backend.close)
            policy = IdlePolicy(settings, sensor or native_sensor(settings.sensor_helper))
            ledger = LocalLedger(settings, backend.can_run, policy)
            await ledger.open()
            stack.push_async_callback(ledger.close)
            data_dir = str(Path(settings.data_dir).expanduser())
            from fleet_manager.local_control.auth import load_or_create_credentials

            app.state.control_credentials = await asyncio.to_thread(
                load_or_create_credentials, data_dir
            )
            server_settings = ServerSettings(
                data_dir=data_dir,
                host="127.0.0.1",
                port=11435,
                max_retries=0,
                FLEET_TELEMETRY=False,
                mlx_enabled=False,
                context_compaction_enabled=False,
                dynamic_num_ctx=False,
            )
            registry = NodeRegistry(server_settings)
            latency = PeerLatencyStore(data_dir)
            await latency.initialize()
            stack.push_async_callback(latency.close)
            traces = PeerTraceStore(data_dir)
            await traces.initialize()
            stack.push_async_callback(traces.close)
            sessions = SessionAffinityTracker()
            queue = QueueManager(registry, server_settings)
            proxy = LocalStreamingProxy(
                registry,
                latency,
                traces,
                server_settings,
                ledger=ledger,
                inner_transport=backend_transport,
            )
            stack.push_async_callback(proxy.close)
            stack.push_async_callback(queue.shutdown)
            app.state.settings = server_settings
            app.state.backend = backend
            app.state.ledger = ledger
            app.state.idle_policy = policy
            app.state.registry = registry
            app.state.latency_store = latency
            app.state.trace_store = traces
            app.state.sessions = sessions
            app.state.queue_mgr = queue
            app.state.streaming_proxy = proxy
            app.state.scorer = ScoringEngine(server_settings, registry, latency, sessions)
            app.state.mlx_proxy = None
            app.state.context_compactor = None
            app.state.identity = None
            app.state.directory = None
            app.state.peer_error = None
            app.state.peer_task = None
            from fleet_manager.peer.coordination import AdminCoordination

            app.state.coordination = AdminCoordination(settings)
            await app.state.coordination.open()
            stack.push_async_callback(app.state.coordination.close)
            await refresh_local(app)

            peer_task = None
            if settings.peer_endpoint:
                from fleet_manager.peer.discovery import (
                    ModelCapability,
                    PeerDescriptor,
                    PeerDirectory,
                )
                from fleet_manager.peer.identity import IdentityError, PeerIdentity

                try:
                    identity = await PeerIdentity.open(
                        data_dir, ledger.worker_id, ledger.cluster_id
                    )

                    async def advertised_inventory():
                        try:
                            tags, _ = await backend.inventory()
                            models = [
                                ModelCapability(
                                    name=normalize_model_name(m.name), capabilities=m.capabilities
                                )
                                for m in tags.models[:256]
                            ]
                        except AdmissionError:
                            models = []
                        return PeerDescriptor(
                            cluster_id=ledger.cluster_id,
                            worker_id=ledger.worker_id,
                            incarnation=ledger.incarnation,
                            endpoint=settings.peer_endpoint,
                            models=models,
                        )

                    directory = await asyncio.to_thread(
                        PeerDirectory,
                        identity,
                        settings.peer_endpoint,
                        settings.seeds,
                        advertised_inventory,
                        data_dir,
                        ttl=settings.peer_ttl,
                        interval=settings.peer_interval,
                    )
                    stack.push_async_callback(directory.close)
                    await directory.open()
                    directory.participating = ledger.cluster_participating
                    app.state.identity = identity
                    app.state.directory = directory
                    app.state.peer_client = httpx.AsyncClient(
                        verify=await asyncio.to_thread(identity.client_ssl_context),
                        trust_env=False,
                        follow_redirects=False,
                        timeout=httpx.Timeout(600, connect=2),
                    )
                    stack.push_async_callback(app.state.peer_client.aclose)
                    peer_task = asyncio.create_task(directory.run())
                    app.state.peer_task = peer_task
                except (IdentityError, OSError, ValueError) as exc:
                    # Enrollment/credential failures withdraw participation while
                    # preserving eligible local inference without any peer call.
                    app.state.peer_error = str(exc)
                    logger.error("Peer transport unavailable: %s", exc)

            from fleet_manager.peer.model_sharing import ModelSharing
            from fleet_manager.peer.model_transport import ModelTransport

            async def confirm_model_load(manifest):
                # The local runtime owns all execution, including a load probe.
                await refresh_local(app)
                lease = await ledger.acquire(AcquireRequest(model=manifest.model_id, wait_ms=0))
                try:
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:11435"
                    ) as client:
                        response = await client.post(
                            "/api/generate",
                            headers={"x-idle-herd-reservation": lease.token},
                            json={
                                "model": manifest.model_id,
                                "prompt": "",
                                "stream": False,
                                "options": {"num_predict": 1},
                            },
                            timeout=120,
                        )
                    return response.status_code == 200 and response.json().get("done") is True
                finally:
                    await ledger.abandon_unconsumed(lease.token)

            sharing = ModelSharing(
                Path(data_dir) / "model-sharing",
                settings.backend_url,
                transport=backend_transport,
                load_confirm=confirm_model_load,
                bytes_per_second=settings.model_transfer_bytes_per_second,
                max_bytes=settings.max_model_cache_bytes,
                reserved_bytes=settings.model_reserved_disk_bytes,
                reserve_memory_bytes=int(settings.reserve_memory_gb * 1e9),
            )
            await sharing.open()
            stack.push_async_callback(sharing.close)
            app.state.model_sharing = sharing
            app.state.model_transport = ModelTransport(app)

            waiters = OriginWaiters(app)
            await waiters.open()
            stack.push_async_callback(waiters.close)
            app.state.waiters = waiters

            from fleet_manager.peer.job_service import JobService
            from fleet_manager.peer.jobs import JobJournal
            from fleet_manager.peer.replication import JobReplication

            journal = JobJournal(Path(data_dir) / "jobs.sqlite", worker_id=ledger.worker_id)
            await journal.open()
            stack.push_async_callback(journal.close)
            app.state.job_journal = journal
            app.state.job_replication = JobReplication(app)
            jobs = JobService(app)
            app.state.jobs = jobs
            await jobs.open()
            stack.push_async_callback(jobs.close)

            async def inventory_loop():
                while True:
                    await asyncio.sleep(2)
                    try:
                        await refresh_local(app)
                        waiters.changed.set()
                    except Exception:
                        logger.exception("Local inventory refresh failed")
                        registry.handle_drain(ledger.worker_id)

            task = asyncio.create_task(inventory_loop())
            sensor_task = asyncio.create_task(policy.run())
            try:
                yield
            finally:
                # Transfer load probes use waiters, traces and the ledger. Stop them
                # before the exit stack closes those independently owned services.
                await sharing.close()
                sensor_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await sensor_task
                if peer_task:
                    peer_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await peer_task
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    app = FastAPI(title="Idle-Herd local peer", lifespan=lifespan)
    app.add_middleware(OriginDispatch, local=app)
    app.add_middleware(LocalBoundary, local=app)

    # Reuse every inference wire translator. No router lifecycle, mDNS,
    # heartbeat endpoint, remote registry writes, telemetry or preload work.
    from fleet_manager.server.routes import (
        anthropic_compat,
        embedding_compat,
        image_compat,
        ollama_compat,
        openai_compat,
        responses_compat,
        transcription_compat,
    )

    for module in (
        openai_compat,
        ollama_compat,
        anthropic_compat,
        responses_compat,
        image_compat,
        transcription_compat,
        embedding_compat,
    ):
        app.include_router(module.router)

    from fleet_manager.peer.job_service import router as jobs_router

    app.include_router(jobs_router)
    from fleet_manager.local_control.router import create_router

    app.include_router(create_router())
    from fleet_manager.local_control.model_sharing import create_model_router

    app.include_router(create_model_router())
    from fleet_manager.local_control.dashboard import router as dashboard_router

    app.include_router(dashboard_router)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        if request.url.path == "/v1/jobs" or request.url.path.startswith(
            ("/v1/node/", "/v1/models/")
        ):
            # Validation inputs can contain entire prompts and tool arguments.
            reason = (
                "invalid_job_request"
                if request.url.path == "/v1/jobs"
                else "invalid_control_request"
            )
            await trace_failure(app, reason)
            return JSONResponse(status_code=422, content={"error": {"type": reason}})
        from fastapi.exception_handlers import request_validation_exception_handler

        return await request_validation_exception_handler(request, exc)

    @app.exception_handler(AdmissionError)
    async def admission_error(request, exc):
        await app.state.trace_store.record_trace(
            request_id=RequestBinding().request_id,
            model="",
            original_model="",
            node_id=app.state.ledger.worker_id,
            status="failed",
            error_message=exc.reason,
            original_format="capacity",
        )
        return JSONResponse(
            status_code=exc.status,
            content={
                "error": {
                    "type": exc.reason,
                    "message": exc.reason,
                    **({"details": exc.details} if exc.details is not None else {}),
                }
            },
            headers={"Retry-After": "1"} if exc.status == 429 else {},
        )

    @app.exception_handler(httpx.HTTPError)
    async def backend_error(request, exc):
        # The shared queue/proxy already records inference failures. Native
        # non-streaming routes also need an HTTP error envelope for admission.
        status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else 502
        return JSONResponse(status_code=status, content={"error": str(exc)})

    @app.exception_handler(aiosqlite.Error)
    async def storage_error(request, exc):
        # A failed origin/worker write cannot authorize admission or execution.
        await trace_failure(app, "admission_storage_unavailable")
        return JSONResponse(
            status_code=503, content={"error": {"type": "admission_storage_unavailable"}}
        )

    @app.get("/v1/cluster/capacity")
    async def capacity(
        model: str, workload: Workload = "interactive", idle_only: bool = False
    ) -> ClusterCapacity:
        return await cluster_capacity(app, model, workload, idle_only)

    @app.get("/v1/node/eligibility")
    async def eligibility(workload: Workload = "background", idle_only: bool = False):
        return await app.state.idle_policy.decision(workload, idle_only)

    @app.post("/v1/capacity/acquire", status_code=201)
    async def acquire(spec: AcquireRequest, request: Request) -> Lease:
        # Read the body before watching ASGI disconnects, so the watcher never
        # competes with request parsing. Cancel pending admission on disconnect.
        async def disconnected():
            while True:
                message = await request.receive()
                if message["type"] == "http.disconnect":
                    return

        pending = asyncio.create_task(app.state.waiters.acquire(spec))
        gone = asyncio.create_task(disconnected())
        try:
            done, _ = await asyncio.wait({pending, gone}, return_when=asyncio.FIRST_COMPLETED)
            if pending in done:
                return await pending
            raise asyncio.CancelledError()
        finally:
            for task in (pending, gone):
                if not task.done():
                    task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def authorized_reservation(reservation_id: str, request: Request):
        token = request.headers.get("X-Idle-Herd-Reservation", "")
        return await app.state.waiters.reservation(reservation_id, token)

    @app.get("/v1/capacity/reservations/{reservation_id}")
    async def inspect(reservation_id: str, request: Request) -> Lease:
        return await authorized_reservation(reservation_id, request)

    @app.delete("/v1/capacity/reservations/{reservation_id}")
    async def release(reservation_id: str, request: Request) -> Lease:
        return await app.state.waiters.reservation(
            reservation_id, request.headers.get("X-Idle-Herd-Reservation", ""), release=True
        )

    @app.get("/health")
    async def health():
        return {
            "worker_id": app.state.ledger.worker_id,
            "discovery": "peer_to_peer" if app.state.directory else "disabled",
            "peer_error": app.state.peer_error,
            "quorum": app.state.coordination.snapshot().quorum,
        }

    @app.get("/v1/cluster/peers")
    async def peers():
        return {
            "scope": "local_peer_view",
            "globally_authoritative": False,
            "peers": app.state.directory.snapshot() if app.state.directory else [],
        }

    return app


def create_peer_rpc_app(local_app):
    """Serve only on a separately configured TLS listener after local startup."""
    return PeerRPC(local_app)
