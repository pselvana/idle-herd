"""Local Ollama inventory and the sole inference transport admission boundary."""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from contextvars import ContextVar

import httpx
import psutil
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from fleet_manager.models.request import normalize_model_name
from fleet_manager.peer.local_ledger import LocalLedger
from fleet_manager.peer.models import (
    AcquireRequest,
    AdmissionError,
    PeerSettings,
    ResourceFit,
    Workload,
)


class RequestBinding(BaseModel):
    token: str | None = None
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    workload: Workload = "interactive"
    idle_only: bool = False


binding: ContextVar[RequestBinding | None] = ContextVar("peer_request", default=None)


class BackendModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str = Field(validation_alias=AliasChoices("name", "model"))
    size: int = Field(ge=0)
    digest: str = ""
    modified_at: str = ""
    details: dict = Field(default_factory=dict)
    capabilities: list[str] = Field(default_factory=list)
    context_length: int = 0


class ModelInventory(BaseModel):
    models: list[BackendModel]


class OllamaBackend:
    def __init__(self, settings: PeerSettings, transport: httpx.AsyncBaseTransport | None = None):
        self.settings = settings
        self.client = httpx.AsyncClient(
            base_url=settings.backend_url, timeout=5, transport=transport, trust_env=False
        )

    async def inventory(self) -> tuple[ModelInventory, ModelInventory]:
        try:
            tags, running = await asyncio.gather(
                self.client.get("/api/tags"), self.client.get("/api/ps")
            )
            tags.raise_for_status()
            running.raise_for_status()
            return (
                ModelInventory.model_validate(tags.json()),
                ModelInventory.model_validate(running.json()),
            )
        except (httpx.HTTPError, ValueError) as exc:
            raise AdmissionError("backend_unavailable", 503) from exc

    async def can_run(self, model: str) -> ResourceFit:
        tags, running = await self.inventory()
        installed = next((m for m in tags.models if normalize_model_name(m.name) == model), None)
        if installed is None:
            raise AdmissionError("not_installed", 404)
        if installed.size <= 0:
            raise AdmissionError("unknown_model_size", 503)
        loaded = any(
            normalize_model_name(m.name) == model
            and (not installed.digest or m.digest == installed.digest)
            for m in running.models
        )
        memory = await asyncio.to_thread(psutil.virtual_memory)
        cold_weights = 0 if loaded else installed.size / 1e9 * self.settings.weight_multiplier
        return ResourceFit(
            model=model,
            required_gb=cold_weights + self.settings.request_memory_gb,
            available_gb=max(0, memory.available / 1e9 - self.settings.reserve_memory_gb),
        )

    async def close(self):
        await self.client.aclose()


async def settle(ledger: LocalLedger, token: str, stopped: bool):
    # Do not leave a SQLite state change half-awaited on a client disconnect.
    task = asyncio.create_task(ledger.finish(token, stopped=stopped))
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


class FencedResponseStream(httpx.AsyncByteStream):
    def __init__(self, stream, ledger, token, *, ndjson: bool, status: int):
        self.stream = stream
        self.ledger = ledger
        self.token = token
        self.ndjson = ndjson
        self.status = status
        self.pending = b""
        self.saw_done = False
        self.eof = False
        self.closed = False

    def _line(self, line: bytes):
        # Invalid/truncated wire data never confirms execution stopped.
        with contextlib.suppress(ValueError, AttributeError):
            self.saw_done |= json.loads(line).get("done") is True

    async def __aiter__(self):
        async for chunk in self.stream:
            if self.ndjson:
                # Bound the parser independently from untrusted model output.
                self.pending += chunk
                lines = self.pending.split(b"\n")
                self.pending = lines.pop()[-65536:]
                for line in lines:
                    self._line(line)
            yield chunk
        if self.pending:
            self._line(self.pending)
        self.eof = True

    async def aclose(self):
        if self.closed:
            return
        self.closed = True
        try:
            await self.stream.aclose()
        finally:
            # An HTTP error is a confirmed rejection only for a complete 4xx.
            # A 5xx, broken stream, timeout or cancellation is an unknown outcome.
            stopped = (self.eof and 400 <= self.status < 500) or (
                200 <= self.status < 300 and (self.saw_done if self.ndjson else self.eof)
            )
            await settle(self.ledger, self.token, stopped)


class AdmissionTransport(httpx.AsyncBaseTransport):
    """Every inference call, including direct embedding calls, passes here."""

    def __init__(self, ledger: LocalLedger, inner: httpx.AsyncBaseTransport | None = None):
        self.ledger = ledger
        self.inner = inner or httpx.AsyncHTTPTransport(retries=0)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.method != "POST" or request.url.path not in {
            "/api/chat",
            "/api/generate",
            "/api/embed",
            "/api/embeddings",
            "/v1/systemone",
        }:
            return await self.inner.handle_async_request(request)
        body = json.loads(await request.aread())
        model = normalize_model_name(body.get("model", ""))
        current = binding.get() or RequestBinding()
        try:
            token = current.token
            if token is None:
                lease = await self.ledger.acquire(
                    AcquireRequest(
                        model=model,
                        request_id=current.request_id,
                        workload=current.workload,
                        idle_only=current.idle_only,
                    )
                )
                token = lease.token
            await self.ledger.consume(
                token, model, workload=current.workload, idle_only=current.idle_only
            )
        except AdmissionError as exc:
            return httpx.Response(
                exc.status,
                json={"error": exc.reason},
                headers={"Retry-After": "1"} if exc.status == 429 else {},
            )
        try:
            response = await self.inner.handle_async_request(request)
        except BaseException:
            await settle(self.ledger, token, stopped=False)
            raise
        # Rebuild the response so even an eagerly buffered inner transport must
        # traverse and close our accounting stream rather than cached content.
        return httpx.Response(
            response.status_code,
            headers=response.headers,
            extensions=response.extensions,
            stream=FencedResponseStream(
                response.stream,
                self.ledger,
                token,
                ndjson=request.url.path in {"/api/chat", "/api/generate"},
                status=response.status_code,
            ),
        )

    async def aclose(self):
        await self.inner.aclose()
