"""Local Ollama inventory and the sole inference transport admission boundary."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import uuid
from contextvars import ContextVar

import anyio
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
logger = logging.getLogger(__name__)


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
    with anyio.CancelScope(shield=True):
        task = asyncio.create_task(ledger.finish(token, stopped=stopped))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise


class FencedResponseStream(httpx.AsyncByteStream):
    def __init__(
        self,
        stream,
        ledger,
        token,
        *,
        ndjson: bool,
        status: int,
        reservation_id: str = "",
        drain_seconds: float = 0,
        on_finished=None,
    ):
        self.stream = stream
        self.ledger = ledger
        self.token = token
        self.ndjson = ndjson
        self.status = status
        self.pending = b""
        self.saw_done = False
        self.eof = False
        self.closed = False
        self.reservation_id = reservation_id
        self.drain_seconds = drain_seconds
        self.recovery_deadline: float | None = None
        self._iterator = None
        self._read_task = None
        self._drainer = None
        self._read_failed = False
        self._finished = False
        self._on_finished = on_finished

    def _line(self, line: bytes):
        # Invalid/truncated wire data never confirms execution stopped.
        with contextlib.suppress(ValueError, AttributeError):
            self.saw_done |= json.loads(line).get("done") is True

    def _chunk(self, chunk):
        if self.ndjson:
            # Keep only parser state, never accumulate detached output.
            self.pending += chunk
            lines = self.pending.split(b"\n")
            self.pending = lines.pop()[-65536:]
            for line in lines:
                self._line(line)

    async def _next(self):
        if self._iterator is None:
            self._iterator = self.stream.__aiter__()
        if self._read_task is None:
            # A client cancellation must not cancel the original backend read:
            # the same response may still provide authoritative completion.
            self._read_task = asyncio.create_task(anext(self._iterator))
        try:
            chunk = await asyncio.shield(self._read_task)
        except StopAsyncIteration:
            self._read_task = None
            if self.pending:
                self._line(self.pending)
            self.eof = True
            raise
        except asyncio.CancelledError:
            raise
        except Exception:
            self._read_task = None
            self._read_failed = True
            raise
        self._read_task = None
        self._chunk(chunk)
        return chunk

    async def __aiter__(self):
        while True:
            try:
                yield await self._next()
            except StopAsyncIteration:
                return

    def _confirmed(self):
        return (self.eof and 400 <= self.status < 500) or (
            200 <= self.status < 300 and (self.saw_done if self.ndjson else self.eof)
        )

    async def _close_stream(self):
        if self._finished:
            return
        self._finished = True
        try:
            if self._read_task is not None:
                self._read_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._read_task
                self._read_task = None
            await self.stream.aclose()
        finally:
            self.recovery_deadline = None
            if self._on_finished:
                self._on_finished(self)

    async def _recover(self):
        try:
            async with asyncio.timeout(self.drain_seconds):
                while not self.eof and not self._confirmed():
                    try:
                        await self._next()
                    except StopAsyncIteration:
                        break
            if self._confirmed():
                await self.ledger.confirm_backend_completion(self.token)
        except (TimeoutError, asyncio.CancelledError):
            # No completion proof: quarantine remains charged.
            pass
        except Exception as exc:
            logger.warning("Backend completion recovery unavailable: %s", type(exc).__name__)
        finally:
            with anyio.CancelScope(shield=True):
                await self._close_stream()

    async def aclose(self):
        if self.closed:
            return
        self.closed = True
        # Starlette disconnects use AnyIO level cancellation: each await inside
        # its cancelled scope is cancelled again. asyncio.shield alone cannot
        # protect the awaited SQLite cleanup from that repeated cancellation.
        with anyio.CancelScope(shield=True):
            stopped = self._confirmed()
            await settle(self.ledger, self.token, stopped)
            if (
                not stopped
                and not self.eof
                and not self._read_failed
                and 200 <= self.status < 300
                and self.drain_seconds > 0
            ):
                self.recovery_deadline = time.time() + self.drain_seconds
                self._drainer = asyncio.create_task(self._recover())
            else:
                await self._close_stream()

    async def shutdown(self):
        """End daemon-owned readers before closing the worker ledger."""
        with anyio.CancelScope(shield=True):
            if self._drainer is not None:
                self._drainer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._drainer
            if not self.closed:
                self.closed = True
                await settle(self.ledger, self.token, self._confirmed())
            await self._close_stream()


class AdmissionTransport(httpx.AsyncBaseTransport):
    """Every inference call, including direct embedding calls, passes here."""

    def __init__(self, ledger: LocalLedger, inner: httpx.AsyncBaseTransport | None = None):
        self.ledger = ledger
        self.inner = inner or httpx.AsyncHTTPTransport(retries=0)
        self._streams: set[FencedResponseStream] = set()

    def recovery_deadline(self, reservation_id: str) -> float | None:
        return next(
            (s.recovery_deadline for s in self._streams if s.reservation_id == reservation_id),
            None,
        )

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
            consumed = await self.ledger.consume(
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
        stream = FencedResponseStream(
            response.stream,
            self.ledger,
            token,
            ndjson=request.url.path in {"/api/chat", "/api/generate"},
            status=response.status_code,
            reservation_id=consumed.reservation_id,
            drain_seconds=self.ledger.settings.quarantine_drain_seconds,
            on_finished=self._streams.discard,
        )
        self._streams.add(stream)
        return httpx.Response(
            response.status_code,
            headers=response.headers,
            extensions=response.extensions,
            stream=stream,
        )

    async def aclose(self):
        await asyncio.gather(*(s.shutdown() for s in list(self._streams)))
        await self.inner.aclose()
