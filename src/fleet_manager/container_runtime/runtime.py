"""Container boundary: explicit public routes, private backend relay, no host idle claims."""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import time
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import uvicorn
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse

from fleet_manager.cli import peer_cli
from fleet_manager.peer.models import PeerSettings
from fleet_manager.peer.sensors import IdleSignal

NATIVE = "http://127.0.0.1:11435"
BACKEND = "http://127.0.0.1:18080"
DEFAULT_BACKEND = "http://host.docker.internal:11434"

ROUTES = {
    "GET": {
        "/health",
        "/v1/models",
        "/v1/cluster/capacity",
        "/api/version",
        "/api/tags",
        "/api/ps",
        "/api/image-models",
        "/v1/messages",
    },
    "POST": {
        "/v1/chat/completions",
        "/v1/embeddings",
        "/v1/rerank",
        "/v1/images/generations",
        "/v1/systemone",
        "/v1/messages",
        "/v1/messages/count_tokens",
        "/v1/responses",
        "/api/chat",
        "/api/generate",
        "/api/show",
        "/api/embed",
        "/api/embeddings",
        "/api/embed-image",
        "/api/generate-image",
        "/api/transcribe",
        "/v1/capacity/acquire",
        "/v1/jobs",
    },
}
VARIABLE_ROUTES = {
    "GET": re.compile(
        r"/v1/(?:jobs/[A-Za-z0-9_-]{1,128}(?:/result)?|capacity/reservations/[A-Za-z0-9_-]{1,128})"
    ),
    "DELETE": re.compile(
        r"/v1/(?:jobs/[A-Za-z0-9_-]{1,128}|capacity/reservations/[A-Za-z0-9_-]{1,128})"
    ),
}
HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
}


def backend_origin(value: str) -> str:
    try:
        url = urlsplit(value)
        port = url.port  # Validate ports for every hostname before opening listeners.
        if (
            url.scheme != "http"
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.path not in {"", "/"}
            or url.query
            or url.fragment
            or (url.hostname in {"127.0.0.1", "::1", "localhost"} and port in {8080, 11435, 18080})
        ):
            raise ValueError
    except ValueError:
        raise ValueError(
            "container backend must be an HTTP origin without credentials or a path"
        ) from None
    return value.rstrip("/")


def allowed(scope) -> bool:
    path = scope["path"]
    # Encoded or noncanonical paths never get a second interpretation downstream.
    if scope.get("raw_path", path.encode()) != path.encode("ascii", errors="replace"):
        return False
    method = scope["method"]
    return path in ROUTES.get(method, set()) or bool(
        VARIABLE_ROUTES.get(method) and VARIABLE_ROUTES[method].fullmatch(path)
    )


class Proxy:
    def __init__(self, origin: str, *, gateway: bool, transport=None):
        self.origin = origin
        self.gateway = gateway
        self.client = httpx.AsyncClient(
            timeout=None, follow_redirects=False, trust_env=False, transport=transport
        )

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return
        if self.gateway and not allowed(scope):
            return await JSONResponse({"error": "not_found"}, status_code=404)(scope, receive, send)
        if self.gateway and any(
            name.lower() == b"origin"
            or (name.lower() == b"sec-fetch-site" and value.strip().lower() == b"cross-site")
            for name, value in scope["headers"]
        ):
            return await JSONResponse({"error": {"type": "local_clients_only"}}, status_code=403)(
                scope, receive, send
            )
        request = Request(scope, receive)
        connection = {v.strip().lower() for v in request.headers.get("connection", "").split(",")}
        headers = [
            (k, v)
            for k, v in request.headers.items()
            if k.lower() not in HOP | connection and not k.lower().startswith("x-forwarded-")
        ]
        url = self.origin + scope.get("raw_path", scope["path"].encode()).decode("ascii")
        if scope["query_string"]:
            url += "?" + scope["query_string"].decode("ascii")
        upstream = self.client.build_request(
            scope["method"], url, headers=headers, content=request.stream()
        )
        try:
            response = await self.client.send(upstream, stream=True)
        except httpx.HTTPError:
            return await JSONResponse({"error": "upstream_unavailable"}, status_code=502)(
                scope, receive, send
            )
        if not self.gateway and 300 <= response.status_code < 400:
            await response.aclose()
            return await JSONResponse({"error": "backend_redirect_rejected"}, status_code=502)(
                scope, receive, send
            )
        excluded = HOP | {
            v.strip().lower() for v in response.headers.get("connection", "").split(",")
        }
        result = StreamingResponse(
            response.aiter_raw(),
            status_code=response.status_code,
            background=BackgroundTask(response.aclose),
        )
        result.raw_headers = [
            (k, v) for k, v in response.headers.raw if k.decode("latin1").lower() not in excluded
        ]
        try:
            await result(scope, receive, send)
        finally:
            await response.aclose()


class ContainerSensor:
    """Cgroup v2 resource usage; container activity cannot establish host idleness."""

    def __init__(self, root: Path = Path("/sys/fs/cgroup")):
        self.root = root
        self.previous = None

    def _read(self):
        cpu = memory = None
        try:
            usage = int(
                dict(line.split() for line in (self.root / "cpu.stat").read_text().splitlines())[
                    "usage_usec"
                ]
            )
            quota, period = (self.root / "cpu.max").read_text().split()
            now = time.monotonic()
            if self.previous and quota != "max":
                delta = now - self.previous[0]
                if delta > 0:
                    cpu = min(
                        100.0,
                        max(
                            0.0,
                            (usage - self.previous[1])
                            / 1e6
                            / delta
                            / (int(quota) / int(period))
                            * 100,
                        ),
                    )
            self.previous = (now, usage)
        except (OSError, ValueError, KeyError, ZeroDivisionError):
            pass
        try:
            limit = (self.root / "memory.max").read_text().strip()
            if limit != "max":
                memory = max(0, int(limit) - int((self.root / "memory.current").read_text())) / 1e9
        except (OSError, ValueError):
            pass
        return cpu, memory

    async def sample(self):
        cpu, memory = await asyncio.to_thread(self._read)
        return IdleSignal(
            observed_at=time.time(),
            source="container",
            confidence="unknown",
            cpu_percent=cpu,
            memory_available_gb=memory,
        )


async def healthcheck():
    try:
        async with httpx.AsyncClient(timeout=3, trust_env=False) as client:
            for url in ("http://127.0.0.1:8080/health", NATIVE + "/health"):
                if (await client.get(url)).status_code != 200:
                    return False
        return True
    except httpx.HTTPError:
        return False


async def run():
    origin = backend_origin(os.environ.get("IDLE_HERD_CONTAINER_BACKEND_URL", DEFAULT_BACKEND))
    policy = os.environ.get("IDLE_HERD_BACKGROUND_POLICY", "idle_only")
    if policy not in {"idle_only", "utilization_only"}:
        raise ValueError("IDLE_HERD_BACKGROUND_POLICY must be idle_only or utilization_only")
    # Docker bridge networks do not reliably carry physical-LAN multicast.
    # Preserve explicit seeded/enrolled container operation unless opted in.
    discovery_defaults = {
        key: False
        for key in ("mdns_enabled", "lan_auto_trust")
        if "IDLE_HERD_" + key.upper() not in os.environ
    }
    settings = PeerSettings(backend_url=BACKEND, background_policy=policy, **discovery_defaults)
    gateway, relay = Proxy(NATIVE, gateway=True), Proxy(origin, gateway=False)
    servers = [
        uvicorn.Server(
            uvicorn.Config(gateway, host="0.0.0.0", port=8080, lifespan="off", proxy_headers=False)
        ),
        uvicorn.Server(
            uvicorn.Config(relay, host="127.0.0.1", port=18080, lifespan="off", proxy_headers=False)
        ),
    ]
    for server in servers:
        server.capture_signals = contextlib.nullcontext
    tasks = [asyncio.create_task(server.serve()) for server in servers]
    native = asyncio.create_task(
        peer_cli.serve(settings, sensor=ContainerSensor(), peer_bind_host="0.0.0.0")
    )
    try:
        done, _ = await asyncio.wait([native, *tasks], return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
        if native not in done:
            raise RuntimeError("container proxy listener exited")
    finally:
        for server in servers:
            server.should_exit = True
        if not native.done():
            native.cancel()
        await asyncio.gather(native, *tasks, return_exceptions=True)
        await gateway.client.aclose()
        await relay.client.aclose()
