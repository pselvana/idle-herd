import asyncio
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from fleet_manager.container_runtime import runtime
from fleet_manager.container_runtime.runtime import ContainerSensor, Proxy, backend_origin
from fleet_manager.peer.models import PeerSettings
from fleet_manager.peer.policy import IdlePolicy


@pytest.mark.asyncio
async def test_gateway_allows_only_exact_public_routes_and_streams():
    seen = []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"first\n"
            yield b"second\n"

    async def upstream(request):
        seen.append((request.url.path, request.headers.get("host"), await request.aread()))
        return httpx.Response(200, stream=Stream(), headers={"content-type": "text/event-stream"})

    proxy = Proxy("http://127.0.0.1:11435", gateway=True, transport=httpx.MockTransport(upstream))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=proxy), base_url="http://gateway"
    ) as client:
        response = await client.post("/v1/chat/completions", content=b"prompt")
        assert response.content == b"first\nsecond\n"
        assert response.headers["content-type"] == "text/event-stream"
        for path in (
            "/v1/node/status",
            "/dashboard",
            "/docs",
            "/openapi.json",
            "/v1/models/artifacts",
            "/v1/peer/status",
            "/v1/%6eode/status",
        ):
            response = await client.get(
                path, headers={"Authorization": "Bearer stolen-owner-token"}
            )
            assert response.status_code == 404
        assert (await client.post("/v1/models", content=b"x")).status_code == 404
        for headers in (
            {"origin": "http://evil.example", "connection": "origin"},
            {"sec-fetch-site": "cross-site", "connection": "sec-fetch-site"},
        ):
            response = await client.post("/v1/chat/completions", headers=headers)
            assert response.status_code == 403
    assert seen == [("/v1/chat/completions", "127.0.0.1:11435", b"prompt")]
    await proxy.client.aclose()


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com:11434",
        "http://user:pass@example.com:11434",
        "http://example.com:11434/path",
        "http://example.com:11434/?x=1",
        "http://localhost:18080",
        "http://host.docker.internal:invalid",
        "http://host.docker.internal:99999",
    ],
)
def test_backend_rejects_unsafe_origin(url):
    with pytest.raises(ValueError):
        backend_origin(url)


@pytest.mark.asyncio
async def test_backend_relay_rejects_redirects():
    async def upstream(request):
        return httpx.Response(302, headers={"location": "http://other/"})

    proxy = Proxy(
        backend_origin("http://host.docker.internal:11434"),
        gateway=False,
        transport=httpx.MockTransport(upstream),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=proxy), base_url="http://relay"
    ) as client:
        assert (await client.get("/api/tags")).status_code == 502
    await proxy.client.aclose()


@pytest.mark.asyncio
async def test_container_sensor_never_claims_human_idle(tmp_path):
    (tmp_path / "cpu.stat").write_text("usage_usec 1000\n")
    (tmp_path / "cpu.max").write_text("100000 100000\n")
    (tmp_path / "memory.max").write_text("8000000000\n")
    (tmp_path / "memory.current").write_text("1000000000\n")
    sensor = ContainerSensor(tmp_path)
    await sensor.sample()
    signal = await sensor.sample()
    assert signal.source == "container"
    assert signal.confidence == "unknown" and signal.idle_seconds is None
    assert signal.memory_available_gb == 7
    policy = IdlePolicy(PeerSettings(idle_hysteresis_seconds=0), sensor)
    await policy._observe()
    decision = await policy.decision("background", idle_only=True)
    assert not decision.eligible
    assert signal.observed_at <= time.time()


@pytest.mark.asyncio
async def test_proxy_failure_stops_native_and_other_listener(monkeypatch):
    native_started = asyncio.Event()
    native_stopped = asyncio.Event()
    servers = []

    class Server:
        def __init__(self, config):
            self.config = config
            self.should_exit = False
            servers.append(self)

        async def serve(self):
            await native_started.wait()
            if self.config.port == 8080:
                raise RuntimeError("listener failed")
            while not self.should_exit:
                await asyncio.sleep(0)

    async def serve_native(*args, **kwargs):
        try:
            native_started.set()
            await asyncio.Event().wait()
        finally:
            native_stopped.set()

    monkeypatch.setattr(runtime.uvicorn, "Server", Server)
    monkeypatch.setattr(runtime.peer_cli, "serve", serve_native)
    with pytest.raises(RuntimeError, match="listener failed"):
        await asyncio.wait_for(runtime.run(), timeout=2)
    assert native_stopped.is_set()
    assert all(server.should_exit for server in servers)


@pytest.mark.asyncio
async def test_cancel_native_stops_tls_listener_before_lifespan_closes(monkeypatch, tmp_path):
    started = asyncio.Event()
    stopped = asyncio.Event()
    closed = asyncio.Event()

    @asynccontextmanager
    async def lifespan(app):
        app.state.identity = SimpleNamespace(cert_path=tmp_path / "cert", key_path=tmp_path / "key")
        app.state.directory = None
        app.state.peer_error = None
        try:
            yield
        finally:
            assert stopped.is_set()
            closed.set()

    app = FastAPI(lifespan=lifespan)

    class Server:
        def __init__(self, config):
            self.config = config
            self.should_exit = False

        async def serve(self):
            if self.config.port == 11435:
                await asyncio.Event().wait()
            else:
                assert self.config.host == "0.0.0.0"
                started.set()
                while not self.should_exit:
                    await asyncio.sleep(0)
                stopped.set()

    monkeypatch.setattr(runtime.peer_cli, "create_peer_app", lambda settings, sensor: app)
    monkeypatch.setattr(runtime.peer_cli.uvicorn, "Server", Server)
    settings = PeerSettings(peer_endpoint="https://127.0.0.1:17435")
    task = asyncio.create_task(
        runtime.peer_cli.serve(settings, sensor=ContainerSensor(), peer_bind_host="0.0.0.0")
    )
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert closed.is_set()
