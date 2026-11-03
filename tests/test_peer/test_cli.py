"""Optional transport startup must never own the localhost daemon's lifetime."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

from fastapi import FastAPI

from fleet_manager.cli.peer_cli import serve
from fleet_manager.peer.models import PeerSettings


async def test_multicast_failure_preserves_tls_peer_and_local_listener(monkeypatch, tmp_path):
    @asynccontextmanager
    async def lifespan(app):
        app.state.identity = SimpleNamespace(cert_path=tmp_path / "cert", key_path=tmp_path / "key")
        app.state.directory = object()
        app.state.peer_error = None
        yield

    app = FastAPI(lifespan=lifespan)
    local_completed = False
    peer_stopped = False

    class Server:
        def __init__(self, config):
            self.config = config
            self.should_exit = False
            self.started = False

        async def serve(self):
            nonlocal local_completed, peer_stopped
            self.started = True
            if self.config.port == 11435:
                async with asyncio.timeout(2):
                    while app.state.mdns_error is None:
                        await asyncio.sleep(0)
                assert app.state.peer_error is None
                assert app.state.directory is not None
                local_completed = True
            else:
                while not self.should_exit:
                    await asyncio.sleep(0)
                peer_stopped = True

    class BrokenBonjour:
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            raise PermissionError("test multicast denied")

        async def close(self):
            pass

    monkeypatch.setattr("fleet_manager.cli.peer_cli.create_peer_app", lambda settings: app)
    monkeypatch.setattr("fleet_manager.cli.peer_cli.uvicorn.Server", Server)
    monkeypatch.setattr("fleet_manager.peer.bonjour.BonjourPeers", BrokenBonjour)
    await serve(PeerSettings(peer_endpoint="https://127.0.0.1:17435"))
    assert local_completed and peer_stopped


async def test_peer_listener_bind_failure_keeps_localhost_running(monkeypatch, tmp_path):
    @asynccontextmanager
    async def lifespan(app):
        app.state.identity = SimpleNamespace(cert_path=tmp_path / "cert", key_path=tmp_path / "key")
        app.state.directory = object()
        app.state.peer_error = None
        yield

    app = FastAPI(lifespan=lifespan)
    local_completed = False

    class Server:
        def __init__(self, config):
            self.config = config
            self.should_exit = False

        async def serve(self):
            nonlocal local_completed
            if self.config.port != 11435:
                raise SystemExit(1)  # uvicorn bind failure behavior
            await asyncio.sleep(0.01)
            assert not self.should_exit
            assert app.state.peer_error.startswith("Peer listener unavailable")
            assert app.state.directory is None
            local_completed = True

    monkeypatch.setattr("fleet_manager.cli.peer_cli.create_peer_app", lambda settings: app)
    monkeypatch.setattr("fleet_manager.cli.peer_cli.uvicorn.Server", Server)
    await serve(PeerSettings(peer_endpoint="https://127.0.0.1:17435"))
    assert local_completed
