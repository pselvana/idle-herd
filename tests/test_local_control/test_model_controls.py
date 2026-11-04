import asyncio
import hashlib
import json

import httpx
import pytest
from starlette.requests import Request

from fleet_manager.local_control.models import ModeUpdate
from fleet_manager.local_control.router import create_router
from fleet_manager.peer.app import create_peer_app, create_peer_rpc_app
from fleet_manager.peer.model_sharing import ArtifactManifest, TransferRequest
from fleet_manager.peer.models import AcquireRequest, AdmissionError, PeerSettings


@pytest.mark.asyncio
async def test_selected_peer_inventory_is_scoped_and_failures_are_not_empty(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from fleet_manager.peer.model_transport import ModelInventory

    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path / "state")),
        backend_transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"models": []})),
    )
    manifest = ArtifactManifest(
        model_id="peer-model",
        digest="a" * 64,
        size_bytes=100,
        quantization="Q4",
        provenance="publisher@revision",
        license="MIT",
        required_memory_bytes=1,
        manifest_digest="0" * 64,
    )
    manifest.manifest_digest = manifest.calculated_digest()
    async with app.router.lifespan_context(app):
        transport = app.state.model_transport
        peers = [SimpleNamespace(worker_id=name) for name in ("chosen", "other")]
        monkeypatch.setattr(transport, "_peers", lambda: peers)
        rpc = AsyncMock(return_value=ModelInventory(models=[manifest]))
        monkeypatch.setattr(transport, "_rpc", rpc)
        monkeypatch.setattr(
            app.state.model_sharing, "inventory", AsyncMock(side_effect=AssertionError)
        )
        read = {"Authorization": "Bearer " + app.state.control_credentials.read_token}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as client:
            path = "/v1/models/inventory?source_worker_id=chosen"
            assert (await client.get(path)).status_code == 401
            rpc.assert_not_awaited()
            result = await client.get(path, headers=read)
            assert result.status_code == 200
            assert result.headers["cache-control"] == "no-store"
            assert result.json()["models"] == [
                {**manifest.model_dump(), "source_worker_id": "chosen", "local": False}
            ]
            assert rpc.await_count == 1
            assert rpc.call_args.args[0].worker_id == "chosen"
            rpc.return_value = ModelInventory(models=[])
            assert (await client.get(path, headers=read)).json()["models"] == []
            rpc.side_effect = httpx.ReadError("disconnected")
            assert (await client.get(path, headers=read)).status_code == 503
            assert (
                await client.get("/v1/models/inventory?source_worker_id=unknown", headers=read)
            ).status_code == 503
            assert rpc.await_count == 3
            assert (
                await client.get("/v1/models/inventory?source_worker_id=", headers=read)
            ).status_code == 422
            for mode in ("offline", "paused"):
                await app.state.ledger.set_mode(ModeUpdate(mode=mode))
                assert (await client.get(path, headers=read)).status_code == 503
            assert rpc.await_count == 3


@pytest.mark.asyncio
async def test_owner_permissions_inventory_events_and_peer_isolation(tmp_path):
    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path / "state")),
        backend_transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"models": []})),
    )
    data = b"GGUFfake fixture"
    source = tmp_path / "private-weights.gguf"
    source.write_bytes(data)
    manifest = ArtifactManifest(
        model_id="test-model",
        digest=hashlib.sha256(data).hexdigest(),
        size_bytes=len(data),
        quantization="Q4_K_M",
        provenance="publisher/repo@revision",
        license="MIT",
        required_memory_bytes=1,
        manifest_digest="0" * 64,
    )
    manifest.manifest_digest = manifest.calculated_digest()
    async with app.router.lifespan_context(app):
        creds = app.state.control_credentials
        read = {"Authorization": "Bearer " + creds.read_token}
        control = {"Authorization": "Bearer " + creds.control_token}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as client:
            assert (await client.get("/v1/models/inventory")).status_code == 401
            spec = {
                "manifest": manifest.model_dump(),
                "path": str(source),
                "sharing_enabled": False,
            }
            assert (
                await client.post("/v1/models/artifacts", headers=read, json=spec)
            ).status_code == 403
            assert (
                await client.post("/v1/models/artifacts", headers=control, json=spec)
            ).status_code == 201
            result = await client.get("/v1/models/inventory", headers=read)
            assert result.headers["cache-control"] == "no-store"
            assert result.json()["models"][0]["digest"] == manifest.digest
            assert str(source) not in result.text
            assert await app.state.model_sharing.inventory("peer") == []
            path = f"/v1/models/artifacts/{manifest.digest}/permission"
            allowed = {"sharing_enabled": True, "license_approved": True, "allowed_peers": ["peer"]}
            assert (await client.put(path, headers=control, json=allowed)).status_code == 200
            assert len(await app.state.model_sharing.inventory("peer")) == 1
            denied = await client.put(
                path, headers={**control, "Origin": "https://evil.example"}, json={}
            )
            assert denied.status_code == 403
            assert (await client.put(path, headers=control, json={})).status_code == 200
            assert await app.state.model_sharing.inventory("peer") == []
            malformed = await client.post(
                "/v1/models/transfers", headers=control, json={"private_path": str(source)}
            )
            assert malformed.status_code == 422 and str(source) not in malformed.text
            consent = {
                "digest": manifest.digest,
                "manifest_digest": manifest.manifest_digest,
                "source_worker_id": "peer",
            }
            assert (
                await client.post("/v1/models/transfers", headers=control, json=consent)
            ).status_code == 403

        async def receive():
            await asyncio.sleep(60)
            return {"type": "http.disconnect"}

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/v1/node/events",
                "headers": [(b"authorization", f"Bearer {creds.read_token}".encode())],
                "app": app,
            },
            receive,
        )
        endpoint = next(r.endpoint for r in create_router().routes if r.path == "/v1/node/events")
        response = await endpoint(request)
        initial = await anext(response.body_iterator)
        event = json.loads(
            next(line[6:] for line in initial.splitlines() if line.startswith("data: "))
        )
        assert event["data"]["model_inventory"][0]["digest"] == manifest.digest
        assert str(source) not in initial and creds.control_token not in initial

        async def unavailable(*_args):
            await asyncio.Event().wait()

        await app.state.model_sharing.start_transfer(
            TransferRequest(
                digest=manifest.digest,
                manifest_digest=manifest.manifest_digest,
                source="peer",
                permit=True,
                license_approved=True,
            ),
            unavailable,
            unavailable,
        )
        changed = await anext(response.body_iterator)
        assert "event: transfer" in changed
        assert manifest.digest in changed and str(source) not in changed
        await app.state.model_sharing.pause(manifest.digest)
        await response.body_iterator.aclose()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_peer_rpc_app(app)), base_url="https://peer"
        ) as peer:
            assert (
                await peer.post("/v1/models/artifacts", headers=control, json=spec)
            ).status_code == 404


def test_copy_cli_requires_explicit_consent():
    from typer.testing import CliRunner

    from fleet_manager.cli.peer_cli import cli

    result = CliRunner().invoke(cli, ["models", "copy", "a" * 64, "b" * 64, "source"])
    assert result.exit_code != 0
    assert "--permit" in result.output and "--accept-license" in result.output


@pytest.mark.asyncio
async def test_model_load_confirmation_obeys_worker_slot_and_pause(tmp_path):
    generated = []

    async def backend(request):
        if request.url.path in {"/api/tags", "/api/ps"}:
            return httpx.Response(200, json={"models": [{"name": "tiny:latest", "size": 1}]})
        if request.url.path == "/api/generate":
            generated.append(request)
            return httpx.Response(200, json={"model": "tiny", "done": True})
        return httpx.Response(200, json={"capabilities": ["completion"]})

    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path), request_memory_gb=0.000001, reserve_memory_gb=0),
        backend_transport=httpx.MockTransport(backend),
    )
    manifest = ArtifactManifest(
        model_id="tiny",
        digest="a" * 64,
        size_bytes=1,
        quantization="Q4",
        provenance="test@revision",
        license="MIT",
        required_memory_bytes=1,
        manifest_digest="b" * 64,
    )
    async with app.router.lifespan_context(app):
        ledger = app.state.ledger
        held = await ledger.acquire(AcquireRequest(model="tiny", wait_ms=0))
        with pytest.raises(AdmissionError):
            await app.state.model_sharing.load_confirm(manifest)
        assert generated == []
        assert (await ledger.inspect(held.token)).state == "reserved"
        await ledger.release(held.token)
        assert await app.state.model_sharing.load_confirm(manifest)
        assert len(generated) == 1
        await ledger.set_mode(ModeUpdate(mode="paused"))
        with pytest.raises(AdmissionError):
            await app.state.model_sharing.load_confirm(manifest)
        assert len(generated) == 1
