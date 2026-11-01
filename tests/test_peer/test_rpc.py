"""Peer RPC checks using real signed credentials and a mocked local backend."""

import asyncio
import json
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest

from fleet_manager.peer.app import create_peer_app, create_peer_rpc_app
from fleet_manager.peer.identity import PeerIdentity
from fleet_manager.peer.models import PeerSettings
from fleet_manager.peer.network import MAX_INFERENCE_BODY
from fleet_manager.peer.sensors import IdleSignal


class HealthySensor:
    async def sample(self):
        return IdleSignal(observed_at=time.time(), source="test", memory_available_gb=48)


class WireBytes(httpx.AsyncByteStream):
    def __init__(self, data, broken=False):
        self.data = data
        self.broken = broken

    async def __aiter__(self):
        yield self.data
        if self.broken:
            raise httpx.ReadError("backend disconnected")

    async def aclose(self):
        pass


@asynccontextmanager
async def worker(path, monkeypatch, *, incomplete=False, buffered=True, broken=False):
    monkeypatch.setattr(
        "fleet_manager.peer.backend.psutil.virtual_memory",
        lambda: SimpleNamespace(total=64e9, used=16e9, available=48e9),
    )

    async def backend(request):
        if request.url.path in {"/api/tags", "/api/ps"}:
            return httpx.Response(200, json={"models": [{"name": "tiny:latest", "size": 1000}]})
        if request.url.path == "/api/show":
            return httpx.Response(200, json={"capabilities": ["completion"]})
        content = (
            json.dumps({"model": "tiny:latest", "response": "hello", "done": not incomplete}) + "\n"
        ).encode()
        if buffered:
            return httpx.Response(200, content=content)
        return httpx.Response(200, stream=WireBytes(content, broken=broken))

    app = create_peer_app(
        PeerSettings(data_dir=str(path), request_memory_gb=0.000001, background_policy="always"),
        backend_transport=httpx.MockTransport(backend),
        sensor=HealthySensor(),
    )
    async with app.router.lifespan_context(app):
        identity = await PeerIdentity.open(
            path, app.state.ledger.worker_id, app.state.ledger.cluster_id
        )
        app.state.identity = identity
        app.state.directory = object()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_peer_rpc_app(app)), base_url="https://peer"
        ) as client:
            yield app, identity, client


async def enrolled(authority, path, worker_id):
    request = await PeerIdentity.enrollment_request(path, worker_id)
    credential = await authority.issue_credential(request)
    await PeerIdentity.import_credential(path, credential, authority.fingerprint)
    return await PeerIdentity.open(path, worker_id, authority.cluster_id)


def signed(identity, path, data):
    body = json.dumps(data, separators=(",", ":")).encode()
    return body, identity.sign_request("POST", path, body)


async def test_replay_is_durable_across_worker_restart(tmp_path, monkeypatch):
    async with worker(tmp_path / "worker", monkeypatch) as (_, authority, client):
        principal = await enrolled(authority, tmp_path / "origin", "origin")
        path = "/peer/v1/acquire"
        body, headers = signed(principal, path, {"model": "tiny", "request_id": "replay"})
        response = await client.post(path, content=body, headers=headers)
        assert response.status_code == 200, response.text
        principal.verify_response(headers["x-idle-nonce"], 200, response.content, response.headers)
    async with worker(tmp_path / "worker", monkeypatch) as (_, _, client):
        response = await client.post(path, content=body, headers=headers)
        assert response.status_code == 403
        assert response.json()["error"]["type"] == "replayed_peer_request"


async def test_foreign_principal_cannot_release_or_execute_lease(tmp_path, monkeypatch):
    async with worker(tmp_path / "worker", monkeypatch) as (app, authority, client):
        owner = await enrolled(authority, tmp_path / "owner", "owner")
        other = await enrolled(authority, tmp_path / "other", "other")
        path = "/peer/v1/acquire"
        body, headers = signed(owner, path, {"model": "tiny", "request_id": "r1"})
        grant = await client.post(path, content=body, headers=headers)
        assert grant.status_code == 200, grant.text
        token = grant.json()["token"]
        for path in ("/peer/v1/release", "/peer/v1/execute"):
            body, headers = signed(
                other,
                path,
                {
                    "path": "/api/generate",
                    "token": token,
                    "payload": {"model": "tiny", "prompt": "hi", "stream": False},
                },
            )
            response = await client.post(path, content=body, headers=headers)
            assert response.status_code == 403, response.text
            assert response.json()["error"]["type"] == "reservation_principal_mismatch"
        assert (await app.state.ledger.inspect(token)).state == "reserved"


async def test_rpc_scope_and_bounded_body(tmp_path, monkeypatch):
    async with worker(tmp_path / "worker", monkeypatch) as (_, authority, client):
        for path in ("/health", "/v1/capacity/acquire", "/api/generate", "/peer/v1/acquire?x=1"):
            response = await client.post(path, content=b"{}")
            assert response.status_code == 404
        path = "/peer/v1/acquire"
        response = await client.post(path, content=b"x" * (MAX_INFERENCE_BODY + 1))
        assert response.status_code == 413
        secret = "private-prompt-must-not-appear-in-diagnostics"
        body, headers = signed(
            authority,
            path,
            {
                "model": "tiny",
                "request_id": "invalid",
                "unexpected_prompt": secret,
            },
        )
        response = await client.post(path, content=body, headers=headers)
        assert response.status_code == 422
        assert secret not in response.text
        body, headers = signed(authority, path, {"model": "tiny", "request_id": "r"})
        headers["origin"] = "https://browser.example"
        response = await client.post(path, content=body, headers=headers)
        assert response.status_code == 403


@pytest.mark.parametrize(("buffered", "broken"), [(True, False), (False, False), (False, True)])
async def test_incomplete_remote_execution_stays_quarantined(
    tmp_path, monkeypatch, buffered, broken
):
    async with worker(
        tmp_path / "worker", monkeypatch, incomplete=True, buffered=buffered, broken=broken
    ) as (
        app,
        authority,
        client,
    ):
        owner = await enrolled(authority, tmp_path / "owner", "owner")
        path = "/peer/v1/acquire"
        body, headers = signed(owner, path, {"model": "tiny", "request_id": "r1"})
        response = await client.post(path, content=body, headers=headers)
        assert response.status_code == 200, response.text
        token = response.json()["token"]
        path = "/peer/v1/execute"
        body, headers = signed(
            owner,
            path,
            {
                "path": "/api/generate",
                "token": token,
                "payload": {"model": "tiny", "prompt": "hi", "stream": True},
            },
        )
        await client.post(path, content=body, headers=headers)
        for _ in range(100):
            state = (await app.state.ledger.inspect(token)).state
            if state == "quarantined":
                break
            await asyncio.sleep(0.01)
        assert state == "quarantined"


async def test_expired_peer_identity_keeps_local_inference_available(tmp_path, monkeypatch):
    from fleet_manager.peer.identity import IdentityError

    monkeypatch.setattr(
        "fleet_manager.peer.backend.psutil.virtual_memory",
        lambda: SimpleNamespace(total=64e9, used=16e9, available=48e9),
    )

    async def expired(*args, **kwargs):
        raise IdentityError("expired certificate")

    async def backend(request):
        if request.url.path in {"/api/tags", "/api/ps"}:
            return httpx.Response(200, json={"models": [{"name": "tiny:latest", "size": 1000}]})
        return httpx.Response(200, json={"model": "tiny", "embeddings": [[1.0]]})

    monkeypatch.setattr(PeerIdentity, "open", expired)
    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path), peer_endpoint="https://127.0.0.1:18080"),
        backend_transport=httpx.MockTransport(backend),
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as client,
    ):
        health = await client.get("/health")
        assert health.json()["discovery"] == "disabled"
        assert "expired" in health.json()["peer_error"]
        response = await client.post("/api/embed", json={"model": "tiny", "input": "hi"})
        assert response.status_code == 200, response.text
        assert response.json()["embeddings"] == [[1.0]]


def test_enrollment_cli_round_trip(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from fleet_manager.cli.peer_cli import cli

    runner = CliRunner()
    authority = tmp_path / "authority"
    joining = tmp_path / "joining"
    request = tmp_path / "request.json"
    credential = tmp_path / "credential.json"
    monkeypatch.setenv("IDLE_HERD_DATA_DIR", str(authority))
    result = runner.invoke(cli, ["init"])
    assert result.exit_code == 0, result.output
    initialized = json.loads(result.output)
    monkeypatch.setenv("IDLE_HERD_DATA_DIR", str(joining))
    result = runner.invoke(cli, ["enrollment-request", "--output", str(request)])
    assert result.exit_code == 0, result.output
    monkeypatch.setenv("IDLE_HERD_DATA_DIR", str(authority))
    result = runner.invoke(cli, ["issue-credential", str(request), "--output", str(credential)])
    assert result.exit_code == 0, result.output
    assert "PRIVATE" not in request.read_text() + credential.read_text()
    monkeypatch.setenv("IDLE_HERD_DATA_DIR", str(joining))
    result = runner.invoke(
        cli, ["join", str(credential), "--fingerprint", initialized["trust_fingerprint"]]
    )
    assert result.exit_code == 0, result.output
    result = runner.invoke(cli, ["init"])
    assert result.exit_code == 0, result.output
    joined = json.loads(result.output)
    assert joined["cluster_id"] == initialized["cluster_id"]
    assert joined["trust_fingerprint"] == initialized["trust_fingerprint"]
    assert joined["worker_id"] != initialized["worker_id"]


async def test_origin_anthropic_key_is_checked_before_peer_dispatch(tmp_path, monkeypatch):
    async with worker(tmp_path / "worker", monkeypatch) as (app, _, _):
        app.state.settings.anthropic_require_key = True
        app.state.settings.anthropic_api_key = "origin-private-key"
        # This fixture's directory has no candidate method: auth must run first.
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as local:
            response = await local.post(
                "/v1/messages", json={"model": "remote", "messages": [], "max_tokens": 16}
            )
        assert response.status_code == 401
        count = await (await app.state.ledger._db.execute("SELECT count(*) FROM leases")).fetchone()
        assert count[0] == 0


async def test_peer_authorization_does_not_forward_origin_api_key(tmp_path, monkeypatch):
    async with worker(tmp_path / "worker", monkeypatch) as (app, authority, client):
        app.state.settings.anthropic_require_key = True
        app.state.settings.anthropic_api_key = "worker-private-key"
        owner = await enrolled(authority, tmp_path / "owner", "owner")
        body, headers = signed(owner, "/peer/v1/acquire", {"model": "tiny", "request_id": "auth"})
        grant = await client.post("/peer/v1/acquire", content=body, headers=headers)
        body, headers = signed(
            owner,
            "/peer/v1/execute",
            {
                "path": "/v1/messages",
                "token": grant.json()["token"],
                "payload": {"model": "tiny", "messages": [], "max_tokens": 16, "stream": False},
            },
        )
        response = await client.post("/peer/v1/execute", content=body, headers=headers)
        assert response.status_code == 200, response.text
        assert "worker-private-key" not in response.text
