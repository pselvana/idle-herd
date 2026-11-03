"""Worker policy applies to authenticated peer grants and their consumption."""

import json
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest

from fleet_manager.peer.app import create_peer_app, create_peer_rpc_app
from fleet_manager.peer.identity import PeerIdentity
from fleet_manager.peer.models import PeerSettings
from fleet_manager.peer.sensors import IdleSignal


class MutableSensor:
    def __init__(self):
        self.idle_seconds = 1200
        self.confidence = "host_sensor"

    async def sample(self):
        return IdleSignal(
            observed_at=time.time(),
            source="test",
            confidence=self.confidence,
            idle_seconds=self.idle_seconds,
            session_locked=False,
            cpu_percent=1,
            memory_available_gb=48,
            memory_pressure="normal",
            on_ac=True,
            thermal_state="nominal",
        )


@asynccontextmanager
async def policy_worker(path, monkeypatch):
    monkeypatch.setattr(
        "fleet_manager.peer.backend.psutil.virtual_memory",
        lambda: SimpleNamespace(total=64e9, used=16e9, available=48e9),
    )
    sensor = MutableSensor()
    executions = []

    async def backend(request):
        if request.url.path in {"/api/tags", "/api/ps"}:
            return httpx.Response(200, json={"models": [{"name": "tiny:latest", "size": 1000}]})
        if request.url.path == "/api/show":
            return httpx.Response(200, json={"capabilities": ["completion"]})
        executions.append(request.url.path)
        return httpx.Response(200, json={"model": "tiny", "response": "ok", "done": True})

    app = create_peer_app(
        PeerSettings(data_dir=str(path), request_memory_gb=0.000001, idle_hysteresis_seconds=0),
        backend_transport=httpx.MockTransport(backend),
        sensor=sensor,
    )
    async with app.router.lifespan_context(app):
        authority = await PeerIdentity.open(
            path, app.state.ledger.worker_id, app.state.ledger.cluster_id
        )
        app.state.identity = authority
        app.state.directory = object()
        origin_path = path.parent / "origin"
        request = await PeerIdentity.enrollment_request(origin_path, "origin")
        credential = await authority.issue_credential(request)
        await PeerIdentity.import_credential(origin_path, credential, authority.fingerprint)
        origin = await PeerIdentity.open(origin_path, "origin", authority.cluster_id)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_peer_rpc_app(app)), base_url="https://peer"
        ) as client:
            yield app, sensor, executions, origin, client


async def rpc(client, origin, path, payload):
    body = json.dumps(payload, separators=(",", ":")).encode()
    return await client.post(path, content=body, headers=origin.sign_request("POST", path, body))


async def test_menu_bar_policy_update_allows_peer_execution_while_owner_is_active(
    tmp_path, monkeypatch
):
    async with policy_worker(tmp_path / "worker", monkeypatch) as (
        app,
        sensor,
        executions,
        origin,
        client,
    ):
        sensor.idle_seconds = 0
        payload = {"model": "tiny", "request_id": "active-owner", "workload": "interactive"}
        denied = await rpc(client, origin, "/peer/v1/acquire", payload)
        assert denied.status_code == 503
        # The menu bar saves through this authenticated, versioned owner API.
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:11435",
            headers={"Authorization": "Bearer " + app.state.control_credentials.control_token},
        ) as owner:
            preferences = (await owner.get("/v1/node/preferences")).json()
            updated = await owner.patch(
                "/v1/node/preferences",
                json={
                    "expected_version": preferences["version"],
                    "values": {"background_policy": "always"},
                },
            )
            assert updated.status_code == 200, updated.text
            assert updated.json()["values"]["background_policy"] == "always"
        grant = await rpc(client, origin, "/peer/v1/acquire", payload)
        assert grant.status_code == 200, grant.text
        assert grant.json()["workload"] == "background"
        result = await rpc(
            client,
            origin,
            "/peer/v1/execute",
            {
                "path": "/api/generate",
                "token": grant.json()["token"],
                "payload": {"model": "tiny", "prompt": "hi", "stream": False},
            },
        )
        assert result.status_code == 200, result.text
        assert executions == ["/api/generate"]


@pytest.mark.parametrize("activity", ["active", "unknown"])
async def test_peer_cannot_claim_interactive_to_bypass_live_owner_policy(
    tmp_path, monkeypatch, activity
):
    async with policy_worker(tmp_path / "worker", monkeypatch) as (
        _,
        sensor,
        executions,
        origin,
        client,
    ):
        # Any earlier advisory capacity snapshot can be stale; the owner samples now.
        if activity == "active":
            sensor.idle_seconds = 0
        else:
            sensor.confidence = "unknown"
            sensor.idle_seconds = None
        response = await rpc(
            client,
            origin,
            "/peer/v1/acquire",
            {
                "model": "tiny",
                "request_id": "r1",
                "workload": "interactive",
            },
        )
        assert response.status_code >= 400, response.text
        assert not executions


async def test_peer_reservation_rechecks_owner_policy_before_execution(tmp_path, monkeypatch):
    async with policy_worker(tmp_path / "worker", monkeypatch) as (
        _,
        sensor,
        executions,
        origin,
        client,
    ):
        grant = await rpc(
            client,
            origin,
            "/peer/v1/acquire",
            {
                "model": "tiny",
                "request_id": "r1",
                "workload": "interactive",
                "idle_only": True,
            },
        )
        assert grant.status_code == 200, grant.text
        assert grant.json()["workload"] == "background"
        assert grant.json()["idle_only"] is True
        sensor.idle_seconds = 0
        response = await rpc(
            client,
            origin,
            "/peer/v1/execute",
            {
                "path": "/api/generate",
                "token": grant.json()["token"],
                "payload": {"model": "tiny", "prompt": "hi", "stream": False},
            },
        )
        assert response.status_code >= 400, response.text
        assert not executions


async def test_local_pregrant_preserves_header_policy(tmp_path, monkeypatch):
    async with policy_worker(tmp_path / "worker", monkeypatch) as (app, _, _, _, _):
        app.state.directory = SimpleNamespace(candidates=lambda model: [object()])
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as local:
            response = await local.post(
                "/api/generate",
                headers={"x-idle-herd-workload": "background", "x-idle-herd-idle-only": "true"},
                json={"model": "tiny", "prompt": "hi", "stream": False},
            )
        assert response.status_code == 200, response.text
        row = await (
            await app.state.ledger._db.execute("SELECT workload, idle_only FROM leases")
        ).fetchone()
        assert tuple(row) == ("background", 1)
