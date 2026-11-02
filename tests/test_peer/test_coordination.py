"""Admin consensus can never grant work or become an inference dependency."""

import asyncio
import time

import httpx
import pytest
from pydantic import ValidationError

from fleet_manager.peer.app import create_peer_app
from fleet_manager.peer.coordination import (
    AdminCoordination,
    CoordinationUnavailable,
    MetadataUpdate,
    preferred_peer,
)
from fleet_manager.peer.models import PeerSettings


def test_advice_is_deterministic_per_component_and_does_not_claim_uniqueness():
    peers = ["a", "b", "c"]
    chosen = preferred_peer("cluster", peers)
    assert chosen == preferred_peer("cluster", list(reversed(peers)) + peers)
    assert preferred_peer("cluster", [chosen]) == chosen
    assert preferred_peer("cluster", []) is None
    assert preferred_peer("cluster", [p for p in peers if p != chosen]) != chosen


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com:9000",
        "http://127.0.0.1:11435",
        "http://127.0.0.1",
        "http://127.0.0.1:0",
        "https://127.0.0.1:9000",
        "http://user@127.0.0.1:9000",
        "http://127.0.0.1:9000/path",
    ],
)
def test_coordination_address_is_explicit_private_loopback(url):
    with pytest.raises(ValidationError):
        PeerSettings(raft_url=url)


async def test_status_expires_and_malformed_or_unauthenticated_sidecar_is_unavailable(tmp_path):
    token = tmp_path / "token"
    token.write_text("a" * 32)
    token.chmod(0o600)
    healthy = True

    def handler(request):
        assert request.headers["authorization"] == "Bearer " + "a" * 32
        if healthy:
            return httpx.Response(
                200, json={"node_id": "a", "state": "Leader", "leader": "a", "quorum": True}
            )
        return httpx.Response(401)

    admin = AdminCoordination(
        PeerSettings(raft_url="http://127.0.0.1:19000", raft_token_file=str(token)),
        transport=httpx.MockTransport(handler),
    )
    try:
        await admin.refresh()
        assert admin.snapshot().quorum is True
        admin._observed = time.monotonic() - 4
        assert admin.snapshot().quorum is None
        assert not admin.snapshot().reachable
        healthy = False
        await admin.refresh()
        assert admin.snapshot().quorum is None
        assert not admin.snapshot().reachable
        with pytest.raises(CoordinationUnavailable):
            await admin.metadata()
    finally:
        await admin.close()


async def test_missing_sidecar_credentials_dont_block_startup_capacity_waits_or_inference(tmp_path):
    def backend(request):
        if request.url.path == "/api/tags":
            return httpx.Response(200, json={"models": [{"name": "tiny:latest", "size": 1000}]})
        if request.url.path == "/api/ps":
            return httpx.Response(200, json={"models": []})
        return httpx.Response(200, json={"model": "tiny:latest", "embeddings": [[1.0]]})

    app = create_peer_app(
        PeerSettings(
            data_dir=str(tmp_path), raft_url="http://127.0.0.1:19000", reserve_memory_gb=0
        ),
        backend_transport=httpx.MockTransport(backend),
    )
    async with app.router.lifespan_context(app):
        creds = app.state.control_credentials
        read = {"Authorization": "Bearer " + creds.read_token}
        control = {"Authorization": "Bearer " + creds.control_token}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://localhost"
        ) as client:
            status = (await client.get("/v1/node/status", headers=read)).json()
            assert status["coordination"]["enabled"]
            assert status["coordination"]["quorum"] is None
            assert status["coordination"]["preferred_peer"] == status["worker_id"]
            assert (await client.get("/v1/cluster/capacity?model=tiny")).json()["quorum"] is None
            path = "/v1/node/coordination/metadata"
            assert (await client.get(path)).status_code == 401
            assert (
                await client.put(path, headers=read, json={"key": "cluster_label", "value": "a"})
            ).status_code == 403
            assert (
                await client.put(path, headers=control, json={"key": "leases", "value": "a"})
            ).status_code == 422
            oversized = await client.put(path, headers=control, content=b"x" * 4097)
            assert oversized.status_code == 413
            assert oversized.json()["error"]["type"] == "admin_request_too_large"
            assert (
                await client.put(path, headers=control, json={"key": "cluster_label", "value": "a"})
            ).status_code == 503
            hold = await client.post("/v1/capacity/acquire", json={"model": "tiny", "wait_ms": 0})
            assert hold.status_code == 201, hold.text
            pending = asyncio.create_task(
                client.post("/v1/capacity/acquire", json={"model": "tiny", "wait_ms": 3000})
            )
            await asyncio.sleep(0.1)
            assert not pending.done()
            lease = hold.json()
            await client.delete(
                "/v1/capacity/reservations/" + lease["reservation_id"],
                headers={"X-Idle-Herd-Reservation": lease["token"]},
            )
            acquired = await pending
            assert acquired.status_code == 201, acquired.text
            response = await client.post(
                "/api/embed",
                json={"model": "tiny", "input": "hi"},
                headers={"X-Idle-Herd-Reservation": acquired.json()["token"]},
            )
            assert response.status_code == 200, response.text
            await client.put("/v1/node/mode", headers=control, json={"mode": "offline"})
            offline = (await client.get("/v1/node/status", headers=read)).json()
            assert offline["coordination"]["preferred_peer"] is None
            # Withdrawing from cluster advice still retains local inference.
            assert (
                await client.post("/api/embed", json={"model": "tiny", "input": "offline"})
            ).status_code == 200


@pytest.mark.parametrize(
    "key", ["worker_leases", "jobs", "peer_acl", "idle_policy", "model_permissions"]
)
def test_runtime_authority_is_excluded_from_admin_fsm_contract(key):
    with pytest.raises(ValidationError):
        MetadataUpdate(key=key, value="override")
