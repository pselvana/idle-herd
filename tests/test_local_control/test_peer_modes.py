"""Authenticated stale peer requests cannot bypass a worker's owner mode."""

import asyncio
from types import SimpleNamespace

import httpx
import pytest

from fleet_manager.local_control.models import ModeUpdate
from tests.test_peer.test_policy_rpc import policy_worker, rpc


@pytest.mark.parametrize("mode", ["draining", "offline", "paused"])
async def test_stale_peer_grant_fenced_even_through_localhost(tmp_path, monkeypatch, mode):
    async with policy_worker(tmp_path / "worker", monkeypatch) as (
        app,
        _,
        executions,
        origin,
        client,
    ):
        grant = await rpc(client, origin, "/peer/v1/acquire", {"model": "tiny"})
        assert grant.status_code == 200
        token = grant.json()["token"]
        await app.state.ledger.set_mode(ModeUpdate(mode=mode))
        assert (await app.state.ledger.inspect(token)).state == "released"
        denied = await rpc(client, origin, "/peer/v1/acquire", {"model": "tiny"})
        assert denied.status_code == 503
        denied = await rpc(
            client,
            origin,
            "/peer/v1/execute",
            {
                "path": "/api/generate",
                "token": token,
                "payload": {"model": "tiny", "prompt": "secret", "stream": False},
            },
        )
        assert denied.status_code >= 400
        # A peer cannot hand its grant to a localhost caller to evade the mode.
        app.state.directory = None
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as local:
            denied = await local.post(
                "/api/generate",
                headers={
                    "x-idle-herd-reservation": token,
                },
                json={"model": "tiny", "prompt": "secret", "stream": False},
            )
            assert denied.status_code >= 400
            assert not executions
            response = await local.post(
                "/api/generate",
                json={
                    "model": "tiny",
                    "prompt": "owner",
                    "stream": False,
                },
            )
            assert response.status_code == (503 if mode == "paused" else 200)


async def test_offline_stops_outbound_dispatch_and_capacity_probes(tmp_path, monkeypatch):
    async with policy_worker(tmp_path / "worker", monkeypatch) as (app, _, _, _, _):

        def forbidden(*args):
            raise AssertionError("offline must not select a remote worker")

        app.state.directory = SimpleNamespace(
            candidates=forbidden,
            snapshot=lambda: [],
            ttl=5,
            interval=1,
        )
        await app.state.ledger.set_mode(ModeUpdate(mode="offline"))
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as client:
            response = await client.post(
                "/api/generate",
                json={
                    "model": "tiny",
                    "stream": False,
                },
            )
            assert response.status_code == 200
            capacity = await client.get("/v1/cluster/capacity", params={"model": "tiny"})
            assert capacity.status_code == 200
            assert capacity.json()["observed_available_slots"] == 1


async def test_inflight_remote_execution_drains_after_offline(tmp_path, monkeypatch):
    async with policy_worker(tmp_path / "worker", monkeypatch) as (app, _, _, origin, client):
        grant = await rpc(client, origin, "/peer/v1/acquire", {"model": "tiny"})
        lease = await app.state.ledger.consume(grant.json()["token"], "tiny:latest")
        # Simulate the backend boundary holding running execution. The same
        # transport's completion attestation is the only operation that frees it.
        await app.state.ledger.set_mode(ModeUpdate(mode="offline"))
        assert (await app.state.ledger.inspect(lease.token)).state == "running"
        await asyncio.sleep(0)
        assert (await app.state.ledger.capacity(lease.model)).observed_available_slots == 0
        inspected = await rpc(
            client,
            origin,
            "/peer/v1/inspect",
            {
                "path": "/api/generate",
                "token": lease.token,
                "payload": {},
            },
        )
        assert inspected.status_code == 200
        await app.state.ledger.finish(lease.token, stopped=True)
        assert (await app.state.ledger.capacity(lease.model)).observed_available_slots == 1
