"""Durable jobs across real daemon crashes and independent native peers."""

import asyncio
import json

import httpx
import pytest
from test_native import eventually_state, native


def submission(key):
    return {"model": "tiny", "request": {"messages": []}, "idempotency_key": key}


async def wait_job(client, url, job_id, state):
    async with asyncio.timeout(10):
        while True:
            response = await client.get(f"{url}/v1/jobs/{job_id}")
            assert response.status_code == 200, response.text
            record = response.json()
            if record["state"] == state:
                return record
            await asyncio.sleep(0.05)


async def test_native_completed_job_survives_daemon_restart(tmp_path):
    data = tmp_path / "worker"
    async with (
        httpx.AsyncClient(trust_env=False, timeout=5) as client,
        native(tmp_path, "backend") as (_, backend),
    ):
        async with native(tmp_path, "peer", data, backend) as (process, peer):
            response = await client.post(peer + "/v1/jobs", json=submission("persist"))
            assert response.status_code == 202, response.text
            job_id = response.json()["job_id"]
            await wait_job(client, peer, job_id, "completed")
            result = (await client.get(f"{peer}/v1/jobs/{job_id}/result")).json()
            assert (
                json.loads(result["result"]["body"])["choices"][0]["message"]["content"] == "hello"
            )
            process.kill()
            await process.wait()
        async with native(tmp_path, "peer", data, backend) as (_, peer):
            await wait_job(client, peer, job_id, "completed")
            restored = await client.get(f"{peer}/v1/jobs/{job_id}/result")
            assert restored.json() == result
            repeated = await client.post(peer + "/v1/jobs", json=submission("persist"))
            assert repeated.status_code == 202
            assert repeated.json()["job_id"] == job_id
            assert (await client.get(backend + "/state")).json()["started"] == 1


async def test_native_waiting_job_survives_daemon_crash(tmp_path):
    data = tmp_path / "worker"
    async with (
        httpx.AsyncClient(trust_env=False, timeout=5) as client,
        native(tmp_path, "backend") as (_, backend),
    ):
        async with native(tmp_path, "peer", data, backend) as (process, peer):
            reservation = await client.post(
                peer + "/v1/capacity/acquire",
                json={"model": "tiny", "request_id": "hold-slot", "ttl_ms": 60000},
            )
            assert reservation.status_code == 201, reservation.text
            response = await client.post(peer + "/v1/jobs", json=submission("waiting"))
            assert response.status_code == 202, response.text
            job_id = response.json()["job_id"]
            await wait_job(client, peer, job_id, "reserving")
            assert (await client.get(backend + "/state")).json()["started"] == 0
            process.kill()
            await process.wait()
        async with native(tmp_path, "peer", data, backend) as (_, peer):
            await wait_job(client, peer, job_id, "completed")
            result = await client.get(f"{peer}/v1/jobs/{job_id}/result")
            assert result.status_code == 200, result.text
            assert (await client.get(backend + "/state")).json()["started"] == 1


async def test_native_unsafe_running_job_is_not_retried_after_crash(tmp_path):
    data = tmp_path / "worker"
    async with (
        httpx.AsyncClient(trust_env=False, timeout=5) as client,
        native(tmp_path, "backend") as (_, backend),
    ):
        await client.post(backend + "/block")
        async with native(tmp_path, "peer", data, backend) as (process, peer):
            response = await client.post(peer + "/v1/jobs", json=submission("unsafe"))
            assert response.status_code == 202, response.text
            job_id = response.json()["job_id"]
            await eventually_state(client, backend, 1)
            process.kill()
            await process.wait()
        async with native(tmp_path, "peer", data, backend) as (_, peer):
            await wait_job(client, peer, job_id, "unknown_outcome")
            await asyncio.sleep(0.6)
            state = (await client.get(backend + "/state")).json()
            assert state["started"] == 1 and state["active"] == 1
            capacity = (await client.get(peer + "/v1/cluster/capacity?model=tiny")).json()
            assert capacity["quarantined_slots"] == 1
            await client.post(backend + "/finish")
            await eventually_state(client, backend, 0)


@pytest.mark.parametrize("peer_count", [2, 3])
async def test_native_configured_peer_survives_other_peer_loss(tmp_path, peer_count):
    from contextlib import AsyncExitStack

    from test_peer_network import free_port, provision, wait_chat
    from test_peer_network import native as network_native

    paths = [tmp_path / str(i) for i in range(peer_count)]
    await provision(paths)
    ports = [free_port() for _ in paths]
    endpoints = [f"https://127.0.0.1:{port}" for port in ports]
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(httpx.AsyncClient(trust_env=False, timeout=10))
        backends = [
            (await stack.enter_async_context(network_native(tmp_path, "backend", model)))[1]["url"]
            for model in ("alpha", "beta", "gamma")[:peer_count]
        ]
        peers = [
            await stack.enter_async_context(
                network_native(
                    tmp_path,
                    "peer",
                    paths[i],
                    backends[i],
                    ports[i],
                    json.dumps([endpoint for j, endpoint in enumerate(endpoints) if j != i]),
                )
            )
            for i in range(peer_count)
        ]
        survivor = peers[0][1]
        for model in ("beta", "gamma")[: peer_count - 1]:
            await wait_chat(client, survivor["url"], model)
        for process, _ in peers[1:]:
            process.kill()
            await process.wait()
        spec = {"model": "alpha", "request": {"messages": []}, "idempotency_key": "survivor"}
        response = await client.post(survivor["url"] + "/v1/jobs", json=spec)
        assert response.status_code == 202, response.text
        job_id = response.json()["job_id"]
        await wait_job(client, survivor["url"], job_id, "completed")
        result = await client.get(f"{survivor['url']}/v1/jobs/{job_id}/result")
        assert "alpha" in result.json()["result"]["body"]
