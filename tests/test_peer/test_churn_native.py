"""Seeded native-process churn over signed TLS RPC and fabricated backends.

Partitions are application-level RPC filters, not physical network or sleep/wake tests.
"""

import asyncio
import json
import random
from contextlib import AsyncExitStack

import httpx
import pytest
from test_jobs_native import wait_job
from test_peer_network import chat, free_port, native, provision, wait_chat


@pytest.mark.parametrize("peer_count", [1, 2, 3, 5])
async def test_seeded_partition_restart_and_local_progress(tmp_path, peer_count, monkeypatch):
    # Fixed seed makes the order reproducible; each peer is isolated and restarted once.
    monkeypatch.delenv("IDLE_HERD_RAFT_URL", raising=False)
    monkeypatch.delenv("IDLE_HERD_RAFT_TOKEN_FILE", raising=False)
    rng = random.Random(9031 + peer_count)
    order = list(range(peer_count))
    rng.shuffle(order)
    paths = [tmp_path / f"peer-{i}" for i in range(peer_count)]
    identities = await provision(paths)
    ports = [free_port() for _ in paths]
    endpoints = [f"https://127.0.0.1:{port}" for port in ports]
    models = [f"model{i}" for i in range(peer_count)]

    async with asyncio.timeout(120), AsyncExitStack() as stack:
        client = await stack.enter_async_context(httpx.AsyncClient(trust_env=False, timeout=10))
        backends = [
            (await stack.enter_async_context(native(tmp_path, "backend", model)))[1]["url"]
            for model in models
        ]
        processes = [None] * peer_count
        urls = [None] * peer_count

        async def start(i):
            seeds = [endpoint for j, endpoint in enumerate(endpoints) if j != i]
            processes[i], info = await stack.enter_async_context(
                native(tmp_path, "peer", paths[i], backends[i], ports[i], json.dumps(seeds))
            )
            urls[i] = info["url"]

        for i in range(peer_count):
            await start(i)
        if peer_count > 1:
            await wait_chat(client, urls[order[0]], models[order[1]])

        for step, isolated in enumerate(order):
            # Reject signed RPC across both sides while retaining loopback ingress.
            for i in range(peer_count):
                blocked = [
                    identities[j].worker_id
                    for j in range(peer_count)
                    if (i == isolated) != (j == isolated)
                ]
                response = await client.post(urls[i] + "/test/block-peers", json=blocked)
                assert response.status_code == 200
            # A stale discovery snapshot must not gate each side's own inference.
            for i in rng.sample(range(peer_count), peer_count):
                response = await chat(client, urls[i], models[i])
                assert response.status_code == 200, response.text
                assert models[i] in response.text

            # One held local slot forces a bounded waiter to wait for live release.
            held = await client.post(
                urls[isolated] + "/v1/capacity/acquire",
                json={"model": models[isolated], "request_id": f"held-{step}", "ttl_ms": 60000},
            )
            assert held.status_code == 201, held.text
            waiter = asyncio.create_task(
                client.post(
                    urls[isolated] + "/v1/capacity/acquire",
                    json={"model": models[isolated], "request_id": f"wait-{step}", "wait_ms": 5000},
                )
            )
            await asyncio.sleep(0.15)
            assert not waiter.done()
            released = await client.delete(
                urls[isolated] + "/v1/capacity/reservations/" + held.json()["reservation_id"],
                headers={"X-Idle-Herd-Reservation": held.json()["token"]},
            )
            assert released.status_code == 200, released.text
            acquired = await waiter
            assert acquired.status_code == 201, acquired.text
            released = await client.delete(
                urls[isolated] + "/v1/capacity/reservations/" + acquired.json()["reservation_id"],
                headers={"X-Idle-Herd-Reservation": acquired.json()["token"]},
            )
            assert released.status_code == 200, released.text

            job = await client.post(
                urls[isolated] + "/v1/jobs",
                json={
                    "model": models[isolated],
                    "request": {"messages": []},
                    "idempotency_key": f"churn-{step}",
                },
            )
            assert job.status_code == 202, job.text
            job_id = job.json()["job_id"]
            await wait_job(client, urls[isolated], job_id, "completed")
            result = await client.get(f"{urls[isolated]}/v1/jobs/{job_id}/result")
            assert result.status_code == 200, result.text
            assert models[isolated] in result.text

            # Abrupt process loss, followed by restart against the same durable ledger.
            processes[isolated].kill()
            await processes[isolated].wait()
            for i in range(peer_count):
                if i != isolated:
                    response = await chat(client, urls[i], models[i])
                    assert response.status_code == 200, response.text
            await start(isolated)
            restored = await client.get(f"{urls[isolated]}/v1/jobs/{job_id}/result")
            assert restored.status_code == 200 and restored.json() == result.json()
            response = await chat(client, urls[isolated], models[isolated])
            assert response.status_code == 200, response.text
            for i in range(peer_count):
                response = await client.post(urls[i] + "/test/block-peers", json=[])
                assert response.status_code == 200
            if peer_count > 1:
                other = next(i for i in range(peer_count) if i != isolated)
                await wait_chat(client, urls[other], models[isolated])

        for backend in backends:
            state = (await client.get(backend + "/state")).json()
            assert state["peak"] <= 1
