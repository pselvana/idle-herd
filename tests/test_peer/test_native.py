"""Separate native processes prove kill/restart fencing, without a real model."""

import asyncio
import json
import os
import sys
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest

from fleet_manager.peer.local_ledger import LocalLedger
from fleet_manager.peer.models import AcquireRequest, AdmissionError, PeerSettings, ResourceFit

RUNNER = Path(__file__).with_name("native_runner.py")


@asynccontextmanager
async def native(tmp_path, role, *args):
    log = tmp_path / f"{role}-{uuid.uuid4()}.log"
    with log.open("wb") as stderr:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(RUNNER),
            role,
            *map(str, args),
            stdout=asyncio.subprocess.PIPE,
            stderr=stderr,
        )
        try:
            line = await asyncio.wait_for(process.stdout.readline(), 20)
            assert line, log.read_text()
            yield process, json.loads(line)["url"]
        finally:
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 10)
                except TimeoutError:
                    process.kill()
                    await process.wait()


async def eventually_state(client, backend_url, expected):
    async with asyncio.timeout(5):
        while True:
            state = (await client.get(backend_url + "/state")).json()
            if state["active"] == expected:
                return state
            await asyncio.sleep(0.02)


async def test_native_single_peer_streaming_embeddings_and_running_crash(tmp_path):
    data = tmp_path / "worker"
    async with httpx.AsyncClient(trust_env=False, timeout=5) as client:
        async with native(tmp_path, "backend") as (_, backend_url):
            async with native(tmp_path, "peer", data, backend_url) as (process, peer_url):
                # Real HTTP, native processes, fabricated output, no remote peers.
                replies = await asyncio.gather(
                    *(
                        client.post(
                            peer_url + "/v1/capacity/acquire",
                            json={
                                "request_id": f"origin-{i % 2}/request-{i}",
                                "model": "tiny",
                            },
                        )
                        for i in range(100)
                    )
                )
                assert sum(reply.status_code == 200 for reply in replies) == 1
                assert sum(reply.status_code == 429 for reply in replies) == 99
                granted = next(reply.json() for reply in replies if reply.status_code == 200)
                await client.delete(
                    peer_url + "/v1/capacity/reservations/" + granted["reservation_id"],
                    headers={"X-Idle-Herd-Reservation": granted["token"]},
                )
                normal = await client.post(
                    peer_url + "/v1/chat/completions",
                    json={
                        "model": "tiny",
                        "messages": [],
                        "stream": False,
                    },
                )
                assert normal.status_code == 200, normal.text
                assert normal.json()["choices"][0]["message"]["content"] == "hello"
                embeddings = await client.post(
                    peer_url + "/v1/embeddings",
                    json={
                        "model": "tiny",
                        "input": "hi",
                    },
                )
                assert embeddings.status_code == 200, embeddings.text
                identity = (await client.get(peer_url + "/health")).json()["worker_id"]
                lease = (
                    await client.post(
                        peer_url + "/v1/capacity/acquire",
                        json={
                            "model": "tiny",
                            "request_id": "crash-running",
                            "ttl_ms": 1000,
                        },
                    )
                ).json()
                await client.post(backend_url + "/block")
                async with client.stream(
                    "POST",
                    peer_url + "/v1/chat/completions",
                    headers={"X-Idle-Herd-Reservation": lease["token"]},
                    json={"model": "tiny", "messages": [], "stream": True},
                ) as response:
                    async for line in response.aiter_lines():
                        if "hello" in line:
                            break
                    state = await eventually_state(client, backend_url, 1)
                    assert state["peak"] == 1
                    process.kill()  # No lifespan shutdown, DB cleanup or lease release.
                    await process.wait()
                # Killing ingress didn't stop the independently running backend.
                assert (await client.get(backend_url + "/state")).json()["active"] == 1
            async with native(tmp_path, "peer", data, backend_url) as (_, peer_url):
                assert (await client.get(peer_url + "/health")).json()["worker_id"] == identity
                capacity = (await client.get(peer_url + "/v1/cluster/capacity?model=tiny")).json()
                assert capacity["quarantined_slots"] == 1
                assert capacity["observed_available_slots"] == 0
                stale = await client.get(
                    peer_url + "/v1/capacity/reservations/" + lease["reservation_id"],
                    headers={"X-Idle-Herd-Reservation": lease["token"]},
                )
                assert stale.status_code == 409
                assert stale.json()["error"]["type"] == "stale_incarnation"
                denied = await client.post(
                    peer_url + "/v1/capacity/acquire",
                    json={
                        "model": "tiny",
                        "request_id": "must-not-overbook",
                    },
                )
                assert denied.status_code == 429
                assert (await client.get(backend_url + "/state")).json()["started"] == 2
                await client.post(backend_url + "/finish")
                await eventually_state(client, backend_url, 0)
                # Even /api/ps empty and orphan eventually ending are not a
                # request-specific backend acknowledgment to the new daemon.
                capacity = (await client.get(peer_url + "/v1/cluster/capacity?model=tiny")).json()
                assert capacity["quarantined_slots"] == 1
        # Both peer and backend are now confirmed stopped, so attest offline.
        env = {**os.environ, "IDLE_HERD_DATA_DIR": str(data)}
        repair = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "fleet_manager.cli.peer_cli",
            "reconcile",
            "--confirm-backend-stopped",
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(repair.communicate(), 10)
        assert repair.returncode == 0, stderr.decode()
        assert "released" in stdout.decode()
        async with (
            native(tmp_path, "backend") as (_, backend_url),
            native(tmp_path, "peer", data, backend_url) as (_, peer_url),
        ):
            replacement = (
                await client.post(
                    peer_url + "/v1/capacity/acquire",
                    json={
                        "model": "tiny",
                        "request_id": "after-repair",
                    },
                )
            ).json()
            assert replacement["slot_generation"] > lease["slot_generation"]
            response = await client.post(
                peer_url + "/api/chat",
                headers={"X-Idle-Herd-Reservation": replacement["token"]},
                json={"model": "tiny", "messages": [], "stream": False},
            )
            assert response.status_code == 200, response.text


async def test_native_crash_after_commit_before_origin_receives_reply(tmp_path):
    # Fabricate the exact lost-response window: commit admission, abruptly exit
    # without close(), then inspect persisted evidence from a new process owner.
    script = """
import asyncio, os, sys
from fleet_manager.peer.local_ledger import LocalLedger
from fleet_manager.peer.models import PeerSettings, ResourceFit, AcquireRequest
from fleet_manager.peer.policy import IdlePolicy
from fleet_manager.peer.sensors import IdleSignal
class Sensor:
    async def sample(self):
        import time
        return IdleSignal(observed_at=time.time(), source="test", memory_available_gb=48)
async def fit(model):
    return ResourceFit(model=model, required_gb=1, available_gb=100)
async def run():
    settings = PeerSettings(data_dir=sys.argv[1])
    ledger = LocalLedger(settings, fit, IdlePolicy(settings, Sensor()))
    await ledger.open()
    lease = await ledger.acquire(AcquireRequest(model="tiny", request_id="lost"))
    print(lease.model_dump_json(), flush=True)
    os._exit(0)
asyncio.run(run())
"""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        script,
        str(tmp_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
    assert process.returncode == 0, stderr.decode()
    lost = json.loads(stdout)

    async def fit(model):
        return ResourceFit(model=model, required_gb=1, available_gb=100)

    worker = LocalLedger(PeerSettings(data_dir=str(tmp_path)), fit)
    await worker.open()
    try:
        assert worker.worker_id == lost["worker_id"]
        with pytest.raises(AdmissionError, match="stale_incarnation"):
            await worker.consume(lost["token"], "tiny:latest")
        lease = await worker.acquire(AcquireRequest(model="tiny", request_id="new-origin-attempt"))
        assert lease.slot_generation > lost["slot_generation"]
    finally:
        await worker.close()


async def test_native_headless_background_without_activity_helper_or_peers(tmp_path):
    async with (
        httpx.AsyncClient(trust_env=False, timeout=5) as client,
        native(tmp_path, "backend") as (_, backend_url),
        native(tmp_path, "peer", tmp_path / "worker", backend_url, "headless") as (_, peer_url),
    ):
        status = (await client.get(peer_url + "/v1/node/eligibility")).json()
        assert status["eligible"] and status["activity"] == "unknown"
        assert status["policy"] == "utilization_only"
        response = await client.post(
            peer_url + "/api/embed",
            headers={"X-Idle-Herd-Workload": "background"},
            json={"model": "tiny", "input": "hi"},
        )
        assert response.status_code == 200, response.text
        response = await client.post(
            peer_url + "/api/embed",
            headers={"X-Idle-Herd-Workload": "background", "X-Idle-Herd-Idle-Only": "true"},
            json={"model": "tiny", "input": "hi"},
        )
        assert response.status_code == 503, response.text
        assert "activity_unknown" in response.json()["error"]
