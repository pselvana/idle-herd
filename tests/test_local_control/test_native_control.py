"""Native loopback owner-control lifecycle with a mock independent backend."""

import asyncio
import json
import sys
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx

RUNNER = Path(__file__).parents[1] / "test_peer" / "native_runner.py"


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


async def eventually(client, url, predicate):
    async with asyncio.timeout(10):
        while True:
            response = await client.get(url)
            assert response.status_code == 200, response.text
            value = response.json()
            if predicate(value):
                return value
            await asyncio.sleep(0.03)


async def test_native_control_disconnect_and_restart(tmp_path, monkeypatch):
    monkeypatch.setenv("IDLE_HERD_SLOTS", "2")
    monkeypatch.setenv("IDLE_HERD_TEST_TWO_MODELS", "1")
    data = tmp_path / "worker"
    async with (
        httpx.AsyncClient(trust_env=False, timeout=15) as client,
        native(tmp_path, "backend") as (_, backend),
    ):
        async with native(tmp_path, "peer", data, backend) as (process, peer):
            credentials = json.loads((data / "control-credentials.json").read_text())
            read = {"Authorization": "Bearer " + credentials["read_token"]}
            control = {"Authorization": "Bearer " + credentials["control_token"]}
            assert (await client.get(peer + "/v1/node/status")).status_code == 401
            await client.post(backend + "/block")
            reservations = []
            for i in range(2):
                response = await client.post(
                    peer + "/v1/capacity/acquire",
                    json={"model": ("tiny" if i == 0 else "tiny2"), "request_id": f"native-{i}"},
                )
                assert response.status_code == 201, response.text
                reservations.append(response.json())
            jobs = [
                asyncio.create_task(
                    client.post(
                        peer + "/v1/chat/completions",
                        headers={"X-Idle-Herd-Reservation": reservation["token"]},
                        json={"model": reservation["model"], "messages": [], "stream": False},
                    )
                )
                for reservation in reservations
            ]
            try:
                await eventually(client, backend + "/state", lambda s: s["active"] == 2)
                async with httpx.AsyncClient(trust_env=False, headers=read) as ui:
                    snapshot = await eventually(
                        ui, peer + "/v1/node/status", lambda s: s["slots"]["running"] == 2
                    )
                    assert snapshot["slots"]["total"] == 2
                    response = await client.put(
                        peer + "/v1/node/mode",
                        headers=control,
                        json={"mode": "offline", "duration_seconds": 60},
                    )
                    assert response.status_code == 200, response.text
                # Closing the UI client is independent of the daemon and both backend jobs.
                assert process.returncode is None
                assert (await client.get(backend + "/state")).json()["active"] == 2
                assert (await client.get(peer + "/v1/node/status", headers=read)).json()["mode"][
                    "mode"
                ] == "offline"
                await client.post(backend + "/finish")
                replies = await asyncio.gather(*jobs)
                assert all(reply.status_code == 200 for reply in replies)
                local = await client.post(
                    peer + "/api/embed", json={"model": "tiny", "input": "hi"}
                )
                assert local.status_code == 200, local.text
                preferences = (await client.get(peer + "/v1/node/preferences", headers=read)).json()
                updated = await client.patch(
                    peer + "/v1/node/preferences",
                    headers=control,
                    json={
                        "expected_version": preferences["version"],
                        "values": {"require_ac": True},
                    },
                )
                assert updated.status_code == 200, updated.text
                manual = await client.put(
                    peer + "/v1/node/mode", headers=control, json={"mode": "offline"}
                )
                assert manual.status_code == 200, manual.text
            finally:
                await client.post(backend + "/finish")
                await asyncio.gather(*jobs, return_exceptions=True)
        async with native(tmp_path, "peer", data, backend) as (_, peer):
            status = await client.get(peer + "/v1/node/status", headers=read)
            assert status.status_code == 200, status.text
            assert status.json()["mode"]["mode"] == "offline"
            preferences = await client.get(peer + "/v1/node/preferences", headers=read)
            assert preferences.json()["values"]["require_ac"] is True
