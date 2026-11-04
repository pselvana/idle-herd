"""Actual two-process authenticated sharing, interruption and standalone survival."""

import asyncio
import hashlib
import json
import sys
import uuid
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

import httpx
import pytest
from test_peer_network import chat, free_port, provision, wait_for

from fleet_manager.peer.model_sharing import CHUNK_SIZE, ArtifactManifest

RUNNER = Path(__file__).with_name("model_sharing_runner.py")


@asynccontextmanager
async def native(tmp_path, role, *args):
    log = tmp_path / f"sharing-{role}-{uuid.uuid4()}.log"
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
            yield process, json.loads(line)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()


def auth(path):
    token = json.loads((path / "control-credentials.json").read_text())["control_token"]
    return {"Authorization": "Bearer " + token}


@pytest.mark.parametrize("embedding", [False, True])
async def test_native_sharing_resumes_after_source_loss(tmp_path, embedding):
    paths = [tmp_path / "source", tmp_path / "destination"]
    identities = await provision(paths)
    ports = [free_port(), free_port()]
    endpoints = [f"https://127.0.0.1:{port}" for port in ports]
    data = b"GGUF" + b"model contents!" * (CHUNK_SIZE // 5)
    digest = hashlib.sha256(data).hexdigest()
    model_file = tmp_path / "shared.gguf"
    model_file.write_bytes(data)
    shared_model = "shared-embedding" if embedding else "shared"
    manifest = ArtifactManifest(
        model_id=shared_model,
        digest=digest,
        size_bytes=len(data),
        quantization="Q4",
        provenance="test fixture",
        license="test license",
        required_memory_bytes=1,
        manifest_digest="0" * 64,
    )
    manifest.manifest_digest = manifest.calculated_digest()
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(httpx.AsyncClient(trust_env=False, timeout=10))
        backend = [
            (await stack.enter_async_context(native(tmp_path, "backend", model)))[1]["url"]
            for model in ("alpha", "beta")
        ]
        source_process, source = await stack.enter_async_context(
            native(tmp_path, "peer", paths[0], backend[0], ports[0], json.dumps([endpoints[1]]))
        )
        _, dest = await stack.enter_async_context(
            native(tmp_path, "peer", paths[1], backend[1], ports[1], json.dumps([endpoints[0]]))
        )
        source_auth, dest_auth = auth(paths[0]), auth(paths[1])
        response = await client.post(
            source["url"] + "/v1/models/artifacts",
            headers=source_auth,
            json={
                "manifest": manifest.model_dump(),
                "path": str(model_file),
                "allowed_peers": [identities[1].worker_id],
                "sharing_enabled": True,
                "license_approved": True,
            },
        )
        assert response.status_code == 201, response.text

        async def discovered():
            result = await client.get(dest["url"] + "/v1/models/inventory", headers=dest_auth)
            return any(x["digest"] == digest for x in result.json()["models"])

        await wait_for(discovered)
        consent = {
            "digest": digest,
            "manifest_digest": manifest.manifest_digest,
            "source_worker_id": identities[0].worker_id,
            "permission": True,
            "license_accepted": True,
        }
        rejected = await client.post(
            dest["url"] + "/v1/models/transfers",
            headers=dest_auth,
            json={**consent, "permission": False},
        )
        assert rejected.status_code == 403
        response = await client.post(
            dest["url"] + "/v1/models/transfers", headers=dest_auth, json=consent
        )
        assert response.status_code == 202, response.text
        observed = {}

        async def partial():
            nonlocal observed
            response = await client.get(dest["url"] + "/v1/models/transfers", headers=dest_auth)
            observed = response.json()["transfers"][0]
            return 0 < observed["completed_bytes"] < len(data)

        await wait_for(partial)
        source_process.kill()
        await source_process.wait()

        async def failed():
            response = await client.get(dest["url"] + "/v1/models/transfers", headers=dest_auth)
            return response.json()["transfers"][0]["state"] == "failed"

        await wait_for(failed)
        partial_path = paths[1] / "model-sharing" / (digest + ".partial")
        assert partial_path.stat().st_size >= CHUNK_SIZE
        restarted, _ = await stack.enter_async_context(
            native(tmp_path, "peer", paths[0], backend[0], ports[0], json.dumps([endpoints[1]]))
        )
        await wait_for(discovered)
        response = await client.post(
            dest["url"] + f"/v1/models/transfers/{digest}/retry", headers=dest_auth, json=consent
        )
        assert response.status_code == 202, response.text

        async def ready():
            response = await client.get(dest["url"] + "/v1/models/transfers", headers=dest_auth)
            transfer = response.json()["transfers"][0]
            return transfer["state"] == "ready"

        await wait_for(ready, timeout=30)
        assert (paths[1] / "model-sharing" / (digest + ".gguf")).read_bytes() == data
        restarted.kill()
        await restarted.wait()
        assert (await chat(client, dest["url"], "beta")).status_code == 200
        generated = await client.post(
            dest["url"] + ("/api/embed" if embedding else "/api/generate"),
            json={"model": shared_model, "input": "survivor check"}
            if embedding
            else {"model": shared_model, "prompt": "", "stream": False},
        )
        assert generated.status_code == 200, generated.text
        if embedding:
            assert generated.json()["embeddings"] == [[0.1, 0.2]]
        else:
            assert generated.json()["done"] is True
