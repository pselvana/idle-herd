"""Opt-in Docker integration. Requires a working Docker daemon and Compose v2.

Run: IDLE_HERD_TEST_CONTAINERS=1 uv run pytest -q tests/test_container/test_compose_integration.py
These tests run only fabricated inference, not a real model or host hardware.
"""

import asyncio
import base64
import json
import os
import uuid
from pathlib import Path

import httpx
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("IDLE_HERD_TEST_CONTAINERS") != "1",
    reason="set IDLE_HERD_TEST_CONTAINERS=1 to run Docker Compose integration tests",
)
ROOT = Path(__file__).resolve().parents[2]


class Compose:
    def __init__(self, tmp_path, count):
        self.project = "idleherdtest" + uuid.uuid4().hex[:12]
        self.file = tmp_path / "compose.json"
        self.image = self.project + ":test"
        services = {}
        volumes = {}
        subnet_octet = int(uuid.uuid4().hex[:2], 16)
        subnet = f"172.28.{subnet_octet}.0/24"
        for i in range(count):
            name = f"peer{i}"
            backend = f"backend{i}"
            seeds = [
                f"https://172.28.{subnet_octet}.{10 + j}:11500" for j in range(count) if j != i
            ]
            services[name] = {
                "image": self.image,
                "build": {"context": str(ROOT), "dockerfile": "container/Dockerfile.peer"},
                "environment": {
                    "IDLE_HERD_CONTAINER_BACKEND_URL": f"http://{backend}:11434",
                    "IDLE_HERD_PEER_ENDPOINT": f"https://172.28.{subnet_octet}.{10 + i}:11500",
                    "IDLE_HERD_SEEDS": json.dumps(seeds),
                    "IDLE_HERD_PEER_INTERVAL": "0.2",
                    "IDLE_HERD_PEER_TTL": "2",
                    "IDLE_HERD_REQUEST_MEMORY_GB": "0.000001",
                    "IDLE_HERD_RESERVE_MEMORY_GB": "0",
                    "IDLE_HERD_BACKGROUND_POLICY": "utilization_only",
                    "IDLE_HERD_IDLE_HYSTERESIS_SECONDS": "0",
                    "IDLE_HERD_CPU_BUSY_PERCENT": "99",
                    "IDLE_HERD_CPU_IDLE_PERCENT": "98",
                },
                "volumes": [f"{name}-data:/var/lib/idle-herd"],
                "ports": ["127.0.0.1::8080"],
                "networks": {"default": {"ipv4_address": f"172.28.{subnet_octet}.{10 + i}"}},
                "read_only": True,
                "cap_drop": ["ALL"],
                "security_opt": ["no-new-privileges:true"],
                "tmpfs": ["/tmp:rw,noexec,nosuid,size=64m"],
                "mem_limit": "1g",
                "cpus": 2,
            }
            volumes[f"{name}-data"] = {}
            services[backend] = {
                "image": self.image,
                "entrypoint": ["python", "-m", "uvicorn"],
                "command": [
                    "backend:make_app",
                    "--factory",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "11434",
                ],
                "environment": {"PYTHONPATH": "/fixture", "TEST_MODEL": f"model{i}"},
                "volumes": [f"{ROOT / 'tests/test_container'}:/fixture:ro"],
                "ports": ["127.0.0.1::11434"],
            }
        self.file.write_text(
            json.dumps(
                {
                    "services": services,
                    "volumes": volumes,
                    "networks": {"default": {"ipam": {"config": [{"subnet": subnet}]}}},
                }
            )
        )

    async def run(self, *args, check=True):
        process = await asyncio.create_subprocess_exec(
            "docker",
            "compose",
            "-p",
            self.project,
            "-f",
            str(self.file),
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if check and process.returncode:
            raise AssertionError(
                f"compose {' '.join(args)} failed:\n{stderr.decode()}\n{stdout.decode()}"
            )
        return stdout.decode().strip()

    async def offline(self, index, *args):
        return await self.run(
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "idle-herd",
            f"peer{index}",
            *args,
        )

    async def file_read(self, index, path):
        return await self.run(
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "python",
            f"peer{index}",
            "-c",
            "import sys; print(open(sys.argv[1]).read())",
            path,
        )

    async def file_write(self, index, path, content):
        encoded = base64.b64encode(content.encode()).decode()
        await self.run(
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "python",
            f"peer{index}",
            "-c",
            "import base64,sys; open(sys.argv[1], 'wb').write(base64.b64decode(sys.argv[2]))",
            path,
            encoded,
        )

    async def provision(self, count):
        info = json.loads(await self.offline(0, "init"))
        for i in range(1, count):
            await self.offline(
                i, "enrollment-request", "--output", "/var/lib/idle-herd/request.json"
            )
            request = await self.file_read(i, "/var/lib/idle-herd/request.json")
            await self.file_write(0, "/var/lib/idle-herd/request.json", request)
            await self.offline(
                0,
                "issue-credential",
                "/var/lib/idle-herd/request.json",
                "--output",
                "/var/lib/idle-herd/credential.json",
            )
            credential = await self.file_read(0, "/var/lib/idle-herd/credential.json")
            await self.file_write(i, "/var/lib/idle-herd/credential.json", credential)
            await self.offline(
                i,
                "join",
                "/var/lib/idle-herd/credential.json",
                "--fingerprint",
                info["trust_fingerprint"],
            )
        return info

    async def url(self, name, port):
        address = await self.run("port", name, str(port))
        return "http://" + address.splitlines()[0]


async def eventually(operation, timeout=30):
    async with asyncio.timeout(timeout):
        while True:
            try:
                value = await operation()
                if value:
                    return value
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.2)


async def chat(client, url, model):
    return await client.post(
        url + "/v1/chat/completions",
        json={"model": model, "messages": [{"role": "user", "content": "hi"}], "stream": True},
    )


@pytest.mark.parametrize("count", [1, 2, 3])
async def test_compose_enrollment_churn_and_restart(tmp_path, count):
    compose = Compose(tmp_path, count)
    try:
        await compose.run("build", "peer0")
        initial = await compose.provision(count)
        await compose.run("up", "-d", "--no-build")
        async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
            urls = [await compose.url(f"peer{i}", 8080) for i in range(count)]

            async def works(origin, model):
                response = await chat(client, urls[origin], model)
                return response.status_code == 200 and model in response.text

            for i in range(count):
                await eventually(lambda i=i: works(i, f"model{i}"))
            # Distinct backend models prove signed discovery and remote dispatch.
            for i in range(1, count):
                await eventually(lambda i=i: works(0, f"model{i}"))

            for survivor in range(count):
                others = [f"peer{i}" for i in range(count) if i != survivor]
                if others:
                    await compose.run("stop", *others)
                assert await works(survivor, f"model{survivor}")
                if others:
                    await compose.run("start", *others)
                    urls = [await compose.url(f"peer{i}", 8080) for i in range(count)]
                    for i in range(count):
                        await eventually(lambda i=i: works(i, f"model{i}"))

            spec = {"model": "model0", "request": {"messages": []}, "idempotency_key": "persist"}
            job = await client.post(urls[0] + "/v1/jobs", json=spec)
            assert job.status_code == 202, job.text
            job_id = job.json()["job_id"]

            async def completed():
                response = await client.get(urls[0] + f"/v1/jobs/{job_id}")
                return response.status_code == 200 and response.json()["state"] == "completed"

            await eventually(completed)
            result = await client.get(urls[0] + f"/v1/jobs/{job_id}/result")
            assert result.status_code == 200, result.text
            assert await compose.run("exec", "-T", "peer0", "idle-herd", "status")
            unauthenticated = await compose.run(
                "exec",
                "-T",
                "peer0",
                "python",
                "-c",
                "import httpx; print(httpx.get('http://127.0.0.1:11435/v1/node/status').status_code)",
            )
            assert unauthenticated == "401"
            credentials = json.loads(
                await compose.run(
                    "exec", "-T", "peer0", "cat", "/var/lib/idle-herd/control-credentials.json"
                )
            )
            for token in (None, credentials["read_token"], credentials["control_token"]):
                headers = {"Authorization": "Bearer " + token} if token else {}
                response = await client.get(urls[0] + "/v1/node/status", headers=headers)
                assert response.status_code in (403, 404), response.text
            response = await client.put(
                urls[0] + "/v1/node/mode",
                headers={"Authorization": "Bearer " + credentials["control_token"]},
                json={"mode": "paused"},
            )
            assert response.status_code in (403, 404), response.text
            await compose.run("stop", "peer0")
            assert json.loads(await compose.offline(0, "init")) == initial
            await compose.run("start", "peer0")
            urls[0] = await compose.url("peer0", 8080)
            await eventually(lambda: works(0, "model0"))
            restored = await client.get(urls[0] + f"/v1/jobs/{job_id}/result")
            assert restored.status_code == 200 and restored.json() == result.json()
            repeated = await client.post(urls[0] + "/v1/jobs", json=spec)
            assert repeated.status_code == 202 and repeated.json()["job_id"] == job_id

            # A backend can keep executing after the peer dies. Its slot must remain
            # quarantined and the origin must not silently replay an unsafe job.
            if count == 1:
                backend = await compose.url("backend0", 11434)
                started = (await client.get(backend + "/state")).json()["started"]
                assert (await client.post(backend + "/block")).status_code == 200
                unsafe = await client.post(
                    urls[0] + "/v1/jobs", json={**spec, "idempotency_key": "unsafe"}
                )
                assert unsafe.status_code == 202, unsafe.text
                unsafe_id = unsafe.json()["job_id"]

                async def backend_running():
                    return (await client.get(backend + "/state")).json()["active"] == 1

                await eventually(backend_running)
                await compose.run("kill", "peer0")
                await compose.run("start", "peer0")
                urls[0] = await compose.url("peer0", 8080)

                async def uncertain():
                    response = await client.get(urls[0] + f"/v1/jobs/{unsafe_id}")
                    return (
                        response.status_code == 200
                        and response.json()["state"] == "unknown_outcome"
                    )

                await eventually(uncertain)
                state = (await client.get(backend + "/state")).json()
                assert state["started"] == started + 1 and state["active"] == 1
                capacity = await client.get(urls[0] + "/v1/cluster/capacity?model=model0")
                assert capacity.status_code == 200 and capacity.json()["quarantined_slots"] == 1
                await client.post(backend + "/finish")
    finally:
        await compose.run("down", "--volumes", "--remove-orphans", "--rmi", "all", check=False)


async def test_mixed_native_container_dispatch_and_survivors(tmp_path):
    """Real signed TLS between a native process and container, with fake backends."""
    import socket
    import sys

    from fleet_manager.peer.identity import DeviceCredential, PeerIdentity
    from fleet_manager.peer.local_ledger import LocalLedger
    from fleet_manager.peer.models import PeerSettings

    def free_port():
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(("192.0.2.1", 9))
        host_ip = sock.getsockname()[0]
    native_backend, native_ingress, native_rpc, container_rpc = (free_port() for _ in range(4))
    native_endpoint = f"https://{host_ip}:{native_rpc}"
    container_endpoint = f"https://{host_ip}:{container_rpc}"
    compose = Compose(tmp_path, 1)
    config = json.loads(compose.file.read_text())
    peer = config["services"]["peer0"]
    peer["environment"]["IDLE_HERD_PEER_ENDPOINT"] = container_endpoint
    peer["environment"]["IDLE_HERD_SEEDS"] = json.dumps([native_endpoint])
    peer["ports"].append(f"0.0.0.0:{container_rpc}:{container_rpc}")
    compose.file.write_text(json.dumps(config))
    process = None
    try:
        await compose.run("build", "peer0")
        authority = await compose.provision(1)

        async def unused_fit(model):
            raise AssertionError("offline provisioning cannot admit work")

        data = tmp_path / "native-data"
        ledger = LocalLedger(PeerSettings(data_dir=str(data)), unused_fit)
        await ledger.open()
        try:
            request = await PeerIdentity.enrollment_request(data, ledger.worker_id)
            await compose.file_write(
                0, "/var/lib/idle-herd/native-request.json", request.model_dump_json()
            )
            await compose.offline(
                0,
                "issue-credential",
                "/var/lib/idle-herd/native-request.json",
                "--output",
                "/var/lib/idle-herd/native-credential.json",
            )
            credential = DeviceCredential.model_validate_json(
                await compose.file_read(0, "/var/lib/idle-herd/native-credential.json")
            )
            await PeerIdentity.import_credential(data, credential, authority["trust_fingerprint"])
            await ledger.adopt_cluster(credential.cluster_id)
        finally:
            await ledger.close()
        await compose.run("up", "-d", "--no-build")
        env = {**os.environ, "TEST_MODEL": "native"}
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(Path(__file__).with_name("mixed_runner.py")),
            str(data),
            str(native_backend),
            str(native_ingress),
            str(native_rpc),
            host_ip,
            container_endpoint,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        line = await asyncio.wait_for(process.stdout.readline(), 30)
        assert line, (await process.stderr.read()).decode()
        async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
            container_url = await compose.url("peer0", 8080)
            native_url = f"http://127.0.0.1:{native_ingress}"

            async def works(url, model):
                response = await chat(client, url, model)
                return response.status_code == 200 and model in response.text

            await eventually(lambda: works(container_url, "native"), timeout=45)
            await eventually(lambda: works(native_url, "model0"), timeout=45)
            await compose.run("stop", "peer0")
            assert await works(native_url, "native")
            await compose.run("start", "peer0")
            container_url = await compose.url("peer0", 8080)
            await eventually(lambda: works(container_url, "native"), timeout=45)
            process.kill()
            await process.wait()
            assert await works(container_url, "model0")
    finally:
        if process and process.returncode is None:
            process.kill()
            await process.wait()
        await compose.run("down", "--volumes", "--remove-orphans", "--rmi", "all", check=False)
