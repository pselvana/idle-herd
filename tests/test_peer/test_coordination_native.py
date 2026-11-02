"""Coordinator outages do not gate native worker admission or durable jobs."""

import asyncio
import datetime
import ipaddress
import json
import os
import shutil
import socket
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from test_jobs_native import submission, wait_job
from test_native import native


@pytest.fixture(scope="module")
def raft_binary(tmp_path_factory):
    configured = os.environ.get("IDLE_HERD_RAFT_BINARY")
    if configured:
        binary = Path(configured).resolve()
        assert binary.is_file(), f"IDLE_HERD_RAFT_BINARY does not exist: {binary}"
        return binary
    go = shutil.which("go")
    if not go:
        pytest.skip("Go or IDLE_HERD_RAFT_BINARY is required for native coordinator tests")
    binary = tmp_path_factory.mktemp("raft-build") / "idle-herd-raft"
    result = subprocess.run(
        [go, "build", "-o", str(binary), "./..."],
        cwd=Path(__file__).resolve().parents[2] / "coordination",
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return binary


def credentials(path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "idle-herd test CA")])
    now = datetime.datetime.now(datetime.UTC)
    ca = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    node_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    node = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "raft-test")]))
        .issuer_name(name)
        .public_key(node_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    ca_path, cert_path, key_path = (path / name for name in ("ca.pem", "cert.pem", "key.pem"))
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    cert_path.write_bytes(node.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        node_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_path.chmod(0o600)
    return ca_path, cert_path, key_path


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def wait_until(predicate, timeout=20):
    async with asyncio.timeout(timeout):
        while True:
            value = await predicate()
            if value:
                return value
            await asyncio.sleep(0.1)


@asynccontextmanager
async def voters(binary, root, token_file, certs, ports, bootstrap):
    ca, cert, key = certs
    processes = []
    logs = []
    try:
        for index, (raft_port, http_port) in enumerate(ports):
            directory = root / f"voter-{index}"
            directory.mkdir(mode=0o700, exist_ok=True)
            args = [
                str(binary),
                "--id",
                f"voter-{index}",
                "--raft-address",
                f"127.0.0.1:{raft_port}",
                "--http-address",
                f"127.0.0.1:{http_port}",
                "--data-dir",
                str(directory),
                "--token-file",
                str(token_file),
                "--cert",
                str(cert),
                "--key",
                str(key),
                "--ca",
                str(ca),
            ]
            if bootstrap and index == 0:
                args.append("--bootstrap")
                for member, (member_port, _) in enumerate(ports):
                    args.extend(["--voter", f"voter-{member}=127.0.0.1:{member_port}"])
            log = (root / f"voter-{index}-{'initial' if bootstrap else 'restart'}.log").open("wb")
            logs.append(log)
            processes.append(await asyncio.create_subprocess_exec(*args, stdout=log, stderr=log))
        yield processes
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
        await asyncio.gather(*(process.wait() for process in processes))
        for log in logs:
            log.close()


async def test_all_voters_lost_then_restart_preserves_local_work(
    tmp_path, monkeypatch, raft_binary
):
    certs = credentials(tmp_path)
    token_file = tmp_path / "raft-token"
    token_file.write_text("native-test-secret-for-raft-32-bytes-minimum\n")
    token_file.chmod(0o600)
    ports = [(free_port(), free_port()) for _ in range(3)]
    urls = [f"http://127.0.0.1:{port}" for _, port in ports]
    headers = {"Authorization": "Bearer native-test-secret-for-raft-32-bytes-minimum"}
    monkeypatch.setenv("IDLE_HERD_RAFT_URL", urls[0])
    monkeypatch.setenv("IDLE_HERD_RAFT_TOKEN_FILE", str(token_file))
    data = tmp_path / "peer"

    async with httpx.AsyncClient(trust_env=False, timeout=10) as client:

        async def leader():
            for url in urls:
                try:
                    response = await client.get(url + "/v1/status", headers=headers)
                    if (
                        response.status_code == 200
                        and response.json().get("state", "").lower() == "leader"
                        and response.json().get("quorum")
                    ):
                        return url
                except httpx.TransportError:
                    pass
            return None

        async with voters(raft_binary, tmp_path, token_file, certs, ports, True) as processes:
            leader_url = await wait_until(leader)
            monkeypatch.setenv("IDLE_HERD_RAFT_URL", leader_url)
            written = await client.put(
                leader_url + "/v1/metadata",
                headers=headers,
                json={"key": "cluster_label", "value": "before-outage"},
            )
            assert written.status_code == 200, written.text
            async with (
                native(tmp_path, "backend") as (_, backend),
                native(tmp_path, "peer", data, backend) as (_, peer),
            ):
                read_token = json.loads((data / "control-credentials.json").read_text())[
                    "read_token"
                ]
                peer_headers = {"Authorization": "Bearer " + read_token}
                control_token = json.loads((data / "control-credentials.json").read_text())[
                    "control_token"
                ]
                forwarded = await client.put(
                    peer + "/v1/node/coordination/metadata",
                    headers={"Authorization": "Bearer " + control_token},
                    json={"key": "operator_note", "value": "scoped owner API"},
                )
                assert forwarded.status_code == 200, forwarded.text
                assert forwarded.json()["consistent"]
                assert forwarded.json()["metadata"]["operator_note"] == "scoped owner API"

                async def peer_coordination(expected):
                    status = await client.get(peer + "/v1/node/status", headers=peer_headers)
                    assert status.status_code == 200, status.text
                    coordination = status.json()["coordination"]
                    return coordination if coordination["reachable"] is expected else None

                up = await wait_until(lambda: peer_coordination(True))
                assert up["enabled"] and up["quorum"] is True
                assert up["state"] and up["leader"]
                capacity = (await client.get(peer + "/v1/cluster/capacity?model=tiny")).json()
                assert capacity["quorum"] == up["quorum"]
                lease_response = await client.post(
                    peer + "/v1/capacity/acquire",
                    json={"model": "tiny", "request_id": "before-loss", "ttl_ms": 60000},
                )
                assert lease_response.status_code == 201, lease_response.text
                lease = lease_response.json()
                waiter = asyncio.create_task(
                    client.post(
                        peer + "/v1/capacity/acquire",
                        json={"model": "tiny", "request_id": "wait-through-loss", "wait_ms": 10000},
                    )
                )
                await asyncio.sleep(0.2)
                assert not waiter.done()
                for process in processes:
                    process.kill()
                await asyncio.gather(*(process.wait() for process in processes))
                down = await wait_until(lambda: peer_coordination(False))
                assert down["enabled"] and down["quorum"] is None
                assert not down["local_write_available"]
                unavailable = await client.put(
                    peer + "/v1/node/coordination/metadata",
                    headers={"Authorization": "Bearer " + control_token},
                    json={"key": "cluster_label", "value": "must not commit"},
                )
                assert unavailable.status_code == 503, unavailable.text
                capacity = (await client.get(peer + "/v1/cluster/capacity?model=tiny")).json()
                assert capacity["quorum"] == down["quorum"]
                inference = await client.post(
                    peer + "/v1/chat/completions",
                    headers={"X-Idle-Herd-Reservation": lease["token"]},
                    json={"model": "tiny", "messages": [], "stream": False},
                )
                assert inference.status_code == 200, inference.text
                assert inference.json()["choices"][0]["message"]["content"] == "hello"
                acquired = await waiter
                assert acquired.status_code == 201, acquired.text
                release = await client.delete(
                    peer + "/v1/capacity/reservations/" + acquired.json()["reservation_id"],
                    headers={"X-Idle-Herd-Reservation": acquired.json()["token"]},
                )
                assert release.status_code == 200, release.text
                job = await client.post(peer + "/v1/jobs", json=submission("coordination-outage"))
                assert job.status_code == 202, job.text
                job_id = job.json()["job_id"]
                await wait_job(client, peer, job_id, "completed")
                result = await client.get(f"{peer}/v1/jobs/{job_id}/result")
                assert result.status_code == 200, result.text
                async with voters(raft_binary, tmp_path, token_file, certs, ports, False):
                    recovered_leader = await wait_until(leader)
                    restored = await client.get(recovered_leader + "/v1/metadata", headers=headers)
                    assert restored.status_code == 200, restored.text
                    assert restored.json()["metadata"]["cluster_label"] == "before-outage"
                    assert restored.json()["consistent"] is True
                    changed = await client.put(
                        recovered_leader + "/v1/metadata",
                        headers=headers,
                        json={"key": "cluster_label", "value": "after-restart"},
                    )
                    assert changed.status_code == 200, changed.text
                    await wait_until(lambda: peer_coordination(True))
                    assert (
                        await client.get(f"{peer}/v1/jobs/{job_id}/result")
                    ).json() == result.json()
                    response = await client.post(
                        peer + "/api/embed", json={"model": "tiny", "input": "after restart"}
                    )
                    assert response.status_code == 200, response.text
