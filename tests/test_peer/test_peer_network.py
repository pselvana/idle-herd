"""Native loopback integration for authenticated multi-peer discovery and dispatch."""

import asyncio
import json
import sys
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest

RUNNER = Path(__file__).with_name("network_runner.py")


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
            yield process, json.loads(line)
        finally:
            if process.returncode is None:
                # Two uvicorn listeners share this process; abrupt shutdown also
                # exercises the outage path without waiting on signal handlers.
                if role == "peer":
                    process.kill()
                else:
                    process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 10)
                except TimeoutError:
                    process.kill()
                    await process.wait()


async def wait_for(predicate, timeout=15):
    async with asyncio.timeout(timeout):
        while not await predicate():
            await asyncio.sleep(0.1)


async def backend_state(client: httpx.AsyncClient, backend: str, active: int):
    state = (await client.get(backend + "/state")).json()
    return state["active"] == active


def free_port():
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def provision(paths):
    from fleet_manager.peer.identity import PeerIdentity
    from fleet_manager.peer.local_ledger import LocalLedger
    from fleet_manager.peer.models import PeerSettings

    async def unused_fit(model):
        raise AssertionError("provisioning cannot admit work")

    authority = None
    identities = []
    for path in paths:
        ledger = LocalLedger(PeerSettings(data_dir=str(path)), unused_fit)
        await ledger.open()
        try:
            if authority is None:
                authority = await PeerIdentity.open(path, ledger.worker_id, ledger.cluster_id)
                identity = authority
            else:
                request = await PeerIdentity.enrollment_request(path, ledger.worker_id)
                credential = await authority.issue_credential(request)
                await PeerIdentity.import_credential(path, credential, authority.fingerprint)
                await ledger.adopt_cluster(authority.cluster_id)
                identity = await PeerIdentity.open(path, ledger.worker_id, authority.cluster_id)
            identities.append(identity)
        finally:
            await ledger.close()
    return identities


async def chat(client, peer, model, stream=False):
    return await client.post(
        peer + "/v1/chat/completions",
        json={"model": model, "messages": [{"role": "user", "content": "hi"}], "stream": stream},
    )


async def wait_chat(client, peer, model):
    response = None
    failures = []

    async def works():
        nonlocal response
        response = await chat(client, peer, model, stream=True)
        if (response.status_code != 200 or model not in response.text) and len(failures) < 5:
            failures.append((response.status_code, response.text))
        return response.status_code == 200 and model in response.text

    try:
        await wait_for(works)
    except TimeoutError as exc:
        status = await client.get(peer + "/v1/cluster/peers")
        raise AssertionError(
            f"{model} unavailable: last={response.status_code if response else None} "
            f"{response.text if response else ''}; first failures={failures}; peers={status.text}"
        ) from exc
    return response


async def test_native_two_peers_remote_streaming_and_survivors(tmp_path):
    from contextlib import AsyncExitStack

    paths = [tmp_path / "a", tmp_path / "b"]
    identities = await provision(paths)
    ports = [free_port(), free_port()]
    endpoints = [f"https://127.0.0.1:{port}" for port in ports]
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(httpx.AsyncClient(trust_env=False, timeout=10))
        backends = [
            (await stack.enter_async_context(native(tmp_path, "backend", model)))[1]["url"]
            for model in ("alpha", "beta")
        ]
        first, a = await stack.enter_async_context(
            native(tmp_path, "peer", paths[0], backends[0], ports[0], json.dumps([endpoints[1]]))
        )
        second, b = await stack.enter_async_context(
            native(tmp_path, "peer", paths[1], backends[1], ports[1], json.dumps([endpoints[0]]))
        )
        await wait_chat(client, a["url"], "beta")
        await wait_chat(client, b["url"], "alpha")
        await client.post(backends[1] + "/block")
        try:
            async with client.stream(
                "POST",
                a["url"] + "/v1/chat/completions",
                json={
                    "model": "beta",
                    "messages": [{"role": "user", "content": "hi"}],
                    "stream": True,
                },
            ) as streamed:
                assert streamed.status_code == 200
                lines = streamed.aiter_lines()
                async with asyncio.timeout(5):
                    async for line in lines:
                        if "beta" in line:
                            break
                assert (await client.get(backends[1] + "/state")).json()["active"] == 1
                await client.post(backends[1] + "/finish")
                async for _ in lines:
                    pass
        finally:
            await client.post(backends[1] + "/finish")
        for path, payload in (
            ("/api/chat", {"messages": [{"role": "user", "content": "hi"}]}),
            ("/v1/messages", {"messages": [{"role": "user", "content": "hi"}], "max_tokens": 16}),
            ("/v1/responses", {"input": "hi", "store": False}),
            ("/v1/embeddings", {"input": "hi"}),
        ):
            response = await client.post(a["url"] + path, json={"model": "beta", **payload})
            assert response.status_code == 200, (path, response.text)
            assert ("1.0" if path == "/v1/embeddings" else "beta") in response.text
        before = (await client.get(backends[1] + "/state")).json()["started"]
        await client.post(backends[1] + "/block")
        requests = [asyncio.create_task(chat(client, a["url"], "beta")) for _ in range(20)]
        try:
            await wait_for(lambda: backend_state(client, backends[1], 1))
            async with asyncio.timeout(8):
                while sum(task.done() for task in requests) < 19:
                    await asyncio.sleep(0.05)
            state = (await client.get(backends[1] + "/state")).json()
            assert state["started"] == before + 1
            assert state["peak"] == 1
        finally:
            await client.post(backends[1] + "/finish")
            responses = await asyncio.gather(*requests)
        assert sum(response.status_code == 200 for response in responses) == 1
        # The peer endpoint is TLS protected and requires a signed, fresh envelope.
        from fleet_manager.peer.discovery import Exchange, PeerDescriptor

        body = (
            Exchange(
                sender=PeerDescriptor(
                    cluster_id=identities[0].cluster_id,
                    worker_id=identities[0].worker_id,
                    incarnation="test-incarnation",
                    endpoint=endpoints[0],
                ),
            )
            .model_dump_json()
            .encode()
        )
        path = "/peer/v1/exchange"
        async with httpx.AsyncClient(
            verify=identities[0].client_ssl_context(), trust_env=False, timeout=5
        ) as tls:
            missing = await tls.post(endpoints[1] + path, content=body)
            assert missing.status_code in (401, 403)
            forged_headers = identities[0].sign_request("POST", path, body)
            forged = await tls.post(
                endpoints[1] + path, content=body + b" ", headers=forged_headers
            )
            assert forged.status_code in (401, 403)
            headers = identities[0].sign_request("POST", path, body)
            accepted = await tls.post(endpoints[1] + path, content=body, headers=headers)
            assert accepted.status_code == 200, accepted.text
            replay = await tls.post(endpoints[1] + path, content=body, headers=headers)
            assert replay.status_code in (401, 403)
        first.kill()
        await first.wait()
        response = await chat(client, b["url"], "beta")
        assert response.status_code == 200, response.text
        assert "beta" in response.text
        # Restore A and stop B: each survivor must work independently.
        _, a = await stack.enter_async_context(
            native(tmp_path, "peer", paths[0], backends[0], ports[0], json.dumps([endpoints[1]]))
        )
        await wait_chat(client, a["url"], "beta")
        second.kill()
        await second.wait()
        response = await chat(client, a["url"], "alpha")
        assert response.status_code == 200, response.text
        assert "alpha" in response.text


@pytest.mark.parametrize("survivor", [0, 1, 2])
async def test_native_three_peers_seed_loss_direct_dispatch_and_single_survivor(tmp_path, survivor):
    from contextlib import AsyncExitStack

    paths = [tmp_path / name for name in ("seed", "a", "b")]
    identities = await provision(paths)
    ports = [free_port() for _ in paths]
    endpoints = [f"https://127.0.0.1:{port}" for port in ports]
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(httpx.AsyncClient(trust_env=False, timeout=10))
        backends = [
            (await stack.enter_async_context(native(tmp_path, "backend", model)))[1]["url"]
            for model in ("seedmodel", "alpha", "beta")
        ]
        processes, peers = [], []
        for i in range(3):
            process, peer = await stack.enter_async_context(
                native(
                    tmp_path,
                    "peer",
                    paths[i],
                    backends[i],
                    ports[i],
                    json.dumps([] if i == 0 else [endpoints[0]]),
                )
            )
            processes.append(process)
            peers.append(peer["url"])
        # Both peers must learn and directly contact each other through referrals.
        await wait_chat(client, peers[1], "beta")
        await wait_chat(client, peers[2], "alpha")
        before = (await client.get(backends[2] + "/state")).json()["started"]
        await client.post(backends[2] + "/block")
        calls = [asyncio.create_task(chat(client, peers[i % 2], "beta")) for i in range(100)]
        try:
            await wait_for(lambda: backend_state(client, backends[2], 1))
            async with asyncio.timeout(15):
                while sum(call.done() for call in calls) < 99:
                    await asyncio.sleep(0.05)
            state = (await client.get(backends[2] + "/state")).json()
            assert state["started"] == before + 1
            assert state["peak"] == 1
        finally:
            await client.post(backends[2] + "/finish")
            results = await asyncio.gather(*calls)
        assert sum(result.status_code == 200 for result in results) == 1
        # Filter signed peer traffic in both directions to model a 2+1 partition.
        await client.post(peers[0] + "/test/block-peers", json=[identities[2].worker_id])
        await client.post(peers[1] + "/test/block-peers", json=[identities[2].worker_id])
        await client.post(
            peers[2] + "/test/block-peers",
            json=[identities[0].worker_id, identities[1].worker_id],
        )
        await asyncio.sleep(1.5)
        await wait_chat(client, peers[0], "alpha")
        alone = await chat(client, peers[2], "beta")
        assert alone.status_code == 200, alone.text
        assert "beta" in alone.text
        for peer in peers:
            await client.post(peer + "/test/block-peers", json=[])
        await wait_chat(client, peers[1], "beta")
        processes[0].kill()
        await processes[0].wait()
        await wait_chat(client, peers[1], "beta")
        await wait_chat(client, peers[2], "alpha")
        if survivor == 0:
            processes[0], restarted = await stack.enter_async_context(
                native(tmp_path, "peer", paths[0], backends[0], ports[0], json.dumps([]))
            )
            peers[0] = restarted["url"]
        for index, process in enumerate(processes):
            if index != survivor and process.returncode is None:
                process.kill()
                await process.wait()
        model = ("seedmodel", "alpha", "beta")[survivor]
        response = await chat(client, peers[survivor], model)
        assert response.status_code == 200, response.text
        assert model in response.text


async def test_origin_death_keeps_remote_execution_charged(tmp_path):
    from contextlib import AsyncExitStack

    paths = [tmp_path / "origin", tmp_path / "worker"]
    await provision(paths)
    ports = [free_port(), free_port()]
    endpoints = [f"https://127.0.0.1:{port}" for port in ports]
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(httpx.AsyncClient(trust_env=False, timeout=10))
        backends = [
            (await stack.enter_async_context(native(tmp_path, "backend", model)))[1]["url"]
            for model in ("alpha", "beta")
        ]
        origin, origin_info = await stack.enter_async_context(
            native(tmp_path, "peer", paths[0], backends[0], ports[0], json.dumps([endpoints[1]]))
        )
        _, worker_info = await stack.enter_async_context(
            native(tmp_path, "peer", paths[1], backends[1], ports[1], json.dumps([endpoints[0]]))
        )
        await wait_chat(client, origin_info["url"], "beta")
        await client.post(backends[1] + "/block")
        try:
            async with client.stream(
                "POST",
                origin_info["url"] + "/v1/chat/completions",
                json={
                    "model": "beta",
                    "messages": [{"role": "user", "content": "hi"}],
                    "stream": True,
                },
            ) as response:
                assert response.status_code == 200
                async with asyncio.timeout(5):
                    async for line in response.aiter_lines():
                        if "beta" in line:
                            break
                assert (await client.get(backends[1] + "/state")).json()["active"] == 1
                origin.kill()
                await origin.wait()
            state = (await client.get(backends[1] + "/state")).json()
            assert state["active"] == 1
            capacity = (
                await client.get(worker_info["url"] + "/v1/cluster/capacity?model=beta")
            ).json()
            assert capacity["observed_available_slots"] == 0
            denied = await client.post(
                worker_info["url"] + "/v1/capacity/acquire",
                json={"model": "beta", "request_id": "after-origin-death", "ttl_ms": 100},
            )
            assert denied.status_code == 429, denied.text
            # Remote origin requests use the default 30-second lease TTL;
            # running or uncertain work must remain charged after it expires.
            await asyncio.sleep(30.2)
            retry = await client.post(
                worker_info["url"] + "/v1/capacity/acquire",
                json={"model": "beta", "request_id": "after-short-ttl", "ttl_ms": 100},
            )
            assert retry.status_code == 429, retry.text
            state = (await client.get(backends[1] + "/state")).json()
            assert state["active"] == 1
            assert state["peak"] == 1
        finally:
            await client.post(backends[1] + "/finish")
            await wait_for(lambda: backend_state(client, backends[1], 0))
