"""Public capacity admission under contention and peer failure."""

import asyncio
import json
from contextlib import AsyncExitStack, asynccontextmanager, suppress

import httpx
from test_app import peer
from test_peer_network import free_port, native, provision, wait_chat


def spec(request_id, **kwargs):
    return {"model": "tiny", "request_id": request_id, **kwargs}


async def release(client, lease):
    return await client.delete(
        "/v1/capacity/reservations/" + lease["reservation_id"],
        headers={"X-Idle-Herd-Reservation": lease["token"]},
    )


async def test_twenty_waiters_share_two_slots_without_overbooking(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch, slots=2) as (app, client, _):
        active = 0
        peak = 0
        lock = asyncio.Lock()

        async def use_slot(i):
            nonlocal active, peak
            response = await client.post(
                "/v1/capacity/acquire", json=spec(f"waiter-{i}", wait_ms=10000)
            )
            assert response.status_code == 201, response.text
            lease = response.json()
            async with lock:
                active += 1
                peak = max(peak, active)
                assert active <= 2
            await asyncio.sleep(0.02)
            async with lock:
                assert (await release(client, lease)).status_code == 200
                active -= 1
            return lease

        leases = await asyncio.gather(*(use_slot(i) for i in range(20)))
        assert peak == 2
        assert len({lease["reservation_id"] for lease in leases}) == 20
        assert not await app.state.ledger._occupied()


async def test_wait_timeout_cancellation_and_stale_capacity(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (_, client, _):
        snapshot = (await client.get("/v1/cluster/capacity?model=tiny")).json()
        assert snapshot["scope"] == "reachable_peers"
        held = await client.post("/v1/capacity/acquire", json=spec("held", wait_ms=0))
        assert held.status_code == 201, held.text
        # A previously observed free slot is advisory, not an admission grant.
        full = await client.post("/v1/capacity/acquire", json=spec("full", wait_ms=0))
        assert full.status_code == 429
        assert full.json()["error"]["type"] == "no_capacity"
        timed = await client.post("/v1/capacity/acquire", json=spec("timed", wait_ms=50))
        assert timed.status_code == 504
        pending = asyncio.create_task(
            client.post("/v1/capacity/acquire", json=spec("cancelled", wait_ms=5000))
        )
        await asyncio.sleep(0.1)
        pending.cancel()
        with suppress(asyncio.CancelledError):
            await pending
        assert (await release(client, held.json())).status_code == 200
        next_lease = await client.post("/v1/capacity/acquire", json=spec("next", wait_ms=1000))
        assert next_lease.status_code == 201, next_lease.text
        assert (await release(client, next_lease.json())).status_code == 200


async def test_origin_storage_failure_cannot_grant_or_dispatch(tmp_path, monkeypatch):
    import aiosqlite

    async with peer(tmp_path, monkeypatch) as (app, client, backend):

        async def failed_save(*args, **kwargs):
            raise aiosqlite.OperationalError("fabricated disk failure")

        monkeypatch.setattr(app.state.waiters, "_save", failed_save)
        response = await client.post("/v1/capacity/acquire", json=spec("failed-write", wait_ms=0))
        assert response.status_code == 503
        assert response.json()["error"]["type"] == "admission_storage_unavailable"
        assert not await app.state.ledger._occupied()
        assert not backend.calls


async def test_remote_explicit_lease_and_single_survivor(tmp_path):
    paths = [tmp_path / "a", tmp_path / "b"]
    identities = await provision(paths)
    ports = [free_port(), free_port()]
    endpoints = [f"https://127.0.0.1:{port}" for port in ports]
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(httpx.AsyncClient(trust_env=False, timeout=15))
        backends = [
            (await stack.enter_async_context(native(tmp_path, "backend", model)))[1]["url"]
            for model in ("alpha", "beta")
        ]
        _, a = await stack.enter_async_context(
            native(tmp_path, "peer", paths[0], backends[0], ports[0], json.dumps([endpoints[1]]))
        )
        second, _ = await stack.enter_async_context(
            native(tmp_path, "peer", paths[1], backends[1], ports[1], json.dumps([endpoints[0]]))
        )
        await wait_chat(client, a["url"], "beta")
        acquired = await client.post(
            a["url"] + "/v1/capacity/acquire",
            json={"model": "beta", "request_id": "remote-explicit", "wait_ms": 5000},
        )
        assert acquired.status_code == 201, acquired.text
        lease = acquired.json()
        assert lease["worker_id"] == identities[1].worker_id
        headers = {"X-Idle-Herd-Reservation": lease["token"]}
        url = a["url"] + "/v1/capacity/reservations/" + lease["reservation_id"]
        assert (await client.get(url, headers=headers)).json()["state"] == "reserved"
        result = await client.post(
            a["url"] + "/api/chat",
            headers=headers,
            json={"model": "beta", "messages": [], "stream": False},
        )
        assert result.status_code == 200, result.text
        acquired = await client.post(
            a["url"] + "/v1/capacity/acquire",
            json={"model": "beta", "request_id": "remote-release", "wait_ms": 5000},
        )
        assert acquired.status_code == 201, acquired.text
        lease = acquired.json()
        url = a["url"] + "/v1/capacity/reservations/" + lease["reservation_id"]
        result = await client.delete(url, headers={"X-Idle-Herd-Reservation": lease["token"]})
        assert result.status_code == 200, result.text
        assert result.json()["state"] == "released"
        second.kill()
        await second.wait()
        local = await client.post(
            a["url"] + "/v1/capacity/acquire",
            json={"model": "alpha", "request_id": "survivor", "wait_ms": 5000},
        )
        assert local.status_code == 201, local.text


@asynccontextmanager
async def signed_mesh(tmp_path, monkeypatch):
    """Real peer signatures and directory observations over an in-process transport."""
    from fleet_manager.peer.app import create_peer_rpc_app
    from fleet_manager.peer.discovery import ModelCapability, PeerDescriptor, PeerDirectory

    paths = [tmp_path / "mesh-a", tmp_path / "mesh-b"]
    identities = await provision(paths)
    endpoints = ["https://127.0.0.1:19001", "https://127.0.0.1:19002"]
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(peer(path, monkeypatch)) for path in paths]
        apps = [node[0] for node in nodes]
        clients = [node[1] for node in nodes]
        down = set()
        transports = {
            endpoint: httpx.ASGITransport(app=create_peer_rpc_app(app))
            for endpoint, app in zip(endpoints, apps, strict=True)
        }

        async def route(request):
            endpoint = str(request.url.copy_with(path="", query=None))
            if endpoint in down:
                raise httpx.ConnectError("peer unavailable", request=request)
            return await transports[endpoint].handle_async_request(request)

        peer_client = await stack.enter_async_context(
            httpx.AsyncClient(transport=httpx.MockTransport(route))
        )
        directories = []
        for i, app in enumerate(apps):
            app.state.identity = identities[i]
            app.state.peer_client = peer_client
            app.state.idle_policy.settings.background_policy = "always"

            async def inventory(index=i):
                return PeerDescriptor(
                    cluster_id=identities[index].cluster_id,
                    worker_id=identities[index].worker_id,
                    incarnation=apps[index].state.ledger.incarnation,
                    endpoint=endpoints[index],
                    models=[ModelCapability(name="tiny:latest")],
                )

            directory = PeerDirectory(
                identities[i],
                endpoints[i],
                [endpoints[1 - i]],
                inventory,
                paths[i],
                client=peer_client,
                ttl=60,
                interval=0.1,
            )
            app.state.directory = directory
            directories.append(directory)
        await asyncio.gather(*(directory.refresh() for directory in directories))
        yield apps, clients, directories, endpoints, down


async def test_signed_mesh_twenty_clients_and_churn(tmp_path, monkeypatch):
    async with signed_mesh(tmp_path, monkeypatch) as (apps, clients, directories, endpoints, down):
        active = set()
        peak = 0
        lock = asyncio.Lock()

        async def job(i):
            nonlocal peak
            client = clients[i % 2]
            result = await client.post(
                "/v1/capacity/acquire", json=spec(f"mesh-{i}", wait_ms=10000)
            )
            assert result.status_code == 201, result.text
            lease = result.json()
            async with lock:
                slot = (lease["worker_id"], lease["slot_id"])
                assert slot not in active
                active.add(slot)
                peak = max(peak, len(active))
            await asyncio.sleep(0.03)
            async with lock:
                result = await release(client, lease)
                assert result.status_code == 200, result.text
                active.remove(slot)

        # Both origins retain a stale advertisement while one remote endpoint leaves.
        down.add(endpoints[1])
        held = await clients[0].post("/v1/capacity/acquire", json=spec("hold", wait_ms=0))
        assert held.status_code == 201, held.text
        pending = asyncio.create_task(
            clients[0].post("/v1/capacity/acquire", json=spec("join", wait_ms=5000))
        )
        await asyncio.sleep(0.2)
        assert not pending.done()
        down.clear()
        await asyncio.gather(*(directory.refresh() for directory in directories))
        await release(clients[0], held.json())
        joined = await pending
        assert joined.status_code == 201, joined.text
        assert (await release(clients[0], joined.json())).status_code == 200
        await asyncio.gather(*(job(i) for i in range(20)))
        assert peak == 2
        assert not await apps[0].state.ledger._occupied()
        assert not await apps[1].state.ledger._occupied()


async def test_signed_mesh_policy_stale_snapshot_and_unreachable_reservation(tmp_path, monkeypatch):
    async with signed_mesh(tmp_path, monkeypatch) as (apps, clients, directories, endpoints, down):
        origin, worker = clients
        # The directory still advertises the model after the worker's owner withdraws it.
        apps[1].state.idle_policy.settings.background_policy = "never"
        local = await origin.post("/v1/capacity/acquire", json=spec("local", wait_ms=0))
        assert local.status_code == 201, local.text
        denied = await origin.post("/v1/capacity/acquire", json=spec("policy", wait_ms=0))
        assert denied.status_code in (429, 503), denied.text
        assert not await apps[1].state.ledger._occupied()
        apps[1].state.idle_policy.settings.background_policy = "always"
        remote = await origin.post("/v1/capacity/acquire", json=spec("remote", wait_ms=1000))
        assert remote.status_code == 201, remote.text
        lease = remote.json()
        assert lease["worker_id"] == apps[1].state.ledger.worker_id
        down.add(endpoints[1])
        url = "/v1/capacity/reservations/" + lease["reservation_id"]
        headers = {"X-Idle-Herd-Reservation": lease["token"]}
        for response in (
            await origin.get(url, headers=headers),
            await origin.delete(url, headers=headers),
        ):
            assert response.status_code == 503, response.text
            assert response.json()["error"]["type"] == "worker_unreachable"
        down.clear()
        assert (await origin.delete(url, headers=headers)).status_code == 200
        assert (await release(origin, local.json())).status_code == 200


async def test_model_reappears_while_waiting_and_live_busy_rejects_snapshot(tmp_path, monkeypatch):
    async with signed_mesh(tmp_path, monkeypatch) as (apps, clients, directories, _, _):
        origin, worker = clients
        # Both slots are actually occupied, despite each directory advertising the model.
        local = await origin.post("/v1/capacity/acquire", json=spec("hold-a", wait_ms=0))
        remote = await worker.post("/v1/capacity/acquire", json=spec("hold-b", wait_ms=0))
        assert local.status_code == remote.status_code == 201
        busy = await origin.post("/v1/capacity/acquire", json=spec("busy", wait_ms=0))
        assert busy.status_code == 429, busy.text
        directory = directories[0]
        entry = next(iter(directory._peers.values()))
        advertised = entry.descriptor
        entry.descriptor = advertised.model_copy(update={"models": []})
        directory.changed.set()
        pending = asyncio.create_task(
            origin.post("/v1/capacity/acquire", json=spec("reappeared", wait_ms=5000))
        )
        await asyncio.sleep(0.15)
        assert not pending.done()
        assert (await release(worker, remote.json())).status_code == 200
        await asyncio.sleep(0.15)
        assert not pending.done()
        entry.descriptor = advertised
        directory.changed.set()
        response = await pending
        assert response.status_code == 201, response.text
        assert response.json()["worker_id"] == apps[1].state.ledger.worker_id
        assert (await release(origin, response.json())).status_code == 200
        assert (await release(origin, local.json())).status_code == 200


async def test_idempotency_is_stable_per_origin_under_concurrent_requests(tmp_path, monkeypatch):
    async with signed_mesh(tmp_path, monkeypatch) as (_, clients, _, _, _):

        async def acquire(client, request_id):
            return await client.post(
                "/v1/capacity/acquire",
                json=spec(request_id, idempotency_key="same-key", wait_ms=3000),
            )

        responses = await asyncio.gather(
            acquire(clients[0], "a1"),
            acquire(clients[0], "a2"),
            acquire(clients[1], "b1"),
            acquire(clients[1], "b2"),
        )
        assert all(response.status_code == 201 for response in responses), [
            r.text for r in responses
        ]
        leases = [response.json() for response in responses]
        assert leases[0]["reservation_id"] == leases[1]["reservation_id"]
        assert leases[2]["reservation_id"] == leases[3]["reservation_id"]
        assert leases[0]["reservation_id"] != leases[2]["reservation_id"]
        assert (await release(clients[0], leases[0])).status_code == 200
        assert (await release(clients[1], leases[2])).status_code == 200


async def test_peer_capacity_requires_authentication_and_signs_errors(tmp_path, monkeypatch):
    async with signed_mesh(tmp_path, monkeypatch) as (apps, _, _, endpoints, _):
        identity = apps[0].state.identity
        client = apps[0].state.peer_client
        path = "/peer/v1/capacity"
        body = b'{"model":"tiny","workload":"interactive","idle_only":false}'
        unsigned = await client.post(endpoints[1] + path, content=body)
        assert unsigned.status_code == 403
        headers = identity.sign_request("POST", path, body)
        good = await client.post(endpoints[1] + path, content=body, headers=headers)
        assert good.status_code == 200, good.text
        identity.verify_response(
            headers["x-idle-nonce"],
            good.status_code,
            good.content,
            good.headers,
            apps[1].state.ledger.worker_id,
        )
        invalid_body = b'{"model":"tiny","extra":"invalid"}'
        headers = identity.sign_request("POST", path, invalid_body)
        invalid = await client.post(endpoints[1] + path, content=invalid_body, headers=headers)
        assert invalid.status_code == 422, invalid.text
        identity.verify_response(
            headers["x-idle-nonce"],
            invalid.status_code,
            invalid.content,
            invalid.headers,
            apps[1].state.ledger.worker_id,
        )
