import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from fleet_manager.idle_herd import acquire
from fleet_manager.peer.capacity import cluster_capacity
from fleet_manager.peer.models import Capacity


def capacity():
    return Capacity(
        worker_id="local",
        worker_incarnation="one",
        model="test",
        total_slots=2,
        observed_available_slots=1,
        quarantined_slots=0,
        eligible=True,
        observed_at=time.time(),
    )


@pytest.mark.asyncio
async def test_local_and_offline_capacity_are_honest():
    ledger = SimpleNamespace(capacity=AsyncMock(return_value=capacity()), cluster_id="cluster")
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    view = await cluster_capacity(app, "test")
    assert (view.known_nodes, view.reachable_nodes, view.busy) == (1, 1, False)
    app.state.directory = SimpleNamespace(
        ttl=5,
        interval=1,
        snapshot=lambda: [
            SimpleNamespace(
                reachable=False,
                descriptor=None,
                endpoint="https://peer",
                worker_id="peer",
                reason="offline",
                observed_at=None,
            )
        ],
    )
    view = await cluster_capacity(app, "test")
    assert (view.known_nodes, view.reachable_nodes, view.offline_nodes) == (2, 1, 1)
    assert view.observed_available_slots == 1
    assert view.busy is False
    ledger.capacity.return_value.observed_available_slots = 0
    view = await cluster_capacity(app, "test")
    assert view.busy is None
    assert view.peers[1].available_slots is None


@pytest.mark.asyncio
async def test_sdk_releases_lease_on_exit():
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(
                200,
                json={
                    "reservation_id": "id",
                    "token": "secret",
                    "cluster_id": "cluster",
                    "worker_id": "local",
                    "worker_incarnation": "one",
                    "slot_id": 0,
                    "slot_generation": 1,
                    "request_id": "request",
                    "model": "test",
                    "expires_at": 12345,
                    "ttl_ms": 30000,
                    "state": "reserved",
                    "memory_gb": 1,
                },
            )
        return httpx.Response(200)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with client, acquire("test", client=client) as lease:
        assert lease.token == "secret"
    assert requests[1].method == "DELETE"
    assert requests[1].headers["x-idle-herd-reservation"] == "secret"


def peer_app(remote, *, reachable=True, descriptor=True):
    local = capacity()
    local.observed_available_slots = 0
    local.model = "test:latest"
    peer = SimpleNamespace(endpoint="https://peer", worker_id="peer", incarnation="two")
    status = SimpleNamespace(
        endpoint=peer.endpoint,
        worker_id="peer",
        reachable=reachable,
        descriptor=peer if descriptor else None,
        reason=None,
        observed_at=time.time(),
    )
    directory = SimpleNamespace(ttl=5, interval=1, snapshot=lambda: [status])
    identity = SimpleNamespace(
        cluster_id="cluster",
        sign_request=lambda *args: {"x-idle-nonce": "n"},
        verify_response=lambda *args: None,
    )

    def handler(request):
        if isinstance(remote, Exception):
            raise remote
        return httpx.Response(200, json=remote.model_dump())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app = SimpleNamespace(
        state=SimpleNamespace(
            ledger=SimpleNamespace(capacity=AsyncMock(return_value=local), cluster_id="cluster"),
            directory=directory,
            identity=identity,
            peer_client=client,
        )
    )
    return app, client


def remote_capacity():
    result = capacity()
    return result.model_copy(
        update={"worker_id": "peer", "worker_incarnation": "two", "model": "test:latest"}
    )


@pytest.mark.asyncio
async def test_fresh_remote_capacity_aggregates():
    app, client = peer_app(remote_capacity())
    async with client:
        view = await cluster_capacity(app, "test")
    assert (view.eligible_reachable_workers, view.observed_available_slots) == (2, 1)
    assert view.observed_total_eligible_slots == 4
    assert view.peers[1].fresh and view.busy is False


@pytest.mark.parametrize(
    "change",
    [
        {"observed_at": time.time() - 60},
        {"observed_at": time.time() + 60},
        {"worker_id": "wrong"},
        {"worker_incarnation": "wrong"},
        {"model": "wrong"},
    ],
)
@pytest.mark.asyncio
async def test_stale_future_or_mismatched_remote_is_excluded(change):
    app, client = peer_app(remote_capacity().model_copy(update=change))
    async with client:
        view = await cluster_capacity(app, "test")
    assert view.observed_available_slots == 0
    assert view.stale_nodes == 1 and view.busy is None
    assert view.peers[1].available_slots is None


@pytest.mark.parametrize("unknown", [False, True])
@pytest.mark.asyncio
async def test_failed_or_unknown_peer_is_partial(unknown):
    app, client = peer_app(httpx.ConnectError("unavailable"), descriptor=not unknown)
    async with client:
        view = await cluster_capacity(app, "test")
    assert view.busy is None and view.stale_nodes == 1


@pytest.mark.asyncio
async def test_complete_busy_and_stale_local_sample():
    local = capacity()
    local.observed_available_slots = 0
    ledger = SimpleNamespace(capacity=AsyncMock(return_value=local), cluster_id="cluster")
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    assert (await cluster_capacity(app, "test")).busy is True
    local.observed_available_slots = 2
    local.observed_at = time.time() - 60
    view = await cluster_capacity(app, "test")
    assert view.busy is None and view.observed_available_slots == 0
    assert not view.peers[0].fresh and view.stale_nodes == 1


@pytest.mark.parametrize("cancel", [False, True])
@pytest.mark.asyncio
async def test_sdk_releases_when_body_raises_or_is_cancelled(cancel):
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(
                200,
                json={
                    "reservation_id": "id",
                    "token": "secret",
                    "cluster_id": "cluster",
                    "worker_id": "local",
                    "worker_incarnation": "one",
                    "slot_id": 0,
                    "slot_generation": 1,
                    "request_id": "request",
                    "model": "test",
                    "expires_at": 12345,
                    "ttl_ms": 30000,
                    "state": "reserved",
                    "memory_gb": 1,
                },
            )
        return httpx.Response(503)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    error = asyncio.CancelledError if cancel else ValueError
    async with client:
        with pytest.raises(error):
            async with acquire("test", client=client):
                raise error()
    assert requests[-1].method == "DELETE"


@pytest.mark.asyncio
async def test_duplicate_worker_endpoints_count_once():
    app, client = peer_app(remote_capacity())
    current = app.state.directory.snapshot()[0]
    old = SimpleNamespace(
        endpoint="https://old",
        worker_id="peer",
        reachable=False,
        descriptor=None,
        reason="offline",
        observed_at=time.time() - 60,
    )
    app.state.directory.snapshot = lambda: [old, current]
    async with client:
        view = await cluster_capacity(app, "test")
    assert view.known_nodes == view.reachable_nodes == 2
    assert view.observed_available_slots == 1
    assert view.eligible_reachable_workers == 2
    assert view.offline_nodes == 0
