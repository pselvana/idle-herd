import asyncio
import json

import httpx
import pytest

from fleet_manager.peer.discovery import (
    Exchange,
    ModelCapability,
    PeerDescriptor,
    PeerDirectory,
    validate_endpoint,
)


class FakeIdentity:
    def __init__(self, worker_id):
        self.worker_id = worker_id

    def sign_request(self, method, path, body):
        return {"x-idle-nonce": self.worker_id}

    def revocations(self):
        return []

    async def merge_revocations(self, statements):
        assert not statements

    def sign_response(self, nonce, status, body):
        return {"x-worker": self.worker_id, "x-nonce": nonce}

    def verify_response(self, nonce, status, body, headers, expected_worker_id=None):
        worker_id = headers["x-worker"]
        if headers["x-nonce"] != nonce or (expected_worker_id and worker_id != expected_worker_id):
            raise ValueError("invalid response")
        return worker_id


@pytest.fixture
async def network(tmp_path):
    nodes = {}
    dead = set()

    async def handle(request):
        endpoint = str(request.url.copy_with(path=""))
        if endpoint in dead:
            raise httpx.ConnectError("unreachable")
        directory = nodes[endpoint]
        incoming = Exchange.model_validate_json(await request.aread())
        result = await directory.exchange(request.headers["x-idle-nonce"], incoming)
        body = result.model_dump_json().encode()
        return httpx.Response(
            200,
            content=body,
            headers=directory.identity.sign_response(request.headers["x-idle-nonce"], 200, body),
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))

    async def create(number, seeds=()):
        endpoint = f"https://127.0.0.{number}:12000"
        descriptor = PeerDescriptor(
            cluster_id="cluster",
            worker_id=f"worker-{number}",
            incarnation="1",
            endpoint=endpoint,
            models=[ModelCapability(name="llama", capabilities=["chat"])],
        )

        async def inventory():
            return descriptor

        directory = PeerDirectory(
            FakeIdentity(descriptor.worker_id),
            endpoint,
            list(seeds),
            inventory,
            tmp_path / str(number),
            client=client,
            ttl=1,
        )
        nodes[endpoint] = directory
        await directory.open()
        return directory

    yield create, nodes, dead
    await client.aclose()


async def test_exchange_converges_without_seed_and_persists(network):
    create, nodes, dead = network
    a = await create(1)
    b = await create(2, [a.endpoint])
    c = await create(3, [a.endpoint])
    await b.refresh()
    await c.refresh()
    await a.refresh()
    await b.refresh()
    await c.refresh()
    assert {p.worker_id for p in b.candidates("llama")} == {"worker-1", "worker-3"}
    dead.add(a.endpoint)
    await b.refresh()
    assert [p.worker_id for p in b.candidates("llama")] == ["worker-3"]
    restored = await create(2)
    await restored.refresh()
    assert [p.worker_id for p in restored.candidates("llama")] == ["worker-3"]
    assert any(not p.reachable and p.reason for p in restored.snapshot())
    assert len(nodes) == 3


async def test_referral_never_renews_liveness_and_expiry(network):
    create, _, _ = network
    a = await create(1)
    b = await create(2, [a.endpoint])
    c = await create(3, [a.endpoint])
    await b.refresh()
    await c.refresh()
    # C learned B as a referral, but has not directly contacted it.
    status = next(p for p in c.snapshot() if p.endpoint == b.endpoint)
    assert not status.reachable
    await c.refresh()
    assert any(p.worker_id == "worker-2" for p in c.candidates("llama"))
    c.ttl = 0.001
    await asyncio.sleep(0.01)
    assert not c.candidates("llama")
    assert all(not p.reachable for p in c.snapshot())


async def test_authenticated_host_label_survives_restart_and_endpoint_migration(network):
    create, _, _ = network
    a, b = await create(1), await create(2)
    await a.discovered(b.endpoint, "worker-2")
    assert a.host_for("worker-2") == "127.0.0.2"
    await a.remember_hostname("worker-2", "pstudio.local")
    restored = await create(1)
    assert restored.host_for("worker-2") == "pstudio.local"
    await restored.discovered("https://127.0.0.2:13000", "worker-2")
    assert restored.host_for("worker-2") == "pstudio.local"
    assert restored.snapshot()[0].hostname == "pstudio.local"
    with pytest.raises(ValueError, match="hostname"):
        await restored.remember_hostname("worker-2", "../private/path")
    assert restored.host_for("not-known") is None


async def test_sender_binding_and_referral_identity_poisoning(network):
    create, _, _ = network
    a, b = await create(1), await create(2)
    sender = await b.inventory()
    forged = sender.model_copy(update={"worker_id": "forged"})
    with pytest.raises(ValueError, match="authenticated sender"):
        await a.exchange("worker-2", Exchange(sender=forged))
    bogus = sender.model_copy(update={"worker_id": "victim", "endpoint": "https://127.0.0.3:12000"})
    await a.exchange("worker-2", Exchange(sender=sender, peers=[bogus]))
    assert next(p for p in a.snapshot() if p.endpoint == bogus.endpoint).worker_id is None


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://127.0.0.1:12",
        "https://example.com:443",
        "https://8.8.8.8:443",
        "https://user@127.0.0.1:12",
        "https://127.0.0.1:12/x",
        "https://127.0.0.1:12?q=1",
        "https://127.0.0.1",
        "https://[::1]:bad",
        "https://0.0.0.0:12",
    ],
)
def test_endpoint_rejects_unsafe_values(endpoint):
    with pytest.raises(ValueError):
        validate_endpoint(endpoint)


async def test_oversized_response_is_rejected(tmp_path):
    async def handler(request):
        return httpx.Response(200, content=b"x" * (256 * 1024 + 1))

    async def inventory():
        return PeerDescriptor(
            cluster_id="c", worker_id="a", incarnation="1", endpoint="https://127.0.0.1:1"
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        directory = PeerDirectory(
            FakeIdentity("a"),
            "https://127.0.0.1:1",
            ["https://127.0.0.2:2"],
            inventory,
            tmp_path,
            client=client,
        )
        await directory.refresh()
        assert "too large" in directory.snapshot()[0].reason
        assert not directory.candidates("model")
        assert json.loads((tmp_path / "known-peers.json").read_text())[0]["worker_id"] is None
