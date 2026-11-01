"""Real reused inference routes with an offline mock Ollama, no router/agent."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import aiosqlite
import httpx
import pytest

from fleet_manager.peer.app import create_peer_app
from fleet_manager.peer.models import PeerSettings


class WireBytes(httpx.AsyncByteStream):
    def __init__(self, content):
        self.content = content.encode()

    async def __aiter__(self):
        yield self.content

    async def aclose(self):
        pass


class MockOllama:
    def __init__(self):
        self.calls = []
        self.block = False
        self.entered = asyncio.Event()
        self.finish = asyncio.Event()
        self.drop = False
        self.down = False

    async def handle(self, request):
        if self.down:
            raise httpx.ConnectError("backend stopped", request=request)
        if request.url.path in {"/api/tags", "/api/ps"}:
            return httpx.Response(
                200,
                json={
                    "models": [
                        {
                            "name": "tiny:latest",
                            "size": 1000,
                            "digest": "sha256:test",
                            "context_length": 8192,
                            "details": {"format": "gguf"},
                        },
                        {"name": "other:latest", "size": 1000},
                    ]
                },
            )
        self.calls.append(request.url.path)
        if request.url.path == "/api/show":
            return httpx.Response(200, json={"capabilities": ["completion", "embedding"]})
        if request.url.path == "/api/version":
            return httpx.Response(200, json={"version": "mock"})
        if self.block:
            self.entered.set()
            await self.finish.wait()
        if request.url.path == "/api/embed":
            return httpx.Response(
                200,
                stream=WireBytes(json.dumps({"model": "tiny:latest", "embeddings": [[1.0, 2.0]]})),
            )
        if request.url.path == "/api/embeddings":
            return httpx.Response(200, json={"embedding": [1.0, 2.0]})
        chunk = {
            "model": "tiny:latest",
            "created_at": "2026-11-01T00:00:00Z",
            "message": {"role": "assistant", "content": "hello"},
            "response": "hello",
            "done": not self.drop,
            "done_reason": "stop",
            "prompt_eval_count": 1,
            "eval_count": 1,
        }
        return httpx.Response(200, stream=WireBytes(json.dumps(chunk) + "\n"))


@asynccontextmanager
async def peer(tmp_path, monkeypatch, *, slots=1):
    # No inference, discovery, external telemetry, host sensors or live backend.
    monkeypatch.setattr(
        "fleet_manager.peer.backend.psutil.virtual_memory",
        lambda: SimpleNamespace(
            total=64e9,
            used=16e9,
            available=48e9,
        ),
    )
    backend = MockOllama()
    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path), slots=slots),
        backend_transport=httpx.MockTransport(backend.handle),
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost:11435"
        ) as client,
    ):
        yield app, client, backend


@pytest.mark.parametrize(
    "path,body",
    [
        ("/v1/chat/completions", {"messages": [{"role": "user", "content": "hi"}]}),
        ("/api/chat", {"messages": [{"role": "user", "content": "hi"}]}),
        ("/api/generate", {"prompt": "hi"}),
        ("/v1/responses", {"input": "hi", "store": False}),
        ("/v1/messages", {"messages": [{"role": "user", "content": "hi"}], "max_tokens": 16}),
    ],
)
@pytest.mark.parametrize("stream", [False, True])
async def test_compatibility_routes_use_durable_admission(
    tmp_path, monkeypatch, path, body, stream
):
    async with peer(tmp_path, monkeypatch) as (app, client, backend):
        response = await client.post(path, json={"model": "tiny", "stream": stream, **body})
        assert response.status_code == 200, response.text
        assert "hello" in response.text
        if stream:
            assert ("data:" in response.text) == path.startswith("/v1/")
        if path == "/v1/chat/completions" and not stream:
            assert response.json()["choices"][0]["message"]["content"] == "hello"
        if path == "/v1/responses":
            assert "response.completed" in response.text or response.json()["status"] == "completed"
        rows = await (await app.state.ledger._db.execute("SELECT state FROM leases")).fetchall()
        assert [row[0] for row in rows] == ["released"]
        assert len(backend.calls) == 1


@pytest.mark.parametrize(
    "path,body,key",
    [
        ("/v1/embeddings", {"input": "hi"}, "data"),
        ("/api/embed", {"input": "hi"}, "embeddings"),
        ("/api/embeddings", {"prompt": "hi"}, "embeddings"),
    ],
)
async def test_embedding_routes_fenced(tmp_path, monkeypatch, path, body, key):
    async with peer(tmp_path, monkeypatch) as (app, client, backend):
        response = await client.post(path, json={"model": "tiny", **body})
        assert response.status_code == 200, response.text
        assert key in response.json()
        assert (await app.state.ledger.capacity("tiny:latest")).observed_available_slots == 1
        assert len(backend.calls) == 1


async def test_inventory_and_capacity_without_any_peer_or_raft(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (app, client, backend):
        models = (await client.get("/v1/models")).json()
        assert {model["id"] for model in models["data"]} == {"tiny:latest", "other:latest"}
        tags = (await client.get("/api/tags")).json()
        assert tags["models"][0]["digest"] == "sha256:test"
        assert (await client.get("/api/ps")).status_code == 200
        assert (await client.get("/heartbeat")).status_code == 404
        snapshot = (await client.get("/v1/cluster/capacity?model=tiny")).json()
        assert snapshot["observed_available_slots"] == 1
        assert snapshot["scope"] == "reachable_peers"
        assert snapshot["globally_authoritative"] is False
        assert snapshot["quorum"] is None
        assert snapshot["discovery_state"] == "complete_known_view"
        backend.down = True
        snapshot = (await client.get("/v1/cluster/capacity?model=tiny")).json()
        assert snapshot["observed_available_slots"] == 0
        assert snapshot["reason"] == "backend_unavailable"
        assert (await client.get("/health")).status_code == 200


async def test_explicit_token_replay_and_reused_queue_context(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (app, client, backend):

        async def acquire(request_id):
            result = await client.post(
                "/v1/capacity/acquire",
                json={
                    "request_id": request_id,
                    "model": "tiny",
                },
            )
            assert result.status_code == 201
            return result.json()

        for i in range(2):
            lease = await acquire(f"request-{i}")
            headers = {"X-Idle-Herd-Reservation": lease["token"]}
            url = "/v1/capacity/reservations/" + lease["reservation_id"]
            assert (await client.get(url, headers=headers)).json()["state"] == "reserved"
            response = await client.post(
                "/api/chat",
                headers=headers,
                json={
                    "model": "tiny",
                    "messages": [{"role": "user", "content": "hi"}],
                    "stream": False,
                },
            )
            assert response.status_code == 200, response.text
            replay = await client.post(
                "/api/chat",
                headers=headers,
                json={
                    "model": "tiny",
                    "messages": [],
                    "stream": False,
                },
            )
            assert replay.status_code == 409
        assert len(backend.calls) == 2
        held = await acquire("release-me")
        released = await client.delete(
            "/v1/capacity/reservations/" + held["reservation_id"],
            headers={"X-Idle-Herd-Reservation": held["token"]},
        )
        assert released.json()["state"] == "released"


async def test_one_hundred_embedding_clients_no_overbooking_across_models(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (app, client, backend):
        backend.block = True
        running = asyncio.create_task(
            client.post("/api/embed", json={"model": "tiny", "input": "hi"})
        )
        await asyncio.wait_for(backend.entered.wait(), 5)
        try:
            replies = await asyncio.gather(
                *(
                    client.post("/api/embed", json={"model": "other", "input": "hi"})
                    for _ in range(99)
                )
            )
            assert all(reply.status_code == 429 for reply in replies)
            assert len(backend.calls) == 1
            assert (await app.state.ledger.capacity("other:latest")).observed_available_slots == 0
        finally:
            backend.finish.set()
            assert (await running).status_code == 200
        assert (await app.state.ledger.capacity("other:latest")).observed_available_slots == 1


async def test_truncated_backend_does_not_free_running_slot(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (app, client, backend):
        backend.drop = True
        response = await client.post(
            "/api/chat",
            json={
                "model": "tiny",
                "messages": [],
                "stream": False,
            },
        )
        assert response.status_code == 502
        capacity = await app.state.ledger.capacity("tiny:latest")
        assert capacity.quarantined_slots == 1
        assert capacity.observed_available_slots == 0
        assert len(backend.calls) == 1


@pytest.mark.parametrize(
    "headers",
    [
        {"Origin": "https://untrusted.example"},
        {"Host": "untrusted.example"},
        {"Sec-Fetch-Site": "cross-site"},
    ],
)
async def test_local_boundary_rejects_browser_and_rebinding(tmp_path, monkeypatch, headers):
    async with peer(tmp_path, monkeypatch) as (_, client, backend):
        assert (
            await client.post(
                "/v1/capacity/acquire",
                headers=headers,
                json={
                    "model": "tiny",
                    "request_id": "browser",
                },
            )
        ).status_code == 403
        assert not backend.calls


async def test_unknown_models_and_invalid_waits_fail_explicitly(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (_, client, backend):
        response = await client.post(
            "/v1/capacity/acquire",
            json={
                "request_id": "missing",
                "model": "missing",
                "wait_ms": 0,
            },
        )
        assert response.status_code == 503
        assert response.json()["error"]["type"] == "no_eligible_worker"
        for extra in ({"wait_ms": -1}, {"wait_ms": 120001}, {"slots": 2}):
            response = await client.post(
                "/v1/capacity/acquire",
                json={
                    "request_id": "future",
                    "model": "tiny",
                    **extra,
                },
            )
            assert response.status_code == 422
        response = await client.post(
            "/v1/capacity/acquire",
            json={"model": "tiny", "request_id": "idle", "idle_only": True, "wait_ms": 0},
        )
        assert response.status_code == 503
        assert response.json()["error"]["type"] == "no_eligible_worker"


async def test_shutdown_drains_traces_and_latency_without_database_races(
    tmp_path, monkeypatch, caplog
):
    async with peer(tmp_path, monkeypatch) as (_, client, _):
        reply = await client.post(
            "/api/chat",
            json={
                "model": "tiny",
                "messages": [],
                "stream": False,
            },
        )
        assert reply.status_code == 200
    assert "closed database" not in caplog.text
    async with aiosqlite.connect(tmp_path / "latency.db") as reader:
        traces = await (await reader.execute("SELECT status FROM request_traces")).fetchall()
        assert [row[0] for row in traces] == ["completed"]
