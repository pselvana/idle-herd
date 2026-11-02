import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from fleet_manager.local_control.models import ModeUpdate
from fleet_manager.peer.app import create_peer_app, create_peer_rpc_app
from fleet_manager.peer.diagnostics import PATH, PeerDiagnostics, PeerObservation
from fleet_manager.peer.identity import PeerIdentity
from fleet_manager.peer.jobs import JobSubmission
from fleet_manager.peer.models import PeerSettings


def observation(**overrides):
    data = dict(
        worker_id="peer",
        worker_incarnation="inc",
        observed_at=time.time(),
        platform="Linux x86_64",
        models=["tiny:latest"],
        sensor_source="test",
        activity="idle",
        eligible=True,
        reserved_slots=0,
        running_slots=0,
        quarantined_slots=0,
        total_slots=1,
        waiters=0,
        queued_jobs=0,
        running_jobs=0,
        replicated_jobs=0,
    )
    data.update(overrides)
    return PeerObservation(**data)


@pytest.mark.asyncio
async def test_signed_rpc_auth_redaction_and_offline(tmp_path):
    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path)),
        backend_transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"models": []})),
    )
    async with app.router.lifespan_context(app):
        identity = await PeerIdentity.open(
            tmp_path, app.state.ledger.worker_id, app.state.ledger.cluster_id
        )
        app.state.identity = identity
        app.state.directory = object()
        secret = "private-prompt-xyz"
        record, _ = await app.state.job_journal.submit(
            JobSubmission(model="tiny", request={"prompt": secret}), app.state.ledger.worker_id
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_peer_rpc_app(app)), base_url="https://peer"
        ) as client:
            assert (await client.post(PATH, content=b"{}")).status_code == 403
            body = b"{}"
            headers = identity.sign_request("POST", PATH, body)
            result = await client.post(PATH, content=body, headers=headers)
            assert result.status_code == 200, result.text
            identity.verify_response(
                headers["x-idle-nonce"],
                200,
                result.content,
                result.headers,
                app.state.ledger.worker_id,
            )
            payload = result.json()
            assert secret not in result.text and record.job_id not in result.text
            assert payload["queued_jobs"] == 1
            assert payload["worker_id"] == app.state.ledger.worker_id
            assert (
                not {"prompt", "result", "token", "job_id", "request", "endpoint"} & payload.keys()
            )
            await app.state.ledger.set_mode(ModeUpdate(mode="offline"))
            headers = identity.sign_request("POST", PATH, body)
            assert (await client.post(PATH, content=body, headers=headers)).status_code == 503
        app.state.directory = None


@pytest.mark.asyncio
async def test_cache_binding_expiry_old_peer_and_offline():
    peer = SimpleNamespace(worker_id="peer", incarnation="inc", endpoint="https://peer")
    status = SimpleNamespace(worker_id="peer", descriptor=peer, reachable=True)
    ledger = SimpleNamespace(cluster_participating=AsyncMock(return_value=True))
    calls = []
    response = observation()
    state = {"status": 200, "item": response}

    def handler(request):
        calls.append(request)
        return httpx.Response(state["status"], content=state["item"].model_dump_json())

    identity = SimpleNamespace(
        sign_request=lambda *args: {"x-idle-nonce": "nonce"},
        verify_response=lambda *args: calls.append("verified"),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        app = SimpleNamespace(
            state=SimpleNamespace(
                ledger=ledger,
                identity=identity,
                peer_client=client,
                directory=SimpleNamespace(snapshot=lambda: [status]),
            )
        )
        cache = PeerDiagnostics(app, ttl=10)
        await cache.refresh()
        assert cache.cached("peer", "inc") == response
        assert "verified" in calls
        assert cache.cached("peer", "wrong") is None
        cache._cache["peer"] = (time.monotonic() - 11, response)
        assert cache.cached("peer", "inc") is None
        state["item"] = observation(worker_incarnation="other")
        await cache.refresh()
        assert cache.cached("peer", "inc") is None
        state["status"] = 404
        await cache.refresh()
        assert cache.cached("peer", "inc") is None
        before = len(calls)
        ledger.cluster_participating.return_value = False
        await cache.refresh()
        assert len(calls) == before


def test_model_names_and_counts_are_bounded():
    from pydantic import ValidationError

    from fleet_manager.peer.diagnostics import SAFE_NAME

    assert SAFE_NAME.fullmatch("org/model:latest")
    for value in ("/Users/name/model", "~/model", "C:/Users/model", "C:\\Users\\model"):
        assert not SAFE_NAME.fullmatch(value)
    with pytest.raises(ValidationError):
        observation(models=["a" * 129])
    with pytest.raises(ValidationError):
        observation(total_slots=129)
    with pytest.raises(ValidationError):
        observation(waiters=10001)


@pytest.mark.asyncio
async def test_poll_recovers_without_logging_error_contents(caplog):
    import asyncio

    cache = PeerDiagnostics(SimpleNamespace(), interval=0.001)
    calls = 0
    finished = asyncio.Event()

    async def refresh():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("private-prompt")
        finished.set()

    cache.refresh = refresh
    task = asyncio.create_task(cache.run())
    await asyncio.wait_for(finished.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls >= 2
    assert "ValueError" in caplog.text
    assert "private-prompt" not in caplog.text
