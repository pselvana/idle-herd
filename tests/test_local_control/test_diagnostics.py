"""Doctor/status expose evidence and correlations without prompts or capabilities."""

import hashlib
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from typer.testing import CliRunner

from fleet_manager.cli import peer_cli
from fleet_manager.local_control.auth import load_or_create_credentials
from fleet_manager.peer.app import create_peer_app
from fleet_manager.peer.discovery import ModelCapability, PeerDescriptor, PeerStatus
from fleet_manager.peer.jobs import JobSubmission
from fleet_manager.peer.models import AcquireRequest, PeerSettings, ResourceFit


def backend(request):
    return httpx.Response(200, json={"models": [{"name": "tiny:latest", "size": 100}]})


@pytest.mark.asyncio
async def test_doctor_fit_redacted_correlations_and_partial_peer_view(tmp_path):
    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path)), backend_transport=httpx.MockTransport(backend)
    )
    async with app.router.lifespan_context(app):
        ledger = app.state.ledger
        ledger.fit_check = AsyncMock(
            return_value=ResourceFit(model="tiny:latest", required_gb=1, available_gb=64)
        )
        raw_id = "sensitive-client-correlation"
        lease = await ledger.acquire(AcquireRequest(model="tiny", request_id=raw_id))
        submission = JobSubmission(model="tiny", request={"prompt": "private prompt"})
        record, _ = await app.state.job_journal.submit(submission, ledger.worker_id)
        await app.state.job_journal.accept(record.job_id, [], "local")
        descriptor = PeerDescriptor(
            cluster_id=ledger.cluster_id,
            worker_id="remote",
            incarnation="old-incarnation",
            endpoint="https://127.0.0.1:25121",
            models=[ModelCapability(name="tiny")],
        )
        seen_at = time.time() - 120
        app.state.directory = SimpleNamespace(
            snapshot=lambda: [
                PeerStatus(
                    endpoint=descriptor.endpoint,
                    worker_id="remote",
                    descriptor=descriptor,
                    reachable=False,
                    observed_at=seen_at,
                    reason="private exception details",
                )
            ]
        )
        headers = {"Authorization": "Bearer " + app.state.control_credentials.read_token}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost:11435"
        ) as client:
            status = await client.get("/v1/node/status", headers=headers)
            assert status.status_code == 200, status.text
            data = status.json()
            assert data["worker_incarnation"] == ledger.incarnation
            assert data["slots"]["reserved"] == 1
            assert data["waiters"] == 0
            assert data["peer_details"][1]["platform"] == "unknown"
            assert data["peer_details"][1]["reachable"] is False
            assert data["peer_details"][1]["reason"] == "unreachable_or_stale"
            assert data["job_history"][0]["effective_durability"] == "local"
            assert data["job_history"][0]["durable_copies"] == 1
            summary = data["worker_leases"][0]
            assert summary["request_id_hash"] == hashlib.sha256(raw_id.encode()).hexdigest()[:16]
            assert summary["lease_id"] == lease.reservation_id
            assert summary["worker_incarnation"] == ledger.incarnation
            for secret in (raw_id, lease.token, "private prompt", "private exception details"):
                assert secret not in status.text
            assert (await client.get("/v1/node/peers")).status_code == 401
            listed = (await client.get("/v1/node/peers", headers=headers)).json()
            assert listed[0]["worker_id"] == ledger.worker_id
            assert listed[1:] == data["peer_details"][1:]
            diagnostic = await client.get("/v1/node/diagnostics?model=tiny", headers=headers)
            assert diagnostic.status_code == 200, diagnostic.text
            report = diagnostic.json()
            assert report["model_fit"]["observed_available_slots"] == 0
            assert report["model_fit"]["reason"] == "busy"
            assert any(check["code"] == "peers" and check["remedy"] for check in report["checks"])
            assert (
                await client.get(
                    "/v1/node/diagnostics", headers={**headers, "Origin": "https://evil.test"}
                )
            ).status_code == 403
            assert (
                await client.get(
                    "/v1/node/diagnostics", params={"model": "x" * 513}, headers=headers
                )
            ).status_code == 422
            await ledger.release(lease.token)
            report = (await client.get("/v1/node/diagnostics?model=tiny", headers=headers)).json()
            assert report["model_fit"]["eligible"] is True
            assert report["model_fit"]["reason"] is None
            ledger.fit_check.assert_awaited_with("tiny:latest")
        app.state.directory = None


@pytest.mark.asyncio
async def test_doctor_reports_missing_backend_and_donation_quota_without_granting(tmp_path):
    def down(request):
        return httpx.Response(503)

    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path), peer_max_concurrent=0),
        backend_transport=httpx.MockTransport(down),
    )
    async with app.router.lifespan_context(app):
        headers = {"Authorization": "Bearer " + app.state.control_credentials.read_token}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost:11435"
        ) as client:
            status = (await client.get("/v1/node/status", headers=headers)).json()
            assert status["idle"]["reason"] == "peer_quota"
            report = (await client.get("/v1/node/diagnostics?model=tiny", headers=headers)).json()
            fit = next(check for check in report["checks"] if check["code"] == "model_fit")
            assert "backend_unavailable" in fit["detail"]
            assert "11434" in fit["remedy"]
            assert report["quarantined_slots"] == 0
            assert (await app.state.ledger.job_summaries()) == []


def test_doctor_friendly_json_and_model_parameter(monkeypatch, tmp_path):
    monkeypatch.setenv("IDLE_HERD_DATA_DIR", str(tmp_path))
    credentials = load_or_create_credentials(tmp_path)
    seen = []
    report = {
        "version": 1,
        "platform": "Test OS",
        "checks": [
            {
                "state": "attention",
                "title": "Activity",
                "detail": "activity_unknown",
                "remedy": "Check host permissions.",
            }
        ],
    }

    def reply(request):
        seen.append(request)
        return httpx.Response(200, json=report)

    real_client = httpx.Client
    monkeypatch.setattr(
        peer_cli.httpx,
        "Client",
        lambda **kw: real_client(transport=httpx.MockTransport(reply), **kw),
    )
    runner = CliRunner()
    output = runner.invoke(peer_cli.cli, ["doctor", "--model", "model/name?a=b"])
    assert output.exit_code == 0, output.output
    assert "Try: Check host permissions." in output.output
    assert seen[0].url.params["model"] == "model/name?a=b"
    assert seen[0].headers["authorization"] == "Bearer " + credentials.read_token
    output = runner.invoke(peer_cli.cli, ["doctor", "--json"])
    assert output.exit_code == 0
    assert json.loads(output.output) == report
