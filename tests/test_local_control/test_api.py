import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from fleet_manager.local_control.auth import load_or_create_credentials
from fleet_manager.local_control.models import ModeState
from fleet_manager.local_control.router import create_router
from fleet_manager.peer.models import EligibilityDecision
from fleet_manager.peer.sensors import IdleSignal


@pytest.mark.asyncio
async def test_worker_jobs_identify_origin_host_and_quarantine_recovery(tmp_path, monkeypatch):
    import time

    from fleet_manager.peer.app import create_peer_app
    from fleet_manager.peer.models import AcquireRequest, PeerSettings

    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path), slots=2, background_policy="always"),
        backend_transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"models": [{"name": "tiny:latest", "size": 1000}]}
            )
        ),
    )
    async with app.router.lifespan_context(app):
        app.state.hostname = "my-mac.local"
        app.state.directory = SimpleNamespace(
            host_for=lambda worker: "pstudio.local" if worker == "remote-worker" else None,
            snapshot=lambda: [],
        )
        ledger = app.state.ledger
        remote = await ledger.acquire(
            AcquireRequest(
                model="tiny", request_id="peer/remote-worker/request", workload="background"
            ),
            peer=True,
        )
        local = await ledger.acquire(AcquireRequest(model="tiny", request_id="local"))
        await ledger.consume(remote.token, remote.model)
        await ledger.finish(remote.token, stopped=False)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:11435",
            headers={"Authorization": "Bearer " + app.state.control_credentials.read_token},
        ) as client:
            response = await client.get("/v1/node/jobs")
            assert response.status_code == 200, response.text
            leases = {j["job_id"]: j for j in response.json()["worker_leases"]}
            assert leases[remote.reservation_id]["origin_worker_id"] == "remote-worker"
            assert leases[remote.reservation_id]["origin_host"] == "pstudio.local"
            assert leases[remote.reservation_id]["quarantine_recovery"] == "manual_required"
            assert leases[local.reservation_id]["origin_host"] == "my-mac.local"
            assert remote.token not in response.text and local.token not in response.text
            monkeypatch.setattr(
                app.state.streaming_proxy, "recovery_deadline", lambda _: time.time() + 30
            )
            response = await client.get("/v1/node/jobs")
            lease = next(
                j for j in response.json()["worker_leases"] if j["job_id"] == remote.reservation_id
            )
            assert lease["quarantine_recovery"] == "awaiting_backend"
            denied = await client.delete(
                "/v1/capacity/reservations/" + remote.reservation_id,
                headers={"X-Idle-Herd-Reservation": remote.token},
            )
            assert denied.status_code == 409


def test_credentials_are_persistent_and_private(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_or_create_credentials(tmp_path, create=False)
    assert not (tmp_path / "control-credentials.json").exists()
    first = load_or_create_credentials(tmp_path)
    assert first == load_or_create_credentials(tmp_path)
    assert first.read_token != first.control_token
    if os.name != "nt":
        assert (tmp_path.stat().st_mode & 0o777) == 0o700
        assert ((tmp_path / "control-credentials.json").stat().st_mode & 0o777) == 0o600
        (tmp_path / "control-credentials.json").chmod(0o644)
        with pytest.raises(PermissionError):
            load_or_create_credentials(tmp_path)


@pytest.mark.asyncio
async def test_scopes_and_redacted_status(tmp_path):
    app = FastAPI()
    app.include_router(create_router())
    app.state.control_credentials = load_or_create_credentials(tmp_path)
    app.state.ledger = SimpleNamespace(
        worker_id="worker",
        settings=SimpleNamespace(slots=2),
        mode_state=AsyncMock(return_value=ModeState(mode="auto", revision=1)),
        set_mode=AsyncMock(return_value=ModeState(mode="offline", revision=2)),
        job_summaries=AsyncMock(return_value=[]),
    )
    decision = EligibilityDecision(
        workload="background",
        policy="idle_only",
        eligible=False,
        reason="activity_unknown",
        state="unknown",
        activity="unknown",
        observed_at=10,
        signal=IdleSignal(observed_at=10, source="unknown"),
    )
    app.state.idle_policy = SimpleNamespace(decision=AsyncMock(return_value=decision))
    app.state.directory = None
    app.state.peer_error = None
    app.state.registry = SimpleNamespace(get_node=lambda _: None)
    app.state.job_journal = SimpleNamespace(list_jobs=AsyncMock(return_value=[]))
    read = {"Authorization": "Bearer " + app.state.control_credentials.read_token}
    control = {"Authorization": "Bearer " + app.state.control_credentials.control_token}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://localhost"
    ) as client:
        assert (await client.get("/v1/node/status")).status_code == 401
        assert (
            await client.put("/v1/node/mode", headers=read, json={"mode": "offline"})
        ).status_code == 403
        assert (
            await client.put(
                "/v1/node/mode",
                headers={**control, "Origin": "https://evil.example"},
                json={"mode": "offline"},
            )
        ).status_code == 403
        assert (
            await client.put("/v1/node/mode", headers=control, json={"mode": "offline"})
        ).status_code == 200
        response = await client.get("/v1/node/status", headers=read)
        assert response.status_code == 200
        assert response.json()["idle"]["reason"] == "activity_unknown"
        assert response.json()["resources"]["cpu_percent"] is None
        assert response.json()["outbound_running"] == 0
        app.state.outbound_running = 2
        updated = await client.get("/v1/node/status", headers=read)
        assert updated.json()["outbound_running"] == 2
        assert app.state.control_credentials.read_token not in response.text
        assert app.state.control_credentials.control_token not in response.text


@pytest.mark.asyncio
async def test_integrated_preferences_and_peer_isolation(tmp_path):
    from fleet_manager.peer.app import create_peer_app, create_peer_rpc_app
    from fleet_manager.peer.models import PeerSettings

    def backend(request):
        return httpx.Response(200, json={"models": []})

    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path)), backend_transport=httpx.MockTransport(backend)
    )
    async with app.router.lifespan_context(app):
        from fleet_manager.peer.jobs import JobSubmission

        record, _ = await app.state.job_journal.submit(
            JobSubmission(model="tiny", request={"prompt": "secret prompt"}),
            app.state.ledger.worker_id,
        )
        await app.state.job_journal.accept(record.job_id, [], "local")
        credentials = app.state.control_credentials
        read = {"Authorization": f"Bearer {credentials.read_token}"}
        control = {"Authorization": f"Bearer {credentials.control_token}"}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost:11435"
        ) as client:
            snapshot = await client.get("/v1/node/status", headers=read)
            assert snapshot.status_code == 200
            assert snapshot.headers["cache-control"] == "no-store"
            listed = await client.get("/v1/node/jobs", headers=read)
            assert listed.json()["worker_leases"] == []
            assert "secret prompt" not in listed.text
            denied = await client.post(f"/v1/node/jobs/{record.job_id}/cancel", headers=read)
            assert denied.status_code == 403
            canceled = await client.post(f"/v1/node/jobs/{record.job_id}/cancel", headers=control)
            assert canceled.status_code == 200
            assert canceled.json()["state"] == "canceled"
            response = await client.get("/v1/node/preferences", headers=read)
            assert response.status_code == 200
            version = response.json()["version"]
            update = {"expected_version": version, "values": {"idle_seconds": 123}}
            assert (
                await client.patch("/v1/node/preferences", headers=control, json=update)
            ).status_code == 200
            assert (
                await client.patch("/v1/node/preferences", headers=control, json=update)
            ).status_code == 409
            assert (
                await client.put("/v1/node/mode", headers=control, json={"mode": "offline"})
            ).status_code == 200
            status = (await client.get("/v1/node/status", headers=read)).json()
            assert status["mode"]["mode"] == "offline"
            assert status["idle"]["reason"] == "manual_offline"
            assert (
                await client.post("/v1/node/jobs/missing/cancel", headers=control)
            ).status_code == 404
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_peer_rpc_app(app)), base_url="http://peer"
        ) as peer:
            assert (await peer.get("/v1/node/status", headers=control)).status_code != 200


def test_credentials_reject_links(tmp_path):
    if os.name == "nt":
        pytest.skip("symlink permissions vary on Windows")
    target = tmp_path / "target"
    target.write_text("not credentials")
    (tmp_path / "control-credentials.json").symlink_to(target)
    with pytest.raises((OSError, PermissionError)):
        load_or_create_credentials(tmp_path)
    assert target.read_text() == "not credentials"


@pytest.mark.asyncio
async def test_service_toggle_uses_only_fixed_helper(tmp_path, monkeypatch):
    from fleet_manager.local_control import router as control_router
    from fleet_manager.peer.app import create_peer_app
    from fleet_manager.peer.models import PeerSettings

    calls = []
    monkeypatch.setattr(control_router.service.sys, "platform", "darwin")
    monkeypatch.setattr(control_router.service, "plist_path", lambda: tmp_path / "service.plist")
    monkeypatch.setattr(
        control_router.service,
        "install",
        lambda data_dir, **kwargs: calls.append(("install", data_dir)),
    )

    def launchctl(*args):
        calls.append(args)
        return SimpleNamespace(stdout='"com.idle-herd.daemon" => enabled')

    monkeypatch.setattr(control_router.service, "_launchctl", launchctl)
    monkeypatch.setattr(control_router.service, "status", lambda: "state = running")
    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path)),
        backend_transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"models": []})),
    )
    async with app.router.lifespan_context(app):
        credentials = app.state.control_credentials
        read = {"Authorization": f"Bearer {credentials.read_token}"}
        control = {"Authorization": f"Bearer {credentials.control_token}"}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost:11435"
        ) as client:
            assert (
                await client.put("/v1/node/service", headers=read, json={"enabled": True})
            ).status_code == 403
            assert (
                await client.put(
                    "/v1/node/service",
                    headers=control,
                    json={"enabled": True, "executable": "/tmp/evil"},
                )
            ).status_code == 422
            result = await client.put("/v1/node/service", headers=control, json={"enabled": True})
            assert result.status_code == 200
            assert result.json()["enabled"] is True
            assert result.json()["running"] is True
            assert calls[0] == ("install", str(tmp_path))
            assert any(call[0] == "enable" for call in calls)
            assert all(call[0] not in {"bootstrap", "bootout"} for call in calls)


@pytest.mark.asyncio
async def test_event_snapshot_heartbeat_and_reconnect_redact(tmp_path):
    import asyncio
    import json

    from starlette.requests import Request

    from fleet_manager.peer.app import create_peer_app
    from fleet_manager.peer.jobs import JobSubmission
    from fleet_manager.peer.models import PeerSettings

    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path)),
        backend_transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"models": []})),
    )
    async with app.router.lifespan_context(app):
        await app.state.job_journal.submit(
            JobSubmission(model="tiny", request={"prompt": "sensitive input"}),
            app.state.ledger.worker_id,
        )
        token = app.state.control_credentials.read_token

        async def receive():
            await asyncio.sleep(60)
            return {"type": "http.disconnect"}

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/v1/node/events",
                "headers": [(b"authorization", f"Bearer {token}".encode())],
                "app": app,
            },
            receive,
        )
        endpoint = next(
            route.endpoint for route in create_router().routes if route.path == "/v1/node/events"
        )
        response = await endpoint(request)
        first = await anext(response.body_iterator)
        second = await anext(response.body_iterator)
        await response.body_iterator.aclose()
        again = await endpoint(request)
        reconnected = await anext(again.body_iterator)
        await again.body_iterator.aclose()
        assert "event: snapshot" in first and "event: heartbeat" in second
        assert "event: snapshot" in reconnected
        for chunk in (first, second, reconnected):
            event = json.loads(
                next(line[6:] for line in chunk.splitlines() if line.startswith("data: "))
            )
            assert event["id"] in chunk
            assert "sensitive input" not in chunk
            assert token not in chunk


@pytest.mark.asyncio
async def test_dashboard_read_only_and_browser_boundaries(tmp_path):
    from fleet_manager.local_control.dashboard import router as dashboard_router
    from fleet_manager.peer.app import create_peer_app
    from fleet_manager.peer.models import PeerSettings

    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path)),
        backend_transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"models": []})),
    )
    app.include_router(dashboard_router)
    async with app.router.lifespan_context(app):
        creds = app.state.control_credentials
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost:11435"
        ) as client:
            html = await client.get("/dashboard")
            assert html.status_code == 200
            assert "frame-ancestors 'none'" in html.headers["content-security-policy"]
            assert html.headers["cache-control"] == "no-store"
            assert creds.read_token not in html.text
            assert creds.control_token not in html.text
            assert "textContent" in html.text
            assert "localStorage" not in html.text and "sessionStorage" not in html.text
            assert (await client.get("/v1/node/status")).status_code == 401
            read = {"Authorization": f"Bearer {creds.read_token}", "Sec-Fetch-Site": "same-origin"}
            assert (await client.get("/v1/node/status", headers=read)).status_code == 200
            assert (
                await client.get(
                    "/v1/node/status", headers={**read, "Origin": "https://example.com"}
                )
            ).status_code == 403
            assert (
                await client.get(
                    "/v1/node/status", headers={**read, "Sec-Fetch-Site": "cross-site"}
                )
            ).status_code == 403
            control = {
                "Authorization": f"Bearer {creds.control_token}",
                "Sec-Fetch-Site": "same-origin",
            }
            assert (
                await client.put("/v1/node/mode", headers=control, json={"mode": "paused"})
            ).status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("platform", ["linux", "win32"])
async def test_service_api_cross_platform_scopes_and_unknown(tmp_path, monkeypatch, platform):
    import subprocess

    from fleet_manager.local_control import service
    from fleet_manager.peer.app import create_peer_app
    from fleet_manager.peer.models import PeerSettings

    monkeypatch.setattr(service.sys, "platform", platform)
    path = tmp_path / "service.conf"
    monkeypatch.setattr(service, "systemd_path", lambda: path)
    monkeypatch.setattr(service, "windows_task_path", lambda: path)
    calls = []

    def install(data_dir, **kwargs):
        calls.append(("install", data_dir))
        path.write_text("installed")
        return path

    def manager(*args, **kwargs):
        calls.append(args)
        if args[0] in ("is-enabled", "is-active"):
            return SimpleNamespace(
                stdout="enabled\n" if args[0] == "is-enabled" else "active\n", returncode=0
            )
        if args[0] == "/Query":
            if "/XML" in args:
                return SimpleNamespace(
                    stdout="<Task><Settings><Enabled>true</Enabled></Settings></Task>"
                )
            if "/FO" in args:
                return SimpleNamespace(stdout='"TaskName","Status"\n"task","Running"\n')
            raise subprocess.CalledProcessError(1, args)
        return SimpleNamespace(stdout="", returncode=0)

    monkeypatch.setattr(service, "install", install)
    monkeypatch.setattr(service, "_systemctl", manager)
    monkeypatch.setattr(service, "_schtasks", manager)
    app = create_peer_app(
        PeerSettings(data_dir=str(tmp_path)),
        backend_transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"models": []})),
    )
    async with app.router.lifespan_context(app):
        credentials = app.state.control_credentials
        read = {"Authorization": f"Bearer {credentials.read_token}"}
        control = {"Authorization": f"Bearer {credentials.control_token}"}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost:11435"
        ) as client:
            assert (
                await client.put("/v1/node/service", headers=read, json={"enabled": True})
            ).status_code == 403
            assert (
                await client.put(
                    "/v1/node/service",
                    headers=control,
                    json={"enabled": True, "executable": "evil"},
                )
            ).status_code == 422
            assert calls == []
            result = await client.put("/v1/node/service", headers=control, json={"enabled": True})
            assert result.status_code == 200
            assert result.json()["platform"] == ("linux" if platform == "linux" else "windows")
            assert result.json()["enabled"] is True
            assert result.json()["running"] is True
            assert all("--now" not in call and "/End" not in call for call in calls)
            if platform == "win32":
                assert any(call[0] == "/Create" and "/F" not in call for call in calls)
            result = await client.put("/v1/node/service", headers=control, json={"enabled": False})
            assert result.status_code == 200
            assert all("--now" not in call and "/End" not in call for call in calls)

            def failure(*args, **kwargs):
                raise OSError("manager unavailable")

            monkeypatch.setattr(service, "_systemctl", failure)
            monkeypatch.setattr(service, "_schtasks", failure)
            result = await client.get("/v1/node/service", headers=read)
            assert result.status_code == 200
            assert result.json()["enabled"] is None
            assert result.json()["running"] is None
