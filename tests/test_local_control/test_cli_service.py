import json
import plistlib
from pathlib import Path

import httpx
from typer.testing import CliRunner

from fleet_manager.cli import peer_cli
from fleet_manager.local_control import service


def test_cli_scopes_and_modes(monkeypatch, tmp_path):
    monkeypatch.setenv("IDLE_HERD_DATA_DIR", str(tmp_path))
    read_token, control_token = "reader" * 8, "controller" * 8
    (tmp_path / "control-credentials.json").write_text(
        json.dumps({"version": 1, "read_token": read_token, "control_token": control_token})
    )
    (tmp_path / "control-credentials.json").chmod(0o600)
    calls = []

    def handler(request):
        calls.append(
            (
                request.method,
                request.url.path,
                request.headers["authorization"],
                json.loads(request.content) if request.content else None,
            )
        )
        return httpx.Response(200, json={"ok": True})

    real_client = httpx.Client
    monkeypatch.setattr(
        peer_cli.httpx,
        "Client",
        lambda **kw: real_client(transport=httpx.MockTransport(handler), **kw),
    )
    runner = CliRunner()
    assert runner.invoke(peer_cli.cli, ["status"]).exit_code == 0
    assert runner.invoke(peer_cli.cli, ["offline", "--for", "1h"]).exit_code == 0
    assert runner.invoke(peer_cli.cli, ["cancel", "job-123"]).exit_code == 0
    assert (
        runner.invoke(
            peer_cli.cli, ["preferences", "--set", '{"require_ac":true}', "--expected-version", "2"]
        ).exit_code
        == 0
    )
    assert calls == [
        ("GET", "/v1/node/status", "Bearer " + read_token, None),
        (
            "PUT",
            "/v1/node/mode",
            "Bearer " + control_token,
            {"mode": "offline", "duration_seconds": 3600},
        ),
        (
            "POST",
            "/v1/node/jobs/job-123/cancel",
            "Bearer " + control_token,
            None,
        ),
        (
            "PATCH",
            "/v1/node/preferences",
            "Bearer " + control_token,
            {"expected_version": 2, "values": {"require_ac": True}},
        ),
    ]
    assert runner.invoke(peer_cli.cli, ["offline", "--for", "0h"]).exit_code != 0
    assert runner.invoke(peer_cli.cli, ["cancel", "../escape"]).exit_code != 0
    (tmp_path / "control-credentials.json").chmod(0o644)
    result = runner.invoke(peer_cli.cli, ["status"])
    assert result.exit_code != 0
    assert read_token not in result.output and control_token not in result.output


def test_launchd_install_and_explicit_registration(monkeypatch, tmp_path):
    monkeypatch.setattr(service.sys, "platform", "darwin")
    path = tmp_path / "agents" / "service.plist"
    monkeypatch.setattr(service, "plist_path", lambda: path)
    executable = tmp_path / "idle-herd"
    executable.write_text("executable")
    commands = []
    monkeypatch.setattr(service, "_launchctl", lambda *args: commands.append(args))
    service.install(str(tmp_path / "data"), str(executable))
    payload = plistlib.loads(path.read_bytes())
    assert payload["ProgramArguments"] == [str(executable)]
    assert Path(payload["WorkingDirectory"]).is_absolute()
    assert payload["EnvironmentVariables"]["IDLE_HERD_DATA_DIR"] == str(tmp_path / "data")
    assert path.stat().st_mode & 0o777 == 0o600
    assert commands == []
    service.enable()
    service.start()
    assert [command[0] for command in commands] == ["enable", "bootstrap", "kickstart"]


def test_service_unsupported(monkeypatch):
    monkeypatch.setattr(service.sys, "platform", "linux")
    for action in ("install", "enable", "disable", "start", "status"):
        result = CliRunner().invoke(peer_cli.cli, ["service", action])
        assert result.exit_code != 0
        assert "only on macOS" in result.output


def test_install_preserves_effective_settings(tmp_path, monkeypatch):
    import json
    import sys

    from fleet_manager.peer.models import PeerSettings

    destination = tmp_path / "agent.plist"
    monkeypatch.setattr(service, "_check_platform", lambda: None)
    monkeypatch.setattr(service, "plist_path", lambda: destination)
    settings = PeerSettings(
        data_dir=str(tmp_path / "data"),
        backend_url="http://127.0.0.1:12345",
        slots=7,
        interactive_policy="manual",
        require_ac=True,
        peer_endpoint="https://127.0.0.1:15000",
        seeds=["https://127.0.0.1:15001"],
        model_slots={"tiny": 2},
    )
    service.install(settings.data_dir, settings=settings)
    payload = plistlib.loads(destination.read_bytes())
    env = payload["EnvironmentVariables"]
    assert payload["ProgramArguments"] == [sys.executable, "-m", "fleet_manager.cli.peer_cli"]
    assert env["IDLE_HERD_BACKEND_URL"] == settings.backend_url
    assert env["IDLE_HERD_SLOTS"] == "7"
    assert env["IDLE_HERD_INTERACTIVE_POLICY"] == "manual"
    assert env["IDLE_HERD_REQUIRE_AC"] == "true"
    assert env["IDLE_HERD_PEER_ENDPOINT"] == settings.peer_endpoint
    assert json.loads(env["IDLE_HERD_SEEDS"]) == settings.seeds
    assert json.loads(env["IDLE_HERD_MODEL_SLOTS"]) == settings.model_slots
    assert destination.stat().st_mode & 0o777 == 0o600
