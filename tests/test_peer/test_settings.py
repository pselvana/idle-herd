"""Configuration must not expose the Phase 1 worker or admit invalid budgets."""

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from fleet_manager.cli.peer_cli import cli
from fleet_manager.peer.models import PeerSettings


@pytest.mark.parametrize(
    "url",
    [
        "http://192.0.2.1:11434",
        "https://localhost:11434",
        "http://localhost:11435",
        "http://user:secret@localhost:11434",
        "http://localhost:11434/path",
    ],
)
def test_backend_must_be_local_and_cannot_recurse_into_ingress(url):
    with pytest.raises(ValidationError):
        PeerSettings(backend_url=url)


def test_resource_budgets_must_be_finite():
    with pytest.raises(ValidationError):
        PeerSettings(request_memory_gb=float("nan"))
    with pytest.raises(ValidationError):
        PeerSettings(slots=0)
    with pytest.raises(ValidationError):
        PeerSettings(model_slots={"tiny": 0})


def test_reconciliation_requires_explicit_backend_stop_attestation(tmp_path, monkeypatch):
    data = tmp_path / "not-initialized"
    monkeypatch.setenv("IDLE_HERD_DATA_DIR", str(data))
    result = CliRunner().invoke(cli, ["reconcile"])
    assert result.exit_code == 2
    assert "confirm-backend-stopped" in result.output
    assert not data.exists()
