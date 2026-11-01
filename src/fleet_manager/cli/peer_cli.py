"""Native peer ingress, offline enrollment and worker recovery commands."""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated
from urllib.parse import urlsplit

import httpx
import typer
import uvicorn

from fleet_manager.local_control import service
from fleet_manager.local_control.auth import load_or_create_credentials
from fleet_manager.peer.app import create_peer_app, create_peer_rpc_app
from fleet_manager.peer.identity import (
    DeviceCredential,
    EnrollmentRequest,
    IdentityError,
    PeerIdentity,
)
from fleet_manager.peer.local_ledger import LocalLedger
from fleet_manager.peer.models import AdmissionError, PeerSettings

cli = typer.Typer(no_args_is_help=False)


@asynccontextmanager
async def offline_worker():
    async def unused_fit(model):
        raise RuntimeError("offline maintenance does not admit inference")

    ledger = LocalLedger(PeerSettings(), unused_fit)
    await ledger.open()  # Also excludes concurrent daemon/issuer/maintenance processes.
    try:
        yield ledger
    finally:
        await ledger.close()


async def serve(settings):
    app = create_peer_app(settings)
    # Own the shared lifespan once. Both listeners stop before durable storage closes.
    async with app.router.lifespan_context(app):
        servers = [
            uvicorn.Server(
                uvicorn.Config(
                    app,
                    host="127.0.0.1",
                    port=11435,
                    lifespan="off",
                    proxy_headers=False,
                )
            )
        ]
        if settings.peer_endpoint and app.state.identity:
            endpoint = urlsplit(settings.peer_endpoint)
            identity = app.state.identity
            servers.append(
                uvicorn.Server(
                    uvicorn.Config(
                        create_peer_rpc_app(app),
                        host=endpoint.hostname,
                        port=endpoint.port,
                        ssl_certfile=str(identity.cert_path),
                        ssl_keyfile=str(identity.key_path),
                        lifespan="off",
                        proxy_headers=False,
                        limit_concurrency=256,
                        timeout_keep_alive=5,
                    )
                )
            )

        async def serve_optional_peer(server):
            # Only localhost ingress owns process signal handling/lifetime.
            server.capture_signals = contextlib.nullcontext
            try:
                await server.serve()
                if not servers[0].should_exit:
                    raise OSError("peer listener exited")
            except (OSError, SystemExit) as exc:
                app.state.peer_error = f"Peer listener unavailable: {exc}"
                app.state.directory = None
                peer_task = getattr(app.state, "peer_task", None)
                if peer_task:
                    peer_task.cancel()

        local_task = asyncio.create_task(servers[0].serve())
        tasks = [local_task]
        if len(servers) > 1:
            tasks.append(asyncio.create_task(serve_optional_peer(servers[1])))
        try:
            await local_task
        finally:
            for server in servers:
                server.should_exit = True
            await asyncio.gather(*tasks)


@cli.callback(invoke_without_command=True)
def run(ctx: typer.Context):
    """Run localhost APIs; optionally enable a separate authenticated TLS peer listener.

    Configure IDLE_HERD_PEER_ENDPOINT and IDLE_HERD_SEEDS (JSON array).
    Peer ports are explicitly allocated; existing backend ports are unchanged.
    """
    if ctx.invoked_subcommand is None:
        asyncio.run(serve(PeerSettings()))


@cli.command()
def init():
    """Initialize a one-node cluster and its private enrollment authority, offline."""

    async def initialize():
        async with offline_worker() as ledger:
            identity = await PeerIdentity.open(
                ledger.settings.data_dir, ledger.worker_id, ledger.cluster_id
            )
            return {
                "worker_id": ledger.worker_id,
                "cluster_id": ledger.cluster_id,
                "trust_fingerprint": identity.fingerprint,
            }

    typer.echo(json.dumps(asyncio.run(initialize()), indent=2))


def write_artifact(path: Path, artifact):
    # Offline CLI only; public credentials and requests, never private keys.
    path.write_text(artifact.model_dump_json(indent=2) + "\n")


@cli.command("enrollment-request")
def enrollment_request(output: Annotated[Path, typer.Option("--output")]):
    """Create a public device request on a new joining worker, before init/serving."""

    async def request():
        async with offline_worker() as ledger:
            return await PeerIdentity.enrollment_request(ledger.settings.data_dir, ledger.worker_id)

    write_artifact(output, asyncio.run(request()))
    typer.echo(f"Public enrollment request written to {output}")


@cli.command("issue-credential")
def issue_credential(
    request: Path,
    output: Annotated[Path, typer.Option("--output")],
    days: int = typer.Option(30, min=1, max=90),
):
    """Approve a device request on the offline authority; transfer only public artifacts."""

    async def issue():
        async with offline_worker() as ledger:
            identity = await PeerIdentity.open_authority(
                ledger.settings.data_dir, ledger.worker_id, ledger.cluster_id
            )
            return await identity.issue_credential(
                EnrollmentRequest.model_validate_json(request.read_bytes()), days
            )

    write_artifact(output, asyncio.run(issue()))
    typer.echo(f"Device credential written to {output}")


@cli.command()
def join(credential: Path, fingerprint: str = typer.Option(..., "--fingerprint")):
    """Import a public device credential using an independently verified CA fingerprint.

    Requires a new joining ledger with no lease history. Existing authorities cannot
    silently change clusters. The daemon must be stopped for every enrollment command.
    """

    async def enroll():
        record = DeviceCredential.model_validate_json(credential.read_bytes())
        async with offline_worker() as ledger:
            if record.worker_id != ledger.worker_id:
                raise IdentityError("credential belongs to another worker")
            # Check eligibility before writing identity material.
            count = await (await ledger._db.execute("SELECT count(*) FROM leases")).fetchone()
            if count[0] and record.cluster_id != ledger.cluster_id:
                raise AdmissionError("cannot_enroll_worker_with_lease_history")
            await PeerIdentity.import_credential(ledger.settings.data_dir, record, fingerprint)
            await ledger.adopt_cluster(record.cluster_id)
            await PeerIdentity.open(ledger.settings.data_dir, ledger.worker_id, ledger.cluster_id)

    asyncio.run(enroll())
    typer.echo("Device enrolled; configure peer endpoint and one or more reachable seeds.")


@cli.command()
def revoke(credential: Path):
    """Revoke a device credential on the offline authority; gossip propagates on reconnect."""

    async def revoke_device():
        async with offline_worker() as ledger:
            identity = await PeerIdentity.open_authority(
                ledger.settings.data_dir, ledger.worker_id, ledger.cluster_id
            )
            await identity.revoke_credential(
                DeviceCredential.model_validate_json(credential.read_bytes())
            )

    asyncio.run(revoke_device())
    typer.echo("Revocation saved; disconnected peers learn it on authenticated reconnect.")


@cli.command()
def reconcile(confirm_backend_stopped: bool = typer.Option(False, "--confirm-backend-stopped")):
    """After stopping the peer AND all Ollama runners, release quarantined slots."""
    if not confirm_backend_stopped:
        raise typer.BadParameter("Stop the peer and ALL backend runners; --confirm-backend-stopped")

    async def repair():
        async with offline_worker() as ledger:
            await ledger.reconcile_stopped_backend()

    asyncio.run(repair())
    typer.echo("Recorded backend-stop attestation; quarantined leases released.")


service_cli = typer.Typer(help="Manage the macOS per-user launchd service.")
cli.add_typer(service_cli, name="service")


def _control_request(method: str, endpoint: str, payload: dict | None = None):
    try:
        credentials = load_or_create_credentials(PeerSettings().data_dir, create=False)
        token = credentials.read_token if method == "GET" else credentials.control_token
        with httpx.Client(timeout=10.0, trust_env=False) as client:
            response = client.request(
                method,
                f"http://127.0.0.1:11435/v1/node/{endpoint}",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )
        if response.is_error:
            typer.echo(f"Local control request failed (HTTP {response.status_code}).", err=True)
            raise typer.Exit(1)
        typer.echo(json.dumps(response.json(), indent=2))
    except (OSError, KeyError, ValueError, httpx.HTTPError):
        typer.echo(
            "Cannot access local control API or credentials; is the daemon running?", err=True
        )
        raise typer.Exit(1) from None


@cli.command()
def status():
    """Show authenticated local daemon status."""
    _control_request("GET", "status")


@cli.command()
def jobs():
    """List local jobs."""
    _control_request("GET", "jobs")


@cli.command()
def doctor():
    """Show local diagnostics."""
    _control_request("GET", "diagnostics")


@cli.command("cancel")
def cancel_local_job(job_id: str):
    """Request cancellation of an origin-owned durable job; outcome may remain unknown."""
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", job_id):
        raise typer.BadParameter("invalid job ID")
    _control_request("POST", f"jobs/{job_id}/cancel")


@cli.command()
def preferences(
    values: str = typer.Option(None, "--set", help="JSON object of preference values."),
    expected_version: int = typer.Option(None, "--expected-version", min=0),
):
    """Read preferences or update them with an expected version."""
    if values is None:
        if expected_version is not None:
            raise typer.BadParameter("--expected-version requires --set")
        _control_request("GET", "preferences")
        return
    if expected_version is None:
        raise typer.BadParameter("--set requires --expected-version")
    try:
        parsed = json.loads(values)
        if not isinstance(parsed, dict):
            raise ValueError
    except ValueError:
        raise typer.BadParameter("--set must be a JSON object") from None
    _control_request(
        "PATCH", "preferences", {"expected_version": expected_version, "values": parsed}
    )


def _duration(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)(s|m|h|d)", value)
    if not match:
        raise typer.BadParameter("duration must be a positive number followed by s, m, h, or d")
    seconds = int(match.group(1)) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[match.group(2)]
    if seconds > 604800:
        raise typer.BadParameter("duration cannot exceed seven days")
    return seconds


@cli.command()
def offline(duration: str = typer.Option(None, "--for", help="Temporary override, e.g. 1h.")):
    """Withdraw from the cluster while retaining localhost inference."""
    payload = {"mode": "offline"}
    if duration is not None:
        payload["duration_seconds"] = _duration(duration)
    _control_request("PUT", "mode", payload)


@cli.command()
def auto():
    """Return to automatic idle policy."""
    _control_request("PUT", "mode", {"mode": "auto"})


@cli.command()
def drain():
    """Drain remote work while retaining local inference."""
    _control_request("PUT", "mode", {"mode": "draining"})


@cli.command()
def pause():
    """Reject new inference while existing work drains."""
    _control_request("PUT", "mode", {"mode": "paused"})


def _service_action(action):
    try:
        result = action()
        if result:
            typer.echo(result)
    except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
        typer.echo(f"Service operation failed: {exc}", err=True)
        raise typer.Exit(1) from None


@service_cli.command("install")
def service_install():
    """Write a launch agent without starting or registering it."""
    settings = PeerSettings()
    _service_action(lambda: service.install(settings.data_dir, settings=settings))


@service_cli.command("enable")
def service_enable():
    """Enable launch at login and register the installed agent."""
    _service_action(service.enable)


@service_cli.command("disable")
def service_disable():
    """Disable launch at login and unload the user service."""
    _service_action(service.disable)


@service_cli.command("start")
@cli.command("start")
def service_start():
    """Start the registered user service."""
    _service_action(service.start)


@service_cli.command("status")
def service_status():
    """Inspect launchd registration and process state."""
    _service_action(service.status)


def main():
    try:
        cli()
    except (IdentityError, AdmissionError) as exc:
        typer.echo(str(exc), err=True)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
