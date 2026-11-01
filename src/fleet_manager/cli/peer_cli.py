"""Native single-node peer CLI; existing herd/herd-node commands remain available."""

from __future__ import annotations

import asyncio

import typer
import uvicorn

from fleet_manager.peer.app import create_peer_app
from fleet_manager.peer.local_ledger import LocalLedger
from fleet_manager.peer.models import PeerSettings

cli = typer.Typer(no_args_is_help=False)


@cli.callback(invoke_without_command=True)
def run(ctx: typer.Context):
    """Run local APIs on 127.0.0.1:11435 with no peers or consensus service.

    Configure via IDLE_HERD_DATA_DIR, IDLE_HERD_BACKEND_URL, IDLE_HERD_SLOTS,
    IDLE_HERD_MODEL_SLOTS (JSON) and the documented memory allowances.
    """
    if ctx.invoked_subcommand is None:
        uvicorn.run(create_peer_app(PeerSettings()), host="127.0.0.1", port=11435)


@cli.command()
def reconcile(
    confirm_backend_stopped: bool = typer.Option(False, "--confirm-backend-stopped"),
):
    """After stopping the peer AND all Ollama runners, release quarantined slots.

    This explicit operator attestation is required: Ollama /api/ps does not
    prove a request ended. Never run this while backend execution may survive.
    """
    if not confirm_backend_stopped:
        raise typer.BadParameter("Stop the peer and ALL backend runners; --confirm-backend-stopped")

    async def repair():
        async def unused_fit(model):
            raise RuntimeError("reconciliation does not admit inference")

        ledger = LocalLedger(PeerSettings(), unused_fit)
        await ledger.open()  # Refuses maintenance while another daemon owns this directory.
        try:
            await ledger.reconcile_stopped_backend()
        finally:
            await ledger.close()

    asyncio.run(repair())
    typer.echo("Recorded backend-stop attestation; quarantined leases released.")


def main():
    cli()


if __name__ == "__main__":
    main()
