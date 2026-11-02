"""Owner-authorized model provisioning; never mounted on the peer listener."""

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field

from fleet_manager.local_control.auth import authorize
from fleet_manager.peer.model_sharing import LocalArtifact, TransferRequest
from fleet_manager.peer.models import AdmissionError


class TransferConsent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_worker_id: str = Field(min_length=1, max_length=128)
    permission: bool = False
    license_accepted: bool = False

    def transfer(self) -> TransferRequest:
        return TransferRequest(
            digest=self.digest,
            manifest_digest=self.manifest_digest,
            source=self.source_worker_id,
            permit=self.permission,
            license_approved=self.license_accepted,
        )


class SharePermission(BaseModel):
    model_config = ConfigDict(extra="forbid")
    allowed_peers: list[str] = Field(default_factory=list, max_length=128)
    sharing_enabled: bool = False
    license_approved: bool = False


async def transfer_summaries(manager):
    return [
        {
            "transfer_id": item.digest,
            "digest": item.digest,
            "manifest_digest": item.manifest_digest,
            "model_id": item.model_id or "[pending manifest]",
            "source_worker_id": item.source,
            "state": item.state,
            "completed_bytes": item.bytes_received,
            "total_bytes": item.total_bytes,
            "reason": item.error,
        }
        for item in await manager.status()
    ]


def create_model_router() -> APIRouter:
    def require_read(request: Request):
        authorize(request)

    router = APIRouter(
        prefix="/v1/models", tags=["model sharing"], dependencies=[Depends(require_read)]
    )

    @router.get("/inventory")
    async def inventory(request: Request):
        state = request.app.state
        local = await state.model_sharing.inventory()
        models = [
            {**item.model_dump(), "source_worker_id": state.ledger.worker_id, "local": True}
            for item in local
        ]
        if (await state.ledger.mode_state()).mode not in {"offline", "paused"}:
            for peer in await state.model_transport.inventory():
                models.extend(
                    {**item.model_dump(), "source_worker_id": peer.worker_id, "local": False}
                    for item in peer.models
                )
        return {"version": 1, "scope": "reachable_peer_view", "models": models}

    @router.get("/transfers")
    async def transfers(request: Request):
        return {
            "version": 1,
            "transfers": await transfer_summaries(request.app.state.model_sharing),
        }

    @router.post("/artifacts", status_code=201)
    async def register(spec: LocalArtifact, request: Request):
        authorize(request, control=True)
        await request.app.state.model_sharing.register(spec)
        return {"digest": spec.manifest.digest}

    @router.put("/artifacts/{digest}/permission")
    async def permission(digest: str, spec: SharePermission, request: Request):
        authorize(request, control=True)
        await request.app.state.model_sharing.set_permission(digest, **spec.model_dump())
        return {"digest": digest, **spec.model_dump()}

    @router.post("/transfers", status_code=202)
    async def start(spec: TransferConsent, request: Request):
        authorize(request, control=True)
        await request.app.state.model_transport.start_transfer(spec.transfer())
        return {"digest": spec.digest, "state": "accepted"}

    @router.post("/transfers/{digest}/retry", status_code=202)
    async def retry(digest: str, spec: TransferConsent, request: Request):
        authorize(request, control=True)
        if digest != spec.digest:
            raise AdmissionError("transfer_digest_mismatch", 422)
        await request.app.state.model_transport.retry(spec.transfer())
        return {"digest": digest, "state": "accepted"}

    @router.post("/transfers/{digest}/pause")
    async def pause(digest: str, request: Request):
        authorize(request, control=True)
        await request.app.state.model_sharing.pause(digest)
        return {"digest": digest, "state": "paused"}

    return router
