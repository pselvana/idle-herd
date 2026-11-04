"""Authenticated model sharing RPC and live, digest-bound source selection."""

from __future__ import annotations

import base64
import hashlib

import httpx
from pydantic import BaseModel, ConfigDict, Field

from fleet_manager.peer.identity import IdentityError
from fleet_manager.peer.model_sharing import (
    CHUNK_SIZE,
    ArtifactManifest,
    ChunkResponse,
    TransferRequest,
)
from fleet_manager.peer.models import AdmissionError
from fleet_manager.peer.network import read_response

MAX_INVENTORY = 128
MAX_MODEL_RESPONSE = 2 * CHUNK_SIZE


class InventoryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ModelInventory(BaseModel):
    model_config = ConfigDict(extra="forbid")
    models: list[ArtifactManifest] = Field(max_length=MAX_INVENTORY)


class ManifestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class PeerInventory(BaseModel):
    worker_id: str
    models: list[ArtifactManifest]


class ModelTransport:
    def __init__(self, local):
        self.local = local

    async def require_online(self):
        if (await self.local.state.ledger.mode_state()).mode in {"offline", "paused"}:
            raise AdmissionError("manual_offline", 503)

    def _peers(self):
        directory = self.local.state.directory
        if directory is None:
            return []
        return [s.descriptor for s in directory.snapshot() if s.reachable and s.descriptor]

    async def _rpc(self, peer, path, request, response_type, limit=MAX_MODEL_RESPONSE):
        await self.require_online()
        identity = self.local.state.identity
        if identity is None:
            raise AdmissionError("peer_transport_disabled", 503)
        body = request.model_dump_json().encode()
        headers = identity.sign_request("POST", path, body)
        async with self.local.state.peer_client.stream(
            "POST", peer.endpoint + path, content=body, headers=headers, timeout=30
        ) as response:
            content = await read_response(response, limit)
            identity.verify_response(
                headers["x-idle-nonce"],
                response.status_code,
                content,
                response.headers,
                peer.worker_id,
            )
            if response.status_code != 200:
                raise AdmissionError("model_peer_declined", response.status_code)
            return response_type.model_validate_json(content)

    async def inventory(self):
        await self.require_online()
        inventories = []
        for peer in self._peers():
            try:
                result = await self._rpc(
                    peer, "/peer/v1/models/inventory", InventoryRequest(), ModelInventory
                )
                inventories.append(PeerInventory(worker_id=peer.worker_id, models=result.models))
            except (httpx.HTTPError, IdentityError, AdmissionError, ValueError):
                continue
        return inventories

    async def peer_inventory(self, worker_id: str) -> PeerInventory:
        """Fetch one live authorized catalog, preserving failures for the owner UI."""
        await self.require_online()
        peer = next((peer for peer in self._peers() if peer.worker_id == worker_id), None)
        if peer is None:
            raise AdmissionError("model_source_unavailable", 503)
        try:
            result = await self._rpc(
                peer, "/peer/v1/models/inventory", InventoryRequest(), ModelInventory
            )
        except (httpx.HTTPError, IdentityError, AdmissionError, ValueError) as exc:
            raise AdmissionError("model_inventory_unavailable", 503) from exc
        return PeerInventory(worker_id=peer.worker_id, models=result.models)

    def _fetchers(self, request: TransferRequest):
        # Every failover source must independently attest to the exact same manifest.
        verified = set()

        async def candidates(preferred):
            await self.require_online()
            peers = self._peers()
            return sorted(peers, key=lambda peer: peer.worker_id != preferred)

        async def manifest_from(peer):
            manifest = await self._rpc(
                peer,
                "/peer/v1/models/manifest",
                ManifestRequest(digest=request.digest),
                ArtifactManifest,
            )
            if (
                manifest.digest != request.digest
                or manifest.manifest_digest != request.manifest_digest
                or manifest.calculated_digest() != request.manifest_digest
            ):
                raise IdentityError("model manifest mismatch")
            verified.add(peer.worker_id)
            return manifest

        async def fetch_manifest(source, digest):
            for peer in await candidates(source):
                try:
                    return await manifest_from(peer)
                except (httpx.HTTPError, IdentityError, AdmissionError, ValueError):
                    continue
            raise AdmissionError("model_source_unavailable", 503)

        async def fetch_chunk(source, chunk):
            for peer in await candidates(source):
                try:
                    if peer.worker_id not in verified:
                        await manifest_from(peer)
                    result = await self._rpc(peer, "/peer/v1/models/chunk", chunk, ChunkResponse)
                    data = base64.b64decode(result.data, validate=True)
                    if (
                        result.offset != chunk.offset
                        or len(data) != chunk.length
                        or hashlib.sha256(data).hexdigest() != result.sha256
                    ):
                        raise IdentityError("invalid model chunk")
                    return result
                except (httpx.HTTPError, IdentityError, AdmissionError, ValueError):
                    continue
            raise AdmissionError("model_source_unavailable", 503)

        return fetch_manifest, fetch_chunk

    async def start_transfer(self, request: TransferRequest):
        await self.require_online()
        await self.local.state.model_sharing.start_transfer(request, *self._fetchers(request))

    async def retry(self, request: TransferRequest):
        await self.require_online()
        await self.local.state.model_sharing.retry(request, *self._fetchers(request))
