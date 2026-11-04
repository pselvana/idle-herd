"""Signed model RPCs enforce per-peer authorization and transfer mode."""

import hashlib

import pytest
from test_rpc import enrolled, signed, worker

from fleet_manager.peer.model_sharing import ArtifactManifest, LocalArtifact


@pytest.mark.asyncio
async def test_model_rpc_authorization_nonce_and_modes(tmp_path, monkeypatch):
    async with worker(tmp_path / "worker", monkeypatch) as (app, authority, client):
        allowed = await enrolled(authority, tmp_path / "allowed", "allowed")
        denied = await enrolled(authority, tmp_path / "denied", "denied")
        data = b"verified model"
        artifact = tmp_path / "model.gguf"
        artifact.write_bytes(data)
        manifest = ArtifactManifest(
            model_id="test",
            digest=hashlib.sha256(data).hexdigest(),
            size_bytes=len(data),
            quantization="Q4",
            provenance="test",
            license="test",
            required_memory_bytes=1,
            manifest_digest="0" * 64,
        )
        manifest.manifest_digest = manifest.calculated_digest()
        await app.state.model_sharing.register(
            LocalArtifact(
                manifest=manifest,
                path=str(artifact),
                allowed_peers=["allowed"],
                sharing_enabled=True,
                license_approved=True,
            )
        )
        path = "/peer/v1/models/inventory"
        body, headers = signed(allowed, path, {})
        response = await client.post(path, content=body, headers=headers)
        assert response.status_code == 200
        allowed.verify_response(headers["x-idle-nonce"], 200, response.content, response.headers)
        assert response.json()["models"][0]["digest"] == manifest.digest
        assert (await client.post(path, content=body, headers=headers)).status_code == 403
        body, headers = signed(denied, path, {})
        assert (await client.post(path, content=body, headers=headers)).json()["models"] == []
        path = "/peer/v1/models/chunk"
        payload = {"digest": manifest.digest, "offset": 0, "length": len(data)}
        body, headers = signed(denied, path, payload)
        assert (await client.post(path, content=body, headers=headers)).status_code == 404
        body, headers = signed(allowed, path, payload)
        response = await client.post(path, content=body, headers=headers)
        assert response.status_code == 200
        assert response.json()["sha256"] == hashlib.sha256(data).hexdigest()
        from fleet_manager.peer.local_ledger import ModeUpdate

        await app.state.ledger.set_mode(ModeUpdate(mode="paused"))
        body, headers = signed(allowed, path, payload)
        assert (await client.post(path, content=body, headers=headers)).status_code == 503


@pytest.mark.asyncio
async def test_failover_requires_matching_manifest_and_valid_chunk():
    import base64
    from types import SimpleNamespace

    import httpx

    from fleet_manager.peer.model_sharing import ChunkRequest, ChunkResponse, TransferRequest
    from fleet_manager.peer.model_transport import ModelTransport

    data = b"model data"
    digest = hashlib.sha256(data).hexdigest()
    manifest = ArtifactManifest(
        model_id="test",
        digest=digest,
        size_bytes=len(data),
        quantization="Q4",
        provenance="test",
        license="test",
        required_memory_bytes=1,
        manifest_digest="0" * 64,
    )
    manifest.manifest_digest = manifest.calculated_digest()
    peers = [SimpleNamespace(worker_id=name) for name in ("first", "wrong", "last")]
    transport = ModelTransport(None)

    async def online():
        pass

    async def rpc(peer, path, request, response_type, limit=None):
        if path.endswith("manifest"):
            if peer.worker_id == "wrong":
                return manifest.model_copy(update={"provenance": "other"})
            return manifest
        if peer.worker_id == "first":
            raise httpx.ReadError("disconnected")
        return ChunkResponse(offset=0, data=base64.b64encode(data).decode(), sha256=digest)

    transport.require_online = online
    transport._peers = lambda: peers
    transport._rpc = rpc
    request = TransferRequest(
        digest=digest,
        manifest_digest=manifest.manifest_digest,
        source="first",
        permit=True,
        license_approved=True,
    )
    fetch_manifest, fetch_chunk = transport._fetchers(request)
    assert await fetch_manifest("first", digest) == manifest
    result = await fetch_chunk("first", ChunkRequest(digest=digest, offset=0, length=len(data)))
    assert base64.b64decode(result.data) == data


@pytest.mark.parametrize("failure", ["signature", "forbidden", "unavailable"])
async def test_inventory_negotiation_never_downgrades_an_authentication_or_transport_failure(
    failure,
):
    from unittest.mock import AsyncMock

    import httpx

    from fleet_manager.peer.identity import IdentityError
    from fleet_manager.peer.model_transport import ModelTransport
    from fleet_manager.peer.models import AdmissionError

    errors = {
        "signature": IdentityError("bad signature"),
        "forbidden": AdmissionError("denied", 403),
        "unavailable": httpx.ReadError("disconnected"),
    }
    transport = ModelTransport(None)
    transport._rpc = AsyncMock(side_effect=errors[failure])
    with pytest.raises(type(errors[failure])):
        await transport._inventory_from(object())
    transport._rpc.assert_awaited_once()


@pytest.mark.parametrize("corruption", ["offset", "length", "hash", "base64", "manifest"])
async def test_chunk_failover_rejects_invalid_data_and_independently_verifies_next_source(
    corruption,
):
    import base64
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from fleet_manager.peer.model_sharing import ChunkRequest, ChunkResponse, TransferRequest
    from fleet_manager.peer.model_transport import ModelTransport

    data = b"complete model"
    digest = hashlib.sha256(data).hexdigest()
    manifest = ArtifactManifest(
        model_id="test",
        digest=digest,
        size_bytes=len(data),
        quantization="Q4",
        provenance="test",
        license="MIT",
        required_memory_bytes=1,
        manifest_digest="0" * 64,
    )
    manifest.manifest_digest = manifest.calculated_digest()
    transport = ModelTransport(None)
    transport.require_online = AsyncMock()
    transport._peers = lambda: [SimpleNamespace(worker_id=w) for w in ("bad", "good")]
    calls = []

    async def rpc(peer, path, request, response_type):
        calls.append((peer.worker_id, path.rsplit("/", 1)[1]))
        if path.endswith("manifest"):
            return (
                manifest.model_copy(update={"license": "changed"})
                if peer.worker_id == "bad" and corruption == "manifest"
                else manifest
            )
        result = ChunkResponse(offset=0, data=base64.b64encode(data).decode(), sha256=digest)
        if peer.worker_id == "bad":
            result = result.model_copy(
                update={
                    "offset": {"offset": 1}.get(corruption, 0),
                    "data": "!!"
                    if corruption == "base64"
                    else base64.b64encode(data[:-1] if corruption == "length" else data).decode(),
                    "sha256": "0" * 64 if corruption == "hash" else digest,
                }
            )
        return result

    transport._rpc = rpc
    request = TransferRequest(digest=digest, manifest_digest=manifest.manifest_digest, source="bad")
    _, fetch = transport._fetchers(request)
    result = await fetch("bad", ChunkRequest(digest=digest, offset=0, length=len(data)))
    assert base64.b64decode(result.data) == data
    assert calls[-2:] == [("good", "manifest"), ("good", "chunk")]
    assert (("bad", "chunk") in calls) == (corruption != "manifest")
