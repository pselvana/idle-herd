import hashlib

import httpx
import pytest

from fleet_manager.peer.model_sharing import (
    ArtifactManifest,
    ChunkRequest,
    LocalArtifact,
    ModelSharing,
    TransferRequest,
)
from fleet_manager.peer.models import AdmissionError


def manifest(data):
    value = ArtifactManifest(
        model_id="sample",
        digest=hashlib.sha256(data).hexdigest(),
        size_bytes=len(data),
        quantization="q4",
        provenance="example@revision",
        license="MIT",
        required_memory_bytes=1,
        manifest_digest="0" * 64,
    )
    value.manifest_digest = value.calculated_digest()
    return value


@pytest.mark.asyncio
async def test_authorization_and_verified_transfer(tmp_path):
    data = b"GGUF" + b"gguf data" * 100
    path = tmp_path / "original.gguf"
    path.write_bytes(data)
    artifact = manifest(data)

    created = False

    async def backend(req):
        nonlocal created
        if req.url.path == "/api/tags":
            return httpx.Response(
                200,
                json={
                    "models": [{"name": "sample", "digest": "backend-digest"}] if created else []
                },
            )
        if req.url.path == "/api/create":
            created = True
            return httpx.Response(200, json={"status": "success"})
        return httpx.Response(201)

    async def load(_manifest):
        return True

    source = ModelSharing(tmp_path / "source", "http://ollama")
    dest = ModelSharing(
        tmp_path / "dest",
        "http://ollama",
        reserved_bytes=0,
        reserve_memory_bytes=0,
        transport=httpx.MockTransport(backend),
        load_confirm=load,
    )
    await source.open()
    await dest.open()
    try:
        await source.register(
            LocalArtifact(
                manifest=artifact,
                path=str(path),
                allowed_peers=["dest"],
                sharing_enabled=True,
                license_approved=True,
            )
        )
        assert len(await source.inventory("dest")) == 1
        assert await source.inventory("other") == []
        with pytest.raises(AdmissionError):
            await source.source_chunk(
                "other", ChunkRequest(digest=artifact.digest, offset=0, length=2)
            )

        async def get_manifest(_peer, digest):
            return await source.source_manifest("dest", digest)

        async def get_chunk(_peer, request):
            return await source.source_chunk("dest", request)

        request = TransferRequest(
            digest=artifact.digest,
            manifest_digest=artifact.manifest_digest,
            source="source",
            permit=True,
            license_approved=True,
        )
        await dest.start_transfer(request, get_manifest, get_chunk)
        await dest._tasks[artifact.digest]
        assert (await dest.status())[0].state == "ready"
        assert (tmp_path / "dest" / (artifact.digest + ".gguf")).read_bytes() == data
        created = False
        assert (await dest.status())[0].state == "backend_missing"
        created = True
        assert (await dest.status())[0].state == "ready"
        await source.set_permission(artifact.digest, [], False, False)
        assert not await source.inventory("dest")
    finally:
        await source.close()
        await dest.close()


@pytest.mark.asyncio
async def test_manager_exclusion_and_no_unsafe_load(tmp_path):
    first = ModelSharing(tmp_path, "http://ollama")
    second = ModelSharing(tmp_path, "http://ollama")
    await first.open()
    try:
        with pytest.raises(AdmissionError, match="sharing_manager_already_running"):
            await second.open()
        with pytest.raises(AdmissionError, match="backend_load_confirmation_unavailable"):
            await first._import(manifest(b"x"), tmp_path / "unused")
    finally:
        await first.close()
        await second.close()


@pytest.mark.asyncio
async def test_resume_after_restart_and_corruption(tmp_path):
    import base64

    from fleet_manager.peer.model_sharing import CHUNK_SIZE, ChunkResponse

    data = b"x" * (CHUNK_SIZE + 10)
    item = manifest(data)
    request = TransferRequest(
        digest=item.digest,
        manifest_digest=item.manifest_digest,
        source="one",
        permit=True,
        license_approved=True,
    )
    manager = ModelSharing(tmp_path, "http://ollama", reserved_bytes=0, reserve_memory_bytes=0)
    await manager.open()
    offsets = []

    async def get_manifest(_source, _digest):
        return item

    async def interrupted(_source, chunk):
        offsets.append(chunk.offset)
        if chunk.offset:
            raise RuntimeError("source disconnected")
        block = data[chunk.offset : chunk.offset + chunk.length]
        return ChunkResponse(
            offset=chunk.offset,
            data=base64.b64encode(block).decode(),
            sha256=hashlib.sha256(block).hexdigest(),
        )

    await manager.start_transfer(request, get_manifest, interrupted)
    await manager._tasks[item.digest]
    assert (await manager.status())[0].bytes_received == CHUNK_SIZE
    await manager.close()
    manager = ModelSharing(tmp_path, "http://ollama", reserved_bytes=0, reserve_memory_bytes=0)
    await manager.open()

    async def corrupt(_source, chunk):
        offsets.append(chunk.offset)
        return ChunkResponse(
            offset=chunk.offset,
            data=base64.b64encode(b"z" * chunk.length).decode(),
            sha256="0" * 64,
        )

    try:
        assert (await manager.status())[0].manifest_digest == item.manifest_digest
        await manager.retry(request, get_manifest, corrupt)
        await manager._tasks[item.digest]
        status = (await manager.status())[0]
        assert offsets[-1] == CHUNK_SIZE
        assert status.state == "failed" and status.error == "invalid_chunk"
        assert not (tmp_path / (item.digest + ".gguf")).exists()
    finally:
        await manager.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["quota", "memory", "unsupported", "symlink"])
async def test_rejections_before_fetching_chunks(tmp_path, mode):
    item = manifest(b"test")
    if mode == "unsupported":
        item.format = "mlx-safetensors"
        item.backend = "mlx"
        item.manifest_digest = item.calculated_digest()
    if mode == "memory":
        item.required_memory_bytes = 2**63
        item.manifest_digest = item.calculated_digest()
    manager = ModelSharing(
        tmp_path / "dest",
        "http://ollama",
        reserved_bytes=0,
        reserve_memory_bytes=0,
        max_bytes=1 if mode == "quota" else 100,
    )
    await manager.open()
    if mode == "symlink":
        target = tmp_path / "target"
        target.write_bytes(b"test")
        (tmp_path / "dest" / (item.digest + ".partial")).symlink_to(target)
    calls = []

    async def get_manifest(_source, _digest):
        return item

    async def get_chunk(_source, chunk):
        calls.append(chunk)
        raise AssertionError("must reject before fetching")

    request = TransferRequest(
        digest=item.digest,
        manifest_digest=item.manifest_digest,
        source="one",
        permit=True,
        license_approved=True,
    )
    try:
        await manager.start_transfer(request, get_manifest, get_chunk)
        await manager._tasks[item.digest]
        assert (await manager.status())[0].state == "failed"
        assert calls == []
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_concurrent_start_and_bad_final_hash(tmp_path):
    import asyncio
    import base64

    from fleet_manager.peer.model_sharing import ChunkResponse

    item = manifest(b"original")
    request = TransferRequest(
        digest=item.digest,
        manifest_digest=item.manifest_digest,
        source="one",
        permit=True,
        license_approved=True,
    )
    manager = ModelSharing(tmp_path, "http://ollama", reserved_bytes=0, reserve_memory_bytes=0)
    await manager.open()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def get_manifest(_source, _digest):
        entered.set()
        await release.wait()
        return item

    async def altered(_source, chunk):
        block = b"tampered"
        assert len(block) == chunk.length
        return ChunkResponse(
            offset=chunk.offset,
            data=base64.b64encode(block).decode(),
            sha256=hashlib.sha256(block).hexdigest(),
        )

    try:
        await manager.start_transfer(request, get_manifest, altered)
        task = manager._tasks[item.digest]
        await entered.wait()
        with pytest.raises(AdmissionError, match="transfer_in_progress"):
            await manager.start_transfer(request, get_manifest, altered)
        release.set()
        await task
        status = (await manager.status())[0]
        assert status.error == "artifact_digest_mismatch"
        assert status.bytes_received == 0
        assert not (tmp_path / (item.digest + ".partial")).exists()
    finally:
        await manager.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["os", "cpu", "backend_version", "disk"])
async def test_compatibility_and_disk_rejected_before_chunks(tmp_path, monkeypatch, mode):
    import shutil

    item = manifest(b"GGUFfake")
    if mode == "os":
        item.required_os = "unsupported-os"
    if mode == "cpu":
        item.required_cpu = "unsupported-cpu"
    if mode == "backend_version":
        item.minimum_backend_version = "999.0.0"
    item.manifest_digest = item.calculated_digest()

    async def backend(_request):
        return httpx.Response(200, json={"version": "0.1.0"})

    if mode == "disk":
        monkeypatch.setattr(shutil, "disk_usage", lambda _path: (100, 90, 10))
    manager = ModelSharing(
        tmp_path,
        "http://ollama",
        reserved_bytes=0,
        reserve_memory_bytes=0,
        transport=httpx.MockTransport(backend),
    )
    await manager.open()
    called = []

    async def get_manifest(_source, _digest):
        return item

    async def get_chunk(_source, _chunk):
        called.append(True)
        raise AssertionError("incompatible transfer must not fetch chunks")

    request = TransferRequest(
        digest=item.digest,
        manifest_digest=item.manifest_digest,
        source="one",
        permit=True,
        license_approved=True,
    )
    try:
        await manager.start_transfer(request, get_manifest, get_chunk)
        await manager._tasks[item.digest]
        assert (await manager.status())[0].state == "failed"
        assert not called
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_registered_manifest_is_immutable_and_symlinks_rejected(tmp_path):
    data = b"GGUFregistered"
    file = tmp_path / "model.gguf"
    file.write_bytes(data)
    item = manifest(data)
    manager = ModelSharing(tmp_path / "store", "http://ollama")
    await manager.open()
    try:
        registration = LocalArtifact(
            manifest=item,
            path=str(file),
            allowed_peers=["one"],
            sharing_enabled=True,
            license_approved=True,
        )
        await manager.register(registration)
        altered = item.model_copy(deep=True)
        altered.provenance = "another origin"
        altered.manifest_digest = altered.calculated_digest()
        with pytest.raises(AdmissionError, match="artifact_manifest_conflict"):
            await manager.register(LocalArtifact(manifest=altered, path=str(file)))
        assert await manager.source_manifest("one", item.digest) == item
        link = tmp_path / "alias.gguf"
        link.symlink_to(file)
        link_item = manifest(data + b"different")
        with pytest.raises(AdmissionError, match="artifact_verification_failed"):
            await manager.register(LocalArtifact(manifest=link_item, path=str(link)))
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_source_queued_reads_observe_revocation(tmp_path):
    import asyncio

    data = b"GGUFsource"
    file = tmp_path / "model.gguf"
    file.write_bytes(data)
    item = manifest(data)
    manager = ModelSharing(tmp_path / "store", "http://ollama")
    await manager.open()
    try:
        await manager.register(
            LocalArtifact(
                manifest=item,
                path=str(file),
                allowed_peers=["one"],
                sharing_enabled=True,
                license_approved=True,
            )
        )
        manager._source_next = asyncio.get_running_loop().time() + 0.05
        task = asyncio.create_task(
            manager.source_chunk("one", ChunkRequest(digest=item.digest, offset=0, length=4))
        )
        await asyncio.sleep(0)
        await manager.set_permission(item.digest, [], False, False)
        with pytest.raises(AdmissionError, match="artifact_not_found"):
            await task
    finally:
        await manager.close()
