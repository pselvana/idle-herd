"""Complete package exports, content-bound resume and safe backend installation."""

import base64
import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from pydantic import ValidationError

from fleet_manager.peer.model_packages import (
    PackageFile,
    directory_artifact,
    read_range,
    verify_package,
)
from fleet_manager.peer.model_sharing import (
    CHUNK_SIZE,
    ArtifactManifest,
    ChunkRequest,
    ChunkResponse,
    ModelSharing,
    TransferRequest,
)
from fleet_manager.peer.model_transport import InventoryRequest, ModelInventory, ModelTransport
from fleet_manager.peer.models import AdmissionError
from fleet_manager.peer.ollama_catalog import OllamaCatalog
from fleet_manager.peer.ollama_exports import manifest_path


def safetensors(name="weight", data=b"\x00\x00\x80?"):
    header = json.dumps(
        {name: {"dtype": "F32", "shape": [1], "data_offsets": [0, len(data)]}},
        separators=(",", ":"),
    ).encode()
    return len(header).to_bytes(8, "little") + header + data


def export_directory(tmp_path, format="mlx", *, large=False):
    directory = tmp_path / "export"
    directory.mkdir()
    if format == "mlx":
        files = {
            "config.json": b'{"model_type":"test"}',
            "tokenizer.json": b'{"vocab":{}}',
            "model-00001-of-00002.safetensors": safetensors("one"),
            "model-00002-of-00002.safetensors": safetensors("two"),
            "model.safetensors.index.json": json.dumps(
                {
                    "weight_map": {
                        "one": "model-00001-of-00002.safetensors",
                        "two": "model-00002-of-00002.safetensors",
                    }
                }
            ).encode(),
            "chat_template.jinja": b"{{ messages }}",
            "LICENSE": b"MIT",
        }
    else:
        files = {
            "model.gguf": b"GGUF" + b"a" * (CHUNK_SIZE - 100 if large else 64),
            "projector.gguf": b"GGUF" + b"p" * 256,
            "adapters/lora.gguf": b"GGUFadapter",
            "draft/model.gguf": b"GGUFdraft",
        }
    files["empty.txt"] = b""
    for name, data in files.items():
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return directory_artifact(
        directory,
        model_id="copied:latest",
        format=format,
        license="MIT",
        provenance="test@revision",
    ), files


class ImportBackend:
    def __init__(self):
        self.blobs = {}
        self.created = []

    async def __call__(self, request):
        if request.url.path == "/api/version":
            return httpx.Response(200, json={"version": "0.40.11"})
        if request.url.path == "/api/tags":
            return httpx.Response(
                200,
                json={
                    "models": [{"name": "copied:latest", "digest": "backend-digest"}]
                    if self.created
                    else []
                },
            )
        if request.url.path.startswith("/api/blobs/"):
            data = await request.aread()
            digest = request.url.path.rsplit("/", 1)[1]
            assert digest == "sha256:" + hashlib.sha256(data).hexdigest()
            self.blobs[digest] = data
            return httpx.Response(201)
        assert request.url.path == "/api/create"
        spec = json.loads(request.content)
        for key in ("files", "adapters", "draft_files"):
            assert all(digest in self.blobs for digest in spec.get(key, {}).values())
        self.created.append(spec)
        return httpx.Response(200, json={"status": "success"})


@pytest.mark.asyncio
@pytest.mark.parametrize("format", ["mlx", "gguf-package"])
async def test_complete_export_import_and_cached_resharing(tmp_path, monkeypatch, format):
    monkeypatch.setattr("fleet_manager.peer.model_sharing.platform.system", lambda: "Darwin")
    monkeypatch.setattr("fleet_manager.peer.model_sharing.platform.machine", lambda: "arm64")
    artifact, files = export_directory(tmp_path, format)
    backend = ImportBackend()
    load = AsyncMock(return_value=True)
    source = ModelSharing(tmp_path / "source", "http://backend")
    dest = ModelSharing(
        tmp_path / "dest",
        "http://backend",
        transport=httpx.MockTransport(backend),
        load_confirm=load,
        reserve_memory_bytes=0,
        reserved_bytes=0,
    )
    await source.open()
    await dest.open()
    try:
        await source.register(artifact)
        manifest = (await source.inventory("dest"))[0]
        assert manifest.digest == manifest.calculated_package_digest()
        assert str(tmp_path) not in manifest.model_dump_json()
        assert await source.inventory("legacy", include_packages=False) == []
        assert all(json.loads(item.model_dump_json())["name"] in files for item in manifest.files)

        async def fetch_manifest(_source, digest):
            return await source.source_manifest("dest", digest)

        async def fetch_chunk(_source, request):
            return await source.source_chunk("dest", request)

        request = TransferRequest(
            digest=manifest.digest,
            manifest_digest=manifest.manifest_digest,
            source="source",
            permit=True,
            license_approved=True,
        )
        await dest.start_transfer(request, fetch_manifest, fetch_chunk)
        await dest._tasks[request.digest]
        status = (await dest.status())[0]
        assert status.state == "ready", status.error
        assert status.bytes_received == sum(len(data) for data in files.values())
        assert len(backend.created) == 1
        spec = backend.created[0]
        expected = {
            name: data
            for name, data in files.items()
            if not name.startswith(("adapters/", "draft/"))
        }
        assert {name: backend.blobs[digest] for name, digest in spec["files"].items()} == expected
        if format == "gguf-package":
            assert (
                backend.blobs[spec["adapters"]["adapters/lora.gguf"]] == files["adapters/lora.gguf"]
            )
            assert backend.blobs[spec["draft_files"]["model.gguf"]] == files["draft/model.gguf"]
        load.assert_awaited_once()
        reshared = await dest.source_manifest("another-peer", manifest.digest)
        assert reshared == manifest
        chunk = await dest.source_chunk(
            "another-peer",
            ChunkRequest(
                digest=manifest.digest, offset=0, length=min(CHUNK_SIZE, manifest.size_bytes)
            ),
        )
        assert base64.b64decode(chunk.data) == read_range(
            artifact, 0, min(CHUNK_SIZE, manifest.size_bytes)
        )
        assert (tmp_path / "dest" / (manifest.digest + ".package")).is_file()
        await source.set_permission(manifest.digest, [], False, False)
        with pytest.raises(AdmissionError):
            await source.source_manifest("dest", manifest.digest)
    finally:
        await source.close()
        await dest.close()


@pytest.mark.asyncio
async def test_package_resume_across_boundary_restart_and_failed_load(tmp_path):
    artifact, _ = export_directory(tmp_path, "gguf-package", large=True)
    backend = ImportBackend()
    requests = []
    interrupted, loaded = True, False

    async def fetch_manifest(_source, _digest):
        return artifact.manifest

    async def fetch_chunk(_source, request):
        requests.append(request.offset)
        if interrupted and request.offset:
            raise RuntimeError("source disconnected")
        data = read_range(artifact, request.offset, request.length)
        return ChunkResponse(
            offset=request.offset,
            data=base64.b64encode(data).decode(),
            sha256=hashlib.sha256(data).hexdigest(),
        )

    async def load(_manifest):
        return loaded

    def manager():
        return ModelSharing(
            tmp_path / "dest",
            "http://backend",
            transport=httpx.MockTransport(backend),
            load_confirm=load,
            reserve_memory_bytes=0,
            reserved_bytes=0,
        )

    request = TransferRequest(
        digest=artifact.manifest.digest,
        manifest_digest=artifact.manifest.manifest_digest,
        source="source",
        permit=True,
        license_approved=True,
    )
    first = manager()
    await first.open()
    await first.start_transfer(request, fetch_manifest, fetch_chunk)
    await first._tasks[request.digest]
    assert (await first.status())[0].bytes_received == CHUNK_SIZE
    await first.close()
    interrupted = False
    second = manager()
    await second.open()
    await second.retry(request, fetch_manifest, fetch_chunk)
    await second._tasks[request.digest]
    assert (await second.status())[0].error == "backend_load_unconfirmed"
    assert requests.count(0) == 1 and len(backend.created) == 1
    await second.close()
    loaded = True
    third = manager()
    await third.open()
    try:
        before = list(requests)
        await third.retry(request, fetch_manifest, fetch_chunk)
        await third._tasks[request.digest]
        assert (await third.status())[0].state == "ready"
        assert requests == before and len(backend.created) == 1
    finally:
        await third.close()


@pytest.mark.asyncio
async def test_corrupt_component_never_uploads_or_imports(tmp_path):
    artifact, _ = export_directory(tmp_path, "gguf-package")
    backend = ImportBackend()
    dest = ModelSharing(
        tmp_path / "dest",
        "http://backend",
        transport=httpx.MockTransport(backend),
        load_confirm=AsyncMock(return_value=True),
        reserve_memory_bytes=0,
        reserved_bytes=0,
    )
    await dest.open()
    try:

        async def manifest(*_):
            return artifact.manifest

        async def chunk(_source, request):
            data = bytearray(read_range(artifact, request.offset, request.length))
            data[-1] ^= 1
            return ChunkResponse(
                offset=request.offset,
                data=base64.b64encode(data).decode(),
                sha256=hashlib.sha256(data).hexdigest(),
            )

        request = TransferRequest(
            digest=artifact.manifest.digest,
            manifest_digest=artifact.manifest.manifest_digest,
            source="source",
            permit=True,
            license_approved=True,
        )
        await dest.start_transfer(request, manifest, chunk)
        await dest._tasks[request.digest]
        assert (await dest.status())[0].error == "package_file_digest_mismatch"
        assert not backend.blobs and not backend.created
        assert not (dest.directory / (request.digest + ".partial")).exists()
    finally:
        await dest.close()


@pytest.mark.parametrize(
    "name",
    [
        "../weights",
        "/weights",
        "C:/weights",
        "x\\weights",
        "foo/../weights",
        "CON.txt",
        "x./model.gguf",
    ],
)
def test_package_filenames_are_portable_and_cannot_escape(name):
    with pytest.raises(ValidationError):
        PackageFile(name=name, size_bytes=1, digest="a" * 64)


def test_missing_index_shards_duplicate_names_and_symlinks_are_rejected(tmp_path):
    artifact, _ = export_directory(tmp_path)
    index = Path(artifact.source_files["model.safetensors.index.json"])
    index.write_text('{"weight_map":{"one":"missing.safetensors"}}')
    artifact = directory_artifact(
        index.parent, model_id="test", format="mlx", license="MIT", provenance="test"
    )
    with pytest.raises(AdmissionError, match="incomplete_model_shards"):
        verify_package(artifact)
    artifact.manifest.files.append(artifact.manifest.files[0].model_copy())
    artifact.manifest.files[-1].name = artifact.manifest.files[-1].name.upper()
    artifact.manifest.digest = artifact.manifest.calculated_package_digest()
    artifact.manifest.manifest_digest = artifact.manifest.calculated_digest()
    with pytest.raises(AdmissionError, match="invalid_package_files"):
        ModelSharing._validate(artifact.manifest)
    link = index.parent / "alias.json"
    link.symlink_to(index)
    with pytest.raises((AdmissionError, OSError)):
        directory_artifact(
            index.parent, model_id="test", format="mlx", license="MIT", provenance="test"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "format,reason", [("mlx", "mlx_requires_apple_silicon"), ("gguf-package", "quota_exceeded")]
)
async def test_mlx_platform_and_aggregate_quota_are_checked_before_any_download(
    tmp_path, monkeypatch, format, reason
):
    artifact, _ = export_directory(tmp_path, format)
    monkeypatch.setattr("fleet_manager.peer.model_sharing.platform.system", lambda: "Linux")
    dest = ModelSharing(
        tmp_path / "dest", "http://backend", max_bytes=1, load_confirm=AsyncMock(return_value=True)
    )
    await dest.open()
    chunks = AsyncMock()
    try:
        manifest = AsyncMock(return_value=artifact.manifest)
        request = TransferRequest(
            digest=artifact.manifest.digest,
            manifest_digest=artifact.manifest.manifest_digest,
            source="source",
            permit=True,
            license_approved=True,
        )
        await dest.start_transfer(request, manifest, chunks)
        await dest._tasks[request.digest]
        assert (await dest.status())[0].error == reason
        chunks.assert_not_awaited()
    finally:
        await dest.close()


@pytest.mark.asyncio
async def test_inventory_negotiates_package_support_and_falls_back_for_old_peers():
    transport = ModelTransport(None)
    calls = []

    async def rpc(peer, path, request, response):
        calls.append(json.loads(request.model_dump_json()))
        if request.packages:
            raise AdmissionError("old_strict_schema", 422)
        return ModelInventory(models=[])

    transport._rpc = rpc
    assert (await transport._inventory_from(object())).models == []
    assert calls == [{"packages": True}, {}]
    assert InventoryRequest().model_dump_json() == "{}"


def installed_fixture(tmp_path, format):
    root = tmp_path / "models"
    (root / "blobs").mkdir(parents=True)
    layers = []

    def blob(data, kind, name=""):
        digest = hashlib.sha256(data).hexdigest()
        (root / "blobs" / ("sha256-" + digest)).write_bytes(data)
        layer = {
            "digest": "sha256:" + digest,
            "size": len(data),
            "mediaType": "application/vnd.ollama.image." + kind,
        }
        if name:
            layer["name"] = name
        layers.append(layer)
        return layer

    if format == "safetensors":
        blob(b'{"model_type":"test"}', "json", "config.json")
        blob(b'{"vocab":{}}', "json", "tokenizer.json")
        blob(
            b'{"weight_map":{"old":"unavailable-source.safetensors"}}',
            "json",
            "model.safetensors.index.json",
        )
        blob(safetensors("one"), "tensor", "model.layers.0.weight")
        blob(safetensors("two"), "tensor", "model.layers.1.weight")
        modelfile = "FROM test:latest\n"
    else:
        blob(b"GGUFshard1", "model")
        blob(b"GGUFshard2", "model")
        blob(b"GGUFprojector", "projector")
        modelfile = "FROM " + str(root / "blobs" / layers[0]["digest"].replace(":", "-"))
    manifest = {"schemaVersion": 2, "layers": layers}
    data = json.dumps(manifest).encode()
    path = manifest_path(root, "test:latest")
    path.parent.mkdir(parents=True)
    path.write_bytes(data)
    show = {
        "details": {"format": format},
        "modelfile": modelfile,
        "license": "MIT",
        "renderer": "test-renderer",
        "parser": "test-parser",
        "capabilities": ["completion", "vision"],
    }
    return root, path, show, hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize("field", ["name", "role", "size_bytes", "digest", "template"])
def test_package_identity_binds_file_inventory_and_import_metadata(tmp_path, field):
    artifact, _ = export_directory(tmp_path, "gguf-package")
    changed = artifact.manifest.model_copy(deep=True)
    if field == "template":
        from fleet_manager.peer.model_sharing import OllamaImportOptions

        changed.ollama = OllamaImportOptions(template="changed template")
    else:
        item = changed.files[0]
        setattr(
            item,
            field,
            {"name": "renamed.gguf", "role": "file", "size_bytes": 999, "digest": "f" * 64}[field],
        )
    assert changed.calculated_package_digest() != artifact.manifest.digest
    with pytest.raises(AdmissionError, match="manifest_digest_mismatch"):
        ModelSharing._validate(changed)
    # Re-signing just the outer manifest cannot reuse the original package identity.
    changed.manifest_digest = changed.calculated_digest()
    with pytest.raises(AdmissionError, match="package_(digest|size)_mismatch"):
        ModelSharing._validate(changed)


@pytest.mark.parametrize("mutation", ["missing", "extra", "overlap", "resized", "symlink"])
def test_package_sources_cannot_change_after_discovery(tmp_path, mutation):
    artifact, _ = export_directory(tmp_path, "gguf-package")
    name = artifact.manifest.files[0].name
    path = Path(artifact.source_files[name])
    if mutation == "missing":
        artifact.source_files.pop(name)
    elif mutation == "extra":
        artifact.source_files["extra.gguf"] = str(path)
    elif mutation == "overlap":
        artifact.inline_files[name] = base64.b64encode(path.read_bytes()).decode()
    elif mutation == "resized":
        path.write_bytes(path.read_bytes() + b"changed")
    else:
        target = path.with_suffix(".original")
        path.rename(target)
        path.symlink_to(target)
    with pytest.raises(AdmissionError, match="package_source_mismatch|artifact_changed"):
        read_range(artifact, 0, artifact.manifest.size_bytes)


@pytest.mark.parametrize(
    "name,data,reason",
    [
        ("config.json", b"[]", "invalid_package_metadata"),
        ("model-00001-of-00002.safetensors", b"x" * 10, "invalid_safetensors"),
        ("model.safetensors.index.json", b'{"weight_map":{}}', "incomplete_model_shards"),
        (
            "model.safetensors.index.json",
            b'{"weight_map":{"one":"../outside.safetensors"}}',
            "incomplete_model_shards",
        ),
    ],
)
def test_correct_file_digest_does_not_authorize_invalid_package_metadata(
    tmp_path, name, data, reason
):
    artifact, _ = export_directory(tmp_path)
    Path(artifact.source_files[name]).write_bytes(data)
    artifact = directory_artifact(
        Path(artifact.path), model_id="test", format="mlx", license="MIT", provenance="test"
    )
    with pytest.raises(AdmissionError, match=reason):
        verify_package(artifact)


@pytest.mark.parametrize("mutation", ["manifest", "blob_size", "cloud", "unknown_layer"])
async def test_managed_export_fails_closed_when_source_manifest_is_not_complete(tmp_path, mutation):
    root, path, show, digest = installed_fixture(tmp_path, "gguf")
    if mutation == "manifest":
        path.write_text(path.read_text() + " ")
    elif mutation == "blob_size":
        blob = next((root / "blobs").iterdir())
        blob.write_bytes(blob.read_bytes() + b"changed")
    else:
        manifest = json.loads(path.read_bytes())
        if mutation == "cloud":
            data = b'{"remote_host":"https://cloud.example","remote_model":"private"}'
            config_digest = hashlib.sha256(data).hexdigest()
            (root / "blobs" / ("sha256-" + config_digest)).write_bytes(data)
            manifest["config"] = {
                "digest": "sha256:" + config_digest,
                "size": len(data),
                "mediaType": "application/vnd.docker.container.image.v1+json",
            }
        else:
            manifest["layers"][-1]["mediaType"] = "application/vnd.ollama.image.unknown"
        data = json.dumps(manifest).encode()
        path.write_bytes(data)
        digest = hashlib.sha256(data).hexdigest()

    def backend(request):
        return httpx.Response(
            200,
            json={"models": [{"name": "test:latest", "digest": digest}]}
            if request.url.path == "/api/tags"
            else show,
        )

    async with httpx.AsyncClient(
        base_url="http://backend", transport=httpx.MockTransport(backend)
    ) as client:
        catalog = OllamaCatalog(client, models_dir=str(root))
        try:
            assert await catalog.artifacts() == []
        finally:
            await catalog.close()


@pytest.mark.parametrize(
    "version,reason",
    [
        ("0.39.99", "backend_version_incompatible"),
        ("0.40.0-rc1", "backend_version_unknown"),
        ("not-a-version", "backend_version_unknown"),
    ],
)
async def test_mlx_version_gate_precedes_download_or_import(tmp_path, monkeypatch, version, reason):
    monkeypatch.setattr("fleet_manager.peer.model_sharing.platform.system", lambda: "Darwin")
    monkeypatch.setattr("fleet_manager.peer.model_sharing.platform.machine", lambda: "arm64")
    artifact, _ = export_directory(tmp_path)
    calls = []

    def backend(request):
        calls.append(request.url.path)
        assert request.url.path == "/api/version"
        return httpx.Response(200, json={"version": version})

    dest = ModelSharing(
        tmp_path / "dest",
        "http://backend",
        transport=httpx.MockTransport(backend),
        load_confirm=AsyncMock(return_value=True),
    )
    await dest.open()
    chunks = AsyncMock()
    try:
        request = TransferRequest(
            digest=artifact.manifest.digest,
            manifest_digest=artifact.manifest.manifest_digest,
            source="source",
            permit=True,
            license_approved=True,
        )
        await dest.start_transfer(request, AsyncMock(return_value=artifact.manifest), chunks)
        await dest._tasks[request.digest]
        status = (await dest.status())[0]
        assert status.state == "failed" and status.error == reason
        assert status.bytes_received == 0
        chunks.assert_not_awaited()
        assert calls == ["/api/version"]
        assert await dest.inventory("peer") == []
    finally:
        await dest.close()


@pytest.mark.parametrize("role,directory", [("draft", "draft"), ("adapter", "adapters")])
def test_auxiliary_model_shards_must_be_complete_before_becoming_an_offer(
    tmp_path, role, directory
):
    artifact, _ = export_directory(tmp_path, "gguf-package")
    root = Path(artifact.path)
    first = root / directory / "model-00001-of-00002.gguf"
    first.write_bytes(b"GGUFfirst")
    with pytest.raises(AdmissionError, match="incomplete_model_shards"):
        directory_artifact(
            root, model_id="test", format="gguf-package", license="MIT", provenance="test"
        )
    (first.parent / "model-00002-of-00002.gguf").write_bytes(b"GGUFsecond")
    complete = directory_artifact(
        root, model_id="test", format="gguf-package", license="MIT", provenance="test"
    )
    verify_package(complete)
    assert all(item.role == role for item in complete.manifest.files if "-of-" in item.name)
    # A peer cannot complete one import role with a shard assigned to another.
    first_shard = next(item for item in complete.manifest.files if "-00001-of-" in item.name)
    first_shard.role = "file"
    complete.manifest.digest = complete.manifest.calculated_package_digest()
    complete.manifest.manifest_digest = complete.manifest.calculated_digest()
    with pytest.raises(AdmissionError, match="incomplete_model_shards"):
        ModelSharing._validate(complete.manifest)


@pytest.mark.asyncio
@pytest.mark.parametrize("format", ["safetensors", "gguf"])
async def test_managed_exports_include_all_layers_and_rewrite_tensor_index(tmp_path, format):
    root, _, show, digest = installed_fixture(tmp_path, format)

    def backend(request):
        return httpx.Response(
            200,
            json={"models": [{"name": "test:latest", "digest": digest}]}
            if request.url.path == "/api/tags"
            else show,
        )

    async with httpx.AsyncClient(
        base_url="http://backend", transport=httpx.MockTransport(backend)
    ) as client:
        catalog = OllamaCatalog(client, models_dir=str(root))
        try:
            offer = (await catalog.artifacts())[0]
            assert offer.manifest.format == ("mlx" if format == "safetensors" else "gguf-package")
            assert str(root) not in offer.manifest.model_dump_json()
            assert offer.manifest.ollama.capabilities == ["completion", "vision"]
            assert offer.manifest.ollama.renderer == "test-renderer"
            verify_package(offer)
            if format == "safetensors":
                index = json.loads(
                    base64.b64decode(offer.inline_files["model.safetensors.index.json"])
                )
                assert set(index["weight_map"]) == {"one", "two"}
                assert set(index["weight_map"].values()) <= set(offer.source_files)
                assert "unavailable-source" not in json.dumps(index)
            else:
                assert len(offer.manifest.files) == 3
        finally:
            await catalog.close()


@pytest.mark.asyncio
async def test_standalone_exports_are_available_without_running_ollama(tmp_path):
    artifact, _ = export_directory(tmp_path)

    def offline(request):
        raise httpx.ConnectError("Ollama stopped")

    async with httpx.AsyncClient(
        base_url="http://backend", transport=httpx.MockTransport(offline)
    ) as client:
        catalog = OllamaCatalog(client, exports={"standalone:latest": artifact.path})
        try:
            offers = await catalog.artifacts()
            assert len(offers) == 1 and offers[0].manifest.format == "mlx"
            verify_package(offers[0])
        finally:
            await catalog.close()


@pytest.mark.asyncio
async def test_signed_inventory_negotiation_keeps_legacy_peers_and_enforces_revocation(
    tmp_path, monkeypatch
):
    from test_rpc import enrolled, signed, worker

    artifact, _ = export_directory(tmp_path, "gguf-package")
    async with worker(tmp_path / "worker", monkeypatch) as (app, authority, client):
        await app.state.model_sharing.register(artifact)
        peer = await enrolled(authority, tmp_path / "dest", "destination")
        path = "/peer/v1/models/inventory"
        for payload, expected in [({}, 0), ({"packages": True}, 1)]:
            body, headers = signed(peer, path, payload)
            result = await client.post(path, content=body, headers=headers)
            assert result.status_code == 200
            peer.verify_response(headers["x-idle-nonce"], 200, result.content, result.headers)
            assert len(result.json()["models"]) == expected
            assert str(tmp_path) not in result.text and "source_files" not in result.text
        path = "/peer/v1/models/chunk"
        payload = {"digest": artifact.manifest.digest, "offset": 0, "length": 20}
        body, headers = signed(peer, path, payload)
        result = await client.post(path, content=body, headers=headers)
        assert result.status_code == 200
        peer.verify_response(headers["x-idle-nonce"], 200, result.content, result.headers)
        assert base64.b64decode(result.json()["data"]) == read_range(artifact, 0, 20)
        assert (await client.post(path, content=body, headers=headers)).status_code == 403
        await app.state.model_sharing.set_permission(artifact.manifest.digest, [], False, False)
        body, headers = signed(peer, path, payload)
        assert (await client.post(path, content=body, headers=headers)).status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("digest_kind", ["parent", "child"])
async def test_mlx_manifest_list_resolves_only_the_selected_verified_child(tmp_path, digest_kind):
    root, path, show, child_digest = installed_fixture(tmp_path, "safetensors")
    (root / "blobs" / ("sha256-" + child_digest)).write_bytes(path.read_bytes())
    path.write_text(
        json.dumps(
            {
                "schemaVersion": 2,
                "manifests": [
                    {"digest": "sha256:" + child_digest, "format": "safetensors", "runner": "mlx"}
                ],
            }
        )
    )
    show["manifests"] = [{"digest": "sha256:" + child_digest, "selected": True}]
    digest = (
        hashlib.sha256(path.read_bytes()).hexdigest() if digest_kind == "parent" else child_digest
    )

    def backend(request):
        return httpx.Response(
            200,
            json={"models": [{"name": "test:latest", "digest": digest}]}
            if request.url.path == "/api/tags"
            else show,
        )

    async with httpx.AsyncClient(
        base_url="http://backend", transport=httpx.MockTransport(backend)
    ) as client:
        catalog = OllamaCatalog(client, models_dir=str(root))
        try:
            offer = (await catalog.artifacts())[0]
            verify_package(offer)
            assert len(offer.inline_files) == 1
        finally:
            await catalog.close()


def test_legacy_canonical_hash_and_wire_fields_remain_unchanged():
    legacy = {
        "model_id": "legacy",
        "digest": "a" * 64,
        "size_bytes": 10,
        "format": "gguf",
        "backend": "ollama",
        "quantization": "Q4",
        "provenance": "test",
        "license": "MIT",
        "required_memory_bytes": 1,
        "required_os": None,
        "required_cpu": None,
        "minimum_backend_version": None,
        "ollama": {"template": None, "system": None, "parameters": {}},
    }
    value = ArtifactManifest.model_validate({**legacy, "manifest_digest": "0" * 64})
    assert value.model_dump(exclude={"manifest_digest"}) == legacy
    assert value.legacy_compatible
    value.ollama.renderer = "advanced-renderer"
    assert not value.legacy_compatible
    value.ollama.renderer = None
    assert (
        value.calculated_digest()
        == hashlib.sha256(
            json.dumps(legacy, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        ).hexdigest()
    )


def test_register_directory_cli_builds_manifest_without_manual_hashes(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from fleet_manager.cli import peer_cli

    artifact, _ = export_directory(tmp_path)
    calls = []
    monkeypatch.setattr(
        peer_cli, "_control_request", lambda *args, **kwargs: calls.append((args, kwargs))
    )
    result = CliRunner().invoke(
        peer_cli.cli,
        [
            "models",
            "register-directory",
            artifact.path,
            "--model",
            "local-mlx",
            "--license",
            "MIT",
            "--provenance",
            "publisher@commit",
        ],
    )
    assert result.exit_code == 0, result.output
    args, kwargs = calls[0]
    assert args[:2] == ("POST", "artifacts")
    assert kwargs["timeout"] == 3600
    assert args[2]["manifest"]["format"] == "mlx"
    assert len(args[2]["manifest"]["files"]) == len(artifact.manifest.files)


@pytest.mark.asyncio
async def test_catalog_shutdown_waits_for_export_hashing_to_stop(tmp_path, monkeypatch):
    import asyncio
    import threading
    import time

    artifact, _ = export_directory(tmp_path)
    entered, stopped = threading.Event(), threading.Event()

    def hashing(*args, cancelled, **kwargs):
        entered.set()
        try:
            while not cancelled():
                time.sleep(0.001)
            raise ValueError("cancelled")
        finally:
            stopped.set()

    monkeypatch.setattr("fleet_manager.peer.ollama_catalog.directory_artifact", hashing)
    async with httpx.AsyncClient(
        base_url="http://backend",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"models": []})),
    ) as client:
        catalog = OllamaCatalog(client, exports={"mlx": artifact.path})
        await catalog.artifacts(wait=False)
        assert await asyncio.to_thread(entered.wait, 2)
        await asyncio.wait_for(catalog.close(), timeout=2)
        assert stopped.is_set()


@pytest.mark.asyncio
async def test_special_export_files_cannot_block_a_source_reader(tmp_path):
    import asyncio
    import os

    if not hasattr(os, "mkfifo"):
        pytest.skip("POSIX FIFO test")
    artifact, _ = export_directory(tmp_path)
    os.mkfifo(Path(artifact.path) / "pipe")
    with pytest.raises(AdmissionError, match="artifact_changed"):
        await asyncio.wait_for(
            asyncio.to_thread(
                directory_artifact,
                Path(artifact.path),
                model_id="test",
                format="mlx",
                license="MIT",
                provenance="test",
            ),
            timeout=2,
        )


@pytest.mark.asyncio
async def test_slow_discovery_does_not_stall_existing_chunks_or_revocation(tmp_path, monkeypatch):
    import asyncio

    artifact, _ = export_directory(tmp_path)
    manager = ModelSharing(
        tmp_path / "source",
        "http://backend",
        discover_installed=True,
        model_exports={"standalone": artifact.path},
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"models": []})),
    )
    await manager.open()
    try:
        offer = (await manager.inventory("peer"))[0]

        async def slow_refresh():
            await asyncio.Event().wait()

        monkeypatch.setattr(manager.catalog, "_refresh", slow_refresh)
        manager.catalog._expires = 0
        request = ChunkRequest(digest=offer.digest, offset=0, length=10)
        result = await asyncio.wait_for(manager.source_chunk("peer", request), timeout=1)
        assert len(base64.b64decode(result.data)) == 10
        await asyncio.wait_for(manager.set_permission(offer.digest, [], False, False), timeout=1)
        with pytest.raises(AdmissionError, match="artifact_not_found"):
            await asyncio.wait_for(manager.source_chunk("peer", request), timeout=1)
    finally:
        await manager.close()
