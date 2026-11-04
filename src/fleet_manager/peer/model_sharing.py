"""Default-shared, content-verified GGUF offers and local Ollama import.

Partial files and install ownership survive daemon restarts. A partial is never an
inventory item, and neither peer identities nor paths supplied over the wire select
local filesystem locations.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import platform
import re
import shutil
import sqlite3
import stat
from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import httpx
import psutil
from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_serializer

from fleet_manager.peer.model_packages import (
    MAX_FILES,
    PackageFile,
    read_range,
    validate_layout,
    verify_package,
)
from fleet_manager.peer.models import AdmissionError

CHUNK_SIZE = 1024 * 1024


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OllamaImportOptions(StrictModel):
    template: str | None = Field(default=None, max_length=65536)
    system: str | None = Field(default=None, max_length=65536)
    parameters: dict[str, JsonValue] = Field(default_factory=dict, max_length=128)
    renderer: str | None = Field(default=None, max_length=128)
    parser: str | None = Field(default=None, max_length=128)
    messages: list[dict[str, JsonValue]] | None = Field(default=None, max_length=128)
    capabilities: list[str] | None = Field(default=None, max_length=32)

    @model_serializer(mode="wrap")
    def serialize(self, handler):
        value = handler(self)
        for key in ("renderer", "parser", "messages", "capabilities"):
            if getattr(self, key) is None:
                value.pop(key, None)
        return value


class ArtifactManifest(StrictModel):
    model_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.:/-]*$")
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(gt=0, le=2**63 - 1)
    format: str = Field(default="gguf", min_length=1, max_length=64)
    backend: str = Field(default="ollama", min_length=1, max_length=64)
    quantization: str = Field(min_length=1, max_length=64)
    provenance: str = Field(min_length=1, max_length=512)
    license: str = Field(min_length=1, max_length=65536)
    required_memory_bytes: int = Field(gt=0, le=2**63 - 1)
    required_os: str | None = Field(default=None, max_length=32)
    required_cpu: str | None = Field(default=None, max_length=32)
    minimum_backend_version: str | None = Field(default=None, pattern=r"^\d+\.\d+\.\d+$")
    manifest_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    ollama: OllamaImportOptions | None = None
    files: list[PackageFile] | None = Field(default=None, min_length=1, max_length=MAX_FILES)

    @model_serializer(mode="wrap")
    def serialize(self, handler):
        value = handler(self)
        # Preserve canonical digests and wire shape for existing standalone offers.
        if self.ollama is None:
            value.pop("ollama", None)
        if self.files is None:
            value.pop("files", None)
        return value

    def calculated_digest(self) -> str:
        return _sha(
            json.dumps(
                self.model_dump(exclude={"manifest_digest"}),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode()
        )

    def calculated_package_digest(self) -> str:
        return _sha(
            json.dumps(
                self.model_dump(exclude={"digest", "manifest_digest"}),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode()
        )

    @property
    def cache_suffix(self) -> str:
        return ".package" if self.files is not None else ".gguf"

    @property
    def legacy_compatible(self) -> bool:
        return self.files is None and (
            self.ollama is None
            or all(
                getattr(self.ollama, field) is None
                for field in ("renderer", "parser", "messages", "capabilities")
            )
        )


class LocalArtifact(StrictModel):
    manifest: ArtifactManifest
    path: str = Field(min_length=1, max_length=4096)
    allowed_peers: list[Annotated[str, Field(min_length=1, max_length=128)]] = Field(
        default_factory=lambda: ["*"], max_length=128
    )
    sharing_enabled: bool = True
    license_approved: bool = True
    discovered: bool = False
    source_files: dict[str, str] = Field(default_factory=dict, max_length=MAX_FILES)
    inline_files: dict[str, str] = Field(default_factory=dict, max_length=MAX_FILES)

    def permits(self, principal: str) -> bool:
        return (
            self.sharing_enabled
            and self.license_approved
            and ("*" in self.allowed_peers or principal in self.allowed_peers)
        )


class ChunkRequest(StrictModel):
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    offset: int = Field(ge=0)
    length: int = Field(gt=0, le=CHUNK_SIZE)


class ChunkResponse(StrictModel):
    offset: int = Field(ge=0)
    data: str = Field(max_length=4 * ((CHUNK_SIZE + 2) // 3))
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class TransferRequest(StrictModel):
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source: str = Field(min_length=1, max_length=256)
    permit: bool = False
    license_approved: bool = False


class TransferStatus(StrictModel):
    digest: str
    source: str
    state: str
    bytes_received: int = 0
    total_bytes: int = 0
    error: str | None = None
    model_id: str | None = None
    manifest_digest: str


ManifestFetcher = Callable[[str, str], Awaitable[ArtifactManifest]]
ChunkFetcher = Callable[[str, ChunkRequest], Awaitable[ChunkResponse]]


def _regular(path: Path) -> bool:
    # Reject symlinks in every component, including a symlinked parent.
    absolute = path.absolute()
    return all(not p.is_symlink() for p in (absolute, *absolute.parents)) and path.is_file()


def _open_source(path: Path):
    """Open each POSIX path component without following symlinks."""
    if os.name == "nt" or os.open not in os.supports_dir_fd:
        if not _regular(path):
            raise AdmissionError("artifact_changed", 409)
        fd = os.open(
            path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        )
    else:
        parts = Path(os.path.abspath(path)).parts
        directory = os.open(parts[0], os.O_RDONLY | os.O_DIRECTORY)
        try:
            for part in parts[1:-1]:
                child = os.open(
                    part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory
                )
                os.close(directory)
                directory = child
            fd = os.open(
                parts[-1],
                os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0),
                dir_fd=directory,
            )
        finally:
            os.close(directory)
    f = os.fdopen(fd, "rb")
    if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):
        f.close()
        raise AdmissionError("artifact_changed", 409)
    return f


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(block)
    return h.hexdigest()


class ModelSharing:
    def __init__(
        self,
        directory: str | Path,
        backend_url: str,
        *,
        max_bytes: int = 20 * 1024**3,
        reserved_bytes: int = 1024**3,
        reserve_memory_bytes: int = 1024**3,
        transport: httpx.AsyncBaseTransport | None = None,
        load_confirm: Callable[[ArtifactManifest], Awaitable[bool]] | None = None,
        bytes_per_second: int = 10 * 1024 * 1024,
        discover_installed: bool = False,
        model_exports: dict[str, str] | None = None,
        ollama_models_dir: str | None = None,
    ):
        self.directory = Path(directory)
        self.max_bytes = max_bytes
        self.reserved_bytes = reserved_bytes
        self.reserve_memory_bytes = reserve_memory_bytes
        self.client = httpx.AsyncClient(
            base_url=backend_url, timeout=120, transport=transport, trust_env=False
        )
        if bytes_per_second <= 0:
            raise ValueError("bytes_per_second must be positive")
        self.bytes_per_second = bytes_per_second
        self.load_confirm = load_confirm
        self._tasks: dict[str, asyncio.Task] = {}
        self._install_guard = asyncio.Lock()
        self._lock_file = None
        self._guard = asyncio.Lock()
        self._closed = False
        self._source_guard = asyncio.Lock()
        self._source_pending = 0
        self._source_next = 0.0
        self.catalog = None
        if discover_installed:
            from fleet_manager.peer.ollama_catalog import OllamaCatalog

            self.catalog = OllamaCatalog(
                self.client, exports=model_exports, models_dir=ollama_models_dir
            )

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.directory / "sharing.sqlite3", timeout=30)
        try:
            db.execute("PRAGMA synchronous=FULL")
            db.row_factory = sqlite3.Row
            with db:
                yield db
        finally:
            db.close()

    async def open(self):
        def initialize():
            self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            lock = (self.directory / "manager.lock").open("a+b")
            try:
                if os.name == "nt":
                    import msvcrt

                    lock.seek(0)
                    lock.write(b"0")
                    lock.flush()
                    lock.seek(0)
                    msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                lock.close()
                raise AdmissionError("sharing_manager_already_running", 409) from exc
            self._lock_file = lock
            with self._db() as db:
                db.executescript("""
                CREATE TABLE IF NOT EXISTS artifacts
                    (digest TEXT PRIMARY KEY, record TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS transfers (digest TEXT PRIMARY KEY, source TEXT NOT NULL,
                    manifest_digest TEXT NOT NULL, state TEXT NOT NULL, received INTEGER NOT NULL,
                    total INTEGER NOT NULL, error TEXT, model_id TEXT);
                CREATE TABLE IF NOT EXISTS backend_imports
                    (digest TEXT PRIMARY KEY, model_id TEXT NOT NULL, backend_digest TEXT NOT NULL);
                UPDATE transfers SET state='paused', error='daemon restarted'
                    WHERE state IN ('transferring','importing');
                """)
                columns = {row[1] for row in db.execute("PRAGMA table_info(transfers)")}
                if "model_id" not in columns:
                    db.execute("ALTER TABLE transfers ADD COLUMN model_id TEXT")

        await asyncio.to_thread(initialize)

    async def close(self):
        async with self._guard:
            self._closed = True
            tasks = list(self._tasks.values())
            for task in tasks:
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.catalog is not None:
            await self.catalog.close()
        await self.client.aclose()
        if self._lock_file is not None:
            self._lock_file.close()
            self._lock_file = None

    @staticmethod
    def _validate(manifest: ArtifactManifest):
        if manifest.calculated_digest() != manifest.manifest_digest:
            raise AdmissionError("manifest_digest_mismatch", 400)
        if manifest.backend != "ollama":
            raise AdmissionError("unsupported_artifact_format", 400)
        if manifest.files is not None:
            validate_layout(manifest)
            if len(manifest.model_dump_json().encode()) > 2 * CHUNK_SIZE - 1024:
                raise AdmissionError("package_manifest_too_large", 400)
            if manifest.calculated_package_digest() != manifest.digest:
                raise AdmissionError("package_digest_mismatch", 400)
        elif manifest.format != "gguf":
            raise AdmissionError("unsupported_artifact_format", 400)

    async def register(self, artifact: LocalArtifact):
        self._validate(artifact.manifest)
        if artifact.sharing_enabled and (
            not artifact.license_approved or not artifact.allowed_peers
        ):
            raise AdmissionError("sharing_permission_required", 403)
        path = Path(artifact.path)

        def check():
            try:
                if artifact.manifest.files is not None:
                    verify_package(artifact)
                    return True
                with _open_source(path) as f:
                    if os.fstat(f.fileno()).st_size != artifact.manifest.size_bytes:
                        return False
                    h = hashlib.sha256()
                    for block in iter(lambda: f.read(CHUNK_SIZE), b""):
                        h.update(block)
                    return h.hexdigest() == artifact.manifest.digest
            except (OSError, AdmissionError, ValueError):
                return False

        if not await asyncio.to_thread(check):
            raise AdmissionError("artifact_verification_failed", 400)

        def save():
            with self._db() as db:
                row = db.execute(
                    "SELECT record FROM artifacts WHERE digest=?", (artifact.manifest.digest,)
                ).fetchone()
                if row is not None:
                    existing = LocalArtifact.model_validate_json(row[0])
                    if existing.manifest.manifest_digest != artifact.manifest.manifest_digest:
                        raise AdmissionError("artifact_manifest_conflict", 409)
                    return
                db.execute(
                    "INSERT INTO artifacts VALUES (?, ?)",
                    (artifact.manifest.digest, artifact.model_dump_json()),
                )

        await asyncio.to_thread(save)

    async def set_permission(
        self, digest: str, allowed_peers: list[str], sharing_enabled: bool, license_approved: bool
    ):
        if sharing_enabled and (not license_approved or not allowed_peers):
            raise AdmissionError("sharing_permission_required", 403)
        if len(allowed_peers) > 128 or any(not peer or len(peer) > 128 for peer in allowed_peers):
            raise AdmissionError("invalid_peer_permissions", 400)
        automatic = {
            item.manifest.digest: item for item in await self._automatic_artifacts(wait=False)
        }

        def update():
            with self._db() as db:
                row = db.execute(
                    "SELECT record FROM artifacts WHERE digest=?", (digest,)
                ).fetchone()
                if row is None:
                    if digest not in automatic:
                        raise AdmissionError("artifact_not_found", 404)
                    artifact = automatic[digest]
                else:
                    artifact = LocalArtifact.model_validate_json(row[0])
                artifact.allowed_peers = allowed_peers
                artifact.sharing_enabled = sharing_enabled
                artifact.license_approved = license_approved
                db.execute(
                    "INSERT OR REPLACE INTO artifacts (digest, record) VALUES (?, ?)",
                    (digest, artifact.model_dump_json()),
                )

        await asyncio.to_thread(update)

    async def _automatic_artifacts(self, *, wait: bool = True) -> list[LocalArtifact]:
        return await self.catalog.artifacts(wait=wait) if self.catalog is not None else []

    async def _artifacts(self, *, wait: bool = True) -> dict[str, LocalArtifact]:
        automatic = {
            item.manifest.digest: item for item in await self._automatic_artifacts(wait=wait)
        }

        def read():
            result = automatic.copy()
            with self._db() as db:
                for row in db.execute("SELECT record FROM artifacts"):
                    item = LocalArtifact.model_validate_json(row[0])
                    if item.discovered:
                        current = automatic.get(item.manifest.digest)
                        if current is None:
                            continue
                        item = current.model_copy(
                            update={
                                "allowed_peers": item.allowed_peers,
                                "sharing_enabled": item.sharing_enabled,
                                "license_approved": item.license_approved,
                            }
                        )
                    result[item.manifest.digest] = item
            return result

        return await asyncio.to_thread(read)

    async def inventory(
        self, principal: str | None = None, *, include_packages: bool = True
    ) -> list[ArtifactManifest]:
        items = (await self._artifacts(wait=principal is not None)).values()
        result = [
            item.manifest
            for item in items
            if (principal is None or item.permits(principal))
            and (include_packages or item.manifest.legacy_compatible)
        ]
        if principal is None:
            return result
        # Full licenses/templates can be much larger than legacy standalone
        # metadata. Keep the signed inventory inside the existing RPC budget.
        bounded = []
        size = 32
        for item in result[:128]:
            size += len(item.model_dump_json().encode()) + 1
            if size > 2 * CHUNK_SIZE - 1024:
                break
            bounded.append(item)
        return bounded

    async def _artifact(self, principal: str, digest: str) -> LocalArtifact:
        # A new/changed directory may take minutes to hash. Existing verified
        # offers and durable revocation checks must not wait behind that refresh.
        item = (await self._artifacts(wait=False)).get(digest)
        if item is None or not item.permits(principal):
            raise AdmissionError("artifact_not_found", 404)
        return item

    async def source_manifest(self, principal: str, digest: str) -> ArtifactManifest:
        return (await self._artifact(principal, digest)).manifest

    async def source_chunk(self, principal: str, request: ChunkRequest) -> ChunkResponse:
        # Bound concurrent queued readers as well as aggregate outgoing bandwidth.
        if self._source_pending >= 8:
            raise AdmissionError("model_source_busy", 429)
        self._source_pending += 1
        try:
            async with self._source_guard:
                delay = self._source_next - asyncio.get_running_loop().time()
                if delay > 0:
                    await asyncio.sleep(delay)
                # Check ACL after waiting, so a queued read observes revocation.
                item = await self._artifact(principal, request.digest)
                if request.offset >= item.manifest.size_bytes:
                    raise AdmissionError("invalid_offset", 400)

                def read():
                    if item.manifest.files is not None:
                        return read_range(
                            item,
                            request.offset,
                            min(request.length, item.manifest.size_bytes - request.offset),
                        )
                    try:
                        with _open_source(Path(item.path)) as f:
                            info = os.fstat(f.fileno())
                            if info.st_size != item.manifest.size_bytes:
                                raise AdmissionError("artifact_changed", 409)
                            f.seek(request.offset)
                            return f.read(
                                min(request.length, item.manifest.size_bytes - request.offset)
                            )
                    except OSError as exc:
                        raise AdmissionError("artifact_changed", 409) from exc

                data = await self._finish_thread(read)
                self._source_next = (
                    asyncio.get_running_loop().time() + len(data) / self.bytes_per_second
                )
                return ChunkResponse(
                    offset=request.offset, data=base64.b64encode(data).decode(), sha256=_sha(data)
                )
        finally:
            self._source_pending -= 1

    async def status(self) -> list[TransferStatus]:
        def read():
            with self._db() as db:
                return [
                    TransferStatus(
                        digest=r["digest"],
                        source=r["source"],
                        state=r["state"],
                        bytes_received=r["received"],
                        total_bytes=r["total"],
                        error=r["error"],
                        model_id=r["model_id"],
                        manifest_digest=r["manifest_digest"],
                    )
                    for r in db.execute("SELECT * FROM transfers ORDER BY digest")
                ]

        statuses = await asyncio.to_thread(read)
        ready = [item for item in statuses if item.state == "ready"]
        if not ready:
            return statuses
        try:
            response = await self.client.get("/api/tags", timeout=2)
            response.raise_for_status()
            models = response.json()["models"]
            if not isinstance(models, list):
                raise ValueError("invalid backend inventory")

            def imports():
                with self._db() as db:
                    return {
                        r["digest"]: (r["model_id"], r["backend_digest"])
                        for r in db.execute("SELECT * FROM backend_imports")
                    }

            records = await asyncio.to_thread(imports)
            for item in ready:
                recorded = records.get(item.digest)
                if recorded is None:
                    item.state, item.error = "backend_unknown", "backend_import_unconfirmed"
                    continue
                name, digest = recorded
                installed = next(
                    (
                        m
                        for m in models
                        if isinstance(m, dict)
                        and m.get("name", m.get("model")) in (name, name + ":latest")
                    ),
                    None,
                )
                if installed is None:
                    item.state, item.error = "backend_missing", "backend_model_missing"
                elif installed.get("digest") != digest:
                    item.state, item.error = "backend_changed", "backend_model_changed"
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            for item in ready:
                item.state, item.error = "backend_unknown", "backend_inventory_unavailable"
        return statuses

    async def _state(
        self, digest: str, state: str, received: int, total: int, error: str | None = None
    ):
        def update():
            with self._db() as db:
                db.execute(
                    "UPDATE transfers SET state=?, received=?, total=?, error=? WHERE digest=?",
                    (state, received, total, error, digest),
                )

        await self._finish_thread(update)

    async def start_transfer(
        self, request: TransferRequest, fetch_manifest: ManifestFetcher, fetch_chunk: ChunkFetcher
    ):
        if not request.permit or not request.license_approved:
            raise AdmissionError("transfer_permission_required", 403)
        async with self._guard:
            if self._closed:
                raise AdmissionError("sharing_manager_closed", 503)
            if request.digest in self._tasks:
                raise AdmissionError("transfer_in_progress", 409)

            def claim():
                with self._db() as db:
                    try:
                        db.execute(
                            "INSERT INTO transfers (digest, source, manifest_digest, state, "
                            "received, total, error) "
                            "VALUES (?, ?, ?, ?, 0, 0, NULL)",
                            (request.digest, request.source, request.manifest_digest, "paused"),
                        )
                    except sqlite3.IntegrityError as exc:
                        raise AdmissionError("transfer_exists_use_retry", 409) from exc

            await asyncio.to_thread(claim)
            self._launch(request, fetch_manifest, fetch_chunk)

    def _launch(self, request, fetch_manifest, fetch_chunk):
        task = asyncio.create_task(self._transfer(request, fetch_manifest, fetch_chunk))
        self._tasks[request.digest] = task
        task.add_done_callback(lambda _: self._tasks.pop(request.digest, None))

    async def retry(
        self, request: TransferRequest, fetch_manifest: ManifestFetcher, fetch_chunk: ChunkFetcher
    ):
        if not request.permit or not request.license_approved:
            raise AdmissionError("transfer_permission_required", 403)
        async with self._guard:
            if self._closed:
                raise AdmissionError("sharing_manager_closed", 503)
            if request.digest in self._tasks:
                raise AdmissionError("transfer_in_progress", 409)

            def claim():
                with self._db() as db:
                    row = db.execute(
                        "SELECT * FROM transfers WHERE digest=?", (request.digest,)
                    ).fetchone()
                    if (
                        row is None
                        or row["manifest_digest"] != request.manifest_digest
                        or row["state"] == "ready"
                    ):
                        raise AdmissionError("transfer_not_retryable", 409)
                    db.execute(
                        "UPDATE transfers SET source=? WHERE digest=?",
                        (request.source, request.digest),
                    )

            await asyncio.to_thread(claim)
            self._launch(request, fetch_manifest, fetch_chunk)

    async def pause(self, digest: str):
        task = self._tasks.get(digest)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _transfer(self, request, fetch_manifest, fetch_chunk):
        async with self._install_guard:
            await self._run_transfer(request, fetch_manifest, fetch_chunk)

    async def _compatibility(self, manifest: ArtifactManifest):
        if manifest.format == "mlx" and (
            platform.system() != "Darwin" or platform.machine().lower() not in {"arm64", "aarch64"}
        ):
            raise AdmissionError("mlx_requires_apple_silicon", 400)
        if manifest.required_os and manifest.required_os.lower() != platform.system().lower():
            raise AdmissionError("incompatible_os", 400)
        aliases = {"amd64": "x86_64", "aarch64": "arm64"}
        cpu = platform.machine().lower()
        required = (manifest.required_cpu or "").lower()
        if required and aliases.get(required, required) != aliases.get(cpu, cpu):
            raise AdmissionError("incompatible_cpu", 400)
        minimum_version = manifest.minimum_backend_version
        if manifest.format == "mlx":
            minimum_version = max(
                minimum_version or "0.0.0", "0.40.0", key=lambda v: tuple(map(int, v.split(".")))
            )
        if minimum_version:
            try:
                response = await self.client.get("/api/version")
                response.raise_for_status()
                current = response.json().get("version", "")
                match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:\+.*)?", current)
                if not match:
                    raise AdmissionError("backend_version_unknown", 400)
                installed = tuple(int(part) for part in match.groups())
                minimum = tuple(int(part) for part in minimum_version.split("."))
                if installed < minimum:
                    raise AdmissionError("backend_version_incompatible", 400)
            except httpx.HTTPError as exc:
                raise AdmissionError("backend_version_unknown", 503) from exc

    @staticmethod
    async def _finish_thread(func):
        # Shield file mutations; cancellation waits until the thread has stopped.
        task = asyncio.create_task(asyncio.to_thread(func))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def _run_transfer(self, request, fetch_manifest, fetch_chunk):
        received = total = 0
        try:
            manifest = await fetch_manifest(request.source, request.digest)
            self._validate(manifest)
            if (
                manifest.digest != request.digest
                or manifest.manifest_digest != request.manifest_digest
            ):
                raise AdmissionError("manifest_mismatch", 400)
            await self._compatibility(manifest)
            total = manifest.size_bytes

            def save_model_id():
                with self._db() as db:
                    db.execute(
                        "UPDATE transfers SET model_id=? WHERE digest=?",
                        (manifest.model_id, request.digest),
                    )

            await asyncio.to_thread(save_model_id)
            if total > self.max_bytes:
                raise AdmissionError("quota_exceeded", 400)
            available = (await asyncio.to_thread(psutil.virtual_memory)).available
            if manifest.required_memory_bytes + self.reserve_memory_bytes > available:
                raise AdmissionError("insufficient_memory", 400)
            path = self.directory / (request.digest + ".partial")

            def inspect():
                if path.exists() and not _regular(path):
                    raise AdmissionError("invalid_partial", 400)
                size = path.stat().st_size if path.exists() else 0
                if size > total:
                    raise AdmissionError("invalid_partial", 400)
                used = sum(
                    p.stat().st_size
                    for pattern in ("*.partial", "*.gguf", "*.package")
                    for p in self.directory.glob(pattern)
                    if _regular(p)
                )
                if used + total - size > self.max_bytes:
                    raise AdmissionError("quota_exceeded", 400)
                if (
                    shutil.disk_usage(self.directory).free
                    < total - size + total + self.reserved_bytes
                ):
                    raise AdmissionError("insufficient_disk", 400)
                return size

            received = await asyncio.to_thread(inspect)
            await self._state(request.digest, "transferring", received, total)
            next_chunk_at = asyncio.get_running_loop().time()
            while received < total:
                length = min(CHUNK_SIZE, total - received)
                delay = next_chunk_at - asyncio.get_running_loop().time()
                if delay > 0:
                    await asyncio.sleep(delay)
                chunk = await fetch_chunk(
                    request.source,
                    ChunkRequest(digest=request.digest, offset=received, length=length),
                )
                try:
                    data = base64.b64decode(chunk.data, validate=True)
                except ValueError as exc:
                    raise AdmissionError("invalid_chunk", 400) from exc
                if chunk.offset != received or len(data) != length or _sha(data) != chunk.sha256:
                    raise AdmissionError("invalid_chunk", 400)

                def append(data=data):
                    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
                    fd = os.open(path, flags, 0o600)
                    with os.fdopen(fd, "ab") as f:
                        f.write(data)
                        f.flush()
                        os.fsync(f.fileno())

                await self._finish_thread(append)
                next_chunk_at = (
                    asyncio.get_running_loop().time() + len(data) / self.bytes_per_second
                )
                received += len(data)
                await self._state(request.digest, "transferring", received, total)
            if manifest.files is not None:
                try:
                    await asyncio.to_thread(
                        verify_package, LocalArtifact(manifest=manifest, path=str(path))
                    )
                except (AdmissionError, ValueError) as exc:
                    await asyncio.to_thread(path.unlink)
                    received = 0
                    if isinstance(exc, AdmissionError):
                        raise
                    raise AdmissionError("invalid_package_metadata", 400) from exc
            elif await asyncio.to_thread(_hash_file, path) != request.digest:
                await asyncio.to_thread(path.unlink)
                received = 0
                raise AdmissionError("artifact_digest_mismatch", 400)

            def valid_magic():
                with path.open("rb") as f:
                    return f.read(4) == b"GGUF"

            if manifest.files is None and not await asyncio.to_thread(valid_magic):
                raise AdmissionError("invalid_gguf", 400)
            await self._state(request.digest, "importing", received, total)
            await self._import(manifest, path)

            # Publish only after verified import and a successful load.
            def publish():
                os.replace(path, self.directory / (request.digest + manifest.cache_suffix))
                tmp = self.directory / (request.digest + ".manifest.tmp")
                with tmp.open("w", encoding="utf-8") as f:
                    f.write(manifest.model_dump_json())
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self.directory / (request.digest + ".manifest.json"))
                if os.name != "nt":
                    directory_fd = os.open(self.directory, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)

            await self._finish_thread(publish)
            await self.register(
                LocalArtifact(
                    manifest=manifest,
                    path=str(self.directory / (request.digest + manifest.cache_suffix)),
                )
            )
            await self._state(request.digest, "ready", received, total)
        except asyncio.CancelledError:
            await asyncio.shield(self._state(request.digest, "paused", received, total, "paused"))
            raise
        except Exception as exc:
            reason = (
                exc.reason
                if isinstance(exc, AdmissionError)
                else (
                    f"backend_import_rejected_http_{exc.response.status_code}"
                    if isinstance(exc, httpx.HTTPStatusError)
                    else "transfer_failed"
                )
            )
            await self._state(request.digest, "failed", received, total, str(reason)[:256])

    async def _import(self, manifest: ArtifactManifest, path: Path):
        digest = "sha256:" + manifest.digest

        async def content(offset=0, length=None):
            f = await asyncio.to_thread(path.open, "rb")
            try:
                await asyncio.to_thread(f.seek, offset)
                remaining = length if length is not None else manifest.size_bytes
                while remaining:
                    block = await asyncio.to_thread(f.read, min(CHUNK_SIZE, remaining))
                    if not block:
                        raise AdmissionError("artifact_changed", 409)
                    remaining -= len(block)
                    yield block
            finally:
                await asyncio.to_thread(f.close)

        if self.load_confirm is None:
            raise AdmissionError("backend_load_confirmation_unavailable", 503)
        response = await self.client.get("/api/tags")
        response.raise_for_status()
        existing = response.json().get("models", [])
        installed = next(
            (
                item
                for item in existing
                if item.get("name", item.get("model"))
                in (manifest.model_id, manifest.model_id + ":latest")
            ),
            None,
        )

        def import_record():
            with self._db() as db:
                return db.execute(
                    "SELECT model_id, backend_digest FROM backend_imports WHERE digest=?",
                    (manifest.digest,),
                ).fetchone()

        record = await asyncio.to_thread(import_record)
        if installed is not None:
            if (
                record is None
                or record["model_id"] != manifest.model_id
                or not installed.get("digest")
                or installed["digest"] != record["backend_digest"]
            ):
                raise AdmissionError("model_name_already_installed", 409)
        else:
            files, adapters, draft_files = {}, {}, {}
            if manifest.files is None:
                response = await self.client.post("/api/blobs/" + digest, content=content())
                response.raise_for_status()
                files["model.gguf"] = digest
            else:
                offset = 0
                uploaded = set()
                for item in manifest.files:
                    blob_digest = "sha256:" + item.digest
                    if blob_digest not in uploaded:
                        response = await self.client.post(
                            "/api/blobs/" + blob_digest, content=content(offset, item.size_bytes)
                        )
                        response.raise_for_status()
                        uploaded.add(blob_digest)
                    target = (
                        adapters
                        if item.role == "adapter"
                        else draft_files
                        if item.role == "draft"
                        else files
                    )
                    target[
                        item.name.removeprefix("draft/") if item.role == "draft" else item.name
                    ] = blob_digest
                    offset += item.size_bytes
            options = manifest.ollama.model_dump(exclude_none=True) if manifest.ollama else {}
            if adapters:
                options["adapters"] = adapters
            if draft_files:
                options["draft_files"] = draft_files
            response = await self.client.post(
                "/api/create",
                json={
                    "model": manifest.model_id,
                    "files": files,
                    "stream": False,
                    "license": manifest.license,
                    **options,
                },
            )
            response.raise_for_status()
            if response.json().get("status") != "success":
                raise AdmissionError("backend_import_unconfirmed", 502)
            response = await self.client.get("/api/tags")
            response.raise_for_status()
            installed = next(
                (
                    item
                    for item in response.json().get("models", [])
                    if item.get("name", item.get("model"))
                    in (manifest.model_id, manifest.model_id + ":latest")
                ),
                None,
            )
            if not installed or not installed.get("digest"):
                raise AdmissionError("backend_import_unconfirmed", 502)

            def save_import():
                with self._db() as db:
                    db.execute(
                        "INSERT OR REPLACE INTO backend_imports VALUES (?, ?, ?)",
                        (manifest.digest, manifest.model_id, installed["digest"]),
                    )

            await asyncio.to_thread(save_import)
        if not await self.load_confirm(manifest):
            raise AdmissionError("backend_load_unconfirmed", 502)
