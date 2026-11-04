"""Bounded multi-file model packages; no archive extraction or peer-selected paths."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from fleet_manager.peer.models import AdmissionError

if TYPE_CHECKING:
    from fleet_manager.peer.model_sharing import ArtifactManifest, LocalArtifact

MAX_FILES = 4096
MAX_METADATA = 16 * 1024 * 1024


def safe_name(value: str) -> str:
    if len(value) > 240:
        raise ValueError("package filename too long")
    for part in value.split("/"):
        if (
            not re.fullmatch(r"[A-Za-z0-9_.][A-Za-z0-9_.-]*", part)
            or part in {".", ".."}
            or part.endswith(".")
            or part.split(".")[0].upper()
            in {
                "CON",
                "PRN",
                "AUX",
                "NUL",
                *[f"COM{i}" for i in range(1, 10)],
                *[f"LPT{i}" for i in range(1, 10)],
            }
        ):
            raise ValueError("unsafe package filename")
    return value


class PackageFile(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0, le=2**63 - 1)
    role: Literal["file", "adapter", "draft"] = "file"

    _safe_name = field_validator("name")(safe_name)


class ExportFile(BaseModel):
    """One local file or bounded generated metadata; never a peer-RPC payload."""

    model_config = ConfigDict(extra="forbid")
    file: PackageFile
    path: str | None = Field(default=None, min_length=1, max_length=4096)
    content: bytes | None = Field(default=None, max_length=MAX_METADATA)

    @model_validator(mode="after")
    def one_source(self):
        if (self.path is None) == (self.content is None):
            raise ValueError("export file must have exactly one local source")
        return self


def validate_layout(manifest: ArtifactManifest):
    files = manifest.files or []
    names = [item.name.casefold() for item in files]
    if not files or len(set(names)) != len(names):
        raise AdmissionError("invalid_package_files", 400)
    if sum(item.size_bytes for item in files) != manifest.size_bytes:
        raise AdmissionError("package_size_mismatch", 400)
    base = [item.name for item in files if item.role == "file"]
    if manifest.format == "mlx":
        if "config.json" not in base or not any(name.endswith(".safetensors") for name in base):
            raise AdmissionError("incomplete_mlx_package", 400)
    elif manifest.format != "gguf-package" or not any(name.endswith(".gguf") for name in base):
        raise AdmissionError("unsupported_artifact_format", 400)
    groups = {}
    for item in files:
        match = re.fullmatch(r"(.*)-(\d{5})-of-(\d{5})(\.(?:gguf|safetensors))", item.name)
        if match:
            # Draft models and adapters are part of the complete export too.
            # Shards assigned to another import role cannot complete this group.
            key = (item.role, match[1], match[3], match[4])
            groups.setdefault(key, set()).add(int(match[2]))
    for (_, _, count, _), indices in groups.items():
        if len(indices) != int(count) or min(indices) != 1 or max(indices) != int(count):
            raise AdmissionError("incomplete_model_shards", 400)


def read_range(artifact: LocalArtifact, offset: int, length: int) -> bytes:
    from fleet_manager.peer.model_sharing import _open_source

    component_sources = bool(artifact.source_files or artifact.inline_files)
    if component_sources:
        sources = set(artifact.source_files)
        inline = set(artifact.inline_files)
        expected = {item.name for item in artifact.manifest.files or []}
        if sources & inline or sources | inline != expected:
            raise AdmissionError("package_source_mismatch", 409)
        segments = ((item.name, item.size_bytes) for item in artifact.manifest.files)
    else:
        segments = [(None, artifact.manifest.size_bytes)]
    result = bytearray()
    try:
        for name, size in segments:
            if offset >= size:
                offset -= size
                continue
            path = (
                base64.b64decode(artifact.inline_files[name], validate=True)
                if name in artifact.inline_files
                else Path(artifact.source_files[name])
                if component_sources
                else Path(artifact.path)
            )
            if isinstance(path, bytes):
                if len(path) != size:
                    raise AdmissionError("artifact_changed", 409)
                block = path[offset : offset + min(length - len(result), size - offset)]
                result.extend(block)
                offset = 0
                if len(result) == length:
                    break
                continue
            with _open_source(path) as source:
                if os.fstat(source.fileno()).st_size != size:
                    raise AdmissionError("artifact_changed", 409)
                source.seek(offset)
                count = min(length - len(result), size - offset)
                block = source.read(count)
                if len(block) != count:
                    raise AdmissionError("artifact_changed", 409)
                result.extend(block)
            offset = 0
            if len(result) == length:
                break
    except OSError as exc:
        raise AdmissionError("artifact_changed", 409) from exc
    if len(result) != length:
        raise AdmissionError("artifact_changed", 409)
    return bytes(result)


def verify_package(artifact: LocalArtifact):
    from fleet_manager.peer.model_sharing import CHUNK_SIZE

    offset = 0
    names = {item.name for item in artifact.manifest.files or []}
    for item in artifact.manifest.files or []:
        digest = hashlib.sha256()
        for relative in range(0, item.size_bytes, CHUNK_SIZE):
            digest.update(
                read_range(artifact, offset + relative, min(CHUNK_SIZE, item.size_bytes - relative))
            )
        if digest.hexdigest() != item.digest:
            raise AdmissionError("package_file_digest_mismatch", 400)
        if (
            item.name.endswith(".gguf")
            and read_range(artifact, offset, min(4, item.size_bytes)) != b"GGUF"
        ):
            raise AdmissionError("invalid_gguf", 400)
        if item.name.endswith(".safetensors"):
            if item.size_bytes < 10:
                raise AdmissionError("invalid_safetensors", 400)
            header_length = int.from_bytes(read_range(artifact, offset, 8), "little")
            if not 2 <= header_length <= min(MAX_METADATA, item.size_bytes - 8):
                raise AdmissionError("invalid_safetensors", 400)
            header = json.loads(read_range(artifact, offset + 8, header_length))
            if not isinstance(header, dict):
                raise AdmissionError("invalid_safetensors", 400)
        if item.name.endswith(("config.json", "model.safetensors.index.json")):
            if item.size_bytes > MAX_METADATA:
                raise AdmissionError("package_metadata_too_large", 400)
            metadata = json.loads(read_range(artifact, offset, item.size_bytes))
            if not isinstance(metadata, dict):
                raise AdmissionError("invalid_package_metadata", 400)
            if item.name.endswith(".index.json"):
                mapping = metadata.get("weight_map")
                prefix = item.name.rpartition("/")[0]
                if (
                    not isinstance(mapping, dict)
                    or not mapping
                    or any(
                        not isinstance(name, str)
                        or (prefix + "/" + name if prefix else name) not in names
                        for name in mapping.values()
                    )
                ):
                    raise AdmissionError("incomplete_model_shards", 400)
        offset += item.size_bytes


def directory_artifact(
    directory: Path,
    *,
    model_id: str,
    license: str,
    provenance: str,
    format: Literal["mlx", "gguf-package"],
    required_memory_bytes: int | None = None,
    cancelled: Callable[[], bool] | None = None,
):
    """Explicit owner-selected exports, including standalone MLX snapshots."""
    from fleet_manager.peer.model_sharing import (
        CHUNK_SIZE,
        ArtifactManifest,
        LocalArtifact,
        _open_source,
    )

    directory = directory.absolute()
    if not directory.is_dir() or any(path.is_symlink() for path in (directory, *directory.parents)):
        raise ValueError("export directory must not be a symlink")
    files, sources = [], {}
    for root, directories, filenames in os.walk(directory, followlinks=False):
        directories[:] = [name for name in directories if name not in {".git", ".cache"}]
        if any((Path(root) / name).is_symlink() for name in directories):
            raise ValueError("export contains a symlinked directory")
        for name in filenames:
            if name == ".DS_Store":
                continue
            if cancelled and cancelled():
                raise ValueError("export discovery cancelled")
            path = Path(root) / name
            relative = safe_name(path.relative_to(directory).as_posix())
            digest = hashlib.sha256()
            with _open_source(path) as source:
                size = os.fstat(source.fileno()).st_size
                for block in iter(lambda: source.read(CHUNK_SIZE), b""):
                    if cancelled and cancelled():
                        raise ValueError("export discovery cancelled")
                    digest.update(block)
            files.append(
                PackageFile(
                    name=relative,
                    digest=digest.hexdigest(),
                    size_bytes=size,
                    role="adapter"
                    if relative.startswith("adapters/")
                    else "draft"
                    if relative.startswith("draft/")
                    else "file",
                )
            )
            sources[relative] = str(path)
            if len(files) > MAX_FILES:
                raise ValueError("too many export files")
    size = sum(item.size_bytes for item in files)
    manifest = ArtifactManifest(
        model_id=model_id,
        digest="0" * 64,
        manifest_digest="0" * 64,
        size_bytes=size,
        format=format,
        quantization="source",
        license=license,
        provenance=provenance,
        required_memory_bytes=required_memory_bytes or max(1, int(size * 1.2)),
        required_os="Darwin" if format == "mlx" else None,
        required_cpu="arm64" if format == "mlx" else None,
        minimum_backend_version="0.40.0" if format == "mlx" else None,
        files=sorted(files, key=lambda item: item.name),
    )
    manifest.digest = manifest.calculated_package_digest()
    manifest.manifest_digest = manifest.calculated_digest()
    validate_layout(manifest)
    from fleet_manager.peer.model_sharing import ModelSharing

    ModelSharing._validate(manifest)
    return LocalArtifact(manifest=manifest, path=str(directory), source_files=sources)
