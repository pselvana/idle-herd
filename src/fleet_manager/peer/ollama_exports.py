"""Read-only Ollama export inventory, bound to the backend's advertised manifest."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
from pathlib import Path

from pydantic import BaseModel, Field

from fleet_manager.peer.model_packages import (
    MAX_FILES,
    MAX_METADATA,
    ExportFile,
    PackageFile,
    validate_layout,
)
from fleet_manager.peer.model_sharing import (
    ArtifactManifest,
    LocalArtifact,
    ModelSharing,
    _open_source,
)

MAX_MANIFEST = 2 * 1024 * 1024


class Layer(BaseModel):
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    size: int = Field(ge=0, le=2**63 - 1)
    mediaType: str
    name: str = ""


def read_json(path: Path, expected: str | None = None, *, with_digest: bool = False):
    with _open_source(path) as source:
        data = source.read(MAX_MANIFEST + 1)
    if len(data) > MAX_MANIFEST:
        raise ValueError("Ollama metadata exceeds export limit")
    if expected is not None and hashlib.sha256(data).hexdigest() != expected.removeprefix(
        "sha256:"
    ):
        raise ValueError("Ollama manifest changed during export")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("invalid Ollama metadata")
    return (value, hashlib.sha256(data).hexdigest()) if with_digest else value


def manifest_path(root: Path, model: str) -> Path:
    parts = model.split("/")
    leaf = parts.pop()
    model_name, separator, tag = leaf.rpartition(":")
    if not separator:
        model_name, tag = leaf, "latest"
    if parts and ("." in parts[0] or ":" in parts[0] or parts[0] == "localhost"):
        host = parts.pop(0)
    else:
        host = "registry.ollama.ai"
    components = [host, *(parts or ["library"]), model_name, tag]
    if any(
        not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.:-]*", value) or value in {".", ".."}
        for value in components
    ):
        raise ValueError("invalid model name for local export")
    return root / "manifests" / Path(*components)


def _selected_manifest(root, value, *, format, selected_digest):
    seen = set()
    for _ in range(4):
        if "manifests" not in value:
            return value
        children = value["manifests"]
        if not isinstance(children, list) or not children or len(children) > 32:
            raise ValueError("invalid Ollama manifest list")
        selected = [
            child
            for child in children
            if selected_digest and child.get("digest") == selected_digest
        ]
        if not selected:
            selected = [
                child
                for child in children
                if child.get("format") == format
                or (format == "safetensors" and child.get("runner") == "mlx")
            ]
        if len(selected) != 1:
            raise ValueError("ambiguous Ollama runner manifest")
        child = selected[0]
        if child.get("layers"):
            value = child
            continue
        digest = child.get("digest", "")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest) or digest in seen:
            raise ValueError("invalid Ollama child manifest")
        seen.add(digest)
        value = read_json(root / "blobs" / digest.replace(":", "-"), digest)
    raise ValueError("nested Ollama manifest limit")


def installed_files(model, digest, show, *, models_dir: str | None = None):
    """Enumerate all selected layers, never recursively copy a backend store."""
    roots = []
    if models_dir:
        roots.append(Path(models_dir).expanduser())
    else:
        for line in show.modelfile.splitlines():
            if line.startswith("FROM "):
                path = Path(line[5:].strip().strip('"'))
                if path.is_absolute() and path.parent.name == "blobs":
                    roots.append(path.parent.parent)
        configured = os.environ.get("OLLAMA_MODELS")
        if configured:
            roots.append(Path(configured).expanduser())
        roots.extend([Path.home() / ".ollama" / "models", Path("/usr/share/ollama/.ollama/models")])
    selected_digest = next(
        (item.get("digest") for item in show.manifests if item.get("selected")), None
    )
    for root in dict.fromkeys(roots):
        path = manifest_path(root, model)
        try:
            value, observed_digest = read_json(path, with_digest=True)
        except FileNotFoundError:
            continue
        if observed_digest != digest.removeprefix("sha256:"):
            expected_child = "sha256:" + digest.removeprefix("sha256:")
            if not any(
                child.get("digest") == expected_child for child in value.get("manifests", [])
            ):
                raise ValueError("Ollama manifest changed during export")
            if selected_digest and selected_digest != expected_child:
                raise ValueError("Ollama selected manifest changed during export")
            selected_digest = expected_child
        value = _selected_manifest(
            root, value, format=show.details.format, selected_digest=selected_digest
        )
        config = value.get("config")
        if config:
            config = Layer.model_validate(config)
            settings = read_json(root / "blobs" / config.digest.replace(":", "-"), config.digest)
            if settings.get("remote_host") or settings.get("remote_model"):
                raise ValueError("cloud models have no complete local export")
        layers = value.get("layers")
        if not isinstance(layers, list) or not 1 <= len(layers) <= MAX_FILES:
            raise ValueError("invalid Ollama layer inventory")
        layers = [Layer.model_validate(layer) for layer in layers]
        count = sum(layer.mediaType.endswith(".model") for layer in layers)
        components = []
        tensor_indices = {}
        model_index = 0
        metadata_types = {"template", "system", "license", "params", "messages", "prompt"}
        for index, layer in enumerate(layers):
            kind = layer.mediaType.removeprefix("application/vnd.ollama.image.")
            if kind in metadata_types:
                continue
            role = (
                "adapter"
                if kind == "adapter"
                else "draft"
                if kind == "draft" or layer.name.startswith(("draft/", "draft."))
                else "file"
            )
            name = layer.name
            if show.details.format == "safetensors" and kind == "tensor":
                prefix = "draft/" if role == "draft" else ""
                name = f"{prefix}tensor-{index:05}.safetensors"
                tensor_path = root / "blobs" / layer.digest.replace(":", "-")
                with _open_source(tensor_path) as source:
                    length = int.from_bytes(source.read(8), "little")
                    if not 2 <= length <= min(MAX_METADATA, layer.size - 8):
                        raise ValueError("invalid installed safetensors header")
                    header = json.loads(source.read(length))
                if not isinstance(header, dict):
                    raise ValueError("invalid installed tensor header")
                mapping = tensor_indices.setdefault(prefix, {})
                for tensor in header:
                    if tensor == "__metadata__":
                        continue
                    if tensor in mapping:
                        raise ValueError("duplicate tensor in installed MLX export")
                    mapping[tensor] = name.removeprefix(prefix)
            elif show.details.format == "safetensors" and name.endswith(
                "model.safetensors.index.json"
            ):
                # The old index names pre-import shards instead of installed blobs.
                continue
            if show.details.format == "gguf":
                if kind == "model":
                    model_index += 1
                    name = name or (
                        f"model-{model_index:05}-of-{count:05}.gguf" if count > 1 else "model.gguf"
                    )
                elif kind in {"projector", "adapter", "draft"}:
                    name = name or f"{kind}-{index}.gguf"
                else:
                    raise ValueError(f"unknown GGUF export layer: {kind}")
            elif not name:
                raise ValueError("safetensors export layer is missing its filename")
            if role == "draft" and name.startswith("draft."):
                name = "draft/" + name[6:]
            path = root / "blobs" / layer.digest.replace(":", "-")
            with _open_source(path) as source:
                if os.fstat(source.fileno()).st_size != layer.size:
                    raise ValueError("Ollama layer size mismatch")
            components.append(
                ExportFile(
                    file=PackageFile(
                        name=name, digest=layer.digest[7:], size_bytes=layer.size, role=role
                    ),
                    path=str(path),
                )
            )
        for prefix, mapping in tensor_indices.items():
            if not mapping:
                raise ValueError("empty installed MLX tensor inventory")
            data = json.dumps(
                {"weight_map": mapping}, sort_keys=True, separators=(",", ":")
            ).encode()
            if len(data) > MAX_METADATA:
                raise ValueError("installed MLX tensor index exceeds export limit")
            name = prefix + "model.safetensors.index.json"
            components.append(
                ExportFile(
                    file=PackageFile(
                        name=name,
                        digest=hashlib.sha256(data).hexdigest(),
                        size_bytes=len(data),
                        role="draft" if prefix else "file",
                    ),
                    content=data,
                )
            )
        return components
    return None


def package_artifact(
    *,
    model,
    format,
    components,
    options,
    license,
    provenance,
    minimum_version=None,
    quantization="source",
):
    files = sorted([item.file for item in components], key=lambda item: item.name)
    size = sum(item.size_bytes for item in files)
    manifest = ArtifactManifest(
        model_id=model,
        format=format,
        digest="0" * 64,
        manifest_digest="0" * 64,
        files=files,
        size_bytes=size,
        quantization=quantization,
        backend="ollama",
        license=license or "License not reported by source; review with its owner before copying.",
        provenance=provenance,
        required_memory_bytes=max(1, int(size * 1.2)),
        required_os="Darwin" if format == "mlx" else None,
        required_cpu="arm64" if format == "mlx" else None,
        minimum_backend_version=minimum_version or ("0.40.0" if format == "mlx" else None),
        ollama=options,
    )
    validate_layout(manifest)
    manifest.digest = manifest.calculated_package_digest()
    manifest.manifest_digest = manifest.calculated_digest()
    ModelSharing._validate(manifest)
    return LocalArtifact(
        manifest=manifest,
        path=next(item.path for item in components if item.path),
        source_files={item.file.name: item.path for item in components if item.path},
        inline_files={
            item.file.name: base64.b64encode(item.content).decode()
            for item in components
            if item.content is not None
        },
        discovered=True,
    )
