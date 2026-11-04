"""Discover local, standalone GGUF offers through Ollama's public metadata API."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
from pathlib import Path

import httpx
from pydantic import BaseModel, Field, ValidationError

from fleet_manager.peer.model_packages import ExportFile, PackageFile, directory_artifact
from fleet_manager.peer.model_sharing import (
    ArtifactManifest,
    LocalArtifact,
    OllamaImportOptions,
    _open_source,
)
from fleet_manager.peer.models import AdmissionError
from fleet_manager.peer.ollama_exports import installed_files, package_artifact

logger = logging.getLogger(__name__)


class OllamaTag(BaseModel):
    name: str
    digest: str


class OllamaTags(BaseModel):
    models: list[OllamaTag]


class OllamaDetails(BaseModel):
    format: str = ""
    quantization_level: str = "unknown"


class OllamaShow(BaseModel):
    modelfile: str
    details: OllamaDetails
    license: str = "License not reported by source; review with its owner before copying."
    template: str | None = None
    system: str | None = None
    parameters: str = ""
    capabilities: list[str] = Field(default_factory=list)
    renderer: str | None = None
    parser: str | None = None
    messages: list[dict] | None = None
    manifests: list[dict] = Field(default_factory=list)
    model_info: dict = Field(default_factory=dict)
    requires: str | None = None


def import_options(show: OllamaShow, *, package: bool = False) -> OllamaImportOptions:
    parameters = {}
    for line in show.parameters.splitlines():
        if not line.strip():
            continue
        key, raw = line.strip().split(maxsplit=1)
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", key):
            raise ValueError("invalid Ollama parameter")
        value = json.loads(raw)
        if isinstance(value, (dict, list)) or value is None:
            raise ValueError("unsupported Ollama parameter")
        if key in parameters:
            previous = parameters[key]
            parameters[key] = (
                [*previous, value] if isinstance(previous, list) else [previous, value]
            )
        else:
            parameters[key] = value
    return OllamaImportOptions(
        template=show.template,
        system=show.system,
        parameters=parameters,
        renderer=show.renderer,
        parser=show.parser,
        messages=show.messages,
        capabilities=show.capabilities if package else None,
    )


class OllamaCatalog:
    def __init__(
        self,
        client: httpx.AsyncClient,
        refresh_seconds: float = 30,
        *,
        exports: dict[str, str] | None = None,
        models_dir: str | None = None,
    ):
        self.client = client
        self.refresh_seconds = refresh_seconds
        self._expires = 0.0
        self._offers: list[LocalArtifact] = []
        self._lock = asyncio.Lock()
        self._refresh_task: asyncio.Task | None = None
        self.exports = exports or {}
        self.models_dir = models_dir
        self._export_cache = {}
        self._stop = threading.Event()

    async def close(self):
        self._stop.set()
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)

    async def artifacts(self, *, wait: bool = True) -> list[LocalArtifact]:
        if asyncio.get_running_loop().time() < self._expires:
            return self._offers
        if self._refresh_task is None or self._refresh_task.done():
            self._refresh_task = asyncio.create_task(self._refresh())
        if wait:
            return await asyncio.shield(self._refresh_task)
        return self._offers

    async def _refresh(self) -> list[LocalArtifact]:
        async with self._lock:
            if asyncio.get_running_loop().time() < self._expires:
                return self._offers
            unavailable = False
            try:
                response = await self.client.get("/api/tags", timeout=5)
                response.raise_for_status()
                tags = OllamaTags.model_validate(response.json())
            except (httpx.HTTPError, ValueError):
                # Previously observed readable GGUFs remain valid file offers even
                # when the source backend is stopped. Never create offers on failure.
                logger.warning("Automatic model discovery: backend inventory unavailable")
                tags = OllamaTags(models=[])
                unavailable = True
            semaphore = asyncio.Semaphore(4)

            async def discover(tag):
                async with semaphore:
                    try:
                        return await self._offer(tag)
                    except ValidationError as exc:
                        logger.warning(
                            "Automatic sharing unavailable for %s: invalid metadata fields %s",
                            tag.name,
                            [error["loc"] for error in exc.errors()],
                        )
                        return None
                    except (httpx.HTTPError, OSError, ValueError, AdmissionError) as exc:
                        logger.warning("Automatic sharing unavailable for %s: %s", tag.name, exc)
                        return None

            offers = await asyncio.gather(
                *(discover(tag) for tag in sorted(tags.models, key=lambda t: t.name))
            )
            if unavailable:
                offers.extend(
                    offer
                    for offer in self._offers
                    if offer.manifest.provenance.startswith("ollama:")
                )
            for name, directory in self.exports.items():
                try:
                    task = asyncio.create_task(
                        asyncio.to_thread(self._export, name, Path(directory))
                    )
                    try:
                        offers.append(await asyncio.shield(task))
                    except asyncio.CancelledError:
                        self._stop.set()
                        await asyncio.gather(task, return_exceptions=True)
                        raise
                except (OSError, ValueError, AdmissionError) as exc:
                    logger.warning("Standalone model export unavailable for %s: %s", name, exc)
            # Existing transfer authority is content-addressed, including model aliases.
            by_digest = {}
            for offer in offers:
                if offer is not None:
                    by_digest.setdefault(offer.manifest.digest, offer)
            self._offers = list(by_digest.values())
            self._expires = asyncio.get_running_loop().time() + self.refresh_seconds
            return self._offers

    async def _offer(self, tag: OllamaTag) -> LocalArtifact:
        response = await self.client.post("/api/show", json={"model": tag.name}, timeout=5)
        response.raise_for_status()
        show = OllamaShow.model_validate(response.json())
        if show.details.format not in {"gguf", "safetensors"}:
            raise ValueError("unsupported model format")
        components = await asyncio.to_thread(
            installed_files, tag.name, tag.digest, show, models_dir=self.models_dir
        )
        if components is not None and (
            show.details.format == "safetensors"
            or len(components) > 1
            or set(show.capabilities) & {"vision", "audio"}
        ):
            return package_artifact(
                model=tag.name,
                format="mlx" if show.details.format == "safetensors" else "gguf-package",
                components=components,
                quantization=show.details.quantization_level or "unknown",
                options=import_options(show, package=True),
                license=show.license,
                provenance=f"ollama:{tag.name}@{tag.digest}",
                minimum_version=show.requires,
            )
        if show.details.format == "safetensors":
            raise ValueError("readable Ollama manifest required for complete MLX export")
        directives = []
        multiline = False
        for line in show.modelfile.splitlines():
            if not multiline:
                match = re.fullmatch(r"(FROM|PROJECTOR|ADAPTER|DRAFT)\s+(.+)", line.strip())
                if match:
                    directives.append((match[1], Path(match[2].strip().strip('"'))))
            if not line.lstrip().startswith("#") and line.count('"""') % 2:
                multiline = not multiline
        if not directives:
            raise ValueError("missing local GGUF")
        if components is None and show.model_info.get("split.count", 1) > 1:
            raise ValueError("complete shard inventory requires readable Ollama manifest")
        if len(directives) > 1 or set(show.capabilities) & {"vision", "audio"}:
            parts = []
            for index, (role, path) in enumerate(directives):
                expected, size = await asyncio.to_thread(self._inspect_blob, path)
                part = PackageFile(
                    name=f"{role.lower()}-{index}.gguf",
                    digest=expected,
                    size_bytes=size,
                    role="adapter" if role == "ADAPTER" else "draft" if role == "DRAFT" else "file",
                )
                parts.append(ExportFile(file=part, path=str(path)))
            return package_artifact(
                model=tag.name,
                format="gguf-package",
                components=parts,
                quantization=show.details.quantization_level or "unknown",
                options=import_options(show, package=True),
                license=show.license,
                provenance=f"ollama:{tag.name}@{tag.digest}",
                minimum_version=show.requires,
            )
        path = directives[0][1]
        digest, size = await asyncio.to_thread(self._inspect_blob, path)
        manifest = ArtifactManifest(
            model_id=tag.name,
            digest=digest,
            size_bytes=size,
            quantization=show.details.quantization_level or "unknown",
            provenance=f"ollama:{tag.name}@{tag.digest}",
            license=show.license
            or "License not reported by source; review with its owner before copying.",
            required_memory_bytes=max(1, int(size * 1.2)),
            manifest_digest="0" * 64,
            ollama=import_options(show),
        )
        manifest.manifest_digest = manifest.calculated_digest()
        return LocalArtifact(manifest=manifest, path=str(path), discovered=True)

    @staticmethod
    def _inspect_blob(path):
        digest_match = re.fullmatch(r"sha256[:-]([0-9a-f]{64})", path.name)
        if not path.is_absolute() or digest_match is None:
            raise ValueError("model is not a local content-addressed blob")

        with _open_source(path) as source:
            size = os.fstat(source.fileno()).st_size
            if source.read(4) != b"GGUF":
                raise ValueError("not a GGUF")
        return digest_match[1], size

    def _export(self, name, directory):
        # Rehash only when directory entries or file identity/size/mtime change.
        if not directory.is_dir() or any(
            path.is_symlink() for path in (directory, *directory.parents)
        ):
            raise ValueError("export must be a regular directory without symlink parents")
        fingerprint = []
        for root, dirs, names in os.walk(directory, followlinks=False):
            dirs[:] = [name for name in dirs if name not in {".git", ".cache"}]
            for entry in [*dirs, *names]:
                if self._stop.is_set():
                    raise ValueError("export discovery cancelled")
                if entry == ".DS_Store":
                    continue
                path = Path(root) / entry
                if path.is_symlink():
                    raise ValueError("export contains a symlink")
                info = path.lstat()
                fingerprint.append((str(path), info.st_ino, info.st_size, info.st_mtime_ns))
                if len(fingerprint) > 8192:
                    raise ValueError("export exceeds directory entry limit")
        fingerprint = sorted(fingerprint)
        cached = self._export_cache.get(name)
        if cached and cached[0] == fingerprint:
            return cached[1]
        format = (
            "mlx"
            if any(item[0].endswith(".safetensors") for item in fingerprint)
            else "gguf-package"
        )
        license = "License not reported by source; review with its owner before copying."
        for candidate in ("LICENSE", "LICENSE.txt", "LICENSE.md"):
            try:
                with _open_source(directory / candidate) as source:
                    data = source.read(65537)
                if len(data) <= 65536:
                    license = data.decode()
                break
            except FileNotFoundError:
                continue
        artifact = directory_artifact(
            directory,
            model_id=name,
            format=format,
            license=license,
            provenance=f"local-export:{name}",
            cancelled=self._stop.is_set,
        )
        artifact.discovered = True
        self._export_cache[name] = (fingerprint, artifact)
        return artifact
