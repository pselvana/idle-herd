"""Discover local, standalone GGUF offers through Ollama's public metadata API."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path

import httpx
from pydantic import BaseModel, Field, ValidationError

from fleet_manager.peer.model_sharing import (
    ArtifactManifest,
    LocalArtifact,
    OllamaImportOptions,
    _open_source,
)
from fleet_manager.peer.models import AdmissionError

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


def import_options(show: OllamaShow) -> OllamaImportOptions:
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
    return OllamaImportOptions(template=show.template, system=show.system, parameters=parameters)


class OllamaCatalog:
    def __init__(self, client: httpx.AsyncClient, refresh_seconds: float = 30):
        self.client = client
        self.refresh_seconds = refresh_seconds
        self._expires = 0.0
        self._offers: list[LocalArtifact] = []
        self._lock = asyncio.Lock()
        self._refresh_task: asyncio.Task | None = None

    async def close(self):
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
            try:
                response = await self.client.get("/api/tags", timeout=5)
                response.raise_for_status()
                tags = OllamaTags.model_validate(response.json())
            except (httpx.HTTPError, ValueError):
                # Previously observed readable GGUFs remain valid file offers even
                # when the source backend is stopped. Never create offers on failure.
                logger.warning("Automatic model discovery: backend inventory unavailable")
                self._expires = asyncio.get_running_loop().time() + 5
                return self._offers
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
        # Multi-file, adapter and multimodal copying needs a different import
        # adapter; exporting only their base weights would misrepresent the model.
        if show.details.format != "gguf" or set(show.capabilities) & {"vision", "audio"}:
            raise ValueError("unsupported model format")
        if re.search(r"(?im)^\s*(ADAPTER|PROJECTOR)\s", show.modelfile):
            raise ValueError("multi-file model")
        match = re.search(r"(?im)^FROM\s+(.+)$", show.modelfile)
        if match is None:
            raise ValueError("missing local GGUF")
        path = Path(match[1].strip().strip('"'))
        digest_match = re.fullmatch(r"sha256[:-]([0-9a-f]{64})", path.name)
        if not path.is_absolute() or digest_match is None:
            raise ValueError("model is not a local content-addressed blob")

        def inspect():
            with _open_source(path) as source:
                size = os.fstat(source.fileno()).st_size
                if source.read(4) != b"GGUF":
                    raise ValueError("not a GGUF")
                return size

        size = await asyncio.to_thread(inspect)
        manifest = ArtifactManifest(
            model_id=tag.name,
            digest=digest_match[1],
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
