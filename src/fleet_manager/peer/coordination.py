"""Optional admin consensus observations; no admission/runtime authority lives here."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import os
import re
import stat
import time
from pathlib import Path
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class CoordinationStatus(BaseModel):
    enabled: bool = False
    reachable: bool = False
    quorum: bool | None = None
    local_write_available: bool = False
    state: str = "disabled"
    leader: str | None = None
    observed_at: float | None = None
    preferred_peer: str | None = None
    advisory_only: Literal[True] = True


class RaftStatus(BaseModel):
    node_id: str = Field(max_length=128)
    state: Literal["Leader", "Follower", "Candidate", "Shutdown"]
    leader: str = Field(max_length=256)
    quorum: bool | None
    local_write_available: bool = False


class MetadataUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    key: Literal["cluster_label", "operator_note"]
    value: str = Field(max_length=1024)


class MetadataView(BaseModel):
    model_config = ConfigDict(extra="forbid")
    metadata: dict[Literal["cluster_label", "operator_note"], str]
    consistent: bool


class CoordinationUnavailable(Exception):
    pass


def _token(path: str | None) -> str:
    if path is None:
        raise ValueError("missing coordination token file")
    fd = os.open(
        Path(path), os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    )
    with os.fdopen(fd) as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or (os.name != "nt" and info.st_mode & 0o077):
            raise PermissionError("coordination token must be a private regular file")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise PermissionError("coordination token owned by another user")
        value = stream.read(257).strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{32,256}", value):
        raise ValueError("invalid coordination token")
    return value


def preferred_peer(
    cluster_id: str, candidates: list[str], task: str = "optimization"
) -> str | None:
    """Rendezvous advice over one observed component, with no uniqueness claim."""
    return max(
        set(candidates),
        key=lambda peer: (hashlib.sha256(f"{cluster_id}\0{task}\0{peer}".encode()).digest(), peer),
        default=None,
    )


class AdminCoordination:
    def __init__(self, settings, *, transport=None):
        self.settings = settings
        self._status = CoordinationStatus(
            enabled=settings.raft_url is not None,
            state="unavailable" if settings.raft_url else "disabled",
        )
        self._observed = 0.0
        self._task = None
        self._client = httpx.AsyncClient(
            transport=transport, trust_env=False, follow_redirects=False, timeout=0.75
        )

    async def open(self):
        # No network or credential read in peer startup. Even an absent or broken
        # sidecar is only a diagnostic; the independent poll owns its failure.
        if self.settings.raft_url:
            self._task = asyncio.create_task(self._run())

    async def close(self):
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._client.aclose()

    def snapshot(self, app=None, *, participating=True) -> CoordinationStatus:
        status = self._status
        if status.enabled and time.monotonic() - self._observed > 3:
            status = status.model_copy(
                update={
                    "reachable": False,
                    "quorum": None,
                    "local_write_available": False,
                    "state": "unavailable",
                    "leader": None,
                }
            )
        if app is not None and not status.quorum and participating:
            ledger = app.state.ledger
            candidates = [ledger.worker_id]
            if app.state.directory and participating:
                candidates += [
                    p.worker_id
                    for p in app.state.directory.snapshot()
                    if p.reachable and p.worker_id
                ]
            status = status.model_copy(
                update={"preferred_peer": preferred_peer(ledger.cluster_id, candidates)}
            )
        return status

    async def _request(self, method: str, path: str, spec: MetadataUpdate | None = None):
        if not self.settings.raft_url:
            raise CoordinationUnavailable()
        try:
            token = await asyncio.to_thread(_token, self.settings.raft_token_file)
            deadline = 1 if path == "/v1/status" else 5
            async with asyncio.timeout(deadline):
                async with self._client.stream(
                    method,
                    self.settings.raft_url + path,
                    headers={"Authorization": "Bearer " + token},
                    json=spec.model_dump() if spec else None,
                    timeout=deadline,
                ) as response:
                    response.raise_for_status()
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > 16384:
                            raise ValueError("oversized coordination response")
                    return bytes(body)
        except (OSError, ValueError, httpx.HTTPError, TimeoutError) as exc:
            raise CoordinationUnavailable() from exc

    async def refresh(self):
        try:
            status = RaftStatus.model_validate_json(await self._request("GET", "/v1/status"))
            self._status = CoordinationStatus(
                enabled=True,
                reachable=True,
                quorum=status.quorum,
                local_write_available=status.local_write_available,
                state=status.state,
                leader=status.leader or None,
                observed_at=time.time(),
            )
            self._observed = time.monotonic()
        except (CoordinationUnavailable, ValueError):
            if self._status.reachable:
                logger.warning(
                    "Optional admin coordination unavailable; inference remains independent"
                )
            self._status = CoordinationStatus(enabled=True, state="unavailable")
            self._observed = 0

    async def _run(self):
        while True:
            await self.refresh()
            await asyncio.sleep(1)

    async def metadata(self, spec: MetadataUpdate | None = None) -> MetadataView:
        try:
            return MetadataView.model_validate_json(
                await self._request("PUT" if spec else "GET", "/v1/metadata", spec)
            )
        except ValueError as exc:
            raise CoordinationUnavailable() from exc
