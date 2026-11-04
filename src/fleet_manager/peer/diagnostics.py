"""Bounded, advisory diagnostics exchanged only among authenticated peers."""

from __future__ import annotations

import asyncio
import logging
import platform
import re
import time
from typing import Annotated, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from fleet_manager.peer.discovery import MAX_BODY, peer_observation_is_fresh
from fleet_manager.peer.hosts import hostname_label

log = logging.getLogger(__name__)

PATH = "/peer/v1/status"
SAFE_NAME = re.compile(r"^(?![A-Za-z]:/)[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$")
REASONS = {
    "policy_never",
    "manual_approval_required",
    "sensor_stale",
    "memory_pressure",
    "thermal_limit",
    "ac_required",
    "battery_low",
    "owner_active",
    "activity_unknown",
    "utilization_unknown",
    "cpu_busy",
    "cpu_recovery",
    "gpu_busy",
    "idle_hysteresis",
    "quiet_hours",
    "manual_offline",
    "manual_paused",
    "manual_draining",
    "peer_quota",
}


class StatusQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PeerObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    version: Literal[1] = 1
    worker_id: str = Field(min_length=1, max_length=128)
    worker_incarnation: str = Field(min_length=1, max_length=128)
    hostname: str | None = Field(default=None, max_length=253)
    observed_at: float = Field(ge=0)
    platform: str = Field(max_length=128)
    backend: Literal["ollama"] = "ollama"
    models: list[Annotated[str, Field(min_length=1, max_length=128)]] = Field(max_length=256)
    sensor_source: str = Field(max_length=64, pattern=r"^[a-z0-9_-]+$")
    activity: Literal["idle", "active", "unknown"]
    eligibility_reason: str | None = Field(default=None, max_length=64, pattern=r"^[a-z0-9_]+$")
    eligible: bool
    reserved_slots: int = Field(ge=0, le=128)
    running_slots: int = Field(ge=0, le=128)
    quarantined_slots: int = Field(ge=0, le=128)
    total_slots: int = Field(ge=0, le=128)
    waiters: int = Field(ge=0, le=10000)
    queued_jobs: int = Field(ge=0, le=10000)
    running_jobs: int = Field(ge=0, le=10000)
    replicated_jobs: int = Field(ge=0, le=10000)

    @field_validator("hostname")
    @classmethod
    def valid_hostname(cls, value):
        if value is not None and hostname_label(value) is None:
            raise ValueError("invalid hostname")
        return hostname_label(value)


async def local_observation(app) -> PeerObservation:
    state = app.state
    ledger = state.ledger
    decision = await state.idle_policy.decision("background", peer=True)
    mode = (await ledger.mode_state()).mode
    reason = decision.reason
    eligible = decision.eligible
    if mode != "auto":
        reason, eligible = "manual_" + mode, False
    elif ledger.settings.peer_max_concurrent == 0:
        reason, eligible = "peer_quota", False
    leases = await ledger.job_summaries()
    # Aggregate in SQLite so diagnostics never loads job inputs or results.
    journal = state.job_journal
    async with journal._lock:
        cursor = await journal._conn().execute(
            "SELECT "
            "COALESCE(SUM(origin_id = ? AND json_extract(body, '$.state') = 'queued'), 0), "
            "COALESCE(SUM(origin_id = ? AND json_extract(body, '$.state') = 'running'), 0), "
            "COALESCE(SUM(json_extract(body, '$.effective_durability') = 'replicated'), 0) "
            "FROM jobs",
            (ledger.worker_id, ledger.worker_id),
        )
        queued_jobs, running_jobs, replicated_jobs = await cursor.fetchone()
    node = state.registry.get_node(ledger.worker_id)
    models = node.ollama.models_available if node and node.ollama else []
    return PeerObservation(
        worker_id=ledger.worker_id,
        worker_incarnation=ledger.incarnation,
        hostname=getattr(state, "hostname", None),
        observed_at=time.time(),
        platform=f"{platform.system()} {platform.machine()}"[:128],
        models=[m for m in models[:256] if SAFE_NAME.fullmatch(m)],
        sensor_source=decision.signal.source,
        activity=decision.activity,
        eligibility_reason=reason if reason in REASONS else None,
        eligible=eligible,
        reserved_slots=sum(item.state == "reserved" for item in leases),
        running_slots=sum(item.state == "running" for item in leases),
        quarantined_slots=sum(item.state == "quarantined" for item in leases),
        total_slots=ledger.settings.slots,
        waiters=state.waiters.pending_count(),
        queued_jobs=queued_jobs,
        running_jobs=running_jobs,
        replicated_jobs=replicated_jobs,
    )


class PeerDiagnostics:
    def __init__(self, app, *, ttl: float = 30, interval: float = 5):
        self.app = app
        self.ttl = ttl
        self.interval = interval
        self._cache: dict[str, tuple[float, PeerObservation]] = {}
        self._semaphore = asyncio.Semaphore(8)

    def cached(self, worker_id: str, incarnation: str) -> PeerObservation | None:
        record = self._cache.get(worker_id)
        if not record:
            return None
        received, item = record
        if (
            item.worker_incarnation != incarnation
            or not 0 <= time.monotonic() - received <= self.ttl
            or not peer_observation_is_fresh(item.observed_at, self.ttl)
        ):
            return None
        return item

    async def _fetch(self, descriptor):
        async with self._semaphore:
            if not await self.app.state.ledger.cluster_participating():
                return
            identity = self.app.state.identity
            body = b"{}"
            headers = identity.sign_request("POST", PATH, body)
            # Older clients keep receiving the original strict status schema.
            # Older servers ignore this optional header and return no hostname.
            headers["x-idle-status-hostname"] = "1"
            async with self.app.state.peer_client.stream(
                "POST", descriptor.endpoint + PATH, content=body, headers=headers, timeout=5
            ) as response:
                content = bytearray()
                async for chunk in response.aiter_bytes():
                    content.extend(chunk)
                    if len(content) > MAX_BODY:
                        raise ValueError("diagnostic response too large")
                response.raise_for_status()
                identity.verify_response(
                    headers["x-idle-nonce"],
                    response.status_code,
                    bytes(content),
                    response.headers,
                    descriptor.worker_id,
                )
            result = PeerObservation.model_validate_json(content)
            if (
                result.worker_id != descriptor.worker_id
                or result.worker_incarnation != descriptor.incarnation
            ):
                raise ValueError("diagnostic response binding mismatch")
            if not peer_observation_is_fresh(result.observed_at, self.ttl):
                raise ValueError("diagnostic response stale")
            if await self.app.state.ledger.cluster_participating():
                remember = getattr(self.app.state.directory, "remember_hostname", None)
                if result.hostname and remember is not None:
                    await remember(descriptor.worker_id, result.hostname)
                self._cache[descriptor.worker_id] = (time.monotonic(), result)

    async def refresh(self):
        directory = self.app.state.directory
        if directory is None or not await self.app.state.ledger.cluster_participating():
            self._cache.clear()
            return
        peers = {
            p.worker_id: p.descriptor
            for p in directory.snapshot()
            if p.reachable and p.worker_id and p.descriptor
        }
        self._cache = {key: value for key, value in self._cache.items() if key in peers}

        async def collect(descriptor):
            try:
                await asyncio.wait_for(self._fetch(descriptor), timeout=6)
            except (httpx.HTTPError, OSError, ValueError, TimeoutError):
                self._cache.pop(descriptor.worker_id, None)

        await asyncio.gather(*(collect(d) for d in peers.values()))

    async def run(self):
        while True:
            try:
                await self.refresh()
            except Exception as exc:
                # Only the exception type is logged; peer data may contain private text.
                self._cache.clear()
                log.warning("Peer diagnostics refresh failed: %s", type(exc).__name__)
            await asyncio.sleep(self.interval)
