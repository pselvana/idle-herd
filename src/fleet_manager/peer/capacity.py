"""Origin-local, advisory capacity observations from authenticated workers."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from fleet_manager.models.request import normalize_model_name
from fleet_manager.peer.discovery import MAX_BODY
from fleet_manager.peer.models import Capacity, EligibilityDecision, Workload


class CapacityQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    model: str = Field(min_length=1, max_length=512)
    workload: Workload = "interactive"
    idle_only: bool = False


class PeerCapacity(BaseModel):
    node_id: str
    reachable: bool
    fresh: bool
    observed_at: datetime | None
    available_slots: int | None = None
    eligible: bool | None = None
    reason: str | None = None


class ClusterCapacity(BaseModel):
    cluster_id: str
    model: str
    scope: Literal["reachable_peers"] = "reachable_peers"
    observed_at: datetime
    stale_after_ms: int
    known_nodes: int
    reachable_nodes: int
    stale_nodes: int
    offline_nodes: int
    eligible_reachable_workers: int
    observed_available_slots: int
    observed_total_eligible_slots: int
    busy: bool | None
    globally_authoritative: Literal[False] = False
    discovery_state: Literal["complete_known_view", "partial", "unknown"]
    quorum: bool | None = None
    peers: list[PeerCapacity]
    # Local fields preserve the Phase 1 response's useful compatibility surface.
    worker_id: str
    worker_incarnation: str
    total_slots: int
    quarantined_slots: int
    eligible: bool
    reason: str | None
    eligibility: EligibilityDecision | None
    next_check_ms: int | None = None
    no_reachable_eligible_workers: bool = False


async def _remote(app, descriptor, query: CapacityQuery) -> Capacity:
    identity = app.state.identity
    path = "/peer/v1/capacity"
    body = query.model_dump_json().encode()
    headers = identity.sign_request("POST", path, body)
    async with app.state.peer_client.stream(
        "POST", descriptor.endpoint + path, content=body, headers=headers, timeout=5
    ) as response:
        content = bytearray()
        async for chunk in response.aiter_bytes():
            content.extend(chunk)
            if len(content) > MAX_BODY:
                raise ValueError("capacity response too large")
        content = bytes(content)
        response.raise_for_status()
        identity.verify_response(
            headers["x-idle-nonce"],
            response.status_code,
            content,
            response.headers,
            descriptor.worker_id,
        )
    result = Capacity.model_validate_json(content)
    if (
        result.worker_id != descriptor.worker_id
        or result.worker_incarnation != descriptor.incarnation
        or result.model != query.model
    ):
        raise ValueError("capacity response binding mismatch")
    return result


async def cluster_capacity(
    local_app, model: str, workload: Workload = "interactive", idle_only: bool = False
) -> ClusterCapacity:
    model = normalize_model_name(model)
    local = await local_app.state.ledger.capacity(model, workload, idle_only)
    directory = local_app.state.directory
    snapshots = directory.snapshot() if directory else []
    participating = await local_app.state.ledger.cluster_participating()
    # A worker may move endpoints while its old discovery record remains. Choose
    # one direct observation per identity; unknown endpoints remain distinct.
    unique = {}
    for status in snapshots:
        key = ("worker", status.worker_id) if status.worker_id else ("endpoint", status.endpoint)
        previous = unique.get(key)
        rank = (status.reachable, status.observed_at or 0)
        if previous is None or rank > (previous.reachable, previous.observed_at or 0):
            unique[key] = status
    statuses = list(unique.values())
    query = CapacityQuery(model=model, workload=workload, idle_only=idle_only)
    candidates = [s for s in statuses if participating and s.reachable and s.descriptor is not None]
    results = await asyncio.gather(
        *(asyncio.wait_for(_remote(local_app, s.descriptor, query), timeout=5) for s in candidates),
        return_exceptions=True,
    )
    remote = dict(zip((s.endpoint for s in candidates), results, strict=True))
    stale_after = directory.ttl if directory else 5
    local_fresh = 0 <= time.time() - local.observed_at <= stale_after
    peers = [
        PeerCapacity(
            node_id=local.worker_id,
            reachable=True,
            fresh=local_fresh,
            observed_at=datetime.fromtimestamp(local.observed_at, UTC),
            available_slots=(local.observed_available_slots if local.eligible else 0)
            if local_fresh
            else None,
            eligible=local.eligible if local_fresh else None,
            reason=local.reason,
        )
    ]
    available = local.observed_available_slots if local_fresh and local.eligible else 0
    total = local.total_slots if local_fresh and local.eligible else 0
    eligible = int(local_fresh and local.eligible)
    stale, offline = int(not local_fresh), 0
    for status in statuses:
        result = remote.get(status.endpoint)
        valid = (
            isinstance(result, Capacity) and 0 <= time.time() - result.observed_at <= directory.ttl
        )
        if valid:
            available += result.observed_available_slots if result.eligible else 0
            total += result.total_slots if result.eligible else 0
            eligible += int(result.eligible)
        else:
            if status.reachable:
                stale += 1
            else:
                offline += 1
        peers.append(
            PeerCapacity(
                node_id=status.worker_id or status.endpoint,
                reachable=valid,
                fresh=valid,
                observed_at=datetime.fromtimestamp(result.observed_at, UTC) if valid else None,
                available_slots=(result.observed_available_slots if result.eligible else 0)
                if valid
                else None,
                eligible=result.eligible if valid else None,
                reason=result.reason if valid else status.reason or "capacity unavailable",
            )
        )
    partial = bool(stale or offline)
    cluster_id = getattr(local_app.state.ledger, "cluster_id", None) or getattr(
        getattr(local_app.state, "identity", None), "cluster_id", "local"
    )
    return ClusterCapacity(
        quorum=(
            local_app.state.coordination.snapshot().quorum
            if getattr(local_app.state, "coordination", None)
            else None
        ),
        cluster_id=cluster_id,
        model=model,
        observed_at=datetime.now(UTC),
        stale_after_ms=int(stale_after * 1000),
        known_nodes=len(peers),
        reachable_nodes=sum(p.reachable for p in peers),
        stale_nodes=stale,
        offline_nodes=offline,
        eligible_reachable_workers=eligible,
        observed_available_slots=available,
        observed_total_eligible_slots=total,
        busy=False if available else (True if eligible and not partial else None),
        next_check_ms=int(directory.interval * 1000) if directory and not eligible else None,
        no_reachable_eligible_workers=eligible == 0,
        discovery_state="partial" if partial else "complete_known_view",
        peers=peers,
        worker_id=local.worker_id,
        worker_incarnation=local.worker_incarnation,
        total_slots=local.total_slots,
        quarantined_slots=local.quarantined_slots,
        eligible=local.eligible,
        reason=local.reason,
        eligibility=local.eligibility,
    )
