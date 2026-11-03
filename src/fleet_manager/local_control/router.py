"""Authenticated local owner API. Mount only on the loopback ingress app."""

from __future__ import annotations

import asyncio
import platform
import subprocess
import time
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from fleet_manager.local_control import service
from fleet_manager.local_control.auth import authorize
from fleet_manager.local_control.diagnostics import diagnose
from fleet_manager.local_control.diagnostics import redacted_model_name as _model_name
from fleet_manager.local_control.model_sharing import transfer_summaries
from fleet_manager.local_control.models import (
    BackendStatus,
    Diagnostics,
    Event,
    IdleStatus,
    JobCounts,
    JobsResponse,
    JobSummary,
    ModeState,
    ModeUpdate,
    NodeStatus,
    PeerCounts,
    PeerDetail,
    PreferencesState,
    PreferencesUpdate,
    ResourceStatus,
    ServiceState,
    ServiceUpdate,
    SlotStatus,
    WorkerLease,
)
from fleet_manager.peer.coordination import (
    CoordinationStatus,
    CoordinationUnavailable,
    MetadataUpdate,
    MetadataView,
)
from fleet_manager.peer.jobs import JobConflict


def create_router() -> APIRouter:
    def require_read(request: Request):
        authorize(request)

    service_lock = asyncio.Lock()
    router = APIRouter(
        prefix="/v1/node", tags=["local owner controls"], dependencies=[Depends(require_read)]
    )

    async def summaries(request: Request) -> JobsResponse:
        records = await request.app.state.job_journal.list_jobs()
        return JobsResponse(
            jobs=[
                JobSummary(
                    job_id=record.job_id,
                    state=record.state,
                    model=_model_name(record.submission.model),
                    created_at=record.created_at,
                    updated_at=record.updated_at,
                    cancel_requested=record.cancel_requested,
                    worker_id=record.attempts[-1].worker_id if record.attempts else None,
                    effective_durability=record.effective_durability,
                    durable_copies=(1 + len(record.acks)) if record.accepted else 0,
                    attempt_count=len(record.attempts),
                    possible_duplicates=record.submission.retry_policy.allow_possible_duplicates,
                )
                for record in records
                if record.origin_id == request.app.state.ledger.worker_id
            ],
            worker_leases=[
                WorkerLease.model_validate(item).model_copy(
                    update={"model": _model_name(WorkerLease.model_validate(item).model)}
                )
                for item in await request.app.state.ledger.job_summaries()
            ],
        )

    async def snapshot(request: Request) -> NodeStatus:
        state = request.app.state
        mode = ModeState.model_validate(await state.ledger.mode_state())
        jobs = await summaries(request)
        leases = jobs.worker_leases
        decision = await state.idle_policy.decision("background", peer=True)
        peers = state.directory.snapshot() if state.directory else []
        node = state.registry.get_node(state.ledger.worker_id)
        signal = decision.signal
        settings = state.ledger.settings
        if mode.mode != "auto":
            decision = decision.model_copy(
                update={
                    "eligible": False,
                    "reason": "manual_" + mode.mode,
                    "state": "ineligible",
                }
            )
        elif getattr(settings, "peer_max_concurrent", 128) == 0:
            decision = decision.model_copy(
                update={"eligible": False, "reason": "peer_quota", "state": "ineligible"}
            )
        backend_state = (
            "unknown" if node is None else ("offline" if node.status == "offline" else "online")
        )
        if getattr(state, "backend_reachable", None) is False:
            backend_state = "offline"
        sharing = getattr(state, "model_sharing", None)
        transfers = await transfer_summaries(sharing) if sharing else []
        inventory = await sharing.inventory() if sharing else []

        def count(name):
            return sum(
                (item.state if hasattr(item, "state") else item["state"]) == name for item in leases
            )

        observed = time.time()
        local_platform = f"{platform.system()} {platform.machine()}"
        local_models = (
            [_model_name(m) for m in node.ollama.models_available] if node and node.ollama else []
        )
        details = [
            PeerDetail(
                worker_id=state.ledger.worker_id,
                local=True,
                reachable=True,
                incarnation=getattr(state.ledger, "incarnation", None),
                platform=local_platform,
                backend="ollama",
                models=local_models,
                observed_at=node.last_heartbeat if node else None,
                reason="backend_unavailable" if backend_state != "online" else None,
                diagnostics_observed_at=observed,
                sensor_source=signal.source,
                activity=decision.activity,
                eligible=decision.eligible,
                eligibility_reason=decision.reason,
                slots=SlotStatus(
                    total=state.ledger.settings.slots,
                    occupied=len(leases),
                    reserved=count("reserved"),
                    running=count("running"),
                    quarantined=count("quarantined"),
                ),
                waiters=getattr(state, "waiters", None).pending_count()
                if getattr(state, "waiters", None)
                else 0,
                queued_jobs=sum(
                    j.state in {"queued", "reserving", "retry_pending"} for j in jobs.jobs
                ),
                running_jobs=sum(j.state == "running" for j in jobs.jobs),
                replicated_jobs=sum(j.effective_durability == "replicated" for j in jobs.jobs),
            )
        ]
        for peer in peers:
            reachable = mode.mode != "offline" and peer.reachable
            descriptor = peer.descriptor
            cache = getattr(state, "peer_diagnostics", None)
            observation = (
                cache.cached(peer.worker_id, descriptor.incarnation)
                if reachable and cache and descriptor
                else None
            )
            details.append(
                PeerDetail(
                    worker_id=peer.worker_id,
                    reachable=reachable,
                    incarnation=descriptor.incarnation if descriptor else None,
                    models=[_model_name(m) for m in observation.models]
                    if observation
                    else ([_model_name(m.name) for m in descriptor.models] if descriptor else []),
                    platform=observation.platform if observation else "unknown",
                    backend=observation.backend if observation else "unknown",
                    observed_at=peer.observed_at,
                    diagnostics_observed_at=observation.observed_at if observation else None,
                    sensor_source=observation.sensor_source if observation else None,
                    activity=observation.activity if observation else None,
                    eligible=observation.eligible if observation else None,
                    eligibility_reason=observation.eligibility_reason if observation else None,
                    slots=SlotStatus(
                        total=observation.total_slots,
                        occupied=observation.reserved_slots
                        + observation.running_slots
                        + observation.quarantined_slots,
                        reserved=observation.reserved_slots,
                        running=observation.running_slots,
                        quarantined=observation.quarantined_slots,
                    )
                    if observation
                    else None,
                    waiters=observation.waiters if observation else None,
                    queued_jobs=observation.queued_jobs if observation else None,
                    running_jobs=observation.running_jobs if observation else None,
                    replicated_jobs=observation.replicated_jobs if observation else None,
                    reason=None
                    if reachable
                    else ("manual_offline" if mode.mode == "offline" else "unreachable_or_stale"),
                )
            )
        waiters = getattr(state, "waiters", None)
        # Only sum fresh, authenticated observations. A missing observation must
        # not turn a partial view into a claim of zero available remote capacity.
        reachable_details = [item for item in details if not item.local and item.reachable]
        remote_slots = (
            sum(
                max(0, item.slots.total - item.slots.occupied) if item.eligible else 0
                for item in reachable_details
                if item.slots is not None
            )
            if all(
                item.slots is not None and item.eligible is not None for item in reachable_details
            )
            else None
        )

        return NodeStatus(
            worker_id=state.ledger.worker_id,
            mode=mode,
            observed_at=observed,
            worker_incarnation=getattr(state.ledger, "incarnation", None),
            platform=local_platform,
            peer_details=details,
            waiters=waiters.pending_count() if waiters else 0,
            job_history=sorted(jobs.jobs, key=lambda item: item.updated_at, reverse=True)[:50],
            worker_leases=leases,
            notifications_enabled=getattr(settings, "notifications_enabled", False),
            hide_job_counts=getattr(settings, "hide_job_counts", False),
            idle=IdleStatus(
                **decision.model_dump(
                    include={
                        "workload",
                        "policy",
                        "eligible",
                        "reason",
                        "state",
                        "activity",
                        "observed_at",
                    }
                )
            ),
            resources=ResourceStatus(
                cpu_percent=signal.cpu_percent,
                gpu_percent=signal.gpu_percent,
                memory_available_gb=signal.memory_available_gb,
                memory_total_gb=(
                    signal.memory_total_gb
                    if signal.memory_total_gb is not None
                    else (node.memory.total_gb if node and node.memory else None)
                ),
                memory_pressure=signal.memory_pressure,
                on_ac=signal.on_ac,
                battery_percent=signal.battery_percent,
                thermal_state=signal.thermal_state,
                observed_at=signal.observed_at,
                source=signal.source,
            ),
            backend=BackendStatus(
                state=backend_state,
                models_available=local_models,
                models_loaded=[_model_name(m.name) for m in node.ollama.models_loaded]
                if node and node.ollama
                else [],
                observed_at=getattr(
                    state, "backend_observed_at", node.last_heartbeat if node else None
                ),
            ),
            slots=SlotStatus(
                total=state.ledger.settings.slots,
                occupied=len(leases),
                reserved=count("reserved"),
                running=count("running"),
                quarantined=count("quarantined"),
            ),
            peers=PeerCounts(
                known=len(peers),
                stale=sum(mode.mode == "offline" or not p.reachable for p in peers),
                reachable=sum(mode.mode != "offline" and p.reachable for p in peers),
                unreachable=sum(mode.mode == "offline" or not p.reachable for p in peers),
                observed_available_slots=remote_slots,
            ),
            jobs=JobCounts(
                queued=sum(j.state in {"queued", "reserving", "retry_pending"} for j in jobs.jobs),
                running=sum(j.state == "running" for j in jobs.jobs),
                total=len(jobs.jobs),
            ),
            transfers=transfers,
            model_inventory=[
                item.model_dump(include={"digest", "model_id", "size_bytes", "format"})
                for item in inventory
            ],
            coordination=(
                state.coordination.snapshot(request.app, participating=mode.mode != "offline")
                if getattr(state, "coordination", None)
                else CoordinationStatus()
            ),
        )

    @router.get("/coordination/metadata", response_model=MetadataView)
    async def metadata(request: Request):
        authorize(request)
        try:
            return await request.app.state.coordination.metadata()
        except CoordinationUnavailable as exc:
            raise HTTPException(503, "optional admin coordination unavailable") from exc

    @router.put("/coordination/metadata", response_model=MetadataView)
    async def update_metadata(spec: MetadataUpdate, request: Request):
        authorize(request, control=True)
        try:
            return await request.app.state.coordination.metadata(spec)
        except CoordinationUnavailable as exc:
            raise HTTPException(503, "optional admin coordination unavailable") from exc

    @router.get("/status", response_model=NodeStatus)
    async def status(request: Request):
        authorize(request)
        return await snapshot(request)

    @router.get("/jobs", response_model=JobsResponse)
    async def jobs(request: Request):
        authorize(request)
        return await summaries(request)

    @router.get("/peers", response_model=list[PeerDetail])
    async def peers(request: Request):
        authorize(request)
        return (await snapshot(request)).peer_details

    @router.post("/jobs/{job_id}/cancel", response_model=JobSummary)
    async def cancel(job_id: str, request: Request):
        authorize(request, control=True)
        try:
            record = await request.app.state.jobs.cancel(job_id)
        except JobConflict as exc:
            raise HTTPException(409, "job cannot be canceled") from exc
        return JobSummary(
            job_id=record.job_id,
            state=record.state,
            model=_model_name(record.submission.model),
            created_at=record.created_at,
            updated_at=record.updated_at,
            cancel_requested=record.cancel_requested,
            worker_id=record.attempts[-1].worker_id if record.attempts else None,
        )

    @router.put("/mode", response_model=ModeState)
    async def mode(spec: ModeUpdate, request: Request):
        authorize(request, control=True)
        return await request.app.state.ledger.set_mode(spec)

    @router.get("/preferences", response_model=PreferencesState)
    async def preferences(request: Request):
        authorize(request)
        return await request.app.state.ledger.preferences()

    @router.patch("/preferences", response_model=PreferencesState)
    async def update_preferences(spec: PreferencesUpdate, request: Request):
        authorize(request, control=True)
        if any(value is None for value in spec.values.model_dump(exclude_unset=True).values()):
            raise HTTPException(422, "preference values cannot be null")
        return await request.app.state.ledger.update_preferences(spec)

    @router.get("/diagnostics", response_model=Diagnostics)
    async def diagnostics(
        request: Request, model: str | None = Query(default=None, min_length=1, max_length=512)
    ):
        authorize(request)
        status = await snapshot(request)
        return await diagnose(request.app, status, model)

    def service_state() -> ServiceState:
        platform, installed, enabled, running = service.login_state()
        return ServiceState(
            supported=platform != "unsupported",
            platform=platform,
            installed=installed,
            enabled=enabled,
            running=running,
        )

    @router.get("/service", response_model=ServiceState)
    async def get_service(request: Request):
        authorize(request)
        return await asyncio.to_thread(service_state)

    @router.put("/service", response_model=ServiceState)
    async def set_service(spec: ServiceUpdate, request: Request):
        authorize(request, control=True)
        if service.platform_name() == "unsupported":
            raise HTTPException(501, "login service is unavailable on this platform")
        async with service_lock:
            try:
                await asyncio.to_thread(
                    service.set_login_enabled,
                    spec.enabled,
                    request.app.state.ledger.settings.data_dir,
                    settings=request.app.state.ledger.settings,
                )
                return await asyncio.to_thread(service_state)
            except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
                raise HTTPException(503, "login service configuration failed") from exc

    @router.get("/events")
    async def events(request: Request):
        authorize(request)

        async def stream():
            # No replay cursor is trusted: each connection begins with a fresh snapshot.
            previous = None
            while not await request.is_disconnected():
                current = await snapshot(request)
                comparison = current.model_dump(
                    exclude={
                        "observed_at": True,
                        "idle": {"observed_at"},
                        "peer_details": {"__all__": {"diagnostics_observed_at", "observed_at"}},
                    }
                )
                kind = "snapshot" if previous is None else "heartbeat"
                if previous is not None:
                    for field, event_type in (
                        ("transfers", "transfer"),
                        ("model_inventory", "model"),
                        ("mode", "mode"),
                        ("jobs", "job"),
                        ("slots", "capacity"),
                        ("idle", "idle"),
                        ("peers", "peer"),
                        ("peer_details", "peer"),
                        ("job_history", "job"),
                        ("waiters", "capacity"),
                    ):
                        if comparison[field] != previous[field]:
                            kind = event_type
                            break
                event = Event(
                    id=str(uuid.uuid4()), type=kind, observed_at=time.time(), data=current
                )
                yield f"id: {event.id}\nevent: {kind}\ndata: {event.model_dump_json()}\n\n"
                previous = comparison
                await asyncio.sleep(1)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )

    return router
