"""Versioned, redacted local owner control contracts."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from fleet_manager.peer.coordination import CoordinationStatus
from fleet_manager.peer.models import IdleMode


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class ModeUpdate(Strict):
    mode: Literal["auto", "draining", "offline", "paused"]
    duration_seconds: float | None = Field(default=None, ge=1, le=604800)
    expires_at: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_expiry(self):
        import time

        if self.duration_seconds is not None and self.expires_at is not None:
            raise ValueError("choose duration_seconds or expires_at")
        if self.mode == "auto" and (
            self.duration_seconds is not None or self.expires_at is not None
        ):
            raise ValueError("auto cannot have an expiry")
        if (
            self.expires_at is not None
            and not time.time() < self.expires_at <= time.time() + 604800
        ):
            raise ValueError("expiry must be within seven days")
        return self


class ModeState(Strict):
    mode: Literal["auto", "draining", "offline", "paused"]
    expires_at: float | None = None
    revision: int = Field(ge=0)


class PreferencesValues(Strict):
    interactive_policy: IdleMode
    background_policy: IdleMode
    idle_seconds: float = Field(ge=0, le=86400)
    idle_hysteresis_seconds: float = Field(ge=0, le=3600)
    cpu_busy_percent: float = Field(gt=0, le=100)
    cpu_idle_percent: float = Field(ge=0, le=100)
    gpu_busy_percent: float = Field(gt=0, le=100)
    battery_floor_percent: float = Field(ge=0, le=100)
    require_ac: bool
    reserve_memory_gb: float = Field(ge=0)
    quiet_hours_enabled: bool = False
    quiet_hours_start_minute: int = Field(default=1320, ge=0, le=1439)
    quiet_hours_end_minute: int = Field(default=420, ge=0, le=1439)
    peer_max_concurrent: int = Field(default=128, ge=0, le=128)
    notifications_enabled: bool = False
    hide_job_counts: bool = False


class PreferencesPatch(Strict):
    interactive_policy: IdleMode | None = None
    background_policy: IdleMode | None = None
    idle_seconds: float | None = Field(default=None, ge=0, le=86400)
    idle_hysteresis_seconds: float | None = Field(default=None, ge=0, le=3600)
    cpu_busy_percent: float | None = Field(default=None, gt=0, le=100)
    cpu_idle_percent: float | None = Field(default=None, ge=0, le=100)
    gpu_busy_percent: float | None = Field(default=None, gt=0, le=100)
    battery_floor_percent: float | None = Field(default=None, ge=0, le=100)
    require_ac: bool | None = None
    reserve_memory_gb: float | None = Field(default=None, ge=0)
    quiet_hours_enabled: bool | None = None
    quiet_hours_start_minute: int | None = Field(default=None, ge=0, le=1439)
    quiet_hours_end_minute: int | None = Field(default=None, ge=0, le=1439)
    peer_max_concurrent: int | None = Field(default=None, ge=0, le=128)
    notifications_enabled: bool | None = None
    hide_job_counts: bool | None = None


class PreferencesUpdate(Strict):
    expected_version: int = Field(ge=1)
    values: PreferencesPatch


class PreferencesState(Strict):
    version: int = Field(ge=1)
    values: PreferencesValues


class IdleStatus(Strict):
    workload: str
    policy: str
    eligible: bool
    reason: str | None
    state: str
    activity: str
    observed_at: float


class SlotStatus(Strict):
    total: int
    occupied: int
    reserved: int
    running: int
    quarantined: int


class PeerCounts(Strict):
    known: int
    reachable: int
    unreachable: int
    stale: int
    observed_available_slots: int | None = None


class JobCounts(Strict):
    queued: int
    running: int
    total: int


class ResourceStatus(Strict):
    cpu_percent: float | None
    gpu_percent: float | None
    memory_available_gb: float | None
    memory_total_gb: float | None
    memory_pressure: str
    on_ac: bool | None
    battery_percent: float | None
    thermal_state: str
    observed_at: float
    source: str


class BackendStatus(Strict):
    state: Literal["online", "offline", "unknown"]
    models_available: list[str]
    models_loaded: list[str]
    observed_at: float | None


class NodeStatus(Strict):
    hostname: str | None = Field(default=None, max_length=253)
    version: Literal[1] = 1
    worker_id: str
    mode: ModeState
    observed_at: float
    idle: IdleStatus
    resources: ResourceStatus
    backend: BackendStatus
    slots: SlotStatus
    peers: PeerCounts
    jobs: JobCounts
    worker_incarnation: str | None = None
    platform: str = "unknown"
    waiters: int = 0
    peer_details: list["PeerDetail"] = Field(default_factory=list)
    job_history: list["JobSummary"] = Field(default_factory=list)
    worker_leases: list["WorkerLease"] = Field(default_factory=list)
    notifications_enabled: bool = False
    hide_job_counts: bool = False
    transfers: list["TransferSummary"] = Field(default_factory=list)
    model_inventory: list["InventorySummary"] = Field(default_factory=list)
    coordination: CoordinationStatus = Field(default_factory=CoordinationStatus)
    service: Literal["running"] = "running"


class TransferSummary(Strict):
    transfer_id: str
    digest: str
    manifest_digest: str
    model_id: str
    source_worker_id: str
    state: str
    completed_bytes: int
    total_bytes: int
    reason: str | None = None


class InventorySummary(Strict):
    digest: str
    model_id: str
    size_bytes: int
    format: str


class JobSummary(Strict):
    origin_worker_id: str | None = None
    origin_host: str | None = Field(default=None, max_length=253)
    execution_host: str | None = Field(default=None, max_length=253)
    job_id: str
    state: str
    model: str
    created_at: float
    updated_at: float
    cancel_requested: bool
    worker_id: str | None = None
    effective_durability: str | None = None
    durable_copies: int = 0
    attempt_count: int = 0
    possible_duplicates: bool = False


class PeerDetail(Strict):
    hostname: str | None = Field(default=None, max_length=253)
    worker_id: str | None
    local: bool = False
    reachable: bool
    incarnation: str | None = None
    platform: str = "unknown"
    backend: str = "unknown"
    models: list[str] = Field(default_factory=list)
    observed_at: float | None = None
    reason: str | None = None
    diagnostics_observed_at: float | None = None
    sensor_source: str | None = None
    activity: str | None = None
    eligible: bool | None = None
    eligibility_reason: str | None = None
    slots: SlotStatus | None = None
    waiters: int | None = None
    queued_jobs: int | None = None
    running_jobs: int | None = None
    replicated_jobs: int | None = None


class DiagnosticCheck(Strict):
    code: str
    title: str
    state: Literal["ok", "attention", "unknown"]
    detail: str
    remedy: str | None = None


class ModelFit(Strict):
    model: str
    eligible: bool
    reason: str | None
    observed_available_slots: int
    observed_at: float


class WorkerLease(Strict):
    origin_worker_id: str | None = None
    origin_host: str | None = Field(default=None, max_length=253)
    quarantine_recovery: Literal["awaiting_backend", "manual_required"] | None = None
    quarantine_recovery_deadline: float | None = None
    job_id: str
    state: str
    model: str
    workload: str
    peer_origin: bool
    created_at: float
    updated_at: float
    lease_id: str | None = None
    request_id_hash: str | None = None
    worker_incarnation: str | None = None
    slot_generation: int | None = None


class JobsResponse(Strict):
    version: Literal[1] = 1
    jobs: list[JobSummary]
    worker_leases: list[WorkerLease] = Field(default_factory=list)


class Diagnostics(Strict):
    version: Literal[1] = 1
    service: Literal["running"] = "running"
    peer_discovery: Literal["enabled", "disabled"]
    peer_error: bool
    quarantined_slots: int
    platform: str = "unknown"
    worker_id: str | None = None
    worker_incarnation: str | None = None
    observed_at: float = 0
    checks: list[DiagnosticCheck] = Field(default_factory=list)
    model_fit: ModelFit | None = None


class Event(Strict):
    version: Literal[1] = 1
    id: str
    type: Literal[
        "snapshot", "heartbeat", "mode", "job", "capacity", "idle", "peer", "transfer", "model"
    ]
    scope: Literal["local"] = "local"
    observed_at: float
    data: NodeStatus


class ServiceUpdate(Strict):
    enabled: bool


class ServiceState(Strict):
    version: Literal[1] = 1
    supported: bool
    platform: Literal["macos", "linux", "windows", "unsupported"]
    installed: bool | None
    enabled: bool | None
    running: bool | None
