"""Versioned, redacted local owner control contracts."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

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
    transfers: list["TransferSummary"] = Field(default_factory=list)
    model_inventory: list["InventorySummary"] = Field(default_factory=list)
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
    job_id: str
    state: str
    model: str
    created_at: float
    updated_at: float
    cancel_requested: bool
    worker_id: str | None = None


class WorkerLease(Strict):
    job_id: str
    state: str
    model: str
    workload: str
    peer_origin: bool
    created_at: float
    updated_at: float


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
    platform: Literal["macos", "unsupported"]
    installed: bool | None
    enabled: bool | None
    running: bool | None
