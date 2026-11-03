"""Worker admission and bounded origin-local capacity contracts."""

from __future__ import annotations

from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from fleet_manager.models.request import normalize_model_name
from fleet_manager.peer.sensors import IdleSignal

Workload = Literal["interactive", "background"]
IdleMode = Literal["always", "idle_only", "utilization_only", "manual", "never"]


class PeerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="IDLE_HERD_", allow_inf_nan=False)

    data_dir: str = "~/.idle-herd"
    backend_url: str = "http://127.0.0.1:11434"
    slots: int = Field(default=1, ge=1, le=128)
    model_slots: dict[str, int] = Field(default_factory=dict)
    # Conservative cold-weight multiplier plus per-admission KV/runtime allowance.
    weight_multiplier: float = Field(default=1.2, ge=1)
    request_memory_gb: float = Field(default=1, gt=0)
    reserve_memory_gb: float = Field(default=2, ge=0)
    interactive_policy: IdleMode = "always"
    background_policy: IdleMode = "idle_only"
    idle_seconds: float = Field(default=120, ge=0, le=86400)
    idle_hysteresis_seconds: float = Field(default=15, ge=0, le=3600)
    cpu_busy_percent: float = Field(default=25, gt=0, le=100)
    cpu_idle_percent: float = Field(default=20, ge=0, le=100)
    gpu_busy_percent: float = Field(default=25, gt=0, le=100)
    battery_floor_percent: float = Field(default=30, ge=0, le=100)
    require_ac: bool = False
    quiet_hours_enabled: bool = False
    quiet_hours_start_minute: int = Field(default=1320, ge=0, le=1439)
    quiet_hours_end_minute: int = Field(default=420, ge=0, le=1439)
    peer_max_concurrent: int = Field(default=128, ge=0, le=128)
    notifications_enabled: bool = False
    hide_job_counts: bool = False
    sensor_max_age: float = Field(default=5, ge=0.1, le=60)
    sensor_interval: float = Field(default=2, ge=0.1, le=30)
    sensor_helper: str | None = None

    @model_validator(mode="after")
    def sensor_thresholds(self):
        from pathlib import Path

        if self.cpu_idle_percent > self.cpu_busy_percent:
            raise ValueError("cpu_idle_percent must not exceed cpu_busy_percent")
        if self.sensor_interval >= self.sensor_max_age:
            raise ValueError("sensor_interval must be less than sensor_max_age")
        if self.sensor_helper is not None and not Path(self.sensor_helper).is_absolute():
            raise ValueError("sensor_helper must be an absolute executable path")
        return self

    # Native daemons discover LAN peers by default. Restricted deployments can
    # disable automatic trust independently of Bonjour or disable both entirely.
    mdns_enabled: bool = True
    lan_auto_trust: bool = True
    mdns_service_type: str = Field(
        default="_idle-herd._tcp.local.", pattern=r"^_[a-z][a-z0-9-]{0,13}\._tcp\.local\.$"
    )
    # An unset endpoint uses an OS-assigned peer port, outside the backend map.
    peer_endpoint: str | None = None
    seeds: list[str] = Field(default_factory=list, max_length=128)
    peer_interval: float = Field(default=5, ge=0.1, le=300)
    peer_ttl: float = Field(default=30, ge=0.2, le=600)
    max_waiters: int = Field(default=256, ge=1, le=10000)
    # Separately activated admin sidecar. Never consulted by admission/execution.
    raft_url: str | None = None
    raft_token_file: str | None = None

    @field_validator("raft_url")
    @classmethod
    def loopback_raft(cls, value: str | None) -> str | None:
        from urllib.parse import urlsplit

        if value is None:
            return None
        url = urlsplit(value)
        if (
            url.scheme != "http"
            or url.hostname not in {"127.0.0.1", "::1"}
            or url.username is not None
            or url.password is not None
            or url.path not in {"", "/"}
            or url.query
            or url.fragment
            or not url.port
            or 11434 <= url.port <= 11449
        ):
            raise ValueError("raft_url must be explicit loopback HTTP on a non-backend port")
        return value.rstrip("/")

    @field_validator("raft_token_file")
    @classmethod
    def absolute_raft_token(cls, value: str | None) -> str | None:
        from pathlib import Path

        if value is not None and not Path(value).is_absolute():
            raise ValueError("raft_token_file must be an absolute path")
        return value

    model_transfer_bytes_per_second: int = Field(default=10 * 1024 * 1024, ge=1, le=1024**3)
    max_model_cache_bytes: int = Field(default=20 * 1024**3, ge=1)
    model_reserved_disk_bytes: int = Field(default=1024**3, ge=0)

    @field_validator("peer_endpoint")
    @classmethod
    def peer_address(cls, value: str | None) -> str | None:
        from fleet_manager.peer.discovery import validate_endpoint

        if value is None:
            return None
        endpoint = validate_endpoint(value)
        from urllib.parse import urlsplit

        if 11434 <= urlsplit(endpoint).port <= 11449:
            raise ValueError("peer listener must use an explicitly allocated non-backend port")
        return endpoint

    @field_validator("seeds")
    @classmethod
    def seed_addresses(cls, value: list[str]) -> list[str]:
        from fleet_manager.peer.discovery import validate_endpoint

        return [validate_endpoint(endpoint) for endpoint in value]

    @field_validator("backend_url")
    @classmethod
    def loopback_backend(cls, value: str) -> str:
        from urllib.parse import urlsplit

        url = urlsplit(value)
        if (
            url.scheme != "http"
            or url.hostname not in {"127.0.0.1", "localhost", "::1"}
            or url.username
            or url.password
            or url.path not in {"", "/"}
            or url.query
            or url.fragment
            or url.port == 11435
        ):
            raise ValueError("Phase 1 backend must be a local HTTP backend, not ingress :11435")
        return value.rstrip("/")

    @field_validator("model_slots")
    @classmethod
    def model_limits(cls, value: dict[str, int]) -> dict[str, int]:
        if any(limit < 1 for limit in value.values()):
            raise ValueError("model slot limits must be positive")
        return {normalize_model_name(model): limit for model, limit in value.items()}


class AcquireRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model: str = Field(min_length=1, max_length=512)
    request_id: str = Field(default_factory=lambda: str(uuid4()), min_length=1, max_length=128)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)
    ttl_ms: int = Field(default=30000, ge=100, le=120000)
    slots: Literal[1] = 1
    wait_ms: int = Field(default=60000, ge=0, le=120000)
    workload: Workload = "interactive"
    idle_only: bool = False

    @field_validator("model")
    @classmethod
    def normalize(cls, value: str) -> str:
        return normalize_model_name(value)


class ResourceFit(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    model: str
    required_gb: float = Field(gt=0)
    available_gb: float = Field(ge=0)


class Lease(BaseModel):
    reservation_id: str
    token: str
    cluster_id: str
    worker_id: str
    worker_incarnation: str
    slot_id: int
    slot_generation: int
    request_id: str
    model: str
    expires_at: float
    ttl_ms: int
    state: Literal["reserved", "running", "released", "expired", "quarantined"]
    memory_gb: float
    workload: Workload = "interactive"
    idle_only: bool = False
    peer_origin: bool = False
    created_at: float = 0
    updated_at: float = 0


class EligibilityDecision(BaseModel):
    workload: Workload
    policy: IdleMode
    eligible: bool
    reason: str | None = None
    state: Literal["eligible", "draining", "ineligible", "unknown"]
    activity: Literal["idle", "active", "unknown"]
    signal: IdleSignal
    observed_at: float


class Capacity(BaseModel):
    scope: Literal["local_worker"] = "local_worker"
    globally_authoritative: Literal[False] = False
    quorum: None = None
    discovery_state: Literal["disabled"] = "disabled"
    worker_id: str
    worker_incarnation: str
    model: str
    total_slots: int
    observed_available_slots: int
    quarantined_slots: int
    eligible: bool
    reason: str | None = None
    observed_at: float
    eligibility: EligibilityDecision | None = None


class AdmissionError(Exception):
    def __init__(self, reason: str, status: int = 409, details: dict[str, int] | None = None):
        super().__init__(reason)
        self.reason = reason
        self.status = status
        self.details = details
