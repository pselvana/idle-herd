"""Phase 1 local worker contracts (cluster-wide waits arrive in Phase 4)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from fleet_manager.models.request import normalize_model_name


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
    # Peer transport is opt-in and uses an explicitly allocated, separate port.
    peer_endpoint: str | None = None
    seeds: list[str] = Field(default_factory=list, max_length=128)
    peer_interval: float = Field(default=5, ge=0.1, le=300)
    peer_ttl: float = Field(default=30, ge=0.2, le=600)

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
    request_id: str = Field(min_length=1, max_length=128)
    ttl_ms: int = Field(default=30000, ge=100, le=120000)
    slots: Literal[1] = 1
    wait_ms: Literal[0] = 0

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


class AdmissionError(Exception):
    def __init__(self, reason: str, status: int = 409):
        super().__init__(reason)
        self.reason = reason
        self.status = status
