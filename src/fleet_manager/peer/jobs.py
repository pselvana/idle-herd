"""Origin-local durable jobs and authenticated-transport-independent replica records.

This journal never owns worker leases; uncertain execution remains charged by the
worker ledger. Replicas are immutable snapshots, not a distributed lock.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Literal

import aiosqlite
from pydantic import BaseModel, ConfigDict, Field, model_validator

from fleet_manager.models.request import normalize_model_name

JobState = Literal[
    "queued",
    "reserving",
    "running",
    "retry_pending",
    "completed",
    "canceled",
    "failed",
    "unknown_outcome",
]
Durability = Literal["local", "replicated"]
PATHS = {
    "/v1/chat/completions",
    "/v1/completions",
    "/v1/embeddings",
    "/v1/messages",
    "/v1/responses",
    "/api/chat",
    "/api/generate",
    "/api/embed",
    "/api/embeddings",
}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class RetryPolicy(StrictModel):
    safe: bool = False
    max_attempts: int = Field(default=1, ge=1, le=10)
    delay: float = Field(default=30, ge=1, le=86400)
    allow_possible_duplicates: bool = False

    @model_validator(mode="after")
    def valid_retry(self):
        if self.max_attempts > 1 and (not self.safe or not self.allow_possible_duplicates):
            raise ValueError("retries require safe and allow_possible_duplicates")
        return self


class ExecutionPolicy(StrictModel):
    workload: Literal["interactive", "background"] = "interactive"
    idle_only: bool = False


def _has_actions(value: Any) -> bool:
    if isinstance(value, dict):
        if any(
            key in value
            for key in (
                "tools",
                "tool_choice",
                "functions",
                "function_call",
                "tool_calls",
                "actions",
            )
        ):
            return True
        if value.get("role") in ("tool", "function") or value.get("type") in (
            "function_call",
            "function_call_output",
            "tool_use",
            "tool_result",
            "computer_call",
            "computer_call_output",
        ):
            return True
        return any(_has_actions(item) for item in value.values())
    if isinstance(value, list):
        return any(_has_actions(item) for item in value)
    return False


class JobSubmission(StrictModel):
    model: str = Field(min_length=1, max_length=256)
    request: dict[str, Any]
    path: str = "/v1/chat/completions"
    priority: int = Field(default=0, ge=-100, le=100)
    idempotency_key: str = Field(
        default_factory=lambda: str(uuid.uuid4()), min_length=1, max_length=256
    )
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    execution_policy: ExecutionPolicy = Field(default_factory=ExecutionPolicy)
    durability: Durability = "local"
    replica_count: int = Field(default=1, ge=1, le=16)
    allow_durability_downgrade: bool = False

    @model_validator(mode="after")
    def valid_request(self):
        if self.path not in PATHS:
            raise ValueError("unsupported inference path")
        self.model = normalize_model_name(self.model)
        request_model = self.request.get("model", self.model)
        if not isinstance(request_model, str) or not request_model:
            raise ValueError("request model must be a nonempty string")
        if normalize_model_name(request_model) != self.model:
            raise ValueError("request model differs from job model")
        if "model" in self.request:
            self.request = {**self.request, "model": self.model}
        if self.request.get("stream", False):
            raise ValueError("durable jobs require non-streaming requests")
        if self.retry_policy.safe and _has_actions(self.request):
            raise ValueError("tool or function requests cannot be declared retry-safe")
        return self


class JobAttempt(StrictModel):
    attempt_id: str
    worker_id: str | None = None
    lease_id: str | None = None
    state: JobState = "reserving"
    started_at: float
    finished_at: float | None = None
    result: Any = None
    result_conflicts: list[Any] = Field(default_factory=list)
    execution_started: bool = False
    admission_only: bool = False


class JobRecord(StrictModel):
    job_id: str
    origin_id: str
    submission: JobSubmission
    state: JobState = "queued"
    accepted: bool = False
    execution_ready: bool = False
    cancel_requested: bool = False
    effective_durability: Durability | None = None
    acks: list[str] = Field(default_factory=list)
    attempts: list[JobAttempt] = Field(default_factory=list)
    created_at: float
    updated_at: float
    retry_at: float | None = None
    revision: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def valid_certificate(self):
        if len({attempt.attempt_id for attempt in self.attempts}) != len(self.attempts):
            raise ValueError("duplicate attempt ID")
        if len(set(self.acks)) != len(self.acks):
            raise ValueError("duplicate acknowledgments")
        if not self.accepted:
            if self.effective_durability is not None or self.acks or self.attempts:
                raise ValueError("unaccepted job has execution or acceptance evidence")
        elif self.effective_durability == "replicated":
            if (
                self.submission.durability != "replicated"
                or self.origin_id in self.acks
                or len(self.acks) < self.submission.replica_count
            ):
                raise ValueError("invalid replica acceptance certificate")
        elif self.effective_durability == "local":
            if (
                self.submission.durability == "replicated"
                and not self.submission.allow_durability_downgrade
            ):
                raise ValueError("unauthorized durability downgrade")
        else:
            raise ValueError("accepted job requires effective durability")
        return self

    @property
    def execution_attempt_count(self) -> int:
        return sum(not attempt.admission_only for attempt in self.attempts)

    def summary(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "origin_id": self.origin_id,
            "model": self.submission.model,
            "state": self.state,
            "accepted": self.accepted,
            "execution_ready": self.execution_ready,
            "cancel_requested": self.cancel_requested,
            "effective_durability": self.effective_durability,
            "acks": self.acks,
            "attempt_count": len(self.attempts),
            "execution_attempt_count": self.execution_attempt_count,
            "attempts": [
                {
                    "attempt_id": attempt.attempt_id,
                    "worker_id": attempt.worker_id,
                    "lease_id": attempt.lease_id,
                    "state": attempt.state,
                    "admission_only": attempt.admission_only,
                    "started_at": attempt.started_at,
                    "finished_at": attempt.finished_at,
                }
                for attempt in self.attempts
            ],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "retry_at": self.retry_at,
            "possible_duplicates": self.submission.retry_policy.allow_possible_duplicates,
        }


class ReplicaSnapshot(StrictModel):
    version: Literal[1] = 1
    record: JobRecord
    digest: str


class JobConflict(ValueError):
    pass


class JobCapacityError(ValueError):
    pass


def _json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def _digest(record: JobRecord) -> str:
    data = record.model_dump(mode="json")
    data.pop("execution_ready")
    return hashlib.sha256(_json(data).encode()).hexdigest()


class JobJournal:
    def __init__(
        self,
        path: str | Path,
        *,
        max_jobs: int = 10000,
        max_bytes: int = 64 * 1024 * 1024,
        max_record_bytes: int = 1024 * 1024,
        worker_id: str | None = None,
    ):
        self.path = Path(path).expanduser()
        self.worker_id = worker_id
        self.max_jobs, self.max_bytes, self.max_record_bytes = max_jobs, max_bytes, max_record_bytes
        self._db: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()
        self.changed = asyncio.Event()

    async def open(self):
        await asyncio.to_thread(self.path.parent.mkdir, parents=True, exist_ok=True, mode=0o700)
        self._db = await aiosqlite.connect(self.path)
        try:
            await self._db.execute("PRAGMA journal_mode=WAL")
            await self._db.execute("PRAGMA synchronous=FULL")
            await self._db.execute(
                "CREATE TABLE IF NOT EXISTS jobs (job_id TEXT PRIMARY KEY, "
                "origin_id TEXT NOT NULL, idem TEXT NOT NULL, body TEXT NOT NULL, "
                "UNIQUE(origin_id, idem))"
            )
            await self._db.commit()
            async with self._lock:
                rows = await (await self._db.execute("SELECT body FROM jobs")).fetchall()
                for (body,) in rows:
                    record = JobRecord.model_validate_json(body)
                    if record.state in ("reserving", "running"):
                        attempt = record.attempts[-1] if record.attempts else None
                        local_pending = (
                            self.worker_id is not None
                            and record.origin_id == self.worker_id
                            and record.execution_ready
                            and not record.cancel_requested
                            and record.state == "reserving"
                            and attempt is not None
                            and attempt.state == "reserving"
                            and not attempt.execution_started
                        )
                        if local_pending:
                            # Running is committed before dispatch; this never crossed it.
                            attempt.state = "canceled"
                            attempt.admission_only = True
                            attempt.finished_at = time.time()
                            record.state = "queued"
                        else:
                            # A crash never proves that started backend execution stopped.
                            record.state = "unknown_outcome"
                            if attempt and attempt.state in ("reserving", "running"):
                                attempt.state = "unknown_outcome"
                                attempt.finished_at = time.time()
                        await self._save(record)
                await self._db.commit()
        except BaseException:
            try:
                await self._db.rollback()
            finally:
                await self._db.close()
                self._db = None
            raise
        return self

    async def close(self):
        if self._db is not None:
            await self._db.close()
            self._db = None

    def _conn(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("journal is not open")
        return self._db

    async def _get(self, job_id: str) -> JobRecord | None:
        row = await (
            await self._conn().execute("SELECT body FROM jobs WHERE job_id=?", (job_id,))
        ).fetchone()
        return JobRecord.model_validate_json(row[0]) if row else None

    async def get(self, job_id: str) -> JobRecord | None:
        async with self._lock:
            return await self._get(job_id)

    async def _save(self, record: JobRecord):
        db = self._conn()
        record.updated_at = time.time()
        record.revision += 1
        body = _json(record.model_dump(mode="json"))
        size = len(body.encode())
        row = await (
            await db.execute(
                "SELECT COUNT(*), COALESCE(SUM(length(CAST(body AS BLOB))),0) "
                "FROM jobs WHERE job_id != ?",
                (record.job_id,),
            )
        ).fetchone()
        if (
            size > self.max_record_bytes
            or row[0] + 1 > self.max_jobs
            or row[1] + size > self.max_bytes
        ):
            raise JobCapacityError("job journal budget exceeded")
        await db.execute(
            "INSERT INTO jobs VALUES (?,?,?,?) ON CONFLICT(job_id) "
            "DO UPDATE SET body=excluded.body",
            (record.job_id, record.origin_id, record.submission.idempotency_key, body),
        )

    async def submit(self, spec: JobSubmission, origin_id: str) -> tuple[JobRecord, bool]:
        async with self._lock:
            db = self._conn()
            row = await (
                await db.execute(
                    "SELECT body FROM jobs WHERE origin_id=? AND idem=?",
                    (origin_id, spec.idempotency_key),
                )
            ).fetchone()
            if row:
                prior = JobRecord.model_validate_json(row[0])
                if prior.submission != spec:
                    raise JobConflict("idempotency key already used with different submission")
                return prior, False
            now = time.time()
            record = JobRecord(
                job_id=str(uuid.uuid4()),
                origin_id=origin_id,
                submission=spec,
                created_at=now,
                updated_at=now,
            )
            try:
                await self._save(record)
                await db.commit()
            except BaseException:
                await db.rollback()
                raise
            self.changed.set()
            return record, True

    async def accept(
        self, job_id: str, acks: list[str], effective_durability: Durability
    ) -> JobRecord:
        async with self._lock:
            record = await self._required(job_id)
            unique = sorted(set(acks))
            if record.accepted:
                if record.acks != unique or record.effective_durability != effective_durability:
                    raise JobConflict("acceptance is immutable")
                return record
            spec = record.submission
            if effective_durability == "replicated" and (
                spec.durability != "replicated"
                or len(unique) < spec.replica_count
                or record.origin_id in unique
            ):
                raise JobConflict("insufficient remote acknowledgments")
            if (
                effective_durability == "local"
                and spec.durability == "replicated"
                and not spec.allow_durability_downgrade
            ):
                raise JobConflict("durability downgrade not permitted")
            record.accepted, record.acks, record.effective_durability = (
                True,
                unique,
                effective_durability,
            )
            record.execution_ready = effective_durability == "local"
            return await self._commit(record)

    async def _required(self, job_id: str) -> JobRecord:
        record = await self._get(job_id)
        if record is None:
            raise KeyError(job_id)
        return record

    async def _commit(self, record: JobRecord) -> JobRecord:
        try:
            await self._save(record)
            await self._conn().commit()
        except BaseException:
            await self._conn().rollback()
            raise
        self.changed.set()
        return record

    async def list_runnable(self) -> list[JobRecord]:
        async with self._lock:
            rows = await (await self._conn().execute("SELECT body FROM jobs")).fetchall()
            records = [JobRecord.model_validate_json(row[0]) for row in rows]
            return sorted(
                (
                    r
                    for r in records
                    if r.accepted
                    and r.execution_ready
                    and not r.cancel_requested
                    and r.state in ("queued", "retry_pending")
                    and (r.retry_at is None or r.retry_at <= time.time())
                ),
                key=lambda r: (-r.submission.priority, r.created_at),
            )

    async def activate(self, job_id: str) -> JobRecord:
        async with self._lock:
            record = await self._required(job_id)
            if not record.accepted:
                raise JobConflict("job has not been accepted")
            if record.execution_ready:
                return record
            record.execution_ready = True
            try:
                await self._conn().execute(
                    "UPDATE jobs SET body=? WHERE job_id=?",
                    (_json(record.model_dump(mode="json")), job_id),
                )
                await self._conn().commit()
            except BaseException:
                await self._conn().rollback()
                raise
            self.changed.set()
            return record

    async def list_jobs(self) -> list[JobRecord]:
        async with self._lock:
            rows = await (await self._conn().execute("SELECT body FROM jobs")).fetchall()
            return [JobRecord.model_validate_json(row[0]) for row in rows]

    async def bind_attempt(
        self, job_id: str, attempt_id: str, worker_id: str, lease_id: str
    ) -> JobRecord:
        async with self._lock:
            record = await self._required(job_id)
            if (
                record.state != "reserving"
                or not record.attempts
                or record.attempts[-1].attempt_id != attempt_id
            ):
                raise JobConflict("attempt is not reserving")
            attempt = record.attempts[-1]
            attempt.worker_id, attempt.lease_id, attempt.state = worker_id, lease_id, "running"
            attempt.execution_started = True
            record.state = "running"
            return await self._commit(record)

    async def transition(self, job_id: str, state: JobState) -> JobRecord:
        allowed = {
            "queued": {"reserving", "canceled"},
            "reserving": {"running", "queued", "unknown_outcome", "failed", "canceled"},
            "running": {"completed", "failed", "unknown_outcome"},
            "retry_pending": {"reserving", "canceled"},
            "unknown_outcome": {"retry_pending"},
        }
        async with self._lock:
            record = await self._required(job_id)
            if not record.accepted or state not in allowed.get(record.state, set()):
                raise JobConflict("invalid job transition")
            if state == "retry_pending":
                policy = record.submission.retry_policy
                if (
                    record.cancel_requested
                    or not policy.safe
                    or not policy.allow_possible_duplicates
                    or record.execution_attempt_count >= policy.max_attempts
                ):
                    raise JobConflict("retry policy forbids retry")
                record.retry_at = time.time() + policy.delay
            record.state = state
            return await self._commit(record)

    async def cancel(self, job_id: str) -> JobRecord:
        async with self._lock:
            record = await self._required(job_id)
            if not record.accepted:
                raise JobConflict("job has not been accepted")
            if record.cancel_requested:
                return record
            record.cancel_requested = True
            if record.state in ("queued", "retry_pending", "reserving"):
                record.state = "canceled"
                if record.attempts and record.attempts[-1].state == "reserving":
                    record.attempts[-1].state = "canceled"
                    record.attempts[-1].finished_at = time.time()
            elif record.state == "running":
                record.state = "unknown_outcome"
                if record.attempts and record.attempts[-1].state == "running":
                    record.attempts[-1].state = "unknown_outcome"
                    record.attempts[-1].finished_at = time.time()
            record.retry_at = None
            return await self._commit(record)

    async def begin_attempt(
        self, job_id: str, worker_id: str | None = None, lease_id: str | None = None
    ) -> JobAttempt:
        async with self._lock:
            record = await self._required(job_id)
            if (
                not record.accepted
                or not record.execution_ready
                or record.cancel_requested
                or record.state not in ("queued", "retry_pending")
                or (record.retry_at is not None and record.retry_at > time.time())
                or record.execution_attempt_count >= record.submission.retry_policy.max_attempts
            ):
                raise JobConflict("job cannot start an attempt")
            attempt = JobAttempt(
                attempt_id=str(uuid.uuid4()),
                worker_id=worker_id,
                lease_id=lease_id,
                started_at=time.time(),
            )
            record.attempts.append(attempt)
            record.state, record.retry_at = "reserving", None
            await self._commit(record)
            return attempt

    async def finish_attempt(
        self,
        job_id: str,
        attempt_id: str,
        state: Literal["completed", "failed", "unknown_outcome"],
        result: Any = None,
    ) -> JobRecord:
        async with self._lock:
            record = await self._required(job_id)
            if (
                not record.attempts
                or record.attempts[-1].attempt_id != attempt_id
                or record.state not in ("reserving", "running")
            ):
                raise JobConflict("attempt is not active")
            attempt = record.attempts[-1]
            attempt.state, attempt.result, attempt.finished_at = state, result, time.time()
            record.state = state
            return await self._commit(record)

    async def export_replica(self, job_id: str) -> ReplicaSnapshot:
        async with self._lock:
            record = await self._required(job_id)
            return ReplicaSnapshot(record=record, digest=_digest(record))

    async def reconcile_snapshot(self, snapshot: ReplicaSnapshot) -> JobRecord:
        """Merge independently durable attempt evidence without assuming exclusive execution.

        Conflicting terminal evidence remains unknown. Locally active work remains
        active and its attempt remains last for the runner's completion fence.
        """
        async with self._lock:
            incoming = snapshot.record.model_copy(deep=True)
            if snapshot.digest != _digest(incoming):
                raise JobConflict("replica digest mismatch")
            local = await self._required(incoming.job_id)
            if local.origin_id != incoming.origin_id or local.submission != incoming.submission:
                raise JobConflict("replica identity mismatch")
            if (
                local.accepted
                and incoming.accepted
                and (
                    local.acks != incoming.acks
                    or local.effective_durability != incoming.effective_durability
                )
            ):
                raise JobConflict("replica acceptance mismatch")
            if not local.accepted and incoming.accepted:
                raise JobConflict("unaccepted local job cannot import acceptance")

            local.cancel_requested = (
                local.cancel_requested or incoming.cancel_requested or incoming.state == "canceled"
            )
            local_active = local.execution_ready and local.state in ("running", "reserving")
            active_id = local.attempts[-1].attempt_id if local_active and local.attempts else None
            attempts = {attempt.attempt_id: attempt for attempt in local.attempts}
            contradictory = False
            terminal = {"completed", "failed", "canceled"}
            for remote in incoming.attempts:
                existing = attempts.get(remote.attempt_id)
                if existing is None:
                    copied = remote.model_copy(deep=True)
                    if copied.state in ("running", "reserving"):
                        copied.state = "unknown_outcome"
                    attempts[copied.attempt_id] = copied
                    continue
                if (
                    existing.worker_id
                    and remote.worker_id
                    and existing.worker_id != remote.worker_id
                ) or (
                    existing.lease_id and remote.lease_id and existing.lease_id != remote.lease_id
                ):
                    raise JobConflict("attempt identity mismatch")
                existing.worker_id = existing.worker_id or remote.worker_id
                existing.lease_id = existing.lease_id or remote.lease_id
                if existing.attempt_id == active_id:
                    continue
                if existing.state in terminal and remote.state in terminal:
                    if existing.state != remote.state or existing.result != remote.result:
                        if existing.result not in existing.result_conflicts:
                            existing.result_conflicts.append(existing.result)
                        if remote.result not in existing.result_conflicts:
                            existing.result_conflicts.append(remote.result)
                        existing.state = "unknown_outcome"
                        contradictory = True
                elif remote.state in terminal and not existing.result_conflicts:
                    existing.state = remote.state
                    existing.result = remote.result
                    existing.finished_at = remote.finished_at
                elif existing.result_conflicts:
                    contradictory = True
                for result in remote.result_conflicts:
                    if result not in existing.result_conflicts:
                        existing.result_conflicts.append(result)
                    contradictory = True
                    existing.state = "unknown_outcome"

            merged = list(attempts.values())
            merged.sort(key=lambda attempt: (attempt.started_at, attempt.attempt_id))
            if active_id:
                merged.sort(key=lambda attempt: attempt.attempt_id == active_id)
            local.attempts = merged
            if not local_active and local.state != "canceled":
                if local.cancel_requested and local.state not in ("completed", "failed"):
                    local.state = "unknown_outcome" if local.attempts else "canceled"
                    local.retry_at = None
                elif contradictory:
                    local.state = "unknown_outcome"
                elif any(attempt.state == "completed" for attempt in merged):
                    local.state = "completed"
                    local.retry_at = None
                elif local.state not in ("completed", "failed") and incoming.state in (
                    "running",
                    "reserving",
                    "unknown_outcome",
                ):
                    local.state = "unknown_outcome"
                    local.retry_at = None
                elif local.state not in ("completed", "failed") and incoming.state == "failed":
                    local.state = "failed"
            # Never let a lower remote revision make a later local snapshot stale.
            local.revision = max(local.revision, incoming.revision)
            return await self._commit(local)

    async def import_replica(self, snapshot: ReplicaSnapshot) -> JobRecord:
        async with self._lock:
            incoming = snapshot.record.model_copy(deep=True)
            if snapshot.digest != _digest(incoming):
                raise JobConflict("replica digest mismatch")
            prior = await self._get(incoming.job_id)
            if prior:
                if prior.origin_id != incoming.origin_id or prior.submission != incoming.submission:
                    raise JobConflict("replica identity mismatch")
                if (
                    prior.execution_ready
                    and prior.state in ("reserving", "running")
                    and _digest(incoming) != _digest(prior)
                ):
                    raise JobConflict("active execution requires reconciliation")
                if incoming.revision <= prior.revision:
                    if incoming.revision == prior.revision and _digest(incoming) == _digest(prior):
                        return prior
                    raise JobConflict("stale or conflicting replica")
                if prior.accepted and (
                    not incoming.accepted
                    or prior.acks != incoming.acks
                    or prior.effective_durability != incoming.effective_durability
                ):
                    raise JobConflict("replica acceptance mismatch")
            # Execution activation is local authority, never copied from a peer.
            incoming.execution_ready = prior.execution_ready if prior else False
            # Preserve source revision and timestamps.
            incoming.revision -= 1
            updated = incoming.updated_at
            try:
                await self._save(incoming)
                incoming.updated_at = updated
                body = _json(incoming.model_dump(mode="json"))
                await self._conn().execute(
                    "UPDATE jobs SET body=? WHERE job_id=?", (body, incoming.job_id)
                )
                await self._conn().commit()
            except BaseException:
                await self._conn().rollback()
                raise
            self.changed.set()
            return incoming
