"""Authenticated, opt-in job snapshot replication over the peer transport."""

from __future__ import annotations

import contextlib

import httpx
from pydantic import BaseModel, ConfigDict, Field

from fleet_manager.peer.identity import IdentityError
from fleet_manager.peer.jobs import JobConflict, ReplicaSnapshot
from fleet_manager.peer.models import AdmissionError
from fleet_manager.peer.network import MAX_INFERENCE_BODY, read_response


class StoreRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    snapshot: ReplicaSnapshot


class FetchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    job_id: str = Field(min_length=1, max_length=128)


class ReplicaAck(BaseModel):
    model_config = ConfigDict(extra="forbid")
    job_id: str
    digest: str
    worker_id: str


class JobReplication:
    def __init__(self, local):
        self.local = local

    @property
    def journal(self):
        return self.local.state.job_journal

    async def store(self, principal: str, request: StoreRequest) -> ReplicaAck:
        snapshot = request.snapshot
        record = snapshot.record
        identity = self.local.state.identity
        # Only the origin may publish the immutable acceptance certificate or state.
        if principal != record.origin_id or principal == identity.worker_id:
            raise AdmissionError("replica_origin_mismatch", 403)
        if record.accepted and (
            record.effective_durability != "replicated"
            or identity.worker_id not in record.acks
            or len(set(record.acks)) < record.submission.replica_count
        ):
            raise AdmissionError("invalid_replica_certificate", 403)
        try:
            await self.journal.import_replica(snapshot)
        except JobConflict as exc:
            prior = await self.journal.get(record.job_id)
            if prior is None or not prior.accepted or not record.accepted:
                raise AdmissionError("replica_conflict", 409) from exc
            try:
                await self.journal.reconcile_snapshot(snapshot)
            except JobConflict as conflict:
                raise AdmissionError("replica_conflict", 409) from conflict
        return ReplicaAck(
            job_id=record.job_id, digest=snapshot.digest, worker_id=identity.worker_id
        )

    async def fetch(self, principal: str, request: FetchRequest) -> ReplicaSnapshot:
        record = await self.journal.get(request.job_id)
        if (
            record is None
            or not record.accepted
            or principal not in {record.origin_id, *record.acks}
        ):
            raise AdmissionError("replica_not_found", 404)
        return await self.journal.export_replica(request.job_id)

    def _peers(self):
        directory = self.local.state.directory
        if directory is None:
            return []
        return [s.descriptor for s in directory.snapshot() if s.reachable and s.descriptor]

    async def _rpc(self, peer, path: str, request: BaseModel, response_type):
        identity = self.local.state.identity
        body = request.model_dump_json().encode()
        if len(body) > MAX_INFERENCE_BODY:
            raise AdmissionError("replica_too_large", 413)
        headers = identity.sign_request("POST", path, body)
        async with self.local.state.peer_client.stream(
            "POST", peer.endpoint + path, content=body, headers=headers, timeout=5
        ) as response:
            content = await read_response(response, MAX_INFERENCE_BODY)
            identity.verify_response(
                headers["x-idle-nonce"],
                response.status_code,
                content,
                response.headers,
                peer.worker_id,
            )
            if response.status_code != 200:
                raise AdmissionError("replica_request_declined", response.status_code)
            return response_type.model_validate_json(content)

    async def _store(self, peer, snapshot):
        ack = await self._rpc(
            peer, "/peer/v1/jobs/store", StoreRequest(snapshot=snapshot), ReplicaAck
        )
        if (
            ack.job_id != snapshot.record.job_id
            or ack.digest != snapshot.digest
            or ack.worker_id != peer.worker_id
        ):
            raise IdentityError("replica acknowledgment mismatch")
        return ack.worker_id

    async def _probe_origin(self, peer, job_id):
        path = "/peer/v1/jobs/fetch"
        body = FetchRequest(job_id=job_id).model_dump_json().encode()
        identity = self.local.state.identity
        headers = identity.sign_request("POST", path, body)
        async with self.local.state.peer_client.stream(
            "POST", peer.endpoint + path, content=body, headers=headers, timeout=5
        ) as response:
            content = await read_response(response, MAX_INFERENCE_BODY)
            identity.verify_response(
                headers["x-idle-nonce"],
                response.status_code,
                content,
                response.headers,
                peer.worker_id,
            )
            # Any authenticated reply proves the origin is reachable, including
            # an error. It must never trigger takeover by itself.
            if response.status_code != 200:
                return None
            try:
                return ReplicaSnapshot.model_validate_json(content)
            except ValueError:
                return None

    async def replicate(self, job_id: str) -> list[str]:
        if self.local.state.identity is None or self.local.state.directory is None:
            return []
        snapshot = await self.journal.export_replica(job_id)
        if (
            snapshot.record.origin_id != self.local.state.identity.worker_id
            or snapshot.record.accepted
        ):
            raise JobConflict("only unaccepted origin jobs can be prepared")
        acks = []
        for peer in self._peers():
            try:
                ack = await self._store(peer, snapshot)
                if ack not in acks:
                    acks.append(ack)
            except (httpx.HTTPError, IdentityError, AdmissionError, ValueError):
                continue
            if len(acks) >= snapshot.record.submission.replica_count:
                break
        return acks

    async def sync(self, job_id: str) -> list[str]:
        if self.local.state.identity is None or self.local.state.directory is None:
            return []
        snapshot = await self.journal.export_replica(job_id)
        if (
            snapshot.record.origin_id != self.local.state.identity.worker_id
            or not snapshot.record.accepted
        ):
            return []
        synced = []
        for peer in self._peers():
            if peer.worker_id not in snapshot.record.acks:
                continue
            if peer.worker_id in synced:
                continue
            try:
                synced.append(await self._store(peer, snapshot))
            except (httpx.HTTPError, IdentityError, AdmissionError, ValueError):
                continue
        return synced

    async def reconcile(self, job_id: str):
        record = await self.journal.get(job_id)
        if (
            record is None
            or not record.accepted
            or self.local.state.identity is None
            or self.local.state.directory is None
        ):
            return record
        for peer in self._peers():
            if peer.worker_id not in {record.origin_id, *record.acks}:
                continue
            try:
                snapshot = await self._rpc(
                    peer, "/peer/v1/jobs/fetch", FetchRequest(job_id=job_id), ReplicaSnapshot
                )
                if (
                    snapshot.record.job_id != job_id
                    or snapshot.record.origin_id != record.origin_id
                ):
                    continue
                record = await self.journal.reconcile_snapshot(snapshot)
            except (httpx.HTTPError, IdentityError, AdmissionError, ValueError):
                continue
        return record

    async def tick(self, takeover_delay: float = 30) -> None:
        """Probe origins directly and persist uncertainty before eligible takeover.

        An unreachable origin is not proof that it stopped executing. Recovery can
        duplicate work and is restricted to explicit retry-safe submissions.
        """
        import time

        from fleet_manager.peer.jobs import JobRecord

        if not 1 <= takeover_delay <= 3600:
            raise ValueError("takeover delay must be between 1 and 3600 seconds")
        if self.local.state.identity is None or self.local.state.directory is None:
            return
        journal = self.journal
        async with journal._lock:
            db = journal._conn()
            await db.execute(
                "CREATE TABLE IF NOT EXISTS replica_recovery "
                "(job_id TEXT PRIMARY KEY, unreachable_since REAL NOT NULL, "
                "active INTEGER NOT NULL)"
            )
            rows = await (await db.execute("SELECT body FROM jobs")).fetchall()
            await db.commit()
        local_id = self.local.state.identity.worker_id
        records = [JobRecord.model_validate_json(row[0]) for row in rows]
        for record in records:
            if record.job_id in getattr(getattr(self.local.state, "jobs", None), "_tasks", {}):
                continue
            policy = record.submission.retry_policy
            if (
                record.origin_id == local_id
                or not record.accepted
                or record.cancel_requested
                or record.effective_durability != "replicated"
                or local_id not in record.acks
                or not policy.safe
                or policy.max_attempts <= 1
                or not policy.allow_possible_duplicates
                or record.state
                not in ("queued", "retry_pending", "unknown_outcome", "running", "reserving")
                or record.execution_attempt_count >= policy.max_attempts
            ):
                continue
            statuses = self.local.state.directory.snapshot() if self.local.state.directory else []
            origin = next((s for s in statuses if s.worker_id == record.origin_id), None)
            if origin is None:
                # Without a known endpoint there has been no direct probe.
                async with journal._lock:
                    await journal._conn().execute(
                        "DELETE FROM replica_recovery WHERE job_id=?", (record.job_id,)
                    )
                    await journal._conn().commit()
                continue
            reachable = False
            if origin is not None:
                try:
                    snapshot = await self._probe_origin(origin, record.job_id)
                    reachable = True
                    if (
                        snapshot is not None
                        and snapshot.record.job_id == record.job_id
                        and snapshot.record.origin_id == record.origin_id
                    ):
                        reachable = True
                        await journal.reconcile_snapshot(snapshot)
                except (httpx.HTTPError, IdentityError, AdmissionError, ValueError):
                    pass
            async with journal._lock:
                db = journal._conn()
                if reachable:
                    await db.execute(
                        "DELETE FROM replica_recovery WHERE job_id=?", (record.job_id,)
                    )
                else:
                    now = time.time()
                    await db.execute(
                        "INSERT OR IGNORE INTO replica_recovery VALUES (?, ?, 0)",
                        (record.job_id, now),
                    )
                    await db.execute(
                        "UPDATE replica_recovery SET active=1 WHERE job_id=? "
                        "AND unreachable_since<=?",
                        (record.job_id, now - takeover_delay),
                    )
                await db.commit()
            if not reachable and await self.may_run(record):
                await journal.activate(record.job_id)
            if not reachable and await self.may_run(record):
                with contextlib.suppress(JobConflict):
                    if record.state in ("running", "reserving"):
                        record = await journal.finish_attempt(
                            record.job_id, record.attempts[-1].attempt_id, "unknown_outcome"
                        )
                    if record.state == "unknown_outcome":
                        await journal.transition(record.job_id, "retry_pending")

    async def may_run(self, record) -> bool:
        identity = self.local.state.identity
        if identity is None:
            return record.origin_id == self.local.state.ledger.worker_id
        if record.origin_id == identity.worker_id:
            return True
        if (
            not record.accepted
            or record.cancel_requested
            or not record.submission.retry_policy.safe
            or not record.submission.retry_policy.allow_possible_duplicates
        ):
            return False
        async with self.journal._lock:
            db = self.journal._conn()
            cursor = await db.execute(
                "SELECT active FROM replica_recovery WHERE job_id=?", (record.job_id,)
            )
            row = await cursor.fetchone()
            return bool(row and row[0])
