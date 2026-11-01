"""Worker-owned durable leases. Expiry never frees running backend execution."""

from __future__ import annotations

import asyncio
import secrets
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from fleet_manager.peer.models import (
    AcquireRequest,
    AdmissionError,
    Capacity,
    Lease,
    PeerSettings,
    ResourceFit,
)

FitCheck = Callable[[str], Awaitable[ResourceFit]]
OCCUPIED = "('reserved','running','quarantined')"


class LocalLedger:
    def __init__(self, settings: PeerSettings, fit_check: FitCheck):
        self.settings = settings
        self.fit_check = fit_check
        self._lock = asyncio.Lock()
        self._db: aiosqlite.Connection | None = None
        self._owner: aiosqlite.Connection | None = None
        self.worker_id = ""
        self.cluster_id = ""
        self.incarnation = str(uuid.uuid4())

    async def open(self):
        directory = Path(self.settings.data_dir).expanduser()
        await asyncio.to_thread(directory.mkdir, parents=True, exist_ok=True, mode=0o700)
        await asyncio.to_thread(directory.chmod, 0o700)
        try:
            # Separate non-WAL database: an OS-released SQLite write lock, held
            # for the daemon lifetime, excludes a second authority on every OS.
            self._owner = await aiosqlite.connect(directory / "worker-owner.sqlite", timeout=0)
            await self._owner.execute("BEGIN EXCLUSIVE")
            self._db = await aiosqlite.connect(directory / "worker.sqlite")
            self._db.row_factory = aiosqlite.Row
            await self._db.execute("PRAGMA journal_mode=WAL")
            await self._db.execute("PRAGMA synchronous=FULL")
            await self._db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    worker_id TEXT NOT NULL, cluster_id TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS slots (
                    slot_id INTEGER PRIMARY KEY, generation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS leases (
                    reservation_id TEXT PRIMARY KEY, token TEXT NOT NULL UNIQUE,
                    cluster_id TEXT NOT NULL, worker_id TEXT NOT NULL,
                    worker_incarnation TEXT NOT NULL, slot_id INTEGER NOT NULL,
                    slot_generation INTEGER NOT NULL, request_id TEXT NOT NULL UNIQUE,
                    model TEXT NOT NULL, expires_at REAL NOT NULL, ttl_ms INTEGER NOT NULL,
                    state TEXT NOT NULL, memory_gb REAL NOT NULL);
                CREATE UNIQUE INDEX IF NOT EXISTS occupied_slot ON leases(slot_id)
                    WHERE state IN ('reserved','running','quarantined');
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY, reservation_id TEXT,
                    state TEXT NOT NULL, observed_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS peer_nonces (
                    nonce TEXT PRIMARY KEY, expires_at REAL NOT NULL);
            """)
            async with self._transaction():
                await self._db.execute(
                    "INSERT OR IGNORE INTO identity VALUES (1,?,?)",
                    (str(uuid.uuid4()), str(uuid.uuid4())),
                )
                row = await (await self._db.execute("SELECT * FROM identity")).fetchone()
                self.worker_id, self.cluster_id = row["worker_id"], row["cluster_id"]
                # Reserved means no execution could have started. Running means
                # dispatch MAY have happened, even if the daemon never got a reply.
                await self._db.execute(
                    "INSERT INTO events(reservation_id,state,observed_at) "
                    "SELECT reservation_id,'quarantined',? FROM leases WHERE state='running'",
                    (time.time(),),
                )
                await self._db.execute(
                    "UPDATE leases SET state='quarantined' WHERE state='running'"
                )
                await self._db.execute("UPDATE leases SET state='expired' WHERE state='reserved'")
        except BaseException:
            await self.close()
            raise

    @asynccontextmanager
    async def _transaction(self):
        async with self._lock:
            try:
                await self._db.execute("BEGIN IMMEDIATE")
                yield
                await self._db.commit()
            except BaseException:
                await asyncio.shield(self._db.rollback())
                raise

    async def _expire(self):
        await self._db.execute(
            "UPDATE leases SET state='expired' WHERE state='reserved' AND expires_at<=?",
            (time.time(),),
        )

    async def adopt_cluster(self, cluster_id: str):
        """Offline enrollment only: never reassign an authority with lease history."""
        async with self._transaction():
            if cluster_id == self.cluster_id:
                return
            row = await (await self._db.execute("SELECT count(*) FROM leases")).fetchone()
            if row[0]:
                raise AdmissionError("cannot_enroll_worker_with_lease_history")
            await self._db.execute("UPDATE identity SET cluster_id=?", (cluster_id,))
        self.cluster_id = cluster_id

    async def accept_peer_nonce(self, worker_id: str, nonce: str, timestamp: int):
        """Persist authenticated RPC replay fences before any mutation/acknowledgment."""
        async with self._transaction():
            await self._db.execute("DELETE FROM peer_nonces WHERE expires_at<?", (time.time(),))
            row = await (await self._db.execute("SELECT count(*) FROM peer_nonces")).fetchone()
            if row[0] >= 10000:
                raise AdmissionError("peer_replay_budget_exhausted", 429)
            try:
                await self._db.execute(
                    "INSERT INTO peer_nonces VALUES (?,?)",
                    (worker_id + ":" + nonce, timestamp + 121),
                )
            except aiosqlite.IntegrityError as exc:
                raise AdmissionError("replayed_peer_request", 403) from exc

    async def _occupied(self) -> list[Lease]:
        rows = await (
            await self._db.execute(f"SELECT * FROM leases WHERE state IN {OCCUPIED}")
        ).fetchall()
        return [Lease(**dict(row)) for row in rows]

    async def _event(self, lease: Lease, state: str):
        await self._db.execute(
            "UPDATE leases SET state=? WHERE reservation_id=?", (state, lease.reservation_id)
        )
        await self._db.execute(
            "INSERT INTO events(reservation_id,state,observed_at) VALUES (?,?,?)",
            (lease.reservation_id, state, time.time()),
        )

    async def acquire(self, spec: AcquireRequest) -> Lease:
        async with self._transaction():
            await self._expire()
            row = await (
                await self._db.execute(
                    "SELECT * FROM leases WHERE request_id=?", (spec.request_id,)
                )
            ).fetchone()
            if row:
                lease = Lease(**dict(row))
                if lease.model != spec.model or lease.ttl_ms != spec.ttl_ms:
                    raise AdmissionError("idempotency_conflict")
                if lease.worker_incarnation == self.incarnation and lease.state == "reserved":
                    return lease
                raise AdmissionError("request_already_used")
            fit = await self.fit_check(spec.model)
            occupied = await self._occupied()
            model_limit = self.settings.model_slots.get(spec.model, self.settings.slots)
            if (
                len(occupied) >= self.settings.slots
                or sum(item.model == spec.model for item in occupied) >= model_limit
            ):
                raise AdmissionError("busy", 429)
            if sum(item.memory_gb for item in occupied) + fit.required_gb > fit.available_gb:
                raise AdmissionError("insufficient_memory", 503)
            taken = {item.slot_id for item in occupied}
            slot = next(i for i in range(self.settings.slots) if i not in taken)
            await self._db.execute("INSERT OR IGNORE INTO slots VALUES (?,0)", (slot,))
            await self._db.execute(
                "UPDATE slots SET generation=generation+1 WHERE slot_id=?", (slot,)
            )
            row = await (
                await self._db.execute("SELECT generation FROM slots WHERE slot_id=?", (slot,))
            ).fetchone()
            lease = Lease(
                reservation_id=str(uuid.uuid4()),
                token=secrets.token_urlsafe(32),
                cluster_id=self.cluster_id,
                worker_id=self.worker_id,
                worker_incarnation=self.incarnation,
                slot_id=slot,
                slot_generation=row["generation"],
                request_id=spec.request_id,
                model=spec.model,
                expires_at=time.time() + spec.ttl_ms / 1000,
                ttl_ms=spec.ttl_ms,
                state="reserved",
                memory_gb=fit.required_gb,
            )
            fields = lease.model_dump()
            await self._db.execute(
                f"INSERT INTO leases ({','.join(fields)}) VALUES ({','.join('?' for _ in fields)})",
                tuple(fields.values()),
            )
            await self._event(lease, "reserved")
            # _transaction commits (FULL synchronous) before caller sees the token.
            return lease

    async def _authenticate(self, token: str) -> Lease:
        row = await (
            await self._db.execute("SELECT * FROM leases WHERE token=?", (token,))
        ).fetchone()
        if not row or not secrets.compare_digest(row["token"], token):
            raise AdmissionError("invalid_reservation", 403)
        lease = Lease(**dict(row))
        if lease.worker_incarnation != self.incarnation:
            raise AdmissionError("stale_incarnation")
        generation = await (
            await self._db.execute("SELECT generation FROM slots WHERE slot_id=?", (lease.slot_id,))
        ).fetchone()
        if generation["generation"] != lease.slot_generation:
            raise AdmissionError("stale_generation")
        return lease

    async def consume(self, token: str, model: str) -> Lease:
        async with self._transaction():
            await self._expire()
            lease = await self._authenticate(token)
            if lease.state != "reserved" or lease.expires_at <= time.time():
                raise AdmissionError("reservation_not_active")
            if lease.model != model:
                raise AdmissionError("reservation_model_mismatch")
            fit = await self.fit_check(model)
            occupied = await self._occupied()
            # Recheck live resources and retain every reservation's budget.
            required = max(lease.memory_gb, fit.required_gb)
            if sum(item.memory_gb for item in occupied) - lease.memory_gb + required > (
                fit.available_gb
            ):
                raise AdmissionError("insufficient_memory", 503)
            await self._db.execute(
                "UPDATE leases SET memory_gb=? WHERE reservation_id=?",
                (required, lease.reservation_id),
            )
            await self._event(lease, "running")
            return lease.model_copy(update={"state": "running", "memory_gb": required})

    async def inspect(self, token: str) -> Lease:
        async with self._transaction():
            await self._expire()
            return await self._authenticate(token)

    async def release(self, token: str) -> Lease:
        async with self._transaction():
            lease = await self._authenticate(token)
            if lease.state in {"running", "quarantined"}:
                raise AdmissionError("execution_not_confirmed_stopped")
            await self._event(lease, "released")
            return lease.model_copy(update={"state": "released"})

    async def abandon_unconsumed(self, token: str):
        """Internal origin cleanup; never alter running work or a reused slot."""
        async with self._transaction():
            row = await (
                await self._db.execute("SELECT * FROM leases WHERE token=?", (token,))
            ).fetchone()
            if row and row["state"] == "reserved" and row["worker_incarnation"] == self.incarnation:
                await self._event(Lease(**dict(row)), "released")

    async def finish(self, token: str, *, stopped: bool):
        """Internal transport confirmation; never exposed as a client release."""
        async with self._transaction():
            lease = await self._authenticate(token)
            if lease.state != "running":
                raise AdmissionError("execution_not_running")
            await self._event(lease, "released" if stopped else "quarantined")

    async def capacity(self, model: str) -> Capacity:
        async with self._transaction():
            await self._expire()
            occupied = await self._occupied()
            reason = None
            try:
                fit = await self.fit_check(model)
                remaining_gb = fit.available_gb - sum(item.memory_gb for item in occupied)
                memory_slots = max(0, int(remaining_gb / fit.required_gb))
            except AdmissionError as exc:
                reason, memory_slots = exc.reason, 0
            available = max(
                0,
                min(
                    self.settings.slots - len(occupied),
                    self.settings.model_slots.get(model, self.settings.slots)
                    - sum(item.model == model for item in occupied),
                    memory_slots,
                ),
            )
            return Capacity(
                worker_id=self.worker_id,
                worker_incarnation=self.incarnation,
                model=model,
                total_slots=self.settings.slots,
                observed_available_slots=available,
                quarantined_slots=sum(item.state == "quarantined" for item in occupied),
                eligible=reason is None,
                reason=reason,
                observed_at=time.time(),
            )

    async def reconcile_stopped_backend(self):
        """Offline maintenance only, after operator confirms ALL backend runners stopped.

        Taking the owner lock excludes the daemon. This cannot be inferred from
        /api/ps, a failed health probe, a timeout, or a lease's expiry.
        """
        async with self._transaction():
            await self._db.execute(
                "INSERT INTO events(reservation_id,state,observed_at) "
                "SELECT reservation_id,'operator_confirmed_stopped',? FROM leases "
                "WHERE state='quarantined'",
                (time.time(),),
            )
            await self._db.execute("UPDATE leases SET state='released' WHERE state='quarantined'")

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None
        if self._owner:
            await self._owner.close()
            self._owner = None
