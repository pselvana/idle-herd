"""Worker-owned durable leases. Expiry never frees running backend execution."""

from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite
from pydantic import ValidationError

from fleet_manager.local_control.models import (
    ModeState,
    ModeUpdate,
    PreferencesState,
    PreferencesUpdate,
    PreferencesValues,
)
from fleet_manager.peer.models import (
    AcquireRequest,
    AdmissionError,
    Capacity,
    Lease,
    PeerSettings,
    ResourceFit,
    Workload,
)
from fleet_manager.peer.policy import IdlePolicy
from fleet_manager.peer.sensors import native_sensor

FitCheck = Callable[[str], Awaitable[ResourceFit]]
OCCUPIED = "('reserved','running','quarantined')"


class LocalLedger:
    def __init__(
        self, settings: PeerSettings, fit_check: FitCheck, policy: IdlePolicy | None = None
    ):
        self.settings = settings
        self.fit_check = fit_check
        self.policy = policy or IdlePolicy(settings, native_sensor(settings.sensor_helper))
        self._lock = asyncio.Lock()
        self.changed = asyncio.Event()
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
                CREATE TABLE IF NOT EXISTS control_state (
                    key TEXT PRIMARY KEY, body TEXT NOT NULL);
            """)
            # Upgrade Phase 1/2 ledgers without changing fences or execution state.
            columns = {
                row["name"]
                for row in await (await self._db.execute("PRAGMA table_info(leases)")).fetchall()
            }
            if "workload" not in columns:
                await self._db.execute(
                    "ALTER TABLE leases ADD COLUMN workload TEXT NOT NULL DEFAULT 'interactive'"
                )
            if "idle_only" not in columns:
                await self._db.execute(
                    "ALTER TABLE leases ADD COLUMN idle_only INTEGER NOT NULL DEFAULT 0"
                )
            for name, declaration in {
                "peer_origin": "INTEGER NOT NULL DEFAULT 0",
                "created_at": "REAL NOT NULL DEFAULT 0",
                "updated_at": "REAL NOT NULL DEFAULT 0",
            }.items():
                if name not in columns:
                    await self._db.execute(f"ALTER TABLE leases ADD COLUMN {name} {declaration}")
            # Older peer grants are recognizable by their authenticated namespace.
            await self._db.execute("UPDATE leases SET peer_origin=1 WHERE request_id LIKE 'peer/%'")
            await self._db.commit()
            async with self._transaction():
                await self._db.execute(
                    "INSERT OR IGNORE INTO control_state VALUES ('mode',?)",
                    (ModeState(mode="auto", revision=0).model_dump_json(),),
                )
                await self._mode_in_transaction()
                preferences = await self._preferences_in_transaction()
                self._apply_preferences(preferences.values)
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
            self._lease_changed = False
            try:
                await self._db.execute("BEGIN IMMEDIATE")
                yield
                await self._db.commit()
                if self._lease_changed:
                    self.changed.set()
            except BaseException:
                await asyncio.shield(self._db.rollback())
                raise

    async def _expire(self):
        cursor = await self._db.execute(
            "UPDATE leases SET state='expired' WHERE state='reserved' AND expires_at<=?",
            (time.time(),),
        )
        self._lease_changed |= cursor.rowcount > 0

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

    async def _mode_in_transaction(self) -> ModeState:
        row = await (
            await self._db.execute("SELECT body FROM control_state WHERE key='mode'")
        ).fetchone()
        state = ModeState.model_validate_json(row[0])
        if state.expires_at is not None and state.expires_at <= time.time():
            state = ModeState(mode="auto", revision=state.revision + 1)
            await self._db.execute(
                "UPDATE control_state SET body=? WHERE key='mode'", (state.model_dump_json(),)
            )
            self._lease_changed = True
        return state

    async def mode_state(self) -> ModeState:
        async with self._transaction():
            return await self._mode_in_transaction()

    async def _require_mode(self, *, peer: bool = False, dispatch: bool = False):
        mode = (await self._mode_in_transaction()).mode
        if (
            mode == "paused"
            or (peer and mode in {"draining", "offline"})
            or (dispatch and mode == "offline")
        ):
            raise AdmissionError("manual_" + mode, 503)

    async def require_dispatch(self):
        """Fence each new outbound acquisition/execution using current owner mode."""
        async with self._transaction():
            await self._require_mode(dispatch=True)

    async def cluster_participating(self) -> bool:
        # Pause retains discovery/diagnostics but cannot issue or dispatch work.
        return (await self.mode_state()).mode != "offline"

    async def set_mode(self, spec: ModeUpdate) -> ModeState:
        async with self._transaction():
            previous = await self._mode_in_transaction()
            deadline = spec.expires_at
            if spec.duration_seconds is not None:
                deadline = time.time() + spec.duration_seconds
            if deadline is not None and (spec.mode == "auto" or deadline <= time.time()):
                raise AdmissionError("invalid_mode_expiry", 422)
            state = ModeState(mode=spec.mode, expires_at=deadline, revision=previous.revision + 1)
            await self._db.execute(
                "UPDATE control_state SET body=? WHERE key='mode'", (state.model_dump_json(),)
            )
            # Invalidate grants under the same lock as consumption. Running and
            # quarantined executions retain all budgets; mode changes never cancel.
            if spec.mode != "auto":
                condition = "" if spec.mode == "paused" else " AND peer_origin=1"
                rows = await (
                    await self._db.execute(
                        "SELECT * FROM leases WHERE state='reserved'" + condition
                    )
                ).fetchall()
                for row in rows:
                    await self._event(Lease(**dict(row)), "released")
            self._lease_changed = True
            return state

    async def _preferences_in_transaction(self) -> PreferencesState:
        row = await (
            await self._db.execute("SELECT body FROM control_state WHERE key='preferences'")
        ).fetchone()
        if row:
            stored = json.loads(row[0])
            defaults = {
                name: getattr(self.settings, name) for name in PreferencesValues.model_fields
            }
            stored["values"] = defaults | stored["values"]
            return PreferencesState.model_validate(stored)
        values = PreferencesValues(
            **{name: getattr(self.settings, name) for name in PreferencesValues.model_fields}
        )
        # Enrollment and offline recovery also open this ledger. Merely opening
        # it must not freeze their default policies over the later daemon config.
        # Only an explicit owner PATCH creates a durable preference override.
        return PreferencesState(version=1, values=values)

    def _apply_preferences(self, values: PreferencesValues):
        for name, value in values.model_dump().items():
            setattr(self.settings, name, value)
            setattr(self.policy.settings, name, value)
        # Every settings change requires a fresh idle recovery window.
        self.policy._since.clear()
        self.policy._ready.clear()

    async def preferences(self) -> PreferencesState:
        async with self._transaction():
            return await self._preferences_in_transaction()

    get_preferences = preferences

    async def update_preferences(self, patch: PreferencesUpdate) -> PreferencesState:
        async with self._transaction():
            previous = await self._preferences_in_transaction()
            if patch.expected_version != previous.version:
                raise AdmissionError("preferences_version_conflict", 409)
            updates = patch.values.model_dump(exclude_unset=True)
            if not updates or any(value is None for value in updates.values()):
                raise AdmissionError("invalid_preferences", 422)
            try:
                combined = previous.values.model_dump() | updates
                PeerSettings.model_validate(self.settings.model_dump() | combined)
                values = PreferencesValues.model_validate(combined)
            except ValidationError as exc:
                raise AdmissionError("invalid_preferences", 422) from exc
            state = PreferencesState(version=previous.version + 1, values=values)
            await self._db.execute(
                "INSERT OR REPLACE INTO control_state VALUES ('preferences',?)",
                (state.model_dump_json(),),
            )
            self._lease_changed = True
        # Apply only a durably committed update, without yielding to new admission.
        self._apply_preferences(values)
        return state

    async def job_summaries(self):
        from fleet_manager.local_control.models import WorkerLease

        async with self._transaction():
            await self._expire()
            return [
                WorkerLease(
                    job_id=item.reservation_id,
                    lease_id=item.reservation_id,
                    request_id_hash=hashlib.sha256(item.request_id.encode()).hexdigest()[:16],
                    worker_incarnation=item.worker_incarnation,
                    slot_generation=item.slot_generation,
                    state=item.state,
                    model=item.model,
                    workload=item.workload,
                    peer_origin=item.peer_origin,
                    created_at=item.created_at,
                    updated_at=item.updated_at,
                )
                for item in await self._occupied()
            ]

    async def _occupied(self) -> list[Lease]:
        rows = await (
            await self._db.execute(f"SELECT * FROM leases WHERE state IN {OCCUPIED}")
        ).fetchall()
        return [Lease(**dict(row)) for row in rows]

    def _check_peer_quota(self, request_id: str, occupied: list[Lease], *, existing: bool):
        # Peer request IDs are namespaced by their authenticated principal.
        parts = request_id.split("/", 2)
        principal = parts[1] if len(parts) == 3 and parts[0] == "peer" and parts[1] else None

        def same_principal(item: Lease) -> bool:
            other = item.request_id.split("/", 2)
            other_principal = other[1] if len(other) == 3 and other[0] == "peer" else None
            return item.peer_origin and principal == other_principal

        used = sum(same_principal(item) for item in occupied)
        if used > self.settings.peer_max_concurrent or (
            not existing and used >= self.settings.peer_max_concurrent
        ):
            raise AdmissionError("peer_quota", 429)

    async def _event(self, lease: Lease, state: str):
        self._lease_changed = True
        await self._db.execute(
            "UPDATE leases SET state=?,updated_at=? WHERE reservation_id=?",
            (state, time.time(), lease.reservation_id),
        )
        await self._db.execute(
            "INSERT INTO events(reservation_id,state,observed_at) VALUES (?,?,?)",
            (lease.reservation_id, state, time.time()),
        )

    async def acquire(self, spec: AcquireRequest, *, peer: bool = False) -> Lease:
        async with self._transaction():
            await self._require_mode(peer=peer)
            await self._expire()
            row = await (
                await self._db.execute(
                    "SELECT * FROM leases WHERE request_id=?", (spec.request_id,)
                )
            ).fetchone()
            if row:
                lease = Lease(**dict(row))
                if (
                    lease.model != spec.model
                    or lease.ttl_ms != spec.ttl_ms
                    or lease.workload != spec.workload
                    or lease.idle_only != spec.idle_only
                    or lease.peer_origin != peer
                ):
                    raise AdmissionError("idempotency_conflict")
                if lease.worker_incarnation == self.incarnation and lease.state == "reserved":
                    if peer:
                        self._check_peer_quota(
                            lease.request_id, await self._occupied(), existing=True
                        )
                    await self.policy.require(lease.workload, lease.idle_only, peer=peer)
                    if lease.expires_at <= time.time():
                        raise AdmissionError("reservation_not_active")
                    return lease
                raise AdmissionError("request_already_used")
            fit = await self.fit_check(spec.model)
            occupied = await self._occupied()
            if peer:
                self._check_peer_quota(spec.request_id, occupied, existing=False)
            model_limit = self.settings.model_slots.get(spec.model, self.settings.slots)
            if (
                len(occupied) >= self.settings.slots
                or sum(item.model == spec.model for item in occupied) >= model_limit
            ):
                raise AdmissionError("busy", 429)
            if (
                self.settings.memory_admission_mode == "strict"
                and sum(item.memory_gb for item in occupied) + fit.required_gb > fit.available_gb
            ):
                raise AdmissionError("insufficient_memory", 503)
            # Sample after backend probes, immediately before the durable grant.
            await self.policy.require(spec.workload, spec.idle_only, peer=peer)
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
                workload=spec.workload,
                idle_only=spec.idle_only,
                peer_origin=peer,
                created_at=time.time(),
                updated_at=time.time(),
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

    async def consume(
        self,
        token: str,
        model: str,
        *,
        workload: Workload | None = None,
        idle_only: bool = False,
    ) -> Lease:
        async with self._transaction():
            await self._expire()
            lease = await self._authenticate(token)
            await self._require_mode(peer=lease.peer_origin)
            if lease.state != "reserved" or lease.expires_at <= time.time():
                raise AdmissionError("reservation_not_active")
            if lease.model != model:
                raise AdmissionError("reservation_model_mismatch")
            fit = await self.fit_check(model)
            occupied = await self._occupied()
            if lease.peer_origin:
                self._check_peer_quota(lease.request_id, occupied, existing=True)
            # Retain estimates for diagnostics; only strict mode treats them as
            # a RAM budget. Backend mode lets Ollama reclaim/evict resident data.
            required = max(lease.memory_gb, fit.required_gb)
            if self.settings.memory_admission_mode == "strict" and (
                sum(item.memory_gb for item in occupied) - lease.memory_gb + required
                > fit.available_gb
            ):
                raise AdmissionError("insufficient_memory", 503)
            additional = (
                "background"
                if workload == "background" and lease.workload != "background"
                else lease.workload
                if idle_only and not lease.idle_only
                else None
            )
            await self.policy.require(
                lease.workload,
                lease.idle_only,
                additional_workload=additional,
                additional_idle_only=idle_only,
                peer=lease.peer_origin,
            )
            if lease.expires_at <= time.time():
                raise AdmissionError("reservation_not_active")
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

    async def capacity(
        self,
        model: str,
        workload: Workload = "interactive",
        idle_only: bool = False,
        *,
        peer: bool = False,
    ) -> Capacity:
        async with self._transaction():
            await self._expire()
            occupied = await self._occupied()
            reason = None
            try:
                await self._require_mode(peer=peer)
            except AdmissionError as exc:
                reason = exc.reason
            try:
                fit = await self.fit_check(model)
                if self.settings.memory_admission_mode == "strict":
                    remaining_gb = fit.available_gb - sum(item.memory_gb for item in occupied)
                    memory_slots = max(0, int(remaining_gb / fit.required_gb))
                    if memory_slots == 0:
                        reason = reason or "insufficient_memory"
                else:
                    memory_slots = self.settings.slots
            except AdmissionError as exc:
                reason, memory_slots = exc.reason, 0
            decision = await self.policy.decision(workload, idle_only, peer=peer)
            if not decision.eligible:
                reason, memory_slots = reason or decision.reason, 0
            if peer and self.settings.peer_max_concurrent == 0:
                reason, memory_slots = reason or "peer_quota", 0
            if reason:
                memory_slots = 0
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
                total_slots=min(
                    self.settings.slots, self.settings.model_slots.get(model, self.settings.slots)
                ),
                observed_available_slots=available,
                quarantined_slots=sum(item.state == "quarantined" for item in occupied),
                eligible=reason is None,
                reason=reason,
                observed_at=time.time(),
                eligibility=decision,
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
