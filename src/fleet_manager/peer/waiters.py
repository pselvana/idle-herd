"""Bounded origin-side scheduling; worker ledgers remain the grant authority."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import aiosqlite
import httpx

from fleet_manager.peer.discovery import MAX_BODY, PeerDescriptor
from fleet_manager.peer.identity import IdentityError
from fleet_manager.peer.models import AcquireRequest, AdmissionError, Lease
from fleet_manager.peer.network import Execution, OriginDispatch, read_response

logger = logging.getLogger(__name__)


@dataclass
class _Pending:
    spec: AcquireRequest
    future: asyncio.Future
    users: int = 0
    delivered: bool = False
    nonblocking: list[asyncio.Future] = field(default_factory=list)
    details: dict[str, int] = field(default_factory=dict)


class OriginWaiters:
    def __init__(self, local_app):
        self.local = local_app
        self.dispatch = OriginDispatch(local_app, local_app)
        self._pending = {}
        self._queue = deque()
        self._wake = asyncio.Event()
        self.changed = self._wake
        self._lock = asyncio.Lock()
        self._task = None
        self._inflight = set()
        self._db = None

    def pending_count(self) -> int:
        """Local observations only; multiple callers may share one idempotent wait."""
        return sum(item.users for item in self._pending.values() if not item.future.done())

    async def open(self):
        path = Path(self.local.state.ledger.settings.data_dir).expanduser() / "origin.sqlite"
        self._db = await aiosqlite.connect(path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=FULL")
        await self._db.execute(
            "CREATE TABLE IF NOT EXISTS claims (key TEXT PRIMARY KEY, params TEXT NOT NULL, "
            "request_id TEXT NOT NULL, target TEXT, lease TEXT)"
        )
        await self._db.execute(
            "CREATE TABLE IF NOT EXISTS request_aliases "
            "(request_id TEXT PRIMARY KEY, key TEXT NOT NULL)"
        )
        await self._db.execute(
            "INSERT OR IGNORE INTO request_aliases SELECT request_id,key FROM claims"
        )
        await self._db.commit()
        self._task = asyncio.create_task(self._run())

    async def close(self):
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        for item in self._pending.values():
            for future in item.nonblocking:
                future.cancel()
            if not item.future.done():
                item.future.cancel()
        await asyncio.gather(*self._inflight, return_exceptions=True)
        if self._db:
            await self._db.close()

    @staticmethod
    def _key(spec):
        return spec.idempotency_key or spec.request_id

    @staticmethod
    def _params(spec):
        return json.dumps(
            {
                "model": spec.model,
                "ttl_ms": spec.ttl_ms,
                "slots": spec.slots,
                "workload": spec.workload,
                "idle_only": spec.idle_only,
            },
            sort_keys=True,
        )

    async def acquire(self, spec: AcquireRequest) -> Lease:
        deadline = time.monotonic() + spec.wait_ms / 1000 if spec.wait_ms else None
        try:
            async with asyncio.timeout_at(deadline):
                return await self._acquire(spec, deadline)
        except TimeoutError as exc:
            item = self._pending.get(self._key(spec))
            details = item.details.copy() if item else None
            raise AdmissionError("capacity_wait_timeout", 504, details) from exc

    async def _acquire(self, spec, deadline):
        if not spec.request_id:
            spec = spec.model_copy(update={"request_id": str(uuid.uuid4())})
        key = self._key(spec)
        async with self._lock:
            row = await (
                await self._db.execute("SELECT * FROM claims WHERE key=?", (key,))
            ).fetchone()
            if row and row["params"] != self._params(spec):
                raise AdmissionError("idempotency_conflict", 409)
            alias = await (
                await self._db.execute(
                    "SELECT key FROM request_aliases WHERE request_id=?", (spec.request_id,)
                )
            ).fetchone()
            if alias and alias["key"] != key:
                raise AdmissionError("idempotency_conflict", 409)
            await self._db.execute(
                "INSERT OR IGNORE INTO request_aliases VALUES (?,?)", (spec.request_id, key)
            )
            if row:
                spec = spec.model_copy(update={"request_id": row["request_id"]})
            else:
                await self._db.execute(
                    "INSERT INTO claims(key,params,request_id) VALUES (?,?,?)",
                    (key, self._params(spec), spec.request_id),
                )
            await self._db.commit()
            if deadline is not None and time.monotonic() >= deadline:
                raise AdmissionError("capacity_wait_timeout", 504)
            item = self._pending.get(key)
            if item is not None and item.future.cancelled():
                raise AdmissionError("worker_unreachable", 503)
            if item is None:
                if len(self._pending) >= self.local.state.ledger.settings.max_waiters:
                    raise AdmissionError("too_many_waiters", 429)
                item = _Pending(
                    spec,
                    asyncio.get_running_loop().create_future(),
                )
                self._pending[key] = item
                self._queue.append(key)
                self._wake.set()
            item.users += 1
            immediate = None
            if spec.wait_ms == 0:
                immediate = asyncio.get_running_loop().create_future()
                item.nonblocking.append(immediate)
        delivered = False
        try:
            try:
                if spec.wait_ms:
                    result = await asyncio.wait_for(
                        asyncio.shield(item.future), max(0, deadline - time.monotonic())
                    )
                else:
                    result = await asyncio.shield(immediate)
            except TimeoutError as exc:
                raise AdmissionError("capacity_wait_timeout", 504, item.details.copy()) from exc
            delivered = True
            item.delivered = True
            return result
        finally:
            async with self._lock:
                item.users -= 1
                if immediate is not None:
                    if immediate in item.nonblocking:
                        item.nonblocking.remove(immediate)
                    if not immediate.done():
                        immediate.cancel()
                if item.users == 0 and not item.future.done():
                    item.future.cancel()
                    self._wake.set()
                elif (
                    item.users == 0
                    and not delivered
                    and not item.delivered
                    and item.future.done()
                    and not item.future.cancelled()
                    and item.future.exception() is None
                ):
                    await self._release_lease(item.future.result())

    async def _run(self):
        while True:
            count = len(self._queue)
            for _ in range(count):
                key = self._queue.popleft()
                item = self._pending.get(key)
                if item is None:
                    continue
                if item.future.cancelled():
                    self._pending.pop(key, None)
                    continue
                task = asyncio.create_task(self._round(key, item))
                self._inflight.add(task)
                task.add_done_callback(self._inflight.discard)
            self._wake.clear()
            events = [self._wake]
            for source in (self.local.state.ledger, self.local.state.directory):
                event = getattr(source, "changed", None)
                if isinstance(event, asyncio.Event):
                    events.append(event)
            tasks = [asyncio.create_task(event.wait()) for event in events]
            policy = getattr(self.local.state, "idle_policy", None)
            if policy is not None:
                tasks.append(asyncio.create_task(policy.wait_for_change(policy.revision, 0.25)))
            try:
                await asyncio.wait(tasks, timeout=0.25, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                for event in events[1:]:
                    event.clear()

    async def _round(self, key, item):
        try:
            lease, reason = await self._attempt(key, item.spec, item)
            if lease:
                if lease.expires_at <= time.time():
                    await self._release_lease(lease)
                    raise AdmissionError("reservation_expired", 410)
                if item.future.cancelled():
                    await self._release_lease(lease)
                else:
                    item.future.set_result(lease)
                    for future in item.nonblocking:
                        if not future.done():
                            future.set_result(lease)
            elif not item.future.cancelled():
                for future in item.nonblocking:
                    if not future.done():
                        future.set_exception(
                            AdmissionError(
                                reason,
                                429 if reason == "no_capacity" else 503,
                                item.details.copy(),
                            )
                        )
                if item.users <= len(item.nonblocking):
                    item.future.cancel()
                item.nonblocking.clear()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            for future in item.nonblocking:
                if not future.done():
                    future.set_exception(exc)
            if not item.future.done():
                if item.users > len(item.nonblocking):
                    item.future.set_exception(exc)
                else:
                    item.future.cancel()
            item.nonblocking.clear()
        if item.future.done():
            self._pending.pop(key, None)
        else:
            self._queue.append(key)

    async def _save(self, key, target, lease=None):
        await self._db.execute(
            "UPDATE claims SET target=?,lease=? WHERE key=?",
            (target, lease.model_dump_json() if lease else None, key),
        )
        await self._db.commit()

    async def _attempt(self, key, spec, item):
        row = await (await self._db.execute("SELECT * FROM claims WHERE key=?", (key,))).fetchone()
        mode = (await self.local.state.ledger.mode_state()).mode
        if mode == "paused":
            raise AdmissionError("manual_paused", 503)
        if mode == "offline" and row["target"] not in {None, "local"}:
            # A potentially granted remote claim cannot be redirected to a new
            # authority merely because the owner withdrew from the cluster.
            raise AdmissionError("manual_offline", 503)
        if row["lease"]:
            lease = Lease.model_validate_json(row["lease"])
            current = await self.reservation(lease.reservation_id, lease.token)
            if current.state != "reserved" or current.expires_at <= time.time():
                raise AdmissionError("request_already_used", 409)
            return current, "no_capacity"
        directory = self.local.state.directory
        candidates = directory.candidates(spec.model) if directory else []
        if mode == "offline":
            candidates = []
        targets = ["local"] + [p.model_dump_json() for p in candidates]
        if row["target"]:
            targets = [row["target"]]
        saw_busy = False
        saw_reachable = False
        item.details = {"attempted": 0, "busy": 0, "ineligible": 0, "unreachable": 0}
        for target in targets:
            if item.future.cancelled():
                return None, "no_capacity"
            peer = None if target == "local" else PeerDescriptor.model_validate_json(target)
            await self._save(key, target)
            item.details["attempted"] += 1
            try:
                nonblocking = spec.model_copy(update={"wait_ms": 0})
                lease = (
                    await self.local.state.ledger.acquire(nonblocking)
                    if peer is None
                    else await self.dispatch._acquire(peer, nonblocking)
                )
            except AdmissionError as exc:
                saw_reachable |= exc.reason not in {"backend_unavailable", "peer_unavailable"}
                saw_busy |= exc.status == 429
                if exc.status == 429:
                    item.details["busy"] += 1
                elif exc.reason in {"backend_unavailable", "peer_unavailable"}:
                    item.details["unreachable"] += 1
                else:
                    item.details["ineligible"] += 1
                if exc.status == 409:
                    raise
                await self._save(key, None)
                continue
            except (httpx.HTTPError, IdentityError, ValueError):
                item.details["unreachable"] += 1
                # The request may have committed remotely. Never change targets.
                return None, "no_reachable_worker"
            try:
                await self._save(key, target, lease)
            except BaseException:
                # A known grant must be released even if persisting its reply fails.
                with contextlib.suppress(Exception):
                    if peer is None:
                        await asyncio.shield(self.local.state.ledger.release(lease.token))
                    else:
                        await asyncio.shield(self._remote(peer, lease, True))
                raise
            return lease, "no_capacity"
        return None, "no_capacity" if saw_busy else (
            "no_eligible_worker" if saw_reachable else "no_reachable_worker"
        )

    async def _remote(self, peer, lease, release):
        path = "/peer/v1/release" if release else "/peer/v1/inspect"
        body = (
            Execution(path="/api/generate", token=lease.token, payload={})
            .model_dump_json()
            .encode()
        )
        identity = self.local.state.identity
        headers = identity.sign_request("POST", path, body)
        async with self.local.state.peer_client.stream(
            "POST", peer.endpoint + path, content=body, headers=headers, timeout=5
        ) as response:
            content = await read_response(response, MAX_BODY)
            identity.verify_response(
                headers["x-idle-nonce"],
                response.status_code,
                content,
                response.headers,
                peer.worker_id,
            )
            if response.status_code != 200:
                raise AdmissionError("remote_reservation_unavailable", response.status_code)
            result = Lease.model_validate_json(content)
            immutable = {
                "reservation_id",
                "token",
                "cluster_id",
                "worker_id",
                "worker_incarnation",
                "slot_id",
                "slot_generation",
                "request_id",
                "model",
                "expires_at",
                "ttl_ms",
                "workload",
                "idle_only",
            }
            if result.worker_id != peer.worker_id or result.model_dump(
                include=immutable
            ) != lease.model_dump(include=immutable):
                raise IdentityError("worker lease binding mismatch")
            return result

    async def resolve_token(self, token):
        rows = await (
            await self._db.execute("SELECT target,lease FROM claims WHERE lease IS NOT NULL")
        ).fetchall()
        for row in rows:
            lease = Lease.model_validate_json(row["lease"])
            if (
                hashlib.sha256(lease.token.encode()).digest()
                == hashlib.sha256(token.encode()).digest()
            ):
                peer = (
                    None
                    if row["target"] == "local"
                    else PeerDescriptor.model_validate_json(row["target"])
                )
                return peer, lease
        lease = await self.local.state.ledger.inspect(token)
        return None, lease

    async def reservation(self, reservation_id, token, release=False):
        peer, lease = await self.resolve_token(token)
        if lease.reservation_id != reservation_id:
            raise AdmissionError("invalid_reservation", 403)
        if peer is None:
            return await (
                self.local.state.ledger.release(token)
                if release
                else self.local.state.ledger.inspect(token)
            )
        try:
            return await self._remote(peer, lease, release)
        except (httpx.HTTPError, IdentityError) as exc:
            raise AdmissionError("worker_unreachable", 503) from exc

    async def _release_lease(self, lease):
        try:
            await self.reservation(lease.reservation_id, lease.token, release=True)
        except (AdmissionError, httpx.HTTPError, IdentityError) as exc:
            logger.warning(
                "Reservation cleanup unconfirmed for worker %s: %s",
                lease.worker_id,
                type(exc).__name__,
            )
