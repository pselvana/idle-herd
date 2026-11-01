"""Durable origin scheduling through live worker admission, without consensus."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from fleet_manager.peer.jobs import JobCapacityError, JobConflict, JobSubmission
from fleet_manager.peer.models import AcquireRequest, AdmissionError
from fleet_manager.peer.network import trace_failure

log = logging.getLogger(__name__)
MAX_RESULT_BYTES = 32 * 1024


class JobService:
    def __init__(self, local):
        self.local = local
        self.journal = local.state.job_journal
        self.replication = local.state.job_replication
        self._tasks = {}
        self._submit_locks = {}
        self._task = None
        self._maintenance_task = None
        self._maintenance_cursor = 0

    async def open(self):
        self._task = asyncio.create_task(self._loop())
        self._maintenance_task = asyncio.create_task(self._maintenance())

    async def close(self):
        for background in (self._task, self._maintenance_task):
            if background is None:
                continue
            background.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await background
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def submit(self, spec):
        key = spec.idempotency_key
        entry = self._submit_locks.setdefault(key, [asyncio.Lock(), 0])
        entry[1] += 1
        try:
            async with entry[0]:
                return await self._submit(spec)
        finally:
            entry[1] -= 1
            if not entry[1]:
                self._submit_locks.pop(key, None)

    async def _submit(self, spec):
        if (await self.local.state.ledger.mode_state()).mode == "paused":
            raise AdmissionError("manual_paused", 503)
        record, _ = await self.journal.submit(spec, self.local.state.ledger.worker_id)
        if not record.accepted:
            acks = []
            durability = spec.durability
            if durability == "replicated":
                acks = await self.replication.replicate(record.job_id)
                if len(acks) < spec.replica_count:
                    if not spec.allow_durability_downgrade:
                        raise AdmissionError("job_durability_unavailable", 503)
                    durability = "local"
            record = await self.journal.accept(record.job_id, acks, durability)
        if not record.execution_ready:
            if record.effective_durability == "replicated":
                synced = await self.replication.sync(record.job_id)
                if len(synced) < spec.replica_count:
                    # Some holders may have the accepted certificate even though
                    # its full acknowledgment threshold was not observed here.
                    raise AdmissionError("job_acceptance_unknown", 503)
            record = await self.journal.activate(record.job_id)
        return record

    async def cancel(self, job_id):
        record = await self.journal.get(job_id)
        if record is None:
            raise AdmissionError("job_not_found", 404)
        if record.origin_id != self.local.state.ledger.worker_id:
            raise AdmissionError("job_origin_required", 403)
        record = await self.journal.cancel(job_id)
        task = self._tasks.get(job_id)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self.replication.sync(job_id)
        return record

    async def _maintenance(self):
        while True:
            try:
                await self.replication.tick()
                records = await self.journal.list_jobs()
                # Bound each batch; old completed jobs must also reconcile after
                # reconnect so independent attempts/results are not discarded.
                if records:
                    offset = self._maintenance_cursor % len(records)
                    records = (records[offset:] + records[:offset])[:16]
                    self._maintenance_cursor += len(records)
                for record in records:
                    if record.job_id in self._tasks or not record.accepted:
                        continue
                    if record.origin_id == self.local.state.ledger.worker_id:
                        if not record.execution_ready:
                            # A crash during acceptance never starts an unconfirmed job.
                            await self.submit(record.submission)
                            continue
                        if record.effective_durability == "replicated":
                            await self.replication.reconcile(record.job_id)
                            record = await self.journal.get(record.job_id)
                            await self.replication.sync(record.job_id)
                        if record.state == "unknown_outcome":
                            policy = record.submission.retry_policy
                            if (
                                record.state == "unknown_outcome"
                                and not record.cancel_requested
                                and policy.safe
                                and policy.allow_possible_duplicates
                                and record.execution_attempt_count < policy.max_attempts
                            ):
                                await self.journal.transition(record.job_id, "retry_pending")
            except asyncio.CancelledError:
                raise
            except Exception:
                log.error("Durable job recovery unavailable")
                await trace_failure(self.local, "job_recovery_unavailable")
            await asyncio.sleep(1)

    async def _loop(self):
        while True:
            try:
                if (await self.local.state.ledger.mode_state()).mode == "paused":
                    await asyncio.sleep(0.25)
                    continue
                for record in await self.journal.list_runnable():
                    if len(self._tasks) >= 16:
                        break
                    if record.job_id in self._tasks:
                        continue
                    if (
                        record.origin_id != self.local.state.ledger.worker_id
                        and not await self.replication.may_run(record)
                    ):
                        continue
                    task = asyncio.create_task(self._execute(record.job_id))
                    self._tasks[record.job_id] = task
                    task.add_done_callback(lambda t, key=record.job_id: self._finished(key, t))
            except asyncio.CancelledError:
                raise
            except Exception:
                # Exception text can include payloads or credentials from lower layers.
                log.error("Durable job scheduling unavailable")
                await trace_failure(self.local, "job_scheduler_unavailable")
            await asyncio.sleep(0.25)

    def _finished(self, job_id, task):
        self._tasks.pop(job_id, None)
        if not task.cancelled() and task.exception() is not None:
            log.error("Durable job task failed; outcome requires journal recovery")
            asyncio.create_task(
                trace_failure(self.local, "job_task_unconfirmed", request_id=job_id)
            )

    async def _execute(self, job_id):
        attempt = None
        lease = None
        dispatched = False
        try:
            attempt = await self.journal.begin_attempt(job_id)
            record = await self.journal.get(job_id)
            await self.replication.sync(job_id)
            spec = record.submission
            policy = spec.execution_policy
            acquisition = AcquireRequest(
                model=spec.model,
                request_id=attempt.attempt_id,
                wait_ms=1000,
                workload=policy.workload,
                idle_only=policy.idle_only,
            )
            while True:
                try:
                    lease = await self.local.state.waiters.acquire(acquisition)
                    break
                except AdmissionError as exc:
                    if exc.reason in {"manual_paused", "manual_offline"}:
                        await asyncio.sleep(0.25)
                        continue
                    if exc.reason != "capacity_wait_timeout":
                        raise
            await self.journal.bind_attempt(
                job_id, attempt.attempt_id, lease.worker_id, lease.reservation_id
            )
            await self.replication.sync(job_id)
            # Persist the attempt/worker before any execution byte is sent.
            dispatched = True
            result = await self._request(spec, lease.token)
            state = (
                "completed"
                if result["status_code"] < 400
                else ("failed" if result["status_code"] < 500 else "unknown_outcome")
            )
            await self.journal.finish_attempt(job_id, attempt.attempt_id, state, result)
        except asyncio.CancelledError:
            if attempt:
                await self._uncertain(job_id, attempt.attempt_id)
            raise
        except Exception:
            if attempt:
                await self._uncertain(job_id, attempt.attempt_id)
            await trace_failure(self.local, "job_execution_unconfirmed", request_id=job_id)
        finally:
            if lease and not dispatched:
                with contextlib.suppress(Exception):
                    await self.local.state.waiters.reservation(
                        lease.reservation_id, lease.token, release=True
                    )
            await self.replication.sync(job_id)

    async def _uncertain(self, job_id, attempt_id):
        record = await self.journal.get(job_id)
        if record.state in {"reserving", "running"}:
            await self.journal.finish_attempt(job_id, attempt_id, "unknown_outcome")

    async def _request(self, spec, token):
        body = json.dumps({**spec.request, "model": spec.model, "stream": False}).encode()
        supplied = False
        pending = asyncio.Event()
        status = 502
        content_type = "application/json"
        result = bytearray()
        complete = False

        async def receive():
            nonlocal supplied
            if not supplied:
                supplied = True
                return {"type": "http.request", "body": body, "more_body": False}
            await pending.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            nonlocal status, content_type, complete
            if message["type"] == "http.response.start":
                status = message["status"]
                content_type = (
                    dict(message.get("headers", []))
                    .get(b"content-type", b"application/json")
                    .decode("latin1")
                )
            elif message["type"] == "http.response.body":
                result.extend(message.get("body", b""))
                if len(result) > MAX_RESULT_BYTES:
                    raise ValueError("job result exceeds budget")
                complete = not message.get("more_body", False)

        await self.local(
            {
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": "POST",
                "scheme": "http",
                "path": spec.path,
                "raw_path": spec.path.encode(),
                "root_path": "",
                "query_string": b"",
                "server": ("127.0.0.1", 11435),
                "client": ("127.0.0.1", 0),
                "headers": [
                    (b"host", b"localhost"),
                    (b"content-type", b"application/json"),
                    (b"x-idle-herd-reservation", token.encode()),
                ],
            },
            receive,
            send,
        )
        if not complete:
            raise ValueError("incomplete job response")
        return {"status_code": status, "content_type": content_type, "body": result.decode()}


router = APIRouter()


@router.post("/v1/jobs", status_code=202)
async def submit_job(spec: JobSubmission, request: Request):
    try:
        record = await request.app.state.jobs.submit(spec)
    except JobConflict as exc:
        raise AdmissionError("job_idempotency_conflict", 409) from exc
    except JobCapacityError as exc:
        raise AdmissionError("job_journal_full", 507) from exc
    return JSONResponse(
        status_code=202,
        content=record.summary(),
        headers={"Location": "/v1/jobs/" + record.job_id},
    )


@router.get("/v1/jobs/{job_id}")
async def get_job(job_id: str, request: Request):
    record = await request.app.state.job_journal.get(job_id)
    if record is None:
        raise AdmissionError("job_not_found", 404)
    return record.summary()


@router.delete("/v1/jobs/{job_id}")
async def cancel_job(job_id: str, request: Request):
    from fleet_manager.local_control.auth import authorize

    authorize(request, control=True)
    return (await request.app.state.jobs.cancel(job_id)).summary()


@router.get("/v1/jobs/{job_id}/result")
async def get_result(job_id: str, request: Request):
    record = await request.app.state.job_journal.get(job_id)
    if record is None:
        raise AdmissionError("job_not_found", 404)
    results = [a for a in record.attempts if a.state == "completed" and a.result is not None]
    if not results:
        raise AdmissionError("job_result_unavailable", 409)
    return {"job_id": job_id, "attempt_id": results[-1].attempt_id, "result": results[-1].result}
