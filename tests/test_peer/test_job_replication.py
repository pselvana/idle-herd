from types import SimpleNamespace

import pytest

from fleet_manager.peer.jobs import JobJournal, JobSubmission
from fleet_manager.peer.models import AdmissionError
from fleet_manager.peer.replication import FetchRequest, JobReplication, StoreRequest


@pytest.mark.asyncio
async def test_replica_requires_origin_and_limits_reads(tmp_path):
    origin = await JobJournal(tmp_path / "origin.db").open()
    replica = await JobJournal(tmp_path / "replica.db").open()
    service = JobReplication(
        SimpleNamespace(
            state=SimpleNamespace(
                job_journal=replica, identity=SimpleNamespace(worker_id="replica")
            )
        )
    )
    try:
        record, _ = await origin.submit(
            JobSubmission(
                model="test",
                request={"model": "test"},
                idempotency_key="key",
                durability="replicated",
            ),
            "origin",
        )
        prepared = await origin.export_replica(record.job_id)
        with pytest.raises(AdmissionError):
            await service.store("other", StoreRequest(snapshot=prepared))
        ack = await service.store("origin", StoreRequest(snapshot=prepared))
        assert (ack.worker_id, ack.digest) == ("replica", prepared.digest)
        assert await replica.list_runnable() == []
        with pytest.raises(AdmissionError):
            await service.fetch("origin", FetchRequest(job_id=record.job_id))
        await origin.accept(record.job_id, ["replica"], "replicated")
        accepted = await origin.export_replica(record.job_id)
        await service.store("origin", StoreRequest(snapshot=accepted))
        assert await replica.list_runnable() == []
        with pytest.raises(AdmissionError):
            await service.fetch("other", FetchRequest(job_id=record.job_id))
        assert (await service.fetch("origin", FetchRequest(job_id=record.job_id))).record.accepted
    finally:
        await origin.close()
        await replica.close()


@pytest.mark.asyncio
async def test_disabled_replication_allows_local_jobs(tmp_path):
    journal = await JobJournal(tmp_path / "jobs.db").open()
    service = JobReplication(
        SimpleNamespace(
            state=SimpleNamespace(
                job_journal=journal,
                identity=None,
                directory=None,
                ledger=SimpleNamespace(worker_id="local"),
            )
        )
    )
    try:
        record, _ = await journal.submit(
            JobSubmission(model="test", request={"model": "test"}, idempotency_key="key"), "local"
        )
        assert await service.replicate(record.job_id) == []
        assert await service.sync(record.job_id) == []
        await service.tick()
        assert await service.may_run(record)
    finally:
        await journal.close()


async def test_accepted_sync_counts_distinct_workers_not_endpoints(tmp_path, monkeypatch):
    journal = await JobJournal(tmp_path / "jobs.db").open()
    service = JobReplication(
        SimpleNamespace(
            state=SimpleNamespace(
                job_journal=journal,
                identity=SimpleNamespace(worker_id="origin"),
                directory=object(),
            )
        )
    )
    try:
        record, _ = await journal.submit(
            JobSubmission(
                model="test",
                request={},
                durability="replicated",
                replica_count=2,
            ),
            "origin",
        )
        await journal.accept(record.job_id, ["copy-a", "copy-b"], "replicated")
        monkeypatch.setattr(
            service,
            "_peers",
            lambda: [
                SimpleNamespace(worker_id="copy-a", endpoint="https://127.0.0.1:18000"),
                SimpleNamespace(worker_id="copy-a", endpoint="https://127.0.0.1:18001"),
            ],
        )

        async def copied(peer, snapshot):
            return peer.worker_id

        monkeypatch.setattr(service, "_store", copied)
        assert await service.sync(record.job_id) == ["copy-a"]
        assert not (await journal.get(record.job_id)).execution_ready
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_running_replica_waits_then_schedules_safe_retry(tmp_path):
    import httpx

    from fleet_manager.peer.jobs import RetryPolicy

    origin = await JobJournal(tmp_path / "origin.db").open()
    replica = await JobJournal(tmp_path / "replica.db").open()
    peer = SimpleNamespace(worker_id="origin", endpoint="https://127.0.0.1:1234")
    service = JobReplication(
        SimpleNamespace(
            state=SimpleNamespace(
                job_journal=replica,
                identity=SimpleNamespace(worker_id="replica"),
                directory=SimpleNamespace(snapshot=lambda: [peer]),
            )
        )
    )

    async def unavailable(*args):
        raise httpx.ConnectError("offline")

    service._probe_origin = unavailable
    try:
        record, _ = await origin.submit(
            JobSubmission(
                model="test",
                request={"model": "test"},
                idempotency_key="key",
                durability="replicated",
                retry_policy=RetryPolicy(
                    safe=True, allow_possible_duplicates=True, max_attempts=2, delay=1
                ),
            ),
            "origin",
        )
        await origin.accept(record.job_id, ["replica"], "replicated")
        await origin.activate(record.job_id)
        await origin.begin_attempt(record.job_id)
        await origin.transition(record.job_id, "running")
        await replica.import_replica(await origin.export_replica(record.job_id))
        await service.tick(1)
        assert not await service.may_run(await replica.get(record.job_id))
        async with replica._lock:
            await replica._conn().execute(
                "UPDATE replica_recovery SET unreachable_since=0 WHERE job_id=?", (record.job_id,)
            )
            await replica._conn().commit()
        await service.tick(1)
        recovered = await replica.get(record.job_id)
        assert recovered.state == "retry_pending"
        assert recovered.attempts[0].state == "unknown_outcome"
        assert recovered.execution_ready
        assert await service.may_run(recovered)
    finally:
        await origin.close()
        await replica.close()


@pytest.mark.asyncio
async def test_signed_service_acceptance_sync_and_survivor_execution(tmp_path, monkeypatch):
    import asyncio
    import contextlib

    from tests.test_peer.test_capacity_integration import signed_mesh

    async with signed_mesh(tmp_path, monkeypatch) as (apps, clients, _, endpoints, down):
        for app in apps:
            app.state.jobs._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await app.state.jobs._task
        origin, survivor = apps
        remote = survivor.state.job_replication
        store = remote.store

        async def fail_accepted(principal, request):
            if request.snapshot.record.accepted:
                raise AdmissionError("storage_failure", 503)
            return await store(principal, request)

        monkeypatch.setattr(remote, "store", fail_accepted)
        request = {
            "model": "tiny",
            "request": {"messages": []},
            "idempotency_key": "mesh",
            "durability": "replicated",
            "retry_policy": {
                "safe": True,
                "allow_possible_duplicates": True,
                "max_attempts": 2,
                "delay": 1,
            },
        }
        response = await clients[0].post("/v1/jobs", json=request)
        assert response.status_code == 503, response.text
        record = (await origin.state.job_journal.list_jobs())[0]
        staged = await survivor.state.job_journal.get(record.job_id)
        assert record.accepted and not record.execution_ready
        assert staged and not staged.accepted
        monkeypatch.setattr(remote, "store", store)
        response = await clients[0].post("/v1/jobs", json=request)
        assert response.status_code == 202, response.text
        replica = await survivor.state.job_journal.get(record.job_id)
        assert replica.accepted and not replica.execution_ready
        # The acknowledged record is visible to a second connection to the durable database.
        import aiosqlite

        async with aiosqlite.connect(survivor.state.job_journal.path) as db:
            row = await (
                await db.execute("SELECT body FROM jobs WHERE job_id=?", (record.job_id,))
            ).fetchone()
            assert '"accepted":true' in row[0]
        down.add(endpoints[0])
        await remote.tick(1)
        async with survivor.state.job_journal._lock:
            db = survivor.state.job_journal._conn()
            await db.execute("UPDATE replica_recovery SET unreachable_since=0")
            await db.commit()
        await remote.tick(1)
        recovered = await survivor.state.job_journal.get(record.job_id)
        assert await remote.may_run(recovered)
        await survivor.state.jobs._execute(record.job_id)
        completed = await clients[1].get("/v1/jobs/" + record.job_id + "/result")
        assert completed.status_code == 200, completed.text
        assert len((await survivor.state.job_journal.get(record.job_id)).attempts) == 1


@pytest.mark.asyncio
async def test_signed_completed_result_replication_and_unsafe_no_takeover(tmp_path, monkeypatch):
    import asyncio
    import contextlib

    from tests.test_peer.test_capacity_integration import signed_mesh

    async with signed_mesh(tmp_path, monkeypatch) as (apps, clients, _, endpoints, down):
        for app in apps:
            app.state.jobs._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await app.state.jobs._task
        origin, survivor = apps
        request = {
            "model": "tiny",
            "request": {"messages": []},
            "idempotency_key": "completed",
            "durability": "replicated",
        }
        response = await clients[0].post("/v1/jobs", json=request)
        assert response.status_code == 202, response.text
        job_id = response.json()["job_id"]
        await origin.state.jobs._execute(job_id)
        remote_result = await clients[1].get("/v1/jobs/" + job_id + "/result")
        assert remote_result.status_code == 200, remote_result.text
        assert "hello" in remote_result.json()["result"]["body"]
        request["idempotency_key"] = "unsafe"
        response = await clients[0].post("/v1/jobs", json=request)
        assert response.status_code == 202, response.text
        unsafe_id = response.json()["job_id"]
        down.add(endpoints[0])
        await survivor.state.job_replication.tick(1)
        unsafe = await survivor.state.job_journal.get(unsafe_id)
        assert not unsafe.execution_ready
        assert not await survivor.state.job_replication.may_run(unsafe)
