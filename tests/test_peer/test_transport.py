"""Backend outcomes, cancellation and durable-before-dispatch failure windows."""

import asyncio
import sqlite3

import aiosqlite
import anyio
import httpx
import pytest

from fleet_manager.peer.backend import AdmissionTransport
from fleet_manager.peer.local_ledger import LocalLedger
from fleet_manager.peer.models import AcquireRequest, PeerSettings, ResourceFit


async def eligible(model):
    return ResourceFit(model=model, required_gb=1, available_gb=100)


@pytest.fixture
async def ledger(tmp_path):
    worker = LocalLedger(PeerSettings(data_dir=str(tmp_path)), eligible)
    await worker.open()
    try:
        yield worker
    finally:
        await worker.close()


class Wire(httpx.AsyncByteStream):
    def __init__(self, chunks, started=None):
        self.chunks = chunks
        self.started = started

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk
        if self.started:
            self.started.set()
            await asyncio.Event().wait()

    async def aclose(self):
        pass


@pytest.mark.parametrize(
    "status,chunks,free",
    [
        (200, [b'{"done":', b"true}\n"], True),
        (200, [b'{"done":true}'], True),
        (200, [b'{"done":false}\n'], False),
        (200, [b"not JSON\n"], False),
        (200, [b""], False),
        (400, [b'{"error":"bad request"}'], True),
        (500, [b'{"error":"failed"}'], False),
    ],
)
async def test_completion_and_unknown_outcomes(ledger, status, chunks, free):
    async def backend(request):
        return httpx.Response(status, stream=Wire(chunks))

    async with httpx.AsyncClient(
        transport=AdmissionTransport(ledger, httpx.MockTransport(backend)),
        base_url="http://localhost",
    ) as client:
        await client.post("/api/chat", json={"model": "tiny"})
    snapshot = await ledger.capacity("tiny:latest")
    assert snapshot.observed_available_slots == int(free)
    assert snapshot.quarantined_slots == int(not free)


@pytest.mark.parametrize("stage", ["dispatch", "body"])
async def test_cancel_or_disconnect_keeps_execution_budget(ledger, stage):
    started = asyncio.Event()

    async def backend(request):
        if stage == "dispatch":
            started.set()
            await asyncio.Event().wait()
        return httpx.Response(200, stream=Wire([b'{"done":false}\n'], started))

    async with httpx.AsyncClient(
        transport=AdmissionTransport(ledger, httpx.MockTransport(backend)),
        base_url="http://localhost",
    ) as client:
        execution = asyncio.create_task(client.post("/api/chat", json={"model": "tiny"}))
        await asyncio.wait_for(started.wait(), 5)
        execution.cancel()
        with pytest.raises(asyncio.CancelledError):
            await execution
    snapshot = await ledger.capacity("tiny:latest")
    assert snapshot.observed_available_slots == 0
    assert snapshot.quarantined_slots == 1


async def test_timeout_after_dispatch_is_quarantined(ledger):
    async def backend(request):
        raise httpx.ReadTimeout("no response", request=request)

    async with httpx.AsyncClient(
        transport=AdmissionTransport(ledger, httpx.MockTransport(backend)),
        base_url="http://localhost",
    ) as client:
        with pytest.raises(httpx.ReadTimeout):
            await client.post("/api/embed", json={"model": "tiny", "input": "hi"})
    assert (await ledger.capacity("tiny:latest")).quarantined_slots == 1


async def test_level_cancellation_before_backend_headers_still_commits_quarantine(ledger):
    started = asyncio.Event()

    async def backend(request):
        started.set()
        await asyncio.Event().wait()

    async with httpx.AsyncClient(
        transport=AdmissionTransport(ledger, httpx.MockTransport(backend)),
        base_url="http://localhost",
    ) as client:

        async def execute():
            await client.post("/api/chat", json={"model": "tiny"})

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(execute)
            await asyncio.wait_for(started.wait(), 2)
            tasks.cancel_scope.cancel()
    capacity = await ledger.capacity("tiny:latest")
    assert capacity.quarantined_slots == 1
    assert capacity.observed_available_slots == 0


async def test_cancel_during_admission_rolls_back_and_unblocks_next_client(ledger):
    started = asyncio.Event()

    async def slow_fit(model):
        started.set()
        await asyncio.Event().wait()

    ledger.fit_check = slow_fit
    admission = asyncio.create_task(
        ledger.acquire(AcquireRequest(model="tiny", request_id="cancel"))
    )
    await asyncio.wait_for(started.wait(), 5)
    admission.cancel()
    with pytest.raises(asyncio.CancelledError):
        await admission
    ledger.fit_check = eligible
    assert (await ledger.capacity("tiny:latest")).observed_available_slots == 1
    assert (
        await ledger.acquire(AcquireRequest(model="tiny", request_id="next"))
    ).slot_generation == 1


async def test_failed_consume_commit_never_dispatches_backend(ledger, monkeypatch):
    commit = ledger._db.commit
    commits = 0
    called = False

    async def fail_consume_commit():
        nonlocal commits
        commits += 1
        if commits == 2:
            raise sqlite3.OperationalError("simulated disk full before consume commit")
        await commit()

    async def backend(request):
        nonlocal called
        called = True
        return httpx.Response(200, stream=Wire([b'{"done":true}\n']))

    monkeypatch.setattr(ledger._db, "commit", fail_consume_commit)
    async with httpx.AsyncClient(
        transport=AdmissionTransport(ledger, httpx.MockTransport(backend)),
        base_url="http://localhost",
    ) as client:
        with pytest.raises(sqlite3.OperationalError, match="disk full"):
            await client.post("/api/chat", json={"model": "tiny"})
    assert not called
    rows = await (await ledger._db.execute("SELECT state FROM leases")).fetchall()
    assert [row[0] for row in rows] == ["reserved"]


async def test_backend_observes_durably_consumed_lease_before_execution(ledger, tmp_path):
    async def backend(request):
        async with aiosqlite.connect(tmp_path / "worker.sqlite") as reader:
            rows = await (await reader.execute("SELECT state FROM leases")).fetchall()
            assert [row[0] for row in rows] == ["running"]
        return httpx.Response(200, stream=Wire([b'{"done":true}\n']))

    async with httpx.AsyncClient(
        transport=AdmissionTransport(ledger, httpx.MockTransport(backend)),
        base_url="http://localhost",
    ) as client:
        assert (await client.post("/api/chat", json={"model": "tiny"})).status_code == 200
