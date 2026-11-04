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


@pytest.mark.parametrize(
    "terminal,recovers", [(b'{"done":true}\n', True), (b'{"done":false}\n', False)]
)
async def test_disconnected_response_recovers_only_with_original_completion(
    ledger, terminal, recovers
):
    release = asyncio.Event()

    class LateResponse(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"done":false}\n'
            await release.wait()
            yield terminal
            if recovers:
                # A terminal record is proof even if the socket stays open.
                await asyncio.Event().wait()

        async def aclose(self):
            pass

    async with httpx.AsyncClient(
        transport=AdmissionTransport(
            ledger, httpx.MockTransport(lambda request: httpx.Response(200, stream=LateResponse()))
        ),
        base_url="http://localhost",
    ) as client:
        async with client.stream("POST", "/api/chat", json={"model": "tiny"}) as response:
            async for _chunk in response.aiter_bytes():
                break
        assert (await ledger.capacity("tiny:latest")).quarantined_slots == 1
        release.set()
        async with asyncio.timeout(2):
            while True:
                view = await ledger.capacity("tiny:latest")
                if view.quarantined_slots == int(not recovers) and (
                    recovers or not client._transport._streams
                ):
                    break
                await asyncio.sleep(0.01)
        assert view.observed_available_slots == int(recovers)


async def test_recovery_timeout_never_frees_unconfirmed_execution(ledger):
    ledger.settings.quarantine_drain_seconds = 0.03
    stopped = asyncio.Event()

    class HungResponse(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"done":false}\n'
            await asyncio.Event().wait()

        async def aclose(self):
            stopped.set()

    transport = AdmissionTransport(
        ledger, httpx.MockTransport(lambda request: httpx.Response(200, stream=HungResponse()))
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
        async with client.stream("POST", "/api/chat", json={"model": "tiny"}) as response:
            async for _chunk in response.aiter_bytes():
                break
        await asyncio.wait_for(stopped.wait(), 1)
        view = await ledger.capacity("tiny:latest")
        assert view.quarantined_slots == 1 and view.observed_available_slots == 0
        assert not transport._streams


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


@pytest.mark.parametrize("done", [1, "true", None, [], {}])
async def test_only_boolean_terminal_is_completion_proof(ledger, done):
    import json

    transport = AdmissionTransport(
        ledger,
        httpx.MockTransport(
            lambda request: httpx.Response(200, stream=Wire([json.dumps({"done": done}).encode()]))
        ),
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
        await client.post("/api/chat", json={"model": "tiny"})
    assert (await ledger.capacity("tiny:latest")).quarantined_slots == 1


@pytest.mark.parametrize("path", ["/api/embed", "/api/embeddings"])
async def test_disconnected_embedding_requires_complete_original_body(ledger, path):
    release = asyncio.Event()
    closed = asyncio.Event()

    class Embedding(httpx.AsyncByteStream):
        async def __aiter__(self):
            # A done-looking fragment is not completion proof for synchronous embeddings.
            yield b'{"done":true,"embeddings":['
            await release.wait()
            yield b"[1.0]]}"

        async def aclose(self):
            closed.set()

    transport = AdmissionTransport(
        ledger, httpx.MockTransport(lambda request: httpx.Response(200, stream=Embedding()))
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
        async with client.stream("POST", path, json={"model": "tiny"}) as response:
            async for _ in response.aiter_bytes():
                break
        assert (await ledger.capacity("tiny:latest")).quarantined_slots == 1
        assert len(transport._streams) == 1
        release.set()
        await asyncio.wait_for(closed.wait(), 2)
        assert (await ledger.capacity("tiny:latest")).observed_available_slots == 1
        assert not transport._streams


async def test_shutdown_cancels_original_pending_read_and_preserves_quarantine(ledger):
    reading = asyncio.Event()
    cancelled = asyncio.Event()
    closed = asyncio.Event()

    class Pending(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"done":false}\n'
            reading.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        async def aclose(self):
            closed.set()

    transport = AdmissionTransport(
        ledger, httpx.MockTransport(lambda request: httpx.Response(200, stream=Pending()))
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
        async with client.stream("POST", "/api/chat", json={"model": "tiny"}) as response:
            async for _ in response.aiter_bytes():
                break
        await asyncio.wait_for(reading.wait(), 2)
        stream = next(iter(transport._streams))
        assert transport.recovery_deadline(stream.reservation_id) is not None
    assert cancelled.is_set() and closed.is_set()
    assert not transport._streams
    assert transport.recovery_deadline(stream.reservation_id) is None
    assert (await ledger.capacity("tiny:latest")).quarantined_slots == 1
    # Repeated cleanup must not change the charge or resurrect a detached reader.
    await stream.aclose()
    await stream.shutdown()
    assert (await ledger.capacity("tiny:latest")).quarantined_slots == 1


@pytest.mark.parametrize("status", [500, 503])
async def test_error_response_cannot_free_slot_even_with_terminal_record(ledger, status):
    transport = AdmissionTransport(
        ledger,
        httpx.MockTransport(
            lambda request: httpx.Response(status, stream=Wire([b'{"done":true}\n']))
        ),
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
        await client.post("/api/chat", json={"model": "tiny"})
    view = await ledger.capacity("tiny:latest")
    assert view.quarantined_slots == 1 and view.observed_available_slots == 0
