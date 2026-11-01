"""Safety tests at the worker authority, independent of routing/queue estimates."""

import asyncio
import sqlite3
from types import SimpleNamespace

import aiosqlite
import pytest

from fleet_manager.peer.local_ledger import LocalLedger
from fleet_manager.peer.models import AcquireRequest, AdmissionError, PeerSettings, ResourceFit


async def eligible(model):
    await asyncio.sleep(0)  # Force races across real async admission interleavings.
    return ResourceFit(model=model, required_gb=1, available_gb=100)


@pytest.fixture
async def ledger(tmp_path):
    worker = LocalLedger(PeerSettings(data_dir=str(tmp_path)), eligible)
    await worker.open()
    try:
        yield worker
    finally:
        await worker.close()


def spec(request_id="request", model="small", **kwargs):
    return AcquireRequest(request_id=request_id, model=model, **kwargs)


@pytest.mark.parametrize("slots", [1, 2, 3])
async def test_one_hundred_origins_cannot_overbook_worker(ledger, slots):
    ledger.settings.slots = slots
    attempts = await asyncio.gather(
        *(ledger.acquire(spec(f"origin-{i % 2}/request-{i}")) for i in range(100)),
        return_exceptions=True,
    )
    granted = [result for result in attempts if not isinstance(result, Exception)]
    assert len(granted) == slots
    assert all(result.reason == "busy" for result in attempts if isinstance(result, AdmissionError))
    assert (await ledger.capacity("small:latest")).observed_available_slots == 0
    await ledger.release(granted[0].token)
    replacement = await ledger.acquire(spec("next"))
    assert replacement.slot_generation > granted[0].slot_generation
    with pytest.raises(AdmissionError, match="stale_generation"):
        await ledger.consume(granted[0].token, "small:latest")


async def test_idempotency_and_consume_once(ledger):
    attempts = await asyncio.gather(*(ledger.acquire(spec()) for _ in range(100)))
    assert len({lease.token for lease in attempts}) == 1
    with pytest.raises(AdmissionError, match="idempotency_conflict"):
        await ledger.acquire(spec(model="other"))
    with pytest.raises(AdmissionError, match="idempotency_conflict"):
        await ledger.acquire(spec(ttl_ms=100))
    consumed = await asyncio.gather(
        *(ledger.consume(attempts[0].token, "small:latest") for _ in range(100)),
        return_exceptions=True,
    )
    assert sum(not isinstance(result, Exception) for result in consumed) == 1
    with pytest.raises(AdmissionError, match="execution_not_confirmed_stopped"):
        await ledger.release(attempts[0].token)
    with pytest.raises(AdmissionError, match="request_already_used"):
        await ledger.acquire(spec())


async def test_reject_forged_wrong_model_and_wrong_worker_tokens(ledger, tmp_path):
    lease = await ledger.acquire(spec())
    with pytest.raises(AdmissionError, match="invalid_reservation"):
        await ledger.consume(lease.token + "forged", "small:latest")
    with pytest.raises(AdmissionError, match="reservation_model_mismatch"):
        await ledger.consume(lease.token, "other:latest")
    other = LocalLedger(PeerSettings(data_dir=str(tmp_path / "other")), eligible)
    await other.open()
    try:
        with pytest.raises(AdmissionError, match="invalid_reservation"):
            await other.consume(lease.token, "small:latest")
    finally:
        await other.close()


async def test_ttl_only_expires_unconsumed_work(ledger, monkeypatch):
    clock = SimpleNamespace(time=lambda: 1000)
    monkeypatch.setattr("fleet_manager.peer.local_ledger.time", clock)
    reserved = await ledger.acquire(spec(ttl_ms=100))
    clock.time = lambda: 1001
    with pytest.raises(AdmissionError, match="reservation_not_active"):
        await ledger.consume(reserved.token, "small:latest")
    running = await ledger.acquire(spec("running", ttl_ms=100))
    await ledger.consume(running.token, running.model)
    clock.time = lambda: 2000
    assert (await ledger.capacity(running.model)).observed_available_slots == 0
    await ledger.finish(running.token, stopped=False)
    assert (await ledger.capacity(running.model)).quarantined_slots == 1


async def test_committed_before_reply_and_restart_fences_old_incarnation(ledger, tmp_path):
    lease = await ledger.acquire(spec())
    # A separate SQLite reader observes the acknowledged durable grant.
    async with aiosqlite.connect(tmp_path / "worker.sqlite") as reader:
        row = await (await reader.execute("SELECT token FROM leases")).fetchone()
        assert row[0] == lease.token
        pragma = await (await ledger._db.execute("PRAGMA synchronous")).fetchone()
        assert pragma[0] == 2  # FULL
    identity = ledger.worker_id
    await ledger.close()
    restarted = LocalLedger(ledger.settings, eligible)
    await restarted.open()
    try:
        assert restarted.worker_id == identity
        assert restarted.incarnation != lease.worker_incarnation
        for action in (restarted.inspect, restarted.release):
            with pytest.raises(AdmissionError, match="stale_incarnation"):
                await action(lease.token)
        with pytest.raises(AdmissionError, match="stale_incarnation"):
            await restarted.consume(lease.token, lease.model)
        replacement = await restarted.acquire(spec("replacement"))
        assert replacement.slot_generation > lease.slot_generation
    finally:
        await restarted.close()


async def test_orphan_execution_keeps_budget_across_restart(ledger):
    lease = await ledger.acquire(spec())
    await ledger.consume(lease.token, lease.model)
    await ledger.close()
    restarted = LocalLedger(ledger.settings, eligible)
    await restarted.open()
    try:
        capacity = await restarted.capacity(lease.model)
        assert capacity.quarantined_slots == 1
        assert capacity.observed_available_slots == 0
        with pytest.raises(AdmissionError, match="busy"):
            await restarted.acquire(spec("after-crash"))
        await restarted.reconcile_stopped_backend()
        next_lease = await restarted.acquire(spec("after-reconciliation"))
        assert next_lease.slot_generation > lease.slot_generation
    finally:
        await restarted.close()


async def test_second_authority_excluded(ledger):
    other = LocalLedger(ledger.settings, eligible)
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        await other.open()
    # The failed open did not revoke the original authority.
    assert (await ledger.acquire(spec())).worker_id == ledger.worker_id


async def test_shared_model_memory_and_model_slot_limits(tmp_path):
    settings = PeerSettings(data_dir=str(tmp_path), slots=4, model_slots={"small": 1})

    async def memory_fit(model):
        return ResourceFit(model=model, required_gb=2, available_gb=4)

    ledger = LocalLedger(settings, memory_fit)
    await ledger.open()
    try:
        await ledger.acquire(spec("a"))
        with pytest.raises(AdmissionError, match="busy"):
            await ledger.acquire(spec("b"))
        await ledger.acquire(spec("c", "other"))
        with pytest.raises(AdmissionError, match="insufficient_memory"):
            await ledger.acquire(spec("d", "third"))
        assert (await ledger.capacity("third:latest")).observed_available_slots == 0
    finally:
        await ledger.close()


async def test_consume_rechecks_live_memory_and_backend(ledger):
    lease = await ledger.acquire(spec())

    async def no_memory(model):
        return ResourceFit(model=model, required_gb=1, available_gb=0)

    ledger.fit_check = no_memory
    with pytest.raises(AdmissionError, match="insufficient_memory"):
        await ledger.consume(lease.token, lease.model)

    async def backend_down(model):
        raise AdmissionError("backend_unavailable", 503)

    ledger.fit_check = backend_down
    with pytest.raises(AdmissionError, match="backend_unavailable"):
        await ledger.consume(lease.token, lease.model)
    assert (await ledger.inspect(lease.token)).state == "reserved"
