"""Acceptance certificates and direct-origin evidence must fence replica recovery."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from fleet_manager.peer.identity import IdentityError
from fleet_manager.peer.jobs import JobJournal, JobSubmission, RetryPolicy
from fleet_manager.peer.models import AdmissionError
from fleet_manager.peer.replication import JobReplication, ReplicaAck, StoreRequest


@pytest.fixture
async def copies(tmp_path):
    origin = await JobJournal(tmp_path / "origin.sqlite").open()
    replica = await JobJournal(tmp_path / "replica.sqlite").open()
    peer = SimpleNamespace(worker_id="origin", endpoint="https://192.168.1.2:17435")
    local = SimpleNamespace(
        state=SimpleNamespace(
            job_journal=replica,
            identity=SimpleNamespace(worker_id="replica"),
            ledger=SimpleNamespace(
                worker_id="replica", cluster_participating=AsyncMock(return_value=True)
            ),
            directory=SimpleNamespace(snapshot=lambda: [peer]),
        )
    )
    try:
        yield origin, replica, JobReplication(local), peer
    finally:
        await origin.close()
        await replica.close()


async def accepted(origin, *, replica_count=1):
    record, _ = await origin.submit(
        JobSubmission(
            model="tiny",
            request={},
            durability="replicated",
            replica_count=replica_count,
            retry_policy=RetryPolicy(safe=True, allow_possible_duplicates=True, max_attempts=2),
        ),
        "origin",
    )
    await origin.accept(record.job_id, ["replica", "second"][:replica_count], "replicated")
    return record.job_id


@pytest.mark.parametrize("field", ["job_id", "digest", "worker_id"])
async def test_replica_ack_must_bind_job_snapshot_and_expected_device(copies, field):
    origin, _, service, peer = copies
    job_id = await accepted(origin)
    snapshot = await origin.export_replica(job_id)
    ack = ReplicaAck(job_id=job_id, digest=snapshot.digest, worker_id=peer.worker_id)
    service._rpc = AsyncMock(return_value=ack.model_copy(update={field: "wrong"}))
    with pytest.raises(IdentityError, match="acknowledgment mismatch"):
        await service._store(peer, snapshot)


@pytest.mark.parametrize("mutation", ["missing_local", "duplicate_acks", "local_durability"])
async def test_invalid_acceptance_certificate_never_persists_or_activates(copies, mutation):
    origin, replica, service, _ = copies
    job_id = await accepted(origin, replica_count=2)
    snapshot = await origin.export_replica(job_id)
    if mutation == "missing_local":
        snapshot.record.acks = ["other", "second"]
    elif mutation == "duplicate_acks":
        snapshot.record.acks = ["replica", "replica"]
    else:
        snapshot.record.effective_durability = "local"
    with pytest.raises(AdmissionError, match="invalid_replica_certificate"):
        await service.store("origin", StoreRequest(snapshot=snapshot))
    assert await replica.get(job_id) is None
    assert await replica.list_runnable() == []


@pytest.mark.parametrize("status", [200, 401, 404, 503])
async def test_authenticated_origin_reply_resets_takeover_even_without_a_valid_snapshot(
    copies, status
):
    origin, replica, service, _ = copies
    job_id = await accepted(origin)
    await origin.activate(job_id)
    await origin.begin_attempt(job_id)
    await origin.transition(job_id, "running")
    await replica.import_replica(await origin.export_replica(job_id))
    probe = service._probe_origin
    service._probe_origin = AsyncMock(side_effect=httpx.ConnectError("partition"))
    await service.tick(1)
    await replica._conn().execute("UPDATE replica_recovery SET unreachable_since=0")
    await replica._conn().commit()

    identity = service.local.state.identity
    identity.sign_request = Mock(return_value={"x-idle-nonce": "nonce"})
    identity.verify_response = Mock()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(status, content=b"invalid snapshot"))
    ) as client:
        service.local.state.peer_client = client
        service._probe_origin = probe
        await service.tick(1)
    identity.verify_response.assert_called_once()
    record = await replica.get(job_id)
    assert record.state == "running" and not record.execution_ready
    assert not await service.may_run(record)
    assert await (await replica._conn().execute("SELECT * FROM replica_recovery")).fetchall() == []


async def test_origin_endpoint_loss_discards_old_unreachable_window(copies):
    origin, replica, service, peer = copies
    job_id = await accepted(origin)
    await replica.import_replica(await origin.export_replica(job_id))
    service._probe_origin = AsyncMock(side_effect=httpx.ConnectError("partition"))
    await service.tick(1)
    await replica._conn().execute("UPDATE replica_recovery SET unreachable_since=0")
    await replica._conn().commit()
    service.local.state.directory.snapshot = lambda: []
    await service.tick(1)
    service.local.state.directory.snapshot = lambda: [peer]
    await service.tick(1)
    record = await replica.get(job_id)
    assert not record.execution_ready
    assert not await service.may_run(record)


async def test_store_merges_completed_result_into_an_already_accepted_replica(copies):
    origin, replica, service, _ = copies
    job_id = await accepted(origin)
    await service.store("origin", StoreRequest(snapshot=await origin.export_replica(job_id)))
    await origin.activate(job_id)
    attempt = await origin.begin_attempt(job_id)
    await origin.finish_attempt(job_id, attempt.attempt_id, "completed", {"answer": 42})
    snapshot = await origin.export_replica(job_id)
    ack = await service.store("origin", StoreRequest(snapshot=snapshot))
    assert ack.digest == snapshot.digest
    record = await replica.get(job_id)
    assert record.state == "completed"
    assert record.attempts[0].result == {"answer": 42}
    assert await replica.list_runnable() == []
