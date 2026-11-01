import asyncio

import pytest
from pydantic import ValidationError

from fleet_manager.peer.jobs import (
    JobCapacityError,
    JobConflict,
    JobJournal,
    JobSubmission,
    RetryPolicy,
)


def submission(**kwargs):
    return JobSubmission(model="llama", request={"messages": []}, idempotency_key="key", **kwargs)


@pytest.mark.asyncio
async def test_idempotency_and_restart_recovery(tmp_path):
    journal = await JobJournal(tmp_path / "jobs.sqlite").open()
    records = await asyncio.gather(*(journal.submit(submission(), "origin") for _ in range(20)))
    assert sum(created for _, created in records) == 1
    job = records[0][0]
    assert not await journal.list_runnable()
    with pytest.raises(JobConflict):
        await journal.submit(submission(priority=1), "origin")
    await journal.accept(job.job_id, [], "local")
    attempt = await journal.begin_attempt(job.job_id)
    await journal.bind_attempt(job.job_id, attempt.attempt_id, "worker", "lease")
    await journal.close()
    journal = await JobJournal(tmp_path / "jobs.sqlite").open()
    recovered = await journal.get(job.job_id)
    assert recovered.state == "unknown_outcome"
    assert recovered.attempts[0].state == "unknown_outcome"
    with pytest.raises(JobConflict):
        await journal.transition(job.job_id, "retry_pending")
    await journal.close()


@pytest.mark.asyncio
async def test_replica_acceptance_and_tampering(tmp_path):
    origin = await JobJournal(tmp_path / "origin").open()
    replica = await JobJournal(tmp_path / "replica").open()
    job, _ = await origin.submit(submission(durability="replicated"), "origin")
    prepared = await origin.export_replica(job.job_id)
    await replica.import_replica(prepared)
    assert not await replica.list_runnable()
    with pytest.raises(JobConflict):
        await origin.accept(job.job_id, [], "replicated")
    with pytest.raises(JobConflict):
        await origin.accept(job.job_id, [], "local")
    await origin.accept(job.job_id, ["peer"], "replicated")
    accepted = await origin.export_replica(job.job_id)
    await replica.import_replica(accepted)
    assert (await replica.get(job.job_id)).accepted
    assert not await replica.list_runnable()
    with pytest.raises(JobConflict):
        await replica.begin_attempt(job.job_id)
    await replica.activate(job.job_id)
    assert len(await replica.list_runnable()) == 1
    accepted.record.submission.request["secret"] = "tampered"
    with pytest.raises(JobConflict):
        await replica.import_replica(accepted)
    await origin.close()
    await replica.close()


@pytest.mark.asyncio
async def test_budget_and_retry_safety(tmp_path):
    journal = await JobJournal(tmp_path / "journal", max_jobs=1).open()
    job, _ = await journal.submit(submission(), "origin")
    with pytest.raises(JobCapacityError):
        await journal.submit(
            JobSubmission(model="x", request={}, idempotency_key="another"), "origin"
        )
    assert await journal.get(job.job_id)
    with pytest.raises(ValidationError):
        RetryPolicy(safe=True, max_attempts=2)
    with pytest.raises(ValidationError):
        JobSubmission(model="x", request={"tools": []}, retry_policy=RetryPolicy(safe=True))
    await journal.close()


@pytest.mark.asyncio
async def test_reconcile_independent_attempts_preserves_active_and_results(tmp_path):
    from fleet_manager.peer.jobs import JobAttempt

    origin = await JobJournal(tmp_path / "origin").open()
    replica = await JobJournal(tmp_path / "replica").open()
    spec = submission(
        durability="replicated",
        retry_policy=RetryPolicy(safe=True, max_attempts=3, allow_possible_duplicates=True),
    )
    job, _ = await origin.submit(spec, "origin")
    await replica.import_replica(await origin.export_replica(job.job_id))
    await origin.accept(job.job_id, ["replica"], "replicated")
    await replica.import_replica(await origin.export_replica(job.job_id))
    await origin.activate(job.job_id)
    local_attempt = await origin.begin_attempt(job.job_id)
    snapshot = await replica.export_replica(job.job_id)
    snapshot.record.attempts.append(
        JobAttempt(
            attempt_id="remote-attempt",
            state="completed",
            started_at=1,
            finished_at=2,
            result={"answer": 42},
        )
    )
    snapshot.record.state = "completed"
    from fleet_manager.peer.jobs import _digest

    snapshot.digest = _digest(snapshot.record)
    merged = await origin.reconcile_snapshot(snapshot)
    assert merged.state == "reserving"
    assert merged.attempts[-1].attempt_id == local_attempt.attempt_id
    assert merged.attempts[0].result == {"answer": 42}
    await origin.finish_attempt(job.job_id, local_attempt.attempt_id, "unknown_outcome")
    merged = await origin.reconcile_snapshot(snapshot)
    assert merged.state == "completed"
    assert len(merged.attempts) == 2
    await origin.close()
    await replica.close()


@pytest.mark.asyncio
async def test_reconcile_conflicting_result_is_unknown(tmp_path):
    from fleet_manager.peer.jobs import _digest

    journal = await JobJournal(tmp_path / "journal").open()
    job, _ = await journal.submit(submission(), "origin")
    await journal.accept(job.job_id, [], "local")
    attempt = await journal.begin_attempt(job.job_id)
    await journal.finish_attempt(job.job_id, attempt.attempt_id, "completed", {"value": 1})
    snapshot = await journal.export_replica(job.job_id)
    snapshot.record.attempts[0].result = {"value": 2}
    snapshot.digest = _digest(snapshot.record)
    record = await journal.reconcile_snapshot(snapshot)
    assert record.state == "unknown_outcome"
    assert record.attempts[0].result == {"value": 1}
    assert record.attempts[0].result_conflicts == [{"value": 1}, {"value": 2}]
    await journal.close()


@pytest.mark.asyncio
async def test_retry_budget_and_delay(tmp_path, monkeypatch):
    from fleet_manager.peer import jobs

    now = 1000.0
    monkeypatch.setattr(jobs.time, "time", lambda: now)
    journal = await JobJournal(tmp_path / "journal").open()
    job, _ = await journal.submit(
        submission(
            retry_policy=RetryPolicy(
                safe=True, max_attempts=2, delay=2, allow_possible_duplicates=True
            )
        ),
        "origin",
    )
    await journal.accept(job.job_id, [], "local")
    first = await journal.begin_attempt(job.job_id)
    await journal.finish_attempt(job.job_id, first.attempt_id, "unknown_outcome")
    await journal.transition(job.job_id, "retry_pending")
    with pytest.raises(JobConflict):
        await journal.begin_attempt(job.job_id)
    now += 2
    second = await journal.begin_attempt(job.job_id)
    await journal.finish_attempt(job.job_id, second.attempt_id, "unknown_outcome")
    with pytest.raises(JobConflict):
        await journal.transition(job.job_id, "retry_pending")
    await journal.close()


@pytest.mark.asyncio
async def test_failed_commit_rolls_back_submission(tmp_path, monkeypatch):
    journal = await JobJournal(tmp_path / "journal").open()
    db = journal._conn()
    commit = db.commit

    async def fail_commit():
        raise OSError("simulated disk failure")

    monkeypatch.setattr(db, "commit", fail_commit)
    with pytest.raises(OSError):
        await journal.submit(submission(), "origin")
    monkeypatch.setattr(db, "commit", commit)
    assert await journal.list_jobs() == []
    job, created = await journal.submit(submission(), "origin")
    assert created
    await journal.close()
    journal = await JobJournal(tmp_path / "journal").open()
    assert (await journal.get(job.job_id)).job_id == job.job_id
    await journal.close()


@pytest.mark.asyncio
async def test_cancel_running_is_unknown_and_never_retries(tmp_path):
    journal = await JobJournal(tmp_path / "journal").open()
    job, _ = await journal.submit(
        submission(
            retry_policy=RetryPolicy(safe=True, max_attempts=2, allow_possible_duplicates=True)
        ),
        "origin",
    )
    await journal.accept(job.job_id, [], "local")
    attempt = await journal.begin_attempt(job.job_id)
    await journal.bind_attempt(job.job_id, attempt.attempt_id, "worker", "lease")
    canceled = await journal.cancel(job.job_id)
    assert canceled.cancel_requested
    assert canceled.state == "unknown_outcome"
    assert canceled.attempts[-1].state == "unknown_outcome"
    with pytest.raises(JobConflict):
        await journal.transition(job.job_id, "retry_pending")
    assert await journal.list_runnable() == []
    await journal.close()
    journal = await JobJournal(tmp_path / "journal").open()
    assert (await journal.get(job.job_id)).cancel_requested
    with pytest.raises(JobConflict):
        await journal.transition(job.job_id, "retry_pending")
    await journal.close()


def test_request_model_validation_and_canonical_idempotency():
    first = JobSubmission(model="m", request={"model": "m"}, idempotency_key="id")
    second = JobSubmission(model="m:latest", request={"model": "m:latest"}, idempotency_key="id")
    assert first == second
    for value in (None, 3, {}, []):
        with pytest.raises(ValidationError):
            JobSubmission(model="m", request={"model": value})


@pytest.mark.asyncio
async def test_replica_second_write_failure_rolls_back(tmp_path, monkeypatch):
    origin = await JobJournal(tmp_path / "origin").open()
    replica = await JobJournal(tmp_path / "replica").open()
    job, _ = await origin.submit(submission(), "origin")
    snapshot = await origin.export_replica(job.job_id)
    db = replica._conn()
    execute = db.execute

    def fail_second_update(sql, *args):
        if sql.startswith("UPDATE jobs SET body="):
            raise OSError("simulated second write failure")
        return execute(sql, *args)

    monkeypatch.setattr(db, "execute", fail_second_update)
    with pytest.raises(OSError):
        await replica.import_replica(snapshot)
    monkeypatch.setattr(db, "execute", execute)
    assert await replica.get(job.job_id) is None
    await replica.import_replica(snapshot)
    await replica.close()
    replica = await JobJournal(tmp_path / "replica").open()
    assert await replica.get(job.job_id)
    await origin.close()
    await replica.close()


@pytest.mark.asyncio
async def test_summary_excludes_request_and_result(tmp_path):
    journal = await JobJournal(tmp_path / "journal").open()
    job, _ = await journal.submit(JobSubmission(model="m", request={"secret": "prompt"}), "origin")
    await journal.accept(job.job_id, [], "local")
    attempt = await journal.begin_attempt(job.job_id)
    await journal.bind_attempt(job.job_id, attempt.attempt_id, "worker", "lease")
    record = await journal.finish_attempt(
        job.job_id, attempt.attempt_id, "completed", {"secret": "answer"}
    )
    summary = record.summary()
    assert summary["attempts"][0]["attempt_id"] == attempt.attempt_id
    assert "secret" not in str(summary)
    await journal.close()


@pytest.mark.asyncio
async def test_restart_local_undispatched_admission_can_resume(tmp_path):
    path = tmp_path / "journal"
    journal = await JobJournal(path, worker_id="origin").open()
    job, _ = await journal.submit(submission(), "origin")
    await journal.accept(job.job_id, [], "local")
    first = await journal.begin_attempt(job.job_id)
    await journal.close()
    journal = await JobJournal(path, worker_id="origin").open()
    recovered = await journal.get(job.job_id)
    assert recovered.state == "queued"
    assert recovered.attempts[0].admission_only
    assert recovered.execution_attempt_count == 0
    second = await journal.begin_attempt(job.job_id)
    assert second.attempt_id != first.attempt_id
    await journal.bind_attempt(job.job_id, second.attempt_id, "worker", "lease")
    await journal.close()
    journal = await JobJournal(path, worker_id="origin").open()
    recovered = await journal.get(job.job_id)
    assert recovered.state == "unknown_outcome"
    assert recovered.execution_attempt_count == 1
    with pytest.raises(JobConflict):
        await journal.transition(job.job_id, "retry_pending")
    await journal.close()


@pytest.mark.asyncio
async def test_restart_replica_reserving_is_uncertain(tmp_path):
    path = tmp_path / "journal"
    journal = await JobJournal(path, worker_id="replica").open()
    job, _ = await journal.submit(submission(), "origin")
    await journal.accept(job.job_id, [], "local")
    await journal.begin_attempt(job.job_id)
    await journal.close()
    journal = await JobJournal(path, worker_id="replica").open()
    assert (await journal.get(job.job_id)).state == "unknown_outcome"
    await journal.close()
