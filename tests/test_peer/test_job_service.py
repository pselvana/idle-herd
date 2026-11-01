"""Durable jobs use existing wire translators and worker fencing."""

import asyncio
import json

import aiosqlite
import pytest

from tests.test_peer.test_app import peer


async def state(client, job_id, expected):
    async with asyncio.timeout(5):
        while True:
            record = (await client.get("/v1/jobs/" + job_id)).json()
            if record["state"] in expected:
                return record
            await asyncio.sleep(0.01)


def submission(**extra):
    return {"model": "tiny", "request": {"messages": []}, "idempotency_key": "job", **extra}


async def test_local_job_persists_result_and_idempotency_after_restart(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (app, client, backend):
        submitted = await client.post("/v1/jobs", json=submission())
        assert submitted.status_code == 202, submitted.text
        job_id = submitted.json()["job_id"]
        assert submitted.headers["location"] == "/v1/jobs/" + job_id
        await state(client, job_id, {"completed"})
        result = (await client.get("/v1/jobs/" + job_id + "/result")).json()
        assert json.loads(result["result"]["body"])["choices"][0]["message"]["content"] == "hello"
        assert len(backend.calls) == 1
        assert "request" not in submitted.json()
        assert "token" not in (await client.get("/v1/jobs/" + job_id)).text
    async with peer(tmp_path, monkeypatch) as (_, client, backend):
        repeated = await client.post("/v1/jobs", json=submission())
        assert repeated.status_code == 202
        assert repeated.json()["job_id"] == job_id
        assert repeated.json()["state"] == "completed"
        assert (await client.get("/v1/jobs/" + job_id + "/result")).json() == result
        conflict = await client.post(
            "/v1/jobs", json=submission(request={"messages": [], "seed": 2})
        )
        assert conflict.status_code == 409
        assert not backend.calls


async def test_replicated_requires_ack_unless_explicit_downgrade(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (app, client, backend):
        failed = await client.post("/v1/jobs", json=submission(durability="replicated"))
        assert failed.status_code == 503, failed.text
        record = (await app.state.job_journal.list_jobs())[0]
        assert not record.accepted
        await asyncio.sleep(0.3)
        assert not backend.calls
        accepted = await client.post(
            "/v1/jobs",
            json=submission(
                idempotency_key="downgrade",
                durability="replicated",
                allow_durability_downgrade=True,
            ),
        )
        assert accepted.status_code == 202, accepted.text
        assert accepted.json()["effective_durability"] == "local"
        await state(client, accepted.json()["job_id"], {"completed"})


async def test_concurrent_submissions_run_once(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (_, client, backend):
        replies = await asyncio.gather(
            *(client.post("/v1/jobs", json=submission()) for _ in range(30))
        )
        assert {r.status_code for r in replies} == {202}
        ids = {r.json()["job_id"] for r in replies}
        assert len(ids) == 1
        await state(client, ids.pop(), {"completed"})
        assert len(backend.calls) == 1


async def test_cancel_running_is_unknown_and_keeps_worker_quarantined(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (app, client, backend):
        backend.block = True
        accepted = await client.post(
            "/v1/jobs",
            json=submission(
                retry_policy={
                    "safe": True,
                    "allow_possible_duplicates": True,
                    "max_attempts": 3,
                    "delay": 1,
                }
            ),
        )
        job_id = accepted.json()["job_id"]
        await asyncio.wait_for(backend.entered.wait(), 5)
        response = await client.delete("/v1/jobs/" + job_id)
        assert response.status_code == 200, response.text
        assert response.json()["state"] == "unknown_outcome"
        await asyncio.sleep(1.3)
        assert len(backend.calls) == 1
        capacity = await app.state.ledger.capacity("tiny:latest")
        assert capacity.quarantined_slots == 1
        assert capacity.observed_available_slots == 0
        backend.finish.set()


async def test_cancel_queued_and_missing_result(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (_, client, backend):
        result = await client.post(
            "/v1/jobs", json=submission(execution_policy={"idle_only": True})
        )
        job_id = result.json()["job_id"]
        canceled = await client.delete("/v1/jobs/" + job_id)
        assert canceled.json()["state"] == "canceled"
        assert (await client.get("/v1/jobs/" + job_id + "/result")).status_code == 409
        assert (await client.get("/v1/jobs/missing")).status_code == 404
        assert not backend.calls


@pytest.mark.parametrize(
    "extra",
    [
        {"path": "/v1/node/mode"},
        {"request": {"stream": True}},
        {"retry_policy": {"max_attempts": 2}},
        {"request": {"tools": []}, "retry_policy": {"safe": True}},
    ],
)
async def test_invalid_or_unsafe_job_rejected(tmp_path, monkeypatch, extra):
    async with peer(tmp_path, monkeypatch) as (_, client, backend):
        response = await client.post("/v1/jobs", json=submission(**extra))
        assert response.status_code == 422, response.text
        assert not backend.calls


async def test_job_limits_and_validation_never_echo_payload(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (_, client, backend):
        secret = "private-prompt-for-validation"
        invalid = await client.post(
            "/v1/jobs", json=submission(request={"model": {}, "prompt": secret})
        )
        assert invalid.status_code == 422
        assert secret not in invalid.text
        oversized = await client.post(
            "/v1/jobs", json=submission(request={"prompt": "x" * (256 * 1024)})
        )
        assert oversized.status_code == 413
        assert not backend.calls


async def test_slow_replication_cannot_block_unrelated_local_job(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (app, client, _):
        entered = asyncio.Event()
        release = asyncio.Event()

        async def slow_copy(job_id):
            entered.set()
            await release.wait()
            return []

        monkeypatch.setattr(app.state.job_replication, "replicate", slow_copy)
        remote = asyncio.create_task(
            client.post(
                "/v1/jobs", json=submission(idempotency_key="slow", durability="replicated")
            )
        )
        try:
            await asyncio.wait_for(entered.wait(), 5)
            local = await asyncio.wait_for(client.post("/v1/jobs", json=submission()), 0.5)
            assert local.status_code == 202
            await state(client, local.json()["job_id"], {"completed"})
        finally:
            release.set()
            assert (await remote).status_code == 503


async def test_failed_accept_commit_never_acknowledges_or_executes(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (app, client, backend):
        commit = app.state.job_journal._db.commit

        async def unavailable():
            raise aiosqlite.OperationalError("injected storage failure")

        monkeypatch.setattr(app.state.job_journal._db, "commit", unavailable)
        response = await client.post("/v1/jobs", json=submission())
        assert response.status_code == 503
        monkeypatch.setattr(app.state.job_journal._db, "commit", commit)
        assert await app.state.job_journal.list_jobs() == []
        assert not backend.calls


async def test_dispatch_boundary_write_failure_never_reaches_backend(tmp_path, monkeypatch):
    async with peer(tmp_path, monkeypatch) as (app, client, backend):

        async def unavailable(*args):
            raise aiosqlite.OperationalError("injected assignment failure")

        monkeypatch.setattr(app.state.job_journal, "bind_attempt", unavailable)
        accepted = await client.post("/v1/jobs", json=submission())
        job_id = accepted.json()["job_id"]
        await state(client, job_id, {"unknown_outcome"})
        async with asyncio.timeout(5):
            while (await app.state.ledger.capacity("tiny:latest")).observed_available_slots != 1:
                await asyncio.sleep(0.01)
        assert not backend.calls
