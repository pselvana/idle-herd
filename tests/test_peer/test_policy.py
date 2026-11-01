"""Live policy, recovery windows and durable admission under changing host state."""

import asyncio
import time
from contextlib import asynccontextmanager

import aiosqlite
import httpx
import pytest
from pydantic import ValidationError

from fleet_manager.peer.app import create_peer_app
from fleet_manager.peer.local_ledger import LocalLedger
from fleet_manager.peer.models import AcquireRequest, AdmissionError, PeerSettings, ResourceFit
from fleet_manager.peer.policy import IdlePolicy
from fleet_manager.peer.sensors import IdleSignal


class MutableSensor:
    def __init__(self, **fields):
        self.fields = dict(
            source="test",
            confidence="host_sensor",
            idle_seconds=1000,
            cpu_percent=1,
            memory_available_gb=48,
            memory_pressure="normal",
            thermal_state="nominal",
            on_ac=True,
        )
        self.fields.update(fields)
        self.failure = False
        self.delay = 0

    async def sample(self):
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.failure:
            raise OSError("private helper output must not appear")
        return IdleSignal(**({"observed_at": time.time()} | self.fields))


def policy(sensor=None, **settings):
    return IdlePolicy(
        PeerSettings(idle_hysteresis_seconds=0, **settings), sensor or MutableSensor()
    )


@pytest.mark.parametrize("idle", [None, 1000])
async def test_unknown_confidence_cannot_grant_idle_only_even_with_idle_duration(idle):
    rule = policy(MutableSensor(confidence="unknown", idle_seconds=idle))
    decision = await rule.decision("background")
    assert not decision.eligible and decision.reason == "activity_unknown"
    assert decision.state == "unknown"
    assert (await rule.decision("interactive")).eligible
    assert not (await rule.decision("interactive", True)).eligible


async def test_explicit_headless_utilization_has_truthful_unknown_activity():
    rule = policy(
        MutableSensor(confidence="unknown", idle_seconds=None),
        background_policy="utilization_only",
    )
    decision = await rule.decision("background")
    assert decision.eligible and decision.activity == "unknown"
    assert decision.policy == "utilization_only"
    assert not (await rule.decision("background", True)).eligible


@pytest.mark.parametrize(
    ("fields", "reason"),
    [
        ({"idle_seconds": 0}, "owner_active"),
        ({"cpu_percent": 25}, "cpu_busy"),
        ({"cpu_percent": None}, "utilization_unknown"),
        ({"memory_available_gb": None}, "utilization_unknown"),
        ({"memory_available_gb": 1}, "memory_pressure"),
        ({"memory_pressure": "warning"}, "memory_pressure"),
        ({"memory_pressure": "critical"}, "memory_pressure"),
        ({"thermal_state": "serious"}, "thermal_limit"),
        ({"thermal_state": "critical"}, "thermal_limit"),
        ({"on_ac": False, "battery_percent": 10}, "battery_low"),
        ({"on_ac": None, "battery_percent": 10}, "battery_low"),
        ({"gpu_percent": 25}, "gpu_busy"),
        ({"observed_at": time.time() - 100}, "sensor_stale"),
        ({"observed_at": time.time() + 3600}, "sensor_stale"),
    ],
)
async def test_safeguards_reject_new_work(fields, reason):
    decision = await policy(MutableSensor(**fields)).decision("background")
    assert not decision.eligible and decision.reason == reason


async def test_power_and_optional_unknowns():
    sensor = MutableSensor(on_ac=None, battery_percent=None, thermal_state="unknown")
    assert (await policy(sensor).decision("background")).eligible
    assert (await policy(sensor, require_ac=True).decision("background")).reason == "ac_required"


@pytest.mark.parametrize("mode", ["manual", "never"])
async def test_caller_cannot_override_owner_policy(mode):
    rule = policy(interactive_policy=mode, background_policy=mode)
    for kind in ("interactive", "background"):
        for strict in (False, True):
            assert not (await rule.decision(kind, strict)).eligible


async def test_helper_failure_fails_closed_for_idle_and_utilization_but_keeps_local_always(caplog):
    sensor = MutableSensor()
    sensor.failure = True
    rule = policy(sensor)
    assert (await rule.decision("background")).reason == "activity_unknown"
    assert (await rule.decision("interactive")).eligible  # backend still checks live model/memory
    assert not (
        await policy(sensor, background_policy="utilization_only").decision("background")
    ).eligible
    assert "private helper output" not in caplog.text


async def test_hysteresis_immediate_drain_recovery_and_sleep_gap():
    sensor = MutableSensor()
    clock = [0.0]
    rule = IdlePolicy(PeerSettings(idle_hysteresis_seconds=4), sensor, clock=lambda: clock[0])
    assert (await rule.decision("background")).reason == "idle_hysteresis"
    for instant in (2, 4):
        clock[0] = instant
        decision = await rule.decision("background")
    assert decision.eligible
    sensor.fields["cpu_percent"] = 24
    assert (await rule.decision("background")).eligible  # retain eligibility below busy threshold
    sensor.fields["idle_seconds"] = 0
    assert (await rule.decision("background")).reason == "owner_active"
    sensor.fields.update(idle_seconds=1000, cpu_percent=24)
    assert (await rule.decision("background")).reason == "cpu_recovery"
    sensor.fields["cpu_percent"] = 1
    assert (await rule.decision("background")).reason == "idle_hysteresis"
    for instant in (6, 8):
        clock[0] = instant
        decision = await rule.decision("background")
    assert decision.eligible
    clock[0] = 100  # a daemon stall/sleep must start a new window
    assert (await rule.decision("background")).reason == "idle_hysteresis"


async def test_idle_recovery_wakes_waiters_including_stricter_callers():
    clock = [0.0]
    rule = IdlePolicy(
        PeerSettings(
            interactive_policy="always", background_policy="always", idle_hysteresis_seconds=2
        ),
        MutableSensor(),
        clock=lambda: clock[0],
    )
    assert not (await rule.decision("interactive", True)).eligible
    revision = rule.revision
    waiter = asyncio.create_task(rule.wait_for_change(revision, 1))
    clock[0] = 2
    assert (await rule.decision("interactive", True)).eligible
    assert await waiter > revision
    # An event before subscribing must also be observed (no lost wakeup).
    assert await rule.wait_for_change(revision, 1) > revision
    with pytest.raises(TimeoutError):
        await rule.wait_for_change(rule.revision, 0.01)


@pytest.mark.parametrize(
    "settings",
    [
        {"cpu_idle_percent": 30},
        {"sensor_interval": 5},
        {"sensor_helper": "relative/path"},
        {"idle_seconds": float("nan")},
        {"idle_hysteresis_seconds": -1},
        {"background_policy": "guess_idle"},
    ],
)
def test_invalid_sensor_settings(settings):
    with pytest.raises(ValidationError):
        PeerSettings(**settings)


@asynccontextmanager
async def ledger_for(tmp_path, sensor, **kwargs):
    async def fit(model):
        return ResourceFit(model=model, required_gb=1, available_gb=48)

    settings = PeerSettings(data_dir=str(tmp_path), idle_hysteresis_seconds=0, **kwargs)
    ledger = LocalLedger(settings, fit, IdlePolicy(settings, sensor))
    await ledger.open()
    try:
        yield ledger
    finally:
        await ledger.close()


def spec(request_id="request", **fields):
    return AcquireRequest(model="tiny", request_id=request_id, workload="background", **fields)


async def test_live_admission_ignores_prior_idle_capacity_and_does_not_cancel_running(tmp_path):
    sensor = MutableSensor()
    async with ledger_for(tmp_path, sensor, slots=2) as ledger:
        snapshot = await ledger.capacity("tiny:latest", "background")
        assert snapshot.eligible and snapshot.observed_available_slots == 2
        running = await ledger.acquire(spec("running"))
        await ledger.consume(running.token, running.model)
        reserved = await ledger.acquire(spec("reserved"))
        sensor.fields["idle_seconds"] = 0
        with pytest.raises(AdmissionError, match="owner_active"):
            await ledger.consume(reserved.token, reserved.model)
        await ledger.release(reserved.token)
        with pytest.raises(AdmissionError, match="owner_active"):
            await ledger.acquire(spec("stale-snapshot"))
        assert (await ledger.inspect(running.token)).state == "running"
        await ledger.finish(running.token, stopped=False)
        assert (await ledger.capacity(running.model)).quarantined_slots == 1


async def test_persisted_policy_idempotency_and_stricter_consumption(tmp_path):
    sensor = MutableSensor()
    async with ledger_for(tmp_path, sensor) as ledger:
        grant = await ledger.acquire(spec(idle_only=True))
        assert (await ledger.inspect(grant.token)).idle_only
        assert (await ledger.acquire(spec(idle_only=True))).token == grant.token
        with pytest.raises(AdmissionError, match="idempotency_conflict"):
            await ledger.acquire(spec(idle_only=False))
        with pytest.raises(AdmissionError, match="idempotency_conflict"):
            await ledger.acquire(AcquireRequest(model="tiny", request_id="request", idle_only=True))
        await ledger.release(grant.token)
        local = await ledger.acquire(AcquireRequest(model="tiny", request_id="interactive"))
        sensor.fields["idle_seconds"] = 0
        with pytest.raises(AdmissionError, match="owner_active"):
            await ledger.consume(local.token, local.model, workload="background")
        with pytest.raises(AdmissionError, match="owner_active"):
            await ledger.consume(local.token, local.model, idle_only=True)


async def test_policy_delay_cannot_consume_or_reissue_expired_lease(tmp_path):
    sensor = MutableSensor()
    async with ledger_for(tmp_path, sensor) as ledger:
        grant = await ledger.acquire(spec(ttl_ms=100))
        sensor.delay = 0.15
        with pytest.raises(AdmissionError, match="reservation_not_active"):
            await ledger.consume(grant.token, grant.model)
        sensor.delay = 0
        grant = await ledger.acquire(spec("idempotent", ttl_ms=100))
        sensor.delay = 0.15
        with pytest.raises(AdmissionError, match="reservation_not_active"):
            await ledger.acquire(spec("idempotent", ttl_ms=100))


async def test_stored_and_tightened_policy_use_one_live_observation(tmp_path):
    class CountingSensor(MutableSensor):
        calls = 0

        async def sample(self):
            self.calls += 1
            return await super().sample()

    sensor = CountingSensor()
    async with ledger_for(tmp_path, sensor, interactive_policy="idle_only") as ledger:
        grant = await ledger.acquire(AcquireRequest(model="tiny", request_id="one-sample"))
        before = sensor.calls
        await ledger.consume(grant.token, grant.model, workload="background", idle_only=True)
        assert sensor.calls == before + 1


async def test_background_lease_does_not_inherit_default_interactive_policy(tmp_path):
    async with ledger_for(tmp_path, MutableSensor(), interactive_policy="never") as ledger:
        grant = await ledger.acquire(spec())
        running = await ledger.consume(grant.token, grant.model, workload="interactive")
        assert running.state == "running" and running.workload == "background"


async def test_phase2_schema_migration_keeps_orphan_charged_and_fences(tmp_path):
    sensor = MutableSensor()
    async with ledger_for(tmp_path, sensor) as ledger:
        grant = await ledger.acquire(spec())
        await ledger.consume(grant.token, grant.model)
    async with aiosqlite.connect(tmp_path / "worker.sqlite") as db:
        await db.execute("ALTER TABLE leases DROP COLUMN workload")
        await db.execute("ALTER TABLE leases DROP COLUMN idle_only")
        await db.commit()
    async with ledger_for(tmp_path, sensor) as restarted:
        snapshot = await restarted.capacity(grant.model)
        assert snapshot.quarantined_slots == 1 and snapshot.observed_available_slots == 0
        with pytest.raises(AdmissionError, match="stale_incarnation"):
            await restarted.consume(grant.token, grant.model)
        await restarted.reconcile_stopped_backend()
        replacement = await restarted.acquire(spec("next"))
        assert replacement.slot_generation > grant.slot_generation


async def test_n1_headless_local_api_and_background_policy_without_helpers(tmp_path, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(
        "fleet_manager.peer.backend.psutil.virtual_memory",
        lambda: SimpleNamespace(total=64e9, used=16e9, available=48e9),
    )
    sensor = MutableSensor(confidence="unknown", idle_seconds=None)
    executions = []

    async def backend(request):
        if request.url.path in {"/api/tags", "/api/ps"}:
            return httpx.Response(200, json={"models": [{"name": "tiny", "size": 1000}]})
        executions.append(request.url.path)
        return httpx.Response(200, json={"model": "tiny", "embeddings": [[1.0]]})

    app = create_peer_app(
        PeerSettings(
            data_dir=str(tmp_path), background_policy="utilization_only", idle_hysteresis_seconds=0
        ),
        backend_transport=httpx.MockTransport(backend),
        sensor=sensor,
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as client,
    ):
        response = await client.post(
            "/api/embed",
            headers={"X-Idle-Herd-Workload": "background"},
            json={"model": "tiny", "input": "hi"},
        )
        assert response.status_code == 200, response.text
        status = (await client.get("/v1/node/eligibility")).json()
        assert status["policy"] == "utilization_only" and status["activity"] == "unknown"
        assert app.state.directory is None
        response = await client.post(
            "/api/embed",
            headers={"X-Idle-Herd-Idle-Only": "true"},
            json={"model": "tiny", "input": "hi"},
        )
        assert response.status_code == 503, response.text
        response = await client.post(
            "/api/embed", headers={"X-Idle-Herd-Workload": "invalid"}, json={"model": "tiny"}
        )
        assert response.status_code == 422
        assert len(executions) == 1
