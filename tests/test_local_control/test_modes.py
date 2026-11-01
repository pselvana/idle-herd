"""Worker-authoritative mode fencing and durable preferences."""

import asyncio
from types import SimpleNamespace

import pytest

from fleet_manager.local_control.models import ModeUpdate, PreferencesUpdate
from fleet_manager.peer.local_ledger import LocalLedger
from fleet_manager.peer.models import AcquireRequest, AdmissionError, PeerSettings, ResourceFit
from fleet_manager.peer.sensors import IdleSignal


async def eligible(model):
    return ResourceFit(model=model, required_gb=1, available_gb=100)


async def test_enrollment_does_not_override_daemon_policy_configuration(tmp_path):
    enrollment = LocalLedger(PeerSettings(data_dir=str(tmp_path)), eligible)
    await enrollment.open()
    await enrollment.close()
    worker = LocalLedger(
        PeerSettings(data_dir=str(tmp_path), background_policy="always", reserve_memory_gb=0),
        eligible,
    )
    await worker.open()
    try:
        values = (await worker.preferences()).values
        assert values.background_policy == "always"
        assert values.reserve_memory_gb == 0
        await worker.acquire(request("peer-after-enrollment", workload="background"), peer=True)
    finally:
        await worker.close()


class ActiveSensor:
    async def sample(self):
        import time

        return IdleSignal(
            source="test",
            confidence="host_sensor",
            idle_seconds=0,
            cpu_percent=1,
            memory_available_gb=48,
            memory_pressure="normal",
            thermal_state="nominal",
            on_ac=True,
            observed_at=time.time(),
        )


@pytest.fixture
async def ledger(tmp_path):
    worker = LocalLedger(PeerSettings(data_dir=str(tmp_path), slots=4), eligible)
    await worker.open()
    try:
        yield worker
    finally:
        await worker.close()


def request(name, **kwargs):
    return AcquireRequest(request_id=name, model="small", **kwargs)


@pytest.mark.parametrize("mode", ["auto", "draining", "offline", "paused"])
async def test_mode_gates_local_and_peer_and_invalidates_reservations(ledger, mode):
    local = await ledger.acquire(request("local"))
    remote = await ledger.acquire(request("remote"), peer=True)
    await ledger.set_mode(ModeUpdate(mode=mode))
    assert (await ledger.inspect(local.token)).state == (
        "released" if mode == "paused" else "reserved"
    )
    assert (await ledger.inspect(remote.token)).state == (
        "reserved" if mode == "auto" else "released"
    )
    for peer, permitted in [(False, mode != "paused"), (True, mode == "auto")]:
        if permitted:
            await ledger.acquire(request(f"new-{peer}"), peer=peer)
        else:
            with pytest.raises(AdmissionError, match=f"manual_{mode}"):
                await ledger.acquire(request(f"new-{peer}"), peer=peer)
    if mode in {"offline", "paused"}:
        with pytest.raises(AdmissionError, match=f"manual_{mode}"):
            await ledger.require_dispatch()
    else:
        await ledger.require_dispatch()


async def test_pause_serializes_with_inflight_consume(ledger):
    lease = await ledger.acquire(request("race"), peer=True)
    entered, resume = asyncio.Event(), asyncio.Event()

    async def delayed_fit(model):
        entered.set()
        await resume.wait()
        return await eligible(model)

    ledger.fit_check = delayed_fit
    consume = asyncio.create_task(ledger.consume(lease.token, lease.model))
    await entered.wait()
    pause = asyncio.create_task(ledger.set_mode(ModeUpdate(mode="paused")))
    await asyncio.sleep(0)
    resume.set()
    await consume
    await pause
    assert (await ledger.inspect(lease.token)).state == "running"
    with pytest.raises(AdmissionError, match="manual_paused"):
        await ledger.acquire(request("after"))
    await ledger.set_mode(ModeUpdate(mode="auto"))
    assert (await ledger.capacity(lease.model)).observed_available_slots == 3


async def test_pause_first_fences_consume_and_preserves_quarantine(ledger):
    running = await ledger.acquire(request("running"))
    await ledger.consume(running.token, running.model)
    uncertain = await ledger.acquire(request("uncertain"))
    await ledger.consume(uncertain.token, uncertain.model)
    await ledger.finish(uncertain.token, stopped=False)
    reserved = await ledger.acquire(request("reserved"), peer=True)
    await ledger.set_mode(ModeUpdate(mode="paused"))
    with pytest.raises(AdmissionError, match="manual_paused"):
        await ledger.consume(reserved.token, reserved.model)
    capacity = await ledger.capacity(running.model)
    assert capacity.quarantined_slots == 1
    assert capacity.observed_available_slots == 0  # paused advertises no capacity
    assert (await ledger.inspect(running.token)).state == "running"
    await ledger.set_mode(ModeUpdate(mode="auto"))
    assert (await ledger.capacity(running.model)).observed_available_slots == 2
    with pytest.raises(AdmissionError, match="reservation_not_active"):
        await ledger.consume(reserved.token, reserved.model)


async def test_timed_offline_expires_but_active_owner_still_blocks_idle_work(ledger, monkeypatch):
    import time

    clock = SimpleNamespace(time=lambda: time.time())
    base = time.time()
    clock.time = lambda: base
    monkeypatch.setattr("fleet_manager.peer.local_ledger.time", clock)
    ledger.policy.sensor = ActiveSensor()
    await ledger.set_mode(ModeUpdate(mode="offline", duration_seconds=10))
    clock.time = lambda: base + 11
    assert (await ledger.mode_state()).mode == "auto"
    with pytest.raises(AdmissionError, match="owner_active"):
        await ledger.acquire(request("idle", workload="background"))
    await ledger.acquire(request("interactive"))


async def test_restart_restores_mode_preferences_and_expires_override(ledger, monkeypatch):
    import time

    base = time.time()
    clock = SimpleNamespace(time=lambda: base)
    monkeypatch.setattr("fleet_manager.peer.local_ledger.time", clock)
    before = await ledger.preferences()
    updated = await ledger.update_preferences(
        PreferencesUpdate(expected_version=before.version, values={"require_ac": True})
    )
    await ledger.set_mode(ModeUpdate(mode="offline", duration_seconds=10))
    settings = ledger.settings.model_copy(deep=True)
    await ledger.close()
    restarted = LocalLedger(settings, eligible)
    await restarted.open()
    try:
        assert (await restarted.mode_state()).mode == "offline"
        assert (await restarted.preferences()) == updated
        assert restarted.settings.require_ac
        clock.time = lambda: base + 11
        assert (await restarted.mode_state()).mode == "auto"
    finally:
        await restarted.close()


async def test_failed_preference_commit_does_not_change_live_or_durable_settings(
    ledger, monkeypatch
):
    original = await ledger.preferences()
    old_require_ac = ledger.settings.require_ac
    real_commit = ledger._db.commit

    async def fail_commit():
        raise OSError("simulated commit failure")

    monkeypatch.setattr(ledger._db, "commit", fail_commit)
    with pytest.raises(OSError, match="simulated commit failure"):
        await ledger.update_preferences(
            PreferencesUpdate(
                expected_version=original.version, values={"require_ac": not old_require_ac}
            )
        )
    assert ledger.settings.require_ac == old_require_ac
    monkeypatch.setattr(ledger._db, "commit", real_commit)
    assert await ledger.preferences() == original
