import math
from collections import namedtuple

import pytest
from pydantic import ValidationError

from fleet_manager.peer import sensors


@pytest.mark.asyncio
async def test_macos_hid_and_failure(monkeypatch):
    async def command(*args):
        return '"HIDIdleTime" = 12500000000'

    monkeypatch.setattr(sensors, "_command", command)
    assert await sensors._mac_activity() == (12.5, None)

    async def invalid(*args):
        return '"HIDIdleTime" = 1\n"HIDIdleTime" = 2'

    monkeypatch.setattr(sensors, "_command", invalid)
    assert await sensors._mac_activity() == (None, None)


@pytest.mark.asyncio
async def test_linux_checks_all_sessions(monkeypatch):
    async def command(*args):
        if "list-sessions" in args:
            return "1 1000 alice seat0\n2 1001 bob seat1\n"
        if "1" in args:
            return "IdleHint=yes\nIdleSinceHintMonotonic=1000000\nLockedHint=yes\n"
        return "IdleHint=no\nIdleSinceHintMonotonic=0\nLockedHint=no\n"

    monkeypatch.setattr(sensors, "_command", command)
    monkeypatch.setattr(sensors.time, "monotonic", lambda: 100)
    assert await sensors._linux_activity() == (0, False)


@pytest.mark.asyncio
async def test_linux_missing_or_future_data_is_unknown(monkeypatch):
    async def command(*args):
        if "list-sessions" in args:
            return "1 1000 alice seat0\n"
        return "IdleHint=yes\nIdleSinceHintMonotonic=999999999999999\nLockedHint=yes\n"

    monkeypatch.setattr(sensors, "_command", command)
    assert await sensors._linux_activity() == (None, None)


@pytest.mark.asyncio
async def test_resource_first_sample_and_unavailable_windows(monkeypatch):
    cpu = namedtuple("CPU", "user idle")
    values = iter([cpu(10, 90), cpu(30, 170)])
    monkeypatch.setattr(sensors.psutil, "cpu_times", lambda: next(values))
    monkeypatch.setattr(sensors.psutil, "virtual_memory", lambda: (_ for _ in ()).throw(OSError()))
    monkeypatch.setattr(sensors.psutil, "sensors_battery", lambda: None)
    sensor = sensors.NativeSensor("win32")
    first, second = await sensor.sample(), await sensor.sample()
    assert first.confidence == "unknown" and first.idle_seconds is None
    assert first.cpu_percent is None and first.memory_available_gb is None
    assert second.cpu_percent == pytest.approx(20)


def test_signal_rejects_invalid_values():
    with pytest.raises(ValidationError):
        sensors.IdleSignal(observed_at=1, source="unknown", idle_seconds=math.inf)
    with pytest.raises(ValidationError):
        sensors.IdleSignal(observed_at=1, source="sensitive/path")


@pytest.mark.asyncio
async def test_rapid_identical_counters_retain_recent_cpu_with_original_timestamp(monkeypatch):
    cpu = namedtuple("CPU", "user idle")
    counters = iter([cpu(10, 90), cpu(30, 170), cpu(30, 170), cpu(30, 170), cpu(40, 260)])
    clock = [100.0]
    sensor = sensors.NativeSensor("other")
    monkeypatch.setattr(sensor, "_resources", lambda: {"_cpu_times": next(counters)})
    monkeypatch.setattr(sensors.time, "time", lambda: clock[0])
    assert (await sensor.sample()).cpu_percent is None
    clock[0] = 102
    valid = await sensor.sample()
    assert valid.cpu_percent == pytest.approx(20)
    clock[0] = 102.01
    repeated = await sensor.sample()
    assert repeated.cpu_percent == pytest.approx(20)
    assert repeated.observed_at == valid.observed_at == 102
    # Repeated reads must never refresh old CPU data indefinitely.
    clock[0] = 104
    assert (await sensor.sample()).cpu_percent is None
    clock[0] = 106
    assert (await sensor.sample()).cpu_percent == pytest.approx(10)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [None, (1, 1)])
async def test_cpu_probe_failure_or_counter_reset_discards_cached_reading(monkeypatch, invalid):
    cpu = namedtuple("CPU", "user idle")
    invalid = cpu(*invalid) if invalid is not None else None
    counters = iter([cpu(10, 90), cpu(30, 170), invalid, cpu(30, 170)])
    sensor = sensors.NativeSensor("other")
    monkeypatch.setattr(sensor, "_resources", lambda: {"_cpu_times": next(counters)})
    assert (await sensor.sample()).cpu_percent is None
    assert (await sensor.sample()).cpu_percent == pytest.approx(20)
    assert (await sensor.sample()).cpu_percent is None
    recovered = await sensor.sample()
    if invalid is None:
        assert recovered.cpu_percent is None  # Missing baseline cannot imply idle.
    else:
        assert recovered.cpu_percent is not None  # New counters provide a real delta.


@pytest.mark.asyncio
async def test_helper_rejects_stale_and_missing_activity(monkeypatch):
    values = iter(
        [
            '{"version":1,"observed_at":94.0,"idle_seconds":30.0,"session_locked":false}',
            '{"version":1,"observed_at":100.0,"session_locked":false}',
            '{"version":1,"observed_at":100.0,"idle_seconds":30.0,"session_locked":false}',
        ]
    )

    async def command(*args):
        assert args == ("/opt/idle-sensor", "--idle-herd-sensor-v1")
        return next(values)

    monkeypatch.setattr(sensors, "_command", command)
    monkeypatch.setattr(sensors.time, "time", lambda: 100.0)
    assert await sensors._helper_activity("/opt/idle-sensor") == (None, None, None)
    assert await sensors._helper_activity("/opt/idle-sensor") == (None, None, None)
    assert await sensors._helper_activity("/opt/idle-sensor") == (30, False, 100)
    with pytest.raises(ValueError):
        sensors.native_sensor("relative")


@pytest.mark.asyncio
async def test_linux_session_churn_rejected(monkeypatch):
    listings = iter(["1 1000 alice", "1 1000 alice\n2 1001 bob"])

    async def command(*args):
        if "list-sessions" in args:
            return next(listings)
        return "IdleHint=no\nIdleSinceHintMonotonic=0\nLockedHint=no\n"

    monkeypatch.setattr(sensors, "_command", command)
    assert await sensors._linux_activity() == (None, None)


@pytest.mark.asyncio
async def test_command_caps_output_and_cleans_up_on_cancel():
    import asyncio
    import sys

    assert await sensors._command(sys.executable, "-c", "print('x' * 70000)") is None
    task = asyncio.create_task(
        sensors._command(sys.executable, "-c", "import time; time.sleep(30)")
    )
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_sample_deadline_and_resources_survive_activity_timeout(monkeypatch):
    import asyncio

    sensor = sensors.NativeSensor("linux")
    monkeypatch.setattr(sensor, "_resources", lambda: {"memory_available_gb": 2.0})

    async def slow():
        await asyncio.sleep(30)

    monkeypatch.setattr(sensors, "_linux_activity", slow)
    result = await sensor.sample()
    assert result.idle_seconds is None and result.confidence == "unknown"
    assert result.memory_available_gb == 2.0


@pytest.mark.asyncio
async def test_helper_observation_timestamp_is_preserved(monkeypatch):
    sensor = sensors.NativeSensor("linux", "/helper")
    monkeypatch.setattr(sensor, "_resources", lambda: {})

    async def helper(path):
        return 120.0, False, 96.0

    monkeypatch.setattr(sensors, "_helper_activity", helper)
    monkeypatch.setattr(sensors.time, "time", lambda: 100.0)
    assert (await sensor.sample()).observed_at == 96.0


@pytest.mark.asyncio
async def test_helper_invalid_timestamps_and_nonfinite(monkeypatch):
    values = iter(
        [
            '{"version":1,"observed_at":101.0,"idle_seconds":30.0,"session_locked":false}',
            '{"version":1,"observed_at":100.0,"idle_seconds":1e999,"session_locked":false}',
        ]
    )

    async def command(*args):
        return next(values)

    monkeypatch.setattr(sensors, "_command", command)
    monkeypatch.setattr(sensors.time, "time", lambda: 100.0)
    assert await sensors._helper_activity("/helper") == (None, None, None)
    assert await sensors._helper_activity("/helper") == (None, None, None)


@pytest.mark.asyncio
async def test_command_timeout():
    import sys

    assert await sensors._command(sys.executable, "-c", "import time; time.sleep(30)") is None


def test_partial_resource_failure_and_pressure(monkeypatch):
    cpu = namedtuple("CPU", "user idle")
    memory = namedtuple("Memory", "available total")
    monkeypatch.setattr(sensors.psutil, "cpu_times", lambda: cpu(10, 90))
    monkeypatch.setattr(sensors.psutil, "virtual_memory", lambda: memory(10, 100))
    monkeypatch.setattr(sensors.psutil, "sensors_battery", lambda: (_ for _ in ()).throw(OSError()))
    monkeypatch.setattr(sensors.psutil, "sensors_temperatures", lambda: {}, raising=False)
    result = sensors.NativeSensor("linux")._resources()
    assert result["memory_pressure"] == "warning"
    assert "on_ac" not in result and "thermal_state" not in result


@pytest.mark.parametrize(
    ("ticks", "expected"),
    [
        ((20_000, 15_000), 5.0),
        ((0x100000005, 0xFFFFFFFB), 0.01),
        ((-1, 0), None),
        ((1000, 2000), None),
        ((1000, -1), None),
        (None, None),
    ],
)
def test_windows_session_ticks(monkeypatch, ticks, expected):
    monkeypatch.setattr(sensors, "_windows_input_ticks", lambda: ticks)
    assert sensors._windows_activity() == expected


@pytest.mark.asyncio
async def test_windows_session_reading_never_claims_host_confidence(monkeypatch):
    sensor = sensors.NativeSensor("win32")
    monkeypatch.setattr(sensor, "_resources", lambda: {})
    monkeypatch.setattr(sensors, "_windows_input_ticks", lambda: (2500, 500))
    result = await sensor.sample()
    assert result.source == "windows_session"
    assert result.idle_seconds == 2
    assert result.confidence == "unknown" and result.session_locked is None


def test_windows_api_unavailable_is_unknown():
    # This test also exercises safe import and fallback on non-Windows hosts.
    import sys

    if sys.platform != "win32":
        assert sensors._windows_input_ticks() is None


def test_windows_api_failure_and_permission_denial(monkeypatch):
    import ctypes

    class Function:
        def __init__(self, result):
            self.result = result

        def __call__(self, *args):
            return self.result

    class Library:
        GetLastInputInfo = Function(0)
        GetTickCount64 = Function(1234)

    monkeypatch.setattr(ctypes, "WinDLL", lambda *args, **kwargs: Library(), raising=False)
    assert sensors._windows_input_ticks() is None

    def denied(*args, **kwargs):
        raise PermissionError()

    monkeypatch.setattr(ctypes, "WinDLL", denied)
    assert sensors._windows_input_ticks() is None


@pytest.mark.asyncio
async def test_cancelled_resource_worker_cannot_advance_cpu_baseline(monkeypatch):
    import asyncio
    import threading

    cpu = namedtuple("CPU", "user idle")
    started = threading.Event()
    release = threading.Event()
    calls = 0
    sensor = sensors.NativeSensor("other")

    def collect():
        nonlocal calls
        calls += 1
        if calls == 2:
            started.set()
            release.wait(5)
        return {"_cpu_times": cpu(calls * 10, calls * 90)}

    monkeypatch.setattr(sensor, "_collect_resources", collect)
    assert (await sensor.sample()).cpu_percent is None
    pending = asyncio.create_task(sensor.sample())
    assert await asyncio.to_thread(started.wait, 1)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    for _ in range(3):
        assert (await sensor.sample()).cpu_percent is None
        assert calls == 2
    release.set()
    await asyncio.wait_for(asyncio.shield(sensor._pending_resources), 1)
    assert (await sensor.sample()).cpu_percent is None
    assert (await sensor.sample()).cpu_percent == pytest.approx(10)


def test_memory_available_and_total_use_decimal_gigabytes(monkeypatch):
    memory = namedtuple("Memory", "available total")
    monkeypatch.setattr(
        sensors.psutil, "virtual_memory", lambda: memory(2_000_000_000, 4_000_000_000)
    )
    resources = sensors.NativeSensor("other")._resources()
    assert resources["memory_available_gb"] == 2
    assert resources["memory_total_gb"] == 4
