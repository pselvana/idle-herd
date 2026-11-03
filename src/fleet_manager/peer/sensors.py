"""Independent, conservative native host activity and resource observations."""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import os
import re
import sys
import threading
import time
from typing import Literal, Protocol

import psutil
from pydantic import BaseModel, ConfigDict, Field


class IdleSignal(BaseModel):
    """One timestamped observation; missing measurements never imply idleness."""

    model_config = ConfigDict(allow_inf_nan=False, extra="forbid")

    observed_at: float = Field(ge=0)
    source: str = Field(min_length=1, max_length=64, pattern=r"^[a-z0-9_-]+$")
    confidence: Literal["host_sensor", "unknown"] = "unknown"
    idle_seconds: float | None = Field(default=None, ge=0)
    session_locked: bool | None = None
    cpu_percent: float | None = Field(default=None, ge=0, le=100)
    memory_available_gb: float | None = Field(default=None, ge=0)
    memory_total_gb: float | None = Field(default=None, gt=0)
    memory_pressure: Literal["normal", "warning", "critical", "unknown"] = "unknown"
    on_ac: bool | None = None
    battery_percent: float | None = Field(default=None, ge=0, le=100)
    thermal_state: Literal["nominal", "fair", "serious", "critical", "unknown"] = "unknown"
    gpu_percent: float | None = Field(default=None, ge=0, le=100)


class _HelperSignal(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    version: Literal[1]
    observed_at: float = Field(ge=0)
    idle_seconds: float | None = Field(ge=0)
    session_locked: bool | None


class Sensor(Protocol):
    async def sample(self) -> IdleSignal: ...


async def _command(*argv: str) -> str | None:
    """Run a fixed native command without a shell, bounding time and output."""
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        assert process.stdout is not None
        async with asyncio.timeout(2):
            output = bytearray()
            while True:
                chunk = await process.stdout.read(min(8192, 65537 - len(output)))
                if not chunk:
                    break
                output.extend(chunk)
                if len(output) > 65536:
                    return None
            if await process.wait() != 0:
                return None
        return output.decode("utf-8", errors="strict")
    except (OSError, UnicodeError, TimeoutError):
        return None
    finally:
        if process is not None and process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            await process.wait()


async def _mac_activity() -> tuple[float | None, bool | None]:
    # IOHIDSystem reports nanoseconds since the latest HID event across the host.
    output = await _command("/usr/sbin/ioreg", "-c", "IOHIDSystem", "-r")
    if output is None:
        return None, None
    values = re.findall(r'"HIDIdleTime"\s*=\s*(\d+)', output)
    if len(values) != 1:
        return None, None
    try:
        seconds = int(values[0]) / 1_000_000_000
        return (seconds, None) if math.isfinite(seconds) else (None, None)
    except (ValueError, OverflowError):
        return None, None


async def _linux_activity() -> tuple[float | None, bool | None]:
    # Every logind session must give a usable observation. Never choose a single
    # idle session while another local or remote session may have active users.
    listing = await _command("/usr/bin/loginctl", "list-sessions", "--no-legend", "--no-pager")
    if listing is None:
        return None, None
    sessions = [line.split()[0] for line in listing.splitlines() if line.strip()]
    if (
        not sessions
        or len(sessions) > 128
        or any(not re.fullmatch(r"[A-Za-z0-9_-]+", session) for session in sessions)
    ):
        return None, None
    idle_values: list[float] = []
    locks: list[bool] = []
    for session in sessions:
        output = await _command(
            "/usr/bin/loginctl",
            "show-session",
            session,
            "--no-pager",
            "--property=IdleHint",
            "--property=IdleSinceHintMonotonic",
            "--property=LockedHint",
        )
        if output is None:
            return None, None
        pairs = [line.partition("=") for line in output.splitlines()]
        if any(not sep for _, sep, _ in pairs):
            return None, None
        values = {key: value for key, _, value in pairs}
        if len(values) != len(pairs) or values.get("IdleHint") not in ("yes", "no"):
            return None, None
        if values.get("LockedHint") not in ("yes", "no"):
            return None, None
        locks.append(values["LockedHint"] == "yes")
        if values["IdleHint"] == "no":
            idle_values.append(0.0)
            continue
        stamp = values.get("IdleSinceHintMonotonic", "")
        if not stamp.isdecimal() or len(stamp) > 20 or int(stamp) == 0:
            return None, None
        seconds = time.monotonic() - int(stamp) / 1_000_000
        if seconds < 0 or not math.isfinite(seconds):
            return None, None
        idle_values.append(seconds)
    final_listing = await _command(
        "/usr/bin/loginctl", "list-sessions", "--no-legend", "--no-pager"
    )
    if final_listing is None or sorted(
        line.split()[0] for line in final_listing.splitlines() if line.strip()
    ) != sorted(sessions):
        return None, None
    return min(idle_values), all(locks)


async def _helper_activity(path: str) -> tuple[float | None, bool | None, float | None]:
    """Version 1 JSON contains version, observed_at, idle_seconds and session_locked.

    The helper must report host-wide activity, not just its own session. It is
    an explicitly configured trust boundary; malformed or stale output fails closed.
    """
    output = await _command(path, "--idle-herd-sensor-v1")
    if output is None:
        return None, None, None

    def unique_pairs(pairs: list[tuple[str, object]]) -> dict:
        values = dict(pairs)
        if len(values) != len(pairs):
            raise ValueError("duplicate field")
        return values

    try:
        signal = _HelperSignal.model_validate(json.loads(output, object_pairs_hook=unique_pairs))
        age = time.time() - signal.observed_at
        if age < 0 or age > 5 or signal.idle_seconds is None:
            return None, None, None
        return signal.idle_seconds, signal.session_locked, signal.observed_at
    except (ValueError, OverflowError):
        return None, None, None


def _windows_input_ticks() -> tuple[int, int] | None:
    """Read ticks for this process's session, never an all-session observation.

    https://learn.microsoft.com/en-us/windows/win32/api/winuser/nf-winuser-getlastinputinfo
    """
    import ctypes

    class LastInputInfo(ctypes.Structure):
        _fields_ = [("size", ctypes.c_uint32), ("time", ctypes.c_uint32)]

    try:
        user32 = ctypes.WinDLL("user32", use_last_error=True)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        user32.GetLastInputInfo.argtypes = [ctypes.POINTER(LastInputInfo)]
        user32.GetLastInputInfo.restype = ctypes.c_int
        kernel32.GetTickCount64.argtypes = []
        kernel32.GetTickCount64.restype = ctypes.c_uint64
        value = LastInputInfo(ctypes.sizeof(LastInputInfo), 0)
        if not user32.GetLastInputInfo(ctypes.byref(value)):
            return None
        return int(kernel32.GetTickCount64()), int(value.time)
    except (OSError, AttributeError, ValueError):
        return None


def _windows_activity() -> float | None:
    ticks = _windows_input_ticks()
    if ticks is None:
        return None
    now, last = ticks
    if now < 0 or not 0 <= last <= 0xFFFFFFFF:
        return None
    elapsed = ((now & 0xFFFFFFFF) - last) & 0xFFFFFFFF
    # Very old/future injected ticks are indistinguishable from wraparound.
    if elapsed > 0x7FFFFFFF:
        return None
    return elapsed / 1000


class NativeSensor:
    def __init__(self, platform: str | None = None, helper: str | None = None) -> None:
        if helper is not None and not os.path.isabs(helper):
            raise ValueError("sensor helper must be an absolute path")
        self.helper = helper
        self.platform = platform or sys.platform
        self._cpu_times = None
        self._resource_lock = threading.Lock()
        self._pending_resources: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    def _resources(self) -> dict:
        # A cancelled to_thread call can keep running; serialize native reads
        # without ever blocking the event loop on the threading lock.
        with self._resource_lock:
            return self._collect_resources()

    def _collect_resources(self) -> dict:
        result: dict = {}
        with contextlib.suppress(
            OSError, ValueError, RuntimeError, AttributeError, TypeError, psutil.Error
        ):
            result["_cpu_times"] = psutil.cpu_times()
        try:
            vm = psutil.virtual_memory()
            memory = vm.available / 1_000_000_000
            if math.isfinite(memory) and memory >= 0:
                result["memory_available_gb"] = memory
            total = vm.total / 1_000_000_000
            if math.isfinite(total) and total > 0:
                result["memory_total_gb"] = total
            if self.platform.startswith("linux") and vm.total > 0 and 0 <= vm.available <= vm.total:
                used = 100 * (vm.total - vm.available) / vm.total
                result["memory_pressure"] = (
                    "critical" if used >= 95 else "warning" if used >= 85 else "normal"
                )
        except (OSError, ValueError, RuntimeError, AttributeError, psutil.Error):
            pass
        try:
            battery = psutil.sensors_battery()
            if battery is not None:
                if isinstance(battery.power_plugged, bool):
                    result["on_ac"] = battery.power_plugged
                if math.isfinite(battery.percent) and 0 <= battery.percent <= 100:
                    result["battery_percent"] = battery.percent
        except (
            OSError,
            ValueError,
            RuntimeError,
            AttributeError,
            TypeError,
            NotImplementedError,
            psutil.Error,
        ):
            pass
        if self.platform.startswith("linux") or self.platform == "win32":
            try:
                temperatures = psutil.sensors_temperatures()
                readings = [
                    reading.current
                    for group in temperatures.values()
                    for reading in group
                    if reading.current is not None
                    and math.isfinite(reading.current)
                    and 0 < reading.current <= 150
                ]
                if readings and max(readings) >= 85:
                    result["thermal_state"] = "serious"
                elif readings and any(
                    temperatures.get(driver)
                    for driver in ("coretemp", "k10temp", "zenpower", "cpu_thermal")
                ):
                    result["thermal_state"] = "nominal"
            except (OSError, ValueError, RuntimeError, AttributeError, TypeError, psutil.Error):
                pass
        return result

    async def sample(self) -> IdleSignal:
        async with self._lock:
            observed_at = time.time()
            resources: dict = {}
            source = "unknown"
            idle, locked = None, None
            try:
                async with asyncio.timeout(3):
                    if self._pending_resources is not None and self._pending_resources.done():
                        # An abandoned result is never accepted as a new baseline.
                        self._pending_resources = None
                    if self._pending_resources is None:
                        task = asyncio.create_task(asyncio.to_thread(self._resources))
                        # Retrieve late exceptions even if no later sample is requested.
                        task.add_done_callback(
                            lambda finished: (
                                finished.exception() if not finished.cancelled() else None
                            )
                        )
                        self._pending_resources = task
                        try:
                            resources = await asyncio.shield(task)
                        except Exception:
                            resources = {}
                        self._pending_resources = None
                    current = resources.pop("_cpu_times", None)
                    previous, self._cpu_times = self._cpu_times, current
                    if current is not None and previous is not None:
                        try:
                            total = sum(current) - sum(previous)
                            idle_ticks = (current.idle + getattr(current, "iowait", 0)) - (
                                previous.idle + getattr(previous, "iowait", 0)
                            )
                            if total > 0 and 0 <= idle_ticks <= total:
                                resources["cpu_percent"] = 100 * (total - idle_ticks) / total
                        except (ValueError, TypeError, AttributeError):
                            self._cpu_times = None
                    if self.helper is not None:
                        source = "native_helper"
                        idle, locked, helper_at = await _helper_activity(self.helper)
                        if helper_at is not None:
                            observed_at = min(observed_at, helper_at)
                    elif self.platform == "darwin":
                        source = "macos_iokit"
                        idle, locked = await _mac_activity()
                    elif self.platform.startswith("linux"):
                        source = "linux_logind"
                        idle, locked = await _linux_activity()
                    elif self.platform == "win32":
                        source = "windows_session"
                        idle = await asyncio.to_thread(_windows_activity)
                    if self.platform == "darwin":
                        pressure = await _command(
                            "/usr/sbin/sysctl", "-n", "kern.memorystatus_vm_pressure_level"
                        )
                        resources["memory_pressure"] = {
                            "1": "normal",
                            "2": "warning",
                            "4": "critical",
                        }.get(pressure.strip() if pressure else "", "unknown")
            except TimeoutError:
                self._cpu_times = None
                idle, locked = None, None
            except asyncio.CancelledError:
                self._cpu_times = None
                raise
            return IdleSignal(
                observed_at=observed_at,
                source=source,
                confidence=(
                    "host_sensor" if idle is not None and source != "windows_session" else "unknown"
                ),
                idle_seconds=idle,
                session_locked=locked,
                **resources,
            )


def native_sensor(helper: str | None = None) -> Sensor:
    return NativeSensor(helper=helper)
