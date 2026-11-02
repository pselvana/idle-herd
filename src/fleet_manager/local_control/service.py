"""Explicit per-user native service configuration and lifecycle."""

from __future__ import annotations

import contextlib
import getpass
import hashlib
import json
import os
import plistlib
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

LABEL = "com.idle-herd.daemon"
UNIT = "idle-herd.service"


def plist_path() -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{LABEL}.plist"


def systemd_path() -> Path:
    return Path.home() / ".config" / "systemd" / "user" / UNIT


def windows_task_path() -> Path:
    return Path.home() / "AppData" / "Local" / "IdleHerd" / "service" / "task.xml"


def windows_task_name() -> str:
    # Task Scheduler names are machine-wide; scope this fixed name to the account.
    account = f"{os.environ.get('USERDOMAIN', '')}\\{getpass.getuser()}".casefold()
    suffix = hashlib.sha256(account.encode("utf-8")).hexdigest()[:16]
    return f"{LABEL}.{suffix}"


def _check_platform() -> None:
    if sys.platform not in ("darwin", "linux", "win32"):
        raise RuntimeError("user services are supported only on macOS, Linux and Windows")


def _atomic_write(destination: Path, content: bytes) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix=".idle-herd-", dir=destination.parent)
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(content)
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _systemd_quote(value: str) -> str:
    if any(c in value for c in "\r\n\0"):
        raise RuntimeError("service settings cannot contain newlines or NUL")
    return '"' + value.replace("%", "%%").replace("\\", "\\\\").replace('"', '\\"') + '"'


def _windows_xml(python: str, launcher: Path) -> bytes:
    task = ET.Element(
        "Task", {"version": "1.2", "xmlns": "http://schemas.microsoft.com/windows/2004/02/mit/task"}
    )
    triggers = ET.SubElement(task, "Triggers")
    ET.SubElement(triggers, "LogonTrigger")
    principals = ET.SubElement(task, "Principals")
    principal = ET.SubElement(principals, "Principal", {"id": "Owner"})
    ET.SubElement(principal, "UserId").text = getpass.getuser()
    ET.SubElement(principal, "LogonType").text = "InteractiveToken"
    ET.SubElement(principal, "RunLevel").text = "LeastPrivilege"
    settings = ET.SubElement(task, "Settings")
    ET.SubElement(settings, "ExecutionTimeLimit").text = "PT0S"
    ET.SubElement(settings, "MultipleInstancesPolicy").text = "IgnoreNew"
    ET.SubElement(settings, "StopIfGoingOnBatteries").text = "false"
    ET.SubElement(settings, "DisallowStartIfOnBatteries").text = "false"
    actions = ET.SubElement(task, "Actions", {"Context": "Owner"})
    action = ET.SubElement(actions, "Exec")
    ET.SubElement(action, "Command").text = python
    ET.SubElement(action, "Arguments").text = subprocess.list2cmdline([str(launcher)])
    return ET.tostring(task, encoding="utf-16", xml_declaration=True)


def install(data_dir: str, executable: str | None = None, *, settings=None) -> Path:
    """Write private service configuration with effective settings; do not register it."""
    from fleet_manager.peer.models import PeerSettings

    _check_platform()
    command = executable or sys.executable
    if not command or not Path(command).is_absolute() or not Path(command).is_file():
        raise RuntimeError("daemon executable must be an existing absolute path")
    values = (settings or PeerSettings(data_dir=data_dir)).model_dump()
    values["data_dir"] = str(Path(data_dir).expanduser().resolve())
    environment = {
        "IDLE_HERD_" + name.upper(): (
            json.dumps(value, separators=(",", ":"))
            if isinstance(value, (bool, list, dict))
            else str(value)
        )
        for name, value in values.items()
        if value is not None
    }
    arguments = [str(Path(command).absolute())]
    if executable is None:
        arguments += ["-m", "fleet_manager.cli.peer_cli"]
    working_directory = str(Path.cwd().resolve())
    if sys.platform == "linux":
        destination = systemd_path()
        lines = ["[Unit]", "Description=Idle-Herd peer daemon", "", "[Service]", "Type=simple"]
        lines += [f"Environment={_systemd_quote(k + '=' + v)}" for k, v in environment.items()]
        lines += [
            "WorkingDirectory=" + _systemd_quote(working_directory),
            "ExecStart=" + " ".join(_systemd_quote(a.replace("$", "$$")) for a in arguments),
            "Restart=on-failure",
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
        _atomic_write(destination, "\n".join(lines).encode())
    elif sys.platform == "win32":
        destination = windows_task_path()
        launcher = destination.with_suffix(".py")
        script = (
            "import os, runpy, sys\n"
            f"os.environ.update({environment!r})\n"
            f"os.chdir({working_directory!r})\n"
        )
        if executable is None:
            script += (
                'sys.argv = ["idle-herd"]\n'
                'runpy.run_module("fleet_manager.cli.peer_cli", run_name="__main__")\n'
            )
        else:
            script += f"os.execv({arguments[0]!r}, {arguments!r})\n"
        _atomic_write(launcher, script.encode("utf-8"))
        _atomic_write(destination, _windows_xml(sys.executable, launcher))
    else:
        destination = plist_path()
        payload = {
            "Label": LABEL,
            "ProgramArguments": arguments,
            "WorkingDirectory": working_directory,
            "EnvironmentVariables": environment,
            "RunAtLoad": True,
            "KeepAlive": True,
            "ProcessType": "Background",
        }
        _atomic_write(destination, plistlib.dumps(payload))
    return destination


def _launchctl(*arguments: str) -> subprocess.CompletedProcess[str]:
    _check_platform()
    return subprocess.run(
        ["launchctl", *arguments], text=True, capture_output=True, check=True, timeout=10
    )


def _systemctl(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["systemctl", "--user", *arguments], text=True, capture_output=True, check=check, timeout=10
    )


def _schtasks(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["schtasks", *arguments], text=True, capture_output=True, check=check, timeout=10
    )


def enable() -> None:
    _check_platform()
    path = {"darwin": plist_path, "linux": systemd_path, "win32": windows_task_path}[sys.platform]()
    if not path.is_file():
        raise RuntimeError("service is not installed; run idle-herd service install first")
    if sys.platform == "linux":
        _systemctl("daemon-reload")
        _systemctl("enable", "--now", UNIT)
    elif sys.platform == "win32":
        _schtasks("/Create", "/TN", windows_task_name(), "/XML", str(path), "/F")
        _schtasks("/Run", "/TN", windows_task_name())
    else:
        domain = f"gui/{os.getuid()}"
        _launchctl("enable", f"{domain}/{LABEL}")
        _launchctl("bootstrap", domain, str(path))


def disable() -> None:
    """Disable login launch and stop the user service."""
    _check_platform()
    if sys.platform == "linux":
        _systemctl("disable", "--now", UNIT)
    elif sys.platform == "win32":
        running = _windows_running()
        if running is None:
            raise RuntimeError("cannot determine Windows task state")
        _schtasks("/Change", "/TN", windows_task_name(), "/Disable")
        if running:
            _schtasks("/End", "/TN", windows_task_name())
    else:
        domain = f"gui/{os.getuid()}"
        _launchctl("disable", f"{domain}/{LABEL}")
        _launchctl("bootout", f"{domain}/{LABEL}")


def start() -> None:
    _check_platform()
    if sys.platform == "linux":
        _systemctl("start", UNIT)
    elif sys.platform == "win32":
        _schtasks("/Run", "/TN", windows_task_name())
    else:
        _launchctl("kickstart", f"gui/{os.getuid()}/{LABEL}")


def status() -> str:
    _check_platform()
    if sys.platform == "linux":
        return _systemctl("status", "--no-pager", UNIT).stdout
    if sys.platform == "win32":
        return _schtasks("/Query", "/TN", windows_task_name(), "/V", "/FO", "LIST").stdout
    return _launchctl("print", f"gui/{os.getuid()}/{LABEL}").stdout


def platform_name() -> str:
    return {"darwin": "macos", "linux": "linux", "win32": "windows"}.get(
        sys.platform, "unsupported"
    )


def _windows_running() -> bool | None:
    import csv
    import io

    output = _schtasks("/Query", "/TN", windows_task_name(), "/FO", "CSV").stdout[:65536]
    rows = list(csv.DictReader(io.StringIO(output)))
    if len(rows) == 1:
        value = rows[0].get("Status", "").strip().lower()
        if value in ("running", "ready", "disabled"):
            return value == "running"
    return None


def login_state() -> tuple[str, bool | None, bool | None, bool | None]:
    """Read bounded manager output; unknown manager state is represented by None."""
    import csv
    import re

    platform = platform_name()
    if platform == "unsupported":
        return platform, None, None, None
    path = {"macos": plist_path, "linux": systemd_path, "windows": windows_task_path}[platform]()
    installed = path.is_file()
    enabled = running = None
    if platform == "macos":
        try:
            output = _launchctl("print-disabled", f"gui/{os.getuid()}").stdout[:65536]
            match = re.search(r'"' + re.escape(LABEL) + r'"\s*=>\s*(enabled|disabled)', output)
            if match:
                enabled = match.group(1) == "enabled"
        except (OSError, RuntimeError, subprocess.SubprocessError):
            pass
        try:
            match = re.search(r"\bstate\s*=\s*(running|waiting|exited)\b", status()[:65536])
            if match:
                running = match.group(1) == "running"
        except (OSError, RuntimeError, subprocess.SubprocessError):
            pass
    elif platform == "linux":
        try:
            result = _systemctl("is-enabled", UNIT, check=False)
            state = result.stdout[:65536].strip()
            if (result.returncode, state) in ((0, "enabled"), (1, "disabled")):
                enabled = state == "enabled"
        except (OSError, RuntimeError, subprocess.SubprocessError):
            pass
        try:
            result = _systemctl("is-active", UNIT, check=False)
            state = result.stdout[:65536].strip()
            if (result.returncode, state) in ((0, "active"), (3, "inactive"), (3, "failed")):
                running = state == "active"
        except (OSError, RuntimeError, subprocess.SubprocessError):
            pass
    else:
        try:
            output = _schtasks("/Query", "/TN", windows_task_name(), "/XML").stdout[:65536]
            root = ET.fromstring(output)
            value = root.findtext("{*}Settings/{*}Enabled")
            if value is not None and value.lower() in ("true", "false"):
                enabled = value.lower() == "true"
        except (OSError, RuntimeError, subprocess.SubprocessError, ET.ParseError):
            pass
        with contextlib.suppress(OSError, RuntimeError, subprocess.SubprocessError, csv.Error):
            running = _windows_running()
    return platform, installed, enabled, running


def set_login_enabled(enabled: bool, data_dir: str, *, settings=None) -> None:
    """Change login startup only, leaving the current daemon running."""
    _check_platform()
    if enabled:
        path = install(data_dir, settings=settings)
    if sys.platform == "darwin":
        _launchctl("enable" if enabled else "disable", f"gui/{os.getuid()}/{LABEL}")
    elif sys.platform == "linux":
        if enabled:
            _systemctl("daemon-reload")
        _systemctl("enable" if enabled else "disable", UNIT)
    elif enabled:
        # A failed query is ambiguous. Create without /F so a pre-existing task is
        # never overwritten when the manager is inaccessible or returns an error.
        try:
            _schtasks("/Query", "/TN", windows_task_name())
        except (OSError, RuntimeError, subprocess.SubprocessError):
            _schtasks("/Create", "/TN", windows_task_name(), "/XML", str(path))
        else:
            _schtasks("/Change", "/TN", windows_task_name(), "/Enable")
    else:
        _schtasks("/Change", "/TN", windows_task_name(), "/Disable")
