"""Explicit macOS per-user launchd service management."""

from __future__ import annotations

import json
import os
import plistlib
import subprocess
import sys
import tempfile
from pathlib import Path

LABEL = "com.idle-herd.daemon"


def plist_path() -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{LABEL}.plist"


def _check_platform() -> None:
    if sys.platform != "darwin":
        raise RuntimeError("launchd user services are available only on macOS")


def install(data_dir: str, executable: str | None = None, *, settings=None) -> Path:
    """Write a private launch agent with the effective daemon settings."""
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
    destination = plist_path()
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "Label": LABEL,
        "ProgramArguments": arguments,
        "WorkingDirectory": str(Path.cwd().resolve()),
        "EnvironmentVariables": environment,
        "RunAtLoad": True,
        "KeepAlive": True,
        "ProcessType": "Background",
    }
    fd, temporary = tempfile.mkstemp(prefix=".idle-herd-", dir=destination.parent)
    try:
        with os.fdopen(fd, "wb") as file:
            plistlib.dump(payload, file)
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return destination


def _launchctl(*arguments: str) -> subprocess.CompletedProcess[str]:
    _check_platform()
    return subprocess.run(
        ["launchctl", *arguments], text=True, capture_output=True, check=True, timeout=10
    )


def enable() -> None:
    _check_platform()
    path = plist_path()
    if not path.is_file():
        raise RuntimeError("service is not installed; run idle-herd service install first")
    domain = f"gui/{os.getuid()}"
    _launchctl("enable", f"{domain}/{LABEL}")
    _launchctl("bootstrap", domain, str(path))


def disable() -> None:
    """Disable login launch and unload the service if it is registered."""
    _check_platform()
    domain = f"gui/{os.getuid()}"
    _launchctl("disable", f"{domain}/{LABEL}")
    _launchctl("bootout", f"{domain}/{LABEL}")


def start() -> None:
    _check_platform()
    _launchctl("kickstart", f"gui/{os.getuid()}/{LABEL}")


def status() -> str:
    _check_platform()
    return _launchctl("print", f"gui/{os.getuid()}/{LABEL}").stdout
