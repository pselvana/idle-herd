"""Separate local read and control bearer credentials, never peer credentials."""

from __future__ import annotations

import json
import os
import secrets
import stat
from pathlib import Path

from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field


class ControlCredentials(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: int = Field(default=1, ge=1, le=1)
    read_token: str = Field(min_length=32, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    control_token: str = Field(min_length=32, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")


def load_or_create_credentials(data_dir: str | Path, *, create: bool = True) -> ControlCredentials:
    directory = Path(data_dir).expanduser()
    if create:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory_info = directory.lstat()
    if not stat.S_ISDIR(directory_info.st_mode):
        raise PermissionError("control data directory must be a directory, not a link")
    if hasattr(os, "getuid") and directory_info.st_uid != os.getuid():
        raise PermissionError("control data directory owned by another user")
    if create:
        directory.chmod(0o700)
    elif os.name != "nt" and directory_info.st_mode & 0o077:
        raise PermissionError("insecure control data directory")
    path = directory / "control-credentials.json"
    generated = ControlCredentials(
        read_token=secrets.token_urlsafe(32), control_token=secrets.token_urlsafe(32)
    )
    if create:
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            pass
        else:
            with os.fdopen(fd, "w") as stream:
                json.dump(generated.model_dump(), stream)
                stream.flush()
                os.fsync(stream.fileno())
    # Reject links and insecure permissions instead of following attacker-controlled files.
    path_info = path.lstat()
    if not stat.S_ISREG(path_info.st_mode):
        raise PermissionError("control credentials must be a regular file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    with os.fdopen(fd) as stream:
        info = os.fstat(stream.fileno())
        if (info.st_dev, info.st_ino) != (path_info.st_dev, path_info.st_ino):
            raise PermissionError("control credentials changed while opening")
        if not stat.S_ISREG(info.st_mode) or (os.name != "nt" and info.st_mode & 0o077):
            raise PermissionError("insecure control credentials")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise PermissionError("control credentials owned by another user")
        credentials = ControlCredentials.model_validate_json(stream.read(4097))
    if secrets.compare_digest(credentials.read_token, credentials.control_token):
        raise ValueError("control credentials must be distinct")
    return credentials


def authorize(request: Request, *, control: bool = False) -> None:
    # Read-only same-origin browser fetches power the local dashboard. Writes
    # reject browser fetch metadata even when a control token is supplied.
    fetch_site = request.headers.get("sec-fetch-site")
    if "origin" in request.headers or (
        fetch_site is not None and (control or fetch_site not in {"same-origin", "none"})
    ):
        raise HTTPException(403, "browser request is not permitted")
    value = request.headers.get("authorization", "")
    scheme, _, token = value.partition(" ")
    creds = request.app.state.control_credentials
    candidate = token.encode("utf-8")
    is_control = scheme.lower() == "bearer" and secrets.compare_digest(
        candidate, creds.control_token.encode("utf-8")
    )
    is_read = scheme.lower() == "bearer" and secrets.compare_digest(
        candidate, creds.read_token.encode("utf-8")
    )
    if not (is_control or (is_read and not control)):
        raise HTTPException(403 if is_read else 401, "insufficient local scope")
