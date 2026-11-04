"""Bounded host labels for authenticated diagnostics, never routing authority."""

import re
import socket


def hostname_label(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.rstrip(".")
    if not value or len(value) > 253:
        return None
    if any(
        not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", part)
        for part in value.split(".")
    ):
        return None
    return value


def local_hostname() -> str | None:
    try:
        return hostname_label(socket.gethostname())
    except OSError:
        return None
