"""Async convenience client for worker-issued capacity reservations."""

from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager

import httpx

from fleet_manager.peer.models import Lease

log = logging.getLogger(__name__)


async def _release(client, url, lease):
    try:
        response = await client.delete(
            url, headers={"X-Idle-Herd-Reservation": lease.token}, timeout=5
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        log.warning("Reservation release failed: HTTP %s", exc.response.status_code)
    except httpx.HTTPError as exc:
        log.warning("Reservation release failed: %s", type(exc).__name__)


@asynccontextmanager
async def acquire(
    model: str,
    timeout: float = 60,
    base_url: str = "http://127.0.0.1:11435",
    client: httpx.AsyncClient | None = None,
):
    """Acquire one worker lease and release it when the context exits.

    A failed release leaves the worker authoritative; the token must not be reused.
    """
    if not 0 <= timeout <= 120:
        raise ValueError("timeout must be between 0 and 120 seconds")
    own_client = client is None
    client = client or httpx.AsyncClient(base_url=base_url, timeout=timeout + 5)
    try:
        response = await client.post(
            base_url.rstrip("/") + "/v1/capacity/acquire",
            json={"model": model, "request_id": str(uuid.uuid4()), "wait_ms": int(timeout * 1000)},
            timeout=timeout + 5,
        )
        response.raise_for_status()
        lease = Lease.model_validate(response.json())
        try:
            yield lease
        finally:
            task = asyncio.create_task(
                _release(
                    client,
                    base_url.rstrip("/") + "/v1/capacity/reservations/" + lease.reservation_id,
                    lease,
                )
            )
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise
    finally:
        if own_client:
            await client.aclose()
