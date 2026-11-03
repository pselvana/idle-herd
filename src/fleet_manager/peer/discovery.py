"""Authenticated, bounded peer discovery. Advertisements are only hints."""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import json
import logging
import os
import tempfile
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from fleet_manager.models.request import normalize_model_name
from fleet_manager.peer.identity import MAX_REVOCATIONS, RevocationStatement

log = logging.getLogger(__name__)
MAX_PEERS = 128
MAX_BODY = 256 * 1024
EXCHANGE_PATH = "/peer/v1/exchange"
_NETWORKS = tuple(
    ipaddress.ip_network(net)
    for net in (
        "127.0.0.0/8",
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "169.254.0.0/16",
        "100.64.0.0/10",
        "::1/128",
        "fc00::/7",
        "fe80::/10",
    )
)


def validate_endpoint(value: str) -> str:
    """Constrain referrals to literal private addresses and HTTPS, without URL tricks."""
    try:
        url = urlsplit(value)
        address = ipaddress.ip_address(url.hostname or "")
        port = url.port
    except ValueError as exc:
        raise ValueError("peer endpoint must contain a literal IP address and valid port") from exc
    if (
        url.scheme != "https"
        or not port
        or url.username is not None
        or url.password is not None
        or url.path not in ("", "/")
        or url.query
        or url.fragment
        or not any(address in net for net in _NETWORKS)
        or address.is_unspecified
        or address.is_multicast
    ):
        raise ValueError("peer endpoint must be HTTPS on an allowed private IP and explicit port")
    host = f"[{address}]" if address.version == 6 else str(address)
    return f"https://{host}:{port}"


class ModelCapability(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=512)
    capabilities: list[str] = Field(default_factory=list, max_length=32)

    @field_validator("capabilities")
    @classmethod
    def bounded_capabilities(cls, value: list[str]) -> list[str]:
        if any(not item or len(item) > 128 for item in value):
            raise ValueError("invalid capability")
        return value


class PeerDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cluster_id: str = Field(min_length=1, max_length=128)
    worker_id: str = Field(min_length=1, max_length=128)
    incarnation: str = Field(min_length=1, max_length=128)
    endpoint: str = Field(max_length=256)
    models: list[ModelCapability] = Field(default_factory=list, max_length=256)

    @field_validator("endpoint")
    @classmethod
    def endpoint_allowed(cls, value: str) -> str:
        return validate_endpoint(value)

    @model_validator(mode="after")
    def bounded_descriptor(self):
        if len(self.model_dump_json().encode()) > 48 * 1024:
            raise ValueError("peer descriptor too large")
        return self


class Exchange(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sender: PeerDescriptor
    peers: list[PeerDescriptor] = Field(default_factory=list, max_length=MAX_PEERS)
    revocations: list[RevocationStatement] = Field(default_factory=list, max_length=MAX_REVOCATIONS)


@dataclass
class _Peer:
    endpoint: str
    worker_id: str | None = None
    descriptor: PeerDescriptor | None = None
    last_seen: float | None = None
    observed_at: float | None = None
    reason: str = "not yet contacted"


class PeerStatus(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    endpoint: str
    worker_id: str | None
    descriptor: PeerDescriptor | None
    reachable: bool
    observed_at: float | None
    reason: str | None


class PeerDirectory:
    def __init__(
        self,
        identity,
        endpoint: str,
        seeds: list[str],
        inventory: Callable[[], Awaitable[PeerDescriptor]],
        data_dir: str | Path,
        *,
        client: httpx.AsyncClient | None = None,
        ttl: float = 30,
        interval: float = 5,
    ):
        if ttl <= 0 or interval <= 0:
            raise ValueError("discovery timers must be positive")
        self.identity = identity
        self.endpoint = validate_endpoint(endpoint)
        self.inventory = inventory
        self.ttl, self.interval = ttl, interval
        self.path = Path(data_dir).expanduser() / "known-peers.json"
        self._peers: dict[str, _Peer] = {}
        self._persist_lock = asyncio.Lock()
        self.changed = asyncio.Event()
        self._wake = asyncio.Event()
        self.participating = None
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(
            verify=identity.client_ssl_context(), timeout=5, follow_redirects=False, trust_env=False
        )
        for seed in seeds:
            self._remember(validate_endpoint(seed))

    def _remember(self, endpoint: str, worker_id: str | None = None) -> None:
        existing = self._peers.get(endpoint)
        if existing is not None:
            if existing.worker_id is None and worker_id is not None:
                existing.worker_id = worker_id
            return
        if endpoint == self.endpoint or len(self._peers) >= MAX_PEERS:
            return
        self._peers[endpoint] = _Peer(endpoint=endpoint, worker_id=worker_id)

    async def open(self) -> None:
        def read():
            try:
                return json.loads(self.path.read_text())
            except FileNotFoundError:
                return []

        try:
            rows = await asyncio.to_thread(read)
            if not isinstance(rows, list) or len(rows) > MAX_PEERS:
                raise ValueError("invalid stored peer list")
            for row in rows:
                endpoint = validate_endpoint(row["endpoint"])
                worker_id = row.get("worker_id")
                if worker_id is not None and (
                    not isinstance(worker_id, str) or len(worker_id) > 128
                ):
                    raise ValueError("invalid stored worker ID")
                self._remember(endpoint, worker_id)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            log.warning("Could not load known peer endpoints: %s", exc)

    async def _persist(self) -> None:
        async with self._persist_lock:
            rows = [
                {"endpoint": p.endpoint, "worker_id": p.worker_id} for p in self._peers.values()
            ]
            path = self.path

            def write():
                path.parent.mkdir(parents=True, exist_ok=True)
                fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".known-peers-")
                try:
                    with os.fdopen(fd, "w") as file:
                        json.dump(rows, file)
                        file.flush()
                        os.fsync(file.fileno())
                    os.replace(temporary, path)
                finally:
                    if os.path.exists(temporary):
                        os.unlink(temporary)

            await asyncio.to_thread(write)

    async def _local(self) -> PeerDescriptor:
        descriptor = await self.inventory()
        if descriptor.endpoint != self.endpoint:
            raise ValueError("local descriptor endpoint mismatch")
        if (
            getattr(self.identity, "worker_id", descriptor.worker_id) != descriptor.worker_id
            or getattr(self.identity, "cluster_id", descriptor.cluster_id) != descriptor.cluster_id
        ):
            raise ValueError("local descriptor identity mismatch")
        return descriptor

    def _accepted(self, peer: PeerDescriptor, local: PeerDescriptor) -> bool:
        accepts = getattr(self.identity, "accepts_peer", None)
        return (
            accepts(peer.worker_id, peer.cluster_id)
            if accepts is not None
            else peer.cluster_id == local.cluster_id
        )

    async def discovered(self, endpoint: str, worker_id: str) -> None:
        """A verified Bonjour identity supplies a candidate, never live capacity."""
        endpoint = validate_endpoint(endpoint)
        if worker_id == self.identity.worker_id:
            return
        old = self._peers.get(endpoint)
        if old is not None and old.worker_id not in {None, worker_id}:
            raise ValueError("endpoint identity conflict")
        # Automatically allocated ports can change after a restart. Replace the
        # pinned worker's previous endpoint instead of exhausting the directory.
        for previous, peer in list(self._peers.items()):
            if peer.worker_id == worker_id and previous != endpoint:
                del self._peers[previous]
        if endpoint not in self._peers and len(self._peers) >= MAX_PEERS:
            raise ValueError("peer directory is full")
        self._remember(endpoint, worker_id)
        self._wake.set()
        await self._persist()

    async def _merge_revocations(self, statements):
        if getattr(self.identity, "lan_trust", None) is not None:
            # Each authority still owns its own revocations. Bonjour federation
            # does not confer authority over another worker's credential domain.
            statements = [s for s in statements if s.cluster_id == self.identity.cluster_id]
        await self.identity.merge_revocations(statements)

    def _referrals(self, peers: list[PeerDescriptor], local: PeerDescriptor) -> None:
        for peer in peers:
            if self._accepted(peer, local) and peer.worker_id != local.worker_id:
                # A referral can initiate a direct verification, never make a peer fresh.
                self._remember(peer.endpoint)

    def _observe(
        self, descriptor: PeerDescriptor, authenticated_worker_id: str, local: PeerDescriptor
    ):
        if (
            descriptor.worker_id != authenticated_worker_id
            or not self._accepted(descriptor, local)
            or descriptor.worker_id == local.worker_id
            or descriptor.endpoint == self.endpoint
        ):
            raise ValueError("authenticated sender does not match descriptor")
        peer = self._peers.get(descriptor.endpoint)
        if peer is None:
            self._remember(descriptor.endpoint, authenticated_worker_id)
            peer = self._peers.get(descriptor.endpoint)
        if peer is None:
            raise ValueError("peer directory is full")
        if peer.worker_id is not None and peer.worker_id != authenticated_worker_id:
            raise ValueError("endpoint identity conflict")
        peer.worker_id = authenticated_worker_id
        peer.descriptor = descriptor
        peer.last_seen = time.monotonic()
        peer.observed_at = time.time()
        peer.reason = ""
        self.changed.set()

    async def exchange(self, authenticated_worker_id: str, payload: Exchange) -> Exchange:
        if self.participating is not None and not await self.participating():
            raise ValueError("manual_offline")
        local = await self._local()
        if (
            payload.sender.worker_id != authenticated_worker_id
            or not self._accepted(payload.sender, local)
            or payload.sender.worker_id == local.worker_id
            or payload.sender.endpoint == self.endpoint
        ):
            raise ValueError("authenticated sender does not match descriptor")
        await self._merge_revocations(payload.revocations)
        self._observe(payload.sender, authenticated_worker_id, local)
        self._referrals(payload.peers, local)
        await self._persist()
        return Exchange(
            sender=local, peers=self._advertisements(), revocations=self.identity.revocations()
        )

    def _advertisements(self) -> list[PeerDescriptor]:
        result = []
        size = 0
        for peer in self._peers.values():
            if peer.descriptor is not None:
                size += len(peer.descriptor.model_dump_json().encode()) + 1
                if size > 140 * 1024:
                    break
                result.append(peer.descriptor)
        return result

    async def _probe(self, peer: _Peer, local: PeerDescriptor) -> None:
        payload = Exchange(
            sender=local, peers=self._advertisements(), revocations=self.identity.revocations()
        )
        body = payload.model_dump_json().encode()
        if len(body) > MAX_BODY:
            raise ValueError("local discovery payload too large")
        headers = self.identity.sign_request("POST", EXCHANGE_PATH, body)
        headers["content-type"] = "application/json"
        try:
            async with self.client.stream(
                "POST", peer.endpoint + EXCHANGE_PATH, content=body, headers=headers
            ) as response:
                content = bytearray()
                async for chunk in response.aiter_bytes():
                    content.extend(chunk)
                    if len(content) > MAX_BODY:
                        raise ValueError("peer discovery response too large")
                signer = self.identity.verify_response(
                    headers["x-idle-nonce"],
                    response.status_code,
                    bytes(content),
                    response.headers,
                    expected_worker_id=peer.worker_id,
                )
                response.raise_for_status()
                exchange = Exchange.model_validate_json(content)
                if exchange.sender.endpoint != peer.endpoint:
                    raise ValueError("peer endpoint does not match contacted endpoint")
                await self._merge_revocations(exchange.revocations)
                self._observe(exchange.sender, signer, local)
                self._referrals(exchange.peers, local)
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            peer.reason = f"{type(exc).__name__}: {str(exc)[:200]}"
            # A failed direct probe makes the peer unavailable immediately.
            peer.last_seen = None

    async def refresh(self) -> None:
        if self.participating is not None and not await self.participating():
            return
        local = await self._local()
        await asyncio.gather(*(self._probe(peer, local) for peer in list(self._peers.values())))
        await self._persist()

    def snapshot(self) -> list[PeerStatus]:
        now = time.monotonic()
        result = []
        for peer in self._peers.values():
            live = peer.last_seen is not None and now - peer.last_seen <= self.ttl
            reason = None if live else peer.reason or "last direct exchange expired"
            result.append(
                PeerStatus(
                    endpoint=peer.endpoint,
                    worker_id=peer.worker_id,
                    descriptor=peer.descriptor,
                    reachable=live,
                    observed_at=peer.observed_at,
                    reason=reason,
                )
            )
        return result

    def candidates(self, model: str) -> list[PeerDescriptor]:
        wanted = normalize_model_name(model)
        return [
            status.descriptor
            for status in self.snapshot()
            if status.reachable
            and status.descriptor is not None
            and any(normalize_model_name(m.name) == wanted for m in status.descriptor.models)
        ]

    def unreachable(self, endpoint: str, reason: str):
        peer = self._peers.get(endpoint)
        if peer:
            peer.last_seen = None
            peer.reason = reason
            self.changed.set()

    async def run(self) -> None:
        while True:
            self._wake.clear()
            try:
                await self.refresh()
            except (OSError, ValueError, httpx.HTTPError) as exc:
                log.warning("Peer discovery refresh failed: %s", exc)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self.interval)

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()
