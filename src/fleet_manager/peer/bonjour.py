"""Native, symmetric Bonjour discovery for the authenticated inference mesh."""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import logging
import socket
from urllib.parse import urlsplit

import psutil
from zeroconf import Error as ZeroconfError
from zeroconf import IPVersion, ServiceStateChange
from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf

from fleet_manager.common.system_metrics import get_local_ip
from fleet_manager.peer.discovery import MAX_PEERS, validate_endpoint
from fleet_manager.peer.identity import IdentityError, _check_cert
from fleet_manager.peer.lan_trust import LanAnnouncement, certificates

log = logging.getLogger(__name__)
SERVICE_TYPE = "_idle-herd._tcp.local."
MAX_TXT = 4096
CHUNK_SIZE = 200
MAX_CHUNKS = 20


def lan_ip() -> str:
    """Prefer the default LAN route, with an interface fallback for isolated LANs."""
    preferred = get_local_ip()
    stats = psutil.net_if_stats()
    candidates = [preferred] + [
        item.address
        for name, addresses in psutil.net_if_addrs().items()
        if name in stats and stats[name].isup
        for item in addresses
        if item.family == socket.AF_INET
    ]
    for candidate in candidates:
        address = ipaddress.ip_address(candidate)
        if address.version != 4 or address.is_loopback or address.is_link_local:
            continue
        try:
            validate_endpoint(f"https://{address}:1")
            return str(address)
        except ValueError:
            pass
    return "127.0.0.1"


def automatic_listener():
    """Reserve an unused OS-assigned peer port without changing the backend map."""
    address = lan_ip()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Keep the same listener/port as interfaces roam. Only peer RPC is mounted.
        listener.bind(("0.0.0.0", 0))
        port = listener.getsockname()[1]
        if 11434 <= port <= 11449:
            raise OSError("OS assigned a reserved backend port")
        listener.setblocking(False)
        return listener, f"https://{address}:{port}"
    except BaseException:
        listener.close()
        raise


def properties(announcement: LanAnnouncement) -> dict[bytes, bytes]:
    content = announcement.model_dump_json().encode()
    if len(content) > MAX_TXT:
        raise ValueError("Bonjour identity record too large")
    chunks = [content[i : i + CHUNK_SIZE] for i in range(0, len(content), CHUNK_SIZE)]
    return {b"version": b"1", b"chunks": str(len(chunks)).encode()} | {
        f"id{i}".encode(): chunk for i, chunk in enumerate(chunks)
    }


def announcement_from_info(info) -> LanAnnouncement:
    props = info.properties
    if sum(len(k) + len(v or b"") for k, v in props.items()) > MAX_TXT + 256:
        raise ValueError("Bonjour TXT record too large")
    if props.get(b"version") != b"1":
        raise ValueError("unsupported Bonjour protocol")
    count = int(props.get(b"chunks", b"0"))
    if not 1 <= count <= MAX_CHUNKS:
        raise ValueError("invalid Bonjour chunk count")
    content = b"".join(props[f"id{i}".encode()] for i in range(count))
    if len(content) > MAX_TXT:
        raise ValueError("Bonjour identity record too large")
    record = LanAnnouncement.model_validate_json(content)
    endpoint = urlsplit(validate_endpoint(record.endpoint))
    if (
        endpoint.port != info.port
        or endpoint.hostname not in info.parsed_addresses()
        or 11434 <= endpoint.port <= 11449
    ):
        raise ValueError("Bonjour endpoint does not match resolved service")
    record.verify()
    return record


class BonjourPeers:
    def __init__(self, app, settings, *, automatic_address: bool = False):
        self.app, self.settings = app, settings
        self.automatic_address = automatic_address
        self.service_type = settings.mdns_service_type
        self.name = f"{app.state.identity.worker_id}.{self.service_type}"
        self.azc = None
        self.browser = None
        self.info = None
        self.task = None
        self.resolving: dict[str, asyncio.Task] = {}
        self.endpoints: dict[str, str] = {}
        self.closed = False

    async def start(self):
        self.azc = AsyncZeroconf(ip_version=IPVersion.All)
        try:
            await self.sync()
            self.task = asyncio.create_task(self.run())
        except BaseException:
            await self.close()
            raise

    def changed(self, zeroconf, service_type, name, state_change):
        if self.closed or name == self.name:
            return
        if state_change is ServiceStateChange.Removed:
            task = self.resolving.pop(name, None)
            if task:
                task.cancel()
            endpoint = self.endpoints.pop(name, None)
            if endpoint and self.app.state.directory is not None:
                self.app.state.directory.unreachable(endpoint, "Bonjour service withdrawn")
            return
        if name in self.resolving or len(self.resolving) >= MAX_PEERS:
            return
        if name not in self.endpoints and len(self.endpoints) >= MAX_PEERS:
            return
        task = asyncio.create_task(self.resolve(service_type, name))
        self.resolving[name] = task

        def completed(done):
            if self.resolving.get(name) is done:
                self.resolving.pop(name, None)

        task.add_done_callback(completed)

    async def resolve(self, service_type, name):
        try:
            info = AsyncServiceInfo(service_type, name)
            if not await info.async_request(self.azc.zeroconf, 2000):
                return
            record = announcement_from_info(info)
            identity = self.app.state.identity
            if record.credential.worker_id == identity.worker_id:
                return
            if not await self.app.state.ledger.cluster_participating():
                return
            if identity.lan_trust is not None:
                await identity.lan_trust.remember(record)
            else:
                # Strict mode discovers enrolled peers without trusting a new CA.
                _, cert = certificates(record.credential)
                _check_cert(cert, identity.ca, identity.cluster_id)
            if not await self.app.state.ledger.cluster_participating():
                return
            await self.app.state.directory.discovered(record.endpoint, record.credential.worker_id)
            self.endpoints[name] = record.endpoint
        except (OSError, ValueError, TypeError, KeyError) as exc:
            # Do not render untrusted TXT contents in logs or owner diagnostics.
            log.warning("Bonjour peer rejected: %s", type(exc).__name__)

    async def withdraw(self):
        if self.browser is not None:
            await self.browser.async_cancel()
            self.browser = None
        if self.info is not None:
            await self.azc.async_unregister_service(self.info)
            self.info = None
        tasks = list(self.resolving.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def sync(self):
        if not await self.app.state.ledger.cluster_participating():
            await self.withdraw()
            return
        self.app.state.identity.check_local()
        if self.browser is None:
            self.browser = AsyncServiceBrowser(
                self.azc.zeroconf, self.service_type, handlers=[self.changed]
            )
        if self.automatic_address:
            host = await asyncio.to_thread(lan_ip)
            port = urlsplit(self.settings.peer_endpoint).port
            endpoint = f"https://{host}:{port}"
            self.settings.peer_endpoint = endpoint
            self.app.state.directory.endpoint = endpoint
        endpoint = self.settings.peer_endpoint
        url = urlsplit(endpoint)
        address = ipaddress.ip_address(url.hostname)
        if address.is_loopback:
            if self.info is not None:
                await self.azc.async_unregister_service(self.info)
                self.info = None
            return
        if (
            self.info is not None
            and self.info.port == url.port
            and str(address) in self.info.parsed_addresses()
        ):
            return
        record = LanAnnouncement.create(self.app.state.identity, endpoint)
        info = AsyncServiceInfo(
            self.service_type,
            self.name,
            addresses=[address.packed],
            port=url.port,
            properties=properties(record),
            server=f"idle-herd-{self.app.state.identity.worker_id}.local.",
        )
        if self.info is None:
            await self.azc.async_register_service(info, ttl=30)
        else:
            await self.azc.async_update_service(info)
        self.info = info
        self.app.state.mdns_error = None

    async def run(self):
        while True:
            await asyncio.sleep(min(self.settings.peer_interval, 5))
            try:
                await self.sync()
            except (OSError, ValueError, IdentityError, ZeroconfError) as exc:
                self.app.state.mdns_error = type(exc).__name__
                log.warning("Bonjour discovery unavailable: %s", type(exc).__name__)
                await self.withdraw()

    async def close(self):
        self.closed = True
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task
        if self.azc:
            try:
                await self.withdraw()
            finally:
                await self.azc.async_close()
