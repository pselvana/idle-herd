import asyncio
import socket
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from zeroconf import ServiceStateChange
from zeroconf.asyncio import AsyncServiceInfo

from fleet_manager.peer.bonjour import (
    SERVICE_TYPE,
    BonjourPeers,
    announcement_from_info,
    properties,
)
from fleet_manager.peer.discovery import PeerDirectory
from fleet_manager.peer.identity import PeerIdentity
from fleet_manager.peer.lan_trust import LanAnnouncement, LanTrust
from fleet_manager.peer.models import PeerSettings


async def identity(path):
    return await PeerIdentity.open(path, str(uuid.uuid4()), str(uuid.uuid4()))


def service(record, *, address="192.168.1.20", port=17435):
    return AsyncServiceInfo(
        SERVICE_TYPE,
        "worker." + SERVICE_TYPE,
        addresses=[socket.inet_aton(address)],
        port=port,
        properties=properties(record),
    )


async def test_txt_proof_matches_resolved_address_and_port(tmp_path):
    remote = await identity(tmp_path)
    record = LanAnnouncement.create(remote, "https://192.168.1.20:17435")
    info = service(record)
    assert announcement_from_info(info) == record
    assert all(len(k) + len(v) + 1 < 256 for k, v in info.properties.items())
    for bad in (service(record, address="192.168.1.21"), service(record, port=17436)):
        with pytest.raises(ValueError, match="resolved service"):
            announcement_from_info(bad)
    info = SimpleNamespace(properties={b"version": b"1", b"chunks": b"99999"})
    with pytest.raises(ValueError, match="chunk count"):
        announcement_from_info(info)


async def test_signed_hint_requires_direct_exchange_and_replaces_old_port(tmp_path):
    local, remote = await identity(tmp_path / "a"), await identity(tmp_path / "b")
    directory = PeerDirectory(local, "https://192.168.1.10:17435", [], AsyncMock(), tmp_path / "a")
    try:
        await directory.discovered("https://192.168.1.20:17435", remote.worker_id)
        assert not directory.snapshot()[0].reachable
        assert directory.candidates("tiny") == []
        await directory.discovered("https://192.168.1.20:17436", remote.worker_id)
        assert [p.endpoint for p in directory.snapshot()] == ["https://192.168.1.20:17436"]
        with pytest.raises(ValueError, match="identity conflict"):
            await directory.discovered("https://192.168.1.20:17436", "impostor")
    finally:
        await directory.close()


async def test_offline_withdraws_and_online_readvertises_without_owning_daemon(
    tmp_path, monkeypatch
):
    local = await identity(tmp_path)
    ledger = SimpleNamespace(cluster_participating=AsyncMock(return_value=True))
    app = SimpleNamespace(
        state=SimpleNamespace(
            identity=local,
            ledger=ledger,
            directory=SimpleNamespace(endpoint="https://192.168.1.10:17435"),
        )
    )
    settings = PeerSettings(peer_endpoint="https://192.168.1.10:17435")
    azc = SimpleNamespace(
        zeroconf=object(),
        async_register_service=AsyncMock(),
        async_update_service=AsyncMock(),
        async_unregister_service=AsyncMock(),
        async_close=AsyncMock(),
    )
    browser = SimpleNamespace(async_cancel=AsyncMock())
    monkeypatch.setattr("fleet_manager.peer.bonjour.AsyncZeroconf", Mock(return_value=azc))
    make_browser = Mock(return_value=browser)
    monkeypatch.setattr("fleet_manager.peer.bonjour.AsyncServiceBrowser", make_browser)
    peer = BonjourPeers(app, settings)
    await peer.start()
    try:
        azc.async_register_service.assert_awaited_once()
        ledger.cluster_participating.return_value = False
        await peer.sync()
        azc.async_unregister_service.assert_awaited_once()
        browser.async_cancel.assert_awaited_once()
        ledger.cluster_participating.return_value = True
        await peer.sync()
        assert azc.async_register_service.await_count == 2
        assert make_browser.call_count == 2
    finally:
        await peer.close()
    azc.async_close.assert_awaited_once()


async def test_resolved_peer_is_automatically_trusted_and_goodbye_marks_unreachable(
    tmp_path, monkeypatch
):
    local, remote = await identity(tmp_path / "a"), await identity(tmp_path / "b")
    local.lan_trust = await LanTrust.open(local)
    record = LanAnnouncement.create(remote, "https://192.168.1.20:17435")
    info = service(record)
    resolved = SimpleNamespace(
        async_request=AsyncMock(return_value=True),
        properties=info.properties,
        port=info.port,
        parsed_addresses=info.parsed_addresses,
    )
    monkeypatch.setattr("fleet_manager.peer.bonjour.AsyncServiceInfo", Mock(return_value=resolved))
    directory = SimpleNamespace(discovered=AsyncMock(), unreachable=Mock())
    app = SimpleNamespace(
        state=SimpleNamespace(
            identity=local,
            directory=directory,
            ledger=SimpleNamespace(cluster_participating=AsyncMock(return_value=True)),
        )
    )
    peer = BonjourPeers(app, PeerSettings())
    peer.azc = SimpleNamespace(zeroconf=object())
    await peer.resolve(SERVICE_TYPE, "remote." + SERVICE_TYPE)
    assert local.accepts_peer(remote.worker_id, remote.cluster_id)
    directory.discovered.assert_awaited_once_with(record.endpoint, remote.worker_id)
    peer.changed(None, SERVICE_TYPE, "remote." + SERVICE_TYPE, ServiceStateChange.Removed)
    directory.unreachable.assert_called_once_with(record.endpoint, "Bonjour service withdrawn")
    # Reject a different authority in strict mode; discovering it grants nothing.
    local.lan_trust = None
    directory.discovered.reset_mock()
    await peer.resolve(SERVICE_TYPE, "remote." + SERVICE_TYPE)
    directory.discovered.assert_not_awaited()


async def test_discovery_resolution_is_bounded(tmp_path, monkeypatch):
    local = await identity(tmp_path)
    app = SimpleNamespace(state=SimpleNamespace(identity=local))
    peer = BonjourPeers(app, PeerSettings())

    # A blocked resolver cannot create unbounded tasks from LAN announcements.
    async def blocked(*args):
        await asyncio.Event().wait()

    monkeypatch.setattr(peer, "resolve", blocked)
    for i in range(1000):
        peer.changed(None, SERVICE_TYPE, f"remote-{i}.{SERVICE_TYPE}", ServiceStateChange.Added)
    assert len(peer.resolving) == 128
    for task in peer.resolving.values():
        task.cancel()
    await asyncio.gather(*list(peer.resolving.values()), return_exceptions=True)
