import uuid

import pytest

from fleet_manager.peer.identity import IdentityError, PeerIdentity
from fleet_manager.peer.lan_trust import LanAnnouncement, LanTrust


async def identity(path, worker=None):
    return await PeerIdentity.open(path, worker or str(uuid.uuid4()), str(uuid.uuid4()))


async def test_independent_nodes_authenticate_only_after_signed_bonjour_trust(tmp_path):
    a, b = await identity(tmp_path / "a"), await identity(tmp_path / "b")
    a.lan_trust = await LanTrust.open(a)
    # Exercise a context created before discovery: in-flight clients stay usable.
    context = a.client_ssl_context()
    before = context.cert_store_stats()["x509_ca"]
    with pytest.raises(IdentityError):
        a.verify_request(
            "POST", "/peer/v1/exchange", b"{}", b.sign_request("POST", "/peer/v1/exchange", b"{}")
        )
    record = LanAnnouncement.create(b, "https://192.168.1.20:17435")
    await a.lan_trust.remember(record)
    assert context.cert_store_stats()["x509_ca"] == before + 1
    assert a.accepts_peer(b.worker_id, b.cluster_id)
    assert not a.accepts_peer(b.worker_id, a.cluster_id)
    assert not a.accepts_peer(b.worker_id, "another-cluster")
    headers = b.sign_request("POST", "/peer/v1/exchange", b"{}")
    assert a.verify_request("POST", "/peer/v1/exchange", b"{}", headers) == b.worker_id
    with pytest.raises(IdentityError, match="replayed"):
        a.verify_request("POST", "/peer/v1/exchange", b"{}", headers)
    restored = await PeerIdentity.open(tmp_path / "a", a.worker_id, a.cluster_id)
    # Automatic trust can be switched off even with existing stored pins.
    with pytest.raises(IdentityError):
        restored.verify_request(
            "POST", "/peer/v1/exchange", b"{}", b.sign_request("POST", "/peer/v1/exchange", b"{}")
        )
    restored.lan_trust = await LanTrust.open(restored)
    assert (
        restored.verify_request(
            "POST", "/peer/v1/exchange", b"{}", b.sign_request("POST", "/peer/v1/exchange", b"{}")
        )
        == b.worker_id
    )
    assert restored.cluster_id == a.cluster_id != b.cluster_id


async def test_bonjour_endpoint_and_certificate_are_signed(tmp_path):
    a = await identity(tmp_path / "a")
    record = LanAnnouncement.create(a, "https://192.168.1.10:17435")
    record.verify()
    with pytest.raises(IdentityError, match="identity proof"):
        record.model_copy(update={"endpoint": "https://192.168.1.99:17435"}).verify()
    with pytest.raises(IdentityError, match="identity mismatch"):
        record.model_copy(
            update={"credential": record.credential.model_copy(update={"worker_id": "forged"})}
        ).verify()


async def test_existing_pin_cannot_be_replaced_by_new_lan_key(tmp_path):
    a, b = await identity(tmp_path / "a"), await identity(tmp_path / "b")
    a.lan_trust = await LanTrust.open(a)
    await a.lan_trust.remember(LanAnnouncement.create(b, "https://192.168.1.20:17435"))
    saved = a.lan_trust.path.read_bytes()
    impostor = await identity(tmp_path / "impostor", b.worker_id)
    with pytest.raises(IdentityError, match="identity changed"):
        await a.lan_trust.remember(LanAnnouncement.create(impostor, "https://192.168.1.20:17436"))
    assert a.lan_trust.path.read_bytes() == saved
    assert a.verify_peer_certificate(b.cert) == b.worker_id
    with pytest.raises(IdentityError):
        a.verify_peer_certificate(impostor.cert)


async def test_self_id_conflict_never_enrolls(tmp_path):
    a = await identity(tmp_path / "a")
    impostor = await identity(tmp_path / "impostor", a.worker_id)
    trust = await LanTrust.open(a)
    with pytest.raises(IdentityError, match="local identity"):
        await trust.remember(LanAnnouncement.create(impostor, "https://192.168.1.20:17435"))
    assert not trust.path.exists()
