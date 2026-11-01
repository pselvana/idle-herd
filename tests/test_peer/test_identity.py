import json
import ssl

import pytest

from fleet_manager.peer.identity import (
    DeviceCredential,
    EnrollmentRequest,
    IdentityError,
    PeerIdentity,
)


@pytest.mark.asyncio
async def test_enrollment_and_authenticated_messages(tmp_path):
    admin = await PeerIdentity.open(tmp_path / "admin", "worker-a", "cluster-a")
    request = await PeerIdentity.enrollment_request(tmp_path / "peer", "worker-b")
    assert "PRIVATE" not in request.model_dump_json()
    assert EnrollmentRequest.model_validate_json(request.model_dump_json()) == request
    credential = await admin.issue_credential(request)
    assert "PRIVATE" not in credential.model_dump_json()
    assert DeviceCredential.model_validate_json(credential.model_dump_json()) == credential
    with pytest.raises(ValueError):
        EnrollmentRequest.model_validate({**request.model_dump(), "version": "1"})
    with pytest.raises(ValueError):
        DeviceCredential.model_validate({**credential.model_dump(), "extra": "forbidden"})
    with pytest.raises(IdentityError, match="fingerprint"):
        await PeerIdentity.import_credential(tmp_path / "peer", credential, "00" * 32)
    await PeerIdentity.import_credential(tmp_path / "peer", credential, admin.fingerprint)
    peer = await PeerIdentity.open(tmp_path / "peer", "worker-b", "cluster-a")
    reopened = await PeerIdentity.open(tmp_path / "peer", "worker-b", "cluster-a")
    assert reopened.fingerprint == admin.fingerprint
    assert peer.client_ssl_context().verify_mode == ssl.CERT_REQUIRED
    headers = peer.sign_request("POST", "/peer/hello", b"hello")
    with pytest.raises(IdentityError):
        admin.verify_request("POST", "/peer/hello", b"changed", headers)
    assert admin.verify_request("POST", "/peer/hello", b"hello", headers) == "worker-b"
    with pytest.raises(IdentityError, match="replayed"):
        admin.verify_request("POST", "/peer/hello", b"hello", headers)
    response = admin.sign_response(headers["x-idle-nonce"], 200, b"answer")
    with pytest.raises(IdentityError):
        peer.verify_response("wrong", 200, b"answer", response)
    assert (
        peer.verify_response(headers["x-idle-nonce"], 200, b"answer", response, "worker-a")
        == "worker-a"
    )


@pytest.mark.asyncio
async def test_enrollment_rejects_conflicts_and_untrusted_peers(tmp_path):
    admin = await PeerIdentity.open(tmp_path / "admin", "worker-a", "cluster-a")
    outsider = await PeerIdentity.open(tmp_path / "outsider", "worker-c", "cluster-c")
    req = await PeerIdentity.enrollment_request(tmp_path / "one", "worker-b")
    cred = await admin.issue_credential(req)
    conflicting = await PeerIdentity.enrollment_request(tmp_path / "two", "worker-b")
    with pytest.raises(IdentityError, match="already bound"):
        await admin.issue_credential(conflicting)
    with pytest.raises(IdentityError):
        await PeerIdentity.import_credential(tmp_path / "two", cred, admin.fingerprint)
    headers = outsider.sign_request("GET", "/peer/hello", b"")
    with pytest.raises(IdentityError):
        admin.verify_request("GET", "/peer/hello", b"", headers)
    with pytest.raises(IdentityError):
        await PeerIdentity.open(tmp_path / "admin", "worker-a", "other-cluster")


@pytest.mark.asyncio
async def test_revocation_and_stale_messages(tmp_path, monkeypatch):
    admin = await PeerIdentity.open(tmp_path / "admin", "worker-a", "cluster-a")
    req = await PeerIdentity.enrollment_request(tmp_path / "peer", "worker-b")
    cred = await admin.issue_credential(req)
    await PeerIdentity.import_credential(tmp_path / "peer", cred, admin.fingerprint)
    peer = await PeerIdentity.open(tmp_path / "peer", "worker-b", "cluster-a")
    signed = peer.sign_request("GET", "/", b"")
    monkeypatch.setattr(
        "fleet_manager.peer.identity.time.time", lambda: int(signed["x-idle-time"]) + 121
    )
    with pytest.raises(IdentityError, match="stale"):
        admin.verify_request("GET", "/", b"", signed)
    monkeypatch.undo()
    (admin.directory / "revoked.json").write_text(json.dumps([str(peer.cert.serial_number)]))
    admin = await PeerIdentity.open(tmp_path / "admin", "worker-a", "cluster-a")
    with pytest.raises(IdentityError, match="revoked"):
        admin.verify_request("GET", "/", b"", peer.sign_request("GET", "/", b""))
    (admin.directory / "revoked.json").write_text('{"bad": "format"}')
    with pytest.raises(IdentityError, match="revocation list"):
        await PeerIdentity.open(tmp_path / "admin", "worker-a", "cluster-a")


@pytest.mark.asyncio
async def test_stream_response_requires_same_tls_certificate(tmp_path):
    import httpx
    from cryptography.hazmat.primitives.serialization import Encoding

    admin = await PeerIdentity.open(tmp_path / "admin", "worker-a", "cluster-a")
    request = await PeerIdentity.enrollment_request(tmp_path / "peer", "worker-b")
    credential = await admin.issue_credential(request)
    await PeerIdentity.import_credential(tmp_path / "peer", credential, admin.fingerprint)
    peer = await PeerIdentity.open(tmp_path / "peer", "worker-b", "cluster-a")

    class TLS:
        def __init__(self, cert):
            self.cert = cert

        def getpeercert(self, binary_form=False):
            assert binary_form
            return self.cert

    class Stream:
        def __init__(self, tls):
            self.tls = tls

        def get_extra_info(self, name):
            assert name == "ssl_object"
            return self.tls

    headers = peer.sign_response("request-nonce", 200, b"")
    no_tls = httpx.Response(200, headers=headers)
    with pytest.raises(IdentityError, match="TLS"):
        admin.verify_stream_response("request-nonce", no_tls, "worker-b")
    wrong_tls = httpx.Response(
        200,
        headers=headers,
        extensions={"network_stream": Stream(TLS(admin.cert.public_bytes(Encoding.DER)))},
    )
    with pytest.raises(IdentityError, match="does not match"):
        admin.verify_stream_response("request-nonce", wrong_tls, "worker-b")
    real_tls = httpx.Response(
        200,
        headers=headers,
        extensions={"network_stream": Stream(TLS(peer.cert.public_bytes(Encoding.DER)))},
    )
    assert admin.verify_stream_response("request-nonce", real_tls, "worker-b") == "worker-b"
    with pytest.raises(IdentityError, match="replayed"):
        admin.verify_stream_response("request-nonce", real_tls, "worker-b")


@pytest.mark.asyncio
async def test_authority_revocation_gossips_and_survives_restart(tmp_path):
    import httpx

    from fleet_manager.peer.discovery import Exchange, PeerDescriptor, PeerDirectory

    admin = await PeerIdentity.open(tmp_path / "a", "a", "cluster")

    async def enroll(name):
        request = await PeerIdentity.enrollment_request(tmp_path / name, name)
        credential = await admin.issue_credential(request)
        await PeerIdentity.import_credential(tmp_path / name, credential, admin.fingerprint)
        return await PeerIdentity.open(tmp_path / name, name, "cluster"), credential

    victim, victim_credential = await enroll("victim")
    b, _ = await enroll("b")
    c, _ = await enroll("c")
    statement = await admin.revoke_credential(victim_credential)
    signed_request = victim.sign_request("POST", "/", b"")

    def descriptor(worker, endpoint):
        return PeerDescriptor(
            cluster_id="cluster", worker_id=worker, incarnation="1", endpoint=endpoint
        )

    a_desc = descriptor("a", "https://127.0.0.1:1234")
    b_desc = descriptor("b", "https://127.0.0.2:1234")
    c_desc = descriptor("c", "https://127.0.0.3:1234")
    client = httpx.AsyncClient()

    async def b_inventory():
        return b_desc

    async def c_inventory():
        return c_desc

    directory_b = PeerDirectory(b, b_desc.endpoint, [], b_inventory, tmp_path / "b", client=client)
    directory_c = PeerDirectory(c, c_desc.endpoint, [], c_inventory, tmp_path / "c", client=client)
    response = await directory_b.exchange("a", Exchange(sender=a_desc, revocations=[statement]))
    await directory_c.exchange("b", Exchange(sender=b_desc, revocations=response.revocations))
    for identity in (b, c):
        with pytest.raises(IdentityError, match="revoked"):
            identity.verify_request("POST", "/", b"", signed_request)
    restored = await PeerIdentity.open(tmp_path / "c", "c", "cluster")
    with pytest.raises(IdentityError, match="revoked"):
        restored.verify_request("POST", "/", b"", signed_request)
    forged = statement.model_copy(update={"serial": "123"})
    with pytest.raises(IdentityError, match="signature"):
        await c.merge_revocations([forged])
    expired = statement.model_copy(update={"expires_at": 1})
    with pytest.raises(IdentityError, match="expired"):
        await c.merge_revocations([expired])
    with pytest.raises(IdentityError, match="authority"):
        await b.revoke_credential(victim_credential)
    await client.aclose()


@pytest.mark.asyncio
async def test_offline_authority_renews_expired_node_credential(tmp_path, monkeypatch):
    from datetime import datetime, timedelta

    class Earlier(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) - timedelta(days=91)

    monkeypatch.setattr("fleet_manager.peer.identity.datetime", Earlier)
    await PeerIdentity.open(tmp_path / "authority", "worker-a", "cluster-a")
    monkeypatch.undo()
    with pytest.raises(IdentityError, match="expired"):
        await PeerIdentity.open(tmp_path / "authority", "worker-a", "cluster-a")
    authority = await PeerIdentity.open_authority(tmp_path / "authority", "worker-a", "cluster-a")
    with pytest.raises(IdentityError, match="expired"):
        authority.check_local()
    with pytest.raises(IdentityError, match="expired"):
        authority.sign_request("GET", "/", b"")
    with pytest.raises(IdentityError, match="expired"):
        authority.verify_request("GET", "/", b"", {})
    request = await PeerIdentity.enrollment_request(tmp_path / "authority", "worker-a")
    renewed = await authority.issue_credential(request)
    await PeerIdentity.import_credential(tmp_path / "authority", renewed, authority.fingerprint)
    restored = await PeerIdentity.open(tmp_path / "authority", "worker-a", "cluster-a")
    restored.check_local()
    with pytest.raises(IdentityError, match="does not exist"):
        await PeerIdentity.open_authority(tmp_path / "absent", "worker", "cluster-a")
    (restored.directory / "authority-key.pem").unlink()
    with pytest.raises(IdentityError, match="authority key"):
        await PeerIdentity.open_authority(tmp_path / "authority", "worker-a", "cluster-a")
