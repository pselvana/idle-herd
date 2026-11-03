"""Device-specific peer credentials and authenticated request envelopes.

Enrollment artifacts contain public material only.  The authority's private key
must remain on the administrator's machine; an enrolled worker cannot issue peers.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import secrets
import ssl
import tempfile
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.x509.oid import NameOID, ObjectIdentifier
from pydantic import BaseModel, ConfigDict, Field

_CLUSTER = ObjectIdentifier("1.3.6.1.4.1.57264.1.1")
_WINDOW = 120
_MAX_REPLAYS = 10000
MAX_REVOCATIONS = 128


class EnrollmentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    version: int = Field(ge=1, le=1)
    worker_id: str = Field(min_length=1, max_length=64)
    public_key: str = Field(min_length=44, max_length=44)
    proof: str = Field(min_length=88, max_length=88)


class DeviceCredential(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    version: int = Field(ge=1, le=1)
    cluster_id: str = Field(min_length=1, max_length=64)
    worker_id: str = Field(min_length=1, max_length=64)
    ca: str = Field(min_length=1, max_length=8192)
    certificate: str = Field(min_length=1, max_length=8192)


class RevocationStatement(BaseModel):
    """A finite-lived, authority-signed certificate revocation."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    version: int = Field(ge=1, le=1)
    cluster_id: str = Field(min_length=1, max_length=64)
    worker_id: str = Field(min_length=1, max_length=64)
    serial: str = Field(min_length=1, max_length=64, pattern=r"^[0-9]+$")
    expires_at: int = Field(gt=0)
    signature: str = Field(min_length=1, max_length=128)


def _revocation_payload(statement: RevocationStatement) -> bytes:
    return json.dumps(
        [
            "idle-herd-revocation-v1",
            statement.cluster_id,
            statement.worker_id,
            statement.serial,
            statement.expires_at,
        ],
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()


class IdentityError(ValueError):
    """A credential or authenticated message is invalid."""


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _unb64(value: str) -> bytes:
    try:
        return base64.b64decode(value, validate=True)
    except Exception as exc:
        raise IdentityError("invalid base64") from exc


def _public(key: Ed25519PublicKey) -> bytes:
    return key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)


def _private(key: Ed25519PrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


def _pem(cert: x509.Certificate) -> bytes:
    return cert.public_bytes(serialization.Encoding.PEM)


def _atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".identity-")
    try:
        os.fchmod(fd, 0o600) if hasattr(os, "fchmod") else None
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def _key(data: bytes) -> Ed25519PrivateKey:
    key = serialization.load_pem_private_key(data, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise IdentityError("unsupported private key")
    return key


def _name(value: str) -> x509.Name:
    if not value or len(value) > 64 or any(ord(c) < 33 or ord(c) > 126 for c in value):
        raise IdentityError("invalid identity")
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, value)])


def _cert_name(cert: x509.Certificate) -> str:
    values = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    if len(values) != 1:
        raise IdentityError("invalid certificate subject")
    return values[0].value


def _check_cert(
    cert: x509.Certificate,
    ca: x509.Certificate,
    cluster_id: str,
    *,
    allow_expired: bool = False,
) -> str:
    try:
        now = datetime.now(UTC)
        if not (
            cert.not_valid_before_utc <= now
            and (allow_expired or now <= cert.not_valid_after_utc)
            and ca.not_valid_before_utc <= now <= ca.not_valid_after_utc
        ):
            raise IdentityError("expired or not yet valid certificate")
        if cert.issuer != ca.subject:
            raise IdentityError("wrong issuer")
        ca.public_key().verify(cert.signature, cert.tbs_certificate_bytes)
        if not ca.extensions.get_extension_for_class(x509.BasicConstraints).value.ca:
            raise IdentityError("invalid authority")
        if cert.extensions.get_extension_for_class(x509.BasicConstraints).value.ca:
            raise IdentityError("authority cannot authenticate as peer")
        if cert.extensions.get_extension_for_oid(_CLUSTER).value.value.decode() != cluster_id:
            raise IdentityError("wrong cluster")
        if not isinstance(cert.public_key(), Ed25519PublicKey):
            raise IdentityError("unsupported certificate key")
        return _cert_name(cert)
    except IdentityError:
        raise
    except Exception as exc:
        raise IdentityError("invalid certificate") from exc


def _issue(
    ca: x509.Certificate,
    key: Ed25519PrivateKey,
    public: Ed25519PublicKey,
    worker_id: str,
    cluster_id: str,
    days: int,
) -> x509.Certificate:
    if not 1 <= days <= 90:
        raise IdentityError("certificate lifetime must be between 1 and 90 days")
    now = datetime.now(UTC)
    return (
        x509.CertificateBuilder()
        .subject_name(_name(worker_id))
        .issuer_name(ca.subject)
        .public_key(public)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=2))
        .not_valid_after(min(now + timedelta(days=days), ca.not_valid_after_utc))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(public), critical=False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(key.public_key()), critical=False
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=None,
                decipher_only=None,
            ),
            critical=True,
        )
        .add_extension(x509.UnrecognizedExtension(_CLUSTER, cluster_id.encode()), critical=False)
        .sign(key, algorithm=None)
    )


class PeerIdentity:
    def __init__(self, directory: Path, worker_id: str, cluster_id: str):
        self.directory = directory
        self.worker_id = worker_id
        self.cluster_id = cluster_id
        self.key_path = directory / "node-key.pem"
        self.cert_path = directory / "node-cert.pem"
        self.ca_path = directory / "ca-cert.pem"
        self._replays: OrderedDict[str, int] = OrderedDict()
        self._lock = threading.Lock()
        self._bindings: dict[str, bytes] = {}
        self.lan_trust = None
        self._client_contexts: list[ssl.SSLContext] = []

    @classmethod
    async def open(cls, data_dir: str | Path, worker_id: str, cluster_id: str) -> PeerIdentity:
        obj = cls(Path(data_dir).expanduser() / "peer-identity", worker_id, cluster_id)
        await asyncio.to_thread(obj._open)
        return obj

    @classmethod
    async def open_authority(
        cls, data_dir: str | Path, worker_id: str, cluster_id: str
    ) -> PeerIdentity:
        """Open an existing authority offline, allowing only node-cert expiry.

        Peer signing and verification continue to enforce certificate validity;
        this path exists solely to issue a replacement device credential.
        """
        obj = cls(Path(data_dir).expanduser() / "peer-identity", worker_id, cluster_id)
        await asyncio.to_thread(obj._open, True)
        return obj

    def _open(self, authority_maintenance: bool = False) -> None:
        _name(self.worker_id)
        _name(self.cluster_id)
        paths = [self.key_path, self.cert_path, self.ca_path]
        if not any(p.exists() for p in paths):
            if authority_maintenance:
                raise IdentityError("authority identity does not exist")
            key = Ed25519PrivateKey.generate()
            authority = Ed25519PrivateKey.generate()
            now = datetime.now(UTC)
            name = _name("idle-herd-" + hashlib.sha256(self.cluster_id.encode()).hexdigest()[:32])
            ca = (
                x509.CertificateBuilder()
                .subject_name(name)
                .issuer_name(name)
                .public_key(authority.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now - timedelta(minutes=2))
                .not_valid_after(now + timedelta(days=3650))
                .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
                .add_extension(
                    x509.SubjectKeyIdentifier.from_public_key(authority.public_key()),
                    critical=False,
                )
                .add_extension(
                    x509.AuthorityKeyIdentifier.from_issuer_public_key(authority.public_key()),
                    critical=False,
                )
                .add_extension(
                    x509.KeyUsage(
                        digital_signature=True,
                        content_commitment=False,
                        key_encipherment=False,
                        data_encipherment=False,
                        key_agreement=False,
                        key_cert_sign=True,
                        crl_sign=True,
                        encipher_only=None,
                        decipher_only=None,
                    ),
                    critical=True,
                )
                .sign(authority, algorithm=None)
            )
            cert = _issue(ca, authority, key.public_key(), self.worker_id, self.cluster_id, 90)
            _atomic(self.directory / "authority-key.pem", _private(authority))
            _atomic(self.key_path, _private(key))
            _atomic(self.ca_path, _pem(ca))
            _atomic(self.cert_path, _pem(cert))
        if not all(p.exists() for p in paths):
            raise IdentityError("incomplete peer identity; enrollment must be completed offline")
        try:
            self.key = _key(self.key_path.read_bytes())
            self.ca = x509.load_pem_x509_certificate(self.ca_path.read_bytes())
            self.cert = x509.load_pem_x509_certificate(self.cert_path.read_bytes())
        except Exception as exc:
            raise IdentityError("invalid local peer credentials") from exc
        if (
            _check_cert(self.cert, self.ca, self.cluster_id, allow_expired=authority_maintenance)
            != self.worker_id
        ):
            raise IdentityError("worker identity mismatch")
        if _public(self.key.public_key()) != _public(self.cert.public_key()):
            raise IdentityError("certificate does not match private key")
        if authority_maintenance:
            try:
                authority = _key((self.directory / "authority-key.pem").read_bytes())
                if _public(authority.public_key()) != _public(self.ca.public_key()):
                    raise IdentityError("authority key does not match trust anchor")
            except IdentityError:
                raise
            except Exception as exc:
                raise IdentityError("missing or invalid authority key") from exc
        self._bindings[self.worker_id] = _public(self.key.public_key())
        revocations = self.directory / "revoked.json"
        try:
            revoked = json.loads(revocations.read_text()) if revocations.exists() else []
            if not isinstance(revoked, list) or any(
                not isinstance(serial, str) or not serial.isascii() or not serial.isdecimal()
                for serial in revoked
            ):
                raise ValueError("expected a list of decimal certificate serial numbers")
            self.revoked = set(revoked)
        except (OSError, ValueError) as exc:
            raise IdentityError("invalid local revocation list") from exc
        self._statements: dict[str, RevocationStatement] = {}
        statements_path = self.directory / "revocation-statements.json"
        try:
            rows = json.loads(statements_path.read_text()) if statements_path.exists() else []
            if not isinstance(rows, list) or len(rows) > MAX_REVOCATIONS:
                raise ValueError("invalid statement list")
            for row in rows:
                statement = RevocationStatement.model_validate(row)
                self._verify_revocation(statement, allow_expired=True)
                if statement.expires_at > int(time.time()):
                    self._statements[statement.serial] = statement
                    self.revoked.add(statement.serial)
        except (OSError, ValueError) as exc:
            raise IdentityError("invalid stored revocation statements") from exc
        if str(self.cert.serial_number) in self.revoked:
            raise IdentityError("local certificate revoked")

    @property
    def fingerprint(self) -> str:
        return self.ca.fingerprint(hashes.SHA256()).hex()

    def _verify_revocation(
        self, statement: RevocationStatement, *, allow_expired: bool = False
    ) -> None:
        try:
            if statement.cluster_id != self.cluster_id:
                raise IdentityError("revocation belongs to another cluster")
            _name(statement.worker_id)
            if not allow_expired and statement.expires_at <= int(time.time()):
                raise IdentityError("expired revocation")
            self.ca.public_key().verify(_unb64(statement.signature), _revocation_payload(statement))
        except IdentityError:
            raise
        except Exception as exc:
            raise IdentityError("invalid revocation signature") from exc

    def revocations(self) -> list[RevocationStatement]:
        now = int(time.time())
        with self._lock:
            return [s for s in self._statements.values() if s.expires_at > now]

    async def merge_revocations(self, statements: list[RevocationStatement]) -> None:
        await asyncio.to_thread(self._merge_revocations, statements)

    def _merge_revocations(self, statements: list[RevocationStatement]) -> None:
        if len(statements) > MAX_REVOCATIONS:
            raise IdentityError("too many revocations")
        checked = []
        for statement in statements:
            try:
                statement = RevocationStatement.model_validate(statement)
            except ValueError as exc:
                raise IdentityError("invalid revocation statement") from exc
            self._verify_revocation(statement)
            checked.append(statement)
        with self._lock:
            now = int(time.time())
            merged = {
                serial: stmt for serial, stmt in self._statements.items() if stmt.expires_at > now
            }
            for statement in checked:
                existing = merged.get(statement.serial)
                if existing is not None and existing != statement:
                    raise IdentityError("conflicting certificate revocation")
                merged[statement.serial] = statement
            if len(merged) > MAX_REVOCATIONS:
                raise IdentityError("revocation store full")
            if merged != self._statements:
                data = json.dumps(
                    [s.model_dump() for s in merged.values()], sort_keys=True
                ).encode()
                _atomic(self.directory / "revocation-statements.json", data)
                self._statements = merged
                self.revoked.update(merged)

    async def revoke_credential(self, credential: DeviceCredential) -> RevocationStatement:
        return await asyncio.to_thread(self._revoke_credential, credential)

    def _revoke_credential(self, credential: DeviceCredential) -> RevocationStatement:
        try:
            if credential.version != 1 or credential.cluster_id != self.cluster_id:
                raise IdentityError("invalid credential cluster or version")
            ca = x509.load_pem_x509_certificate(_unb64(credential.ca))
            if ca.fingerprint(hashes.SHA256()) != self.ca.fingerprint(hashes.SHA256()):
                raise IdentityError("credential has another trust anchor")
            cert = x509.load_pem_x509_certificate(_unb64(credential.certificate))
            worker = _check_cert(cert, self.ca, self.cluster_id)
            if worker != credential.worker_id:
                raise IdentityError("worker identity mismatch")
            authority = _key((self.directory / "authority-key.pem").read_bytes())
            if _public(authority.public_key()) != _public(self.ca.public_key()):
                raise IdentityError("authority key does not match trust anchor")
            unsigned = RevocationStatement(
                version=1,
                cluster_id=self.cluster_id,
                worker_id=worker,
                serial=str(cert.serial_number),
                expires_at=int(cert.not_valid_after_utc.timestamp()),
                signature="pending",
            )
            statement = unsigned.model_copy(
                update={"signature": _b64(authority.sign(_revocation_payload(unsigned)))}
            )
            self._merge_revocations([statement])
            return statement
        except IdentityError:
            raise
        except Exception as exc:
            raise IdentityError("invalid credential or missing authority key") from exc

    @classmethod
    async def enrollment_request(cls, data_dir: str | Path, worker_id: str) -> EnrollmentRequest:
        return await asyncio.to_thread(
            cls._enrollment_request, Path(data_dir).expanduser(), worker_id
        )

    @staticmethod
    def _enrollment_request(data_dir: Path, worker_id: str) -> EnrollmentRequest:
        _name(worker_id)
        directory = data_dir / "peer-identity"
        path = directory / "node-key.pem"
        if path.exists():
            key = _key(path.read_bytes())
        else:
            key = Ed25519PrivateKey.generate()
            _atomic(path, _private(key))
        # Proof of possession prevents an intermediary changing the request key.
        public = _public(key.public_key())
        payload = b"idle-herd-enrollment-v1\0" + worker_id.encode() + b"\0" + public
        return EnrollmentRequest(
            version=1, worker_id=worker_id, public_key=_b64(public), proof=_b64(key.sign(payload))
        )

    async def issue_credential(
        self, request: EnrollmentRequest, days: int = 30
    ) -> DeviceCredential:
        return await asyncio.to_thread(self._issue_credential, request, days)

    def _issue_credential(self, request: EnrollmentRequest, days: int) -> DeviceCredential:
        try:
            if request.version != 1:
                raise IdentityError("unsupported enrollment version")
            worker = request.worker_id
            _name(worker)
            public_bytes = _unb64(request.public_key)
            public = Ed25519PublicKey.from_public_bytes(public_bytes)
            public.verify(
                _unb64(request.proof),
                b"idle-herd-enrollment-v1\0" + worker.encode() + b"\0" + public_bytes,
            )
            authority = _key((self.directory / "authority-key.pem").read_bytes())
            if _public(authority.public_key()) != _public(self.ca.public_key()):
                raise IdentityError("authority key does not match trust anchor")
            # Keep a durable binding across restarts; disallow reassigning a worker ID.
            bindings_path = self.directory / "issued.json"
            with self._lock:
                bindings = json.loads(bindings_path.read_text()) if bindings_path.exists() else {}
                bindings.setdefault(self.worker_id, _b64(_public(self.key.public_key())))
                if worker in bindings and bindings[worker] != request.public_key:
                    raise IdentityError("worker ID already bound to another key")
                if any(w != worker and k == request.public_key for w, k in bindings.items()):
                    raise IdentityError("key already bound to another worker ID")
                bindings[worker] = request.public_key
                _atomic(bindings_path, json.dumps(bindings, sort_keys=True).encode())
                cert = _issue(self.ca, authority, public, worker, self.cluster_id, days)
            return DeviceCredential(
                version=1,
                cluster_id=self.cluster_id,
                worker_id=worker,
                ca=_b64(_pem(self.ca)),
                certificate=_b64(_pem(cert)),
            )
        except IdentityError:
            raise
        except Exception as exc:
            raise IdentityError("invalid enrollment request or missing authority key") from exc

    @classmethod
    async def import_credential(
        cls, data_dir: str | Path, credential: DeviceCredential, expected_fingerprint: str
    ) -> None:
        await asyncio.to_thread(
            cls._import_credential, Path(data_dir).expanduser(), credential, expected_fingerprint
        )

    @staticmethod
    def _import_credential(
        data_dir: Path, credential: DeviceCredential, expected_fingerprint: str
    ) -> None:
        directory = data_dir / "peer-identity"
        try:
            if credential.version != 1:
                raise IdentityError("unsupported enrollment version")
            ca = x509.load_pem_x509_certificate(_unb64(credential.ca))
            cert = x509.load_pem_x509_certificate(_unb64(credential.certificate))
            if not secrets.compare_digest(
                ca.fingerprint(hashes.SHA256()).hex(), expected_fingerprint.lower().replace(":", "")
            ):
                raise IdentityError("trust anchor fingerprint mismatch")
            worker = _check_cert(cert, ca, credential.cluster_id)
            if worker != credential.worker_id:
                raise IdentityError("worker identity mismatch")
            key = _key((directory / "node-key.pem").read_bytes())
            if _public(key.public_key()) != _public(cert.public_key()):
                raise IdentityError("credential does not match local private key")
            existing_ca = directory / "ca-cert.pem"
            if existing_ca.exists() and existing_ca.read_bytes() != _pem(ca):
                raise IdentityError(
                    "existing trust anchor conflicts; remove identity explicitly offline"
                )
            _atomic(existing_ca, _pem(ca))
            _atomic(directory / "node-cert.pem", _pem(cert))
        except IdentityError:
            raise
        except Exception as exc:
            raise IdentityError("invalid credential") from exc

    def server_ssl_context(self) -> ssl.SSLContext:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(str(self.cert_path), str(self.key_path))
        return context

    def client_ssl_context(self) -> ssl.SSLContext:
        context = ssl.create_default_context(cafile=str(self.ca_path))
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.check_hostname = False  # Signed response authenticates the worker, including seeds.
        if self.lan_trust is not None:
            self.lan_trust.configure_tls(context)
        self._client_contexts.append(context)
        return context

    def accepts_peer(self, worker_id: str, cluster_id: str) -> bool:
        if self.lan_trust is not None:
            pinned = self.lan_trust.cluster_for(worker_id)
            if pinned is not None:
                return cluster_id == pinned
        return cluster_id == self.cluster_id

    def verify_peer_certificate(self, cert: x509.Certificate) -> str:
        if self.lan_trust is not None and self.lan_trust.cluster_for(_cert_name(cert)) is not None:
            return self.lan_trust.verify_certificate(cert)
        return _check_cert(cert, self.ca, self.cluster_id)

    def check_local(self) -> None:
        """Expired/revoked local credentials withdraw peer participation only."""
        if _check_cert(self.cert, self.ca, self.cluster_id) != self.worker_id:
            raise IdentityError("worker identity mismatch")
        if str(self.cert.serial_number) in self.revoked:
            raise IdentityError("local certificate revoked")

    def _sign(self, kind: str, first: str, second: str, body: bytes) -> dict[str, str]:
        # The process can outlive its short-lived credential.
        self.check_local()
        timestamp = str(int(time.time()))
        nonce = secrets.token_hex(16)
        payload = self._payload(kind, first, second, body, timestamp, nonce)
        return {
            "x-idle-cert": _b64(_pem(self.cert)),
            "x-idle-time": timestamp,
            "x-idle-nonce": nonce,
            "x-idle-signature": _b64(self.key.sign(payload)),
        }

    @staticmethod
    def _payload(
        kind: str, first: str, second: str, body: bytes, timestamp: str, nonce: str
    ) -> bytes:
        return json.dumps(
            [
                "idle-herd-rpc-v1",
                kind,
                first,
                second,
                timestamp,
                nonce,
                hashlib.sha256(body).hexdigest(),
            ],
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()

    def _verify(
        self, kind: str, first: str, second: str, body: bytes, headers: Mapping[str, str]
    ) -> str:
        try:
            self.check_local()
            headers = {k.lower(): v for k, v in headers.items()}
            timestamp, nonce = headers["x-idle-time"], headers["x-idle-nonce"]
            if len(nonce) != 32 or any(c not in "0123456789abcdef" for c in nonce):
                raise IdentityError("invalid nonce")
            now = int(time.time())
            if abs(now - int(timestamp)) > _WINDOW:
                raise IdentityError("stale message")
            encoded = headers["x-idle-cert"]
            if len(encoded) > 8192:
                raise IdentityError("certificate too large")
            cert = x509.load_pem_x509_certificate(_unb64(encoded))
            worker = self.verify_peer_certificate(cert)
            if str(cert.serial_number) in self.revoked:
                raise IdentityError("certificate revoked")
            public = cert.public_key()
            public.verify(
                _unb64(headers["x-idle-signature"]),
                self._payload(kind, first, second, body, timestamp, nonce),
            )
            raw = _public(public)
            with self._lock:
                if worker in self._bindings and self._bindings[worker] != raw:
                    raise IdentityError("worker ID/key conflict")
                if any(w != worker and k == raw for w, k in self._bindings.items()):
                    raise IdentityError("key/worker ID conflict")
                replay = worker + ":" + nonce
                for old, expiry in list(self._replays.items()):
                    if expiry < now:
                        del self._replays[old]
                if replay in self._replays:
                    raise IdentityError("replayed message")
                if len(self._replays) >= _MAX_REPLAYS:
                    raise IdentityError("replay cache full")
                self._replays[replay] = int(timestamp) + _WINDOW
                self._bindings[worker] = raw
            return worker
        except IdentityError:
            raise
        except Exception as exc:
            raise IdentityError("invalid authenticated message") from exc

    def sign_request(self, method: str, path: str, body: bytes) -> dict[str, str]:
        return self._sign("request", method.upper(), path, body)

    def verify_request(
        self, method: str, path: str, body: bytes, headers: Mapping[str, str]
    ) -> str:
        return self._verify("request", method.upper(), path, body, headers)

    def sign_response(self, request_nonce: str, status: int, body: bytes) -> dict[str, str]:
        return self._sign("response", request_nonce, str(status), body)

    def verify_response(
        self,
        request_nonce: str,
        status: int,
        body: bytes,
        headers: Mapping[str, str],
        expected_worker_id: str | None = None,
    ) -> str:
        worker = self._verify("response", request_nonce, str(status), body, headers)
        if expected_worker_id is not None and worker != expected_worker_id:
            raise IdentityError("unexpected response worker")
        return worker

    def verify_stream_response(
        self,
        request_nonce: str,
        response: httpx.Response,
        expected_worker_id: str | None = None,
    ) -> str:
        """Authenticate streaming headers and bind them to the TLS connection.

        The TLS record layer authenticates the subsequent stream.  Requiring the
        response signer and TLS endpoint to use the same certificate prevents a
        different enrolled peer from relaying headers and replacing stream bytes.
        Call before consuming or forwarding any part of the response body.
        """
        try:
            stream = response.extensions["network_stream"]
            tls = stream.get_extra_info("ssl_object")
            if tls is None:
                raise IdentityError("stream response has no TLS connection")
            peer_der = tls.getpeercert(binary_form=True)
            if not peer_der:
                raise IdentityError("stream response has no TLS peer certificate")
            encoded = response.headers["x-idle-cert"]
            if len(encoded) > 8192:
                raise IdentityError("certificate too large")
            signer = x509.load_pem_x509_certificate(_unb64(encoded))
            signer_der = signer.public_bytes(serialization.Encoding.DER)
            if not secrets.compare_digest(peer_der, signer_der):
                raise IdentityError("response signer does not match TLS peer")
        except IdentityError:
            raise
        except Exception as exc:
            raise IdentityError("invalid TLS stream identity") from exc
        return self.verify_response(
            request_nonce, response.status_code, b"", response.headers, expected_worker_id
        )
