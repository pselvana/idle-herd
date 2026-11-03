"""Public Bonjour identity proofs and durable, device-specific LAN trust pins.

Automatic LAN trust authorizes first contact, not key replacement. Each worker
retains its ledger/authority; joining the inference mesh never rewrites history.
"""

from __future__ import annotations

import asyncio
import json
import ssl
import threading

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from pydantic import BaseModel, ConfigDict, Field

from fleet_manager.peer.discovery import MAX_PEERS, validate_endpoint
from fleet_manager.peer.identity import (
    DeviceCredential,
    IdentityError,
    _atomic,
    _b64,
    _cert_name,
    _check_cert,
    _pem,
    _public,
    _unb64,
)


class LanAnnouncement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: int = Field(default=1, ge=1, le=1)
    endpoint: str = Field(max_length=256)
    credential: DeviceCredential
    signature: str = Field(min_length=88, max_length=88)

    def payload(self) -> bytes:
        return (
            b"idle-herd-bonjour-v1\0"
            + json.dumps(
                self.model_dump(exclude={"signature"}), sort_keys=True, separators=(",", ":")
            ).encode()
        )

    @classmethod
    def create(cls, identity, endpoint: str) -> LanAnnouncement:
        identity.check_local()
        record = cls(
            endpoint=validate_endpoint(endpoint),
            credential=DeviceCredential(
                version=1,
                cluster_id=identity.cluster_id,
                worker_id=identity.worker_id,
                ca=_pem(identity.ca).decode(),
                certificate=_pem(identity.cert).decode(),
            ),
            signature="A" * 88,
        )
        return record.model_copy(update={"signature": _b64(identity.key.sign(record.payload()))})

    def verify(self) -> None:
        validate_endpoint(self.endpoint)
        ca, cert = certificates(self.credential)
        if _check_cert(cert, ca, self.credential.cluster_id) != self.credential.worker_id:
            raise IdentityError("Bonjour worker identity mismatch")
        try:
            cert.public_key().verify(_unb64(self.signature), self.payload())
        except Exception as exc:
            raise IdentityError("invalid Bonjour identity proof") from exc


def certificates(record: DeviceCredential):
    try:
        return (
            x509.load_pem_x509_certificate(record.ca.encode()),
            x509.load_pem_x509_certificate(record.certificate.encode()),
        )
    except ValueError as exc:
        raise IdentityError("invalid LAN certificate") from exc


class LanTrust:
    def __init__(self, identity):
        self.identity = identity
        self.path = identity.directory / "lan-peers.json"
        self._records: dict[str, DeviceCredential] = {}
        self._lock = threading.RLock()

    @classmethod
    async def open(cls, identity) -> LanTrust:
        trust = cls(identity)
        await asyncio.to_thread(trust._load)
        return trust

    def _load(self):
        try:
            content = self.path.read_bytes()
        except FileNotFoundError:
            return
        if len(content) > MAX_PEERS * 20000:
            raise IdentityError("LAN trust store too large")
        rows = json.loads(content)
        if not isinstance(rows, list) or len(rows) > MAX_PEERS:
            raise IdentityError("invalid LAN trust store")
        for row in rows:
            record = DeviceCredential.model_validate(row)
            ca, cert = certificates(record)
            if _check_cert(cert, ca, record.cluster_id, allow_expired=True) != record.worker_id:
                raise IdentityError("LAN trust identity mismatch")
            self._check_pin(record)
            self._records[record.worker_id] = record

    def _check_pin(self, record):
        ca, cert = certificates(record)
        public = _public(cert.public_key())
        if record.worker_id == self.identity.worker_id:
            raise IdentityError("Bonjour worker ID conflicts with local identity")
        existing = self._records.get(record.worker_id)
        if existing:
            old_ca, old_cert = certificates(existing)
            if (
                existing.cluster_id != record.cluster_id
                or old_ca.fingerprint(hashes.SHA256()) != ca.fingerprint(hashes.SHA256())
                or _public(old_cert.public_key()) != public
            ):
                raise IdentityError("LAN peer identity changed; existing trust pin retained")
        for worker, other in self._records.items():
            if (
                worker != record.worker_id
                and _public(certificates(other)[1].public_key()) == public
            ):
                raise IdentityError("LAN key/worker ID conflict")
        with self.identity._lock:
            bound = self.identity._bindings.get(record.worker_id)
            if bound is not None and bound != public:
                raise IdentityError("LAN worker ID/key conflict")

    async def remember(self, announcement: LanAnnouncement):
        announcement.verify()
        await asyncio.to_thread(self._remember, announcement.credential)

    def _remember(self, record):
        with self._lock:
            self._check_pin(record)
            if self._records.get(record.worker_id) == record:
                return
            if record.worker_id not in self._records and len(self._records) >= MAX_PEERS:
                raise IdentityError("LAN trust store full")
            records = {**self._records, record.worker_id: record}
            _atomic(self.path, json.dumps([r.model_dump() for r in records.values()]).encode())
            # Persist before granting trust. Existing HTTP clients share these
            # contexts, so new peers work without discarding in-flight streams.
            for context in self.identity._client_contexts:
                context.load_verify_locations(cadata=record.ca)
            self._records = records

    def configure_tls(self, context: ssl.SSLContext):
        with self._lock:
            for record in self._records.values():
                context.load_verify_locations(cadata=record.ca)

    def cluster_for(self, worker_id: str) -> str | None:
        with self._lock:
            record = self._records.get(worker_id)
            return record.cluster_id if record is not None else None

    def verify_certificate(self, cert: x509.Certificate) -> str:
        with self._lock:
            worker = _cert_name(cert)
            record = self._records.get(worker)
            if record is None:
                raise IdentityError("unknown LAN peer")
            ca, pinned = certificates(record)
            _check_cert(cert, ca, record.cluster_id)
            if _public(cert.public_key()) != _public(pinned.public_key()):
                raise IdentityError("LAN peer key conflicts with trust pin")
            return worker
