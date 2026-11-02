# Security boundary: Phases 1–7 and target design

The table below is the target threat model. Phase 1 `idle-herd` binds ingress to
127.0.0.1, rejects non-loopback clients, unexpected Host and browser Origin/cross-site
requests, and validates worker-issued one-use reservation capabilities. Its state
directory is restricted to the current user (POSIX mode 0700; Windows requires
appropriate per-user ACLs). Local inference and initial reservation issuance are
available to local processes; Phase 6 separately authenticates privileged owner
controls. Model transfer is added separately in Phase 7. Phase 2 adds a
separate TLS listener with signed, replay-fenced RPC, CA enrollment and eventual
authority-signed revocation. Phase 4 adds capacity and inspect RPC to
exchange/acquire/execute/release; all require independent peer authentication;
local-only routes remain inaccessible on that listener. See
[Phase 2 security and operating limits](phase-2.md).
Phase 3 enforces the worker's background policy on every peer reservation and
rechecks persisted policy before execution; peer-supplied workload labels cannot
override it. The optional host activity helper is a trusted local executable,
configured by absolute path and launched without a shell. Its versioned bounded
JSON must attest host-wide activity, not only its own session. Permissions must
protect the executable and its parent directories against untrusted replacement.
Unknown/stale activity cannot grant idle-only work. Read-only eligibility exposes
aggregate observations, never input events or session identifiers. See
[Phase 3 limitations](phase-3.md).
Phase 4 signs authenticated negative admission replies as well as grants: only
a verified decline permits the origin to try another worker. Unknown replies
retain a durable target claim. `origin.sqlite` holds reservation capabilities;
protect it like the worker ledger. Waiter count is bounded with
`IDLE_HERD_MAX_WAITERS` (default 256); historical claims currently have no expiry.
Peer capacity is advisory even after signature and freshness checks. See
[Phase 4 limitations](phase-4.md).
Phase 5 adds signed job store/fetch RPCs: only the origin publishes snapshots;
only the origin and acknowledged holders can fetch them. Commit precedes each
digest-bound acknowledgment, and accepted replicas are required before replicated
202 responses. Private SQLite files contain full prompts/results; protect storage
and backups. TLS protects transport; application-level encryption at rest and
same-user job isolation are not provided. Job validation errors omit submitted
inputs, and state summaries omit payloads, results and tokens. See [job limits and
recovery](phase-5.md).
Phase 6 provisions distinct private read/control bearer tokens and rejects insecure
credential files and symlinks where supported. Mode changes and lease consumption
share the worker transaction. Loopback/Host/Origin checks apply independently of
token authorization; no control endpoint is mounted on the peer RPC listener.
The browser dashboard permits same-origin authenticated reads only and retains
its token in page memory. Browser writes remain forbidden. The Swift client
imports its control token into Keychain, refuses redirects and uses an ephemeral
HTTP session. A process running as the same OS user can still read private files;
this is not protection from malicious same-user code. Windows ACL enforcement,
physical Keychain prompts and native GUI lifecycle remain unverified. Rotation
currently requires stopping the daemon, replacing its private credentials and
reimporting the UI credential. See [Phase 6 security and limits](phase-6.md).
Phase 7 source sharing is deny-by-default per destination and artifact. Signed
chunks and manifests bind content/metadata; license/provenance claims are authenticated
to a peer, not certified by the publisher. Protected staging, quotas, source/destination
throttles and destination-local install locks constrain transfers. Owner consent,
revocation checks per chunk, no-follow source opens on POSIX and final digest checks
precede publication. Windows reparse-point handling remains best effort.
See [Phase 7 security and limits](phase-7.md).
[The audit](upstream-audit.md) records inherited unauthenticated
routes, wildcard binds and external telemetry. Do not expose that runtime as an
authenticated Idle-Herd peer. Its optional Anthropic API key protects only that
surface, and CORS does not authorize network clients.

| Boundary | Required enforcement | Failure behavior |
|---|---|---|
| Local inference ingress | Loopback by default on each peer; preserve compatibility shapes; local resource policy and admission on every execution | Paused rejects new work; offline still serves localhost |
| Local owner API | Loopback only, platform-protected scoped bearer credential, per-operation authorization, strict Host/Origin validation and CSRF defenses | Missing credential 401, insufficient scope 403; no mutation |
| Peer listener | Separate authenticated transport and peer identity/cluster authorization; no local owner routes or backend URL passthrough | Untrusted discovery cannot enroll, reserve, execute or transfer |
| Worker/backend | Worker validates token identity, incarnation, persistent generation, workload binding and consumption under local serialization | Invalid/replayed/expired tokens denied; uncertain running work holds resources |
| Model transfer | Authorized manifests/digests, license/share policy, format/runtime checks, bounded storage, destination lock and private staging | Reject traversal/symlinks, oversized input and incompatible artifacts; no partial publication |
| Durable jobs/results | Origin/replica ACLs, protected local storage, authenticated encrypted transport and explicit durable acknowledgments | Unknown outcomes remain unknown; no silent durability downgrade |

Assume malicious LAN peers, stale/forged discovery, a browser attacking localhost,
unprivileged local applications, leaked or replayed credentials and untrusted model
files/output. Host administrator compromise is outside the protection offered by a
per-user daemon. A bearer credential must not be accessible to unrelated users;
same-user process isolation depends on the OS credential mechanism. Never place
tokens in URLs or expose them in status, SSE, diagnostics or logs. Do not return raw
prompts/completions or process argv/private paths in the menu by default. The
upstream trace store must be audited separately for content and retention.

Enrollment creates a persistent node keypair and cluster identity, with protected
administrator-signed device-specific time-limited credentials or protected pairing.
Seeds and mDNS only locate candidates. Reject conflicting IDs, keys and trust
anchors; require protocol compatibility before use. Transport certificate/pinning,
credential provisioning/rotation, secure-store access for a background daemon and
UI, replay windows, clock skew, expiry, and request size/rate limits must be fixed
before their implementation phases. Phase 2 fixes peer enrollment, TLS/signature
checks, replay fences and bounded RPC in its runtime documentation; OS secure-store
integration, owner scopes and model transfers remain future work. The draft
OpenAPI does not define those later implementation details.

Revocation is eventually propagated: a partition cannot guarantee immediate global
revocation or globally single-use enrollment invites. Short-lived credentials and
local revocation caches reduce exposure; stale optional administrative consensus
cannot override the owner's local offline policy or issue worker leases. Quorum
loss changes optional strict admin writes only.

Require adversarial tests for forged/replayed leases, credential scope separation,
DNS rebinding/Host and Origin attacks, missing credentials, stale permission data,
resource exhaustion, symlink/path traversal and UI output injection. The declarative
[failure scenarios](../../tests/contracts/idle_herd.feature) are future test
requirements, not completed security verification. No live fleet was deployed or
credentials generated during Phase 0.
