# Security boundary: Phases 1–2 and target design

The table below is the target threat model. Phase 1 `idle-herd` binds ingress to
127.0.0.1, rejects non-loopback clients, unexpected Host and browser Origin/cross-site
requests, and validates worker-issued one-use reservation capabilities. Its state
directory is restricted to the current user (POSIX mode 0700; Windows requires
appropriate per-user ACLs). Local inference and initial reservation issuance are
available to local processes; **scoped caller authentication is not implemented**.
It exposes no privileged owner controls or transfer endpoints. Phase 2 adds a
separate TLS listener with signed, replay-fenced RPC, CA enrollment and eventual
authority-signed revocation. It exposes only exchange/acquire/execute/release;
local-only routes remain inaccessible on that listener. See
[Phase 2 security and operating limits](phase-2.md).
See [Phase 1](phase-1.md); Phase 6's authenticated owner API remains required.
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
