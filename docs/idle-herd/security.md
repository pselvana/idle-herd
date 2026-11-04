# Security boundary: Phases 1–10 and target design

Native daemons now default to [automatic Bonjour discovery and LAN trust](bonjour.md).
The LAN authorizes first contact; signed public announcements establish persistent
device-specific pins, followed by verified TLS and signed/replay-fenced RPC.
This intentionally replaces mandatory manual enrollment for native LAN mesh
formation. Set `IDLE_HERD_LAN_AUTO_TRUST=false` to retain enrollment-only authority
boundaries. Model sharing and loopback owner credentials keep their separate scopes.
Existing identity domains/ledgers and optional admin membership are not merged.

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
Source sharing now defaults to all authenticated peers for installed readable
Ollama GGUF/MLX models, complete multimodal/multi-file packages, configured export
directories and new registered/imported artifacts, as requested by the owner.
This includes privately installed/fine-tuned models; the source
policy no longer requires a separate per-model opt-in. Existing explicit denials
and allowlists persist; per-chunk revocation still applies. Disable automatic
backend discovery with `IDLE_HERD_MODEL_AUTO_SHARE=false`, and use per-artifact
permissions to restrict registered/imported offers. Native automatic LAN trust
therefore also grants access to default-shared offers. Destination copy permission
and license acceptance remain explicit. Signed
chunks and manifests bind content/metadata; license/provenance claims are authenticated
to a peer, not certified by the publisher. Protected staging, quotas, source/destination
throttles and destination-local install locks constrain transfers. Package names,
roles, total size, per-file digests and import metadata are signed/content-bound.
Offsets address a fixed concatenation; the destination does not extract archives
or install backend-private directories. Traversal, drive/device names and
case-folding collisions are rejected before download. Blob and safetensors-index
references are bounded/validated, and all components are hashed before backend
uploads. Old peers negotiate only their original offer schema. Owner consent,
revocation checks per chunk, no-follow source opens on POSIX and final digest checks
precede publication. Windows reparse-point handling remains best effort.
See [Phase 7 security and limits](phase-7.md).
Phase 8 adds a separate admin-only Raft domain. Dedicated-CA mTLS protects voter
RPC; possession of a voter-CA-issued certificate authorizes protocol participation,
so ordinary inference-peer certificates must never be issued by that CA. A separate
private bearer credential and strict loopback Host/Origin checks protect admin
HTTP. Only labels/notes enter the FSM; metadata never updates worker policy, trust,
leases or jobs. Quorum is unknown when unavailable or observed from a follower;
only verified leaders accept writes. No automatic voter rebootstrap or discovery-
driven membership change exists. See [Phase 8 limits](phase-8.md).
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

## Phase 9 operation and diagnostics

The authenticated read-only dashboard keeps its token only in connected-tab memory,
uses hash-restricted inline assets and text-node insertion, and aborts/discards old
connection results. Browser owner writes remain rejected. Generic health alerts
require a persisted daemon opt-in plus tab/browser permission; no prompts, model
names or job IDs appear in notification text. `hide_job_counts` applies to the
public macOS menu-bar label, not authorized private job history.

Signed optional `/peer/v1/status` reveals only bounded aggregate platform/backend,
model-name, sensor/activity, policy and slot/job/waiter observations to enrolled
peers. No owner controls or job payloads are exposed. Cached responses bind to
worker/incarnation and expire by both receipt and source observation age. Failed
probes/old implementations produce unknown values; the independent poller never
authorizes inference. Status counts are advisory under churn and partitions.

Generated user-service settings/Windows launchers contain local configuration and
must be private. Windows ACLs and localized manager output need native validation;
unknown state is not guessed. Offline backup/restore must preserve a single worker
authority and cannot resurrect a ledger while old backend execution may survive.
[Phase 9 operation](phase-9.md) documents rolling upgrade, backup and explicit
acceptance limits. No native service or release was installed/published during this
implementation. Upstream MIT attribution remains intact.


## Phase 10 container boundary

See [deployment and limitations](phase-10.md). Container :8080 forwards only exact
public inference/capacity/job methods and paths, rejects browser-origin/cross-site
requests before removing hop headers, and never exposes owner controls/dashboard,
even with a valid owner token. Host publication is loopback; its Docker bridge
must also contain only trusted workloads because inference/results are
unauthenticated there. Backend relay and native owner ingress are bound to container
loopback and unpublished. A container sharing its network namespace must be trusted.
Peer TLS ports alone may be published on selected private interfaces after enrollment.
Do not mount the Docker socket, session buses or input devices. Dedicated UID 10001,
private persistent volumes, read-only rootfs, dropped capabilities and protected
credential-file mounts retain the separate peer/owner/voter trust domains. Volume
backups contain prompts, results and private keys. Container/VM utilization does
not attest host human inactivity, GPU fit, AC or thermal health.
