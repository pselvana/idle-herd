# Idle-Herd API contract — roadmap and Phases 1–7 subset

[openapi.yaml](openapi.yaml) remains the OpenAPI 3.1 future cluster/control
contract. The running `/openapi.json` describes the implemented subset. See
[Phase 1](phase-1.md) for local ledger and recovery, [Phase 2](phase-2.md) for
authenticated peer transport, [Phase 3](phase-3.md) for idle policy, and
[Phase 4](phase-4.md) for advisory cluster capacity, bounded acquisition,
inspection, release and their actual fields and limitations. [Phase 5](phase-5.md)
defines durable jobs and [Phase 6](phase-6.md) defines the implemented local
control scopes, modes, preferences, events and companion. Other draft fields
below are not necessarily implemented.
[Phase 7](phase-7.md) defines the implemented model inventory, permission, transfer
and progress endpoints, source-peer and local-owner authorization, and
single-file GGUF/Ollama limitations.
Existing inference translators are reused; legacy `herd` retains upstream routes.

## Trust and visibility

All draft operations require authorization by default. Capacity access, local telemetry read, and local control are separate scopes; per-operation OpenAPI security specifies the required credential. Credential issuance and rotation remain to be designed. Local node APIs bind only to loopback, and privileged control is never exposed on the peer RPC listener. Loopback is not authentication: use scoped secrets protected by the platform credential store or equivalent file permissions, verify authorization on each request, and prevent privilege escalation between read and control. Peer RPC uses independently authenticated peer identity and authorization, not the local control credential. Disable cross-origin browser access by default; validate Origin and protect state-changing requests against CSRF and localhost attacks. Never put secrets in URLs, logs, or events. An unauthorized caller gets 401, an authenticated caller without scope gets 403.

`GET /v1/cluster/capacity` is an origin-local, best-effort view of known reachable peers, not a cluster-wide reservation or a promise of availability. Timestamps, per-peer freshness and `scope` qualify the data; stale and unreachable nodes cannot contribute available slots. `busy: null` means the view cannot support an answer. The local peer is included in its own reachable view, so an eligible single-node deployment has one known/reachable worker without discovery. Ordinary inference or acquisition must never return `cluster_no_quorum`. Discovery may miss unknown peers even when the known view is complete. A `quorum` value describes only optional administrative coordination; quorum never gates ordinary inference or capacity acquisition. Unknown sensor values are null or explicitly `unknown`, never fabricated zeroes. Local status reports authoritative running, queued and reserved job counts, uptime, and local slot counts at `observed_at`. It separately reports local and remote eligibility with policy reasons: offline may be locally eligible while remote admission is denied. Idle state includes sensor provenance and sample time; resource readings carry their own sampling time and provenance.

## Admission and failures

`POST /v1/capacity/acquire` defaults to a 60-second bounded wait; 120 seconds is the maximum. `wait_ms: 0` is nonblocking. The MVP admits exactly one slot atomically on one worker; cross-worker multi-slot admission is unsupported. An origin's bounded fair waiters retry on relevant changes and periodic backoff, and stop at the deadline or cancellation. Gossip only ranks candidates. A worker rechecks local policy and resources and durably records its own reservation before returning 201. It issues an opaque short-lived token bound to cluster, worker identity and incarnation, slot and persistent generation, and request identity. Consumption is serialized and single-use; only the worker can acknowledge renewal or release. On restart it reconciles backend state, fences old incarnations and persists monotonically advancing slot generations. Token expiry alone must not allow a still-running backend operation to be overbooked. Inference may use `X-Idle-Herd-Reservation`; token validation and execution are worker-authoritative.

The origin stores idempotency keys and worker claims are independently idempotent. A retry with the same key and same request at the same origin returns the prior result or continues its pending operation without booking another slot; reuse with different parameters is `409 idempotency_conflict`. Key retention and behavior after retention expires must be specified before implementation. This is not cluster-global deduplication. A timeout or lost response is not proof that a worker did nothing; inspect or retry with the same key. Reservation inspection and release contact the issuing worker; `503 worker_unreachable` means state or release is unknown, not active or released. Invalid and expired tokens use `409 reservation_invalid` and `410 reservation_expired`. Nonblocking contention returns `429 no_capacity` with `Retry-After`; no reachable worker returns `503 no_reachable_worker`, inconclusive eligibility `503 no_eligible_worker`, conclusively incompatible model `422 model_not_compatible`, and an elapsed wait `504 capacity_wait_timeout` with available diagnostic counters. Ordinary failures use the versioned error envelope in the schema.

Worker-local slot safety does not imply globally exactly-once jobs. Partitions or uncertain outcomes can produce duplicate logical attempts on different workers. Future durable job recovery must identify attempts, report `unknown_outcome` where appropriate, and provide at-least-once semantics for eligible retries; never automatically fail over side-effecting or non-idempotent work. Local persistence only survives while its storage does; replicated durability requires acknowledged durable copies of both metadata and payload and must not be silently downgraded. Phase 5 implements durable job endpoints; see [the actual job contract](phase-5.md). Model sharing uses the [Phase 7 subset](phase-7.md).

## Local control and events

`auto` applies idle and resource policy. `draining` rejects new remote admissions while allowing local inference and outbound dispatch. `offline` immediately rejects new remote admissions and outbound cluster dispatch but retains localhost inference. `paused` denies all new inference admissions. Existing work drains by default; explicit cancellation is separate and best effort. Mode changes and token consumption share worker-local serialization before propagation. Deny or invalidate unconsumed peer reservations for offline/draining and unconsumed local plus peer reservations for paused. Recheck mode on consumption so previously issued tokens cannot bypass pause; stale peer advertisements cannot override the worker. Temporary non-auto overrides use either a future `expires_at` or a positive `duration_seconds` measured from the local application time, never both. Reject a past or present expiry and reject expiry or duration with `auto`. Persist the resulting absolute deadline; at or after that deadline, including on service restart, transition to `auto` before evaluating new admissions, subject to eligibility policy. Manual offline persists across restart by default. Cancellation requires ownership or authorization; `unknown_outcome` does not mean cancellation or absence of side effects. Preferences use an expected version and are validated and persisted atomically.

Both watch endpoints use `text/event-stream`. Each connection, including reconnection with `Last-Event-ID`, starts with a fresh `snapshot` event. Frames have an opaque `id`, an `event` matching the versioned JSON envelope's `type`, and `data` containing that envelope. Heartbeat comments are not state changes. Replay is best effort; clients must replace local state from the snapshot and treat missing, invalid, or expired cursors as a resynchronization, not assume a complete history. Capacity streams describe only the origin's reachable view; local streams describe local state. Clients should reconnect with backoff and can poll if streaming is unavailable. Stream ordering applies only within a connection; no cross-node ordering or exactly-once delivery is promised.

Job summaries identify whether their origin and execution are local, peer, unassigned (execution only), or unknown without including job contents. Status, jobs, events, errors and diagnostics contain safe summaries only: no prompt/completion content, credentials, reservation tokens, private paths, or raw user activity. Diagnostic provenance and explanations must be redacted before display. UI lifetime is independent of the daemon; a disconnected UI must show unknown/reconnecting rather than stale healthy state.

### Local login service

The running loopback daemon exposes `GET /v1/node/service` to read-scoped clients
and `PUT /v1/node/service` with `{"enabled": true}` or `{"enabled": false}` to
control-scoped clients. Responses contain `version`, `supported`, `platform`,
`installed`, `enabled`, and `running`; the last three may be `null` when unknown.
On macOS, Linux and Windows this configures login startup without stopping
the current daemon. Unsupported platforms return 501 for a change. Manager errors
or unrecognized/localized state remain unknown, not guessed. The installer uses
the daemon's Python interpreter and copies its effective `IDLE_HERD_` settings into
the owner-protected launch agent, including the data directory and peer settings.

### Read-only dashboard

`GET /dashboard` serves the Phase 9 local dashboard. Enter the local read token to
fetch the redacted status snapshot; the page keeps the token only in browser memory.
The page offers no management actions. Versioned events, independent polling, model capacity and diagnostic views share
the menu bar semantics; see [Phase 9](phase-9.md).
Same-origin browser reads require the bearer token; browser control writes and
cross-origin requests remain rejected.

## Phase 8 optional administrative coordination

The implemented local status/dashboard and sampled events include `coordination`
with enabled/reachable state, observed quorum/leader, local write availability and
advisory preferred peer. Cluster capacity/health expose cached quorum independently
of slots. `null` means unknown (including followers or an unreachable service),
or disabled; consult authenticated node status for the distinction. No core path
contacts the optional service.

`GET /v1/node/coordination/metadata` requires the existing read credential;
`PUT` requires the control credential and a bounded `{key,value}` body restricted
to `cluster_label` or `operator_note`. Missing service, non-leader writes and
quorum loss return 503 only for these administrative operations. Read responses
mark `consistent: false` for potentially stale local committed data. The peer RPC
listener has no admin route. [Phase 8](phase-8.md) defines the separate Go API,
bootstrap and dedicated voter credentials; the draft OpenAPI remains a subset.

## Phase 9 diagnostic/status additions

`GET /v1/node/peers` uses the local read scope and returns a list of local/remote
observations. `/v1/node/status` adds `worker_incarnation`, `platform`, `waiters`,
`peer_details`, `worker_leases`, latest-50 `job_history`, `notifications_enabled` and
`hide_job_counts`. Existing fields retain their shapes. `idle` describes peer
background eligibility; local inference retains its own configured policy.
Peer counts/slots are observations, not grants; inaccessible/older peers show null
diagnostics. Job history adds effective durability, acknowledged copy count,
attempt count and duplicate consent. Worker leases add lease ID, hashed request
correlation and incarnation/generation; reservation bearer tokens are excluded.

Node status and peer details include optional `hostname`. Job summaries include
`origin_worker_id`, `origin_host` and `execution_host`; worker leases include
`origin_worker_id` and `origin_host`. Origins come from the authenticated peer
principal, not caller-supplied display text. Host labels use the last authenticated
hostname or the known endpoint IP, with worker identity as the UI fallback.
Quarantined worker leases report `quarantine_recovery: awaiting_backend` with a
Unix-seconds `quarantine_recovery_deadline` while the original backend stream is
being read for completion proof, otherwise `manual_required`. Other states use
null. A deadline is a recovery window, never permission to free uncertain work.

`GET /v1/node/diagnostics?model=NAME` accepts an optional 1–512 character model name
and returns the existing fields plus platform/worker observation and structured
`checks` (`code`, `title`, `state`, `detail`, `remedy`) and optional live local
`model_fit`. States are `ok`, `attention` or `unknown`. It never grants a slot or
probes a remote worker; the existing cluster capacity endpoint remains advisory.

Optional signed `POST /peer/v1/status` accepts only `{}` from an enrolled peer,
rejects offline participation, and returns bounded aggregate platform/backend/model,
sensor/activity/eligibility and slot/job/waiter counts bound to worker/incarnation.
It exposes no owner control or private job records. An independent semaphore-bounded
background poller verifies signatures/identity/incarnation/age and expires cached
results. A 404 from older peers leaves diagnostics unknown without altering
discovery/admission. The discovery descriptor schema is unchanged.

New status clients send `X-Idle-Status-Hostname: 1` to request the optional
`hostname` field in the signed response. The request body remains `{}`. Servers
omit the field for older strict clients; older servers ignore the header. Host
labels are validated and retained only after response signature, identity,
incarnation and freshness checks, and survive peer-directory reloads.

New owner preferences: `quiet_hours_enabled`, start/end minute-of-day (0–1439),
`peer_max_concurrent` (0–128 per authenticated origin), `notifications_enabled` and
`hide_job_counts`. They use existing control scope and expected-version persistence.
See [policy semantics](phase-9-policy.md), including local-time and quota limits.
The standalone draft OpenAPI file remains a subset; runtime `/openapi.json` describes
implemented loopback routes, while the signed peer envelope is documented here.
