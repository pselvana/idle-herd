# Idle-Herd implementation status

## Phase 5 — durable jobs and acknowledged replicas, November 1, 2026

`idle-herd` now serves origin-local persistent jobs and opt-in authenticated peer
copies. See [the actual API, recovery semantics and limits](phase-5.md). Earlier
phase sections remain historical records. Next: **Phase 6**, authenticated owner
controls and the native macOS companion.

- [x] Separate SQLite WAL/FULL journal with complete input/result persistence,
  origin-scoped idempotency, crash-consistent state and distinct attempt IDs.
- [x] POST/status/result/cancel localhost job APIs using existing inference wire
  translators and live worker admission. No central router, leader or quorum.
- [x] Digest-bound signed durable acknowledgments, prepared/accepted replica
  stages, explicit additional-copy counts and caller-authorized downgrade only.
  No 202 or origin execution activation before the accepted-copy threshold.
- [x] Restart recovery distinguishing provably undispatched local admission from
  unknown dispatched execution; bounded retry and delayed replica takeover only
  with explicit retry safety and possible-duplicate consent.
- [x] Reconciliation of independent attempt/result histories, conflict evidence,
  persistent cancellation propagation and no unsafe automatic tool/action replay.
- [x] Bounded inputs, results and logical journal storage; safe summaries and
  validation diagnostics; peer fetch restricted to origin/certificate holders.
- [ ] Real model/hardware, power-loss/storage certification, Linux/Windows native
  service execution and physical sensor acceptance remain unverified.
- [ ] Phases 6–10: owner controls/UI, verified model sharing, optional admin Raft,
  diagnostics/installers and finally Docker/Compose.

### Phase 5 evidence and limitations

Unit/ASGI tests cover concurrent same-key submissions, changed parameters, committed
input/result retrieval after reopen, failed commits and dispatch-boundary writes,
accepted-copy acknowledgment thresholds checked through an independent SQLite
reader, rejected foreign replica reads/writes, completion synchronization, simulated
origin outage and safe takeover, unsafe takeover refusal, conflicting result
histories, retry budget exhaustion and cancellation of retry-safe running work.
A slow copy request cannot block acceptance/execution of an unrelated local job.
Oversized requests and malformed payloads return bounded, redacted errors.

Native processes over real loopback HTTP/TLS use fabricated inference backends.
They kill/restart the daemon with completed jobs and pending admission, preserve
results/idempotency, and verify that unsafe running work remains unknown with the
independent backend still active and the slot quarantined. Configured N=2 and N=3
clusters shrink to one survivor, which accepts and completes a local durable job.
Signed ASGI peer meshes exercise replicated acceptance and takeover; a real-model
or native-process replicated takeover was not run. Existing native churn/streaming
and compatibility tests continue to cover the earlier phases.

Job persistence is separate from worker authority. No retry, cancellation or TTL
clears uncertain backend execution. No Raft service exists; ordinary paths have no
quorum dependency. No Docker, model sharing, owner control endpoint, UI, deployment,
service install, release, commit or publication was added. Terra subagents were
used as requested; work stayed on main without worktrees.

Replicated recovery needs a complete accepted copy, known origin endpoint, explicit
safe/duplicate consent, remaining known attempt budget, eligible local backend and
persisted direct-probe delay. Partitions can run duplicate attempts and temporarily
hide cancellation or newer results; at-least-once recovery is not global uniqueness.
Submission response loss may leave an accepted job; repeat its idempotency key.
Local durability cannot survive loss/unavailability of its only storage.

Logical journal limits are 10,000 jobs, 64 MiB total JSON and 1 MiB per job; WAL and
filesystem overhead are additional. Submissions are capped at 256 KiB and result
bodies at 32 KiB. History is retained, with no automatic archival or external blob
store. Oversized/incomplete results remain unknown; contradictory histories can
require operator interpretation. Private local SQLite files contain full inputs
and outputs. TLS protects peer transport; at-rest application encryption and
same-user caller isolation remain absent. Phase 6 owner scopes are still needed.

### Phase 5 commands and results

Host: macOS arm64/Python 3.14.2, existing core/dev environment. `uv` below denotes
`/private/tmp/idle-herd-phase0-tools/bin/uv`, with
`UV_CACHE_DIR=/private/tmp/idle-herd-phase0-uv-cache` on every invocation.
The first restricted full run reported **19 failed, 2177 passed, 8 skipped**;
failures were denied native socket/OS probes. Native access was then approved;
assertions were retained. The five extra restricted skips were inherited remote
telemetry-schema checks. `uv --offline` controls dependency access, not test network
access. The three remaining skips and coroutine warnings are inherited, as is the
pytest-asyncio fixture-loop deprecation notice.

| Command | Result |
|---|---|
| `uv run --offline pytest --tb=short -q > /private/tmp/idle-herd-phase5-verified-pytest.log 2>&1` with native access | Exit 0: **2214 passed, 3 skipped, 3 warnings in 89.26s**; 2217 collected |
| `.venv/bin/pytest tests/test_peer/test_job_service.py tests/test_peer/test_job_replication.py tests/test_peer/test_jobs_journal.py --tb=short -q` | Final focused rerun after attempt-state review: **32 passed in 3.78s** |
| `.venv/bin/pytest tests/test_peer/test_jobs_native.py -q --tb=short` with native access | **5 passed**: queued/completed restart, unknown running execution, N=2/N=3 survivors; included in full suite |
| `uv run --offline ruff check src/` | Exit 0: all checks passed |
| `uv run --offline ruff format --check src/` | Exit 0: 103 files already formatted |
| `.venv/bin/ruff check tests/test_peer` and `.venv/bin/ruff format --check tests/test_peer` | Exit 0: all checks passed, 23 files formatted |
| `create_peer_app().openapi()` assertions using `.venv/bin/python` | Durable job submission/status/result/cancellation routes present |
| `git diff --check` | Exit 0 |

The full suite adds 37 Phase 5 cases to the Phase 4 baseline. The final focused
rerun also verifies the distinct-worker acknowledgment regression and the
interrupted attempt's persisted unknown state. The three skips are optional ONNX
cases; the three warnings are inherited unawaited `_dummy_process` coroutines.
No Go or container commands, CVE audit, model downloads or release checks ran.

### Exact Phase 5 changed files

Modified: `AGENTS.md`, `CLAUDE.md`, `IDLE-HERD_IMPLEMENTATION_PLAN.md`, `README.md`,
`src/fleet_manager/peer/app.py`, `src/fleet_manager/peer/network.py`, and
`docs/idle-herd/{adr-003-identity-and-jobs.md,api.md,architecture.md,development.md,security.md,implementation-status.md}`.

Added: `src/fleet_manager/peer/{jobs.py,job_service.py,replication.py}`,
`tests/test_peer/{test_jobs_journal.py,test_job_service.py,test_job_replication.py,test_jobs_native.py}`,
and `docs/idle-herd/phase-5.md`.

## Phase 4 — visibility-aware capacity and origin waiters, November 1, 2026

`idle-herd` now supports advisory capacity across its reachable peer view and
bounded public acquisition on a live worker. See [runtime contract and helper](phase-4.md).
Earlier phase sections below remain historical records.

- [x] Model-specific capacity totals, known/reachable/stale/offline counts, per-peer
  freshness, nullable busy, timestamps and explicit non-authoritative scope.
  Signed live remote probes exclude stale/future, mismatched and failed replies;
  multiple endpoints for one worker cannot inflate counts.
- [x] Public acquire defaults to a 60-second wait, bounded at 120 seconds;
  `wait_ms=0` is nonblocking. Successful admission returns 201 and the worker's
  persisted fenced token. Contention is 429 with Retry-After; unavailable/ineligible
  views are 503; expired waits are 504 with latest-round diagnostic counters.
- [x] Bounded origin-local retry rounds, compatible-waiter fairness, independent
  slow remote attempts, lease/discovery/policy/inventory wakeups and 250 ms fallback.
- [x] Durable origin idempotency/target claims, request-ID conflict fences and
  same-key concurrent callers. Unknown remote replies pin the target before retry;
  only authenticated declines permit trying another worker. Client retries never
  treat an uncertain response as proof of no reservation.
- [x] Worker-authoritative local/remote inspect and release, explicit remote token
  execution through the origin, authenticated negative RPC replies and immutable
  token-binding verification. Unreachable workers return `worker_unreachable`;
  a cached lease cannot prove active state or release.
- [x] Deadline/disconnect/cancellation handling and best-effort late-grant cleanup,
  including independent deadlines for shared-key callers and refusal of expired
  post-commit grants. Storage errors fail closed and are traced. Known
  running/quarantined work remains charged.
- [x] Python `async with idle_herd.acquire(model, timeout=...)` helper with
  cancellation-aware release and safe logging of unconfirmed cleanup.
- [ ] Optional SSE capacity watch (the helper satisfies the Phase 4 alternative).
- [ ] Real Ollama/MLX, physical sensors, Linux/Windows native execution, service
  installers and hardware/sleep/wake acceptance. These remain unverified.
- [ ] Phases 5–10: durable jobs, owner controls/UI, sharing, optional admin Raft,
  diagnostics/packaging and finally Docker/Compose.

### Phase 4 evidence and limitations

ASGI tests with fabricated backends exercise 20 simultaneous clients on two local
slots, then 20 callers across two origins/two signed peer workers. They verify
worker slot uniqueness under contention, peer withdrawal/rejoin, model
removal/reappearance, stale busy/idle observations and live policy refusal.
Other tests cover FIFO compatible waiters, new arrivals, queue bounds, shared-key
cancellation, timer fallback, immediate lease/policy wakeups, timeout diagnostics,
slow remote attempts, late grant cleanup, signed response tampering, stale/future
capacity timestamps, duplicate endpoint identities and helper cancellation.

The origin SQLite tests persist claims across scheduler restart, reject conflicting
parameters and retain the selected worker after a fabricated lost reply. Native
process fixtures use real loopback HTTP/TLS and fabricated models for explicit
remote acquisition/inspection/execution/release and a two-peer survivor.
Existing native tests cover either N=2 survivor, each N=3 survivor, partition/rejoin,
seed loss, 100-client contention, commit-before-lost-reply, running worker crash
and origin death with a backend still running. These are runtime tests with mock
models, not real backend or physical sensor acceptance.

Local admission, inference and capacity waits contain no quorum dependency.
No Raft sidecar exists to kill; the implementation cannot demonstrate a real
Raft-loss integration until Phase 8. There is no central queue, required seed,
port reassignment or Docker integration. OpenAI/Ollama, Anthropic Messages and
OpenAI Responses translators remain regression-tested.

Claims and request-ID aliases currently persist indefinitely; automatic retention
and indexing/compaction for long-lived histories remain work. Protect
`origin.sqlite` because it contains bearer reservation capabilities. Origin
restart preserves idempotency claims, but does not autonomously resume waiting
HTTP clients or create durable jobs. A lost reply can retain an unused worker
reservation until TTL; failed cancellation/release is an unknown outcome.
Retries remain pinned to an uncertain worker, possibly sacrificing progress until
the deadline rather than risking another slot claim. Worker restart fences old
attempts; expired/running/quarantined state cannot become a new grant.

Only one slot per acquisition is supported; `context_tokens` and unsupported
fields are rejected. Model fit remains the Phase 1 conservative memory estimate,
not a proof of arbitrary context/VRAM compatibility. Local caller scopes/owner
controls remain Phase 6 work. The running `/openapi.json` describes actual typed
capacity models; the older YAML still includes future authentication, owner
controls and different draft envelope/lease names. No global exactly-once,
global fairness or complete capacity guarantee is claimed. Next: **Phase 5**.

### Phase 4 commands and results

Host: macOS arm64, Python 3.14.2, existing core/dev environment. `uv` was absent
from PATH; it was found at `/private/tmp/idle-herd-phase0-tools/bin/uv`. All uv
commands use `UV_CACHE_DIR=/private/tmp/idle-herd-phase0-uv-cache`,
`--offline --no-sync` and the existing environment.

| Command | Result |
|---|---|
| `uv run --offline --no-sync pytest -q --tb=short > /private/tmp/idle-herd-phase4-final-tests.log 2>&1` | Exit 0: **2177 passed, 3 skipped, 3 warnings in 76.39s**; 2180 collected, including native loopback process tests |
| `uv run --offline --no-sync ruff check src/` | Exit 0: all checks passed |
| `uv run --offline --no-sync ruff format --check src/` | Exit 0: 100 files already formatted |
| `.venv/bin/ruff check tests/test_peer` | Exit 0: all checks passed |
| `.venv/bin/ruff format --check tests/test_peer` | Exit 0: 19 files already formatted |
| `.venv/bin/pytest -q tests/test_peer/test_waiters.py` | Exit 0: 16 passed, including independent shared-key deadlines and expired late grants |
| `.venv/bin/pytest -q tests/test_peer/test_capacity_view.py` | Exit 0: 14 passed, including stale/future observations, worker deduplication and SDK cancellation |
| `.venv/bin/pytest -q tests/test_peer/test_capacity_integration.py -k 'not remote_explicit' --tb=short` | Exit 0: 8 passed, 1 native deselected for this sandbox-only focused run; the final full suite includes that native test |
| App OpenAPI inspection via `.venv/bin/python` | Capacity 200 schema is `ClusterCapacity`; acquire 201 schema is `Lease` |
| `git diff --check` | Exit 0 |

Initial native tests hit the sandbox's loopback bind restriction; they were rerun
with approved loopback access. An initial native explicit-token test used
`/api/generate`, which its fabricated backend does not implement; it now uses the
fixture's supported `/api/chat` route. No failed assertion was skipped.
Inherited optional ONNX skips, queue coroutine warnings and fixture-loop-scope
deprecation remain. Five inherited telemetry-schema tests may access the internet
despite uv's offline flag; model-backend tests use mock/loopback transports.
No Go/container tests, dependency changes, live model execution, fleet deployment,
installer/soak, CVE audit, commit, push or publication was performed.
Work stayed on `main` using GPT-6.1 Terra subagents and no worktrees.

### Exact Phase 4 changed files

Added source:
`src/fleet_manager/peer/capacity.py`,
`src/fleet_manager/peer/waiters.py`,
`src/fleet_manager/idle_herd.py`.

Modified source:
`src/fleet_manager/peer/app.py`,
`src/fleet_manager/peer/discovery.py`,
`src/fleet_manager/peer/local_ledger.py`,
`src/fleet_manager/peer/models.py`,
`src/fleet_manager/peer/network.py`.

Added tests:
`tests/test_peer/test_capacity_integration.py`,
`tests/test_peer/test_capacity_view.py`,
`tests/test_peer/test_waiters.py`.

Modified tests:
`tests/test_peer/test_app.py`,
`tests/test_peer/test_ledger.py`,
`tests/test_peer/test_native.py`,
`tests/test_peer/test_peer_network.py`.

Documentation:
`AGENTS.md`, `CLAUDE.md`, `IDLE-HERD_IMPLEMENTATION_PLAN.md`, `README.md`,
`docs/idle-herd/adr-002-admission-and-capacity.md`,
`docs/idle-herd/api.md`,
`docs/idle-herd/architecture.md`,
`docs/idle-herd/development.md`,
`docs/idle-herd/security.md`,
`docs/idle-herd/implementation-status.md`,
and new `docs/idle-herd/phase-4.md`.

## Phase 3 — implemented native sensor interfaces and idle policy, November 1, 2026

`idle-herd` now owns typed native host observations and worker-local eligibility
policy independently of any UI. See [operation/configuration](phase-3.md).
Earlier phase sections below remain historical records.

- [x] Typed `IdleSignal`, sensor protocol and `EligibilityDecision` with activity,
  source, confidence, freshness, eligibility and refusal reason kept distinct.
- [x] macOS IOHID idle/kernel memory pressure, Linux logind session coverage,
  Windows session-scoped native diagnostics, portable CPU/memory/battery and
  available Linux/Windows temperature readings. Unsupported values stay unknown.
- [x] Trusted absolute-path executable helper option with strict versioned JSON,
  timestamp validation, duplicate/size/time bounds and subprocess cleanup.
- [x] Static independent interactive/background policies: `always`, `idle_only`,
  `utilization_only`, `manual`, `never`; conservative activity requirements and
  explicit headless utilization policy. Manual refuses until Phase 6 controls.
- [x] Immediate new-admission drain on activity, CPU recovery thresholds/dwell,
  sample-gap recovery reset, memory pressure, battery/AC and known thermal limits.
- [x] Durable workload/idle-only lease fields and migration; idempotency matching,
  live checks at grant/consumption, deadline recheck after awaited checks. Peer
  requests cannot claim interactive policy. Stricter local headers cannot weaken
  the stored policy.
- [x] Daemon sampling and revision/condition wakeups for local retry loops, plus
  loopback read-only eligibility and policy-aware local capacity diagnostics.
- [ ] Physical sensor, power/thermal, sleep/wake, real model/backend, Linux/Windows
  native process and service-install acceptance. No bundled host-wide Windows helper.
- [ ] Phase 4 public blocking waits/fairness and Phases 5–10 jobs, owner controls/UI,
  sharing, optional administrative Raft and finally Docker/Compose.

### Phase 3 evidence and limitations

Unit/ASGI tests exercise unknown and partial Windows confidence, stale/future
activity, failed/malformed helpers, multi-session Linux activity/churn, command
timeout/output/cancellation, partial resource failure, pending sampling-worker
bounds, first CPU sample, hysteresis, power/pressure/thermal rejection, stricter
caller policy, idle recovery wakeups and sleep-gap recovery. A stalled read cannot
publish a late CPU baseline or queue unlimited resource-sampling threads.

Ledger and signed RPC tests show that prior idle capacity cannot grant work after
the owner becomes active; a previously issued idle/background token is refused at
consumption. Active work drains without cancellation and uncertain work remains
quarantined. Policy fields persist and participate in idempotency; upgrading the
Phase 2 schema preserves fences and orphan charges. Short leases that expire
during policy checks cannot be consumed or returned as live idempotent grants.

Native loopback process fixtures use fabricated models and synthetic sensor
signals. The new N=1 headless case admits background inference under explicit
utilization policy with unknown activity and refuses a stricter idle-only request.
Existing N=1 crash/fencing, either N=2 survivor, each N=3 survivor, seed-loss,
partition/rejoin, contention and origin-death tests remain regression checks.
OpenAI/Ollama, Anthropic Messages and Responses compatibility tests remain intact.
No central router/seed/quorum dependency, port change or global capacity/exactly-once
claim was introduced; running work never becomes free from expiry or owner activity.

Windows GetLastInputInfo is only a session diagnostic; confidence remains unknown
even with a numeric duration. A trusted configured helper must establish host-wide
coverage itself. Linux depends on accurate logind hints and stable enumeration;
macOS lock/thermal and unsupported GPU/power measurements remain unknown. These
are native interfaces with conservative limitations, not real hardware acceptance.
Known bad resource readings deny even `always`; entirely unavailable sensors
cannot establish those safeguards. `always` can still admit local interactive
work subject to the separate live model/backend/memory fit check. Idle/utilization
policies refuse when their required observations are unavailable.
Direct peer dispatch requires both ends to support the Phase 3 workload grant
contract; mixed Phase 2/3 versions may refuse remote work. Local ingress remains
independent; protocol-version negotiation is not implemented here.

An eligibility `draining` state does not implement the future owner mode API.
No mode mutation, scoped control credentials, UI/service installer, public blocking
wait, job recovery or sharing was added. Gherkin/OpenAPI future contracts remain
specifications beyond the implemented subset. Raft and container tests do not
apply in Phase 3. Next scoped phase: **Phase 4, blocking capacity and fairness**.

### Phase 3 commands and results

Host: macOS arm64, Python 3.14.2, existing core/dev environment. `uv` was absent
from PATH, so initial commands returned `command not found`; direct `.venv/bin`
equivalents were used, then the previously installed tool was located at
`/private/tmp/idle-herd-phase0-tools/bin/uv`. `uv` commands below use that path,
`UV_CACHE_DIR=/private/tmp/idle-herd-phase0-uv-cache`, and `--no-sync` to preserve
the tested environment.

| Command | Result |
|---|---|
| `.venv/bin/pytest tests/test_peer/test_policy.py -q --tb=short` | Exit 0: 34 passed in 0.53s |
| `.venv/bin/pytest tests/test_peer/test_sensors.py -q` | Exit 0: 24 passed (adapter agent run) |
| `.venv/bin/pytest tests/test_peer/test_policy_rpc.py tests/test_peer/test_rpc.py -q` | Exit 0: 14 passed (network agent run) |
| `.venv/bin/pytest -q --tb=short` | Exit 0: 2136 passed, 3 skipped, 3 warnings in 73.75s; before final same-observation regressions |
| `uv run --offline --no-sync pytest -q --tb=short` | Exit 0: **2138 passed, 3 skipped, 3 warnings in 74.40s**; 2141 collected |
| `uv run --offline --no-sync ruff check src/` | Exit 0: all checks passed |
| `uv run --offline --no-sync ruff format --check src/` | Exit 0: 97 files already formatted |
| `.venv/bin/ruff check src/ tests/test_peer` | Exit 0: all checks passed |
| `.venv/bin/ruff format --check src/ tests/test_peer` | Exit 0: 113 files already formatted |
| `git diff --check` | Exit 0 |

The initial sandbox peer suite had six native socket-bind failures (EPERM), with
111 passing tests; native tests were rerun with approved loopback access. A later
native headless test reached the correct refusal but expected the unwrapped error
text; its assertion was corrected to the existing compatibility wrapper. No
failing cases were skipped or removed. Inherited optional ONNX skips, queue
coroutine warnings and fixture-loop deprecation remain; inherited telemetry
schema tests can contact the internet. No dependency install, real model run,
sensor hardware acceptance, fleet deployment, installer/soak, CVE audit, build,
commit, push or publication was performed. Work stayed on `main`, with GPT-6.1
Terra subagents and no worktrees.

### Exact Phase 3 changed files

Source: `src/fleet_manager/peer/app.py`, `backend.py`, `local_ledger.py`,
`models.py`, `network.py`; added `policy.py`, `sensors.py` in that directory.

Tests: `tests/test_peer/native_runner.py`, `network_runner.py`, `test_app.py`,
`test_native.py`, `test_rpc.py`; added `conftest.py`, `test_policy.py`,
`test_policy_rpc.py`, `test_sensors.py` in that directory.

Documentation: `AGENTS.md`, `CLAUDE.md`, `IDLE-HERD_IMPLEMENTATION_PLAN.md`,
`README.md`, `docs/idle-herd/adr-004-sensors-and-model-sharing.md`, `api.md`,
`architecture.md`, `development.md`, `implementation-status.md`, `security.md`,
added `docs/idle-herd/phase-3.md`, and appended `docs/issues.md`, `docs/observations.md`.

## Phase 2 — implemented authenticated peer dispatch, November 1, 2026

`uv run idle-herd` remains a native daemon with localhost ingress and its own
Ollama worker ledger. A separately configured TLS listener adds device-specific
CA enrollment, signed RPC, bidirectional exchange, persisted endpoint discovery
and direct worker admission/streaming. There is no fixed leader, required seed,
Raft service or container dependency. See [operation/security](phase-2.md).
Older sections below are historical phase snapshots, not the current scope.

- [x] Stable node keys/cluster identity, offline proof-of-possession enrollment,
  independently pinned trust anchor, bounded time-limited device certificates.
- [x] Authenticated bounded peer exchange, direct freshness/TTL, persisted known
  endpoints, reconnect, partial-view diagnostics and no discovery-based grants.
- [x] Signed requests with durable SQLite replay fences; exact signed response
  bytes; stream signer bound to the actual encrypted TLS connection.
- [x] Authority-signed eventual revocation propagation and persistence; offline
  renewal works after the authority node's own device credential expires.
- [x] Local-first live admission, origin-bound remote worker grants and direct
  streaming through existing compatibility translators; no post-dispatch failover.
- [x] Optional-listener/credential failures preserve eligible localhost inference.
- [x] Native mock processes prove either N=2 survivor, each N=3 survivor, seed loss,
  automatic rejoin, a 2+1 RPC-filter partition, first-byte streaming and contention.
- [ ] Optional mDNS/Tailscale API discovery; static private-IP seeds work today.
- [ ] Real Ollama/MLX, Linux/Windows hosts, OS services, sleep/wake and idle sensors.
- [ ] Phases 3–10: idle policy, blocking fairness, durable jobs, controls/UI, sharing,
  optional administrative Raft and finally Docker/Compose.

### Phase 2 evidence and practical limits

Tests use fabricated backends and temporary private state, never live models.
The native harness uses actual loopback TCP/TLS and multiple OS processes. It
streams OpenAI chat, Ollama chat, Anthropic Messages, Responses and embeddings
between peers, with the first output observed before a blocked backend finishes.
One worker receiving 100 requests from two origins starts exactly one execution,
with peak active count one. Killing an origin mid-stream leaves independent worker
computation active and charged beyond its actual 30-second lease TTL; another local
acquisition remains busy, with no second backend execution. Both partitions retain
local execution; peers recover
when the test-only incoming RPC filter is removed. The filter is not an OS
firewall/NAT integration test. Local N=1 crash/fencing tests remain in the suite.

Security/failure tests reject forged, missing, stale and replayed credentials,
including replay across restart; foreign principals cannot use another origin's
lease. Revocation propagates over authenticated exchange and survives restart.
Buffered, truncated and disconnected backend streams retain quarantine. Offline
CLI enrollment passes end to end. Reused slot generations and running/quarantined
work survive origin cleanup; optional uvicorn bind failure leaves local serving.

Grants can remain reserved until TTL when their reply is lost. Once execution is
sent there is no automatic retry: the outcome may be unknown and quarantine may
need the documented offline backend-stop attestation. History/lease retention is
unbounded as in Phase 1. These are worker-local safety guarantees, not globally
exactly-once execution or complete capacity. No Raft process exists to kill; its
absence from inference is implemented, not a simulated quorum test.

Private literal-IP HTTPS endpoints only; no arbitrary DNS/public referrals, mDNS
adapter, automatic NAT traversal or fixed peer port. Endpoints are retained up to
128 without eviction, so authenticated referral exhaustion limits discovery;
replacing a worker at a cached address requires offline cache cleanup. Standard
model listings/auto selection and public capacity/acquire remain local-only;
explicit model inference can dispatch to remote peers. Blocking waits are Phase 4.
Revocation is eventual during partitions. Credential import/rotation is offline;
protect a single authority key/issuer registry and apply Windows per-user ACLs.
Mode controls and unknown-human-activity policy are future phases; Phase 2 does
not accept idle-only/background work or add model sharing.

### Phase 2 commands and results

Host: Darwin arm64, Python 3.14.2; core/dev environment with cryptography 50.0.2.
The new declared dependency is `cryptography>=44.0.1`. `uv` means
`/private/tmp/idle-herd-phase0-tools/bin/uv`, with
`UV_CACHE_DIR=/private/tmp/idle-herd-phase0-uv-cache` on each command.

| Command | Result |
|---|---|
| `uv sync --extra dev` | Exit 0; resolves 75 packages, including the crypto dependency installed for focused tests |
| `uv run --offline pytest --tb=short -q > /private/tmp/idle-herd-phase2-final-validation.log 2>&1` | Exit 0: **2073 passed, 3 skipped, 3 warnings in 65.55s**; 2076 collected |
| `uv run --offline ruff check src/` | Exit 0, all checks passed |
| `uv run --offline ruff format --check src/` | Exit 0, 95 files already formatted |
| `.venv/bin/ruff check tests/test_peer/` | Exit 0, all checks passed |
| `.venv/bin/ruff format --check tests/test_peer/` | Exit 0, 12 files already formatted |
| `.venv/bin/pytest -q tests/test_peer/test_rpc.py` | Final follow-up: exit 0, 10 passed in 0.53s, including validation-error redaction and origin API-key checks |
| `.venv/bin/pytest -q tests/test_peer/test_identity.py tests/test_peer/test_rpc.py tests/test_peer/test_cli.py` | Exit 0, 15 passed in 0.40s; includes expired-authority renewal |
| `.venv/bin/pytest -q tests/test_peer/test_ledger.py tests/test_peer/test_rpc.py tests/test_peer/test_cli.py` | Exit 0, 21 passed in 0.58s; final cleanup/listener regressions |
| `.venv/bin/pytest -q tests/test_peer/test_peer_network.py` | Native survivor/partition run: exit 0, 4 passed in 14.61s; final full suite also includes origin death |
| `.venv/bin/pytest -q tests/test_peer/test_peer_network.py::test_origin_death_keeps_remote_execution_charged` | Exit 0, 1 passed in 31.50s; waits beyond actual remote lease TTL |
| `uv run --offline idle-herd --help` | Exit 0, all enrollment/recovery commands visible |
| `uv build --offline --out-dir /private/tmp/idle-herd-phase2-dist` | Exit 0, wheel and sdist 0.12.2 built |
| `git diff --check` | Exit 0 |

Initial offline uv resolution could not find cryptography in its cache; approved
`uv sync --extra dev` resolved it. Native socket tests initially encountered the
sandbox bind restriction and were rerun with approved native access. No failing
assertions were skipped. Three optional ONNX skips, three inherited queue coroutine
warnings and the fixture-loop-scope deprecation remain. Five inherited telemetry
schema tests can contact the internet despite `uv --offline`; inference fixtures
use loopback/mock transports only. No Go/container tests, CVE audit, real backend,
fleet deployment, installer, soak, commit, push or publication was performed.
Work stayed on `main`, with GPT-6.1 Terra subagents and no worktrees.

### Exact Phase 2 changed files

Modified source/configuration:

- `pyproject.toml`
- `src/fleet_manager/cli/peer_cli.py`
- `src/fleet_manager/peer/__init__.py`
- `src/fleet_manager/peer/app.py`
- `src/fleet_manager/peer/backend.py`
- `src/fleet_manager/peer/local_ledger.py`
- `src/fleet_manager/peer/models.py`
- `tests/test_peer/test_ledger.py`

Added source/tests:

- `src/fleet_manager/peer/identity.py`
- `src/fleet_manager/peer/discovery.py`
- `src/fleet_manager/peer/network.py`
- `tests/test_peer/network_runner.py`
- `tests/test_peer/test_cli.py`
- `tests/test_peer/test_discovery.py`
- `tests/test_peer/test_identity.py`
- `tests/test_peer/test_peer_network.py`
- `tests/test_peer/test_rpc.py`

Documentation:

- `AGENTS.md`
- `CLAUDE.md`
- `IDLE-HERD_IMPLEMENTATION_PLAN.md` (preserves the pre-existing starter-prompt edit)
- `README.md`
- `docs/idle-herd/adr-002-admission-and-capacity.md`
- `docs/idle-herd/adr-003-identity-and-jobs.md`
- `docs/idle-herd/api.md`
- `docs/idle-herd/architecture.md`
- `docs/idle-herd/development.md`
- `docs/idle-herd/implementation-status.md`
- `docs/idle-herd/security.md`
- `docs/idle-herd/phase-2.md` (added)
- `docs/issues.md`
- `docs/observations.md`

Next scoped phase: **Phase 3, native OS idle-awareness**. Future Gherkin/OpenAPI
scenarios remain specifications unless explicitly exercised by these runtime tests.

## Phase 1 — implemented single-node worker, November 1, 2026

Current command: `uv run idle-herd`. It runs localhost ingress and an Ollama
adapter with a durable worker-owned slot/memory ledger in one native process,
without discovery, seeds, Raft or the upstream router lifecycle. Legacy commands
remain available. [Runtime, actual API subset and recovery](phase-1.md) describes
defaults and limits; the future OpenAPI YAML and Gherkin are still specifications.
Package identity/version and `UPSTREAM_BASE_COMMIT.txt` remain unchanged.

- [x] Single-process localhost ingress, inventory and worker execution on port 11435.
- [x] SQLite WAL/FULL leases, persistent identity/generations, opaque tokens,
  serialized acquire/consume/release, expiry and exclusive worker ownership.
- [x] Shared cross-model slot/memory accounting and model-specific limits with
  live backend/model/memory checks at acquire and consume.
- [x] OpenAI/Ollama streaming/embeddings and model listings; reused Anthropic
  Messages and Responses translators. Standalone non-Ollama backends remain legacy.
- [x] Minimal nonblocking local capacity/acquire/inspect/release handlers.
- [x] Restart invalidation and quarantine of uncertain execution, plus explicit
  offline maintenance after operator-confirmed backend stop.
- [x] Native mock-process streaming, 100-way HTTP contention and kill/restart tests.
- [ ] Real model/hardware acceptance, Linux/Windows execution and OS service tests.
- [ ] Phases 2–10, including discovery, idle sensors, fair blocking waits, jobs,
  owner controls/UI, model sharing, optional admin Raft and finally containers.

### Verified safety and compatibility

55 new pytest cases cover local admission, API integration, settings, failure
injection and two native process scenarios. One/two/three-slot worker budgets
admit exactly the configured number among 100 racing callers. Shared model/memory
limits prevent per-model queues from exceeding the worker budget. Repeated
acquisition is idempotent; 100 concurrent consumers of one token admit once.
Foreign/forged tokens, model mismatch, old generations and old incarnations fail.

A separate SQLite reader observes reservation commit before acknowledgment and
`running` commit before the mock backend starts. Injected consume-commit failure
dispatches nothing. Expiry only frees unconsumed reservations; timeout,
cancellation, incomplete output, HTTP 5xx and abrupt death retain resource charges.

Native peer/backend processes use real loopback HTTP and fabricated model output.
100 competing acquisitions return one grant and 99 busy responses. Killing the
peer leaves independent mock computation active; restart preserves worker ID,
rejects its old token and denies new work despite `/api/ps` reporting no loaded
models. After confirmed backend stop, offline reconciliation permits a higher
generation. Another process exits via `os._exit` after commit but before replying;
its acknowledged ledger evidence survives and old-incarnation tokens cannot run.
These are N=1 safety tests, **not** Phase 2 multi-peer/partition evidence.

The compatibility tests cover streaming and non-streaming chat through OpenAI,
Ollama, Anthropic and Responses, all three embedding URLs and local model listings.
An inherited non-streaming Ollama path that synthesized `done: true` after a
missing terminal now returns 502. Peer shutdown drains inherited background
trace/latency writes; a regression test verifies persistence without closed-DB
errors. No optional native sidecar or real model was launched.

### Commands and results

Host remains Darwin arm64, Python 3.14.2. `uv` below means executable
`/private/tmp/idle-herd-phase0-tools/bin/uv`, prefixed with
`UV_CACHE_DIR=/private/tmp/idle-herd-phase0-uv-cache` on every invocation.

| Command | Result |
|---|---|
| `uv run --offline pytest --tb=short -q > /private/tmp/idle-herd-phase1-final-pytest.log 2>&1` | Exit 0: **2037 passed, 3 skipped, 3 warnings in 19.90s**; 2040 collected |
| `uv run --offline ruff check src/` | Exit 0: all checks passed |
| `uv run --offline ruff format --check src/` | Exit 0: 92 files already formatted |
| `.venv/bin/ruff check tests/test_peer` | Exit 0: all checks passed |
| `.venv/bin/ruff format --check tests/test_peer` | Exit 0: 6 files already formatted |
| `.venv/bin/pytest tests/test_peer/test_ledger.py tests/test_peer/test_app.py tests/test_peer/test_transport.py tests/test_peer/test_settings.py --tb=short -q` | Exit 0: 53 passed in 0.88s |
| `.venv/bin/pytest tests/test_peer/test_native.py --tb=short -q` | Native rerun before final additions: exit 0, 2 passed in 2.36s; final full suite also runs the added 100-way native HTTP contention |
| `uv run --offline idle-herd --help` | Exit 0: installed entry point and reconciliation help verified |
| `uv build --offline --out-dir /private/tmp/idle-herd-phase1-dist` | Exit 0: sdist and wheel built, distribution `ollama-herd` 0.12.2 |
| Wheel ZIP inspection using `.venv/bin/python` | Contains six peer modules and `idle-herd`, `herd`, `herd-node` entry points |
| `git diff --check` | Exit 0 |

The first restricted native attempt could not bind a loopback socket: 1 failed,
1 passed. Tests were rerun with approved native access; no assertion was skipped
to bypass the restriction. The final full suite also used approved native access
for inherited sockets/kernel probes and remote telemetry-schema tests. Its three
skips are inherited optional ONNX cases; the three warnings are inherited
unawaited `_dummy_process` coroutines. The inherited fixture-loop-scope deprecation
notice also remains. `uv --offline` does not disable network access by tests;
phase-specific inference fixtures use only loopback/mock transports.

No Go, Docker/Compose, fleet deployment, real Ollama/MLX inference, service
installation, CVE audit, release/soak or publication was performed. Work stayed
on `main`; no worktree, commit or remote operation was created.

### Exact Phase 1 changed files

Modified:

- `AGENTS.md`
- `CLAUDE.md`
- `IDLE-HERD_IMPLEMENTATION_PLAN.md`
- `README.md`
- `pyproject.toml`
- `src/fleet_manager/server/routes/ollama_compat.py`
- `docs/idle-herd/adr-002-admission-and-capacity.md`
- `docs/idle-herd/api.md`
- `docs/idle-herd/architecture.md`
- `docs/idle-herd/development.md`
- `docs/idle-herd/implementation-status.md`
- `docs/idle-herd/security.md`
- `docs/issues.md`
- `docs/observations.md`

Added:

- `src/fleet_manager/cli/peer_cli.py`
- `src/fleet_manager/peer/__init__.py`
- `src/fleet_manager/peer/app.py`
- `src/fleet_manager/peer/backend.py`
- `src/fleet_manager/peer/local_ledger.py`
- `src/fleet_manager/peer/models.py`
- `src/fleet_manager/peer/stores.py`
- `tests/test_peer/native_runner.py`
- `tests/test_peer/test_app.py`
- `tests/test_peer/test_ledger.py`
- `tests/test_peer/test_native.py`
- `tests/test_peer/test_settings.py`
- `tests/test_peer/test_transport.py`
- `docs/idle-herd/phase-1.md`

### Remaining Phase 1 risks

Ollama has no authoritative per-request reconciliation/cancellation API in this
adapter. Uncertain work fails closed until an operator stops **all** backend
runners and the daemon, then explicitly attests via offline CLI. False attestation,
direct backend clients, another worker directory using the same backend or a
copied/restored live ledger can invalidate the budget guarantee. Use one worker
authority per backend and healthy local storage. No power-loss/storage-fault
durability or cross-platform filesystem certification was performed.

Memory/model fit uses installed size, loaded digest and configured allowances,
not measured GPU/VRAM/context isolation. Local processes can use inference and
initial acquire without caller credentials; loopback/Host/Origin checks and
one-use reservation tokens do not provide same-user app isolation. Scoped owner
authentication remains Phase 6; no such owner endpoints exist now. Ledger/event
history grows indefinitely, legacy pending queues are not bounded fair waits,
and uncertain backend errors/retries can quarantine slots until maintenance.
The local lease ledger does not persist job inputs or guarantee job recovery.
Global exactly-once execution, complete capacity and N>1 survivor behavior are
not claimed. The next scoped task is **Phase 2**, not containers or Raft.

## Phase 0 — historical design and audit, November 1, 2026

The following records the earlier Phase 0 state and evidence, before Phase 1.

At that point only Phase 0 was implemented. Production Python source, package identity,
dependencies, API behavior, port assignments and native service installation are
unchanged. The running system remains Ollama Herd's router plus node agent.
No future peer, lease ledger, control endpoint, idle helper, model transfer,
SwiftUI app, Raft service, Docker or Compose implementation was added.

Starting revision: `66460f0909f20637bb4afe50bf7e872bc348dacb`, package 0.12.2.
`UPSTREAM_BASE_COMMIT.txt` was missing and now records that exact local revision.
The fork's origin is `https://github.com/pselvana/idle-herd`; there is no configured
upstream remote. The pin records the checkout, not a claim about today's remote
upstream HEAD. Existing MIT copyright/permission and attribution remain intact.

The canonical roadmap is [`IDLE-HERD_IMPLEMENTATION_PLAN.md`](../../IDLE-HERD_IMPLEMENTATION_PLAN.md),
updated with Phase 0 completion, evidence and design clarifications. No branches,
worktrees, commits, PRs or remote publications were created by this task.
All delegated audits used GPT 6.1-Terra as requested.

- [x] Audit actual router/node boundaries, APIs, backends, telemetry and platform behavior.
- [x] Pin the starting checkout and preserve upstream attribution.
- [x] Run the baseline, diagnose restrictions, isolate three test side effects and obtain a passing native rerun.
- [x] Draft ADRs rejecting Raft-global leases and defining survivor availability, worker fencing, churn, identity, jobs, sensors, model sharing and daemon/UI separation.
- [x] Draft and validate versioned OpenAPI for capacity and local controls, with freshness, auth, events and mode semantics.
- [x] Define future invariant scenarios in `tests/contracts/idle_herd.feature`.
- [x] Document native macOS/Linux/Windows development/build/test paths and security boundaries.
- [ ] Phases 1–10: runtime implementation and phase-specific acceptance tests remain unimplemented.

The Gherkin file is a declarative test specification with no runner/step
definitions. It is not collected by pytest and is not counted as passing tests.
N=1 peer independence, last-survivor behavior, fencing/overbooking and truthful
capacity are requirements locked by Phase 0; the current upstream baseline does
**not** demonstrate those future runtime properties.

## Evidence and commands

Host: Darwin arm64; Python 3.14.2; uv 0.13.6. Core + dev dependencies only; no
embedding extra, model weights or live Ollama/MLX service were installed/launched.
See [resolved environment](baseline-environment.txt) for all tested package
versions. The inherited ignore policy excludes `uv.lock`; this snapshot is audit
evidence, not a portable dependency lock.

uv was initially absent. Bootstrap commands:

```sh
python3 -m venv /private/tmp/idle-herd-phase0-tools
/private/tmp/idle-herd-phase0-tools/bin/python -m pip install uv
/private/tmp/idle-herd-phase0-tools/bin/python -m pip install --no-cache-dir uv
/private/tmp/idle-herd-phase0-tools/bin/python -m pip install --no-cache-dir openapi-spec-validator
```

The first pip download and first `uv sync --extra dev` failed with sandbox DNS
restrictions. The pip `--no-cache-dir` bootstrap and sync succeeded with approved
network access. Validator installation was also approved and isolated in the
temporary tools environment; no project dependency was added.

In the following table `uv` denotes the exact executable
`/private/tmp/idle-herd-phase0-tools/bin/uv`; every uv command was prefixed with
`UV_CACHE_DIR=/private/tmp/idle-herd-phase0-uv-cache`. Test output redirection is
shown where relevant.

| Command | Result |
|---|---|
| `git rev-parse HEAD` | `66460f0909f20637bb4afe50bf7e872bc348dacb` |
| `git ls-tree -r HEAD .github/workflows` | No workflow files in the checkout; no CI pipeline to execute |
| `uv sync --extra dev` | Exit 0 after network approval; 72 packages resolved, 55 installed including editable project |
| `uv run --offline pytest --tb=short -q > /private/tmp/idle-herd-phase0-pytest.log 2>&1` | Original sandbox baseline: exit 1; **11 failed, 1966 passed, 8 skipped, 3 warnings in 20.49s** |
| `uv run --offline pytest --tb=short -q > /private/tmp/idle-herd-phase0-native-pytest.log 2>&1` | After test isolation fixes, with approved native OS access: exit 0; **1982 passed, 3 skipped, 3 warnings in 17.69s** |
| `uv run --offline ruff check src/` | Exit 0, all checks passed |
| `uv run --offline ruff format --check src/` | Exit 0, 85 files already formatted |
| `uv build --offline --out-dir /private/tmp/idle-herd-phase0-dist` | Exit 0; built `ollama_herd-0.12.2.tar.gz` and `ollama_herd-0.12.2-py3-none-any.whl` |
| `/private/tmp/idle-herd-phase0-tools/bin/openapi-spec-validator docs/idle-herd/openapi.yaml` | Exit 0, OpenAPI 3.1 valid |
| `.venv/bin/ruff check tests/test_models/test_telemetry_env_contract.py tests/test_node/test_mlx_supervisor.py tests/test_server/test_native_text_proxy.py` | Exit 0, all checks passed |
| `.venv/bin/ruff format --check tests/test_models/test_telemetry_env_contract.py tests/test_node/test_mlx_supervisor.py tests/test_server/test_native_text_proxy.py` | Two formatted files; existing MLX test file has inherited formatting differences, confirmed on its original HEAD content; unrelated lines left unchanged |
| `git diff --check` | Exit 0 |

After formatting the modified MLX test block, the three isolation cases were
rerun inside the original restricted sandbox, exit 0: **3 passed in 0.15s**:

```sh
UV_CACHE_DIR=/private/tmp/idle-herd-phase0-uv-cache /private/tmp/idle-herd-phase0-tools/bin/uv run --offline pytest -q tests/test_models/test_telemetry_env_contract.py::TestNicknameReachesTheSender::test_nickname_reaches_a_built_payload tests/test_server/test_native_text_proxy.py::TestNodeMemoryBound::test_load_truncates_to_the_registry_not_the_model_card tests/test_node/test_mlx_supervisor.py::test_start_fails_when_distributed_and_launch_missing
```

Additional JSON Schema validation used the temporary tools Python with PyYAML
and `Draft202012Validator`: all four OpenAPI schema examples passed (including
date-time formats); six invalid examples were rejected: globally authoritative
capacity, two-slot MVP acquire, wait over 120000 ms, timed auto, simultaneous
duration and expiry, and a local event containing a cluster-capacity snapshot.
These checks validate the draft contract, not live handlers.

The original eleven failures were six denied socket binds, one denied kernel
pressure probe, one denied process memory probe, one real MLX orphan scan, and
two writes beneath the real user data directory. The latter three tests now
inject temporary storage or mock orphan discovery. Assertions were retained.
No production workaround or skipped failing assertion was introduced.

The native rerun collected 1,985 cases. Its three skips are optional ONNX tests
without onnxruntime. The three warnings are inherited unawaited `_dummy_process`
coroutines in queue tests. pytest-asyncio also emits a configuration deprecation
notice before test output; its default fixture loop scope is unset.

Five inherited tests fetch the public telemetry OpenAPI schema and passed with
native network access; they skipped in the restricted run. Thus `uv --offline`
does not imply the test process is offline. No inference backend was contacted.
Neither `scripts/health.sh`'s network CVE audit nor a release checklist was run;
no security-audit, install, soak or publication claim is made.

## Exact changed files

Modified existing files:

- `IDLE-HERD_IMPLEMENTATION_PLAN.md` — record Phase 0 completion, evidence and aligned design/workflow decisions.
- `AGENTS.md` — preserve upstream instructions and add fork invariants/scope.
- `CLAUDE.md` — identify the Phase 0 fork and link current design/evidence.
- `CONTRIBUTING.md` — point contributors to the fork's native workflow.
- `README.md` — distinguish the fork's target availability contract from upstream runtime.
- `docs/issues.md` — append Phase 0 test-isolation fixes and remaining audit gaps.
- `docs/observations.md` — append evidence about sandbox/native baseline differences.
- `tests/test_models/test_telemetry_env_contract.py` — direct rollup storage to `tmp_path`.
- `tests/test_node/test_mlx_supervisor.py` — mock orphan scanning in missing-launch test.
- `tests/test_server/test_native_text_proxy.py` — direct mocked embedding cache to `tmp_path`.

Added files:

- `UPSTREAM_BASE_COMMIT.txt`
- `docs/idle-herd/adr-001-peer-availability.md`
- `docs/idle-herd/adr-002-admission-and-capacity.md`
- `docs/idle-herd/adr-003-identity-and-jobs.md`
- `docs/idle-herd/adr-004-sensors-and-model-sharing.md`
- `docs/idle-herd/adr-005-daemon-ui-and-native-sequence.md`
- `docs/idle-herd/api.md`
- `docs/idle-herd/architecture.md`
- `docs/idle-herd/baseline-environment.txt`
- `docs/idle-herd/development.md`
- `docs/idle-herd/implementation-status.md`
- `docs/idle-herd/openapi.yaml`
- `docs/idle-herd/security.md`
- `docs/idle-herd/ui-and-controls.md`
- `docs/idle-herd/upstream-audit.md`
- `tests/contracts/idle_herd.feature`

Local `.venv`, ignored dependency/cache artifacts and temporary build/tool/log
outputs are not source changes.

## Remaining risks and handoff

1. Current runtime still depends on a router. Peer ingress, worker-owned durable
   admission, restart fencing and survivor guarantees require Phase 1 onward.
   A one-machine upstream router+agent test is not proof of the target N=1 design.
2. Wildcard binds, unauthenticated heartbeat/control routes, optional route-only
   auth and default-on external telemetry remain inherited runtime risks. The
   draft security model is not enforcement. Fork privacy defaults need an
   explicit implementation decision before service use.
3. Linux/Windows native execution, idle sensors and service lifecycle were not
   exercised. macOS checks used native mocks/sockets/probes, not real inference,
   sleep/wake, service supervision, signed app distribution or accessibility QA.
4. Listener allocation, peer TLS/pinning, credential provisioning/rotation,
   Keychain/background-service access, protocol downgrade/replay defenses and
   limits require security review. No new port was assigned. The OpenAPI is a
   draft subset, not complete peer RPC, durable job or model transfer schemas.
5. A durable lease cannot stop an already-running unmanaged backend. Admission
   must budget shared hardware across model queues, reconcile orphan work and
   validate consumption under the same lock as mode changes. Expiry alone must
   never free still-running execution; pause must also fence unconsumed local tokens.
6. Partitions permit duplicate logical attempts and delayed revocation. Payloads,
   models and acknowledged replicas must actually survive for recovery. No
   exactly-once, omniscient capacity or arbitrary-partition strict singleton claim.
7. Dependencies have open lower bounds and the lockfile is ignored; new installs
   can resolve differently. The retained environment snapshot documents this run
   only. Optional ONNX tests remain skipped and inherited warnings remain.

The next scoped implementation is **Phase 1**, with native single-node ingress
and worker-owned persisted admission. Docker/Compose stays deferred until Phase 10.
