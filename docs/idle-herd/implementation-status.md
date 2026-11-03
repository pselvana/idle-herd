# Idle-Herd implementation status

## Backend-managed memory admission — November 3, 2026

Corrected the memory policy after the owner's observation that macOS can reclaim
caches and the backend manages model residency. The earlier 24.6 GB figure was
Idle-Herd's conservative cold-weight/runtime/reserve estimate, not an Ollama
allocation requirement or proof that this model could not execute. The initial
recommendation to free that much RAM was too strong.

`IDLE_HERD_MEMORY_ADMISSION_MODE=backend` is now the default. Model-size and
available-memory estimates remain recorded but do not veto otherwise eligible
worker reservations, token consumption or capacity observations. Ollama owns
actual model loading, idle-model eviction, allocation and context/GPU fit; backend
rejections/failures follow the existing response and quarantine paths. See
[Ollama's concurrency documentation](https://github.com/ollama/ollama/blob/main/docs/faq.mdx#how-does-ollama-handle-concurrent-requests).
No cache or inactive bytes are added to psutil's available value: the installed
macOS psutil implementation already includes free plus inactive memory.

Warning pressure and the minimum-available-memory floor are advisory in default
backend mode; observed values are still reported honestly. Critical pressure,
CPU/idle/activity policy, thermals, power, owner modes, peer quotas, concurrency
limits and persisted worker fencing remain enforced. `strict` memory mode retains
the original estimated RAM budget, host reserve and warning-pressure veto at
reservation and consumption. Doctor reports which memory mode is in effect.

Focused regression: sensor/policy-related earlier fixes remain in this working
change; backend-mode integration tests now reach a mocked Ollama with only 0.5 GB
reported available, while strict mode rejects before backend execution. A ledger
regression admits despite a 22.6 GB estimate and zero reported RAM, enforces the
single slot, and retains quarantine after uncertain backend execution. Critical
pressure rejects both interactive/background work in either mode.

Verification: focused ledger, policy, ingress, diagnostics and backend transport
tests — **102 passed in 2.12s**; the cold-model/low-memory integration pair also
passed separately. Final full native regression — **2339 passed, 8 skipped,
3 inherited warnings in 126.10s**; `/private/tmp/idle-herd-backend-memory-final-pytest.log`.
Ruff lint, format checks across source/modified tests (126 files), and
`git diff --check` passed. No live daemon restart, policy
override, cache purge or real model execution was performed.

## Live reservation timeout diagnosis — November 3, 2026

Investigated a reported HTTP 504 while reserving `gemma4:26b`. Bonjour and
authenticated exchange were working between the local worker and
`pstudio.local` (192.168.68.87). Signed read-only status/capacity requests confirmed
the peer advertised the model, but admission was not available:

- Initially the local worker had about 18.3 GB available and no occupied leases.
  The installed model is 17,987,581,215 bytes; default cold admission estimates
  about 22.6 GB plus the 2 GB host reserve. Subsequent samples also reported
  memory-pressure warning. Discovery and a longer wait cannot resolve memory fit.
- The remote worker reported idle human activity, but successive capacity probes
  alternated missing CPU utilization and idle-recovery hysteresis. Native CPU
  sampling discarded a recent valid measurement when fast callers observed
  identical OS counters, repeatedly withdrawing idle-only eligibility.
- Remote signed observation timestamps were 63–147 ms ahead of local receipt.
  Strict nonnegative age checks incorrectly hid capacity and cached diagnostics
  from this connected, authenticated worker.

Corrected zero-delta CPU sampling to retain a real reading for at most one second
with its original observation timestamp. Missing/invalid/reset counters,
timeouts, cancellation and expired cached data still fail closed. Remote advisory
observations now tolerate up to one second of forward clock skew; old-age TTLs,
identity/incarnation binding, signatures and monotonic cache receipt expiry still
apply. Larger future timestamps remain excluded. Capacity now explicitly reports
`insufficient_memory` when the model cannot fit, without changing lease grants,
fencing, quarantine or memory thresholds.

Verification: focused sensor, policy, RPC, ledger, capacity, diagnostics, discovery
and owner-control tests — **162 passed in 9.42s**. The socket-dependent native
control test was excluded from this focused run; no new full-suite claim is made.
Ruff lint, format checks (26 files) and `git diff --check` passed. Regressions cover
zero counter deltas, cache age/timestamp retention, missing/reset counters, small
positive peer skew, rejection of excessive skew, monotonic cache expiration and
honest empty-slot memory reporting. No real inference was started, no reservation
was acquired, no policies/resources were loosened, and no live daemon was restarted.
Both machines need updated code/restart; sufficient RAM and worker eligibility
remain prerequisites for demonstrating simultaneous execution.

## Native Bonjour mesh formation — November 3, 2026

Implemented the owner's requested automatic Bonjour discovery and automatic LAN
trust. Native `uv run idle-herd` now allocates a separate OS-selected TLS peer port,
creates its device identity, and browses/advertises `_idle-herd._tcp.local.` after
the peer listener is ready. No seeds, enrollment steps, first-node authority or
admin quorum are needed to form the inference mesh. Existing localhost/backend
ports, worker admission, policy, model permissions and API translators are retained.
See [operation, trust controls and limitations](bonjour.md).

Public announcements bind certificates and resolved private endpoints with a
device signature. Device-specific trust pins persist before authorization and
reject changed keys, authorities and identity domains. RPC still requires verified
TLS, signatures and durable anti-replay nonces; discovery never grants capacity.
Independent authority domains federate without rewriting worker/cluster IDs or
durable history. Strict enrolled mode can use Bonjour without automatic LAN trust;
explicit endpoints/seeds remain supported. Containers retain manual defaults.

Departure, offline mode, address changes and restart ports are integrated with
discovery lifecycle. Bonjour failures report separately in health/doctor and leave
localhost inference and available seeded TLS operation running. Concurrent
resolutions, pins and endpoints are bounded. Real-multicast tests use a unique
service type per test and temporary identities, never the production namespace.

Verification on macOS ARM64 / Python 3.14.2:

- Final full regression: `.venv/bin/pytest -q --tb=short` with native sockets —
  **2327 passed, 8 skipped, 3 inherited warnings in 124.66s**;
  `/private/tmp/idle-herd-bonjour-final-pytest.log`.
- Real multicast/TLS test: three independent identities form a mesh with no seeds
  or enrollment, dispatch both ways, survive first-node death, reconnect after
  restart, withdraw in offline mode, retain local inference, then rejoin.
- Unit coverage checks signed TXT/address/port binding, durable trust/replay,
  key/authority/domain pinning, strict rejection, bounded resolutions, candidate
  freshness, endpoint replacement, offline withdrawal and multicast-failure
  isolation. Existing worker-fencing/recovery tests remain intact.
- Enrollment-only native Bonjour test discovers and dispatches both ways without
  seeds or automatic trust. The complete suite includes both real-multicast tests.
- `.venv/bin/ruff check src/` and the modified tests pass; format checks pass
  across all source and changed tests (125 files); `git diff --check` passes.

Limits: native tests use separate processes on this macOS host with fabricated
backends, not a physical two-machine or real-inference acceptance run. Automatic
address selection advertises one private IPv4 interface at a time; multi-homed
LANs, physical roaming, Linux/Windows multicast/firewall behavior and Docker LAN
multicast need host-specific verification. Automatic LAN trust deliberately treats
LAN presence as initial authorization; authority revocation remains domain-local.
Credentials retain their existing finite lifetime and renewal requirements. The
existing two running daemons have not been restarted/deployed by this change;
`pstudio.local` could not be inspected because SSH authentication was unavailable.

## Menu bar UX and status corrections — November 3, 2026

Corrected the status paths behind the menu bar screenshot and reorganized the
existing native companion; this is maintenance within the implemented phases.

- Total memory is sampled with available memory in decimal GB, independently of
  Ollama registry health. A failed initial backend inventory probe now records
  Offline and its observation time, including recovery and subsequent failures.
- Remote slot estimates use fresh, incarnation-bound authenticated peer diagnostics,
  subtracting reserved/running/quarantined occupancy and excluding ineligible peers.
  No reachable peers yields zero; incomplete observations remain unavailable.
  These values remain advisory and never grant worker admission.
- Swift presentation rounds CPU/GPU percentages and memory, spells out power state,
  translates policy reasons, distinguishes unavailable backend inventory from no
  loaded models, and explains unsupported GPU/thermal readings without inventing data.
- The popover has labeled resource rows, explicit empty states, expandable origin
  history/device details, a local-device label, selected mode/expiry indication, and
  persistent header/participation controls/navigation. Diagnostics, refresh and Quit
  UI are in More actions. Preferences display readable background policy choices.
- Status/event snapshots supply job details directly. Older-daemon job fetch errors
  are visible instead of silently clearing jobs. Overlapping polls are serialized;
  slow inventory responses no longer advance the status freshness timestamp.
- The Xcode native app target is now named `Idle-Herd`, resolving its collision with
  the package executable when selecting the UI test host. Debug fixtures include
  realistic resources, empty states and request-body stream handling.

Verification on macOS ARM64, Python 3.14.2, Xcode 16.2:

| Check | Result |
|---|---|
| `.venv/bin/pytest -q --tb=short` with native socket access | **2315 passed, 8 skipped, 3 warnings in 116.75s**; `/private/tmp/idle-herd-ux-pytest.log` |
| Focused owner controls, sensors, policy, peer diagnostics and ingress suite | **135 passed in 10.77s**, including backend-down memory and complete/incomplete/stale remote-capacity regressions |
| `swift test --package-path apps/macos/IdleHerdMenuBar --scratch-path /private/tmp/idle-herd-ux-swift --disable-sandbox` with module caches in `/private/tmp` | **10 passed**; includes formatting, missing/zero/false readings, older snapshots, backend and peer-capacity presentation |
| Swift package and native ARM64 Xcode app/UI-test builds | Passed; local UI runner used ad hoc signing |
| Actual `ContentView` rendered offscreen in an AppKit hosting view using synthetic status | Visually inspected `/private/tmp/idle-herd-ux-preview.png`; row wrapping, unavailable values, fixed controls and footer fit |
| Native XCTest UI execution | **Not verified**: signed runner timed out enabling macOS automation mode; `/private/tmp/idle-herd-ux-xcode-final.log` and `/private/tmp/idle-herd-ux-ui-final.xcresult` |
| Ruff lint/format and `git diff --check` | Passed |

A direct native sensor check also reported total/available memory and AC/battery
without a backend. This is not physical idle, GPU/thermal, sleep/wake, VoiceOver,
service lifecycle or real-inference acceptance. GPU and macOS thermal measurements
remain unsupported/unknown in the daemon; the UI reports that limitation. No live
daemon restart, installation, release, signed distribution or publication was done.
Earlier phase evidence and remaining acceptance gates below remain in force.

## Phase 10 — optional OCI/Compose deployment, November 1, 2026

Implemented optional multi-architecture peer and separate consensus images, locked
container dependencies, single-/dual-/multi-peer and native/container mixed Compose
examples, opt-in Linux NVIDIA backend configuration and non-publishing CI.
See [configuration, enrollment, security and limitations](phase-10.md).
Native defaults, backend URL validation, API translators, worker-local admission,
lease fencing, jobs/model-sharing authority and the quorum-free core are retained.

- [x] Built **linux/amd64 + linux/arm64 OCI archives** for Python peer and optional
  Go consensus; no registry push. Build platform evidence is distinct from runtime.
- [x] Default Compose runs one unprivileged peer with its own durable volume,
  host-loopback inference publication, restart/liveness checks, explicit host-backend
  origin, resource limits, read-only rootfs and no Docker socket. Extra peers get
  separate state and independent backend budgets; mixed peers use explicit private
  advertised host IPs, published TLS ports and offline device-specific enrollment.
- [x] Container launcher streams allowlisted public inference/capacity/jobs APIs
  through unchanged loopback admission; owner controls/dashboard/peer RPC cannot
  cross that gateway even with valid owner tokens. CLI controls inside the container
  remain authenticated. Browser-origin checks precede hop-header removal. Separate
  bind/advertise arguments are opt-in; native listener defaults remain unchanged.
- [x] Container cgroup-v2 observations never claim host idle confidence. Missing
  limits/activity remain unknown and `idle_only` is ineligible. Utilization-only
  contribution is explicit opt-in and does not establish physical host GPU/RAM fit.
- [x] Real Docker Compose tests with fabricated backends: enrolled 1/2/3 peers,
  distinct-model signed remote dispatch, every sole survivor, seed/peer loss,
  volume-retained identity and completed jobs/idempotency after restart, killed
  running job remains quarantined with no unsafe replay, scoped control boundaries.
- [x] Real native Python/container pair over authenticated TLS: remote dispatch
  both ways and local inference with either peer as the sole survivor. Native
  backend and activity observations are fabricated; Docker Desktop networking is real.
- [ ] Physical macOS/Windows host Ollama/MLX/DMR inference, Linux NVIDIA hardware,
  Linux/Windows service and Docker runtime acceptance, real host-sensor bridge,
  and AMD64 container execution. No new MLX/DMR-specific backend/import adapter.
  The host probes on ports 11434/11440/12434 found no running backend. DMR's listed
  Ollama API subset does not prove the required `/api/ps`/weight-size contract.
- [ ] Optional consensus Compose template requires operator-supplied credentials,
  flags and explicit bootstrap; multiarch image build is not containerized voter
  runtime evidence. Native all-voter-loss tests continue to pass. No automatic
  bootstrap/quorum recovery, registry release, signing/notarization, CVE certification
  or physical cross-platform release acceptance is claimed.

### Phase 10 commands and results

Host: macOS ARM64, Python 3.14.2; Docker Desktop CLI 4.97.0, Docker CLI 29.9.2,
Compose 5.6.1, Buildx 0.38.1. Docker Desktop was installed but initially stopped;
started it with approved daemon access. Tests/builds used an isolated Docker
project/subnet/volumes and fabricated backends. Work remains directly on `main`,
without commits, worktrees, installed host inference/services or publication.

| Command | Result |
|---|---|
| `docker buildx build --platform linux/amd64,linux/arm64 -f container/Dockerfile.peer --output type=oci,dest=/private/tmp/idle-herd-peer-multiarch.oci .` | Exit 0; both Linux platforms present in OCI index, final source archive rebuilt after runtime validation |
| Same build for `container/Dockerfile.consensus`, destination `/private/tmp/idle-herd-consensus-multiarch.oci` | Exit 0; both Linux platforms present in OCI index |
| Base, multi-peer, mixed-host, NVIDIA and optional-Raft `docker compose ... config --quiet` with required example environment | Exit 0; no registry publish or GPU execution |
| `IDLE_HERD_TEST_CONTAINERS=1 .venv/bin/pytest -q -x tests/test_container/test_compose_integration.py` with Docker/native socket access | Exit 0: **4 passed in 84.18s**, log `/private/tmp/idle-herd-phase10-compose.log`; fabricated backends, deployed security restrictions, signed native/container pair |
| `PATH=/private/tmp/go/bin:$PATH GOCACHE=/private/tmp/idle-herd-go-cache uv run --offline pytest --tb=short -q` with native access | Exit 0: **2314 passed, 7 skipped, 3 warnings in 128.29s**; log `/private/tmp/idle-herd-phase10-final-pytest.log`. Four opt-in Docker cases skipped here and passed separately above; three optional ONNX skips |
| `uv run --offline ruff check src/ tests/test_container/`; `uv run --offline ruff format --check src/ tests/test_container/` | Exit 0; all checks passed, **122 Python source/test files** formatted |
| In `coordination/`, `GOCACHE=/private/tmp/idle-herd-go-cache /private/tmp/go/bin/go test -race ./...` | Exit 0: `ok idle-herd/coordination (cached)` |
| `uv build --offline --out-dir /private/tmp/idle-herd-phase10-dist` | Exit 0; wheel and sdist, including the container launcher |
| `git diff --check` | Exit 0 |

Early real-container testing caught a nonrelocatable virtualenv shebang; the
image now builds the environment at its final `/opt/idle-herd` path. A shutdown
regression caught cancellation closing shared storage before the TLS listener
stopped; the launcher now awaits the optional listener independently. Docker
startup load required explicit high utilization thresholds in **mock test fixtures**;
production policy thresholds were not relaxed. Optional ONNX skips and inherited
unawaited-coroutine/pytest fixture warnings remain. Normal pytest skips the opt-in
container suite, which must be run separately to supply Docker evidence.

### Phase 10 changed files

- Packaging: `.dockerignore`, `container/{Dockerfile.peer,Dockerfile.consensus,dependencies.lock}`,
  `compose.yaml`, `compose.{mixed,multi,nvidia,raft-optional}.yaml`,
  `.github/workflows/containers.yaml`.
- Container runtime: `src/fleet_manager/container_runtime/{__init__,__main__,runtime}.py`;
  optional sensor/bind arguments and shutdown ordering in `src/fleet_manager/cli/peer_cli.py`.
- Tests: `tests/test_container/{test_runtime,test_compose_integration,backend,mixed_runner}.py`.
- Guidance: `docs/idle-herd/phase-10.md`, this status, architecture/development/security,
  observations and root README/AGENTS/CLAUDE/implementation plan.

Phase 10 is the final planned implementation phase. Remaining work is the listed
physical/backend/platform and release acceptance gates, not an invented Phase 11.

## Phase 9 — native operation, diagnostics and churn, November 1, 2026

Implemented the authenticated local dashboard, friendly doctor/model explanations,
optional signed peer observations, native per-user service integrations, owner
quiet hours and contribution quotas, SwiftUI polish and seeded native churn tests.
See [operation, threat boundaries, backups and rolling upgrades](phase-9.md),
[policy](phase-9-policy.md) and [user-service installation](phase-9-services.md).
Earlier phase sections below retain their historical evidence and limitations.
This historical Phase 9 entry predates the Phase 10 packaging recorded above.
Physical OS/GUI/hardware gates remain open.

- [x] Dashboard uses the same versioned authenticated snapshots/events as the menu
  bar, with independent polling, stale indication and stream reconnect. Connection,
  observation and inspection guards prevent delayed responses overwriting newer
  state. Text nodes and a hash-restricted CSP protect untrusted UI content.
- [x] Local and remote platform/backend/model inventory, sensor provenance, live
  local slot/waiter counts, redacted worker correlations, latest-50 origin job
  history/durability/duplicate consent, transfer progress and optional admin state.
  Unreachable/older peers have unknown current diagnostics. Copy acknowledgments
  are historical evidence, not proof of currently reachable copies.
- [x] Optional signed `/peer/v1/status`, semaphore-bounded independent polling,
  worker/incarnation/signature/age checks and bounded aggregate observations.
  Unknown/404 status never changes discovery/admission. No inference or local status
  request synchronously contacts this cache's remote RPC or admin consensus.
- [x] `idle-herd doctor [--model NAME] [--json]`, `peers`, and existing CLI/API
  controls explain backend fit, owner policy, sensor/session provenance, visibility,
  quarantine and listener state. Advice does not claim a real network/permission audit.
- [x] Persisted quiet hours, AC/battery controls and worker-atomic per-origin peer
  concurrency quotas, checked at grant and consumption. Owner activity overrides
  optimistic peer policy. Running/quarantined work retains its charge. A 20-caller
  quota race admits exactly two at quota two, while another peer can still acquire.
- [x] Opt-in generic open-tab health notifications and public menu-count privacy;
  no prompt/result/credential in status/history/events. Lease/incarnation/generation
  and hashed request correlations support investigation without raw request IDs.
- [x] User-service configuration/install/lifecycle helpers for launchd, systemd user
  and account-scoped Task Scheduler; authenticated API startup toggles never stop
  the current daemon. Unknown manager state is null. No backend is managed by UI.
- [x] Native mock-process seeded 1/2/3/5-peer partitions, per-peer abrupt restarts,
  local inference, live bounded waits, durable result recovery and remote dispatch
  after healing; backend peak remains within one slot. Existing 2/3-to-one survivor,
  worker crash/quarantine and all-voter-loss tests pass in the full suite.
- [ ] Physical macOS/Linux/Windows service registration/login/logout/restart,
  sleep/wake, network roaming, real Ollama/MLX/hardware execution, browser/GUI and
  VoiceOver acceptance. Optional Windows/Linux tray apps, signed MSI/PKG/notarized
  distribution and a packaged optional Raft-sidecar installer are not provided.
  These are explicit release/native acceptance gates, not passing mock tests.

### Phase 9 commands and results

Host: native macOS arm64, Python 3.14.2, Node 24.5.0. `uv` is
`/private/tmp/idle-herd-phase0-tools/bin/uv`, with
`UV_CACHE_DIR=/private/tmp/idle-herd-phase0-uv-cache`. Go 1.25.5 is in
`/private/tmp/go/bin`; `GOCACHE=/private/tmp/idle-herd-go-cache`. Native loopback/OS
access was approved for the full suite and mock benchmark. Work stays directly on
`main`; no service/backend was installed, no fleet was deployed, and no release,
commit, worktree, Docker or Compose integration was created.

| Command | Result |
|---|---|
| `PATH=/private/tmp/go/bin:$PATH GOCACHE=/private/tmp/idle-herd-go-cache uv run --offline pytest --tb=short -q > /private/tmp/idle-herd-phase9-final-pytest.log 2>&1` with native OS/socket access | Exit 0: **2302 passed, 3 skipped, 3 warnings in 129.98s** |
| Initial full-suite run before final peer diagnostics additions | Exit 0: **2297 passed, 3 skipped, 3 warnings in 122.22s**; superseded by final run above |
| `uv run --offline ruff check src/`; `uv run --offline ruff format --check src/` | Exit 0, all checks passed; **115 source files** formatted |
| Ruff check/format of changed/new tests plus `scripts/idle_herd_benchmark.py` | Exit 0; combined source/test check covered **122 files** |
| In `coordination/`, `GOCACHE=/private/tmp/idle-herd-go-cache /private/tmp/go/bin/go test -race ./...` with native access | Exit 0: `ok idle-herd/coordination 3.680s` |
| `CLANG_MODULE_CACHE_PATH=/private/tmp/idle-herd-clang-cache SWIFTPM_MODULECACHE_OVERRIDE=/private/tmp/idle-herd-swift-module-cache swift build --disable-sandbox --package-path apps/macos/IdleHerdMenuBar` | Exit 0; final UI built in 2.30s |
| Same Swift/cache environment, `swift run --disable-sandbox --package-path apps/macos/IdleHerdMenuBar ClientSmoke` | Exit 0; existing authenticated actions, credential, transfer and snapshot-recovery smoke checks pass |
| `node tests/test_local_control/dashboard_smoke.mjs` | Exit 0; mock DOM verifies text-only untrusted content, event EOF reconnect, model query encoding/capacity, delayed old observations, old poll failure after new event, and disconnect fencing |
| `uv build --offline --out-dir /private/tmp/idle-herd-phase9-dist` | Exit 0; wheel + sdist built; wheel contents checked byte-for-byte against current dashboard assets and both diagnostics modules |
| Runtime OpenAPI assertions | Anthropic Messages, OpenAI Responses/chat, Ollama chat, local peers and diagnostics paths present |
| `uv run --offline python scripts/idle_herd_benchmark.py --clients 10 --hold-ms 20` with native access | Exit 0; ten mock-only samples, one slot, p50 **138.111 ms**, p95/max **256.223 ms** |
| `git diff --check`; branch check | Exit 0; work directly on `main` |

Three optional ONNX skips, three inherited unawaited-coroutine warnings and the
pytest-asyncio fixture-loop-scope deprecation remain. Native test attempts inside
the restricted sandbox failed socket binds; approved full native runs above retain
all assertions and pass. A temporary duplicate test basename caused a collection
error during development; the peer file was renamed before the final full run.
Swift emitted restricted user-cache warnings but built successfully. XCTest/GUI
acceptance was not rerun or claimed; the installed Command Line Tools previously
lacked XCTest. The Browser skill reported no available browser, so the DOM smoke is
not browser rendering/accessibility evidence. Inherited telemetry contract tests may
contact their existing schema endpoint; uv offline controls resolution, not test
network access. No dependency/CVE, release-checklist, Homebrew-install or soak claim.

### Phase 9 changed files

- Runtime/CLI: `src/fleet_manager/cli/peer_cli.py`;
  `local_control/{dashboard,diagnostics,models,router,service}.py` and
  `local_control/assets/dashboard.{html,css,js}`;
  `peer/{app,diagnostics,local_ledger,models,network,policy,waiters}.py`.
- Native companion: `apps/macos/IdleHerdMenuBar/Sources/IdleHerdMenuBar/App.swift`.
- Tests/evidence: `tests/test_local_control/{test_api,test_cli_service,test_diagnostics}.py`,
  `tests/test_local_control/dashboard_smoke.mjs`,
  `tests/test_peer/{test_policy,test_churn_native,test_peer_diagnostics}.py`,
  `scripts/idle_herd_benchmark.py`.
- Documentation: `docs/idle-herd/phase-9{,-policy,-services}.md`, this status,
  API/architecture/development/security, observations and root README/AGENTS/CLAUDE/plan.

## Phase 8 — optional native admin consensus, November 1, 2026

Implemented a separate native Go HashiCorp Raft service with persistent Bolt
log/stable storage, file snapshots, dedicated-CA mTLS voters and an authenticated
loopback admin API. Its deterministic allowlist stores only cluster labels and
operator notes. Python polls status independently and exposes scoped owner metadata
operations, cached dashboard status and advisory rendezvous preferred-peer advice.
See [operation/API/security and limits](phase-8.md) and
[ADR 006](adr-006-admin-consensus.md). Phases 9–10 remain future work.

- [x] Default zero voters; explicit empty-state initialization of 1/3/5-voter
  groups. Two-voter bootstrap rejected. Existing state refuses rebootstrap;
  timeouts, partitions and peer discovery never create a replacement history.
- [x] Dedicated voter transport/credentials and state are separate from peer trust,
  local owner scopes, model-sharing permissions, worker leases and origin jobs.
  The only admin FSM keys are `cluster_label` and `operator_note`.
- [x] Cached node/dashboard/event status, truthful unknown quorum for followers or
  unreachable service, leader verification and local write availability. Optional
  service failure never starts a synchronous consensus request in a core path.
- [x] Advisory preferred peer is deterministic for the observed component only;
  offline mode withdraws from advice. No scheduler or resource grant relies on it.
- [x] Real single-voter persistence/API and three-voter Raft partition/restart tests,
  including minority rejection, majority reelection, reconnection and 1/3 survival.
- [x] Three native mTLS voter processes killed together while an independent peer
  consumes a preexisting worker lease, admits a blocked capacity waiter and completes
  a durable job. Non-bootstrap restart restores committed metadata and admin writes;
  job results and local inference remain usable. Backends/sensors are fabricated.
- [ ] Physical LAN partitions, real model/hardware inference, Linux/Windows service
  runtime, five-voter runtime topology and storage power-loss certification. No
  automated permanent-quorum-loss recovery or native sidecar installer is provided.

### Phase 8 commands and results

Host: macOS arm64, Python 3.14.2; portable Go 1.25.5 in `/private/tmp/go`.
The core Python distribution has no new dependency on Go. Build caches/artifacts
are outside the repository. No daemon/service installation or fleet deployment,
release/publication, Docker/Compose, worktree, branch or commit was created.
Work remains directly on `main`; upstream MIT attribution/port map are preserved.

| Command | Result |
|---|---|
| `PATH=/private/tmp/go/bin:$PATH GOCACHE=/private/tmp/idle-herd-go-cache .venv/bin/pytest -q --tb=short > /private/tmp/idle-herd-phase8-native-pytest.log 2>&1` with approved native socket/OS access | Exit 0: **2276 passed, 3 skipped, 3 warnings in 106.05s** |
| Same environment, `.venv/bin/pytest tests/test_peer/test_coordination.py tests/test_peer/test_coordination_native.py -q --tb=short` | Exit 0: **16 passed in 5.91s**, final run including nullable quorum, offline advice, successful/failed scoped metadata forwarding, oversized admin body and zero-port rejection |
| In `coordination/`: `GOCACHE=/private/tmp/idle-herd-go-cache /private/tmp/go/bin/go test -race -count=1 ./...` and `go vet ./...` with the same tool/cache | Exit 0; **four Go tests**, including maximum-escaped snapshot restore and invalid trailing snapshot rejection; final race run 4.311s |
| Same Go/cache, `GOOS=linux GOARCH=amd64 go build -o /private/tmp/idle-herd-raft-linux .` and `GOOS=windows GOARCH=amd64 go build -o /private/tmp/idle-herd-raft-windows.exe .` | Exit 0; cross-compilation only, no Linux/Windows runtime claim |
| `.venv/bin/ruff check src/ tests/test_peer/test_coordination.py tests/test_peer/test_coordination_native.py`; matching `ruff format --check`; `git diff --check` | Exit 0; **115 Python source/test files** checked for formatting |

The first restricted full-suite run failed OS socket/probe cases (**24 failed,
2247 passed, 8 skipped**) and was repeated with approved native access; assertions
were retained. The native full-suite result above supersedes those environment
failures. Three optional ONNX skips, three inherited unawaited-coroutine warnings
and pytest-asyncio's fixture-loop-scope deprecation remain. Inherited telemetry
contract tests may contact their existing schema endpoint. A final focused rerun
covers subsequent small validator/error-label changes; no release/CVE/soak claim.

Status observations expire after three seconds and never authorize a write.
Followers and unreachable voters cannot establish global quorum, so report null;
only a leader reports verified true/false. The Go API independently checks live
leadership/quorum and commits before acknowledging metadata changes. Lost responses
may hide committed metadata writes; repeated identical key/value assignments are
safe, with no compare-and-swap or exactly-once promise. Dedicated-CA certificates
authorize Raft protocol participants rather than binding individual voter IDs;
protect CA issuance and never reuse the inference-peer CA. POSIX file modes do not
certify Windows ACLs or hostile same-user isolation. Go membership is static, with
no discovery-driven enrollment, silent shrink or destructive recovery tool.

### Phase 8 changed files

New Go module: `coordination/{main.go,main_test.go,go.mod,go.sum}`. New Python runtime:
`peer/coordination.py`; modified `peer/{app,models,capacity}.py` and
`local_control/{models,router}.py`. New tests: `tests/test_peer/test_coordination.py`
and `test_coordination_native.py`. New docs: `phase-8.md` and
`adr-006-admin-consensus.md`; root plan/guidance/README and related architecture,
API, development, security and observations now identify the Phase 8 boundary.
Previous phase sections below remain historical evidence.

## Phase 7 — authorized standalone GGUF sharing, November 1, 2026

Implemented source allowlists and owner consent, authenticated digest-bound manifests
and chunk RPC, resumable private staging, persistent destination claims/OS locks,
budgets/throttles, public Ollama import with worker-admitted load confirmation, and
menu bar inventory/progress/pause/retry. See [operation, actual API and limitations](phase-7.md).
Earlier phase sections below are historical records. The next roadmap phase is
**Phase 8**, optional administrative Raft; no Raft or Docker/Compose was introduced.

- [x] Deny-by-default model sharing and separate source-peer/local-owner credentials;
  explicit destination permission/license consent, per-chunk ACL checks and revocation.
- [x] Immutable content/metadata digests, signed bounded chunks/replies, final hash,
  GGUF magic and declared OS/CPU/minimum-backend-version checks before publication.
- [x] Cross-process destination exclusion, unique per-digest journal claim, fsynced
  partials, restart pause, same-manifest source failover and explicit resumable retry.
- [x] Managed cache quota, disk-copy headroom/reserve, manifest RAM budget, source
  reader/bandwidth limits and destination throttling; no automatic manual/pinned eviction.
- [x] Worker-fenced load probes respect busy/paused admission; no execution TTL or
  transfer failure releases running/quarantined worker slots. Backend digest changes
  are reflected in status rather than retaining a false ready observation.
- [x] Authenticated owner API/CLI, redacted sampled `transfer`/`model` events, Swift
  progress and renewed consent on retry. UI lifetime remains independent of daemon.
- [x] Two native daemon processes exercise TLS, permission denial, interrupted source,
  durable partial, source restart/resume/import and local inference with the imported
  model after the source exits again. Existing N=1/2/3 survivor/partition tests rerun.
- [ ] Real GGUF/Ollama hardware and full operator/quantization/context compatibility;
  MLX multi-file import/load, registry fallback and physical GUI/Windows lock acceptance.
  MLX/CUDA-specific/OCI transfers are rejected; no unsupported acceptance is claimed.

### Phase 7 commands and results

Host: native macOS arm64, Python 3.14.2, existing core/dev environment. `uv` denotes
`/private/tmp/idle-herd-phase0-tools/bin/uv` with
`UV_CACHE_DIR=/private/tmp/idle-herd-phase0-uv-cache`. Native loopback test access
was approved; no user service, real model backend, deployment or release was installed.

| Command | Result |
|---|---|
| `uv run --offline pytest --tb=short -q > /private/tmp/idle-herd-phase7-final-pytest.log 2>&1` with native socket/OS access | Exit 0: **2260 passed, 3 skipped, 3 warnings in 97.01s**; final runtime includes transfer shutdown ordering and SQLite connection cleanup |
| `.venv/bin/pytest tests/test_peer/test_model_sharing.py tests/test_peer/test_model_transport.py tests/test_local_control/test_model_controls.py -q --tb=short` | Exit 0: **19 passed in 1.55s**; after full-suite collection, expanded event-transition assertions and added one busy/paused admitted-load regression test |
| `.venv/bin/pytest tests/test_peer/test_model_sharing_native.py -q --tb=short` with native access | Exit 0: **1 passed in 3.94s**; later strengthened to generate with the imported model as survivor, included in final full suite |
| `uv run --offline ruff check src/`; `uv run --offline ruff format --check src/` | Exit 0; all checks passed, **112 files already formatted** |
| Ruff check of all new Phase 7 tests; `git diff --check` | Exit 0 |
| `CLANG_MODULE_CACHE_PATH=/private/tmp/idle-herd-clang-cache swift build --disable-sandbox --package-path apps/macos/IdleHerdMenuBar` | Exit 0; SwiftUI/Core/Smoke targets build |
| Same environment, `swift run --disable-sandbox --package-path apps/macos/IdleHerdMenuBar ClientSmoke` | Exit 0; transfer-event decoding, retry consent, authenticated actions and prior client recovery checks pass |
| Same environment, `swift test --disable-sandbox --package-path apps/macos/IdleHerdMenuBar` | Exit 1: **no such module 'XCTest'** in installed Command Line Tools; not a passing XCTest claim |
| `create_peer_app().openapi()` assertions | Model-sharing, Anthropic Messages and OpenAI Responses routes present |
| Branch and diff checks | Work stays directly on `main`; no worktrees, commits, publication or container changes |

Restricted focused attempts failed native socket binds; repeating with native access
passed without weakening assertions. One early combined collection found duplicate
test module basenames; the local control test was renamed. Development assertions
were corrected for Typer wrapping and the typed mode test interface. Full-suite
results above supersede those development failures. Three optional ONNX skips and
three inherited coroutine warnings remain, plus pytest-asyncio's fixture-loop
deprecation. `uv --offline` controls dependency resolution, not test network access;
inherited telemetry schema tests may contact their existing public schema endpoint.
Swift emitted restricted cache warnings but built; XCTest remains unavailable.

The model-sharing journal/cache is separate from worker leases and origin jobs.
Backend import uses public APIs; a load probe passes through ordinary fenced local
admission. No model transfer needs a coordinator or quorum. Source license/provenance
is authenticated and content-bound, not independently publisher-certified. Optional
platform declarations are constraints rather than universal GGUF compatibility proof.
Ready cache publication is verified; backend metadata may expose verified imported
weights before load confirmation completes. A crash in the backend-create/record gap
requires conservative owner investigation, and external tools can race backend names.
Quota/headroom checks exclude Ollama's separate storage and cannot reserve resources
against external applications. Windows reparse-point fencing and physical GUI use
remain unverified. Real MLX acceptance is an open Phase 7 limitation, not a passed test.

### Phase 7 changed files

New runtime: `peer/model_sharing.py`, `peer/model_transport.py`, and
`local_control/model_sharing.py`. Modified runtime: `peer/{app,models,network}.py`,
`local_control/{models,router}.py`, and `cli/peer_cli.py`. Swift changes:
`Sources/{IdleHerdCore/Client,IdleHerdMenuBar/App,ClientSmoke/main}.swift` and
`Tests/IdleHerdCoreTests/ClientTests.swift` under `apps/macos/IdleHerdMenuBar/`.
New tests: `tests/test_peer/{model_sharing_runner,test_model_sharing,
test_model_sharing_native,test_model_transport}.py` and
`tests/test_local_control/test_model_controls.py`. Documentation: this status,
`phase-7.md`, `api.md`, `architecture.md`, `security.md`, `development.md`,
`ui-and-controls.md`, `adr-004-sensors-and-model-sharing.md`, menu app README,
and root `AGENTS.md`, `CLAUDE.md`, `README.md`, `IDLE-HERD_IMPLEMENTATION_PLAN.md`.
Upstream MIT attribution, legacy compatibility and port assignments are preserved.

## Phase 6 — owner controls and native menu bar, November 1, 2026

Implemented the scoped local control API, persistent owner modes/preferences,
SwiftUI menu bar companion, explicit macOS user-service configuration and limited
read-only dashboard. See [operation/API/security/limits](phase-6.md),
[CLI/service operation](phase-6-service.md) and the
[app build/test instructions](../../apps/macos/IdleHerdMenuBar/README.md).
Earlier phase sections below are historical records. Next: **Phase 7**, verified
compatible model sharing; optional Raft remains Phase 8 and Docker/Compose Phase 10.

- [x] Separate read/control bearer scopes, protected provisioning, loopback/Host,
  Origin/Fetch defenses, peer-listener route isolation and redacted status/events.
- [x] Worker-serialized durable mode changes and grant invalidation; offline local
  inference, paused new-inference refusal, draining existing execution and truthful
  quarantine accounting. Explicit authorized cancellation retains unknown outcomes.
- [x] Persistent versioned preferences, failed-commit handling, timed expiry to
  policy-based auto, and configuration-preserving enrollment/service installation.
- [x] MenuBarExtra local running count, telemetry/age/unknown fields, origin/worker
  job distinctions, mode controls, cancellation, policy editing, diagnostics,
  privacy count hiding, Keychain import and reconnect/polling behavior.
- [x] Swift package, Xcode project/shared UI test scheme, XCTest source, executable
  Foundation mock-client checks, unsigned local app packaging and launchd helpers.
- [x] Native mock-process test with two concurrent jobs, UI HTTP client disconnect,
  offline local inference, daemon restart, persistent modes and preferences.
- [ ] Physical GUI/Quit UI, VoiceOver, Keychain prompts, Xcode UI test execution,
  actual launchd login/logout/sleep/wake/network roam, signing/notarization,
  real model hardware and native Linux/Windows service acceptance.

### Phase 6 commands and results

Host: native macOS arm64/Python 3.14.2, existing core/dev environment. `uv` below
means `/private/tmp/idle-herd-phase0-tools/bin/uv` with
`UV_CACHE_DIR=/private/tmp/idle-herd-phase0-uv-cache`. Native access was approved
for mock loopback processes and inherited OS probes. No user service was registered
or installed; no application was launched or deployment/publication performed.

| Command | Result |
|---|---|
| `uv run --offline pytest --tb=short -q > /private/tmp/idle-herd-phase6-final-pytest.log 2>&1` | Exit 0: **2241 passed, 3 skipped, 3 warnings in 92.11s**; 2244 collected; final run includes credential-read and no-cache hardening |
| `.venv/bin/pytest tests/test_local_control tests/test_peer/test_app.py -q --tb=short -k 'not native'` after credential/no-cache hardening | Exit 0: **48 passed, 1 deselected in 2.14s**; the native case passed in the full suite |
| `uv run --offline pytest tests/test_peer/test_capacity_integration.py::test_remote_explicit_lease_and_single_survivor tests/test_peer/test_peer_network.py::test_native_two_peers_remote_streaming_and_survivors -q --tb=short` | Exit 0: **2 passed in 3.19s**, after fixing enrollment preferences |
| `.venv/bin/ruff check src/` | Exit 0, all checks passed |
| `.venv/bin/ruff format --check src/` | Exit 0, **109 files already formatted** |
| Ruff check for new local-control tests and modified peer test files | Exit 0 |
| `CLANG_MODULE_CACHE_PATH=/private/tmp/idle-herd-clang-cache swift build --disable-sandbox --package-path apps/macos/IdleHerdMenuBar` | Exit 0, native SwiftUI/Core/Smoke targets build |
| `CLANG_MODULE_CACHE_PATH=/private/tmp/idle-herd-clang-cache swift run --disable-sandbox --package-path apps/macos/IdleHerdMenuBar ClientSmoke` | Exit 0: mock credential validation, URL restrictions, snapshot/disconnect/recovery and authenticated cancellation |
| `CLANG_MODULE_CACHE_PATH=/private/tmp/idle-herd-clang-cache swift test --disable-sandbox --package-path apps/macos/IdleHerdMenuBar` | Exit 1: installed Command Line Tools have **no such module 'XCTest'**; not a passing test claim |
| `CLANG_MODULE_CACHE_PATH=/private/tmp/idle-herd-clang-cache apps/macos/IdleHerdMenuBar/scripts/package.sh` with native build access | Exit 0; initial production build **35.24s**, then incremental rebuilds; creates ignored unsigned `apps/macos/IdleHerdMenuBar/build/Idle-Herd.app` |
| `plutil -lint` for Xcode project and packaged app Info.plist; `xmllint --noout` shared scheme | Exit 0, syntax validation only; no Xcode build/UI test execution |
| `git diff --check`; branch check | Exit 0; work stays directly on `main` |

An early restricted focused run had denied native socket binds and incomplete
test-double interfaces. A development full run before the enrollment fix reported
9 failures, 2230 passes and 3 skips. Enrollment/maintenance was incorrectly saving
its default policy, overriding the actual daemon's configured peer policy. Only
explicit owner preference patches now persist overrides, and a regression test
covers reopening after enrollment. All assertions remain; the final full run is
the result above. Three optional ONNX skips and inherited coroutine warnings remain,
as does pytest-asyncio's fixture-loop deprecation. Five inherited telemetry schema
tests may contact their public schema endpoint; `uv --offline` is not process-wide
network isolation. Swift emitted restricted user-cache warnings but built; no
cache access warning was recast as a passing XCTest run.

The full suite reruns prior native N=1/2/3 survival/partition/fencing scenarios.
New signed-peer tests prove stale grants cannot evade modes through either peer
RPC or localhost. Local slot counts remain worker-authoritative; cluster reachability
is advisory and remote slot indications remain unknown when unmeasured. No future
model transfer, Raft or container acceptance is claimed. The native UI-client close
test proves service separation at the API/process boundary, not physical GUI clicks.
Default Keychain access and launch-at-login UI use require real signed/installed
macOS acceptance. Windows ACL enforcement and hardware activity remain unverified.

### Phase 6 changed files

New source: `src/fleet_manager/local_control/{auth,models,router,dashboard,service}.py`
and package initializer. Modified runtime: `cli/peer_cli.py` and
`peer/{app,models,local_ledger,network,discovery,capacity,waiters,job_service,replication}.py`.
The menu app's `Package.swift`, Xcode project/shared scheme, Swift Core/App/Smoke
sources, XCTest/UI test sources, README, ignore rules and packaging script live in
`apps/macos/IdleHerdMenuBar/`. Tests: new `tests/test_local_control/` and updates to
`tests/test_peer/{native_runner,test_capacity_view,test_job_replication,test_job_service,test_waiters}.py`.
Documentation: this status, `phase-6.md`, `phase-6-service.md`, `api.md`,
`ui-and-controls.md`, `development.md`, `security.md`, and root plan/README/AGENTS/CLAUDE.
Root `.gitignore` excludes Swift build state. MIT attribution and legacy ingress
surfaces remain; no worktree, release, real backend deployment or container change.

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
