# Idle-Herd implementation status

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
