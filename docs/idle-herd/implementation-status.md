# Idle-Herd implementation status

## Phase 0 — completed design and audit, November 1, 2026

Only Phase 0 was implemented. Production Python source, package identity,
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
