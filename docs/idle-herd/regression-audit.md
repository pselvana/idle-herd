# Last 29 commits: regression audit

Reviewed November 3, 2026, at `0571cc5`. Scope is the 29 commits from
`9dcb72f` through `0571cc5`, inclusive (base: `66460f0`). Tests use fabricated
backends, isolated temporary state and mock peer transports. Native integration
tests start their own loopback processes; no installed model or running owner
daemon is required.

## Coverage of the commit range

The earlier phase commits already include concurrency, restart, protocol and
survivor tests. This pass supplements those suites with failure cases around the
subsequent changes. Documentation and packaging instructions are reviewed as
instructions; a Python assertion cannot establish a physical installation passed.

| Commits | Behavior reviewed | Automated evidence |
|---|---|---|
| `9dcb72f`, `2ba98a4` | Plan, invariants, compatibility baseline | Existing compatibility/contract suites; phase requirements remain distinct from acceptance evidence |
| `941bcbc` | Durable admission, slots, memory, quarantine | Existing ledger/app/native tests; new strict terminal-value checks, late embedding completion and shutdown during detached recovery in `test_transport.py` |
| `3ca7a49` | Identity, signed RPC, direct dispatch, survivors | Existing identity/RPC/network/native suites; new durable LAN pin-write and model-response authentication tests |
| `54b4292` | Worker-local idle policy | Existing policy/sensor suites, including unknown activity, hysteresis and live consumption checks |
| `361c56a` | Capacity snapshots, bounded waiters, idempotency | Existing capacity/waiter suites, including stale capacity, independent deadlines and cancellation |
| `9d9c1bc` | Durable jobs and accepted replicas | New `test_replication_regressions.py`: bound acknowledgments, invalid certificates, authenticated origin errors, endpoint-loss reset and completed-result merge; existing job restart/cancellation/survivor tests |
| `48a15dd` | Owner controls, CLI, menu bar | Existing scope/mode/persistence suites; new Swift credential destination, credential bounds and invalid-event tests |
| `e69d1af` | Resumable sharing and local install | Existing sharing/install-lock/resume suites; new corrupt failover chunks, manifest-bound failover and no authentication downgrade |
| `3bb6761` | Optional admin consensus | Existing Python tests for disabled/missing/expired coordination and independent inference; native Go limitation below |
| `a03d354` | Diagnostics, quiet hours, service installers, churn | Existing mocked service/diagnostics/policy/churn suites; dashboard JavaScript smoke test |
| `542094e` | Container inference boundary | New exact dynamic job/reservation routes, encoded/private/wrong-method route rejection, capability/query preservation, hop-header filtering and upstream cleanup |
| `6e6cfa2`, `07d6f6a` | Menu bar prerequisites and local install instructions | Swift package compilation/tests exercise the local build; login/service/GUI installation remains a physical acceptance gate |
| `720e51d` | Status decoding and missing measurements | Existing presentation/client tests; new malformed-event rejection with forward-compatible fields |
| `828db6a` | Bonjour and automatic durable LAN trust | New reserved-port cleanup, interface fallback, offline-during-resolution, failed pin-write, corrupt-store and same-device endpoint-move tests |
| `8a8b089` | Strict versus backend-managed memory | Existing ledger, policy, sensor and app tests cover low available RAM, critical pressure and live fit checks |
| `4a1c2bc` | Always policy while owner active | Existing policy/RPC tests cover active-owner execution, explicit idle-only tightening, quiet hours and resource safeguards |
| `ad0f112`, `049de22` | Cancellation drainage, quarantine recovery, host labels | New synchronous embedding completion and detached-reader shutdown tests; existing origin-host and recovery status tests |
| `daaf85d` | Preferences drafts and menu organization | Existing draft validation/change-only save tests; Swift core tests compile with the companion |
| `1f1eac0`, `227aa21` | Execution location and narrow status icon | New Swift freshness matrix covers local/peer/origin observations independently, outgoing-count override and connected-peer artwork |
| `d458390`, `481e398` | Selected-peer copying and default sharing | Existing selected-peer/authentication/permission-persistence tests; new retry request verifies original manifest, renewed consent and alternate source |
| `510909b` | Embedding-only model copying | Existing admitted embedding load-confirmation and native fabricated embedding-copy scenarios; new embedding-body recovery tests |
| `48d4df2` | Complete managed/standalone packages | New file/import-metadata digest binding, source mutation, invalid metadata, cloud/changed/unknown installed layers, MLX pre-download version gates and auxiliary-shard completeness tests |
| `6e0102f`, `0571cc5` | Yellow admission blocks and green running work | New Swift matrix tests stale versus fresh observations and green/yellow/system priority; owner mode reasons override backend/resource warnings |

## Defect found and fixed

`validate_layout()` checked split-shard completeness only for the base `file`
role. An export containing `draft/model-00001-of-00002.gguf` or
`adapters/model-00001-of-00002.gguf` could pass with its second shard absent.
Both regression cases failed before the fix. Validation now checks every role
and keeps shard groups separate by role. Adding the missing shard succeeds and
the complete package still passes content verification.

## Verification and limits

The original focused fork suite passed **436 tests, 1 skipped**, with **85%**
combined statement/branch coverage of `peer`, `local_control` and
`container_runtime` (coverage.py; subprocess execution is not instrumented).
Coverage is diagnostic, not proof of every interleaving or hardware behavior.

Added **78 Python cases and seven Swift tests**. The full Python suite passes
**2,496 tests, 8 skipped**, with **86%** combined statement/branch coverage over
the source scope above. The final changed Python suites pass **144 tests**.
The **38 Swift package tests** pass. Ruff source checks, formatting checks of
source/changed tests and the dashboard script smoke test pass. Exact commands,
logs and warnings are summarized in
[implementation status](implementation-status.md).

Reproduce the main checks from the repository root:

```console
uv run --no-sync pytest --tb=short -q
swift test --package-path apps/macos/IdleHerdMenuBar
uv run --no-sync ruff check src/
node tests/test_local_control/dashboard_smoke.mjs
git diff --check
```

The Python run has the three known `_dummy_process` coroutine warnings.

The host has no `go` executable, so the optional native Raft fixture skips and a
direct `go test -race ./...` cannot run. Three ONNX tests skip because onnxruntime
is unavailable. Four Docker/Compose scenarios require explicit opt-in; this pass
does not claim new live-container evidence. Physical GUI and
VoiceOver, login/sleep/wake, Linux/Windows service execution and real
Ollama/MLX/GPU/model acceptance remain unverified. The tests do not restart or
deploy the owner's daemon or menu bar application.
