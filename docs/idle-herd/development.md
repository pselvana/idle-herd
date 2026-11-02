# Native development, build and test paths

Phases 1–7 retain Python package `fleet_manager`, distribution `ollama-herd` 0.12.2,
and commands `herd` / `herd-node`, and adds `idle-herd` single-node ingress.
Run `uv run idle-herd` with separately managed local Ollama on port 11434;
ingress binds 127.0.0.1:11435. See [runtime/configuration/recovery](phase-1.md).
Phase 2 adds opt-in authenticated peers; see [enrollment/operation](phase-2.md).
Phase 3 adds [native sensor interfaces and worker-local idle policy](phase-3.md).
Phase 4 adds [capacity views, bounded waiters and the Python helper](phase-4.md).
Phase 5 adds [durable job APIs and acknowledged replica recovery](phase-5.md).
Sensor tests mock native APIs and helpers; native process fixtures inject synthetic
signals. They do not verify physical activity or hardware thermal/power behavior.
Phase 6 adds [local owner controls and the SwiftUI menu bar](phase-6.md), plus
explicit macOS user launchd configuration. The app has a Swift package and Xcode
project/shared UI-test scheme. No Go sidecar or packaged native installer exists.
See [verification](implementation-status.md)
for the actual host/results and [upstream audit](upstream-audit.md) for limitations.
Docker/Compose is Phase 10 only; it is not a development prerequisite.

Use Python 3.11+ and uv on macOS, Linux or Windows. From the repository root:

```console
uv sync --extra dev
uv run pytest --tb=short -q
uv run ruff check src/
uv run ruff format --check src/
uv build
```

These commands work in POSIX shells and PowerShell; no shell script is required
for core build/test. `uv build` creates a wheel and source distribution in `dist/`;
it does not install native services or publish anything. Tests use mocked inference
backends and need no Ollama/model downloads. Five inherited telemetry contract
tests fetch the public schema at `https://ollamaherd.com/api/v1/openapi.json` and
skip when it is unavailable; `uv run --offline` only disables uv dependency
network access, not network access inside tests. Some upstream tests also use real
ephemeral sockets and macOS kernel/process probes: a restricted sandbox may deny
them. Report those failures and rerun with appropriate native access; do not skip
failing cases to claim a green baseline. Three host-side-effect leaks found in
Phase 0 now use temporary directories or mock orphan discovery.

`uv sync --all-extras` adds optional ONNX/fastembed dependencies. Optional native
MLX/image/STT binaries are separate, platform-specific installations; do not
install them just to run mocked tests. A later `uv sync --extra dev` removes
unrequested extras; retain `--all-extras` on an embedding-enabled deployment.
`uv.lock` is ignored by inherited policy; the Phase 0 environment snapshot is
recorded for audit, but fresh dependency resolution may differ. The tested Python
was 3.14.2 on macOS arm64; Python 3.11/Linux/Windows support is not verified by
that result.

Targeted native test paths:

```console
uv run pytest tests/test_node/ --tb=short -q
uv run pytest tests/test_server/ --tb=short -q
uv run pytest tests/test_models/ tests/test_common/ --tb=short -q
uv run pytest tests/test_local_control/ --tb=short -q
```

For macOS app builds and mock-client checks:

```sh
swift build --package-path apps/macos/IdleHerdMenuBar
swift run --package-path apps/macos/IdleHerdMenuBar ClientSmoke
swift test --package-path apps/macos/IdleHerdMenuBar
apps/macos/IdleHerdMenuBar/scripts/package.sh
```

XCTest and UI automation require full Xcode; see the app README for its shared
scheme command. A Command Line Tools-only Swift installation can build/run the
Foundation smoke target but may lack XCTest. Packaging produces an unsigned local
development app, without signing/notarization or installing a daemon service.

`./scripts/health.sh` is an existing Bash wrapper for tests, lint, formatting,
documentation counts and a network-dependent CVE audit. Its historical count
comparison uses passed tests, not all collected tests; platform skips can make
it disagree with the documented 1,985 collected cases. It is not a substitute
for retaining pytest's actual exit code. Run the underlying commands on Windows
without Bash. A release still requires the full inherited release checklist;
Phase 0 performs no deployment, publication, Homebrew installation or soak.

To validate the design contract in an isolated tool environment without changing
project dependencies:

```console
uv tool run --from openapi-spec-validator==0.9.0 openapi-spec-validator docs/idle-herd/openapi.yaml
```

The [Gherkin scenarios](../../tests/contracts/idle_herd.feature) define future
native acceptance tests. They have no step definitions and pytest does not execute
them. Implement them against actual worker/admission boundaries in their assigned
phases; they do not prove current survivor or fencing guarantees.

For an optional manual upstream smoke run, first start a separately owned Ollama
backend on its unchanged port 11434. The current architecture still needs both
router and agent, even on one machine. Opt out of external telemetry and bind the
router locally in the first terminal:

```sh
# macOS / Linux (POSIX shell)
FLEET_NODE_TELEMETRY=false uv run herd --host 127.0.0.1
```

```powershell
# Windows PowerShell, alternative first-terminal command
$env:FLEET_NODE_TELEMETRY = "false"
uv run herd --host 127.0.0.1
```

In a second terminal set the same telemetry opt-out and run:

```console
uv run herd-node --router-url http://127.0.0.1:11435
```

Review the [audit's network exposure](upstream-audit.md) first: node sidecars and
the automatic Ollama LAN proxy can bind beyond loopback even when the router
does not. Use host firewall restrictions for a deliberate smoke deployment.
Inspect `/v1/models`, `/api/tags`, `/fleet/status`, `/dashboard/api/health` and a
compatible streaming request; this remains an upstream router smoke test, not
the future N=1 peer acceptance test. Phase 0 did not start those services.

Native lifecycle work is staged as follows:

| Host | Existing material / development path | Future evidence required |
|---|---|---|
| macOS | Python/uv; upstream [launchd examples](../examples/launchd/README.md) describe two legacy processes | Independent daemon user service, restart/sleep/wake, sensor permissions; Phase 6 SwiftUI MenuBarExtra/XCTest with a mocked secure API; UI exit leaves jobs alive |
| Linux | Python/uv; no Idle-Herd systemd unit supplied | User/system service scope, session vs headless idle, restart/network changes, CPU and supported GPU backends |
| Windows | Python/uv in PowerShell; no Idle-Herd service/task installer supplied | Scoped service or scheduled task, session idle provenance, graceful shutdown, restart/sleep/wake and supported CPU/GPU backends |

Do not copy reference-fleet process-kill recipes from historical CLAUDE.md onto an
unrelated machine. Native service installation and companion builds arrive in
later phases; the UI never supervises daemon execution. Existing ports remain
unchanged. Peer listeners use explicitly allocated unused private HTTPS endpoints;
there is no default new peer port.

## Phase 7 model-sharing verification

The focused suite is `uv run pytest tests/test_peer/test_model_sharing.py
tests/test_peer/test_model_transport.py tests/test_peer/test_model_sharing_native.py
tests/test_local_control/test_model_controls.py -q`. The native case starts real
loopback HTTP/TLS processes with fabricated GGUF/Ollama, so it requires local socket
access. It exercises permission denial, interrupted source, persistent partial,
restart/resume and local survivor inference. Swift `ClientSmoke` verifies progress
event decoding and consent-bearing retry; full XCTest requires Xcode. These tests
do not establish real-model/MLX or Windows hardware acceptance. See [Phase 7](phase-7.md).
