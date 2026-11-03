# Phase 9: native operation, diagnostics and churn

Phase 9 extends the existing native peer runtime. Inference, worker leases, origin
jobs and model sharing keep their existing authorities. No new coordinator or port
is required. Docker/Compose remains Phase 10. Legacy `herd`/`herd-node`, Anthropic
Messages and OpenAI Responses remain available; one ingress owns port 11435.

## Dashboard and local UI

Open `http://127.0.0.1:11435/dashboard` and enter the **read** token from the private
`control-credentials.json` in the configured data directory. The token stays in tab
memory; disconnect clears it and aborts pending requests. There is no token in the
URL, HTML, browser storage or cookie. The page is read-only: authenticated owner
changes use the CLI/API or menu bar. Browser control writes remain forbidden.

The dashboard consumes the same version-1 `/v1/node/status` snapshots and
`/v1/node/events` envelopes as the menu bar. It polls independently of the stream,
marks a quiet connection stale after five seconds, reconnects streams and discards
responses from old connections or older observations. Model inspections are fenced
against a changed selection. Tables use text nodes, not HTML interpolation, and a
hash-based CSP excludes external scripts/styles and framing. The dashboard has
semantic tables, text state labels, visible keyboard focus, responsive scrolling
and no animation. Real browser layout, screen-reader and GUI accessibility testing
remain open; the Node smoke harness is a DOM simulation.

The local snapshot includes authoritative local occupied/reserved/running/quarantined
counts, origin waiters, resource timestamps/provenance, per-peer observations,
redacted worker correlations, authorized transfer progress and the latest 50 local
origin-job summaries. History exposes durability, acknowledged copy count, attempts
and possible-duplicate consent, never job inputs/results. Copy acknowledgments are
historical durability evidence, not proof those machines are currently reachable.
The origin journal still stores job payloads as required for durable execution.

Updated peers support an optional signed `POST /peer/v1/status` with `{}`. A bounded
background poller caches platform, backend, model names, sensor provenance/activity,
policy reason, slot/job/waiter counts and replicated-job counts. Responses bind to
the authenticated worker and incarnation, expire by receipt and observation age,
and disappear from current status when discovery says unreachable or local mode is
offline. Remote status is advisory and cannot issue leases. Old peers returning 404
show unknown diagnostics; the discovery descriptor format is unchanged. The status
RPC neither exposes owner credentials nor provides control operations. Unknown
fields/measurements remain unknown; failed probes do not certify a partition.
Selected-model capacity uses the existing live best-effort capacity API and reports
freshness and eligibility separately from any actual grant. Optional admin
leader/quorum is a cached observation and never gates inference.

SwiftUI now scrolls its content and preferences, displays peer evidence and structured
diagnostics, and edits quiet hours and contribution limits through the same versioned
owner API. `hide_job_counts` hides the public menu bar count and its accessibility
label; it does not remove authorized private dashboard/job history. Quit UI leaves
the daemon and sensors running. The package and client smoke target build on this
host; full GUI/sleep/wake/VoiceOver acceptance is not established.

Health notifications require three opt-ins: `notifications_enabled` in daemon
preferences, this tab's checkbox, and browser permission. Alerts have generic text
without model names, prompts, IDs or outputs. Job completions remain silent. Alerts
are limited to one per five minutes per connected tab; the page may alert on repeated
backend failures, failed/unknown jobs, disk-related transfer errors or no reachable
known peers. This is a best-effort open-tab feature, not an OS background alert
service. Multiple tabs can independently alert. SwiftUI exposes the daemon preference;
it does not install a native notification agent.

## Doctor and owner preferences

```console
uv run idle-herd doctor
uv run idle-herd doctor --model tiny:latest
uv run idle-herd doctor --json
uv run idle-herd peers
uv run idle-herd preferences
```

Doctor verifies read-scope access, explains backend/model readiness, background
policy, sensor provenance, quarantine, discovery/listener health and optional admin
coordination. Model inspection reads live local fit; it does not acquire a slot or
contact peers. RAM estimation does not certify arbitrary context/GPU compatibility.
A missing control credential or disconnected daemon is reported as a connection
failure; local API authentication does not certify Windows ACLs or hostile same-user
isolation. Network advice identifies checks to perform, not an unperformed firewall,
certificate-expiry or physical reachability audit.

Use the returned preferences version for a compare-and-set update:

```console
uv run idle-herd preferences --expected-version 1 --set '{"quiet_hours_enabled":true,"quiet_hours_start_minute":1320,"quiet_hours_end_minute":420,"peer_max_concurrent":1,"require_ac":true}'
```

In PowerShell pass the same JSON as one quoted argument (or use the local API).
The version shown here is illustrative; stale versions return 409. See
[worker policy details](phase-9-policy.md). Quiet hours follow the host's local
clock/timezone, including DST changes; equal endpoints block peer work all day.
Owner activity blocks `idle_only` work; explicit `always` permits active-owner
peer work, and `utilization_only` permits it when system usage is low. Quiet hours
remain independent of the configured background policy.
Local interactive inference retains its configured policy. AC-only, battery floor,
thermal and memory checks remain grounded in host observations. No synthetic watts
or energy savings are reported. Per-origin concurrency quotas count reserved,
running and quarantined work atomically; they are not tokens/day or time budgets.
Neither a preference update nor a clock change releases uncertain execution.

## Install, autostart and upgrade

Use a stable Python 3.11+ environment and install the checked-out project, for example
`uv tool install .` from the repository, or install a locally built wheel in a
persistent virtual environment. The distribution name remains `ollama-herd` 0.12.2;
this is not a newly published release. Start the separately owned backend using its
normal tools, then run `idle-herd` for foreground operation. Review settings before
writing the per-user service configuration:

```console
idle-herd service install
idle-herd service enable
idle-herd service status
```

See [macOS launchd, Linux systemd user and Windows Task Scheduler details](phase-9-services.md).
Install writes configuration; enable registers/starts the user service. CLI disable
also stops it, while the authenticated `/v1/node/service` startup toggle leaves the
running daemon alone. Reinstall service configuration after moving the environment
or changing daemon environment settings. No backend is installed, killed or owned by
the UI/service helper. Linux logout persistence depends on the user manager/linger;
Windows uses the current interactive account. No elevated system service, signed
MSI/PKG, notarized app or packaged Raft-sidecar installer is provided. Native manager
registration and lifecycle are mocked here, not certified on any OS.

For a rolling upgrade, drain one worker at a time, wait for work to finish, stop its
own service, back up offline state, update its stable environment, reinstall service
configuration and restart. Check `doctor`, localhost inference and peer reachability
before moving to the next machine. A single survivor must remain eligible for its
own backend/model; a remote-only model or payload cannot be conjured by an upgrade.
Retain persisted modes/preferences and identity. New preference fields default
without deleting old values. Optional diagnostics can be absent on old peers.
No downgrade compatibility of newly persisted preference fields is promised: restore
a coordinated offline backup if reverting software requires it. Never restore old
worker state while its backend might still execute newer leases. Do not rebootstrap
admin consensus during an inference-peer upgrade or after voter loss.

## Backups, threat boundaries and troubleshooting

Use healthy local storage. Drain and stop the daemon before backing up its entire
data directory, including identity, worker ledger/owner lock databases, origin and
job journals, control credentials, trust/revocation records and model-sharing state.
Include SQLite WAL files or use a consistent offline checkpoint/copy; do not copy
just an active main database. Stop **all backend runners** before restoring worker
state. Keep encrypted copies private. Restore only to the original worker authority;
never clone the same identity/ledger into two live workers. Do not copy staged model
partials into installed inventory. Optional Raft has separate storage and recovery
rules; do not silently merge/rebootstrap voter histories. A backup alone does not
prove a running backend stopped. Explicit reconciliation still needs the backend-stop
attestation described in [Phase 1](phase-1.md).

Loopback controls require scoped tokens, Host/Origin boundaries and browser-write
rejection. Peer trust is distinct. A trusted peer can learn aggregate status and
model names; do not enroll an untrusted machine or assume instantaneous revocation
across a partition. Status omits private model paths, raw request IDs, prompts,
results and credentials. Lease IDs, generations/incarnations and hashed request
correlations allow local investigation; request hashes are correlation metadata,
not anonymous data. Diagnostic failure reports avoid arbitrary private exception
strings. Protect generated service launchers/settings against same-user and ACL
attacks. Preserve [upstream MIT attribution](../../LICENSE) in distributions and
respect independent model-license permission. See [security](security.md).

| Observation | Action |
|---|---|
| `activity_unknown` | Check the native helper/session provenance; unknown cannot grant idle-only work |
| `quiet_hours` / `peer_quota` | Inspect owner schedule and per-origin occupied leases; local work is independent |
| `backend_unavailable` / `not_installed` | Check separately managed Ollama and the requested model |
| Zero slots with an eligible model | Wait for live admission; check busy or quarantined work |
| Stale/unreachable peers | Check private endpoints, firewall, clock and mutual enrollment/trust; infer locally if eligible |
| No admin quorum | Strict admin writes may fail; continue local inference; never auto-rebootstrap |
| Quarantined work | Stop daemon and all backend runners before explicit offline reconciliation |
| Disconnected menu/dashboard | Check service/API/credential state; old samples are not current availability |

## Evidence and remaining native gates

`tests/test_peer/test_churn_native.py` uses seeded schedules for 1, 2, 3 and 5 native
Python peers and fabricated HTTP backends. Each peer is isolated by signed-RPC
filters, then killed/restarted; each component continues local inference, waits for
live release and retrieves durable job results. Reconnection restores remote
execution and backend peak concurrency remains within the worker budget. These are
application-level partitions, not physical LAN or sleep/wake tests. Existing native
fencing/quarantine tests and all-voter-loss tests remain applicable. Unit tests also
exercise conservative unknown activity and hysteresis after a sampling gap.

Run the dependency-free dashboard smoke harness with
`node tests/test_local_control/dashboard_smoke.mjs`. It checks text-only insertion,
SSE updates, model-query encoding, authorization and stale responses after disconnect.
No browser was available in this session; visual/browser QA is an open gate.

`scripts/idle_herd_benchmark.py --clients 10 --hold-ms 20` measures one-slot loopback
mock acquire contention in temporary storage. On native macOS arm64, the recorded
sample of ten clients measured p50 **138.111 ms**, p95/max **256.223 ms** with 20 ms
holds. This includes deliberate queueing and is not an inference-throughput,
energy, physical-network or Linux/Windows benchmark. Repeat with the same parameters
to compare changes; no universal latency target is asserted.

See [implementation status](implementation-status.md) for final commands/results.
Physical macOS/Linux/Windows service login/restart/sleep/wake/network roaming,
real Ollama/MLX model execution and signed desktop distribution remain acceptance
gates. Optional Windows/Linux tray apps were not implemented; native CLI/API
operation is supported by the new service integrations. Stop at Phase 9;
Phase 10 container work remains unimplemented.
