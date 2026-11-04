# Phase 6: local owner controls and macOS menu bar

`idle-herd` owns admission, sensors, jobs, mode and preferences. The native
SwiftUI `MenuBarExtra` is an optional authenticated client. Quitting the UI
does not signal, start, stop or supervise the daemon. Windows/Linux keep the
same foreground daemon and CLI/control API; macOS launchd is optional.

## Run and connect

```sh
uv run idle-herd
uv run idle-herd status
uv run idle-herd offline --for 1h
uv run idle-herd auto
uv run idle-herd drain
uv run idle-herd pause
uv run idle-herd jobs
uv run idle-herd doctor
uv run idle-herd preferences
uv run idle-herd cancel JOB_ID
```

Build the [Swift package](../../apps/macos/IdleHerdMenuBar/README.md) with
`swift build --package-path apps/macos/IdleHerdMenuBar`. Its packaging script
creates an unsigned `.app` with `LSUIElement=true`, targeting macOS 13+.
Import the daemon's `~/.idle-herd/control-credentials.json` (or configured data
directory equivalent) in Preferences; its control credential is saved in
Keychain. Credentials never appear in URLs. Start the daemon independently
first. [User-service commands](phase-6-service.md) provide explicit install,
enable/start and status operations; the UI can configure next-login launch
without unloading or restarting current work. Service installation captures
the effective settings and current Python environment; move/reinstall it if
that environment or working directory changes.

The menu shows authoritative local running lease count, reservations and
quarantine separately; origin jobs include their last known executing worker.
The compact menu bar icon shows a left llama for local-origin execution on this
Mac. Its right llama is outlined with connected peers, gray-filled for outgoing
execution, and filled in the menu bar tint for incoming execution (incoming takes
priority during overlap). No reachable peers removes the right llama entirely.
The new `outbound_running` observation covers ordinary forwarded requests as well
as durable jobs; older daemons fall back to journaled remote jobs only. Stale
status clears fills and hides the right llama. These are presentation observations,
not admission or remote-completion authority.
Resource observations include provenance/age and unknown values. Unoccupied
slots are a budget observation, not proof of model fit. Cluster reachability
is a local estimate; remote slot counts stay unknown without a measured view.
No model transfer is implemented or claimed before Phase 7. Job throughput
and ETA are unknown where no backend measurement exists. The dashboard action
opens a limited read-only local status page; paste the read credential there
for a memory-only session. Phase 9's richer cluster dashboard remains future.

## Modes and worker authority

| Mode | New peer admission | New outbound inference | New local inference |
|---|---|---|---|
| `auto` | Live worker policy | Allowed | Live worker policy |
| `draining` | Refused | Allowed | Live worker policy |
| `offline` | Refused | Refused | Live worker policy |
| `paused` | Refused | Refused | Refused |

All modes finish already-running work by default. Only an explicit authorized
cancel command requests cancellation of an origin-owned durable job. Backend
cancellation may leave `unknown_outcome` and quarantine; no mode, cancel command
or timeout clears quarantine. Raw non-journal inference has no reliable
per-request backend cancellation API. Existing peer streams continue; issuing
origins may inspect/release previous grants while the worker is offline.

SQLite WAL/FULL mode changes share the worker transaction/lock with acquire and
consume. Offline/draining invalidate unconsumed peer grants; paused invalidates
all unconsumed grants. The persisted peer-origin bit prevents handing a peer
token to localhost to evade the mode. Existing running/quarantined charges
remain. Only unconsumed reservations expire. Local mode is checked before
discovery exchange, replica traffic, remote acquisition and execution; stale
advertisements cannot grant work. Already-started network operations may finish
after the transition; they cannot bypass the next execution/admission check.
Uncertain remote origin claims remain pinned rather than silently redirected.

Manual modes survive restart. A non-auto mode may have `duration_seconds` or
`expires_at`, at most seven days, mutually exclusive. Expiry returns to `auto`
before admission, including after restart. It never forces a busy owner or an
unknown idle sensor eligible. Preference updates use `expected_version` and a
typed `values` patch, validate combined thresholds, commit before live application,
and reset idle recovery hysteresis. Only explicit patches persist preference
overrides; enrollment/maintenance opens do not freeze their default policies over
the daemon's later environment configuration. Saved owner preferences take
precedence over environment values for their supported fields.

## Actual local API

| Operation | Scope and behavior |
|---|---|
| `GET /v1/node/status` | Read; mode, worker-local running/reserved/quarantined slots, origin job counts, resources, backend inventory and peer reachability |
| `GET /v1/node/jobs` | Read; redacted origin summaries and worker leases; no prompts, results, tokens or request IDs |
| `GET /v1/node/events` | Read; version 1 SSE envelope, opaque ID, local scope and redacted snapshot data |
| `PUT /v1/node/mode` | Control; `{mode, duration_seconds?}` or `{mode, expires_at?}`; returns persisted mode/revision/deadline |
| `GET /v1/node/diagnostics` | Read; service/discovery and quarantine indicators |
| `GET/PATCH /v1/node/preferences` | Read/control; `{version, values}` and `{expected_version, values}` |
| `POST /v1/node/jobs/{id}/cancel` | Control; origin ownership required, best effort with truthful unknown outcomes |
| `DELETE /v1/jobs/{id}` | Existing cancel surface now also requires the control credential |
| `GET/PUT /v1/node/service` | Read/control; macOS next-login service configuration, unsupported changes return 501 elsewhere |

Fresh authenticated SSE subscriptions always start with a snapshot, even with
`Last-Event-ID`. Subsequent one-second samples provide mode/job/capacity/idle/peer
events or heartbeat snapshots; no durable event replay or complete change history
is promised. Three-second client polling remains active if SSE is unavailable,
and a five-second snapshot-age watchdog clears the live indicator even if a
request/connection is stalled. Client and server disable status caching. A failed or
invalid status request removes the live indicator and job count until a valid
fresh snapshot is received. A failed management request never optimistically
changes the displayed mode; control failures remain visible until dismissed or
another action is attempted. No animation is needed to understand status; labels
and job-count hiding support accessibility/privacy. Physical VoiceOver and
keyboard acceptance remain unverified.

The private data directory is POSIX 0700 and credentials are 0600, distinct
random read/control bearer secrets, unrelated to peer TLS credentials. Startup
rejects insecure credentials, symlinks and wrong owners where supported. The
peer RPC listener exposes no local control routes. Host/client loopback checks,
scopes and Origin/Fetch checks apply independently. The browser dashboard may
perform same-origin authenticated reads; browser management writes and
cross-origin requests are refused. Tokens grant their scopes to anyone who can
read them: this does not isolate malicious processes running as the same OS user.
Protect Windows per-user ACLs; actual Windows ACL/hardware execution is unverified.
Credential rotation is an offline operation: stop the daemon, replace its private
credential file, restart and reimport into Keychain; existing subscriptions end
with daemon shutdown. Live rotation/revocation is not implemented.

## Evidence and remaining acceptance

Automated tests cover scope/CSRF denial, peer-listener isolation, redaction,
credential files, persisted mode/preference changes and conflicts, failed commits,
expiry while the owner is active, mode/consume interleavings, stale signed peer
requests, local inference offline, SSE resynchronization and mocked launchd.
Native processes with fabricated backends run two jobs, close a UI HTTP client,
retain both executions, finish them, infer locally offline and restart with saved
mode/preferences. Existing N=1/2/3 survivor, partition and fencing tests remain
applicable. See [exact commands and results](implementation-status.md).

Swift builds and the Foundation mock-client smoke test runs. XCTest client/UI
sources and an Xcode project/shared test scheme are provided. This host's Command
Line Tools lack XCTest; project/scheme syntax was checked, but its Xcode build and
UI test compilation/execution remain unverified. Actual app clicks/Quit UI, Keychain prompts,
login/logout, launchd supervision, sleep/wake, network roam, signing/notarization,
real inference hardware and Linux/Windows services remain unverified. No user
service was installed, daemon deployed, or release published during this work.
No Raft or Docker/Compose integration was introduced. Next: Phase 7 model sharing.
