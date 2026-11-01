# Local UI and controls: Phase 6 contract

Phase 6 implements the optional macOS `MenuBarExtra`, authenticated peer-local
mode API, preferences and redacted SSE. See [runtime operation and limits](phase-6.md)
and [test evidence](implementation-status.md). CLI and UI share the versioned
loopback API; the daemon owns policy, persistence, sensors, reservations and jobs.
UI lifetime is independent. Windows/Linux retain CLI/API parity; tray clients
remain optional. The layout below is the product mock, not a hardware screenshot.

| Mode | New remote admissions | Local-origin dispatch to peers | New local interactive inference | Existing work |
|---|---|---|---|---|
| `auto` | Per current policy | Yes | Yes | Normal policy |
| `draining` | No | Yes | Yes | Finish by default |
| `offline` | No; withdraw and refuse despite stale advertisements | No | Yes, localhost only | Drain by default |
| `paused` | No | No | No new inference | Finish by default |

Apply mode locally before advertising it, under the same serialization as token consumption. Deny or invalidate unconsumed reservations made ineligible: peer reservations for offline/draining, local and peer reservations for paused. Already-running work retains its slots until confirmed complete; previously issued tokens cannot bypass pause. Cancellation is separate and explicit. Persist manual offline across service restarts by default; a temporary override expires back to `auto`, which still requires live idle/resource checks. Status must separately show service connectivity, owner activity, policy eligibility and mode. The local active count is authoritative; cluster totals are reachability- and freshness-labeled estimates.

Illustrative menu layout:

```text
Idle-Herd · 2 jobs
Auto · Idle 18m · Accepting work
Local: 2 running · 1 queued · 1 reserved
Resources: CPU 18% · memory 24/36 GB · slots 1/4
Cluster (reachable estimate): 2 reachable · 1 stale · ~4 remote slots
[Auto] [Drain] [Offline] [Pause] [Offline for 1 hour]
[Jobs] [Models] [Dashboard] [Diagnostics] [Quit UI]
```

The local status, jobs, events, mode, cancel and diagnostics interfaces require
scoped bearer credentials; loopback alone does not authorize them. The daemon
provisions protected distinct read/control credentials and the macOS client imports
control into Keychain. CLI reads enforce the same file ownership/link/permission
checks. Origin/Fetch defenses reject browser control writes and cross-origin
reads. Secrets never belong in URLs. Rotation currently requires a stopped daemon
and credential reimport; live revocation is future work. No prompt, completion,
secret or private path belongs in default status/events. The version 1 envelope
uses `version`, `id` (matching SSE ID), `type`, `scope`, `observed_at` and typed
redacted `data`; runtime `/openapi.json` describes actual fields, while the YAML
retains future extensions. Events include job, mode, capacity, idle and peer
changes. Model transfers remain Phase 7. Reconnect obtains a fresh snapshot;
a disconnected daemon never appears live from cached state. See
[ADR 005](adr-005-daemon-ui-and-native-sequence.md) for service ownership.

Automated acceptance covers stale peer reservations during mode changes,
offline local inference, pause, timed expiry while the owner is active, service
restart persistence, scope/CSRF rejection and redaction. Native tests disconnect
a UI HTTP client during two jobs and verify daemon continuity. Swift builds and
a mocked status/disconnect/recovery client smoke test runs. Actual menu clicks,
physical Quit UI, VoiceOver, Keychain and OS login/sleep lifecycle need a full
Xcode/hardware acceptance run; XCTest sources are not evidence of execution.
