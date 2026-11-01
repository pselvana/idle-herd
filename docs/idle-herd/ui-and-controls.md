# Local UI and controls: planned contract

**Phase 0 design only.** No SwiftUI app, peer mode API or event stream is claimed to exist. The future macOS `MenuBarExtra` is an optional client of the daemon's versioned authenticated loopback management API. CLI and UI share this API; the daemon owns policy, persistence, sensor observation, reservations and job lifecycle. Quitting or crashing the UI leaves daemon and work running. Windows/Linux retain CLI/API parity; tray clients are optional later work.

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

The proposed local status, jobs, events, mode, cancel and diagnostics interfaces use scoped credentials stored in a platform credential store and loopback-only management; loopback alone is not authorization. Authenticate subscriptions and writes, enforce scopes and origin/CSRF protections, and never place credentials in URLs. Privileged control commands must be cryptographically authenticated; scoped bearer credentials are the current API draft, while provisioning, rotation and replay defenses require review before implementation. No prompt, completion, secret or private path belongs in default status events or the menu. The [OpenAPI draft](openapi.yaml) defines the canonical event envelope: `version`, `id` (matching SSE `id`), `type`, `scope`, `observed_at` and redacted typed `data`. Node identity is in local snapshot data. Event types include job, mode, capacity, idle, transfer and peer-reachability changes. Reconnect obtains a fresh authoritative local snapshot; a disconnected daemon must never appear live based on stale cached state. See [ADR 005](adr-005-daemon-ui-and-native-sequence.md) for service ownership and native sequencing.

Future acceptance: toggle offline during a remote reservation and verify immediate rejection while existing work drains and localhost inference continues. Paused rejects new local inference. Quit UI during two jobs and verify daemon/CLI continuity. Restart daemon and confirm disconnected/reconnecting state and authenticated snapshot recovery. Expire offline while the owner is active and verify auto remains ineligible for idle-only work. Reject unauthenticated and cross-origin mode writes. Verify accessible labels and reduced-motion behavior without relying on color or animation; inspect status/events/logs for sensitive content.
