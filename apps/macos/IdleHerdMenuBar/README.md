# Idle-Herd menu bar

Requires macOS 13 or later, Swift 5.9 or later, and the full Xcode installation
with its SwiftUI macro plugins. The standalone Command Line Tools package is not
sufficient to build this SwiftUI app. If `xcodebuild -version` fails, install
Xcode and select its developer directory with
`sudo xcode-select --switch /Applications/Xcode.app/Contents/Developer`.
Run `swift test` and
`scripts/package.sh`, then open `build/Idle-Herd.app`. The package script
creates an unsigned local development app; distribution signing and notarization
are not provided. Start the daemon separately. In Preferences, import the
owner-only `~/.idle-herd/control-credentials.json` file (or the corresponding
file in `IDLE_HERD_DATA_DIR`) into Keychain. The app never starts or stops the
daemon. The app connects only to `127.0.0.1:11435`.

The UI polls status alongside the event stream, and shows disconnected/stale
when the daemon cannot be reached. The displayed cluster view is an estimate.
The menu bar uses two llamas shoulder to shoulder. The left fills for local-origin
work executing on this Mac. With remote hosts connected, the right is outlined
when idle, fills gray for this Mac’s work executing remotely, and fills with the
normal menu bar tint for peer-origin work executing on this Mac. Incoming work
takes visual priority if both directions are active; accessibility describes both.
With no reachable remote hosts, the right llama is removed and the icon shrinks
from 31 to 15 points wide. Stale/disconnected status also shows one outlined llama.
Reservations and quarantines do not light the icon. Template artwork follows the
system menu bar tint, with half-opacity gray for outgoing work.

Updated daemons expose `outbound_running` in authenticated status/events to observe
ordinary forwarded API requests and durable jobs while execution under a signed
worker grant is in flight, including the wait for non-streaming response headers.
Completion, rejection, errors and cancellation clear that observation without
changing worker leases or quarantine. Restart the
daemon when safe and reopen the rebuilt app to enable this addition. Older daemons
fall back to fresh journaled jobs with explicit local-origin and remote-worker
identities; their ordinary forwarded API requests have no activity observation.
CPU/GPU usage and memory use bounded precision and explicit units; power is labeled
as adapter or battery. Unsupported metrics appear as **Not reported**, with help
explaining sensor limitations. Missing values never imply zero usage or eligibility.
Total host memory comes from the sensor even when Ollama is offline. Failed backend
probes display Offline, with a recovery hint. Remote capacity is zero with no
reachable peers, and remains unavailable until every reachable peer has a fresh
authenticated observation. It is still an advisory estimate, not admission.

The panel uses a connection header, an active-inference card and a cluster topology
with separate cards for this Mac and each peer. It adapts to light and dark macOS
appearances. Running leases show the model, originating host and elapsed lease age;
the blue bar measures **local slot occupancy**, not inference completion. Token
speed and completion percentage are not claimed without backend measurements.
Disconnected views label cached jobs and peer observations as last received/seen.

The header, participation controls and footer stay visible while scrolling. The
current mode has a checkmark; timed modes show their return time. **Pause for 1 hr**
requires confirmation, then returns to Automatic through the daemon's existing mode
expiry. The header mode menu also offers timed offline mode. Resources/backend,
origin history, peer models and sharing details are expandable. Diagnostics,
manual refresh and Quit UI are in **More actions** (the ellipsis menu). Empty model
inventory means no shareable models were found, not that no models are loaded.
Phase 7 displays model inventory, transfer
progress and reasons with authenticated pause/copy/retry. Copy and retry require
explicit permission and license acceptance; progress updates through snapshots and
SSE. See [model sharing](../../../docs/idle-herd/phase-7.md). Login launch
uses macOS Service Management and may require a signed installed app.

**Copy model from peer** opens a light/dark dialog matching Preferences. Choose a
reachable peer by hostname, refresh its authorized model list, and select an offer
to review license/provenance and confirm consent. Model digests and peer identities
are filled internally. Changing the selection or refreshing requires fresh consent;
retry is limited to the original model revision. Empty catalogs, unreachable peers
and failed starts have distinct states. Reopen the rebuilt app and update/restart
the daemon when safe to enable the peer-specific inventory request. Models must
still be registered and explicitly shared by the source owner.

Preferences opens on demand in a separate window with **General**, **Daemon
Policy**, **Power & Limits**, and **Security & RPC** tabs. Grouped cards adapt to
light/dark appearance, while Close, Revert changes, Save Daemon Policy and status
messages stay visible below the scrolling content. Quiet hours use 24-hour `HH:mm`
inputs in this host’s local time; matching start/end times block peer work all day.
App visibility and login options apply immediately; daemon policy edits require
Save. Revert restores the last loaded policy without changing daemon defaults.
Saving validates numeric ranges and submits only changed fields under the loaded
revision. Failed saves retain edits; a revision conflict requires Reload policy
before saving again. Credential import remains in Keychain under Security & RPC.
The dialog does not configure peer trust, job kill deadlines or heartbeat timeouts.

Job rows show **From hostname**, such as `From pstudio.local`, instead of a generic
peer-origin label. Older peers fall back to endpoint IP or worker identity.
Quarantined rows distinguish a bounded wait for original-backend completion from
unknown outcomes requiring stopped-backend recovery. The daemon defaults to a
30-second recovery window; it releases a slot only when that original response
confirms completion, never just because time elapsed. Already orphaned execution
still requires [offline reconciliation](../../../docs/idle-herd/phase-1.md#durable-authority-and-recovery).
Update/restart each daemon and reopen the rebuilt app to use these additions.

`ClientSmoke` is a Foundation-only mock transport check for authenticated
snapshot, disconnect, and recovery: `swift run ClientSmoke`. The included Xcode project and shared scheme configure the app and UI test
host. On a Mac with full Xcode, run:

```sh
xcodebuild -project IdleHerdMenuBar.xcodeproj -scheme IdleHerdMenuBar -destination 'platform=macOS' ONLY_ACTIVE_ARCH=YES CODE_SIGN_IDENTITY=- test
```

The UI test uses a debug-only in-process mock selected by
`IDLE_HERD_UI_TESTING=1`; it does not verify real daemon continuity. The native app
target is named `Idle-Herd` to distinguish it from the Swift package executable.
On November 3, 2026 the app and UI tests built with Xcode 16.2 on ARM64, but UI
execution timed out enabling macOS automation mode. Swift package tests passed;
this does not establish GUI interaction or VoiceOver acceptance.

Open Dashboard opens the local read-only dashboard. Paste the daemon's read
credential into the dashboard when prompted; it stays in the page's memory and
is not placed in the URL. Closing the page discards it.
