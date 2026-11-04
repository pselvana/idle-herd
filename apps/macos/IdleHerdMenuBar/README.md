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
CPU/GPU usage and memory use bounded precision and explicit units; power is labeled
as adapter or battery. Unsupported metrics appear as **Not reported**, with help
explaining sensor limitations. Missing values never imply zero usage or eligibility.
Total host memory comes from the sensor even when Ollama is offline. Failed backend
probes display Offline, with a recovery hint. Remote capacity is zero with no
reachable peers, and remains unavailable until every reachable peer has a fresh
authenticated observation. It is still an advisory estimate, not admission.

The header, participation controls and footer stay visible while scrolling. The
current mode has a checkmark; timed modes show their return time. Origin history
and device details are expandable, with this Mac identified separately. Diagnostics,
manual refresh and Quit UI are in **More actions** (the ellipsis menu). Empty model
inventory means no shareable models were found, not that no models are loaded.
Phase 7 displays model inventory, transfer
progress and reasons with authenticated pause/copy/retry. Copy and retry require
explicit permission and license acceptance; progress updates through snapshots and
SSE. See [model sharing](../../../docs/idle-herd/phase-7.md). Login launch
uses macOS Service Management and may require a signed installed app.

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
