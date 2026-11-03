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
Unsupported metrics appear as unknown. Phase 7 displays model inventory, transfer
progress and reasons with authenticated pause/copy/retry. Copy and retry require
explicit permission and license acceptance; progress updates through snapshots and
SSE. See [model sharing](../../../docs/idle-herd/phase-7.md). Login launch
uses macOS Service Management and may require a signed installed app.

`ClientSmoke` is a Foundation-only mock transport check for authenticated
snapshot, disconnect, and recovery: `swift run ClientSmoke`. The included Xcode project and shared scheme configure the app and UI test
host. On a Mac with full Xcode, run:

```sh
xcodebuild -project IdleHerdMenuBar.xcodeproj -scheme IdleHerdMenuBar -destination 'platform=macOS' test
```

The UI test uses a debug-only in-process mock selected by
`IDLE_HERD_UI_TESTING=1`; it does not verify real daemon continuity. The Xcode
project and UI test have not been built or run on this Command Line Tools-only
host.

Open Dashboard opens the local read-only dashboard. Paste the daemon's read
credential into the dashboard when prompted; it stays in the page's memory and
is not placed in the URL. Closing the page discards it.
