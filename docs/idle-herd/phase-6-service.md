# Phase 6 local CLI and macOS user service

The daemon remains the owner of jobs and inference when the menu bar app quits.
On macOS, install from an environment with a stable Python interpreter and
installed Idle-Herd package. `idle-herd service install` writes a private user
LaunchAgent plist with that interpreter, the current working directory, and effective
peer settings;
it does not register or start anything. `idle-herd service enable` enables login
launch and bootstraps it in the current GUI session. `idle-herd start` (or
`idle-herd service start`) starts a registered service; `idle-herd service status`
shows launchd state. `idle-herd service disable` disables login launch and unloads
the service. Disabling stops the daemon; existing backend execution may need
reconciliation after an unclean shutdown. Service operations are macOS-only; the
CLI and foreground daemon remain available on other platforms. Changing executable,
working directory or effective peer settings requires reinstalling the plist. These commands
never manage Ollama or other backend services.

`idle-herd status`, `jobs`, `doctor`, and `preferences` read the authenticated
local API. `offline --for 1h`, `auto`, `drain`, and `pause` use the same scoped
control API as the app; omit `--for` for persistent offline mode. Preferences can
be changed with `preferences --set '{"require_ac":true}' --expected-version N`,
using the version from a fresh read. CLI reads the daemon-provisioned credentials
from its configured data directory, uses a fixed loopback endpoint, and does not put
tokens in URLs. Service installation and launchd operations are explicit CLI actions.

The automated service tests mock launchctl and do not register a real user service.
Login/logout, sleep/wake, and actual launchd lifecycle still need native macOS
integration verification.
