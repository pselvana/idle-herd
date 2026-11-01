# ADR 005: Native daemon authority and optional local UI

**Status: proposed Phase 0 decision; runtime implementation deferred.**

The daemon owns worker admission, identity, journals, preferences, mode and host
sensors. Native service supervision owns its lifetime: launchd on macOS, systemd
on Linux, and an appropriately scoped Windows service or scheduled task. A future
SwiftUI MenuBarExtra app is a replaceable local client. UI startup, exit, crash,
login and logout must not implicitly terminate daemon work or create a second
scheduler. Sensor observation cannot depend on a visible UI.

CLI and UI use the same versioned, scoped, authenticated loopback control API.
Read credentials cannot change policy; peer credentials cannot invoke owner
controls. Loopback alone does not authorize callers. The daemon confirms a
transition before the UI displays success. See [controls](ui-and-controls.md) and
[security](security.md).

`offline` withdraws discovery, inbound cluster admission and outbound dispatch;
localhost inference stays available. `paused` stops all new inference. `draining`
stops new remote admissions but permits local requests and outbound dispatch.
Existing execution finishes by default, with explicit cancellation separate.
Apply transitions under local admission serialization before advertising them,
deny or invalidate unconsumed reservations made ineligible by the new mode:
peer reservations for offline/draining, and local plus peer reservations for
paused. Token consumption uses the same serialization and rechecks mode, so a
previously issued token cannot bypass pause. Retain ownership of running slots
until backend completion is confirmed. Persist manual offline across restart;
timed overrides return to policy-based auto, never forced availability.

Native macOS/Linux/Windows execution, sensors and service lifecycle come first.
Docker, Compose, OCI/DMR integration and container-specific tests are **Phase 10**.
Keep backend interfaces portable without adding early container dependencies.
Do not reassign the existing port map; listener allocation for future authenticated
peer RPC remains an explicit implementation decision.

Future acceptance: quit/crash the UI during two jobs and verify daemon/CLI
continuity; restart the daemon and show a disconnected UI until an authenticated
snapshot arrives; race offline against lease consumption; expire offline while
the owner is active and remain ineligible for idle-only remote work. Native service
restart, sleep/wake and login/logout tests require evidence on each target OS.
