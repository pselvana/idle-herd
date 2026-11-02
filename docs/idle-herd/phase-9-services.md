# Native user services

Install Idle-Herd in a stable Python environment and run `idle-herd service install`
from the working directory you want the daemon to use. This writes configuration
containing the effective peer settings and an absolute interpreter path; it does not
register or start the daemon. Reinstall after changing settings or moving the Python
environment or working directory. Keep the service configuration and data directory
private to the owner; the Windows launcher inherits the containing directory's ACL,
which must restrict access to that user. Settings may contain sensitive local paths.

Run `idle-herd service enable` to register login startup. On macOS this bootstraps a
LaunchAgent, and on Linux it enables and starts a systemd user unit. On Windows it
registers and starts an interactive, least-privilege Task Scheduler logon task. `idle-herd service status` queries the native
manager, and `idle-herd service disable` disables login startup and stops the daemon.
Stopping the daemon can leave backend work needing reconciliation; it does not stop or
manage Ollama. The optional desktop UI does not own the daemon. The authenticated local service control API can change login startup on all three
platforms without stopping the running daemon. Windows registers a missing task
without replacing an existing task. Service status may be unknown when the manager
is unavailable or returns an unrecognized or localized response. CLI commands work
independently of the API.

The Linux unit is at `~/.config/systemd/user/idle-herd.service` and requires a running
systemd user manager. Survival after logout depends on the host's user-manager and
linger configuration; this command does not change linger. Windows uses an account-scoped task
name and a launcher under `%USERPROFILE%\AppData\Local\IdleHerd\service` with the
current interactive user's permissions. Existing tasks with that account-scoped name are replaced
on enable. Review the launcher's inherited ACL before enabling. Neither platform
requires administrator or system service installation.

Automated tests inspect generated configuration and mock manager commands only.
Actual Linux and Windows task registration, login/logout, restart, sleep/wake,
network changes and permissions have not been verified on native hosts. No service
was installed on the development host.
