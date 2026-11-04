# Phase 3: native idle policy and admission

`idle-herd` samples host activity and resources in the daemon, independently of
any future UI. Worker-local policy gates new reservations and rechecks the
persisted policy at consumption. A peer request is always classified as
background by the receiving worker, even if its origin called it interactive.
Capacity and eligibility are observations, not grants: the worker must still
admit each request. A change to draining eligibility rejects new work; it does
not cancel running execution or release its lease. Phase 1's quarantine and
recovery rules still apply.

Local inference defaults to interactive. Clients may set
`X-Idle-Herd-Workload: background` and/or `X-Idle-Herd-Idle-Only: true` on
localhost inference requests. The latter tightens `always` or
`utilization_only` to `idle_only`; it cannot override `manual` or `never`.
`POST /v1/capacity/acquire` accepts `workload` and `idle_only` with the same
defaults (`interactive`, `false`). Both fields are persisted in the lease and
are part of idempotency matching. The worker rechecks them before execution.
Stored and caller-tightened requirements use the same live observation; request
headers cannot weaken a lease's recorded requirements. Deadlines are checked
again after awaited resource and policy checks.

Upgrade origins and receiving workers together: Phase 3 direct dispatch requires
background/idle-only fields in peer grants. Older Phase 2 peers do not negotiate
this policy contract, so mixed-phase remote dispatch may be refused. Independent
localhost inference remains available; no downgrade bypasses background policy.

`GET /v1/node/eligibility?workload=background` returns a local read-only
eligibility decision, including policy, reason, activity, timestamped signal,
source and confidence. `GET /v1/cluster/capacity?model=...` also accepts
`workload` and `idle_only` query parameters and includes the local decision.
Neither endpoint promises cluster-wide capacity. A daemon-local condition hook
can wake retry loops on eligibility changes, but there is no public blocking
capacity API or waiter fairness yet; those are Phase 4 work.

## Policy settings

These environment variables use the `IDLE_HERD_` prefix. Policies are static
configuration in this phase; there are no mode or owner approval controls yet.
`manual` therefore refuses admission until authenticated controls are available
in Phase 6. `never` refuses admission. `always` still applies freshness, memory,
thermal and power safeguards. `idle_only` additionally requires known idle
human activity and utilization; `utilization_only` requires utilization but
not a human-idle signal. For a native headless host with no trustworthy activity
signal, explicitly set `IDLE_HERD_BACKGROUND_POLICY=utilization_only`.

A local Mac on battery below the configured floor cannot serve a second
concurrent request even with `interactive_policy=always`. If its peer's only
slot is occupied, ordinary inference returns 429 immediately. Origin dispatch
reports the authenticated worker reason (for example `busy`), includes the local
refusal as `error.local_reason` (for example `battery_low`), and supplies
`Retry-After: 1`. Connecting AC can restore local eligibility; use Phase 4's
explicit capacity reservation API for a bounded wait instead of immediate refusal.

| Variable | Default | Meaning |
| --- | --- | --- |
| `IDLE_HERD_INTERACTIVE_POLICY` | `always` | Interactive admission policy. |
| `IDLE_HERD_BACKGROUND_POLICY` | `idle_only` | Background admission policy. |
| `IDLE_HERD_IDLE_SECONDS` | `120` | Minimum observed human idle time in seconds. |
| `IDLE_HERD_IDLE_HYSTERESIS_SECONDS` | `15` | Continuous recovery interval before idle/utilization policies become eligible. |
| `IDLE_HERD_CPU_BUSY_PERCENT` | `25` | CPU utilization at or above which those policies drain. |
| `IDLE_HERD_CPU_IDLE_PERCENT` | `20` | Maximum CPU utilization for recovery; must not exceed busy threshold. |
| `IDLE_HERD_GPU_BUSY_PERCENT` | `25` | GPU busy threshold when a measurement is available. |
| `IDLE_HERD_MEMORY_ADMISSION_MODE` | `backend` | Backend-managed model fit; `strict` enforces estimated memory budgets. |
| `IDLE_HERD_RESERVE_MEMORY_GB` | `2` | Available-memory floor/host reserve, enforced in strict memory mode. |
| `IDLE_HERD_BATTERY_FLOOR_PERCENT` | `30` | Battery floor when not known to be on AC and battery percentage is available. |
| `IDLE_HERD_REQUIRE_AC` | `false` | Require a positive AC observation. |
| `IDLE_HERD_SENSOR_MAX_AGE` | `5` | Maximum observation age in seconds. |
| `IDLE_HERD_SENSOR_INTERVAL` | `2` | Background sampling interval in seconds; must be less than maximum age. |
| `IDLE_HERD_SENSOR_HELPER` | unset | Absolute path to a trusted native host-wide activity helper. |

A stale observation blocks admission. A failed sensor reports unknown activity
and resources; unknown activity cannot grant `idle_only`, and missing CPU or
available memory cannot grant either
`idle_only` or `utilization_only`. Critical memory pressure, serious/critical
thermal readings and configured power limits block admission. In default `backend`
memory mode, warning pressure and low available RAM are advisory: Ollama manages
actual model fit/loading and the OS can reclaim caches. Strict memory mode also
blocks warning pressure and enforces the host RAM reserve. Unknown optional thermal, GPU, battery and pressure readings
are not themselves proof of safety; they are reported as unknown. Long sample
gaps reset the recovery window. These checks are best-effort observations, not
a guarantee against resource changes immediately after a check.

Native sampling uses macOS IOHID activity and Linux logind session observations
where available; Linux requires usable observations for all listed sessions.
On Windows, the native `GetLastInputInfo` reading covers only the daemon's
session and is diagnostic only: its confidence remains unknown and it cannot
grant `idle_only`. No host-wide Windows helper is bundled. A trusted external
helper may attest host-wide activity; otherwise headless Windows can explicitly
use `utilization_only`. Real Windows activity behavior has not been validated.
Resource readings are platform dependent. The first CPU sample may be unknown
until a second sample supplies a delta. An explicitly configured helper is a
trust boundary: it must report host-wide activity, implement the version 1 JSON
protocol (`--idle-herd-sensor-v1`: `version`, `observed_at`, `idle_seconds`,
`session_locked`), and return fresh valid output. A helper reporting only its
own session is insufficient. Failure, malformed output or stale timestamps
fail closed for activity. No raw input events or session names are exposed.

Protect the helper executable and its parent directories with owner permissions.
It receives the single fixed argument `--idle-herd-sensor-v1` and must emit, for
example, `{"version":1,"observed_at":1793520000.0,"idle_seconds":180.0,"session_locked":false}`
with the actual current Unix observation time. All four fields are required;
`idle_seconds` and `session_locked` may be null. Null idle duration, duplicate or
extra fields, nonfinite values, future times, or times older than five seconds
cannot grant activity confidence. Output is limited to 64 KiB and two seconds;
the complete native sample has a three-second deadline. Late resource reads
cannot update CPU state or queue unbounded sampling threads.

macOS lock state and thermal state remain unknown without a richer sensor;
IOHID reports aggregate input idleness. Linux logind depends on desktop/session
software publishing meaningful hints, and requires stable session enumeration.
Linux memory pressure is an available-memory estimate; temperature-derived
limits on Linux/Windows use available sensor readings rather than OS-wide thermal
pressure. Missing GPU/thermal readings remain unknown. A failed entire sensor
cannot establish resource safeguards: `always` still permits local interactive
work subject to live backend/model checks and the configured memory mode. Operators
requiring measured utilization should select `utilization_only` explicitly.

No native service installer or UI is included in this phase. Sensor and policy
tests use deterministic mocks; real host hardware and activity integration have
not been validated here. See [implementation status](implementation-status.md)
for exact evidence, [Phase 1](phase-1.md) for backend recovery and
[Phase 2](phase-2.md) for peer security. Containers remain Phase 10; no Raft
or model sharing is introduced here.
