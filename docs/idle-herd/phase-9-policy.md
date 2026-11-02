# Phase 9 worker policy controls

Preferences are owner-authenticated, versioned and persisted in the worker ledger.
Existing `require_ac` and `battery_floor_percent` use the host power observation;
unknown AC cannot satisfy AC-only. No ungrounded power estimate is used.

`quiet_hours_enabled` blocks new peer-origin reservations and their consumption.
`quiet_hours_start_minute` and `quiet_hours_end_minute` are minutes after midnight
in the worker host's current local timezone. Start is inclusive, end exclusive;
intervals can cross midnight, and equal endpoints mean all day. Clock and timezone
changes take effect on the next admission. Local inference is unaffected. A
fresh sensor observation showing owner activity also blocks peer-origin admission
and consumption, regardless of optimistic configured policy. Unknown activity
never grants `idle_only`; explicit `always` and `utilization_only` retain their
existing treatment of unknown activity.

`peer_max_concurrent` limits each authenticated originating peer separately,
from zero to 128 (default 128). Reserved, running and quarantined leases count.
The worker checks the limit within the serialized durable transaction at grant
and again when a reservation is consumed. Lowering the limit can block previously
issued reservations until enough occupied slots are released; it never cancels
running work or frees quarantine. Expiry releases only unconsumed reservations.
The shared worker and per-model limits still apply. This is a concurrency limit,
not a tokens or time budget. Capacity advertisements are advisory and do not
include individual peers' occupancy; a zero limit blocks all peer capacity.

`notifications_enabled` and `hide_job_counts` default to false and are owner
preferences; no notification permission is implied by storing the setting.
