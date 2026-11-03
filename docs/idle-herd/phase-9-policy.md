# Phase 9 worker policy controls

Preferences are owner-authenticated, versioned and persisted in the worker ledger.
Existing `require_ac` and `battery_floor_percent` use the host power observation;
unknown AC cannot satisfy AC-only. No ungrounded power estimate is used.

`quiet_hours_enabled` blocks new peer-origin reservations and their consumption.
`quiet_hours_start_minute` and `quiet_hours_end_minute` are minutes after midnight
in the worker host's current local timezone. Start is inclusive, end exclusive;
intervals can cross midnight, and equal endpoints mean all day. Clock and timezone
changes take effect on the next admission. Local inference is unaffected. A
fresh sensor observation showing owner activity blocks peer-origin admission
and consumption when the effective policy is `idle_only`. Explicit `always`
permits work while the owner is active, subject to other safeguards;
`utilization_only` permits active-owner work when utilization is low. Requests
that explicitly require idle-only work still require known idle activity.
Unknown activity never grants `idle_only` work.

In the macOS menu bar, open **Preferences… → Background policy**, select
**Always (subject to resource limits)**, and click **Save daemon policy**. Keep
the main participation mode set to **Auto**. Repeat on each node that should
accept peer work while its owner is using it. Saved policy applies immediately
and persists across restart. Quiet hours, power/resource safeguards and peer
concurrency limits remain independent controls. Older daemons with the unconditional
active-owner veto must be updated and restarted once; no menu bar rebuild is
needed for this daemon-side correction.

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
