# ADR 002: Worker-owned fenced leases and advisory capacity

**Status: Phases 1–2 local ledger and authenticated remote admission implemented;
idle policy and blocking waits remain planned.** See [Phase 2](phase-2.md),
[recovery](phase-1.md) and
[verification](implementation-status.md). Native mock processes verify commit,
contention and restart fencing. Uncertain backend execution is quarantined;
only an offline, operator-confirmed backend stop permits reclamation. This is
conservative reconciliation, not automatic request-specific backend cancellation.

Each worker serializes live admission against its own persistent ledger and weighted resource budget, including local and remote requests. It checks model/backend health, current idle policy, memory and concurrency before issuing an opaque authenticated, bounded-TTL token. The persisted lease binds cluster ID, worker ID and incarnation, slot ID and monotonically persisted generation, request ID and reservation identity. Commit before acknowledging; persist per-worker request idempotency. Consume once under the same local authority; renew only with worker approval. A worker may atomically reserve multiple of its own slots, but cross-worker atomic allocation is deferred.

A restart changes incarnation and rejects old tokens. Reconcile running backend work and orphaned reservations before freeing or reissuing capacity: an expired lease alone does not prove a backend stopped. An unreachable worker cannot be declared released from an origin cache. A copied ledger is not authority for a second worker while the original may run. These rules protect resource capacity on each worker, not global uniqueness of a job.

Authenticated snapshots are candidate-ranking hints, never grants. Capacity reads identify scope, timestamp, freshness, reachable/stale/offline peers and uncertainty; omit stale or unreachable peers from available totals. A bounded origin-local fair waiter retries live admission on release, idle transitions, model readiness, peer join and backoff; it honors cancellation and deadline. The default wait is 60 seconds and the draft protocol maximum is 120 seconds; a deployment may configure a lower limit, but increasing the protocol maximum requires contract review; long-lived work uses the future durable job API. Waiter lifetime is tied to its origin. There is no global fair queue.

Future acceptance: race 100 clients from two origins for one worker slot and assert at most one admission, including lost replies and retries. Crash between ledger commit and reply and during execution; restart with an active backend and prove no reuse until reconciled, then reject old-incarnation and old-generation tokens. A stale idle snapshot must not override an active, offline or draining worker. Exercise waits through release, joining, cancellation, missed events and timeout; offline peers cannot inflate reported availability.
