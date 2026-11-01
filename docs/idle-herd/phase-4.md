# Phase 4: capacity visibility and bounded acquisition

Each origin provides an advisory capacity view and bounded, origin-local waiters.
The worker remains the only authority that can persist and grant its slot; neither
peer discovery nor an optional quorum grants capacity. See [Phase 1](phase-1.md)
for fencing and recovery and [Phase 3](phase-3.md) for worker-local idle policy.

## Capacity view

`GET /v1/cluster/capacity?model=...&workload=interactive&idle_only=false`
includes the local worker and known peers. It reports known, reachable, stale and
offline counts, per-peer freshness, eligible workers, observed eligible slots and
an observation time. Discovery-disabled single-node operation includes the local
worker. Peer capacity is queried live using signed peer RPC, bounded to five
seconds, and responses must be bound to the discovered worker, incarnation and
model and have a timestamp within the discovery TTL. Remote policy is evaluated as
background work. Stale, unreachable or failed responses contribute no available
slots. `busy` is false when available slots are observed; it is true only when the
known view is complete, at least one worker is eligible and no slots are observed;
otherwise it is null. A complete *known* view does not prove discovery found every
peer. This endpoint is a point-in-time estimate, not a reservation.

## Acquisition and lifetime

`POST /v1/capacity/acquire` accepts `model`, optional `request_id` and
`idempotency_key`, `wait_ms` (default 60000, maximum 120000, zero for
nonblocking), `ttl_ms` (default 30000, range 100–120000), `workload` (default
`interactive`), `idle_only` (default false), and `slots` (only 1). Model names are
normalized. Unknown fields, including `context_tokens`, are rejected. A successful
201 response is the worker's `Lease`, including its opaque `token`, worker and
incarnation, slot and generation, request identity and expiry. Pass that token in
`X-Idle-Herd-Reservation` for inference, inspection, and release. `GET` and
`DELETE /v1/capacity/reservations/{reservation_id}` consult the issuing worker;
an unreachable worker cannot be declared released from an origin cache.

The origin maintains bounded, locally fair retry rounds and retries live worker
admission after wakeups and periodic backoff. Local settings bound waiter count;
`IDLE_HERD_MAX_WAITERS` defaults to 256 (range 1–10000). Retry fallback is 250 ms;
local lease commits, discovery changes, inventory refresh and policy revisions
wake retries sooner. Slow remote attempts do not hold up unrelated waiters.
There is no global queue or global ordering. Nonblocking contention can return
`429 no_capacity`, an elapsed wait returns `504 capacity_wait_timeout`, and
unavailable or inconclusive workers can return `503 no_reachable_worker` or
`503 no_eligible_worker`. Each caller sharing an idempotency key retains its own
deadline; a short or nonblocking caller cannot terminate a longer caller's wait.
An expired late grant is released and returns `410 reservation_expired`.
Storage failures return `503 admission_storage_unavailable` without authorizing work.
Operational errors use the existing `error.type` and
`error.message` envelope, with `error.details` counters for the latest attempted
round (`attempted`, `busy`, `ineligible`, `unreachable`); the draft YAML's versioned
envelope and full error classification are not yet the runtime contract.

The origin persists idempotency claims in `origin.sqlite`; `idempotency_key`, or
`request_id` when no key is supplied, identifies a claim at that origin. Reusing
it with different admission parameters returns `409 idempotency_conflict`. Wait
length is not part of the claim parameters. The saved target is retained when a
remote outcome is uncertain, so retries do not silently book another worker.
Claims currently have no automatic retention or garbage collection; protect the
state directory as it contains lease tokens. This is origin-scoped idempotency,
not cluster-wide exactly-once execution. Retry uncertain requests with the same
key at the same origin. A timeout, disconnect, failed response or release attempt
is not proof that a remote worker did nothing. Cancellation stops waiting and
attempts to release a late grant, but communication failure can leave its outcome
unknown. Worker fencing, TTL and quarantine rules continue to apply.

## Python helper

```python
from fleet_manager import idle_herd

async with idle_herd.acquire("example-model", timeout=60) as lease:
    token = lease.token  # supply as X-Idle-Herd-Reservation to inference
```

The helper accepts an optional `base_url` and `httpx.AsyncClient`. It acquires one
lease and makes a best-effort release on exit; failure to communicate does not
prove release. It generates a fresh request identity for each context, so callers
requiring retry of an uncertain acquisition should use the HTTP API with a stable
idempotency key. The optional SSE capacity watch is not implemented.

The peer listener uses the separate authenticated TLS and signed RPC trust domain
from [Phase 2](phase-2.md). The localhost API's future scoped owner/control
authentication is not implemented; do not expose it to untrusted clients. No Raft,
model sharing, durable jobs, multi-slot transactions or future control API is
introduced by this phase. The running `/openapi.json` describes runtime models;
[openapi.yaml](openapi.yaml) remains a broader draft and is not a conformance test.

For a checkout with dependencies installed, focused checks can be run as
`.venv/bin/pytest -q tests/test_peer/` and `.venv/bin/ruff check src/fleet_manager/peer`;
where `uv` is installed, use the equivalent `uv run pytest -q tests/test_peer/`
and `uv run ruff check src/fleet_manager/peer`.
