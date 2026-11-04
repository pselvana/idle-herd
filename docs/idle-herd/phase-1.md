# Phase 1: native single-node worker

`idle-herd` combines local ingress, Ollama inventory/execution and durable worker
admission in one native Python process. No discovery, seeds, peer calls, consensus,
upstream router lifecycle, preloader or community telemetry sender runs.
Legacy `herd` / `herd-node` remain available. One ingress can bind port 11435
at a time; use a separately managed local Ollama on its unchanged port 11434.

```console
uv sync --extra dev
uv run idle-herd
```

Start Ollama and install a compatible model using its normal tools first. Point
clients at `http://127.0.0.1:11435/v1`. The listener binds IPv4 loopback;
IPv6-only localhost resolution may require `127.0.0.1` explicitly. This is a
foreground daemon; service/autostart installers arrive later. UI lifetime does
not own daemon lifetime.

The peer reuses OpenAI chat/completions and embeddings, Ollama chat/generate/embed
and model inventory, Anthropic Messages and OpenAI Responses translators, including
SSE/NDJSON streaming. Missing terminals in non-streaming Ollama responses now
return 502 instead of synthetic `done: true`. Additional modality routes remain
registered but unsupported sidecars are not advertised. Phase 1 serves Ollama;
it does not launch standalone MLX, fastembed, image or STT services. Legacy
commands retain those integrations.

## Configuration and eligibility

Settings read environment variables directly, without loading the legacy
`~/.fleet-manager/env`. Existing inference translation tuning still uses
`FLEET_*` where applicable; it cannot enable discovery, standalone MLX execution
or a telemetry sender in this lifecycle.

| Variable | Default | Meaning |
|---|---|---|
| `IDLE_HERD_DATA_DIR` | `~/.idle-herd` | Persistent private worker directory |
| `IDLE_HERD_BACKEND_URL` | `http://127.0.0.1:11434` | Local HTTP Ollama; reject remote addresses, credentials, paths and ingress port 11435 |
| `IDLE_HERD_SLOTS` | `1` | Shared worker concurrency budget across all models, range 1–128 |
| `IDLE_HERD_MODEL_SLOTS` | `{}` | JSON model-specific limits, further bounded by the shared budget; names normalize to `:latest` |
| `IDLE_HERD_QUARANTINE_DRAIN_SECONDS` | `30` | Keep reading an interrupted original backend response for completion proof, range 0–120 seconds; 0 disables. Timeout never releases uncertain work. |
| `IDLE_HERD_MEMORY_ADMISSION_MODE` | `backend` | Ollama manages actual model fit/loading/eviction. `strict` additionally enforces host RAM estimates. |
| `IDLE_HERD_WEIGHT_MULTIPLIER` | `1.2` | Cold model disk-weight estimate multiplier, minimum 1; enforced only in strict mode |
| `IDLE_HERD_REQUEST_MEMORY_GB` | `1` | Estimated per-admission KV/runtime allowance; enforced only in strict mode |
| `IDLE_HERD_RESERVE_MEMORY_GB` | `2` | Host RAM reserve enforced only in strict mode |

Reservation and consumption both fetch live `/api/tags` and `/api/ps`. Missing
models, unknown model size and failed inventory checks cannot grant work. Loaded
weights match name and, when supplied, digest. Native workers now default to
`backend` memory admission: available RAM, model disk size and cold-weight/runtime
estimates do not veto an otherwise eligible slot. Ollama owns actual allocation,
model residency, eviction, context/GPU fit and its final acceptance or rejection.
macOS can reclaim cached pages; a point-in-time RAM estimate is not an allocation
limit. The default does not add cache/inactive bytes to psutil's available value.

Explicit `strict` mode retains the original serialized memory-budget behavior:
cold weights plus request allowance and other lease budgets must fit after the
host reserve, both at reservation and consumption. This can conservatively count
resident memory twice. Disk size plus headroom remains an **estimate**. Worker
slot limits, per-model limits, peer quotas, durable fencing and quarantine apply
in both modes. Critical pressure rejects new work in either mode; warning pressure
and the minimum-available-memory threshold reject only in strict mode. Observed
pressure and available memory stay visible in diagnostics. Backend-managed memory
does not guarantee successful allocation or authorize execution outside a lease.
Phase 1 originally admitted local interactive work; it did not
collect human activity or support `idle_only`, background policy, modes or jobs.
Unknown activity never grants idle-only work.

## Actual local API subset

The peer's `/openapi.json` describes implementation. The separate `openapi.yaml`
remains the **future** Phase 4/6 cluster/control contract.

| Operation | Phase 1 behavior |
|---|---|
| `GET /health` | Liveness/stable worker ID; no backend-readiness claim |
| `GET /v1/cluster/capacity?model=NAME` | Model-specific `scope: local_worker`, `globally_authoritative: false`, `discovery_state: disabled`, `quorum: null`, timestamp and observed free/quarantined slots |
| `POST /v1/capacity/acquire` | Body requires `model`, `request_id`; optional `ttl_ms` (100–120000, default 30000), `slots: 1`, `wait_ms: 0`; returns 200 and durable lease |
| `GET /v1/capacity/reservations/{id}` | Inspect using matching `X-Idle-Herd-Reservation` token |
| `DELETE /v1/capacity/reservations/{id}` | Release using token; never frees running/quarantined execution |
| Inference with `X-Idle-Herd-Reservation` | Consume token once for its reserved model; auto-routing must resolve to the same model |
| Inference without that header | Internally reserve/consume before backend dispatch |

Lease fields: `reservation_id`, opaque `token`, `cluster_id`, `worker_id`,
`worker_incarnation`, `slot_id`, `slot_generation`, `request_id`, normalized
`model`, `expires_at` (Unix seconds), `ttl_ms`, `state`, `memory_gb`.
Retrying the same request ID/model/TTL returns its current unconsumed lease.
Changed parameters return 409; consumed, expired or previous-incarnation request
IDs cannot start another execution. Use a new ID for a new attempt. Idempotency
is worker-local; records currently persist indefinitely.

Contention returns 429 `busy`; inventory/resource failures return 503
`backend_unavailable`/`insufficient_memory`/`unknown_model_size`; missing model
returns 404 `not_installed`. Forged/foreign tokens return 403. Stale incarnations,
generations, expired/consumed tokens, model mismatches and conflicting IDs return
409. Unsupported waits, multi-slot requests and unknown fields return 422.
After streaming headers are sent, failures use in-band errors/incomplete streams.
No denied request reaches backend inference.

Capacity is a snapshot, never a grant. Listings refresh every two seconds and
may lag failure briefly; admission checks live backend state. Legacy queues
schedule candidates but cannot grant capacity. Bounded fair waits/deadlines and
the full cluster schema are Phase 4 work.

## Durable authority and recovery

`worker.sqlite` uses SQLite WAL and `synchronous=FULL`. A serialized transaction
checks shared/per-model slots (and aggregate RAM estimates in strict mode), advances a persistent slot
generation and commits its token before acknowledging. Consumption checks token,
incarnation, generation, expiry, model, single-use state and live fit, then commits
`running` **before** backend dispatch. Database failure cannot acknowledge a grant
or start uncommitted work. Lease transitions/backend outcomes are recorded in
`events` without prompts. Inference traces/latency use inherited `latency.db`;
shutdown drains scheduled writes before storage closes.

`worker-owner.sqlite` holds a lifetime SQLite exclusive transaction. Another
daemon or maintenance command using that directory fails with a lock error;
process death releases the OS lock. Use healthy **local** storage. Never delete,
restore or copy a live ledger to another authority. The directory is POSIX mode
0700; secure the corresponding per-user Windows ACL.

Restart preserves worker/cluster IDs, creates a new incarnation, invalidates old
unconsumed grants and quarantines old running leases. Acknowledged slot
generations never decrease. Only unconsumed reservations expire. A confirmed
Ollama `done: true`, complete synchronous embedding response, or complete HTTP
4xx rejection permits release. Disconnect, cancellation, timeout, incomplete
response, HTTP 5xx or process death may leave backend computation alive, so its
charge remains quarantined. An empty `/api/ps`, failed probe or lease expiry
cannot prove a request stopped; Ollama provides no authoritative request-specific
reconciliation API here.

After a client disconnect, a still-readable successful backend response is retained
for up to `IDLE_HERD_QUARANTINE_DRAIN_SECONDS`. Its output is discarded. If the
original response supplies completion proof during this window, the worker
automatically releases the quarantined slot using the original token, incarnation
and slot generation. It neither starts another backend request nor replays work.
A terminal `done: true` record suffices even if the socket stays open; synchronous
responses require their complete body. Deadline expiry, incomplete output or a
lost backend connection preserves quarantine. Daemon shutdown stops the detached
readers before closing the ledger; restart-orphaned work has no original response
to reconcile automatically.

To reclaim quarantine without original-response proof, stop `idle-herd` and **all Ollama/backend runner processes**,
including independently supervised runners. Confirm they are stopped, then use
the same data directory:

```console
uv run idle-herd reconcile --confirm-backend-stopped
```

This records an operator attestation and releases quarantine under the worker
lock. Restart backend and peer afterward. The command does not detect/stop
backend processes; false attestation invalidates safety. It refuses maintenance
while a daemon owns that directory. No HTTP endpoint clears running execution.

## Evidence and limits

Tests use fabricated backends, including separate native processes over real
loopback HTTP. They race 100 callers, cover one/two/three-slot budgets, replay and
foreign tokens, expiry, changing fit, write failure, cancellation, truncated
output, commit-before-lost-reply and process death with surviving backend work.
The restarted peer rejects old tokens and keeps the orphan charged until an
offline backend-stop attestation. See [exact results](implementation-status.md).

One worker must exclusively own its backend budget: direct Ollama clients,
another data directory pointing to the same backend, copied ledgers and unhealthy
storage lie outside this authority. Memory estimates do not provide GPU isolation.
Ollama owns final model/context compatibility. Growing history and legacy pending
queues need bounded retention/queue work later. Local inference and initial
reservation issuance allow local processes without caller credentials. Host/
Origin checks and one-use reservation capabilities do not isolate malicious
same-user apps. No privileged owner-control API exists yet.

Real Ollama/MLX inference, Linux/Windows execution, OS services, sleep/wake, idle
sensors, multi-peer survival/partitions and Raft loss were **not** implemented or
tested here. Logical retries may execute at least once; no durable job recovery
or global exactly-once guarantee exists. Next: Phase 2 authenticated peer
discovery/dispatch. Docker/Compose remains Phase 10.
