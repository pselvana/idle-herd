# Idle-Herd — Implementation Plan

> **Status:** Revised availability-first engineering plan for a fork of [geeks-accelerator/ollama-herd](https://github.com/geeks-accelerator/ollama-herd).  
> **Prepared:** November 1, 2026  
> **Primary development environment:** Visual Studio Code + OpenAI Codex IDE extension  
> **Target:** Cross-platform, peer-to-peer, idle-aware independent LLM inference; no dedicated inference router; **fully functional with 1, 2, or 3+ intermittently connected nodes and no guaranteed Raft quorum**.
> **Revision:** 3 — native-first development, macOS menu bar companion, optional cross-platform tray, Dockerization moved to the final phase; retains quorum-free worker-local scheduling.

## 0. Executive summary and availability contract

**Idle-Herd** forks Ollama Herd's Python/FastAPI multi-backend inference router and node agent into **symmetric, independent peers**. Every node exposes the same local OpenAI/Ollama-compatible API. The local peer executes a request itself or directly negotiates an available slot with another compatible peer. Nodes may be laptops, desktops, Linux/Windows PCs, servers, or Docker-managed agents; **no node is assumed to be continuously available, and no dedicated router is required**.

### Non-negotiable availability contract

1. **One node online:** It must provide a functional local API, accept locally executable jobs, atomically reserve its own capacity, and allow clients to wait for eligibility/capacity. It must work with no other nodes, elected leader, or Raft quorum.
2. **Two nodes online, either may disappear:** Both accept jobs via their own localhost APIs. When one disappears, the other can keep executing jobs on its *own* available backend and wait for remote capacity to reappear. It must not require a surviving voter or manually rebootstrap a consensus group.
3. **Three or more nodes, arbitrary churn:** Every reachable connected component can independently process work on its own eligible workers. A node that comes online exchanges state and becomes schedulable after authentication and fresh admission checks.
4. **Partitions:** Split networks may each execute independent jobs using only locally reachable workers. Cross-partition **global ordering, globally unique job execution, and perfectly accurate capacity totals are not promised**. No worker grants more than its own configured capacity regardless of how many peers contact it.
5. **All nodes offline:** No inference can execute. Previously accepted jobs survive only where their job records, inputs, and required model artifacts are persisted or replicated. Work can resume when a node with sufficient persisted state becomes available. No design can guarantee progress with zero live machines.
6. **Local-only degraded operation is normal:** It is not an unsafe emergency override. When disconnected from peers or quorum, a peer continues to use its own resource authority and may accept remote requests from already authenticated reachable peers.

**Core architectural choice:** **Worker-local admission is the source of truth.** Peer discovery and capacity summaries are eventually consistent hints. A worker serializes and persists reservations for its own slots and can refuse a dispatch based on real-time idle, memory, model, and backend conditions. A client NEVER treats a gossip snapshot as an allocated slot.

### Leader election / Raft, without sacrificing availability

Raft can be supported as an **optional coordination service**, useful for strongly consistent cluster preferences, administrative configuration, or other metadata *while a quorum exists*. The Raft leader is neither an inference router nor the issuer of worker-slot leases. **Scheduling, reservations, capacity waiting, and local job execution never require Raft quorum.** If a cluster has no reliable quorum, optional Raft operations suspend without stopping inference.

A **best-effort coordinator** can be selected from currently reachable peers for advisory work (e.g., summary/dashboard, transfer recommendations). It may change or have multiple concurrent instances across partitions; it MUST NOT own exclusive resources or authorize non-idempotent actions. This is leader preference, **not** consensus-based leadership.

Do not implement automatic Raft self-bootstrap or silently shrink a voter set after quorum loss; that risks irreconcilable control-plane histories. This fork prioritizes useful computation during churn over globally linearizable scheduling.

### Design objectives

1. **Local-first:** `http://localhost:11435/v1` works identically on every peer.
2. **Symmetric data plane:** Any peer may receive, negotiate, dispatch, stream, and execute; no central inference router.
3. **Cross-platform:** macOS, Linux, Windows; mixed Apple Silicon/AMD64, CPU/GPU, Ollama/MLX/other compatible backends.
4. **Idle-aware:** Protect host users with reliable available signals and conservative fallback when desktop idle state is unknown.
5. **Local atomic reservations:** Per-worker serialized, crash-recoverable admission using durable local state and expiring lease tokens.
6. **Capacity-aware parallelism:** Expose point-in-time capacity inspection, nonblocking acquire, and blocking wait that survives peer churn while the caller remains online.
7. **Durable optional job execution:** Job journal persists locally; replicated job manifests/payloads enable *best-effort* failover when copies are reachable. State guarantees clearly distinguish local durability, replicated durability, and execution uniqueness.
8. **Peer-to-peer model sharing:** Reuse content-verified compatible model artifacts from peers subject to licensing, architecture, disk, and resource policies.
9. **Native-first, container-ready:** Build and test locally installed peer services first on macOS/Linux/Windows, with native host signals and an optional macOS menu bar companion; add multi-architecture Docker/Compose packaging only in the final implementation phase.
10. **Raft optional:** A healthy quorum may enhance coordination; its loss cannot interrupt local execution or peer-to-peer capacity negotiation.
11. **Local desktop control:** A macOS menu bar companion displays local running jobs and health, permits deliberate offline/drain/pause controls, and never becomes a dependency of the worker service.
12. **Fork-friendly:** Maintain Ollama Herd API compatibility, upstream attribution, and small reviewable milestones.

### Non-goals, first release

- Sharding one LLM across machines; migrating GPU memory or inference KV state.
- Exactly-once task execution across failures or network partitions.
- Globally consistent capacity counts while the cluster is partitioned.
- Automatic completion of a job whose only durable copy is on an offline/lost node.
- Reliable human-idle measurement inside unprivileged Docker Desktop containers without a host integration (container support is deferred to Phase 10).
- Automatic permissionless redistribution of private, restricted, or incompatible models.

### Terminology

- **Peer:** Node running the local API, gossip/discovery, scheduler, and admission server.
- **Worker:** A peer with a healthy, locally accessible inference backend.
- **Origin:** Peer that accepted a job and maintains its durable journal and result endpoint.
- **Worker-local lease:** A reservation **issued and fenced by the worker itself** for its own slots.
- **Reachable view:** Fresh, authenticated peer information, NOT an authoritative global cluster snapshot.
- **Coordination leader:** Optional Raft leader for quorum-dependent administrative state; **not** required for inference.

---

## 1. Confirmed upstream foundation and fork strategy

Upstream: https://github.com/geeks-accelerator/ollama-herd (MIT). Its published structure includes:

| Upstream file / subsystem | Reuse / planned change |
|---|---|
| `src/fleet_manager/server/app.py` | Convert router FastAPI factory to a per-peer API factory; keep compatible routes. |
| `server/scorer.py` | Reuse model-fit, thermal, memory, and context scoring; add freshness, idle, lease, and backend eligibility constraints. |
| `server/queue_manager.py` | Split local execution queues from cluster-wide reservations; preserve backend concurrency limits. |
| `server/streaming.py` | Keep request proxying and SSE/NDJSON conversion; target reserved peer instead of centrally selected router worker. |
| `server/registry.py` | Rework from in-memory router registry into a local authenticated best-effort peer view; only a target worker is authoritative for its own admission. |
| `server/routes/openai_compat.py`, `ollama_compat.py` | Maintain external request contracts. Route via local peer coordinator. |
| `server/routes/fleet.py`, `dashboard.py` | Adapt fleet status and dashboard to cluster state and decentralized ingress. |
| `node/agent.py`, `collector.py` | Combine node agent with local API process; retain cross-platform collectors and add host-integrated idle sensors. |
| `node/capacity_learner.py` | Keep as optional prediction signal, **not** as a replacement for immediate idle admission. |
| `node/mlx_supervisor.py`, `mlx_client.py` | Reuse for native macOS backends; introduce backend adapters for containerized deployments. |
| `common/system_metrics.py` | Extend cross-platform host sensor and container fallback behavior. |
| `models/node.py`, `models/config.py` | Define versioned peer, capability, worker-local lease, job, and model state; optional admin-only Raft schemas. |

Upstream instructions: `AGENTS.md`, `CLAUDE.md`, `docs/fleet-manager-routing-engine.md`, `docs/operations-guide.md`, `docs/issues.md`, `docs/observations.md`. **Codex must read these on the actual cloned revision before changing anything.** The published layout may change: obtain a commit hash at fork time, inventory the checked-out tree, and amend paths when needed; do not infer that documentation is a fully verified current code audit.

### Repository workflow

```bash
# In your GitHub account, fork geeks-accelerator/ollama-herd as idle-herd.
git clone git@github.com:YOUR_USERNAME/idle-herd.git
cd idle-herd
git remote add upstream https://github.com/geeks-accelerator/ollama-herd.git
git rev-parse HEAD > UPSTREAM_BASE_COMMIT.txt

git switch -c feat/idle-herd-foundation
uv sync --extra dev
uv run pytest
uv run ruff check src/
code .
```

- Preserve upstream `LICENSE` and acknowledgments. Rename package/commands incrementally, keeping legacy aliases temporarily (`herd`, `herd-node`) for migration tests.
- **Naming conventions:** `Idle-Herd` is the display/project name; `idle-herd` is the repository/CLI/daemon-facing prefix; `idle_herd` is the Python module name; `IDLE_HERD_*` are environment variables; and `IdleHerdMenuBar` is an internal Swift/Xcode identifier. Hyphens cannot appear in Python module identifiers or shell environment-variable names. The macOS app display name must be **Idle-Herd**.
- Propose new command `idle-herd` and background process `idle-herd-daemon` (name optional); do not rename every internal Python import in Phase 1.
- Extend upstream root `AGENTS.md` with Idle-Herd-specific rules; don't erase existing developer instructions.
- Add architectural decision records (ADRs) for consensus, idle detection, lease semantics, and model replication.
- Do one small feature branch / PR per phase; keep baseline upstream tests green.

---

## 2. Always-available peer architecture and coordination

```text
  App A                         App B                         App C
    |                             |                             |
localhost:11435              localhost:11435              localhost:11435
    |                             |                             |
+---v----------------+   +--------v-----------+   +-------------v-------+
| Peer A             |   | Peer B             |   | Peer C              |
| ingress + jobs DB  |   | ingress + jobs DB  |   | ingress + jobs DB   |
| peer discovery     |<->| peer discovery     |<->| peer discovery      |
| scheduler + waiters|   | scheduler + waiters|   | scheduler + waiters |
| admission slots    |   | admission slots    |   | admission slots     |
| Ollama / MLX       |   | Ollama / CUDA      |   | Ollama / LM Studio  |
+--------------------+   +--------------------+   +---------------------+
           ^                    ^                         ^
           +--------- authenticated peer RPC -------------+

     Worker receiving a reservation is the ONLY authority for that worker's slots.
     Inference and model transfers are direct between source and target peers.

 OPTIONAL: Raft sidecars on selected peers (when a quorum happens to be online)
   -> elected coordination leader for administrative metadata and preferences
   -> NEVER a prerequisite for booking slots, waiting, or executing jobs
```

### Required roles (every node, including a single-node cluster)

- **Peer ingress:** Hosts OpenAI/Ollama-compatible local API, forwards inference to the accepted worker, streams response directly back to the caller, handles local authorization.
- **Discovery:** Static seeds and bidirectional authenticated peer exchange with optional mDNS and Tailscale discovery. Every peer maintains last-seen, endpoint, capabilities, TTL, identity, and reachability; seed discovery must not require the seed to remain online after convergence.
- **Local admission authority:** Reserves/releases its *own* model slots, serializes contention, persists lease records, and enforces idle policy and resource checks at dispatch time.
- **Origin job journal:** Persists jobs accepted through that peer (SQLite WAL or another local embedded store), retry metadata, and result locations. Optional encrypted peer replication requires explicit durability acknowledgments.
- **Backend/sensor adapters:** Collect host metrics and execute inference using local or host-exposed engines.

### Optional roles (not needed for safety of worker reservations)

- **Raft coordinator:** Small Go sidecar using HashiCorp Raft and persistent state. It replicates non-critical cluster settings/administrative metadata only when quorum is present. No prompt, model weights, lease record, or live job execution path goes through the coordinator.
- **Advisory coordinator:** A preferred peer selected using deterministic rendezvous hashing / freshness to perform optimization tasks. If two components select different coordinators, both may publish *advice*, but no correctness property depends on either being unique. Avoid calling this a Raft election.
- **Replica peer:** Stores encrypted or authenticated job manifest/input/result copies and may undertake allowed retryable jobs after an origin disappears, subject to duplicate-execution tradeoffs documented below.

### Discovery, trust, membership

1. `idle-herd init` on the first node creates a stable cluster UUID, local node keypair, a trust anchor, and a persistent local state directory. A one-node deployment is complete; **no Raft bootstrap is required**.
2. Add a second/third node with a pre-authorized invite signed by a trusted administrator or pair directly over a protected channel. Authenticated peers exchange known endpoints and signed inventory digests. Join is independent of voting membership.
3. Peer liveness is suspicion based on TTL/active RPC, not a permanent deletion. Offline peers stay known for reconnection, are excluded from scheduling after timeout, and automatically re-enter once a fresh authenticated heartbeat and backend check succeed.
4. New node identities and ACLs are durable across native service restarts (and, in Phase 10, container restarts). Keys do not change across restarts. Reject node ID/key conflicts and clusters with conflicting trust anchors.
5. For partition-tolerant enrollment, a pre-signed device-specific certificate may be accepted without quorum. **Instant global revocation and globally single-use invites cannot be guaranteed during a partition**; design time-limited credentials plus synchronized revocation state and conservative expiry.
6. Peer discovery may use gossip, but every admission/transfer requires direct authenticated RPC with the intended worker. Never forward untrusted internal backend URLs to clients.

### Optional Raft strategy

- Support **0 voters** (default / recommended for intermittent home machines), **1 voter** (development or optional metadata persistence), **3 or 5 voters** (when sufficiently stable). Avoid 2 voters; they cannot retain quorum after one failure.
- Any number of offline voters is fine for **inference**, because reservations do not use Raft; non-critical Raft-committed settings and administration become read-only when quorum is absent.
- An application must not silently rebuild the old Raft cluster as a new singleton when other voters disappear. Require an explicit admin-guided recovery if preserving old committed metadata matters.
- If the old leader returns, peers reconcile authenticated metadata and resume optional consensus only once quorum is restored. Do not let a stale leader invalidate worker-issued reservations or overwrite newer local job journals.
- Treat partition-tolerant settings as **per-node policy or signed policy bundles**. Strict, globally serialized admin changes may only apply if committed by a healthy quorum.
- If the user strictly requires a **single globally authoritative leader during arbitrary network partitions**, explain that this cannot be guaranteed at the same time as always accepting writes on every connected component. Idle-Herd deliberately does not make that guarantee.

### Availability versus consistency choices

| Concern | Authority | Without any Raft quorum | Guarantee |
|---|---|---|---|
| Local inference eligibility/slots | Each worker | Fully operational | Worker cannot overbook its own slots |
| Remote dispatch | Direct worker RPC | Operational when worker reachable | Accepted only by target worker |
| Blocking capacity wait | Requesting peer + peer events | Operational | Rechecks live admission before claiming success |
| Discovery/model catalogs | Gossip + authenticated probes | Operational, potentially stale | Clearly labeled freshness |
| Job queue metadata | Origin's durable journal | Operational while origin is online | Local durability, not global uniqueness |
| Job recovery after origin loss | Optional replicated copies | Possible if a complete copy survives | At-least-once, possible duplicates |
| Global capacity count | No single authority | Approximate | Never advertise as linearizable |
| Administrative config transactions | Optional Raft | Read-only/deferred | Strict consistency only with quorum |
| Model transfer | Destination-local install lock | Operational | One installation per digest per destination |

---

## 3. Churn-tolerant scheduling, idle detection, and local leases

### Worker capability snapshot (gossip is advisory)

Each node publishes a timestamped authenticated capability snapshot, e.g. every 2–5 seconds while connected and on changes. Include observed freshness and sensor quality. This can be delayed or missing; **admission always rechecks resources on the target worker**.

```json
{
  "node_id": "mac-studio-a",
  "platform": "darwin",
  "arch": "arm64",
  "backend_types": ["ollama", "mlx", "docker-model-runner"],
  "backend_status": {"ollama": "healthy", "mlx": "healthy"},
  "idle": {"state": "idle", "confidence": "host-sensor", "idle_seconds": 180},
  "resources": {
    "ram_total_gb": 36,
    "ram_available_gb": 18,
    "vram_available_gb": null,
    "cpu_percent": 8,
    "gpu_util_percent": null,
    "power": "ac",
    "thermal_state": "nominal",
    "disk_free_gb": 110
  },
  "models": [
    {"id": "example-model", "format": "gguf", "digest": "sha256:...",
     "backend": "ollama", "loaded": true, "max_slots": 2, "used_slots": 0}
  ],
  "observed_at": "2026-10-31T22:00:00Z",
  "valid_for_ms": 5000
}
```

Values are illustrative; unknown measurements remain `null`. Each peer may report different capability sets and model formats.

### Idle policy (enforced on each worker)

1. Protect the owner: recent input, screen lock/session, CPU/memory pressure, GPU activity if reliable, battery/AC, thermal limits, and a manual override.
2. Configurable policy: `always`, `idle_only`, `utilization_only`, `manual`, `never`, independently for interactive and background workloads.
3. Example hysteresis: idle for 120 seconds, CPU under 25% over a window, adequate memory and model headroom, acceptable temperature, AC or sufficient battery. User return immediately marks `draining`; gradual re-entry prevents flapping.
4. `unknown` required human-idle signals mean **not eligible for `idle_only`**. A user may expressly select `utilization_only` for headless systems (and, in Phase 10, container deployments), and status must describe that difference.
5. Preemption is opt-in for background jobs; draining does not terminate a partially streamed interactive response without explicit policy.
6. Model downloads/warmups, background jobs, and concurrent model contexts share the same budget and policy enforcement.

| Deployment | Human-idle signal | GPU inference | Implementation |
|---|---|---|---|
| Native Apple Silicon macOS | Host native sensor | Native MLX/Ollama | Full idle-aware option |
| Native Linux desktop | Session idle helper when available; otherwise unknown | Ollama/host backend with supported CUDA/CPU | Idle-aware where verified; conservative unknown fallback |
| Native Windows desktop | Native input-idle helper when available | Ollama/host backend, CPU/GPU as compatible | Idle-aware where verified; conservative unknown fallback |
| Headless native server | User-input idleness usually irrelevant | Backend-specific | Explicit `utilization_only`/`always` |
| Docker Desktop macOS (Phase 10) | Host helper needed | Host Ollama/DMR/MLX server | Container agent + host integrations |
| Linux Docker + NVIDIA (Phase 10) | Host-dependent; unknown when unavailable | CUDA container or host backend | Explicit opt-in utilization-only policy |
| Windows Docker Desktop (Phase 10) | Host helper needed | Host Ollama/other backend | Container agent + optional sensor |

### Peer-local reservation protocol: the core safety invariant

**Each worker manages its own finite slot inventory** in a serialized admission loop using an embedded durable ledger. An incoming remote or local request does **not** receive a reservation from a cluster leader.

1. Requesting peer finds likely candidates from recent authenticated gossip (including itself); candidate ordering prefers loaded compatible models, idle eligibility, memory headroom, lower latency, and locality. Stale/unreachable entries are skipped.
2. Origin sends `PrepareReservation` directly to candidate peer with model digest, resource demands, context size, job class, request ID, idempotency key, and a requested short TTL.
3. Worker checks current backend health, compatibility, live idle signal, RAM/VRAM/concurrency budget, and local ledger. **Under one local transaction/lock**, it either creates a unique unexpired slot lease or responds `busy`, `ineligible`, `not_installed`, or `unreachable`.
4. Worker generates `reservation_id`, opaque authenticated token, worker `node_id`, `worker_incarnation`, `slot_id`, *locally monotonic* `slot_generation` (fencing number), and bounded TTL. Persist the lease **before** acknowledging success. The origin cannot invent or extend a worker lease.
5. Origin starts/streams the job directly to the worker using that token. Worker confirms lease ownership and active/consumed state and serializes actual admission with the same slot lock. Lease is bound to the job and can only be consumed once; renewing requires worker acknowledgment.
6. Worker releases the slot on completion/cancellation, timeout, or policy-governed drain. On process restart it detects/reconciles orphaned backend jobs before reissuing a freed slot. Expired records alone must not imply a still-running inference process has stopped.
7. Requests are idempotent **per worker** using a persistent `(origin_id, request_id)` key. Cross-worker/global deduplication during partition is **not guaranteed**; callers must account for at-least-once execution.

**Concurrency guarantee:** For each particular worker and model/backend resource, `admitted_slots <= configured_slots`, even if 100 origins contend and no leader exists. Persist slot generations across restarts to reject stale tokens. Worker-only fencing prevents a disconnected leader, coordinator, or remote peer from double-booking a worker.

**Multi-slot reservation:** Atomic allocation on *one worker* is feasible. Cross-worker all-or-nothing reservations require a separate distributed transaction/coordinator; defer this feature from MVP. Do not claim atomic multi-worker capacity when it isn't provided. The client can acquire N independent leases and release on partial failure, but concurrent races remain possible.

### Blocking when all compatible workers are busy

- `GET capacity` provides a best-effort picture, not a reservation. A caller should use `POST /v1/capacity/acquire` with `wait_ms` or send a normal inference request with a documented wait policy.
- Each origin maintains a **local, bounded, fair waiting list** (not a central queue). Waiters wake on slot release notifications, new peer joins, idle eligibility transitions, model-load completion, and a bounded backoff check to recover from missed events.
- When no worker is currently reachable, a waiting request can remain pending until its timeout rather than reporting that capacity will never exist. Return diagnostic `no_reachable_eligible_workers` and `next_check_ms` in query responses.
- A wait is live only while its origin stays running. For waits that must survive origin restarts, submit a persistent background job instead; persistence/failover depends on journal replication.
- Every candidate response is subject to live admission, so concurrent waiters on different origin nodes may compete but **cannot oversubscribe a worker**.
- Bound the wait/lease TTL, pending count, and request cancellation; use request deadlines and honor HTTP client/proxy timeouts.

### Failure and partition rules

| Event | Required response |
|---|---|
| Single node running | Reserve and execute locally without discovery/quorum |
| Second node joins | Exchange authenticated state; it becomes eligible only after health/admission checks |
| Remote node leaves | Expire reachability snapshot; retry other reachable workers or wait |
| Origin node dies after dispatch | Worker continues or cancels according to token/job policy; origin result may become inaccessible unless replicated |
| Worker dies before first output | Mark attempt unknown/failed and choose another available worker if request is retry-safe |
| Worker dies after stream begins | Stream fails visibly; no seamless restart of token stream |
| Network partitions | Each component schedules only to reachable workers; there may be no common cluster-wide view |
| Two origins reserve same worker | Only one can receive each worker-owned slot; loser retries or waits |
| Raft leader disappears | **No effect on core admission or inference**; optional admin changes pause if quorum lost |
| No eligible model/capacity anywhere reachable | Wait until eligible worker appears or return clear timeout/unavailable result |
| All nodes shut down | No processing; local persistent journals enable resume after recovery where data survives |

### Reservation safety versus job uniqueness

A worker can guarantee no simultaneous overbooking of **its own resources**. A distributed cluster with intermittent nodes and partitions cannot simultaneously guarantee globally exactly-once execution and unrestricted availability. An origin failure/retry may run the same logical job on different workers at different times, possibly overlapping during uncertainty. Idle-Herd therefore uses explicit idempotency keys, attempt IDs, result reconciliation, and **no automatic retry for non-idempotent workloads**. For strict singleton jobs or shared global side effects, require an external durable authority or a healthy Raft quorum and accept reduced availability.

---

## 4. API specification for arbitrary node availability

### Standard inference APIs

Retain `/v1/chat/completions`, `/v1/embeddings`, `/v1/models`, `/api/chat`, `/api/generate`, and upstream-supported multimodal endpoints where backends support them. Every node serves the same local API. Each inference request uses a worker-local reservation internally; no client has to manage it explicitly unless requesting explicit parallel slots or batch scheduling.

### Capacity snapshot: truthful under partial visibility

`GET /v1/cluster/capacity?model=example-model&idle_only=true`

```json
{
  "cluster_id": "home-ai",
  "model": "example-model",
  "scope": "reachable_peers",
  "known_nodes": 3,
  "reachable_nodes": 1,
  "eligible_reachable_workers": 1,
  "observed_available_slots": 1,
  "observed_total_eligible_slots": 2,
  "busy": false,
  "globally_authoritative": false,
  "discovery_state": "partial",
  "quorum": null,
  "peers": [{"node_id": "mac-studio-a", "available_slots": 1, "fresh": true}],
  "observed_at": "2026-10-31T22:00:00Z",
  "stale_after_ms": 5000
}
```

- `busy=true` means **no capacity is currently observed among fresh eligible reachable peers for this request**, not that all cluster nodes are busy. `busy` should be `null` if discovery/eligibility is too incomplete to assess; client should call acquire for ground truth.
- A dead/offline node never artificially contributes available slots. `known_nodes` may include offline peers; a temporarily unreachable machine may return later.
- `quorum` is `null` when the optional Raft service is disabled, otherwise `true`/`false`. **Never conflate `quorum=false` with `available_slots=0`.**
- Model-specific context, backend, idle policy, quantization, architecture, and expected resource headroom affect estimates. Keep `observed_` prefix or equivalent on all gossip-derived totals.
- Offer `GET /v1/cluster/status`, `/v1/cluster/nodes`, and `/v1/cluster/models` with per-peer freshness and optional coordinator health.

### Reservation and optional blocking wait

`POST /v1/capacity/acquire`

```json
{
  "model": "example-model",
  "wait_ms": 60000,
  "slots": 1,
  "idle_only": true,
  "idempotency_key": "client-job-123",
  "job_class": "background",
  "context_tokens": 8192
}
```

`201 Created` (after successful **worker** admission):

```json
{
  "reservation_id": "rsv_abc123",
  "status": "reserved",
  "worker_node_id": "mac-mini-b",
  "worker_incarnation": "boot-a13e",
  "slot_id": "ollama:example-model:0",
  "slot_generation": 42,
  "model": "example-model",
  "expires_at": "2026-10-31T22:01:00Z",
  "reservation_token": "opaque-short-lived-worker-token"
}
```

**Status behavior:**

| Condition | Result |
|---|---|
| One compatible local or remote slot accepted | `201 Created` with worker-issued reservation |
| `wait_ms=0`, all currently eligible reachable workers busy | `429 no_capacity`, `Retry-After` |
| `wait_ms>0`, all known workers busy/offline | Origin waits, re-discovers peers, retries under deadline |
| Wait deadline expires without admission | `504 capacity_wait_timeout` with diagnostic counters |
| No compatible backend anywhere in reachable view | `422 model_not_compatible` if conclusively incompatible; otherwise `503 no_eligible_worker` or wait for node return/provisioning |
| Raft leader/quorum unavailable | **No effect** on core request; expose optional coordination status separately |
| No usable local backend and no remote workers reachable | `503 no_reachable_worker` when `wait_ms=0`; otherwise wait |
| Invalid/expired/stale worker token | `409 reservation_invalid` / `410 reservation_expired` |

- `slots=1` is the fully atomic MVP. `slots>1` can be supported **atomically on one worker**. If the request requires simultaneous slots on multiple workers, reject as unsupported until a separately specified cross-worker coordinator is implemented.
- Store idempotency for client acquire requests at the origin, and for slot claims at each target worker. The **same origin** must not book an extra slot when the caller retries. Cluster-global idempotency under network partitions is not promised.
- Execution uses `X-Idle-Herd-Reservation` through the origin's local inference endpoint. It routes directly to the selected worker and consumes the worker-issued token exactly once. The worker owns expiry, renewal, and release; origin may not extend a lease unilaterally.
- `GET /v1/capacity/reservations/{id}` and `DELETE /v1/capacity/reservations/{id}` inspect and release via the worker or return an explicit `worker_unreachable` result; local cache cannot claim authoritative active status for an unreachable worker.
- Provide `GET /v1/capacity/watch` (SSE) for availability transitions, or an SDK `acquire(wait=True)`; it watches **the origin's reachable peer set** and re-attempts admission, not a centralized capacity queue.

### Durable independent jobs

`POST /v1/jobs` accepts `{model, request, priority, idempotency_key, retry_policy, execution_policy, durability}` and returns `202 Accepted` plus a job ID. `GET /v1/jobs/{id}`, `DELETE /v1/jobs/{id}`, and result retrieval are served by the origin; peers with authorized replicated records may serve or recover jobs according to policy.

- **`durability: local` (default for one node):** Atomically persist complete job record/input or an immutable local blob reference before returning 202. Jobs survive native service restarts when the persistent data directory survives (and container restarts with a persistent volume in Phase 10), but cannot progress while their only copy's node is offline.
- **`durability: replicated` (optional for 2+):** Return 202 only after the local journal and a configured number of remote peers acknowledge durable copies of both required metadata and payload (e.g., one additional copy). If insufficient peers are online, return a clearly specified unavailable response, or downgrade **only if the caller explicitly permits** `allow_durability_downgrade=true`.
- **Work takeover:** A surviving replica can recover an eligible, retry-safe job when origin is unavailable. With no quorum/central arbiter, takeover is best-effort and may duplicate attempts across partitions. Use durable job IDs, attempt IDs, retry delays, and result reconciliation; never automatically retry side-effecting/non-idempotent jobs.
- **Origin recovery:** Reconnect and reconcile attempt and completion journals from replicas; never assume a disconnected worker was not running. Publish `unknown_outcome` when results cannot be established.
- **Input/output:** Keep large prompt/response blobs out of Raft and gossip. Store in local persistent volumes and optional secure replicated object store, content-addressed with encryption/ACLs. Job records use a bounded local SQLite WAL journal and crash-consistent updates.
- **API reachability:** If a user calls `localhost` on an offline host, that host cannot answer. A surviving host can accept new work; retrieving a job originally submitted elsewhere requires its journal/result to be reachable or replicated.
- **Streaming:** Failure after output starts is visible; a partial stream cannot be seamlessly replayed with an exactly identical continuation.

Job states: `queued -> reserving -> running -> completed`, plus `retry_pending`, `canceled`, `failed`, and `unknown_outcome`. Store `origin_id`, `attempt_id`, assigned worker, lease ID, last durable state, timestamps, replication acknowledgments, and attempt-specific results.

### Administrative API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/cluster/status` | Peer view, online count, optional Raft leader/quorum, current local health |
| `GET` | `/v1/cluster/nodes` | Per-peer capabilities and last-seen/freshness |
| `GET` | `/v1/cluster/models` | Available/compatible models across reachable peers |
| `POST` | `/v1/cluster/nodes/{id}/drain` | Authenticated advisory drain; worker enforces local policy |
| `GET` | `/v1/models/transfers` | Authorized model transfers and status |
| `POST` | `/v1/models/ensure` | Install/load model on eligible peer without consensus dependency |
| `GET` | `/healthz` | Local service liveness |
| `GET` | `/readyz` | Local readiness; report remote/discovery health separately |
| `GET` | `/v1/capacity/watch` | Optional SSE for local best-effort capacity changes |
| `GET` | `/v1/node/status` | Authoritative **local** worker state for menu bar/CLI; mode, active job counts, capacity, idle reason, health, resource use, uptime |
| `GET` | `/v1/node/jobs` | Locally running and queued jobs (safe summaries; no prompts/completions by default) |
| `GET` | `/v1/node/events` | Authenticated local SSE for job, mode, capacity, idle-state, transfer and peer-reachability transitions |
| `PUT` | `/v1/node/mode` | Set `auto`, `draining`, `offline`, or `paused`, with optional expiry; privileged **local** management only |
| `POST` | `/v1/node/jobs/{id}/cancel` | Best-effort cancel when caller owns or is authorized for the job; report cancellation vs `unknown_outcome` precisely |
| `GET` | `/v1/node/diagnostics` | Explain local scheduling eligibility, backend failures, model fit, and sensor provenance (no private input) |
| `GET` | `/v1/node/preferences` | Read per-user idle policy, concurrency, battery/thermal/network limits, and notification settings |
| `PATCH` | `/v1/node/preferences` | Versioned, authenticated policy updates; atomically validate and persist preferences |

### Local node mode and UI contracts (native-first)

The macOS menu bar app and CLI use the **same local management API**. The peer daemon owns state, persists durable preferences, performs all admission decisions, and continues working if the UI quits or crashes. The UI must never manipulate the worker's SQLite ledger directly or own a second scheduler.

| Mode | Accept new remote jobs? | Dispatch locally-originated jobs to peers? | Local interactive inference? | Existing work |
|---|---|---|---|---|
| `auto` (default) | Only when local idle and safety policy allow | Yes | Yes, subject to explicit local policy | Continue |
| `draining` | **No** | Yes | Yes | Allow running jobs to finish; new remote leases refused immediately |
| `offline` | **No**; withdraw from discovery and deny admission even if peers have stale gossip | **No** | **Yes** (localhost only) | Drain existing remote jobs by default; explicit cancel is separate |
| `paused` | **No** | **No** | **No new inference jobs** | Running work finishes by default unless user explicitly cancels |

- `offline` means **cluster-disconnected**, not network-device-offline and not a kill switch. Keep localhost health/status/settings and locally requested inference available; do not auto-send local requests to remote peers. `paused` is the explicit no-new-inference mode. The UI must show this distinction.
- All transitions apply at the local worker **before** being gossiped; stale remote peers cannot override offline/draining/paused. Pending reservations are invalidated/declined according to fencing and lease policy; running jobs follow configured graceful-drain behavior.
- `PUT /v1/node/mode` accepts an optional `expires_at` or `duration_seconds` for a temporary override (e.g., offline for one hour). Persist until expiry/reboot according to user policy; default manual `offline` stays in effect across service restarts. On expiry, return to `auto`, not forced available.
- Administrative operations require a separate local control credential (macOS Keychain for the menu app; scoped secret for CLI; protected file permissions or platform credential store). **Loopback binding alone is not sufficient authorization.** Disable cross-origin browser access by default and defend against CSRF; never expose control endpoints on the peer RPC listener. Read-only telemetry may use a less privileged scoped token.
- `GET /v1/node/status` reports authoritative **local** admission and running-job counts, with timestamps and resource sampling provenance. Cluster totals are separately marked `reachable_estimate`; no omniscient live-cluster count is implied. Redact prompt and completion bodies, path secrets and credentials from the tray by default.
- `GET /v1/node/events` uses SSE with heartbeat, event IDs/reconnect, and snapshot-on-reconnect; the menu app uses backoff polling as a fallback if SSE is unavailable. Event payloads and error contracts are versioned and tested.
- Distinguish *owner activity* (idle/active), *worker eligibility* (can accept), *node mode* (auto/offline/etc.), and *service health* (daemon online/offline). Never collapse these into a single ambiguous green/red status.

### Scope and correctness warnings

Never report an approximate cluster capacity read as proof of slots. Never return `503 cluster_no_quorum` for ordinary inference, acquisition, or model transfer. A hard leader/quorum dependency is **only** allowed on specifically marked optional strongly consistent *administrative* endpoints. Capacity response must include visibility/freshness; acquire response must come from a live worker admission path.

---

## 5. Cross-platform backend adapters and native-first packaging

**Sequencing decision:** Implement and validate native peer services and native OS idle signals first. **Docker/Compose is Phase 10, the final phase**, not a prerequisite for single-node, multi-node, model-sharing, or UI milestones. Keep backend interfaces container-compatible, but do not spend early milestones on OCI images, Compose, host-container discovery, GPU passthrough or container-only test environments.

### Backend interface

Implement `InferenceBackend` (Python protocol/abstract interface):

```python
class InferenceBackend(Protocol):
    async def health(self) -> BackendHealth: ...
    async def list_models(self) -> list[ModelDescriptor]: ...
    async def can_run(self, spec: WorkloadSpec) -> CompatibilityResult: ...
    async def estimate_resources(self, spec: WorkloadSpec) -> ResourceEstimate: ...
    async def warm_model(self, model: ModelDescriptor) -> None: ...
    async def execute(self, spec: WorkloadSpec) -> AsyncIterator[bytes]: ...
    async def ensure_model(self, manifest: ModelManifest) -> InstallResult: ...
```

Initially implement and smoke-test `OllamaBackend`, native `MLXBackend` on Apple Silicon, and `OpenAICompatibleBackend` for LM Studio/vLLM/llama.cpp where supported by upstream. Capability descriptors record modality, supported API shapes, architecture/ABI, context window, accelerator, model format and actual resource fit. `DockerModelRunnerBackend` remains an adapter option but **container integration testing and packaging wait until Phase 10**.

### Native host install and lifecycle

| Host | First-class deployment | Idle sensors / controls | Later packaging |
|---|---|---|---|
| macOS (Apple Silicon and supported Intel) | Python/uv or Homebrew-style package + `launchd` user service | Native host helper and optional SwiftUI menu bar companion | Phase 10 Docker, using host inference for Metal |
| Linux (x86_64/arm64) | Python/uv package + `systemd --user` or system unit as appropriate | CPU/GPU/memory/thermal metrics; user-session idle where available; headless mode | Phase 10 Docker + optional NVIDIA backend integration |
| Windows (supported x86_64/arm64 as validated) | Python package + Task Scheduler or Windows service/notification-area app when ready | Native host helper where available; conservative idle fallback | Phase 10 Docker Desktop/host backend |

- Service lifetime is **independent of desktop UI lifetime**. UI quitting must not interrupt job execution; service crashing must show a disconnected state and offer diagnostics rather than silently showing idle.
- Provide `idle-herd start`, `idle-herd status`, `idle-herd peers`, `idle-herd offline --for 1h`, `idle-herd auto`, `idle-herd drain`, `idle-herd pause`, and `idle-herd doctor` where CLI naming fits existing upstream commands. All mode actions call the same local API as the menu bar app.
- Persist per-host identity, trusted peers, preferences, job journals and model cache in OS-appropriate user data paths. Avoid requiring root/admin privileges for normal inferencing or menu bar operations.
- Native `launchd`/`systemd`/Windows lifecycle must be tested with restarts, sleep/wake, network changes, local VPN use, and login/logout. Avoid managing Ollama or other backends destructively when the user owns those processes.
- Use an authenticated local control interface rather than depending on privileged shell calls or direct SQLite mutation from a UI.

## 6. Peer-to-peer model sharing

### Data model

Each node advertises *model manifests* rather than only friendly names:

```json
{
  "model_id": "example-model-4bit",
  "digest": "sha256:...",
  "format": "mlx-safetensors",
  "quantization": "4bit",
  "backend": "mlx",
  "architecture_requirements": {"os": "darwin", "cpu": "arm64", "accelerator": "metal"},
  "files": [{"path": "weights.safetensors", "sha256": "...", "size_bytes": 1048576}],
  "total_bytes": 1048576,
  "provenance": "huggingface-repository-and-revision",
  "license": "source-license-identifier",
  "share_policy": "cluster"
}
```

This manifest is illustrative; full installs will also include tokenizer, config, adapters, templates, and any backend-specific files. Preserve exact upstream repository revision and content digests to prevent name collisions.

### Compatibility matrix

| Source artifact | Destination | Transfer policy |
|---|---|---|
| GGUF used by Ollama / llama.cpp | macOS, Linux, Windows with compatible backend | Usually portable as a file; **validate architecture/operators, quantization, context, GPU/CPU support and backend version** |
| MLX safetensors | Apple Silicon macOS with functioning MLX/Metal-capable backend | Yes, after manifest and backend compatibility checks; **not** useful for generic Linux NVIDIA inference |
| CUDA-oriented safetensors / runtime assets | Linux/Windows GPU backend | Only if engine, tensor layout, GPU capability, quantization and runtime versions match |
| Ollama model registry object | Another Ollama node | Prefer content-addressed blobs and supported Ollama model APIs; don't write undocumented private directories blindly |
| Docker Model Runner OCI model artifact | Another DMR-compatible node | Prefer registry/OCI semantics or validated import support; test actual management API |

### Transfer workflow

1. Destination queries its **own cached peer inventory and live reachable sources** for digest, preferred backend, size, source, license and share permission; there is no mandatory leader. **Model provisioning is opt-in per model/policy**.
2. Select source preferring same LAN, healthy peer, transfer capacity and allowed sharing; fall back to configured official registry if no peer source is compatible.
3. Ensure destination has **disk space >= model bytes + temporary working headroom**, plus a configurable reserved free-space minimum. If a loaded model is required, separately check anticipated RAM/VRAM, context/KV cache and backend support; disk capacity alone is insufficient.
4. Acquire a **destination-local** persistent `model:digest` import/download lock and idempotency token; prevent concurrent downloads to the same destination. No global consensus is needed.
5. Use authenticated chunked HTTP transfer, resumable offsets/ranges, rate limiting and per-chunk/final SHA-256 verification. Never trust a peer-supplied absolute filesystem path. Ignore symlink/traversal entries and unexpected file types.
6. Stage files in a temporary directory; atomically publish only after full verification and validation; preserve manifests, licenses, provenance and access policy. Backend adapter completes import; `ready` only after backend confirms it can load.
7. Publish inventory by authenticated gossip, optionally prewarm under idle budget, and notify local/remote waiting capacity requests; peers offline during notification learn the update on reconnect.
8. On source disconnection/transfer failures, try another verified matching source or wait, then resume or discard partial data according to disk policy. No cross-format conversion in MVP; optionally support explicit offline conversion later.

**Eviction policy:** Never delete a manually installed or pinned model automatically. An Idle-Herd-managed cache can use LRU/TTL subject to a minimum free-disk threshold, only after ensuring model is not loaded, transferring, or reserved for queued work. Expose manual approval and disk quotas.

**Security/licensing:** No automatic sharing of private HF repositories, gated weights, proprietary models or user fine-tunes without explicit policy. Do not send registry tokens or full file paths to peers. Account for local storage encryption and authenticated model transfer endpoints.

### Rollout order

MVP transfers **GGUF/Ollama artifacts first**, then MLX snapshots among Apple Silicon peers, then OCI/DMR artifacts. MLX transfer must be a real tested end-to-end scenario, not just a filename copy.

---

## 7. Proposed project modules and separation of concerns

```text
src/fleet_manager/                         # initially preserve upstream Python package
  peer/
    app.py                                 # symmetric local ingress + worker runtime
    discovery.py                           # authenticated seed exchange/gossip; no leader dependency
    peer_client.py                         # retries, TLS, direct request forwarding
    admission.py                           # WORKER-local atomic lease/slot owner
    local_ledger.py                        # durable lease generations/incarnations/reconciliation
    scheduler.py                           # candidate ranking and direct negotiation
    waiters.py                             # origin-local queue, notifications, re-discovery
    stream_proxy.py                        # direct peer response streaming
  capacity/
    models.py                              # Pydantic model, resource, slot, freshness contracts
    policy.py                              # idle/busy/drain eligibility
    sensors/
      base.py
      linux.py
      macos_helper_client.py
      windows_helper_client.py
      container.py
  jobs/
    schemas.py
    journal.py                              # origin-local WAL and recovery
    lifecycle.py
    attempts.py                             # retryable attempts, unknown outcome
    replication.py                         # opt-in durable cross-peer blob/job copies
    result_store.py
  backends/
    base.py
    ollama.py
    mlx.py
    docker_model_runner.py
    openai_compatible.py
  modelsync/
    manifest.py
    compatibility.py
    inventory.py                            # gossip inventory; live destination checks
    transfer.py
    local_install_lock.py                  # destination owns model import lock
    importers.py
  coordination/
    interface.py                           # optional admin metadata interface
    preferred_peer.py                      # best-effort coordinator, advisory ONLY
    raft_client.py                         # optional, MUST NOT block inference
  routes/
    cluster.py
    capacity.py
    jobs.py
    modelsync.py
    local_node.py                         # local status, mode, jobs, preferences, diagnostics
  local_control/
    modes.py                              # persistent auto/draining/offline/paused state machine
    events.py                             # local SSE events + reconnect snapshots
    preferences.py                        # user settings, validation, migration
    auth.py                               # local scope-separated control credentials
apps/
  macos/IdleHerdMenuBar/                  # Phase 6: SwiftUI MenuBarExtra, native helper
    IdleHerdMenuBar.xcodeproj
    Sources/                              # tray, local API client, events, preferences, UI tests
  windows/IdleHerdTray/                   # optional Phase 9: tray after native client parity
  linux/idle-herd-tray/                    # optional Phase 9: StatusNotifier/AppIndicator
consensus/                                 # OPTIONAL Go sidecar; not a core dependency
  cmd/idle-herd-consensus/
  internal/raftnode/
  internal/adminfsm/                       # cluster-admin metadata, no worker slot leases
  internal/api/
  internal/auth/
  internal/snapshot/
container/                                 # Phase 10 ONLY: do not build before native readiness
  Dockerfile.peer
  Dockerfile.consensus                     # optional profile
  compose.yaml
  compose.raft-optional.yaml
  compose.linux-nvidia.yaml
  compose.mac-dmr.yaml
  compose.windows-host-backend.yaml
docs/idle-herd/
  0001-symmetric-peers.md
  0002-churn-and-consistency.md
  0003-worker-local-leases.md
  0004-capacity-contract.md
  0005-idle-platform-sensors.md
  0006-replicated-job-limits.md
  0007-peer-model-sharing.md
  0008-optional-raft.md
  0009-local-mode-and-menu-bar.md
  0010-energy-network-privacy-policies.md
  api.md
  deployment.md
  security.md
  ui-and-controls.md
  troubleshooting.md
tests/
  test_peer/
  test_admission/
  test_capacity/
  test_jobs/
  test_backends/
  test_modelsync/
  test_optional_consensus/
  test_local_control/
  integration/
  native_macos/                           # Swift package/XCTest unit+UI tests
```

Implementation requirements:

- The entire Python peer must start, schedule, run, and store local jobs with `IDLE_HERD_CONSENSUS_MODE=disabled` and no Go sidecar running.
- Preserve upstream routing/streaming adapters and model/scoring logic until compatibility tests pass; do not rewrite every module at once.
- Keep peer membership (authorization and gossip), model inventory (best-effort), worker slot ledger (locally authoritative), job journal (origin-owned), and optional Raft state (administrative) in **separate data domains**.
- Never copy a worker's live slot ledger to another node and allow the replica to issue leases. Worker-restart reconciliation is local; a backup of its data is not a second authority while original worker may still run.
- Use versioned messages, consistent `node_id`/`worker_incarnation` naming, monotonic per-slot generation, idempotency keys, and enough request tracing to understand partition behavior.

---

## 8. Native macOS menu bar companion and host ownership controls

### Product intent and non-goals

The menu bar companion is a **lightweight control surface for a single local Idle-Herd daemon**, not a centralized cluster router or a requirement for Linux/Windows servers. Build a real macOS SwiftUI `MenuBarExtra` application (`LSUIElement=true`, optional `.menuBarExtraStyle(.window)` for a richer popover). Read status over the local authenticated API and subscribe to `/v1/node/events`. Keep the daemon, idle observer and all scheduling logic outside the SwiftUI view layer. Cross-platform equivalents are **optional**: Windows notification-area and Linux StatusNotifier/AppIndicator clients can reuse the same protocol after macOS validation. Headless setups always retain CLI/API parity.

### Menu bar mock layout and behavior

```text
Idle-Herd  ● 2 jobs            [status glyph + active-job count]
┌─────────────────────────────────────────────┐
│ Idle-Herd — Mac Studio                        │
│ Auto · Idle for 18m · Accepting work         │
│                                              │
│ LOCAL JOBS                                   │
│ ● 2 running · 1 queued · 1 reserved          │
│   qwen3-coder:30b   01:42   ~22 tok/s        │
│   gemma:27b         00:38   ~31 tok/s        │
│                                              │
│ RESOURCES                                    │
│ CPU 18% · Unified memory 24 / 36 GB          │
│ GPU 68% · 1 / 4 inference slots available    │
│ Power: AC · Battery: n/a · Temperature: OK   │
│                                              │
│ CLUSTER (reachable estimate)                 │
│ 3 peers known · 2 reachable · 1 stale        │
│ ~4 remote slots indicated (not reserved)   │
│                                              │
│ MODE                                         │
│ (●) Automatic   ( ) Drain   ( ) Offline      │
│ ( ) Pause all new inference                  │
│ [Offline for 1 hour] [Back to Automatic]    │
│                                              │
│ [Jobs…] [Models…] [Open dashboard]           │
│ [Preferences…] [Diagnostics…] [Quit UI]       │
└─────────────────────────────────────────────┘
```

*Example values are illustrative; UI must show `unknown` instead of guessing metrics unavailable on a given hardware/backend.*

### Minimum useful capabilities

| UI feature | Specification / expected behavior | Priority |
|---|---|---|
| Active job indicator | Menu bar shows 0/1/N currently **local** running jobs; optional animated glyph while running, with reduced-motion support. Distinguish locally originated jobs currently executing remotely in popover. | P0 |
| Mode control | `Automatic`, `Drain`, `Offline`, `Pause new inference`; label and explain distinct semantics in §4. Confirm destructive actions; modes persisted by daemon. | P0 |
| Graceful offline | Default **finish running jobs**; reject new peer admissions immediately; menu shows draining state until complete. Separate explicit cancel action. | P0 |
| Local metrics | CPU, GPU where measurable, memory/unified-memory and pressure, available slots, backend/model, age of telemetry; show unsupported as unknown. | P0 |
| Cluster summary | Known/reachable/stale peers, best-effort eligible slots and last seen; never promise globally current state or guaranteed acquired slots. | P0 |
| Why unavailable? | Human-readable reason (`owner active`, `memory pressure`, `battery below floor`, `manual offline`, `no compatible backend`, `peer unreachable`), plus last sensor update. | P0 |
| Start at login / service status | User can install/enable auto-start; show daemon disconnected and retry safely; UI quitting does not stop daemon. | P0 |
| Per-job information | Model, elapsed time, throughput where available, origin/local vs remote, ETA only if meaningful, cancel action with authorization. Hide prompts/responses by default. | P1 |
| Time-limited overrides | Offline/pause for 30 minutes, 1 hour, until next login or indefinitely; auto-resume to **policy-based auto**, never force a busy machine available. | P1 |
| Policy/preferences | Minimum idle duration, maximum CPU/GPU load, RAM reserve, maximum slots, laptop battery threshold, AC-only, thermal cutoff, idle hours/quiet hours. | P1 |
| Model inventory/transfers | Installed/loaded/in-flight models, size, cache use, copy progress, disk allowance, authorize/deny peer transfer, evict only eligible managed artifacts. | P1 |
| Alerts | Opt-in notifications for repeated backend failure, model transfer disk exhaustion, job failure, lost all peers; silence normal job completions by default. | P1 |
| Network and privacy | Trusted-network profiles, Tailscale/LAN availability, peer allowlist and per-model sharing permissions, bandwidth limits and UI controls for privacy-sensitive machines. | P1 |
| Activity insights | Local jobs/tokens/compute time per day, successful/failed requests, jobs served to peers, estimated power only when grounded; all retained locally by default. | P2 |
| Cross-platform tray | Windows notification-area client and Linux tray client using same local API, optionally sharing visual spec; CLI/API always sufficient. | P2 |

### Privacy, accessibility and service separation

- No unapproved Apple event or screen-capture permission for idle detection. Prefer low-privilege aggregate last-input-time APIs (where available), CPU/thermal/memory pressure, and conservative fallback. Clearly disclose what is sampled and keep raw keystrokes, application contents and private prompts out of telemetry.
- Make icons accessible with text/status shape, VoiceOver labels, keyboard operation, and an option to hide job counts from the public menu bar. Respect system reduced motion.
- Enable start-at-login via standard user-level mechanisms; a separate service can continue if the menu app is closed. A UI-only `Quit` must not terminate the peer daemon.
- Menu commands are authenticated against **localhost control** and must surface failures; never optimistically display `Offline` until the daemon confirms its transition.
- Avoid claiming real-time, cluster-wide status: a disconnected peer has an unknown current state even if its last report was idle.

### Additional features recommended for Idle-Herd

These features strengthen the original product premise and should be recorded as explicit roadmap items (not all required in the first menu bar milestone):

1. **Idle confidence and policy explainability (P0/P1):** Report exactly why admission is enabled/disabled and the age/source of idle signals; make user-activity detection overrides visible.
2. **Owner-first scheduling / fast drain (P0):** Prefer the local user's interactive work over donated inference; stop new remote admissions immediately on activity, and configure whether running work is allowed to finish or is explicitly cancellable after a grace period.
3. **Power and thermal safety (P1):** AC-only option, battery minimum, low-power mode handling, thermal cap, and separate presets for desktop and laptop. Avoid permanently claiming `available` while asleep.
4. **Quiet hours and timed offline (P1):** Rule-based schedules and convenient short-term suspend controls. Distinguish scheduled policy from a user's manual persistent offline choice.
5. **Per-peer trust and contribution quotas (P1):** Approve peers, limit max concurrent jobs and tokens/time per peer, allow local-only models, and define priority between owner jobs and remote requests.
6. **Transfer controls and disk hygiene (P1):** Model-sharing opt-in, disk quota, minimum free space, bandwidth limits, checksum and license verification, and never delete manually installed models automatically.
7. **Cluster diagnostics / `idle-herd doctor` (P1):** Explain discovery failures, NAT/firewall issues, asymmetric trust, GPU/model incompatibility, token expiry, and why `acquire()` is blocking.
8. **Sleep/wake, network roam and reconnection (P0):** Stop advertising stale capacity, revoke or fence old reservations, restart discovery quickly, and restore chosen mode without remote coordination.
9. **Workload classes and fair use (P2):** Interactive vs background, job max runtime, per-origin fair share, and optional budget for how much a personal machine contributes.
10. **Safe update/compatibility path (P2):** Node protocol version negotiation, staged upgrades, config migrations, and an explanation when an outdated peer cannot serve a newer model or workload.

The MVP should implement items 1, 2, and 8 as part of idle admission/peer reliability; the rest can be follow-on settings and diagnostics rather than blocking the core fork.

---

## 9. Incremental implementation milestones for Codex in VS Code

**Development order matters:** Build a reliable single-node peer and **worker-owned lease ledger first**, then add discovery, multi-peer dispatch, idle sensors, blocking capacity and jobs, a native menu bar companion, model sharing and optional Raft/admin coordination. **Dockerization is the final Phase 10.** Never implement Raft as the request scheduling dependency and later try to work around quorum loss.

### Phase 0 — Verify upstream and lock failure contracts

- [ ] Fork Ollama Herd; pin upstream commit; run existing CI/tests and note all supported APIs/backends.
- [ ] Inspect actual router/node boundaries; identify reusable streaming proxy, scorer, queue, model logic and telemetry.
- [ ] Write ADRs for symmetric peers, Raft-free admission, node churn, ID/credential handling, job durability, and local daemon/UI separation; explicitly pin Docker to Phase 10.
- [ ] Define invariants in tests: N=1 works, loss of all coordination services doesn't stop local work, per-worker overbooking impossible, no false globally authoritative capacity.
- [ ] Define security boundary and peer enrollment; prevent accidental exposure of unauthenticated endpoints.

**Acceptance:** Baseline passes; OpenAPI changes drafted; an ADR explicitly rejects Raft-global leases and explains why.

### Phase 1 — Production-capable **single-node mode**

- [ ] Start local ingress, inference adapter and worker agent together with `idle-herd`.
- [ ] Expose `http://localhost:11435` even with **no peers, no seeds, no consensus service**.
- [ ] Implement a worker-local serialized, persisted slot ledger: reservation, token, consumption, release, expiry, process-restart reconciliation and crash-safe generation.
- [ ] Keep old OpenAI/Ollama routes, streaming/embeddings, and local model inventory compatible.
- [ ] Add per-model fit checks and a minimal local `GET capacity` and `POST acquire` path.

**Acceptance:** With one native service process and offline network, run and stream models; concurrent clients never exceed configured backend capacity; restart preserves identity and rejects stale lease tokens.

### Phase 2 — Peer-to-peer discovery and dispatch with intermittent nodes

- [ ] Add signed identity, trust anchor, seed discovery, bidirectional peer exchange, TTL reachability cache and optional LAN mDNS.
- [ ] Authenticate peer RPC and directly negotiate reservations with receiving worker; **no elected leader** involved.
- [ ] Route requests through local ingress and stream responses directly from accepted workers.
- [ ] Implement churn handling for a peer joining/leaving/restarting, stale capacity records, and firewall/NAT failure diagnostics.
- [ ] Test one-peer, two-peer, three-peer, and partitioned groups, including intentional leader/seed outages.

**Acceptance:** With two peers, either can dispatch to the other. Shut off either node: survivor still serves its local backend. Bring peer back: it rejoins automatically. With three peers, shut down two: remaining one works. Break the network into 2+1: both sides execute locally.

### Phase 3 — Idle-awareness on each OS

- [ ] Define typed `IdleSignal`/`EligibilityDecision`; local policy controls all admission.
- [ ] Add macOS native helper option, Linux resource/desktop signals where accessible, Windows native helper option, and `utilization_only` fallback for headless/native servers; defer Docker sensor integration to Phase 10.
- [ ] Add user-activity drain, hysteresis, memory pressure, power/thermal safeguards and policies for interactive versus background requests.
- [ ] Test `unknown` policy and host helper failure; avoid treating a container's own CPU statistics as the host user's activity.

**Acceptance:** A worker becoming busy declines new background work regardless of any peer's stale snapshot. A return to idle wakes waiting requesters. One node remains functional without desktop sensors when explicitly configured for utilization-based mode.

### Phase 4 — Blocking capacity, fairness and parallel-job SDK

- [ ] Implement final `GET /v1/cluster/capacity` visibility-aware response and `POST /v1/capacity/acquire` with `wait_ms`, release, inspect, idempotency, deadlines and `429`/`504`/`503` conditions.
- [ ] Implement per-origin bounded fair waiter lists that re-check live worker admission on peer joins, slot releases, idle state changes, model installation, and timer fallback.
- [ ] Add `GET /v1/capacity/watch` (optional) and a Python helper `async with idle_herd.acquire(model, timeout=...)`.
- [ ] Validate N-way contention and cancellation. Keep cross-worker atomic multi-slot acquisition explicitly unsupported in MVP.

**Acceptance:** With 2 slots across changing peers, 20 simultaneous callers never double-book either worker, waiters unblock when available, and one online node never incorrectly receives `cluster_no_quorum`.

### Phase 5 — Local durable jobs and optional replicated recovery

- [ ] Implement SQLite WAL journal on the origin, persistent payload/result storage, job state machine, idempotency and retry policy.
- [ ] Define `durability=local` and optional `durability=replicated` with explicit ack count, persistence and downgrade semantics.
- [ ] Add replica recovery for **retry-safe** jobs with attempt IDs, bounded takeover delays, outcome reconciliation, and an explicit possible-duplicate warning under partitions.
- [ ] Prevent unsafe automatic retry for tools/actions with side effects. Expose `unknown_outcome` where attribution is uncertain.

**Acceptance:** One-node jobs survive native service restarts; two-node replicated jobs can resume on surviving peer only when the complete accepted replica is present; no promises of completion while all relevant durable copies are offline; long jobs can be retried under documented at-least-once semantics.

### Phase 6 — Native macOS menu bar app and local owner controls

- [ ] Implement `GET /v1/node/status`, `GET /v1/node/jobs`, `GET /v1/node/events`, `PUT /v1/node/mode`, `GET /v1/node/diagnostics`, `GET/PATCH /v1/node/preferences`, and authorized cancel commands; create explicit local control/read-only auth scopes.
- [ ] Implement persistent `auto`, `draining`, `offline`, `paused` state machine. **Offline stops cluster participation but preserves localhost inference**; paused rejects new inference. Apply mode before gossip and reject stale remote reservations.
- [ ] Create native SwiftUI `MenuBarExtra` macOS app; show running jobs/busy icon, backend and resource usage, peers/reachability, transfer status, idle eligibility reason, and service health. Use a snapshot + local SSE stream with polling fallback.
- [ ] Provide `Automatic`, `Drain`, `Offline`, `Pause`, `Offline for 1 hour`, `Back to Auto`, and `Open dashboard` actions. All actions use the daemon's local management API and provide error feedback.
- [ ] Integrate `launchd` user service + launch-at-login preferences; UI closing must leave daemon running. Protect local mode controls with scoped credentials; don't expose them on peer listener.
- [ ] Unit-test state transitions, TTL expiry, service restarts and user-active conflicts; add XCTest UI tests and integration tests using a mocked local API, including UI disconnected/reconnect state.

**Acceptance:** On one Mac, the menu bar shows an active job and real local stats (unknown where sensors unsupported). Toggling Offline immediately prevents peer admissions but localhost inference continues; existing running jobs finish by default. When daemon or UI restarts, settings and service behavior are correct. Remote peer loss does not freeze the UI.

### Phase 7 — Peer-to-peer model replication and provisioning

- [ ] Build digest-based inventories, compatibility rules (GGUF/Ollama, Apple Silicon MLX, backends/quantization), share permission checks and destination disk/RAM budget.
- [ ] Implement destination-owned installation locks, authenticated resumable chunk transfer, checksum validation, staging, atomic import and optional registry fallback.
- [ ] Do not require the leader for transfers, and avoid cluster-global locks. Prevent duplicate destination downloads locally, with useful retry behavior after source node disappears.
- [ ] Implement managed cache limits, privacy/license controls, source preference, and eventual reconciliation on peer return.
- [ ] Publish transfer status and model inventory to local node events for the menu bar, including pause/retry and disk-rejection reasons.

**Acceptance:** A compatible peer copies a permitted model from a currently reachable source and successfully loads it, even with no coordinator. If source disappears mid-transfer, partial content remains untrusted until verified. Incompatible MLX->Linux target is rejected; menu bar displays accurate transfer state.

### Phase 8 — **Optional Raft coordination / advisory leader election**

- [ ] Add a Go HashiCorp Raft component as a **separately activated native optional service**, with a deterministic FSM limited to optional admin metadata/settings (not worker reservations, not durable job runtime).
- [ ] Support zero voters (default), one voter (optional), and 3/5 voters (when stable enough). A 2-voter setup may be allowed for experimentation but must warn it tolerates zero failures.
- [ ] Integrate Raft status into dashboard/local node status. On leader loss or no quorum, pause only quorum-dependent settings changes. Inference, local queues, peer admission, capacity wait and model transfer MUST stay functional.
- [ ] Implement advisory preferred-peer selection for non-critical background optimization when no Raft leader is available, with no correctness reliance on uniqueness.
- [ ] Test leader disappearance, quorum loss, voter restart, 2:1 partition, 1 of 3 still online, and reconnection. **Never silently self-bootstrap a new Raft group from a partition.**

**Acceptance:** Fault injection takes down every voter: any still-running peer completes locally admissible jobs. On restored quorum, optional metadata consensus resumes without invalidating worker-issued leases or durable local jobs.

### Phase 9 — Native desktop polish, diagnostics, chaos tests, release readiness

- [ ] Show each peer, platform/backend, model inventory, sensor provenance, slot admissions, waiters, replicated job state, optional Raft leader/quorum, partitions and last seen in the existing dashboard; ensure local menu and dashboard use the same event semantics.
- [ ] Add friendly `idle-herd doctor` with machine-specific capability/model fit explanations, permission/network health, and 'why isn't my machine eligible?' details.
- [ ] Add energy-aware/quiet-hours settings and peer contribution quotas; ensure owner activity always overrides optimistic availability. Add privacy-aware, opt-in notifications and basic per-job local history.
- [ ] Optionally prototype native Windows notification-area and Linux tray clients from the same local endpoints; they must not block the native CLI/API release.
- [ ] Instrument request ID/lease ID/worker incarnation, retry/unknown outcome and model transfer without storing prompts by default.
- [ ] Run native-process churn tests with 1, 2 and 3+ nodes, including all optional voters offline, random node flaps, restarts, sleep/wake, and partitions.
- [ ] Add native install/autostart docs, threat model, backups, rolling upgrade, upstream license attribution, performance benchmarks, accessibility and operational troubleshooting.

**Acceptance:** Every core API remains usable with only one worker alive regardless of prior cluster size. macOS menu bar is stable and accessible, has a trustworthy state after disconnect/reconnect, and is not required for the daemon to run. Cross-platform service tests identify actual verified platforms.

### Phase 10 — Dockerization and container deployment (**LAST PHASE**)

- [ ] Only after Phase 9 native readiness, build `linux/amd64` and `linux/arm64` OCI peer images plus Compose single/dual/multi-peer examples using **only the peer service** by default.
- [ ] Provide persistent identity, job journal/cache volumes, peer listen ports, host backend configuration, trust/secrets, firewall guidance, image version pinning, restart policies and health checks.
- [ ] Smoke-test host Ollama/MLX on Apple Silicon, Linux NVIDIA backend where available, Windows Docker Desktop host backend, and Docker Model Runner only where supported.
- [ ] Explain that generic macOS Linux containers do not receive native Metal access or accurate keyboard-idle signals; host inference and a native authenticated sensor helper remain necessary for host-aware policy. Container-only utilization policy must be explicit opt-in.
- [ ] Build Compose churn tests (1/2/3+ peers, seeds going down, optional Raft absent, no quorum) and test permission boundaries of local-control endpoints. Docker deployment must not require the menu bar app.
- [ ] Do not mount the Docker socket by default. Do not assume mDNS or Linux host networking works identically across macOS, Linux and Windows Docker deployments.

**Acceptance:** `docker compose up -d` runs one functional containerized peer with a reachable backend, persisted identity and jobs, and clear sensor limitations. Two and 3+ native/container mixed clusters keep scheduling with arbitrary node churn. Docker does not become a precondition for native install or menu bar use.

#### Deferred Docker implementation notes (reference only until Phase 10)

##### Containerized packaging

- Multi-arch `linux/amd64`, `linux/arm64` OCI images for the Python peer. Build a **separate optional** multi-arch Go consensus image. Linux/Windows/macOS use the same Linux containerized peer, but backend endpoints and host sensors differ.
- `docker-compose.yml` for a regular peer with **no Raft sidecar**; an optional `compose.raft-optional.yaml` or Compose `raft` profile on admins that want quorum-dependent metadata. Host volumes for peer identity, local ledger, job journal, caches, logs and optional Raft snapshots.
- Default bind local API to localhost on the *host* for clients; expose authenticated peer ports only to trusted LAN/Tailscale. Don't expose unauthenticated model APIs publicly.
- Do **not** mount `/var/run/docker.sock` as the default way to manage models; it grants broad Docker host privileges.
- On Docker Desktop, use published port mappings and seed URLs. Avoid assuming `--network host` gives physical host access or that mDNS/multicast works across platforms.
- For Linux GPU inference inside a container, provide an **opt-in** NVIDIA-specific Compose override. Don't make GPU passthrough a requirement for the control agent.
- On Macs, use host Ollama at `http://host.docker.internal:11434` (confirm host binding and firewall) or Docker Model Runner accessible to containers at `http://model-runner.docker.internal` (API subpaths depend on selected provider). A native MLX service can also be configured as an external local endpoint.
- Docker Model Runner on macOS runs inference engines **on the host, not inside an ordinary Linux application container**, and current Docker support includes `vllm-metal` for compatible Apple Silicon MLX models. Require a per-backend capability smoke test before advertising it.

##### Example Compose skeleton (developer template, not production-final)

```yaml
services:
  idle-herd:
    image: ghcr.io/YOUR_USERNAME/idle-herd:dev
    restart: unless-stopped
    ports:
      - "127.0.0.1:11435:11435" # local client
      - "11446:11446"           # authenticated peer RPC; firewall it
    environment:
      IDLE_HERD_NODE_ID: ${IDLE_HERD_NODE_ID}
      IDLE_HERD_SEEDS: ${IDLE_HERD_SEEDS}
      IDLE_HERD_BACKEND_TYPE: ${IDLE_HERD_BACKEND_TYPE:-ollama}
      IDLE_HERD_BACKEND_URL: ${IDLE_HERD_BACKEND_URL:-http://host.docker.internal:11434}
      IDLE_HERD_IDLE_MODE: ${IDLE_HERD_IDLE_MODE:-utilization_only}
      IDLE_HERD_DATA_DIR: /var/lib/idle-herd
      IDLE_HERD_CONSENSUS_MODE: disabled # optional Raft is NEVER on request path
      # Prefer Compose secrets / files for tokens, not inline plaintext.
    volumes:
      - idle-herd-data:/var/lib/idle-herd
    # Add an authenticated local-host sensor endpoint only if enabled.
volumes:
  idle-herd-data:
```

Production Compose must have separate secrets for node enrollment, CA and peer auth; health checks, network policy, security profiles, image version pinning, and restricted administration. The **optional** Raft override runs the Go binary in its own service with persistent store and a reachable authenticated Raft transport port. Peers function normally when no such container exists or when all optional voters disappear. Don't publish Go management API to untrusted interfaces.

### Release slices

- **Native MVP-A (essential):** Phases 0–4: one node, changing peer membership, worker-local reservations, blocking capacity and automatic native idle policy. **No Docker required.**
- **Native MVP-B:** Phases 5–6: local/replicated jobs, macOS menu bar controls, local status/events/preferences and native service lifecycle.
- **Native v1:** Phases 7–9: model sharing, optional Raft experiments (not on critical path), dashboard/diagnostics, real-hardware churn testing and owner-friendly settings.
- **Final packaging phase:** Phase 10 Docker/Compose for multi-arch deployments and native/container mixed integration.

---

## 10. Codex workflow in Visual Studio Code

### Setup and review discipline

1. Install the **OpenAI Codex VS Code extension** and authenticate. Clone the Idle-Herd GitHub fork into a workspace.
2. Place this Markdown file at repository root and ask Codex to read `AGENTS.md`, `CLAUDE.md`, `UPSTREAM_BASE_COMMIT.txt`, the actual repository tree, and this plan before writing code.
3. Complete one small numbered phase/PR at a time. Have Codex show a short change plan, modify code, add tests, run tests/lints, document assumptions and summarize verification. Record platform tests actually performed.
4. Maintain current upstream MIT license and attribution. Keep upstream's compat routes and streaming parity, and do not rewrite Python package imports all at once.
5. Update `docs/idle-herd/implementation-status.md` with only evidenced completions; write ADRs for changes to admission, persistence, or failure guarantees.
6. Reject scope creep: do not allow Codex to reintroduce a mandatory central router, mandatory Raft sidecar, quorum-guarded ordinary request path, or gossip-based slot acceptance.

### Starter Codex prompt (paste into extension)

> You are implementing **Idle-Herd**, a cross-platform, native-first, churn-tolerant peer-to-peer fork of Ollama Herd. Read `AGENTS.md`, `CLAUDE.md`, `UPSTREAM_BASE_COMMIT.txt`, and `IDLE-HERD_IMPLEMENTATION_PLAN.md`, then inspect the actual checked-out files. **Critical invariants:** (1) localhost OpenAI/Ollama API on EVERY node, NO central router; (2) N=1 works without peers or Raft; (3) N=2 or N>=3 may shrink to one surviving peer and that peer must still run local jobs; (4) worker-local atomic admission and durable fenced lease tokens; (5) cluster capacity is advisory while blocking acquisition retries against live worker admission; (6) optional Raft does NOT gate inference; (7) native OS execution/idle policy on macOS/Linux/Windows first; (8) **Docker/Compose integration ONLY in Phase 10**; (9) SwiftUI macOS menu bar companion uses versioned secure localhost control API and is NOT responsible for daemon lifetime; (10) `offline` withdraws a peer from the cluster but retains localhost inference, unlike `paused`; (11) verified compatible peer-to-peer model sharing; (12) honest at-least-once recovery and partitions. **For this task implement Phase 0 only**: audit upstream, run baseline tests, draft ADRs/OpenAPI, add local node mode/UI separation to architecture, and document native development/build/test paths. Do not implement future phases. Report exact changed files, commands/results, and remaining risks.

### Phase-specific prompts

| Phase | Focused Codex request |
|---|---|
| 1 | “Implement Phase 1. Make a native single-node peer expose the local OpenAI API and durable worker-owned slot reservations, with no Raft or discovery. Prove no overbooking and crash-safe fencing.” |
| 2 | “Implement Phase 2. Peer enrollment, authenticated discovery/gossip, remote direct admission/streaming. Prove a survivor works after arbitrary peer outages, with no fixed leader or seed dependency.” |
| 3 | “Implement Phase 3. Native host-aware idle policy/sensor interfaces for macOS/Linux/Windows; fail safely on unknown user-activity signals and provide explicit headless utilization mode. Defer container support.” |
| 4 | “Implement Phase 4. Visibility-aware capacity GET, blocking acquire with worker tokens and origin waiters. Test 20 clients/2 slots, churn, cancellation, and stale snapshots.” |
| 5 | “Implement Phase 5. Origin-local persistent job journal and optional replicated durability with acknowledgments and explicitly at-least-once recovery. Do not automatically retry side-effecting work.” |
| 6 | “Implement Phase 6. SwiftUI macOS MenuBarExtra with live active-job indicator, local/cluster stats, clear offline/drain/pause/auto modes and local secure management API. Daemon survives Quit UI; test restart and offline conflicts. Follow §8 mock and requirements.” |
| 7 | “Implement Phase 7. Authenticated peer-to-peer model sharing with digests, compatibility, resumable transfers, user permission and destination-local install locks; expose progress to menu bar events.” |
| 8 | “Implement Phase 8. OPTIONAL native Raft coordination strictly for admin metadata. Kill all voters and prove capacity acquisition/inference still works. No automatic voter rebootstrap.” |
| 9 | “Implement Phase 9. Dashboard, native installers/autostart, status diagnostics, idle policy/energy controls, local UI polish and churn testing for 1, 2, and 3+ intermittently connected peers.” |
| 10 | “Implement Phase 10 LAST. Build multi-arch OCI peer image and Compose examples for native/container mixed clusters; document host Ollama/MLX/DMR and sensor limitations; run container-specific tests without changing the quorum-free core.” |

### Standard completion instruction for every Codex phase

> Use typed interfaces and tests before major refactors when practical. Preserve existing API shapes and endpoint compatibility. Add concurrency/failure/partition tests, including N=1 and node-down tests for each phase. Run `uv run pytest`, `uv run ruff check src/`, `uv run ruff format --check src/`, and relevant `go test ./...` only when Phase 8 code exists; execute container/Compose tests only in Phase 10. Never claim a real OS/back-end integration was tested unless actually run. Stop after requested phase. Return modified files, test commands/output, invariants proven, unresolved risks, and next phase.

### Repository working documents

- Root `IDLE-HERD_IMPLEMENTATION_PLAN.md`: maintained architectural plan and phase roadmap.
- Root `AGENTS.md`: add fork invariants while preserving upstream instructions.
- `docs/idle-herd/adr-*.md`: rationale for worker-owned leases, optional Raft, partition behavior, job replication and recovery.
- `docs/idle-herd/api.md`: versioned OpenAPI source of truth with scope/freshness semantics.
- `docs/idle-herd/implementation-status.md`: phase checkboxes, PR links and test evidence.
- `docs/idle-herd/ui-and-controls.md`: mode semantics, menu bar mock, event schema, privacy/accessibility, signed local control commands, UI test plan.
- `apps/macos/IdleHerdMenuBar/`: SwiftUI app, optional native sensor connector, XCTests and packaging files (introduced Phase 6).

---

## 11. Test matrix and failure injection

### Availability/churn matrix (REQUIRED, not optional)

| Nodes originally configured | Nodes currently online | Optional Raft quorum | Expected core behavior |
|---:|---:|---|---|
| 1 | 1 | Disabled | Local inference, slots, blocking waits, persisted local jobs all work |
| 2 | 2 | Disabled | Both local endpoints and bidirectional dispatch work |
| 2 | 1 (either one) | Disabled or unavailable | Survivor serves local jobs; remote status marked offline; blocking waits work |
| 2 | 0 | Unavailable | No execution; persisted jobs resume after a node with data returns |
| 3 | 3 | Disabled | Capacity shared across compatible reachable workers |
| 3 | 2 | Disabled or unavailable | Both survivors negotiate slots and continue |
| 3 | 1 (any one) | Unavailable | Single survivor continues with local slots, no leader requirement |
| 3 | 0 | Unavailable | No execution; persistence limits apply |
| 5 | 2 | Unavailable | Connected pair can work; other offline peers excluded |
| 5 | 1 | Unavailable | Last reachable peer can work locally |
| 3 | 2+1 partition | Majority/minority in optional Raft | **Both** partitions may run local jobs; only quorum side may update strict admin state |
| 4 | 2+2 partition | Usually no quorum | Both components may execute on their own workers; no globally unique job claims |
| Any | Random peer flapping | Arbitrary | No false slot grants, leaks, or permanently stuck waiters; peers rejoin |

### Functional/failure tests

| Scenario | Expected result |
|---|---|
| Only one backend/model installed | Inference works from its own localhost, without any cluster coordinator |
| macOS/Windows/Linux mixed peers | Calls choose only compatible backend/format/accelerator |
| User becomes active on Mac | Worker stops new idle-only admissions, regardless of stale remote metrics |
| No host activity sensor in Docker (Phase 10) | `idle_only` is conservative; optional utilization policy works when expressly enabled |
| 100 clients on two origins compete for one worker slot | Exactly one active admission in that worker slot; others wait/retry |
| Two workers accept same logical retry in a partition | Possible; traced with unique attempt IDs; documentation makes no exactly-once claim |
| Request waits while every eligible worker busy | It wakes on slot release, idle transition, or peer join, and times out correctly |
| Request waits while no remote peer reachable | Local peer remains responsive; waits and reevaluates until deadline |
| Worker crashes with active reservation | Restart reconciles backend state; stale token/old incarnation refused |
| Origin crashes during job | Worker follows defined orphan policy; job record on origin survives restart if volume persists |
| Origin offline, job locally durable only | No misleading guarantee of execution by remaining nodes |
| Origin offline, replicated retryable job and payload acknowledged | Surviving replica may retry subject to at-least-once/duplicate risk |
| Source model peer vanishes mid-transfer | Transfer resumes from another compatible authorized source or waits; partial files never advertised as installed |
| Both source and destination have incompatible MLX/CUDA formats | Transfer rejected before download |
| Every Raft voter killed | Core inference/acquire/model sharing still works among surviving peers |
| 3-voter Raft leader killed | Optional admin leader re-elects only if quorum; inference unaffected either way |
| Separate partition signs forged admission tokens | Target worker refuses tokens it did not issue / cannot validate |
| Docker volume persists across restart (Phase 10) | Identity, worker ledger, journal, cache survive; no falsely reused slot IDs |
| Concurrent install of same model digest on one node | Exactly one destination-local import owns installation lock |
| No model available on any reachable peer | Clear `no_eligible_worker`/wait/provisioning state, no fake available slots |

### Local owner controls and menu bar tests (Phase 6 onward)

| Event | Expected outcome |
|---|---|
| Remote reservation arrives while user toggles `offline` | Worker rejects new lease immediately; stale peer advert does not permit admission; in-flight work drains by default |
| UI quits while two local jobs run | Daemon stays up, jobs continue, CLI and local API report accurate state |
| Daemon crashes or restarts | UI shows disconnected/reconnecting and never displays stale green idle state as live; authenticated reconnection restores snapshot |
| Mac sleeps, wakes, changes Wi-Fi or Tailscale connection | No phantom availability; reservations expire/fence; node obeys retained manual mode and reconnects as authorized |
| Offline for one hour expires while owner is active | Revert to `auto`, but remain ineligible for remote jobs until owner is idle and resource thresholds pass |
| Client tries unauthenticated `PUT /v1/node/mode` | `401`/`403`; settings unchanged; localhost/CSRF bypass disallowed |
| 1/2/3+ cluster online with one local active job | Menu bar local active count authoritative; cluster estimates annotated with reachability/freshness |
| UI reduces motion or uses screen reader | Meaningful accessible labels and job state without reliance on animation or color alone |
| Job output contains untrusted content or secrets | No prompt, completion or secret inadvertently placed in menu bar/status events/logs |

### Invariants to assert in automated property/chaos tests

1. Every admitted lease is worker-issued and bound to `(cluster_id, worker_id, worker_incarnation, slot_id, slot_generation, request_id)`.
2. A worker never has more admitted/running weighted-resource usage than its capacity, including during concurrent retries and after worker restart.
3. Gossip never issues reservations; it only ranks candidates. A formerly idle snapshot cannot overrule current worker busy/draining state.
4. Any healthy node with a compatible local backend can complete local inference without discovering, contacting, or electing another node.
5. A loss of any or all optional Raft voters cannot cause ordinary inference to return `cluster_no_quorum`.
6. A fresh local capacity view cannot claim global completeness while some known peers are offline or stale.
7. An optional Raft partition never triggers automatic voter rebootstrap or conflicting committed admin-history claims.
8. Origin-local job persistence is crash consistent; replicated durability is not acknowledged before the configured durable-copy threshold is reached.
9. Network-partition retries cannot be described as globally exactly once. Non-idempotent work does not automatically fail over.
10. Model formats, privacy flags, and destination capacity are checked before model transfer/import.

### Test layers

- Unit/property: slot ledger, idempotency, worker incarnation/fences, stale tokens, idle policy, capacity aggregation semantics, model compatibility.
- Integration (Phases 1–9): 1/2/3/5 **native processes** with mock model backends and proxy-injected network partitions; randomized churn tests. Add Compose topology only in Phase 10.
- Real hardware: native Apple Silicon with host Ollama/MLX; Linux with supported CPU/NVIDIA inference; native Windows host backend where supported. Add Docker Model Runner and Docker Desktop-specific tests in Phase 10. Don't label as verified without evidence.
- Security: pairing, revocation delays in partitions, path traversal and symlink transfer rejection, malicious peers, replay/DoS limits, loopback control-token scopes, localhost CSRF attempts and UI injection/redaction.
- Performance: wait queue latency, slot contention throughput, token-time overhead from peer proxy, model transfer bandwidth and idle sensor overhead.

### Definition of done (v1)

1. Single-node mode is a supported production setup; no peers or quorum required.
2. Any surviving peer can locally execute work after arbitrary other peers (including original leader/voters) go offline.
3. Per-worker capacity is never overbooked, across multiple origin nodes and partitions.
4. Capacity inspection states visibility, freshness and uncertainty; blocking reservation handles online/offline events without a global leader.
5. Native macOS/Linux/Windows peer packaging is supported with correctly scoped host-idle signals; **Docker multi-arch deployment is the final Phase 10 acceptance gate**.
6. Cross-platform backend tests and API compatibility pass.
7. Optional replicated jobs truthfully expose durability and duplicate-execution tradeoffs.
8. Model sharing is authorized, verified, resumable, compatible and independent of coordinator uptime.
9. Optional Raft leader election works when quorum exists, but loss of quorum only affects quorum-dependent admin state.
10. macOS menu bar clearly shows active jobs and trustworthy local metrics; `offline`/`draining`/`paused` work independently from the UI's lifetime, and secure API/CLI parity is tested.
11. Upstream MIT attribution, documentation, CI, chaos tests, and threat model are complete.

---

## 12. Decision record, risks, and suggested defaults

| Decision / risk | Default | Rationale |
|---|---|---|
| Critical-path Raft? | **NO** | Intermittent fleet cannot assume voter quorum; worker-local leases remove need for it |
| Optional Raft enabled? | **Disabled** initially | Avoid extra services and admin quorum failures for hobby/home setups |
| Optional leader election | HashiCorp Raft for strict admin metadata when available | Strong consistency only with quorum; no cluster-wide scheduling gate |
| Without Raft quorum | **Peer-to-peer operations continue** | Core user requirement: last live node still useful |
| Zero peers discovered | **Act as single-node cluster** | Works with one node and intermittent LAN |
| Two nodes | Both peers are ingress and workers; neither is mandatory leader | Either surviving node should continue locally |
| Stale peer capacity | Exclude from *available* count; probe worker before admission | Avoid phantom slots |
| Worker resource authority | Per-worker persistent slot ledger, local fencing, explicit acceptance | Never double-book a worker even across origin contention |
| Blocking capacity | Per-origin bounded fair waits with event and retry wakeups | Works without a globally available queue |
| Globally exactly-once jobs | **Not promised** | Partitions and independent origins can duplicate logical attempts |
| Strict singleton/non-idempotent jobs | Require external strong coordinator or disable automatic failover | Cannot get strict global safety plus no-quorum availability |
| Job durability with 1 node | Local journal + persistent volume | Survives restart, not destruction/offline period |
| Job durability with 2+ nodes | Optional acknowledged replication | Only surviving durable copies can resume work |
| Default new job retry | Retry-safe inference-only, bounded attempts | Avoid unintentional duplicate side effects |
| User returns during stream | Drain new background admissions; finish interactive stream by default | Avoid abrupt user-visible failures |
| Unknown idle signal | Ineligible for `idle_only` | Protect owner; headless user may opt into utilization mode |
| Model transfers | Destination-local import lock, signed/authenticated manifests, hashes | No leader dependency; safe retry after source leaves |
| Native deployment first | `launchd`/systemd/Windows lifecycle, UI only on desktop | Validate host idle/model execution before container abstraction |
| Mac GPU in Docker (Phase 10) | Docker peer + host Ollama/MLX/DMR | Generic Linux container can't assume native Metal access |
| Peer trust/revocation | Persistent identity and signed short-lived invites, eventual revocation | Partition prevents instantaneous global revocation |
| Data-plane routing | Origin directly to reserved worker | No central proxy or bottleneck |
| Menu bar daemon ownership | Separate SwiftUI client + Python daemon | Quitting the UI must not kill jobs |
| Offline mode | Stop inbound/outbound cluster work; keep localhost inference | Explicitly different from `paused` and `draining` |
| Local control security | Scoped token from platform credential store, loopback-only admin endpoints | A random local application should not be able to disable worker without authorization |

### Guarantees and hard limits to include in README

Idle-Herd guarantees only the following conditional property: **when at least one peer is online, the client can reach it, it has an eligible compatible backend, and local storage/resources are healthy, that peer can accept and execute work without contacting any other peer or leader**. If the only node that has a required model/job payload is offline, a different live node may be unable to run it until the model/data returns or is fetched from an authorized alternative. If zero machines are online, no work can run.

**A split-brain-safe globally unique leader and 100% availability for all network partitions are mutually incompatible without weakening guarantees or relying on another durable authority.** Raft cannot be made automatically quorum-free just by re-electing a leader among the remaining peers. Idle-Herd avoids that problem by keeping resource authority local to each worker and explicitly not promising global exactly-once execution or globally linearizable capacities.

### Questions for Phase 0 (defaults provided, not launch blockers)

1. Should every peer automatically become a worker? **Default: yes when backend compatible and local policy allows; otherwise ingress-only.**
2. Should `idle_only` be required on laptops and `utilization_only` on Docker/headless systems? **Default: host-aware auto selection with conservative unknown-state behavior and explicit opt-in policy.**
3. How long should wait requests last? **Default: bounded 60-second wait and 120-second maximum configurable; durable jobs for longer waits.**
4. How many copies before a job is called `replicated`? **Default: local + one verified durable replica; when one node only, reject replicated request unless caller explicitly downgrades.**
5. May a replica autonomously take over a running job when origin disappears? **Default: retry-safe jobs only, explicit possible-duplicate acknowledgement.**
6. Which backends first? **Ollama GGUF, then MLX on Apple Silicon, then Docker Model Runner/OCI; retain upstream additional backend support.**
7. What if a client requires a strict single logical execution? **Document that this requires an available authoritative store/consensus and reduced partition availability; don't silently claim the guarantee.**
8. Does `offline` terminate already-running remote inference? **Default: no; drain existing runs, refuse new peer jobs immediately; make cancel explicit.**
9. What is the UI packaging target? **Default: macOS SwiftUI MenuBarExtra first; Windows/Linux CLI now, optional tray implementations later.**
10. Does the macOS UI need to run to provide idle signals? **Default: no. Separate host sensor/helper if necessary; daemon must survive Quit UI.**

---

## 13. References and verification notes

**This document is an updated implementation specification, not a claim that the fork has been modified or tested.** Actual upstream module locations and dependency APIs must be checked by Codex against the pinned fork commit.

- Ollama Herd upstream repository, platform/backend support and current router+node architecture: https://github.com/geeks-accelerator/ollama-herd
- Ollama Herd coding-agent notes: https://github.com/geeks-accelerator/ollama-herd/blob/main/AGENTS.md
- Ollama Herd operational behavior and router retries: https://github.com/geeks-accelerator/ollama-herd/blob/main/docs/operations-guide.md
- HashiCorp Raft library, majority rules, nonvoting servers, and quorum loss: https://github.com/hashicorp/raft/blob/main/README.md
- HashiCorp Consul Raft quorum overview: https://developer.hashicorp.com/consul/docs/concept/consensus
- Docker Model Runner platform support and host/container separation: https://docs.docker.com/ai/model-runner
- Docker Model Runner API: https://docs.docker.com/ai/model-runner/api-reference/
- Ollama model/API reference: https://github.com/ollama/ollama/blob/main/docs/api.md
- Apple SwiftUI `MenuBarExtra` and agent app behavior: https://developer.apple.com/documentation/swiftui/menubarextra
- OpenAI Codex VS Code extension: https://marketplace.visualstudio.com/items?itemName=OpenAI.chatgpt
- Codex IDE workflow: https://learn.chatgpt.com/docs/codex/ide

**Revision:** November 1, 2026 (v3). Preserves quorum-free, worker-local reservations and single-survivor availability, introduces native macOS menu bar controls and local mode/status endpoints, and moves all Dockerization to final Phase 10. Optional Raft remains noncritical to inference. Any upstream guidance suggesting an always-on router or mandatory Raft must be interpreted using current §§0–4, 8–9, 11–12.

**End of plan.**

