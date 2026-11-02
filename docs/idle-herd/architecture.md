# Idle-Herd architecture: Phase 8 and target design

**Current runtime:** `idle-herd` combines localhost ingress, local Ollama inventory,
and durable worker admission in one native process. Phase 2 adds a separately
configured TLS peer listener, device-specific CA enrollment, signed peer exchange,
persisted endpoint reconnection and direct worker admission/streaming. Local
execution never contacts discovery or a seed. The origin tries its local worker
before directly negotiating a remote grant; remote capacity is advisory. Existing
wire translators and worker fencing serve both kinds of requests. See
[Phase 2](phase-2.md), [Phase 1 recovery](phase-1.md) and
[verification](implementation-status.md). Remaining sections describe future phases
where they go beyond these implementations. Legacy commands retain upstream behavior.

[Phase 3](phase-3.md) adds daemon-owned native observations and live worker-local
policy checks before granting and consuming durable leases. Interactive localhost
work defaults to `always`; peer work always uses the worker's background policy,
defaulting to `idle_only`. Unknown activity cannot grant idle-only work. Explicit
`utilization_only` supports native headless hosts. An eligibility transition drains
new admissions without cancelling or freeing running/quarantined execution.

[Phase 4](phase-4.md) adds visibility-aware capacity inspection, durable origin
idempotency and bounded fair retry rounds. Each round obtains a live worker grant;
snapshots never allocate slots. Origin deadlines and cancellation stop waiting
without declaring uncertain worker work stopped. Inspect/release and explicit
token execution route directly to the issuing worker. No global queue is required.

[Phase 5](phase-5.md) adds a separate origin-owned WAL job journal with bounded
input/result storage, durable attempt histories and optional acknowledged peer copies.
Only explicitly retry-safe jobs with duplicate consent may recover on another peer.
Reconciliation preserves independent attempts; cancellation cannot free worker
quarantine. Local jobs and inference have no replication or quorum prerequisite.

[Phase 7](phase-7.md) adds a private model-sharing journal and cache, worker-local
import locks, per-destination source permission and signed resumable chunks.
GGUF publication follows verification and an admitted Ollama load. No model
operation requires a coordinator. MLX/OCI import remains unsupported.

The target is a symmetric native peer: every peer has local ingress, an origin scheduler and journal, worker-local admission, backend adapters, authenticated discovery and host sensors. A peer may be ingress-only if its backend or policy is ineligible. The origin ranks candidates from a freshness-labeled peer view, asks a worker directly for admission, then dispatches using that worker's token. The worker owns its ledger and checks current resources and policy. Jobs, membership, model inventory, lease ledger and optional administrative consensus have distinct authorities; gossip cannot allocate resources. See the ADRs in this directory and the planned API contract in [api.md](api.md).

A reachable healthy peer with an eligible compatible local backend and healthy local storage/resources must accept and execute local work without contacting any other peer or leader. Either survivor of two peers, or any survivor of a larger fleet, retains this property. Both sides of a partition can run independent work. Zero live machines cannot execute work, and a survivor cannot execute a job or model available only on an offline machine. Global capacity, ordering and exactly-once execution are not promised.

Optional Raft is a separately activated native Go service limited to labels/notes
and off by default. Loss of quorum suspends admin writes only; inference,
acquisition, waiting, local jobs and model transfers never contact it. Python polls
status independently with bounded freshness. Advisory preferred-peer selection has
no correctness authority. No automatic voter rebootstrap is allowed. See
[Phase 8](phase-8.md), [ADR 006](adr-006-admin-consensus.md) and the original
[availability decision](adr-001-peer-availability.md).

The daemon owns policy and execution; a future SwiftUI menu bar app and CLI are authenticated clients of its local API, not its lifetime or scheduling authority. Native macOS, Linux and Windows development precedes Docker/Compose, which is reserved for Phase 10. Existing port assignments remain unchanged: Ollama 11434, router 11435, image 11436, STT 11437, vision embedding 11438, text embedding 11439 and MLX 11440+. Future peer interfaces must not silently reassign or collide with them.

Acceptance scenarios in the ADRs and [declarative test specification](../../tests/contracts/idle_herd.feature) are **future test requirements**, not evidence of current behavior. Native multi-process tests should use mock backends and injected partitions; real hardware claims require separate evidence. See the implementation plan for the full phase and failure matrix, and [ADR 005](adr-005-daemon-ui-and-native-sequence.md) for daemon/UI separation.
