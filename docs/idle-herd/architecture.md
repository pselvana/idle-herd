# Idle-Herd architecture: Phase 1 and target design

**Current runtime:** `idle-herd` combines local ingress, inventory refresh,
the Ollama adapter and durable admission in one native process. It registers
the existing inference translators with a local-only registry and a fenced backend
transport, without upstream's router lifecycle, discovery, heartbeat receiver,
preloader or telemetry. The ledger owns a shared cross-model slot/memory budget;
legacy queues only schedule candidates. See [Phase 1](phase-1.md) for code
boundaries, mock-process evidence, conservative crash quarantine and limitations.
The remaining architecture below describes future phases. Legacy commands retain
their original router/node lifecycle and additional backend integrations.

The target is a symmetric native peer: every peer has local ingress, an origin scheduler and journal, worker-local admission, backend adapters, authenticated discovery and host sensors. A peer may be ingress-only if its backend or policy is ineligible. The origin ranks candidates from a freshness-labeled peer view, asks a worker directly for admission, then dispatches using that worker's token. The worker owns its ledger and checks current resources and policy. Jobs, membership, model inventory, lease ledger and optional administrative consensus have distinct authorities; gossip cannot allocate resources. See the ADRs in this directory and the planned API contract in [api.md](api.md).

A reachable healthy peer with an eligible compatible local backend and healthy local storage/resources must accept and execute local work without contacting any other peer or leader. Either survivor of two peers, or any survivor of a larger fleet, retains this property. Both sides of a partition can run independent work. Zero live machines cannot execute work, and a survivor cannot execute a job or model available only on an offline machine. Global capacity, ordering and exactly-once execution are not promised.

Optional Raft, if implemented, is limited to strict administrative metadata and is off by default. Loss of quorum suspends quorum-dependent admin writes only; it must never block inference, acquisition, waiting, local jobs or model transfers. No automatic voter rebootstrap is allowed. See [ADR: peer availability](adr-001-peer-availability.md).

The daemon owns policy and execution; a future SwiftUI menu bar app and CLI are authenticated clients of its local API, not its lifetime or scheduling authority. Native macOS, Linux and Windows development precedes Docker/Compose, which is reserved for Phase 10. Existing port assignments remain unchanged: Ollama 11434, router 11435, image 11436, STT 11437, vision embedding 11438, text embedding 11439 and MLX 11440+. Future peer interfaces must not silently reassign or collide with them.

Acceptance scenarios in the ADRs and [declarative test specification](../../tests/contracts/idle_herd.feature) are **future test requirements**, not evidence of current behavior. Native multi-process tests should use mock backends and injected partitions; real hardware claims require separate evidence. See the implementation plan for the full phase and failure matrix, and [ADR 005](adr-005-daemon-ui-and-native-sequence.md) for daemon/UI separation.
