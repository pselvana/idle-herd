# ADR 001: Symmetric peers and optional admin consensus

**Status: proposed Phase 0 decision; not implemented.**

Every peer is an ingress and, where locally eligible, a worker. N=1 is a complete deployment. Discovery and seed peers are conveniences, never dependencies after startup. Each connected component can execute against its own reachable eligible workers during arbitrary peer failures and partitions. A local backend, client reachability, healthy storage and resources remain prerequisites; no work runs with zero nodes.

Optional Raft may serialize strict administrative metadata when quorum exists. It is disabled by default, and loss of all voters leaves inference, capacity acquisition, waiting and model sharing operational. Quorum-dependent admin writes become unavailable. Never automatically rebootstrap voters after quorum loss: recovery is explicit and admin-guided. Signed local policy can still operate independently. **Reject Raft-global leases:** placing worker slot issuance, renewal or validation in consensus would make admission depend on quorum and violate survivor availability. Raft cannot give every partition a globally unique leader while accepting writes in every partition.

Consequences: peer views and capacity are partial; partitions can execute duplicate logical jobs. Neither globally ordered execution nor globally linearizable availability is promised. Strict singleton work needs an available external strong authority or an explicitly quorum-dependent service and must sacrifice partition availability.

Future acceptance: run native mock peers with 1, 2, 3 and 5 members; remove any peers until one remains, and verify local inference and admission without discovery or voters. Partition 3 into 2+1 and 4 into 2+2; both components execute local work, while quorum-dependent admin writes fail where quorum is absent. Kill all voters and verify core paths; ensure no voter rebootstrap occurs.
