# ADR 006: separately activated administrative Raft

**Status: implemented Phase 8, November 1, 2026.**

Use a separate native Go process for optional strict administrative metadata.
HashiCorp Raft persists its own logs, stable state and snapshots; a deterministic
FSM allows only `cluster_label` and `operator_note`. Neither field changes runtime
policy. Worker leases, origin jobs, enrollment credentials and model permissions
retain their existing independent authorities. Zero voters remains the default.

A dedicated voter CA authorizes mTLS participants. A separate bearer token protects
the loopback admin API. Python observes the service asynchronously, caches bounded
status and exposes scoped owner metadata operations. No execution or admission
path contacts Raft, even when it is configured. Unavailable consensus blocks only
admin writes; follower/local reads explicitly disclose their consistency limits.

Initialize only by explicit bootstrap of an empty 1/3/5-voter group. Persist the
original membership and never infer a new voter set from peer discovery, timeouts
or liveness. Restarts restore original state without bootstrap. Refuse bootstrap
against existing state. Permanently losing quorum has no automated recovery path.

An independently calculated preferred peer uses deterministic rendezvous hashing
on one observed component. This is advisory only and may disagree across network
partitions. Nothing relies on its uniqueness or uses it to grant resources.

[Operation, test coverage and remaining limitations](phase-8.md) document the exact
implementation. Native tests kill every voter while worker-issued leases, capacity
waits, inference and durable jobs remain functional. Consensus does not improve
job exactly-once semantics or cluster-wide capacity visibility.
