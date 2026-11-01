# Phase 0 acceptance specification, not an executable pytest suite.
# No step definitions exist yet. Implement these against native peers/mock
# backends in the indicated phases; never count these scenarios as passed.
Feature: Independent native peers retain worker-local authority during churn

  # Phase 1
  Scenario: A single node needs neither discovery nor consensus
    Given one native peer with a compatible local backend and healthy storage
    And all discovery and coordination services are unavailable
    When clients request streaming inference through localhost OpenAI and Ollama APIs
    Then local admission and inference complete without contacting another peer
    And every accepted request has a worker-persisted lease and an outcome trace

  # Phase 2; repeat for every survivor, not just the original bootstrap peer
  Scenario Outline: The last survivor remains useful
    Given <configured> authenticated peers with eligible local backends
    When all peers except <survivor> disappear
    Then the survivor executes new locally originated work through its localhost API
    And stale or offline peers contribute no observed available slots
    Examples:
      | configured | survivor |
      | 2          | A        |
      | 2          | B        |
      | 3          | A        |
      | 3          | B        |
      | 3          | C        |
      | 5          | E        |

  # Phases 2 and 8
  Scenario: Partition availability does not depend on optional Raft
    Given native peers partitioned into groups of two and one
    And the optional Raft majority is unreachable from the singleton
    When both partitions submit locally executable work
    Then each partition continues admission and inference
    And only quorum-dependent administrative writes require consensus
    And no peer automatically rebootstrap the voter set as a new singleton

  # Phases 1 and 2
  Scenario: Contending origins cannot overbook a worker
    Given one worker with one shared backend resource slot
    When 100 clients on two origins race to acquire and consume it
    And admission responses are lost and retried with the same request identity
    Then at most one execution occupies the slot at any instant
    And successful admission was durably committed before acknowledgment
    And changing models cannot bypass the shared backend resource budget

  # Phase 1
  Scenario: Crash recovery fences old tokens without freeing live work
    Given a worker persisted a lease and its backend is still executing
    When the worker crashes after commit but before acknowledgment
    And the daemon restarts with a new incarnation
    Then old-incarnation and old-generation tokens cannot start execution
    And backend execution is reconciled before capacity can be reissued
    And expiry alone does not establish that an execution stopped

  # Phases 3 and 4
  Scenario: Stale capacity cannot grant admission
    Given an origin observed an idle worker with an available slot
    When the worker becomes active or its idle sensor becomes unknown
    Then the worker refuses idle-only admission despite the stale snapshot
    And the snapshot remains explicitly advisory with scope and freshness
    And busy is unknown if the view cannot support a conclusion

  # Phase 4
  Scenario: Blocking acquisition retries live admission through churn
    Given bounded origin-local waiters and no currently reachable eligible slot
    When a peer joins or releases capacity without a reliable event notification
    Then retry backoff rechecks live worker admission before the deadline
    And a successful response contains the accepting worker's fenced token
    And cancellation releases only worker-confirmed reservations
    And deadline exhaustion reports capacity_wait_timeout rather than no quorum

  # Phases 5 and 7
  Scenario: Missing data is not recoverable by assertion
    Given a model or job payload exists only on a lost node
    When a survivor receives related work
    Then it reports missing compatible artifacts or waits under a deadline
    And it does not claim the unavailable payload is replicated

  # Phase 5
  Scenario: Replicated recovery is at least once
    Given a retry-safe job with acknowledged durable metadata and payload replicas
    When origin and replica cannot communicate while a worker may still execute
    Then recovery uses distinct durable attempt identities
    And duplicate logical execution is possible and reported truthfully
    And non-idempotent jobs are not automatically failed over
    And uncertain completion is reported as unknown_outcome

  # Phase 6
  Scenario: Offline preserves localhost while paused denies new inference
    Given a running daemon with existing local and remote work
    When the owner selects offline through authenticated local control
    Then new peer admission and outbound cluster dispatch stop immediately
    And unconsumed peer leases cannot bypass the local transition
    And existing work drains while new localhost inference remains available
    When the owner selects paused
    Then all new inference is rejected while health and controls remain reachable
    And a local token issued before pause cannot be consumed afterward
    And the pause-versus-consumption race is serialized with worker admission

  # Phase 6
  Scenario: UI lifetime does not own daemon lifetime
    Given two active jobs and the menu bar connected with a read-scoped credential
    When the UI quits or crashes
    Then daemon execution and CLI status continue
    And after reconnect the UI replaces stale state with an authenticated snapshot
    And read scope cannot modify mode or preferences
    And unauthenticated or cross-origin control writes do not change state

  # Phase 7
  Scenario: Peer model installation verifies compatibility and content
    Given an authorized source with a complete digest-pinned model manifest
    When two transfers race for one destination and the source disappears midway
    Then only one destination-local digest lock owns installation
    And a compatible authorized matching source may resume verified chunks
    And incompatible artifacts, traversal, symlinks and insufficient space are rejected
    And the model becomes ready only after complete verification and backend import
