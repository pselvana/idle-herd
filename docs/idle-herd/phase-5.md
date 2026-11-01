# Phase 5: durable jobs and optional peer copies

`idle-herd` serves durable jobs on every localhost ingress. The origin owns
`jobs.sqlite`, separate from worker leases (`worker.sqlite`) and origin reservation
claims (`origin.sqlite`). SQLite WAL with `synchronous=FULL` commits the complete
input, job metadata, attempts and results. No leader, seed or Raft is required for
local jobs. Execution still requires a live worker-issued fenced reservation.

## Local API

```console
curl http://127.0.0.1:11435/v1/jobs -H 'Content-Type: application/json' -d '{
  "model": "llama3.2",
  "request": {"messages": [{"role": "user", "content": "Explain WAL briefly."}]},
  "idempotency_key": "my-durable-request"
}'
```

The default path is `/v1/chat/completions`. `path` may select the existing
OpenAI chat/completions/embeddings, Responses, Anthropic Messages or Ollama
chat/generate/embed/embeddings translator. `request` contains that translator's
JSON input. The job model is explicit; payload model names must agree. Jobs use
non-streaming responses; `stream: true` is rejected. Live inference streaming
remains available on its existing endpoints.

| Operation | Contract |
|---|---|
| `POST /v1/jobs` | 202 with safe summary, stable job ID and Location after durable acceptance |
| `GET /v1/jobs/{id}` | Safe state, local execution readiness, durability, copy IDs and attempt summaries; no inputs, outputs or bearer tokens |
| `GET /v1/jobs/{id}/result` | Explicit content retrieval: job ID, attempt ID and `{status_code, content_type, body}` for a completed attempt; 409 while unavailable |
| `DELETE /v1/jobs/{id}` | Persist cancellation and stop local scheduling; running work becomes `unknown_outcome`, pending work becomes `canceled` |

`idempotency_key` is optional; omission creates a fresh key/job. Repeating an
explicit key with the same canonical submission returns the same job; changed
parameters return 409. Idempotency is origin-local. Results on an authorized
surviving replica are available through that replica's localhost API using the
same job ID. Cancellation must be requested at the origin; it cannot guarantee
instant propagation to partitioned replicas.

`priority` ranges from -100 to 100, default 0. `execution_policy` accepts
`workload` (`interactive` default or `background`) and `idle_only` (false default).
Peer dispatch always enforces the destination's background policy, regardless of
the origin label. Unknown activity cannot grant idle-only work.

## Durability and acknowledgments

`durability: local` is the default. The full record/input is committed before
202; the job survives daemon restart while that directory survives. Its sole
copy cannot execute or answer queries while its host is down. Local acceptance
does not contact peers, and peer maintenance runs separately from scheduling.

For `durability: replicated`, `replica_count` specifies additional durable peer
copies (1–16, default 1). Only authenticated enrolled peers with known reachable
endpoints are candidates; model availability is not required to store a copy.
The origin first commits locally, then sends the full digest-bound snapshot over
the existing authenticated TLS peer transport. Each recipient commits before
returning a signed acknowledgment binding worker ID, job ID and digest. The
origin commits the acceptance certificate naming these holders, sends the
accepted snapshot, and verifies the configured number of durable accepted-copy
acknowledgments before 202 and local execution activation. Prepared copies cannot
run. Replicas do not copy the origin's local execution activation flag.

Insufficient prepared copies return 503 `job_durability_unavailable`. Failure to
confirm the accepted-copy threshold returns 503 `job_acceptance_unknown`; some
accepted copies may already recover eligible work. Downgrade requires
`allow_durability_downgrade: true`; the response reports actual local durability.
A failed/lost submission response may leave a prepared or accepted record. Retry
the same idempotency key to establish its state. An accepted certificate can
already exist remotely when the final response is lost; retry-safe takeover can
therefore happen even if the client did not observe 202. Never infer nonexecution
from a lost response. Admission and execution use distinct durable attempt IDs.

Store RPC is restricted to the original origin principal. Fetch RPC requires the
origin or a listed copy holder. Other trusted peers cannot read job contents.
Snapshots/results stay out of discovery and gossip. Local files are private
per-user data, not application-encrypted storage; use OS disk encryption and
protect Windows ACLs as well as the POSIX private directory. These local job
endpoints retain the existing loopback/Host/Origin boundary; same-user caller
scopes and privileged owner control remain Phase 6.

## Recovery and cancellation

States are `queued`, `reserving`, `running`, `retry_pending`, `completed`,
`failed`, `canceled` and `unknown_outcome`. The journal commits an attempt before
acquiring capacity and commits its worker/lease assignment and dispatch boundary
before execution. A restarted origin may requeue its own admission-only attempt
that never crossed this boundary, retaining that history without consuming an
execution retry. Interrupted dispatched work remains unknown. Worker quarantine
is independent: job recovery, cancellation and lease TTL cannot free uncertain
backend execution. Follow [Phase 1 offline recovery](phase-1.md).

Default policy is one attempt and no automatic replay. To permit bounded retries
and replica takeover explicitly supply, for example:

```json
{"retry_policy": {
  "safe": true, "max_attempts": 3, "delay": 30,
  "allow_possible_duplicates": true
}}
```

Maximum attempts is 1–10; retry delay is 1–86400 seconds. Tool, function, action
and tool-result inputs cannot be declared retry-safe. The service does not
execute external tools. Side-effecting/non-idempotent work is never automatically
replayed. Running cancellation persistently disables further retries and reports
unknown because stopping an HTTP task does not prove that Ollama stopped.

A complete accepted replica can take over only with explicit retry safety,
duplicate consent and remaining attempt budget. It must know the origin endpoint,
probe it directly, and persist at least 30 seconds of unreachability. Any verified
origin reply, including an error, proves reachability and suppresses takeover.
Unknown or missing endpoint information cannot authorize recovery. Attempts and
results reconcile by ID when copies reconnect; conflicting outcomes remain
unknown, with evidence retained. Completed copies are also reconciled after the
origin returns. Local scheduling and worker fencing remain independent of these
best-effort probes.

This is **at least once for eligible retries**, not global exactly once. A
partitioned origin or disconnected worker may still execute. Independent replicas
can overlap, and attempt budgets apply to each currently known history rather
than a globally exclusive counter. A survivor also needs the model, eligible
resources and local policy. No durable copies online means no progress.

## Bounds and evidence

Submission bodies are limited to 256 KiB; stored response bodies to 32 KiB.
Oversized requests return 413. Oversized/incomplete execution output is an
unknown outcome, never a fabricated completed result. The journal caps logical
records at 10,000 jobs, 64 MiB total JSON and 1 MiB per record; full submissions
return 507. SQLite WAL/page overhead and disk high-water marks are additional.
History is retained; cancellation does not erase it. Automatic archival/compaction
and large external blob storage are not provided. At most 16 local jobs wait/run
concurrently; remaining accepted jobs stay durably queued. Priority applies when
selecting queued jobs; running or already waiting jobs are not preempted.

Tests cover journal commit failure, idempotency races, accepted-copy verification,
signed replica ACLs, failed acceptance, result synchronization, simulated origin
loss/takeover, divergent attempts, cancellation and bounded retry. Native processes
with fabricated backends cover queued and completed job restart, unknown running
execution, and N=2/N=3 shrinking to one local-job survivor. No real-model,
power-loss, Linux/Windows, OS service or physical sensor acceptance is claimed.
See [exact evidence](implementation-status.md). Phase 6 is next; model transfer,
optional admin Raft and Docker/Compose remain later phases.
