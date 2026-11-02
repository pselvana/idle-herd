# Phase 8: optional native admin consensus

Inference, worker admission, capacity waits, durable jobs and model sharing do not
use consensus. The default installation runs zero voters and needs no Go binary.
`coordination/` is a separate native Go module using HashiCorp Raft, Bolt log/stable
storage and file snapshots. Its deterministic FSM accepts exactly two string keys:
`cluster_label` and `operator_note`, each at most 1024 UTF-8 bytes. Neither value is
applied to scheduling, owner preferences, enrollment, model permissions or execution.
Do not store secrets, prompts, job records or weights in this metadata.

## Build and explicit activation

From `coordination/`, with Go 1.22 or newer:

```console
go build -o /absolute/private/bin/idle-herd-raft .
go test -race ./...
go vet ./...
```

Build outputs are separate from the Python distribution. The executable runs in
the foreground and can be independently supervised by launchd, systemd or a native
Windows service wrapper. No service is installed or launched by `idle-herd`, its
UI or its service installer. There are no automatic voter enrollment, membership
changes or quorum-loss recovery operations. Native service installation and
Linux/Windows runtime acceptance have not been exercised for this component.

Provision a **dedicated voter CA** and separate client/server certificates with
server/client authentication EKUs and appropriate IP/DNS SANs. The Raft listener
requires mTLS, including on loopback. CA issuance authorizes a process to speak the
Raft protocol; certificates are not individually mapped to voter IDs. Never reuse
the inference-peer enrollment CA or distribute voter certificates to ordinary
inference peers. Protect certificates, private keys and voter state with OS user
permissions. Windows requires appropriate per-user ACLs.

Provision a private regular token file (POSIX 0600) containing a random 32–256
character URL-safe bearer token. Use a separate token from inference-peer and
local read/control credentials. Provision a private state directory (0700).
Select explicit unused listener ports outside 11434–11449; there is no default
new port and the existing port map is unchanged. For a single optional voter:

```console
/absolute/private/bin/idle-herd-raft --id admin-a --raft-address 127.0.0.1:19451 --http-address 127.0.0.1:19452 --data-dir /absolute/private/admin-a --token-file /absolute/private/admin-token --cert /absolute/private/admin-a.pem --key /absolute/private/admin-a-key.pem --ca /absolute/private/voter-ca.pem --bootstrap --voter admin-a=127.0.0.1:19451
```

Ports in examples are operator choices. Keep each state directory bound to its
original voter ID and address. For three or five voters, start every service with
its own ID, addresses, key and state directory. On **one initializer only**, pass
`--bootstrap` and repeat `--voter ID=HOST:PORT` for the complete agreed membership,
including itself. Other empty voters start without bootstrap and receive the log.
One voter tolerates zero failures; three tolerate one; five tolerate two. Two
voters are rejected because they tolerate zero failures. Zero voters is the default.

On every later start, omit **both** `--bootstrap` and `--voter`. Existing state
rejects bootstrap. An empty node without bootstrap stays uninitialized: it never
forms its own singleton after a timeout or partition. Losing quorum requires
restoring the original voters/state; no automatic shrink or replacement history
is attempted. Permanent loss of a majority requires a separately reviewed manual
recovery procedure; this implementation provides no destructive recovery tool.

## Local API and observation

The sidecar admin HTTP listener must bind a loopback literal IP. Every request
requires `Authorization: Bearer TOKEN` and matching Host; browser Origin/fetch
requests are refused. Raft traffic uses the separately configured mTLS listener.
Do not expose the admin HTTP port through a proxy.

| Method and sidecar path | Behavior |
|---|---|
| `GET /v1/status` | Node ID, Raft state, leader ID, verified quorum and commit index |
| `GET /v1/metadata` | Committed local FSM data and `consistent`; followers/no-quorum return a potentially stale local view with `consistent: false` |
| `PUT /v1/metadata` | `{ "key": "cluster_label", "value": "Home fleet" }`; unknown keys/fields and oversized bodies rejected; only a verified leader commits, otherwise 503 |

Writes are acknowledged after Raft applies the committed command. A failed or lost
HTTP response may conceal a committed write; read after quorum returns before
assuming it failed. Repeating the same key/value is safe but not a compare-and-swap
transaction. Followers do not proxy admin writes; direct the admin client to the
current leader. `quorum: null` on a follower means global quorum is unknown to that observer.
Only a leader reports true/false after verifying quorum. `local_write_available`
is false on followers and unavailable services; admin writes still require the leader.

Python observes a sidecar only when explicitly configured:

```sh
export IDLE_HERD_RAFT_URL=http://127.0.0.1:19452
export IDLE_HERD_RAFT_TOKEN_FILE=/absolute/private/admin-token
uv run idle-herd
```

In PowerShell set the same names under `$env:`. The token path must be absolute;
missing/insecure credentials or a missing/hung/malformed service only make admin
coordination unavailable. A background poll has a one-second deadline and a
one-second interval. Cached observations expire after three seconds. Startup,
admission, inference and capacity requests never call the sidecar. An enabled but
unavailable service reports unknown quorum (`null`), as do followers; `enabled`,
`reachable` and `state` distinguish these from disabled. A verified leader reports
true/false. Cached status never grants administrative writes.

The authenticated `GET /v1/node/status`, local status dashboard and sampled local
events include a `coordination` object: enabled, reachable, quorum, local write availability, state, leader,
observation time, preferred peer and `advisory_only: true`. Health and cluster
capacity expose cached quorum independently of slot counts. The read/control
scopes also protect `GET`/`PUT /v1/node/coordination/metadata`; writes require the
control credential. These endpoints forward to the configured local sidecar only,
return 503 when unavailable, and are absent from the peer RPC listener. No token,
private file path or admin metadata value is embedded in status events.

When no verified Raft leader is observed, rendezvous hashing selects a preferred
peer from the local worker and currently reachable peer identities, scoped to the
cluster and optimization task. Offline owner mode withdraws the node from preferred-peer advice.
This is advice for non-critical optimization only; there is no elected global
singleton, background scheduler or correctness dependency. Different partitions
can produce different preferred peers. No admission or model-transfer code consumes
this advice.

## Verification and limits

`tests/test_peer/test_coordination_native.py` builds the Go binary when Go is on
PATH, or uses `IDLE_HERD_RAFT_BINARY`. It explicitly skips when neither is present.
Three real native voter processes use mTLS and durable state. The test kills every
voter while a separate peer/backend remains alive: a previously issued worker
lease still executes, a blocked capacity waiter acquires, and a durable origin job
completes. Restoring original voters without bootstrap restores metadata, resumes
admin writes and leaves job results and local inference usable. Backends and sensor
signals are fabricated; no real model is needed.

Go tests run real Raft with fault-controlled in-memory transport and persistent
Bolt/file stores for majority/minority partition, leader replacement, reconnection,
one-of-three survival, rebootstrap refusal and metadata recovery. These are not
physical LAN partition, storage power-loss or cross-platform service certification.
Python tests exercise missing sidecars/credentials, stale observations, scoped
owner access, restricted metadata and local acquisition/inference independence.
See [recorded commands and results](implementation-status.md). Phase 9 diagnostics
and release readiness remain future work; Docker/Compose remains Phase 10.
