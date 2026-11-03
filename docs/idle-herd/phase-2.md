# Phase 2: authenticated peers and direct dispatch

**Current native default:** [Bonjour discovery and automatic LAN trust](bonjour.md)
connect peers without seeds or manual enrollment. The workflow below remains
supported with `IDLE_HERD_LAN_AUTO_TRUST=false`; this document records Phase 2's
original manually configured transport.

Every `idle-herd` process still exposes its own localhost OpenAI/Ollama ingress on
127.0.0.1:11435 and owns its durable worker ledger. A separate, opt-in TLS listener
adds enrollment, peer exchange and direct admission/execution. No router, seed,
leader, quorum, service container or UI is needed for local inference. Start with
[Phase 1](phase-1.md) for backend configuration, memory estimates and quarantine
recovery. Legacy `herd` / `herd-node` remain unchanged.

## Enrollment and operation

Use separate data directories per machine and one authority per cluster. All
identity/maintenance commands take the same exclusive worker lock and require the
daemon stopped. Protect the data directory with per-user OS permissions; private
keys are written with POSIX mode 0600. On Windows, apply per-user ACLs. Enrollment
artifacts contain public material only. Verify the authority fingerprint through
an independent trusted channel before import.

On the first machine (authority), using its normal `IDLE_HERD_DATA_DIR`:

```console
uv run idle-herd init
```

Keep the returned trust fingerprint. On a **new joining machine/data directory**,
before initializing an independent authority:

```console
uv run idle-herd enrollment-request --output device-request.json
```

Transfer the public request to the authority. Inspect/approve the intended device,
then issue a device-specific credential (default 30 days, maximum 90):

```console
uv run idle-herd issue-credential device-request.json --output device-credential.json --days 30
```

Transfer the credential back to that device and import it:

```console
uv run idle-herd join device-credential.json --fingerprint VERIFIED_CA_SHA256
```

A joining ledger adopts the authorized cluster UUID while preserving its worker
UUID. Cluster reassignment with any lease history is rejected. Existing trust
anchors cannot silently change; use a new data directory to join an existing
singleton authority to a different cluster. Never copy live ledgers/private device
keys. Renew a credential by issuing/importing another credential for the same
public enrollment request, using the same fingerprint, with the daemon stopped.

Configure each device's native environment before running `uv run idle-herd`:

| Variable | Default | Meaning |
|---|---|---|
| `IDLE_HERD_PEER_ENDPOINT` | unset | Opt-in listener/advertised HTTPS endpoint with a literal private IP and explicit unused port |
| `IDLE_HERD_SEEDS` | `[]` | JSON array of candidate HTTPS peer endpoints; discovery hints only |
| `IDLE_HERD_PEER_INTERVAL` | `5` | Seconds between direct exchanges, minimum 0.1 |
| `IDLE_HERD_PEER_TTL` | `30` | Seconds before a direct observation becomes stale, minimum 0.2 |

For example, set the first peer endpoint to `https://192.168.1.10:17435` and its
seed list to `["https://192.168.1.11:17435"]`; reverse the addresses on the second
machine. These are examples of explicitly allocated peer ports, not changes to
the existing port map. Choose a free port that does not overlap **any** configured
backend/MLX service and permit it only across the intended private network. The
CLI binds the peer listener to the advertised address. Public IPs, arbitrary DNS
names, URL credentials, paths, query strings and redirects are rejected. Private
IPv4/IPv6, loopback, link-local and Tailscale/CGNAT literals are supported; mDNS and
Tailscale API discovery are optional integrations not implemented in Phase 2.

Once authenticated peers converge, they exchange known endpoints bidirectionally.
Endpoints persist in `known-peers.json`; restart re-probes them without consulting
an original seed. Only a direct authenticated exchange establishes freshness.
A failed probe immediately removes a candidate from routing; TTL handles missing
refreshes. Offline endpoints remain known for automatic retries. Directory limits
are 128 endpoints, 256 models per peer and 256 KiB per exchange. Endpoints are
retained without automatic eviction; an authorized peer can exhaust this bounded
directory. Replacing a worker at the same address requires removing the old entry
from `known-peers.json` while stopped. Endpoint exhaustion affects discovery, not
local admission.

`GET /v1/cluster/peers` on localhost shows this peer's partial view, timestamps,
model advertisements and reachability reasons. Connection failures usually mean
an unavailable peer, wrong listening address or firewall/NAT; TLS errors mean an
untrusted/expired credential or different CA; identity errors mean a bad signature,
replay or conflicting worker identity. Check both devices' diagnostics. This is
an observed local view, never complete cluster capacity. Phase 1 capacity/acquire
endpoints remain **local-worker-only**; `wait_ms > 0` is still rejected. Standard
model listings and `auto` model selection retain their local behavior; remote
advertisements are available through peer diagnostics. Request an explicit remote
model name to dispatch to it.

## Admission and streaming

For explicit model inference without a reservation header, local ingress tries
local worker admission first. If that worker declines and a fresh compatible
advertisement exists, the origin directly asks each candidate for a durable
worker-issued lease. The receiving worker checks its live backend, model and
resource budget under the same SQLite serialization used by local requests.
Advertisements never grant slots. There is no remote routing through a seed.

The origin then forwards the original API payload directly to the accepted worker.
The worker executes its existing OpenAI, Ollama, Anthropic Messages or Responses
translator with the fenced token, and streams bytes over TLS back through the
origin. Remote leases are bound to their authenticated origin; a different peer
cannot execute/release them even if it learns the token. The peer listener exposes
only four POST operations under `/peer/v1`: `exchange`, `acquire`, `execute`,
`release`. It cannot call arbitrary backend URLs, owner controls or public routes.
Inference envelopes are bounded to 2 MiB. Local caller secrets/credentials are not
forwarded to another peer.

After execution dispatch there is **no automatic retry/failover**. Disconnects,
timeouts and incomplete backend streams may leave work running; the worker keeps
it quarantined according to Phase 1 recovery rules. A lost grant reply can retain
an unconsumed reservation until its TTL. An origin restart does not reconstruct a
stream or durable job. Partitions can produce duplicate logical attempts when a
caller retries with another attempt ID; global exactly-once is not promised.
Blocking waits/fairness/deadlines arrive in Phase 4 and durable jobs in Phase 5.

## Transport and trust

Device Ed25519 keys and CA-signed X.509 certificates persist in `peer-identity/`.
The authority key stays on the administrator's device; enrolled workers cannot
issue credentials. Requests sign protocol version, method, path, body digest,
timestamp and random nonce. Clock skew is bounded to 120 seconds. A bounded
SQLite nonce ledger commits before RPC mutations/acknowledgment and rejects replay
across restart. Grants/exchanges sign the exact response bytes and request nonce.
Streaming response signatures bind the nonce/status and are checked against the
actual TLS server certificate before forwarding body bytes; TLS authenticates and
encrypts the stream. CA verification is mandatory even for initial seed contact.
This uses signed application RPC over TLS, not mutual TLS client certificates.

Revoke a public credential on the stopped authority:

```console
uv run idle-herd revoke device-credential.json
```

Authority-signed revocation statements propagate by authenticated gossip and
persist on receivers. They expire with the revoked certificate; at most 128 active
statements are supported. Revocation is eventual: a partitioned peer may accept
an otherwise valid credential until it learns revocation or the credential expires.
It cannot guarantee instantaneous global revocation or globally single-use invites.
Expired/revoked/broken local peer credentials withdraw participation while eligible
localhost inference remains available. An optional peer listener that cannot bind
also leaves the localhost listener running and reports `peer_error` in `/health`. Credential renewal needs offline import and
restart. Offline `issue-credential`/`revoke` can use a still-valid authority key
even after its node certificate expires; this does not permit expired peer RPC.
Keep one protected authority state; copying its private key/issuer registry
or running independent issuers undermines identity collision protection.

## Evidence and scope

[Implementation status](implementation-status.md) records exact commands/results.
Phase 2 tests use real native TLS sockets/processes with fabricated model backends,
and offline ASGI/transport unit tests. Killing an origin during streaming leaves
the independent backend charged beyond its 30-second lease TTL and denies
additional admission. No real inference, Linux/Windows host run,
OS services, idle sensors, mode controls, UI, model transfers or optional Raft was
introduced/tested in this phase. Docker/Compose remains Phase 10. Local inference
is independent of peer credentials/discovery and of any optional future Raft role.
