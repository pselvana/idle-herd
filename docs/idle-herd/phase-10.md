# Phase 10: optional container deployment

The Linux peer image packages the existing peer, not a central router. Every
container publishes its own localhost OpenAI/Ollama ingress. Native installs and
the menu bar need no Docker. Neither image contains model weights. Optional Raft
is a separate image and is absent from the default Compose topology.

## One peer with host Ollama

From this checkout, with Docker Engine/Desktop and Compose 2.24.4 or newer:

```sh
docker compose build peer
docker compose up -d
docker compose exec peer idle-herd doctor --json
curl http://127.0.0.1:11435/v1/models
curl http://127.0.0.1:11435/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"YOUR_INSTALLED_MODEL","messages":[{"role":"user","content":"Hello"}],"stream":true}'
```

A reachable, compatible Ollama with an installed model is required for inference;
it is not required for peer liveness. The default backend URL is
`http://host.docker.internal:11434`. Container localhost means the container, not
the physical host. Docker Desktop provides the host alias; the Compose
`host-gateway` mapping also supports Linux Engine. Host backend binding/firewall
must allow the bridge/VM to reach it. A host service bound only to 127.0.0.1 may
need a protected interface binding. Do not expose unauthenticated Ollama to a LAN
or the Internet. Confirm routing on the actual OS; neither host networking nor
mDNS is assumed. See [Docker networking](https://docs.docker.com/compose/how-tos/networking/).

Settings supplied through the shell or a private `.env` file:

| Setting | Default / purpose |
|---|---|
| `IDLE_HERD_CONTAINER_BACKEND_URL` | Explicit HTTP backend origin; no credentials, paths, query or redirects |
| `IDLE_HERD_HOST_PORT` | 11435, bound to host 127.0.0.1; alternate ports for extra peers on one machine |
| `IDLE_HERD_BACKGROUND_POLICY` | `idle_only`; explicitly set `utilization_only` to contribute without host activity evidence |
| `IDLE_HERD_CPUS`, `IDLE_HERD_MEMORY_LIMIT` | 2 CPUs, 4g; finite cgroup v2 limits for container utilization readings |
| `IDLE_HERD_PEER_IMAGE` | `idle-herd-peer:local`; set a reviewed immutable tag/digest for deployment |

Further `IDLE_HERD_*` settings must be added explicitly to Compose's `environment`
through an override; Compose does not forward arbitrary shell settings. Native
`IDLE_HERD_BACKEND_URL` remains loopback-only: the launcher supplies a private relay
at 127.0.0.1:18080 to reach the explicitly configured container backend origin.
Do not use the container launcher for native host services.

`peer-data` stores the entire private `/var/lib/idle-herd`: identity and trust,
worker ledger/fences, origin/job journals and results, owner credentials/preferences,
known endpoints, model staging/cache and traces. UID/GID 10001 owns it. Named
volumes are initialized with image ownership; bind mounts require matching owner
and private permissions. Use healthy local storage with SQLite locking/fsync,
not a shared live ledger on network storage. One volume belongs to one worker.
Never clone a ledger/key into another running peer. `docker compose down` retains
data; `down --volumes` destroys it. Back up the entire directory only while stopped,
as described in [Phase 9](phase-9.md). A destroyed volume destroys local-only jobs.

The image runs without root, with read-only rootfs, dropped capabilities,
no-new-privileges and a bounded temporary filesystem in Compose. There is no Docker
socket mount. `unless-stopped` controls daemon lifetime; no menu bar or GUI is
required. Health checks test both gateway and native liveness, independent of
backend availability, seeds, or Raft. Use `doctor` for readiness and policy reasons.

## Inference and owner-control boundaries

Host 127.0.0.1:11435 maps to container port 8080. This listener has an exact
method/path allowlist for the existing inference, inventory, capacity and durable
job APIs, including Anthropic Messages and OpenAI Responses. Requests stream
through to the normal loopback ingress and worker admission. Browser Origin and
cross-site fetches are rejected. Owner routes, dashboard, docs and peer RPC are
not on the allowlist, even with a valid owner bearer token. The native listener
remains on container 127.0.0.1:11435, and the backend relay on 127.0.0.1:18080.
Neither is published. Do not publish them or share the network namespace with
untrusted containers. The inference gateway is unauthenticated like native local
inference; containers on its bridge can also reach port 8080. Restrict that bridge
to trusted workloads. Job results remain accessible to inference clients.

Use authenticated CLI controls inside the container:

```sh
docker compose exec peer idle-herd status
docker compose exec peer idle-herd offline
docker compose exec peer idle-herd auto
docker compose exec peer idle-herd pause
```

Offline retains localhost inference and drains existing work while withdrawing
from cluster participation; Pause rejects all new inference. Quitting a host menu
bar cannot kill container jobs. The host SwiftUI companion cannot directly access
container owner controls: this phase supplies CLI access, not a host-control proxy
or automatic credential export. Docker access itself is an administrative privilege.
Private owner credentials, peer identity and voter credentials remain separate.
Do not bake enrollment authority/device private keys into images, build arguments,
Compose environment or a shared secret. The allowlisted build context excludes
operator files. Transfer only public device requests/credentials; retain private
keys in their destination volume. Protect backups and optional secret-file mounts.

## Native/container mixed clusters

Enroll before starting the joining daemon. A standalone first start initializes
its own authority; to join an existing cluster, start with a new volume and create
an enrollment request instead. The authority and joining daemon must be stopped
during their offline CLI operations. For an existing *native* authority:

```sh
# Joining container (its volume is new):
docker compose run --rm --no-deps peer enrollment-request \
  --output /var/lib/idle-herd/device-request.json
docker compose run --rm --no-deps --entrypoint cat peer \
  /var/lib/idle-herd/device-request.json > device-request.json
# On the stopped native authority, after reviewing that public request:
uv run idle-herd issue-credential device-request.json --output device-credential.json
# Transfer that public credential back. Verify the CA fingerprint independently:
docker compose run --rm --no-deps \
  -v "$PWD/device-credential.json:/run/device-credential.json:ro" \
  peer join /run/device-credential.json --fingerprint VERIFIED_CA_SHA256
```

An all-container authority uses `docker compose run --rm --no-deps peer init`
and the same offline `issue-credential` command with public artifacts mounted
read-only. Never share its private CA key with joining peers. Read
[Phase 2 enrollment, rotation and revocation](phase-2.md) for the full protocol.

For a mixed topology, edit these example addresses to reachable private literals:

```sh
export IDLE_HERD_ADVERTISE_IP=192.168.1.20
export IDLE_HERD_PEER_BIND_IP=192.168.1.20
export IDLE_HERD_SEEDS='["https://192.168.1.21:12001"]'
export IDLE_HERD_BACKGROUND_POLICY=utilization_only
docker compose -f compose.yaml -f compose.mixed.yaml up -d
```

The mixed overlay publishes authenticated TLS TCP 12001 on the selected host
interface. The launcher binds the container peer listener to 0.0.0.0 but advertises
the configured private host address and same port. The native peer at .21 must
advertise its own reachable literal endpoint and can seed .20. Permit only trusted
LAN/Tailscale sources through the host firewall. Ingress stays host-loopback-only.
Do not publish peer RPC on a public interface. Docker Desktop NAT, VPN routing,
hairpinning and host firewalls vary: verify direct connectivity both ways. Backend
and peer ports are distinct; this example's 12001 is an explicitly allocated peer
port, not a reassignment of any existing backend port.

For two or three containers on one Engine, `compose.multi.yaml` provides `dual`
and `multi` profiles, separate data volumes, localhost ingress ports 11435/11450/
11451 and private bridge endpoints 172.30.40.11/.12/.13:12001. Configure
`IDLE_HERD_PEER2_BACKEND_URL` and, for the multi profile,
`IDLE_HERD_PEER3_BACKEND_URL`; each must represent independent backend capacity.
Enroll all workers under one authority while stopped, then set JSON
`IDLE_HERD_PEER1_SEEDS`, `IDLE_HERD_PEER2_SEEDS` and `IDLE_HERD_PEER3_SEEDS` to
some other enrolled endpoints. For example peer1 seeds .12 and .13, peer2 seeds
.11 and peer3 seeds .12. Then:

```sh
docker compose -f compose.yaml -f compose.multi.yaml --profile dual up -d
# Or --profile multi for all three.
```

Adjust the subnet if it conflicts with a VPN/LAN. These bridge addresses are not
host-routable on Docker Desktop; use the mixed overlay for native peers, rather
than advertising an unreachable bridge IP. Never point independent worker ledgers
at the same Ollama capacity: worker-local fencing cannot account for another
worker or direct clients bypassing it. Seeds are reconnection hints, not routers.
After authenticated exchange, persisted endpoints allow seed loss; any eligible
survivor retains local execution without quorum. Capacity remains advisory.

## Host inference and sensor limits

| Backend/platform | Configuration and acceptance boundary |
|---|---|
| Host Ollama, macOS/Linux/Windows | Host alias or explicitly reachable private origin, installed model, live `/api/tags` and `/api/ps` with known positive model sizes; smoke inference on the actual host is still required |
| Linux NVIDIA Ollama container | Opt-in `compose.nvidia.yaml --profile nvidia`; NVIDIA Container Toolkit/driver required; install a model explicitly; GPU devices attach only to Ollama |
| Native Apple Silicon MLX | Keep MLX native for Metal; this peer's inventory/execution adapter remains Ollama-only. An OpenAI `/v1` URL is not an Ollama inventory adapter. Native legacy MLX support does not imply support in the peer container |
| Docker Model Runner | Candidate origin `http://model-runner.docker.internal` on Desktop (Linux Engine may need host-gateway plus port 12434); only usable when its Ollama compatibility supplies the complete required inventory/execution contract. No DMR-specific adapter, OCI model import or real-engine acceptance is claimed |

The opt-in NVIDIA example is started with
`docker compose -f compose.yaml -f compose.nvidia.yaml --profile nvidia up -d`.
Use `docker compose ... exec ollama ollama pull YOUR_MODEL` explicitly afterward.
[Docker GPU reservations](https://docs.docker.com/compose/how-tos/gpu-support/)
describe the host prerequisites. No GPU access is required by the peer process.

Docker documents DMR's OpenAI/Ollama API routes, but its listed Ollama subset does
not establish the `/api/ps` and positive weight-size contract required here.
A reachable `/engines/v1/models` alone must never create available slots. Probe
`/api/tags`, `/api/ps`, then run a model through the peer and verify a terminal
stream before claiming backend compatibility. DMR URLs and provider support may
change; consult its [API reference](https://docs.docker.com/ai/model-runner/api-reference/).
On macOS the Linux application container does not acquire native Metal simply by
using the ARM64 image; use host inference with a supported adapter. Do not use a
Docker socket mount to bypass backend compatibility checks.

`ContainerSensor` intentionally reports `confidence: unknown` and no human idle
or lock state. It reads finite cgroup v2 CPU quota and memory limits; absent limits,
v1 layouts, missing permissions or stale readings remain unknown. Keyboard idle,
physical host battery/AC, GPU/VRAM and host thermal pressure cannot be inferred
from VM/container CPU. `idle_only` fails closed. An owner can explicitly choose
`utilization_only` after accepting these limits; caller `idle_only:true` remains
conservative. `require_ac` also fails when AC is unknown. This launcher does not
run the native host helper or mount session buses/input devices. A future trusted
host-sensor bridge would need authenticated, freshness-checked host-wide evidence;
no such bridge is implemented here. Prefer a native peer for host-aware policy.

Cgroup limits protect the peer process, not an external Ollama/MLX/DMR process.
The existing backend fit estimator still reads VM-visible `psutil` memory and
model weight estimates; it is not authoritative host RAM/VRAM accounting. The
container sensor supplies an additional cgroup headroom check, not host capacity.
Choose conservative slots, model limits and backend context/resources, keep one
worker per backend budget, and verify actual host fit. The default four-gigabyte
peer limit is not a promise that a four-gigabyte model fits or that external model
weights consume that cgroup budget.

## Recovery, optional consensus and builds

Restart retains identity and fences while changing worker incarnation. An
unconsumed grant can expire; uncertain running execution stays quarantined even
if its container dies. The external host/backend container may continue computing.
Stop the peer **and all backend runners** before offline reconciliation:

```sh
docker compose stop peer
# Separately stop and verify ALL host/backend runners before this attestation:
docker compose run --rm --no-deps peer reconcile --confirm-backend-stopped
```

A container restart or empty backend inventory alone never authorizes clearing
quarantine. At-least-once job recovery, duplicate consent, durable-copy thresholds
and model-sharing permission/digest/install locks are unchanged. OCI images do
not add cross-format model sharing or global exactly-once execution.

`compose.raft-optional.yaml` defines an inactive `raft` profile and separate volume.
It shares the trusted peer's network namespace to keep admin HTTP loopback-only.
Supply dedicated readable voter TLS CA/cert/key and a private token file under
`IDLE_HERD_RAFT_CREDENTIALS_DIR`, exact service flags and explicit bootstrap as in
[Phase 8](phase-8.md). The template intentionally has no implicit bootstrap or
ready-to-run anonymous voter. Set peer `IDLE_HERD_RAFT_URL` and
`IDLE_HERD_RAFT_TOKEN_FILE` and mount only its admin token through an operator
override; the peer never needs the voter CA private key. Set `IDLE_HERD_RAFT_IMAGE`
to a reviewed digest for deployment. Never publish admin HTTP. Voter loss affects
only strict admin labels/notes, not inference; do not automatically rebootstrap.

Build OCI archives without publishing:

```sh
docker buildx build --platform linux/amd64,linux/arm64 \
  -f container/Dockerfile.peer --output type=oci,dest=/tmp/idle-herd-peer.oci .
docker buildx build --platform linux/amd64,linux/arm64 \
  -f container/Dockerfile.consensus --output type=oci,dest=/tmp/idle-herd-consensus.oci .
```

`container/dependencies.lock` is a committed build lock independent of the ignored
root `uv.lock`. Refresh it deliberately when project dependencies change; the build
uses `uv sync --locked`. For example `uv lock --python 3.12` then copy that reviewed
lock to `container/dependencies.lock`. Base-image version tags can move; production
builds should also pin reviewed base digests. This phase builds local artifacts,
not a registry publication, signed release or supply-chain/CVE certification.
The MIT license and upstream package attribution remain in both images.

The non-publishing CI workflow builds both architectures and runs opt-in mocked
Compose tests. Locally:

```sh
uv run pytest tests/test_container/test_runtime.py -q
IDLE_HERD_TEST_CONTAINERS=1 uv run pytest tests/test_container/test_compose_integration.py -q
```

Container integration requires a working daemon; normal offline pytest skips it
with an explicit reason. The tests isolate their own project, named volumes,
subnet and ephemeral host ports and remove those resources afterward. They do not
stop existing user services or require a real Ollama model. See
[implementation status](implementation-status.md) for actual results, architecture
build evidence and remaining physical hardware/platform gates.
