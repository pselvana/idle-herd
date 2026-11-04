# Phase 7: authenticated model sharing

Each daemon owns its model catalog, staging directory, SQLite transfer journal and
installation lock. Transfers use direct authenticated peer RPC; discovery seeds,
leaders and Raft have no authority over installation or inference. The implemented
adapter imports **one standalone GGUF into Ollama** through supported blob/create
APIs. It does not copy Ollama's private model directories or convert formats.

## Default sharing and operation

The owner now enables source sharing by default. Daemons discover installed
standalone Ollama GGUF models through `/api/tags` and `/api/show`, then offer them
to all authenticated peers without manual registration. New registrations and
successfully imported copies also default to `allowed_peers:["*"]`,
`sharing_enabled:true` and `license_approved:true`. This source policy does not
establish a publisher's license grant; destination permission and license acceptance
remain explicit for each copy/retry. Existing explicit denials and allowlists are
preserved. Peer authentication, TLS and signed/replay-protected requests still
precede every inventory, manifest and chunk operation. See [peer trust](bonjour.md).

`IDLE_HERD_MODEL_AUTO_SHARE=false` disables automatic backend discovery; manually
registered artifacts retain their per-model permissions. Discovery refreshes within
30 seconds, shares installed models even when unloaded, and deduplicates identical
weight digests under one deterministic model name. It opens only absolute
content-addressed blob paths returned by local Ollama's public metadata API,
rejects source symlinks, and checks GGUF magic/size. It never walks or copies Ollama's
private directories. The blob filename supplies the expected content digest;
the destination verifies the complete downloaded content before import. If Ollama
is temporarily unavailable, last observed readable file offers can remain; removed
models withdraw after a successful inventory refresh. Discovery failures are logged.

Automatic offers include content-bound full license text, template, system preset
and parsed parameters, preserving custom Ollama model behavior through supported
create APIs. An absent license is explicitly labeled as unknown for owner review.
Unreadable files, non-GGUF, multimodal/projector and adapter/multi-file models are
not advertised as standalone copies; those export adapters remain unimplemented.
The daemon must be able to read the path returned by its backend, including in
containers. This does not expose arbitrary backend-host files to a container.

Manual registration remains available for standalone GGUFs not installed in Ollama.
Prepare a `LocalArtifact` JSON with `manifest` and an absolute local `path`.
Use optional `allowed_peers`, `sharing_enabled` and `license_approved` to override
the default source permission. To create the manifest, use
`fleet_manager.peer.model_sharing.ArtifactManifest`: supply
`model_id`, file SHA-256 `digest` (64 lowercase hex characters), `size_bytes`,
`format:"gguf"`, `backend:"ollama"`, `quantization`, exact revision `provenance`,
`license`, and conservative `required_memory_bytes`. Optional `required_os`
(`Darwin`, `Linux`, `Windows`), `required_cpu` and `minimum_backend_version` constrain
the destination. Initialize `manifest_digest` with 64 zeroes, then replace it with
`manifest.calculated_digest()`. That hash binds canonical JSON, including default
and null fields; changing metadata requires a new manifest digest.

```sh
uv run idle-herd models register artifact.json
uv run idle-herd models inventory
uv run idle-herd models transfers
uv run idle-herd models copy FILE_SHA256 MANIFEST_SHA256 SOURCE_WORKER_ID --permit --accept-license
uv run idle-herd models pause FILE_SHA256
uv run idle-herd models copy FILE_SHA256 MANIFEST_SHA256 SOURCE_WORKER_ID --permit --accept-license --retry
uv run idle-herd models permission FILE_SHA256 permission.json
```

`permission.json` with `{"sharing_enabled":false}` revokes future source reads,
including the next chunk of an ongoing transfer. Already delivered bytes cannot be
recalled. `{"sharing_enabled":true}` restores the default all-authenticated-peer
grant; set `allowed_peers` to specific worker IDs to restrict it, or
`license_approved:false` to withdraw source license permission. Overrides for
automatically discovered models persist across refresh and restart. A registered local artifact is a
verified file offer, not proof that the source has loaded it in Ollama.

The menu bar lists inventory and transfers, shows completed/total bytes and failure
reasons, and provides pause and consent-based copy/retry. Retry requires renewed
permission and license acceptance. Its daemon remains independently owned.

The macOS copy dialog uses the same light/dark cards and pinned actions as
Preferences. Select a reachable peer by hostname to fetch its live authorized
catalog, then select a model to review its size, format, quantization, license and
provenance. Digests and source identities come from that offer; there are no manual
SHA or worker-ID inputs. Switching peer/model or refreshing clears consent. Retry
only offers the original file and manifest revision, including from another peer.
An empty authorized catalog is distinct from a failed fetch. Start errors stay in
the dialog so the selection can be reviewed before trying again.

## Implemented API

All localhost model endpoints use the existing read/control bearer credentials,
loopback/Host/Origin checks and no-store responses. Source paths come from local
Ollama metadata or control-authorized registration; paths never cross peer RPC or events.

| Endpoint | Scope / body |
|---|---|
| `GET /v1/models/inventory` | Read; `{version,scope,models}` with local manifests and live authorized peer offers, labeled `source_worker_id` and `local`. Optional `source_worker_id` fetches only that reachable peer; failure returns 503 rather than an empty list. |
| `GET /v1/models/transfers` | Read; `{version,transfers}` with digest, manifest digest, model ID, preferred source, state, completed/total bytes and reason |
| `POST /v1/models/artifacts` | Control; `LocalArtifact` registration; returns 201 after source content verification |
| `PUT /v1/models/artifacts/{digest}/permission` | Control; allowlist/share/license update |
| `POST /v1/models/transfers` | Control; `{digest,manifest_digest,source_worker_id,permission:true,license_accepted:true}`; 202 means locally accepted transfer, not installed |
| `POST /v1/models/transfers/{digest}/pause` | Control; retains private untrusted partial |
| `POST /v1/models/transfers/{digest}/retry` | Control; same consent body, exact previous manifest digest |

Signed POST RPCs `/peer/v1/models/{inventory,manifest,chunk}` reuse TLS, peer
credentials, request nonce replay fencing and signed bounded responses. Every
chunk rechecks the source's per-model destination allowlist. Owner routes are not
mounted there. Live inventory polling reconciles offers after reconnect; the
existing discovery descriptor remains a routing hint rather than model authority.
Inventory is a reachable local view and never promises completeness.
Signed inventory retains the 128-offer/2-MiB bound, including full model metadata.
Automatic offers add optional content-bound `ollama` import options; update both
daemons to receive those offers. Legacy standalone manifests omit this field and
retain their original canonical hashes and wire shape.

`/v1/node/status` and version 1 SSE snapshots add `transfers` and `model_inventory`.
`transfer` and `model` events signal sampled changes; reconnect gets a fresh snapshot.
Events contain no source file paths, prompts or credentials. The preferred source
label records the request preference; matching alternative sources may supply chunks.

## Safety, budgets and recovery

A unique SQLite digest claim and an exclusive OS lock on the destination's sharing
directory prevent duplicate downloads across tasks and daemon processes. Imports
are serialized locally, never globally. Chunk offsets and private files survive
restart. Restart pauses incomplete work; an explicit consent-bearing retry resumes
the partial. A vanished source can be replaced by another authorized live source
attesting the **same file and manifest digests**. Otherwise the transfer fails with
a useful reason and retains the partial. There is no automatic retry loop or registry
fallback. A repeated start returns conflict; use retry for an existing failed/paused
claim. Corrupt final content is discarded before import.

Chunks are at most 1 MiB, base64 encoded in signed bounded JSON, with individual
SHA-256 checks. Final content hash and GGUF magic must pass before backend import.
Only fixed destination filenames derived from validated hex digests are used.
Source symlinks and symlinked parents are rejected; peer requests contain no file
paths. Staging is private and never a ready inventory entry. Full-sync journal
writes and fsynced file publication precede ready state.

| Setting | Default / purpose |
|---|---|
| `IDLE_HERD_MODEL_AUTO_SHARE` | `true`, discover installed readable standalone GGUF models for default sharing |
| `IDLE_HERD_MAX_MODEL_CACHE_BYTES` | 20 GiB, total managed partial/GGUF content quota |
| `IDLE_HERD_MODEL_RESERVED_DISK_BYTES` | 1 GiB remaining reserve, in addition to remaining download plus one full import-copy allowance |
| `IDLE_HERD_MODEL_TRANSFER_BYTES_PER_SECOND` | 10 MiB/s destination transfer throttle |
| `IDLE_HERD_RESERVE_MEMORY_GB` | Existing worker memory reserve; manifest requirement must fit above it |

Disk and RAM are checked before chunks; destination import load also uses live
worker-local fenced admission. Busy or paused workers cannot bypass admission for
a load probe. Offline/paused stop new peer traffic at the next RPC/chunk; local
inference offline retains the earlier contract. No TTL, transfer pause or failure
releases an uncertain worker execution. Slots remain governed by the worker ledger.

Ollama import uses `/api/blobs/sha256:...` and `/api/create`, preserving the supplied
license, then confirms a load via the admitted local inference path. Ready follows
successful load and verified cache publication. Existing model names are protected
against replacement. A previous imported name may be retried only when the recorded
backend digest still matches. A crash between backend create and recording that
digest conservatively requires owner investigation. A backend may expose an already
verified imported model before the transfer's load-confirmation/ready step completes.
The public import contract is described in [Ollama's API documentation](https://github.com/ollama/ollama/blob/main/docs/api.md).
Repeated local registration preserves the existing path and permission; conflicting
manifest metadata for a digest is rejected. Use the permission endpoint for ACL changes.
Transfer status checks current backend model digests and reports `backend_missing`,
`backend_changed` or `backend_unknown` when the previous installation cannot be confirmed.

## Limits and evidence

Automated tests use fabricated GGUF bytes and mock Ollama APIs. Native processes
exercise TLS, source loss, durable partials, restart/resume/import and single-survivor
inference. They do **not** establish real GGUF loading or hardware compatibility.
See [exact commands/results](implementation-status.md).

Declared OS/CPU/backend-version constraints are checked before download; unsupported
MLX, CUDA-specific and OCI artifacts are rejected. Full operator/quantization/context
support is ultimately delegated to Ollama and the admitted load. Automatic offers
preserve templates, system presets and parameters. Adapters and multi-file snapshots
are not cloned; choose standalone GGUF artifacts.
Real MLX end-to-end import remains unimplemented, and no MLX acceptance is claimed.
Provenance/license metadata is content-bound and authenticated to the enrolled
source, not independently certified by the original publisher.

The cache never automatically deletes manual or pinned models. Quotas exclude
Ollama's own storage, SQLite overhead and external writes; disk checks cannot reserve
space against other programs or measure a backend store on another filesystem.
External tools can race Ollama model-name changes outside the daemon's local lock.
At-rest encryption, registry fallback, full MLX support, real hardware/Windows lock
acceptance and physical GUI tests remain open. No Raft, Docker/Compose, deployment
or publishing was introduced. Next roadmap phase is optional administrative Raft
(Phase 8), with these Phase 7 acceptance limitations retained.
