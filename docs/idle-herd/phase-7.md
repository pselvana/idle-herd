# Phase 7: authenticated model sharing

Each daemon owns its model catalog, staging directory, SQLite transfer journal and
installation lock. Transfers use direct authenticated peer RPC; discovery seeds,
leaders and Raft have no authority over installation or inference. The implemented
adapters import standalone GGUFs and complete **MLX/safetensors, multimodal and
multi-file packages into Ollama** through supported blob/create APIs. Installed
Ollama exports read only the selected manifest and its referenced blobs; backend
directories are never installed or overwritten directly. Standalone MLX export
directories are also supported as sources. No separate mlx_lm service is introduced.

## Default sharing and operation

The owner now enables source sharing by default. Daemons discover installed
Ollama models through `/api/tags`, `/api/show` and a read-only local manifest adapter, then offer them
to all authenticated peers without manual registration. New registrations and
successfully imported copies also default to `allowed_peers:["*"]`,
`sharing_enabled:true` and `license_approved:true`. This source policy does not
establish a publisher's license grant; destination permission and license acceptance
remain explicit for each copy/retry. Existing explicit denials and allowlists are
preserved. Peer authentication, TLS and signed/replay-protected requests still
precede every inventory, manifest and chunk operation. See [peer trust](bonjour.md).

`IDLE_HERD_MODEL_AUTO_SHARE=false` disables automatic model discovery; manually
registered artifacts retain their per-model permissions. Discovery refreshes within
30 seconds and shares installed models even when unloaded. Legacy standalone offers
deduplicate identical weight digests; package identities also bind model metadata.
It opens absolute content-addressed blobs from local Ollama's metadata or its
digest-verified selected manifest. Source symlinks and special files are rejected.
The destination verifies every downloaded component before import. If Ollama
is temporarily unavailable, last observed readable file offers can remain; removed
models withdraw after a successful inventory refresh. Discovery failures are logged.

Automatic offers include content-bound full license text, template, system preset
and parsed parameters, preserving custom Ollama model behavior through supported
create APIs. An absent license is explicitly labeled as unknown for owner review.
MLX packages include configuration/tokenizer files and all installed tensor blobs;
their safetensors shard index is regenerated from tensor headers rather than naming
pre-import source shards. GGUF packages include all model shards, projectors,
adapters and draft-model files described by the source. Renderer, parser, preset
messages, capabilities, template/system/parameters and license are content-bound.
Backend support for a particular architecture or adapter remains required; an
import rejection is reported without treating the copy as ready.

MLX destinations require Apple Silicon macOS and Ollama 0.40.0 or newer with a
functioning safetensors/MLX importer. Backend validation is still authoritative.
Conversational multimodal models retain their declared vision/audio capabilities;
vision-capable load checks include a small valid image. A load smoke check is not
a full evaluation of every modality, operator, quantization or context size.
The daemon must be able to read its source paths, including in containers. A
container cannot obtain backend-host files merely by pointing at its HTTP endpoint.

### Standalone export directories and custom Ollama storage

Configure absolute materialized model directories in the source daemon's environment:

```sh
IDLE_HERD_MODEL_EXPORTS='{"local-qwen:latest":"/absolute/path/to/mlx-export"}'
IDLE_HERD_OLLAMA_MODELS_DIR=/absolute/path/to/ollama/models
```

The second setting is optional; discovery also checks a store inferred from a GGUF
blob path, `OLLAMA_MODELS`, and normal per-user/service locations. MLX's public
Modelfile may contain a model name rather than filenames, so its local manifest
must be readable. Manifest lists resolve the exact selected child and verify its
digest before enumerating files. No unselected runner or registry fallback is used.

Configured directories are hashed on first discovery and when file identity,
size or modification time changes. Local status reads remain nonblocking while
this runs, and active transfers/revocation are not blocked by unrelated export
hashing. Shutdown cancels and awaits discovery. Model files, nested configuration,
tokenizers and templates are retained; `.git`, `.cache` and `.DS_Store` are excluded.
Symlink-based Hugging Face snapshots must first be materialized as regular files.
Source Ollama need not be running to offer configured standalone exports.

Alternatively register a directory once without entering per-file hashes:

```sh
uv run idle-herd models register-directory /absolute/path/to/export \
  --model local-qwen:latest --format mlx \
  --license 'actual model license' --provenance 'publisher/repository@revision'
```

Use `--format gguf-package` for GGUF/projector/shard exports. `adapters/` and
`draft/` select their corresponding create-API roles. Registration verifies every
file; its longer control timeout allows large exports. Directory registration
requires the existing control credential. The destination imports through its
local Ollama backend; direct standalone mlx_lm serving is outside this adapter.

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
Manual package manifests add `files` (name, role, byte size and SHA-256). Their
`digest` is `manifest.calculated_package_digest()`, which binds the full export
descriptor excluding its two digest fields, rather than a concatenated-file hash.
`source_files` and optional generated `inline_files` stay local to the owner record.
The GUI and directory command supply these identities automatically. Packages use
`format:"mlx"` or `format:"gguf-package"`, both with `backend:"ollama"`; legacy
single-file registrations retain `format:"gguf"` and their original file SHA.

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
Inventory is a reachable local view and never promises completeness. New clients
send signed `{"packages":true}` inventory requests; legacy `{}` requests receive
only offers matching their original schema. A verified 400/422 from an older
strict server triggers a fresh signed `{}` fallback. Signature failures never
trigger fallback. Upgrade both peers for package discovery/transfer.
Signed inventory retains the 128-offer/2-MiB bound, including full model metadata.
Automatic offers add optional content-bound `ollama` import options; update both
daemons to receive those offers. Legacy standalone manifests omit this field and
retain their original canonical hashes and wire shape.
Package manifests add up to 4,096 uniquely named files. Names are bounded portable
relative paths without traversal, drive prefixes, reserved device names or
case-folding collisions. The complete ordered descriptor, roles, size totals,
metadata and per-file SHA-256s bind the package identity. Chunk offsets address
the ordered concatenation of file contents; no zip/tar extraction is involved.

`/v1/node/status` and version 1 SSE snapshots add `transfers` and `model_inventory`.
`transfer` and `model` events signal sampled changes; reconnect gets a fresh snapshot.
Events contain no source file paths, inference request prompts/results or credentials.
Shared model definitions can include templates, system presets and preset messages.
The preferred source
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
Completed packages use `.package` cache files, included in the same aggregate cache
quota as GGUFs and partials. Every component is hashed before any backend blob is
uploaded. GGUF magic, safetensors header bounds, JSON configuration and shard-index
references are checked; missing shards, duplicate names or corrupt files fail
before backend creation. Splits can resume across file boundaries and daemon
restart. A complete download with a failed load check reuses the full partial and
matching backend-import record on explicit retry.
Source symlinks and symlinked parents are rejected; peer requests contain no file
paths. Staging is private and never a ready inventory entry. Full-sync journal
writes and fsynced file publication precede ready state.

| Setting | Default / purpose |
|---|---|
| `IDLE_HERD_MODEL_AUTO_SHARE` | `true`, discover installed models and configured standalone exports |
| `IDLE_HERD_MODEL_EXPORTS` | `{}`, JSON mapping model IDs to absolute standalone export directories |
| `IDLE_HERD_OLLAMA_MODELS_DIR` | unset, optional absolute source Ollama models-store override |
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
license, then confirms a load via the admitted local inference path.
For packages, blobs are uploaded independently and original relative names map to
`files`, `adapters` and `draft_files` in the create request. Draft names are relative
to the draft root. The destination never writes a source manifest directly into
Ollama storage. Ollama versions that no longer accept LoRA adapters reject such
imports explicitly; byte transport does not override backend capabilities.
Ready follows successful load and verified cache publication. Existing model names are protected
against replacement. A previous imported name may be retried only when the recorded
backend digest still matches. A crash between backend create and recording that
digest conservatively requires owner investigation. A backend may expose an already
verified imported model before the transfer's load-confirmation/ready step completes.
The public import contract is described in [Ollama's API documentation](https://github.com/ollama/ollama/blob/main/docs/api.md).

Load confirmation inspects the imported model's live `/api/show` capabilities.
Embedding-only models such as `bge-m3` use a small
[`/api/embed`](https://docs.ollama.com/api/embed) request and must return a nonempty
numeric embedding vector; completion-capable models retain the generation check.
Both checks run through the destination's local ingress and fenced worker
admission. Native Idle-Herd keeps Ollama embedding models on that admitted backend,
including names handled by the legacy router's separate fastembed adapter.

The menu bar's byte bar measures download progress. At full size, installation and
load verification may still be running. `backend_load_unconfirmed` means the bytes
were downloaded and the backend import was recorded, but its usable load was not
confirmed. With the capability-aware fix installed on the destination, use Retry
and renew consent; an intact full partial and matching recorded backend installation
are reused across daemon restart. Do not delete the downloaded file or imported
model to recover from this particular error. Changed backend identities, corrupt
partials and uncertain execution retain their existing protections.
Repeated local registration preserves the existing path and permission; conflicting
manifest metadata for a digest is rejected. Use the permission endpoint for ACL changes.
Transfer status checks current backend model digests and reports `backend_missing`,
`backend_changed` or `backend_unknown` when the previous installation cannot be confirmed.

## Limits and evidence

Automated tests use fabricated GGUF bytes and mock Ollama APIs. Native processes
exercise TLS, source loss, durable partials, restart/resume/import and single-survivor
inference. They do **not** establish real GGUF loading or hardware compatibility.
See [exact commands/results](implementation-status.md).

Declared OS/CPU/backend-version constraints are checked before download; MLX
destinations additionally enforce Apple Silicon and a compatible backend version.
CUDA-specific and OCI adapters remain unsupported. Full operator/quantization/context
support is ultimately delegated to Ollama and the admitted load. Automatic offers
preserve templates, system presets and parameters. Model packages carry all required
files; standalone MLX snapshots and installed MLX tensors have implemented export/
import paths. Native mock-process tests cover complete MLX and multimodal imports,
source loss, resume and local survivor inference. Real MLX/Metal execution, real
multimodal inference and broad architecture/operator acceptance remain unverified.
Provenance/license metadata is content-bound and authenticated to the enrolled
source, not independently certified by the original publisher.

The cache never automatically deletes manual or pinned models. Quotas exclude
Ollama's own storage, SQLite overhead and external writes; disk checks cannot reserve
space against other programs or measure a backend store on another filesystem.
External tools can race Ollama model-name changes outside the daemon's local lock.
At-rest encryption, registry fallback, real-model MLX acceptance, real hardware/Windows lock
acceptance and physical GUI tests remain open. No Raft, Docker/Compose, deployment
or publishing was introduced. Next roadmap phase is optional administrative Raft
(Phase 8), with these Phase 7 acceptance limitations retained.
