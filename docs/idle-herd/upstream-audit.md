# Upstream audit (Phase 0)

Scope: static inspection of checkout `66460f0909f20637bb4afe50bf7e872bc348dacb`; no live services were contacted. This records current upstream behavior, not the proposed Idle-Herd peer design. The implementation plan's upstream pin is maintained separately. Project metadata identifies `ollama-herd` 0.12.2, Python >=3.11, with `herd` and `herd-node` entry points (`pyproject.toml`). `AGENTS.md`'s v0.7.0 description and seven-signal summary are historical; `ScoringEngine.score_request` now also applies session/prefix affinity. Both onboarding files' 1,985-test count is a release snapshot, not a fresh measurement. FastAPI metadata in `server/app.py:create_app` still says 0.1.0, distinct from package version.

## Boundaries and reuse

- `server/app.py:lifespan` constructs a router-owned registry, scorer, queues, proxy and stores, advertises mDNS, and starts monitors, optimizer, rebalancer and preloader. `node/agent.py:NodeAgent.start` separately discovers or uses one configured router, starts available local services, and sends periodic heartbeats. The node's discovery precedes sidecar startup; the current pair does not establish the plan's independent peer ingress or guaranteed disconnected local scheduling.
- `server/registry.py:NodeRegistry.update_from_heartbeat` maintains mutable, in-memory router state keyed by node ID; heartbeat monitoring and drain change status. `routes/heartbeat.py:receive_heartbeat` also returns optimizer commands. These are router observations, not authenticated membership, durable identity, or worker-issued capacity authority.
- `server/scorer.py:ScoringEngine` and `server/model_knowledge.py` provide useful model fit and ranking heuristics using registry data; thermal, memory, queue depth, wait, role, availability trend, context, session and prefix signals are not admission guarantees. `node/capacity_learner.py:AdaptiveCapacityLearner` predicts capacity from history, not immediate host-idle consent.
- `server/queue_manager.py:QueueManager` owns per-node/model in-process queues, workers, client limits and progress-based zombie reaping at the router. `enqueue`, `_worker`, and slot release do not provide durable worker-local fenced leases or prevent another independent router from submitting work. `server/streaming.py:StreamingProxy` provides backend proxying, retry, stream conversion, context protection and trace handling, but resolves nodes via router registry and queue machinery. Preserve failure and disconnect tracing when adapting it (`routes/routing.py:dispatched_stream`, `record_routing_rejection`, `server/trace_store.py:TraceStore`).
- `node/collector.py:collect_heartbeat`, `common/system_metrics.py` and shared `models/node.py` contain reusable host and backend observations. OS-specific signal support varies; thermal and memory pressure probes explicitly branch by platform. They are not the proposed user-activity sensors. `node/mlx_supervisor.py` and `node/mlx_client.py` supervise/report MLX servers; `server/mlx_proxy.py` is a distinct opt-in path.
- Native Ollama is auto-started if needed, and the agent may create a LAN proxy for a loopback-only Ollama (`NodeAgent._ensure_ollama`, `_ensure_lan_proxy`). In-process image and embedding servers and transcription/MLX child processes are started where available. POSIX signal handlers initiate drain; Windows deliberately lacks these handlers and relies on heartbeat expiry after termination (`NodeAgent._install_drain_handlers`). This is not a native service installer or cross-platform graceful-drain guarantee.

## Registered router API surface

`server/app.py:create_app` includes the following route modules. This inventory lists registered paths, not a promise of full upstream protocol parity or availability of every backend. FastAPI also supplies its default OpenAPI/docs routes.

- General: `GET /` (dashboard redirect), `HEAD /`, `POST /heartbeat`.
- OpenAI-compatible: `GET /v1/models`; `POST /v1/chat/completions`, `/v1/embeddings`, `/v1/images/generations`, `/v1/rerank`, `/v1/systemone`, `/v1/responses`.
- Anthropic-compatible: `POST /v1/messages`, `/v1/messages/count_tokens`; `GET /v1/messages`.
- Ollama-compatible: `POST /api/chat`, `/api/generate`, `/api/show`, `/api/pull`, `/api/embed`, `/api/embeddings`; `GET /api/version`, `/api/tags`, `/api/ps`.
- Other modalities: `POST /api/embed-image`, `/api/transcribe`, `/api/generate-image`; `GET /api/image-models`.
- Fleet: `GET /fleet/status`, `/fleet/limits`, `/fleet/queue`; `POST /fleet/pin`; `DELETE /fleet/pin/{model:path}`.
- Dashboard: `GET /dashboard/events`; pages `/dashboard`, `/dashboard/trends`, `/dashboard/models`, `/dashboard/tags`, `/dashboard/apps`, `/dashboard/benchmarks`, `/dashboard/health`, `/dashboard/recommendations`, `/dashboard/settings`, `/dashboard/color-states`; favicon paths `/favicon.svg`, `/favicon.ico`, `/favicon-alert.svg`.
- Dashboard API: `GET /dashboard/api/trends`, `/models`, `/overview`, `/usage`, `/traces`, `/tags`, `/apps`, `/tags/daily`, `/apps/daily`, `/benchmarks`, `/benchmarks/progress`, `/benchmarks/{run_id}`, `/health`, `/context-usage`, `/briefing`, `/briefing/history`, `/recommendations`, `/settings`, `/image-stats`, `/transcription-stats`, `/model-management`, `/pinned-models`; `POST /dashboard/api/benchmarks`, `/benchmarks/start`, `/benchmarks/cancel`, `/settings`, `/pull`, `/delete`, `/pinned-models`. All abbreviated paths in this bullet use the `/dashboard/api` prefix.

See `server/routes/{openai_compat,ollama_compat,anthropic_compat,responses_compat,embedding_compat,text_embedding_compat,image_compat,transcription_compat,fleet,dashboard,heartbeat}.py`. Text embedding dispatch is called by compatibility routes rather than registered as a separate router. No planned `/v1/cluster/*`, reservation, job journal, peer enrollment or model transfer routes are present in this inventory.

## Backends and security observations

- Actual paths cover Ollama; opt-in `mlx_lm.server`; native fastembed text and ONNX vision embeddings; mflux/DiffusionKit image generation; and `mlx-qwen3-asr` transcription (`node/agent.py`, `node/image_server.py`, `server/routes/*_compat.py`). Model names, installed binaries, enabled settings, hardware and reachable node ports affect availability. The plan's generic LM Studio/vLLM/llama.cpp backend adapter is proposed, not established by these paths.
- `models/config.py:ServerSettings` defaults router host to `0.0.0.0:11435`. Agent-started image, transcription and native embedding services bind `0.0.0.0`; Ollama auto-start defaults to `0.0.0.0:11434`, and a LAN proxy may expose existing loopback Ollama. MLX subprocesses default to loopback (`NodeSettings.mlx_bind_host`). Review actual bind and firewall configuration on each platform before exposing a deployment.
- `create_app` installs no global authentication middleware. `receive_heartbeat` accepts registration and drain without a credential; dashboard and fleet include state-changing routes. `anthropic_compat.py:_check_auth` checks an optional `FLEET_ANTHROPIC_API_KEY` on the messages route, default empty; it is not fleet-wide authentication. Default CORS is unset (`server/cors.py`, `ServerSettings.cors_origins`), which does not authenticate network clients or substitute for CSRF protections. Design authenticated peer and separately protected local control boundaries before reuse.
- Despite the onboarding's zero-cloud description, `ServerSettings.telemetry` defaults on: `server/community_telemetry.py:run_scheduler` may POST daily aggregated telemetry to `https://ollamaherd.com/api/v1/telemetry`, including an install ID and salted device IDs, hardware/version and request aggregates. Opt out via `FLEET_NODE_TELEMETRY=false` or `FLEET_TELEMETRY=false`; URL aliases are configurable. This is external network traffic and must be explicitly accounted for in fork defaults and privacy design. Local JSONL and SQLite trace data are separate (`common/logging_config.py`, `server/trace_store.py`); review their contents and retention independently.

## Provenance and constraints

`LICENSE` is MIT, copyright 2025 Ollama Herd Contributors; retain its copyright and permission notice when copying substantial portions. `pyproject.toml` identifies the upstream repository as `https://github.com/geeks-accelerator/ollama-herd`. Third-party packages, model weights and binaries have their own terms; the repository's MIT license does not grant model redistribution rights. The plan's module table uses paths relative to `src/fleet_manager/` after its first row; proposed `peer/`, peer RPC and adapter modules in section 7 are future structure, not current upstream modules.

The operational record in `docs/issues.md` and `docs/observations.md` contains release-specific incidents and caveats: preserve request-outcome tracing, validate streaming completion and queue reaping, and do not infer Windows/macOS/Linux runtime coverage from mocks or a single-host soak. Baseline test and lint results belong to the Phase 0 verification report; this audit did not run them.

The checked-out `.github` tree has issue templates but no Actions workflows. The
available aggregate check is `scripts/health.sh`; Phase 0 ran its core test/lint/
source-format commands directly, not a CI pipeline. Despite onboarding's offline
suite claim, `tests/test_node/test_telemetry_openapi_contract.py` fetches a live
public schema in five cases and skips if unavailable. Other tests bind real
ephemeral sockets and probe macOS kernel/process state. Phase 0 isolated two
real-home writes and one MLX orphan scan in tests; see the verification report
for both the original sandbox failures and the successful native rerun.
