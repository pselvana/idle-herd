"""Actionable local explanations, without payloads, credentials or network probes."""

from __future__ import annotations

from fleet_manager.local_control.models import DiagnosticCheck, Diagnostics, ModelFit, NodeStatus
from fleet_manager.models.request import normalize_model_name
from fleet_manager.peer.models import AdmissionError

_REMEDIES = {
    "activity_unknown": (
        "Check host sensor/session permissions. Unknown activity cannot grant idle-only work."
    ),
    "owner_active": "Wait until the owner is idle; peer reports cannot override local activity.",
    "sensor_stale": (
        "Check the native sensor helper and daemon status after wake; wait for a fresh sample."
    ),
    "utilization_unknown": "Check host metric support; do not treat an unknown metric as zero.",
    "idle_hysteresis": "Wait for the configured recovery interval with fresh idle samples.",
    "cpu_busy": "Wait for CPU load to fall below the configured thresholds.",
    "cpu_recovery": "Wait for CPU load to fall below the recovery threshold.",
    "gpu_busy": "Wait for GPU load to fall below the configured threshold.",
    "memory_pressure": "Reduce memory use or choose a smaller model; verify the RAM reserve.",
    "insufficient_memory": (
        "Choose a smaller model or reduce competing memory use; fit is an estimate."
    ),
    "thermal_limit": "Allow the host to cool before accepting more work.",
    "ac_required": "Connect AC power or explicitly change the owner's AC-only preference.",
    "battery_low": "Charge the battery above the configured floor.",
    "quiet_hours": "Wait until quiet hours end or change the owner's local-time schedule.",
    "peer_quota": (
        "Wait for this peer's existing work to finish or change its contribution limit."
    ),
    "peer_quota_exceeded": (
        "Wait for this peer's existing work to finish or change its contribution limit."
    ),
    "not_installed": (
        "Install a compatible local model or authorize a verified model copy from a reachable peer."
    ),
    "unknown_model_size": (
        "Check the backend inventory; admission requires a known positive model size."
    ),
    "backend_unavailable": (
        "Start or repair the separately managed local Ollama backend on port 11434."
    ),
    "busy": "Wait for worker-issued capacity; observed cluster slots are advisory.",
    "manual_offline": (
        "Use idle-herd auto to rejoin peers. Offline still permits eligible localhost inference."
    ),
    "manual_draining": "Use idle-herd auto to accept peer work again; existing execution drains.",
    "manual_paused": (
        "Use idle-herd auto to allow new local and peer inference; existing execution drains."
    ),
    "policy_never": "Change the workload policy only if the owner wants to permit this work.",
    "manual_approval_required": (
        "Choose an explicitly authorized automatic policy to admit new work."
    ),
}


def remedy(reason: str | None) -> str | None:
    return (
        _REMEDIES.get(reason, "Inspect local preferences and backend readiness.")
        if reason
        else None
    )


def redacted_model_name(value: str) -> str:
    if value.startswith(("/", "~", "\\")) or (len(value) > 2 and value[1:3] in (":/", ":\\")):
        return "[local model]"
    return value


async def diagnose(app, status: NodeStatus, model: str | None) -> Diagnostics:
    checks = [
        DiagnosticCheck(
            code="local_ingress",
            title="Local API",
            state="ok",
            detail="This daemon serves localhost independently of peers and optional admin quorum.",
        ),
        DiagnosticCheck(
            code="backend",
            title="Local backend",
            state="ok" if status.backend.state == "online" else "attention",
            detail=(
                f"Backend {status.backend.state}; "
                f"{len(status.backend.models_available)} installed model(s) observed."
            ),
            remedy=None if status.backend.state == "online" else _REMEDIES["backend_unavailable"],
        ),
        DiagnosticCheck(
            code="background_policy",
            title="Peer background eligibility",
            state="ok" if status.idle.eligible else "attention",
            detail=status.idle.reason
            or "Eligible at this observation; live worker admission is still required.",
            remedy=remedy(status.idle.reason),
        ),
        DiagnosticCheck(
            code="sensor",
            title="Host activity and provenance",
            state="unknown" if status.idle.activity == "unknown" else "ok",
            detail=(
                f"Activity {status.idle.activity}; source {status.resources.source}. "
                "Missing measurements remain unknown."
            ),
            remedy=_REMEDIES["activity_unknown"] if status.idle.activity == "unknown" else None,
        ),
        DiagnosticCheck(
            code="quarantine",
            title="Uncertain backend execution",
            state="attention" if status.slots.quarantined else "ok",
            detail=(
                f"{status.slots.quarantined} quarantined slot(s). "
                "Expiry cannot prove a backend stopped."
            ),
            remedy=(
                "Stop the daemon AND all backend runners, then use "
                "idle-herd reconcile --confirm-backend-stopped."
            )
            if status.slots.quarantined
            else None,
        ),
        DiagnosticCheck(
            code="peers",
            title="Peer visibility",
            state="attention" if status.peers.unreachable else "ok",
            detail=(
                f"{status.peers.reachable} reachable, {status.peers.unreachable} "
                "unreachable known peers. This is a partial local view, "
                "not proof of a partition."
            ),
            remedy=(
                "Check private endpoint/firewall, clock, enrollment, certificate expiry "
                "and mutual trust. Either survivor can run eligible local work."
            )
            if status.peers.unreachable
            else None,
        ),
        DiagnosticCheck(
            code="peer_listener",
            title="Peer listener",
            state="attention" if app.state.peer_error else "ok",
            detail="Listener unavailable; local ingress remains independent."
            if app.state.peer_error
            else (
                "Authenticated discovery configured."
                if app.state.directory
                else "Discovery disabled; supported single-node operation."
            ),
            remedy="Check the configured private TLS endpoint and local port conflicts."
            if app.state.peer_error
            else None,
        ),
        DiagnosticCheck(
            code="bonjour",
            title="Bonjour LAN discovery",
            state="attention" if getattr(app.state, "mdns_error", None) else "ok",
            detail=(
                "Multicast discovery unavailable; local inference remains independent."
                if getattr(app.state, "mdns_error", None)
                else "Automatic Bonjour discovery configured."
                if app.state.ledger.settings.mdns_enabled
                else "Bonjour disabled; explicit peer endpoints/seeds remain supported."
            ),
            remedy="Check multicast UDP 5353, interface permissions and LAN client isolation."
            if getattr(app.state, "mdns_error", None)
            else None,
        ),
        DiagnosticCheck(
            code="coordination",
            title="Optional admin coordination",
            state="ok" if not status.coordination.enabled else "unknown",
            detail="Disabled by default; admin quorum never grants inference capacity."
            if not status.coordination.enabled
            else (
                "Cached admin observation only; loss of voters affects strict metadata, "
                "not local execution."
            ),
        ),
        DiagnosticCheck(
            code="permissions",
            title="Owner control authentication",
            state="ok",
            detail=(
                "Read credential verified. Control scope is separate from peer identity; "
                "this does not certify Windows ACLs or same-user isolation."
            ),
        ),
    ]
    fit = None
    if model is not None:
        try:
            capacity = await app.state.ledger.capacity(normalize_model_name(model))
            fit = ModelFit(
                model=redacted_model_name(capacity.model),
                eligible=capacity.eligible and capacity.observed_available_slots > 0,
                reason=capacity.reason
                or ("busy" if capacity.observed_available_slots == 0 else None),
                observed_available_slots=capacity.observed_available_slots,
                observed_at=capacity.observed_at,
            )
            checks.append(
                DiagnosticCheck(
                    code="model_fit",
                    title="Requested local model",
                    state="ok" if fit.eligible else "attention",
                    detail=(
                        f"{fit.model}: {fit.reason or 'eligible'}; "
                        f"{fit.observed_available_slots} slot(s) observed. "
                        "Weight/runtime RAM estimates do not certify arbitrary context or GPU fit."
                    ),
                    remedy=remedy(fit.reason),
                )
            )
        except AdmissionError as exc:
            checks.append(
                DiagnosticCheck(
                    code="model_fit",
                    title="Requested local model",
                    state="attention",
                    detail=exc.reason,
                    remedy=remedy(exc.reason),
                )
            )
    return Diagnostics(
        peer_discovery="enabled" if app.state.directory else "disabled",
        peer_error=bool(app.state.peer_error),
        quarantined_slots=status.slots.quarantined,
        worker_id=status.worker_id,
        worker_incarnation=status.worker_incarnation,
        platform=status.platform,
        observed_at=status.observed_at,
        checks=checks,
        model_fit=fit,
    )
