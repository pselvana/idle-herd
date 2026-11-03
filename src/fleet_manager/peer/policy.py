"""Worker-owned host policy. Observations are never admission capabilities."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime

from fleet_manager.peer.models import (
    AdmissionError,
    EligibilityDecision,
    IdleMode,
    PeerSettings,
    Workload,
)
from fleet_manager.peer.sensors import IdleSignal, Sensor

logger = logging.getLogger(__name__)


class IdlePolicy:
    def __init__(self, settings: PeerSettings, sensor: Sensor, *, clock=time.monotonic):
        self.settings = settings
        self.sensor = sensor
        self.clock = clock
        self._lock = asyncio.Lock()
        self._changed = asyncio.Condition()
        self.revision = 0
        self._since: dict[IdleMode, float] = {}
        self._ready: dict[IdleMode, bool] = {}
        self._last_sample: float | None = None
        self._decisions: dict[tuple[Workload, bool], EligibilityDecision] = {}

    async def _sample(self) -> IdleSignal:
        try:
            async with asyncio.timeout(self.settings.sensor_max_age):
                return await self.sensor.sample()
        except Exception as exc:
            # No helper output, path, session name or input data in diagnostics.
            logger.warning("Host sensor unavailable (%s)", type(exc).__name__)
            return IdleSignal(observed_at=time.time(), source="sensor_failure")

    def _evaluate(
        self, signal: IdleSignal, workload: Workload, idle_only: bool, now: float
    ) -> EligibilityDecision:
        configured = (
            self.settings.interactive_policy
            if workload == "interactive"
            else self.settings.background_policy
        )
        # A caller may tighten a policy, but cannot override never/manual.
        policy: IdleMode = (
            "idle_only"
            if idle_only and configured in {"always", "utilization_only"}
            else configured
        )
        age = time.time() - signal.observed_at
        fresh = 0 <= age <= self.settings.sensor_max_age
        activity = "unknown"
        if fresh and signal.confidence == "host_sensor" and signal.idle_seconds is not None:
            activity = "idle" if signal.idle_seconds >= self.settings.idle_seconds else "active"
        reason = None
        if policy == "never":
            reason = "policy_never"
        elif policy == "manual":
            # Authenticated owner controls arrive in Phase 6. No HTTP caller
            # can turn a static manual policy into a grant in Phase 3.
            reason = "manual_approval_required"
        elif not fresh:
            reason = "sensor_stale"
        elif (
            signal.memory_pressure == "critical"
            or self.settings.memory_admission_mode == "strict"
            and (
                signal.memory_pressure == "warning"
                or signal.memory_available_gb is not None
                and signal.memory_available_gb < self.settings.reserve_memory_gb
            )
        ):
            reason = "memory_pressure"
        elif signal.thermal_state in {"serious", "critical"}:
            reason = "thermal_limit"
        elif self.settings.require_ac and signal.on_ac is not True:
            reason = "ac_required"
        elif (
            signal.on_ac is not True
            and signal.battery_percent is not None
            and signal.battery_percent < self.settings.battery_floor_percent
        ):
            reason = "battery_low"
        elif policy == "idle_only" and activity != "idle":
            reason = "owner_active" if activity == "active" else "activity_unknown"
        elif policy in {"idle_only", "utilization_only"}:
            if signal.cpu_percent is None or signal.memory_available_gb is None:
                reason = "utilization_unknown"
            elif signal.cpu_percent >= self.settings.cpu_busy_percent:
                reason = "cpu_busy"
            elif not self._ready.get(policy, False) and (
                signal.cpu_percent > self.settings.cpu_idle_percent
            ):
                reason = "cpu_recovery"
            elif signal.gpu_percent is not None and (
                signal.gpu_percent >= self.settings.gpu_busy_percent
            ):
                reason = "gpu_busy"
        if reason:
            self._since.pop(policy, None)
            self._ready[policy] = False
        elif policy in {"idle_only", "utilization_only"}:
            since = self._since.setdefault(policy, now)
            if now - since < self.settings.idle_hysteresis_seconds:
                reason = "idle_hysteresis"
            else:
                self._ready[policy] = True
        eligible = reason is None
        return EligibilityDecision(
            workload=workload,
            policy=policy,
            eligible=eligible,
            reason=reason,
            state=(
                "eligible"
                if eligible
                else (
                    "unknown"
                    if reason in {"sensor_stale", "activity_unknown", "utilization_unknown"}
                    else ("ineligible" if policy in {"never", "manual"} else "draining")
                )
            ),
            activity=activity,
            signal=signal,
            observed_at=time.time(),
        )

    async def _observe(self) -> dict[tuple[Workload, bool], EligibilityDecision]:
        async with self._lock:
            signal = await self._sample()
            now = self.clock()
            # Long gaps (including sleep/wake) require a new recovery window.
            if (
                self._last_sample is not None
                and now - self._last_sample > self.settings.sensor_max_age
            ):
                self._since.clear()
                self._ready.clear()
            self._last_sample = now
            decisions = {
                (kind, strict): self._evaluate(signal, kind, strict, now)
                for kind in ("interactive", "background")
                for strict in (False, True)
            }
            changed = any(
                kind not in self._decisions
                or (decision.eligible, decision.reason, decision.activity)
                != (
                    self._decisions[kind].eligible,
                    self._decisions[kind].reason,
                    self._decisions[kind].activity,
                )
                for kind, decision in decisions.items()
            )
            self._decisions = decisions
            if changed:
                async with self._changed:
                    self.revision += 1
                    self._changed.notify_all()
            return decisions

    def _peer_decision(self, decision: EligibilityDecision) -> EligibilityDecision:
        reason = None
        if decision.activity == "active":
            reason = "owner_active"
        elif self.settings.quiet_hours_enabled:
            local = datetime.now().astimezone()
            minute = local.hour * 60 + local.minute
            start = self.settings.quiet_hours_start_minute
            end = self.settings.quiet_hours_end_minute
            in_quiet_hours = (
                start <= minute < end if start < end else minute >= start or minute < end
            )
            if start == end or in_quiet_hours:
                reason = "quiet_hours"
        if reason:
            return decision.model_copy(
                update={"eligible": False, "reason": reason, "state": "draining"}
            )
        return decision

    async def decision(
        self, workload: Workload = "interactive", idle_only: bool = False, *, peer: bool = False
    ) -> EligibilityDecision:
        decision = (await self._observe())[workload, idle_only]
        return self._peer_decision(decision) if peer else decision

    async def require(
        self,
        workload: Workload,
        idle_only: bool = False,
        *,
        additional_workload: Workload | None = None,
        additional_idle_only: bool = False,
        peer: bool = False,
    ) -> EligibilityDecision:
        # Evaluate stored and caller-tightened constraints from one live sample;
        # a second sample must not invalidate the first check unnoticed.
        decisions = await self._observe()
        if peer:
            decisions = {key: self._peer_decision(decision) for key, decision in decisions.items()}
        required = [(workload, idle_only)]
        if additional_workload is not None:
            required.append((additional_workload, additional_idle_only))
        for key in required:
            decision = decisions[key]
            if not decision.eligible:
                raise AdmissionError(decision.reason or "policy_ineligible", 503)
        return decisions[workload, idle_only]

    async def wait_for_change(self, revision: int, timeout: float) -> int:
        """Lossless local wakeup hook for eligibility retry loops (not a slot grant)."""
        async with self._changed, asyncio.timeout(timeout):
            await self._changed.wait_for(lambda: self.revision != revision)
            return self.revision

    async def run(self):
        while True:
            await self.decision()
            await asyncio.sleep(self.settings.sensor_interval)
