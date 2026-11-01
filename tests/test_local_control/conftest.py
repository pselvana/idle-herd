"""Owner controls never depend on physical desktop activity or power state."""

import time

import pytest

from fleet_manager.peer.sensors import IdleSignal


class UnknownActivity:
    async def sample(self):
        return IdleSignal(
            observed_at=time.time(),
            source="test_unknown",
            cpu_percent=1,
            memory_available_gb=48,
        )


@pytest.fixture(autouse=True)
def isolate_host_activity(monkeypatch):
    monkeypatch.setattr(
        "fleet_manager.peer.app.native_sensor", lambda helper=None: UnknownActivity()
    )
    monkeypatch.setattr(
        "fleet_manager.peer.local_ledger.native_sensor", lambda helper=None: UnknownActivity()
    )
