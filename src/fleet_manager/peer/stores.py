"""Drain inherited asynchronous trace/latency writes before closing peer storage."""

import asyncio

from fleet_manager.server.latency_store import LatencyStore
from fleet_manager.server.trace_store import TraceStore


class DrainWrites:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pending_writes = set()

    async def tracked_write(self, method, args, kwargs):
        task = asyncio.current_task()
        self._pending_writes.add(task)
        try:
            return await method(*args, **kwargs)
        finally:
            self._pending_writes.discard(task)

    async def close(self):
        # Shared routes schedule write coroutines with create_task/ensure_future.
        # Let those already-scheduled calls enter tracked_write before draining.
        await asyncio.sleep(0)
        try:
            if self._pending_writes:
                await asyncio.gather(*self._pending_writes)
        finally:
            await super().close()


class PeerTraceStore(DrainWrites, TraceStore):
    async def record_trace(self, *args, **kwargs):
        return await self.tracked_write(super().record_trace, args, kwargs)


class PeerLatencyStore(DrainWrites, LatencyStore):
    async def record(self, *args, **kwargs):
        return await self.tracked_write(super().record, args, kwargs)
