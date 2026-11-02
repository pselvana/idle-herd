#!/usr/bin/env python3
"""Measure mock-only native lease contention; never contacts a real model or fleet."""

import argparse
import asyncio
import json
import math
import os
import statistics
import sys
import tempfile
import time
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "tests" / "test_peer" / "native_runner.py"


@asynccontextmanager
async def process(role, *args):
    env = {key: value for key, value in os.environ.items() if not key.startswith("IDLE_HERD_")}
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        str(RUNNER),
        role,
        *map(str, args),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        env=env,
    )
    try:
        line = await asyncio.wait_for(child.stdout.readline(), 20)
        if not line:
            raise RuntimeError(f"mock {role} failed to start (exit {child.returncode})")
        yield json.loads(line)["url"]
    finally:
        if child.returncode is None:
            child.terminate()
            try:
                await asyncio.wait_for(child.wait(), 10)
            except TimeoutError:
                child.kill()
                await child.wait()


def percentile(values, p):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * p) - 1)]


async def run(clients, hold_ms):
    async with asyncio.timeout(75), AsyncExitStack() as stack:
        directory = stack.enter_context(tempfile.TemporaryDirectory(prefix="idle-herd-bench-"))
        backend = await stack.enter_async_context(process("backend"))
        peer = await stack.enter_async_context(process("peer", Path(directory) / "peer", backend))
        client = await stack.enter_async_context(httpx.AsyncClient(trust_env=False, timeout=55))
        gate = asyncio.Event()

        async def attempt(index):
            await gate.wait()
            start = time.perf_counter()
            reply = await client.post(
                peer + "/v1/capacity/acquire",
                json={"model": "tiny", "request_id": f"benchmark-{index}", "wait_ms": 50000},
            )
            elapsed = (time.perf_counter() - start) * 1000
            if reply.status_code != 201:
                raise RuntimeError(f"acquire failed: {reply.status_code} {reply.text}")
            lease = reply.json()
            try:
                await asyncio.sleep(hold_ms / 1000)
            finally:
                release = await client.delete(
                    peer + "/v1/capacity/reservations/" + lease["reservation_id"],
                    headers={"X-Idle-Herd-Reservation": lease["token"]},
                )
                release.raise_for_status()
            return elapsed

        tasks = [asyncio.create_task(attempt(i)) for i in range(clients)]
        gate.set()
        samples = await asyncio.gather(*tasks)
        return {
            "scope": "mock backend; native peer; loopback HTTP; one slot; no model inference",
            "clients": clients,
            "hold_ms": hold_ms,
            "samples": len(samples),
            "acquire_latency_ms": {
                "p50": round(statistics.median(samples), 3),
                "p95_nearest_rank": round(percentile(samples, 0.95), 3),
                "max": round(max(samples), 3),
            },
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clients", type=int, default=10, choices=range(1, 51), metavar="1..50")
    parser.add_argument("--hold-ms", type=int, default=20, choices=range(0, 501), metavar="0..500")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.clients, args.hold_ms)), indent=2))


if __name__ == "__main__":
    main()
