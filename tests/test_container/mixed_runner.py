"""Fabricated native backend and peer for the mixed Docker integration test."""

import asyncio
import contextlib
import json
import sys
import time

import uvicorn
from backend import make_app

from fleet_manager.peer.app import create_peer_app, create_peer_rpc_app
from fleet_manager.peer.models import PeerSettings
from fleet_manager.peer.sensors import IdleSignal


class HealthySensor:
    async def sample(self):
        return IdleSignal(observed_at=time.time(), source="test", memory_available_gb=48)


async def main():
    directory, backend_port, ingress_port, rpc_port, host_ip, seed = sys.argv[1:]
    settings = PeerSettings(
        data_dir=directory,
        backend_url=f"http://127.0.0.1:{backend_port}",
        peer_endpoint=f"https://{host_ip}:{rpc_port}",
        seeds=[seed],
        peer_interval=0.2,
        peer_ttl=2,
        request_memory_gb=0.000001,
        reserve_memory_gb=0,
        background_policy="always",
    )
    app = create_peer_app(settings, sensor=HealthySensor())
    async with app.router.lifespan_context(app):
        identity = app.state.identity
        servers = [
            uvicorn.Server(
                uvicorn.Config(
                    make_app(),
                    host="127.0.0.1",
                    port=int(backend_port),
                    lifespan="off",
                    access_log=False,
                    log_level="warning",
                )
            ),
            uvicorn.Server(
                uvicorn.Config(
                    app,
                    host="127.0.0.1",
                    port=int(ingress_port),
                    lifespan="off",
                    access_log=False,
                    log_level="warning",
                )
            ),
            uvicorn.Server(
                uvicorn.Config(
                    create_peer_rpc_app(app),
                    host="0.0.0.0",
                    port=int(rpc_port),
                    ssl_certfile=str(identity.cert_path),
                    ssl_keyfile=str(identity.key_path),
                    lifespan="off",
                    access_log=False,
                    log_level="warning",
                )
            ),
        ]
        for server in servers:
            server.capture_signals = contextlib.nullcontext
        tasks = [asyncio.create_task(server.serve()) for server in servers]
        try:
            while not all(server.started for server in servers):
                for task in tasks:
                    if task.done():
                        await task
                        raise RuntimeError("mixed fixture listener exited")
                await asyncio.sleep(0.05)
            print(json.dumps({"ready": True}), flush=True)
            await asyncio.gather(*tasks)
        finally:
            for server in servers:
                server.should_exit = True
            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
