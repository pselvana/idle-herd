"""Native Phase 2 test processes; all backend data and inference are fabricated."""

import asyncio
import json
import sys
import time

import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse


def mock_backend(model):
    app = FastAPI()
    state = {"active": 0, "peak": 0, "started": 0, "blocked": False}
    finish = asyncio.Event()
    jobs = set()

    @app.get("/api/tags")
    async def tags():
        return {"models": [{"name": f"{model}:latest", "size": 1000}]}

    @app.get("/api/ps")
    async def loaded():
        return {"models": []}

    @app.get("/state")
    async def get_state():
        return state

    @app.post("/block")
    async def block():
        finish.clear()
        state["blocked"] = True
        return state

    @app.post("/finish")
    async def unblock():
        state["blocked"] = False
        finish.set()
        return state

    @app.post("/api/chat")
    async def chat():
        async def execute():
            state["active"] += 1
            state["started"] += 1
            state["peak"] = max(state["peak"], state["active"])
            try:
                if state["blocked"]:
                    await finish.wait()
            finally:
                state["active"] -= 1

        job = asyncio.create_task(execute())
        jobs.add(job)
        job.add_done_callback(jobs.discard)

        async def output():
            yield json.dumps({"message": {"content": model}, "done": False}) + "\n"
            await asyncio.shield(job)
            yield (
                json.dumps(
                    {
                        "model": f"{model}:latest",
                        "message": {"content": ""},
                        "done": True,
                        "done_reason": "stop",
                        "eval_count": 1,
                    }
                )
                + "\n"
            )

        return StreamingResponse(output(), media_type="application/x-ndjson")

    @app.post("/api/embed")
    async def embed():
        return {"model": f"{model}:latest", "embeddings": [[1.0, 2.0]]}

    return app


async def serve_backend(model):
    server = uvicorn.Server(
        uvicorn.Config(mock_backend(model), host="127.0.0.1", port=0, log_level="warning")
    )
    serving = asyncio.create_task(server.serve())
    while not server.started:
        if serving.done():
            await serving
            raise RuntimeError("fixture backend exited before startup")
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    print(json.dumps({"url": f"http://127.0.0.1:{port}"}), flush=True)
    await serving


async def serve_peer(directory, backend, port, seeds):
    import base64

    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from fastapi import Request
    from starlette.responses import Response

    from fleet_manager.peer.app import create_peer_app, create_peer_rpc_app
    from fleet_manager.peer.models import PeerSettings
    from fleet_manager.peer.sensors import IdleSignal

    class HealthySensor:
        async def sample(self):
            return IdleSignal(observed_at=time.time(), source="test", memory_available_gb=48)

    settings = PeerSettings(
        data_dir=directory,
        backend_url=backend,
        request_memory_gb=0.000001,
        background_policy="always",
        reserve_memory_gb=0,
        peer_endpoint=f"https://127.0.0.1:{port}",
        seeds=seeds,
        peer_interval=0.2,
        peer_ttl=1,
    )
    app = create_peer_app(settings, sensor=HealthySensor())
    blocked = set()

    @app.post("/test/block-peers")
    async def block_peers(request: Request):
        blocked.clear()
        blocked.update(await request.json())
        return {"blocked": sorted(blocked)}

    class Partition:
        def __init__(self, target):
            self.target = target

        async def __call__(self, scope, receive, send):
            if scope["type"] == "http":
                headers = dict(scope["headers"])
                try:
                    cert = x509.load_pem_x509_certificate(base64.b64decode(headers[b"x-idle-cert"]))
                    worker = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
                    if worker in blocked:
                        await Response(status_code=503)(scope, receive, send)
                        return
                except (KeyError, ValueError, IndexError):
                    pass
            await self.target(scope, receive, send)

    local = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning"))
    local_task = asyncio.create_task(local.serve())
    rpc_task = None
    try:
        while not local.started:
            if local_task.done():
                await local_task
                raise RuntimeError("local server exited before startup")
            await asyncio.sleep(0.01)
        identity = app.state.identity
        rpc = uvicorn.Server(
            uvicorn.Config(
                Partition(create_peer_rpc_app(app)),
                host="127.0.0.1",
                port=int(port),
                ssl_certfile=str(identity.cert_path),
                ssl_keyfile=str(identity.key_path),
                log_level="warning",
            )
        )
        rpc_task = asyncio.create_task(rpc.serve())
        while not rpc.started:
            if rpc_task.done():
                await rpc_task
                raise RuntimeError("RPC server exited before startup")
            await asyncio.sleep(0.01)
        local_port = local.servers[0].sockets[0].getsockname()[1]
        print(json.dumps({"url": f"http://127.0.0.1:{local_port}"}), flush=True)
        await local_task
    finally:
        if rpc_task is not None:
            rpc.should_exit = True
            await rpc_task


if __name__ == "__main__":
    if sys.argv[1] == "backend":
        asyncio.run(serve_backend(sys.argv[2]))
    elif sys.argv[1] == "peer":
        asyncio.run(serve_peer(*sys.argv[2:5], json.loads(sys.argv[5])))
    else:
        raise ValueError(f"unknown role: {sys.argv[1]}")
