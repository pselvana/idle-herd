"""Separate native processes for model-sharing integration, with fabricated Ollama."""

import asyncio
import json
import sys

import network_runner
from fastapi import Request
from fastapi.responses import JSONResponse

original_backend = network_runner.mock_backend


def backend(model):
    app = original_backend(model)
    app.router.routes = [
        route for route in app.router.routes if route.path not in {"/api/tags", "/api/embed"}
    ]
    state = {"models": {f"{model}:latest": {"name": f"{model}:latest", "size": 1000}}, "blobs": {}}

    @app.get("/api/tags")
    async def tags():
        return {"models": list(state["models"].values())}

    @app.post("/api/blobs/{digest}")
    async def blob(digest: str, request: Request):
        state["blobs"][digest] = await request.body()
        return {"status": "ok"}

    @app.post("/api/create")
    async def create(request: Request):
        spec = await request.json()
        digest = spec["files"]["model.gguf"]
        assert digest in state["blobs"]
        name = spec["model"] + ":latest"
        state["models"][name] = {
            "name": name,
            "size": len(state["blobs"][digest]),
            "digest": digest,
        }
        return {"status": "success"}

    @app.post("/api/show")
    async def show(request: Request):
        spec = await request.json()
        name = spec["model"] if ":" in spec["model"] else spec["model"] + ":latest"
        assert name in state["models"]
        return {"capabilities": ["embedding" if "-embedding:" in name else "completion"]}

    @app.post("/api/embed")
    async def embed(request: Request):
        spec = await request.json()
        name = spec["model"] if ":" in spec["model"] else spec["model"] + ":latest"
        assert name in state["models"] and "-embedding:" in name
        return {"model": spec["model"], "embeddings": [[0.1, 0.2]]}

    @app.post("/api/generate")
    async def generate(request: Request):
        spec = await request.json()
        name = spec["model"] if ":" in spec["model"] else spec["model"] + ":latest"
        assert name in state["models"]
        if "-embedding:" in name:
            return JSONResponse(
                status_code=400, content={"error": "model does not support generate"}
            )
        return {"model": spec["model"], "response": "", "done": True, "done_reason": "stop"}

    return app


async def main():
    if sys.argv[1] == "backend":
        network_runner.mock_backend = backend
        await network_runner.serve_backend(sys.argv[2])
        return

    # Slow the second chunk at the source so interruption is deterministic.
    import fleet_manager.peer.app as peer_app

    original = peer_app.create_peer_rpc_app

    def rpc(app):
        target = original(app)
        count = 0

        async def delayed(scope, receive, send):
            nonlocal count
            if scope["type"] == "http" and scope["path"] == "/peer/v1/models/chunk":
                count += 1
                if count >= 2:
                    await asyncio.sleep(1)
            await target(scope, receive, send)

        return delayed

    peer_app.create_peer_rpc_app = rpc
    await network_runner.serve_peer(*sys.argv[2:5], json.loads(sys.argv[5]))


if __name__ == "__main__":
    asyncio.run(main())
