"""Native subprocess fixture; only loopback sockets and fabricated model output."""

import asyncio
import json
import sys

import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from fleet_manager.peer.app import create_peer_app
from fleet_manager.peer.models import PeerSettings


def mock_backend():
    app = FastAPI()
    state = {"blocked": False, "active": 0, "peak": 0, "started": 0}
    finish = asyncio.Event()
    jobs = set()

    @app.get("/api/tags")
    async def tags():
        return {"models": [{"name": "tiny:latest", "size": 1000}]}

    @app.get("/api/ps")
    async def models():
        # Empty loaded models does NOT mean no queued/running execution.
        return {"models": []}

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

    @app.get("/state")
    async def get_state():
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

        # Fabricated inference is independent of the HTTP response's lifetime.
        # Killing ingress may close its socket while a real backend keeps computing.
        job = asyncio.create_task(execute())
        jobs.add(job)
        job.add_done_callback(jobs.discard)

        async def output():
            yield json.dumps({"message": {"content": "hello"}, "done": False}) + "\n"
            await asyncio.shield(job)
            yield (
                json.dumps(
                    {
                        "model": "tiny:latest",
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
        return {"model": "tiny:latest", "embeddings": [[1.0, 2.0]]}

    return app


async def main():
    if sys.argv[1] == "backend":
        app = mock_backend()
    else:
        app = create_peer_app(
            PeerSettings(
                data_dir=sys.argv[2],
                backend_url=sys.argv[3],
                request_memory_gb=0.000001,
                reserve_memory_gb=0,
            )
        )
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning"))
    serving = asyncio.create_task(server.serve())
    while not server.started:
        if serving.done():
            await serving
            raise RuntimeError("fixture server exited before startup")
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    print(json.dumps({"url": f"http://127.0.0.1:{port}"}), flush=True)
    await serving


if __name__ == "__main__":
    asyncio.run(main())
