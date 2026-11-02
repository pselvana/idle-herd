"""Fabricated Ollama backend for the opt-in Compose tests only."""

import asyncio
import json
import os

from fastapi import FastAPI
from fastapi.responses import StreamingResponse


def make_app():
    app = FastAPI()
    model = os.environ["TEST_MODEL"]
    state = {"active": 0, "started": 0, "blocked": False}
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

    return app
