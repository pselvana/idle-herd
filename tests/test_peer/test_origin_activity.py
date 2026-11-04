"""Outgoing activity is an observation, independent of worker lease authority."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from fleet_manager.peer.identity import IdentityError
from fleet_manager.peer.network import OriginDispatch


class HeldStream(httpx.AsyncByteStream):
    def __init__(self, *, fail=False):
        self.release = asyncio.Event()
        self.fail = fail

    async def __aiter__(self):
        yield b"first chunk"
        await self.release.wait()
        if self.fail:
            raise httpx.ReadError("incomplete response")
        yield b"last chunk"


def origin(client):
    return SimpleNamespace(
        state=SimpleNamespace(
            outbound_running=0,
            peer_client=client,
            identity=SimpleNamespace(
                sign_request=Mock(return_value={"x-idle-nonce": "nonce"}),
                verify_stream_response=Mock(),
            ),
            ledger=SimpleNamespace(require_dispatch=AsyncMock()),
            trace_store=SimpleNamespace(record_trace=AsyncMock()),
        )
    )


async def execute(app, send):
    await OriginDispatch(app, app)._execute(
        SimpleNamespace(endpoint="https://remote.test", worker_id="remote"),
        SimpleNamespace(token="private-token"),
        {"path": "/api/generate"},
        {"model": "tiny", "prompt": "private-prompt"},
        AsyncMock(),
        send,
        "request",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("finish", ["complete", "cancel", "stream_error", "send_error"])
async def test_outgoing_observation_clears_on_every_exit(finish):
    stream = HeldStream(fail=finish == "stream_error")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))
    ) as client:
        app = origin(client)
        began = asyncio.Event()

        async def send(message):
            assert app.state.outbound_running == 1
            began.set()
            if finish == "send_error":
                raise RuntimeError("client disconnected")

        task = asyncio.create_task(execute(app, send))
        try:
            await asyncio.wait_for(began.wait(), 1)
            if finish != "send_error":
                assert app.state.outbound_running == 1
            if finish == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                stream.release.set()
                if finish in {"stream_error", "send_error"}:
                    with pytest.raises(RuntimeError):
                        await task
                else:
                    await task
            assert app.state.outbound_running == 0
            app.state.trace_store.record_trace.assert_awaited_once()
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_concurrent_outgoing_observations_finish_independently():
    streams = [HeldStream(), HeldStream()]
    responses = iter(streams)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=next(responses)))
    ) as client:
        app = origin(client)
        both_started = asyncio.Event()

        async def send(message):
            if app.state.outbound_running == 2:
                both_started.set()

        tasks = [asyncio.create_task(execute(app, send)) for _ in streams]
        try:
            await asyncio.wait_for(both_started.wait(), 1)
            assert app.state.outbound_running == 2
            streams[0].release.set()
            await tasks[0]
            assert app.state.outbound_running == 1
            streams[1].release.set()
            await tasks[1]
            assert app.state.outbound_running == 0
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["declined", "unauthenticated", "transport"])
async def test_unconfirmed_or_declined_response_clears_outgoing_activity(failure):
    def respond(request):
        if failure == "transport":
            raise httpx.ConnectError("unreachable")
        # A stream rather than eager content keeps aiter_raw available.
        return httpx.Response(403 if failure == "declined" else 200, stream=httpx.ByteStream(b""))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        app = origin(client)
        if failure == "unauthenticated":
            app.state.identity.verify_stream_response.side_effect = IdentityError("invalid peer")

        async def send(message):
            assert app.state.outbound_running == 0

        if failure == "declined":
            await execute(app, send)
        else:
            with pytest.raises((IdentityError, httpx.ConnectError)):
                await execute(app, send)
        assert app.state.outbound_running == 0
        app.state.trace_store.record_trace.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_nonstreaming_execution_is_observed_while_waiting_for_headers(cancel):
    dispatched = asyncio.Event()
    complete = asyncio.Event()

    async def respond(request):
        dispatched.set()
        await complete.wait()
        return httpx.Response(200, stream=httpx.ByteStream(b"result"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        app = origin(client)
        task = asyncio.create_task(execute(app, AsyncMock()))
        try:
            await asyncio.wait_for(dispatched.wait(), 1)
            assert app.state.outbound_running == 1
            if cancel:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                complete.set()
                await task
            assert app.state.outbound_running == 0
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
