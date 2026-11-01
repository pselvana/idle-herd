import asyncio
import time
from types import SimpleNamespace

import pytest

from fleet_manager.peer.models import AcquireRequest, AdmissionError, Lease
from fleet_manager.peer.waiters import OriginWaiters


class Ledger:
    def __init__(self, path):
        self.settings = SimpleNamespace(data_dir=str(path), max_waiters=10)
        self.leases = {}
        self.busy = False

    async def acquire(self, spec):
        if self.busy:
            raise AdmissionError("busy", 429)
        if spec.request_id not in self.leases:
            self.leases[spec.request_id] = Lease(
                reservation_id=spec.request_id,
                token=spec.request_id,
                cluster_id="cluster",
                worker_id="worker",
                worker_incarnation="incarnation",
                slot_id=0,
                slot_generation=1,
                request_id=spec.request_id,
                model=spec.model,
                expires_at=time.time() + 30,
                ttl_ms=spec.ttl_ms,
                state="reserved",
                memory_gb=1,
            )
        return self.leases[spec.request_id]

    async def inspect(self, token):
        return self.leases[token]

    async def release(self, token):
        lease = self.leases[token].model_copy(update={"state": "released"})
        self.leases[token] = lease
        return lease


@pytest.mark.asyncio
async def test_replay_survives_origin_restart_and_conflicts(tmp_path):
    ledger = Ledger(tmp_path)
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    waiters = OriginWaiters(app)
    await waiters.open()
    spec = AcquireRequest(model="llama", request_id="one", idempotency_key="key", wait_ms=0)
    first = await waiters.acquire(spec)
    await waiters.close()
    waiters = OriginWaiters(app)
    await waiters.open()
    try:
        again = await waiters.acquire(spec.model_copy(update={"request_id": "two"}))
        assert first == again
        with pytest.raises(AdmissionError, match="idempotency_conflict"):
            await waiters.acquire(spec.model_copy(update={"model": "other"}))
        assert (
            await waiters.reservation(first.reservation_id, first.token, True)
        ).state == "released"
    finally:
        await waiters.close()


@pytest.mark.asyncio
async def test_waiter_retries_after_capacity_returns(tmp_path):
    ledger = Ledger(tmp_path)
    ledger.busy = True
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    waiters = OriginWaiters(app)
    await waiters.open()
    try:
        task = asyncio.create_task(
            waiters.acquire(AcquireRequest(model="llama", request_id="one", wait_ms=1500))
        )
        await asyncio.sleep(0.3)
        ledger.busy = False
        assert (await asyncio.wait_for(task, 1)).request_id == "one"
    finally:
        await waiters.close()


@pytest.mark.asyncio
async def test_uncertain_remote_attempt_stays_pinned_after_restart(tmp_path):
    import httpx

    from fleet_manager.peer.discovery import PeerDescriptor

    ledger = Ledger(tmp_path)
    ledger.busy = True
    peer = PeerDescriptor(
        cluster_id="cluster",
        worker_id="remote",
        incarnation="incarnation",
        endpoint="https://127.0.0.1:12345",
        models=[],
    )
    other = peer.model_copy(update={"worker_id": "other", "endpoint": "https://127.0.0.1:12346"})
    directory = SimpleNamespace(candidates=lambda model: [peer, other])
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=directory))
    waiters = OriginWaiters(app)
    await waiters.open()
    calls = []

    async def uncertain(target, spec):
        calls.append(target.worker_id)
        raise httpx.ConnectError("lost reply")

    waiters.dispatch._acquire = uncertain
    spec = AcquireRequest(model="llama", request_id="one", wait_ms=0)
    with pytest.raises(AdmissionError, match="no_reachable_worker"):
        await waiters.acquire(spec)
    await waiters.close()
    waiters = OriginWaiters(app)
    await waiters.open()
    waiters.dispatch._acquire = uncertain
    try:
        with pytest.raises(AdmissionError, match="no_reachable_worker"):
            await waiters.acquire(spec)
        assert calls == ["remote", "remote"]
    finally:
        await waiters.close()


@pytest.mark.asyncio
async def test_slow_remote_does_not_block_other_model_and_late_grant_is_released(tmp_path):
    from fleet_manager.peer.discovery import PeerDescriptor

    ledger = Ledger(tmp_path)
    original = ledger.acquire
    entered = asyncio.Event()
    finish = asyncio.Event()
    peer = PeerDescriptor(
        cluster_id="cluster",
        worker_id="remote",
        incarnation="incarnation",
        endpoint="https://127.0.0.1:12345",
        models=[],
    )

    async def local_acquire(spec):
        if spec.model.startswith("slow"):
            raise AdmissionError("busy", 429)
        return await original(spec)

    ledger.acquire = local_acquire
    app = SimpleNamespace(
        state=SimpleNamespace(
            ledger=ledger,
            directory=SimpleNamespace(candidates=lambda model: [peer]),
        )
    )
    waiters = OriginWaiters(app)
    await waiters.open()

    async def slow(target, spec):
        entered.set()
        await finish.wait()
        return (await original(spec)).model_copy(update={"worker_id": "remote"})

    released = []

    async def release(lease):
        released.append(lease)

    waiters.dispatch._acquire = slow
    waiters._release_lease = release
    try:
        task = asyncio.create_task(
            waiters.acquire(
                AcquireRequest(model="slow", request_id="slow", wait_ms=100),
            )
        )
        await asyncio.wait_for(entered.wait(), 1)
        fast = await asyncio.wait_for(
            waiters.acquire(
                AcquireRequest(model="fast", request_id="fast", wait_ms=1000),
            ),
            0.5,
        )
        assert fast.request_id == "fast"
        with pytest.raises(AdmissionError, match="capacity_wait_timeout"):
            await task
        finish.set()
        for _ in range(30):
            if released:
                break
            await asyncio.sleep(0.01)
        assert released[0].request_id == "slow"
    finally:
        finish.set()
        await waiters.close()


@pytest.mark.asyncio
async def test_queue_bound_and_capacity_diagnostics(tmp_path):
    ledger = Ledger(tmp_path)
    ledger.settings.max_waiters = 1
    ledger.busy = True
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    waiters = OriginWaiters(app)
    await waiters.open()
    try:
        task = asyncio.create_task(
            waiters.acquire(
                AcquireRequest(model="first", request_id="first", wait_ms=300),
            )
        )
        await asyncio.sleep(0.05)
        with pytest.raises(AdmissionError, match="too_many_waiters"):
            await waiters.acquire(AcquireRequest(model="second", request_id="second", wait_ms=0))
        with pytest.raises(AdmissionError, match="capacity_wait_timeout") as error:
            await task
        assert error.value.details == {"attempted": 1, "busy": 1, "ineligible": 0, "unreachable": 0}
    finally:
        await waiters.close()


@pytest.mark.asyncio
async def test_backend_unavailable_is_unreachable(tmp_path):
    ledger = Ledger(tmp_path)

    async def unavailable(spec):
        raise AdmissionError("backend_unavailable", 503)

    ledger.acquire = unavailable
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    waiters = OriginWaiters(app)
    await waiters.open()
    try:
        with pytest.raises(AdmissionError, match="no_reachable_worker") as error:
            await waiters.acquire(AcquireRequest(model="model", request_id="id", wait_ms=0))
        assert error.value.details["unreachable"] == 1
    finally:
        await waiters.close()


@pytest.mark.asyncio
async def test_remote_reservation_authenticates_errors_and_checks_binding(tmp_path):
    import httpx

    from fleet_manager.peer.discovery import PeerDescriptor
    from fleet_manager.peer.identity import IdentityError

    ledger = Ledger(tmp_path)
    lease = (await ledger.acquire(AcquireRequest(model="model", request_id="id"))).model_copy(
        update={"worker_id": "remote"}
    )
    peer = PeerDescriptor(
        cluster_id="cluster",
        worker_id="remote",
        incarnation="incarnation",
        endpoint="https://127.0.0.1:12345",
        models=[],
    )
    checked = []

    class Identity:
        def sign_request(self, method, path, body):
            return {"x-idle-nonce": "nonce"}

        def verify_response(self, nonce, status, body, headers, worker_id):
            checked.append(status)
            if status == 403:
                raise IdentityError("bad signature")

    status = 403

    def respond(request):
        return httpx.Response(
            status, json=lease.model_copy(update={"slot_generation": 2}).model_dump()
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    app = SimpleNamespace(
        state=SimpleNamespace(
            ledger=ledger, directory=None, identity=Identity(), peer_client=client
        )
    )
    waiters = OriginWaiters(app)
    try:
        with pytest.raises(IdentityError, match="bad signature"):
            await waiters._remote(peer, lease, False)
        assert checked == [403]
        status = 200
        with pytest.raises(IdentityError, match="binding mismatch"):
            await waiters._remote(peer, lease, False)
    finally:
        await client.aclose()


class SlotLedger(Ledger):
    def __init__(self, path):
        super().__init__(path)
        self.changed = asyncio.Event()
        self.owner = "held"
        self.attempted = []
        self.grants = []

    async def acquire(self, spec):
        self.attempted.append(spec.request_id)
        if self.owner is not None:
            raise AdmissionError("busy", 429)
        self.owner = spec.request_id
        self.grants.append(spec.request_id)
        return await super().acquire(spec)

    async def release(self, token):
        self.owner = None
        self.changed.set()
        return await super().release(token)


async def until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_older_compatible_waiters_win_before_new_arrivals(tmp_path):
    ledger = SlotLedger(tmp_path)
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    waiters = OriginWaiters(app)
    await waiters.open()
    tasks = []
    try:
        for name in ("first", "second"):
            tasks.append(
                asyncio.create_task(
                    waiters.acquire(AcquireRequest(model="model", request_id=name, wait_ms=2000))
                )
            )
            await until(lambda name=name: name in ledger.attempted)
        ledger.owner = None
        ledger.changed.set()
        first = await asyncio.wait_for(tasks[0], 1)
        assert first.request_id == "first"
        tasks.append(
            asyncio.create_task(
                waiters.acquire(AcquireRequest(model="model", request_id="new", wait_ms=2000))
            )
        )
        await until(lambda: "new" in ledger.attempted)
        await waiters.reservation(first.reservation_id, first.token, release=True)
        second = await asyncio.wait_for(tasks[1], 1)
        assert second.request_id == "second"
        await waiters.reservation(second.reservation_id, second.token, release=True)
        assert (await asyncio.wait_for(tasks[2], 1)).request_id == "new"
        assert ledger.grants == ["first", "second", "new"]
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await waiters.close()


@pytest.mark.asyncio
async def test_shared_key_one_cancellation_does_not_release_survivors_grant(tmp_path):
    ledger = Ledger(tmp_path)
    started = asyncio.Event()
    resume = asyncio.Event()
    original = ledger.acquire

    async def delayed(spec):
        started.set()
        await resume.wait()
        return await original(spec)

    ledger.acquire = delayed
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    waiters = OriginWaiters(app)
    await waiters.open()
    spec = AcquireRequest(model="model", request_id="id", idempotency_key="key", wait_ms=1000)
    first = asyncio.create_task(waiters.acquire(spec))
    second = asyncio.create_task(waiters.acquire(spec))
    try:
        await asyncio.wait_for(started.wait(), 1)
        await until(lambda: waiters._pending["key"].users == 2)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        resume.set()
        lease = await asyncio.wait_for(second, 1)
        assert (await ledger.inspect(lease.token)).state == "reserved"
        assert len(ledger.leases) == 1
    finally:
        resume.set()
        if not second.done():
            second.cancel()
        await asyncio.gather(first, second, return_exceptions=True)
        await waiters.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["ledger", "policy"])
async def test_changes_wake_waiters_before_timer_fallback(tmp_path, source):
    ledger = SlotLedger(tmp_path)

    class Policy:
        def __init__(self):
            self.revision = 0
            self.changed = asyncio.Event()
            self.listening = asyncio.Event()

        async def wait_for_change(self, revision, timeout):
            self.listening.set()
            await asyncio.wait_for(self.changed.wait(), timeout)
            return self.revision

    policy = Policy()
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None, idle_policy=policy))
    waiters = OriginWaiters(app)
    await waiters.open()
    task = asyncio.create_task(
        waiters.acquire(AcquireRequest(model="model", request_id="one", wait_ms=1500))
    )
    try:
        await until(lambda: "one" in ledger.attempted)
        await asyncio.wait_for(policy.listening.wait(), 1)
        ledger.owner = None
        if source == "ledger":
            ledger.changed.set()
        else:
            policy.revision += 1
            policy.changed.set()
        assert (await asyncio.wait_for(task, 0.2)).request_id == "one"
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await waiters.close()


@pytest.mark.asyncio
async def test_short_shared_deadline_does_not_end_longer_wait(tmp_path):
    ledger = SlotLedger(tmp_path)
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    waiters = OriginWaiters(app)
    await waiters.open()
    short = asyncio.create_task(
        waiters.acquire(
            AcquireRequest(model="model", request_id="id", idempotency_key="key", wait_ms=100)
        )
    )
    long = asyncio.create_task(
        waiters.acquire(
            AcquireRequest(model="model", request_id="id", idempotency_key="key", wait_ms=1500)
        )
    )
    try:
        with pytest.raises(AdmissionError, match="capacity_wait_timeout"):
            await short
        assert not long.done()
        ledger.owner = None
        ledger.changed.set()
        assert (await asyncio.wait_for(long, 1)).request_id == "id"
        assert ledger.grants == ["id"]
    finally:
        for task in (short, long):
            if not task.done():
                task.cancel()
        await asyncio.gather(short, long, return_exceptions=True)
        await waiters.close()


@pytest.mark.asyncio
async def test_nonblocking_shared_caller_does_not_end_blocking_follower(tmp_path):
    ledger = SlotLedger(tmp_path)
    entered = asyncio.Event()
    resume = asyncio.Event()
    original = ledger.acquire

    async def delayed(spec):
        entered.set()
        await resume.wait()
        return await original(spec)

    ledger.acquire = delayed
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    waiters = OriginWaiters(app)
    await waiters.open()
    immediate = asyncio.create_task(
        waiters.acquire(
            AcquireRequest(model="model", request_id="id", idempotency_key="key", wait_ms=0)
        )
    )
    follower = None
    try:
        await asyncio.wait_for(entered.wait(), 1)
        follower = asyncio.create_task(
            waiters.acquire(
                AcquireRequest(model="model", request_id="id", idempotency_key="key", wait_ms=1500)
            )
        )
        await until(lambda: waiters._pending["key"].users == 2)
        resume.set()
        with pytest.raises(AdmissionError, match="no_capacity"):
            await immediate
        assert not follower.done()
        ledger.owner = None
        ledger.changed.set()
        assert (await asyncio.wait_for(follower, 1)).request_id == "id"
        assert ledger.grants == ["id"]
    finally:
        resume.set()
        for task in (immediate, follower):
            if task and not task.done():
                task.cancel()
        await asyncio.gather(*(t for t in (immediate, follower) if t), return_exceptions=True)
        await waiters.close()


@pytest.mark.asyncio
async def test_deadline_includes_waiting_for_claim_lock(tmp_path):
    ledger = Ledger(tmp_path)
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    waiters = OriginWaiters(app)
    await waiters.open()
    await waiters._lock.acquire()
    try:
        with pytest.raises(AdmissionError, match="capacity_wait_timeout"):
            await asyncio.wait_for(
                waiters.acquire(AcquireRequest(model="model", request_id="id", wait_ms=50)),
                0.2,
            )
        assert not ledger.leases
    finally:
        waiters._lock.release()
        await waiters.close()


@pytest.mark.asyncio
async def test_expired_lease_after_claim_commit_is_not_returned(tmp_path):
    ledger = Ledger(tmp_path)
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    waiters = OriginWaiters(app)
    await waiters.open()
    save = waiters._save

    async def slow_save(key, target, lease=None):
        if lease is not None:
            lease.expires_at = time.time() - 1
        await save(key, target, lease)

    waiters._save = slow_save
    try:
        with pytest.raises(AdmissionError, match="reservation_expired") as error:
            await waiters.acquire(AcquireRequest(model="model", request_id="id", wait_ms=1000))
        assert error.value.status == 410
        assert ledger.leases["id"].state == "released"
    finally:
        await waiters.close()


@pytest.mark.asyncio
async def test_immediate_denial_can_be_retried_immediately(tmp_path):
    ledger = Ledger(tmp_path)
    ledger.busy = True
    app = SimpleNamespace(state=SimpleNamespace(ledger=ledger, directory=None))
    waiters = OriginWaiters(app)
    await waiters.open()
    try:
        spec = AcquireRequest(model="model", request_id="id", wait_ms=0)
        for _ in range(2):
            with pytest.raises(AdmissionError, match="no_capacity"):
                await waiters.acquire(spec)
    finally:
        await waiters.close()
