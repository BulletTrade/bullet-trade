from datetime import date
from dataclasses import replace
from contextlib import contextmanager
import multiprocessing
import threading
import time

import pytest

from bullet_trade.integrations.ths.request_store import RequestStore, UnresolvedSubmission
from bullet_trade.integrations.ths.runtime import (
    ActorBusy, BrokerAcceptance, BrokerRejection, DriverUnavailable,
    GateProof, GuiRuntime,
)


def enqueue(store, key="one", ttl=60):
    return store.enqueue("sim", date.today().isoformat(), key, "limit_buy",
                         {"security": "600000", "quantity": 100, "price": "10.00"},
                         time.time() + ttl,
                         origin={"virtual_account_id": "offline-v1"})


class FakeDriver:
    def __init__(self, store, outcome="accepted"):
        self.store = store
        self.outcome = outcome
        self.calls = []

    def proof(self, request, *, allowed=True):
        return GateProof(request.request_id, request.account, request.trade_day,
                         "offline-session", "fake-evidence", time.monotonic(), allowed, True, True,
                         True, True, True)

    def preflight(self, request):
        self.calls.append("preflight")
        return self.proof(request, allowed=self.outcome != "gate_fail")

    def prepare(self, request):
        self.calls.append("prepare")
        assert self.store.get(request.request_id).state == "preparing"

    def validate_readback(self, request):
        self.calls.append("readback")
        return self.proof(request)

    def submit(self, request):
        self.calls.append("submit")
        assert self.store.get(request.request_id).state == "submit_unknown"
        if self.outcome == "timeout":
            raise TimeoutError("API caller or GUI result timed out")
        if self.outcome == "rejected":
            return BrokerRejection("broker-explicit-reject")
        return BrokerAcceptance("000123", "broker-order:000123")


def test_submission_marker_precedes_driver_and_timeout_never_replays(tmp_path):
    store = RequestStore(tmp_path / "r.db")
    request = enqueue(store)
    driver = FakeDriver(store, "timeout")
    runtime = GuiRuntime(store, driver, tmp_path / "actor.lock")
    with pytest.raises(TimeoutError):
        runtime.run_once()
    assert driver.calls == ["preflight", "prepare", "readback", "submit"]
    assert RequestStore(tmp_path / "r.db").get(request.request_id).state == "submit_unknown"
    with pytest.raises(UnresolvedSubmission):
        GuiRuntime(RequestStore(tmp_path / "r.db"), driver, tmp_path / "actor.lock").run_once()
    assert driver.calls.count("submit") == 1


def test_preparing_crash_and_gate_rejection(tmp_path):
    store = RequestStore(tmp_path / "r.db")
    request = enqueue(store)
    store.mark_preparing(request.request_id)
    driver = FakeDriver(store)
    with pytest.raises(UnresolvedSubmission):
        GuiRuntime(store, driver, tmp_path / "actor.lock").run_once()
    assert driver.calls == []
    store.mark_local_aborted(request.request_id, "operator-reviewed-preflight-failure")
    another = enqueue(store, "two")
    driver.outcome = "gate_fail"
    assert GuiRuntime(store, driver, tmp_path / "actor.lock").run_once().state == "local_aborted"
    assert store.get(another.request_id).state == "local_aborted"
    assert driver.calls == ["preflight"]


def test_missing_driver_expiry_and_accepted_receipt(tmp_path):
    store = RequestStore(tmp_path / "r.db")
    runtime = GuiRuntime(store, None, tmp_path / "actor.lock")
    with pytest.raises(DriverUnavailable):
        runtime.run_once()
    old = enqueue(store, "old", ttl=-1)
    driver = FakeDriver(store)
    runtime = GuiRuntime(store, driver, tmp_path / "actor.lock")
    assert runtime.run_once() is None
    assert store.get(old.request_id).state == "expired"
    request = enqueue(store, "new")
    result = runtime.run_once()
    assert result.state == "accepted" and result.broker_contract_no == "000123"
    assert result.request_id == request.request_id


def _hold_lock(path, ready, release):
    from filelock import FileLock
    with FileLock(str(path)).acquire(timeout=1):
        ready.set()
        release.wait(5)


def test_second_process_cannot_acquire_actor(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    ready, release = ctx.Event(), ctx.Event()
    lock_path = tmp_path / "actor.lock"
    proc = ctx.Process(target=_hold_lock, args=(lock_path, ready, release))
    proc.start()
    try:
        assert ready.wait(5)
        runtime = GuiRuntime(RequestStore(tmp_path / "r.db"), FakeDriver(None), lock_path)
        with pytest.raises(ActorBusy):
            with runtime.actor():
                pass
    finally:
        release.set()
        proc.join(5)
        if proc.is_alive():
            proc.terminate()
            proc.join(5)
    assert proc.exitcode == 0


def test_expiry_during_prepare_rejects_before_marker(tmp_path):
    store = RequestStore(tmp_path / "r.db")
    request = enqueue(store, ttl=0.05)

    class SlowPrepare(FakeDriver):
        def prepare(self, item):
            super().prepare(item)
            time.sleep(0.08)

    driver = SlowPrepare(store)
    result = GuiRuntime(store, driver, tmp_path / "actor.lock").run_once()
    assert result.state == "local_aborted"
    assert result.evidence_ref == "local:expired_before_submit"
    assert "submit" not in driver.calls
    assert store.get(request.request_id).state == "local_aborted"


def test_expiry_after_marker_stays_unknown_without_submit(tmp_path, monkeypatch):
    store = RequestStore(tmp_path / "r.db")
    request = enqueue(store, ttl=0.1)
    original = store.mark_submit_unknown

    def delayed_mark(request_id):
        result = original(request_id)
        time.sleep(0.15)
        return result

    monkeypatch.setattr(store, "mark_submit_unknown", delayed_mark)
    driver = FakeDriver(store)
    result = GuiRuntime(store, driver, tmp_path / "actor.lock").run_once()
    assert result.state == "submit_unknown"
    assert "submit" not in driver.calls
    assert store.get(request.request_id).state == "submit_unknown"


def test_truthy_strings_are_not_gate_proofs(tmp_path):
    store = RequestStore(tmp_path / "r.db")
    enqueue(store)

    class UntruthfulDriver(FakeDriver):
        def preflight(self, request):
            self.calls.append("preflight")
            return replace(self.proof(request), capacity_ready="true")

    driver = UntruthfulDriver(store)
    result = GuiRuntime(store, driver, tmp_path / "actor.lock").run_once()
    assert result.state == "local_aborted"
    assert driver.calls == ["preflight"]


def test_explicit_broker_rejection_is_distinct_from_local_stop(tmp_path):
    store = RequestStore(tmp_path / "r.db")
    request = enqueue(store)
    driver = FakeDriver(store, "rejected")
    result = GuiRuntime(store, driver, tmp_path / "actor.lock").run_once()
    assert result.state == "rejected"
    assert result.evidence_ref == "broker-explicit-reject"
    assert driver.calls[-1] == "submit"
    assert store.get(request.request_id).state == "rejected"


def test_injected_client_lock_covers_actor_and_releases_after_driver_error(tmp_path):
    store = RequestStore(tmp_path / "r.db")
    enqueue(store)
    held = [False]
    events = []

    @contextmanager
    def shared_client_lock():
        assert not held[0]
        held[0] = True
        events.append("client_acquired")
        try:
            yield
        finally:
            held[0] = False
            events.append("client_released")

    class LockedDriver(FakeDriver):
        def preflight(self, request):
            assert held[0]
            return super().preflight(request)

        def submit(self, request):
            assert held[0]
            return super().submit(request)

    driver = LockedDriver(store, "timeout")
    runtime = GuiRuntime(store, driver, tmp_path / "actor.lock",
                         client_lock_factory=shared_client_lock)
    with pytest.raises(TimeoutError):
        runtime.run_once()
    assert events == ["client_acquired", "client_released"]
    assert not held[0]
    with runtime.actor():
        assert held[0]
    assert events[-2:] == ["client_acquired", "client_released"]


def test_unavailable_shared_client_lock_blocks_gui_before_file_lock(tmp_path):
    store = RequestStore(tmp_path / "r.db")
    request = enqueue(store)
    shared = threading.Lock()

    @contextmanager
    def shared_client_lock():
        if not shared.acquire(blocking=False):
            raise ActorBusy("shared client is busy")
        try:
            yield
        finally:
            shared.release()

    driver = FakeDriver(store)
    runtime = GuiRuntime(store, driver, tmp_path / "actor.lock",
                         client_lock_factory=shared_client_lock)
    shared.acquire()
    try:
        with pytest.raises(ActorBusy, match="shared client is busy"):
            runtime.run_once()
    finally:
        shared.release()
    assert driver.calls == []
    assert store.get(request.request_id).state == "queued"
    # A failed shared-lock acquisition did not leave the file lock held.
    with runtime.actor():
        pass


def test_file_lock_failure_releases_injected_client_lock(tmp_path):
    from filelock import FileLock

    store = RequestStore(tmp_path / "r.db")
    request = enqueue(store)
    driver = FakeDriver(store)
    held = [False]

    @contextmanager
    def shared_client_lock():
        held[0] = True
        try:
            yield
        finally:
            held[0] = False

    path = tmp_path / "actor.lock"
    runtime = GuiRuntime(store, driver, path,
                         client_lock_factory=shared_client_lock)
    with FileLock(str(path)).acquire(timeout=1):
        with pytest.raises(ActorBusy):
            runtime.run_once()
    assert not held[0]
    assert driver.calls == []
    assert store.get(request.request_id).state == "queued"


def test_readback_expiry_cleans_binding_under_lock_and_next_request_runs(tmp_path, monkeypatch):
    from bullet_trade.integrations.ths import runtime as runtime_module
    store = RequestStore(tmp_path / 'r.db')
    first = enqueue(store)
    original_now = time.time()
    now = [original_now]
    monkeypatch.setattr(runtime_module.time, 'time', lambda: now[0])

    class PreparedDriver(FakeDriver):
        bound = False
        expire_first = True
        def prepare(self, request):
            assert not self.bound
            super().prepare(request)
            self.bound = True
        def validate_readback(self, request):
            result = super().validate_readback(request)
            if self.expire_first:
                now[0] = request.expires_at + 1
                self.expire_first = False
            return result
        def abort_prepared(self, request):
            assert runtime.lock.is_locked
            self.calls.append('cleanup')
            self.bound = False

    driver = PreparedDriver(store)
    runtime = GuiRuntime(store, driver, tmp_path / 'actor.lock')
    assert runtime.run_once().state == 'local_aborted'
    assert not driver.bound and driver.calls[-1] == 'cleanup'
    assert store.get(first.request_id).state == 'local_aborted'
    second = enqueue(store, 'second')
    assert runtime.run_once().request_id == second.request_id
    assert store.get(second.request_id).state == 'accepted'


@pytest.mark.parametrize('after_marker', [False, True])
def test_cleanup_failure_keeps_unresolved_and_blocks_next_write(tmp_path, monkeypatch, after_marker):
    from bullet_trade.integrations.ths import runtime as runtime_module
    from bullet_trade.integrations.ths.request_store import RequestError
    store = RequestStore(tmp_path / 'r.db')
    request = enqueue(store)
    now = [time.time()]
    monkeypatch.setattr(runtime_module.time, 'time', lambda: now[0])
    original_mark = store.mark_submit_unknown
    def mark(request_id):
        result = original_mark(request_id)
        now[0] = request.expires_at + 1
        return result
    if after_marker:
        monkeypatch.setattr(store, 'mark_submit_unknown', mark)

    class CleanupFailure(FakeDriver):
        def validate_readback(self, item):
            result = super().validate_readback(item)
            if not after_marker:
                now[0] = request.expires_at + 1
            return result
        def abort_prepared(self, item):
            self.calls.append('cleanup')
            raise RuntimeError('private control details')
    driver = CleanupFailure(store)
    runtime = GuiRuntime(store, driver, tmp_path / 'actor.lock')
    with pytest.raises(RequestError, match='^driver preparation cleanup failed$'):
        runtime.run_once()
    assert store.get(request.request_id).state == ('submit_unknown' if after_marker else 'preparing')
    assert 'submit' not in driver.calls
    with pytest.raises(UnresolvedSubmission):
        runtime.run_once()
