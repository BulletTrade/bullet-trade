from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from threading import Event, Thread
import time

import pytest

from bullet_trade.integrations.ths.actor_service import ActorService, CollectedSnapshot
from bullet_trade.integrations.ths.request_store import RequestStore
from bullet_trade.integrations.ths.snapshot_store import SnapshotStore


def test_query_failure_preserves_data_and_marks_error(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    snapshots.publish_success("paper", "positions", [{"quantity": 100}],
                              datetime.now(timezone.utc), complete=True)
    def failed(*args):
        raise TimeoutError("captcha")
    actor = ActorService(RequestStore(tmp_path / "requests.db"), snapshots,
                         SimpleNamespace(query=failed), account="paper",
                         lock_path=tmp_path / "actor.lock", kinds=("positions",))
    import pytest
    with pytest.raises(TimeoutError):
        actor.step()
    result = snapshots.read("paper", "positions")
    assert result["data"] == [{"quantity": 100}]
    assert result["last_error"] == "TimeoutError"


def test_health_tracks_each_current_query_error_and_keeps_historical_error(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    requests = RequestStore(tmp_path / "requests.db")
    failing = {"positions": True}

    def query(kind, should_yield):
        if failing.get(kind):
            raise TimeoutError("temporary authentication prompt")
        return CollectedSnapshot("paper", kind, [], datetime.now(timezone.utc), True)

    actor = ActorService(requests, snapshots, SimpleNamespace(query=query, ready=True),
                         account="paper", lock_path=tmp_path / "actor.lock",
                         kinds=("orders", "positions"), write_enabled=True)
    with pytest.raises(TimeoutError):
        actor._refresh("positions")(None)
    actor._last_error = "TimeoutError"
    actor._last_error_at = datetime.now(timezone.utc).isoformat()
    actor._refresh("orders")(None)
    actor._publish_health()
    state = snapshots.read("paper", "actor_health")["data"]
    assert state["required_snapshot_kinds"] == ["orders", "positions"]
    assert state["current_query_errors"] == {"positions": "TimeoutError"}
    assert state["last_error"] == "TimeoutError"
    assert state["queries_ready"] is False and state["trading_ready"] is False

    failing["positions"] = False
    actor._refresh("positions")(None)
    actor._publish_health()
    state = snapshots.read("paper", "actor_health")["data"]
    assert state["current_query_errors"] == {}
    assert state["last_error"] == "TimeoutError"  # Historical audit survives recovery.
    assert state["queries_ready"] is True and state["trading_ready"] is True

    request = requests.enqueue("paper", "2026-10-04", "synthetic:one", "limit_buy",
                               {"security": "600000.XSHG", "quantity": 100,
                                "price": "10.00"}, time.time() + 60,
                               origin={"virtual_account_id": "synthetic"})
    requests.mark_preparing(request.request_id)
    actor._publish_health()
    state = snapshots.read("paper", "actor_health")["data"]
    assert state["queries_ready"] is True
    assert state["writes_stopped"] is True and state["trading_ready"] is False


def test_actor_health_default_age_recovers_without_gui_call(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    snapshots.publish_success("paper", "positions", [],
                              datetime.now(timezone.utc) - timedelta(seconds=121),
                              complete=True)
    driver = SimpleNamespace(ready=True, query=lambda *_: (_ for _ in ()).throw(
        AssertionError("heartbeat must not query GUI")))
    actor = ActorService(RequestStore(tmp_path / "requests.db"), snapshots, driver,
                         account="paper", lock_path=tmp_path / "actor.lock",
                         kinds=("positions",), write_enabled=True)
    assert actor.snapshot_max_age_seconds == 120
    actor._publish_health()
    state = snapshots.read("paper", "actor_health")["data"]
    assert state["current_query_errors"] == {"positions": "stale"}
    assert state["trading_ready"] is False

    snapshots.publish_success("paper", "positions", [], datetime.now(timezone.utc),
                              complete=True)
    actor._publish_health()
    state = snapshots.read("paper", "actor_health")["data"]
    assert state["current_query_errors"] == {}
    assert state["trading_ready"] is True


def test_query_rejects_wrong_account(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    driver = SimpleNamespace(query=lambda *args: CollectedSnapshot(
        "wrong", "positions", [], datetime.now(timezone.utc), True))
    actor = ActorService(RequestStore(tmp_path / "requests.db"), snapshots, driver,
                         account="paper", lock_path=tmp_path / "actor.lock", kinds=("positions",))
    import pytest
    with pytest.raises(ValueError):
        actor.step()
    result = snapshots.read("paper", "positions")
    assert result["data"] is None and result["complete"] is False


def test_snapshot_read_does_not_wait_for_gui_query(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    snapshots.publish_success("paper", "positions", [{"quantity": 100}],
                              datetime.now(timezone.utc), complete=True)
    entered = Event()
    release = Event()
    read_finished = Event()
    errors = []

    def query(*args):
        entered.set()
        assert release.wait(5)
        return CollectedSnapshot("paper", "positions", [{"quantity": 200}],
                                 datetime.now(timezone.utc), True)

    actor = ActorService(RequestStore(tmp_path / "requests.db"), snapshots,
                         SimpleNamespace(query=query), account="paper",
                         lock_path=tmp_path / "actor.lock", kinds=("positions",))
    worker = Thread(target=actor.step)
    worker.start()
    try:
        assert entered.wait(5)

        def read():
            try:
                assert snapshots.read("paper", "positions")["data"] == [{"quantity": 100}]
            except Exception as exc:
                errors.append(exc)
            finally:
                read_finished.set()

        reader = Thread(target=read)
        reader.start()
        assert read_finished.wait(2), "snapshot read blocked behind GUI query"
        reader.join(timeout=2)
        assert not errors
    finally:
        release.set()
        worker.join(timeout=5)
    assert not worker.is_alive()
    assert snapshots.read("paper", "positions")["data"] == [{"quantity": 200}]


def test_default_read_only_actor_never_claims_queued_write(tmp_path):
    requests = RequestStore(tmp_path / "requests.db")
    queued = requests.enqueue(
        "paper", "2026-09-30", "virtual-a:one", "limit_buy",
        {"security": "600000.XSHG", "quantity": 100, "price": "10.00"},
        time.time() + 60, origin={"virtual_account_id": "virtual-a"},
    )
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    queries = []

    def query(kind, should_yield):
        queries.append(kind)
        return CollectedSnapshot("paper", kind, [], datetime.now(timezone.utc), True)

    driver = SimpleNamespace(query=query, preflight=lambda *_: (_ for _ in ()).throw(
        AssertionError("read-only actor reached write driver")))
    actor = ActorService(requests, snapshots, driver, account="paper",
                         lock_path=tmp_path / "actor.lock", kinds=("orders",))
    actor.step()
    assert queries == ["orders"]
    assert requests.get(queued.request_id).state == "queued"
    assert snapshots.read("paper", "orders")["data"] == []
