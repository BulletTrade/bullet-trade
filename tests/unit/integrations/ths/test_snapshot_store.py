from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from multiprocessing import get_context
import sqlite3
import time

import pytest

from bullet_trade.integrations.ths.snapshot_store import SnapshotStore


NOW = datetime(2026, 9, 29, 9, 30, tzinfo=timezone.utc)


def _process_writer(path):
    store = SnapshotStore(path)
    for seq in range(20):
        store.publish_success("A", "trades", [{"seq": seq}], NOW, complete=True)


def test_unknown_failure_and_atomic_replacement(tmp_path):
    store = SnapshotStore(tmp_path / "snapshots.sqlite")
    unknown = store.read("A", "positions")
    assert unknown["status"] == "unknown" and unknown["data"] is None
    assert unknown["version"] == 0

    first = store.publish_success("A", "positions", [{"qty": 4}], NOW, complete=True,
                                  metadata={"trading_day": "20260929", "session_id": "S1"})
    assert first["version"] == 1 and first["status"] == "complete"
    with pytest.raises(ValueError):
        store.publish_success("A", "positions", [], NOW, complete=False)
    with pytest.raises(ValueError):
        store.publish_success("A", "positions", None, NOW, complete=True)
    assert store.record_error("A", "positions", "copy failed", at=NOW)["status"] == "error"
    old = SnapshotStore(tmp_path / "snapshots.sqlite").read("A", "positions")
    assert old["version"] == 1 and old["data"] == [{"qty": 4}]
    assert old["last_error"] == "copy failed" and old["complete"] is True
    assert old["stale"] is True
    assert store.read("B", "positions")["status"] == "unknown"
    assert store.read("A", "orders")["status"] == "unknown"

    second = store.publish_success("A", "positions", [], NOW, complete=True)
    assert second["status"] == "complete" and second["data"] == []
    assert second["version"] == 2 and second["last_error"] is None


def test_age_and_immutable_return_copy(tmp_path):
    store = SnapshotStore(tmp_path / "snapshots.sqlite")
    store.publish_success("A", "account", {"nested": [1]}, NOW, complete=True)
    result = store.read("A", "account", max_age_seconds=20,
                        now=datetime(2026, 9, 29, 9, 30, 21, tzinfo=timezone.utc))
    assert result["age_ms"] == 21000 and result["stale"] is True
    result["data"]["nested"].append(2)
    assert store.read("A", "account")["data"] == {"nested": [1]}
    with pytest.raises(ValueError):
        store.publish_success("A", "account", {}, NOW, complete=True,
                              metadata={"verified": True})
    future = store.read("A", "account", max_age_seconds=20,
                        now=datetime(2026, 9, 29, 9, 29, tzinfo=timezone.utc))
    assert future["clock_skew"] is True and future["stale"] is True
    assert store.read("B", "account")["stale"] is True
    with pytest.raises(ValueError):
        store.read("A", "account", max_age_seconds=float("nan"))
    with pytest.raises(ValueError):
        store.publish_success("A", "account", {},
                              datetime(2026, 9, 29, 9, 29, tzinfo=timezone.utc),
                              complete=True)
    assert store.read("A", "account")["version"] == 1


def test_busy_timeout_is_bounded_and_connections_close(tmp_path):
    store = SnapshotStore(tmp_path / "snapshots.sqlite", busy_timeout_ms=100)
    holder = sqlite3.connect(str(store.path))
    holder.execute("BEGIN IMMEDIATE")
    started = time.monotonic()
    try:
        with pytest.raises(sqlite3.OperationalError):
            store.publish_success("A", "account", {}, NOW, complete=True)
    finally:
        holder.rollback()
        holder.close()
    assert time.monotonic() - started < 0.5
    assert store.read("A", "account")["status"] == "unknown"


def test_concurrent_cross_connection_reads_and_writes(tmp_path):
    path = tmp_path / "snapshots.sqlite"
    SnapshotStore(path)

    def writer(index):
        SnapshotStore(path).publish_success("A", "orders", [{"seq": index}], NOW,
                                            complete=True)

    def reader(_):
        row = SnapshotStore(path).read("A", "orders")
        assert row["version"] == 0 or row["data"][0]["seq"] >= 0

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: writer(i) if i % 2 else reader(i), range(80)))
    assert SnapshotStore(path).read("A", "orders")["version"] == 40


def test_cross_process_writer_and_reader(tmp_path):
    path = tmp_path / "snapshots.sqlite"
    SnapshotStore(path)
    process = get_context("spawn").Process(target=_process_writer, args=(path,))
    process.start()
    while process.is_alive():
        row = SnapshotStore(path).read("A", "trades")
        assert row["version"] == 0 or len(row["data"]) == 1
    process.join(timeout=5)
    assert process.exitcode == 0
    assert SnapshotStore(path).read("A", "trades")["version"] == 20


def test_history_pagination_retention_and_errors(tmp_path):
    store = SnapshotStore(tmp_path / "snapshots.sqlite", history_limit=3)
    for seq in range(5):
        store.publish_success("A", "orders", [{"seq": seq}], NOW, complete=True,
                              metadata={"page": seq})
    store.record_error("A", "orders", "copy failed", at=NOW)
    first = store.history("A", "orders", limit=2)
    assert [item["version"] for item in first["items"]] == [5, 4]
    assert first["items"][0]["data"] == [{"seq": 4}]
    assert first["items"][0]["metadata"] == {"page": 4}
    second = store.history("A", "orders", limit=2,
                           before_version=first["next_before_version"])
    assert [item["version"] for item in second["items"]] == [3]
    assert second["next_before_version"] is None
    assert store.history("A", "orders", since_version=4)["items"][0]["version"] == 5
    assert store.history("B", "orders")["items"] == []
    assert store.read("A", "orders")["last_error"] == "copy failed"
    with pytest.raises(ValueError):
        store.history("A", "orders", limit=201)
    with pytest.raises(ValueError):
        store.history("A", "orders", before_version=3, since_version=3)


def test_history_age_retention_preserves_latest_success(tmp_path):
    store = SnapshotStore(tmp_path / "snapshots.sqlite", history_days=1)
    store.publish_success("A", "account", {"cash": 1}, NOW, complete=True)
    with sqlite3.connect(store.path) as db:
        db.execute("UPDATE snapshot_history SET published_at=?",
                   ("2020-01-01T00:00:00+00:00",))
    store.publish_success("A", "account", {"cash": 2}, NOW, complete=True)
    assert [item["version"] for item in store.history("A", "account")["items"]] == [2]
    assert store.read("A", "account")["data"] == {"cash": 2}


def test_migrate_legacy_head_once_and_keep_failure_out_of_history(tmp_path):
    path = tmp_path / "legacy.sqlite"
    with sqlite3.connect(path) as db:
        db.execute("""CREATE TABLE snapshots (
            account_id TEXT NOT NULL, kind TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 0, collected_at TEXT,
            complete INTEGER NOT NULL DEFAULT 0, data_json TEXT,
            metadata_json TEXT, last_error TEXT, last_error_at TEXT,
            PRIMARY KEY (account_id, kind))""")
        db.execute("INSERT INTO snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   ("A", "positions", 7, NOW.isoformat(), 1, '[{"qty":4}]',
                    '{"source":"legacy"}', 'copy failed', NOW.isoformat()))
        db.execute("PRAGMA user_version=1")
    for _ in range(2):
        store = SnapshotStore(path)
        assert store.read("A", "positions")["version"] == 7
        rows = store.history("A", "positions")["items"]
        assert len(rows) == 1 and rows[0]["version"] == 7
        assert rows[0]["data"] == [{"qty": 4}]
    store.publish_success("A", "positions", [], NOW, complete=True)
    assert [item["version"] for item in store.history("A", "positions")["items"]] == [8, 7]


def test_history_and_head_share_one_commit(tmp_path):
    path = tmp_path / "snapshots.sqlite"
    store = SnapshotStore(path)
    with sqlite3.connect(path) as observer:
        observer.execute("BEGIN")
        assert observer.execute("SELECT count(*) FROM snapshots").fetchone()[0] == 0
        store.publish_success("A", "account", {"cash": 1}, NOW, complete=True)
        # An existing read transaction sees neither part of the new commit.
        assert observer.execute("SELECT count(*) FROM snapshots").fetchone()[0] == 0
        assert observer.execute("SELECT count(*) FROM snapshot_history").fetchone()[0] == 0
        observer.rollback()
        assert observer.execute("SELECT version FROM snapshots").fetchone()[0] == 1
        assert observer.execute("SELECT version FROM snapshot_history").fetchone()[0] == 1
