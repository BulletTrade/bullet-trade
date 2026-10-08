"""跨进程的 THS 查询快照；只有明确完整的采集才能替换旧数据。"""

from __future__ import annotations

import json
import math
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Union


def _timestamp(value: Optional[Union[str, datetime]]) -> str:
    if value is None:
        value = datetime.now(timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("timestamp must include timezone")
        return value.isoformat()
    if not isinstance(value, str) or not value:
        raise ValueError("timestamp must be a nonempty ISO string or aware datetime")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return value


class SnapshotStore:
    """一个文件对应一个持久存储；每次操作使用独立 SQLite 连接。"""

    def __init__(self, path: Union[str, Path], *, busy_timeout_ms: int = 250,
                 history_limit: int = 1000, history_days: int = 30) -> None:
        self.path = Path(path)
        if str(path) == ":memory:":
            raise ValueError("snapshot store requires a disk path")
        if not isinstance(busy_timeout_ms, int) or not 0 <= busy_timeout_ms <= 250:
            raise ValueError("busy_timeout_ms must be between 0 and 250")
        if not isinstance(history_limit, int) or not 1 <= history_limit <= 100000:
            raise ValueError("history_limit must be between 1 and 100000")
        if not isinstance(history_days, int) or not 1 <= history_days <= 3650:
            raise ValueError("history_days must be between 1 and 3650")
        self.busy_timeout_ms = busy_timeout_ms
        self.history_limit = history_limit
        self.history_days = history_days
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("BEGIN IMMEDIATE")
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2):
                raise ValueError("unsupported snapshot schema version")
            db.execute(
                """CREATE TABLE IF NOT EXISTS snapshots (
                    account_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 0,
                    collected_at TEXT,
                    complete INTEGER NOT NULL DEFAULT 0,
                    data_json TEXT,
                    metadata_json TEXT,
                    last_error TEXT,
                    last_error_at TEXT,
                    PRIMARY KEY (account_id, kind)
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS snapshot_history (
                    account_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    collected_at TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    PRIMARY KEY (account_id, kind, version)
                )"""
            )
            if version < 2:
                # Legacy v1 stores only the head. Preserve its last success once.
                db.execute(
                    """INSERT OR IGNORE INTO snapshot_history
                       (account_id, kind, version, collected_at, data_json,
                        metadata_json, published_at)
                       SELECT account_id, kind, version, collected_at, data_json,
                              COALESCE(metadata_json, '{}'), ? FROM snapshots
                       WHERE version > 0 AND complete = 1 AND data_json IS NOT NULL""",
                    (datetime.now(timezone.utc).isoformat(),),
                )
                db.execute("PRAGMA user_version=2")
            db.commit()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(str(self.path), timeout=self.busy_timeout_ms / 1000)
        db.execute("PRAGMA busy_timeout={}".format(self.busy_timeout_ms))
        return db

    @contextmanager
    def _connection(self):
        db = self._connect()
        try:
            yield db
        finally:
            db.close()

    @staticmethod
    def _key(account_id: str, kind: str) -> None:
        if not isinstance(account_id, str) or not account_id.strip():
            raise ValueError("account_id is required")
        if not isinstance(kind, str) or not kind.strip():
            raise ValueError("kind is required")

    def publish_success(
        self,
        account_id: str,
        kind: str,
        data: Any,
        collected_at: Union[str, datetime],
        *,
        complete: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """调用方须证明整页/全量采集完成；显式空列表可以是完整结果。"""
        self._key(account_id, kind)
        if complete is not True:
            raise ValueError("complete=True is required for replacement")
        if not isinstance(data, (dict, list)):
            raise ValueError("snapshot data must be a JSON object or array")
        timestamp = _timestamp(collected_at)
        payload = json.dumps(data, ensure_ascii=False, allow_nan=False)
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, dict) or any("verif" in str(key).lower() for key in metadata):
            raise ValueError("metadata cannot assert verification")
        metadata_payload = json.dumps(metadata, ensure_ascii=False, allow_nan=False)
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute(
                "SELECT collected_at FROM snapshots WHERE account_id=? AND kind=?",
                (account_id, kind),
            ).fetchone()
            if previous and previous[0] is not None:
                previous_at = datetime.fromisoformat(previous[0].replace("Z", "+00:00"))
                new_at = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                if new_at < previous_at:
                    raise ValueError("older snapshot cannot replace newer snapshot")
            db.execute(
                """INSERT INTO snapshots
                   (account_id, kind, version, collected_at, complete, data_json, metadata_json)
                   VALUES (?, ?, 1, ?, 1, ?, ?)
                   ON CONFLICT(account_id, kind) DO UPDATE SET
                       version=version+1, collected_at=excluded.collected_at,
                       complete=1, data_json=excluded.data_json,
                       metadata_json=excluded.metadata_json,
                       last_error=NULL, last_error_at=NULL""",
                (account_id, kind, timestamp, payload, metadata_payload),
            )
            row = db.execute(
                """SELECT version, collected_at, complete, data_json, metadata_json,
                          last_error, last_error_at FROM snapshots
                   WHERE account_id=? AND kind=?""", (account_id, kind)
            ).fetchone()
            version = row[0]
            db.execute(
                """INSERT INTO snapshot_history
                   (account_id, kind, version, collected_at, data_json,
                    metadata_json, published_at) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (account_id, kind, version, timestamp, payload, metadata_payload,
                 datetime.now(timezone.utc).isoformat()),
            )
            db.execute(
                """DELETE FROM snapshot_history WHERE account_id=? AND kind=?
                   AND version <= ?""",
                (account_id, kind, version - self.history_limit),
            )
            cutoff = (datetime.now(timezone.utc) - timedelta(days=self.history_days)).isoformat()
            db.execute(
                """DELETE FROM snapshot_history WHERE account_id=? AND kind=?
                   AND version < ? AND published_at < ?""",
                (account_id, kind, version, cutoff),
            )
            db.commit()
        return self._format_row(account_id, kind, row, None, None)

    def record_error(
        self,
        account_id: str,
        kind: str,
        error: str,
        *,
        at: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        """记录最新采集错误，不改变上次成功的版本和数据。"""
        self._key(account_id, kind)
        if not isinstance(error, str) or not error.strip():
            raise ValueError("nonempty error is required")
        timestamp = _timestamp(at)
        with self._connection() as db, db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """INSERT INTO snapshots (account_id, kind, last_error, last_error_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(account_id, kind) DO UPDATE SET
                       last_error=excluded.last_error,
                       last_error_at=excluded.last_error_at""",
                (account_id, kind, error, timestamp),
            )
        return self.read(account_id, kind)

    def read(
        self,
        account_id: str,
        kind: str,
        *,
        max_age_seconds: Optional[float] = None,
        now: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        """返回反序列化副本；没有成功采集时 data 始终为 None。"""
        self._key(account_id, kind)
        with self._connection() as db:
            row = db.execute(
                """SELECT version, collected_at, complete, data_json, metadata_json,
                          last_error, last_error_at FROM snapshots
                   WHERE account_id=? AND kind=?""",
                (account_id, kind),
            ).fetchone()
        return self._format_row(account_id, kind, row, max_age_seconds, now)

    @staticmethod
    def _format_row(account_id, kind, row, max_age_seconds, now):
        if row is None:
            row = (0, None, 0, None, None, None, None)
        version, collected_at, complete, payload, metadata_payload, error, error_at = row
        age_ms = None
        clock_skew = False
        if collected_at is not None:
            observed = datetime.fromisoformat(_timestamp(now))
            sampled = datetime.fromisoformat(collected_at.replace("Z", "+00:00"))
            age_ms = int((observed - sampled).total_seconds() * 1000)
            clock_skew = age_ms < 0
        if max_age_seconds is not None and (
            not math.isfinite(max_age_seconds) or max_age_seconds < 0
        ):
            raise ValueError("max_age_seconds must be finite and nonnegative")
        stale = bool(age_ms is None or clock_skew or error is not None or
                     (max_age_seconds is not None and age_ms > max_age_seconds * 1000))
        status = "unknown" if not complete else "error" if error is not None else "complete"
        return {
            "account_id": account_id,
            "kind": kind,
            "status": status,
            "version": version,
            "collected_at": collected_at,
            "complete": bool(complete),
            "last_error": error,
            "last_error_at": error_at,
            "data": json.loads(payload) if payload is not None else None,
            "metadata": json.loads(metadata_payload) if metadata_payload is not None else {},
            "age_ms": age_ms,
            "stale": stale,
            "clock_skew": clock_skew,
        }

    def history(self, account_id: str, kind: str, *, limit: int = 50,
                before_version: Optional[int] = None,
                since_version: Optional[int] = None) -> Dict[str, Any]:
        """成功快照按版本倒序分页；游标为排他的版本号。"""
        self._key(account_id, kind)
        if not isinstance(limit, int) or not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        for cursor in (before_version, since_version):
            if cursor is not None and (not isinstance(cursor, int) or cursor < 1):
                raise ValueError("version cursor must be positive")
        if before_version is not None and since_version is not None and before_version <= since_version:
            raise ValueError("before_version must exceed since_version")
        clauses = ["account_id=?", "kind=?"]
        args = [account_id, kind]
        if before_version is not None:
            clauses.append("version < ?")
            args.append(before_version)
        if since_version is not None:
            clauses.append("version > ?")
            args.append(since_version)
        args.append(limit + 1)
        with self._connection() as db:
            rows = db.execute(
                "SELECT version, collected_at, data_json, metadata_json FROM snapshot_history "
                "WHERE " + " AND ".join(clauses) + " ORDER BY version DESC LIMIT ?", args,
            ).fetchall()
        more = len(rows) > limit
        items = [{"account_id": account_id, "kind": kind, "version": version,
                  "collected_at": collected_at, "complete": True,
                  "data": json.loads(payload), "metadata": json.loads(metadata)}
                 for version, collected_at, payload, metadata in rows[:limit]]
        return {"account_id": account_id, "kind": kind, "items": items,
                "next_before_version": items[-1]["version"] if more else None}
