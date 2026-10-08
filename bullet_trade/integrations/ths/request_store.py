"""Durable, fail-closed queue for one THS GUI actor.

Local request IDs are correlation IDs. Broker contract numbers are separate,
verbatim strings and never inferred from a GUI timeout or an empty result.
Idempotency keys are global within the parent account and trade day; callers
should prefix them with the virtual account identity to partition key space.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping, Optional


class RequestError(ValueError):
    pass


class RequestConflict(RequestError):
    pass


class StoreBusy(RuntimeError):
    pass


class UnresolvedSubmission(RequestConflict):
    pass


@dataclass(frozen=True)
class Request:
    request_id: str
    account: str
    trade_day: str
    idempotency_key: str
    kind: str
    params: dict
    origin: dict
    state: str
    expires_at: float
    broker_contract_no: Optional[str]
    evidence_ref: Optional[str]
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class RequestEvent:
    event_id: int
    request_id: str
    event_type: str
    state: str
    occurred_at: float
    broker_contract_no: Optional[str]
    evidence_ref: Optional[str]


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise RequestError("%s must be nonempty, unpadded text" % field)
    return value


def _validate(account: str, trade_day: str, key: str, kind: str,
              params: Mapping[str, object], expires_at: float,
              origin: Optional[Mapping[str, str]]) -> tuple[str, str]:
    _required_text(account, "account")
    _required_text(key, "idempotency_key")
    try:
        if date.fromisoformat(trade_day).isoformat() != trade_day:
            raise ValueError
    except (ValueError, TypeError) as exc:
        raise RequestError("trade_day must be YYYY-MM-DD") from exc
    if not isinstance(expires_at, (int, float)) or isinstance(expires_at, bool) or not 0 < expires_at < float("inf"):
        raise RequestError("expires_at must be a finite Unix timestamp")
    if not isinstance(params, Mapping):
        raise RequestError("params must be a mapping")
    params = dict(params)
    if kind in ("limit_buy", "limit_sell"):
        if set(params) != {"security", "quantity", "price"}:
            raise RequestError("limit request requires security, quantity, price")
        _required_text(params["security"], "security")
        if isinstance(params["quantity"], bool) or not isinstance(params["quantity"], int) or params["quantity"] <= 0:
            raise RequestError("quantity must be a positive integer")
        if not isinstance(params["price"], str):
            raise RequestError("price must be a decimal string")
        try:
            price = Decimal(params["price"])
        except InvalidOperation as exc:
            raise RequestError("invalid price") from exc
        if not price.is_finite() or price <= 0:
            raise RequestError("price must be positive and finite")
    elif kind == "cancel":
        if set(params) != {"broker_contract_no"}:
            raise RequestError("cancel requires original broker_contract_no only")
        _required_text(params["broker_contract_no"], "broker_contract_no")
    else:
        raise RequestError("unsupported request kind")
    if origin is None:
        origin = {}
    if not isinstance(origin, Mapping) or not set(origin) <= {
        "virtual_account_id", "subaccount_key", "strategy_id", "client_order_id"
    }:
        raise RequestError("unsupported origin fields")
    for field, value in origin.items():
        _required_text(value, field)
    if not (origin.get("virtual_account_id") or origin.get("subaccount_key")):
        raise RequestError("virtual_account_id or subaccount_key is required")
    return (json.dumps(params, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            json.dumps(dict(origin), ensure_ascii=False, sort_keys=True, separators=(",", ":")))


class RequestStore:
    """Each operation opens its own short-lived SQLite connection."""

    def __init__(self, db_path: str | Path, *, max_pending: int = 100,
                 busy_timeout_ms: int = 250):
        self.path = Path(db_path)
        if str(db_path) == ":memory:":
            raise RequestError("persistent database path required")
        if max_pending < 1 or busy_timeout_ms < 1:
            raise RequestError("positive capacity and busy timeout required")
        self.max_pending = max_pending
        self.busy_timeout_ms = busy_timeout_ms
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect(configure_wal=True) as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""CREATE TABLE IF NOT EXISTS ths_requests (
                request_id TEXT PRIMARY KEY, account TEXT NOT NULL,
                trade_day TEXT NOT NULL, idempotency_key TEXT NOT NULL,
                kind TEXT NOT NULL, params_json TEXT NOT NULL, origin_json TEXT NOT NULL,
                params_digest TEXT NOT NULL, state TEXT NOT NULL,
                expires_at REAL NOT NULL, broker_contract_no TEXT,
                evidence_ref TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL,
                UNIQUE(account, trade_day, idempotency_key))""")
            db.execute("CREATE INDEX IF NOT EXISTS ths_requests_queue ON ths_requests(state, created_at)")
            db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ths_order_contract_owner
                ON ths_requests(account, trade_day, broker_contract_no)
                WHERE kind IN ('limit_buy','limit_sell') AND broker_contract_no IS NOT NULL""")
            db.execute("""CREATE TABLE IF NOT EXISTS ths_request_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL REFERENCES ths_requests(request_id),
                event_type TEXT NOT NULL, state TEXT NOT NULL,
                occurred_at REAL NOT NULL, broker_contract_no TEXT, evidence_ref TEXT)""")
            db.execute("CREATE INDEX IF NOT EXISTS ths_request_events_page ON ths_request_events(request_id, event_id)")
            # An existing database has current state only. Record that fact once;
            # do not reconstruct transitions which were never persisted.
            db.execute("""INSERT INTO ths_request_events
                (request_id,event_type,state,occurred_at,broker_contract_no,evidence_ref)
                SELECT r.request_id,'migration_current_state',r.state,r.updated_at,
                       r.broker_contract_no,r.evidence_ref FROM ths_requests r
                WHERE NOT EXISTS (SELECT 1 FROM ths_request_events e
                                  WHERE e.request_id=r.request_id)""")

    @contextmanager
    def _connect(self, *, configure_wal: bool = False):
        db = None
        try:
            db = sqlite3.connect(str(self.path), timeout=self.busy_timeout_ms / 1000,
                                 isolation_level=None)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA busy_timeout=%d" % self.busy_timeout_ms)
            if configure_wal:
                mode = db.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                if str(mode).lower() != "wal":
                    raise RequestError("SQLite WAL required")
            db.execute("PRAGMA synchronous=FULL")
            if db.execute("PRAGMA synchronous").fetchone()[0] != 2:
                raise RequestError("SQLite FULL synchronous required")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise
        except sqlite3.OperationalError as exc:
            raise StoreBusy(str(exc)) from exc
        finally:
            if db is not None:
                db.close()

    @staticmethod
    def _row(row: sqlite3.Row | None) -> Optional[Request]:
        if row is None:
            return None
        return Request(row["request_id"], row["account"], row["trade_day"],
                       row["idempotency_key"], row["kind"], json.loads(row["params_json"]),
                       json.loads(row["origin_json"]),
                       row["state"], row["expires_at"], row["broker_contract_no"],
                       row["evidence_ref"], row["created_at"], row["updated_at"])

    def close(self) -> None:
        """Connections are per operation; retained for caller lifecycle symmetry."""

    def get(self, request_id: str) -> Optional[Request]:
        with self._connect() as db:
            return self._row(db.execute("SELECT * FROM ths_requests WHERE request_id=?", (request_id,)).fetchone())

    def events(self, request_id: str, after_id: int = 0, limit: int = 100) -> list[RequestEvent]:
        """Return this request's persisted events in increasing event_id order."""
        _required_text(request_id, "request_id")
        if isinstance(after_id, bool) or not isinstance(after_id, int) or after_id < 0:
            raise RequestError("after_id must be a nonnegative integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise RequestError("limit must be an integer from 1 to 1000")
        with self._connect() as db:
            rows = db.execute("""SELECT * FROM ths_request_events
                WHERE request_id=? AND event_id>? ORDER BY event_id LIMIT ?""",
                (request_id, after_id, limit)).fetchall()
            return [RequestEvent(row["event_id"], row["request_id"], row["event_type"],
                                 row["state"], row["occurred_at"], row["broker_contract_no"],
                                 row["evidence_ref"]) for row in rows]

    @staticmethod
    def _event(db: sqlite3.Connection, request_id: str, event_type: str,
               state: str, occurred_at: float,
               broker_contract_no: Optional[str] = None,
               evidence_ref: Optional[str] = None) -> None:
        db.execute("""INSERT INTO ths_request_events
            (request_id,event_type,state,occurred_at,broker_contract_no,evidence_ref)
            VALUES (?,?,?,?,?,?)""", (request_id, event_type, state, occurred_at,
                                      broker_contract_no, evidence_ref))

    @classmethod
    def _expire_queued(cls, db: sqlite3.Connection, now: float) -> None:
        rows = db.execute("""SELECT request_id FROM ths_requests
            WHERE state='queued' AND expires_at<=?""", (now,)).fetchall()
        for row in rows:
            request_id = row["request_id"]
            db.execute("""UPDATE ths_requests SET state='expired',updated_at=?
                WHERE request_id=?""", (now, request_id))
            cls._event(db, request_id, "expired", "expired", now)

    def get_by_key(self, account: str, trade_day: str, idempotency_key: str) -> Optional[Request]:
        with self._connect() as db:
            return self._row(db.execute("""SELECT * FROM ths_requests WHERE account=? AND trade_day=?
                                           AND idempotency_key=?""", (account, trade_day, idempotency_key)).fetchone())

    def get_by_key_any_day(self, account: str, idempotency_key: str) -> Optional[Request]:
        """Recover the original date after restart; ambiguous reuse never picks a day."""
        _required_text(account, "account")
        _required_text(idempotency_key, "idempotency_key")
        with self._connect() as db:
            rows = db.execute("""SELECT * FROM ths_requests WHERE account=?
                AND idempotency_key=? LIMIT 2""", (account, idempotency_key)).fetchall()
            if len(rows) > 1:
                raise RequestConflict("idempotency key exists on multiple trade days; original day required")
            return self._row(rows[0] if rows else None)

    def get_by_contract(self, account: str, trade_day: str,
                        broker_contract_no: str) -> Optional[Request]:
        """Return only the accepted originating order, never a cancel receipt."""
        _required_text(account, "account")
        _required_text(broker_contract_no, "broker_contract_no")
        try:
            if date.fromisoformat(trade_day).isoformat() != trade_day:
                raise ValueError
        except (TypeError, ValueError) as exc:
            raise RequestError("trade_day must be YYYY-MM-DD") from exc
        with self._connect() as db:
            return self._row(db.execute("""SELECT * FROM ths_requests WHERE account=?
                AND trade_day=? AND broker_contract_no=? AND state='accepted'
                AND kind IN ('limit_buy','limit_sell')""",
                (account, trade_day, broker_contract_no)).fetchone())

    @staticmethod
    def _unresolved(db: sqlite3.Connection) -> bool:
        return db.execute("SELECT 1 FROM ths_requests WHERE state IN ('preparing','submit_unknown') LIMIT 1").fetchone() is not None

    def has_unresolved(self) -> bool:
        with self._connect() as db:
            return self._unresolved(db)

    def enqueue(self, account: str, trade_day: str, idempotency_key: str,
                kind: str, params: Mapping[str, object], expires_at: float,
                *, origin: Optional[Mapping[str, str]] = None) -> Request:
        raw, origin_raw = _validate(account, trade_day, idempotency_key, kind, params, expires_at, origin)
        digest = hashlib.sha256((kind + "\0" + raw + "\0" + origin_raw).encode("utf-8")).hexdigest()
        try:
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("""SELECT * FROM ths_requests WHERE account=? AND trade_day=?
                                    AND idempotency_key=?""", (account, trade_day, idempotency_key)).fetchone()
                if row is not None:
                    if row["params_digest"] != digest or row["expires_at"] != expires_at:
                        raise RequestConflict("idempotency key reused with different request")
                    return self._row(row)
                if self._unresolved(db):
                    raise UnresolvedSubmission("unresolved request blocks writes")
                if kind == "cancel":
                    target = db.execute("""SELECT origin_json FROM ths_requests
                        WHERE account=? AND trade_day=? AND broker_contract_no=?
                        AND kind IN ('limit_buy','limit_sell') AND state='accepted'""",
                        (account, trade_day, params["broker_contract_no"])).fetchone()
                    if target is None:
                        raise RequestConflict("cancel target is not an owned accepted order")
                    owner = json.loads(target["origin_json"])
                    caller = json.loads(origin_raw)
                    if (owner.get("virtual_account_id") != caller.get("virtual_account_id")
                            or owner.get("subaccount_key") != caller.get("subaccount_key")):
                        raise RequestConflict("cancel target belongs to another virtual account")
                now = time.time()
                # An idle actor may not have called next_queued() since these
                # requests expired. Reclaim capacity in this same transaction.
                self._expire_queued(db, now)
                count = db.execute("SELECT COUNT(*) FROM ths_requests WHERE state='queued'").fetchone()[0]
                state = "expired" if expires_at <= now else "queued"
                if state == "queued" and count >= self.max_pending:
                    raise StoreBusy("request queue full")
                request_id = str(uuid.uuid4())
                db.execute("""INSERT INTO ths_requests VALUES
                    (?,?,?,?,?,?,?,?,?,?,NULL,NULL,?,?)""",
                    (request_id, account, trade_day, idempotency_key, kind, raw, origin_raw, digest,
                     state, expires_at, now, now))
                self._event(db, request_id, state, state, now)
                return self._row(db.execute("SELECT * FROM ths_requests WHERE request_id=?", (request_id,)).fetchone())
        except sqlite3.OperationalError as exc:
            raise StoreBusy(str(exc)) from exc
        except sqlite3.IntegrityError as exc:
            raise RequestConflict("broker contract is already owned by another request") from exc

    def next_queued(self, now: Optional[float] = None) -> Optional[Request]:
        """Expire stale queued work atomically. This never claims a request."""
        now = time.time() if now is None else now
        try:
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                self._expire_queued(db, now)
                return self._row(db.execute("""SELECT * FROM ths_requests WHERE state='queued'
                    ORDER BY created_at, rowid LIMIT 1""").fetchone())
        except sqlite3.OperationalError as exc:
            raise StoreBusy(str(exc)) from exc

    def _transition(self, request_id: str, expected: str, state: str,
                    *, broker_contract_no: Optional[str] = None,
                    evidence_ref: Optional[str] = None) -> Request:
        try:
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("SELECT * FROM ths_requests WHERE request_id=?", (request_id,)).fetchone()
                if row is None or row["state"] != expected:
                    raise RequestConflict("request state changed or missing")
                if state == "preparing":
                    if self._unresolved(db):
                        raise UnresolvedSubmission("another request is unresolved")
                    if row["expires_at"] <= time.time():
                        now = time.time()
                        db.execute("UPDATE ths_requests SET state='expired',updated_at=? WHERE request_id=?", (now, request_id))
                        self._event(db, request_id, "expired", "expired", now)
                        return self._row(db.execute("SELECT * FROM ths_requests WHERE request_id=?", (request_id,)).fetchone())
                now = time.time()
                db.execute("""UPDATE ths_requests SET state=?,broker_contract_no=?,
                    evidence_ref=?,updated_at=? WHERE request_id=?""",
                    (state, broker_contract_no, evidence_ref, now, request_id))
                self._event(db, request_id, state, state, now, broker_contract_no, evidence_ref)
                return self._row(db.execute("SELECT * FROM ths_requests WHERE request_id=?", (request_id,)).fetchone())
        except sqlite3.OperationalError as exc:
            raise StoreBusy(str(exc)) from exc
        except sqlite3.IntegrityError as exc:
            raise RequestConflict("broker contract is already owned by another request") from exc

    def mark_preparing(self, request_id: str) -> Request:
        return self._transition(request_id, "queued", "preparing")

    def mark_submit_unknown(self, request_id: str) -> Request:
        return self._transition(request_id, "preparing", "submit_unknown")

    def mark_local_aborted(self, request_id: str, evidence_ref: str) -> Request:
        """Record a proven stop before any possibly submitting GUI action."""
        _required_text(evidence_ref, "evidence_ref")
        return self._transition(request_id, "preparing", "local_aborted",
                                evidence_ref=evidence_ref)

    def mark_cancel_not_submitted(self, request_id: str, evidence_ref: str) -> Request:
        """Operator recovery only: a reviewed cancel failed before its first click.

        This does not broaden the runtime's preparing-only local abort. The
        recovery command must validate the bound source and exception evidence
        and persist its audit before invoking this narrow CAS transition.
        """
        _required_text(request_id, "request_id")
        _required_text(evidence_ref, "evidence_ref")
        try:
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                now = time.time()
                changed = db.execute("""UPDATE ths_requests
                    SET state='local_aborted',broker_contract_no=NULL,
                        evidence_ref=?,updated_at=?
                    WHERE request_id=? AND kind='cancel' AND state='submit_unknown'""",
                    (evidence_ref, now, request_id))
                if changed.rowcount != 1:
                    raise RequestConflict("only an unknown cancel can be reviewed as not submitted")
                self._event(db, request_id, "operator_reviewed_not_submitted",
                            "local_aborted", now, evidence_ref=evidence_ref)
                return self._row(db.execute(
                    "SELECT * FROM ths_requests WHERE request_id=?", (request_id,)).fetchone())
        except sqlite3.OperationalError as exc:
            raise StoreBusy(str(exc)) from exc

    def mark_accepted(self, request_id: str, broker_contract_no: str,
                      evidence_ref: str) -> Request:
        _required_text(broker_contract_no, "broker_contract_no")
        _required_text(evidence_ref, "evidence_ref")
        current = self.get(request_id)
        if current is not None and current.kind == "cancel" and broker_contract_no != current.params["broker_contract_no"]:
            raise RequestConflict("cancel acceptance must reference original target contract")
        # For cancel, accepted means the cancel request was received, not that
        # the original order is fully cancelled. Final state requires queries.
        return self._transition(request_id, "submit_unknown", "accepted",
                                broker_contract_no=broker_contract_no,
                                evidence_ref=evidence_ref)

    def mark_rejected(self, request_id: str, evidence_ref: str) -> Request:
        """Record an explicit broker rejection after the submit boundary."""
        _required_text(evidence_ref, "evidence_ref")
        return self._transition(request_id, "submit_unknown", "rejected",
                                evidence_ref=evidence_ref)
