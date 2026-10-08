"""Local snapshot/request API; handlers never import or call a GUI driver.

The request acknowledgement is not broker acceptance. Deploy behind a dedicated
authenticated tunnel if remote access is required; the listener is loopback-only.
"""
from __future__ import annotations

import hmac
import json
import math
import sqlite3
import threading
from dataclasses import asdict
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .snapshot_store import SnapshotStore
from .request_store import RequestConflict, RequestStore, StoreBusy, UnresolvedSubmission


KINDS = frozenset({"account", "positions", "orders", "trades", "cancelable"})


class ServiceApplication:
    def __init__(self, directory, *, account: str, allow_requests: bool = False,
                 max_age_seconds: float = 60):
        if not account or account != account.strip():
            raise ValueError("explicit account required")
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.account = account
        self.allow_requests = allow_requests
        if not math.isfinite(max_age_seconds) or max_age_seconds < 0:
            raise ValueError("finite nonnegative snapshot max age required")
        self.max_age_seconds = max_age_seconds
        # Bootstrap schema before starting handlers. Each handler opens its own
        # connection, so no sqlite connection crosses threads.
        self.snapshots = self.directory / "snapshots.sqlite3"
        self.requests = self.directory / "requests.sqlite3"
        self.snapshot_store = SnapshotStore(self.snapshots)
        self.request_store = RequestStore(self.requests)

    def dispatch(self, method: str, target: str, body=None):
        url = urlsplit(target)
        query = parse_qs(url.query, keep_blank_values=True)
        if query.get("account", [self.account]) != [self.account]:
            return 400, {"error": "account_mismatch"}
        if method == "GET" and url.path == "/health":
            unresolved = self.request_store.has_unresolved()
            actor = self.snapshot_store.read(self.account, "actor_health", max_age_seconds=5)
            state = actor["data"] if actor["status"] == "complete" and not actor["stale"] else None
            state = state if isinstance(state, dict) else {}
            actor_fresh = bool(actor["status"] == "complete" and not actor["stale"])
            required = state.get("required_snapshot_kinds")
            valid_required = (type(required) is list and bool(required)
                              and all(type(kind) is str and kind in KINDS for kind in required)
                              and len(set(required)) == len(required))
            snapshot_health = {}
            if valid_required:
                for kind in required:
                    snapshot = self.snapshot_store.read(
                        self.account, kind, max_age_seconds=self.max_age_seconds)
                    snapshot_health[kind] = {
                        "status": snapshot["status"], "stale": snapshot["stale"],
                        "last_error": snapshot["last_error"],
                    }
            snapshots_ready = bool(valid_required and all(
                item["status"] == "complete" and item["stale"] is False
                for item in snapshot_health.values()))
            ready = bool(self.allow_requests is True
                         and actor_fresh and state.get("driver_ready") is True
                         and state.get("writes_enabled") is True
                         and state.get("writes_stopped") is False
                         and state.get("queries_ready") is True
                         and snapshots_ready and not unresolved)
            return 200, {"api": "ready", "account": self.account,
                         "request_intake_enabled": self.allow_requests,
                         "unresolved_submission": unresolved,
                         "gui": "ready" if actor_fresh and state.get("driver_ready") is True
                                else "unverified",
                         "trading_ready": ready,
                         "required_snapshots_ready": snapshots_ready,
                         "snapshot_health": snapshot_health,
                         "actor_health": actor,
                         "actor_fresh": actor_fresh}
        if method == "GET" and url.path.startswith("/snapshots/"):
            suffix = url.path.removeprefix("/snapshots/")
            history = suffix.endswith("/history")
            kind = suffix.removesuffix("/history") if history else suffix
            if kind not in KINDS:
                return 404, {"error": "unknown_snapshot_kind"}
            if history:
                if set(query) - {"account", "limit", "before_version", "since_version"}:
                    return 400, {"error": "snapshot_history_query_invalid"}
                try:
                    params = {}
                    for name in ("limit", "before_version", "since_version"):
                        if name in query:
                            values = query[name]
                            if len(values) != 1 or not values[0].isascii() or not values[0].isdecimal():
                                raise ValueError
                            params[name] = int(values[0])
                    return 200, self.snapshot_store.history(self.account, kind, **params)
                except ValueError:
                    return 400, {"error": "snapshot_history_query_invalid"}
            snapshot = self.snapshot_store.read(
                self.account, kind, max_age_seconds=self.max_age_seconds)
            return 200, snapshot
        if method == "GET" and url.path == "/requests":
            # Receipt loss leaves the caller with its original key, not the
            # server-generated UUID. This lookup never enqueues or replays.
            if set(query) - {"account", "trade_day", "idempotency_key"}:
                return 400, {"error": "request_lookup_invalid"}
            day = query.get("trade_day", [])
            key = query.get("idempotency_key", [])
            if (len(day) > 1 or len(key) != 1 or not key[0]
                    or len(key[0]) > 256 or key[0] != key[0].strip()):
                return 400, {"error": "request_lookup_invalid"}
            if day:
                try:
                    if date.fromisoformat(day[0]).isoformat() != day[0]:
                        raise ValueError
                except ValueError:
                    return 400, {"error": "request_lookup_invalid"}
                request = self.request_store.get_by_key(self.account, day[0], key[0])
            else:
                request = self.request_store.get_by_key_any_day(self.account, key[0])
            if request is None:
                return 404, {"error": "request_not_found"}
            return 200, asdict(request)
        if method == "GET" and url.path.startswith("/requests/"):
            suffix = url.path.removeprefix("/requests/")
            events = suffix.endswith("/events")
            request_id = suffix.removesuffix("/events") if events else suffix
            request = self.request_store.get(request_id)
            if request is None or request.account != self.account:
                return 404, {"error": "request_not_found"}
            if events:
                if set(query) - {"account", "after_id", "limit"}:
                    return 400, {"error": "request_events_query_invalid"}
                try:
                    params = {}
                    for name in ("after_id", "limit"):
                        if name in query:
                            values = query[name]
                            if len(values) != 1 or not values[0].isascii() or not values[0].isdecimal():
                                raise ValueError
                            params[name] = int(values[0])
                    after_id = params.get("after_id", 0)
                    limit = params.get("limit", 100)
                    if not 0 <= after_id or not 1 <= limit <= 200:
                        raise ValueError
                except ValueError:
                    return 400, {"error": "request_events_query_invalid"}
                rows = self.request_store.events(request_id, after_id=after_id, limit=limit + 1)
                more = len(rows) > limit
                items = [asdict(event) for event in rows[:limit]]
                return 200, {"request_id": request_id, "events": items,
                             "next_after_id": items[-1]["event_id"] if more else None}
            return 200, asdict(request)
        if method == "POST" and url.path == "/requests":
            if not self.allow_requests:
                return 503, {"error": "write_transport_not_qualified"}
            if not isinstance(body, dict) or body.get("account") != self.account:
                return 400, {"error": "account_mismatch"}
            required = {"account", "trade_day", "idempotency_key", "kind", "params", "expires_at"}
            if not required <= set(body) or set(body) - required - {"origin"}:
                return 400, {"error": "request_schema_invalid"}
            request = self.request_store.enqueue(**body)
            return 202, {**asdict(request), "acknowledgement": "service_recorded_request"}
        return 404, {"error": "not_found"}


class LocalServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    request_queue_size = 32

    def __init__(self, address, application, token):
        if address[0] not in {"127.0.0.1", "localhost"}:
            raise ValueError("loopback_listener_required")
        if not isinstance(token, str) or len(token) < 24 or not token.isascii():
            raise ValueError("token must contain at least 24 ASCII characters")
        self.application = application
        self.token = token
        self._slots = threading.BoundedSemaphore(32)
        super().__init__(address, Handler)

    def process_request(self, request, client_address):
        if not self._slots.acquire(blocking=False):
            try:
                request.settimeout(0.1)
                request.sendall(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(2)

    def log_message(self, format, *args):
        # URL and body can contain account identifiers; leave audit to the store.
        pass

    def _handle(self):
        authorization = self.headers.get("Authorization", "")
        if not authorization.isascii() or not hmac.compare_digest(
                authorization, "Bearer " + self.server.token):
            return self._reply(401, {"error": "unauthorized"})
        try:
            body = None
            if self.command == "POST":
                if self.headers.get("Transfer-Encoding"):
                    return self._reply(400, {"error": "transfer_encoding_unsupported"})
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 16384:
                    return self._reply(413, {"error": "body_size_invalid"})
                body = json.loads(self.rfile.read(length))
            status, result = self.server.application.dispatch(self.command, self.path, body)
        except (sqlite3.OperationalError, StoreBusy):
            status, result = 503, {"error": "store_busy_or_unavailable"}
        except UnresolvedSubmission:
            status, result = 409, {"error": "unresolved_submission"}
        except RequestConflict:
            status, result = 409, {"error": "request_conflict"}
        except (ValueError, TypeError, KeyError):
            status, result = 400, {"error": "invalid_request"}
        except TimeoutError:
            status, result = 408, {"error": "request_read_timeout"}
        except Exception as exc:
            # Do not leak paths, SQL, params, or credentials through error text.
            status, result = 503, {"error": type(exc).__name__}
        self._reply(status, result)

    def _reply(self, status, value):
        payload = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(payload)
        self.close_connection = True

    do_GET = _handle
    do_POST = _handle
