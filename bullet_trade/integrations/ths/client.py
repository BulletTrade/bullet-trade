"""Bounded client for the local THS service; no automatic trade retries."""
from __future__ import annotations

import json
import math
from datetime import date
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, ProxyHandler, build_opener


class ServiceUnavailable(RuntimeError):
    pass


class SnapshotUnavailable(ServiceUnavailable):
    pass


class RequestNotFound(ServiceUnavailable):
    """A read-only lookup found no durable request under the original key."""


class RequestAmbiguous(ServiceUnavailable):
    """The same key exists on multiple trading days; supply day or request ID."""


class SubmissionUncertain(ServiceUnavailable):
    """HTTP failure cannot prove whether the service persisted a request."""


class ThsServiceClient:
    def __init__(self, base_url, token, *, account, timeout=2.0):
        parsed = urlsplit(base_url)
        if (parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"}
                or parsed.username or parsed.password or parsed.query or parsed.fragment
                or parsed.path not in {"", "/"}):
            raise ValueError("explicit loopback HTTP URL required")
        if not math.isfinite(timeout) or not 0 < timeout <= 10:
            raise ValueError("timeout must be finite and at most ten seconds")
        if not isinstance(token, str) or not token.isascii() or len(token) < 24:
            raise ValueError("ASCII service token required")
        if not isinstance(account, str) or not account or account != account.strip():
            raise ValueError("account required")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.account = account
        self.timeout = timeout
        # Never route local tokens through environment-configured HTTP proxies.
        self.opener = build_opener(ProxyHandler({}))

    def _call(self, path, payload=None):
        data = None if payload is None else json.dumps(payload, allow_nan=False).encode()
        request = Request(self.base_url + path, data=data,
                          headers={"Authorization": "Bearer " + self.token,
                                   "Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read(8 * 1024 * 1024 + 1)
                if len(raw) > 8 * 1024 * 1024:
                    raise ServiceUnavailable("response_too_large")
                result = json.loads(raw)
                if not isinstance(result, dict):
                    raise ServiceUnavailable("invalid_service_response")
                return result
        except HTTPError as exc:
            if exc.code == 404 and path.startswith("/requests?"):
                raise RequestNotFound("request_not_found") from exc
            if exc.code == 409 and path.startswith("/requests?"):
                raise RequestAmbiguous("request_key_ambiguous_across_days") from exc
            raise ServiceUnavailable("service_http_%s" % exc.code) from exc
        except (URLError, OSError, ValueError) as exc:
            raise ServiceUnavailable("service_transport_or_response_failed") from exc

    def snapshot(self, kind):
        if kind not in {"account", "positions", "orders", "trades", "cancelable"}:
            raise ValueError("unknown snapshot kind")
        result = self._call("/snapshots/" + kind + "?account=" + quote(self.account, safe=""))
        if result.get("account_id") != self.account or result.get("kind") != kind:
            raise SnapshotUnavailable("snapshot_identity_mismatch")
        return result

    def qualified_snapshot(self, kind):
        """Return the whole current snapshot, including verified scope metadata."""
        result = self.snapshot(kind)
        if (result.get("complete") is not True or result.get("stale") is not False
                or result.get("status") != "complete" or result.get("last_error")
                or result.get("data") is None):
            raise SnapshotUnavailable("snapshot_not_current_and_complete")
        metadata = result.get("metadata")
        if not isinstance(metadata, dict) or metadata.get("schema") != "bullettrade_broker_v1":
            raise SnapshotUnavailable("broker_snapshot_schema_unqualified")
        expected = dict if kind == "account" else list
        if not isinstance(result["data"], expected):
            raise SnapshotUnavailable("snapshot_data_shape_invalid")
        return result

    def current_data(self, kind):
        """Strict adapter accessor; diagnostics may use snapshot() for stale data."""
        return self.qualified_snapshot(kind)["data"]

    def record_request(self, payload):
        if payload.get("account") != self.account:
            raise ValueError("account mismatch")
        try:
            result = self._call("/requests", payload)
        except ServiceUnavailable as exc:
            # Caller retains the exact body/key and reconciles. No blind retry.
            raise SubmissionUncertain("request_receipt_not_observed") from exc
        if (result.get("account") != self.account or not result.get("request_id")
                or result.get("acknowledgement") != "service_recorded_request"):
            raise SubmissionUncertain("invalid_request_receipt")
        return result

    def request_status(self, request_id):
        result = self._call("/requests/" + quote(request_id, safe=""))
        if result.get("request_id") != request_id or result.get("account") != self.account:
            raise ServiceUnavailable("request_identity_mismatch")
        return result

    def request_status_by_key(self, trade_day=None, idempotency_key=None):
        """Read the durable result after a lost POST receipt; never submit again."""
        if trade_day is not None:
            if not isinstance(trade_day, str):
                raise ValueError("trade_day must be YYYY-MM-DD")
            try:
                if date.fromisoformat(trade_day).isoformat() != trade_day:
                    raise ValueError
            except ValueError as exc:
                raise ValueError("trade_day must be YYYY-MM-DD") from exc
        if (not isinstance(idempotency_key, str) or not idempotency_key
                or idempotency_key != idempotency_key.strip()):
            raise ValueError("idempotency_key required")
        path = "/requests?account=" + quote(self.account, safe="")
        if trade_day is not None:
            path += "&trade_day=" + quote(trade_day, safe="")
        path += "&idempotency_key=" + quote(idempotency_key, safe="")
        result = self._call(path)
        result_day = result.get("trade_day")
        try:
            valid_result_day = (isinstance(result_day, str)
                                and date.fromisoformat(result_day).isoformat() == result_day)
        except ValueError:
            valid_result_day = False
        if (result.get("account") != self.account
                or (trade_day is not None and result.get("trade_day") != trade_day)
                or not valid_result_day
                or result.get("idempotency_key") != idempotency_key
                or not result.get("request_id")):
            raise ServiceUnavailable("request_identity_mismatch")
        return result
