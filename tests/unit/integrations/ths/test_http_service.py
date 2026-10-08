import json
import threading
import time
from http.client import HTTPConnection

import pytest

from bullet_trade.integrations.ths.http_service import KINDS, LocalServer, ServiceApplication


@pytest.fixture
def api(tmp_path):
    app = ServiceApplication(tmp_path, account="paper")
    token = "test-only-token-01234567890123456789"
    with LocalServer(("127.0.0.1", 0), app, token) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        yield app, server, token
        server.shutdown()
        thread.join(2)


def call(api, method, path, payload=None, authorized=True):
    _, server, token = api
    conn = HTTPConnection(*server.server_address, timeout=2)
    try:
        headers = {"Authorization": "Bearer " + token} if authorized else {}
        body = json.dumps(payload) if payload is not None else None
        conn.request(method, path, body, headers)
        response = conn.getresponse()
        return response.status, json.loads(response.read())
    finally:
        conn.close()


def test_api_auth_and_unknown_not_empty(api):
    assert call(api, "GET", "/health", authorized=False)[0] == 401
    status, result = call(api, "GET", "/snapshots/positions")
    assert status == 200 and result["data"] is None
    assert result["status"] == "unknown"
    assert call(api, "GET", "/snapshots/positions?account=other")[0] == 400
    assert call(api, "POST", "/requests", {})[0] == 503


def test_non_ascii_auth_is_unauthorized_not_disconnect(api):
    _, server, _ = api
    conn = HTTPConnection(*server.server_address, timeout=2)
    try:
        conn.request("GET", "/health", headers={"Authorization": "Bearer café"})
        response = conn.getresponse()
        assert response.status == 401
        response.read()
    finally:
        conn.close()


def test_ack_is_durable_and_retry_does_not_duplicate(api):
    app, _, _ = api
    app.allow_requests = True  # Test only; production CLI offers no enable flag.
    payload = dict(account="paper", trade_day="2026-09-30", idempotency_key="va1:buy1",
                   kind="limit_buy", params=dict(security="518880.XSHG", quantity=100, price="8.500"),
                   origin=dict(virtual_account_id="va1"), expires_at=time.time() + 60)
    status, first = call(api, "POST", "/requests", payload)
    assert status == 202, first
    assert first["broker_contract_no"] is None
    assert first["acknowledgement"] == "service_recorded_request"
    status, second = call(api, "POST", "/requests", payload)
    assert status == 202 and first["request_id"] == second["request_id"]
    status, stored = call(api, "GET", "/requests/" + first["request_id"])
    assert status == 200 and stored["origin"]["virtual_account_id"] == "va1"
    payload["params"]["price"] = "8.501"
    assert call(api, "POST", "/requests", payload)[0] == 409


def test_snapshot_api_does_not_wait_for_gui(api):
    app, _, _ = api
    from datetime import datetime, timezone
    app.snapshot_store.publish_success("paper", "positions", [{"security": "518880"}],
                                       datetime.now(timezone.utc), complete=True)
    blocked = threading.Event()
    fake_gui = threading.Thread(target=lambda: blocked.wait(10), daemon=True)
    fake_gui.start()
    try:
        started = time.monotonic()
        status, result = call(api, "GET", "/snapshots/positions")
        assert status == 200 and result["data"][0]["security"] == "518880"
        assert time.monotonic() - started < 1
    finally:
        blocked.set()
        fake_gui.join(2)


def test_lookup_by_original_key_recovers_lost_receipt_after_reopen(api):
    app, _, _ = api
    app.allow_requests = True
    payload = dict(account='paper', trade_day='2026-09-30', idempotency_key='va1:lost-receipt',
                   kind='limit_buy', params=dict(security='518880.XSHG', quantity=100, price='8.500'),
                   origin=dict(virtual_account_id='va1'), expires_at=time.time() + 60)
    # Simulate a lost response: caller does not retain the UUID from POST.
    assert call(api, 'POST', '/requests', payload)[0] == 202
    from bullet_trade.integrations.ths.request_store import RequestStore
    app.request_store = RequestStore(app.requests)
    status, recovered = call(api, 'GET', '/requests?account=paper&trade_day=2026-09-30&idempotency_key=va1%3Alost-receipt')
    assert status == 200 and recovered['state'] == 'queued'
    assert recovered['params'] == payload['params']
    assert recovered['origin'] == payload['origin']
    assert recovered['broker_contract_no'] is None
    assert call(api, 'GET', '/requests?account=other&trade_day=2026-09-30&idempotency_key=va1%3Alost-receipt')[0] == 400
    assert call(api, 'GET', '/requests?trade_day=2026-09-30&idempotency_key=missing')[0] == 404


def test_key_lookup_without_trade_day_requires_unique_match(api):
    app, _, _ = api
    payload = dict(account="paper", trade_day="2026-09-30", idempotency_key="va:cross-day",
                   kind="limit_buy", params=dict(security="518880.XSHG", quantity=100,
                                                  price="8.500"),
                   origin=dict(virtual_account_id="va"), expires_at=time.time() + 60)
    first = app.request_store.enqueue(**payload)
    status, found = call(api, "GET", "/requests?account=paper&idempotency_key=va%3Across-day")
    assert status == 200 and found["request_id"] == first.request_id
    assert call(api, "GET", "/requests?idempotency_key=missing") == (
        404, {"error": "request_not_found"})
    assert call(api, "GET", "/requests?account=other&idempotency_key=va%3Across-day")[0] == 400
    app.request_store.enqueue(**{**payload, "trade_day": "2026-10-01"})
    assert call(api, "GET", "/requests?idempotency_key=va%3Across-day") == (
        409, {"error": "request_conflict"})
    status, scoped = call(api, "GET", "/requests?trade_day=2026-09-30&idempotency_key=va%3Across-day")
    assert status == 200 and scoped["request_id"] == first.request_id


@pytest.mark.parametrize('query', [
    '', 'trade_day=2026-09-30', 'trade_day=20260930&idempotency_key=x',
    'trade_day=2026-09-30&idempotency_key=',
    'trade_day=2026-09-30&idempotency_key=x&idempotency_key=y',
    'trade_day=2026-09-30&trade_day=2026-09-29&idempotency_key=x',
    'trade_day=2026-09-30&idempotency_key=%20x',
    'trade_day=2026-09-30&idempotency_key=x&unknown=y',
])
def test_key_lookup_rejects_ambiguous_or_invalid_scope(api, query):
    assert call(api, 'GET', '/requests?' + query)[0] == 400


def test_unresolved_submission_has_explicit_error_and_does_not_enqueue(api):
    app, _, _ = api
    app.allow_requests = True
    payload = dict(account='paper', trade_day='2026-09-30', idempotency_key='va1:unknown',
                   kind='limit_buy', params=dict(security='518880.XSHG', quantity=100, price='8.500'),
                   origin=dict(virtual_account_id='va1'), expires_at=time.time() + 60)
    _, first = call(api, 'POST', '/requests', payload)
    app.request_store.mark_preparing(first['request_id'])
    app.request_store.mark_submit_unknown(first['request_id'])
    payload['idempotency_key'] = 'va1:new'
    assert call(api, 'POST', '/requests', payload) == (409, {'error': 'unresolved_submission'})
    assert app.request_store.get_by_key('paper', '2026-09-30', 'va1:new') is None


def test_snapshot_history_dispatch_is_paginated_and_read_only(tmp_path):
    from datetime import datetime, timezone
    app = ServiceApplication(tmp_path, account="paper")
    for seq in range(3):
        app.snapshot_store.publish_success("paper", "positions", [{"seq": seq}],
                                           datetime.now(timezone.utc), complete=True)
    app.snapshot_store.record_error("paper", "positions", "query failed")
    status, page = app.dispatch("GET", "/snapshots/positions/history?limit=2")
    assert status == 200
    assert [item["version"] for item in page["items"]] == [3, 2]
    assert page["next_before_version"] == 2
    status, next_page = app.dispatch("GET", "/snapshots/positions/history?before_version=2")
    assert status == 200 and [item["version"] for item in next_page["items"]] == [1]
    assert app.dispatch("GET", "/snapshots/positions")[1]["last_error"] == "query failed"
    assert app.allow_requests is False
    assert app.dispatch("GET", "/snapshots/positions/history?account=other")[0] == 400
    assert app.dispatch("GET", "/snapshots/unknown/history")[0] == 404


@pytest.mark.parametrize("query", ["limit=0", "limit=201", "limit=x", "limit=1&limit=2",
                                    "before_version=0", "since_version=-1", "extra=1",
                                    "before_version=2&since_version=2"])
def test_snapshot_history_rejects_invalid_bounds(tmp_path, query):
    app = ServiceApplication(tmp_path, account="paper")
    assert app.dispatch("GET", "/snapshots/positions/history?" + query) == (
        400, {"error": "snapshot_history_query_invalid"})


def test_health_requires_fresh_actor_heartbeat_and_safe_write_state(tmp_path):
    from datetime import datetime, timedelta, timezone
    app = ServiceApplication(tmp_path, account="paper")
    health = app.dispatch("GET", "/health")[1]
    assert health["trading_ready"] is False and health["actor_fresh"] is False
    state = {"driver_ready": True, "writes_enabled": True, "writes_stopped": False,
             "busy": True, "queries_ready": True,
             "required_snapshot_kinds": sorted(KINDS)}
    app.snapshot_store.publish_success("paper", "actor_health", state,
                                       datetime.now(timezone.utc), complete=True)
    health = app.dispatch("GET", "/health")[1]
    assert health["trading_ready"] is False
    assert health["required_snapshots_ready"] is False
    for kind in KINDS:
        app.snapshot_store.publish_success(
            "paper", kind, {} if kind == "account" else [],
            datetime.now(timezone.utc), complete=True)
    health = app.dispatch("GET", "/health")[1]
    assert health["trading_ready"] is False
    assert health["required_snapshots_ready"] is True
    assert health["request_intake_enabled"] is False
    app.allow_requests = True
    health = app.dispatch("GET", "/health")[1]
    assert health["trading_ready"] is True and health["actor_fresh"] is True
    assert health["request_intake_enabled"] is True
    assert health["actor_health"]["data"]["busy"] is True
    request = app.request_store.enqueue(
        "paper", "2026-10-01", "va:health", "limit_buy",
        {"security": "518880.XSHG", "quantity": 100, "price": "8.500"},
        time.time() + 60, origin={"virtual_account_id": "va"})
    app.request_store.mark_preparing(request.request_id)
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is False
    app.request_store.mark_submit_unknown(request.request_id)
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is False
    app.request_store.mark_rejected(request.request_id, evidence_ref="test:rejected")
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is True
    app.snapshot_store.publish_success("paper", "actor_health",
                                       {**state, "writes_stopped": True},
                                       datetime.now(timezone.utc), complete=True)
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is False
    app.snapshot_store.publish_success("paper", "actor_health", state,
                                       datetime.now(timezone.utc), complete=True)
    # A failed heartbeat retains its success value but must fail closed.
    app.snapshot_store.record_error("paper", "actor_health", "heartbeat failed")
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is False
    app.snapshot_store.publish_success("paper", "actor_health", state,
                                       datetime.now(timezone.utc) + timedelta(seconds=10),
                                       complete=True)
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is False


def test_health_keeps_each_query_error_until_that_kind_recovers(tmp_path):
    from datetime import datetime, timezone
    app = ServiceApplication(tmp_path, account="paper", allow_requests=True,
                             max_age_seconds=60)
    now = datetime.now(timezone.utc)
    for kind in KINDS:
        app.snapshot_store.publish_success(
            "paper", kind, {} if kind == "account" else [], now, complete=True)
    state = {"driver_ready": True, "writes_enabled": True, "writes_stopped": False,
             "queries_ready": True, "required_snapshot_kinds": sorted(KINDS)}
    app.snapshot_store.publish_success("paper", "actor_health", state, now, complete=True)
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is True

    app.snapshot_store.record_error("paper", "positions", "DriverBlocked")
    app.snapshot_store.publish_success("paper", "orders", [],
                                       datetime.now(timezone.utc), complete=True)
    health = app.dispatch("GET", "/health")[1]
    assert health["trading_ready"] is False
    assert health["snapshot_health"]["positions"]["last_error"] == "DriverBlocked"
    assert health["snapshot_health"]["orders"]["status"] == "complete"
    app.snapshot_store.publish_success("paper", "positions", [],
                                       datetime.now(timezone.utc), complete=True)
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is True

    app.snapshot_store.publish_success("paper", "actor_health", {
        **state, "queries_ready": False}, datetime.now(timezone.utc), complete=True)
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is False
    app.snapshot_store.publish_success("paper", "actor_health", state,
                                       datetime.now(timezone.utc), complete=True)
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is True

    app.max_age_seconds = 0
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is False
    app.max_age_seconds = 60
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is True

    app.snapshot_store.publish_success("paper", "actor_health", {
        **state, "required_snapshot_kinds": ["actor_health"]},
        datetime.now(timezone.utc), complete=True)
    assert app.dispatch("GET", "/health")[1]["trading_ready"] is False


def test_request_events_scope_and_pagination(tmp_path):
    app = ServiceApplication(tmp_path, account="paper")
    request = app.request_store.enqueue(
        "paper", "2026-10-01", "va:one", "limit_buy",
        {"security": "518880.XSHG", "quantity": 100, "price": "8.500"},
        time.time() + 60, origin={"virtual_account_id": "va"})
    app.request_store.mark_preparing(request.request_id)
    status, first = app.dispatch("GET", "/requests/" + request.request_id + "/events?limit=1")
    assert status == 200 and len(first["events"]) == 1
    assert first["next_after_id"] == first["events"][0]["event_id"]
    status, rest = app.dispatch("GET", "/requests/" + request.request_id +
                                "/events?after_id=" + str(first["next_after_id"]))
    assert status == 200 and rest["events"][0]["event_id"] > first["next_after_id"]
    assert app.dispatch("GET", "/requests/missing/events")[0] == 404
    other = ServiceApplication(tmp_path, account="other")
    assert other.dispatch("GET", "/requests/" + request.request_id + "/events")[0] == 404
    assert app.dispatch("GET", "/requests/" + request.request_id +
                        "/events?limit=201") == (400, {"error": "request_events_query_invalid"})
    assert app.dispatch("GET", "/requests/" + request.request_id +
                        "/events?after_id=1&after_id=2")[0] == 400
