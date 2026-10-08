"""Offline tests for adapter boundaries; no socket, GUI, or broker access."""

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from bullet_trade.integrations.ths.client import (
    RequestAmbiguous, RequestNotFound, SubmissionUncertain, ThsServiceClient,
)
from bullet_trade.server.adapters.base import AccountRouter
from bullet_trade.server.adapters.ths import (
    ThsBrokerAdapter, ThsNotReadyError, ThsRequestPending, ThsUnsupportedError,
)
from bullet_trade.server.config import AccountConfig


TOKEN = "test-only-loopback-token-0123456789"


def test_client_cross_day_lookup_omits_day_and_keeps_returned_day(monkeypatch):
    client = ThsServiceClient("http://127.0.0.1:8781", TOKEN, account="paper-id")
    paths = []
    def call(path, payload=None):
        paths.append(path)
        return {"account": "paper-id", "trade_day": "2026-09-30",
                "idempotency_key": "durable-key", "request_id": "local-uuid"}
    monkeypatch.setattr(client, "_call", call)
    result = client.request_status_by_key(None, "durable-key")
    assert result["trade_day"] == "2026-09-30"
    assert paths == ["/requests?account=paper-id&idempotency_key=durable-key"]


def _adapter(*, wait_seconds=0):
    router = AccountRouter([AccountConfig(key="paper", account_id="paper-id")])
    client = ThsServiceClient("http://127.0.0.1:8781", TOKEN, account="paper-id")
    return ThsBrokerAdapter(router, transport=client, wait_seconds=wait_seconds), client, router.get("paper")


def _order(**extra):
    return {"security": "518880.XSHG", "side": "BUY", "amount": 100,
            "style": {"type": "limit", "price": "8.500"},
            "idempotency_key": "order-1", **extra}


def test_limit_order_uses_durable_request_and_original_contract(monkeypatch):
    adapter, client, account = _adapter()
    writes = []
    monkeypatch.setattr(client, "request_status_by_key", lambda *_: (_ for _ in ()).throw(RequestNotFound()))

    def record(body):
        writes.append(body)
        return {**body, "request_id": "local-uuid", "state": "accepted",
                "broker_contract_no": "00012345", "acknowledgement": "service_recorded_request"}

    monkeypatch.setattr(client, "record_request", record)
    result = asyncio.run(adapter.place_order(account, _order()))
    assert result["order_id"] == "00012345"
    assert result["request_id"] == "local-uuid"
    assert writes[0]["params"] == {"security": "518880.XSHG", "quantity": 100, "price": "8.5"}
    assert writes[0]["origin"] == {"subaccount_key": "parent:paper"}
    assert writes[0]["idempotency_key"].startswith("ths:")
    assert len(writes[0]["idempotency_key"]) == 93


@pytest.mark.parametrize("action,payload", [
    ("buy", _order(request_id="missing-uuid")),
    ("sell", _order(side="SELL", request_id="missing-uuid")),
    ("cancel", {"order_id": "00012345", "idempotency_key": "cancel-1",
                "request_id": "missing-uuid"}),
])
def test_explicit_missing_request_id_never_posts(monkeypatch, action, payload):
    adapter, client, account = _adapter()
    calls = []

    def missing(request_id):
        calls.append(("GET", request_id))
        raise RequestNotFound()

    monkeypatch.setattr(client, "request_status", missing)
    monkeypatch.setattr(client, "request_status_by_key",
                        lambda *_: pytest.fail("unexpected key lookup"))
    monkeypatch.setattr(client, "record_request",
                        lambda *_: pytest.fail("unexpected POST"))
    with pytest.raises(ThsNotReadyError, match="explicit request_id not found"):
        if action == "cancel":
            asyncio.run(adapter.cancel_order_request(account, payload))
        else:
            asyncio.run(adapter.place_order(account, payload))
    assert calls == [("GET", "missing-uuid")]


def test_subaccounts_partition_keys_and_origin(monkeypatch):
    adapter, client, account = _adapter()
    writes = []
    monkeypatch.setattr(client, "request_status_by_key", lambda *_: (_ for _ in ()).throw(RequestNotFound()))

    def record(body):
        writes.append(body)
        return {**body, "request_id": "local-uuid", "state": "queued",
                "broker_contract_no": None}

    monkeypatch.setattr(client, "record_request", record)
    for sub in ("alpha", "beta"):
        with pytest.raises(ThsRequestPending) as caught:
            asyncio.run(adapter.place_order(account, _order(sub_account_id=sub)))
        assert caught.value.request_id == "local-uuid"
    assert writes[0]["idempotency_key"] != writes[1]["idempotency_key"]
    assert writes[0]["origin"] == {"virtual_account_id": "alpha"}
    assert writes[1]["origin"] == {"virtual_account_id": "beta"}


def test_lost_receipt_never_posts_again(monkeypatch):
    adapter, client, account = _adapter()
    calls = []

    def lookup(*_):
        calls.append("GET")
        if len(calls) == 1:
            raise RequestNotFound()
        raise RequestNotFound()

    def record(_):
        calls.append("POST")
        raise SubmissionUncertain("receipt lost")

    monkeypatch.setattr(client, "request_status_by_key", lookup)
    monkeypatch.setattr(client, "record_request", record)
    with pytest.raises(ThsRequestPending) as caught:
        asyncio.run(adapter.place_order(account, _order()))
    assert calls == ["GET", "POST", "GET"]
    assert caught.value.state == "receipt_unknown"
    assert caught.value.request_id is None
    assert caught.value.idempotency_key.startswith("ths:")
    assert caught.value.source_idempotency_key == "order-1"


def test_existing_request_is_read_only_and_payload_mismatch_rejected(monkeypatch):
    adapter, client, account = _adapter()
    monkeypatch.setattr(client, "record_request", lambda *_: pytest.fail("unexpected POST"))
    origin, durable_key = adapter._origin_and_key(account, _order())
    previous = {"account": "paper-id", "trade_day": "2026-09-30",
                "idempotency_key": durable_key,
                "kind": "limit_buy", "params": {"security": "518880.XSHG",
                "quantity": 100, "price": "8.5"},
                "origin": origin,
                "request_id": "previous-uuid", "state": "accepted",
                "broker_contract_no": "00012345"}
    lookups = []
    def lookup(day, key):
        lookups.append((day, key))
        return previous
    monkeypatch.setattr(client, "request_status_by_key", lookup)
    result = asyncio.run(adapter.place_order(account, _order()))
    assert result["order_id"] == "00012345"
    assert result["trade_day"] == "2026-09-30"
    assert lookups[0] == (None, durable_key)
    with pytest.raises(ValueError, match="reused with different request"):
        asyncio.run(adapter.place_order(account, _order(amount=200)))


def test_decimal_price_forms_share_same_durable_body():
    assert ThsBrokerAdapter._order_request(_order(style={"type": "limit", "price": "10"})) == (
        "limit_buy", {"security": "518880.XSHG", "quantity": 100, "price": "10"})
    assert ThsBrokerAdapter._order_request(_order(style={"type": "limit", "price": "10.00"})) == (
        "limit_buy", {"security": "518880.XSHG", "quantity": 100, "price": "10"})


def test_late_acceptance_is_resolved_by_original_key_without_post(monkeypatch):
    adapter, client, account = _adapter()
    request = _order()
    durable = {}
    posts = []

    def lookup(day, key):
        if not durable:
            raise RequestNotFound()
        assert key == durable["idempotency_key"]
        return {**durable, "state": "accepted", "broker_contract_no": "00012345"}

    def record(body):
        posts.append(body)
        durable.update({**body, "request_id": "local-uuid"})
        return {**durable, "state": "queued", "broker_contract_no": None}

    monkeypatch.setattr(client, "request_status_by_key", lookup)
    monkeypatch.setattr(client, "record_request", record)
    with pytest.raises(ThsRequestPending):
        asyncio.run(adapter.place_order(account, request))
    resolved = asyncio.run(adapter.resolve_submission(account, {
        "write_action": "broker.place_order", "idempotency_key": "order-1",
        "request_payload": request,
    }))
    assert len(posts) == 1
    assert resolved["status"] == "accepted"
    assert resolved["request_id"] == "local-uuid"
    assert resolved["order_id"] == "00012345"
    assert resolved["resolved_result"]["side"] == "BUY"
    assert resolved["resolved_result"]["amount"] == 100
    assert resolved["trade_day"] == durable["trade_day"]


def test_resolution_rejects_other_subaccount_request(monkeypatch):
    adapter, client, account = _adapter()
    original = _order(sub_account_id="alpha")
    _, other_key = adapter._origin_and_key(account, _order(sub_account_id="beta"))
    other = {"account": "paper-id", "idempotency_key": other_key,
             "kind": "limit_buy", "params": {"security": "518880.XSHG",
             "quantity": 100, "price": "8.5"},
             "origin": {"virtual_account_id": "beta"}, "request_id": "other-uuid",
             "state": "accepted", "broker_contract_no": "00098765"}
    monkeypatch.setattr(client, "request_status_by_key", lambda *_: other)
    monkeypatch.setattr(client, "record_request", lambda *_: pytest.fail("unexpected POST"))
    with pytest.raises(ValueError, match="identity mismatch"):
        asyncio.run(adapter.resolve_submission(account, {
            "write_action": "broker.place_order", "idempotency_key": "order-1",
            "request_payload": original,
        }))
    with pytest.raises(ValueError, match="sub-account identity mismatch"):
        asyncio.run(adapter.resolve_submission(account, {
            "write_action": "broker.place_order", "idempotency_key": "order-1",
            "sub_account_id": "beta", "request_payload": original,
        }))


def test_cancel_acceptance_does_not_claim_cancellation_complete(monkeypatch):
    adapter, client, account = _adapter()
    monkeypatch.setattr(client, "request_status_by_key", lambda *_: (_ for _ in ()).throw(RequestNotFound()))

    def record(body):
        assert body["kind"] == "cancel"
        assert body["params"] == {"broker_contract_no": "00012345"}
        return {**body, "request_id": "cancel-uuid", "state": "accepted",
                "broker_contract_no": "00012345"}

    monkeypatch.setattr(client, "record_request", record)
    with pytest.raises(ThsRequestPending) as caught:
        asyncio.run(adapter.cancel_order_request(
            account, {"order_id": "00012345", "idempotency_key": "cancel-1"}))
    assert caught.value.state == "cancel_received"
    assert caught.value.request_id == "cancel-uuid"


def test_cancel_requires_exact_final_snapshot_before_resolved(monkeypatch):
    adapter, client, account = _adapter()
    original = {"order_id": "00012345", "idempotency_key": "cancel-1"}
    origin, key = adapter._origin_and_key(account, original)
    day = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
    persisted = {"account": "paper-id", "trade_day": day, "idempotency_key": key,
                 "kind": "cancel", "params": {"broker_contract_no": "00012345"},
                 "origin": origin, "request_id": "cancel-uuid", "state": "accepted",
                 "broker_contract_no": "00012345"}
    monkeypatch.setattr(client, "request_status_by_key", lambda *_: persisted)
    monkeypatch.setattr(client, "record_request", lambda *_: pytest.fail("unexpected POST"))
    row = {"order_id": "00012345", "security": "518880.XSHG",
           "amount": 100, "filled": 0, "order_price": "8.5",
           "is_buy": True, "status": "open"}
    monkeypatch.setattr(client, "qualified_snapshot", lambda *_: {
        "metadata": {"trade_day": day}, "data": [row]})
    query = {"write_action": "broker.cancel_order", "idempotency_key": "cancel-1",
             "request_payload": original}
    assert asyncio.run(adapter.resolve_submission(account, query))["status"] == "reconciling"
    row["status"] = "cancelled"
    result = asyncio.run(adapter.resolve_submission(account, query))
    assert result["status"] == "accepted"
    assert result["resolved_result"]["last_snapshot"]["status"] == "cancelled"
    direct = asyncio.run(adapter.cancel_order_request(account, original))
    assert direct["value"] is True
    assert direct["last_snapshot"]["order_id"] == "00012345"
    scoped = {**row, "trade_day": day,
              "trade_day_source": "durable_accepted_request",
              "request_id": "origin-uuid"}
    monkeypatch.setattr(client, "qualified_snapshot", lambda *_: {
        "metadata": {"schema": "bullettrade_broker_v1"}, "data": [scoped]})
    scoped_direct = asyncio.run(adapter.cancel_order_request(account, original))
    assert scoped_direct["value"] is True
    assert scoped_direct["last_snapshot"]["trade_day_source"] == "durable_accepted_request"
    monkeypatch.setattr(client, "qualified_snapshot", lambda *_: {
        "metadata": {"trade_day": "2026-09-30"}, "data": [row]})
    assert asyncio.run(adapter.resolve_submission(account, query))["status"] == "reconciling"
    monkeypatch.setattr(client, "qualified_snapshot", lambda *_: {
        "metadata": {"trade_day": day}, "data": [{**row, "status": "filled"}]})
    rejected = asyncio.run(adapter.resolve_submission(account, query))
    assert rejected["status"] == "rejected"
    assert rejected["resolved_result"]["value"] is False


def test_cancel_terminal_rejects_unknown_or_conflicting_row_scope(monkeypatch):
    adapter, client, account = _adapter()
    day = "2025-01-06"
    row = {"order_id": "SYN001", "security": "518880.XSHG",
           "amount": 100, "filled": 0, "order_price": "8.5",
           "is_buy": True, "status": "canceled"}
    snapshot = {"metadata": {"schema": "bullettrade_broker_v1"},
                "data": [row]}
    monkeypatch.setattr(client, "qualified_snapshot", lambda *_: snapshot)
    assert asyncio.run(adapter._cancel_terminal(account, "SYN001", day))[0] == "unknown"
    for changed in (
        {"trade_day": day},
        {"trade_day": day, "trade_day_source": "durable_accepted_request"},
        {"trade_day": day, "trade_day_source": "durable_accepted_request",
         "request_id": ""},
        {"trade_day": "2025-01-03", "trade_day_source": "durable_accepted_request",
         "request_id": "origin-uuid"},
        {"trade_day": day, "trade_day_source": "durable_accepted_request",
         "request_id": "origin-uuid", "order_time": "2025-01-03T09:35:00+08:00"},
    ):
        snapshot["data"] = [{**row, **changed}]
        assert asyncio.run(adapter._cancel_terminal(account, "SYN001", day))[0] == "unknown"
    snapshot["data"] = [{**row, "trade_day": day,
                         "trade_day_source": "durable_accepted_request",
                         "request_id": "origin-uuid"},
                        {**row, "order_id": "SYN002", "trade_day": day}]
    assert asyncio.run(adapter._cancel_terminal(account, "SYN001", day))[0] == "cancelled"
    snapshot["data"] = [snapshot["data"][0], dict(snapshot["data"][0])]
    assert asyncio.run(adapter._cancel_terminal(account, "SYN001", day))[0] == "unknown"
    snapshot["metadata"]["trade_day"] = "2025-01-03"
    snapshot["data"] = [snapshot["data"][0]]
    assert asyncio.run(adapter._cancel_terminal(account, "SYN001", day))[0] == "unknown"
    snapshot["metadata"]["trade_day"] = day
    snapshot["data"] = [{**row, "trade_day": day}]
    assert asyncio.run(adapter._cancel_terminal(account, "SYN001", day))[0] == "cancelled"
    snapshot["data"] = [{**row, "order_time": "2025-01-03T09:35:00+08:00"}]
    assert asyncio.run(adapter._cancel_terminal(account, "SYN001", day))[0] == "unknown"


def test_ambiguous_cross_day_key_never_creates_request(monkeypatch):
    adapter, client, account = _adapter()
    monkeypatch.setattr(client, "request_status_by_key", lambda *_: (
        _ for _ in ()).throw(RequestAmbiguous("multiple days")))
    monkeypatch.setattr(client, "record_request", lambda *_: pytest.fail("unexpected POST"))
    with pytest.raises(Exception, match="ambiguous across trading days"):
        asyncio.run(adapter.place_order(account, _order()))
    result = asyncio.run(adapter.resolve_submission(account, {
        "write_action": "broker.place_order", "idempotency_key": "order-1",
        "request_payload": _order()}))
    assert result["status"] == "reconciling"
    assert result["reason"] == "request_key_ambiguous_across_days"


@pytest.mark.parametrize("change", [
    {"style": {"type": "market", "price": "8.500"}},
    {"market": True},
    {"style": {"type": "limit"}},
])
def test_market_and_missing_price_never_reach_service(monkeypatch, change):
    adapter, client, account = _adapter()
    monkeypatch.setattr(client, "record_request", lambda *_: pytest.fail("unexpected POST"))
    with pytest.raises(ThsUnsupportedError):
        asyncio.run(adapter.place_order(account, _order(**change)))
