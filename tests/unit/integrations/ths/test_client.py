import asyncio
import json

import pytest

from bullet_trade.integrations.ths.client import ThsServiceClient, ServiceUnavailable, SnapshotUnavailable, SubmissionUncertain
from bullet_trade.server.adapters.base import AccountRouter
from bullet_trade.server.config import AccountConfig
from bullet_trade.server.adapters.ths import ThsBrokerAdapter, ThsNotReadyError, ThsUnsupportedError


class Response:
    def __init__(self, payload):
        self.payload = payload
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def read(self, limit):
        return json.dumps(self.payload).encode()


class Opener:
    def __init__(self, payload=None, error=None):
        self.payload, self.error, self.calls = payload, error, 0
        self.requests = []
    def open(self, request, timeout):
        self.calls += 1
        self.requests.append(request)
        if self.error:
            raise self.error
        return Response(self.payload)


def client(payload=None, error=None):
    if isinstance(payload, dict) and "kind" in payload:
        payload.setdefault("metadata", {"schema": "bullettrade_broker_v1"})
    instance = ThsServiceClient("http://127.0.0.1:18765", "a" * 32, account="paper")
    instance.opener = Opener(payload, error)
    return instance


@pytest.mark.parametrize("change", [dict(stale=True), dict(complete=False),
    dict(account_id="other"), dict(status="error"), dict(data=None), dict(data={}),
    dict(metadata={"schema": "ths_diagnostic_v1"})])
def test_adapter_rejects_untrusted_or_stale_snapshot(change):
    payload = dict(account_id="paper", kind="positions", complete=True, stale=False,
                   status="complete", last_error=None, data=[])
    payload.update(change)
    transport = client(payload)
    router = AccountRouter([AccountConfig(key="sim", account_id="paper")])
    adapter = ThsBrokerAdapter(router, transport=transport)
    with pytest.raises(ThsNotReadyError):
        asyncio.run(adapter.get_positions(router.get("sim")))


def test_adapter_reads_data_only_for_bound_account():
    row = {"security": "518880.XSHG", "amount": 100, "closeable_amount": 100,
           "avg_cost": "8.500", "current_price": "8.510", "market_value": "851.00"}
    transport = client(dict(account_id="paper", kind="positions", complete=True,
                            stale=False, status="complete", data=[row]))
    router = AccountRouter([AccountConfig(key="sim", account_id="paper"),
                            AccountConfig(key="other", account_id="other")])
    adapter = ThsBrokerAdapter(router, transport=transport)
    result = asyncio.run(adapter.get_positions(router.get("sim")))
    assert result == [{**row, "avg_cost": 8.5, "current_price": 8.51, "market_value": 851.0}]
    assert row["avg_cost"] == "8.500"  # Stored exact decimal is unchanged.
    with pytest.raises(ThsNotReadyError):
        asyncio.run(adapter.get_positions(router.get("other")))


def test_lost_receipt_does_not_retry_or_invent_broker_id():
    transport = client(error=TimeoutError())
    body = {"account": "paper", "trade_day": "2026-09-30",
            "idempotency_key": "virtual-a:order-1", "kind": "limit_buy",
            "params": {"security": "518880.XSHG", "quantity": 100, "price": "8.500"},
            "expires_at": 1790751600, "origin": {"virtual_account_id": "virtual-a"}}
    with pytest.raises(SubmissionUncertain):
        transport.record_request(body)
    assert transport.opener.calls == 1
    request = transport.opener.requests[0]
    assert request.get_method() == "POST"
    assert json.loads(request.data) == body


def test_known_request_status_queries_persisted_request_without_resubmitting():
    transport = client({"request_id": "local-uuid", "account": "paper",
                        "idempotency_key": "virtual-a:order-1", "state": "submit_unknown",
                        "broker_contract_no": None})
    result = transport.request_status("local-uuid")
    assert result["state"] == "submit_unknown"
    assert result["broker_contract_no"] is None
    assert transport.opener.calls == 1
    assert transport.opener.requests[0].get_method() == "GET"
    assert transport.opener.requests[0].full_url.endswith("/requests/local-uuid")


def test_lost_receipt_recovers_by_durable_key_with_get_only():
    transport = client(error=TimeoutError())
    body = {"account": "paper", "trade_day": "2026-09-30",
            "idempotency_key": "virtual-a:order-1", "kind": "limit_buy",
            "params": {"security": "518880.XSHG", "quantity": 100, "price": "8.500"},
            "expires_at": 1790751600, "origin": {"virtual_account_id": "virtual-a"}}
    with pytest.raises(SubmissionUncertain):
        transport.record_request(body)
    transport.opener.error = None
    transport.opener.payload = {"request_id": "local-uuid", "account": "paper",
                                "trade_day": body["trade_day"],
                                "idempotency_key": body["idempotency_key"],
                                "state": "submit_unknown", "broker_contract_no": None}
    recovered = transport.request_status_by_key(body["trade_day"], body["idempotency_key"])
    assert recovered["state"] == "submit_unknown"
    assert [request.get_method() for request in transport.opener.requests] == ["POST", "GET"]
    assert transport.opener.requests[1].full_url.endswith(
        "/requests?account=paper&trade_day=2026-09-30&idempotency_key=virtual-a%3Aorder-1")


@pytest.mark.parametrize("trade_day,key", [("2026-9-30", "key"), ("2026-09-30", " key "),
                                          ("2026-09-31", "key"), ("2026-09-30", "")])
def test_request_status_by_key_rejects_invalid_identity_before_http(trade_day, key):
    transport = client()
    with pytest.raises(ValueError):
        transport.request_status_by_key(trade_day, key)
    assert transport.opener.calls == 0


def test_request_status_by_key_rejects_mismatched_durable_record():
    transport = client({"request_id": "local-uuid", "account": "paper",
                        "trade_day": "2026-09-30", "idempotency_key": "other-key"})
    with pytest.raises(ServiceUnavailable, match="request_identity_mismatch"):
        transport.request_status_by_key("2026-09-30", "virtual-a:order-1")


def test_adapter_preserves_share_units_zero_closeable_and_unknown_order():
    rows = [{"security": "518880.XSHG", "amount": 100, "closeable_amount": 0,
             "avg_cost": "8.500", "current_price": "8.510", "market_value": "851.00"}]
    transport = client(dict(account_id="paper", kind="positions", complete=True,
                            stale=False, status="complete", data=rows))
    router = AccountRouter([AccountConfig(key="sim", account_id="paper")])
    adapter = ThsBrokerAdapter(router, transport=transport)
    positions = asyncio.run(adapter.get_positions(router.get("sim")))
    assert positions[0]["amount"] == 100
    assert positions[0]["closeable_amount"] == 0
    assert positions[0]["security"] == "518880.XSHG"

    order = {"order_id": "0001", "security": "518880.XSHG", "amount": 100,
             "filled": 0, "order_price": "8.500", "is_buy": True, "status": "unknown"}
    transport.opener.payload = dict(account_id="paper", kind="orders", complete=True,
                                    stale=False, status="complete", data=[order],
                                    metadata={"schema": "bullettrade_broker_v1"})
    result = asyncio.run(adapter.get_order_status(router.get("sim"), "0001"))
    assert result["order_id"] == "0001"
    assert result["status"] == "unknown"


@pytest.mark.parametrize("kind,method", [("orders", "list_orders"), ("trades", "list_trades")])
def test_adapter_accepts_server_routing_payload_but_rejects_real_filters(kind, method):
    transport = client(dict(account_id="paper", kind=kind, complete=True,
                            stale=False, status="complete", data=[]))
    router = AccountRouter([AccountConfig(key="sim", account_id="paper")])
    adapter = ThsBrokerAdapter(router, transport=transport)
    read = getattr(adapter, method)
    assert asyncio.run(read(router.get("sim"), {"account_key": "sim", "filters": {}})) == []
    with pytest.raises(ThsUnsupportedError, match="filters unsupported"):
        asyncio.run(read(router.get("sim"), {"account_key": "sim", "filters": {"status": "filled"}}))


@pytest.mark.parametrize("rows", [[], [{"order_id": "0001"}, {"order_id": "0001"}],
                                   [{"order_id": "1"}]])
def test_order_status_requires_unique_original_contract(rows):
    rows = [{"security": "518880.XSHG", "amount": 100, "filled": 0,
             "order_price": "8.500", "is_buy": True, "status": "unknown", **row} for row in rows]
    transport = client(dict(account_id="paper", kind="orders", complete=True,
                            stale=False, status="complete", data=rows))
    router = AccountRouter([AccountConfig(key="sim", account_id="paper")])
    with pytest.raises(ThsNotReadyError):
        asyncio.run(ThsBrokerAdapter(router, transport=transport).get_order_status(router.get("sim"), "0001"))
