"""Synthetic loopback contract test; no THS GUI or broker is involved."""

import asyncio
from datetime import date, datetime, timedelta, timezone
from threading import Thread
import time

import pytest

from bullet_trade.integrations.ths.client import (
    ServiceUnavailable, SnapshotUnavailable, SubmissionUncertain, ThsServiceClient,
)
from bullet_trade.integrations.ths.http_service import LocalServer, ServiceApplication
from bullet_trade.server.adapters.base import AccountRouter
from bullet_trade.server.adapters.ths import ThsBrokerAdapter, ThsNotReadyError
from bullet_trade.server.config import AccountConfig


ACCOUNT = "synthetic-paper"
TOKEN = "test-only-loopback-token-0123456789"
SCHEMA = {"schema": "bullettrade_broker_v1"}
ORDER_ID = "000123456"


@pytest.fixture
def service(tmp_path):
    app = ServiceApplication(tmp_path, account=ACCOUNT, allow_requests=True,
                             max_age_seconds=60)
    with LocalServer(("127.0.0.1", 0), app, TOKEN) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = ThsServiceClient(
            "http://127.0.0.1:%d" % server.server_address[1], TOKEN,
            account=ACCOUNT,
        )
        router = AccountRouter([AccountConfig(key="paper", account_id=ACCOUNT)])
        yield app, server, client, ThsBrokerAdapter(router, transport=client), router.get("paper")
        server.shutdown()
        thread.join(timeout=2)


def _publish(app, kind, data, *, age_seconds=0):
    app.snapshot_store.publish_success(
        ACCOUNT, kind, data,
        datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
        complete=True, metadata=SCHEMA,
    )


def test_five_snapshots_and_exact_original_contract_over_real_http(service):
    app, _, client, adapter, account = service
    for kind in ("account", "positions", "orders", "trades", "cancelable"):
        assert client.snapshot(kind)["status"] == "unknown"
        with pytest.raises(SnapshotUnavailable):
            client.current_data(kind)

    position = {"security": "518880.XSHG", "amount": 100, "closeable_amount": 0,
                "avg_cost": "8.500", "current_price": "8.510", "market_value": "851.00"}
    order = {"order_id": ORDER_ID, "security": "518880.XSHG", "amount": 100,
             "filled": 0, "order_price": "8.500", "is_buy": True, "status": "unknown"}
    trade = {"trade_id": "000987", "order_id": ORDER_ID,
             "security": "518880.XSHG", "amount": 100, "price": "8.500",
             "deal_balance": "850.00", "time": "2026-09-30T10:00:00+08:00",
             "is_buy": True}
    _publish(app, "positions", [position], age_seconds=120)
    assert client.snapshot("positions")["stale"] is True
    with pytest.raises(ThsNotReadyError):
        asyncio.run(adapter.get_positions(account))

    _publish(app, "account", {"available_cash": "1000.00", "total_value": "1851.00"})
    _publish(app, "positions", [position])
    _publish(app, "orders", [order])
    _publish(app, "trades", [trade])
    _publish(app, "cancelable", [order])
    for kind in ("account", "positions", "orders", "trades", "cancelable"):
        assert client.snapshot(kind)["stale"] is False
        assert client.current_data(kind)

    assert asyncio.run(adapter.get_account_info(account))["value"]["available_cash"] == 1000.0
    assert asyncio.run(adapter.get_positions(account))[0]["closeable_amount"] == 0
    assert asyncio.run(adapter.list_orders(account))[0]["order_id"] == ORDER_ID
    assert asyncio.run(adapter.list_trades(account))[0]["trade_id"] == "000987"
    assert asyncio.run(adapter.get_order_status(account, ORDER_ID))["status"] == "unknown"
    with pytest.raises(ThsNotReadyError):
        asyncio.run(adapter.get_order_status(account, ORDER_ID.lstrip("0")))

    app.snapshot_store.record_error(ACCOUNT, "trades", "synthetic_query_timeout")
    assert client.snapshot("trades")["stale"] is True
    with pytest.raises(ThsNotReadyError):
        asyncio.run(adapter.list_trades(account))


def test_lost_receipt_recovers_by_key_after_service_reopen(service):
    app, server, client, _, _ = service
    day = date.today().isoformat()
    payload = {
        "account": ACCOUNT, "trade_day": day,
        "idempotency_key": "virtual-a:synthetic-1", "kind": "limit_buy",
        "params": {"security": "518880.XSHG", "quantity": 100, "price": "8.500"},
        "origin": {"virtual_account_id": "virtual-a", "strategy_id": "synthetic"},
        "expires_at": time.time() + 60,
    }
    original_call = client._call
    methods = []

    def drop_receipt(path, body=None):
        methods.append("POST" if body is not None else "GET")
        result = original_call(path, body)
        if body is not None:
            raise ServiceUnavailable("synthetic_lost_receipt")
        return result

    client._call = drop_receipt
    with pytest.raises(SubmissionUncertain):
        client.record_request(payload)
    server.application = ServiceApplication(app.directory, account=ACCOUNT,
                                            allow_requests=True)
    recovered = client.request_status_by_key(day, payload["idempotency_key"])
    assert methods == ["POST", "GET"]
    assert recovered["state"] == "queued"
    assert recovered["broker_contract_no"] is None
    assert recovered["origin"] == payload["origin"]
    assert recovered["params"] == payload["params"]
    assert client.request_status(recovered["request_id"])["request_id"] == recovered["request_id"]

    store = server.application.request_store
    store.mark_preparing(recovered["request_id"])
    store.mark_submit_unknown(recovered["request_id"])
    server.application = ServiceApplication(app.directory, account=ACCOUNT,
                                            allow_requests=True)
    unknown = client.request_status_by_key(day, payload["idempotency_key"])
    assert unknown["state"] == "submit_unknown"
    assert unknown["broker_contract_no"] is None
    assert server.application.request_store.has_unresolved()

    server.application.request_store.mark_accepted(
        recovered["request_id"], ORDER_ID, "synthetic:broker_acceptance"
    )
    server.application = ServiceApplication(app.directory, account=ACCOUNT,
                                            allow_requests=True)
    accepted = client.request_status_by_key(day, payload["idempotency_key"])
    assert accepted["state"] == "accepted"
    assert accepted["broker_contract_no"] == ORDER_ID
    assert accepted["origin"] == payload["origin"]
    assert methods.count("POST") == 1
