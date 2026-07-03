import asyncio

import pytest

from bullet_trade.server.adapters import get_adapter
from bullet_trade.server.adapters.base import AccountRouter, AdapterBundle
from bullet_trade.server.adapters.big_qmt import (
    BigQmtBrokerAdapter,
    BigQmtDataAdapter,
    BigQmtGatewayConfig,
    BigQmtGatewayError,
    build_big_qmt_bundle,
)
from bullet_trade.server.app import ServerApplication
from bullet_trade.server.config import AccountConfig, ServerConfig


class _FakeGatewayClient:
    def __init__(self, responses, config=None):
        self.responses = responses
        self.calls = []
        self.config = config or BigQmtGatewayConfig()

    async def post(self, path, payload=None):
        self.calls.append(("POST", path, payload or {}))
        value = self.responses[path]
        if isinstance(value, Exception):
            raise value
        if callable(value):
            return value(payload or {})
        return value

    async def post_first(self, paths, payload=None):
        for path in paths:
            if path in self.responses:
                return await self.post(path, payload)
        raise BigQmtGatewayError("missing", code="NOT_IMPLEMENTED")

    async def health(self):
        return self.responses.get("/health", {"ready": True})

    def qmt_status(self):
        return {
            "backend_type": "big_qmt",
            "ready": True,
            "big_qmt_gateway": {"ready": True},
            "actions": self.config.action_status,
        }


def _server_config(enable_data=True, enable_broker=True):
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    return ServerConfig(
        server_type="big_qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=enable_data,
        enable_broker=enable_broker,
        accounts=[AccountConfig(key="default", account_id="demo", account_type="stock")],
    )


def test_big_qmt_adapter_is_registered_and_health_reports_backend(monkeypatch):
    monkeypatch.delenv("BIG_QMT_ENABLE_TRADING", raising=False)
    monkeypatch.delenv("BIG_QMT_ENABLE_CANCEL_ORDER", raising=False)

    config = _server_config()
    router = AccountRouter(config.accounts)
    bundle = build_big_qmt_bundle(config, router)
    app = ServerApplication(config, router, bundle)

    assert get_adapter("big_qmt") is build_big_qmt_bundle
    assert get_adapter("big-qmt") is build_big_qmt_bundle

    health = app._health_snapshot()["value"]
    assert health["backend_type"] == "big_qmt"
    assert health["qmt"]["actions"]["data.snapshot"]["status"] == "ready"
    assert health["qmt"]["actions"]["data.current_tick"]["status"] == "ready"
    assert health["qmt"]["actions"]["data.subscribe"]["status"] == "degraded"
    assert health["qmt"]["actions"]["broker.place_order"]["status"] == "unavailable"
    assert health["qmt"]["actions"]["broker.cancel_order"]["status"] == "unavailable"


@pytest.mark.asyncio
async def test_big_qmt_data_adapter_normalizes_gateway_payloads():
    client = _FakeGatewayClient(
        {
            "/data/history": {"records": [{"open": 1.0, "close": 2.0}]},
            "/data/snapshot": {"ticks": {"000001.XSHE": {"lastPrice": 12.3, "time": 1783043331000, "bidPrice": [12.2]}}},
            "/data/live_current": {
                "ticks": {
                    "000001.XSHE": {
                        "lastPrice": 12.5,
                        "high_limit": 13.75,
                        "low_limit": 11.25,
                        "openInt": 13,
                        "bidPrice": [12.4],
                    }
                }
            },
            "/data/current_tick": {"ticks": {"000001.XSHE": {"lastPrice": 12.4, "timetag": "20260703 09:30:00"}}},
            "/data/trade_days": {"values": ["20260701"]},
            "/data/security_info": {"display_name": "平安银行", "type": "stock"},
            "/data/ensure_cache": {"requested": True, "security": "000001.XSHE"},
            "/data/all_securities": {"records": [{"security": "000001.XSHE", "sector": "沪深A股"}]},
            "/data/index_stocks": {"stocks": ["000001.XSHE", "000002.XSHE"]},
            "/data/split_dividend": {"events": [{"security": "000001.XSHE"}]},
        }
    )
    adapter = BigQmtDataAdapter(client)

    history = await adapter.get_history({"security": "000001.XSHE"})
    assert history["dtype"] == "dataframe"
    assert history["columns"] == ["open", "close"]
    assert history["records"] == [[1.0, 2.0]]

    snapshot = await adapter.get_snapshot({"security": "000001.XSHE"})
    assert snapshot == {"sid": "000001.XSHE", "last_price": 12.3, "dt": 1783043331000}

    live_current = await adapter.get_live_current({"security": "000001.XSHE"})
    assert live_current == {
        "last_price": 12.5,
        "high_limit": 13.75,
        "low_limit": 11.25,
        "paused": False,
    }

    current_tick = await adapter.get_current_tick("000001.XSHE")
    assert current_tick == {
        "sid": "000001.XSHE",
        "last_price": 12.4,
        "dt": "20260703 09:30:00",
    }

    trade_days = await adapter.get_trade_days({"count": 1})
    assert trade_days == {"dtype": "list", "values": ["2026-07-01 00:00:00"]}

    info = await adapter.get_security_info({"security": "000001.XSHE"})
    assert info["dtype"] == "dict"
    assert info["display_name"] == "平安银行"

    cache = await adapter.ensure_cache({"security": "000001.XSHE"})
    assert cache["dtype"] == "dict"
    assert cache["value"]["requested"] is True

    securities = await adapter.get_all_securities({"types": ["stock"]})
    assert securities["dtype"] == "dataframe"
    assert securities["records"] == [["000001.XSHE", "沪深A股"]]

    stocks = await adapter.get_index_stocks({"index_symbol": "000300.XSHG"})
    assert stocks["values"] == ["000001.XSHE", "000002.XSHE"]

    events = await adapter.get_split_dividend({"security": "000001.XSHE"})
    assert events["events"] == [{"security": "000001.XSHE"}]


@pytest.mark.asyncio
async def test_server_dispatches_data_current_tick_with_payload():
    client = _FakeGatewayClient(
        {
            "/data/snapshot": {"ticks": {"000001.XSHE": {"lastPrice": 12.3, "time": 1783043331000}}},
        }
    )
    config = _server_config(enable_broker=False)
    router = AccountRouter(config.accounts)
    adapter = BigQmtDataAdapter(client)
    app = ServerApplication(config, router, AdapterBundle(data_adapter=adapter, broker_adapter=None))

    current_tick = await app._dispatch_data("current_tick", {"security": "000001.XSHE"})

    assert current_tick == {"sid": "000001.XSHE", "last_price": 12.3, "dt": 1783043331000}
    assert client.calls == [("POST", "/data/snapshot", {"security": "000001.XSHE"})]


@pytest.mark.asyncio
async def test_big_qmt_trade_days_accepts_multiple_gateway_date_formats():
    client = _FakeGatewayClient(
        {
            "/data/trade_days": {
                "dtype": "list",
                "values": [
                    "20260629",
                    "2026-06-30",
                    "2026-07-01 00:00:00",
                    20260702,
                ],
            },
        }
    )
    adapter = BigQmtDataAdapter(client)

    trade_days = await adapter.get_trade_days({"start": "2026-06-29", "end": "2026-07-02"})

    assert trade_days == {
        "dtype": "list",
        "values": [
            "2026-06-29 00:00:00",
            "2026-06-30 00:00:00",
            "2026-07-01 00:00:00",
            "2026-07-02 00:00:00",
        ],
    }


@pytest.mark.asyncio
async def test_big_qmt_broker_adapter_normalizes_account_positions_orders_trades():
    client = _FakeGatewayClient(
        {
            "/account": {"available_cash": 10000, "total_value": 12000},
            "/positions": {
                "positions": [
                    {
                        "m_strInstrumentID": "510050",
                        "m_strExchangeID": "SH",
                        "m_nVolume": 1000,
                        "m_nCanUseVolume": 800,
                        "m_dOpenPrice": 2.5,
                    }
                ]
            },
            "/orders": {
                "orders": [
                    {
                        "m_strOrderSysID": "O1",
                        "m_strInstrumentID": "510050",
                        "m_strExchangeID": "SH",
                        "m_nOrderStatus": 56,
                        "m_nVolume": 1000,
                        "m_nTradedVolume": 1000,
                        "m_strRemark": "sub:sub-a|bt:alpha:abcd1234",
                    }
                ]
            },
            "/trades": {
                "trades": [
                    {
                        "m_strTradeID": "T1",
                        "m_strOrderSysID": "O1",
                        "m_strInstrumentID": "510050",
                        "m_strExchangeID": "SH",
                        "m_nVolume": 1000,
                        "m_dTradePrice": 2.6,
                        "m_strRemark": "sub:sub-a|bt:alpha:abcd1234",
                    }
                ]
            },
            "/order_status": {
                "order": {
                    "m_strOrderSysID": "O1",
                    "m_nOrderStatus": 57,
                }
            },
        }
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    ctx = router.get("default")
    adapter = BigQmtBrokerAdapter(config, router, client)

    account = await adapter.get_account_info(ctx)
    assert account["dtype"] == "dict"
    assert account["available_cash"] == 10000

    positions = await adapter.get_positions(ctx)
    assert positions[0]["security"] == "510050.XSHG"
    assert positions[0]["amount"] == 1000
    assert positions[0]["closeable_amount"] == 800

    orders = await adapter.list_orders(ctx, {"order_id": "O1"})
    assert orders[0]["status"] == "filled"
    assert orders[0]["raw_status"] == 56
    assert orders[0]["security"] == "510050.XSHG"
    assert orders[0]["sub_account_id"] == "sub-a"

    trades = await adapter.list_trades(ctx, {"order_id": "O1"})
    assert trades[0]["trade_id"] == "T1"
    assert trades[0]["security"] == "510050.XSHG"
    assert trades[0]["price"] == 2.6
    assert trades[0]["sub_account_id"] == "sub-a"

    status = await adapter.get_order_status(ctx, "O1")
    assert status["status"] == "rejected"
    assert status["raw_status"] == 57


@pytest.mark.asyncio
async def test_big_qmt_trading_and_cancel_are_disabled_by_default():
    config = _server_config()
    router = AccountRouter(config.accounts)
    ctx = router.get("default")
    adapter = BigQmtBrokerAdapter(config, router, _FakeGatewayClient({}))

    with pytest.raises(BigQmtGatewayError) as place_error:
        await adapter.place_order(ctx, {"security": "000001.XSHE", "amount": 100, "side": "BUY"})
    assert place_error.value.code == "TRADING_DISABLED"

    with pytest.raises(BigQmtGatewayError) as cancel_error:
        await adapter.cancel_order(ctx, "O1")
    assert cancel_error.value.code == "CANCEL_ORDER_DISABLED"


@pytest.mark.asyncio
async def test_big_qmt_trading_and_cancel_forward_account_payload_when_enabled():
    gateway_config = BigQmtGatewayConfig(enable_trading=True, enable_cancel_order=True)
    client = _FakeGatewayClient(
        {
            "/place_order": {"order_id": "O2", "m_nOrderStatus": 50},
            "/cancel_order": {"success": True},
        },
        config=gateway_config,
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    ctx = router.get("default")
    adapter = BigQmtBrokerAdapter(config, router, client)

    order = await adapter.place_order(
        ctx,
        {
            "security": "000001.XSHE",
            "amount": 100,
            "side": "BUY",
            "sub_account_id": "sub-a",
            "order_remark": "bt:alpha:abcd1234",
        },
    )
    cancel = await adapter.cancel_order(ctx, "O2")

    assert order["status"] == "open"
    assert cancel["value"]["value"] is True
    assert client.calls[0][1] == "/place_order"
    assert client.calls[0][2]["account_id"] == "demo"
    assert client.calls[0][2]["sub_account_id"] == "sub-a"
    assert client.calls[0][2]["order_remark"] == "sub:sub-a|bt:alpha:abcd1234"
    assert client.calls[1][1] == "/cancel_order"
    assert client.calls[1][2]["order_id"] == "O2"


@pytest.mark.asyncio
async def test_big_qmt_place_order_confirms_submission_in_adapter():
    gateway_config = BigQmtGatewayConfig(enable_trading=True)
    orders_calls = 0

    def _orders(_payload):
        nonlocal orders_calls
        orders_calls += 1
        if orders_calls == 1:
            return {"orders": []}
        return {
            "orders": [
                {
                    "order_id": "O-confirmed",
                    "security": "000001.XSHE",
                    "amount": 100,
                    "order_price": 1.0,
                    "raw_status": 50,
                    "order_remark": "sub:sub-a|bt:alpha:abcd1234",
                    "sub_account_id": "sub-a",
                }
            ]
        }

    client = _FakeGatewayClient(
        {
            "/place_order": {
                "order_id": "",
                "passorder_return": 0,
                "security": "000001.XSHE",
                "amount": 100,
                "price": 1.0,
                "order_remark": "sub:sub-a|bt:alpha:abcd1234",
                "sub_account_id": "sub-a",
            },
            "/orders": _orders,
        },
        config=gateway_config,
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    ctx = router.get("default")
    adapter = BigQmtBrokerAdapter(config, router, client)

    order = await adapter.place_order(
        ctx,
        {
            "security": "000001.XSHE",
            "amount": 100,
            "side": "BUY",
            "style": {"type": "limit", "price": 1.0},
            "sub_account_id": "sub-a",
            "order_remark": "bt:alpha:abcd1234",
            "wait_timeout": 0.05,
        },
    )

    assert order["order_id"] == "O-confirmed"
    assert order["status"] == "open"
    assert order["timed_out"] is False
    assert client.calls[0][1] == "/orders"
    assert client.calls[0][2]["security"] == "000001.XSHE"
    assert client.calls[1][1] == "/place_order"
    assert client.calls[2][1] == "/orders"
    assert "sub_account_id" not in client.calls[2][2]


@pytest.mark.asyncio
async def test_big_qmt_place_order_skips_known_order_ids_when_confirming():
    gateway_config = BigQmtGatewayConfig(enable_trading=True)
    orders_calls = 0

    def _orders(_payload):
        nonlocal orders_calls
        orders_calls += 1
        old_order = {
            "order_id": "O-old",
            "security": "000001.XSHE",
            "amount": 100,
            "order_price": 10.0,
            "raw_status": 56,
            "order_remark": "sub:sub-a|bt:old",
            "sub_account_id": "sub-a",
            "qmt_user_order_id": "BT-old",
        }
        new_order = {
            "order_id": "O-new",
            "security": "000001.XSHE",
            "amount": 100,
            "order_price": 10.0,
            "raw_status": 56,
            "order_remark": "BT-new",
            "qmt_user_order_id": "BT-new",
        }
        if orders_calls == 1:
            return {"orders": [old_order]}
        return {"orders": [old_order, new_order]}

    client = _FakeGatewayClient(
        {
            "/place_order": {
                "order_id": "",
                "passorder_return": 0,
                "security": "000001.XSHE",
                "amount": 100,
                "price": 10.0,
                "qmt_user_order_id": "BT-new",
                "order_remark": "sub:sub-a|bt:new",
                "sub_account_id": "sub-a",
            },
            "/orders": _orders,
        },
        config=gateway_config,
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    ctx = router.get("default")
    adapter = BigQmtBrokerAdapter(config, router, client)

    order = await adapter.place_order(
        ctx,
        {
            "security": "000001.XSHE",
            "amount": 100,
            "side": "BUY",
            "style": {"type": "limit", "price": 10.0},
            "sub_account_id": "sub-a",
            "order_remark": "bt:new",
            "wait_timeout": 0.05,
        },
    )

    assert order["order_id"] == "O-new"
    assert order["qmt_user_order_id"] == "BT-new"
    assert order["order_remark"] == "sub:sub-a|bt:new"


@pytest.mark.asyncio
async def test_big_qmt_place_order_waits_for_non_empty_order_id():
    gateway_config = BigQmtGatewayConfig(enable_trading=True)
    orders_calls = 0

    def _orders(_payload):
        nonlocal orders_calls
        orders_calls += 1
        if orders_calls == 1:
            return {"orders": []}
        row = {
            "order_id": "" if orders_calls == 2 else "O-ready",
            "security": "000001.XSHE",
            "amount": 100,
            "order_price": 10.0,
            "raw_status": 50,
            "order_remark": "BT-ready",
            "qmt_user_order_id": "BT-ready",
        }
        return {"orders": [row]}

    client = _FakeGatewayClient(
        {
            "/place_order": {
                "order_id": "",
                "passorder_return": 0,
                "security": "000001.XSHE",
                "amount": 100,
                "price": 10.0,
                "qmt_user_order_id": "BT-ready",
            },
            "/orders": _orders,
        },
        config=gateway_config,
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    ctx = router.get("default")
    adapter = BigQmtBrokerAdapter(config, router, client)

    order = await adapter.place_order(
        ctx,
        {
            "security": "000001.XSHE",
            "amount": 100,
            "side": "BUY",
            "style": {"type": "limit", "price": 10.0},
            "wait_timeout": 0.3,
        },
    )

    assert order["order_id"] == "O-ready"
    assert order["timed_out"] is False
    assert orders_calls >= 3


@pytest.mark.asyncio
async def test_big_qmt_place_order_matches_new_order_when_gateway_tag_is_stale():
    gateway_config = BigQmtGatewayConfig(enable_trading=True)
    orders_calls = 0

    def _orders(_payload):
        nonlocal orders_calls
        orders_calls += 1
        old_order = {
            "order_id": "O-old",
            "security": "000001.XSHE",
            "amount": 100,
            "order_price": 1.0,
            "raw_status": 54,
            "order_remark": "sub:stale",
            "sub_account_id": "stale",
        }
        if orders_calls == 1:
            return {"orders": [old_order]}
        return {
            "orders": [
                old_order,
                {
                    "order_id": "",
                    "security": "000001.XSHE",
                    "amount": 100,
                    "order_price": 1.0,
                    "raw_status": 50,
                },
                {
                    "order_id": "O-new",
                    "security": "000001.XSHE",
                    "amount": 100,
                    "order_price": 1.0,
                    "raw_status": 50,
                    "order_remark": "sub:stale",
                    "sub_account_id": "stale",
                },
            ]
        }

    client = _FakeGatewayClient(
        {
            "/place_order": {
                "order_id": "",
                "passorder_return": 0,
                "security": "000001.XSHE",
                "amount": 100,
                "price": 1.0,
            },
            "/orders": _orders,
        },
        config=gateway_config,
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    ctx = router.get("default")
    adapter = BigQmtBrokerAdapter(config, router, client)

    order = await adapter.place_order(
        ctx,
        {
            "security": "000001.XSHE",
            "amount": 100,
            "side": "BUY",
            "style": {"type": "limit", "price": 1.0},
            "sub_account_id": "sim_a",
            "order_remark": "bt:test",
            "wait_timeout": 0.05,
        },
    )
    filtered_orders = await adapter.list_orders(ctx, {"sub_account_id": "sim_a"})

    assert order["order_id"] == "O-new"
    assert order["sub_account_id"] == "sim_a"
    assert order["order_remark"] == "sub:sim_a|bt:test"
    assert filtered_orders[-1]["order_id"] == "O-new"
    assert filtered_orders[-1]["sub_account_id"] == "sim_a"


@pytest.mark.asyncio
async def test_big_qmt_place_order_returns_submit_unknown_when_not_visible():
    gateway_config = BigQmtGatewayConfig(enable_trading=True)
    client = _FakeGatewayClient(
        {
            "/place_order": {
                "order_id": "",
                "passorder_return": 0,
                "security": "000001.XSHE",
                "amount": 100,
                "price": 1.0,
            },
            "/orders": {"orders": []},
        },
        config=gateway_config,
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    ctx = router.get("default")
    adapter = BigQmtBrokerAdapter(config, router, client)

    order = await adapter.place_order(
        ctx,
        {
            "security": "000001.XSHE",
            "amount": 100,
            "side": "BUY",
            "style": {"type": "limit", "price": 1.0},
            "wait_timeout": 0.01,
        },
    )

    assert order["status"] == "submit_unknown"
    assert order["timed_out"] is True
    assert order["async_tracking"] is True
    assert "no matching order" in order["warning"]
