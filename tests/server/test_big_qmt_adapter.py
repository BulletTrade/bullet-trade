import asyncio
import time
from pathlib import Path

import pytest

from bullet_trade.server.adapters import get_adapter
from bullet_trade.server.adapters.base import AccountRouter, AdapterBundle
from bullet_trade.server.adapters.big_qmt import (
    BigQmtBrokerAdapter,
    BigQmtGatewayClient,
    BigQmtDataAdapter,
    BigQmtGatewayConfig,
    BigQmtGatewayError,
    _normalize_position,
    build_big_qmt_bundle,
)
from bullet_trade.server.app import ServerApplication
from bullet_trade.server.config import AccountConfig, ServerConfig


def test_big_qmt_server_env_example_contains_account_route_and_timeout_order():
    path = Path(__file__).resolve().parents[2] / "env.bigqmt.example"
    values = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()

    assert values["QMT_SERVER_TYPE"] == "big_qmt"
    assert values["QMT_SERVER_ACCOUNTS"].startswith("default=")
    assert int(values["QMT_SERVER_REQUEST_TIMEOUT_SECONDS"]) > int(
        values["BIG_QMT_GATEWAY_TIMEOUT_SECONDS"]
    )


def test_big_qmt_gateway_error_preserves_broker_called_false() -> None:
    """Gateway 明确未调用 passorder 时必须把该事实保留在异常中。

    Returns:
        None: 断言 Server 会话可继续把未提交事实传给 V2 客户端。
    """

    client = BigQmtGatewayClient(BigQmtGatewayConfig())

    with pytest.raises(BigQmtGatewayError) as raised:
        client._unwrap_response(
            {
                "ok": False,
                "code": "LOCAL_REJECTED",
                "message": "rejected before passorder",
                "broker_called": False,
            }
        )

    assert raised.value.code == "LOCAL_REJECTED"
    assert raised.value.broker_called is False


def test_big_qmt_gateway_stable_pre_passorder_error_implies_not_called() -> None:
    """旧 helper 未回传边界字段时，稳定调用前错误码仍必须安全释放。

    Returns:
        None: 断言 QMT_NOT_READY 被解释为尚未调用 passorder。
    """

    client = BigQmtGatewayClient(BigQmtGatewayConfig())

    with pytest.raises(BigQmtGatewayError) as raised:
        client._unwrap_response(
            {
                "ok": False,
                "code": "QMT_NOT_READY",
                "message": "ContextInfo is required before trading",
            }
        )

    assert raised.value.broker_called is False


class _FakeGatewayClient:
    def __init__(self, responses, config=None):
        self.responses = responses
        self.calls = []
        self.config = config or BigQmtGatewayConfig()
        self.last_health = None

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
        self.calls.append(("GET", "/health", {}))
        value = self.responses.get("/health", {"ready": True})
        if isinstance(value, Exception):
            raise value
        self.last_health = value
        return value

    def qmt_status(self):
        health = self.last_health or {}
        ready = health.get("ready")
        return {
            "backend_type": "big_qmt",
            "ready": ready,
            "state": "ready" if ready else "unknown",
            "big_qmt_gateway": health,
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
    assert health["qmt"]["actions"]["broker.place_order"]["status"] == "ready"
    assert health["qmt"]["actions"]["broker.cancel_order"]["status"] == "ready"
    assert health["idempotency"]["mode"] == "process_memory"
    assert health["idempotency"]["cross_restart_exactly_once"] is False


def test_big_qmt_health_cache_expires_instead_of_staying_green(monkeypatch):
    """验证 sidecar health 缓存过期后状态变为 unknown，而不是继续假绿。

    Args:
        monkeypatch: pytest 时间替换工具。

    Returns:
        None。
    """

    from bullet_trade.server.adapters.big_qmt import BigQmtGatewayClient

    client = BigQmtGatewayClient(BigQmtGatewayConfig(health_ttl_seconds=15.0))
    client._last_health = {"ready": True}
    client._last_health_at = 100.0
    monkeypatch.setattr(time, "time", lambda: 116.0)

    status = client.qmt_status()

    assert status["ready"] is None
    assert status["state"] == "unknown"
    assert status["health_cache_expired"] is True


@pytest.mark.asyncio
async def test_admin_health_refreshes_big_qmt_gateway_before_snapshot():
    """验证 admin.health 返回前主动刷新 helper，而不是永久读取启动缓存。

    Returns:
        None: 断言实时 health 已进入 sidecar 快照，且只发起一次只读刷新。
    """

    client = _FakeGatewayClient(
        {
            "/health": {
                "ready": True,
                "http_alive": True,
                "qmt_api_ready": True,
                "direct_dispatch": True,
            }
        }
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    broker = BigQmtBrokerAdapter(config, router, client)
    data = BigQmtDataAdapter(client)
    app = ServerApplication(
        config,
        router,
        AdapterBundle(data_adapter=data, broker_adapter=broker),
    )

    health = await app.handle_request(None, "admin.health", {})

    assert client.calls == [("GET", "/health", {})]
    assert health["value"]["qmt"]["ready"] is True
    assert health["value"]["qmt"]["state"] == "ready"
    assert health["value"]["big_qmt_gateway"]["qmt_api_ready"] is True


@pytest.mark.asyncio
async def test_big_qmt_data_adapter_normalizes_gateway_payloads():
    client = _FakeGatewayClient(
        {
            "/data/history": {"records": [{"open": 1.0, "close": 2.0}]},
            "/data/snapshot": {
                "ticks": {
                    "000001.XSHE": {"lastPrice": 12.3, "time": 1783043331000, "bidPrice": [12.2]}
                }
            },
            "/data/live_current": {
                "ticks": {
                    "000001.XSHE": {
                        "lastPrice": 12.5,
                        "high_limit": 13.75,
                        "low_limit": 11.25,
                        "openInt": 13,
                        "bidPrice": [12.4],
                        "askPrice": [12.6],
                        "source": "big_qmt_full_tick",
                        "source_time": "2026-07-03T09:30:00+08:00",
                        "received_time": "2026-07-03T09:30:01+08:00",
                        "query_completed_time": "2026-07-03T09:30:01+08:00",
                        "feed_health": {"status": "healthy", "query_succeeded": True},
                    }
                }
            },
            "/data/current_tick": {
                "ticks": {"000001.XSHE": {"lastPrice": 12.4, "timetag": "20260703 09:30:00"}}
            },
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
    assert snapshot["sid"] == "000001.XSHE"
    assert snapshot["last_price"] == 12.3
    assert snapshot["dt"] == 1783043331000

    live_current = await adapter.get_live_current({"security": "000001.XSHE"})
    assert live_current["last_price"] == 12.5
    assert live_current["high_limit"] == 13.75
    assert live_current["low_limit"] == 11.25
    assert live_current["paused"] is False
    assert live_current["security"] == "000001.XSHE"
    assert live_current["source"] == "big_qmt_full_tick"
    assert live_current["source_time"] == "2026-07-03T09:30:00+08:00"
    assert live_current["query_completed_time"] == "2026-07-03T09:30:01+08:00"
    assert live_current["feed_health"]["status"] == "healthy"
    assert live_current["bid_price1"] == pytest.approx(12.4)
    assert live_current["ask_price1"] == pytest.approx(12.6)

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
            "/data/snapshot": {
                "ticks": {"000001.XSHE": {"lastPrice": 12.3, "time": 1783043331000}}
            },
        }
    )
    config = _server_config(enable_broker=False)
    router = AccountRouter(config.accounts)
    adapter = BigQmtDataAdapter(client)
    app = ServerApplication(
        config, router, AdapterBundle(data_adapter=adapter, broker_adapter=None)
    )

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
                        "m_dAvgOpenPrice": 2.5,
                        "m_dOpenPrice": -10.0,
                        "m_dLastPrice": 2.6,
                        "m_dMarketValue": 2600.0,
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
    assert positions[0]["avg_cost"] == 2.5
    assert positions[0]["cost_basis"] == 2.5
    assert positions[0]["current_price"] == 2.6
    assert positions[0]["market_value"] == 2600.0

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
async def test_big_qmt_trading_and_cancel_forward_by_default():
    client = _FakeGatewayClient(
        {
            "/place_order": {"order_id": "O-default", "m_nOrderStatus": 50},
            "/cancel_order": {"success": True},
        },
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    ctx = router.get("default")
    adapter = BigQmtBrokerAdapter(config, router, client)

    order = await adapter.place_order(
        ctx, {"security": "000001.XSHE", "amount": 100, "side": "BUY"}
    )
    cancel = await adapter.cancel_order(ctx, "O-default")

    assert order["status"] == "submit_unknown"
    assert cancel["value"]["success"] is True
    assert client.calls[0][1] == "/place_order"
    assert client.calls[1][1] == "/cancel_order"


@pytest.mark.asyncio
async def test_big_qmt_trading_and_cancel_forward_account_payload_when_enabled():
    client = _FakeGatewayClient(
        {
            "/place_order": {"order_id": "O2", "m_nOrderStatus": 50},
            "/cancel_order": {"success": True},
        },
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

    assert order["status"] == "submit_unknown"
    assert cancel["value"]["value"] is True
    assert client.calls[0][1] == "/place_order"
    assert client.calls[0][2]["account_id"] == "demo"
    assert client.calls[0][2]["sub_account_id"] == "sub-a"
    assert client.calls[0][2]["order_remark"] == "sub:sub-a|bt:alpha:abcd1234"
    assert client.calls[1][1] == "/cancel_order"
    assert client.calls[1][2]["order_id"] == "O2"


@pytest.mark.asyncio
async def test_big_qmt_server_account_identity_cannot_be_overridden_by_client_payload():
    """验证客户端 payload 不能覆盖 sidecar 已解析的实体账号身份。

    Returns:
        None。
    """

    client = _FakeGatewayClient(
        {
            "/place_order": {"order_id": "O-authoritative", "m_nOrderStatus": 50},
            "/orders": {"orders": []},
            "/trades": {"trades": []},
        },
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    ctx = router.get("default")
    adapter = BigQmtBrokerAdapter(config, router, client)

    await adapter.place_order(
        ctx,
        {
            "security": "000001.XSHE",
            "amount": 100,
            "side": "BUY",
            "account_key": "spoofed",
            "account_id": "spoofed-account",
            "account_type": "spoofed-type",
        },
    )

    submitted = client.calls[0][2]
    assert submitted["account_key"] == "default"
    assert submitted["account_id"] == "demo"
    assert submitted["account_type"] == "stock"

    spoofed_filters = {
        "account_key": "spoofed",
        "account_id": "spoofed-account",
        "account_type": "spoofed-type",
    }
    await adapter.list_orders(ctx, spoofed_filters)
    await adapter.list_trades(ctx, spoofed_filters)
    for _, _, query in client.calls[1:]:
        assert query["account_key"] == "default"
        assert query["account_id"] == "demo"
        assert query["account_type"] == "stock"


def test_big_qmt_position_zero_average_cost_falls_back_to_positive_open_price():
    """验证零平均成本不遮蔽后续有效的正开仓价。

    Returns:
        None。
    """

    result = _normalize_position(
        {
            "security": "510050.XSHG",
            "avg_cost": 0.0,
            "m_dOpenPrice": 2.5,
        }
    )

    assert result["avg_cost"] == 2.5
    assert result["cost_basis"] == 2.5


@pytest.mark.asyncio
async def test_big_qmt_place_order_confirms_submission_in_adapter():
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
                    "side": "BUY",
                    "amount": 100,
                    "order_price": 1.0,
                    "raw_status": 50,
                    "order_remark": "sub:sub-a|bt:alpha:abcd1234",
                    "sub_account_id": "sub-a",
                    "order_time": time.time(),
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
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    ctx = router.get("default")
    adapter = BigQmtBrokerAdapter(config, router, client)

    order = await adapter.place_order(
        ctx,
        {
            "security": "000001.XSHE",
            "side": "BUY",
            "amount": 100,
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
            "side": "BUY",
            "order_time": time.time() - 30,
        }
        new_order = {
            "order_id": "O-new",
            "security": "000001.XSHE",
            "amount": 100,
            "order_price": 10.0,
            "raw_status": 56,
            "order_remark": "sub:sub-a|bt:new",
            "qmt_user_order_id": "BT-new",
            "side": "BUY",
            "order_time": time.time(),
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
            "qmt_user_order_id": "BT-new",
            "wait_timeout": 0.05,
        },
    )

    assert order["order_id"] == "O-new"
    assert order["qmt_user_order_id"] == "BT-new"
    assert order["order_remark"] == "sub:sub-a|bt:new"


@pytest.mark.asyncio
async def test_big_qmt_place_order_waits_for_non_empty_order_id():
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
            "side": "BUY",
            "order_time": time.time(),
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
            "qmt_user_order_id": "BT-ready",
            "wait_timeout": 0.3,
        },
    )

    assert order["order_id"] == "O-ready"
    assert order["timed_out"] is False
    assert orders_calls >= 3


@pytest.mark.asyncio
async def test_big_qmt_place_order_does_not_match_new_order_when_gateway_tag_is_stale():
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

    assert order["status"] == "submit_unknown"


@pytest.mark.asyncio
async def test_big_qmt_place_order_returns_submit_unknown_when_not_visible():
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


@pytest.mark.asyncio
async def test_big_qmt_confirmation_rejects_reverse_order_with_same_economic_fields():
    """同标的/数量/价格的 SELL 不能确认本次 BUY，必须保持 submit_unknown。"""

    calls = 0
    submission_identity = {"value": ""}

    def _place(payload):
        submission_identity["value"] = payload["qmt_user_order_id"]
        return {"order_id": "", "passorder_return": 0}

    def _orders(_payload):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {"orders": []}
        return {
            "orders": [
                {
                    "order_id": "reverse-sell",
                    "security": "511880.XSHG",
                    "side": "SELL",
                    "amount": 100,
                    "order_price": 100.0,
                    "qmt_user_order_id": submission_identity["value"],
                    "order_time": time.time(),
                    "raw_status": 50,
                }
            ]
        }

    client = _FakeGatewayClient({"/place_order": _place, "/orders": _orders})
    config = _server_config()
    router = AccountRouter(config.accounts)
    adapter = BigQmtBrokerAdapter(config, router, client)

    order = await adapter.place_order(
        router.get("default"),
        {
            "security": "511880.XSHG",
            "side": "BUY",
            "amount": 100,
            "style": {"type": "limit", "price": 100.0},
            "idempotency_key": "reverse-direction-key",
            "wait_timeout": 0.01,
        },
    )

    assert order["status"] == "submit_unknown"
    assert submission_identity["value"].startswith("BT-")


@pytest.mark.asyncio
async def test_big_qmt_place_order_confirms_live_gateway_raw_order_shape():
    """验证真机 raw 方向、时间和强订单标识足以确认实际订单号。

    Returns:
        None。
    """

    calls = 0
    submission_identity = {"value": ""}

    def _place(payload):
        """记录 sidecar 实际收到的强订单标识。

        Args:
            payload: adapter 发给 BigQMT helper 的下单载荷。

        Returns:
            dict: 模拟 passorder 未同步返回订单号的响应。
        """

        submission_identity["value"] = payload["qmt_user_order_id"]
        return {"order_id": "", "passorder_return": 0}

    def _orders(_payload):
        """先返回下单前快照，再返回真机字段形态的新订单。

        Args:
            _payload: 订单查询过滤条件，本测试不使用。

        Returns:
            dict: helper 的订单列表响应。
        """

        nonlocal calls
        calls += 1
        if calls == 1:
            return {"orders": []}
        return {
            "orders": [
                {
                    "order_id": "5236",
                    "security": "511880.XSHG",
                    "amount": 100,
                    "filled": 100,
                    "order_price": 110.836,
                    "raw_status": 56,
                    "order_remark": submission_identity["value"],
                    "qmt_user_order_id": submission_identity["value"],
                    "raw": {
                        "m_nOpType": 23,
                        "m_strInsertDate": time.strftime("%Y%m%d"),
                        "m_strInsertTime": time.strftime("%H%M%S"),
                        "m_strRemark": submission_identity["value"],
                    },
                }
            ]
        }

    client = _FakeGatewayClient({"/place_order": _place, "/orders": _orders})
    config = _server_config()
    router = AccountRouter(config.accounts)
    adapter = BigQmtBrokerAdapter(config, router, client)

    order = await adapter.place_order(
        router.get("default"),
        {
            "security": "511880.XSHG",
            "side": "BUY",
            "amount": 100,
            "style": {"type": "market", "protect_price": 120.0},
            "market_type": "opponent_best",
            "order_remark": "strategy-business-remark",
            "idempotency_key": "live-shape-confirmation",
            "wait_timeout": 0.05,
        },
    )

    assert order["order_id"] == "5236"
    assert order["status"] == "filled"
    assert order["side"] == "BUY"
    assert client.calls[1][2]["pr_type"] == 44


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("security", "market_type", "expected_pr_type"),
    [
        ("511880.XSHG", None, 44),
        ("511880.XSHG", "opponent_best", 44),
        ("511880.XSHG", "home_best", 45),
        ("511880.XSHG", "five_level_ioc", 42),
        ("511880.XSHG", "five_level_to_limit", 43),
        ("159001.XSHE", None, 44),
        ("159001.XSHE", "opponent_best", 44),
        ("159001.XSHE", "home_best", 45),
        ("159001.XSHE", "immediate_or_cancel", 46),
        ("159001.XSHE", "five_level_ioc", 47),
        ("159001.XSHE", "fill_or_kill", 48),
    ],
)
async def test_big_qmt_maps_canonical_market_type_to_native_pr_type(
    security, market_type, expected_pr_type
):
    """验证沪深公共市价类型映射为已真机验证的 BigQMT pr_type。

    Args:
        security: 带交易所后缀的证券代码。
        market_type: 公共 canonical 市价类型。
        expected_pr_type: BigQMT 原生价格类型。

    Returns:
        None。
    """

    client = _FakeGatewayClient({"/place_order": {"order_id": "", "passorder_return": 0}})
    config = _server_config()
    router = AccountRouter(config.accounts)
    adapter = BigQmtBrokerAdapter(config, router, client)

    await adapter.place_order(
        router.get("default"),
        {
            "security": security,
            "side": "BUY",
            "amount": 100,
            "style": {"type": "market", "protect_price": 100.0},
            **({"market_type": market_type} if market_type else {}),
        },
    )

    assert client.calls[0][2]["pr_type"] == expected_pr_type


@pytest.mark.asyncio
async def test_big_qmt_preserves_explicit_native_pr_type():
    """验证调用方显式传入的原生 pr_type 不会被公共映射覆盖。

    Returns:
        None。
    """

    client = _FakeGatewayClient({"/place_order": {"order_id": "", "passorder_return": 0}})
    config = _server_config()
    router = AccountRouter(config.accounts)
    adapter = BigQmtBrokerAdapter(config, router, client)

    await adapter.place_order(
        router.get("default"),
        {
            "security": "511880.XSHG",
            "side": "BUY",
            "amount": 100,
            "style": {"type": "market", "protect_price": 100.0},
            "market_type": "opponent_best",
            "pr_type": 5,
        },
    )

    assert client.calls[0][2]["pr_type"] == 5


@pytest.mark.asyncio
async def test_big_qmt_cancel_request_confirms_exact_order_terminal_status():
    """验证撤单只发送一次，并以精确订单号的已撤状态收口。

    Returns:
        None。
    """

    client = _FakeGatewayClient(
        {
            "/cancel_order": {"order_id": "6128", "success": True},
            "/order_status": {
                "order_id": "6128",
                "security": "511880.XSHG",
                "raw_status": 54,
                "amount": 100,
                "filled": 0,
            },
        }
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    adapter = BigQmtBrokerAdapter(config, router, client)

    result = await adapter.cancel_order_request(
        router.get("default"),
        {"order_id": "6128", "idempotency_key": "cancel-exact-6128"},
    )

    assert result["order_id"] == "6128"
    assert result["status"] == "partly_canceled"
    assert result["value"] is True
    assert result["last_snapshot"]["raw_status"] == 54
    assert [path for _, path, _ in client.calls].count("/cancel_order") == 1
