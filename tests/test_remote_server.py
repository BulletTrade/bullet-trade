import asyncio
import threading
import time

import pandas as pd
import pytest

from bullet_trade.broker import RemoteQmtBroker
from bullet_trade.remote import RemoteQmtConnection
from bullet_trade.server.adapters import register_adapter
from bullet_trade.server.adapters.stub import build_stub_bundle  # noqa: F401
from bullet_trade.server.adapters.base import AccountRouter
from bullet_trade.server.app import ServerApplication
from bullet_trade.server.config import AccountConfig, ServerConfig

"""
这些测试使用 stub server 验证 RemoteQmtConnection/RemoteQmtBroker 的端到端行为。
若要连接真实的远程 qmt server，可在 bullet-trade/.env 中设置：

QMT_SERVER_HOST=远程 IP 或域名
QMT_SERVER_PORT=58620
QMT_SERVER_TOKEN=服务端 token
QMT_SERVER_ACCOUNT_KEY=main
QMT_SERVER_SUB_ACCOUNT=demo@main
QMT_SERVER_TLS_CERT=/path/to/ca.pem  # 如启用了 TLS

并根据需要补充 DEFAULT_DATA_PROVIDER/DEFAULT_BROKER=qmt-remote。
"""


def _ensure_current_event_loop() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


@pytest.fixture(scope="module")
def stub_server():
    port = 59321
    config = ServerConfig(
        server_type="stub",
        listen="127.0.0.1",
        port=port,
        token="stub-token",
        enable_data=True,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    _ensure_current_event_loop()
    router = AccountRouter(config.accounts)
    # 注册 stub builder（import 时已经执行）
    bundle = build_stub_bundle(config, router)
    app = ServerApplication(config, router, bundle)
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=_run_loop, args=(loop, app), daemon=True)
    thread.start()
    asyncio.run_coroutine_threadsafe(app.wait_started(), loop).result(timeout=5)
    yield config
    asyncio.run_coroutine_threadsafe(app.shutdown(), loop).result(timeout=5)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)


def _run_loop(loop: asyncio.AbstractEventLoop, app: ServerApplication) -> None:
    asyncio.set_event_loop(loop)
    loop.create_task(app.start())
    loop.run_forever()


def _make_connection(cfg: ServerConfig) -> RemoteQmtConnection:
    conn = RemoteQmtConnection(cfg.listen, cfg.port, cfg.token)
    conn.start()
    return conn


def test_stub_server_history(stub_server):
    conn = _make_connection(stub_server)
    try:
        resp = conn.request("data.history", {"security": "000001.XSHE"})
        assert resp["dtype"] == "dataframe"
    finally:
        conn.close()


def test_stub_server_security_info_compat(stub_server):
    conn = _make_connection(stub_server)
    try:
        resp = conn.request("data.security_info", {"security": "000001.XSHE"})
        assert resp["value"]["display_name"] == "Stub 000001.XSHE"
        assert resp["display_name"] == "Stub 000001.XSHE"
        assert resp["type"] == "stock"
    finally:
        conn.close()


def test_stub_server_order_flow(stub_server):
    conn = _make_connection(stub_server)
    try:
        order = conn.request(
            "broker.place_order",
            {"security": "000001.XSHE", "side": "BUY", "amount": 100, "style": {"type": "limit", "price": 10.0}},
        )
        assert order["order_id"].startswith("stub-")
        orders = conn.request("broker.orders", {})
        assert len(orders) == 1
        cancel = conn.request("broker.cancel_order", {"order_id": order["order_id"]})
        assert cancel.get("value") is True
    finally:
        conn.close()


def test_stub_server_filled_scenario_updates_trades_positions_and_account():
    config = ServerConfig(
        server_type="stub",
        listen="127.0.0.1",
        port=59323,
        token="stub-token",
        enable_data=True,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    _ensure_current_event_loop()
    router = AccountRouter(config.accounts)
    bundle = build_stub_bundle(config, router)
    app = ServerApplication(config, router, bundle)
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=_run_loop, args=(loop, app), daemon=True)
    thread.start()
    asyncio.run_coroutine_threadsafe(app.wait_started(), loop).result(timeout=5)
    conn = _make_connection(config)
    try:
        order = conn.request(
            "broker.place_order",
            {
                "security": "510050.XSHG",
                "side": "BUY",
                "amount": 33800,
                "style": {"type": "limit", "price": 2.951},
                "stub_scenario": {
                    "status": "filled",
                    "filled": 33800,
                    "traded_price": 2.906,
                    "commission_fee": 0.0,
                },
            },
        )
        assert order["status"] == "filled"
        assert order["traded_price"] == pytest.approx(2.906)
        trades = conn.request("broker.trades", {"order_id": order["order_id"]})
        assert len(trades) == 1
        assert trades[0]["price"] == pytest.approx(2.906)
        positions = conn.request("broker.positions", {})
        assert positions[0]["security"] == "510050.XSHG"
        assert positions[0]["amount"] == 33800
        assert positions[0]["available_amount"] == 0
        account = conn.request("broker.account", {})
        assert account["value"]["available_cash"] == pytest.approx(1000000 - (33800 * 2.906))
    finally:
        conn.close()
        asyncio.run_coroutine_threadsafe(app.shutdown(), loop).result(timeout=5)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)


def test_stub_server_cancel_risk_controls(monkeypatch):
    monkeypatch.setenv("MAX_DAILY_CANCELS", "1")
    monkeypatch.setenv("MIN_CANCEL_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("MAX_CANCEL_PER_ORDER", "1")

    config = ServerConfig(
        server_type="stub",
        listen="127.0.0.1",
        port=59322,
        token="stub-token",
        enable_data=True,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
        order_risk_enabled=True,
    )
    _ensure_current_event_loop()
    router = AccountRouter(config.accounts)
    bundle = build_stub_bundle(config, router)
    app = ServerApplication(config, router, bundle)
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=_run_loop, args=(loop, app), daemon=True)
    thread.start()
    asyncio.run_coroutine_threadsafe(app.wait_started(), loop).result(timeout=5)
    conn = _make_connection(config)
    try:
        order = conn.request(
            "broker.place_order",
            {"security": "000001.XSHE", "side": "BUY", "amount": 100, "style": {"type": "limit", "price": 10.0}},
        )
        first = conn.request("broker.cancel_order", {"order_id": order["order_id"]})
        assert first.get("value") is True
        with pytest.raises(RuntimeError, match="当日撤单次数超限"):
            conn.request("broker.cancel_order", {"order_id": order["order_id"]})
    finally:
        conn.close()
        asyncio.run_coroutine_threadsafe(app.shutdown(), loop).result(timeout=5)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)


def test_stub_adapter_open_buy_terminal_execution_releases_reserved_cash_exactly():
    _ensure_current_event_loop()
    config = ServerConfig(
        server_type="stub",
        listen="127.0.0.1",
        port=59331,
        token="stub-token",
        enable_data=True,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    bundle = build_stub_bundle(config, router)
    adapter = bundle.broker_adapter
    account = router.get("default")
    state = adapter._account_state_for(account)
    state["available_cash"] = 50000.0
    state["transferable_cash"] = 50000.0
    state["frozen_cash"] = 0.0
    order = asyncio.get_event_loop().run_until_complete(
        adapter.place_order(
            account,
            {
                "security": "511880.XSHG",
                "side": "BUY",
                "amount": 100,
                "style": {"type": "limit", "price": 10.5},
                "stub_scenario": {"status": "open", "reserve_on_open": True},
            },
        )
    )
    assert state["available_cash"] == pytest.approx(48950.0)
    assert state["frozen_cash"] == pytest.approx(1050.0)

    adapter._apply_terminal_execution(
        account=account,
        order=order,
        status="filled",
        filled=100,
        traded_price=10.1,
        commission=0.05,
        tax=0.0,
    )

    assert state["available_cash"] == pytest.approx(48989.95)
    assert state["frozen_cash"] == pytest.approx(0.0)
    positions = adapter._positions_for(account)
    assert positions["511880.XSHG"]["amount"] == 100
    assert positions["511880.XSHG"]["available_amount"] == 0


def test_stub_adapter_open_sell_terminal_execution_releases_remaining_volume_exactly():
    _ensure_current_event_loop()
    config = ServerConfig(
        server_type="stub",
        listen="127.0.0.1",
        port=59332,
        token="stub-token",
        enable_data=True,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    bundle = build_stub_bundle(config, router)
    adapter = bundle.broker_adapter
    account = router.get("default")
    state = adapter._account_state_for(account)
    state["available_cash"] = 50000.0
    state["transferable_cash"] = 50000.0
    positions = adapter._positions_for(account)
    positions["159915.XSHE"] = {
        "security": "159915.XSHE",
        "amount": 100,
        "available_amount": 100,
        "closeable_amount": 100,
        "can_use_volume": 100,
        "frozen_volume": 0,
        "avg_cost": 10.1,
        "last_price": 10.1,
        "current_price": 10.1,
    }
    order = asyncio.get_event_loop().run_until_complete(
        adapter.place_order(
            account,
            {
                "security": "159915.XSHE",
                "side": "SELL",
                "amount": 100,
                "style": {"type": "limit", "price": 10.9},
                "stub_scenario": {"status": "open", "reserve_on_open": True},
            },
        )
    )
    assert positions["159915.XSHE"]["available_amount"] == 0
    assert positions["159915.XSHE"]["frozen_volume"] == 100

    adapter._apply_terminal_execution(
        account=account,
        order=order,
        status="partly_canceled",
        filled=40,
        traded_price=10.8,
        commission=0.03,
        tax=0.0,
    )

    assert state["available_cash"] == pytest.approx(50431.97)
    assert positions["159915.XSHE"]["amount"] == 60
    assert positions["159915.XSHE"]["available_amount"] == 60
    assert positions["159915.XSHE"]["frozen_volume"] == 0


def test_stub_server_place_order_idempotency(stub_server):
    conn = _make_connection(stub_server)
    try:
        before = conn.request("broker.orders", {})
        payload = {
            "security": "000001.XSHE",
            "side": "BUY",
            "amount": 100,
            "style": {"type": "limit", "price": 10.0},
            "idempotency_key": "idem-order-1",
        }
        first = conn.request("broker.place_order", payload)
        second = conn.request("broker.place_order", payload)
        assert first["order_id"] == second["order_id"]
        orders = conn.request("broker.orders", {})
        assert len(orders) == len(before) + 1
    finally:
        conn.close()


def test_remote_data_provider_dataframe_conversion():
    from bullet_trade.data.providers.remote_qmt import _dataframe_from_payload

    payload = {"dtype": "dataframe", "columns": ["a"], "records": [[1], [2]]}
    df = _dataframe_from_payload(payload)
    assert isinstance(df, pd.DataFrame)
    assert list(df["a"]) == [1, 2]


def test_remote_data_provider_security_info_supports_flat_response():
    from bullet_trade.data.providers.remote_qmt import RemoteQmtProvider

    provider = object.__new__(RemoteQmtProvider)

    class _FakeConnection:
        def request(self, action, payload):
            assert action == "data.security_info"
            return {
                "display_name": "黄金ETF",
                "name": "518880",
                "type": "etf",
            }

    provider._connection = _FakeConnection()
    info = provider.get_security_info("518880.XSHG")
    assert info["display_name"] == "黄金ETF"
    assert info["type"] == "etf"


@pytest.mark.asyncio
async def test_remote_qmt_broker_full_flow(stub_server):
    account_key = stub_server.accounts[0].key if stub_server.accounts else "default"
    broker = RemoteQmtBroker(
        account_id="demo",
        config={
            "host": stub_server.listen,
            "port": stub_server.port,
            "token": stub_server.token,
            "account_key": account_key,
        },
    )
    try:
        assert broker.connect()
        account_info = broker.get_account_info()
        assert account_info["available_cash"] == 1_000_000
        assert broker.get_positions() == []

        limit_order_id, market_order_id = await asyncio.gather(
            broker.buy("000001.XSHE", 100, price=10.5),
            broker.sell("000002.XSHE", 200, price=None),
        )
        assert limit_order_id.startswith("stub-")
        assert market_order_id.startswith("stub-")

        limit_status = await broker.get_order_status(limit_order_id)
        assert limit_status["order_id"] == limit_order_id
        assert limit_status["status"] in {"submitted", "open", "cancelled"}

        snapshot = broker.sync_orders()
        snapshot_ids = {item["order_id"] for item in snapshot}
        assert {limit_order_id, market_order_id}.issubset(snapshot_ids)

        assert await broker.cancel_order(limit_order_id) is True
        updated_status = await broker.get_order_status(limit_order_id)
        assert updated_status["status"] == "cancelled"
    finally:
        broker.disconnect()


def test_remote_qmt_broker_sync_account_contains_positions(stub_server):
    account_key = stub_server.accounts[0].key if stub_server.accounts else "default"
    broker = RemoteQmtBroker(
        account_id="demo",
        config={
            "host": stub_server.listen,
            "port": stub_server.port,
            "token": stub_server.token,
            "account_key": account_key,
        },
    )
    try:
        assert broker.connect() is True
        snapshot = broker.sync_account()
        assert "available_cash" in snapshot
        assert "positions" in snapshot
        assert isinstance(snapshot["positions"], list)
    finally:
        broker.disconnect()
