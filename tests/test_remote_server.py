import asyncio
import concurrent.futures
import threading
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.broker import RemoteQmtBroker
from bullet_trade.remote import RemoteQmtConnection
from bullet_trade.server.adapters.base import AccountRouter
from bullet_trade.server.adapters.stub import build_stub_bundle  # noqa: F401
from bullet_trade.server.app import ServerApplication
from bullet_trade.server.config import AccountConfig, ServerConfig
from bullet_trade.server.session import ClientSession

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
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    if loop.is_closed():
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


def test_remote_connection_request_cancels_pending_future_on_timeout(monkeypatch):
    """同步 request 超时时应取消后台协程，避免长连接 pending 状态残留。"""

    conn = RemoteQmtConnection("127.0.0.1", 0, "token")
    conn._loop = object()  # type: ignore[assignment]
    conn._connected.set()
    background_future: concurrent.futures.Future = concurrent.futures.Future()

    def _run_coroutine_threadsafe(coro, _loop):
        """模拟提交到后台事件循环但永远不返回的 request coroutine。"""

        coro.close()
        return background_future

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", _run_coroutine_threadsafe)

    with pytest.raises(TimeoutError):
        conn.request("broker.place_order", {}, timeout=0.001)

    assert background_future.cancelled()


def test_remote_connection_default_timeout_applies_while_reconnecting(monkeypatch):
    """省略 timeout 时也应使用默认保护窗口，不能在重连状态无限等待。"""

    conn = RemoteQmtConnection("127.0.0.1", 0, "token", request_timeout=60)
    conn.request_timeout = 0.001
    conn._loop = object()  # type: ignore[assignment]
    background_future: concurrent.futures.Future = concurrent.futures.Future()

    def _run_coroutine_threadsafe(coro, _loop):
        """模拟后台协程还在等待连接恢复。"""

        coro.close()
        return background_future

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", _run_coroutine_threadsafe)

    with pytest.raises(TimeoutError):
        conn.request("broker.place_order", {})

    assert background_future.cancelled()


def test_remote_connection_explicit_none_timeout_keeps_legacy_wait(monkeypatch):
    """显式 timeout=None 时保留旧版无限等待语义，省略参数才用默认保护窗口。"""

    class _RecordedFuture:
        def __init__(self):
            self.timeouts = []

        def result(self, timeout=None):
            self.timeouts.append(timeout)
            return {"ok": True}

        def cancel(self):
            return False

    conn = RemoteQmtConnection("127.0.0.1", 0, "token", request_timeout=60)
    conn._loop = object()  # type: ignore[assignment]
    recorded_future = _RecordedFuture()

    def _run_coroutine_threadsafe(coro, _loop):
        """记录 request 传给 Future.result 的 timeout 参数。"""

        coro.close()
        return recorded_future

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", _run_coroutine_threadsafe)

    assert conn.request("broker.account", {}) == {"ok": True}
    assert conn.request("broker.account", {}, timeout=None) == {"ok": True}

    assert recorded_future.timeouts == [60, None]


def test_server_session_extends_place_order_timeout_for_long_wait():
    """broker.place_order 长等待窗口应同步扩展 session 外层请求超时。"""

    session = ClientSession.__new__(ClientSession)

    assert session._request_timeout_for("broker.account", {}) == 60.0
    assert session._request_timeout_for("broker.place_order", {"wait_timeout": 16}) == 60.0
    assert session._request_timeout_for("broker.place_order", {"wait_timeout": 90}) == 120.0
    assert session._request_timeout_for("broker.place_order", {"wait_timeout": "bad"}) == 60.0


def test_server_session_request_timeout_can_be_configured(monkeypatch):
    """慢数据节点可调大 session 外层超时，默认行为不变。"""

    session = ClientSession.__new__(ClientSession)
    monkeypatch.setenv("QMT_SERVER_REQUEST_TIMEOUT_SECONDS", "130")

    assert session._request_timeout_for("broker.account", {}) == 130.0
    assert session._request_timeout_for("data.get_index_stocks", {}) == 130.0
    assert session._request_timeout_for("broker.place_order", {"wait_timeout": 90}) == 130.0


def test_server_session_request_timeout_ignores_invalid_env(monkeypatch):
    """非法超时配置不应覆盖原来的 60 秒默认值。"""

    session = ClientSession.__new__(ClientSession)
    monkeypatch.setenv("QMT_SERVER_REQUEST_TIMEOUT_SECONDS", "bad")
    monkeypatch.setenv("QMT_SERVER_REQUEST_TIMEOUT", "-1")

    assert session._request_timeout_for("broker.account", {}) == 60.0


def test_server_session_treats_winerror64_as_expected_disconnect():
    """Windows 对端断开不应按 session 异常打印 ERROR。"""

    exc = OSError("指定的网络名不再可用。")
    exc.winerror = 64

    assert ClientSession._is_expected_disconnect(exc) is True


def test_server_session_logs_idle_disconnect_as_debug(monkeypatch):
    """空闲连接断开只写 DEBUG；请求处理中断开保留 WARNING。"""

    records = []
    monkeypatch.setattr(
        "bullet_trade.server.session.log",
        SimpleNamespace(
            debug=lambda msg: records.append(("debug", msg)),
            warning=lambda msg: records.append(("warning", msg)),
        ),
    )
    session = ClientSession.__new__(ClientSession)
    session.session_id = "stest"
    session._current_request = None

    session._log_expected_disconnect(OSError("指定的网络名不再可用。"))

    session._current_request = "data.history"
    session._log_expected_disconnect(OSError("指定的网络名不再可用。"))

    assert records[0][0] == "debug"
    assert records[1][0] == "warning"
    assert "current_request=data.history" in records[1][1]


def test_stub_server_history(stub_server):
    conn = _make_connection(stub_server)
    try:
        resp = conn.request("data.history", {"security": "000001.XSHE"})
        assert resp["dtype"] == "dataframe"
        assert resp["columns"] == ["open", "close", "high", "low", "volume", "money"]
        assert len(resp["records"]) == 5
    finally:
        conn.close()


def test_stub_server_security_info_compat(stub_server):
    conn = _make_connection(stub_server)
    try:
        resp = conn.request("data.security_info", {"security": "000001.XSHE"})
        assert resp["value"]["display_name"] == "平安银行"
        assert resp["display_name"] == "平安银行"
        assert resp["type"] == "stock"
        assert resp["qmt_code"] == "000001.SZ"
    finally:
        conn.close()


def test_stub_server_live_current_contract(stub_server):
    conn = _make_connection(stub_server)
    try:
        resp = conn.request("data.live_current", {"security": "159915.XSHE"})
        assert resp["last_price"] > 0
        assert resp["high_limit"] > resp["last_price"]
        assert resp["low_limit"] < resp["last_price"]
        assert resp["paused"] is False
    finally:
        conn.close()


def test_stub_server_order_flow(stub_server):
    conn = _make_connection(stub_server)
    try:
        order = conn.request(
            "broker.place_order",
            {
                "security": "000001.XSHE",
                "side": "BUY",
                "amount": 100,
                "style": {"type": "limit", "price": 10.0},
            },
        )
        assert order["order_id"].startswith("stub-")
        assert order["status"] == "open"
        assert order["raw_status"] == 50
        assert order["price_type"] == 50
        assert order["order_type"] == 23
        assert order["is_buy"] is True
        assert order["order_remark"] == "bullet-trade"
        assert order["strategy_name"] == "bullet-trade"
        orders = conn.request("broker.orders", {})
        assert len(orders) == 1
        assert orders[0]["raw_status"] == 50
        cancel = conn.request("broker.cancel_order", {"order_id": order["order_id"]})
        assert cancel.get("value") is True
        assert cancel["status"] == "canceled"
        assert cancel["raw_status"] == 54
        assert cancel["last_snapshot"]["status"] == "canceled"
        assert cancel["timed_out"] is False
    finally:
        conn.close()


def test_stub_server_market_order_uses_market_price_type(stub_server):
    conn = _make_connection(stub_server)
    try:
        order = conn.request(
            "broker.place_order",
            {
                "security": "518880.XSHG",
                "side": "BUY",
                "amount": 100,
                "market": True,
                "style": {"type": "market", "protect_price": 10.073},
            },
        )
        assert order["price_type"] == 88
        assert order["order_price"] == pytest.approx(10.073)
        assert order["raw_status"] == 50
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
            {
                "security": "000001.XSHE",
                "side": "BUY",
                "amount": 100,
                "style": {"type": "limit", "price": 10.0},
            },
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


def test_stub_server_rejects_buy_below_min_order_value(monkeypatch):
    """测试服务端下单风控会拒绝低于最小金额的买入委托。"""
    monkeypatch.setenv("MIN_BUY_ORDER_VALUE", "2000")

    config = ServerConfig(
        server_type="stub",
        listen="127.0.0.1",
        port=59323,
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
        with pytest.raises(RuntimeError, match="买入订单金额低于最小值"):
            conn.request(
                "broker.place_order",
                {
                    "security": "000001.XSHE",
                    "side": "BUY",
                    "amount": 100,
                    "style": {"type": "limit", "price": 10.0},
                },
            )
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


def test_remote_data_provider_restores_multiindex_payload():
    from bullet_trade.data.providers.remote_qmt import _dataframe_from_payload

    payload = {
        "dtype": "dataframe",
        "columns": ["('open', '600635.XSHG')", "('close', '600635.XSHG')"],
        "column_tuples": [["open", "600635.XSHG"], ["close", "600635.XSHG"]],
        "column_index_names": ["field", "code"],
        "records": [[5.4, 5.49]],
    }

    df = _dataframe_from_payload(payload)

    assert isinstance(df.columns, pd.MultiIndex)
    assert df.columns.names == ["field", "code"]
    assert list(df.columns) == [("open", "600635.XSHG"), ("close", "600635.XSHG")]
    assert df["close"]["600635.XSHG"].iloc[0] == 5.49


def test_remote_data_provider_restores_legacy_stringified_tuple_columns():
    from bullet_trade.data.providers.remote_qmt import _dataframe_from_payload

    payload = {
        "dtype": "dataframe",
        "columns": ["('600635.XSHG', 'open')", "('600635.XSHG', 'close')"],
        "records": [[5.4, 5.49]],
    }

    df = _dataframe_from_payload(payload)

    assert isinstance(df.columns, pd.MultiIndex)
    assert df.columns.names == ["field", "code"]
    assert list(df.columns) == [("open", "600635.XSHG"), ("close", "600635.XSHG")]


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
        assert limit_status["status"] in {"submitted", "open", "canceled"}
        assert "raw_status" in limit_status

        snapshot = broker.sync_orders()
        snapshot_ids = {item["order_id"] for item in snapshot}
        assert {limit_order_id, market_order_id}.issubset(snapshot_ids)

        assert await broker.cancel_order(limit_order_id) is True
        updated_status = await broker.get_order_status(limit_order_id)
        assert updated_status["status"] == "canceled"
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
