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


def test_remote_data_provider_dataframe_conversion():
    from bullet_trade.data.providers.remote_qmt import _dataframe_from_payload

    payload = {"dtype": "dataframe", "columns": ["a"], "records": [[1], [2]]}
    df = _dataframe_from_payload(payload)
    assert isinstance(df, pd.DataFrame)
    assert list(df["a"]) == [1, 2]


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
