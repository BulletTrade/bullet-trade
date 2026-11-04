"""作者：BruceLee。

职责：验证严格事实经真实认证TCP、Server分派和RemoteBroker传递，不执行真实交易。
输入：随机本机端口、原SDK夹具及旧/坏响应；输出：成功/失败、父账户限制和零写调用断言。
上下游：RemoteQmtBroker、ServerApplication、QmtBrokerAdapter；无外部网络或厂商SDK。
约定：远程不支持、协议不完整或SDK失败都不能回退到旧查询补空结果。
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from bullet_trade.broker.qmt import QmtBroker
from bullet_trade.broker.qmt_remote import RemoteQmtBroker
from bullet_trade.server.adapters.base import AccountRouter, AdapterBundle
from bullet_trade.server.adapters.qmt import QmtBrokerAdapter
from bullet_trade.server.adapters.big_qmt import BigQmtBrokerAdapter
from bullet_trade.server.app import ServerApplication
from bullet_trade.server.config import AccountConfig, ServerConfig


@pytest.mark.parametrize("failure", [None, "none", "error"])
def test_real_remote_transport_distinguishes_empty_and_failed_sdk(monkeypatch, failure):
    """输入SDK故障类型，无返回；真实TCP认证后只读，None/异常不能成为成功空列表。"""
    native = QmtBroker("001234")
    native._connected = True
    native._xt_account = object()
    sdk = SimpleNamespace(
        query_stock_asset=Mock(return_value=SimpleNamespace(cash=100, total_asset=100)),
        query_stock_positions=Mock(return_value=[]),
        query_stock_orders=Mock(return_value=[]),
        query_stock_trades=Mock(return_value=[]),
        order_stock=Mock(side_effect=AssertionError("禁止交易")),
    )
    if failure == "none":
        sdk.query_stock_trades.return_value = None
    if failure == "error":
        sdk.query_stock_trades.side_effect = RuntimeError("sdk-secret")
    native._xt_trader = sdk
    config = ServerConfig(
        listen="127.0.0.1",
        port=0,
        token="fixture-token",
        enable_data=False,
        accounts=[AccountConfig("main", "001234", broker_id="guojin")],
    )
    router = AccountRouter(config.accounts)
    adapter = QmtBrokerAdapter(config, router)
    monkeypatch.setattr(adapter, "_broker_for", lambda account: native)

    async def idle():
        """无输入返回；夹具不启动SDK连接、重连或订阅。"""

    adapter.start = adapter.stop = idle
    adapter.place_order = AsyncMock(side_effect=AssertionError("禁止交易"))
    adapter.cancel_order = AsyncMock(side_effect=AssertionError("禁止撤单"))
    server = ServerApplication(config, router, AdapterBundle(None, adapter))

    async def exercise():
        """无输入返回；启动自有Server和Remote客户端，finally关闭所有自有连接。"""
        task = asyncio.create_task(server.start())
        remote = None
        try:
            await asyncio.wait_for(server.wait_started(), 5)
            port = server._server.sockets[0].getsockname()[1]
            remote = RemoteQmtBroker(
                "001234",
                config={
                    "host": "127.0.0.1",
                    "port": port,
                    "token": config.token,
                    "account_key": "main",
                    "rpc_timeout": 2,
                },
            )
            await asyncio.to_thread(remote.connect)
            if failure:
                with pytest.raises(Exception) as error:
                    await asyncio.to_thread(remote.get_account_facts)
                assert "sdk-secret" not in str(error.value)
            else:
                result = await asyncio.to_thread(remote.get_account_facts)
                assert result["status"] == "complete" and result["executions"] == []
                assert result["assets"]["available_cash"] == 100
            with pytest.raises(ValueError):
                await server._dispatch_broker(
                    SimpleNamespace(account_key="main", sub_account_id=None),
                    "facts",
                    {"account_key": "main", "fake": True},
                )
            adapter.place_order.assert_not_called()
            adapter.cancel_order.assert_not_called()
            sdk.order_stock.assert_not_called()
            sdk.query_stock_trades.assert_called_once()
        finally:
            if remote is not None:
                await asyncio.to_thread(remote.disconnect)
            await server.shutdown()
            await asyncio.wait_for(task, 5)
        assert not server._sessions and not server._server.is_serving()

    asyncio.run(exercise())


@pytest.mark.parametrize("bad", [None, {}, {"schema_version": 1, "status": "complete"}])
def test_old_or_bad_server_never_falls_back_to_legacy(bad):
    """输入旧/坏协议响应，无返回；仅调用一次严格方法，不借旧接口制造成功。"""
    broker = RemoteQmtBroker("001234", config={"token": "fixture-token", "account_key": "main"})
    broker._connection = Mock()
    broker._connection.request.return_value = bad
    with pytest.raises(ValueError):
        broker.get_account_facts()
    broker._connection.request.assert_called_once_with("broker.facts", {"account_key": "main"})
    broker.sub_account_id = "child"
    with pytest.raises(NotImplementedError):
        broker.get_account_facts()
    assert broker._connection.request.call_count == 1


@pytest.mark.parametrize("error", [ConnectionError("offline"), TimeoutError("timeout")])
def test_transport_failure_is_not_retried_through_old_queries(error):
    """输入传输错误，无返回；严格查询原样失败且只请求一次，不调用旧查询或交易。"""
    broker = RemoteQmtBroker("001234", config={"token": "fixture-token", "account_key": "main"})
    broker._connection = Mock()
    broker._connection.request.side_effect = error
    with pytest.raises(type(error)):
        broker.get_account_facts()
    broker._connection.request.assert_called_once_with("broker.facts", {"account_key": "main"})


def test_bigqmt_adapter_uses_only_strict_helper_endpoint():
    """无输入返回；大QMT只从/facts读取，不依赖普通查询拼接，未知成本保留None。"""
    config = ServerConfig(accounts=[AccountConfig("main", "001234")])
    router = AccountRouter(config.accounts)
    value = {
        "schema_version": 1,
        "status": "complete",
        "assets": {"total_value": 100, "available_cash": 100},
        "positions": [{"security": "510300.XSHG", "amount": 1, "avg_cost": None}],
        "orders": [],
        "executions": [],
    }
    client = SimpleNamespace(post=AsyncMock(return_value=value))
    adapter = BigQmtBrokerAdapter(config, router, client)
    result = asyncio.run(adapter.get_account_facts(router.get("main")))
    assert result["positions"][0]["avg_cost"] is None
    assert client.post.call_args.args[0] == "/facts" and client.post.call_count == 1
    client.post.return_value = {}
    with pytest.raises(ValueError):
        asyncio.run(adapter.get_account_facts(router.get("main")))
    assert all(call.args[0] == "/facts" for call in client.post.call_args_list)
