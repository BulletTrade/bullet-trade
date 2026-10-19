"""作者：BruceLee。

职责：核验独立只读实体身份合同，不把原账户配置回显、余额或账号类型当券商。
输入：纯字典、隔离SDK/已认证Server接口夹具；输出：结构、来源和零交易写调用断言。
上下游：BrokerBase/QmtBroker/RemoteBroker及原Server分派；不访问生产或真实SDK。
约定：夹具只证明接口映射与调用边界，不能宣称真实券商身份已经验收。
"""

import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from bullet_trade.broker.identity import (
    identity_from_query,
    identity_pending,
    validate_identity_result,
    identity_with_server_declaration,
)
from bullet_trade.broker.qmt import QmtBroker
from bullet_trade.broker.base import BrokerBase
from bullet_trade.broker.qmt_remote import RemoteQmtBroker
from bullet_trade.server.adapters.base import AccountRouter, AdapterBundle
from bullet_trade.server.adapters.big_qmt import BigQmtBrokerAdapter
from bullet_trade.server.adapters.qmt import QmtBrokerAdapter
from bullet_trade.server.app import ServerApplication
from bullet_trade.server.config import AccountConfig, ServerConfig
from bullet_trade.remote import RemoteQmtConnection, classify_remote_action


@pytest.mark.unit
def test_query_identity_normalization_and_partial_observation():
    """输入独立查询事实，返回标准身份；前导零不丢、部分账号不当完整核验。"""
    value = identity_from_query(" GUOJIN ", " 001234 ")
    assert value["identity"] == {"broker_id": "guojin", "account_id": "001234"}
    assert validate_identity_result(value) == value
    assert identity_from_query(None, "001234") == identity_pending(
        "BROKER_UNAVAILABLE", observed_account_id="001234"
    )
    assert identity_from_query("guojin", None) == identity_pending("ACCOUNT_UNAVAILABLE")
    assert validate_identity_result(identity_pending()) == identity_pending()
    assert value != identity_from_query("other", "001234")
    assert BrokerBase.get_account_identity(None) == identity_pending()
    for identifier in (True, 123, "", "a/b", "a b", "a" * 129):
        with pytest.raises(ValueError):
            identity_pending("BROKER_UNAVAILABLE", observed_account_id=identifier)
    for invalid in ("other", [], True):
        with pytest.raises(ValueError):
            identity_pending(invalid)
        with pytest.raises(ValueError):
            identity_from_query("guojin", "001234", broker_source=invalid)


@pytest.mark.unit
@pytest.mark.parametrize(
    "kind",
    [
        "echo",
        "boolean-version",
        "state",
        "extra",
        "account-source",
        "broker-source",
        "different-observation",
        "numeric-account",
        "pending-entity",
        "missing-broker",
        "upper-broker",
    ],
)
def test_wire_identity_rejects_echo_and_invalid_evidence(kind):
    """输入坏结构/来源或旧配置回显，必须拒绝；不以隐式真值或数字账号产生认证。"""
    value = identity_from_query("guojin", "001234")
    if kind == "echo":
        value = {"account_id": "001234", "broker_id": "guojin", "cash": 1000}
    elif kind == "boolean-version":
        value["schema_version"] = True
    elif kind == "state":
        value["state"] = True
    elif kind == "extra":
        value["token"] = "fixture-secret"
    elif kind == "account-source":
        value["evidence"]["account_source"] = "configuration"
    elif kind == "broker-source":
        value["evidence"]["broker_source"] = "client_selection"
    elif kind == "different-observation":
        value["observed_account_id"] = "other"
    elif kind == "numeric-account":
        value["identity"]["account_id"] = 1234
    elif kind == "pending-entity":
        value["state"] = "unverified"
        value["reason_code"] = "UNSUPPORTED"
    elif kind == "missing-broker":
        value["identity"]["broker_id"] = ""
    else:
        value["identity"]["broker_id"] = "GUOJIN"
    with pytest.raises(ValueError):
        validate_identity_result(value)


@pytest.mark.unit
@pytest.mark.parametrize("account", [None, "001234", "other", 1234])
def test_qmt_identity_uses_native_asset_not_configured_snapshot(monkeypatch, account):
    """原快照可回显配置账号，但独立查询只使用原XtAsset字段；不读持仓或交易。"""
    broker = QmtBroker("001234")
    broker._connected = True
    broker._xt_account = object()
    broker._xt_trader = SimpleNamespace(
        query_stock_asset=Mock(return_value=SimpleNamespace(account_id=account, broker_type=2))
    )
    monkeypatch.setattr(
        broker, "_build_account_snapshot", lambda: {"account_id": "001234", "broker_id": "guojin"}
    )
    assert broker.get_account_info()["account_id"] == "001234"
    value = broker.get_account_identity()
    assert value["state"] == "unverified"
    assert value["reason_code"] == (
        {
            None: "ACCOUNT_UNAVAILABLE",
            "001234": "BROKER_UNAVAILABLE",
            "other": "ACCOUNT_MISMATCH",
            1234: "QUERY_FAILED",
        }[account]
    )
    broker._xt_trader.query_stock_asset.assert_called_once_with(broker._xt_account)
    assert value["identity"] is None


@pytest.mark.unit
def test_qmt_identity_error_never_returns_sdk_exception_or_assumes_connection():
    """柜台错误脱密为待核对，未连接不能拿已填账号冒充查询成功；输入为空，无外部I/O。"""
    broker = QmtBroker("001234")
    with pytest.raises(RuntimeError):
        broker.get_account_identity()
    broker._connected = True
    broker._xt_trader = SimpleNamespace(
        query_stock_asset=Mock(side_effect=RuntimeError("fixture-token"))
    )
    value = broker.get_account_identity()
    assert value == identity_pending("QUERY_FAILED") and "fixture-token" not in str(value)


@pytest.mark.unit
def test_remote_identity_sends_only_parent_route_and_no_old_account_fallback(monkeypatch):
    """远程仅调用broker.identity；子路由拒绝，旧响应结构不能以account补查伪造。"""
    monkeypatch.delenv("QMT_SERVER_SUB_ACCOUNT", raising=False)
    broker = RemoteQmtBroker("routing", config={"token": "fixture", "account_key": "parent"})
    result = identity_from_query("guojin", "001234", broker_source="authenticated_server")
    broker._connection = SimpleNamespace(request=Mock(return_value=result))
    assert broker.get_account_identity() == result
    broker._connection.request.assert_called_once_with("broker.identity", {"account_key": "parent"})
    broker._connection.request.reset_mock()
    broker.sub_account_id = "child"
    assert broker.get_account_identity() == identity_pending()
    broker._connection.request.assert_not_called()
    broker.sub_account_id = None
    broker._connection.request.return_value = {"account_id": "001234"}
    with pytest.raises(ValueError):
        broker.get_account_identity()
    assert broker._connection.request.call_count == 1
    assert classify_remote_action("broker.identity") == "none"


@pytest.mark.unit
@pytest.mark.parametrize(
    "raw", [None, {}, {"m_strAccountID": "001234"}, {"m_strAccountID": "other"}]
)
def test_big_qmt_raw_account_query_not_outer_helper_echo(raw):
    """helper外层账号为请求回显，只有原raw柜台字段成为观察，其他结果不认证。"""
    calls = []

    async def post(path, payload):
        """输入只读路径/路由，返回隔离helper接口夹具；记录调用，无真实网络。"""
        calls.append((path, payload))
        return {"dtype": "dict", "value": {"account_id": "001234", "raw": raw}}

    config = ServerConfig(accounts=[AccountConfig("main", "001234")])
    router = AccountRouter(config.accounts)
    adapter = BigQmtBrokerAdapter(config, router, SimpleNamespace(post=post))
    value = asyncio.run(adapter.get_account_identity(router.get("main")))
    assert value["state"] == "unverified"
    assert len(calls) == 1 and calls[0][0] == "/account"
    actual = raw.get("m_strAccountID") if raw else None
    assert value["reason_code"] == (
        "ACCOUNT_MISMATCH"
        if actual == "other"
        else "BROKER_UNAVAILABLE"
        if actual
        else "ACCOUNT_UNAVAILABLE"
    )


@pytest.mark.unit
def test_adapter_missing_query_and_helper_failure_remain_unverified(monkeypatch):
    """旧原生Broker缺能力及helper含秘密异常均固定待核对，不查持仓或返回原异常。"""
    config = ServerConfig(accounts=[AccountConfig("main", "001234")])
    router = AccountRouter(config.accounts)
    adapter = QmtBrokerAdapter(config, router)
    monkeypatch.setattr(adapter, "_broker_for", lambda _: SimpleNamespace())
    assert asyncio.run(adapter.get_account_identity(router.get("main"))) == identity_pending()

    async def failed(path, payload):
        """输入原只读路径和路由，无返回；模拟含令牌异常，不能在身份响应泄漏。"""
        assert path == "/account"
        raise OSError("fixture-secret")

    big = BigQmtBrokerAdapter(config, router, SimpleNamespace(post=failed))
    result = asyncio.run(big.get_account_identity(router.get("main")))
    assert result == identity_pending("QUERY_FAILED") and "fixture-secret" not in str(result)


@pytest.mark.unit
def test_server_identity_dispatch_rejects_client_identity_and_subaccount_and_old_adapter():
    """原Server分派只接父路由，客户端自造身份不进入适配器；旧适配器默认待核对。"""
    calls = []

    async def query(context):
        """输入原父上下文，返回独立协议夹具；不调用订单/资产同步或路由写入。"""
        calls.append(context.config.account_id)
        return identity_from_query("guojin", "001234", broker_source="authenticated_server")

    config = ServerConfig(accounts=[AccountConfig("main", "001234")])
    router = AccountRouter(config.accounts)
    bundle = AdapterBundle(
        data_adapter=None, broker_adapter=SimpleNamespace(get_account_identity=query)
    )
    app = ServerApplication(config, router, bundle)
    session = SimpleNamespace(account_key="main", sub_account_id=None)
    expected = asyncio.run(app._dispatch_broker(session, "identity", {"account_key": "main"}))
    assert expected["identity"]["account_id"] == "001234" and calls == ["001234"]
    for field in ("account_id", "broker_id", "price", "sub_account_id"):
        assert (
            asyncio.run(
                app._dispatch_broker(
                    session, "identity", {"account_key": "main", field: "injected"}
                )
            )
            == identity_pending()
        )
    assert calls == ["001234"]
    bundle.broker_adapter = SimpleNamespace()
    assert (
        asyncio.run(app._dispatch_broker(session, "identity", {"account_key": "main"}))
        == identity_pending()
    )


@pytest.mark.integration
@pytest.mark.parametrize("broker_id", [None, "guojin"])
def test_authenticated_loopback_identity_uses_actual_dispatch_and_native_query(monkeypatch, broker_id):
    """真实本机TCP握手/分派/适配器链，错误令牌和客户端自造身份零柜台查询；无真实交易。"""
    broker = QmtBroker("001234")
    broker._connected = True
    broker._xt_account = object()
    asset = Mock(return_value=SimpleNamespace(account_id="001234", account_type=2))
    broker._xt_trader = SimpleNamespace(query_stock_asset=asset)
    config = ServerConfig(
        listen="127.0.0.1",
        port=0,
        token="fixture-server-token",
        enable_data=False,
        accounts=[AccountConfig("main", "001234", broker_id=broker_id)],
    )
    router = AccountRouter(config.accounts)
    adapter = QmtBrokerAdapter(config, router)
    monkeypatch.setattr(adapter, "_broker_for", lambda _: broker)

    async def no_runtime_side_effect():
        """无输入/返回；本项仅测试已构造SDK接口夹具，不启动真实QMT或后台重连。"""

    monkeypatch.setattr(adapter, "start", no_runtime_side_effect)
    monkeypatch.setattr(adapter, "stop", no_runtime_side_effect)
    app = ServerApplication(config, router, AdapterBundle(None, adapter))

    async def scenario():
        """无输入/返回；只创建自有随机端口和客户端，finally关闭全部句柄，不遗留服务。"""
        serving = asyncio.create_task(app.start())
        client = None
        bad = None
        try:
            await asyncio.wait_for(app.wait_started(), timeout=5)
            port = app._server.sockets[0].getsockname()[1]
            client = RemoteQmtConnection("127.0.0.1", port, config.token, connect_timeout=2)
            await asyncio.to_thread(client.start)
            result = await asyncio.to_thread(
                client.request, "broker.identity", {"account_key": "main"}
            )
            expected = (
                identity_from_query("guojin", "001234", broker_source="authenticated_server")
                if broker_id else identity_pending("BROKER_UNAVAILABLE", observed_account_id="001234")
            )
            assert result == expected
            assert asset.call_count == 1
            forged = await asyncio.to_thread(
                client.request, "broker.identity", {"account_key": "main", "broker_id": "guojin"}
            )
            assert forged == identity_pending() and asset.call_count == 1
            bad = RemoteQmtConnection("127.0.0.1", port, "fixture-wrong-token", connect_timeout=0.2)
            with pytest.raises(RuntimeError):
                await asyncio.to_thread(bad.start)
            assert asset.call_count == 1
        finally:
            if bad:
                await asyncio.to_thread(bad.close)
            if client:
                await asyncio.to_thread(client.close)
            await app.shutdown()
            await asyncio.wait_for(serving, timeout=5)
        assert not app._sessions and not app._server.is_serving()

    asyncio.run(scenario())


@pytest.mark.unit
def test_server_declaration_never_upgrades_missing_query_or_conflict():
    """无输入返回；声明只补真实账号缺券商，缺账号/失败/冲突保留待核对，无配置回显。"""
    observed = identity_from_query(None, "001234")
    original = copy.deepcopy(observed)
    value = identity_with_server_declaration(observed, "GuoJin", "001234")
    assert value == identity_from_query("guojin", "001234", broker_source="authenticated_server")
    assert observed == original
    for reason in ("QUERY_FAILED", "UNSUPPORTED", "ACCOUNT_UNAVAILABLE", "ACCOUNT_MISMATCH"):
        pending = identity_pending(reason)
        assert identity_with_server_declaration(pending, "guojin", "001234") == pending
    assert identity_with_server_declaration(observed, None, "001234") == observed
    assert identity_with_server_declaration(observed, None, "other") == identity_pending("ACCOUNT_MISMATCH", observed_account_id="001234")
    assert identity_with_server_declaration(observed, "guojin", "other") == identity_pending("ACCOUNT_MISMATCH", observed_account_id="001234")
    queried = identity_from_query("guojin", "001234")
    assert identity_with_server_declaration(queried, "guojin", "001234") == queried
    assert identity_with_server_declaration(queried, "other", "001234") == identity_pending("BROKER_MISMATCH", observed_account_id="001234")
    assert identity_with_server_declaration(identity_pending("BROKER_UNAVAILABLE"), "guojin", "001234")["state"] == "unverified"
    with pytest.raises(ValueError):
        identity_with_server_declaration({"account_id": "001234"}, "guojin", "001234")


@pytest.mark.unit
def test_each_server_parent_uses_its_own_declaration_and_query():
    """无输入返回；两个父路由各自声明/账号不串，未知或缺查询不从配置账号生成身份。"""
    accounts = [AccountConfig("main", "001234", broker_id="guojin"), AccountConfig("other", "009876", broker_id="huatai")]
    calls = []

    async def query(context):
        """输入明确路由，返回本项独立只读接口金样；无资产/交易调用。"""
        queried = {"main": "001234", "other": "009876"}[context.config.key]
        calls.append(context.config.key)
        return identity_from_query(None, queried)

    config = ServerConfig(accounts=accounts)
    app = ServerApplication(config, AccountRouter(accounts), AdapterBundle(None, SimpleNamespace(get_account_identity=query)))
    session = SimpleNamespace(account_key="main", sub_account_id=None)
    for key, broker, account in [("main", "guojin", "001234"), ("other", "huatai", "009876")]:
        result = asyncio.run(app._dispatch_broker(session, "identity", {"account_key": key}))
        assert result == identity_from_query(broker, account, broker_source="authenticated_server")
    assert calls == ["main", "other"]
