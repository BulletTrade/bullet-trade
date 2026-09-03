import pytest

from helpers import bullet_trade_jq_remote_helper as helper
from tests.test_remote_server import stub_server  # 复用 stub server fixture


def test_short_lived_client_uses_long_default_only_for_history():
    """历史行情默认等待更久，普通动作和显式超时保持原语义。"""

    client = helper._ShortLivedClient(
        "127.0.0.1",
        58620,
        "token",
        retries=0,
        rpc_timeout=60,
    )

    assert client._resolve_request_timeout("broker.account", None) == 60.0
    assert client._resolve_request_timeout("data.history", None) == 180.0
    assert client._resolve_request_timeout("data.history", 45) == 45.0

    slow_client = helper._ShortLivedClient(
        "127.0.0.1",
        58620,
        "token",
        retries=0,
        rpc_timeout=240,
    )
    assert slow_client._resolve_request_timeout("data.history", None) == 240.0


class _RecordingClient:
    def __init__(self):
        self.requests = []
        self.timeouts = []
        self.positions = [{"security": "000001.XSHE", "amount": 300, "closeable_amount": 300}]

    def request(self, action, payload, timeout=None):
        self.requests.append((action, dict(payload)))
        self.timeouts.append(timeout)
        if action == "broker.positions":
            return self.positions
        if action == "broker.order_status":
            return {"order_id": payload.get("order_id"), "status": "filled"}
        return {"order_id": "oid-1", "status": "open", "amount": payload.get("amount"), "price": 10.0}


class _CompatClient(_RecordingClient):
    def __init__(self):
        super().__init__()
        self.positions = [
            {
                "security": "000001.XSHE",
                "amount": 300,
                "closeable_amount": 200,
                "avg_cost": 9.5,
                "market_value": 3000.0,
            }
        ]

    def request(self, action, payload, timeout=None):
        self.requests.append((action, dict(payload)))
        self.timeouts.append(timeout)
        if action == "broker.account":
            return {"value": {"available_cash": 88888.0, "total_value": 123456.0}}
        if action == "broker.positions":
            return self.positions
        if action == "broker.orders":
            return []
        if action == "broker.trades":
            return []
        if action == "broker.order_status":
            return {"order_id": payload.get("order_id"), "status": "filled"}
        return {"order_id": "oid-compat", "status": "open", "amount": payload.get("amount"), "price": 10.0}


class _TimedOutClient(_RecordingClient):
    def request(self, action, payload, timeout=None):
        self.requests.append((action, dict(payload)))
        self.timeouts.append(timeout)
        return {
            "order_id": "oid-timeout",
            "status": "open",
            "amount": payload.get("amount"),
            "price": 10.0,
            "timed_out": True,
            "async_tracking": True,
            "last_snapshot": {"order_id": "oid-timeout", "status": "open"},
        }


class _FailureStatusClient(_RecordingClient):
    def __init__(self, status):
        super().__init__()
        self.status = status

    def request(self, action, payload, timeout=None):
        self.requests.append((action, dict(payload)))
        self.timeouts.append(timeout)
        return {
            "order_id": "oid-failed",
            "status": self.status,
            "amount": payload.get("amount"),
            "price": 10.0,
        }


class _TimeoutClient(_RecordingClient):
    """模拟短连接下单响应超时的 helper 测试客户端。"""

    def request(self, action, payload, timeout=None):
        """抛出无订单号的下单超时异常。

        Args:
            action: 远程 action 名称。
            payload: 请求载荷。
            timeout: 请求超时秒数。

        Returns:
            None: 本测试桩始终抛异常。

        Raises:
            RuntimeError: 模拟短连接客户端接收响应超时。
        """

        self.requests.append((action, dict(payload)))
        self.timeouts.append(timeout)
        raise RuntimeError(f"接收响应超时: timeout={timeout}s")


class _WaitFailureClient(_RecordingClient):
    """模拟下单后等待阶段查询到异常终态的 helper 测试客户端。"""

    def __init__(self, status):
        """初始化异常终态测试客户端。

        Args:
            status: 等待阶段返回的订单状态。
        """

        super().__init__()
        self.status = status
        self.status_query_count = 0

    def request(self, action, payload, timeout=None):
        """返回下单成功和后续异常终态查询。

        Args:
            action: 远程 action 名称。
            payload: 请求载荷。
            timeout: 请求超时秒数。

        Returns:
            dict: 下单响应或订单状态响应。
        """

        self.requests.append((action, dict(payload)))
        self.timeouts.append(timeout)
        if action == "broker.order_status":
            self.status_query_count += 1
            return {"order_id": payload.get("order_id"), "status": self.status}
        return {"order_id": "oid-wait-failed", "status": "open", "amount": payload.get("amount"), "price": 10.0}


class _OrderQueryClient(_RecordingClient):
    """提供订单和成交查询响应的 helper 测试客户端。"""

    def request(self, action, payload, timeout=None):
        """返回带新增字段和未知字段的测试响应。

        Args:
            action: 远程 action 名称。
            payload: 请求载荷。
            timeout: 请求超时秒数。

        Returns:
            object: helper 可消费的远程响应。
        """

        self.requests.append((action, dict(payload)))
        self.timeouts.append(timeout)
        if action == "broker.orders":
            return [
                {
                    "order_id": "oid-extra",
                    "security": "000001.XSHE",
                    "status": "open",
                    "amount": 100,
                    "price": 10.0,
                    "filled": 0,
                    "timed_out": True,
                    "async_tracking": True,
                    "last_snapshot": {"order_id": "oid-extra", "status": "open"},
                    "new_server_field": {"ignored": True},
                }
            ]
        if action == "broker.trades":
            return [
                {
                    "trade_id": "trade-extra",
                    "order_id": "oid-extra",
                    "security": "000001.XSHE",
                    "amount": 100,
                    "price": 10.0,
                    "new_server_field": "ignored",
                }
            ]
        return super().request(action, payload, timeout=timeout)


class _OpenOrdersClient(_RecordingClient):
    """提供 open order 过滤用订单列表的 helper 测试客户端。"""

    def request(self, action, payload, timeout=None):
        """返回包含 submitted 和 filled 的订单列表。

        Args:
            action: 远程 action 名称。
            payload: 请求载荷。
            timeout: 请求超时秒数。

        Returns:
            object: 订单列表或默认响应。
        """

        self.requests.append((action, dict(payload)))
        self.timeouts.append(timeout)
        if action == "broker.orders":
            return [
                {
                    "order_id": "oid-submitted",
                    "security": "000001.XSHE",
                    "status": "submitted",
                    "amount": 100,
                    "price": 10.0,
                },
                {
                    "order_id": "oid-filled",
                    "security": "000001.XSHE",
                    "status": "filled",
                    "amount": 100,
                    "price": 10.0,
                },
            ]
        return super().request(action, payload, timeout=timeout)


class _FakeDataClient:
    def get_last_price(self, security):
        assert security == "000001.XSHE"
        return 10.0


class _RunParams:
    def __init__(self, run_type):
        self.type = run_type


class _FakeContext:
    def __init__(self, run_type):
        self.run_params = _RunParams(run_type)


def test_helper_restores_legacy_stringified_tuple_columns():
    payload = {
        "dtype": "dataframe",
        "columns": [
            "('600635.XSHG', 'time')",
            "('600635.XSHG', 'open')",
            "('000001.XSHG', 'open')",
        ],
        "records": [[1777392000000.0, 5.4, 4061.822]],
    }

    df = helper._df_from_payload(payload)

    assert isinstance(df.columns, helper.pd.MultiIndex)
    assert df.columns.names == ["field", "code"]
    assert list(df.columns) == [
        ("time", "600635.XSHG"),
        ("open", "600635.XSHG"),
        ("open", "000001.XSHG"),
    ]
    assert df["open"]["600635.XSHG"].iloc[0] == 5.4


def test_helper_order_sends_zero_wait_timeout():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    oid = broker.order("000001.XSHE", 100, 10.0, "BUY", 0)

    assert oid == "oid-1"
    action, payload = client.requests[0]
    assert action == "broker.place_order"
    assert payload["wait_timeout"] == 0
    assert payload["style"] == {"type": "limit", "price": 10.0}
    assert payload["idempotency_key"].startswith("bt-helper-")


def test_helper_idempotency_key_without_time_ns(monkeypatch):
    monkeypatch.delattr(helper.time, "time_ns", raising=False)
    broker = helper.RemoteBrokerClient(_RecordingClient(), account_key="default")

    key = broker._make_idempotency_key("000001.XSHE", 100, "BUY", {"type": "market"})

    assert key.startswith("bt-helper-")
    assert len(key) == len("bt-helper-") + 24


def test_helper_order_without_price_sends_market_sell_payload():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    broker.order("000001.XSHE", -100, price=None, wait_timeout=0)

    action, payload = client.requests[0]
    assert action == "broker.place_order"
    assert payload["side"] == "SELL"
    assert payload["amount"] == 100
    assert payload["market"] is True
    assert payload["style"] == {"type": "market"}


def test_helper_order_target_value_accepts_value_alias_for_market_sell():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")
    broker.bind_data_client(_FakeDataClient())

    broker.order_target_value("000001.XSHE", value=0, wait_timeout=5)

    action, payload = next(item for item in client.requests if item[0] == "broker.place_order")
    assert action == "broker.place_order"
    assert payload["side"] == "SELL"
    assert payload["amount"] == 300
    assert payload["wait_timeout"] == 5
    assert payload["market"] is True
    assert payload["style"] == {"type": "market"}
    place_order_index = next(
        idx for idx, item in enumerate(client.requests) if item[0] == "broker.place_order"
    )
    assert client.timeouts[place_order_index] == 60.0


def test_helper_order_exposes_server_wait_timeout_fields():
    client = _TimedOutClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    oid = broker.order("000001.XSHE", 100, 10.0, "BUY", wait_timeout=10)

    assert oid == "oid-timeout"
    assert client.timeouts[0] == 60.0
    order = broker._place_order("000001.XSHE", 100, 10.0, "BUY", wait_timeout=10)
    assert order.timed_out is True
    assert order.async_tracking is True
    assert order.last_snapshot["status"] == "open"


@pytest.mark.parametrize("status", ["submit_unknown", "rejected", "canceled", "failed", "error"])
def test_helper_order_rejects_failed_or_unknown_status(status):
    """helper 不得把提交未知或明确失败的响应当成成功订单号。"""

    client = _FailureStatusClient(status)
    broker = helper.RemoteBrokerClient(client, account_key="default")

    with pytest.raises(RuntimeError):
        broker.order("000001.XSHE", 100, 10.0, "BUY", wait_timeout=10)


def test_helper_order_maps_network_timeout_to_submit_unknown():
    """helper 网络超时且无订单号时应暴露 submit_unknown 语义。"""

    client = _TimeoutClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    with pytest.raises(RuntimeError, match="submit_unknown"):
        broker.order("000001.XSHE", 100, 10.0, "BUY", wait_timeout=10)


def test_helper_place_order_timeout_margin_is_configurable():
    """helper 下单 RPC 超时余量应可配置且保持默认兼容。"""

    client = _RecordingClient()
    client.rpc_timeout = 20.0
    broker = helper.RemoteBrokerClient(
        client,
        account_key="default",
        place_order_timeout_margin=5.0,
    )

    broker.order("000001.XSHE", 100, 10.0, "BUY", wait_timeout=40)

    assert client.timeouts[0] == 45.0


def test_helper_place_order_timeout_margin_allows_zero():
    """helper 显式配置 0 秒下单余量时不应被默认 30 秒覆盖。"""

    client = _RecordingClient()
    client.rpc_timeout = 20.0
    broker = helper.RemoteBrokerClient(
        client,
        account_key="default",
        place_order_timeout_margin=0.0,
    )

    broker.order("000001.XSHE", 100, 10.0, "BUY", wait_timeout=40)

    assert client.timeouts[0] == 40.0


@pytest.mark.parametrize("status", ["failed", "error"])
def test_helper_wait_order_stops_on_failed_like_status(status):
    """helper 等待阶段遇到异常终态时应立即停止轮询。

    Args:
        status: 等待阶段返回的异常终态。
    """

    client = _WaitFailureClient(status)
    broker = helper.RemoteBrokerClient(client, account_key="default")

    oid = broker.order("000001.XSHE", 100, 10.0, "BUY", wait_timeout=5)

    assert oid == "oid-wait-failed"
    assert client.status_query_count == 1


def test_helper_order_queries_ignore_unknown_added_fields():
    """helper 查询订单/成交时应忽略未知新增字段并保留旧字段。"""

    client = _OrderQueryClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    orders = broker.get_orders(order_id="oid-extra")
    trades = broker.get_trades(order_id="oid-extra")

    order = orders["oid-extra"]
    assert order.order_id == "oid-extra"
    assert order.status == "open"
    assert order.timed_out is True
    assert order.async_tracking is True
    assert order.last_snapshot["status"] == "open"
    assert trades["trade-extra"].order_id == "oid-extra"
    assert trades["trade-extra"].security == "000001.XSHE"


def test_helper_get_open_orders_includes_submitted():
    """submitted 状态应被视为 open order，兼容新版 server 已提交未终态响应。"""

    client = _OpenOrdersClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    orders = broker.get_open_orders()

    assert list(orders.keys()) == ["oid-submitted"]


def test_helper_order_value_without_price_sends_market_sell_payload():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")
    broker.bind_data_client(_FakeDataClient())

    broker.order_value("000001.XSHE", -1000, wait_timeout=0)

    action, payload = client.requests[-1]
    assert action == "broker.place_order"
    assert payload["side"] == "SELL"
    assert payload["amount"] == 100
    assert payload["market"] is True
    assert payload["style"] == {"type": "market"}


def test_helper_order_target_without_price_sends_market_delta_payloads():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    broker.order_target("000001.XSHE", 500, wait_timeout=0)
    broker.order_target("000001.XSHE", 100, wait_timeout=0)

    place_orders = [payload for action, payload in client.requests if action == "broker.place_order"]
    assert place_orders[0]["side"] == "BUY"
    assert place_orders[0]["amount"] == 200
    assert place_orders[0]["style"] == {"type": "market"}
    assert place_orders[1]["side"] == "SELL"
    assert place_orders[1]["amount"] == 200
    assert place_orders[1]["style"] == {"type": "market"}


def test_helper_order_target_value_without_price_sends_market_buy_payload():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")
    broker.bind_data_client(_FakeDataClient())

    broker.order_target_value("000001.XSHE", target_value=5000, wait_timeout=0)

    action, payload = client.requests[-1]
    assert action == "broker.place_order"
    assert payload["side"] == "BUY"
    assert payload["amount"] == 200
    assert payload["market"] is True
    assert payload["style"] == {"type": "market"}


def test_helper_direct_order_accepts_market_order_style():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    broker.order("000001.XSHE", 100, helper.MarketOrderStyle(10.0), wait_timeout=0)

    _, payload = client.requests[0]
    assert payload["market"] is True
    assert payload["style"] == {"type": "market", "protect_price": 10.0}


def test_helper_direct_order_accepts_limit_order_style():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    broker.order("000001.XSHE", 100, helper.LimitOrderStyle(10.0), wait_timeout=0)

    _, payload = client.requests[0]
    assert "market" not in payload
    assert payload["style"] == {"type": "limit", "price": 10.0}


def test_helper_direct_percent_orders_use_remote_total_value():
    client = _CompatClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")
    broker.bind_data_client(_FakeDataClient())

    broker.order_percent("000001.XSHE", 0.1, wait_timeout=0)
    broker.order_target_percent("000001.XSHE", 0.05, wait_timeout=0)

    place_orders = [payload for action, payload in client.requests if action == "broker.place_order"]
    assert place_orders[-2]["side"] == "BUY"
    assert place_orders[-2]["amount"] == 1234
    assert place_orders[-2]["wait_timeout"] == 0
    assert place_orders[-1]["side"] == "BUY"
    assert place_orders[-1]["amount"] == 317
    assert place_orders[-1]["wait_timeout"] == 0


def test_install_jq_compat_noops_in_backtest(monkeypatch):
    calls = []

    def fake_configure(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(helper, "configure", fake_configure)
    namespace = {"order": lambda *args, **kwargs: "jq-order"}
    context = _FakeContext("full_backtest")

    result = helper.install_jq_compat(
        namespace,
        context=context,
        host="127.0.0.1",
        token="secret",
    )

    assert result["enabled"] is False
    assert calls == []
    assert namespace["order"]() == "jq-order"


def test_install_jq_compat_uses_remote_portfolio_and_default_wait(monkeypatch):
    client = _CompatClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")
    broker.bind_data_client(_FakeDataClient())

    def fake_configure(**kwargs):
        helper._BROKER_CLIENT = broker
        helper._DATA_CLIENT = broker._data_client

    monkeypatch.setattr(helper, "configure", fake_configure)
    namespace = {}
    context = _FakeContext("sim_trade")

    result = helper.install_jq_compat(
        namespace,
        context=context,
        host="127.0.0.1",
        token="secret",
        default_wait_timeout=16,
    )

    assert result["enabled"] is True
    assert context.portfolio.available_cash == 88888.0
    assert context.portfolio.total_value == 123456.0
    pos = context.portfolio.positions["000001.XSHE"]
    assert pos.total_amount == 300
    assert pos.closeable_amount == 200
    assert pos.value == 3000.0
    assert context.portfolio.positions["510300.XSHG"].total_amount == 0

    order = namespace["order_target_value"]("000001.XSHE", 5000)

    assert order.order_id == "oid-compat"
    place_orders = [payload for action, payload in client.requests if action == "broker.place_order"]
    assert place_orders[-1]["side"] == "BUY"
    assert place_orders[-1]["amount"] == 200
    assert place_orders[-1]["wait_timeout"] == 16.0
    assert place_orders[-1]["style"] == {"type": "market"}


def test_install_jq_compat_percent_orders_use_remote_total_value(monkeypatch):
    client = _CompatClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")
    broker.bind_data_client(_FakeDataClient())

    def fake_configure(**kwargs):
        helper._BROKER_CLIENT = broker
        helper._DATA_CLIENT = broker._data_client

    monkeypatch.setattr(helper, "configure", fake_configure)
    namespace = {}
    context = _FakeContext("sim_trade")
    helper.install_jq_compat(namespace, context=context, host="127.0.0.1", token="secret")

    order = namespace["order_target_percent"]("000001.XSHE", 0.05)
    namespace["order_percent"]("000001.XSHE", -0.01, wait_timeout=0)

    assert order.order_id == "oid-compat"
    assert order.is_buy is True
    place_orders = [payload for action, payload in client.requests if action == "broker.place_order"]
    assert place_orders[-2]["side"] == "BUY"
    assert place_orders[-2]["amount"] == 317
    assert place_orders[-2]["wait_timeout"] == 16.0
    assert place_orders[-1]["side"] == "SELL"
    assert place_orders[-1]["amount"] == 123
    assert place_orders[-1]["wait_timeout"] == 0.0


def test_install_jq_compat_order_style_mapping(monkeypatch):
    client = _CompatClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    def fake_configure(**kwargs):
        helper._BROKER_CLIENT = broker

    monkeypatch.setattr(helper, "configure", fake_configure)
    namespace = {}
    context = _FakeContext("sim_trade")
    helper.install_jq_compat(namespace, context=context, host="127.0.0.1", token="secret")

    namespace["order"]("000001.XSHE", 100, helper.MarketOrderStyle(10.0))
    namespace["order"]("000001.XSHE", 100, helper.LimitOrderStyle(9.9), wait_timeout=0)

    place_orders = [payload for action, payload in client.requests if action == "broker.place_order"]
    assert place_orders[-2]["style"] == {"type": "market", "protect_price": 10.0}
    assert place_orders[-2]["market"] is True
    assert place_orders[-2]["wait_timeout"] == 16.0
    assert place_orders[-1]["style"] == {"type": "limit", "price": 9.9}
    assert "market" not in place_orders[-1]
    assert place_orders[-1]["wait_timeout"] == 0.0


def test_install_jq_compat_order_target_value_rejects_conflicting_value_aliases(monkeypatch):
    client = _CompatClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    def fake_configure(**kwargs):
        helper._BROKER_CLIENT = broker

    monkeypatch.setattr(helper, "configure", fake_configure)
    namespace = {}
    context = _FakeContext("sim_trade")
    helper.install_jq_compat(namespace, context=context, host="127.0.0.1", token="secret")

    with pytest.raises(TypeError, match="got both"):
        namespace["order_target_value"]("000001.XSHE", 5000, target_value=6000)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"side": "short"},
        {"pindex": 1},
        {"close_today": True},
    ],
)
def test_install_jq_compat_rejects_unsupported_scope(monkeypatch, kwargs):
    broker = helper.RemoteBrokerClient(_CompatClient(), account_key="default")

    def fake_configure(**configure_kwargs):
        helper._BROKER_CLIENT = broker

    monkeypatch.setattr(helper, "configure", fake_configure)
    namespace = {}
    context = _FakeContext("sim_trade")
    helper.install_jq_compat(namespace, context=context, host="127.0.0.1", token="secret")

    with pytest.raises(NotImplementedError):
        namespace["order"]("000001.XSHE", 100, **kwargs)


def test_helper_order_sends_optional_order_payload_fields():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    broker.order(
        "000001.XSHE",
        100,
        price=10.0,
        wait_timeout=0,
        market=True,
        order_remark="bt:test:abcd1234",
        idempotency_key="idem-1",
    )

    _, payload = client.requests[0]
    assert payload["style"] == {"type": "market", "protect_price": 10.0}
    assert payload["market"] is True
    assert payload["order_remark"] == "bt:test:abcd1234"
    assert payload["idempotency_key"] == "idem-1"


def test_helper_order_value_without_price_uses_bound_data_client():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")
    broker.bind_data_client(_FakeDataClient())

    broker.order_value("000001.XSHE", 1000, wait_timeout=0, idempotency_key="idem-value")

    _, payload = client.requests[0]
    assert payload["amount"] == 100
    assert payload["idempotency_key"] == "idem-value"


def test_helper_e2e_with_stub(stub_server):
    helper.configure(
        host=stub_server.listen,
        port=stub_server.port,
        token=stub_server.token,
        account_key=stub_server.accounts[0].key if stub_server.accounts else "default",
        place_order_timeout_margin=12.0,
    )
    assert helper.get_broker_client()._data_client is helper.get_data_client()
    assert helper.get_broker_client().place_order_timeout_margin == 12.0
    # 账户与持仓
    account = helper.get_account()
    assert account.available_cash == 1_000_000
    positions = helper.get_positions()
    assert positions == []

    # 下单（限价 / 自动补价市价）
    oid = helper.order("000001.XSHE", 100, price=10.0, wait_timeout=0)
    assert oid
    status = helper.get_order_status(oid)
    assert status.get("order_id") == oid

    open_orders = helper.get_open_orders()
    assert oid in open_orders
    orders = helper.get_orders(order_id=oid)
    assert oid in orders
    trades = helper.get_trades(order_id=oid)
    assert isinstance(trades, dict)

    cancel_resp = helper.cancel_order(oid)
    assert cancel_resp.get("value") is True or cancel_resp.get("success", True) is True
