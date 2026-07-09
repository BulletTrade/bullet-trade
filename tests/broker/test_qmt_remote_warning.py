import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from bullet_trade.broker.qmt_remote import RemoteQmtBroker


class _FakeConn:
    """记录远程请求的测试连接桩。"""

    def __init__(self, response=None, exc=None):
        """初始化测试连接桩。

        Args:
            response: request 调用时返回的响应字典。
            exc: request 调用时要抛出的异常。
        """

        self.requests = []
        self.timeouts = []
        self.response = response or {"order_id": "oid-1", "warning": "000001.XSHE 停牌，拒绝远程委托"}
        self.exc = exc

    def start(self):
        """模拟连接启动。

        Returns:
            None: 测试中无需真实启动。
        """

        pass

    def close(self):
        """模拟连接关闭。

        Returns:
            None: 测试中无需真实关闭。
        """

        pass

    def request(self, action, payload, timeout=30.0):
        """记录请求并返回预置响应。

        Args:
            action: 远程 action 名称。
            payload: 请求 payload。
            timeout: 本次请求超时。

        Returns:
            dict: 预置响应副本。

        Raises:
            Exception: 如果初始化时传入 exc，则原样抛出。
        """

        self.requests.append((action, payload))
        self.timeouts.append(timeout)
        if self.exc is not None:
            raise self.exc
        return dict(self.response)


class _OrderListConn(_FakeConn):
    """返回订单列表的测试连接桩。"""

    def request(self, action, payload, timeout=30.0):
        """按 action 返回测试订单列表。

        Args:
            action: 远程 action 名称。
            payload: 请求 payload。
            timeout: 本次请求超时。

        Returns:
            object: 订单列表或默认响应。
        """

        self.requests.append((action, payload))
        self.timeouts.append(timeout)
        if action == "broker.orders":
            return [
                {"order_id": "oid-submitted", "status": "submitted"},
                {"order_id": "oid-filled", "status": "filled"},
            ]
        return dict(self.response)


def test_remote_warning_prints_and_captures(capsys, monkeypatch):
    """远程 warning 应打印到 stdout 并保存到 broker 最近 warning。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    broker._connection = _FakeConn()  # type: ignore
    broker.connect()
    broker._place_order_sync("BUY", "000001.XSHE", 100, None, None)
    out = capsys.readouterr().out
    assert "停牌" in out
    assert broker._last_warning and "停牌" in broker._last_warning


def test_remote_qmt_broker_market_order_without_price_does_not_prefill_protect_price(monkeypatch):
    """未传价格的市价单不应自动填充保护价。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    fake_conn = _FakeConn()
    broker._connection = fake_conn  # type: ignore
    broker.connect()

    broker._place_order_sync("SELL", "000001.XSHE", 100, None, None)

    action, payload = fake_conn.requests[0]
    assert action == "broker.place_order"
    assert payload["side"] == "SELL"
    assert payload["market"] is True
    assert payload["style"] == {"type": "market"}


def test_remote_qmt_broker_market_order_with_price_keeps_explicit_protect_price(monkeypatch):
    """显式传入价格的市价单应把价格作为保护价下发。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    fake_conn = _FakeConn()
    broker._connection = fake_conn  # type: ignore
    broker.connect()

    broker._place_order_sync("BUY", "000001.XSHE", 100, 10.5, None, market=True)

    action, payload = fake_conn.requests[0]
    assert action == "broker.place_order"
    assert payload["side"] == "BUY"
    assert payload["market"] is True
    assert payload["style"] == {"type": "market", "protect_price": 10.5}


def test_remote_qmt_broker_forwards_order_extra_metadata(monkeypatch):
    """远程券商下单应把 live 订单扩展审计字段透传到 server。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    fake_conn = _FakeConn()
    broker._connection = fake_conn  # type: ignore
    broker.connect()

    broker._place_order_sync(
        "BUY",
        "000001.XSHE",
        100,
        10.5,
        16,
        remark="sub:btsim_byf_b2|signal:113/exec:151",
        extra={
            "signal_batch_id": 113,
            "execution_batch_id": 151,
            "strategy_name": "btsim_byf_b2",
            "request_id": "sigexec-151-BUY-000001.XSHE",
            "idempotency_key": "sigexec-151-BUY-000001.XSHE",
            "order_price": 10.5,
        },
    )

    action, payload = fake_conn.requests[0]
    assert action == "broker.place_order"
    assert payload["order_remark"] == "sub:btsim_byf_b2|signal:113/exec:151"
    assert payload["signal_batch_id"] == 113
    assert payload["execution_batch_id"] == 151
    assert payload["strategy_name"] == "btsim_byf_b2"
    assert payload["request_id"] == "sigexec-151-BUY-000001.XSHE"
    assert payload["idempotency_key"] == "sigexec-151-BUY-000001.XSHE"
    assert "order_price" not in payload


def test_remote_qmt_broker_place_order_rpc_timeout_has_margin(monkeypatch):
    """单笔等待窗口应自动扩展远程下单 RPC timeout。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc", config={"rpc_timeout": 30, "place_order_timeout_margin": 30})
    fake_conn = _FakeConn()
    broker._connection = fake_conn  # type: ignore
    broker.connect()

    broker._place_order_sync("BUY", "000001.XSHE", 100, 10.5, 30)

    assert fake_conn.timeouts[0] == 60.0
    assert broker.get_last_order_response("oid-1")["order_id"] == "oid-1"


def test_remote_qmt_broker_warns_when_default_timeout_budget_is_risky(monkeypatch):
    """默认 RPC timeout 小于等待窗口加余量时只告警不阻断。"""

    warnings = []
    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    monkeypatch.setenv("TRADE_MAX_WAIT_TIME", "30")
    monkeypatch.setattr(
        "bullet_trade.broker.qmt_remote.log.warning",
        lambda message, *args, **kwargs: warnings.append(message % args if args else message),
    )

    broker = RemoteQmtBroker(account_id="acc", config={"rpc_timeout": 30, "place_order_timeout_margin": 30})

    assert broker.rpc_timeout == 30.0
    assert broker._resolve_place_order_rpc_timeout(30) == 60.0
    assert warnings and "超时配置风险" in warnings[0]


def test_remote_qmt_broker_respects_explicit_zero_timeout_margin(monkeypatch):
    """显式配置 0 秒下单余量时不应被默认值覆盖。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")

    broker = RemoteQmtBroker(account_id="acc", config={"rpc_timeout": 30, "place_order_timeout_margin": 0})

    assert broker.place_order_timeout_margin == 0.0
    assert broker._resolve_place_order_rpc_timeout(30) == 30.0


def test_remote_qmt_broker_uses_default_wait_timeout_for_none(monkeypatch):
    """未传单笔 wait_timeout 时也要按全局默认等待窗口预留下单 RPC 时间。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")

    broker = RemoteQmtBroker(
        account_id="acc",
        config={"rpc_timeout": 30, "wait_timeout": 20, "place_order_timeout_margin": 15},
    )

    assert broker._resolve_place_order_rpc_timeout(None) == 35.0
    assert broker._resolve_place_order_rpc_timeout(0) == 30.0


def test_remote_qmt_broker_sends_configured_default_wait_timeout(monkeypatch):
    """配置了默认等待窗口时，payload 和 RPC timeout 预算必须使用同一个值。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(
        account_id="acc",
        config={"rpc_timeout": 30, "wait_timeout": 20, "place_order_timeout_margin": 15},
    )
    fake_conn = _FakeConn()
    broker._connection = fake_conn  # type: ignore
    broker.connect()

    broker._place_order_sync("BUY", "000001.XSHE", 100, 10.5, None)

    assert fake_conn.requests[0][1]["wait_timeout"] == 20.0
    assert fake_conn.timeouts[0] == 35.0


def test_remote_qmt_broker_keeps_legacy_no_default_wait_payload(monkeypatch):
    """未配置默认等待窗口时不额外写入 wait_timeout，保持旧 server 默认行为。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    monkeypatch.delenv("TRADE_MAX_WAIT_TIME", raising=False)
    broker = RemoteQmtBroker(account_id="acc", config={"rpc_timeout": 30})
    fake_conn = _FakeConn()
    broker._connection = fake_conn  # type: ignore
    broker.connect()

    broker._place_order_sync("BUY", "000001.XSHE", 100, 10.5, None)

    assert "wait_timeout" not in fake_conn.requests[0][1]
    assert fake_conn.timeouts[0] == 30.0


def test_remote_qmt_broker_accepts_legacy_trade_max_wait_time_config(monkeypatch):
    """兼容旧配置名 trade_max_wait_time，避免老用户升级后默认预算丢失。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")

    broker = RemoteQmtBroker(
        account_id="acc",
        config={"rpc_timeout": 30, "trade_max_wait_time": 25, "place_order_timeout_margin": 10},
    )

    assert broker.default_wait_timeout == 25.0
    assert broker._resolve_place_order_rpc_timeout(None) == 35.0


def test_remote_qmt_broker_keeps_timed_out_open_order(monkeypatch):
    """服务端等待终态超时时，客户端应保留 open 订单号。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    fake_conn = _FakeConn(
        response={
            "order_id": "oid-open",
            "status": "open",
            "timed_out": True,
            "async_tracking": True,
        }
    )
    broker._connection = fake_conn  # type: ignore
    broker.connect()

    order_id = broker._place_order_sync("BUY", "000001.XSHE", 100, 10.5, 16)

    assert order_id == "oid-open"
    assert broker.get_last_order_response("oid-open")["timed_out"] is True


def test_remote_qmt_broker_rejects_submit_unknown_response(monkeypatch):
    """服务端明确返回 submit_unknown 时客户端不应当成成功订单。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    broker._connection = _FakeConn(response={"order_id": "submit_unknown:r1", "status": "submit_unknown"})  # type: ignore
    broker.connect()

    try:
        broker._place_order_sync("BUY", "000001.XSHE", 100, 10.5, 16)
    except RuntimeError as exc:
        assert "submit_unknown" in str(exc)
    else:
        raise AssertionError("submit_unknown response should raise")


def test_remote_qmt_broker_rejects_submit_unknown_without_order_id(monkeypatch):
    """服务端返回 submit_unknown 时即使没有订单号也要按提交未知处理。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    broker._connection = _FakeConn(response={"order_id": "", "status": "submit_unknown"})  # type: ignore
    broker.connect()

    try:
        broker._place_order_sync("BUY", "000001.XSHE", 100, 10.5, 16)
    except RuntimeError as exc:
        assert "submit_unknown" in str(exc)
    else:
        raise AssertionError("submit_unknown response should raise")


@pytest.mark.parametrize("status", ["rejected", "canceled", "cancelled", "failed", "error"])
def test_remote_qmt_broker_rejects_terminal_failure_status(monkeypatch, status):
    """服务端明确返回终态失败时客户端不应当成成功订单。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    broker._connection = _FakeConn(response={"order_id": "oid-failed", "status": status})  # type: ignore
    broker.connect()

    with pytest.raises(RuntimeError, match="下单失败"):
        broker._place_order_sync("BUY", "000001.XSHE", 100, 10.5, 16)


def test_remote_qmt_broker_maps_network_timeout_to_submit_unknown(monkeypatch):
    """网络请求超时且无订单号时应映射为 submit_unknown 风险异常。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    broker._connection = _FakeConn(exc=TimeoutError("request timed out"))  # type: ignore
    broker.connect()

    try:
        broker._place_order_sync("BUY", "000001.XSHE", 100, 10.5, 16)
    except RuntimeError as exc:
        assert "submit_unknown" in str(exc)
    else:
        raise AssertionError("network timeout should raise submit_unknown runtime error")


def test_remote_qmt_broker_get_open_orders_includes_submitted(monkeypatch):
    """submitted 状态应被视为 open order，避免新 server 响应被旧查询漏掉。"""

    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    broker._connection = _OrderListConn()  # type: ignore
    broker.connect()

    orders = broker.get_open_orders()

    assert [item["order_id"] for item in orders] == ["oid-submitted"]
