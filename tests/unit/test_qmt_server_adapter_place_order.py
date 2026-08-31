import pytest

from bullet_trade.server.adapters.base import AccountRouter, AdapterBundle
from bullet_trade.server.adapters.qmt import QmtBrokerAdapter
from bullet_trade.server.app import ServerApplication, _attach_sub_account_id
from bullet_trade.server.config import AccountConfig, ServerConfig


class _FakeBroker:
    def __init__(self):
        self.calls = []
        self.wait_results = {}

    async def buy(self, security, amount, price=None, wait_timeout=None, remark=None, market=False):
        self.calls.append(
            {
                "side": "BUY",
                "security": security,
                "amount": amount,
                "price": price,
                "wait_timeout": wait_timeout,
                "remark": remark,
                "market": market,
            }
        )
        return "OID-1"

    async def sell(
        self, security, amount, price=None, wait_timeout=None, remark=None, market=False
    ):
        self.calls.append(
            {
                "side": "SELL",
                "security": security,
                "amount": amount,
                "price": price,
                "wait_timeout": wait_timeout,
                "remark": remark,
                "market": market,
            }
        )
        return "OID-2"

    def get_positions(self):
        return [
            {
                "security": "159967.SZ",
                "closeable_amount": 1000,
                "amount": 1000,
            }
        ]

    def get_last_order_wait_result(self, order_id):
        """返回最近一次下单等待结果。

        Args:
            order_id: 订单号。

        Returns:
            dict: 测试预置的等待结果。
        """
        return self.wait_results.get(order_id, {})


class _FakeRemoteDataAdapter:
    def __init__(self, last_price=194.02):
        self.last_price = last_price

    async def get_snapshot(self, payload):
        return {
            "last_price": self.last_price,
            "high_limit": 220.0,
            "low_limit": 170.0,
            "paused": False,
        }


class _FakeSession:
    account_key = "default"
    sub_account_id = None


def test_server_sub_account_id_attaches_to_list_rows_without_shape_change():
    """server 给订单/成交列表补子账户时应保持 list[dict] 形态。"""

    rows = [{"order_id": "OID-1"}, {"trade_id": "T-1"}]

    result = _attach_sub_account_id(rows, "frank_byf_b4")

    assert result is rows
    assert result[0]["sub_account_id"] == "frank_byf_b4"
    assert result[1]["sub_account_id"] == "frank_byf_b4"


@pytest.mark.asyncio
async def test_qmt_server_market_order_prefers_client_protect_price(monkeypatch):
    from bullet_trade.core import pricing

    async def _fake_snapshot(_security):
        return {
            "last_price": 0.636,
            "high_limit": 0.700,
            "low_limit": 0.600,
            "paused": False,
        }

    def _unexpected_compute(*args, **kwargs):
        raise AssertionError("客户端已提供保护价时，不应在服务端重算")

    monkeypatch.setattr(pricing, "compute_market_protect_price", _unexpected_compute)

    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    adapter = QmtBrokerAdapter(config, router)
    ctx = router.get("default")
    fake_broker = _FakeBroker()
    adapter._brokers[ctx.config.key] = fake_broker
    monkeypatch.setattr(adapter, "_get_live_snapshot", _fake_snapshot)

    result = await adapter.place_order(
        ctx,
        {
            "security": "159967.SZ",
            "side": "SELL",
            "amount": 1000,
            "style": {"type": "market", "price": 0.626},
            "wait_timeout": 5,
            "order_remark": "bt:test:abcd1234",
        },
    )

    assert fake_broker.calls[0]["market"] is True
    assert fake_broker.calls[0]["price"] == pytest.approx(0.626)
    assert fake_broker.calls[0]["wait_timeout"] == 5
    assert result["order_price"] == pytest.approx(0.626)
    assert result["requested_order_price"] == pytest.approx(0.626)


@pytest.mark.asyncio
async def test_qmt_server_clamps_explicit_market_buy_protect_price_to_cage(monkeypatch):
    async def _fake_snapshot(_security):
        return {
            "last_price": 100.0,
            "high_limit": 120.0,
            "low_limit": 80.0,
            "paused": False,
        }

    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    adapter = QmtBrokerAdapter(config, router)
    ctx = router.get("default")
    fake_broker = _FakeBroker()
    adapter._brokers[ctx.config.key] = fake_broker
    monkeypatch.setattr(adapter, "_get_live_snapshot", _fake_snapshot)

    result = await adapter.place_order(
        ctx,
        {
            "security": "159967.SZ",
            "side": "BUY",
            "amount": 1000,
            "style": {"type": "market", "protect_price": 103.0},
        },
    )

    assert fake_broker.calls[0]["market"] is True
    assert fake_broker.calls[0]["price"] == pytest.approx(102.0)
    assert result["order_price"] == pytest.approx(102.0)


@pytest.mark.asyncio
async def test_remote_market_sell_without_price_uses_default_sell_protect_price(monkeypatch):
    from bullet_trade.core import pricing

    async def _qmt_snapshot(_security):
        return {
            "last_price": 191.81,
            "high_limit": 220.0,
            "low_limit": 170.0,
            "paused": False,
        }

    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=True,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    adapter = QmtBrokerAdapter(config, router)
    ctx = router.get("default")
    fake_broker = _FakeBroker()
    adapter._brokers[ctx.config.key] = fake_broker
    monkeypatch.setattr(adapter, "_get_live_snapshot", _qmt_snapshot)
    app = ServerApplication(
        config,
        router,
        AdapterBundle(data_adapter=_FakeRemoteDataAdapter(), broker_adapter=adapter),
    )
    payload = {
        "account_key": "default",
        "security": "159967.SZ",
        "side": "SELL",
        "amount": 1000,
        "style": {"type": "market"},
        "market": True,
        "idempotency_key": "unit-market-sell-default-protect",
    }

    result = await app._dispatch_broker(_FakeSession(), "place_order", payload)

    expected = pricing.compute_market_protect_price(
        "159967.SZ",
        191.81,
        220.0,
        170.0,
        -0.015,
        False,
    )
    assert payload["style"] == {"type": "market"}
    assert payload["_estimated_price"] == pytest.approx(194.02)
    assert fake_broker.calls[0]["market"] is True
    assert fake_broker.calls[0]["price"] == pytest.approx(expected)
    assert result["order_price"] == pytest.approx(expected)


@pytest.mark.asyncio
async def test_remote_market_buy_without_price_ignores_prefill_and_uses_default_buy_protect_price(
    monkeypatch,
):
    from bullet_trade.core import pricing

    async def _qmt_snapshot(_security):
        return {
            "last_price": 100.0,
            "high_limit": 120.0,
            "low_limit": 80.0,
            "paused": False,
        }

    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=True,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    adapter = QmtBrokerAdapter(config, router)
    ctx = router.get("default")
    fake_broker = _FakeBroker()
    adapter._brokers[ctx.config.key] = fake_broker
    monkeypatch.setattr(adapter, "_get_live_snapshot", _qmt_snapshot)
    app = ServerApplication(
        config,
        router,
        AdapterBundle(
            data_adapter=_FakeRemoteDataAdapter(last_price=103.0), broker_adapter=adapter
        ),
    )
    payload = {
        "account_key": "default",
        "security": "159967.SZ",
        "side": "BUY",
        "amount": 1000,
        "style": {"type": "market"},
        "market": True,
        "idempotency_key": "unit-market-buy-default-protect",
    }

    result = await app._dispatch_broker(_FakeSession(), "place_order", payload)

    expected = pricing.compute_market_protect_price(
        "159967.SZ",
        100.0,
        120.0,
        80.0,
        0.015,
        True,
    )
    assert payload["style"] == {"type": "market"}
    assert payload["_estimated_price"] == pytest.approx(103.0)
    assert fake_broker.calls[0]["market"] is True
    assert fake_broker.calls[0]["price"] == pytest.approx(expected)
    assert result["order_price"] == pytest.approx(expected)


@pytest.mark.asyncio
async def test_qmt_server_forwards_zero_wait_timeout(monkeypatch):
    async def _fake_snapshot(_security):
        return {
            "last_price": 10.0,
            "high_limit": 11.0,
            "low_limit": 9.0,
            "paused": False,
        }

    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    adapter = QmtBrokerAdapter(config, router)
    ctx = router.get("default")
    fake_broker = _FakeBroker()
    adapter._brokers[ctx.config.key] = fake_broker
    monkeypatch.setattr(adapter, "_get_live_snapshot", _fake_snapshot)

    result = await adapter.place_order(
        ctx,
        {
            "security": "000001.XSHE",
            "side": "BUY",
            "amount": 100,
            "style": {"type": "limit", "price": 10.0},
            "wait_timeout": 0,
        },
    )

    assert fake_broker.calls[0]["wait_timeout"] == 0
    assert result["order_id"] == "OID-1"
    assert result["status"] == "submitted"


@pytest.mark.asyncio
async def test_qmt_server_includes_order_wait_timeout_snapshot(monkeypatch):
    """服务端下单响应应透传 QMT 等待超时快照。"""

    async def _fake_snapshot(_security):
        """返回测试用实时行情。

        Args:
            _security: 证券代码。

        Returns:
            dict: 固定实时行情。
        """
        return {
            "last_price": 1.75,
            "high_limit": 1.90,
            "low_limit": 1.60,
            "paused": False,
        }

    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    adapter = QmtBrokerAdapter(config, router)
    ctx = router.get("default")
    fake_broker = _FakeBroker()
    fake_broker.wait_results["OID-1"] = {
        "timed_out": True,
        "async_tracking": True,
        "status": "open",
        "raw_status": 50,
        "last_snapshot": {
            "order_id": "OID-1",
            "status": "open",
            "raw_status": 50,
        },
    }
    adapter._brokers[ctx.config.key] = fake_broker
    monkeypatch.setattr(adapter, "_get_live_snapshot", _fake_snapshot)

    result = await adapter.place_order(
        ctx,
        {
            "security": "588000.XSHG",
            "side": "BUY",
            "amount": 1000,
            "style": {"type": "limit", "price": 1.75},
            "wait_timeout": 1,
        },
    )

    assert result["order_id"] == "OID-1"
    assert result["timed_out"] is True
    assert result["async_tracking"] is True
    assert result["status"] == "open"
    assert result["last_snapshot"]["raw_status"] == 50


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,raw_status",
    [
        ("filled", 56),
        ("rejected", 57),
    ],
)
async def test_qmt_server_propagates_terminal_wait_result(monkeypatch, status, raw_status):
    """服务端下单响应应透传等待窗口内的终态结果。"""

    async def _fake_snapshot(_security):
        """返回测试用实时行情。

        Args:
            _security: 证券代码。

        Returns:
            dict: 固定实时行情。
        """

        return {
            "last_price": 1.75,
            "high_limit": 1.90,
            "low_limit": 1.60,
            "paused": False,
        }

    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    adapter = QmtBrokerAdapter(config, router)
    ctx = router.get("default")
    fake_broker = _FakeBroker()
    fake_broker.wait_results["OID-1"] = {
        "timed_out": False,
        "async_tracking": False,
        "status": status,
        "raw_status": raw_status,
        "last_snapshot": {
            "order_id": "OID-1",
            "status": status,
            "raw_status": raw_status,
        },
    }
    adapter._brokers[ctx.config.key] = fake_broker
    monkeypatch.setattr(adapter, "_get_live_snapshot", _fake_snapshot)

    result = await adapter.place_order(
        ctx,
        {
            "security": "588000.XSHG",
            "side": "BUY",
            "amount": 1000,
            "style": {"type": "limit", "price": 1.75},
            "wait_timeout": 1,
        },
    )

    assert result["order_id"] == "OID-1"
    assert result["timed_out"] is False
    assert result["status"] == status
    assert result["last_snapshot"]["raw_status"] == raw_status
