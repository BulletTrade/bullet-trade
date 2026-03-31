import pytest

from bullet_trade.broker.qmt import QmtBroker
from bullet_trade.core.models import OrderStatus


@pytest.mark.unit
def test_qmt_broker_get_orders_filters(monkeypatch):
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    monkeypatch.setattr(
        broker,
        "sync_orders",
        lambda: [
            {"order_id": "1", "security": "000001.XSHE", "status": "open"},
            {"order_id": "2", "security": "000002.XSHE", "status": "filled"},
        ],
    )

    orders = broker.get_orders(status=OrderStatus.open)
    ids = {item.get("order_id") for item in orders}
    assert "1" in ids
    assert "2" not in ids


@pytest.mark.unit
def test_qmt_broker_get_trades_mapping(monkeypatch):
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    class DummyTrade:
        def __init__(self):
            self.trade_id = "t1"
            self.order_id = "o1"
            self.stock_code = "000001.SZ"
            self.trade_volume = 100
            self.trade_price = 10.0
            self.trade_time = "2025-01-01 09:31:00"

    class DummyTrader:
        def query_stock_trades(self, account):
            return [DummyTrade()]

    broker._xt_trader = DummyTrader()
    broker._xt_account = object()

    trades = broker.get_trades(order_id="o1")
    assert len(trades) == 1
    trade = trades[0]
    assert trade.get("trade_id") == "t1"
    assert trade.get("order_id") == "o1"
    assert trade.get("security") == "000001.XSHE"


@pytest.mark.unit
def test_qmt_broker_get_trades_prefers_traded_price_and_preserves_zero_commission(monkeypatch):
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    class DummyTrade:
        def __init__(self):
            self.trade_id = "t2"
            self.order_id = "o2"
            self.stock_code = "510050.SH"
            self.trade_volume = 33800
            self.traded_price = 2.906
            self.trade_price = 0.0
            self.deal_balance = 98222.8
            self.commission_fee = 0.0
            self.tax = 0.0
            self.trade_time = "2026-03-31 09:45:21"

    class DummyTrader:
        def query_stock_trades(self, account):
            return [DummyTrade()]

    broker._xt_trader = DummyTrader()
    broker._xt_account = object()

    trade = broker.get_trades(order_id="o2")[0]
    assert trade["price"] == pytest.approx(2.906)
    assert trade["traded_price"] == pytest.approx(2.906)
    assert trade["deal_balance"] == pytest.approx(98222.8)
    assert trade["commission"] == pytest.approx(0.0)
    assert trade["commission_fee"] == pytest.approx(0.0)


@pytest.mark.unit
def test_qmt_broker_order_snapshot_fields(monkeypatch):
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    monkeypatch.setattr(
        broker,
        "sync_orders",
        lambda: [
            {
                "order_id": "1",
                "security": "000001.XSHE",
                "status": "open",
                "amount": 1000,
                "filled": 300,
                "price": 10.2,
                "order_type": "buy",
                "order_remark": "bt:alpha:abcd1234",
                "strategy_name": "alpha",
            }
        ],
    )

    orders = broker.get_orders()
    assert len(orders) == 1
    order = orders[0]
    assert order.get("filled") == 300
    assert order.get("is_buy") is True
    assert order.get("order_remark") == "bt:alpha:abcd1234"
    assert order.get("strategy_name") == "alpha"
