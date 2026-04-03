import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.models import Context, Order, OrderStatus, Portfolio, Position, SecurityUnitData, Trade
from bullet_trade.core.orders import clear_order_queue, order
from bullet_trade.utils.strategy_helpers import _position_rows


def _dummy_initialize(context):
    return None


def _dummy_handle_data(context, data):
    return None


def test_backtest_engine_order_trade_queries():
    engine = BacktestEngine(initialize=_dummy_initialize, handle_data=_dummy_handle_data)
    order = Order(
        order_id="o1",
        security="000001.XSHE",
        amount=100,
        price=10.0,
        status=OrderStatus.open,
        add_time=datetime.datetime.now(),
        is_buy=True,
    )
    engine._register_order(order)

    orders = engine.get_orders()
    assert "o1" in orders
    open_orders = engine.get_open_orders()
    assert "o1" in open_orders

    trade = Trade(
        order_id="o1",
        security="000001.XSHE",
        amount=100,
        price=10.0,
        time=datetime.datetime.now(),
        trade_id="t1",
    )
    engine.trades.append(trade)
    trades = engine.get_trades(order_id="o1")
    assert "t1" in trades


def test_backtest_order_records_requested_and_fill_price(monkeypatch):
    engine = BacktestEngine(initialize=_dummy_initialize, handle_data=_dummy_handle_data)
    engine.context = Context(
        portfolio=Portfolio(
            total_value=100000.0,
            available_cash=100000.0,
            transferable_cash=100000.0,
            starting_cash=100000.0,
        ),
        current_dt=datetime.datetime(2024, 1, 2, 10, 0, 0),
    )
    engine.start_total_value = 100000.0

    monkeypatch.setattr("bullet_trade.core.orders._trigger_order_processing", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "bullet_trade.data.api.get_current_data",
        lambda: {
            "000001.XSHE": SecurityUnitData(
                security="000001.XSHE",
                last_price=10.0,
                high_limit=11.0,
                low_limit=9.0,
                paused=False,
            )
        },
    )
    monkeypatch.setattr("bullet_trade.core.engine.get_security_info", lambda security: {})
    monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda security, current_dt, fq_mode: 10.0)
    monkeypatch.setattr(engine, "_apply_slippage_price", lambda price, is_buy, security: 10.2)
    monkeypatch.setattr(engine, "_infer_security_category", lambda security, info=None: "stock")
    monkeypatch.setattr(engine, "_infer_tplus_from_info", lambda info: 0)

    clear_order_queue()
    local_order = order("000001.XSHE", 100, price=10.5)
    assert local_order is not None
    assert local_order.extra["order_price"] == 10.5
    assert local_order.extra["requested_order_price"] == 10.5

    engine._process_orders(engine.context.current_dt)

    assert local_order.status == OrderStatus.filled
    assert local_order.price == 10.2
    assert local_order.extra["order_price"] == 10.5
    assert local_order.extra["requested_order_price"] == 10.5
    assert local_order.extra["fill_price"] == 10.2
    position = engine.context.portfolio.positions["000001.XSHE"]
    assert position.buy_time == engine.context.current_dt
    assert position.last_buy_time == engine.context.current_dt
    clear_order_queue()


def test_backtest_update_positions_fetches_each_security_individually(monkeypatch):
    engine = BacktestEngine(initialize=_dummy_initialize, handle_data=_dummy_handle_data)
    portfolio = Portfolio(
        total_value=100000.0,
        available_cash=1000.0,
        transferable_cash=1000.0,
        starting_cash=100000.0,
    )
    position = Position(
        security="513100.SH",
        total_amount=100,
        closeable_amount=100,
        avg_cost=1.8,
        price=1.7,
        value=170.0,
    )
    portfolio.positions["513100.SH"] = position
    portfolio.update_value()
    engine.context = Context(
        portfolio=portfolio,
        current_dt=datetime.datetime(2017, 1, 10, 15, 0, 0),
    )

    calls = []

    def _fake_api_get_price(security, **kwargs):
        calls.append(security)
        if isinstance(security, list):
            raise ValueError("找不到标的513100.SH")
        if security == "513100.SH":
            return pd.DataFrame({"close": [1.832]})
        raise AssertionError(f"unexpected security: {security}")

    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", _fake_api_get_price)

    engine._update_positions()

    assert calls == ["513100.SH"]
    assert position.price == 1.832
    assert position.value == pytest.approx(183.2)
    assert engine.context.portfolio.positions_value == pytest.approx(183.2)
    assert engine.context.portfolio.total_value == pytest.approx(1183.2)


def test_position_rows_display_buy_time_with_minute(monkeypatch):
    buy_dt = datetime.datetime(2017, 1, 3, 9, 40, 0)
    position = Position(
        security="513100.SH",
        total_amount=55400,
        closeable_amount=55400,
        avg_cost=1.793,
        price=1.801,
        value=99775.4,
        buy_time=buy_dt,
        last_buy_time=buy_dt,
    )
    context = SimpleNamespace(
        portfolio=SimpleNamespace(
            positions={"513100.SH": position},
        )
    )
    monkeypatch.setattr(
        "bullet_trade.utils.strategy_helpers.data_api.get_security_info",
        lambda code: {"display_name": "国泰纳斯达克100ETF"},
    )

    rows = _position_rows(context, total_value=100438.2, top_n=None)

    assert rows[0][3] == "2017-01-03 09:40"
