import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.models import (
    Context,
    Order,
    OrderStatus,
    Portfolio,
    Position,
    SecurityUnitData,
    Trade,
)
from bullet_trade.core.orders import (
    LimitOrderStyle,
    MarketOrderStyle,
    clear_order_queue,
    order,
    order_target,
    order_target_value,
    order_value,
)
from bullet_trade.utils.strategy_helpers import _position_rows


def _dummy_initialize(context):
    return None


def _dummy_handle_data(context, data):
    return None


def _build_order_engine(
    monkeypatch,
    *,
    security="000001.XSHE",
    current_price=10.0,
    slippage_price=None,
    high_limit=11.0,
    low_limit=9.0,
    cash=100000.0,
):
    """构造最小回测订单撮合环境。"""
    engine = BacktestEngine(initialize=_dummy_initialize, handle_data=_dummy_handle_data)
    engine.context = Context(
        portfolio=Portfolio(
            total_value=cash,
            available_cash=cash,
            transferable_cash=cash,
            starting_cash=cash,
        ),
        current_dt=datetime.datetime(2024, 1, 2, 10, 0, 0),
    )
    engine.start_total_value = cash

    monkeypatch.setattr(
        "bullet_trade.core.orders._trigger_order_processing", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        "bullet_trade.data.api.get_current_data",
        lambda: {
            security: SecurityUnitData(
                security=security,
                last_price=current_price,
                high_limit=high_limit,
                low_limit=low_limit,
                paused=False,
            )
        },
    )
    monkeypatch.setattr("bullet_trade.core.engine.get_security_info", lambda _security: {})
    monkeypatch.setattr(
        engine, "_resolve_base_exec_price", lambda _security, _dt, _fq: current_price
    )
    monkeypatch.setattr(
        "bullet_trade.core.engine.api_get_price",
        lambda **kwargs: pd.DataFrame(
            {"close": [current_price], "volume": [10000000]},
            index=[pd.Timestamp(kwargs["end_date"])],
        ),
    )
    monkeypatch.setattr(
        engine,
        "_apply_slippage_price",
        lambda price, _is_buy, _security: slippage_price if slippage_price is not None else price,
    )
    monkeypatch.setattr(engine, "_infer_security_category", lambda _security, info=None: "stock")
    monkeypatch.setattr(engine, "_infer_tplus_from_info", lambda info: 0)
    clear_order_queue()
    return engine


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


def test_backtest_engine_get_open_orders_includes_submitted_string_status():
    """submitted 字符串状态应被视为在途订单。"""

    engine = BacktestEngine(initialize=_dummy_initialize, handle_data=_dummy_handle_data)
    order = Order(
        order_id="submitted-1",
        security="000001.XSHE",
        amount=100,
        price=10.0,
        status="submitted",
        add_time=datetime.datetime.now(),
        is_buy=True,
    )
    engine._register_order(order)

    open_orders = engine.get_open_orders()

    assert "submitted-1" in open_orders


def test_split_day_stale_close_keeps_adjusted_position_value(monkeypatch):
    """拆分生效日行情仍返回旧价时，日终估值应保持拆分后口径。"""

    security = "510500.XSHG"
    split_ratio = 166064 / 592400
    engine = BacktestEngine(initialize=_dummy_initialize, handle_data=_dummy_handle_data)
    portfolio = Portfolio(
        total_value=0.0,
        available_cash=140268.85,
        transferable_cash=140268.85,
        starting_cash=1000000.0,
    )
    portfolio.positions[security] = Position(
        security=security,
        total_amount=592400,
        closeable_amount=592400,
        avg_cost=1.7233,
        price=2.243,
        value=592400 * 2.243,
        buy_time=datetime.datetime(2015, 4, 10, 15, 0, 0),
    )
    portfolio.update_value()
    engine.context = Context(
        portfolio=portfolio,
        current_dt=datetime.datetime(2015, 4, 14, 15, 0, 0),
    )
    engine.start_total_value = portfolio.total_value
    before_total_value = portfolio.total_value

    monkeypatch.setattr(
        engine,
        "_load_corporate_actions",
        lambda *_args, **_kwargs: [
            {
                "security": security,
                "date": datetime.date(2015, 4, 14),
                "security_type": "fund",
                "scale_factor": split_ratio,
                "bonus_pre_tax": 0.0,
                "per_base": 10,
            }
        ],
    )
    monkeypatch.setattr(engine, "_is_security_paused_on_date", lambda *_args, **_kwargs: False)
    engine._apply_dividends_for_day(datetime.datetime(2015, 4, 14, 9, 0, 0))

    pos = portfolio.positions[security]
    expected_adjusted_price = 2.243 / split_ratio
    assert pos.total_amount == 166064
    assert pos.price == pytest.approx(expected_adjusted_price)
    assert portfolio.total_value == pytest.approx(before_total_value)

    monkeypatch.setattr(
        "bullet_trade.core.engine.api_get_price",
        lambda *_args, **_kwargs: pd.DataFrame(
            [{"close": 2.243}], index=[pd.Timestamp("2015-04-14")]
        ),
    )
    engine._update_positions()

    assert pos.price == pytest.approx(expected_adjusted_price)
    assert portfolio.total_value == pytest.approx(before_total_value)


def test_price_argument_creates_limit_order_style_for_order_helpers(monkeypatch):
    monkeypatch.setattr(
        "bullet_trade.core.orders._trigger_order_processing", lambda *args, **kwargs: None
    )
    clear_order_queue()
    try:
        orders = [
            order("000001.XSHE", 100, price=10.5),
            order_value("000001.XSHE", 5000, price=10.5),
            order_target("000001.XSHE", 200, price=10.5),
            order_target_value("000001.XSHE", 8000, price=10.5),
        ]
        assert all(local_order is not None for local_order in orders)
        assert all(isinstance(local_order.style, LimitOrderStyle) for local_order in orders)
        assert all(local_order.extra["requested_order_price"] == 10.5 for local_order in orders)
    finally:
        clear_order_queue()


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

    monkeypatch.setattr(
        "bullet_trade.core.orders._trigger_order_processing", lambda *args, **kwargs: None
    )
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
    monkeypatch.setattr(
        engine, "_resolve_base_exec_price", lambda security, current_dt, fq_mode: 10.0
    )
    monkeypatch.setattr(
        "bullet_trade.core.engine.api_get_price",
        lambda **kwargs: pd.DataFrame(
            {"close": [10.0], "volume": [10000000]},
            index=[pd.Timestamp(kwargs["end_date"])],
        ),
    )
    monkeypatch.setattr(engine, "_apply_slippage_price", lambda price, is_buy, security: 10.2)
    monkeypatch.setattr(engine, "_infer_security_category", lambda security, info=None: "stock")
    monkeypatch.setattr(engine, "_infer_tplus_from_info", lambda info: 0)

    clear_order_queue()
    local_order = order("000001.XSHE", 100, price=10.5)
    assert local_order is not None
    assert isinstance(local_order.style, LimitOrderStyle)
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


def test_backtest_limit_buy_fills_when_slippage_reaches_limit(monkeypatch):
    engine = _build_order_engine(
        monkeypatch,
        security="300394.XSHE",
        current_price=81.71,
        slippage_price=81.75,
        high_limit=98.0,
        low_limit=65.0,
        cash=300000.0,
    )

    local_order = order("300394.XSHE", 2400, price=81.75)
    assert local_order is not None
    assert isinstance(local_order.style, LimitOrderStyle)

    engine._process_orders(engine.context.current_dt)

    assert local_order.status == OrderStatus.filled
    assert local_order.price == pytest.approx(81.75)
    assert engine.trades[-1].price == pytest.approx(81.75)
    clear_order_queue()


def test_backtest_limit_buy_caps_slippage_at_limit(monkeypatch):
    engine = _build_order_engine(
        monkeypatch,
        current_price=81.71,
        slippage_price=81.8,
        high_limit=98.0,
        low_limit=65.0,
        cash=300000.0,
    )

    local_order = order("000001.XSHE", 100, price=81.75)
    assert local_order is not None

    engine._process_orders(engine.context.current_dt)

    assert local_order.status == OrderStatus.filled
    assert local_order.price == pytest.approx(81.75)
    assert engine.trades[-1].price == pytest.approx(81.75)
    clear_order_queue()


def test_backtest_market_protect_order_caps_slippage_at_protect_price(monkeypatch):
    engine = _build_order_engine(
        monkeypatch,
        current_price=10.0,
        slippage_price=10.25,
        high_limit=11.0,
        low_limit=9.0,
    )

    local_order = order("000001.XSHE", 100, style=MarketOrderStyle(limit_price=10.12))
    assert local_order is not None

    engine._process_orders(engine.context.current_dt)

    assert local_order.status == OrderStatus.filled
    assert local_order.price == pytest.approx(10.12)
    assert engine.trades[-1].price == pytest.approx(10.12)
    clear_order_queue()


def test_backtest_limit_buy_not_filled_when_bar_price_above_limit(monkeypatch):
    engine = _build_order_engine(
        monkeypatch,
        current_price=10.2,
        slippage_price=10.2,
        high_limit=11.0,
        low_limit=9.0,
    )

    local_order = order("000001.XSHE", 100, price=10.1)
    assert local_order is not None

    engine._process_orders(engine.context.current_dt)

    assert local_order.status == OrderStatus.open
    assert engine.context.portfolio.locked_cash > 0
    assert engine.trades == []
    assert "000001.XSHE" not in engine.context.portfolio.positions
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
