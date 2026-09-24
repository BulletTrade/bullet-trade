"""Synthetic callback-time valuation and strict replay failure contracts."""

from datetime import datetime, time

import pandas as pd
import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.exceptions import BacktestDataError
from bullet_trade.core.models import Context, Portfolio, Position
from bullet_trade.core.scheduler import run_daily, unschedule_all


DAY = datetime(2024, 1, 2)
CODE = "510300.XSHG"
PERIODS = [(time(9, 30), time(11, 30)), (time(13), time(15))]


@pytest.fixture(autouse=True)
def reset_schedule():
    unschedule_all()
    yield
    unschedule_all()


@pytest.fixture
def make_engine(monkeypatch):
    def make(*, strict=True):
        engine = BacktestEngine(strict_data=strict)
        position = Position(security=CODE, total_amount=100, closeable_amount=100, avg_cost=10)
        position.update_price(10)
        portfolio = Portfolio(available_cash=1000, positions={CODE: position})
        portfolio.update_value()
        engine.context = Context(portfolio=portfolio, current_dt=DAY)
        monkeypatch.setattr(engine, "_apply_dividends_for_day", lambda _: None)
        monkeypatch.setattr(engine, "_process_orders", lambda _: None)
        return engine
    return make


def test_callbacks_observe_only_prices_available_at_each_time(make_engine, monkeypatch):
    engine = make_engine()
    seen = {}
    requests = []

    def price(**kwargs):
        requests.append(kwargs)
        value = 11 if kwargs["frequency"] == "minute" else 12
        if kwargs["fields"] == ["open"]:
            value = 10.5
        return pd.DataFrame({kwargs["fields"][0]: [value]})

    def observe(context):
        seen[context.current_dt.time()] = context.portfolio.total_value

    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", price)
    run_daily(observe, "09:00")
    run_daily(observe, "14:50")
    run_daily(observe, "15:00")
    engine.handle_data_func = lambda context, data: observe(context)
    engine._run_trading_day(DAY, PERIODS)

    assert seen == {time(9): 2000, time(9, 30): 2050, time(14, 50): 2100, time(15): 2200}
    assert [(r["end_date"].time(), r["frequency"], r["fields"]) for r in requests] == [
        (time(9, 30), "daily", ["open"]),
        (time(14, 50), "minute", ["close"]),
        (time(15), "daily", ["close"]),
    ]
    assert all(r["fq"] == "none" for r in requests)


@pytest.mark.parametrize("quote", [10.5, 21.0])
def test_split_day_valuation_keeps_share_and_price_units_consistent(make_engine, monkeypatch, quote):
    engine = make_engine()
    portfolio = engine.context.portfolio
    position = portfolio.positions[CODE]
    engine.context.current_dt = DAY.replace(hour=14, minute=50)
    engine._apply_split(code=CODE, pos=position, split_ratio=0.5, eff_date=DAY.date(), portfolio=portfolio)
    monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda *args: quote)

    engine._mark_non_futures_intraday(engine.context.current_dt)

    assert position.total_amount == 50
    assert position.price == 21
    assert position.value == 1050
    assert portfolio.total_value == 2050


@pytest.mark.parametrize("quote", [None, 0, float("nan"), float("inf")])
def test_missing_required_price_stops_before_callback(make_engine, monkeypatch, quote):
    engine = make_engine()
    called = []
    monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda *args: quote)
    run_daily(lambda context: called.append(context.current_dt), "14:50")

    with pytest.raises(BacktestDataError, match=CODE):
        engine._run_trading_day(DAY, PERIODS)

    assert called == []
    assert engine.context.portfolio.total_value == 2000
    assert isinstance(engine.context._backtest_data_error, BacktestDataError)


def test_failed_valuation_does_not_partially_update_other_positions(make_engine, monkeypatch):
    engine = make_engine()
    other = Position(security="510500.XSHG", total_amount=100, price=10, value=1000)
    engine.context.portfolio.positions[other.security] = other
    engine.context.portfolio.update_value()
    monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda code, *args: 11 if code == CODE else None)

    with pytest.raises(BacktestDataError):
        engine._mark_non_futures_intraday(DAY.replace(hour=14, minute=50))

    assert engine.context.portfolio.positions[CODE].price == 10
    assert engine.context.portfolio.total_value == 3000


def test_default_mode_retains_previous_value_when_quote_missing(make_engine, monkeypatch):
    engine = make_engine(strict=False)
    monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda *args: None)
    engine._mark_non_futures_intraday(DAY.replace(hour=14, minute=50))
    assert engine.context.portfolio.total_value == 2000
    assert not hasattr(engine.context, "_backtest_data_error")


def test_futures_in_cash_position_map_are_not_repriced_by_equity_mark(make_engine, monkeypatch):
    engine = make_engine()
    engine.context.portfolio.positions.clear()
    engine.context.portfolio.positions["IF2406.CCFX"] = Position(security="IF2406.CCFX", total_amount=1)
    monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda *args: pytest.fail("cash mark read a futures quote"))
    engine._mark_non_futures_intraday(DAY.replace(hour=14, minute=50))


def test_missing_end_of_day_held_price_invalidates_strict_run(make_engine, monkeypatch):
    engine = make_engine()
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", lambda **kwargs: pd.DataFrame())
    with pytest.raises(BacktestDataError, match=CODE):
        engine._update_positions()


def test_future_minute_row_is_rejected_even_when_request_end_is_correct(make_engine, monkeypatch):
    engine = make_engine()
    now = DAY.replace(hour=14, minute=50)
    frame = pd.DataFrame({"close": [11]}, index=[now.replace(minute=51)])
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", lambda **kwargs: frame)
    with pytest.raises(BacktestDataError, match="Future price row"):
        engine._mark_non_futures_intraday(now)
    assert engine.context.portfolio.total_value == 2000


def test_previous_visible_minute_remains_usable_for_paused_security(make_engine, monkeypatch):
    engine = make_engine()
    now = DAY.replace(hour=14, minute=50)
    frame = pd.DataFrame({"close": [11]}, index=[now.replace(hour=14, minute=49)])
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", lambda **kwargs: frame)
    engine._mark_non_futures_intraday(now)
    assert engine.context.portfolio.total_value == 2100
