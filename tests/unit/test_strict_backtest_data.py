"""Optional strict price reads must invalidate a replay before another fill."""

from datetime import datetime, time
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.exceptions import BacktestDataError
from bullet_trade.core.models import Context, Order, Portfolio, SecurityUnitData
from bullet_trade.core.orders import order
from bullet_trade.core.runtime import set_current_engine
from bullet_trade.core.scheduler import run_daily, unschedule_all
from bullet_trade.core.settings import set_option
from bullet_trade.data import api as data_api


NOW = datetime(2024, 1, 2, 14, 50)
CODE = "510300.XSHG"


class PriceProvider:
    def __init__(self, failures=0, rows=1):
        self.failures = failures
        self.rows = rows
        self.calls = []

    def get_price(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) <= self.failures:
            raise ConnectionError("synthetic connection lost")
        return pd.DataFrame({"close": [10.0] * self.rows})


@pytest.fixture
def install(monkeypatch):
    def apply(provider, *, strict=True, live=False, context=True):
        state = SimpleNamespace(current_dt=NOW, run_params={"is_live": live}, _strict_backtest_data=strict)
        monkeypatch.setattr(data_api, "_current_context", state if context else None)
        monkeypatch.setattr(data_api, "_ensure_auth", lambda: provider)
        monkeypatch.setattr(data_api, "_get_default_provider", lambda: provider)
        set_option("use_real_price", True)
        return state
    return apply


def test_exhausted_price_retry_raises_typed_sticky_error_with_original_cause(install):
    provider = PriceProvider(failures=2)
    state = install(provider)

    with pytest.raises(BacktestDataError) as error:
        data_api.get_price(CODE, count=1, fields=["close"])

    assert state._backtest_data_error is error.value
    assert isinstance(error.value.__cause__, ConnectionError)
    assert len(provider.calls) == 2
    assert all(call["pre_factor_ref_date"] == NOW.date() for call in provider.calls)


def test_successful_same_reference_retry_does_not_invalidate_run(install):
    provider = PriceProvider(failures=1)
    state = install(provider)
    result = data_api.get_price(CODE, count=1, fields=["close"])
    assert result.iloc[0, 0] == 10
    assert len(provider.calls) == 2
    assert not hasattr(state, "_backtest_data_error")


@pytest.mark.parametrize("rows", [0, 3])
def test_successful_empty_or_short_listing_history_is_not_transport_failure(install, rows):
    state = install(PriceProvider(rows=rows))
    result = data_api.get_price(CODE, count=140, fields=["close"])
    assert len(result) == rows
    assert not hasattr(state, "_backtest_data_error")


@pytest.mark.parametrize("mode", ["default", "live", "no_context"])
def test_legacy_and_live_failure_behavior_stays_unchanged(install, mode):
    state = install(PriceProvider(failures=5), strict=mode != "default", live=mode == "live", context=mode != "no_context")
    result = data_api.get_price(CODE, count=1, fields=["close"])
    assert result.empty
    assert not hasattr(state, "_backtest_data_error")


def test_current_data_provider_failure_is_sticky(install):
    state = install(PriceProvider(failures=1))
    with pytest.raises(BacktestDataError) as error:
        data_api.BacktestCurrentData(state)[CODE]
    assert state._backtest_data_error is error.value


def test_authentication_failure_is_sticky(install, monkeypatch):
    state = install(PriceProvider())

    def fail():
        raise ConnectionError("synthetic authentication unavailable")

    monkeypatch.setattr(data_api, "_ensure_auth", fail)
    with pytest.raises(BacktestDataError):
        data_api.get_price(CODE, count=1)
    assert isinstance(state._backtest_data_error.__cause__, ConnectionError)


def test_attribute_history_propagates_strict_error_without_returning_empty(install):
    state = install(PriceProvider(failures=2))
    with pytest.raises(BacktestDataError) as error:
        data_api.attribute_history(CODE, 140, fields=["close"])
    assert state._backtest_data_error is error.value


def test_unsupported_price_reader_cannot_bypass_strict_invalidation(install):
    class UnsupportedProvider(PriceProvider):
        def get_price(self, **kwargs):
            raise NotImplementedError("synthetic price reader unavailable")

    state = install(UnsupportedProvider())
    with pytest.raises(BacktestDataError):
        data_api.get_price(CODE, count=1, fields=["close"])
    assert isinstance(state._backtest_data_error.__cause__, NotImplementedError)


@pytest.mark.parametrize("match_mode", ["immediate", "end_of_bar"])
def test_strategy_cannot_catch_price_failure_and_continue_filling(install, monkeypatch, match_mode):
    install(PriceProvider(failures=10))
    engine = BacktestEngine(strict_data=True)
    engine.context = Context(portfolio=Portfolio(available_cash=10000), current_dt=NOW)
    engine.context.portfolio.update_value()
    set_current_engine(engine)
    set_option("order_match_mode", match_mode)
    monkeypatch.setattr(engine, "_apply_dividends_for_day", lambda _: None)
    attempted = []

    def callback(context):
        try:
            data_api.get_price(CODE, count=1, fields=["close"])
        except Exception:
            pass
        attempted.append(order(CODE, 100))

    unschedule_all()
    run_daily(callback, "14:50")
    try:
        with pytest.raises(BacktestDataError):
            engine._run_trading_day(NOW.replace(hour=0, minute=0), [(time(9, 30), time(15))])
        assert len(attempted) == 1
        assert engine.trades == []
        assert engine.context.portfolio.available_cash == 10000
        assert not engine.context.portfolio.positions
        with pytest.raises(BacktestDataError):
            engine._process_orders(NOW)
    finally:
        unschedule_all()


def test_late_volume_read_failure_cannot_reach_futures_fill(install, monkeypatch):
    provider = PriceProvider(failures=10)
    install(provider)
    code = "IF2406.CCFX"
    engine = BacktestEngine(strict_data=True)
    engine.context = Context(portfolio=Portfolio(available_cash=1000000), current_dt=NOW)
    engine.context.previous_date = datetime(2023, 12, 29).date()
    engine.context._strict_backtest_data = True
    data_api.set_current_context(engine.context)
    quote = SecurityUnitData(security=code, last_price=4000, high_limit=4400, low_limit=3600)
    request = Order(order_id="late-read", security=code, amount=1, add_time=NOW)
    monkeypatch.setattr("bullet_trade.core.engine.get_order_queue", lambda: [request])
    monkeypatch.setattr(data_api, "get_current_data", lambda: {code: quote})
    monkeypatch.setattr("bullet_trade.core.engine.get_security_info", lambda _: {"type": "futures"})
    monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda *args: 4000)
    monkeypatch.setattr(engine, "_apply_slippage_price", lambda price, *args: price)
    monkeypatch.setattr(engine, "_execute_futures_fill", lambda **kwargs: pytest.fail("fill ran after price read failure"))

    with pytest.raises(BacktestDataError):
        engine._process_orders(NOW)

    assert len(provider.calls) == 1
    assert provider.calls[0]["fields"] == ["volume", "high", "low"]
    assert engine.trades == []
    assert engine.context.portfolio.available_cash == 1000000
