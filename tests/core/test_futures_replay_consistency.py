"""期货订单、日 K 撮合约束和连续账本的离线集成回归。"""

from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.contract_specs import FuturesSpecConfig, MarginRateRule
from bullet_trade.core.futures_account import ContractSpecTable, FuturesAccount
from bullet_trade.core.models import Context, OrderStatus, Portfolio, SecurityUnitData
from bullet_trade.core.orders import LimitOrderStyle, MarketOrderStyle, order, order_target
from bullet_trade.core.runtime import set_current_engine
from bullet_trade.core.settings import OrderCost, get_settings, set_option, set_order_cost
from bullet_trade.data.tick_replay import TickSnapshot

CODE = "LH2109.XDCE"
PRICE = 27000.0


@pytest.fixture
def engine(monkeypatch):
    instance = BacktestEngine()
    instance._futures_spec_table = ContractSpecTable(enable_remote=False)
    instance.context = Context(
        portfolio=Portfolio(total_value=1e6, available_cash=1e6, starting_cash=1e6),
        current_dt=datetime(2021, 4, 6, 9, 35),
        previous_date=date(2021, 4, 2),
    )
    set_option("futures_margin_rate", 0.14)
    set_option("order_match_mode", "immediate")
    set_order_cost(OrderCost(open_commission=0, close_commission=0.000023,
                             close_today_commission=0.0023, min_commission=0),
                   type="futures")
    monkeypatch.setattr("bullet_trade.core.engine.get_security_info", lambda _: {})
    monkeypatch.setattr(instance, "_resolve_base_exec_price", lambda *args: PRICE)
    monkeypatch.setattr(instance, "_apply_slippage_price", lambda price, *args: price)
    monkeypatch.setattr(instance, "_futures_contract_end_date", lambda _: date(2021, 9, 27))
    monkeypatch.setattr("bullet_trade.data.api.get_current_data", lambda: {
        CODE: SecurityUnitData(security=CODE, last_price=PRICE, paused=False)
    })
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", lambda **kwargs: pd.DataFrame(
        {"volume": [100.], "high": [PRICE + 100], "low": [PRICE - 100]},
        index=pd.to_datetime([instance.context.current_dt.date()]),
    ))
    set_current_engine(instance)
    return instance


def test_deferred_targets_keep_both_sides(engine, monkeypatch):
    set_option("order_match_mode", "deferred")
    long_order = order_target(CODE, 2, side="long")
    short_order = order_target(CODE, 2, side="short")
    engine._process_orders(engine.context.current_dt)
    assert long_order.status == short_order.status == OrderStatus.filled
    account = engine.context.portfolio.futures_account
    assert account.get_position(CODE, "long").amount == 2
    assert account.get_position(CODE, "short").amount == 2


def test_deferred_same_side_target_still_replaces_old_target(engine, monkeypatch):
    set_option("order_match_mode", "deferred")
    first = order_target(CODE, 1)
    last = order_target(CODE, 3)
    engine._process_orders(engine.context.current_dt)
    assert first.status == OrderStatus.canceled
    assert last.filled == 3


def test_daily_volume_is_an_independent_cap_for_each_order(engine, monkeypatch):
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", lambda **kwargs: pd.DataFrame(
        {"volume": [2.], "high": [PRICE + 100], "low": [PRICE - 100]},
        index=pd.to_datetime([engine.context.current_dt.date()]),
    ))
    first, second = order(CODE, 3), order(CODE, 3)
    assert first.filled == second.filled == 2
    assert first.status == second.status == OrderStatus.canceled
    assert first.extra["cancel_reason"] == second.extra["cancel_reason"] == "insufficient_volume"
    assert sum(abs(trade.amount) for trade in engine.trades) == 4


@pytest.mark.parametrize("side", ["long", "short"])
def test_daily_cap_applies_to_close_orders_without_consuming_prior_fills(engine, monkeypatch, side):
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", lambda **kwargs: pd.DataFrame(
        {"volume": [2.], "high": [PRICE + 100], "low": [PRICE - 100]},
        index=pd.to_datetime([engine.context.current_dt.date()]),
    ))
    assert order(CODE, 2, side=side).filled == 2
    assert order(CODE, 2, side=side).filled == 2
    closing = order(CODE, -3, side=side)
    assert closing.filled == 2
    assert closing.status == OrderStatus.canceled
    assert closing.extra["cancel_reason"] == "insufficient_volume"
    assert engine.context.portfolio.futures_account.get_position(CODE, side).amount == 2


def test_rejected_order_does_not_affect_the_next_order(engine):
    engine.context.portfolio.available_cash = 0
    assert order(CODE, 2).status == OrderStatus.rejected
    engine.context.portfolio.available_cash = 1e6
    assert order(CODE, 2).filled == 2


def test_daily_bar_cache_is_per_contract_and_day(engine, monkeypatch):
    requests = []
    def fetch(**kwargs):
        requests.append(kwargs)
        return pd.DataFrame(
            {"volume": [2.], "high": [PRICE + 100], "low": [PRICE - 100]},
            index=pd.to_datetime([kwargs["end_date"]]),
        )
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", fetch)
    assert order(CODE, 2).filled == order(CODE, 2).filled == 2
    assert len(requests) == 1
    engine.context.current_dt += timedelta(days=1)
    assert order(CODE, 2).filled == 2
    assert len(requests) == 2
    engine._resolve_futures_daily_bar("IF2106.CCFX", engine.context.current_dt)
    assert len(requests) == 3


@pytest.mark.parametrize("frequency", ["day", "minute"])
def test_market_order_uses_daily_bar_even_when_current_minute_has_zero_volume(engine, monkeypatch, frequency):
    engine.frequency = frequency
    requests = []
    def fetch(**kwargs):
        requests.append(kwargs)
        if kwargs["frequency"] == "minute":
            return pd.DataFrame({"volume": [0.]})
        return pd.DataFrame(
            {"volume": [4.], "high": [PRICE + 100], "low": [PRICE - 100]},
            index=pd.to_datetime([engine.context.current_dt.date()]),
        )
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", fetch)
    result = order(CODE, 3, style=MarketOrderStyle())
    assert result.filled == 3
    assert len(requests) == 1
    assert requests[0]["frequency"] == "daily"
    assert requests[0]["fields"] == ["volume", "high", "low"]
    assert requests[0]["start_date"] == requests[0]["end_date"] == engine.context.current_dt.date()


def test_tick_order_without_snapshot_never_falls_back_to_bar(engine, monkeypatch):
    engine.frequency = "tick"
    monkeypatch.setattr(engine, "_resolve_base_exec_price",
                        lambda *args: pytest.fail("tick order fell back to bar"))
    result = order(CODE, 1)
    assert result.status == OrderStatus.rejected
    assert result.extra["rejection_reason"] == "tick_snapshot_unavailable"
    assert not engine.trades


def test_engine_close_allocation_matches_commission(engine, monkeypatch):
    assert order(CODE, 1).filled == 1
    account = engine.context.portfolio.futures_account
    account.settle_day({CODE: PRICE}, day=engine.context.current_dt.date())
    engine.context.portfolio.available_cash = account.cash
    assert order(CODE, 1).filled == 1
    first = order(CODE, -1)
    assert first.commission == pytest.approx(PRICE * 16 * 0.000023)
    assert account.get_position(CODE, "long").today_amount == 1
    second = order(CODE, -1, close_today=True)
    assert second.status == OrderStatus.filled
    assert second.commission == pytest.approx(PRICE * 16 * 0.0023)
    assert first.commission + second.commission == pytest.approx(1003.536)
    assert engine.trades[-1].action == "close"
    assert engine.trades[-1].side == "long"
    assert engine.trades[-1].multiplier == 16


def test_futures_fees_preserve_configured_arithmetic(engine):
    costs = OrderCost(open_commission=0.000023, min_commission=0)
    assert engine._calculate_futures_commission(costs, "open", 1, PRICE * 16, 0) == pytest.approx(9.936)


def test_daily_bar_selects_requested_multiindex_contract(engine, monkeypatch):
    frame = pd.DataFrame({
        ("volume", CODE): [3.], ("high", CODE): [PRICE + 100], ("low", CODE): [PRICE - 100],
        ("volume", "IF2106.CCFX"): [100.], ("high", "IF2106.CCFX"): [4000.],
        ("low", "IF2106.CCFX"): [3900.],
    }, index=pd.to_datetime(["2021-04-06"]))
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", lambda **kwargs: frame)
    bar, reason = engine._resolve_futures_daily_bar(CODE, engine.context.current_dt)
    assert reason is None
    assert bar == {"volume": 3., "high": PRICE + 100, "low": PRICE - 100}


def test_daily_bar_selects_code_and_date_from_long_frame(engine, monkeypatch):
    frame = pd.DataFrame({
        "code": ["IF2106.CCFX", CODE], "time": ["2021-04-06", "2021-04-06"],
        "volume": [100., 3.], "high": [4000., PRICE + 100], "low": [3900., PRICE - 100],
    })
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", lambda **kwargs: frame)
    assert engine._resolve_futures_daily_bar(CODE, engine.context.current_dt) == (
        {"volume": 3., "high": PRICE + 100, "low": PRICE - 100}, None,
    )


@pytest.mark.parametrize("frame,reason", [
    (pd.DataFrame(), "futures_daily_bar_missing"),
    (pd.DataFrame({"volume": [10.], "high": [PRICE], "low": [PRICE]},
                  index=pd.to_datetime(["2021-04-02"])), "futures_daily_bar_date_mismatch"),
    (pd.DataFrame({"volume": [10.], "high": [PRICE], "low": [PRICE]},
                  index=pd.to_datetime(["2021-04-07"])), "futures_daily_bar_date_mismatch"),
    (pd.DataFrame({"volume": [float("nan")], "high": [PRICE], "low": [PRICE]},
                  index=pd.to_datetime(["2021-04-06"])), "futures_daily_bar_invalid"),
    (pd.DataFrame({"volume": [-1.], "high": [PRICE], "low": [PRICE]},
                  index=pd.to_datetime(["2021-04-06"])), "futures_daily_bar_invalid"),
    (pd.DataFrame({"volume": [10.], "high": [PRICE], "low": [float("nan")]},
                  index=pd.to_datetime(["2021-04-06"])), "futures_daily_bar_invalid"),
    (pd.DataFrame({"volume": [10.], "high": [PRICE - 1], "low": [PRICE]},
                  index=pd.to_datetime(["2021-04-06"])), "futures_daily_bar_invalid"),
    (pd.DataFrame({"volume": [10.]}, index=pd.to_datetime(["2021-04-06"])), "futures_daily_bar_invalid"),
    (pd.DataFrame({("volume", "IF2106.CCFX"): [10.]},
                  index=pd.to_datetime(["2021-04-06"])), "futures_daily_bar_invalid"),
])
def test_unverifiable_daily_bar_cancels_without_fill(engine, monkeypatch, frame, reason):
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", lambda **kwargs: frame)
    result = order(CODE, 1)
    assert result.status == OrderStatus.canceled
    assert result.filled == 0
    assert result.extra["cancel_reason"] == reason
    assert not engine.trades
    assert engine.context.portfolio.available_cash == 1e6


@pytest.mark.parametrize("side", ["long", "short"])
def test_canceled_target_close_retains_resolved_order_intent(engine, monkeypatch, side):
    assert order(CODE, 2, side=side).filled == 2
    account = engine.context.portfolio.futures_account
    before = (account.cash, account.margin, account.get_position(CODE, side).amount)
    monkeypatch.setattr(engine, "_resolve_futures_daily_bar", lambda *args: (
        {"volume": 3., "high": PRICE - 10, "low": PRICE - 20}, None,
    ))

    result = order_target(CODE, 0, side=side)

    assert result.status == OrderStatus.canceled
    assert result.filled == 0
    assert result.extra["requested_amount"] == 2
    assert result.extra["cancel_reason"] == "futures_price_outside_daily_range"
    assert result.action == "close"
    assert result.side == side
    assert result.is_buy is (side == "short")
    assert (account.cash, account.margin, account.get_position(CODE, side).amount) == before


@pytest.mark.parametrize("base_price,slippage,filled", [
    (PRICE - 101, 50, 0), (PRICE + 101, -50, 0),
    (PRICE - 100, -50, 1), (PRICE + 100, 50, 1),
])
def test_market_order_checks_unslipped_price_against_daily_range(engine, monkeypatch, base_price, slippage, filled):
    monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda *args: base_price)
    monkeypatch.setattr(engine, "_apply_slippage_price", lambda price, *args: price + slippage)
    result = order(CODE, 1)
    assert result.filled == filled
    if not filled:
        assert result.extra["cancel_reason"] == "futures_price_outside_daily_range"
        assert not engine.trades


def test_daily_bar_exception_closes_order_and_restores_future_guard(engine, monkeypatch):
    import bullet_trade.data.api as data_api
    from bullet_trade.core.exceptions import FutureDataError
    data_api.set_current_context(engine.context)
    set_option("avoid_future_data", True)
    def fetch(**kwargs):
        assert data_api._should_avoid_future() is False
        raise RuntimeError("synthetic missing daily data")
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", fetch)
    try:
        result = order(CODE, 1)
        assert result.extra["cancel_reason"] == "futures_daily_bar_unavailable"
        assert result.filled == 0
        assert data_api._should_avoid_future() is True
        with pytest.raises(FutureDataError):
            data_api.get_price(CODE, end_date=engine.context.current_dt,
                               frequency="daily", fields=["volume", "high", "low"], count=1)
    finally:
        data_api.set_current_context(None)


def test_limit_order_keeps_price_crossing_without_daily_market_cap(engine, monkeypatch):
    monkeypatch.setattr(engine, "_resolve_futures_daily_bar",
                        lambda *args: pytest.fail("limit order used market daily cap"))
    result = order(CODE, 1, style=LimitOrderStyle(PRICE))
    assert result.filled == 1
    missed = order(CODE, 1, style=LimitOrderStyle(PRICE - 100))
    assert missed.status == OrderStatus.canceled
    assert missed.filled == 0


def test_stock_order_and_volume_ratio_default_are_unchanged(engine, monkeypatch):
    stock = "000001.XSHE"
    monkeypatch.setattr(engine, "_resolve_futures_daily_bar",
                        lambda *args: pytest.fail("stock queried futures daily data"))
    monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda *args: 10.)
    monkeypatch.setattr("bullet_trade.data.api.get_current_data", lambda: {
        stock: SecurityUnitData(security=stock, last_price=10., paused=False)
    })
    assert get_settings().options["order_volume_ratio"] == 0.25
    result = order(stock, 100)
    assert result.filled == 100
    assert result.status == OrderStatus.filled


def test_tick_first_available_price_applies_dated_margin_once(engine):
    config = FuturesSpecConfig(margin_rules={
        "LH": MarginRateRule("LH", 0.1, ((date(2021, 4, 6), 0.2), (date(2021, 4, 7), 0.05))),
    })
    account = FuturesAccount(
        cash=1e6, spec_table=ContractSpecTable(config=config, load_config=False),
    )
    for side in ("long", "short"):
        account.open(CODE, side, 1, PRICE, trade_time=datetime(2021, 4, 2, 10))
    engine.context.portfolio.futures_account = account
    engine.context.portfolio.available_cash = account.cash
    engine.frequency = "tick"

    def snapshot(price):
        now = engine.context.current_dt
        return TickSnapshot(
            code=CODE, datetime=now, time=float(now.strftime("%Y%m%d%H%M%S")),
            current=price, high=price, low=price, volume=1, money=price,
            position=2, a1_p=price, a1_v=1, b1_p=price, b1_v=1,
        )

    tick = snapshot(PRICE + 100)
    engine._mark_futures_intraday(engine.context.current_dt, tick)
    assert account.margin == pytest.approx(86400.0)
    engine._tick_snapshots[CODE] = tick
    engine._mark_futures_intraday(engine.context.current_dt, tick)
    assert account.margin == pytest.approx(172800.0)
    assert account.cash == pytest.approx(1e6 - 172800.0)
    for position in account.iter_positions():
        assert position.margin_rate == pytest.approx(0.2)
        assert position.prev_settlement == PRICE
        assert position.today_amount == 1

    engine._tick_snapshots[CODE] = tick = snapshot(PRICE + 200)
    engine._mark_futures_intraday(engine.context.current_dt, tick)
    assert account.margin == pytest.approx(172800.0)
    assert account.cash == pytest.approx(1e6 - 172800.0)
    assert account.total_value == pytest.approx(1e6)

    engine.context.current_dt += timedelta(days=1)
    engine._tick_snapshots[CODE] = tick = snapshot(PRICE + 200)
    engine._mark_futures_intraday(engine.context.current_dt, tick)
    assert account.margin == pytest.approx(43200.0)
    assert account.cash == pytest.approx(1e6 - 43200.0)
    assert account.total_value == pytest.approx(1e6)
