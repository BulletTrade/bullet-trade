from datetime import datetime

import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.models import Context, OrderStatus, Portfolio, Position, SecurityUnitData
from bullet_trade.core.orders import (
    LimitOrderStyle,
    MarketOrderStyle,
    clear_order_queue,
    order,
    order_target,
    order_target_value,
)
from bullet_trade.core.settings import (
    OrderCost,
    get_settings,
    reset_settings,
    set_option,
    set_order_cost,
)


def cost(rate=0.0, minimum=0.0, tax=0.0):
    return OrderCost(
        open_tax=tax, close_tax=tax, open_commission=rate,
        close_commission=rate, min_commission=minimum,
    )


@pytest.fixture(autouse=True)
def reset_state():
    reset_settings()
    clear_order_queue()
    yield
    clear_order_queue()
    reset_settings()


@pytest.fixture
def make_engine(monkeypatch):
    def build(cash=1000.0, price=1.0, category="fund", subtype="etf"):
        code = "510300.XSHG"
        info = {"type": "fund", "subtype": subtype, "category": category, "tplus": 0}
        quote = SecurityUnitData(
            security=code, last_price=price,
            high_limit=price * 1.1, low_limit=price * 0.9, paused=False,
        )
        engine = BacktestEngine(initial_cash=cash)
        engine.context = Context(
            portfolio=Portfolio(available_cash=cash, starting_cash=cash, total_value=cash),
            current_dt=datetime(2024, 1, 2, 10, 0),
        )
        monkeypatch.setattr("bullet_trade.core.orders._trigger_order_processing", lambda *a, **k: None)
        monkeypatch.setattr("bullet_trade.data.api.get_current_data", lambda: {code: quote})
        monkeypatch.setattr("bullet_trade.data.api.get_security_info", lambda _: info)
        monkeypatch.setattr("bullet_trade.core.engine.get_security_info", lambda _: info)
        monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda *a: quote.last_price)
        monkeypatch.setattr(engine, "_apply_slippage_price", lambda price, *a: price)
        engine.test_code = code
        engine.test_quote = quote
        return engine
    return build


def execute(engine, requested):
    engine._process_orders(engine.context.current_dt)
    return requested


def add_position(engine, total, closeable=None):
    position = Position(
        security=engine.test_code, total_amount=total,
        closeable_amount=total if closeable is None else closeable, avg_cost=1,
    )
    position.update_price(engine.test_quote.last_price)
    engine.context.portfolio.positions[engine.test_code] = position
    engine.context.portfolio.update_value()
    return position


@pytest.mark.parametrize("side,boundary,reason", [
    ("buy", 1.1, "price_at_upper_limit"),
    ("sell", 0.9, "price_at_lower_limit"),
])
def test_bar_order_does_not_assume_liquidity_at_price_limit(
    make_engine, side, boundary, reason,
):
    engine = make_engine()
    engine.test_quote.last_price = boundary
    if side == "sell":
        add_position(engine, 100)
        requested = order(engine.test_code, -100)
    else:
        requested = order(engine.test_code, 100)

    execute(engine, requested)

    assert requested.status == OrderStatus.canceled
    assert requested.amount == (100 if side == "buy" else -100)
    assert requested.filled == 0
    assert requested.extra["cancel_reason"] == reason
    assert engine.trades == []


def test_bar_order_with_unknown_price_limit_uses_existing_matching(make_engine):
    engine = make_engine()
    engine.test_quote.low_limit = 0.0
    engine.test_quote.last_price = 0.9
    add_position(engine, 100)

    requested = execute(engine, order(engine.test_code, -100))

    assert requested.status == OrderStatus.filled
    assert requested.filled == 100


def test_market_budget_ignores_implicit_live_protection(make_engine):
    engine = make_engine()
    engine._market_buy_percent = 0.09
    set_order_cost(cost(), type="fund")
    result = execute(engine, order(engine.test_code, 1000))
    assert result.filled == 1000
    assert result.extra["order_price"] == 1
    assert engine.context.portfolio.available_cash == 0


@pytest.mark.parametrize("style", [
    LimitOrderStyle(1.01),
    MarketOrderStyle(limit_price=1.01),
    MarketOrderStyle(buy_price_percent=0.01),
])
def test_explicit_price_protection_still_limits_budget(make_engine, style):
    engine = make_engine()
    set_order_cost(cost(), type="fund")
    result = execute(engine, order(engine.test_code, 1000, style=style))
    assert result.filled == 900
    assert result.extra["order_price"] == 1.01


@pytest.mark.parametrize("cash,expected", [(202.0, 200), (201.99, 100)])
def test_minimum_commission_is_a_floor_not_an_extra_charge(make_engine, cash, expected):
    engine = make_engine(cash=cash)
    set_order_cost(cost(rate=0.01, minimum=2), type="fund")
    result = execute(engine, order(engine.test_code, 200))
    assert result.filled == expected
    assert result.commission == 2
    assert engine.context.portfolio.available_cash == pytest.approx(cash - expected - 2)


def test_budget_accounts_for_tax_and_existing_locked_cash(make_engine):
    engine = make_engine(cash=204.0)
    engine.context.portfolio.locked_cash = 1
    set_order_cost(cost(rate=0.01, minimum=2, tax=0.01), type="fund")
    result = execute(engine, order(engine.test_code, 200))
    assert result.filled == 100
    assert result.commission == 3
    assert engine.context.portfolio.locked_cash == 1


def test_default_budget_does_not_spend_cash_needed_for_fees(make_engine):
    engine = make_engine(cash=20, price=0.2)
    set_order_cost(cost(rate=0.00073, tax=0.00017), type="fund")
    result = execute(engine, order(engine.test_code, 100))
    assert result.filled == 0
    assert engine.context.portfolio.available_cash == 20


@pytest.mark.parametrize("decimals,buy_fee,buy_tax", [(2, 0.01, 0), (None, 0.0146, 0.0034)])
def test_notional_budget_debits_fees_after_principal_check(make_engine, decimals, buy_fee, buy_tax):
    engine = make_engine(cash=20, price=0.2)
    set_option("equity_cash_budget", "notional")
    set_option("equity_cash_decimals", decimals)
    set_order_cost(cost(rate=0.00073, tax=0.00017), type="fund")
    result = execute(engine, order(engine.test_code, 100))
    assert result.filled == 100
    assert engine.trades[-1].commission == pytest.approx(buy_fee)
    assert engine.trades[-1].tax == pytest.approx(buy_tax)
    assert engine.context.portfolio.available_cash == pytest.approx(-buy_fee - buy_tax)
    engine.test_quote.last_price = 0.203
    result = execute(engine, order_target_value(engine.test_code, 0))
    assert result.filled == 100
    sell_fee = 0.01 if decimals == 2 else 20.3 * 0.00073
    sell_tax = 0 if decimals == 2 else 20.3 * 0.00017
    assert engine.trades[-1].commission == pytest.approx(sell_fee)
    assert engine.trades[-1].tax == pytest.approx(sell_tax)
    assert engine.context.portfolio.available_cash == pytest.approx(
        -buy_fee - buy_tax + 20.3 - sell_fee - sell_tax
    )
    assert result.extra["equity_cash_budget"] == "notional"
    assert result.extra["equity_cash_decimals"] is decimals


def test_notional_budget_cannot_borrow_principal(make_engine):
    engine = make_engine(cash=19.99, price=0.2)
    set_option("equity_cash_budget", "notional")
    set_option("equity_cash_decimals", None)
    set_order_cost(cost(), type="fund")
    assert execute(engine, order(engine.test_code, 100)).filled == 0
    assert engine.context.portfolio.available_cash == 19.99


@pytest.mark.parametrize("budget,decimals", [("fees_included", 2), ("notional", None)])
def test_equity_cash_policy_does_not_change_futures_protection(make_engine, monkeypatch, budget, decimals):
    engine = make_engine(cash=20, price=1, category="futures", subtype="futures")
    engine._market_buy_percent = 0.015
    set_option("equity_cash_budget", budget)
    set_option("equity_cash_decimals", decimals)
    monkeypatch.setattr(engine, "_ensure_futures_account", lambda: None)
    monkeypatch.setattr(engine, "_resolve_futures_daily_bar", lambda *a: (
        {"volume": 100, "low": 0.5, "high": 2.0}, None
    ))
    monkeypatch.setattr(engine, "_round_to_tick", lambda price, *a, **k: price)
    monkeypatch.setattr(
        "bullet_trade.core.engine.pricing.clamp_price_to_trade_bounds",
        lambda _code, price, *a: price,
    )
    captured = {}
    monkeypatch.setattr(engine, "_execute_futures_fill", lambda **kwargs: captured.update(kwargs))
    execute(engine, order(engine.test_code, 1))
    assert captured["fund_check_price"] == pytest.approx(1.015)
    assert captured["trade_price"] == 1
    assert captured["lots"] == 1


@pytest.mark.parametrize("decimals,expected", [(2, 0.02), (None, 0.0225)])
def test_distribution_uses_same_cash_precision(make_engine, decimals, expected):
    engine = make_engine(cash=0)
    position = add_position(engine, 3)
    set_option("equity_cash_decimals", decimals)
    engine._apply_cash_dividend(
        code=engine.test_code, pos=position, gross_div=0.0075, base_lot=1,
        tax_rate=0, sec_type="fund", split_ratio=1, pre_event_amount=3,
        eff_date=engine.context.current_dt.date(), portfolio=engine.context.portfolio,
    )
    assert engine.context.portfolio.available_cash == pytest.approx(expected)
    assert engine.events[-1]["cash_in"] == pytest.approx(expected)


@pytest.mark.parametrize("key,value", [
    ("equity_cash_budget", "borrow"), ("equity_cash_budget", None),
    ("equity_cash_decimals", 0), ("equity_cash_decimals", 2.0),
    ("equity_cash_decimals", True),
])
def test_cash_policy_rejects_invalid_values(key, value):
    with pytest.raises(ValueError):
        set_option(key, value)


def test_money_fund_default_and_explicit_parent_fee(make_engine):
    engine = make_engine(category="money_market_fund", subtype="mmf")
    assert engine._get_order_cost_config(engine.test_code).open_commission == 0
    configured = cost(rate=0.01, minimum=2)
    set_order_cost(configured, type="fund")
    assert engine._get_order_cost_config(engine.test_code) is configured


@pytest.mark.parametrize("subtype_first", [False, True])
@pytest.mark.parametrize("subtype", ["money_market_fund", "mmf", "money"])
def test_explicit_zero_subtype_precedes_parent_regardless_of_order(make_engine, subtype_first, subtype):
    engine = make_engine(category="money_market_fund", subtype="mmf")
    zero = cost()
    settings = [(cost(rate=0.01), "fund"), (zero, subtype)]
    if subtype_first:
        settings.reverse()
    for configured, category in settings:
        set_order_cost(configured, type=category)
    assert engine._get_order_cost_config(engine.test_code) is zero


def test_security_override_precedes_explicit_subtype(make_engine):
    engine = make_engine(category="money_market_fund", subtype="mmf")
    set_order_cost(cost(), type="money_market_fund")
    parent_override = cost(rate=0.02)
    set_order_cost(parent_override, type="fund", ref=engine.test_code)
    assert engine._get_order_cost_config(engine.test_code) is parent_override
    specific_override = cost(rate=0.03)
    set_order_cost(specific_override, type="mmf", ref=engine.test_code)
    assert engine._get_order_cost_config(engine.test_code) is specific_override


def test_fund_subtype_precedes_parent(make_engine):
    engine = make_engine()
    subtype = cost(rate=0.02)
    set_order_cost(subtype, type="etf")
    set_order_cost(cost(rate=0.01), type="fund")
    assert engine._get_order_cost_config(engine.test_code) is subtype


@pytest.mark.parametrize("use_reference", [False, True])
@pytest.mark.parametrize("aliases", [
    ("money_market_fund", "mmf", "money"),
    ("mmf", "money", "money_market_fund"),
    ("money", "money_market_fund", "mmf"),
])
def test_fee_aliases_use_latest_explicit_setting_without_changing_keys(make_engine, use_reference, aliases):
    engine = make_engine(category="money_market_fund", subtype="mmf")
    reference = engine.test_code if use_reference else None
    for alias in (*aliases, aliases[0]):
        configured = cost(rate=0.001)
        set_order_cost(configured, type=alias, ref=reference)
        assert engine._get_order_cost_config(engine.test_code) is configured
    configured_costs = (
        get_settings().order_cost_overrides if use_reference else get_settings().order_cost
    )
    for alias in aliases:
        key = f"{alias}_{reference}" if reference else alias
        assert key in configured_costs


def test_reset_removes_explicit_fees_and_cash_policy(make_engine):
    engine = make_engine(category="money_market_fund", subtype="mmf")
    set_order_cost(cost(rate=0.01), type="fund")
    set_order_cost(cost(rate=0.02), type="fund", ref=engine.test_code)
    set_option("equity_cash_budget", "notional")
    set_option("equity_cash_decimals", None)
    reset_settings()
    assert engine._get_order_cost_config(engine.test_code).open_commission == 0
    assert not get_settings().explicit_order_cost_types
    assert not get_settings().order_cost_overrides
    assert "equity_cash_budget" not in get_settings().options
    assert "equity_cash_decimals" not in get_settings().options
    assert not get_settings().order_cost_sequence


@pytest.mark.parametrize("total,closeable,expected,remaining", [
    (1307, 1307, 1307, 0), (7, 7, 7, 0), (1307, 307, 307, 1000),
])
def test_liquidation_includes_all_sellable_residual_shares(make_engine, total, closeable, expected, remaining):
    engine = make_engine(cash=0)
    set_order_cost(cost(minimum=2), type="fund")
    add_position(engine, total, closeable)
    result = execute(engine, order_target_value(engine.test_code, 0))
    assert result.filled == expected
    assert result.commission == 2
    position = engine.context.portfolio.positions.get(engine.test_code)
    assert (position.total_amount if position else 0) == remaining
    assert len(engine.trades) == 1
    if not remaining:
        assert result.status == OrderStatus.filled
        assert execute(engine, order_target_value(engine.test_code, 0)).filled == 0
        assert len(engine.trades) == 1


def test_partial_reduction_keeps_lot_constraint(make_engine):
    engine = make_engine(cash=0)
    add_position(engine, 1307)
    result = execute(engine, order_target(engine.test_code, 1000))
    assert result.filled == 300
    assert engine.context.portfolio.positions[engine.test_code].total_amount == 1007


def test_no_sellable_shares_prevents_liquidation(make_engine):
    engine = make_engine(cash=0)
    add_position(engine, 1307, 0)
    result = execute(engine, order_target_value(engine.test_code, 0))
    assert result.filled == 0
    assert result.status == OrderStatus.rejected
    assert engine.context.portfolio.positions[engine.test_code].total_amount == 1307
