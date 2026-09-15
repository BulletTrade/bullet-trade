"""
作者: BruceLee
文件职责:
    验证回测引擎的期货撮合与日终结算口径，所有期望值均可手工推导。

主要输入:
    BacktestEngine 的期货撮合、结算与最小报价步长方法，配合内存构造的组合与订单。

主要输出:
    pytest 断言结果，确认开平仓现金变动、保证金占用与释放、平今费率、
    超量平仓拒单、合约规格缺失拒单以及结算不改变权益这几条口径成立。

上下游关系:
    上游覆盖 `bullet_trade.core.engine` 的期货分支与 `bullet_trade.core.futures_account` 账本；
    下游保护期货策略回测的每日权益与成交记录不被股票口径污染。

关键环境或配置约定:
    测试只使用内存对象与内置合约规格表，不访问行情、结算价或任何外部数据源。
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.models import Context, Order, OrderStatus, Portfolio
from bullet_trade.core.settings import OrderCost, reset_settings, set_option, set_order_cost

LH = "LH2109.XDCE"
MULTIPLIER = 16.0
MARGIN_RATE = 0.14
CASH = 1_000_000.0


@pytest.fixture(autouse=True)
def _futures_settings():
    """为每个用例配置期货保证金率与手续费。

    Args:
        无。

    Returns:
        Iterator[None]: 用例结束后恢复全局设置。
    """

    reset_settings()
    set_option("futures_margin_rate", MARGIN_RATE)
    set_order_cost(
        OrderCost(
            open_commission=0.000023,
            close_commission=0.000023,
            close_today_commission=0.0023,
        ),
        type="futures",
    )
    yield
    reset_settings()


def _engine(cash: float = CASH) -> BacktestEngine:
    """构造带期货上下文的回测引擎。

    Args:
        cash: 组合可用资金。

    Returns:
        BacktestEngine: 已绑定 Context 的引擎实例。
    """

    engine = BacktestEngine()
    engine.context = Context(
        portfolio=Portfolio(
            total_value=cash,
            available_cash=cash,
            transferable_cash=cash,
            starting_cash=cash,
        ),
        current_dt=datetime(2021, 4, 6, 9, 30),
    )
    return engine


def _order(security: str, amount: int, side: str, **kwargs) -> Order:
    """构造期货订单。

    Args:
        security: 合约代码。
        amount: 手数，正数表示开仓、负数表示平仓。
        side: 'long' 或 'short'。
        **kwargs: 透传给 Order 的其他字段，如 close_today、pindex。

    Returns:
        Order: 未撮合的订单对象。
    """

    action = "open" if amount > 0 else "close"
    is_buy = (action == "open") == (side == "long")
    return Order(
        order_id=f"O-{security}-{action}-{side}",
        security=security,
        amount=abs(amount),
        status=OrderStatus.open,
        add_time=datetime(2021, 4, 6, 9, 30),
        is_buy=is_buy,
        action=action,
        side=side,
        **kwargs,
    )


def _fill(engine, order, lots, price, trade_price=None, current_dt=None) -> None:
    """按期货口径撮合一笔订单。

    Args:
        engine: 回测引擎。
        order: 待撮合订单。
        lots: 意图手数，正数。
        price: 撮合成交价。
        trade_price: 缺省与 price 相同的成交价别名，便于表达锁资价差异。
        current_dt: 成交时间，缺省为引擎当前时间。

    Returns:
        None: 结果写入订单与账本。
    """

    engine._execute_futures_fill(
        order=order,
        lots=lots,
        action=order.action,
        side=order.side,
        is_buy=order.is_buy,
        trade_price=float(price),
        fund_check_price=float(trade_price if trade_price is not None else price),
        current_dt=current_dt or engine.context.current_dt,
    )


def _settle_with(engine: BacktestEngine, prices: dict, trade_day: datetime) -> None:
    """用指定结算价执行一次日终结算。

    Args:
        engine: 回测引擎。
        prices: 合约到结算价的映射，未列出的合约视为缺结算价。
        trade_day: 结算日时间。

    Returns:
        None: 结果写入账本与组合现金。
    """

    engine._fetch_futures_settlement_prices = lambda securities, day: {
        security: prices[security] for security in securities if security in prices
    }
    engine._settle_futures_day(trade_day)


def test_open_short_charges_margin_and_commission_only() -> None:
    """开空 2 手生猪期货应只扣保证金与手续费，不扣合约全额，也不收印花税。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    order = _order(LH, 2, "short")

    _fill(engine, order, 2, 27000.0)

    margin = 2 * 27000.0 * MULTIPLIER * MARGIN_RATE
    commission = 2 * 27000.0 * MULTIPLIER * 0.000023
    portfolio = engine.context.portfolio
    account = portfolio.futures_account

    assert margin == pytest.approx(120960.0)
    assert commission == pytest.approx(19.87, abs=0.005)
    assert order.status == OrderStatus.filled
    # 开空为卖出
    assert order.is_buy is False
    assert account.margin == pytest.approx(margin)
    assert portfolio.available_cash == pytest.approx(CASH - margin - commission)
    assert portfolio.total_value == pytest.approx(CASH - commission)
    assert engine.trades[-1].tax == 0.0
    assert engine.trades[-1].amount == -2


def test_close_short_returns_margin_plus_variation() -> None:
    """结算后平空应释放结转过的保证金，并把相对上一结算价的盯市盈亏计入现金。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    _fill(engine, _order(LH, 2, "short"), 2, 27000.0)
    open_commission = 2 * 27000.0 * MULTIPLIER * 0.000023

    _settle_with(engine, {LH: 26000.0}, datetime(2021, 4, 6, 15, 0))
    settled_margin = 2 * 26000.0 * MULTIPLIER * MARGIN_RATE
    variation = (26000.0 - 27000.0) * 2 * MULTIPLIER * -1
    cash_after_settle = CASH - 2 * 27000.0 * MULTIPLIER * MARGIN_RATE - open_commission
    cash_after_settle += variation - (settled_margin - 2 * 27000.0 * MULTIPLIER * MARGIN_RATE)

    close_order = _order(LH, -2, "short")
    _fill(engine, close_order, 2, 26000.0, current_dt=datetime(2021, 4, 7, 9, 30))
    close_commission = 2 * 26000.0 * MULTIPLIER * 0.000023

    portfolio = engine.context.portfolio
    assert settled_margin == pytest.approx(116480.0)
    assert variation == pytest.approx(32000.0)
    assert cash_after_settle == pytest.approx(915500.13, abs=0.01)
    assert close_commission == pytest.approx(19.14, abs=0.005)
    assert close_order.status == OrderStatus.filled
    # 平空为买入
    assert close_order.is_buy is True
    assert portfolio.futures_account.positions == {}
    assert portfolio.available_cash == pytest.approx(
        cash_after_settle + settled_margin - close_commission
    )
    assert portfolio.total_value == pytest.approx(CASH + variation - open_commission - close_commission)
    assert engine.trades[-1].amount == 2


def test_over_close_is_rejected() -> None:
    """平仓手数超过持仓时应整单拒绝，不得部分成交或反向开仓。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    _fill(engine, _order(LH, 2, "short"), 2, 27000.0)
    cash_before = engine.context.portfolio.available_cash

    order = _order(LH, -3, "short")
    _fill(engine, order, 3, 26000.0)

    assert order.status == OrderStatus.rejected
    assert order.extra["rejection_reason"] == "over_close"
    assert engine.context.portfolio.available_cash == pytest.approx(cash_before)
    assert engine.context.portfolio.futures_account.get_position(LH, "short").amount == 2


def test_close_today_uses_today_commission_rate() -> None:
    """平今仓应按平今费率计费，昨仓部分仍按普通平仓费率。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    _fill(engine, _order(LH, 2, "long"), 2, 27000.0)
    position = engine.context.portfolio.futures_account.get_position(LH, "long")
    notional_per_lot = 26500.0 * MULTIPLIER

    assert position.today_amount == 2
    assert engine._split_futures_close_lots(position, 2, 0, True) == 2
    assert engine._calculate_futures_commission(
        engine._get_order_cost_config(LH), "close", 2, notional_per_lot, 2
    ) == pytest.approx(round(notional_per_lot * 2 * 0.0023 + 1e-12, 2), abs=0.01)

    order = _order(LH, -2, "long", close_today=True)
    _fill(engine, order, 2, 26500.0)

    assert order.status == OrderStatus.filled
    assert engine.trades[-1].commission == pytest.approx(notional_per_lot * 2 * 0.0023, abs=0.01)


def test_close_today_beyond_today_lots_is_rejected() -> None:
    """显式平今超过今仓手数时应拒单，而不是自动降级为平昨。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    _fill(engine, _order(LH, 2, "long"), 2, 27000.0)
    _settle_with(engine, {LH: 27000.0}, datetime(2021, 4, 6, 15, 0))

    order = _order(LH, -1, "long", close_today=True)
    _fill(engine, order, 1, 27100.0, current_dt=datetime(2021, 4, 7, 9, 30))

    assert order.status == OrderStatus.rejected
    assert order.extra["rejection_reason"] == "close_today_exceeds_today_lots"
    assert engine.context.portfolio.futures_account.get_position(LH, "long").amount == 2


def test_insufficient_margin_is_rejected_without_partial_fill() -> None:
    """可用资金连一手保证金都不够时应整单拒绝，不得留下零保证金持仓。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine(cash=50_000.0)
    order = _order(LH, 2, "short")

    _fill(engine, order, 2, 27000.0)

    assert order.status == OrderStatus.rejected
    assert order.extra["rejection_reason"] == "insufficient_margin"
    assert engine.context.portfolio.available_cash == pytest.approx(50_000.0)
    assert engine.context.portfolio.futures_account.positions == {}


def test_open_lots_shrink_to_affordable_margin() -> None:
    """保证金只够一手时应缩量成交，而不是拒单或透支现金。

    Args:
        无。

    Returns:
        None。
    """

    margin_per_lot = 27000.0 * MULTIPLIER * MARGIN_RATE
    engine = _engine(cash=margin_per_lot * 1.5)
    order = _order(LH, 2, "short")

    _fill(engine, order, 2, 27000.0)

    commission = 27000.0 * MULTIPLIER * 0.000023
    portfolio = engine.context.portfolio
    assert order.status == OrderStatus.filled
    assert order.amount == 1
    assert portfolio.futures_account.get_position(LH, "short").amount == 1
    assert portfolio.available_cash == pytest.approx(
        margin_per_lot * 1.5 - margin_per_lot - commission, abs=0.02
    )
    assert portfolio.available_cash > 0


def test_missing_contract_spec_is_rejected() -> None:
    """合约规格缺失时必须拒单，不得退化为乘数 1 或保证金率 0。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    order = _order("ZZ2109.XDCE", 1, "long")

    _fill(engine, order, 1, 10000.0)

    assert order.status == OrderStatus.rejected
    assert order.extra["rejection_reason"] == "futures_contract_spec_missing"
    assert engine.context.portfolio.available_cash == pytest.approx(CASH)
    assert engine.context.portfolio.futures_account.positions == {}


def test_missing_margin_rate_is_rejected() -> None:
    """未配置保证金率时开仓应拒单，而不是按零保证金放行。

    Args:
        无。

    Returns:
        None。
    """

    reset_settings()
    engine = _engine()
    order = _order(LH, 1, "long")

    _fill(engine, order, 1, 27000.0)

    assert order.status == OrderStatus.rejected
    assert order.extra["rejection_reason"] == "futures_contract_spec_missing"


def test_open_after_last_trading_day_is_rejected() -> None:
    """合约过最后交易日后开仓应拒单，已有持仓仍可平掉。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    _fill(engine, _order(LH, 1, "short"), 1, 27000.0)
    engine._futures_contract_end_date = lambda security: date(2021, 4, 6)

    open_order = _order(LH, 1, "short")
    _fill(engine, open_order, 1, 27000.0, current_dt=datetime(2021, 4, 7, 9, 30))

    assert open_order.status == OrderStatus.rejected
    assert open_order.extra["rejection_reason"] == "futures_contract_expired"
    assert engine.context.portfolio.futures_account.get_position(LH, "short").amount == 1

    close_order = _order(LH, -1, "short")
    _fill(engine, close_order, 1, 26800.0, current_dt=datetime(2021, 4, 7, 9, 31))
    assert close_order.status == OrderStatus.filled


def test_settlement_does_not_change_equity() -> None:
    """日终结算只搬动现金与保证金基准，账户权益应与结算前完全一致。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    _fill(engine, _order(LH, 2, "short"), 2, 27000.0)
    engine.context.portfolio.futures_account.mark_prices({LH: 26000.0})
    engine.context.portfolio.update_value()
    equity_before = engine.context.portfolio.total_value

    _settle_with(engine, {LH: 26000.0}, datetime(2021, 4, 6, 15, 0))

    portfolio = engine.context.portfolio
    assert equity_before == pytest.approx(1_000_000.0 - 19.87 + 32_000.0, abs=0.01)
    assert portfolio.total_value == pytest.approx(equity_before)
    assert portfolio.futures_account.margin == pytest.approx(2 * 26000.0 * MULTIPLIER * MARGIN_RATE)
    assert portfolio.futures_account.get_position(LH, "short").prev_settlement == pytest.approx(26000.0)


def test_missing_settlement_price_is_reported_not_substituted() -> None:
    """缺结算价时保持上一基准并上报，不得用收盘价顶替。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    _fill(engine, _order(LH, 2, "short"), 2, 27000.0)
    cash_before = engine.context.portfolio.available_cash

    _settle_with(engine, {}, datetime(2021, 4, 6, 15, 0))

    position = engine.context.portfolio.futures_account.get_position(LH, "short")
    assert engine.context.portfolio.available_cash == pytest.approx(cash_before)
    assert position.prev_settlement == pytest.approx(27000.0)
    assert position.margin_held == pytest.approx(2 * 27000.0 * MULTIPLIER * MARGIN_RATE)


def test_delivery_returns_held_margin_at_settlement() -> None:
    """最后交易日到期了结应按结算价盯市并退回实际占用保证金。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    _fill(engine, _order(LH, 1, "long"), 1, 27000.0)
    engine.context.portfolio.futures_account.get_position(LH, "long").end_date = date(2021, 4, 6)
    cash_before = engine.context.portfolio.available_cash

    _settle_with(engine, {LH: 27500.0}, datetime(2021, 4, 6, 15, 0))

    account = engine.context.portfolio.futures_account
    variation = (27500.0 - 27000.0) * 1 * MULTIPLIER
    assert account.positions == {}
    assert engine.context.portfolio.available_cash == pytest.approx(
        cash_before + variation + 27000.0 * MULTIPLIER * MARGIN_RATE
    )
    assert engine.context.portfolio.total_value == pytest.approx(CASH - 27000.0 * MULTIPLIER * 0.000023 + variation)


def test_futures_tick_step_comes_from_contract_spec() -> None:
    """期货最小报价步长取合约规格表，不受行情缺省的两位小数口径影响。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()

    assert engine._tick_step_for_security(LH) == pytest.approx(5.0)
    assert engine._round_to_tick(27002.0, LH) == pytest.approx(27000.0)
    assert engine._tick_step_for_security("600000.XSHG") == pytest.approx(0.01)


def test_futures_cost_config_never_borrows_stock_rates() -> None:
    """期货必须路由到期货费用口径，成交记录不含印花税。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    cost = engine._get_order_cost_config(LH)

    assert cost.open_commission == pytest.approx(0.000023)
    assert cost.close_commission == pytest.approx(0.000023)
    assert cost.close_today_commission == pytest.approx(0.0023)

    order = _order(LH, 1, "long")
    _fill(engine, order, 1, 27000.0)
    assert order.status == OrderStatus.filled
    assert all(trade.tax == 0.0 for trade in engine.trades)


def test_unconfigured_futures_cost_defaults_to_zero() -> None:
    """策略未配置期货费用时按零成本撮合，而不是借用股票的佣金与印花税。

    Args:
        无。

    Returns:
        None。
    """

    reset_settings()
    set_option("futures_margin_rate", MARGIN_RATE)
    engine = _engine()
    cost = engine._get_order_cost_config(LH)

    assert cost.open_commission == 0.0
    assert cost.close_commission == 0.0
    assert cost.open_tax == 0.0
    assert cost.close_tax == 0.0
    assert cost.min_commission == 0.0

    order = _order(LH, 1, "long")
    _fill(engine, order, 1, 27000.0)

    assert order.status == OrderStatus.filled
    assert engine.trades[-1].commission == 0.0
    assert engine.context.portfolio.available_cash == pytest.approx(
        CASH - 27000.0 * MULTIPLIER * MARGIN_RATE
    )


def test_per_product_margin_rate_beats_global_setting() -> None:
    """set_option('futures_margin_rate.<品种>') 应优先于全局设置。

    点号键只写进 options 字典，引擎必须展开后交给规格表，
    否则按品种降低保证金率的策略会被全局费率盖掉。

    Args:
        无。

    Returns:
        None。
    """

    reset_settings()
    set_option("futures_margin_rate", 0.15)
    set_option("futures_margin_rate.LH", 0.03)
    set_order_cost(
        OrderCost(
            open_commission=0.000023,
            close_commission=0.000023,
            close_today_commission=0.0023,
        ),
        type="futures",
    )
    engine = _engine()
    engine._ensure_futures_account()

    assert engine._futures_margin_rate_by_product() == {"LH": 0.03}
    assert engine._futures_spec_table.margin_rate(LH) == 0.03

    order = _order(LH, 1, "long")
    _fill(engine, order, 1, 27000.0)

    # 单手保证金 = 27000 × 16 × 0.03 = 12960
    assert engine.context.portfolio.available_cash == pytest.approx(
        CASH - 27000.0 * MULTIPLIER * 0.03 - 27000.0 * MULTIPLIER * 0.000023
    )


def test_invalid_per_product_margin_rate_entries_are_ignored() -> None:
    """非字母品种段、非数值与非正的保证金率配置都应被忽略而非报错。

    Args:
        无。

    Returns:
        None。
    """

    reset_settings()
    set_option("futures_margin_rate", MARGIN_RATE)
    set_option("futures_margin_rate.LH", 0.03)
    set_option("futures_margin_rate.12", 0.05)
    set_option("futures_margin_rate.CU", "abc")
    set_option("futures_margin_rate.RB", 0)
    engine = _engine()

    assert engine._futures_margin_rate_by_product() == {"LH": 0.03}
    engine._ensure_futures_account()
    assert engine._futures_spec_table.margin_rate(LH) == 0.03
    assert engine._futures_spec_table.margin_rate("RB2110.XSGE") == MARGIN_RATE
