"""
作者: BruceLee
文件职责:
    验证策略兼容面：子账户声明、订单状态口径别名、订单成交字段与当日开盘价。

主要输入:
    `bullet_trade.core.settings` 的子账户配置接口、`bullet_trade.core.engine` 的
    子账户视图重建与股票/期货撮合分支、`bullet_trade.compat.jqdata` 的状态别名，
    以及 `bullet_trade.data.api.BacktestCurrentData` 的行情快照构造。

主要输出:
    pytest 断言结果，确认声明式子账户能建出对应视图、原生订单状态语义未被改动、
    成交后订单能读出均价与费用、当日开盘价只来自日线 bar。

上下游关系:
    上游覆盖 `bullet_trade.core.settings`、`bullet_trade.core.engine`、
    `bullet_trade.core.models`、`bullet_trade.compat.jqdata` 与 `bullet_trade.data.api`；
    下游保护依赖这些字段编写的策略在回测中读到与文档一致的值。

关键环境或配置约定:
    测试只使用内存对象、内置合约规格表与打桩的行情返回，不访问任何外部数据源。
"""

from __future__ import annotations

import datetime

import pandas as pd
import pytest

from bullet_trade.compat.jqdata import OrderStatus as CompatOrderStatus
from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.models import (
    Context,
    Order,
    OrderStatus,
    Portfolio,
    Position,
    SecurityUnitData,
    SubPortfolio,
)
from bullet_trade.core.orders import clear_order_queue, order
from bullet_trade.core.settings import (
    OrderCost,
    SubPortfolioConfig,
    get_subportfolio_configs,
    reset_settings,
    set_option,
    set_order_cost,
    set_subportfolios,
)
from bullet_trade.data import api as data_api

LH = "LH2109.XDCE"
STOCK = "000001.XSHE"
CASH = 1_000_000.0
MARGIN_RATE = 0.14


@pytest.fixture(autouse=True)
def _clean_settings():
    """每个用例前后重置全局设置与订单队列。

    Args:
        无。

    Returns:
        Iterator[None]: 用例结束后恢复全局状态。
    """

    reset_settings()
    clear_order_queue()
    yield
    clear_order_queue()
    reset_settings()


def _context(cash: float = CASH, current_dt=None) -> Context:
    """构造最小回测上下文。

    Args:
        cash: 组合可用资金。
        current_dt: 当前时间，缺省为 2021-04-06 09:30。

    Returns:
        Context: 已绑定 Portfolio 的上下文。
    """

    return Context(
        portfolio=Portfolio(
            total_value=cash,
            available_cash=cash,
            transferable_cash=cash,
            starting_cash=cash,
        ),
        current_dt=current_dt or datetime.datetime(2021, 4, 6, 9, 30),
    )


def _engine(context: Context | None = None) -> BacktestEngine:
    """构造带上下文的回测引擎。

    Args:
        context: 指定上下文，缺省按 CASH 新建。

    Returns:
        BacktestEngine: 已绑定 Context 的引擎实例。
    """

    engine = BacktestEngine()
    engine.context = context or _context()
    return engine


# ---------------------------------------------------------------------------
# 子账户声明
# ---------------------------------------------------------------------------


def test_set_subportfolios_accepts_config_and_dict() -> None:
    """配置对象与字典都应被规范化成同一种子账户配置。

    Args:
        无。

    Returns:
        None。
    """

    set_subportfolios(
        [SubPortfolioConfig(cash=600000.0, type="futures"), {"cash": 400000.0, "type": "stock"}]
    )

    configs = get_subportfolio_configs()
    assert [c.type for c in configs] == ["futures", "stock"]
    assert [c.cash for c in configs] == [600000.0, 400000.0]


def test_set_subportfolios_rejects_unsupported_element() -> None:
    """无法解释的元素应立即报错，而不是静默生成默认子账户。

    Args:
        无。

    Returns:
        None。
    """

    with pytest.raises(TypeError):
        set_subportfolios(["futures"])


def test_reset_settings_clears_subportfolios() -> None:
    """重置设置应清空子账户声明，避免跨回测串味。

    Args:
        无。

    Returns:
        None。
    """

    set_subportfolios([SubPortfolioConfig(cash=CASH, type="futures")])
    reset_settings()

    assert get_subportfolio_configs() == []


def test_apply_subportfolio_config_builds_futures_view() -> None:
    """声明 futures 子账户后，组合视图应能按类型读到该子账户。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    set_subportfolios([SubPortfolioConfig(cash=CASH, type="futures")])

    engine._apply_subportfolio_config()

    subportfolios = engine.context.portfolio.subportfolios
    assert list(subportfolios) == ["futures"]
    assert subportfolios["futures"].type == "futures"
    assert subportfolios["futures"].available_cash == CASH
    assert engine.context.portfolio.available_cash == CASH


def test_apply_subportfolio_config_keeps_default_without_declaration() -> None:
    """未声明子账户时应保持默认 stock 视图，历史行为不变。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()

    engine._apply_subportfolio_config()

    assert list(engine.context.portfolio.subportfolios) == ["stock"]


def test_apply_subportfolio_config_dedupes_repeated_type() -> None:
    """同一类型声明多次时应各自保留一个键，不互相覆盖。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    set_subportfolios(
        [
            SubPortfolioConfig(cash=300000.0, type="futures"),
            SubPortfolioConfig(cash=700000.0, type="futures"),
        ]
    )

    engine._apply_subportfolio_config()

    subportfolios = engine.context.portfolio.subportfolios
    assert list(subportfolios) == ["futures", "futures_1"]
    assert subportfolios["futures_1"].available_cash == 700000.0


def test_apply_subportfolio_config_preserves_existing_positions() -> None:
    """重建子账户视图时不应丢掉同类型子账户已有持仓。

    Args:
        无。

    Returns:
        None。
    """

    engine = _engine()
    engine.context.portfolio.subportfolios["futures"] = SubPortfolio(type="futures")
    engine.context.portfolio.subportfolios["futures"].positions[STOCK] = Position(
        security=STOCK, total_amount=100, closeable_amount=100, avg_cost=10.0
    )
    set_subportfolios([SubPortfolioConfig(cash=CASH, type="futures")])

    engine._apply_subportfolio_config()

    assert "futures" in engine.context.portfolio.subportfolios
    assert engine.context.portfolio.subportfolios["futures"].positions[STOCK].total_amount == 100


def test_apply_subportfolio_config_warns_when_cash_mismatch(monkeypatch) -> None:
    """声明现金与组合现金池不一致时应给出告警，且不改动现金池。

    Args:
        monkeypatch: pytest 夹具，用于替换引擎日志对象。

    Returns:
        None。
    """

    engine = _engine()
    warnings: list[str] = []
    monkeypatch.setattr(
        "bullet_trade.core.engine.log",
        type("StubLog", (), {"warning": staticmethod(lambda msg, *args: warnings.append(msg % args))}),
    )
    set_subportfolios([SubPortfolioConfig(cash=CASH / 2, type="futures")])

    engine._apply_subportfolio_config()

    assert engine.context.portfolio.available_cash == CASH
    assert len(warnings) == 1
    assert "子账户声明现金合计" in warnings[0]


# ---------------------------------------------------------------------------
# 订单状态口径
# ---------------------------------------------------------------------------


def test_native_order_status_semantics_unchanged() -> None:
    """原生枚举里 held 仍是挂起，与 filled 是两个不同状态。

    Args:
        无。

    Returns:
        None。
    """

    assert OrderStatus.held.value == "held"
    assert OrderStatus.filled.value == "filled"
    assert OrderStatus.held is not OrderStatus.filled


def test_compat_order_status_held_maps_to_filled() -> None:
    """兼容口径下 held 表示订单已完成，应等于引擎写入的 filled。

    Args:
        无。

    Returns:
        None。
    """

    filled_order = Order(order_id="o1", security=STOCK, amount=100)
    filled_order.status = OrderStatus.filled

    assert CompatOrderStatus.held is OrderStatus.filled
    assert filled_order.status == CompatOrderStatus.held
    assert CompatOrderStatus.canceled is OrderStatus.canceled
    assert CompatOrderStatus.rejected is OrderStatus.rejected


def test_compat_order_status_does_not_confuse_native_held() -> None:
    """挂起中的订单在兼容口径下不应被当成已完成。

    Args:
        无。

    Returns:
        None。
    """

    held_order = Order(order_id="o2", security=STOCK, amount=100)
    held_order.status = OrderStatus.held

    assert held_order.status != CompatOrderStatus.held


# ---------------------------------------------------------------------------
# 订单成交字段：期货
# ---------------------------------------------------------------------------


def _futures_engine() -> BacktestEngine:
    """构造带期货费率与保证金率的引擎。

    Args:
        无。

    Returns:
        BacktestEngine: 已配置期货口径的引擎实例。
    """

    set_option("futures_margin_rate", MARGIN_RATE)
    set_order_cost(
        OrderCost(
            open_commission=0.000023,
            close_commission=0.000023,
            close_today_commission=0.0023,
        ),
        type="futures",
    )
    return _engine()


def _futures_order(security: str, amount: int, side: str) -> Order:
    """构造期货订单。

    Args:
        security: 合约代码。
        amount: 手数，正数开仓、负数平仓。
        side: 'long' 或 'short'。

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
        add_time=datetime.datetime(2021, 4, 6, 9, 30),
        is_buy=is_buy,
        action=action,
        side=side,
    )


def _fill_futures(engine: BacktestEngine, futures_order: Order, lots: int, price: float) -> None:
    """按期货口径撮合一笔订单。

    Args:
        engine: 回测引擎。
        futures_order: 待撮合订单。
        lots: 手数，正数。
        price: 成交价。

    Returns:
        None: 结果写入订单与账本。
    """

    engine._execute_futures_fill(
        order=futures_order,
        lots=lots,
        action=futures_order.action,
        side=futures_order.side,
        is_buy=futures_order.is_buy,
        trade_price=float(price),
        fund_check_price=float(price),
        current_dt=engine.context.current_dt,
    )


def test_futures_open_order_records_avg_cost_and_commission() -> None:
    """期货开仓成交后，订单均价应等于成交价，费用应等于按费率算出的手续费。

    Args:
        无。

    Returns:
        None。
    """

    engine = _futures_engine()
    open_order = _futures_order(LH, 2, "long")

    _fill_futures(engine, open_order, 2, 27000.0)

    assert open_order.status == OrderStatus.filled
    assert open_order.avg_cost == 27000.0
    # 27000 * 16 吨/手 * 2 手 * 0.000023 = 19.872，按分四舍五入为 19.87
    assert open_order.commission == pytest.approx(19.87, abs=0.01)


def test_futures_close_order_records_pre_trade_open_price() -> None:
    """期货平仓成交后，订单均价应是平仓前的持仓开仓价，而不是平仓成交价。

    Args:
        无。

    Returns:
        None。
    """

    engine = _futures_engine()
    _fill_futures(engine, _futures_order(LH, 2, "long"), 2, 27000.0)
    close_order = _futures_order(LH, -1, "long")

    _fill_futures(engine, close_order, 1, 28000.0)

    assert close_order.status == OrderStatus.filled
    assert close_order.avg_cost == 27000.0
    assert close_order.price == 28000.0
    assert close_order.commission > 0.0


def test_strategy_style_order_clone_has_no_missing_field() -> None:
    """按策略常见写法克隆订单字典时，不应出现缺字段。

    Args:
        无。

    Returns:
        None。
    """

    engine = _futures_engine()
    open_order = _futures_order(LH, 2, "long")
    _fill_futures(engine, open_order, 2, 27000.0)

    cloned = {
        "status": open_order.status,
        "add_time": open_order.add_time,
        "is_buy": open_order.is_buy,
        "amount": open_order.amount,
        "filled": open_order.filled,
        "security": open_order.security,
        "price": open_order.price,
        "avg_cost": open_order.avg_cost,
        "side": open_order.side,
        "action": open_order.action,
        "commission": open_order.commission,
    }

    assert cloned["status"] == CompatOrderStatus.held
    assert cloned["add_time"] == datetime.datetime(2021, 4, 6, 9, 30)
    assert cloned["is_buy"] is True
    assert cloned["amount"] == 2
    assert cloned["filled"] == 2
    assert cloned["security"] == LH
    assert cloned["price"] == 27000.0
    assert cloned["avg_cost"] == 27000.0
    assert cloned["side"] == "long"
    assert cloned["action"] == "open"
    assert cloned["commission"] == pytest.approx(19.87, abs=0.01)


# ---------------------------------------------------------------------------
# 订单成交字段：股票
# ---------------------------------------------------------------------------


def _stock_engine(monkeypatch, *, price: float = 10.0, cash: float = 200000.0) -> BacktestEngine:
    """构造最小股票撮合环境。

    Args:
        monkeypatch: pytest 夹具，用于打桩行情与撮合价格。
        price: 当前价与成交价。
        cash: 组合可用资金。

    Returns:
        BacktestEngine: 可直接调用 _process_orders 的引擎。
    """

    engine = BacktestEngine()
    engine.context = _context(cash=cash)
    engine.start_total_value = cash

    monkeypatch.setattr(
        "bullet_trade.core.orders._trigger_order_processing", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        "bullet_trade.data.api.get_current_data",
        lambda: {
            STOCK: SecurityUnitData(
                security=STOCK,
                last_price=price,
                high_limit=price * 1.1,
                low_limit=price * 0.9,
                paused=False,
            )
        },
    )
    monkeypatch.setattr("bullet_trade.core.engine.get_security_info", lambda _security: {})
    monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda _s, _dt, _fq: price)
    monkeypatch.setattr(engine, "_apply_slippage_price", lambda p, _is_buy, _security: p)
    monkeypatch.setattr(engine, "_infer_security_category", lambda _s, info=None: "stock")
    monkeypatch.setattr(engine, "_infer_tplus_from_info", lambda info: 0)
    return engine


def _reprice(monkeypatch, engine: BacktestEngine, price: float) -> None:
    """把撮合环境改到新的价格。

    Args:
        monkeypatch: pytest 夹具。
        engine: 回测引擎。
        price: 新的当前价与成交价。

    Returns:
        None。
    """

    monkeypatch.setattr(
        "bullet_trade.data.api.get_current_data",
        lambda: {
            STOCK: SecurityUnitData(
                security=STOCK,
                last_price=price,
                high_limit=price * 1.1,
                low_limit=price * 0.9,
                paused=False,
            )
        },
    )
    monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda _s, _dt, _fq: price)


def test_stock_buy_order_records_avg_cost_and_commission(monkeypatch) -> None:
    """股票买入成交后，订单均价应等于成交价，费用应含最低佣金。

    Args:
        monkeypatch: pytest 夹具。

    Returns:
        None。
    """

    engine = _stock_engine(monkeypatch, price=10.0)

    buy_order = order(STOCK, 100)
    engine._process_orders(engine.context.current_dt)

    assert buy_order.status == OrderStatus.filled
    assert buy_order.avg_cost == 10.0
    # 100 * 10 = 1000 元，万三佣金 0.3 元低于最低佣金 5 元
    assert buy_order.commission == pytest.approx(5.0, abs=0.01)


def test_stock_sell_order_records_pre_trade_avg_cost(monkeypatch) -> None:
    """股票卖出成交后，订单均价应是卖出前的持仓成本，费用应含印花税。

    Args:
        monkeypatch: pytest 夹具。

    Returns:
        None。
    """

    engine = _stock_engine(monkeypatch, price=10.0)
    order(STOCK, 100)
    engine._process_orders(engine.context.current_dt)
    engine.context.portfolio.positions[STOCK].closeable_amount = 100
    clear_order_queue()

    _reprice(monkeypatch, engine, 12.0)
    sell_order = order(STOCK, -100)
    engine._process_orders(engine.context.current_dt)

    assert sell_order.status == OrderStatus.filled
    assert sell_order.price == 12.0
    # 均价是建仓成本 10 元，不是卖出成交价 12 元
    assert sell_order.avg_cost == 10.0
    # 卖出 1200 元：佣金取最低 5 元，印花税千分之一 1.2 元
    assert sell_order.commission == pytest.approx(6.2, abs=0.01)


# ---------------------------------------------------------------------------
# 当日开盘价
# ---------------------------------------------------------------------------


def test_security_unit_data_day_open_defaults_to_zero() -> None:
    """行情源没给开盘价时，day_open 应是 0.0 而不是最新价。

    Args:
        无。

    Returns:
        None。
    """

    data = SecurityUnitData(security=STOCK, last_price=10.0)

    assert data.day_open == 0.0


def _current_data(monkeypatch, frame: pd.DataFrame, current_dt) -> data_api.BacktestCurrentData:
    """构造打桩行情返回的 BacktestCurrentData。

    Args:
        monkeypatch: pytest 夹具，用于替换行情拉取与涨跌停兜底。
        frame: 打桩返回的价格表。
        current_dt: 当前时间，决定走日线还是分钟窗口。

    Returns:
        BacktestCurrentData: 可直接下标访问的行情容器。
    """

    monkeypatch.setattr(
        data_api,
        "_call_provider_get_price_with_security_fallback",
        lambda **kwargs: frame,
    )
    monkeypatch.setattr(
        data_api, "_apply_limit_fallback", lambda *args, **kwargs: (0.0, 0.0)
    )
    return data_api.BacktestCurrentData(_context(current_dt=current_dt))


def test_current_data_day_open_comes_from_daily_row(monkeypatch) -> None:
    """日线窗口下 day_open 应取当日 bar 的开盘价。

    Args:
        monkeypatch: pytest 夹具。

    Returns:
        None。
    """

    frame = pd.DataFrame(
        [
            {
                "time": pd.Timestamp("2021-06-08 00:00:00"),
                "open": 26800.0,
                "close": 27000.0,
            }
        ]
    )
    current_data = _current_data(monkeypatch, frame, datetime.datetime(2021, 6, 8, 9, 0))

    data = current_data[LH]

    assert data.day_open == 26800.0
    assert data.last_price == 27000.0


def test_current_data_day_open_not_taken_from_minute_row(monkeypatch) -> None:
    """分钟窗口下拿到的 open 是当分钟开盘价，不能冒充当日开盘价。

    Args:
        monkeypatch: pytest 夹具。

    Returns:
        None。
    """

    frame = pd.DataFrame(
        [
            {
                "time": pd.Timestamp("2021-06-08 10:00:00"),
                "open": 26950.0,
                "close": 26980.0,
            }
        ]
    )
    current_data = _current_data(monkeypatch, frame, datetime.datetime(2021, 6, 8, 10, 0))

    assert current_data[LH].day_open == 0.0


# ---------------------------------------------------------------------------
# 策略文件加载
# ---------------------------------------------------------------------------

_SYNTHETIC_STRATEGY = """from jqdata import *


def initialize(context):
    set_subportfolios(
        [SubPortfolioConfig(cash=context.portfolio.starting_cash, type='futures')]
    )
    g.held_means_filled = OrderStatus.held == OrderStatus.filled


def handle_tick(context, tick):
    g.tick_count = getattr(g, 'tick_count', 0) + 1
"""


def test_strategy_file_initializes_with_compat_surface(tmp_path) -> None:
    """按兼容口径编写的策略文件应能加载、初始化并建出子账户视图。

    Args:
        tmp_path: pytest 夹具，用于存放合成策略文件。

    Returns:
        None。
    """

    from bullet_trade.core.globals import g

    strategy_file = tmp_path / "synthetic_strategy.py"
    strategy_file.write_text(_SYNTHETIC_STRATEGY, encoding="utf-8")

    engine = BacktestEngine(strategy_file=str(strategy_file), frequency="tick")
    engine.load_strategy()
    engine.context = _context()
    engine.initialize_func(engine.context)

    assert g.held_means_filled is True
    assert callable(engine.handle_tick_func)

    engine._apply_subportfolio_config()

    subportfolios = engine.context.portfolio.subportfolios
    assert list(subportfolios) == ["futures"]
    assert subportfolios["futures"].available_cash == CASH
