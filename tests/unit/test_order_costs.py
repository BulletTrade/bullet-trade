from datetime import datetime
from typing import Dict

import pandas as pd
import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.globals import reset_globals
from bullet_trade.core.models import Context, Portfolio, Position
from bullet_trade.core.orders import order, order_value, clear_order_queue
from bullet_trade.core.runtime import set_current_engine
from bullet_trade.core.settings import reset_settings, set_slippage, FixedSlippage,PriceRelatedSlippage,StepRelatedSlippage
import importlib
from bullet_trade.data import api as data_api
from bullet_trade.data.providers.base import DataProvider


class OrderCostTestProvider(DataProvider):
    name = "order-cost-test"

    def __init__(self, prices: Dict[str, Dict[str, float]], info: Dict[str, Dict[str, str]]):
        self._prices = prices
        self._info = info

    def auth(self, *_, **__):
        return None

    def get_price(
        self,
        security,
        start_date=None,
        end_date=None,
        frequency="daily",
        fields=None,
        skip_paused=False,
        fq="pre",
        count=None,
        panel=True,
        fill_paused=True,
        pre_factor_ref_date=None,
        prefer_engine=False,
    ):
        price = self._prices[security]
        index = [pd.Timestamp(end_date) if end_date is not None else pd.Timestamp("2021-01-04 09:31:00")]
        data = {
            "open": [price["open"]],
            "close": [price["close"]],
            "high_limit": [price.get("high_limit", price["close"] * 1.1)],
            "low_limit": [price.get("low_limit", price["close"] * 0.9)],
            "paused": [0.0],
        }
        return pd.DataFrame(data, index=index)

    def get_trade_days(self, *_, **__):
        return []

    def get_all_securities(self, *_, **__):
        return pd.DataFrame()

    def get_index_stocks(self, *_, **__):
        return []

    def get_split_dividend(self, *_, **__):
        return []

    def get_security_info(self, security: str) -> Dict[str, str]:
        return self._info.get(security, {})


@pytest.fixture
def provider():
    prices = {
        "511880.XSHG": {"open": 1.00215, "close": 1.00215},
        "511010.XSHG": {"open": 102.1, "close": 102.1},
        "517520.XSHG": {"open": 1.2, "close": 1.2},
        "518880.XSHG": {"open": 6.2, "close": 6.2},
        "159949.XSHE": {"open": 1.2345, "close": 1.2345},
        "601318.XSHG": {"open": 77.9, "close": 77.9},
    }
    info = {
        "511880.XSHG": {"type": "fund", "subtype": "money_market_fund"},
        "511010.XSHG": {"type": "fund", "subtype": "etf"},
        "517520.XSHG": {"type": "fund", "subtype": "etf"},
        "518880.XSHG": {"type": "fund", "subtype": "etf"},
        "159949.XSHE": {"type": "fund", "subtype": "etf"},
        "601318.XSHG": {"type": "stock"},
    }
    test_provider = OrderCostTestProvider(prices, info)

    original_provider = data_api._provider  # type: ignore[attr-defined]
    original_auth_attempted = data_api._auth_attempted  # type: ignore[attr-defined]
    original_context = getattr(data_api, "_current_context", None)
    original_security_cache = data_api._security_info_cache.copy()  # type: ignore[attr-defined]

    data_api._provider = test_provider  # type: ignore[attr-defined]
    data_api._auth_attempted = True  # type: ignore[attr-defined]
    data_api._security_info_cache.clear()  # type: ignore[attr-defined]

    yield test_provider

    data_api._provider = original_provider  # type: ignore[attr-defined]
    data_api._auth_attempted = original_auth_attempted  # type: ignore[attr-defined]
    data_api._security_info_cache = original_security_cache  # type: ignore[attr-defined]
    data_api.set_current_context(original_context)


def _setup_engine(current_dt: datetime) -> BacktestEngine:
    # 重新加载 engine 模块，防止其他测试 monkeypatch 默认滑点逻辑
    import bullet_trade.core.engine as _engine_module
    _engine_module = importlib.reload(_engine_module)
    global BacktestEngine
    BacktestEngine = _engine_module.BacktestEngine
    # 恢复默认滑点计算方法，避免外部 monkeypatch 遗留
    BacktestEngine._calc_trade_price_with_default_slippage = (
        _engine_module.BacktestEngine._calc_trade_price_with_default_slippage  # type: ignore[attr-defined]
    )

    reset_globals()
    reset_settings()
    # 显式设置回默认滑点 0.00246（双边各一半）避免全局污染
    #set_slippage(FixedSlippage(0.00246))
    set_slippage(PriceRelatedSlippage(0.00246))
    clear_order_queue()

    engine = BacktestEngine(initial_cash=200000)
    portfolio = Portfolio(
        total_value=200000,
        available_cash=200000,
        transferable_cash=200000,
        locked_cash=0.0,
        starting_cash=200000,
    )
    context = Context(portfolio=portfolio, current_dt=current_dt)
    engine.context = context
    set_current_engine(engine)
    data_api.set_current_context(context)
    engine.trades.clear()
    return engine


def test_money_market_fund_buy_has_no_cost(provider):
    engine = _setup_engine(datetime(2021, 1, 5, 9, 31))

    order_value("511880.XSHG", 40000)
    assert engine.trades, "trade should be recorded"
    trade = engine.trades[-1]
    assert trade.commission == pytest.approx(0.0)
    assert trade.tax == pytest.approx(0.0)
    set_current_engine(None)
    data_api.set_current_context(None)
    clear_order_queue()


def test_etf_sell_skips_stamp_duty(provider):
    engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
    position = Position(security="159949.XSHE", total_amount=1000, closeable_amount=1000, avg_cost=1.20)
    position.update_price(1.2345)
    engine.context.portfolio.positions["159949.XSHE"] = position

    order("159949.XSHE", -1000)
    assert engine.trades, "trade should be recorded"
    trade = engine.trades[-1]
    assert trade.tax == pytest.approx(0.0)
    assert trade.commission == pytest.approx(5.0)
    set_current_engine(None)
    data_api.set_current_context(None)
    clear_order_queue()


def test_stock_sell_charges_stamp_duty(provider):
    engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
    position = Position(security="601318.XSHG", total_amount=600, closeable_amount=600, avg_cost=75.0)
    position.update_price(77.9)
    engine.context.portfolio.positions["601318.XSHG"] = position

    order("601318.XSHG", -600)
    assert engine.trades, "trade should be recorded"
    trade = engine.trades[-1]
    # 默认分类滑点 24.6bps 的单边一半将作用于卖出（聚宽默认）：-0.00123，并按 0.01 取整
    price_raw = 77.9 * (1 - 0.00246/2)
    price = float((__import__('decimal').Decimal(str(price_raw)).quantize(__import__('decimal').Decimal('0.01'), rounding=__import__('decimal').ROUND_HALF_UP)))
    expected_value = price * 600
    import decimal
    def fen(x):
        return float(decimal.Decimal(str(x)).quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP))
    assert trade.tax == pytest.approx(fen(expected_value * 0.001))
    assert trade.commission == pytest.approx(fen(expected_value * 0.0003))
    set_current_engine(None)
    data_api.set_current_context(None) 
    clear_order_queue()


def test_mmf_ignores_explicit_slippage(provider):
    engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
    # 显式设置较大的滑点，MMF 应忽略
    set_slippage(FixedSlippage(0.1))
    order_value("511880.XSHG", 10000)
    assert engine.trades, "trade should be recorded"
    trade = engine.trades[-1]
    # 成交价应等于行情价，且按 0.001 取整
    assert trade.price == pytest.approx(1.002)
    set_current_engine(None)
    data_api.set_current_context(None)
    clear_order_queue()


def test_tplus1_rollover_and_same_day_sell_limit(provider):
    # Day 1: buy 400 T+1 stock
    engine = _setup_engine(datetime(2021, 1, 4, 9, 31))
    order("601318.XSHG", 400)
    pos = engine.context.portfolio.positions["601318.XSHG"]
    assert pos.total_amount == 400
    assert pos.closeable_amount == 0  # T+1 当日不可卖

    # Rollover to Day 2
    engine.context.current_dt = datetime(2021, 1, 5, 9, 30)
    # 直接调用引擎内部解锁（测试便捷）
    engine._rollover_tplus_for_new_day()
    pos = engine.context.portfolio.positions["601318.XSHG"]
    assert pos.closeable_amount == 400

    # Day 2: buy another 200 today
    order("601318.XSHG", 200)
    pos = engine.context.portfolio.positions["601318.XSHG"]
    assert pos.total_amount == 600
    assert pos.closeable_amount == 400  # 今日买入的 200 仍不可卖

    # Try to sell 700; should only fill 400 (yesterday's)
    order("601318.XSHG", -700)
    trade = engine.trades[-1]
    assert trade.amount == -400
    set_current_engine(None)
    data_api.set_current_context(None)
    clear_order_queue()


@pytest.mark.parametrize("security", ("159949.XSHE", "517520.XSHG"))
def test_stock_etf_buy_is_sellable_only_after_rollover(provider, security):
    """验证普通股票 ETF 与 517520 买入当日不可卖、次交易日可卖。

    Args:
        provider: 注入固定行情与证券信息的测试数据源。
        security: 待验证的普通股票 ETF 代码。

    Returns:
        None: 回测买入、当日卖出限制和跨日解锁断言通过后结束。
    """

    engine = _setup_engine(datetime(2021, 1, 4, 9, 31))
    order(security, 200)
    position = engine.context.portfolio.positions[security]
    assert position.total_amount == 200
    assert position.closeable_amount == 0

    trades_after_buy = len(engine.trades)
    order(security, -200)
    assert len(engine.trades) == trades_after_buy
    assert position.total_amount == 200

    engine.context.current_dt = datetime(2021, 1, 5, 9, 30)
    engine._rollover_tplus_for_new_day()
    assert position.closeable_amount == 200

    order(security, -200)
    assert engine.trades[-1].amount == -200
    assert security not in engine.context.portfolio.positions
    set_current_engine(None)
    data_api.set_current_context(None)
    clear_order_queue()


@pytest.mark.parametrize("security", ("518880.XSHG", "511010.XSHG", "511880.XSHG"))
def test_explicit_tplus_zero_fund_is_sellable_on_buy_day(provider, security):
    """验证金、债和现金类明确 T+0 品种在回测买入当日仍可卖。

    Args:
        provider: 注入固定行情与证券信息的测试数据源。
        security: 待验证的明确 T+0 基金代码。

    Returns:
        None: 当日买卖成交断言通过后结束。
    """

    engine = _setup_engine(datetime(2021, 1, 4, 9, 31))
    order(security, 200)
    position = engine.context.portfolio.positions[security]
    assert position.closeable_amount == 200

    order(security, -200)
    assert engine.trades[-1].amount == -200
    assert security not in engine.context.portfolio.positions
    set_current_engine(None)
    data_api.set_current_context(None)
    clear_order_queue()


# =============================================================================
# 滑点类型测试：FixedSlippage / PriceRelatedSlippage / StepRelatedSlippage
# =============================================================================

class TestSlippageTypes:
    """测试三种滑点类型的买卖双向行为"""
    
    def _cleanup(self):
        """测试清理"""
        set_current_engine(None)
        data_api.set_current_context(None)
        clear_order_queue()

    def test_fixed_slippage_buy(self, provider):
        """FixedSlippage 买入：价格 + value/2"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        # 设置固定滑点 0.02 元（双边），单边 0.01 元
        set_slippage(FixedSlippage(0.02))
        
        order("601318.XSHG", 100)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        # 买入价格 = 77.9 + 0.02/2 = 77.91
        assert trade.price == pytest.approx(77.91)
        self._cleanup()

    def test_fixed_slippage_sell(self, provider):
        """FixedSlippage 卖出：价格 - value/2"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        position = Position(security="601318.XSHG", total_amount=100, closeable_amount=100, avg_cost=75.0)
        position.update_price(77.9)
        engine.context.portfolio.positions["601318.XSHG"] = position
        # 设置固定滑点 0.02 元（双边），单边 0.01 元
        set_slippage(FixedSlippage(0.02))
        
        order("601318.XSHG", -100)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        # 卖出价格 = 77.9 - 0.02/2 = 77.89
        assert trade.price == pytest.approx(77.89)
        self._cleanup()

    def test_price_related_slippage_buy(self, provider):
        """PriceRelatedSlippage 买入：价格 * (1 + ratio/2)"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        # 设置比例滑点 1%（双边），单边 0.5%
        set_slippage(PriceRelatedSlippage(0.01))
        
        order("601318.XSHG", 100)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        # 买入价格 = 77.9 * (1 + 0.01/2) = 77.9 * 1.005 = 78.2895 ≈ 78.29
        expected_price = 77.9 * (1 + 0.01 / 2)
        import decimal
        expected_price = float(decimal.Decimal(str(expected_price)).quantize(
            decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP))
        assert trade.price == pytest.approx(expected_price)
        self._cleanup()

    def test_price_related_slippage_sell(self, provider):
        """PriceRelatedSlippage 卖出：价格 * (1 - ratio/2)"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        position = Position(security="601318.XSHG", total_amount=100, closeable_amount=100, avg_cost=75.0)
        position.update_price(77.9)
        engine.context.portfolio.positions["601318.XSHG"] = position
        # 设置比例滑点 1%（双边），单边 0.5%
        set_slippage(PriceRelatedSlippage(0.01))
        
        order("601318.XSHG", -100)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        # 卖出价格 = 77.9 * (1 - 0.01/2) = 77.9 * 0.995 = 77.5105 ≈ 77.51
        expected_price = 77.9 * (1 - 0.01 / 2)
        import decimal
        expected_price = float(decimal.Decimal(str(expected_price)).quantize(
            decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP))
        assert trade.price == pytest.approx(expected_price)
        self._cleanup()

    def test_step_related_slippage_buy(self, provider):
        """StepRelatedSlippage 买入：价格 + steps * tick / 2"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        # 设置跳数滑点 4 跳（双边），单边 2 跳，股票 tick=0.01
        set_slippage(StepRelatedSlippage(4))
        
        order("601318.XSHG", 100)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        # 买入价格 = 77.9 + 4 * 0.01 / 2 = 77.9 + 0.02 = 77.92
        assert trade.price == pytest.approx(77.92)
        self._cleanup()

    def test_step_related_slippage_sell(self, provider):
        """StepRelatedSlippage 卖出：价格 - steps * tick / 2"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        position = Position(security="601318.XSHG", total_amount=100, closeable_amount=100, avg_cost=75.0)
        position.update_price(77.9)
        engine.context.portfolio.positions["601318.XSHG"] = position
        # 设置跳数滑点 4 跳（双边），单边 2 跳，股票 tick=0.01
        set_slippage(StepRelatedSlippage(4))
        
        order("601318.XSHG", -100)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        # 卖出价格 = 77.9 - 4 * 0.01 / 2 = 77.9 - 0.02 = 77.88
        assert trade.price == pytest.approx(77.88)
        self._cleanup()


# =============================================================================
# 费用测试：按品种区分（股票/ETF/MMF）× 买卖方向
# =============================================================================

class TestOrderCostsBySecurityType:
    """测试不同品种的费用规则"""
    
    def _cleanup(self):
        """测试清理"""
        set_current_engine(None)
        data_api.set_current_context(None)
        clear_order_queue()

    def _fen(self, x):
        """四舍五入到分"""
        import decimal
        return float(decimal.Decimal(str(x)).quantize(
            decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP))

    # --------------- 股票 ---------------
    def test_stock_buy_commission_no_stamp_duty(self, provider):
        """股票买入：有佣金（万三，最低5元），无印花税"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        # 不设置滑点，使用默认 PriceRelatedSlippage(0.00246)
        
        order("601318.XSHG", 100)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        # 买入价格 = 77.9 * (1 + 0.00246/2)
        expected_price = 77.9 * (1 + 0.00246 / 2)
        expected_price = self._fen(expected_price)
        expected_value = expected_price * 100
        
        # 印花税 = 0（买入无印花税）
        assert trade.tax == pytest.approx(0.0)
        # 佣金 = max(expected_value * 0.0003, 5.0)
        expected_commission = max(self._fen(expected_value * 0.0003), 5.0)
        assert trade.commission == pytest.approx(expected_commission)
        self._cleanup()

    def test_stock_sell_commission_and_stamp_duty(self, provider):
        """股票卖出：有佣金（万三，最低5元）+ 印花税（千一）"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        position = Position(security="601318.XSHG", total_amount=100, closeable_amount=100, avg_cost=75.0)
        position.update_price(77.9)
        engine.context.portfolio.positions["601318.XSHG"] = position
        
        order("601318.XSHG", -100)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        # 卖出价格 = 77.9 * (1 - 0.00246/2)
        expected_price = 77.9 * (1 - 0.00246 / 2)
        expected_price = self._fen(expected_price)
        expected_value = expected_price * 100
        
        # 印花税 = expected_value * 0.001
        assert trade.tax == pytest.approx(self._fen(expected_value * 0.001))
        # 佣金 = max(expected_value * 0.0003, 5.0)
        expected_commission = max(self._fen(expected_value * 0.0003), 5.0)
        assert trade.commission == pytest.approx(expected_commission)
        self._cleanup()

    # --------------- ETF ---------------
    def test_etf_buy_commission_no_stamp_duty(self, provider):
        """ETF 买入：有佣金（万三，最低5元），无印花税"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        
        order("159949.XSHE", 1000)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        
        # 印花税 = 0
        assert trade.tax == pytest.approx(0.0)
        # 佣金最低 5 元
        assert trade.commission >= 5.0
        self._cleanup()

    def test_etf_sell_commission_no_stamp_duty(self, provider):
        """ETF 卖出：有佣金（万三，最低5元），无印花税"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        position = Position(security="159949.XSHE", total_amount=1000, closeable_amount=1000, avg_cost=1.20)
        position.update_price(1.2345)
        engine.context.portfolio.positions["159949.XSHE"] = position
        
        order("159949.XSHE", -1000)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        
        # 印花税 = 0
        assert trade.tax == pytest.approx(0.0)
        # 佣金最低 5 元
        assert trade.commission >= 5.0
        self._cleanup()

    # --------------- 货币基金 MMF ---------------
    def test_mmf_buy_no_cost(self, provider):
        """MMF 买入：无佣金，无印花税"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        
        order_value("511880.XSHG", 10000)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        
        assert trade.tax == pytest.approx(0.0)
        assert trade.commission == pytest.approx(0.0)
        self._cleanup()

    def test_mmf_sell_no_cost(self, provider):
        """MMF 卖出：无佣金，无印花税"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        position = Position(security="511880.XSHG", total_amount=10000, closeable_amount=10000, avg_cost=1.00)
        position.update_price(1.00215)
        engine.context.portfolio.positions["511880.XSHG"] = position
        
        order("511880.XSHG", -10000)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        
        assert trade.tax == pytest.approx(0.0)
        assert trade.commission == pytest.approx(0.0)
        self._cleanup()


# =============================================================================
# 默认行为测试
# =============================================================================

class TestDefaultSlippageBehavior:
    """测试默认滑点行为"""
    
    def _cleanup(self):
        """测试清理"""
        set_current_engine(None)
        data_api.set_current_context(None)
        clear_order_queue()

    def test_default_slippage_is_price_related_0246bps(self, provider):
        """验证默认滑点是 PriceRelatedSlippage(0.00246)，即 24.6bps"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        # 不调用 set_slippage，使用默认值（_setup_engine 已设置为聚宽默认）
        
        order("601318.XSHG", 100)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        
        # 默认比例滑点 24.6bps，买入价格 = 77.9 * (1 + 0.00246/2)
        expected_price = 77.9 * (1 + 0.00246 / 2)
        import decimal
        expected_price = float(decimal.Decimal(str(expected_price)).quantize(
            decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP))
        assert trade.price == pytest.approx(expected_price)
        self._cleanup()

    def test_mmf_default_slippage_is_zero(self, provider):
        """验证 MMF 默认滑点为 0"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        # 不调用 set_slippage，MMF 应忽略任何滑点
        
        order_value("511880.XSHG", 10000)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        
        # MMF 价格应等于行情价（按 0.001 精度取整）
        # 行情价 1.00215，按 0.001 四舍五入 = 1.002
        assert trade.price == pytest.approx(1.002)
        self._cleanup()

    def test_mmf_ignores_price_related_slippage(self, provider):
        """验证 MMF 忽略显式设置的 PriceRelatedSlippage"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        # 显式设置较大的比例滑点 10%
        set_slippage(PriceRelatedSlippage(0.1))
        
        order_value("511880.XSHG", 10000)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        
        # MMF 应忽略滑点，价格仍为行情价
        assert trade.price == pytest.approx(1.002)
        self._cleanup()

    def test_mmf_ignores_step_related_slippage(self, provider):
        """验证 MMF 忽略显式设置的 StepRelatedSlippage"""
        engine = _setup_engine(datetime(2021, 1, 5, 9, 31))
        # 显式设置较大的跳数滑点
        set_slippage(StepRelatedSlippage(100))
        
        order_value("511880.XSHG", 10000)
        assert engine.trades, "trade should be recorded"
        trade = engine.trades[-1]
        
        # MMF 应忽略滑点，价格仍为行情价
        assert trade.price == pytest.approx(1.002)
        self._cleanup()
