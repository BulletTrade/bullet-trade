"""作者：BruceLee。停复牌限价单与资金守恒回归。

输入：内存分钟行情及真实订单 API；输出：pytest 断言。
测试 BacktestEngine/订单簿/API 协作，不依赖网络、账户或外部配置。
"""

from datetime import datetime, time
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.models import Context, OrderStatus, Portfolio, Position, SecurityUnitData
from bullet_trade.core.orders import (
    LimitOrderStyle, MarketOrderStyle, cancel_all_orders, cancel_order,
    clear_order_queue, order, order_target,
)
from bullet_trade.core.runtime import set_current_engine

CODE = "513100.XSHG"


@pytest.fixture
def market(monkeypatch):
    """输入 pytest patch，返回引擎和可控行情；结束时清理引擎及全局队列。"""
    engine = BacktestEngine()
    engine.context = Context(portfolio=Portfolio(available_cash=10000, starting_cash=10000),
                             current_dt=datetime(2026, 9, 16, 9, 40))
    quote = SecurityUnitData(security=CODE, last_price=2.196, paused=False,
                             high_limit=2.415, low_limit=1.977)
    data = SimpleNamespace(price=2.196, volume=0, lag=0, empty=False)

    def prices(**kwargs):
        """输入行情请求，返回指定时刻价量；无外部依赖，用于可见性断言。"""
        assert kwargs["frequency"] == "minute"
        assert kwargs["end_date"] <= engine.context.current_dt
        if data.empty:
            return pd.DataFrame()
        stamp = kwargs["end_date"] + pd.Timedelta(minutes=data.lag)
        return pd.DataFrame({"close": [data.price], "low": [data.price],
                             "high": [data.price], "volume": [data.volume]}, index=[stamp])

    def daily_prices(**kwargs):
        """输入内部日线量请求，返回当前日期总量；固定样本以data.volume控制日量。"""
        return pd.DataFrame({"volume": [data.volume]}, index=[kwargs["end_date"]])

    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", prices)
    monkeypatch.setattr("bullet_trade.core.engine.get_data_provider",
                        lambda: SimpleNamespace(get_price=daily_prices))
    monkeypatch.setattr("bullet_trade.data.api.get_current_data", lambda: {CODE: quote})
    monkeypatch.setattr("bullet_trade.core.engine.get_security_info", lambda *_: {"type": "fund"})
    monkeypatch.setattr(engine, "_infer_security_category", lambda *_: "fund")
    monkeypatch.setattr(engine, "_infer_tplus_from_info", lambda *_: 0)
    monkeypatch.setattr(engine, "_apply_slippage_price", lambda p, *_: p)
    monkeypatch.setattr(engine, "_get_order_cost_config", lambda *_: SimpleNamespace(
        open_commission=0.00005, close_commission=0.00005, open_tax=0, close_tax=0,
        min_commission=5))
    monkeypatch.setattr("bullet_trade.core.orders._trigger_order_processing", lambda *_: None)
    set_current_engine(engine)
    clear_order_queue()
    yield engine, quote, data
    clear_order_queue()
    set_current_engine(None)


def advance(engine, hour, minute):
    """输入引擎及当日时分，无返回；推进回放时刻并走真实订单处理入口。"""
    engine.context.current_dt = datetime(2026, 9, 16, hour, minute)
    engine._process_orders(engine.context.current_dt)


def test_missing_quote_retains_cancelable_reservation(market, monkeypatch):
    """输入行情 fixture 和 patch，验证缺行情挂单可撤且释放资金；无返回。"""
    engine, _, _ = market
    ticket = order(CODE, 100, style=LimitOrderStyle(2.218))
    advance(engine, 9, 40)
    monkeypatch.setattr("bullet_trade.data.api.get_current_data", lambda: {})
    advance(engine, 9, 41)
    assert ticket.status == OrderStatus.open
    assert ticket.extra["wait_reason"] == "missing_quote"
    assert cancel_order(ticket)
    assert engine.context.portfolio.locked_cash == pytest.approx(0)
    assert engine.context.portfolio.available_cash == pytest.approx(10000)


def test_money_market_limit_has_no_slippage(market, monkeypatch):
    """输入行情 fixture 和 patch，验证货币基金继续零滑点；无返回。"""
    engine, _, data = market
    data.volume = 10000
    monkeypatch.setattr(engine, "_infer_security_category", lambda *_: "money_market_fund")
    monkeypatch.setattr(engine, "_apply_slippage_price", lambda *_: 2.21)
    ticket = order(CODE, 100, style=LimitOrderStyle(2.218))
    advance(engine, 9, 40)
    assert ticket.status == OrderStatus.filled
    assert engine.trades[0].price == 2.196


def test_halt_resume_freezes_then_fills(market):
    """零量挂单应冻结但不增仓；恢复量价后成交，输入 fixture，无返回。"""
    engine, quote, data = market
    ticket = order(CODE, 100, style=LimitOrderStyle(2.218))
    advance(engine, 9, 40)
    p = engine.context.portfolio
    assert ticket.status == OrderStatus.open
    assert p.available_cash == pytest.approx(9773.2)
    assert p.locked_cash == pytest.approx(226.8)
    assert p.total_value == pytest.approx(10000)
    assert not engine.trades and not p.positions
    assert ticket.order_id in engine.get_open_orders()
    data.price, data.volume = 2.194, 10000
    advance(engine, 10, 31)
    assert ticket.status == OrderStatus.filled
    assert engine.trades[0].time.hour == 10 and engine.trades[0].time.minute == 31
    assert ticket.add_time == datetime(2026, 9, 16, 9, 40)
    assert p.available_cash == pytest.approx(9773.2)
    assert p.locked_cash == pytest.approx(0)
    assert p.total_value == pytest.approx(9995)


@pytest.mark.parametrize("lag,empty", [(-1, False), (1, False), (0, True)])
def test_invalid_minute_cannot_fill(market, lag, empty):
    """输入过期/未来/缺失行情参数，断言不成交；无返回。"""
    engine, quote, data = market
    data.volume, data.lag, data.empty = 10000, lag, empty
    ticket = order(CODE, 100, style=LimitOrderStyle(2.218))
    advance(engine, 9, 40)
    assert ticket.status == OrderStatus.open and not engine.trades
    assert cancel_order(ticket.order_id)
    assert engine.context.portfolio.available_cash == pytest.approx(10000)
    assert engine.context.portfolio.locked_cash == pytest.approx(0)


def test_limit_never_repriced_and_day_expiry(market):
    """复牌跳空不能自动提价；输入 fixture，无返回，验证日终释放。"""
    engine, quote, data = market
    ticket = order(CODE, 100, style=LimitOrderStyle(2.218))
    advance(engine, 9, 40)
    data.price, data.volume = 2.3, 10000
    advance(engine, 10, 31)
    assert not engine.trades and ticket.style.price == 2.218
    assert engine._equity_limit_book.expire() == 1
    assert ticket.status == OrderStatus.canceled
    assert engine.context.portfolio.available_cash == pytest.approx(10000)


def test_partial_fill_independent_volume_fee_once(market):
    """每笔独立量，部分成交最低佣金不重复；输入 fixture，无返回。"""
    engine, quote, data = market
    first = order(CODE, 200, style=LimitOrderStyle(2.218))
    second = order(CODE, 100, style=LimitOrderStyle(2.218))
    data.volume = 100
    advance(engine, 9, 40)
    assert first.filled == 100 and second.filled == 100
    assert first.status == OrderStatus.filling
    advance(engine, 9, 40)
    assert len(engine.trades) == 2
    advance(engine, 9, 41)
    assert first.filled == 200 and second.filled == 100
    assert sum(t.commission for t in engine.trades) == pytest.approx(10)
    assert cancel_all_orders() == 0
    assert engine.context.portfolio.locked_cash == pytest.approx(0)
    assert engine.context.portfolio.total_value == pytest.approx(9994.4)


def test_sell_reservation_prevents_double_sell(market):
    """两笔卖单不得重复冻结份额；输入 fixture，无返回，部分卖出后撤单守恒。"""
    engine, quote, data = market
    p = engine.context.portfolio
    p.positions[CODE] = Position(security=CODE, total_amount=200, closeable_amount=200,
                                price=2.196, value=439.2, avg_cost=2.1)
    first = order(CODE, -200, style=LimitOrderStyle(2.19))
    second = order(CODE, -100, style=LimitOrderStyle(2.19))
    advance(engine, 9, 40)
    assert p.positions[CODE].closeable_amount == 0
    assert second.status == OrderStatus.canceled
    data.volume = 100
    advance(engine, 10, 31)
    assert first.filled == 100
    assert p.positions[CODE].total_amount == 100
    assert p.positions[CODE].closeable_amount == 0
    assert cancel_order(first)
    assert p.positions[CODE].closeable_amount == 100
    assert p.total_value == pytest.approx(10433)


def test_target_replaces_pending_and_releases_cash(market):
    """目标单替换不叠加冻结；输入 fixture，无返回。"""
    engine, quote, data = market
    first = order_target(CODE, 200, style=LimitOrderStyle(2.218))
    advance(engine, 9, 40)
    second = order_target(CODE, 100, style=LimitOrderStyle(2.218))
    advance(engine, 9, 41)
    assert first.status == OrderStatus.canceled
    assert second.amount == 100
    assert engine.context.portfolio.locked_cash == pytest.approx(226.8)


@pytest.mark.parametrize("paused", [False, True])
def test_market_no_fill_when_paused_or_zero_volume(market, paused):
    """输入停牌标志，断言市价请求在不可成交时取消，无持仓或费用。"""
    engine, quote, data = market
    quote.paused = paused
    data.volume = 10000 if paused else 0
    ticket = order(CODE, 100)
    advance(engine, 9, 40)
    assert ticket.status == OrderStatus.canceled and not engine.trades
    assert engine.context.portfolio.available_cash == 10000


def test_market_cannot_spend_reserved_cash(market):
    """挂单资金不被市价占用；输入 fixture，无返回。"""
    engine, quote, data = market
    engine.context.portfolio.available_cash = 230
    pending = order(CODE, 100, style=LimitOrderStyle(2.218))
    advance(engine, 9, 40)
    data.price, data.volume = 2.3, 10000
    immediate = order(CODE, 100)
    advance(engine, 10, 31)
    assert pending.status == OrderStatus.open and immediate.filled == 0
    assert engine.context.portfolio.available_cash == pytest.approx(3.2)


def test_native_market_type_not_fabricated(market):
    """无盘口不模拟原生市价类型；输入 fixture，无返回。"""
    engine, quote, data = market
    data.volume = 10000
    ticket = order(CODE, 100, style=MarketOrderStyle(market_type="home_best"))
    advance(engine, 9, 40)
    assert ticket.status == OrderStatus.rejected and not engine.trades


@pytest.mark.parametrize("frequency", ["day", "minute"])
def test_resume_between_callbacks_and_expire_before_after_close(market, monkeypatch, frequency):
    """真实调度须在无回调的复牌分钟撮合；输入 fixture/频率，无返回。"""
    engine, quote, data = market
    engine.frequency = frequency
    calls, tickets, observed = [], [], []

    def submit(context):
        """输入策略上下文，提交原限价单及无法成交的卖单；返回无值。"""
        calls.append(context.current_dt)
        tickets.append(order(CODE, 100, style=LimitOrderStyle(2.218)))

    def prices(**kwargs):
        """输入请求，按历史时间提供停牌及复牌行情；返回单行分钟表。"""
        now = kwargs["end_date"]
        return pd.DataFrame({"close": [2.194], "low": [2.194], "high": [2.194],
                             "volume": [10000 if now.time() >= time(10, 31) else 0]},
                            index=[now])

    def after_close(context):
        """输入收盘上下文，核验没有遗留冻结；无返回。"""
        assert context.portfolio.locked_cash == pytest.approx(0)

    def observe_reopen(context):
        """输入复牌分钟上下文，验证策略回调已经可见该分钟的挂单成交；无返回。"""
        observed.append((tickets[0].filled, context.portfolio.positions[CODE].total_amount))

    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", prices)
    monkeypatch.setattr("bullet_trade.core.engine.generate_daily_schedule", lambda *_a, **_k: {
        datetime(2026, 9, 16, 9, 40): [SimpleNamespace(func=submit)],
        datetime(2026, 9, 16, 10, 31): [SimpleNamespace(func=observe_reopen)]})
    monkeypatch.setattr(engine, "_apply_dividends_for_day", lambda *_: None)
    monkeypatch.setattr(engine, "_mark_non_futures_intraday", lambda *_: None)
    monkeypatch.setattr(engine, "_mark_futures_intraday", lambda *_: None)
    engine.after_trading_end_func = after_close
    engine._run_trading_day(datetime(2026, 9, 16), [(time(9, 30), time(11, 30)), (time(13), time(15))])
    assert calls == [datetime(2026, 9, 16, 9, 40)]
    assert observed == [(100, 100)]
    assert len(engine.trades) == 1
    assert engine.trades[0].time == datetime(2026, 9, 16, 10, 31)


def test_tplus_one_after_partial_fills(market, monkeypatch):
    """输入内存行情及 patch，检查T+1在分次买入后不释放可卖份额，无返回。"""
    engine, quote, data = market
    monkeypatch.setattr(engine, "_infer_tplus_from_info", lambda *_: 1)
    data.volume = 100
    ticket = order(CODE, 200, style=LimitOrderStyle(2.218))
    advance(engine, 9, 40)
    advance(engine, 9, 41)
    pos = engine.context.portfolio.positions[CODE]
    assert ticket.filled == 200 and pos.today_buy_t1 == 200 and pos.closeable_amount == 0


def test_paused_positive_volume_still_waits(market):
    """输入矛盾的停牌与有量行情，验证停牌优先，无返回。"""
    engine, quote, data = market
    quote.paused, data.volume = True, 10000
    ticket = order(CODE, 100, style=LimitOrderStyle(2.218))
    advance(engine, 9, 40)
    assert not engine.trades
    quote.paused = False
    advance(engine, 10, 31)
    assert ticket.filled == 100
