"""作者：BruceLee。

职责：用聚宽平台冻结订单与分钟行情验证权益撮合、资源及可见性边界。
输入：真实平台样本、内存 provider 与真实订单 API；输出：pytest 断言。
上下游：BacktestEngine、EquityLimitBook、settings；不连接网络或生产账户。
"""

import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.core.models import OrderStatus, Position
from bullet_trade.core.orders import LimitOrderStyle, cancel_order, order
from bullet_trade.core.settings import get_settings, reset_settings, set_option
from tests.unit.test_equity_pending_matching import CODE, advance, market


@pytest.fixture
def jq_market(market, monkeypatch):
    """输入基础行情和patch，提供冻结平台行情及可调日量；结束时恢复设置。"""
    reset_settings()
    engine, quote, data = market
    sample = json.loads(
        (Path(__file__).parents[1] / "fixtures/jq_volume_matching_20260930.json").read_text()
    )
    data.price, data.volume = 8.485, 2636400
    data.low, data.high = 8.484, 8.488
    data.daily_volume, data.daily_lag, data.daily_calls = sample["daily_volume"], 0, 0
    data.previous_volume, data.minute_paused = 0, False
    quote.last_price, quote.high_limit, quote.low_limit = 8.485, 20, 0.1
    engine.context.portfolio.available_cash = 2000000000
    engine.context.portfolio.starting_cash = 2000000000

    def prices(**kwargs):
        """输入分钟请求，返回当前和可选的已发生历史量；不返回未来分钟。"""
        now = kwargs["end_date"]
        assert now <= engine.context.current_dt
        stamp = now + pd.Timedelta(minutes=data.lag)
        if data.empty:
            return pd.DataFrame()
        frame = pd.DataFrame(
            {
                "close": [data.price],
                "volume": [data.volume],
                "low": [data.low],
                "high": [data.high],
                "paused": [data.minute_paused],
            },
            index=[stamp],
        )
        if kwargs.get("start_date") and data.previous_volume:
            frame.loc[now - pd.Timedelta(minutes=1)] = [
                data.price,
                data.previous_volume,
                data.low,
                data.high,
                False,
            ]
            frame = frame.sort_index()
        return frame

    def daily(**kwargs):
        """输入引擎内部日量请求，返回指定日期原始量并统计调用；无网络。"""
        assert kwargs["fields"] == ["volume"]
        assert kwargs["fq"] in (None, "none")
        data.daily_calls += 1
        stamp = pd.Timestamp(kwargs["end_date"]).normalize() + pd.Timedelta(days=data.daily_lag)
        return pd.DataFrame({"volume": [data.daily_volume]}, index=[stamp])

    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", prices)
    monkeypatch.setattr(
        "bullet_trade.core.engine.get_data_provider", lambda: SimpleNamespace(get_price=daily)
    )
    yield engine, quote, data
    reset_settings()


@pytest.mark.parametrize("ratio,filled", [(0.00001, 3775), (0.0001, 37755), (1, 500000)])
def test_immediate_uses_daily_volume_per_order(jq_market, ratio, filled):
    """输入平台比例样本，验证同分钟两笔即时单各自日量上限与非整百成交；无返回。"""
    engine, _, data = jq_market
    set_option("order_volume_ratio", ratio)
    tickets = [order(CODE, 500000), order(CODE, 500000)]
    advance(engine, 9, 40)
    assert [ticket.filled for ticket in tickets] == [filled, filled]
    assert data.daily_calls == 1
    assert sum(t.amount for t in engine.trades) == 2 * filled


def test_default_ratio_is_one(jq_market):
    """输入初始化设置，验证与平台未设置比例的有效默认值一致；无返回。"""
    assert get_settings().options["order_volume_ratio"] == 1.0


@pytest.mark.parametrize(
    "price,requested,daily_volume",
    [(2.304, 200000000, 135532268), (1.18, 556600, 466800)],
)
def test_default_ratio_caps_oversized_order_at_raw_daily_volume(
    jq_market, price, requested, daily_volume
):
    """输入聚宽默认上限样本及历史回归样本，验证按原始全天量截断并撤销余量。"""
    engine, quote, data = jq_market
    data.price = quote.last_price = price
    data.high = data.low = price
    data.daily_volume = daily_volume
    ticket = order(CODE, requested)
    advance(engine, 9, 40)
    assert ticket.amount == requested and ticket.filled == daily_volume
    assert ticket.status == OrderStatus.canceled
    assert ticket.extra["cancel_reason"] == "insufficient_daily_volume"
    assert len(engine.trades) == 1 and engine.trades[0].amount == daily_volume


@pytest.mark.parametrize(
    "ratio,first,total", [(0.00001, 26, 52), (0.00002, 52, 104), (0.0001, 263, 524)]
)
def test_pending_matches_platform_minute_fills(jq_market, ratio, first, total):
    """输入三组平台比例，验证挂单每分钟量、穿价、原限价及重复调用不增成交。"""
    engine, _, data = jq_market
    set_option("order_volume_ratio", ratio)
    data.price, data.low, data.volume = 8.488, 8.488, 1663800
    tickets = [order(CODE, 20000, style=LimitOrderStyle(8.485)) for _ in range(2)]
    advance(engine, 9, 39)
    assert not engine.trades
    data.price, data.low, data.volume = 8.485, 8.484, 2636400
    advance(engine, 9, 40)
    assert [o.filled for o in tickets] == [first, first]
    advance(engine, 9, 40)
    assert [o.filled for o in tickets] == [first, first]
    data.price, data.volume = 8.488, 2611700
    advance(engine, 9, 41)
    assert [o.filled for o in tickets] == [total, total]
    assert all(t.price == 8.485 for t in engine.trades)
    assert sum(t.commission for t in engine.trades) == pytest.approx(10)
    for ticket in tickets:
        assert cancel_order(ticket)
    portfolio = engine.context.portfolio
    assert portfolio.locked_cash == pytest.approx(0, abs=1e-6)
    settled = sum(
        (Decimal(str(t.price)) * abs(t.amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        + Decimal(str(t.commission + t.tax))
        for t in engine.trades
    )
    assert portfolio.available_cash == pytest.approx(
        float(Decimal("2000000000") - settled), rel=0, abs=1e-6
    )
    assert portfolio.positions[CODE].total_amount == total * 2


def test_new_limit_cannot_use_pre_order_low_and_equal_low_does_not_cross(jq_market):
    """输入下单前已出现的最低价，验证新单不能补成交及旧挂单严格穿价；无返回。"""
    engine, _, data = jq_market
    data.price, data.low = 8.488, 8.484
    ticket = order(CODE, 20000, style=LimitOrderStyle(8.484))
    advance(engine, 9, 40)
    assert ticket.filled == 0
    advance(engine, 9, 41)
    assert ticket.filled == 0


def test_marketable_limit_gets_daily_cap_then_waits_for_next_bar(jq_market):
    """输入可立即成交的限价单，验证先按日量部分成交，再以原限价按分钟量成交。"""
    engine, _, data = jq_market
    set_option("order_volume_ratio", 0.00001)
    ticket = order(CODE, 20000, style=LimitOrderStyle(8.5))
    advance(engine, 9, 40)
    assert ticket.filled == 3775
    assert engine.trades[-1].price == 8.485
    advance(engine, 9, 40)
    assert ticket.filled == 3775
    data.price, data.volume = 8.488, 2611700
    advance(engine, 9, 41)
    assert ticket.filled == 3801
    assert engine.trades[-1].price == 8.5


def test_marketable_limit_full_day_matches_platform(jq_market, monkeypatch):
    """输入聚宽全天真实价量及57笔成交，逐笔核对时刻、量价和日终余单释放；无网络。"""
    engine, quote, _ = jq_market
    gold_code = "518880.XSHG"
    quote.security = gold_code
    fixtures = Path(__file__).parents[1] / "fixtures"
    sample = json.loads((fixtures / "jq_marketable_limit_20260929.json").read_text())
    frame = pd.read_csv(fixtures / "jq_518880_20260929.csv", index_col=0, parse_dates=True)

    def prices(**kwargs):
        """输入分钟读取参数，返回当时可见的冻结数据；无外部读取或未来行。"""
        assert kwargs["security"] == gold_code
        assert kwargs["end_date"] <= engine.context.current_dt
        visible = frame.loc[: kwargs["end_date"]]
        if kwargs.get("start_date") is not None:
            visible = visible.loc[kwargs["start_date"] :]
        return visible.tail(kwargs.get("count", len(visible))).copy()

    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", prices)
    monkeypatch.setattr("bullet_trade.data.api.get_current_data", lambda: {gold_code: quote})
    set_option("order_volume_ratio", 0.00001)
    engine.context.current_dt = pd.Timestamp("2026-09-29 09:40").to_pydatetime()
    ticket = order(gold_code, 20000, style=LimitOrderStyle(8.5))
    for stamp, row in frame.loc["2026-09-29 09:40":].iterrows():
        engine.context.current_dt = stamp.to_pydatetime()
        quote.last_price = float(row["close"])
        engine._process_orders(engine.context.current_dt)
    actual = [dict(time=str(t.time), amount=t.amount, price=t.price) for t in engine.trades]
    assert actual == sample["fills"]
    assert ticket.filled == sample["total_filled"] == 4863
    assert engine._equity_limit_book.expire() == 1
    assert ticket.status == OrderStatus.canceled
    assert engine.context.portfolio.locked_cash == pytest.approx(0, abs=1e-6)


@pytest.mark.parametrize("earlier_volume,expected", [(0, 0), (100, 100)])
def test_zero_minute_is_not_automatically_halt(jq_market, earlier_volume, expected):
    """输入零量分钟及此前可见成交，区分早盘始终零量保护与普通零量分钟；无返回。"""
    engine, _, data = jq_market
    data.volume, data.previous_volume = 0, earlier_volume
    ticket = order(CODE, 100)
    advance(engine, 9, 40)
    assert ticket.filled == expected


@pytest.mark.parametrize("paused_field", ["quote", "minute"])
def test_explicit_pause_overrides_positive_volume(jq_market, paused_field):
    """输入报价或分钟明确停牌，验证有量也不成交；无返回。"""
    engine, quote, data = jq_market
    if paused_field == "quote":
        quote.paused = True
    else:
        data.minute_paused = True
    ticket = order(CODE, 100)
    advance(engine, 9, 40)
    assert ticket.filled == 0


@pytest.mark.parametrize("lag", [-1, 1])
def test_daily_volume_must_belong_to_matching_day(jq_market, lag):
    """输入前日或后日返回行，验证引擎不会借用错误日期的成交量；无返回。"""
    engine, _, data = jq_market
    data.daily_lag = lag
    ticket = order(CODE, 100)
    advance(engine, 9, 40)
    assert ticket.filled == 0


def test_sell_pending_uses_high_and_original_limit(jq_market):
    """输入待卖持仓与高点穿价，验证非整百分次卖出、原价与冻结释放；无返回。"""
    engine, _, data = jq_market
    set_option("order_volume_ratio", 0.00001)
    portfolio = engine.context.portfolio
    portfolio.positions[CODE] = Position(
        security=CODE,
        total_amount=1000,
        closeable_amount=1000,
        price=8.485,
        value=8485,
        avg_cost=8.485,
    )
    ticket = order(CODE, -1000, style=LimitOrderStyle(8.49))
    advance(engine, 9, 40)
    assert ticket.filled == 0
    data.high = 8.491
    advance(engine, 9, 41)
    assert ticket.filled == 26
    assert engine.trades[-1].price == 8.49
    assert portfolio.positions[CODE].closeable_amount == 0
    assert cancel_order(ticket)
    assert portfolio.positions[CODE].closeable_amount == 974


def test_engine_daily_volume_does_not_disable_strategy_future_guard(jq_market):
    """输入启用未来数据守卫的上下文，验证内部日量撮合不放开策略日线量访问。"""
    from bullet_trade.data import api
    from bullet_trade.core.exceptions import FutureDataError

    engine, _, _ = jq_market
    set_option("avoid_future_data", True)
    api.set_current_context(engine.context)
    try:
        ticket = order(CODE, 100)
        advance(engine, 9, 40)
        assert ticket.filled == 100
        with pytest.raises(FutureDataError):
            api.get_price(
                CODE,
                end_date=engine.context.current_dt,
                count=1,
                frequency="daily",
                fields=["volume"],
            )
    finally:
        api.set_current_context(None)


def test_daily_volume_failure_invalidates_strict_run(jq_market, monkeypatch):
    """输入严格回测及日量读取故障，验证失效状态保留、无交易且不能继续撮合。"""
    from bullet_trade.core.exceptions import BacktestDataError

    engine, _, _ = jq_market
    engine.strict_data = True

    def fail(**kwargs):
        """输入日量请求即抛模拟连接异常，无返回，不连接外部。"""
        raise ConnectionError("frozen daily volume failure")

    monkeypatch.setattr(
        "bullet_trade.core.engine.get_data_provider", lambda: SimpleNamespace(get_price=fail)
    )
    order(CODE, 100)
    with pytest.raises(BacktestDataError):
        advance(engine, 9, 40)
    assert not engine.trades
    with pytest.raises(BacktestDataError):
        advance(engine, 9, 41)


def test_daily_volume_cache_rotates_at_next_day(jq_market):
    """输入两个交易日的日量，验证当天复用和跨日重读；无返回。"""
    engine, _, data = jq_market
    book, now = engine._equity_limit_book, engine.context.current_dt
    assert book.daily_volume(CODE, now) == 377557858
    assert book.daily_volume(CODE, now) == 377557858
    data.daily_volume = 12345
    assert book.daily_volume(CODE, now + pd.Timedelta(days=1)) == 12345
    assert data.daily_calls == 2


@pytest.mark.parametrize("day", ["2026-09-29", "2026-09-30"])
def test_real_nasdaq_halt_waits_for_visible_reopening_bar(jq_market, monkeypatch, day):
    """输入两日聚宽原始分钟样本，验证停牌无成交、原限价保留并在真实有量后成交。"""
    engine, quote, _ = jq_market
    frame = pd.read_csv(
        Path(__file__).parents[1] / "fixtures" / f"jq_513100_halt_{day}.csv",
        index_col=0,
        parse_dates=True,
    )
    assert frame.loc[: day + " 10:30", "volume"].eq(0).all()
    assert frame.loc[day + " 10:31", "volume"] > 0
    # 保留平台错误的 paused=False，保证保护并非依赖测试伪造停牌标签。
    assert frame["paused"].eq(0).all()

    def prices(**kwargs):
        """输入分钟请求，返回冻结数据中当时可见区间，禁止返回未来行；无外部读取。"""
        now = kwargs["end_date"]
        assert now <= engine.context.current_dt
        visible = frame.loc[:now]
        if kwargs.get("start_date") is not None:
            visible = visible.loc[kwargs["start_date"] :]
        elif kwargs.get("count"):
            visible = visible.tail(kwargs["count"])
        return visible.copy()

    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", prices)
    engine.context.current_dt = pd.Timestamp(day + " 09:40").to_pydatetime()
    quote.last_price = float(frame.loc[engine.context.current_dt, "close"])
    market_ticket = order(CODE, 100)
    pending = order(CODE, 100, style=LimitOrderStyle(2.34))
    for stamp in pd.date_range(day + " 09:40", day + " 10:30", freq="min"):
        engine.context.current_dt = stamp.to_pydatetime()
        engine._process_orders(engine.context.current_dt)
        assert not engine.trades
    assert market_ticket.status == OrderStatus.canceled
    assert pending.status == OrderStatus.open
    assert pending.style.price == 2.34
    engine.context.current_dt = pd.Timestamp(day + " 10:31").to_pydatetime()
    quote.last_price = float(frame.loc[engine.context.current_dt, "close"])
    engine._process_orders(engine.context.current_dt)
    assert pending.filled == 100 and len(engine.trades) == 1
    assert engine.trades[0].time == engine.context.current_dt
    assert engine.trades[0].price == 2.34
    assert engine.context.portfolio.locked_cash == pytest.approx(0, abs=1e-6)
