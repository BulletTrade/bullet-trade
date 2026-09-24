"""
作者: BruceLee
文件职责:
    验证 tick 频率回测的合并事件循环：调度任务与逐笔事件同队列有序、订阅门控投递、
    回放缓冲读取、缺数据显式失败，以及日频路径与异步入口的行为约束。

主要输入:
    BacktestEngine 的单日回放入口，配合内存构造的 tick 事件流与本地取数桩。

主要输出:
    pytest 断言结果，确认盘前回调先于首笔 tick、handle_tick 按源时间戳稳定有序投递、
    订阅/退订在回放时点生效、缺日中止报出代码与日期、未订阅缺口不中止、
    整日预取不被未来数据守卫拦下且守卫事后恢复、订单登记时刻取回放时钟、
    市价单保护价以当前 tick 为基准。

上下游关系:
    上游覆盖 `bullet_trade.core.engine` 的 tick 时间轴与 `bullet_trade.core.api` 的订阅接口；
    下游保护 tick 驱动策略在回测中拿到的事件序与实盘投递语义一致。

关键环境或配置约定:
    测试只使用合成 tick 记录与本地取数桩，不访问任何外部数据源；
    异步入口用例只断言报错，不真正跑回测。
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from datetime import time as Time
from typing import Dict, List

import pandas as pd
import pytest

from bullet_trade.core import api as core_api
from bullet_trade.core.async_engine import AsyncBacktestEngine
from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.models import Context, Order, OrderStatus, Portfolio
from bullet_trade.core.scheduler import run_daily, unschedule_all
from bullet_trade.data.tick_replay import TickDataMissingError, TickDayStream

CODE_A = "LH2109.XDCE"
CODE_B = "LH2201.XDCE"
DAY = date(2021, 6, 8)
TRADE_DAY = datetime(2021, 6, 8)
MARKET_PERIODS = [(Time(9, 0), Time(11, 30)), (Time(13, 0), Time(15, 0))]
CASH = 1_000_000.0


def _stream(code: str, rows) -> TickDayStream:
    """把 (原始时间戳, 价格) 序列整理成单日事件流。

    Args:
        code: 标的代码。
        rows: 每项为 (float 时间戳, 最新价)。

    Returns:
        TickDayStream: 已按源时间戳稳定排序的事件流。
    """

    frame = pd.DataFrame([{"time": float(raw), "current": float(price)} for raw, price in rows])
    return TickDayStream(code, DAY, frame)


def _stamps(day: date, seconds_offsets, base_price: float = 17000.0):
    """按当日 09:00 起的秒偏移生成时间戳与价格序列。

    Args:
        day: 自然日。
        seconds_offsets: 相对 09:00:00 的秒偏移列表。
        base_price: 起始价格，每笔递增 5.0。

    Returns:
        List[tuple]: (原始 float 时间戳, 价格)。
    """

    prefix = int(day.strftime("%Y%m%d")) * 1_000_000 + 90000
    return [(prefix + offset, base_price + index * 5.0) for index, offset in enumerate(seconds_offsets)]


@pytest.fixture(autouse=True)
def _clean_scheduler(monkeypatch):
    """每个用例前后清空全局调度任务，避免相互串扰。

    Args:
        无。

    Returns:
        Iterator[None]: 用例结束后再次清空。
    """

    unschedule_all()
    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", lambda **kwargs: pd.DataFrame())
    yield
    unschedule_all()


def _engine(frequency: str = "tick") -> BacktestEngine:
    """构造带上下文的回测引擎。

    Args:
        frequency: 回测频率。

    Returns:
        BacktestEngine: 已绑定 Context 的引擎实例。
    """

    engine = BacktestEngine(frequency=frequency)
    engine.context = Context(
        portfolio=Portfolio(
            total_value=CASH,
            available_cash=CASH,
            transferable_cash=CASH,
            starting_cash=CASH,
        ),
        current_dt=TRADE_DAY,
    )
    return engine


def _patch_loader(monkeypatch, streams: Dict[str, TickDayStream], calls: List[str] = None):
    """把引擎的按日取数替换为本地事件流桩。

    Args:
        monkeypatch: pytest 的 monkeypatch fixture。
        streams: 代码到事件流的映射；缺项表示当日无数据。
        calls: 可选列表，用于记录被请求过的代码。

    Returns:
        None: 直接改写 `bullet_trade.core.engine.load_tick_day`。
    """

    def fake_load(code, day, loader=None, required=True):
        if calls is not None:
            calls.append(code)
        stream = streams.get(code)
        if stream is None:
            if required:
                raise TickDataMissingError(f"{code} 在 {day.isoformat()} 缺少 tick 数据")
            return None
        return stream

    monkeypatch.setattr("bullet_trade.core.engine.load_tick_day", fake_load)


def test_pre_open_callback_runs_before_first_tick(monkeypatch):
    """盘前回调必须先于当日首笔 tick，且共用同一回放时钟。"""
    events: List[tuple] = []
    engine = _engine()
    engine.before_trading_start_func = lambda context: events.append(
        ("before", context.current_dt)
    )
    engine.handle_tick_func = lambda context, tick: events.append(("tick", context.current_dt))
    _patch_loader(monkeypatch, {CODE_A: _stream(CODE_A, _stamps(DAY, [0, 1, 2]))})
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert events[0] == ("before", datetime(2021, 6, 8, 8, 30))
    tick_times = [dt for label, dt in events if label == "tick"]
    assert tick_times == [
        datetime(2021, 6, 8, 9, 0, 0),
        datetime(2021, 6, 8, 9, 0, 1),
        datetime(2021, 6, 8, 9, 0, 2),
    ]
    assert tick_times == sorted(tick_times)


def test_handle_tick_receives_every_tick_in_stable_source_order(monkeypatch):
    """乱序源 tick 应按时间戳稳定排序后逐笔投递，一笔不多一笔不少。"""
    prices: List[float] = []
    engine = _engine()
    engine.handle_tick_func = lambda context, tick: prices.append(tick.current)
    shuffled = _stream(
        CODE_A,
        [
            (20210608090002.0, 300.0),
            (20210608090000.0, 100.0),
            (20210608090001.0, 200.0),
            (20210608090001.0, 250.0),
        ],
    )
    _patch_loader(monkeypatch, {CODE_A: shuffled})
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    # 同时间戳保持源内先后：200.0 在 250.0 之前
    assert prices == [100.0, 200.0, 250.0, 300.0]


def test_scheduled_task_runs_once_with_multiple_ticks_at_same_time(monkeypatch):
    """一个定时事件与多笔同刻行情相遇时，任务只运行一次。"""
    engine = _engine()
    events = []
    run_daily(lambda context: events.append("scheduled"), time="09:00")
    engine.handle_tick_func = lambda context, tick: events.append(tick.code)
    _patch_loader(
        monkeypatch,
        {
            CODE_A: _stream(CODE_A, _stamps(DAY, [0, 0])),
            CODE_B: _stream(CODE_B, _stamps(DAY, [0])),
        },
    )
    engine.register_backtest_tick_subscription(CODE_A)
    engine.register_backtest_tick_subscription(CODE_B)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert events == ["scheduled", CODE_A, CODE_A, CODE_B]


def test_close_event_observes_last_ticks_from_all_subscriptions(monkeypatch):
    """收盘定时任务与盘后回调都必须观察到同刻最后一笔行情。"""
    engine = _engine()
    events = []

    def observe_close(context):
        events.append(
            (
                "scheduled_close",
                core_api.get_current_tick(CODE_A).current,
                core_api.get_current_tick(CODE_B).current,
            )
        )

    run_daily(observe_close, time="15:00")
    engine.handle_tick_func = lambda context, tick: events.append((tick.code, tick.current))
    engine.after_trading_end_func = lambda context: events.append(("after",))
    _patch_loader(
        monkeypatch,
        {
            CODE_A: _stream(CODE_A, [(20210608150000.0, 200.0)]),
            CODE_B: _stream(
                CODE_B, [(20210608150000.0, 300.0), (20210608150000.0, 301.0)]
            ),
        },
    )
    engine.register_backtest_tick_subscription(CODE_A)
    engine.register_backtest_tick_subscription(CODE_B)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert events == [
        (CODE_A, 200.0),
        (CODE_B, 300.0),
        (CODE_B, 301.0),
        ("scheduled_close", 200.0, 301.0),
        ("after",),
    ]


def test_merged_timeline_interleaves_two_subscribed_codes(monkeypatch):
    """多标的订阅时按时间戳归并投递，时钟单调不减。"""
    delivered: List[tuple] = []
    engine = _engine()
    engine.handle_tick_func = lambda context, tick: delivered.append(
        (tick.code, tick.datetime, context.current_dt)
    )
    _patch_loader(
        monkeypatch,
        {
            CODE_A: _stream(CODE_A, _stamps(DAY, [0, 2, 4])),
            CODE_B: _stream(CODE_B, _stamps(DAY, [1, 3, 5], base_price=18000.0)),
        },
    )
    engine.register_backtest_tick_subscription(CODE_A)
    engine.register_backtest_tick_subscription(CODE_B)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert [code for code, _, _ in delivered] == [CODE_A, CODE_B] * 3
    tick_dts = [dt for _, dt, _ in delivered]
    assert tick_dts == sorted(tick_dts)
    # 回放时钟与投递的 tick 时刻一致
    assert all(dt == ctx_dt for _, dt, ctx_dt in delivered)


def test_subscribe_in_before_trading_start_starts_delivery(monkeypatch):
    """盘前订阅的合约自当日首笔 tick 起投递。"""
    delivered: List[str] = []
    engine = _engine()

    def before_trading_start(context):
        core_api.subscribe(CODE_A, "tick")

    engine.before_trading_start_func = before_trading_start
    engine.handle_tick_func = lambda context, tick: delivered.append(tick.code)
    _patch_loader(monkeypatch, {CODE_A: _stream(CODE_A, _stamps(DAY, [0, 1]))})

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert delivered == [CODE_A, CODE_A]


def test_mid_day_subscribe_does_not_backfill_earlier_ticks(monkeypatch):
    """盘中新增订阅只从当前回放时点起投递，此前的 tick 不补投。"""
    delivered: List[tuple] = []
    engine = _engine()

    def handle_tick(context, tick):
        delivered.append((tick.code, tick.datetime))
        if tick.code == CODE_A and len([d for d in delivered if d[0] == CODE_A]) == 2:
            core_api.subscribe(CODE_B, "tick")

    engine.handle_tick_func = handle_tick
    _patch_loader(
        monkeypatch,
        {
            CODE_A: _stream(CODE_A, _stamps(DAY, [0, 10, 20, 30])),
            CODE_B: _stream(CODE_B, _stamps(DAY, [5, 15, 25], base_price=18000.0)),
        },
    )
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    subscribe_at = datetime(2021, 6, 8, 9, 0, 10)
    b_times = [dt for code, dt in delivered if code == CODE_B]
    # 09:00:05 早于订阅时点，被跳过；其余按序投递
    assert b_times == [datetime(2021, 6, 8, 9, 0, 15), datetime(2021, 6, 8, 9, 0, 25)]
    assert all(dt > subscribe_at for dt in b_times)
    assert [dt for code, dt in delivered if code == CODE_A] == [
        datetime(2021, 6, 8, 9, 0, 0),
        subscribe_at,
        datetime(2021, 6, 8, 9, 0, 20),
        datetime(2021, 6, 8, 9, 0, 30),
    ]


def test_unsubscribe_all_stops_delivery(monkeypatch):
    """unsubscribe_all 之后不再有任何 tick 投递。"""
    delivered: List[datetime] = []
    engine = _engine()

    def handle_tick(context, tick):
        delivered.append(tick.datetime)
        if len(delivered) == 2:
            core_api.unsubscribe_all()

    engine.handle_tick_func = handle_tick
    _patch_loader(monkeypatch, {CODE_A: _stream(CODE_A, _stamps(DAY, [0, 1, 2, 3, 4]))})
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert len(delivered) == 2
    assert engine._tick_subscriptions == set()


def test_unsubscribe_single_code_keeps_others(monkeypatch):
    """退订单个合约只停该合约的投递，其余订阅继续。"""
    delivered: List[str] = []
    engine = _engine()

    def handle_tick(context, tick):
        delivered.append(tick.code)
        if tick.code == CODE_A:
            core_api.unsubscribe(CODE_A, "tick")

    engine.handle_tick_func = handle_tick
    _patch_loader(
        monkeypatch,
        {
            CODE_A: _stream(CODE_A, _stamps(DAY, [0, 2, 4])),
            CODE_B: _stream(CODE_B, _stamps(DAY, [1, 3, 5], base_price=18000.0)),
        },
    )
    engine.register_backtest_tick_subscription(CODE_A)
    engine.register_backtest_tick_subscription(CODE_B)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert delivered == [CODE_A, CODE_B, CODE_B, CODE_B]


def test_get_current_tick_reads_replay_buffer(monkeypatch):
    """handle_tick 内查询等于当前 tick；首笔 tick 之前返回 None 而不抛异常。"""
    observations: List[tuple] = []
    engine = _engine()

    def before_trading_start(context):
        observations.append(("before", core_api.get_current_tick(CODE_A)))

    def handle_tick(context, tick):
        observations.append(("tick", core_api.get_current_tick(CODE_A)))

    engine.before_trading_start_func = before_trading_start
    engine.handle_tick_func = handle_tick
    _patch_loader(monkeypatch, {CODE_A: _stream(CODE_A, _stamps(DAY, [0, 1]))})
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert observations[0] == ("before", None)
    tick_snapshots = [value for label, value in observations if label == "tick"]
    assert [snapshot.current for snapshot in tick_snapshots] == [17000.0, 17005.0]
    assert all(snapshot.code == CODE_A for snapshot in tick_snapshots)


def test_get_current_tick_returns_none_for_unsubscribed_code(monkeypatch):
    """未订阅标的在 tick 回放中不得回退到分钟线合成，直接返回 None。"""
    seen: List[object] = []
    engine = _engine()
    engine.handle_tick_func = lambda context, tick: seen.append(
        core_api.get_current_tick(CODE_B)
    )
    _patch_loader(monkeypatch, {CODE_A: _stream(CODE_A, _stamps(DAY, [0]))})
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert seen == [None]


def test_current_data_uses_each_published_tick_without_provider_calls(monkeypatch):
    """当前行情跟随同刻多笔快照更新，盘前及未订阅标的不得用 bar 补价。"""
    from bullet_trade.core.settings import set_option
    from bullet_trade.data import api as data_api

    engine = _engine()
    set_option("avoid_future_data", True)
    observations = []
    containers = []
    provider_calls = []

    def reject_provider():
        provider_calls.append(True)
        raise AssertionError("回放当前行情不得访问数据源")

    monkeypatch.setattr(data_api, "_get_default_provider", reject_provider)

    def before(context):
        data = data_api.get_current_data()
        containers.append(data)
        observations.append(("before", data[CODE_A].last_price, CODE_A in data))

    def handle_tick(context, tick):
        data = containers[0]
        quote = data[CODE_A]
        observations.append(
            (quote.last_price, quote.source, quote.source_time, data[CODE_B].last_price)
        )

    engine.before_trading_start_func = before
    engine.handle_tick_func = handle_tick
    _patch_loader(
        monkeypatch,
        {CODE_A: _stream(CODE_A, [(20210608090001.0, 110.0), (20210608090001.0, 115.0)])},
    )
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    tick_dt = datetime(2021, 6, 8, 9, 0, 1)
    assert observations == [
        ("before", 0.0, False),
        (110.0, "tick_replay", tick_dt, 0.0),
        (115.0, "tick_replay", tick_dt, 0.0),
    ]
    assert provider_calls == []


def test_current_data_hides_snapshots_outside_current_day_and_clock(monkeypatch):
    """缓存的过日或未到达快照不能在当前行情里变成可成交价格。"""
    from bullet_trade.core.runtime import set_current_engine
    from bullet_trade.data import api as data_api

    monkeypatch.setattr(
        data_api, "_get_default_provider", lambda: pytest.fail("回放当前行情不得访问数据源")
    )
    engine = _engine()
    engine.context.current_dt = datetime(2021, 6, 8, 9, 0)
    set_current_engine(engine)
    data_api.set_current_context(engine.context)
    quote = _stream(CODE_A, [(20210608090001.0, 110.0)]).advance()
    engine._tick_snapshots[CODE_A] = quote
    data = data_api.get_current_data()

    assert data[CODE_A].last_price == 0.0
    assert CODE_A not in data
    engine.context.current_dt = datetime(2021, 6, 8, 9, 0, 1)
    assert data[CODE_A].last_price == 110.0
    engine.context.current_dt = datetime(2021, 6, 9, 9, 0)
    assert data[CODE_A].last_price == 0.0


def test_day_open_is_prefetched_before_ticks_and_revealed_with_first_snapshot(monkeypatch):
    """真实日开盘价单独预取，首笔行情价与日开盘价不同也不得混用。"""
    from bullet_trade.core.settings import set_option
    from bullet_trade.data import api as data_api

    engine = _engine()
    set_option("avoid_future_data", True)
    calls = []
    observations = []

    def daily_open(**kwargs):
        calls.append((engine._tick_delivery_started, kwargs))
        return pd.DataFrame({"open": [105.0]}, index=pd.to_datetime([DAY]))

    def before(context):
        core_api.subscribe(CODE_A, "tick")
        with pytest.raises(TickDataMissingError, match="day_open"):
            data_api.get_current_data()[CODE_A].day_open
        observations.append("before_open_hidden")

    def handle_tick(context, tick):
        quote = data_api.get_current_data()[CODE_A]
        observations.append((quote.last_price, quote.day_open))

    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", daily_open)
    engine.before_trading_start_func = before
    engine.handle_tick_func = handle_tick
    _patch_loader(
        monkeypatch,
        {CODE_A: _stream(CODE_A, [(20210608090001.0, 110.0), (20210608090002.0, 115.0)])},
    )

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert observations == ["before_open_hidden", (110.0, 105.0), (115.0, 105.0)]
    assert calls == [
        (
            False,
            {
                "security": CODE_A,
                "start_date": DAY,
                "end_date": DAY,
                "frequency": "daily",
                "fields": ["open"],
                "fq": "none",
            },
        )
    ]
    assert data_api._should_avoid_future() is True


@pytest.mark.parametrize("cached_before_ticks", [False, True])
def test_intraday_subscription_never_fetches_day_open_from_upstream(monkeypatch, cached_before_ticks):
    """投递开始后只能复用已预取开盘价，缺失时显式失败而不查询日线。"""
    from bullet_trade.data import api as data_api

    engine = _engine()
    queries = []
    observed = []

    def daily_open(**kwargs):
        queries.append(kwargs["security"])
        return pd.DataFrame({"open": [105.0]}, index=pd.to_datetime([DAY]))

    def handle_tick(context, tick):
        if tick.code == CODE_A:
            core_api.subscribe(CODE_B, "tick")
        else:
            observed.append(data_api.get_current_data()[CODE_B].day_open)

    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", daily_open)
    engine.handle_tick_func = handle_tick
    engine.register_backtest_tick_subscription(CODE_A)
    if cached_before_ticks:
        engine.register_backtest_tick_subscription(CODE_B)
        engine.before_trading_start_func = lambda context: core_api.unsubscribe(CODE_B, "tick")
    _patch_loader(
        monkeypatch,
        {
            CODE_A: _stream(CODE_A, [(20210608090001.0, 110.0)]),
            CODE_B: _stream(CODE_B, [(20210608090002.0, 115.0)]),
        },
    )

    if cached_before_ticks:
        engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)
        assert observed == [105.0]
        assert queries == [CODE_A, CODE_B]
    else:
        with pytest.raises(TickDataMissingError, match="day_open"):
            engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)
        assert observed == []
        assert queries == [CODE_A]


def test_missing_day_open_in_scheduled_callback_aborts_replay(monkeypatch):
    """定时回调读取缺失元数据必须让回放失败，不能只写日志后继续。"""
    from bullet_trade.data import api as data_api

    engine = _engine()
    run_daily(lambda context: data_api.get_current_data()[CODE_A].day_open, time="09:01")
    _patch_loader(monkeypatch, {CODE_A: _stream(CODE_A, [(20210608090001.0, 110.0)])})
    engine.register_backtest_tick_subscription(CODE_A)

    with pytest.raises(TickDataMissingError, match="day_open"):
        engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)


def test_tick_order_can_fill_without_reading_unavailable_day_open(monkeypatch):
    """不依赖日开盘价的订单仍可按有效 tick 成交，撮合不能隐式读取缺失字段。"""
    from bullet_trade.core.orders import order as place_order
    from bullet_trade.data import api as data_api

    stock = "000001.XSHE"
    engine = _engine()
    engine._update_current_time(datetime(2021, 6, 8, 9), None)
    engine._tick_day = DAY
    engine._tick_snapshots[stock] = _stream(stock, [(20210608090000.0, 100.0)]).advance()
    monkeypatch.setattr(
        "bullet_trade.core.orders._trigger_order_processing", lambda *args, **kwargs: None
    )
    monkeypatch.setattr("bullet_trade.core.engine.get_security_info", lambda _security: {})
    monkeypatch.setattr(engine, "_apply_slippage_price", lambda price, *args: price)
    monkeypatch.setattr(engine, "_infer_security_category", lambda *args, **kwargs: "stock")
    monkeypatch.setattr(engine, "_infer_tplus_from_info", lambda info: 0)
    monkeypatch.setattr(
        data_api, "_get_default_provider", lambda: pytest.fail("撮合不得用数据源补价")
    )

    placed = place_order(stock, 100)
    engine._process_orders(engine.context.current_dt)

    assert placed.status == OrderStatus.filled
    assert placed.price == 100.0
    with pytest.raises(TickDataMissingError, match="day_open"):
        data_api.get_current_data()[stock].day_open


def test_tick_callback_observes_current_futures_equity(monkeypatch):
    """无定时任务的逐笔回调也应先更新权益，并保留今仓数量。"""
    from bullet_trade.core.futures_account import ContractSpecTable, FuturesAccount

    engine = _engine()
    account = FuturesAccount(
        cash=CASH,
        spec_table=ContractSpecTable(margin_rate=0.14, load_config=False, enable_remote=False),
    )
    position = account.open(CODE_A, "long", 1, 100.0, trade_time=TRADE_DAY)
    engine.context.portfolio.futures_account = account
    engine.context.portfolio.available_cash = account.cash
    engine.context.portfolio.update_value()
    observations = []

    def reject_bar(*args, **kwargs):
        raise AssertionError("逐笔估值不得访问 bar")

    monkeypatch.setattr(engine, "_resolve_base_exec_price", reject_bar)
    engine.handle_tick_func = lambda context, tick: observations.append(
        (context.portfolio.total_value, position.last_price, position.today_amount)
    )
    _patch_loader(monkeypatch, {CODE_A: _stream(CODE_A, [(20210608090001.0, 110.0)])})
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert observations == [(pytest.approx(CASH + 160.0), 110.0, 1)]


def test_first_tick_applies_dated_margin_rate_without_settling_price_changes(monkeypatch):
    """费率生效首笔行情补冻保证金，后续价格变化只影响权益且不重设结算价。"""
    from bullet_trade.core.contract_specs import FuturesSpecConfig, MarginRateRule
    from bullet_trade.core.futures_account import ContractSpecTable, FuturesAccount

    config = FuturesSpecConfig(
        margin_rules={
            "LH": MarginRateRule(product="LH", rate=0.10, effective=((DAY, 0.20),)),
        },
    )
    account = FuturesAccount(
        cash=CASH,
        spec_table=ContractSpecTable(config=config, load_config=False, enable_remote=False),
    )
    previous_day = DAY - timedelta(days=1)
    position = account.open(
        CODE_A, "long", 1, 27000.0, trade_time=TRADE_DAY - timedelta(days=1)
    )
    account.settle_day({CODE_A: 27000.0}, day=previous_day)
    engine = _engine()
    engine.context.portfolio.futures_account = account
    engine.context.portfolio.available_cash = account.cash
    engine.context.portfolio.update_value()
    observations = []

    def handle_tick(context, tick):
        observations.append(
            (
                context.portfolio.available_cash,
                account.margin,
                context.portfolio.total_value,
                position.prev_settlement,
                position.margin_rate,
                position.today_amount,
            )
        )

    engine.handle_tick_func = handle_tick
    _patch_loader(
        monkeypatch,
        {CODE_A: _stream(CODE_A, [(20210608090001.0, 27000.0), (20210608090002.0, 28000.0)])},
    )
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert observations == [
        (pytest.approx(CASH - 86400.0), 86400.0, CASH, 27000.0, 0.20, 0),
        (pytest.approx(CASH - 86400.0), 86400.0, CASH + 16000.0, 27000.0, 0.20, 0),
    ]


def test_tick_query_cannot_reach_past_replay_clock(monkeypatch):
    """开启未来数据守卫后，tick 回放中的 tick 查询不得越过回放时钟。"""
    from bullet_trade.core.exceptions import FutureDataError
    from bullet_trade.core.settings import reset_settings, set_option
    from bullet_trade.data import api as data_api

    reset_settings()
    set_option("avoid_future_data", True)
    seen: List[datetime] = []

    class _StubProvider:
        """只记录 end_dt 的桩数据源，不做任何网络访问。"""

        name = "stub"

        def auth(self):
            return None

        def get_ticks(self, **kwargs):
            seen.append(kwargs["end_dt"])
            return []

    monkeypatch.setattr(data_api, "_get_default_provider", lambda: _StubProvider())

    engine = _engine()

    def handle_tick(context, tick):
        with pytest.raises(FutureDataError):
            data_api.get_ticks(CODE_A, end_dt=tick.datetime + timedelta(hours=1), df=True)
        data_api.get_ticks(CODE_A, end_dt=tick.datetime, df=True)

    engine.handle_tick_func = handle_tick
    _patch_loader(monkeypatch, {CODE_A: _stream(CODE_A, _stamps(DAY, [0, 1]))})
    engine.register_backtest_tick_subscription(CODE_A)

    try:
        engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)
    finally:
        reset_settings()

    assert seen == [datetime(2021, 6, 8, 9, 0, 0), datetime(2021, 6, 8, 9, 0, 1)]


def test_missing_tick_day_aborts_with_code_and_date(monkeypatch):
    """已订阅合约缺日必须中止回测，错误信息含代码与日期。"""
    engine = _engine()
    engine.handle_tick_func = lambda context, tick: None
    _patch_loader(monkeypatch, {})
    engine.register_backtest_tick_subscription(CODE_A)

    with pytest.raises(TickDataMissingError) as excinfo:
        engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    message = str(excinfo.value)
    assert CODE_A in message
    assert DAY.isoformat() in message


def test_unsubscribed_symbol_gap_does_not_abort(monkeypatch):
    """未订阅标的缺数据只影响它自己，不得中止回测。"""
    delivered: List[str] = []
    engine = _engine()
    engine.handle_tick_func = lambda context, tick: delivered.append(tick.code)
    calls: List[str] = []
    _patch_loader(monkeypatch, {CODE_A: _stream(CODE_A, _stamps(DAY, [0, 1]))}, calls=calls)
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert delivered == [CODE_A, CODE_A]
    assert calls == [CODE_A]


def test_day_frequency_never_touches_tick_loader(monkeypatch):
    """日频回测不得触发 tick 取数，也不得调用 handle_tick。"""
    delivered: List[str] = []
    calls: List[str] = []
    engine = _engine(frequency="day")
    engine.handle_tick_func = lambda context, tick: delivered.append(tick.code)
    _patch_loader(monkeypatch, {CODE_A: _stream(CODE_A, _stamps(DAY, [0, 1]))}, calls=calls)
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert calls == []
    assert delivered == []
    assert engine.is_tick_backtest() is False
    assert engine.get_current_tick_snapshot(CODE_A) is None


def test_day_frequency_keeps_handle_data_at_open(monkeypatch):
    """tick 改造不得改变日频路径：handle_data 仍在开盘时刻调用一次。"""
    seen: List[datetime] = []
    engine = _engine(frequency="day")
    engine.handle_data_func = lambda context, data: seen.append(context.current_dt)
    _patch_loader(monkeypatch, {})

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert seen == [datetime(2021, 6, 8, 9, 0)]


def test_tick_frequency_still_runs_handle_data_once(monkeypatch):
    """tick 频率下开盘回调仍执行一次，逐笔事件不重复触发它。"""
    seen: List[datetime] = []
    engine = _engine()
    engine.handle_data_func = lambda context, data: seen.append(context.current_dt)
    engine.handle_tick_func = lambda context, tick: None
    _patch_loader(monkeypatch, {CODE_A: _stream(CODE_A, _stamps(DAY, [0, 1, 2]))})
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert seen == [datetime(2021, 6, 8, 9, 0)]


def test_close_callback_not_duplicated_by_tick_at_close(monkeypatch):
    """tick 正好落在收盘时刻时，盘后回调只执行一次。"""
    closes: List[datetime] = []
    delivered: List[datetime] = []
    engine = _engine()
    engine.after_trading_end_func = lambda context: closes.append(context.current_dt)
    engine.handle_tick_func = lambda context, tick: delivered.append(tick.datetime)
    _patch_loader(
        monkeypatch, {CODE_A: _stream(CODE_A, [(20210608145959.0, 17000.0), (20210608150000.0, 17005.0)])}
    )
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert closes == [datetime(2021, 6, 8, 15, 0)]
    assert delivered == [datetime(2021, 6, 8, 14, 59, 59), datetime(2021, 6, 8, 15, 0)]


def test_ticks_after_close_are_drained_with_warning(monkeypatch):
    """行情时段超出交易时段配置时，剩余 tick 仍被消费而不是静默丢弃。"""
    delivered: List[datetime] = []
    engine = _engine()
    engine.handle_tick_func = lambda context, tick: delivered.append(tick.datetime)
    late = _stream(CODE_A, [(20210608153000.0, 17000.0), (20210608154500.0, 17005.0)])
    _patch_loader(monkeypatch, {CODE_A: late})
    engine.register_backtest_tick_subscription(CODE_A)

    engine._run_trading_day(TRADE_DAY, MARKET_PERIODS)

    assert delivered == [datetime(2021, 6, 8, 15, 30), datetime(2021, 6, 8, 15, 45)]


def test_async_entry_rejects_tick_frequency():
    """异步回测入口对 tick 频率必须显式报错，不得静默漏掉逐笔事件。"""
    engine = AsyncBacktestEngine(frequency="tick")

    with pytest.raises(NotImplementedError):
        asyncio.run(engine.run_async(start_date="2021-06-08", end_date="2021-06-08"))


def test_day_prefetch_not_blocked_by_future_guard(monkeypatch):
    """整日预取的取数窗口晚于回放时钟属预期，不得被未来数据守卫拦下；预取后守卫恢复。"""
    from bullet_trade.core.settings import reset_settings, set_option
    from bullet_trade.data import api as data_api

    reset_settings()
    set_option("avoid_future_data", True)
    data_api.set_current_context(_engine().context)

    observed: List[tuple] = []

    def fake_get_ticks(**kwargs):
        observed.append((kwargs["start_dt"], kwargs["end_dt"], data_api._should_avoid_future()))
        return pd.DataFrame([{"time": 20210608090000.0, "current": 17000.0}])

    monkeypatch.setattr(data_api, "get_ticks", fake_get_ticks)

    engine = _engine()
    try:
        stream = engine._load_tick_stream(CODE_A, DAY)
    finally:
        data_api.set_current_context(None)
        reset_settings()

    assert len(stream) == 1
    assert observed == [
        (datetime(2021, 6, 8, 0, 0), datetime(2021, 6, 8, 23, 59, 59), False)
    ]


def test_future_guard_restored_after_prefetch(monkeypatch):
    """守卫只在预取期间暂停，退出后必须恢复拦截越界时刻。"""
    from bullet_trade.core.exceptions import FutureDataError
    from bullet_trade.core.settings import reset_settings, set_option
    from bullet_trade.data import api as data_api

    reset_settings()
    set_option("avoid_future_data", True)
    context = _engine().context
    data_api.set_current_context(context)

    monkeypatch.setattr(
        data_api,
        "get_ticks",
        lambda *args, **kwargs: pd.DataFrame([{"time": 20210608090000.0, "current": 17000.0}]),
    )

    future_dt = TRADE_DAY + timedelta(days=1)
    try:
        with pytest.raises(FutureDataError):
            data_api._ensure_not_future_dt(future_dt, "probe")

        _engine()._load_tick_stream(CODE_A, DAY)

        assert data_api._should_avoid_future() is True
        with pytest.raises(FutureDataError):
            data_api._ensure_not_future_dt(future_dt, "probe")
    finally:
        data_api.set_current_context(None)
        reset_settings()


def test_register_order_stamps_replay_time_once():
    """回测下 add_time 取回放时刻，且只在首次登记时写入，避免挂单时间被后移。"""
    engine = _engine()
    order = Order(
        order_id="test-order-1",
        security=CODE_A,
        amount=1,
        price=17000.0,
        status=OrderStatus.open,
        add_time=datetime(2026, 1, 1, 12, 0),
    )

    engine._register_order(order)
    assert order.add_time == TRADE_DAY

    engine.context.current_dt = TRADE_DAY + timedelta(hours=1)
    engine._register_order(order)
    assert order.add_time == TRADE_DAY


def test_market_order_protect_price_follows_tick_basis(monkeypatch):
    """tick 回放下市价单保护价须与撮合基准同源，bar 价与 tick 价的正常价差不得让订单被取消。"""
    from bullet_trade.core.models import SecurityUnitData
    from bullet_trade.core.orders import clear_order_queue
    from bullet_trade.core.orders import order as place_order
    from bullet_trade.data.tick_replay import TickSnapshot

    stock = "000001.XSHE"
    bar_price = 100.0
    # 低于 bar 价 2%，超过市价单默认 1.5% 的保护带；保护价若取 bar 价就会判为越界
    tick_price = 98.0

    engine = _engine()
    monkeypatch.setattr(
        "bullet_trade.core.orders._trigger_order_processing", lambda *args, **kwargs: None
    )
    monkeypatch.setattr("bullet_trade.core.engine.get_security_info", lambda _security: {})
    monkeypatch.setattr(engine, "_resolve_base_exec_price", lambda _s, _dt, _fq: bar_price)
    monkeypatch.setattr(engine, "_apply_slippage_price", lambda p, _is_buy, _security: p)
    monkeypatch.setattr(engine, "_infer_security_category", lambda _s, info=None: "stock")
    monkeypatch.setattr(engine, "_infer_tplus_from_info", lambda info: 0)

    def _bar_data():
        return {
            stock: SecurityUnitData(
                security=stock,
                last_price=bar_price,
                high_limit=bar_price * 1.1,
                low_limit=bar_price * 0.9,
                paused=False,
            )
        }

    monkeypatch.setattr("bullet_trade.data.api.get_current_data", _bar_data)

    clear_order_queue()
    try:
        engine.context.current_dt = TRADE_DAY + timedelta(hours=9)
        engine._tick_snapshots[stock] = _stream(
            stock, [(20210608090000.0, bar_price)]
        ).advance()
        buy_order = place_order(stock, 100)
        engine._process_orders(engine.context.current_dt)
        assert buy_order.status == OrderStatus.filled
        engine.context.portfolio.positions[stock].closeable_amount = 100
        clear_order_queue()

        engine.context.current_dt = TRADE_DAY + timedelta(hours=9, minutes=30)
        engine._tick_snapshots[stock] = TickSnapshot(
            code=stock,
            datetime=TRADE_DAY + timedelta(hours=9, minutes=30),
            time=20210608093000.0,
            current=tick_price,
            high=bar_price,
            low=tick_price,
            volume=0.0,
            money=0.0,
            position=0.0,
            a1_p=tick_price,
            a1_v=1000.0,
            b1_p=tick_price,
            b1_v=1000.0,
        )

        sell_order = place_order(stock, -100)
        engine._process_orders(engine.context.current_dt)
    finally:
        clear_order_queue()
        engine._tick_snapshots.clear()

    assert sell_order.status == OrderStatus.filled
    assert sell_order.price == pytest.approx(tick_price)
