"""
作者: BruceLee
文件职责:
    验证 tick 回放数据载体的排序、时间戳换算与按日窗口口径，所有期望值均可手工推导。

主要输入:
    内存构造的单日 tick 表（含乱序与同时间戳样例）与 float64 源始时间戳。

主要输出:
    pytest 断言结果，确认稳定排序、时间戳换算精度、盘中订阅跳过、
    单日查询窗口不超过数据源上限以及缺数据显式失败这几条口径成立。

上下游关系:
    上游覆盖 `bullet_trade.data.tick_replay` 的事件流与换算函数；
    下游保护回测引擎合并事件循环的投递顺序与回放时钟不被量化误差打乱。

关键环境或配置约定:
    测试只使用内存 DataFrame 与本地传入的取数函数，不访问任何外部数据源。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from bullet_trade.data.tick_replay import (
    TickDataMissingError,
    TickDayStream,
    day_tick_window,
    load_tick_day,
    tick_time_to_datetime,
    tick_times_to_datetimes,
)

CODE = "LH2109.XDCE"
DAY = date(2021, 6, 8)


def _frame(rows):
    """把 (时间戳, 价格) 序列整理成单日 tick 表。

    Args:
        rows: 每项为 (原始 float 时间戳, 最新价)。

    Returns:
        pd.DataFrame: 含 time/current 两列且索引即源内序号的 tick 表。
    """

    return pd.DataFrame(
        [{"time": float(raw), "current": float(price)} for raw, price in rows]
    )


def test_tick_time_to_datetime_keeps_source_precision():
    """源始时间戳换算后应保留到微秒，且不做四舍五入到毫秒。"""
    stamp = tick_time_to_datetime(20210608085900.02)
    # float64 把 .02 量化成 1/256 秒，真值为 0.019531 秒
    assert stamp == datetime(2021, 6, 8, 8, 59, 0, 19531)
    assert tick_time_to_datetime(20210608090000.5) == datetime(2021, 6, 8, 9, 0, 0, 500000)


def test_tick_time_to_datetime_rejects_bad_input():
    """非法时间戳应直接报错，不得静默落到某个默认时刻。"""
    with pytest.raises(ValueError):
        tick_time_to_datetime(0.0)
    with pytest.raises(ValueError):
        tick_time_to_datetime(float("nan"))


def test_tick_times_to_datetimes_matches_scalar_conversion():
    """批量换算必须与逐条换算完全一致。"""
    raw_times = [20210608085900.02, 20210608090000.5, 20210608143000.0]
    assert tick_times_to_datetimes(raw_times) == [tick_time_to_datetime(v) for v in raw_times]
    assert tick_times_to_datetimes([]) == []


def test_stream_sorts_out_of_order_source_ticks():
    """乱序源 tick 应按原始时间戳升序回放。"""
    stream = TickDayStream(
        CODE,
        DAY,
        _frame(
            [
                (20210608093000.0, 300.0),
                (20210608090000.0, 100.0),
                (20210608091500.0, 200.0),
            ]
        ),
    )
    assert [stream.advance().current for _ in range(3)] == [100.0, 200.0, 300.0]
    assert stream.exhausted
    assert stream.advance() is None


def test_stream_keeps_source_order_for_tied_timestamps():
    """同时间戳的 tick 必须保持源内先后，不能被重排。"""
    stream = TickDayStream(
        CODE,
        DAY,
        _frame(
            [
                (20210608090000.5, 101.0),
                (20210608090000.5, 102.0),
                (20210608090000.5, 103.0),
            ]
        ),
    )
    assert [stream.advance().current for _ in range(3)] == [101.0, 102.0, 103.0]


def test_stream_peek_does_not_consume():
    """peek 只查看不推进游标。"""
    stream = TickDayStream(CODE, DAY, _frame([(20210608090000.0, 100.0)]))
    first = stream.peek()
    assert stream.peek() == first
    assert stream.consumed == 0
    assert stream.advance() == first
    assert stream.peek() is None


def test_stream_snapshot_exposes_quote_fields():
    """快照应携带全部行情字段并提供 last_price 别名。"""
    frame = pd.DataFrame(
        [
            {
                "time": 20210608090000.5,
                "current": 17000.0,
                "high": 17050.0,
                "low": 16980.0,
                "volume": 1200.0,
                "money": 2.04e7,
                "position": 34000.0,
                "a1_p": 17005.0,
                "a1_v": 3.0,
                "b1_p": 17000.0,
                "b1_v": 5.0,
            }
        ]
    )
    snapshot = TickDayStream(CODE, DAY, frame).advance()
    assert snapshot.code == CODE
    assert snapshot.time == 20210608090000.5
    assert snapshot.last_price == snapshot.current == 17000.0
    assert (snapshot.a1_p, snapshot.a1_v, snapshot.b1_p, snapshot.b1_v) == (
        17005.0,
        3.0,
        17000.0,
        5.0,
    )
    assert snapshot.as_dict()["position"] == 34000.0


def test_stream_rejects_frame_without_required_columns():
    """缺少 time 或 current 列时应直接报错。"""
    with pytest.raises(ValueError):
        TickDayStream(CODE, DAY, pd.DataFrame([{"current": 1.0}]))
    with pytest.raises(ValueError):
        TickDayStream(CODE, DAY, None)


def test_skip_until_dt_drops_past_ticks():
    """盘中新增订阅时，起投时刻及之前的 tick 不补投。"""
    stream = TickDayStream(
        CODE,
        DAY,
        _frame(
            [
                (20210608090000.0, 100.0),
                (20210608093000.0, 200.0),
                (20210608100000.0, 300.0),
            ]
        ),
    )
    skipped = stream.skip_until_dt(datetime(2021, 6, 8, 9, 30))
    assert skipped == 2
    assert stream.advance().current == 300.0


def test_day_tick_window_stays_within_source_limit():
    """单日查询窗口不得超过数据源 24 小时上限。"""
    start, end = day_tick_window(DAY)
    assert start == datetime(2021, 6, 8, 0, 0, 0)
    assert end - start == timedelta(hours=23, minutes=59, seconds=59)


def test_load_tick_day_uses_injected_loader():
    """load_tick_day 应把单日窗口透传给取数函数并整理成事件流。"""
    seen = {}

    def loader(security, start_dt, end_dt, df=True):
        seen.update(security=security, start_dt=start_dt, end_dt=end_dt, df=df)
        return _frame([(20210608090000.0, 100.0)])

    stream = load_tick_day(CODE, DAY, loader=loader)
    assert seen == {
        "security": CODE,
        "start_dt": datetime(2021, 6, 8, 0, 0, 0),
        "end_dt": datetime(2021, 6, 8, 23, 59, 59),
        "df": True,
    }
    assert len(stream) == 1


def test_load_tick_day_raises_when_required_and_empty():
    """必需标的当日无数据时抛错，错误信息含代码与日期。"""

    def loader(**kwargs):
        return pd.DataFrame()

    with pytest.raises(TickDataMissingError) as excinfo:
        load_tick_day(CODE, DAY, loader=loader)
    assert CODE in str(excinfo.value)
    assert DAY.isoformat() in str(excinfo.value)


def test_load_tick_day_returns_none_when_optional_and_empty():
    """非必需标的当日无数据时返回 None，不中止回测。"""

    def loader(**kwargs):
        return pd.DataFrame()

    assert load_tick_day(CODE, DAY, loader=loader, required=False) is None


def test_load_tick_day_reuses_session_prefetch():
    """启用数据会话时，同日同标的的第二次加载不得再回源。"""
    from bullet_trade.data.backtest_session import (
        create_backtest_data_session,
        reset_current_backtest_data_session,
        set_current_backtest_data_session,
    )

    calls = []

    def loader(**kwargs):
        calls.append(kwargs)
        return _frame([(20210608090000.0, 100.0), (20210608090001.0, 105.0)])

    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        frequency="tick",
    )
    token = set_current_backtest_data_session(session)
    try:
        assert session.active
        first = load_tick_day(CODE, DAY, loader=loader)
        second = load_tick_day(CODE, DAY, loader=loader)
    finally:
        reset_current_backtest_data_session(token)

    assert len(calls) == 1
    assert len(first) == len(second) == 2
    assert first.advance().current == second.advance().current == 100.0


def test_load_tick_day_without_session_always_calls_loader():
    """未启用数据会话时，每次加载都回源，行为与改造前一致。"""
    calls = []

    def loader(**kwargs):
        calls.append(kwargs)
        return _frame([(20210608090000.0, 100.0)])

    load_tick_day(CODE, DAY, loader=loader)
    load_tick_day(CODE, DAY, loader=loader)

    assert len(calls) == 2
