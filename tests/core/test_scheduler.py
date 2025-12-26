import datetime as dt
from typing import Optional

import pytest

from bullet_trade.core.scheduler import (
    generate_daily_schedule,
    get_market_periods,
    run_daily,
    run_monthly,
    run_weekly,
    unschedule_all,
)
from bullet_trade.core.settings import set_option


@pytest.fixture(autouse=True)
def reset_scheduler():
    unschedule_all()
    yield
    unschedule_all()


def _build_schedule(day: dt.datetime, previous_day: Optional[dt.date] = None):
    periods = get_market_periods()
    return generate_daily_schedule(day, previous_day, periods)


def test_daily_open_minus_offset():
    run_daily(lambda ctx: None, "open-30m")
    trade_day = dt.datetime(2024, 6, 12)
    schedule = _build_schedule(trade_day)
    expected_dt = dt.datetime(2024, 6, 12, 9, 0)
    assert expected_dt in schedule


def test_daily_close_plus_seconds():
    run_daily(lambda ctx: None, "close+30s")
    trade_day = dt.datetime(2024, 6, 12)
    schedule = _build_schedule(trade_day)
    expected_dt = dt.datetime(2024, 6, 12, 15, 0, 30)
    assert expected_dt in schedule


def test_daily_explicit_time():
    run_daily(lambda ctx: None, "10:00:00")
    trade_day = dt.datetime(2024, 6, 12)
    schedule = _build_schedule(trade_day)
    expected_dt = dt.datetime(2024, 6, 12, 10, 0, 0)
    assert expected_dt in schedule


def test_daily_every_minute_range():
    run_daily(lambda ctx: None, "every_minute")
    trade_day = dt.datetime(2024, 6, 12)
    schedule = _build_schedule(trade_day)
    minute_points = [
        dt for dt, tasks in schedule.items()
        if any(task.time == "every_minute" for task in tasks)
    ]
    assert minute_points[0].time() == dt.time(9, 30)
    assert minute_points[-1].time() == dt.time(14, 59)
    assert len(minute_points) == 240  # 120 分钟 * 2 个交易时段


def test_invalid_expression_rejected():
    with pytest.raises(ValueError):
        run_daily(lambda ctx: None, "not-a-valid-time")


def test_weekly_open_offset_only_on_target_weekday():
    run_weekly(lambda ctx: None, weekday=2, time="open-30m")  # 周三
    wednesday = dt.datetime(2024, 6, 12)  # 周三
    tuesday = dt.datetime(2024, 6, 11)    # 周二

    schedule_wed = _build_schedule(wednesday)
    schedule_tue = _build_schedule(tuesday)

    expected_dt = dt.datetime(2024, 6, 12, 9, 0)
    assert expected_dt in schedule_wed
    assert dt.datetime(2024, 6, 11, 9, 0) not in schedule_tue


def test_weekly_force_runs_on_backtest_start_week_when_start_after_weekday():
    # 回测从周三开始，weekly 任务设为每周一。
    # - force=False: 认为本周已错过，不补跑
    # - force=True: 在回测启动当周的首个可用交易日（即回测首日）补跑一次
    set_option('backtest_start_date', dt.date(2024, 6, 12))  # 周三

    run_weekly(lambda ctx: None, weekday=0, time='10:00', force=False)  # 周一
    schedule_no_force = _build_schedule(dt.datetime(2024, 6, 12), previous_day=None)
    assert dt.datetime(2024, 6, 12, 10, 0) not in schedule_no_force

    unschedule_all()
    run_weekly(lambda ctx: None, weekday=0, time='10:00', force=True)
    schedule_force = _build_schedule(dt.datetime(2024, 6, 12), previous_day=None)
    assert dt.datetime(2024, 6, 12, 10, 0) in schedule_force


def test_weekly_force_only_affects_start_week_next_week_normal():
    set_option('backtest_start_date', dt.date(2024, 6, 12))  # 周三
    run_weekly(lambda ctx: None, weekday=0, time='10:00', force=True)  # 周一

    # 下周一（6/17）应按指定 weekday 正常触发
    schedule_next_monday = _build_schedule(dt.datetime(2024, 6, 17), previous_day=dt.date(2024, 6, 14))
    assert dt.datetime(2024, 6, 17, 10, 0) in schedule_next_monday


def test_monthly_close_offset_rolls_forward_for_holiday():
    run_monthly(lambda ctx: None, monthday=15, time="close+1h")
    # 2024-06-17 是周一，15日为周六，应该顺延到 17 日
    trade_day = dt.datetime(2024, 6, 17)
    previous_trade_day = dt.date(2024, 6, 14)
    schedule = _build_schedule(trade_day, previous_trade_day)
    expected = dt.datetime(2024, 6, 17, 16, 0)
    assert expected in schedule


def test_monthly_force_runs_on_backtest_start_month_when_start_after_monthday():
    # 回测从 6/20 开始，月度任务设为每月 1 号。
    # - force=False: 认为当月已错过，不应在 6 月补跑
    # - force=True: 应在 6 月内最近的交易日（即回测首日）补跑一次
    set_option('backtest_start_date', dt.date(2024, 6, 20))

    run_monthly(lambda ctx: None, monthday=1, time='10:00', force=False)
    schedule_no_force = _build_schedule(dt.datetime(2024, 6, 20), previous_day=None)
    assert dt.datetime(2024, 6, 20, 10, 0) not in schedule_no_force

    unschedule_all()
    run_monthly(lambda ctx: None, monthday=1, time='10:00', force=True)
    schedule_force = _build_schedule(dt.datetime(2024, 6, 20), previous_day=None)
    assert dt.datetime(2024, 6, 20, 10, 0) in schedule_force


def test_monthly_force_only_affects_start_month_next_month_normal():
    set_option('backtest_start_date', dt.date(2024, 6, 20))
    run_monthly(lambda ctx: None, monthday=1, time='10:00', force=True)

    # 进入 7 月后，按 monthday=1 正常运行（这里 7/1 是周一）
    schedule_july = _build_schedule(dt.datetime(2024, 7, 1), previous_day=dt.date(2024, 6, 28))
    assert dt.datetime(2024, 7, 1, 10, 0) in schedule_july
