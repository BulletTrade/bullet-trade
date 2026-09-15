import datetime as dt
from typing import List, Optional

import pytest

from bullet_trade.core.scheduler import (
    generate_daily_schedule,
    get_market_periods,
    get_tasks,
    get_trade_calendar,
    run_daily,
    run_monthly,
    run_weekly,
    set_trade_calendar,
    unschedule_all,
)


@pytest.fixture(autouse=True)
def reset_scheduler():
    unschedule_all()
    set_trade_calendar([], dt.date.today())
    yield
    unschedule_all()
    set_trade_calendar([], dt.date.today())


def _build_schedule(
    day: dt.datetime,
    calendar_days: Optional[List[dt.date]] = None,
    start_date: Optional[dt.date] = None,
):
    periods = get_market_periods()
    calendar_days = calendar_days or [day.date()]
    start = start_date or calendar_days[0]
    set_trade_calendar(calendar_days, start)
    return generate_daily_schedule(day, get_trade_calendar(), lambda _ref=None: periods)


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
    run_weekly(lambda ctx: None, weekday=3, time="open-30m")  # 当周第3个交易日
    wednesday = dt.datetime(2024, 6, 12)  # 周三
    tuesday = dt.datetime(2024, 6, 11)    # 周二
    calendar_days = [
        dt.date(2024, 6, 10),
        dt.date(2024, 6, 11),
        dt.date(2024, 6, 12),
        dt.date(2024, 6, 13),
        dt.date(2024, 6, 14),
    ]

    schedule_wed = _build_schedule(wednesday, calendar_days)
    schedule_tue = _build_schedule(tuesday, calendar_days)

    expected_dt = dt.datetime(2024, 6, 12, 9, 0)
    assert expected_dt in schedule_wed
    assert dt.datetime(2024, 6, 11, 9, 0) not in schedule_tue


def test_weekly_force_false_skips_partial_week():
    run_weekly(lambda ctx: None, weekday=1, time="10:00", force=False)
    calendar_days = [
        dt.date(2024, 6, 10),
        dt.date(2024, 6, 11),
        dt.date(2024, 6, 12),
        dt.date(2024, 6, 13),
        dt.date(2024, 6, 14),
        dt.date(2024, 6, 17),
    ]
    # 回测起始日在 6 月 11 日，force=False 不补跑当周
    early_week = _build_schedule(dt.datetime(2024, 6, 11), calendar_days, start_date=dt.date(2024, 6, 11))
    next_week = _build_schedule(dt.datetime(2024, 6, 17), calendar_days, start_date=dt.date(2024, 6, 11))
    assert dt.datetime(2024, 6, 11, 10, 0) not in early_week
    assert dt.datetime(2024, 6, 17, 10, 0) in next_week


def test_weekly_negative_index_triggers_last_trade_day():
    run_weekly(lambda ctx: None, weekday=-1, time="10:00")
    calendar_days = [
        dt.date(2024, 6, 10),
        dt.date(2024, 6, 11),
        dt.date(2024, 6, 12),
        dt.date(2024, 6, 13),
        dt.date(2024, 6, 14),
    ]
    schedule = _build_schedule(dt.datetime(2024, 6, 14), calendar_days)
    assert dt.datetime(2024, 6, 14, 10, 0) in schedule


def test_weekly_monthly_skip_when_day_not_in_calendar():
    run_weekly(lambda ctx: None, weekday=1, time="10:00")
    run_monthly(lambda ctx: None, monthday=1, time="10:00")
    trade_day = dt.datetime(2024, 6, 12)
    calendar_days = [
        dt.date(2024, 6, 11),
    ]
    schedule = _build_schedule(trade_day, calendar_days)
    assert dt.datetime(2024, 6, 12, 10, 0) not in schedule


def test_monthly_close_offset_rolls_forward_for_holiday():
    run_monthly(lambda ctx: None, monthday=5, time="close+1h")
    # 当月仅有 4 个交易日，force=True 默认就近取最后一个交易日
    trade_day = dt.datetime(2024, 6, 17)
    calendar_days = [
        dt.date(2024, 6, 12),
        dt.date(2024, 6, 13),
        dt.date(2024, 6, 14),
        dt.date(2024, 6, 17),
    ]
    schedule = _build_schedule(trade_day, calendar_days)
    expected = dt.datetime(2024, 6, 17, 16, 0)
    assert expected in schedule


def test_monthly_force_false_drops_overflow():
    run_monthly(lambda ctx: None, monthday=5, time="10:00", force=False)
    calendar_days = [
        dt.date(2024, 6, 12),
        dt.date(2024, 6, 13),
        dt.date(2024, 6, 14),
    ]
    schedule = _build_schedule(dt.datetime(2024, 6, 14), calendar_days, start_date=dt.date(2024, 6, 12))
    assert dt.datetime(2024, 6, 14, 10, 0) not in schedule


def test_daily_accepts_reference_security_and_force():
    run_daily(lambda ctx: None, time="08:50", reference_security="LH9999.XDCE", force=False)
    task = get_tasks()[0]
    assert task.reference_security == "LH9999.XDCE"
    assert task.force is False

    schedule = _build_schedule(dt.datetime(2024, 6, 12, 8, 50))
    assert dt.datetime(2024, 6, 12, 8, 50) in schedule


def test_daily_reference_security_defaults_to_none():
    run_daily(lambda ctx: None, "09:30")
    task = get_tasks()[0]
    assert task.reference_security is None
    assert task.force is True
