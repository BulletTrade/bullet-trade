"""
调度框架

提供基于交易时段的定时触发能力（每日/每周/每月与自定义表达式），
保持对现有策略接口的兼容。
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time as Time, timedelta
from enum import Enum
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Any

from .settings import get_settings


# 默认交易时段（A股）
DEFAULT_MARKET_PERIODS: Tuple[Tuple[Time, Time], ...] = (
    (Time(9, 30), Time(11, 30)),
    (Time(13, 0), Time(15, 0)),
)

# 默认时间别名映射
DEFAULT_TIME_ALIASES: Dict[str, str] = {
    "before_open": "open-30m",
    "after_close": "close+30m",
    "morning": "08:00",
    "night": "20:00",
}


def _coerce_time(value) -> Time:
    """将字符串或 time 对象转换为 time"""
    if isinstance(value, Time):
        return value
    if isinstance(value, str):
        value = value.strip()
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(value, fmt).time()
            except ValueError:
                continue
    raise ValueError(f"无法解析时间: {value!r}")


def get_market_periods() -> List[Tuple[Time, Time]]:
    """获取配置的交易时段列表"""
    settings = get_settings()
    custom_periods = settings.options.get("market_period")
    if not custom_periods:
        return [tuple(period) for period in DEFAULT_MARKET_PERIODS]

    normalized: List[Tuple[Time, Time]] = []
    for period in custom_periods:
        if not isinstance(period, (list, tuple)) or len(period) != 2:
            raise ValueError(f"market_period 设置错误: {period}")
        start = _coerce_time(period[0])
        end = _coerce_time(period[1])
        if start >= end:
            raise ValueError(f"market_period 起止时间无效: {period}")
        normalized.append((start, end))
    return normalized


def get_time_aliases() -> Dict[str, str]:
    """合并默认别名与用户自定义别名"""
    settings = get_settings()
    user_aliases = settings.options.get("time_aliases") or {}
    aliases: Dict[str, str] = {**DEFAULT_TIME_ALIASES}
    for key, value in user_aliases.items():
        aliases[str(key).lower()] = str(value)
    return aliases


def parse_market_periods_string(expr: str) -> List[Tuple[Time, Time]]:
    """
    解析环境字符串形式的交易时段，如 "09:30-11:30,13:00-15:00"。
    返回 [(start, end), ...]。
    """
    if not expr:
        return [tuple(period) for period in DEFAULT_MARKET_PERIODS]
    parts = [p.strip() for p in str(expr).split(',') if p.strip()]
    out: List[Tuple[Time, Time]] = []
    for p in parts:
        if '-' not in p:
            raise ValueError(f"market periods 解析失败: {p}")
        a, b = p.split('-', 1)
        out.append((_coerce_time(a), _coerce_time(b)))
    return out


@dataclass(frozen=True)
class TimeExpression:
    """时间表达式，支持显式时间、open/close 偏移、循环等"""

    raw: str
    kind: str
    base: Optional[str] = None
    offset: timedelta = timedelta()
    explicit: Optional[Time] = None

    OFFSET_PATTERN = re.compile(r"(\d+)([hms])")

    @classmethod
    def parse(cls, expr: str, aliases: Dict[str, str]) -> "TimeExpression":
        raw, canon = cls._canonicalize_expr(expr)
        canon = cls._expand_alias(canon, aliases)
        kind = cls._classify_kind(canon)
        if kind == "every_bar":
            return cls(raw=raw, kind=kind)
        if kind == "every_minute":
            return cls(raw=raw, kind=kind)
        explicit = cls._try_parse_explicit(canon)
        if explicit is not None:
            return cls(raw=raw, kind="explicit", explicit=explicit)
        relative = cls._try_parse_relative(canon)
        if relative is not None:
            base, offset = relative
            return cls(raw=raw, kind="relative", base=base, offset=offset)
        raise ValueError(f"无法解析 time 表达式: {raw}")

    @staticmethod
    def _canonicalize_expr(expr: str) -> Tuple[str, str]:
        if not isinstance(expr, str):
            raise ValueError(f"time 参数必须为字符串，收到 {type(expr)}")
        raw = expr
        canon = expr.strip().lower()
        if not canon:
            raise ValueError("time 表达式不能为空")
        return raw, canon

    @staticmethod
    def _expand_alias(expr: str, aliases: Dict[str, str]) -> str:
        return aliases.get(expr, expr)

    @staticmethod
    def _classify_kind(expr: str) -> Optional[str]:
        if expr in ("every_bar",):
            return "every_bar"
        if expr in ("every_minute",):
            return "every_minute"
        return None

    @staticmethod
    def _try_parse_explicit(expr: str) -> Optional[Time]:
        if ":" not in expr:
            return None
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(expr, fmt).time()
            except ValueError:
                continue
        return None

    @classmethod
    def _try_parse_relative(cls, expr: str) -> Optional[Tuple[str, timedelta]]:
        comps = cls._split_relative_components(expr)
        if comps is None:
            return None
        base, sign, offset_str = comps
        if sign is None:
            return base, timedelta()
        offset = cls._parse_offset(offset_str)
        if sign == "-":
            offset = -offset
        return base, offset

    @staticmethod
    def _split_relative_components(expr: str) -> Optional[Tuple[str, Optional[str], Optional[str]]]:
        """拆分相对时间表达式为 (base, sign, offset_str)。
        - 允许 "open"/"close"（无偏移）
        - 允许 "open+30m"/"close-10s" 等形式
        非法形式返回 None。
        """
        for base in ("open", "close"):
            if expr == base:
                return base, None, None
            if expr.startswith(base):
                if len(expr) == len(base):
                    return base, None, None
                sign = expr[len(base)]
                if sign not in "+-":
                    return None
                offset_str = expr[len(base) + 1 :]
                return base, sign, offset_str
        return None

    # --------- 解析与求解的子步骤（内部辅助，便于维护与测试） ---------

    def _base_time(self, market_periods: Sequence[Tuple[Time, Time]]) -> Time:
        if not market_periods:
            raise ValueError("market_periods 不能为空")
        return market_periods[0][0] if self.base == "open" else market_periods[-1][1]

    def _resolve_explicit(self, target_date: date) -> List[datetime]:
        return [datetime.combine(target_date, self.explicit)]

    def _resolve_relative(self, target_date: date, market_periods: Sequence[Tuple[Time, Time]]) -> List[datetime]:
        base_dt = datetime.combine(target_date, self._base_time(market_periods))
        return [base_dt + self.offset]

    def _resolve_every_minute(self, target_date: date, market_periods: Sequence[Tuple[Time, Time]]) -> List[datetime]:
        result: List[datetime] = []
        for start, end in market_periods:
            current = datetime.combine(target_date, start)
            end_dt = datetime.combine(target_date, end)
            while current < end_dt:
                result.append(current)
                current += timedelta(minutes=1)
        return result

    def _resolve_every_bar(self, target_date: date, market_periods: Sequence[Tuple[Time, Time]]) -> List[datetime]:
        """解析 every_bar，返回交易时段内的所有分钟 bar。

        every_bar 与 every_minute 保持一致，使回测和实盘策略调度语义相同。
        """
        return self._resolve_every_minute(target_date, market_periods)

    @classmethod
    def _parse_offset(cls, value: str) -> timedelta:
        if not value:
            raise ValueError("偏移量不能为空")
        pos = 0
        hours = minutes = seconds = 0
        for match in cls.OFFSET_PATTERN.finditer(value):
            if match.start() != pos:
                raise ValueError(f"无法解析偏移量: {value}")
            qty = int(match.group(1))
            unit = match.group(2)
            if unit == "h":
                hours += qty
            elif unit == "m":
                minutes += qty
            elif unit == "s":
                seconds += qty
            pos = match.end()
        if pos != len(value):
            raise ValueError(f"无法解析偏移量: {value}")
        return timedelta(hours=hours, minutes=minutes, seconds=seconds)

    def resolve(self, trade_day: datetime, market_periods: Sequence[Tuple[Time, Time]]) -> List[datetime]:
        target_date = trade_day.date()
        if self.kind == "explicit":
            return self._resolve_explicit(target_date)
        if self.kind == "relative":
            return self._resolve_relative(target_date, market_periods)
        if self.kind == "every_minute":
            return self._resolve_every_minute(target_date, market_periods)
        if self.kind == "every_bar":
            return self._resolve_every_bar(target_date, market_periods)
        raise ValueError(f"未知的时间表达式类型: {self.kind}")


def is_event_expired(scheduled: datetime, now: datetime, timeout_seconds: int) -> bool:
    """
    判断事件是否超时：当 now - scheduled > timeout_seconds 时视为过期不再执行。
    """
    try:
        return (now - scheduled).total_seconds() > max(0, int(timeout_seconds))
    except Exception:
        return False


def next_minute_after(now: datetime) -> datetime:
    """返回当前时间之后的下一个整分钟（例如 9:40:20 -> 9:41:00）。"""
    return (now.replace(second=0, microsecond=0) + timedelta(minutes=1))


def should_run_task(task: "ScheduleTask", current_dt: datetime, is_bar: bool = False) -> bool:
    """兼容层：判断任务在当前时刻是否触发。
    仅用于保留旧接口的行为；新逻辑建议使用 generate_daily_schedule。
    """
    if not getattr(task, "expression", None):
        return False
    if task.time == "every_bar":
        return is_bar
    explicit = TimeExpression._try_parse_explicit(task.time.lower())
    if explicit is None:
        return False
    ct = current_dt.time()
    return ct.hour == explicit.hour and ct.minute == explicit.minute


class ScheduleType(Enum):
    """调度类型"""
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'


@dataclass
class ScheduleTask:
    """
    调度任务
    
    Attributes:
        func: 要执行的函数
        schedule_type: 调度类型
        time: 执行时间
        weekday: 星期几（0=周一，6=周日），仅weekly使用
        monthday: 每月几号，仅monthly使用
    """
    func: Callable
    schedule_type: ScheduleType
    time: str  # 原始表达式
    weekday: Optional[int] = None
    monthday: Optional[int] = None
    expression: Optional[TimeExpression] = None
    reference_security: Optional[str] = None
    force: bool = True
    last_trigger_marker: Optional[Tuple[int, int]] = field(default=None, repr=False)

    def should_run(self, current_dt: datetime, is_bar: bool = False) -> bool:
        return should_run_task(self, current_dt, is_bar)


# 全局任务列表
_tasks: List[ScheduleTask] = []
_trade_calendar: Dict[date, Dict[str, Any]] = {}


def _effective_aliases() -> Dict[str, str]:
    """内部使用：获取合并后的时间别名。"""
    return get_time_aliases()


def run_daily(
    func: Callable,
    time: str = 'every_bar',
    reference_security: Optional[str] = None,
    force: bool = True,
):
    """
    每日运行
    
    Args:
        func: 要执行的函数
        time: 执行时间
            - 'every_bar': 每个交易分钟 bar 触发，回测与实盘语义一致
            - 'every_minute': 每分钟触发一次，与 every_bar 等价
            - 'HH:MM': 特定时间，如 '09:30', '14:00'
        reference_security: 参考标的（决定交易日/时段，未提供时使用默认）
        force: 是否从回测/策略起始日作为第一个交易日起算（默认 True）
    """
    aliases = _effective_aliases()
    expression = TimeExpression.parse(time, aliases)
    task = ScheduleTask(
        func=func,
        schedule_type=ScheduleType.DAILY,
        time=time,
        expression=expression,
        reference_security=reference_security,
        force=bool(force),
    )
    _tasks.append(task)


def run_weekly(
    func: Callable,
    weekday: int,
    time: str = '09:30',
    reference_security: Optional[str] = None,
    force: bool = True,
):
    """
    每周运行（交易日序号语义）
    
    Args:
        func: 要执行的函数
        weekday: 当周第 N 个交易日（支持负数，-1 为最后一个交易日）
        time: 执行时间，格式 'HH:MM' 或 open/close 偏移
        reference_security: 参考标的（决定交易日/时段，未提供时使用默认）
        force: 是否从回测/策略起始日作为第一个交易日起算（默认 True）
    """
    if not isinstance(weekday, int) or weekday == 0:
        raise ValueError("weekday 必须为非零整数，表示交易日序号（正序/倒序）")
    aliases = _effective_aliases()
    expression = TimeExpression.parse(time, aliases)
    task = ScheduleTask(
        func=func,
        schedule_type=ScheduleType.WEEKLY,
        time=time,
        weekday=weekday,
        expression=expression,
        reference_security=reference_security,
        force=bool(force),
    )
    _tasks.append(task)


def run_monthly(
    func: Callable,
    monthday: int,
    time: str = '09:30',
    reference_security: Optional[str] = None,
    force: bool = True,
):
    """
    每月运行（交易日序号语义）
    
    Args:
        func: 要执行的函数
        monthday: 当月第 N 个交易日（支持负数，-1 为最后一个交易日）
        time: 执行时间，格式 'HH:MM' 或 open/close 偏移
        reference_security: 参考标的（决定交易日/时段，未提供时使用默认）
        force: 是否从回测/策略起始日作为第一个交易日起算（默认 True）
    """
    if not isinstance(monthday, int) or monthday == 0:
        raise ValueError("monthday 必须为非零整数，表示交易日序号（正序/倒序）")
    aliases = _effective_aliases()
    expression = TimeExpression.parse(time, aliases)
    task = ScheduleTask(
        func=func,
        schedule_type=ScheduleType.MONTHLY,
        time=time,
        monthday=monthday,
        expression=expression,
        reference_security=reference_security,
        force=bool(force),
    )
    _tasks.append(task)


def unschedule_all():
    """取消所有定时任务"""
    global _tasks
    _tasks = []


def get_tasks() -> List[ScheduleTask]:
    """获取所有任务"""
    return _tasks


def _normalize_trade_days(trade_days: Sequence[date]) -> List[date]:
    uniq = sorted({d for d in trade_days})
    return uniq


def _finalize_negative_indexes(days: List[date], calendar: Dict[date, Dict[str, Any]], neg_key: str, total_key: str) -> None:
    total = len(days)
    for idx, d in enumerate(days):
        calendar[d][neg_key] = idx - total
        calendar[d][total_key] = total


def _build_trade_calendar(trade_days: Sequence[date], start_date: date) -> Dict[date, Dict[str, Any]]:
    """
    构建交易日序号日历，包含周/月正序与倒序索引，以及 force 起算序号。
    """
    normalized = _normalize_trade_days(trade_days)
    if not normalized:
        return {}

    first, last = normalized[0], normalized[-1]
    all_days = set(normalized)
    calendar: Dict[date, Dict[str, Any]] = {}

    week_days: List[date] = []
    month_days: List[date] = []
    week_force_days: List[date] = []
    month_force_days: List[date] = []
    tweekday = tmonthday = 0
    tweekday_force = tmonthday_force = None
    tweekday_force_stop = False
    tmonthday_force_stop = False

    current = first
    while current <= last:
        # 周一切分并收尾上一周索引
        if current.isoweekday() == 1:
            _finalize_negative_indexes(week_days, calendar, "tweekday_negative", "week_tdays")
            week_days = []
            if not tweekday_force_stop:
                _finalize_negative_indexes(
                    week_force_days, calendar, "tweekday_negative_force", "week_tdays_force"
                )
                week_force_days = []
                if current >= start_date:
                    tweekday_force_stop = True
            tweekday = 0

        # 月初切分并收尾上一月索引
        if current.day == 1:
            _finalize_negative_indexes(month_days, calendar, "tmonthday_negative", "month_tdays")
            month_days = []
            if not tmonthday_force_stop:
                _finalize_negative_indexes(
                    month_force_days, calendar, "tmonthday_negative_force", "month_tdays_force"
                )
                month_force_days = []
                if current >= start_date:
                    tmonthday_force_stop = True
            tmonthday = 0

        if current in all_days:
            week_days.append(current)
            month_days.append(current)
            tweekday += 1
            tmonthday += 1

            calendar[current] = {
                "tweekday": tweekday,
                "tmonthday": tmonthday,
            }

            if not tweekday_force_stop:
                if current >= start_date:
                    tweekday_force = 1 if tweekday_force is None else tweekday_force + 1
                calendar[current]["tweekday_force"] = tweekday_force
                if tweekday_force is not None:
                    week_force_days.append(current)

            if not tmonthday_force_stop:
                if current >= start_date:
                    tmonthday_force = 1 if tmonthday_force is None else tmonthday_force + 1
                calendar[current]["tmonthday_force"] = tmonthday_force
                if tmonthday_force is not None:
                    month_force_days.append(current)

        current = current + timedelta(days=1)

    # 收尾最后一周/一月
    _finalize_negative_indexes(week_days, calendar, "tweekday_negative", "week_tdays")
    _finalize_negative_indexes(month_days, calendar, "tmonthday_negative", "month_tdays")
    if not tweekday_force_stop:
        _finalize_negative_indexes(
            week_force_days, calendar, "tweekday_negative_force", "week_tdays_force"
        )
    if not tmonthday_force_stop:
        _finalize_negative_indexes(
            month_force_days, calendar, "tmonthday_negative_force", "month_tdays_force"
        )

    return calendar


def set_trade_calendar(trade_days: Sequence[date], start_date: date) -> None:
    """
    设置全局交易日序号日历，供调度解析使用。
    """
    global _trade_calendar
    _trade_calendar = _build_trade_calendar(trade_days, start_date)


def get_trade_calendar() -> Dict[date, Dict[str, Any]]:
    """返回已缓存的交易日序号日历。"""
    return _trade_calendar


def _resolve_market_periods_for_security(reference_security: Optional[str]) -> List[Tuple[Time, Time]]:
    """
    根据参考标的获取交易时段。
    当前实现使用全局设置，预留 reference_security 扩展。
    """
    _ = reference_security
    return get_market_periods()


def _pick_force_value(info: Dict[str, Any], normal_key: str, force_key: str, force: bool) -> Any:
    if not force:
        return info.get(normal_key)
    return info.get(force_key) if info.get(force_key) is not None else info.get(normal_key)


def _should_trigger_weekly(info: Dict[str, Any], weekday: int, force: bool) -> bool:
    if weekday == 0:
        return False
    week_total = _pick_force_value(info, "week_tdays", "week_tdays_force", force)
    if not week_total:
        return False
    tweekday = _pick_force_value(info, "tweekday", "tweekday_force", force)
    tweekday_neg = _pick_force_value(info, "tweekday_negative", "tweekday_negative_force", force)
    if tweekday is None or tweekday_neg is None:
        return False
    if weekday > 0:
        if weekday > week_total:
            return force and tweekday == week_total
        return tweekday == weekday
    abs_idx = abs(weekday)
    if abs_idx > week_total:
        return force and abs(tweekday_neg) == week_total
    return tweekday_neg == weekday


def _should_trigger_monthly(info: Dict[str, Any], monthday: int, force: bool) -> bool:
    if monthday == 0:
        return False
    month_total = _pick_force_value(info, "month_tdays", "month_tdays_force", force)
    if not month_total:
        return False
    tmonthday = _pick_force_value(info, "tmonthday", "tmonthday_force", force)
    tmonthday_neg = _pick_force_value(info, "tmonthday_negative", "tmonthday_negative_force", force)
    if tmonthday is None or tmonthday_neg is None:
        return False
    if monthday > 0:
        if monthday > month_total:
            return force and tmonthday == month_total
        return tmonthday == monthday
    abs_idx = abs(monthday)
    if abs_idx > month_total:
        return force and abs(tmonthday_neg) == month_total
    return tmonthday_neg == monthday


def generate_daily_schedule(
    trade_day: datetime,
    trade_calendar: Optional[Dict[date, Dict[str, Any]]] = None,
    market_periods_resolver: Optional[Callable[[Optional[str]], Sequence[Tuple[Time, Time]]]] = None,
    tasks: Optional[Sequence[Any]] = None,
) -> Dict[datetime, List[Any]]:
    """
    生成指定交易日的任务时间表。
    返回 dict，键为 datetime，值为在该时间需要执行的任务列表。
    """
    schedule: Dict[datetime, List[Any]] = defaultdict(list)
    calendar = trade_calendar or _trade_calendar or {}
    resolver = market_periods_resolver or _resolve_market_periods_for_security
    target_date = trade_day.date()
    day_info = calendar.get(target_date)
    if day_info is None and not calendar:
        # 回退：若未提供日历，视为单一交易日且序号为 1
        calendar = _build_trade_calendar([target_date], target_date)
        day_info = calendar.get(target_date)

    task_list = list(tasks) if tasks is not None else _tasks
    for task in task_list:
        if getattr(task, "enabled", True) is False:
            continue
        if not getattr(task, "expression", None):
            continue

        ref_security = getattr(task, "reference_security", None)
        market_periods = resolver(ref_security)
        if not market_periods:
            continue

        stype = getattr(task, "schedule_type", None)
        stype_value = getattr(stype, "value", stype)
        if stype_value == ScheduleType.DAILY.value:
            times = task.expression.resolve(trade_day, market_periods)
        elif stype_value == ScheduleType.WEEKLY.value:
            weekday = getattr(task, "weekday", None)
            if day_info is None or weekday is None or not _should_trigger_weekly(day_info, weekday, getattr(task, "force", True)):
                continue
            times = task.expression.resolve(trade_day, market_periods)
        elif stype_value == ScheduleType.MONTHLY.value:
            monthday = getattr(task, "monthday", None)
            if day_info is None or monthday is None or not _should_trigger_monthly(day_info, monthday, getattr(task, "force", True)):
                continue
            times = task.expression.resolve(trade_day, market_periods)
        else:
            continue

        for dt in times:
            schedule[dt].append(task)

    return {dt: schedule[dt] for dt in sorted(schedule.keys())}


def get_tasks_to_run(current_dt: datetime, is_bar: bool = False) -> List[ScheduleTask]:
    """
    兼容旧接口：根据当前时间返回需要执行的任务。
    新逻辑建议使用 `generate_daily_schedule`。
    """
    schedule = generate_daily_schedule(current_dt)
    tasks = schedule.get(current_dt, [])
    if is_bar and not tasks:
        return [task for task in _tasks if task.time == "every_bar"]
    return tasks


__all__ = [
    'run_daily', 'run_weekly', 'run_monthly', 'unschedule_all',
    'get_tasks', 'get_tasks_to_run', 'ScheduleTask', 'generate_daily_schedule',
    'TimeExpression', 'get_market_periods', 'get_time_aliases',
    'set_trade_calendar', 'get_trade_calendar'
]
