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
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

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


def _coerce_date(value) -> Optional[date]:
    """将字符串/datetime/date 转换为 date；无法转换则返回 None。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None
    return None


def _get_backtest_start_date() -> Optional[date]:
    """从 settings.options 读取回测 start_date（若存在）。"""
    settings = get_settings()
    return _coerce_date(settings.options.get("backtest_start_date"))


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
        """解析 every_bar：根据回测频率返回时间点列表。
        
        - frequency=minute: 返回当日所有交易分钟（等价于 every_minute）
        - frequency=day 或其他: 返回开盘时刻
        """
        settings = get_settings()
        frequency = settings.options.get('backtest_frequency', 'day')
        
        if frequency == 'minute':
            # 分钟回测：返回所有交易分钟
            return self._resolve_every_minute(target_date, market_periods)
        else:
            # 日回测或其他：返回开盘时刻
            return [datetime.combine(target_date, market_periods[0][0])]

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
    force: bool = False
    expression: Optional[TimeExpression] = None
    last_trigger_marker: Optional[Tuple[int, int]] = field(default=None, repr=False)

    def should_run(self, current_dt: datetime, is_bar: bool = False) -> bool:
        return should_run_task(self, current_dt, is_bar)


# 全局任务列表
_tasks: List[ScheduleTask] = []


def _effective_aliases() -> Dict[str, str]:
    """内部使用：获取合并后的时间别名。"""
    return get_time_aliases()


def run_daily(func: Callable, time: str = 'every_bar'):
    """
    每日运行
    
    Args:
        func: 要执行的函数
        time: 执行时间
            - 'every_bar': 每个bar触发。在分钟回测（frequency=minute）时每分钟触发一次，
                          在日回测（frequency=day）时每天触发一次（开盘时刻）
            - 'every_minute': 每分钟触发一次（不受频率影响）
            - 'HH:MM': 特定时间，如 '09:30', '14:00'
    """
    aliases = _effective_aliases()
    expression = TimeExpression.parse(time, aliases)
    task = ScheduleTask(
        func=func,
        schedule_type=ScheduleType.DAILY,
        time=time,
        expression=expression,
    )
    _tasks.append(task)


def run_weekly(func: Callable, weekday: int, time: str = '09:30', force: bool = False):
    """
    每周运行

    Args:
        func: 要执行的函数
        weekday: 星期几（0=周一, 1=周二, ..., 6=周日）
        time: 执行时间，格式 'HH:MM'
        force: 若回测 start_day 的 weekday 晚于指定 weekday，则在回测启动当周的首个可用交易日补跑一次。
    """
    aliases = _effective_aliases()
    expression = TimeExpression.parse(time, aliases)
    task = ScheduleTask(
        func=func,
        schedule_type=ScheduleType.WEEKLY,
        time=time,
        weekday=weekday,
        force=bool(force),
        expression=expression,
    )
    _tasks.append(task)


def run_monthly(func: Callable, monthday: int, time: str = '09:30', force: bool = False):
    """
    每月运行
    
    Args:
        func: 要执行的函数
        monthday: 每月几号（1-31）
        time: 执行时间，格式 'HH:MM'
    """
    aliases = _effective_aliases()
    expression = TimeExpression.parse(time, aliases)
    task = ScheduleTask(
        func=func,
        schedule_type=ScheduleType.MONTHLY,
        time=time,
        monthday=monthday,
        force=bool(force),
        expression=expression,
    )
    _tasks.append(task)


def unschedule_all():
    """取消所有定时任务"""
    global _tasks
    _tasks = []


def get_tasks() -> List[ScheduleTask]:
    """获取所有任务"""
    return _tasks


def _should_trigger_monthly(
    task: ScheduleTask,
    current_date: date,
    previous_trade_day: Optional[date],
    backtest_start_date: Optional[date],
) -> bool:
    if task.monthday is None:
        return False
    monthday = task.monthday
    if monthday < 1 or monthday > 31:
        return False

    # force 语义：若回测 start_day 已晚于指定 monthday，默认视为本月已错过。
    # 当 force=True 时，在回测启动当月的“最早可用交易日”补跑一次。
    if backtest_start_date is not None:
        if (backtest_start_date.year, backtest_start_date.month) == (current_date.year, current_date.month):
            if backtest_start_date.day > monthday:
                if not getattr(task, "force", False):
                    return False
                if current_date < backtest_start_date:
                    return False
                # 仅在启动当月的第一天（相对回测开始）触发一次：previous_trade_day 还未进入 start_date 之后
                if previous_trade_day is not None and previous_trade_day >= backtest_start_date:
                    return False
                marker = (current_date.year, current_date.month)
                if task.last_trigger_marker == marker:
                    return False
                task.last_trigger_marker = marker
                return True

    if current_date.day < monthday:
        return False
    marker = (current_date.year, current_date.month)
    if task.last_trigger_marker == marker:
        return False
    if previous_trade_day and previous_trade_day.month == current_date.month and previous_trade_day.day >= monthday:
        return False
    task.last_trigger_marker = marker
    return True


def _week_marker(d: date) -> Tuple[int, int]:
    iso = d.isocalendar()
    return int(iso.year), int(iso.week)


def _should_trigger_weekly(
    task: ScheduleTask,
    current_date: date,
    previous_trade_day: Optional[date],
    backtest_start_date: Optional[date],
) -> bool:
    if task.weekday is None:
        return False
    target_weekday = int(task.weekday)
    if target_weekday < 0 or target_weekday > 6:
        return False

    # force 语义：若回测 start_day 的 weekday 晚于指定 weekday，默认视为本周已错过。
    # 当 force=True 时，在回测启动当周的“最早可用交易日”补跑一次。
    if backtest_start_date is not None:
        if _week_marker(backtest_start_date) == _week_marker(current_date):
            if backtest_start_date.weekday() > target_weekday:
                if not getattr(task, 'force', False):
                    return False
                if current_date < backtest_start_date:
                    return False
                # 仅在回测开始当周的第一天（相对回测开始）触发一次
                if previous_trade_day is not None and previous_trade_day >= backtest_start_date:
                    return False
                marker = _week_marker(current_date)
                if task.last_trigger_marker == marker:
                    return False
                task.last_trigger_marker = marker
                return True

    # 默认行为：仅在指定 weekday 触发
    return current_date.weekday() == target_weekday


def generate_daily_schedule(
    trade_day: datetime,
    previous_trade_day: Optional[date],
    market_periods: Sequence[Tuple[Time, Time]],
) -> Dict[datetime, List[ScheduleTask]]:
    """
    生成指定交易日的任务时间表。
    返回 dict，键为 datetime，值为在该时间需要执行的任务列表。
    """
    schedule: Dict[datetime, List[ScheduleTask]] = defaultdict(list)
    backtest_start_date = _get_backtest_start_date()
    for task in _tasks:
        if not task.expression:
            continue

        if task.schedule_type == ScheduleType.DAILY:
            times = task.expression.resolve(trade_day, market_periods)
        elif task.schedule_type == ScheduleType.WEEKLY:
            if not _should_trigger_weekly(task, trade_day.date(), previous_trade_day, backtest_start_date):
                continue
            times = task.expression.resolve(trade_day, market_periods)
        elif task.schedule_type == ScheduleType.MONTHLY:
            if not _should_trigger_monthly(task, trade_day.date(), previous_trade_day, backtest_start_date):
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
    market_periods = get_market_periods()
    schedule = generate_daily_schedule(current_dt, None, market_periods)
    tasks = schedule.get(current_dt, [])
    if is_bar and not tasks:
        return [task for task in _tasks if task.time == "every_bar"]
    return tasks


__all__ = [
    'run_daily', 'run_weekly', 'run_monthly', 'unschedule_all',
    'get_tasks', 'get_tasks_to_run', 'ScheduleTask', 'generate_daily_schedule',
    'TimeExpression', 'get_market_periods', 'get_time_aliases'
]
