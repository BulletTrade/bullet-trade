"""
作者: BruceLee
文件职责:
    tick 级回放的数据载体：把源 tick 表整理成按时间稳定有序的单日事件流，
    并提供原始 float 时间戳到 datetime 的无损换算。

主要输入:
    数据层 `get_ticks(df=True)` 返回的单日 tick 表，列为
    time/current/high/low/volume/money/position/a1_p/a1_v/b1_p/b1_v。

主要输出:
    TickSnapshot 属性对象与 TickDayStream 单日事件流，供回测引擎按时刻推进回放时钟。

上下游关系:
    上游是 `bullet_trade.data.api.get_ticks` 与数据源缓存；
    下游是 `bullet_trade.core.engine` 的合并事件循环与 `core.api.get_current_tick` 回放缓冲。

关键环境或配置约定:
    - `time` 是 float64 的 YYYYMMDDHHMMSS.小数秒，整数部分已占 14 位有效数字，
      小数秒被 float64 量化到 1/256 秒（约 3.906ms），毫秒级真值不可完全恢复。
    - 排序键必须是 (原始 time 浮点值, 源内序号)：先转 datetime 再排序会丢序并制造同刻并列。
    - 缓存与回放都不得对 `time` 做字符串化或四舍五入。
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

TICK_COLUMNS: tuple = (
    "time",
    "current",
    "high",
    "low",
    "volume",
    "money",
    "position",
    "a1_p",
    "a1_v",
    "b1_p",
    "b1_v",
)

_REQUIRED_TICK_COLUMNS: tuple = ("time", "current")


class TickDataMissingError(RuntimeError):
    """已订阅标的在某个交易日缺 tick 数据时抛出。"""


@dataclass(frozen=True)
class TickSnapshot:
    """一笔 tick 快照。

    Attributes:
        code: 合约或标的代码。
        datetime: 由原始 float 时间戳换算出的回放时刻（微秒精度）。
        time: 源始 float64 时间戳，作为唯一权威排序键原样保留。
        current: 最新价。
        high: 当日累计最高价。
        low: 当日累计最低价。
        volume: 当日累计成交量。
        money: 当日累计成交额。
        position: 持仓量。
        a1_p: 卖一价。
        a1_v: 卖一量。
        b1_p: 买一价。
        b1_v: 买一量。
    """

    code: str
    datetime: datetime
    time: float
    current: float
    high: float
    low: float
    volume: float
    money: float
    position: float
    a1_p: float
    a1_v: float
    b1_p: float
    b1_v: float

    @property
    def last_price(self) -> float:
        """返回最新价别名，兼容按 last_price 读取的调用方。

        Returns:
            float: 与 current 相同的最新价。
        """

        return self.current

    def as_dict(self) -> Dict[str, Any]:
        """返回快照的字典视图。

        Returns:
            Dict[str, Any]: 含代码、时刻与全部行情字段。
        """

        payload: Dict[str, Any] = {"code": self.code, "datetime": self.datetime}
        for column in TICK_COLUMNS:
            payload[column] = getattr(self, column)
        payload["last_price"] = self.current
        return payload


def tick_time_to_datetime(raw: float) -> datetime:
    """把 float64 时间戳换算为 datetime。

    Args:
        raw: 形如 20210608085900.02 的源始时间戳。

    Returns:
        datetime: 微秒精度的时刻；小数秒按四舍五入落到微秒。

    Raises:
        ValueError: 时间戳不是正数或字段越界。
    """

    value = float(raw)
    if not np.isfinite(value) or value <= 0:
        raise ValueError(f"非法 tick 时间戳: {raw!r}")
    seconds = int(value)
    ymd, hms = divmod(seconds, 1_000_000)
    year, mmdd = divmod(ymd, 10_000)
    month, day = divmod(mmdd, 100)
    hour, ms = divmod(hms, 10_000)
    minute, second = divmod(ms, 100)
    micro = int(np.rint((value - seconds) * 1_000_000))
    try:
        return datetime(year, month, day, hour, minute, second, micro)
    except ValueError as exc:
        raise ValueError(f"非法 tick 时间戳: {raw!r}") from exc


def tick_times_to_datetimes(raw_times: Sequence[float]) -> List[datetime]:
    """批量换算 float64 时间戳，避免逐条 strptime。

    Args:
        raw_times: 原始时间戳序列。

    Returns:
        List[datetime]: 与输入等长的时刻列表。
    """

    values = np.asarray(raw_times, dtype="float64")
    if values.size == 0:
        return []
    seconds = values.astype("int64")
    ymd = seconds // 1_000_000
    hms = seconds % 1_000_000
    micros = np.rint((values - seconds.astype("float64")) * 1_000_000).astype("int64")
    base = pd.to_datetime(ymd.astype(str), format="%Y%m%d")
    stamps = (
        base
        + pd.to_timedelta(hms // 10_000, unit="h")
        + pd.to_timedelta((hms // 100) % 100, unit="m")
        + pd.to_timedelta(hms % 100, unit="s")
        + pd.to_timedelta(micros, unit="us")
    )
    return list(stamps.to_pydatetime())


class TickDayStream:
    """单个标的单交易日的 tick 事件流。

    构造时即按 (原始 time 浮点值, 源内序号) 稳定排序，之后只向前消费，
    因此并列时间戳保持源内先后，不会被重排。

    Attributes:
        code: 标的代码。
        day: 所属自然日。
    """

    def __init__(self, code: str, day: Date, frame: pd.DataFrame) -> None:
        """用单日 tick 表构造事件流。

        Args:
            code: 标的代码。
            day: 所属自然日。
            frame: 含 TICK_COLUMNS 的 tick 表，索引即源内序号。

        Raises:
            ValueError: 缺少 time 或 current 列。
        """

        if frame is None or not isinstance(frame, pd.DataFrame):
            raise ValueError(f"{code} tick 数据不是 DataFrame")
        missing = [column for column in _REQUIRED_TICK_COLUMNS if column not in frame.columns]
        if missing:
            raise ValueError(f"{code} tick 数据缺少列: {missing}")

        self.code = code
        self.day = day
        times = frame["time"].to_numpy(dtype="float64")
        # 稳定排序：并列的原始时间戳保持源内序号顺序
        order = np.argsort(times, kind="stable")
        self._times: np.ndarray = times[order]
        records = frame.to_dict(orient="records")
        self._records: List[Mapping[str, Any]] = [records[index] for index in order.tolist()]
        self._datetimes: List[datetime] = tick_times_to_datetimes(self._times)
        self._cursor = 0

    @classmethod
    def from_records(
        cls, code: str, day: Date, records: Iterable[Mapping[str, Any]]
    ) -> "TickDayStream":
        """用记录序列构造事件流，便于测试与离线数据源。

        Args:
            code: 标的代码。
            day: 所属自然日。
            records: 每条含 time/current 等字段的映射。

        Returns:
            TickDayStream: 已排序的事件流。
        """

        return cls(code, day, pd.DataFrame(list(records)))

    def __len__(self) -> int:
        """返回当日 tick 总条数。

        Returns:
            int: 排序后的记录数。
        """

        return len(self._times)

    @property
    def exhausted(self) -> bool:
        """返回是否已消费完当日全部 tick。

        Returns:
            bool: 游标到达末尾时为 True。
        """

        return self._cursor >= len(self._times)

    @property
    def consumed(self) -> int:
        """返回已消费的 tick 条数。

        Returns:
            int: 当前游标位置。
        """

        return self._cursor

    @property
    def next_time(self) -> Optional[float]:
        """返回下一条 tick 的原始时间戳。

        Returns:
            Optional[float]: 已消费完时为 None。
        """

        if self.exhausted:
            return None
        return float(self._times[self._cursor])

    @property
    def next_dt(self) -> Optional[datetime]:
        """返回下一条 tick 的回放时刻。

        Returns:
            Optional[datetime]: 已消费完时为 None。
        """

        if self.exhausted:
            return None
        return self._datetimes[self._cursor]

    def peek(self) -> Optional[TickSnapshot]:
        """查看下一条 tick 但不消费。

        Returns:
            Optional[TickSnapshot]: 已消费完时为 None。
        """

        if self.exhausted:
            return None
        return self._snapshot_at(self._cursor)

    def advance(self) -> Optional[TickSnapshot]:
        """消费并返回下一条 tick。

        Returns:
            Optional[TickSnapshot]: 已消费完时为 None。
        """

        if self.exhausted:
            return None
        snapshot = self._snapshot_at(self._cursor)
        self._cursor += 1
        return snapshot

    def skip_before(self, cutoff: float) -> int:
        """丢弃原始时间戳早于 cutoff 的 tick。

        Args:
            cutoff: 原始 float 时间戳下界（不含）。

        Returns:
            int: 被跳过的条数。
        """

        skipped = 0
        while not self.exhausted and self._times[self._cursor] < cutoff:
            self._cursor += 1
            skipped += 1
        return skipped

    def skip_until_dt(self, cutoff: datetime) -> int:
        """丢弃回放时刻不晚于 cutoff 的 tick。

        盘中新增订阅时用：datetime 由原始 float 单调导出，
        因此可以直接在已排序的时刻列表上做二分定位。

        Args:
            cutoff: 回放时刻下界（含）。

        Returns:
            int: 被跳过的条数。
        """

        index = bisect_right(self._datetimes, cutoff, self._cursor)
        skipped = index - self._cursor
        self._cursor = index
        return skipped

    def _snapshot_at(self, index: int) -> TickSnapshot:
        """按游标位置构造快照。

        Args:
            index: 排序后的下标。

        Returns:
            TickSnapshot: 该条 tick 的属性对象。
        """

        record = self._records[index]
        values: Dict[str, float] = {}
        for column in TICK_COLUMNS:
            if column == "time":
                continue
            values[column] = _to_float(record.get(column))
        return TickSnapshot(
            code=self.code,
            datetime=self._datetimes[index],
            time=float(self._times[index]),
            **values,
        )


def _to_float(value: Any) -> float:
    """把源字段安全转为 float。

    Args:
        value: 原始字段值，可能是 NaN 或缺失。

    Returns:
        float: 转换结果；不可转换时为 0.0。
    """

    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if np.isnan(result) else result


def day_tick_window(day: Date) -> tuple:
    """返回单个自然日的 tick 查询窗口。

    Args:
        day: 自然日。

    Returns:
        tuple: (start_dt, end_dt) 两个 datetime，跨度不超过数据源的 24 小时上限。
    """

    start = datetime.combine(day, datetime.min.time())
    return start, start + timedelta(hours=23, minutes=59, seconds=59)


def _session_tick_key(code: str, day: Date) -> tuple:
    """返回单日 tick 在回测数据会话中的块缓存键。

    Args:
        code: 标的代码。
        day: 自然日。

    Returns:
        tuple: (命名空间, 代码, 日期) 三元组。
    """

    return ("ticks", code, day.isoformat())


def _read_session_tick_block(code: str, day: Date) -> Optional[pd.DataFrame]:
    """从回测数据会话读取已预取的单日 tick 表。

    Args:
        code: 标的代码。
        day: 自然日。

    Returns:
        Optional[pd.DataFrame]: 命中时返回缓存表；会话未启用或未命中时为 None。
    """

    try:
        from .backtest_session import get_current_backtest_data_session

        session = get_current_backtest_data_session()
        if session is None:
            return None
        value = session.get_price_block(_session_tick_key(code, day))
    except Exception:
        return None
    return value if isinstance(value, pd.DataFrame) else None


def _write_session_tick_block(code: str, day: Date, frame: pd.DataFrame) -> None:
    """把单日 tick 表写入回测数据会话的块缓存。

    Args:
        code: 标的代码。
        day: 自然日。
        frame: 单日 tick 表。

    Returns:
        None: 会话未启用、超出内存预算或写入异常时静默跳过。
    """

    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return
    try:
        from .backtest_session import get_current_backtest_data_session

        session = get_current_backtest_data_session()
        if session is None:
            return
        session.set_price_block(_session_tick_key(code, day), frame, rows=len(frame))
    except Exception:
        return


def load_tick_day(
    code: str,
    day: Date,
    loader: Optional[Callable[..., Any]] = None,
    required: bool = True,
) -> Optional[TickDayStream]:
    """拉取单个标的单交易日的 tick 并整理成事件流。

    Args:
        code: 标的代码。
        day: 自然日。
        loader: 取数函数，缺省用数据层 get_ticks；签名需接受
            security/start_dt/end_dt/df 关键字。
        required: True 时无数据即抛错，False 时返回 None。

    Returns:
        Optional[TickDayStream]: 事件流；required=False 且无数据时为 None。

    Raises:
        TickDataMissingError: required=True 且当日取不到任何 tick。
    """

    frame = _read_session_tick_block(code, day)
    if frame is None:
        start_dt, end_dt = day_tick_window(day)
        if loader is None:
            from .api import get_ticks as _get_ticks

            loader = _get_ticks
        frame = loader(security=code, start_dt=start_dt, end_dt=end_dt, df=True)
        if isinstance(frame, pd.DataFrame):
            _write_session_tick_block(code, day, frame)
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        if required:
            raise TickDataMissingError(f"{code} 在 {day.isoformat()} 缺少 tick 数据")
        return None
    return TickDayStream(code, day, frame)
