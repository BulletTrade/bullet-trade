"""大 QMT 事件复权与基础行情聚合的独立纯计算模块。

作者：BruceLee
职责：从显式原始行情、公司行为和覆盖声明构造候选复权结果。
输入：已取回的 OHLC、量额、每股事件事实、前一真实日收盘和固定基准。
输出：不修改输入的 DataFrame；可选 factor 是本次实际乘数，不是聚宽累计因子。
上下游：由数据适配层或离线诊断准备输入，本模块不导入 QMT/JQ 客户端。
环境约定：依赖 pandas/numpy；日期按中国交易日解释；不访问网络、磁盘或交易接口。
此候选算法尚不能据此宣称与聚宽全部真实数据对齐。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext
from typing import Any, Iterable, Mapping, Optional, Tuple, Union

import numpy as np
import pandas as pd

__all__ = [
    "AdjustmentError",
    "AdjustmentEvent",
    "parse_events",
    "parse_frequency",
    "adjust_bars",
    "aggregate_bars",
]

_PRICE_FIELDS = frozenset({"open", "high", "low", "close"})
_RAW_FIELDS = _PRICE_FIELDS | frozenset({"volume", "money", "amount"})
_EVENT_FIELDS = (
    "date",
    "cash_per_share",
    "gift",
    "transfer",
    "rights",
    "rights_price",
    "previous_close",
    "previous_close_date",
)


class AdjustmentError(ValueError):
    """表示纯计算输入、事件覆盖或不支持语义的明确错误，不触发任何兜底。"""


def _as_date(value: Any, name: str) -> date:
    """解析明确交易日期；输入值和字段名，返回 date，非法或含时间时抛错，无副作用。"""
    if isinstance(value, datetime):
        if value.time() != datetime.min.time():
            raise AdjustmentError(f"{name} 必须是日期，不能包含非零时间")
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise AdjustmentError(f"{name} 日期无效: {value}") from exc
    raise AdjustmentError(f"{name} 需要明确 YYYY-MM-DD 日期")


def _decimal(
    value: Any, name: str, *, positive: bool = False, allow_negative: bool = False
) -> Decimal:
    """校验事件数值；输入值、字段名及范围许可，返回 Decimal，非法时抛错，无副作用。"""
    if isinstance(value, (bool, np.bool_)) or value is None:
        raise AdjustmentError(f"{name} 不是有效数值")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise AdjustmentError(f"{name} 不是有效数值") from exc
    if not result.is_finite() or (result < 0 and not allow_negative) or (positive and result <= 0):
        required = "正数" if positive else "数值" if allow_negative else "非负数"
        raise AdjustmentError(f"{name} 必须是有限{required}")
    return result


@dataclass(frozen=True)
class AdjustmentEvent:
    """保存已标准化单次公司行为及前收盘事实，与纯因子函数协作，不持有外部连接。"""

    date: date
    cash_per_share: Decimal
    gift: Decimal
    transfer: Decimal
    rights: Decimal
    rights_price: Decimal
    previous_close: Decimal
    previous_close_date: date
    share_reform: bool = False

    def __post_init__(self) -> None:
        """校验并标准化构造字段；输入为实例字段，返回 None，仅初始化冻结实例，非法抛错。"""
        for name in ("date", "previous_close_date"):
            object.__setattr__(self, name, _as_date(getattr(self, name), name))
        for name in _EVENT_FIELDS[1:-1]:
            object.__setattr__(
                self,
                name,
                _decimal(
                    getattr(self, name),
                    name,
                    positive=name == "previous_close",
                    allow_negative=name == "gift",
                ),
            )
        if self.gift <= -1:
            raise AdjustmentError("gift 必须大于 -1，折算后份额不能为零或负数")
        if self.previous_close_date >= self.date:
            raise AdjustmentError("previous_close_date 必须早于除权日，不能用除权日 preClose")
        if not isinstance(self.share_reform, bool) or self.share_reform:
            raise AdjustmentError("不支持股改事件或不明确的 share_reform 标识")
        if self.rights == 0 and self.rights_price != 0:
            raise AdjustmentError("无配股时 rights_price 必须明确为零")

    def multiplier(self, price_decimals: int) -> Decimal:
        """计算单事件前向乘数；输入报价小数位，返回 Decimal，非正参考价抛错，无副作用。"""
        _validate_decimals(price_decimals)
        try:
            with localcontext() as context:
                context.prec = 40
                numerator = (
                    self.previous_close - self.cash_per_share + self.rights * self.rights_price
                )
                denominator = Decimal(1) + self.gift + self.transfer + self.rights
                if denominator <= 0:
                    raise AdjustmentError(f"{self.date} 折算后总份额必须为正")
                reference = (numerator / denominator).quantize(
                    Decimal(1).scaleb(-price_decimals), rounding=ROUND_HALF_UP
                )
                if reference <= 0:
                    raise AdjustmentError(f"{self.date} 除权参考价必须为正")
                return reference / self.previous_close
        except ArithmeticError as exc:
            raise AdjustmentError(f"{self.date} 事件数值超出支持的十进制计算范围") from exc


def parse_events(
    records: Iterable[Union[Mapping[str, Any], AdjustmentEvent]]
) -> Tuple[AdjustmentEvent, ...]:
    """解析完整事件集合；输入映射或事件序列，返回按日期排序的元组，缺失/重复抛错，无副作用。"""
    if records is None or isinstance(records, (str, bytes, Mapping)):
        raise AdjustmentError("events 必须是明确事件序列，不能以 None 代表查询失败")
    parsed = []
    try:
        iterator = iter(records)
    except TypeError as exc:
        raise AdjustmentError("events 必须可迭代") from exc
    for record in iterator:
        if isinstance(record, AdjustmentEvent):
            parsed.append(record)
            continue
        if not isinstance(record, Mapping):
            raise AdjustmentError("每条事件必须是映射或 AdjustmentEvent")
        missing = [name for name in _EVENT_FIELDS if name not in record]
        if missing:
            raise AdjustmentError(f"事件缺少字段: {', '.join(missing)}")
        parsed.append(
            AdjustmentEvent(
                **{name: record[name] for name in _EVENT_FIELDS},
                share_reform=record.get("share_reform", False),
            )
        )
    parsed.sort(key=lambda event: event.date)
    if len({event.date for event in parsed}) != len(parsed):
        raise AdjustmentError("同一除权日出现重复事件，必须由取数层确认合并事实")
    return tuple(parsed)


def _validate_decimals(value: Any) -> None:
    """校验报价精度；输入整数，返回 None，仅允许 0 至 8 位，否则抛错，无副作用。"""
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)) or not 0 <= value <= 8:
        raise AdjustmentError("price_decimals 必须是 0 至 8 的整数")


def _validate_frame(frame: pd.DataFrame, *, allow_factor: bool = False) -> None:
    """校验纯计算行情；输入表与因子列许可，返回 None，非法字段/索引/数值抛错，不修改输入。"""
    if not isinstance(frame, pd.DataFrame) or not isinstance(frame.index, pd.DatetimeIndex):
        raise AdjustmentError("行情必须是使用 DatetimeIndex 的 DataFrame")
    if frame.columns.has_duplicates or frame.index.has_duplicates or frame.index.hasnans:
        raise AdjustmentError("行情索引/字段不允许重复或缺失")
    if not frame.index.is_monotonic_increasing:
        raise AdjustmentError("行情时间必须严格递增，不能静默重排")
    allowed = _RAW_FIELDS | ({"factor"} if allow_factor else set())
    unsupported = set(frame.columns) - allowed
    if unsupported:
        raise AdjustmentError(f"不支持字段语义: {sorted(unsupported)}；不能将 preClose 当普通价格")
    for column in frame.columns:
        values = frame[column]
        if not pd.api.types.is_numeric_dtype(values.dtype) or pd.api.types.is_bool_dtype(
            values.dtype
        ):
            raise AdjustmentError(f"{column} 必须是数值列")
        if not np.isfinite(values.to_numpy(dtype=float)).all() or (values < 0).any():
            raise AdjustmentError(f"{column} 含非有限或负数")
        if column == "factor" and (values <= 0).any():
            raise AdjustmentError("factor 必须为正")


def _trading_dates(index: pd.DatetimeIndex) -> np.ndarray:
    """取得行情中国交易日；输入日期索引，返回 date 数组，时区转换仅作用于副本，无副作用。"""
    local = index.tz_convert("Asia/Shanghai") if index.tz is not None else index
    return local.date


def adjust_bars(
    raw: pd.DataFrame,
    events: Iterable[Union[Mapping[str, Any], AdjustmentEvent]],
    *,
    fq: Optional[str],
    price_decimals: int,
    reference_date: Any = None,
    post_origin_date: Any = None,
    event_coverage_start: Any = None,
    event_coverage_end: Any = None,
    events_complete: bool = False,
    include_factor: bool = False,
) -> pd.DataFrame:
    """以固定日期锚点复权基础 bar，并按候选契约处理价格和成交量。

    输入 raw 为未经复权的 1m 或 1d 行情，events 为完整每股事件；fq 必须显式
    为 None/none/pre/post。pre 使用 reference_date；post 使用固定 post_origin_date。
    event_coverage_start/end 与 events_complete=True 是调用方对事件覆盖的明确声明，
    本函数不能证明数据源是否漏报事件，也不能证明 previous_close 是最近真实交易日。
    price_decimals 为报价位数；include_factor=True 增加本次实际乘数，非聚宽累计 factor。
    返回新 DataFrame；OHLC 乘数后 numpy.round，volume 除数后整数舍入，money/amount 不变。
    输入与外部状态均不修改；不完整/无效输入抛 AdjustmentError，不转用原生价或原价兜底。
    """
    _validate_frame(raw)
    _validate_decimals(price_decimals)
    if fq is None:
        mode = "none"
    elif isinstance(fq, str) and fq in {"none", "pre", "post"}:
        mode = fq
    else:
        raise AdjustmentError("fq 必须显式为 None、none、pre 或 post；不接受 follow")
    result = raw.copy(deep=True)
    if mode == "none":
        if include_factor:
            result["factor"] = 1.0
        return result
    if events_complete is not True:
        raise AdjustmentError("复权需要显式确认完整事件覆盖，查询失败不能当成无事件")
    coverage_start = _as_date(event_coverage_start, "event_coverage_start")
    coverage_end = _as_date(event_coverage_end, "event_coverage_end")
    if coverage_start > coverage_end:
        raise AdjustmentError("事件覆盖起点晚于终点")
    if mode == "pre":
        if post_origin_date is not None:
            raise AdjustmentError("前复权不能同时指定后复权原点")
        anchor = _as_date(reference_date, "reference_date")
    else:
        if reference_date is not None:
            raise AdjustmentError("后复权不能使用前复权基准日")
        anchor = _as_date(post_origin_date, "post_origin_date")
    dates = _trading_dates(raw.index)
    required_start = min([anchor] + list(dates))
    required_end = max([anchor] + list(dates))
    if mode == "post" and len(dates) and anchor > min(dates):
        raise AdjustmentError("后复权固定原点不能晚于返回行情")
    if coverage_start > required_start or coverage_end < required_end:
        raise AdjustmentError("事件覆盖未包含行情与固定基准日之间的完整区间")
    if events is None or isinstance(events, (str, bytes, Mapping)):
        raise AdjustmentError("events 必须是明确事件序列，不能以 None 代表查询失败")
    relevant_records = []
    try:
        iterator = iter(events)
    except TypeError as exc:
        raise AdjustmentError("events 必须可迭代") from exc
    for record in iterator:
        if isinstance(record, AdjustmentEvent):
            event_date = record.date
        elif isinstance(record, Mapping) and "date" in record:
            event_date = _as_date(record["date"], "date")
        else:
            raise AdjustmentError("事件缺少明确日期，不能判断是否影响本次区间")
        if required_start < event_date <= required_end:
            relevant_records.append(record)
    relevant = parse_events(relevant_records)
    event_factors = [(event.date, event.multiplier(price_decimals)) for event in relevant]
    factors = []
    with localcontext() as context:
        context.prec = 40
        for current in dates:
            factor = Decimal(1)
            for event_date, multiplier in event_factors:
                if current < event_date <= anchor:
                    factor *= multiplier
                elif anchor < event_date <= current:
                    factor /= multiplier
            factors.append(float(factor))
    array = np.asarray(factors, dtype=float)
    if not np.isfinite(array).all() or (array <= 0).any():
        raise AdjustmentError("累计因子超出有效浮点范围")
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        for column in _PRICE_FIELDS.intersection(result.columns):
            result[column] = np.round(result[column].to_numpy(dtype=float) * array, price_decimals)
        if "volume" in result:
            result["volume"] = np.round(result["volume"].to_numpy(dtype=float) / array)
    if include_factor:
        result["factor"] = array
    _validate_frame(result, allow_factor=include_factor)
    return result


def parse_frequency(value: str) -> Tuple[int, str]:
    """解析周期；输入明确别名，返回倍数与 m/d/w/mon，非法或多周多月抛错，无副作用。

    分钟/多日沿用行分组语义；小时换为分钟。自然周/月只接受单周期，1M 表示月，
    不能降为 1m。调用方可复用本函数选择基础行情，不需要导入数据源或复制解析规则。
    """
    if not isinstance(value, str):
        raise AdjustmentError("frequency 必须是明确周期字符串")
    if value.strip() == "1M":
        return 1, "mon"
    if re.fullmatch(r"\d+M", value.strip()):
        raise AdjustmentError("自然月只支持单月周期，不能降为小写分钟周期")
    text = value.lower().strip()
    aliases = {
        "minute": "1m",
        "min": "1m",
        "daily": "1d",
        "day": "1d",
        "week": "1w",
        "weekly": "1w",
        "month": "1mon",
        "monthly": "1mon",
    }
    text = aliases.get(text, text)
    match = re.fullmatch(r"([1-9]\d*)(m|d|h|min|w|mon)", text)
    if match is None:
        raise AdjustmentError("仅支持 Nm/Nd/小时别名和单个自然周/月")
    size, unit = int(match.group(1)), match.group(2)
    if unit in {"w", "mon"} and size != 1:
        raise AdjustmentError("自然周/月只支持单周期，不能按基础行数拼接")
    if unit == "h":
        return size * 60, "m"
    return size, "m" if unit == "min" else unit


_frequency = parse_frequency


def _window_timestamp(value: Any, index: pd.DatetimeIndex, *, end: bool) -> pd.Timestamp:
    """解析窗口边界；输入日期/时间与索引时区，返回同区 Timestamp，纯日期结束覆盖全天，无副作用。"""
    date_only = isinstance(value, date) and not isinstance(value, datetime)
    date_only = date_only or (
        isinstance(value, str) and bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value))
    )
    try:
        stamp = pd.Timestamp(value)
        if pd.isna(stamp):
            raise ValueError("空时间")
        if index.tz is not None:
            stamp = stamp.tz_localize("Asia/Shanghai") if stamp.tzinfo is None else stamp
            stamp = stamp.tz_convert(index.tz)
        elif stamp.tzinfo is not None:
            stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
        if end and date_only:
            stamp += pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
        return stamp
    except (ValueError, TypeError, OverflowError) as exc:
        raise AdjustmentError("行情窗口日期/时间无效") from exc


def _validate_base_index(index: pd.DatetimeIndex, unit: str) -> None:
    """校验基础 bar 的日期粒度；输入时间索引与 m/d，返回 None，不符合声明时抛错，无副作用。"""
    local = index.tz_convert("Asia/Shanghai") if index.tz is not None else index
    if unit == "d":
        if not local.equals(local.normalize()) or pd.Index(local.date).has_duplicates:
            raise AdjustmentError("1d 基础 bar 必须是每交易日唯一的零时日期索引")
    elif ((local.second != 0) | (local.microsecond != 0) | (local.nanosecond != 0)).any():
        raise AdjustmentError("1m 基础 bar 时间不能包含秒或更细粒度")


def aggregate_bars(
    adjusted: pd.DataFrame,
    *,
    base_frequency: str,
    frequency: str,
    start: Any = None,
    end: Any = None,
    count: Optional[int] = None,
) -> pd.DataFrame:
    """按基础行或自然周/月合成行情，不推断自然交易时段或重新复权。

    输入 adjusted 必须已经逐根复权及舍入；base_frequency 仅 1m/1d，frequency 为
    同类 Nm/Nd（小时转分钟）。start 正向分组，count 从 end 取 count*倍数基础行后
    正向分组，末组不足保留；不补缺行、不合并集合竞价、不将 5d 冒充自然周线。
    自然周/月只由 1d 构造，按中国日期的周一至周日或年月分组：先截到 end，
    聚合后才按组末时间裁剪 start/count，start 不截断组内已提供的依赖日线。
    自然尾组保留已形成部分，不声称已经收盘；调用方负责提供自然期起始以来的数据，
    本函数无交易日历，不能证明源日线完整或补齐未提供的首组历史。
    返回新 DataFrame，以组末时间为索引、OHLC/量额按字段聚合、factor 取组末值。
    factor 是组末基础 bar 的乘数而非整组统一乘数。无网络或输入修改；非法契约抛错。
    """
    _validate_frame(adjusted, allow_factor=True)
    base_size, base_unit = parse_frequency(base_frequency)
    size, unit = parse_frequency(frequency)
    calendar_period = unit in {"w", "mon"}
    required_base = "d" if calendar_period else unit
    if base_size != 1 or base_unit not in {"m", "d"} or base_unit != required_base:
        raise AdjustmentError("聚合必须由同类 1m 或 1d 基础 bar 构造")
    _validate_base_index(adjusted.index, base_unit)
    if count is not None:
        if isinstance(count, bool) or not isinstance(count, (int, np.integer)) or count <= 0:
            raise AdjustmentError("count 必须是正整数")
        if start is not None:
            raise AdjustmentError("start 与 count 不能同时指定")
    frame = adjusted.copy(deep=True)
    start_stamp = _window_timestamp(start, frame.index, end=False) if start is not None else None
    end_stamp = _window_timestamp(end, frame.index, end=True) if end is not None else None
    if start_stamp is not None and end_stamp is not None and start_stamp > end_stamp:
        raise AdjustmentError("start 不能晚于 end")
    if start_stamp is not None and not calendar_period:
        frame = frame.loc[frame.index >= start_stamp]
    if end_stamp is not None:
        frame = frame.loc[frame.index <= end_stamp]
    if count is not None and not calendar_period:
        frame = frame.tail(count * size)
    if frame.empty or (size == 1 and not calendar_period):
        return frame
    if calendar_period:
        local = (
            frame.index.tz_convert("Asia/Shanghai") if frame.index.tz is not None else frame.index
        )
        local = local.tz_localize(None) if local.tz is not None else local
        keys = local.to_period("W-SUN" if unit == "w" else "M")
        groups = (group for _, group in frame.groupby(keys, sort=False))
    else:
        groups = (frame.iloc[offset : offset + size] for offset in range(0, len(frame), size))
    rows = []
    indexes = []
    for group in groups:
        row = {}
        for column in frame.columns:
            values = group[column]
            if column == "open":
                row[column] = values.iloc[0]
            elif column == "high":
                row[column] = values.max()
            elif column == "low":
                row[column] = values.min()
            elif column in {"volume", "money", "amount"}:
                row[column] = values.sum()
            else:
                row[column] = values.iloc[-1]
        rows.append(row)
        indexes.append(group.index[-1])
    result = pd.DataFrame(rows, columns=frame.columns, index=pd.DatetimeIndex(indexes))
    result.index.name = frame.index.name
    if calendar_period:
        if start_stamp is not None:
            result = result.loc[result.index >= start_stamp]
        if count is not None:
            result = result.tail(count)
    _validate_frame(result, allow_factor=True)
    return result
