"""候选 QMT 复权纯算法的参数、边界和多周期回归。

作者：BruceLee
职责：验证显式事件与固定基准、价格量额舍入、基础 bar 聚合和失败语义。
输入：本文件独立构造的合成行情及公司行为，不使用聚宽/QMT 网络或生产配置。
输出：pytest 断言结果；上下游为 qmt_adjustment 模块和上层行情诊断。
环境：pandas、numpy、pytest；测试无网络、文件写入、信号或订单副作用。
"""

from datetime import date, datetime
from decimal import Decimal
from math import ceil

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from bullet_trade.data.qmt_adjustment import (
    AdjustmentError,
    adjust_bars,
    aggregate_bars,
    parse_events,
)

pytestmark = pytest.mark.unit


def _event(**overrides):
    """构造现金事件；输入覆盖字段，返回全字段映射，无外部依赖或副作用。"""
    values = {
        "date": "2024-05-10",
        "cash_per_share": "1",
        "gift": "0",
        "transfer": "0",
        "rights": "0",
        "rights_price": "0",
        "previous_close": "10",
        "previous_close_date": "2024-05-09",
    }
    values.update(overrides)
    return values


def _bars(index=None):
    """生成不跨字段语义的原始表；输入可选时间索引，返回 OHLC 量额表，无副作用。"""
    if index is None:
        index = pd.DatetimeIndex(["2024-05-09", "2024-05-10", "2024-05-13"])
    prices = np.where(index.date < date(2024, 5, 10), 10.0, 9.0)
    return pd.DataFrame(
        {
            "open": prices,
            "high": prices + 0.2,
            "low": prices - 0.2,
            "close": prices,
            "volume": np.full(len(index), 100.0),
            "money": np.arange(len(index), dtype=float) + 1000.0,
        },
        index=index,
    )


def _adjust(raw=None, events=None, **overrides):
    """调用固定前复权契约；输入可选行情、事件及参数，返回复权表，非法参数仍向外抛错。"""
    arguments = {
        "fq": "pre",
        "price_decimals": 2,
        "reference_date": "2024-05-13",
        "event_coverage_start": "2024-05-01",
        "event_coverage_end": "2024-05-31",
        "events_complete": True,
        "include_factor": True,
    }
    arguments.update(overrides)
    return adjust_bars(
        _bars() if raw is None else raw,
        [_event()] if events is None else events,
        **arguments,
    )


@pytest.mark.parametrize("fq", ["none", "pre", "post"])
@pytest.mark.parametrize("decimals", [2, 3], ids=["stock", "etf"])
@pytest.mark.parametrize("frequency", ["1m", "5m", "15m", "30m", "60m", "1d"])
def test_adjustment_frequency_matrix(fq, decimals, frequency):
    """验证三复权、两报价精度、六周期；输入参数组合，断言字段与分组数学契约，无外部副作用。"""
    if frequency == "1d":
        raw = _bars()
        base, group = "1d", 1
    else:
        index = pd.date_range("2024-05-09 09:31", periods=61, freq="min").append(
            pd.date_range("2024-05-10 09:31", periods=60, freq="min")
        )
        raw = _bars(index)
        base, group = "1m", int(frequency[:-1])
    before = raw.copy(deep=True)
    kwargs = {"reference_date": "2024-05-13" if fq == "pre" else None}
    if fq == "post":
        kwargs["post_origin_date"] = "2024-05-09"
    adjusted = _adjust(raw, fq=fq, price_decimals=decimals, **kwargs)
    ratios = np.ones(len(raw))
    if fq == "pre":
        ratios[raw.index.date < date(2024, 5, 10)] = 0.9
    elif fq == "post":
        ratios[raw.index.date >= date(2024, 5, 10)] = 1 / 0.9
    expected_close = np.round(raw.close.to_numpy() * ratios, decimals)
    np.testing.assert_array_equal(adjusted.close, expected_close)
    np.testing.assert_array_equal(adjusted.volume, np.round(raw.volume.to_numpy() / ratios))
    np.testing.assert_array_equal(adjusted.money, raw.money)
    result = aggregate_bars(adjusted, base_frequency=base, frequency=frequency)
    assert len(result) == ceil(len(raw) / group)
    assert result.iloc[0].open == adjusted.iloc[0].open
    assert result.iloc[-1].close == adjusted.iloc[-1].close
    assert result.volume.sum() == adjusted.volume.sum()
    assert result.money.sum() == raw.money.sum()
    assert result.index[-1] == raw.index[-1]
    assert_frame_equal(raw, before)


@pytest.mark.parametrize(
    "field", ["cash_per_share", "gift", "transfer", "rights", "rights_price", "previous_close"]
)
@pytest.mark.parametrize("bad", [-1, np.nan, np.inf, None, True])
def test_event_rejects_invalid_numbers(field, bad):
    """验证事件各数值字段拒绝非有限和缺失；输入字段与坏值，期待明确异常，无副作用。"""
    with pytest.raises(AdjustmentError):
        parse_events([_event(**{field: bad})])


@pytest.mark.parametrize(
    "field",
    [
        "date",
        "cash_per_share",
        "gift",
        "transfer",
        "rights",
        "rights_price",
        "previous_close",
        "previous_close_date",
    ],
)
def test_event_rejects_missing_field(field):
    """保证缺字段不能默认零；输入待删字段，期待事件解析异常，无外部副作用。"""
    record = _event()
    del record[field]
    with pytest.raises(AdjustmentError, match="缺少字段"):
        parse_events([record])


@pytest.mark.parametrize("fq", ["", "follow", "front_ratio", "PRE", 0, False])
def test_adjustment_rejects_ambiguous_mode(fq):
    """拒绝界面跟随和隐式模式；输入坏 fq，期待明确异常，不使用任何原生兜底。"""
    with pytest.raises(AdjustmentError, match="fq"):
        _adjust(fq=fq)


@pytest.mark.parametrize("fq", [None, "none"])
def test_none_preserves_raw_without_event_dependency(fq):
    """验证不复权不依赖事件；输入 None/none，返回原始字段精度和新表，不修改原输入。"""
    raw = _bars()
    raw.loc[raw.index[0], "close"] = 10.123456
    result = adjust_bars(raw, None, fq=fq, price_decimals=2)
    assert_frame_equal(result, raw)
    assert result is not raw


@pytest.mark.parametrize("column", ["preClose", "pre_close", "factor", "last_price", "paused"])
def test_adjustment_rejects_unverified_fields(column):
    """防止字段语义误用；输入未支持列，期待错误，不把昨收或上游 factor 当普通 OHLC。"""
    raw = _bars()
    raw[column] = 1.0
    with pytest.raises(AdjustmentError, match="不支持字段"):
        _adjust(raw)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"events_complete": False},
        {"events_complete": None},
        {"event_coverage_start": None},
        {"event_coverage_end": None},
        {"event_coverage_start": "2024-05-10"},
        {"event_coverage_end": "2024-05-10"},
        {"reference_date": None},
        {"post_origin_date": "2024-05-01"},
    ],
)
def test_adjustment_requires_explicit_coverage_and_anchor(kwargs):
    """检查覆盖与固定基准；输入缺失或冲突参数，期待异常，无原价兜底和副作用。"""
    with pytest.raises(AdjustmentError):
        _adjust(**kwargs)


@pytest.mark.parametrize(
    "decimals,cash,expected",
    [
        (2, "0.362", "11.49"),
        (2, "0.365", "11.49"),
        (2, "0.366", "11.48"),
        (3, "0.362", "11.488"),
        (3, "0.3625", "11.488"),
        (3, "0.3626", "11.487"),
    ],
)
def test_reference_price_half_up_before_ratio(decimals, cash, expected):
    """验证参考价先十进制四舍五入；输入精度/现金，返回因子还原到预期参考价，无副作用。"""
    event = parse_events([_event(previous_close="11.85", cash_per_share=cash)])[0]
    restored = event.multiplier(decimals) * Decimal("11.85")
    assert float(restored) == float(expected)


@pytest.mark.parametrize("bad", [-1, 9, 2.5, True, None])
def test_price_decimals_rejects_invalid_values(bad):
    """验证报价精度不是隐式默认值；输入非法位数，期待异常，无外部副作用。"""
    with pytest.raises(AdjustmentError, match="price_decimals"):
        _adjust(price_decimals=bad)


@pytest.mark.parametrize(
    "event",
    [
        _event(previous_close=0),
        _event(cash_per_share=10),
        _event(cash_per_share=11),
        _event(previous_close_date="2024-05-10"),
        _event(previous_close_date="2024-05-13"),
        _event(share_reform="0"),
        _event(rights_price=2),
    ],
)
def test_invalid_event_economics_fails(event):
    """验证非法参考价和事件字段；输入事件映射，期待解析或计算异常，无静默降级。"""
    with pytest.raises(AdjustmentError):
        _adjust(events=[event])


@pytest.mark.parametrize(
    "bad", ["2024-02-30", "20240510", "2024-05-10 09:31:00", None, datetime(2024, 5, 10, 9)]
)
def test_event_dates_are_explicit_dates(bad):
    """阻止不明确除权日期；输入非法日期或盘中时间，期待异常，无副作用。"""
    with pytest.raises(AdjustmentError):
        parse_events([_event(date=bad)])


def test_cash_gift_transfer_rights_combination():
    """验证送转配股与现金联合公式；无参数，断言完整事件参考价，不读取原生 dr。"""
    event = parse_events(
        [_event(gift="0.1", transfer="0.2", rights="0.3", rights_price="4", dr="invalid")]
    )[0]
    assert float(event.multiplier(2)) == pytest.approx(0.638)


def test_multiple_events_apply_in_date_order():
    """验证多事件累计且排序不影响；无参数，返回前复权因子乘积，无外部副作用。"""
    second = _event(
        date="2024-05-13",
        previous_close_date="2024-05-10",
        previous_close="9",
        cash_per_share="0.9",
    )
    result = _adjust(events=[second, _event()])
    np.testing.assert_allclose(result.factor, [0.81, 0.9, 1.0])


def test_event_day_is_not_adjusted_by_its_own_event():
    """验证除权日边界左开右闭；无参数，除权前调整而当日不重复扣减，无副作用。"""
    result = _adjust()
    np.testing.assert_allclose(result.factor, [0.9, 1, 1])


def test_reference_before_returned_bars_reanchors_forward():
    """验证基准日早于返回窗口；无参数，未来 bar 逆因子重锚，不遗漏跨窗口事件。"""
    result = _adjust(_bars().iloc[1:], reference_date="2024-05-09")
    np.testing.assert_allclose(result.factor, [1 / 0.9, 1 / 0.9])
    np.testing.assert_allclose(result.close, [10, 10])


def test_reference_after_returned_bars_includes_outside_event():
    """验证基准日晚于返回窗口；无参数，窗口外除权仍作用到历史 bar，无副作用。"""
    result = _adjust(_bars().iloc[:1])
    assert result.iloc[0].close == 9


def test_unrelated_events_do_not_require_previous_closes():
    """只校验实际影响区间的事实；无参数，无关旧/未来事件不需要收盘数据，无副作用。"""
    result = _adjust(events=[{"date": "2000-01-01"}, _event(), {"date": "2030-01-01"}])
    np.testing.assert_allclose(result.factor, [0.9, 1, 1])


def test_post_fixed_origin_preserves_overlapping_prices():
    """验证后复权重叠窗口稳定；无参数，固定原点不随查询首行改变，不宣称聚宽绝对值对齐。"""
    kwargs = {"fq": "post", "reference_date": None, "post_origin_date": "2024-05-01"}
    full = _adjust(**kwargs)
    short = _adjust(_bars().iloc[1:], **kwargs)
    assert_frame_equal(full.iloc[1:], short)
    np.testing.assert_allclose(full.factor, [1, 1 / 0.9, 1 / 0.9])


def test_post_without_origin_cannot_use_window_start():
    """后复权缺原点必须失败；无参数，期待错误，不以首根行情冒充固定基准。"""
    with pytest.raises(AdjustmentError, match="post_origin_date"):
        _adjust(fq="post", reference_date=None)


def test_post_origin_cannot_follow_returned_bars():
    """验证后复权原点不能晚于数据；无参数，期待明确异常，无副作用。"""
    with pytest.raises(AdjustmentError, match="原点"):
        _adjust(fq="post", reference_date=None, post_origin_date="2024-05-10")


def test_output_uses_numpy_round_not_reference_rounding():
    """区分参考价 HALF_UP 和最终 numpy 舍入；无参数，检查价格/量半值，无副作用。"""
    raw = _bars().iloc[:1].copy()
    raw.loc[:, "close"] = 2.5
    raw.loc[:, "volume"] = 4.5
    result = _adjust(raw, events=[], price_decimals=0)
    assert result.iloc[0].close == 2
    assert result.iloc[0].volume == 4


@pytest.mark.parametrize(
    "kind", ["duplicate", "unsorted", "nan", "negative", "text", "not_datetime"]
)
def test_invalid_raw_frame_fails(kind):
    """验证原始时间轴和数值失败；输入破坏种类，期待异常，不通过排序或 inner join 掩盖问题。"""
    raw = _bars()
    if kind == "duplicate":
        raw = pd.concat([raw, raw.iloc[-1:]])
    elif kind == "unsorted":
        raw = raw.iloc[::-1]
    elif kind == "nan":
        raw.loc[raw.index[0], "close"] = np.nan
    elif kind == "negative":
        raw.loc[raw.index[0], "volume"] = -1
    elif kind == "text":
        raw["close"] = "10"
    else:
        raw.index = pd.RangeIndex(len(raw))
    with pytest.raises(AdjustmentError):
        _adjust(raw)


def test_duplicate_events_fail_instead_of_double_adjustment():
    """阻止重复事件因子重复相乘；无参数，期待明确错误，无外部副作用。"""
    with pytest.raises(AdjustmentError, match="重复事件"):
        _adjust(events=[_event(), _event()])


def test_empty_verified_events_are_identity():
    """合法空事件与取数失败分开；无参数，确认完整覆盖后得到单位因子，无副作用。"""
    result = _adjust(events=[])
    np.testing.assert_array_equal(result.factor, 1)
    assert_frame_equal(result.drop(columns="factor"), _bars())


def test_failed_event_query_none_is_not_identity():
    """事件查询 None 不能伪装合法空列表；无参数，期待异常，无原价兜底。"""
    with pytest.raises(AdjustmentError, match="events"):
        adjust_bars(
            _bars(),
            None,
            fq="pre",
            price_decimals=2,
            reference_date="2024-05-13",
            event_coverage_start="2024-05-01",
            event_coverage_end="2024-05-31",
            events_complete=True,
        )


def test_price_only_request_does_not_need_close_column():
    """字段裁剪不能影响事件依赖；无参数，仅 open 也用独立前收盘事实正确复权，无副作用。"""
    result = _adjust(_bars()[["open"]], include_factor=False)
    assert list(result.columns) == ["open"]
    np.testing.assert_allclose(result.open, [9, 9, 9])


def test_aggregate_cross_event_after_individual_adjustment():
    """跨交易日分组必须先逐根复权；无参数，验证高低价与量额，不能给原始整组乘单一因子。"""
    index = pd.DatetimeIndex(
        [
            "2024-05-09 14:59",
            "2024-05-09 15:00",
            "2024-05-10 09:31",
            "2024-05-10 09:32",
            "2024-05-10 09:33",
        ]
    )
    raw = _bars(index)
    adjusted = _adjust(raw)
    result = aggregate_bars(adjusted, base_frequency="1m", frequency="5m")
    assert result.iloc[0].open == 9
    assert result.iloc[0].high == 9.2
    assert result.iloc[0].low == 8.8
    assert result.iloc[0].volume == 522
    assert result.iloc[0].factor == 1
    assert result.index[0] == index[-1]


def test_aggregate_count_selects_reverse_window_before_grouping():
    """count 必须从 end 逆向选择基础窗口；无参数，验证最后两个分组的位置，无副作用。"""
    index = pd.date_range("2024-05-09 09:31", periods=13, freq="min")
    frame = _bars(index)
    frame["close"] = np.arange(13, dtype=float)
    result = aggregate_bars(frame, base_frequency="1m", frequency="5m", count=2)
    assert result.index.tolist() == [index[7], index[12]]
    assert result.close.tolist() == [7, 12]


def test_aggregate_start_keeps_last_incomplete_group():
    """start 正向分组保留末不足组；无参数，验证时间戳和行数，不跨步丢掉行情。"""
    index = pd.date_range("2024-05-09 09:31", periods=13, freq="min")
    result = aggregate_bars(_bars(index), base_frequency="1m", frequency="5m", start=index[1])
    assert result.index.tolist() == [index[5], index[10], index[12]]


def test_aggregate_multi_day_is_row_group_not_calendar_week():
    """多日周期按基础行分组；无参数，断言三个不连续交易日合为一组，不伪装自然周。"""
    result = aggregate_bars(_adjust(), base_frequency="1d", frequency="3d")
    assert len(result) == 1
    assert result.index[0] == pd.Timestamp("2024-05-13")


@pytest.mark.parametrize("frequency", ["2w", "2mon", "0m", "5x", "12M"])
def test_aggregate_rejects_unverified_frequency(frequency):
    """不支持的周期必须显式失败；输入周期字符串，期待错误，不隐式改为日线。"""
    with pytest.raises(AdjustmentError):
        aggregate_bars(_bars(), base_frequency="1d", frequency=frequency)


def test_hour_alias_preserves_intraday_window():
    """小时别名保留时分秒；无参数，断言 1h 与 60m 相同并使用明确结束时间，无副作用。"""
    index = pd.date_range("2024-05-09 09:31", periods=120, freq="min")
    arguments = {"base_frequency": "1m", "end": "2024-05-09 10:30:00", "count": 1}
    result = aggregate_bars(_bars(index), frequency="1h", **arguments)
    expected = aggregate_bars(_bars(index), frequency="60m", **arguments)
    assert_frame_equal(result, expected)
    assert result.index[-1] == pd.Timestamp("2024-05-09 10:30")


def test_timezone_and_date_end_use_china_trading_day():
    """带时区时间按中国交易日识别；无参数，复权因子和纯日期结束窗口均正确，无副作用。"""
    index = (
        pd.DatetimeIndex(["2024-05-09 15:00", "2024-05-10 09:31"])
        .tz_localize("Asia/Shanghai")
        .tz_convert("UTC")
    )
    frame = _adjust(_bars(index))
    np.testing.assert_allclose(frame.factor, [0.9, 1])
    result = aggregate_bars(frame, base_frequency="1m", frequency="5m", end="2024-05-09")
    assert result.index.tolist() == [index[0]]


def test_empty_window_preserves_schema_and_index():
    """空时间窗口保留列和索引类型；无参数，返回空副本，不制造伪行情。"""
    raw = _bars()
    result = aggregate_bars(raw, base_frequency="1d", frequency="5d", start="2025-01-01")
    assert result.empty
    assert list(result.columns) == list(raw.columns)
    assert isinstance(result.index, pd.DatetimeIndex)


@pytest.mark.parametrize(
    "arguments",
    [
        {"start": "2024-05-09", "count": 1},
        {"start": "2024-05-13", "end": "2024-05-09"},
        {"count": 0},
        {"count": -1},
        {"count": True},
        {"count": 1.5},
    ],
)
def test_aggregate_rejects_ambiguous_windows(arguments):
    """验证窗口参数不隐式猜测；输入冲突或非法 count，期待异常，无外部副作用。"""
    with pytest.raises(AdjustmentError):
        aggregate_bars(_bars(), base_frequency="1d", frequency="5d", **arguments)


@pytest.mark.parametrize(
    "base,frequency,index",
    [
        ("1d", "5d", pd.DatetimeIndex(["2024-05-09 09:31"])),
        ("1m", "5m", pd.DatetimeIndex(["2024-05-09 09:31:01"])),
        ("1m", "1d", pd.DatetimeIndex(["2024-05-09 09:31"])),
        ("5m", "60m", pd.DatetimeIndex(["2024-05-09 09:35"])),
    ],
)
def test_aggregate_validates_base_bar_contract(base, frequency, index):
    """基础 bar 粒度不能伪装；输入错误周期或时间索引，期待异常，不暗中合成全天。"""
    with pytest.raises(AdjustmentError):
        aggregate_bars(_bars(index), base_frequency=base, frequency=frequency)


def test_decimal_extreme_raises_adjustment_error():
    """有限但过大的事件数值仍明确失败；无参数，期待领域异常，不生成无限因子或原价。"""
    with pytest.raises(AdjustmentError, match="计算范围"):
        _adjust(events=[_event(previous_close="1e100")])
