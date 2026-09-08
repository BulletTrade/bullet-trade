"""纯计算扩展的 ETF 折算、自然周月与周期解析回归。

作者：BruceLee
职责：验证合法负送股比例、自然日历分组、窗口和已有算法不变。
输入：本文件构造的完整事件、日线与分钟表，不读取配置或访问数据源。
输出：pytest 断言；上下游为 qmt_adjustment 纯模块及复用周期解析的数据适配层。
环境：pandas、numpy、pytest；不导入策略，不修改输入，不产生网络、交易或文件副作用。
"""

from decimal import Decimal, localcontext

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from bullet_trade.data.qmt_adjustment import (
    AdjustmentError,
    adjust_bars,
    aggregate_bars,
    parse_events,
    parse_frequency,
)

pytestmark = pytest.mark.unit


def _fold_event(**overrides):
    """生成已知 ETF 折算事件；输入字段覆盖，返回映射，不依赖外部状态或修改调用者。"""
    record = {
        "date": "2015-04-15",
        "cash_per_share": "0",
        "gift": "-0.719675",
        "transfer": "0",
        "rights": "0",
        "rights_price": "0",
        "previous_close": "2.243",
        "previous_close_date": "2015-04-10",
        "share_reform": False,
    }
    record.update(overrides)
    return record


def _daily_bars(index=None):
    """构造跨月且交易日不连续的日线；输入可选索引，返回全字段新表，无外部副作用。"""
    if index is None:
        index = pd.DatetimeIndex(
            [
                "2024-01-29",
                "2024-01-30",
                "2024-01-31",
                "2024-02-01",
                "2024-02-02",
                "2024-02-05",
                "2024-02-06",
                "2024-02-08",
                "2024-02-19",
                "2024-02-20",
            ],
            name="time",
        )
    values = np.arange(1, len(index) + 1, dtype=float)
    return pd.DataFrame(
        {
            "open": values,
            "high": values + 0.5,
            "low": values - 0.5,
            "close": values + 0.1,
            "volume": values * 100,
            "money": values * 1000,
            "amount": values * 1000,
            "factor": values / 10,
        },
        index=index,
    )


@pytest.mark.parametrize("fq", ["none", "pre", "post"])
def test_etf_negative_gift_keeps_decimal_algorithm(fq):
    """验证负 gift 三复权方向及量额；输入复权模式，断言现有 C/E 数学结果，无外部副作用。"""
    raw = pd.DataFrame(
        {"close": [2.243, 8.001], "volume": [1000.0, 1000.0], "money": [2243.0, 8001.0]},
        index=pd.DatetimeIndex(["2015-04-10", "2015-04-15"]),
    )
    before = raw.copy(deep=True)
    with localcontext() as context:
        context.prec = 40
        forward = Decimal("8.001") / Decimal("2.243")
        assert parse_events([_fold_event()])[0].multiplier(3) == forward
        expected = {
            "none": [1.0, 1.0],
            "pre": [float(forward), 1.0],
            "post": [1.0, float(Decimal(1) / forward)],
        }[fq]
    result = adjust_bars(
        raw,
        [_fold_event()],
        fq=fq,
        price_decimals=3,
        reference_date="2015-04-15" if fq == "pre" else None,
        post_origin_date="2015-04-10" if fq == "post" else None,
        event_coverage_start="2015-04-10",
        event_coverage_end="2015-04-15",
        events_complete=True,
        include_factor=True,
    )
    np.testing.assert_array_equal(result.factor, expected)
    np.testing.assert_array_equal(result.close, np.round(raw.close.to_numpy() * expected, 3))
    np.testing.assert_array_equal(result.volume, np.round(raw.volume.to_numpy() / expected))
    np.testing.assert_array_equal(result.money, raw.money)
    assert_frame_equal(raw, before)


def test_negative_gift_reverse_reference_and_multiple_events():
    """验证折算后反向重锚与累计不量化；无参数，断言 Decimal40 连乘结果，不访问外部。"""
    records = [
        _fold_event(),
        _fold_event(
            date="2015-04-16",
            previous_close_date="2015-04-15",
            previous_close="8.001",
            cash_per_share="0.031",
            gift="0",
        ),
    ]
    raw = pd.DataFrame({"close": [7.970]}, index=pd.DatetimeIndex(["2015-04-16"]))
    result = adjust_bars(
        raw,
        records,
        fq="pre",
        price_decimals=3,
        reference_date="2015-04-10",
        event_coverage_start="2015-04-10",
        event_coverage_end="2015-04-16",
        events_complete=True,
        include_factor=True,
    )
    with localcontext() as context:
        context.prec = 40
        factor = Decimal(1)
        for event in parse_events(records):
            factor /= event.multiplier(3)
        assert result.factor.iloc[0] == float(factor)
        assert result.factor.iloc[0] != float(factor.quantize(Decimal("0.000001")))
    assert result.close.iloc[0] == 2.243


@pytest.mark.parametrize("gift", ["-1", "-1.01", "NaN", "Infinity", None, True])
def test_invalid_negative_gift_still_fails(gift):
    """拒绝份额耗尽或非法 gift；输入坏值，期待领域异常，不生成非正份额。"""
    with pytest.raises(AdjustmentError):
        parse_events([_fold_event(gift=gift, transfer="2")])


@pytest.mark.parametrize("field", ["cash_per_share", "transfer", "rights", "rights_price"])
def test_other_negative_event_fields_remain_invalid(field):
    """仅 gift 放开合法负值；输入其他字段名，断言负值仍失败，无副作用。"""
    with pytest.raises(AdjustmentError):
        parse_events([_fold_event(**{field: "-0.01"})])


@pytest.mark.parametrize("share_reform", [True, "0", "false", 0, None])
def test_share_reform_not_silently_ignored(share_reform):
    """股改不随折算支持而放行；输入未支持标识，断言明确异常，无副作用。"""
    with pytest.raises(AdjustmentError, match="股改"):
        parse_events([_fold_event(share_reform=share_reform)])


@pytest.mark.parametrize(
    "text,expected",
    [
        ("1w", (1, "w")),
        ("week", (1, "w")),
        ("weekly", (1, "w")),
        ("1M", (1, "mon")),
        ("1mon", (1, "mon")),
        ("month", (1, "mon")),
        ("monthly", (1, "mon")),
        ("1m", (1, "m")),
        ("5min", (5, "m")),
        ("1h", (60, "m")),
        ("2h", (120, "m")),
        ("daily", (1, "d")),
        ("5d", (5, "d")),
        (" minute ", (1, "m")),
    ],
)
def test_public_frequency_contract(text, expected):
    """验证适配器可复用的周期签名；输入别名及目标，断言倍数和单位，无外部副作用。"""
    assert parse_frequency(text) == expected


@pytest.mark.parametrize("text", ["2w", "2mon", "12M", "0M", "0w", "quarterly", "", None, 1])
def test_frequency_does_not_expand_unverified_periods(text):
    """不把未承诺周期静默转义；输入非法或多周多月，期待异常，无副作用。"""
    with pytest.raises(AdjustmentError):
        parse_frequency(text)


@pytest.mark.parametrize("frequency", ["1w", "weekly", "1mon", "monthly", "1M"])
def test_calendar_groups_preserve_all_fields_and_input(frequency):
    """验证自然周期分组和量额守恒；输入周月别名，返回期末索引且不修改源表。"""
    frame = _daily_bars()
    before = frame.copy(deep=True)
    groups = (
        [frame.iloc[:5], frame.iloc[5:8], frame.iloc[8:]]
        if frequency in {"1w", "weekly"}
        else [frame.iloc[:3], frame.iloc[3:]]
    )
    result = aggregate_bars(frame, base_frequency="1d", frequency=frequency)
    assert result.index.tolist() == [group.index[-1] for group in groups]
    assert result.index.name == frame.index.name
    for (_, row), group in zip(result.iterrows(), groups):
        assert row.open == group.open.iloc[0]
        assert row.high == group.high.max()
        assert row.low == group.low.min()
        assert row.close == group.close.iloc[-1]
        assert row.factor == group.factor.iloc[-1]
        for column in ("volume", "money", "amount"):
            assert row[column] == group[column].sum()
    assert_frame_equal(frame, before)


@pytest.mark.parametrize("frequency,start", [("1w", "2024-02-01"), ("1M", "2024-01-31")])
def test_calendar_start_does_not_cut_period_dependencies(frequency, start):
    """start 在聚合后裁剪；输入周期及期中起点，首组仍使用已提供期初日线，无副作用。"""
    result = aggregate_bars(_daily_bars(), base_frequency="1d", frequency=frequency, start=start)
    assert result.open.iloc[0] == 1


@pytest.mark.parametrize(
    "frequency,end,expected_index,expected_open",
    [
        ("1w", "2024-02-06", "2024-02-06", 6),
        ("1M", "2024-02-06", "2024-02-06", 4),
    ],
)
def test_calendar_count_keeps_end_partial_period(frequency, end, expected_index, expected_open):
    """count 取自然组且保留截至 end 尾组；输入预期，断言期初开盘不因 count 截断。"""
    result = aggregate_bars(
        _daily_bars(), base_frequency="1d", frequency=frequency, end=end, count=1
    )
    assert result.index.tolist() == [pd.Timestamp(expected_index)]
    assert result.open.iloc[0] == expected_open
    assert result.close.iloc[0] == pytest.approx(7.1)


def test_week_is_not_five_trading_rows():
    """区分自然周和五行分组；无参数，节间不足五日的周仍独立输出，无副作用。"""
    weekly = aggregate_bars(_daily_bars(), base_frequency="1d", frequency="1w")
    five_days = aggregate_bars(_daily_bars(), base_frequency="1d", frequency="5d")
    assert len(weekly) == 3
    assert len(five_days) == 2
    assert weekly.index[1] == pd.Timestamp("2024-02-08")


@pytest.mark.parametrize("frequency", ["1w", "1M"])
def test_calendar_year_transition_and_china_timezone(frequency):
    """以中国日期划自然期；输入周期，UTC 索引跨年仍按本地日期分组并保留时区。"""
    local = pd.DatetimeIndex(["2023-12-29", "2024-01-02", "2024-01-03"]).tz_localize(
        "Asia/Shanghai"
    )
    index = local.tz_convert("UTC")
    frame = _daily_bars(index)
    result = aggregate_bars(frame, base_frequency="1d", frequency=frequency, end="2024-01-02")
    assert result.index.tolist() == [index[0], index[1]]
    assert str(result.index.tz) == "UTC"


@pytest.mark.parametrize("frequency", ["1w", "1M"])
@pytest.mark.parametrize("kwargs", [{"start": "2025-01-01"}, {"end": "2023-01-01"}])
def test_calendar_empty_window_preserves_schema(frequency, kwargs):
    """空自然周期窗口保留协议；输入周期与空窗口，返回无行新表，不制造行情。"""
    frame = _daily_bars()
    result = aggregate_bars(frame, base_frequency="1d", frequency=frequency, **kwargs)
    assert result.empty
    assert result.columns.tolist() == frame.columns.tolist()
    assert result.index.name == frame.index.name


@pytest.mark.parametrize("base", ["1m", "5d", "1w", "1M"])
@pytest.mark.parametrize("frequency", ["1w", "1M"])
def test_calendar_requires_daily_base(base, frequency):
    """拒绝分钟混为自然日或已聚合数据再聚合；输入基础周期及目标，期待异常，无副作用。"""
    with pytest.raises(AdjustmentError):
        aggregate_bars(_daily_bars(), base_frequency=base, frequency=frequency)


@pytest.mark.parametrize("frequency", ["1w", "1M"])
def test_calendar_aggregates_after_each_bar_adjustment(frequency):
    """跨除权周期先逐日复权；输入周月周期，断言首开盘和最高价避免原价整组误乘。"""
    raw = pd.DataFrame(
        {"open": [10.0, 9.0], "high": [10.0, 9.0], "close": [10.0, 9.0], "volume": [100.0, 100.0]},
        index=pd.DatetimeIndex(["2024-05-09", "2024-05-10"]),
    )
    event = _fold_event(
        date="2024-05-10",
        previous_close_date="2024-05-09",
        previous_close="10",
        cash_per_share="1",
        gift="0",
    )
    adjusted = adjust_bars(
        raw,
        [event],
        fq="pre",
        price_decimals=2,
        reference_date="2024-05-10",
        event_coverage_start="2024-05-09",
        event_coverage_end="2024-05-10",
        events_complete=True,
        include_factor=True,
    )
    result = aggregate_bars(adjusted, base_frequency="1d", frequency=frequency)
    assert result.open.iloc[0] == result.high.iloc[0] == result.close.iloc[0] == 9
    assert result.volume.iloc[0] == 211
    assert result.factor.iloc[0] == 1
