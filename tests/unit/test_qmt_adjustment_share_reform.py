"""QMT 已表达股改权益的统一公式与固定基准回归。

作者：BruceLee
职责：验证真实股改事件的理论参考价、前后复权不变量与严格布尔标识。
输入：2026-09-08 已留证的 000001/600000 QMT 事件及最近真实收盘；行情为理论构造。
输出：pytest 断言；上下游仅为 qmt_adjustment 纯模块，不依赖原生 dr 或聚宽因子。
来源：工作区 remaining-three-progress-20260908.md 股改表及已保存的前收盘查询。
环境：pandas、numpy、pytest；无网络、配置、文件写入或交易副作用。
范围：只调整 QMT 表达的现金、同证券送转及配股，不承诺未表达权益的总回报。
"""

from decimal import Decimal, localcontext

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from bullet_trade.data.qmt_adjustment import AdjustmentError, adjust_bars, parse_events

pytestmark = pytest.mark.unit

_CASES = [
    pytest.param(
        {
            "date": "2007-06-20",
            "previous_close_date": "2007-05-31",
            "previous_close": "28.69",
            "cash_per_share": "0.009",
            "gift": "0.1",
            "transfer": "0",
            "rights": "0",
            "rights_price": "0",
            "share_reform": True,
        },
        "26.07",
        id="000001-20070620",
    ),
    pytest.param(
        {
            "date": "2006-05-12",
            "previous_close_date": "2006-03-20",
            "previous_close": "10.86",
            "cash_per_share": "0",
            "gift": "0.3",
            "transfer": "0",
            "rights": "0",
            "rights_price": "0",
            "share_reform": True,
        },
        "8.35",
        id="600000-20060512",
    ),
]


def _theoretical_bars(record, expected_reference):
    """构造理论价 C/E 两根 bar；输入事件和独立预期 E，返回新表，不冒充真实复牌行情。"""
    prices = [float(record["previous_close"]), float(expected_reference)]
    return pd.DataFrame(
        {
            **{field: prices for field in ("open", "high", "low", "close")},
            "volume": [1000.0, 1500.0],
            "money": [12345.0, 23456.0],
        },
        index=pd.DatetimeIndex([record["previous_close_date"], record["date"]]),
    )


def _calculate(raw, record, fq, reference=None):
    """调用显式固定基准；输入理论行情、事件、模式和可选参考日，返回新表，无外部副作用。"""
    return adjust_bars(
        raw,
        [record],
        fq=fq,
        price_decimals=2,
        reference_date=(reference or record["date"]) if fq == "pre" else None,
        post_origin_date=record["previous_close_date"] if fq == "post" else None,
        event_coverage_start=record["previous_close_date"],
        event_coverage_end=record["date"],
        events_complete=True,
        include_factor=True,
    )


@pytest.mark.parametrize("record,expected_reference", _CASES)
def test_real_reform_events_use_declared_cash_and_shares(record, expected_reference):
    """真实股改字段得到独立理论 E；输入事件和预期价，断言布尔保留且原生 dr 不参与计算。"""
    event = parse_events([dict(record, dr="不得使用", qmt_dr="不得使用")])[0]
    assert event.share_reform is True
    with localcontext() as context:
        context.prec = 40
        expected = Decimal(expected_reference) / Decimal(record["previous_close"])
        assert event.multiplier(2) == expected
        ordinary = parse_events([dict(record, share_reform=False)])[0]
        assert ordinary.share_reform is False
        assert ordinary.multiplier(2) == expected


@pytest.mark.parametrize("record,expected_reference", _CASES)
@pytest.mark.parametrize("fq", ["none", "pre", "post"])
def test_real_reform_pre_post_price_volume_and_overlap(record, expected_reference, fq):
    """验证三方式价格量额及窗口不变量；输入事件、理论价和模式，不改原表，不访问数据源。"""
    raw = _theoretical_bars(record, expected_reference)
    before = raw.copy(deep=True)
    with localcontext() as context:
        context.prec = 40
        forward = Decimal(expected_reference) / Decimal(record["previous_close"])
        expected = {
            "none": [1.0, 1.0],
            "pre": [float(forward), 1.0],
            "post": [1.0, float(Decimal(1) / forward)],
        }[fq]
    result = _calculate(raw, record, fq)
    np.testing.assert_array_equal(result.factor, expected)
    for field in ("open", "high", "low", "close"):
        np.testing.assert_array_equal(result[field], np.round(raw[field].to_numpy() * expected, 2))
    np.testing.assert_array_equal(result.volume, np.round(raw.volume.to_numpy() / expected))
    np.testing.assert_array_equal(result.money, raw.money)
    assert_frame_equal(_calculate(raw.iloc[1:], record, fq), result.iloc[1:])
    if fq == "pre":
        assert result.close.tolist() == [float(expected_reference)] * 2
    elif fq == "post":
        assert result.close.tolist() == [float(record["previous_close"])] * 2
    assert_frame_equal(raw, before)


@pytest.mark.parametrize("record,expected_reference", _CASES)
def test_reform_reference_before_event_matches_fixed_post(record, expected_reference):
    """前复权参考日前移等于相同固定原点的后复权；输入真实事件和理论价，逐格断言无副作用。"""
    raw = _theoretical_bars(record, expected_reference)
    reversed_pre = _calculate(raw, record, "pre", reference=record["previous_close_date"])
    assert_frame_equal(reversed_pre, _calculate(raw, record, "post"))


@pytest.mark.parametrize("flag", [0, 1, "0", "1", "true", None, np.nan, np.bool_(True)])
def test_reform_marker_rejects_non_boolean_values(flag):
    """拒绝可被隐式转真的非布尔标识；输入坏值，期待明确领域错误，不降级或修改事实。"""
    record = dict(_CASES[0].values[0], share_reform=flag)
    with pytest.raises(AdjustmentError, match="share_reform.*布尔"):
        parse_events([record])


def test_reform_does_not_skip_transfer_or_rights():
    """股改标识不丢弃已表达的转增或配股；无输入，断言统一公式独立参考价为 8.00。"""
    record = dict(
        _CASES[0].values[0],
        previous_close="10",
        cash_per_share="1",
        gift="0.1",
        transfer="0.2",
        rights="0.2",
        rights_price="15",
    )
    assert parse_events([record])[0].multiplier(2) == Decimal("0.8")
