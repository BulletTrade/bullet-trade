"""大 QMT 事件复权的脱敏真实日线回归与已知残差诊断。

作者：BruceLee
职责：读取同仓固定行情和现金事件，核对完整时间轴、原始价和候选前复权价。
输入：tests/fixtures/qmt_adjustment_20260908.json，仅含日期、价格和事件事实。
输出：pytest 断言；ETF 残差快照通过仅代表已知差异未变，不代表数值验收通过。
上下游：调用独立 qmt_adjustment 纯计算模块，不导入 QMT 或聚宽客户端。
环境：仅依赖本地 JSON、pandas 和 pytest，不联网、不修改数据或运行状态。
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from bullet_trade.data.qmt_adjustment import adjust_bars, parse_events

_FIXTURE_PATH = Path(__file__).parents[1] / "fixtures" / "qmt_adjustment_20260908.json"
_PRICE_FIELDS = ["open", "high", "low", "close"]


@pytest.fixture(scope="module")
def saved_data():
    """读取本地脱敏金标；无参数，返回字典，仅产生固定文件读取副作用。"""
    return json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))


def _case(saved_data, security):
    """按证券选择固定样本；输入完整金标及证券代码，返回唯一案例字典。"""
    matches = [case for case in saved_data["cases"] if case["security"] == security]
    assert len(matches) == 1
    return matches[0]


def _frame(case, key):
    """还原价格时间轴；输入案例及行集键，返回新 DataFrame，不修改原始字典。"""
    frame = pd.DataFrame(case[key], columns=["date", *_PRICE_FIELDS])
    frame["date"] = pd.to_datetime(frame["date"])
    return frame.set_index("date").astype(float)


def _events(case):
    """解析已核对的纯现金事件；输入案例，返回严格事件对象，不访问外部数据源。"""
    records = []
    for event in case["events"]:
        # 金标六条事件的总股本比例均为一，现金以每股或每份统一计量。
        assert event["scale_factor"] == 1
        records.append(
            {
                "date": event["date"],
                "cash_per_share": event["cash_per_share"],
                "gift": 0,
                "transfer": 0,
                "rights": 0,
                "rights_price": 0,
                "previous_close": event["previous_close"],
                "previous_close_date": event["previous_date"],
            }
        )
    return parse_events(records)


def _adjust_pre(saved_data, case, raw):
    """计算固定参考日前复权；输入金标、案例和原始价，返回新结果，不使用 JQ 因子。"""
    decimals = -Decimal(case["tick_size"]).as_tuple().exponent
    return adjust_bars(
        raw,
        _events(case),
        fq="pre",
        price_decimals=decimals,
        reference_date=saved_data["reference_date"],
        event_coverage_start="2025-01-01",
        event_coverage_end=saved_data["reference_date"],
        events_complete=True,
    )


def _difference_snapshot(actual, expected):
    """生成完整对齐价格差异；输入等索引等列结果，返回逐格差异，不用交集隐藏缺行。"""
    pd.testing.assert_index_equal(actual.index, expected.index)
    pd.testing.assert_index_equal(actual.columns, expected.columns)
    differences = []
    for timestamp in actual.index:
        for field in _PRICE_FIELDS:
            candidate = Decimal(str(actual.at[timestamp, field]))
            golden = Decimal(str(expected.at[timestamp, field]))
            if candidate != golden:
                differences.append(
                    {
                        "date": timestamp.strftime("%Y-%m-%d"),
                        "field": field,
                        "candidate": candidate,
                        "jq": golden,
                        "delta": candidate - golden,
                    }
                )
    return differences


@pytest.mark.unit
@pytest.mark.parametrize("security", ["000001.XSHE", "510500.XSHG"])
def test_saved_data_has_complete_unique_price_indexes(saved_data, security):
    """检查金标时间轴；输入固定数据和证券，断言408日同轴、无缺价或重复，无返回值。"""
    case = _case(saved_data, security)
    raw = _frame(case, "raw_rows")
    expected = _frame(case, "jq_pre_rows")

    assert len(raw) == len(expected) == 408
    assert raw.index.is_unique and expected.index.is_unique
    assert raw.index.is_monotonic_increasing and expected.index.is_monotonic_increasing
    assert raw.index[0] == pd.Timestamp("2025-01-02")
    assert raw.index[-1] == pd.Timestamp("2026-09-07")
    pd.testing.assert_index_equal(raw.index, expected.index)
    assert not raw.isna().any().any()
    assert not expected.isna().any().any()
    assert (raw["low"] <= raw[["open", "close"]].min(axis=1)).all()
    assert (raw["high"] >= raw[["open", "close"]].max(axis=1)).all()


@pytest.mark.unit
@pytest.mark.parametrize("security", ["000001.XSHE", "510500.XSHG"])
def test_saved_cash_events_use_previous_actual_daily_close(saved_data, security):
    """核对事件基价；输入金标和证券，断言采用前一真实交易日收盘而非除权参考价。"""
    case = _case(saved_data, security)
    raw = _frame(case, "raw_rows")

    assert len(case["events"]) == 3
    for event in case["events"]:
        event_date = pd.Timestamp(event["date"])
        previous_date = pd.Timestamp(event["previous_date"])
        assert previous_date == raw.index[raw.index < event_date][-1]
        assert Decimal(str(raw.at[previous_date, "close"])) == Decimal(event["previous_close"])
        assert Decimal(event["previous_close"]) != Decimal(event["qmt_ex_reference"])
        assert Decimal(event["cash_per_share"]) > 0


@pytest.mark.unit
@pytest.mark.parametrize("security", ["000001.XSHE", "510500.XSHG"])
def test_saved_unadjusted_prices_and_inputs_stay_unchanged(saved_data, security):
    """验证原价独立性；输入金标和证券，断言 none 原样返回且前复权不污染输入。"""
    case = _case(saved_data, security)
    raw = _frame(case, "raw_rows")
    original = raw.copy(deep=True)
    decimals = -Decimal(case["tick_size"]).as_tuple().exponent

    result_none = adjust_bars(raw, _events(case), fq="none", price_decimals=decimals)
    _adjust_pre(saved_data, case, raw)

    pd.testing.assert_frame_equal(result_none, original, check_exact=True)
    pd.testing.assert_frame_equal(raw, original, check_exact=True)
    assert result_none is not raw


@pytest.mark.unit
def test_saved_000001_pre_prices_match_all_1632_cells(saved_data):
    """验收股票固定窗口前复权；输入金标，逐格断言408日OHLC完全一致，不外推其他场景。"""
    case = _case(saved_data, "000001.XSHE")
    raw = _frame(case, "raw_rows")
    expected = _frame(case, "jq_pre_rows")

    actual = _adjust_pre(saved_data, case, raw)

    assert expected.size == 1632
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    assert _difference_snapshot(actual, expected) == []


@pytest.mark.unit
def test_saved_510500_pre_residual_snapshot_diagnostic_not_acceptance(saved_data):
    """记录ETF未验收残差；输入金标，断言已知9格差异仍明确存在，通过不代表价格对齐。"""
    case = _case(saved_data, "510500.XSHG")
    raw = _frame(case, "raw_rows")
    expected = _frame(case, "jq_pre_rows")

    actual = _adjust_pre(saved_data, case, raw)
    differences = _difference_snapshot(actual, expected)
    expected_differences = [
        {
            **entry,
            "candidate": Decimal(entry["candidate"]),
            "jq": Decimal(entry["jq"]),
            "delta": Decimal(entry["delta"]),
        }
        for entry in case["diagnostic_expected_differences"]
    ]

    assert differences == expected_differences
    assert expected.size == 1632
    assert len(differences) == 9
    assert expected.size - len(differences) == 1623
    assert sum(entry["field"] == "close" for entry in differences) == 2
    assert max(abs(entry["delta"]) for entry in differences) == Decimal("0.001")
    assert not actual.equals(expected), "已知ETF残差尚未消除，不能宣称数值验收通过"
