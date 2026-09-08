"""停牌空值的基础聚合及大QMT接口契约回归。

作者：BruceLee
职责：验证已确认停牌的NaN在固定/自然周期中的字段传播，不豁免原始坏数据。
输入：本地手算基础bar及既有严格内存Gateway，不读取聚宽或QMT服务。
输出：价格、量额、因子、时间轴和异常断言，不修改外部状态或参考代码。
上下游：独立纯函数及RemoteQmtProvider -> BigQmtDataAdapter -> 测试Gateway。
环境约定：pytest/pandas/numpy，本文件不启动服务、不交易、不写行情缓存。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bullet_trade.data.qmt_adjustment import AdjustmentError, adjust_bars, aggregate_bars

from test_big_qmt_history_standardization import _FakeGateway, _client, _price_request

pytestmark = pytest.mark.unit

_FIELDS = ["open", "high", "low", "close", "volume", "money"]
_PAUSED = {"none": [], "first": [0], "middle": [1], "last": [2], "all": [0, 1, 2]}


def _sample(frequency, fq, position):
    """生成已复权和停牌标准化的独立样本；输入周期/方式/位置，返回表和乘数，无外部副作用。"""
    minute = frequency.endswith("m")
    index = (
        pd.date_range("2026-09-07 09:31:00", periods=3, freq="min")
        if minute
        else pd.date_range("2026-09-07", periods=3, freq="D")
    )
    factor = 0.8 if fq == "pre" else 1.25 if fq == "post" else 1.0
    frame = pd.DataFrame(
        {
            "open": [10.0, 12.0, 11.0],
            "high": [11.0, 13.0, 12.0],
            "low": [9.0, 11.0, 10.0],
            "close": [10.5, 12.5, 11.5],
            "volume": [100.0, 200.0, 300.0],
            "money": [1000.0, 2000.0, 3000.0],
        },
        index=index,
    )
    for field in ("open", "high", "low", "close"):
        frame[field] = np.round(frame[field] * factor, 2)
    frame["volume"] = np.round(frame["volume"] / factor)
    frame["factor"] = factor
    frame.loc[index[_PAUSED[position]], _FIELDS] = np.nan
    if fq is not None:
        frame.loc[index[_PAUSED[position]], "factor"] = np.nan
    return frame, factor


@pytest.mark.parametrize("frequency", ["3m", "3d", "1w", "1mon"])
@pytest.mark.parametrize("fq", [None, "pre", "post"])
@pytest.mark.parametrize("position", list(_PAUSED))
def test_paused_values_follow_fixed_and_natural_period_contract(frequency, fq, position):
    """逐格核对首中末全停牌及正常组；输入周期/方式/位置，无返回，检查固定与自然规则不同。"""
    frame, factor = _sample(frequency, fq, position)
    original = frame.copy(deep=True)
    base = "1m" if frequency.endswith("m") else "1d"
    result = aggregate_bars(frame, base_frequency=base, frequency=frequency)

    paused = _PAUSED[position]
    high = 12.0 if position == "middle" else 13.0
    low = 9.0
    if position in {"first", "all"} or (frequency in {"1w", "1mon"} and paused):
        high = low = np.nan
    expected = [
        np.nan if 0 in paused else float(np.round(10 * factor, 2)),
        float(np.round(high * factor, 2)),
        float(np.round(low * factor, 2)),
        np.nan if 2 in paused else float(np.round(11.5 * factor, 2)),
        np.nan if paused else float(np.round(np.array([100, 200, 300]) / factor).sum()),
        np.nan if paused else 6000.0,
        np.nan if fq is not None and 2 in paused else factor,
    ]
    assert result.index.tolist() == [frame.index[-1]]
    np.testing.assert_allclose(result.iloc[0].to_numpy(), expected, rtol=0, atol=0, equal_nan=True)
    pd.testing.assert_frame_equal(frame, original)


@pytest.mark.parametrize("frequency", ["3m", "3d"])
@pytest.mark.parametrize("window", ["start", "count"])
def test_nan_grouping_preserves_source_windows_and_partial_tail(frequency, window):
    """空值不改变行数锚定；输入固定周期和窗口方式，无返回，验证首尾字段与末组时间。"""
    minute = frequency.endswith("m")
    index = pd.date_range(
        "2026-09-07 09:31" if minute else "2026-09-07", periods=7, freq="min" if minute else "D"
    )
    frame = pd.DataFrame({field: np.arange(1.0, 8.0) for field in _FIELDS}, index=index)
    frame.loc[index[[0, 2, 6]], :] = np.nan
    options = {"start": index[1], "end": index[5]} if window == "start" else {"count": 1}
    result = aggregate_bars(
        frame, base_frequency="1m" if minute else "1d", frequency=frequency, **options
    )
    expected = [[2, 4, 2, 4, np.nan, np.nan], [5, 6, 5, 6, 11, 11]]
    expected_index = [index[3], index[5]]
    if window == "count":
        expected = [[5, 6, 5, np.nan, np.nan, np.nan]]
        expected_index = [index[6]]
    assert result.index.tolist() == expected_index
    np.testing.assert_allclose(result.to_numpy(), expected, rtol=0, atol=0, equal_nan=True)


@pytest.mark.parametrize("field", ["close", "volume", "money"])
def test_source_nan_remains_invalid_before_adjustment(field):
    """源数据缺值仍报错；输入字段，无返回，不因聚合接纳标准化停牌而放松原价校验。"""
    frame, _ = _sample("3d", None, "none")
    frame = frame.drop(columns="factor")
    frame.loc[frame.index[1], field] = np.nan
    with pytest.raises(AdjustmentError):
        adjust_bars(frame, [], fq=None, price_decimals=2)


@pytest.mark.parametrize("field", ["close", "volume", "money"])
@pytest.mark.parametrize("invalid", [-1.0, np.inf, -np.inf])
@pytest.mark.parametrize("operation", ["adjust", "aggregate"])
def test_nonfinite_or_negative_values_are_not_paused_exceptions(field, invalid, operation):
    """无穷和负数不能借停牌放行；输入字段、坏值和阶段，无返回，两个计算阶段均拒绝。"""
    frame, _ = _sample("3d", None, "none")
    frame = frame.drop(columns="factor")
    frame.loc[frame.index[1], field] = invalid
    with pytest.raises(AdjustmentError):
        if operation == "adjust":
            adjust_bars(frame, [], fq=None, price_decimals=2)
        else:
            aggregate_bars(frame, base_frequency="1d", frequency="3d")


@pytest.mark.parametrize("frequency", ["2d", "1w", "1mon"])
@pytest.mark.parametrize("fq", [None, "pre", "post"])
def test_adapter_masks_known_suspension_before_aggregation(frequency, fq):
    """真实适配器先按停牌事实置空再聚合；输入周期/方式，无返回，防止组尾停牌抹掉有效开盘。"""
    gateway = _FakeGateway()
    security = "000001.XSHE"
    gateway.frames[(security, "1d")].loc["2026-09-03", "suspendFlag"] = 1.0
    provider, _ = _client(gateway)
    result = _price_request(
        provider,
        security,
        frequency=frequency,
        fq=fq,
        start_date="2026-09-02",
        end_date="2026-09-04",
        count=None,
        fill_paused=False,
    )
    assert not result.empty
    assert pd.isna(result["volume"].iloc[0])
    assert pd.isna(result["money"].iloc[0])
    assert result["open"].iloc[0] == (8.0 if fq == "pre" else 10.0)
    if frequency == "2d":
        assert result.index.tolist() == pd.to_datetime(["2026-09-03", "2026-09-04"]).tolist()
        assert pd.isna(result.iloc[0]["close"])
        assert result.iloc[0]["high"] == (8.08 if fq == "pre" else 10.1)
        assert result.iloc[0]["low"] == (7.92 if fq == "pre" else 9.9)
        assert result.iloc[1].notna().all()
    else:
        assert result.index.tolist() == [pd.Timestamp("2026-09-04")]
        assert result.iloc[0][["high", "low"]].isna().all()
        assert result.iloc[0]["close"] == (8.0 if fq in {None, "pre"} else 10.0)


@pytest.mark.parametrize("frequency", ["5m", "15m", "30m", "60m"])
@pytest.mark.parametrize("fq", [None, "pre", "post"])
def test_cross_day_minute_group_retains_traded_prefix_and_paused_suffix(frequency, fq):
    """跨日组保留有效前缀与暂停尾部；输入周期/方式，无返回，验证逐根复权后空值传播。"""
    gateway = _FakeGateway()
    security = "000001.XSHE"
    gateway.frames[(security, "1d")].loc["2026-09-03", "suspendFlag"] = 1.0
    gateway.frames[(security, "1m")].loc["2026-09-03", "suspendFlag"] = 1.0
    provider, _ = _client(gateway)
    result = _price_request(
        provider,
        security,
        frequency=frequency,
        fq=fq,
        end_date="2026-09-03 09:32:00",
        count=1,
        fill_paused=False,
    )
    expected = [8.0, 8.08, 7.92] if fq == "pre" else [10.0, 10.1, 9.9]
    expected += [np.nan, np.nan, np.nan]
    assert result.index.tolist() == [pd.Timestamp("2026-09-03 09:32:00")]
    np.testing.assert_allclose(result.iloc[0].to_numpy(), expected, rtol=0, atol=0, equal_nan=True)
