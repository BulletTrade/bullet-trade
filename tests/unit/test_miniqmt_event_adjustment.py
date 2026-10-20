"""MiniQMT 事件复权单元测试。

验证 miniQMT / qmt-direct 的 fq="pre"/"post" 不再使用 xtdata front_ratio/back_ratio（QMT 内置 dr
并非按除权前一日真实收盘计算），而是以 get_divid_factors 事件 + 除权前真实交易日收盘为基准，
因子 = (前收 - 每份现金) / 前收，与大QMT适配器、聚宽一致。

样本取 510500 四次分红：2024-05-17 0.087、2025-01-16 0.091、2026-01-16 0.062、2026-07-15 0.149。
其中 2026-07-15 前收 8.413 为真实值：真实 dr = 8.413 / 8.264 ≈ 1.01803，而 QMT dr = 1.01591。
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from bullet_trade.data.providers.miniqmt import MiniQMTProvider
from bullet_trade.data.qmt_adjustment import AdjustmentError

SECURITY_QMT = "510500.SH"
SECURITY_JQ = "510500.XSHG"

# (除权日, 每份现金)
EVENTS = [
    ("2024-05-17", 0.087),
    ("2025-01-16", 0.091),
    ("2026-01-16", 0.062),
    ("2026-07-15", 0.149),
]
# QMT 自带 dr（仅 2026-07-15 为实测值，用于说明旧口径偏差；新实现不得使用）
QMT_DR = {"2026-07-15": 1.01591}

# 未复权日线：(日期, 收盘, 停牌标记)。2025-01-15 构造为停牌日，前收必须追溯到 2025-01-14。
RAW_DAILY = [
    ("2024-05-15", 5.698, 0),
    ("2024-05-16", 5.712, 0),
    ("2024-05-17", 5.640, 0),
    ("2025-01-13", 5.380, 0),
    ("2025-01-14", 5.402, 0),
    ("2025-01-15", 5.500, 1),
    ("2025-01-16", 5.330, 0),
    ("2026-01-15", 7.105, 0),
    ("2026-01-16", 7.050, 0),
    ("2026-07-14", 8.413, 0),
    ("2026-07-15", 8.270, 0),
    ("2026-07-16", 8.300, 0),
]
# 每个事件对应的真实前收（停牌日被跳过）
PREVIOUS_CLOSE = {
    "2024-05-17": 5.712,
    "2025-01-16": 5.402,
    "2026-01-16": 7.105,
    "2026-07-15": 8.413,
}


def _ms(day: str) -> int:
    ts = pd.Timestamp(day).tz_localize("Asia/Shanghai").tz_convert("UTC")
    return int(ts.value // 10**6)


class FakeXtData:
    """按 start/end 过滤的 xtdata 替身；只提供未复权数据，读取 front/back_ratio 即视为错误。"""

    def __init__(self, divid_error: bool = False) -> None:
        self.local_calls = []
        self.divid_calls = []
        self.divid_error = divid_error
        self.daily = pd.DataFrame(
            {
                "time": [_ms(day) for day, _, _ in RAW_DAILY],
                "open": [close for _, close, _ in RAW_DAILY],
                "high": [close + 0.02 for _, close, _ in RAW_DAILY],
                "low": [close - 0.02 for _, close, _ in RAW_DAILY],
                "close": [close for _, close, _ in RAW_DAILY],
                "volume": [0.0 if flag else 100000.0 for _, _, flag in RAW_DAILY],
                "amount": [0.0 if flag else close * 1e7 for _, close, flag in RAW_DAILY],
                "suspendFlag": [flag for _, _, flag in RAW_DAILY],
            },
            index=[day.replace("-", "") for day, _, _ in RAW_DAILY],
        )

    def get_local_data(self, stock_list, count, period, start_time, end_time, dividend_type):
        security = stock_list[0]
        self.local_calls.append((security, period, dividend_type, start_time, end_time))
        if dividend_type != "none":
            raise AssertionError(f"不应读取 QMT 内置复权 {dividend_type}")
        df = self.daily
        if start_time:
            df = df[df.index >= start_time[:8]]
        if end_time:
            df = df[df.index <= end_time[:8]]
        return {security: df.copy()}

    def get_divid_factors(self, stock_code: str, start_time: str = "", end_time: str = ""):
        self.divid_calls.append((stock_code, start_time, end_time))
        if self.divid_error:
            raise RuntimeError("divid service down")
        return pd.DataFrame(
            {
                "time": [_ms(day) for day, _ in EVENTS],
                "interest": [cash for _, cash in EVENTS],
                "stockBonus": [0.0] * len(EVENTS),
                "stockGift": [0.0] * len(EVENTS),
                "allotPrice": [0.0] * len(EVENTS),
                "allotNum": [0.0] * len(EVENTS),
                "gugai": [0.0] * len(EVENTS),
                "dr": [QMT_DR.get(day, 1.0) for day, _ in EVENTS],
            },
            index=[day.replace("-", "") for day, _ in EVENTS],
        )

    def get_instrument_detail(self, code):
        return {"InstrumentID": "510500", "OpenDate": "20130315", "PriceTick": 0.001}

    def get_trading_dates(self, market, start_time="", end_time="", count=-1):
        return [_ms(day) for day, _, _ in RAW_DAILY]


def _make_provider(monkeypatch, fake_xt: FakeXtData) -> MiniQMTProvider:
    monkeypatch.setattr(MiniQMTProvider, "_ensure_xtdata", staticmethod(lambda: fake_xt))
    monkeypatch.setattr(MiniQMTProvider, "_adjust_today", staticmethod(lambda: date(2026, 10, 20)))
    monkeypatch.delenv("DATA_CACHE_DIR", raising=False)
    return MiniQMTProvider({"cache_dir": None, "auto_download": False})


def _multiplier(event_day: str) -> float:
    cash = dict(EVENTS)[event_day]
    previous_close = PREVIOUS_CLOSE[event_day]
    return (previous_close - cash) / previous_close


def _expected_pre(anchor: str) -> pd.Series:
    """按 (前收−分红)/前收 计算到参考日的累计前复权因子。"""
    factors = {}
    for day, _, _ in RAW_DAILY:
        factor = 1.0
        for event_day, _ in EVENTS:
            if day < event_day <= anchor:
                factor *= _multiplier(event_day)
            elif anchor < event_day <= day:
                factor /= _multiplier(event_day)
        factors[pd.Timestamp(day)] = factor
    return pd.Series(factors)


def _raw_close() -> pd.Series:
    return pd.Series({pd.Timestamp(day): close for day, close, _ in RAW_DAILY})


@pytest.mark.unit
def test_510500_event_multiplier_uses_real_previous_close():
    """2026-07-15 单事件：真实前收算出 dr≈1.01803，与 QMT 内置 dr=1.01591 明显不同。"""
    multiplier = _multiplier("2026-07-15")
    assert multiplier == pytest.approx((8.413 - 0.149) / 8.413)
    assert 1 / multiplier == pytest.approx(1.01803, abs=5e-6)
    assert abs(1 / multiplier - QMT_DR["2026-07-15"]) > 2e-3


@pytest.mark.unit
def test_miniqmt_pre_matches_cash_dividend_with_real_previous_close(monkeypatch):
    fake_xt = FakeXtData()
    provider = _make_provider(monkeypatch, fake_xt)

    result = provider.get_price(
        SECURITY_JQ,
        start_date="2024-05-15",
        end_date="2026-07-16",
        fq="pre",
        fields=["close", "volume"],
    )

    # 默认参考日 = 今天（与 big_qmt 一致），四个事件全部在参考日之前
    expected_factor = _expected_pre("2026-10-20")
    raw_close = _raw_close()
    expected_close = np.round(raw_close * expected_factor, 3)
    pd.testing.assert_series_equal(
        result["close"], expected_close.rename("close"), check_freq=False, check_names=False
    )
    # 相邻两日的价格比 = 单事件因子：除权前一日 / 除权日 的调整价比
    for event_day, _ in EVENTS:
        prev_day = pd.Timestamp([d for d, _, f in RAW_DAILY if d < event_day and not f][-1])
        ratio = expected_factor[prev_day] / expected_factor[pd.Timestamp(event_day)]
        assert ratio == pytest.approx(
            (PREVIOUS_CLOSE[event_day] - dict(EVENTS)[event_day]) / PREVIOUS_CLOSE[event_day]
        )
    # 最后一个事件之后价格不调整
    assert result.loc["2026-07-16", "close"] == pytest.approx(8.300)
    assert result.loc["2026-07-15", "close"] == pytest.approx(8.270)
    assert result.loc["2026-07-14", "close"] == pytest.approx(8.264)
    # 成交量与 adjust_bars 一致：除以因子
    expected_volume = np.round(100000.0 / expected_factor[pd.Timestamp("2024-05-16")])
    assert result.loc["2024-05-16", "volume"] == pytest.approx(expected_volume)
    assert {call[2] for call in fake_xt.local_calls} == {"none"}


@pytest.mark.unit
def test_miniqmt_pre_respects_pre_factor_ref_date(monkeypatch):
    provider = _make_provider(monkeypatch, FakeXtData())

    result = provider.get_price(
        SECURITY_JQ,
        start_date="2024-05-15",
        end_date="2026-07-16",
        fq="pre",
        fields=["close"],
        pre_factor_ref_date="2025-06-01",
    )

    expected = np.round(_raw_close() * _expected_pre("2025-06-01"), 3)
    pd.testing.assert_series_equal(
        result["close"], expected.rename("close"), check_freq=False, check_names=False
    )
    # 参考日所在区间（2025-01-16 ~ 2026-01-15）价格等于真实价
    assert result.loc["2025-01-16", "close"] == pytest.approx(5.330)
    assert result.loc["2026-01-15", "close"] == pytest.approx(7.105)


@pytest.mark.unit
def test_miniqmt_post_uses_listing_origin(monkeypatch):
    provider = _make_provider(monkeypatch, FakeXtData())

    result = provider.get_price(
        SECURITY_JQ,
        start_date="2024-05-15",
        end_date="2026-07-16",
        fq="post",
        fields=["close"],
    )

    # 后复权固定原点 = 上市日 2013-03-15，等价于以最早日期为基准向后累计
    expected = np.round(_raw_close() * _expected_pre("2013-03-15"), 3)
    pd.testing.assert_series_equal(
        result["close"], expected.rename("close"), check_freq=False, check_names=False
    )
    assert result.loc["2024-05-16", "close"] == pytest.approx(5.712)


@pytest.mark.unit
def test_miniqmt_pre_raises_when_dividend_query_fails(monkeypatch):
    """查询除权事件失败不能静默当作无事件，也不能回退到 QMT 内置复权。"""
    provider = _make_provider(monkeypatch, FakeXtData(divid_error=True))

    with pytest.raises(AdjustmentError):
        provider.get_price(
            SECURITY_JQ,
            start_date="2024-05-15",
            end_date="2026-07-16",
            fq="pre",
            fields=["close"],
        )
