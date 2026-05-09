import datetime as dt
from typing import Dict, Tuple

import pandas as pd
import pytest

from bullet_trade.data.providers import miniqmt
from bullet_trade.data.providers.miniqmt import MiniQMTProvider


SECURITY_QMT = "000001.SZ"
SECURITY_JQ = "000001.XSHE"
EVENT_DATE = pd.Timestamp("2025-06-12")
_PRICE_COLUMNS = ["open", "high", "low", "close"]


class FakeXtData:
    def __init__(self, frames: Dict[Tuple[str, str], pd.DataFrame], dividends: Dict[str, pd.DataFrame]) -> None:
        self.frames = frames
        self.dividends = dividends
        self.download_calls = []
        self.data_dir = None

    def set_data_dir(self, path: str) -> None:
        self.data_dir = path

    def download_history_data(self, stock_code: str, period: str) -> None:
        self.download_calls.append((stock_code, period))

    def get_local_data(self, stock_list, count, period, start_time, end_time, dividend_type):
        security = stock_list[0]
        df = self.frames.get((security, dividend_type))
        if df is None:
            df = pd.DataFrame()
        return {security: df.copy()}

    def get_divid_factors(self, stock_code: str, start_time: str = "", end_time: str = ""):
        return self.dividends.get(stock_code)

    def get_trading_dates(self, market: str, start_time: str = "", end_time: str = "", count: int = -1):
        return []

    def get_stock_list_in_sector(self, sector: str):
        return []

    def get_instrument_detail(self, code: str):
        return {}




def _build_sample_frames():
    dates = pd.to_datetime(
        ["2025-05-20", "2025-06-11", "2025-06-12", "2025-06-13", "2025-06-30"]
    )
    base_prices = [12.0, 12.5, 11.0, 11.5, 12.2]
    raw_df = pd.DataFrame(
        {
            "time": [int(ts.value // 10**6) for ts in dates],
            "open": base_prices,
            "high": [p + 0.2 for p in base_prices],
            "low": [p - 0.2 for p in base_prices],
            "close": base_prices,
            "volume": [1000] * len(dates),
            "amount": [p * 1000 for p in base_prices],
        }
    )
    front_ratio_df = pd.DataFrame(columns=raw_df.columns)
    # QMT 分红数据格式：interest 为"每1股"派息
    # 对于股票，MiniQMTProvider 会自动转换为"每10股"口径
    # 即 interest=1.2 会转换为 bonus_pre_tax=12.0, per_base=10
    dividends = pd.DataFrame(
        [
            {
                "time": int(EVENT_DATE.value // 10**6),
                "interest": 1.2,  # 每1股派息1.2元
                "stockGift": 0.0,
                "stockBonus": 0.0,
                "allotNum": 0.0,
            }
        ]
    )
    frames = {
        (SECURITY_QMT, "none"): raw_df,
        (SECURITY_QMT, "front_ratio"): front_ratio_df,
        (SECURITY_QMT, "back_ratio"): pd.DataFrame(columns=raw_df.columns),
    }
    return frames, {SECURITY_QMT: dividends}, dates, raw_df


def _expected_frames(dates: pd.DatetimeIndex, raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    生成期望的未复权和前复权数据框。
    
    前复权算法：使用标准的复权因子公式
    - 除权日前的价格调整：adjusted = original × (preclose - dividend_per_share) / preclose
    - 其中 preclose 是除权前一天收盘价，dividend_per_share 是每股分红
    """
    idx = pd.DatetimeIndex(dates)
    base = raw_df.copy()
    base.index = idx
    base = base.rename(columns={"amount": "money"})
    pre = base.copy()
    
    # 计算前复权因子：除权前一天收盘价为 12.5，每股分红 1.2
    # 复权因子 = (12.5 - 1.2) / 12.5 = 11.3 / 12.5 = 0.904
    preclose = 12.5  # 除权前一天（2025-06-11）的收盘价
    dividend_per_share = 1.2  # 每股分红
    adjustment_factor = (preclose - dividend_per_share) / preclose
    
    # 对除权日之前的数据应用复权因子
    mask = pre.index < EVENT_DATE
    pre.loc[mask, _PRICE_COLUMNS] = pre.loc[mask, _PRICE_COLUMNS] * adjustment_factor
    # 四舍五入到2位小数，符合股票价格精度（分）
    pre.loc[mask, _PRICE_COLUMNS] = pre.loc[mask, _PRICE_COLUMNS].round(2)
    return base, pre


def _make_provider(monkeypatch, fake_xt: FakeXtData, **config):
    monkeypatch.setattr(
        miniqmt.MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(lambda: fake_xt),
    )
    monkeypatch.delenv("DATA_CACHE_DIR", raising=False)
    provider = MiniQMTProvider(config)
    provider.auth()
    return provider


@pytest.mark.unit
def test_ping_an_bank_dividend_alignment(monkeypatch):
    frames, dividends, dates, raw_df = _build_sample_frames()
    fake_xt = FakeXtData(frames, dividends)
    provider = _make_provider(monkeypatch, fake_xt, cache_dir=None)

    raw_expected, pre_expected = _expected_frames(dates, raw_df)

    jq_like = pre_expected.copy()

    result_none = provider.get_price(
        SECURITY_JQ,
        start_date=dates[0],
        end_date=dates[-1],
        fq="none",
        pre_factor_ref_date=EVENT_DATE,
    )
    result_pre = provider.get_price(
        SECURITY_JQ,
        start_date=dates[0],
        end_date=dates[-1],
        fq="pre",
        pre_factor_ref_date=EVENT_DATE,
    )
    result_pre_qmt = provider.get_price(
        SECURITY_QMT,
        start_date=dates[0],
        end_date=dates[-1],
        fq="pre",
        pre_factor_ref_date=EVENT_DATE,
    )

    pd.testing.assert_index_equal(result_none.index, raw_expected.index)
    pd.testing.assert_frame_equal(
        result_none[_PRICE_COLUMNS + ["volume", "money"]],
        raw_expected[_PRICE_COLUMNS + ["volume", "money"]],
    )
    max_diff = (result_pre[_PRICE_COLUMNS] - jq_like[_PRICE_COLUMNS]).abs().max().max()
    # 考虑到浮点数运算误差和小数位舍入（2位精度），使用 0.01 的容差（1分钱）
    #assert max_diff < 0.01, f"最大差异 {max_diff} 超过 0.01 元"
    assert max_diff < 1e-6
    assert result_pre.loc[EVENT_DATE, "close"] == pytest.approx(raw_expected.loc[EVENT_DATE, "close"])
    pd.testing.assert_frame_equal(result_pre, result_pre_qmt)
    assert fake_xt.download_calls
    assert set(fake_xt.download_calls) == {(SECURITY_QMT, "1d")}


@pytest.mark.unit
def test_live_mode_enables_auto_download_by_default(monkeypatch):
    frames, dividends, dates, _ = _build_sample_frames()
    fake_xt = FakeXtData(frames, dividends)
    provider = _make_provider(monkeypatch, fake_xt, cache_dir=None, mode="live")

    assert provider.auto_download is True
    provider.get_price(
        SECURITY_JQ,
        start_date=dates[0],
        end_date=dates[-1],
        fq="none",
    )

    assert fake_xt.download_calls == [(SECURITY_QMT, "1d")]


@pytest.mark.unit
def test_live_mode_respects_explicit_auto_download_false(monkeypatch):
    frames, dividends, dates, _ = _build_sample_frames()
    fake_xt = FakeXtData(frames, dividends)
    provider = _make_provider(monkeypatch, fake_xt, cache_dir=None, mode="live", auto_download=False)

    assert provider.auto_download is False
    provider.get_price(
        SECURITY_JQ,
        start_date=dates[0],
        end_date=dates[-1],
        fq="none",
    )

    assert fake_xt.download_calls == []


@pytest.mark.unit
def test_get_split_dividend_no_cross_provider_fallback(monkeypatch):
    frames, _, dates, raw_df = _build_sample_frames()
    fake_xt = FakeXtData(frames, dividends={SECURITY_QMT: pd.DataFrame()})
    _ = raw_df

    monkeypatch.setattr(
        miniqmt.MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(lambda: fake_xt),
    )
    monkeypatch.delenv("DATA_CACHE_DIR", raising=False)

    provider = MiniQMTProvider({"cache_dir": None})

    events = provider.get_split_dividend(SECURITY_JQ, start_date=dates[0], end_date=dates[-1])

    assert events == []
