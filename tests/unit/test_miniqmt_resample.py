import pandas as pd
import pytest

from bullet_trade.data.backtest_session import (
    BacktestDataSession,
    BacktestDataSessionConfig,
    reset_current_backtest_data_session,
    set_current_backtest_data_session,
)
from bullet_trade.data.providers import miniqmt
from bullet_trade.data.providers.miniqmt import MiniQMTProvider


SECURITY_QMT = "000001.SZ"
SECURITY_JQ = "000001.XSHE"
PRICE_FIELDS = ["open", "high", "low", "close", "volume", "money"]


class FakeXtData:
    def __init__(self, frames):
        self.frames = frames
        self.local_calls = []
        self.download_calls = []

    def download_history_data(self, stock_code: str, period: str, **kwargs) -> None:
        self.download_calls.append((stock_code, period, kwargs))

    def get_local_data(self, stock_list, count, period, start_time, end_time, dividend_type):
        security = stock_list[0]
        self.local_calls.append(
            {
                "security": security,
                "period": period,
                "dividend_type": dividend_type,
                "start_time": start_time,
                "end_time": end_time,
                "count": count,
            }
        )
        df = self.frames.get((security, period, dividend_type), pd.DataFrame())
        if count and count > 0 and not df.empty:
            df = df.tail(count)
        return {security: df.copy()}

    def get_divid_factors(self, stock_code: str, start_time: str = "", end_time: str = ""):
        return pd.DataFrame()

    def get_trading_dates(
        self, market: str, start_time: str = "", end_time: str = "", count: int = -1
    ):
        return []


def _qmt_time_ms(value: str) -> int:
    ts = pd.Timestamp(value).tz_localize("Asia/Shanghai").tz_convert("UTC")
    return int(ts.value // 10**6)


def _build_minute_frame(times, price_scale: float = 1.0) -> pd.DataFrame:
    prices = [10.0 + idx for idx, _ in enumerate(times)]
    volumes = [100.0 + idx for idx, _ in enumerate(times)]
    return pd.DataFrame(
        {
            "time": [_qmt_time_ms(ts) for ts in times],
            "open": [price * price_scale for price in prices],
            "high": [(price + 0.5) * price_scale for price in prices],
            "low": [(price - 0.5) * price_scale for price in prices],
            "close": [price * price_scale for price in prices],
            "volume": volumes,
            "amount": [price * volume for price, volume in zip(prices, volumes)],
        }
    )


def _make_provider(monkeypatch, fake_xt: FakeXtData, **config) -> MiniQMTProvider:
    monkeypatch.setattr(
        miniqmt.MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(lambda: fake_xt),
    )
    monkeypatch.delenv("DATA_CACHE_DIR", raising=False)
    return MiniQMTProvider({"cache_dir": None, "auto_download": False, **config})


@pytest.mark.unit
def test_miniqmt_resamples_5m_with_partial_current_bar(monkeypatch):
    times = [f"2026-05-14 09:{minute:02d}:00" for minute in range(30, 38)]
    raw_1m = _build_minute_frame(times)
    fake_xt = FakeXtData({(SECURITY_QMT, "1m", "none"): raw_1m})
    provider = _make_provider(monkeypatch, fake_xt)

    result = provider.get_price(
        SECURITY_JQ,
        start_date="2026-05-14 09:30:00",
        end_date="2026-05-14 09:37:00",
        frequency="5m",
        fq="none",
        fields=PRICE_FIELDS,
    )

    expected = pd.DataFrame(
        {
            "open": [11.0, 16.0],
            "high": [15.5, 17.5],
            "low": [10.5, 15.5],
            "close": [15.0, 17.0],
            "volume": [515.0, 213.0],
            "money": [
                sum((10.0 + idx) * (100.0 + idx) for idx in range(1, 6)),
                sum((10.0 + idx) * (100.0 + idx) for idx in range(6, 8)),
            ],
        },
        index=pd.to_datetime(["2026-05-14 09:35:00", "2026-05-14 09:37:00"]),
    )
    pd.testing.assert_frame_equal(result, expected)
    assert [call["period"] for call in fake_xt.local_calls] == ["1m"]


@pytest.mark.unit
def test_miniqmt_resample_count_expands_1m_window_and_accepts_min_alias(monkeypatch):
    times = [f"2026-05-14 09:{minute:02d}:00" for minute in range(31, 46)]
    raw_1m = _build_minute_frame(times)
    fake_xt = FakeXtData({(SECURITY_QMT, "1m", "none"): raw_1m})
    provider = _make_provider(monkeypatch, fake_xt)

    result = provider.get_price(
        SECURITY_JQ,
        frequency="5min",
        count=2,
        fq="none",
        fields=["close"],
    )

    assert (
        result.index.tolist()
        == pd.to_datetime(["2026-05-14 09:40:00", "2026-05-14 09:45:00"]).tolist()
    )
    assert result["close"].tolist() == [19.0, 24.0]
    assert fake_xt.local_calls[0]["period"] == "1m"
    assert fake_xt.local_calls[0]["count"] == 10


@pytest.mark.unit
def test_miniqmt_resamples_front_ratio_before_grouping(monkeypatch):
    times = [f"2026-05-14 09:{minute:02d}:00" for minute in range(31, 41)]
    raw_1m = _build_minute_frame(times)
    front_1m = _build_minute_frame(times, price_scale=0.5)
    fake_xt = FakeXtData(
        {
            (SECURITY_QMT, "1m", "none"): raw_1m,
            (SECURITY_QMT, "1m", "front_ratio"): front_1m,
        }
    )
    provider = _make_provider(monkeypatch, fake_xt)

    result = provider.get_price(
        SECURITY_JQ,
        start_date="2026-05-14 09:30:00",
        end_date="2026-05-14 09:40:00",
        frequency="5m",
        fq="pre",
        fields=["open", "high", "low", "close"],
    )

    expected = pd.DataFrame(
        {
            "open": [5.0, 7.5],
            "high": [7.25, 9.75],
            "low": [4.75, 7.25],
            "close": [7.0, 9.5],
        },
        index=pd.to_datetime(["2026-05-14 09:35:00", "2026-05-14 09:40:00"]),
    )
    pd.testing.assert_frame_equal(result, expected)
    assert [call["dividend_type"] for call in fake_xt.local_calls] == ["none", "front_ratio"]


@pytest.mark.unit
def test_miniqmt_resample_falls_back_to_native_period_when_1m_is_empty(monkeypatch):
    native_5m = _build_minute_frame(["2026-05-14 09:35:00"])
    fake_xt = FakeXtData({(SECURITY_QMT, "5m", "none"): native_5m})
    provider = _make_provider(monkeypatch, fake_xt)

    result = provider.get_price(
        SECURITY_JQ,
        frequency="5m",
        fq="none",
        fields=["close"],
    )

    assert result["close"].tolist() == [10.0]
    assert [call["period"] for call in fake_xt.local_calls] == ["1m", "5m"]


@pytest.mark.unit
def test_miniqmt_live_mode_resampled_period_downloads_1m(monkeypatch):
    times = [f"2026-05-14 09:{minute:02d}:00" for minute in range(31, 46)]
    raw_1m = _build_minute_frame(times)
    fake_xt = FakeXtData({(SECURITY_QMT, "1m", "none"): raw_1m})
    provider = _make_provider(monkeypatch, fake_xt, mode="live", auto_download=True)

    result = provider.get_price(
        SECURITY_JQ,
        frequency="15m",
        count=1,
        fq="none",
        fields=["close"],
    )

    assert result.index.tolist() == [pd.Timestamp("2026-05-14 09:45:00")]
    assert result["close"].tolist() == [24.0]
    assert [call[1] for call in fake_xt.download_calls] == ["1m"]
    assert [call["period"] for call in fake_xt.local_calls] == ["1m"]


@pytest.mark.unit
def test_miniqmt_backtest_session_resampled_period_prepares_1m(monkeypatch):
    times = [f"2026-05-14 09:{minute:02d}:00" for minute in range(31, 46)]
    raw_1m = _build_minute_frame(times)
    fake_xt = FakeXtData({(SECURITY_QMT, "1m", "none"): raw_1m})
    provider = _make_provider(monkeypatch, fake_xt, mode="backtest", auto_download=True)
    session = BacktestDataSession(
        BacktestDataSessionConfig(
            enabled=True,
            start_date=pd.Timestamp("2026-05-14 09:30:00").to_pydatetime(),
            end_date=pd.Timestamp("2026-05-14 10:30:00").to_pydatetime(),
        )
    )
    token = set_current_backtest_data_session(session)

    try:
        result = provider.get_price(
            SECURITY_JQ,
            start_date="2026-05-14 09:30:00",
            end_date="2026-05-14 09:45:00",
            frequency="15m",
            fq="none",
            fields=["close"],
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)

    assert result.index.tolist() == [pd.Timestamp("2026-05-14 09:45:00")]
    assert result["close"].tolist() == [24.0]
    assert [call[1] for call in fake_xt.download_calls] == ["1m"]
    assert {call["period"] for call in fake_xt.local_calls} == {"1m"}
