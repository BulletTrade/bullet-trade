import pytest

from bullet_trade.data.providers import miniqmt
from bullet_trade.data.providers.miniqmt import MiniQMTProvider


class _FakeXtData:
    def __init__(self):
        self.download_calls = []

    def download_history_data(
        self,
        stock_code,
        period,
        start_time="",
        end_time="",
    ):
        self.download_calls.append(
            {
                "stock_code": stock_code,
                "period": period,
                "start_time": start_time,
                "end_time": end_time,
            }
        )


def _make_provider(monkeypatch, fake_xt):
    monkeypatch.setattr(
        miniqmt.MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(lambda: fake_xt),
    )
    monkeypatch.delenv("DATA_CACHE_DIR", raising=False)
    return MiniQMTProvider({"cache_dir": None, "mode": "live"})


@pytest.mark.unit
def test_miniqmt_ensure_cache_downloads_normalized_range(monkeypatch):
    fake_xt = _FakeXtData()
    provider = _make_provider(monkeypatch, fake_xt)

    result = provider.ensure_cache(
        "510300.XSHG",
        frequency="daily",
        start="2026-07-01",
        end="2026-07-03",
    )

    assert fake_xt.download_calls == [
        {
            "stock_code": "510300.SH",
            "period": "1d",
            "start_time": "20260701",
            "end_time": "20260703",
        }
    ]
    assert result == {
        "security": "510300.XSHG",
        "qmt_security": "510300.SH",
        "period": "1d",
        "download_period": "1d",
        "start": "2026-07-01",
        "end": "2026-07-03",
        "requested": True,
        "handled_by_session": False,
    }


@pytest.mark.unit
def test_miniqmt_ensure_cache_false_does_not_touch_xtdata(monkeypatch):
    def _unexpected_xtdata():
        raise AssertionError("auto_download=False should not import xtdata")

    monkeypatch.setattr(
        miniqmt.MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(_unexpected_xtdata),
    )
    provider = MiniQMTProvider({"cache_dir": None, "mode": "live"})

    result = provider.ensure_cache(
        "000001.XSHE",
        frequency="minute",
        start="20260703150000",
        end="20260703150000",
        auto_download=False,
    )

    assert result["security"] == "000001.XSHE"
    assert result["qmt_security"] == "000001.SZ"
    assert result["period"] == "1m"
    assert result["download_period"] == "1m"
    assert result["requested"] is False


@pytest.mark.unit
def test_miniqmt_ensure_cache_uses_1m_source_for_resampled_period(monkeypatch):
    fake_xt = _FakeXtData()
    provider = _make_provider(monkeypatch, fake_xt)

    result = provider.ensure_cache(
        "000001.XSHE",
        frequency="5m",
        start="20260703093100",
        end="20260703150000",
        count=2,
    )

    assert fake_xt.download_calls == [
        {
            "stock_code": "000001.SZ",
            "period": "1m",
            "start_time": "20260703093100",
            "end_time": "20260703150000",
        }
    ]
    assert result["period"] == "5m"
    assert result["download_period"] == "1m"
    assert result["requested"] is True
