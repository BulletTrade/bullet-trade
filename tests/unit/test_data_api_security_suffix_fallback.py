from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.core.settings import reset_settings, set_option
from bullet_trade.data import api as data_api
from bullet_trade.data.providers.base import DataProvider


class SuffixFallbackProvider(DataProvider):
    def __init__(self):
        self.price_calls = []
        self.info_calls = []
        self.index_calls = []

    def auth(self, *args, **kwargs):
        return True

    def get_price(self, security, *args, **kwargs):
        self.price_calls.append(security)
        if security == "518880.XSHG":
            return pd.DataFrame(
                {"close": [1.23]},
                index=[pd.Timestamp("2017-01-06 15:00:00")],
            )
        raise ValueError(f"找不到标的{security}")

    def get_security_info(self, security, date=None):
        self.info_calls.append(security)
        if security == "518880.XSHG":
            return {"type": "fund", "display_name": "黄金ETF"}
        raise ValueError(f"找不到标的{security}")

    def get_trade_days(self, *args, **kwargs):
        return [pd.Timestamp("2017-01-06"), pd.Timestamp("2017-01-09")]

    def get_all_securities(self, *args, **kwargs):
        return pd.DataFrame()

    def get_index_stocks(self, index_symbol, date=None):
        self.index_calls.append(index_symbol)
        if index_symbol == "000300.XSHG":
            return ["000001.XSHE"]
        raise ValueError(f"找不到标的{index_symbol}")

    def get_split_dividend(self, *args, **kwargs):
        return []


class MissingSecurityProvider(SuffixFallbackProvider):
    def get_price(self, security, *args, **kwargs):
        self.price_calls.append(security)
        raise ValueError(f"找不到标的{security}")


@pytest.fixture(autouse=True)
def _reset_data_api_state(monkeypatch):
    reset_settings()
    monkeypatch.setattr(data_api, "_security_info_cache", {}, raising=False)
    monkeypatch.setattr(data_api, "_auth_attempted", True, raising=False)
    monkeypatch.setattr(data_api, "_current_context", None, raising=False)
    yield
    reset_settings()
    monkeypatch.setattr(data_api, "_security_info_cache", {}, raising=False)
    monkeypatch.setattr(data_api, "_current_context", None, raising=False)


@pytest.mark.unit
def test_attribute_history_supports_sh_suffix_fallback(monkeypatch):
    provider = SuffixFallbackProvider()
    monkeypatch.setattr(data_api, "_provider", provider, raising=False)
    monkeypatch.setattr(
        data_api,
        "_current_context",
        SimpleNamespace(current_dt=datetime(2017, 1, 9, 9, 40, 0)),
        raising=False,
    )
    set_option("use_real_price", True)

    df = data_api.attribute_history("518880.SH", 2, "1d", ["close"])

    assert list(df.columns) == ["close"]
    assert float(df.close.iloc[-1]) == pytest.approx(1.23)
    assert provider.price_calls[:2] == ["518880.SH", "518880.XSHG"]


@pytest.mark.unit
def test_get_security_info_supports_sh_suffix_fallback(monkeypatch):
    provider = SuffixFallbackProvider()
    monkeypatch.setattr(data_api, "_provider", provider, raising=False)

    info = data_api.get_security_info("518880.SH")

    assert info["type"] == "fund"
    assert info["display_name"] == "黄金ETF"
    assert provider.info_calls == ["518880.SH", "518880.XSHG"]


@pytest.mark.unit
def test_get_index_stocks_supports_sh_suffix_fallback(monkeypatch):
    provider = SuffixFallbackProvider()
    monkeypatch.setattr(data_api, "_provider", provider, raising=False)

    stocks = data_api.get_index_stocks("000300.SH")

    assert stocks == ["000001.XSHE"]
    assert provider.index_calls == ["000300.SH", "000300.XSHG"]


@pytest.mark.unit
def test_get_price_returns_empty_frame_with_requested_fields_when_symbol_missing(monkeypatch):
    provider = MissingSecurityProvider()
    monkeypatch.setattr(data_api, "_provider", provider, raising=False)

    df = data_api.get_price(
        "518880.SH",
        start_date="2017-01-01",
        end_date="2017-01-02",
        frequency="daily",
        fields=["close"],
    )

    assert list(df.columns) == ["close"]
    assert df.empty
    assert df.close.empty
