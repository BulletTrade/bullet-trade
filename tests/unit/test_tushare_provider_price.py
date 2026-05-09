"""
Tushare 数据源基础价格接口测试。
"""

from datetime import date as Date

import pandas as pd
import pytest

from bullet_trade.data.providers.tushare import TushareProvider
from bullet_trade.utils.env_loader import get_env


class DummyTushareModule:
    def __init__(self):
        self.pro_bar_calls = []

    def pro_bar(self, **kwargs):
        self.pro_bar_calls.append(kwargs)
        return pd.DataFrame(
            {
                "ts_code": [kwargs["ts_code"]],
                "trade_date": ["20240102"],
                "open": [1.0],
                "high": [1.1],
                "low": [0.9],
                "close": [1.05],
                "vol": [100.0],
                "amount": [105.0],
            }
        )


def _provider_with_dummy_tushare(monkeypatch):
    provider = TushareProvider({"cache_dir": None})
    dummy_ts = DummyTushareModule()
    monkeypatch.setattr(provider, "_ensure_ts_module", lambda: dummy_ts)
    monkeypatch.setattr(provider, "_ensure_client", lambda: object())
    monkeypatch.setattr(
        provider._cache,
        "cached_call",
        lambda name, kwargs, fn, result_type=None: fn(kwargs),
    )
    return provider, dummy_ts


@pytest.mark.unit
def test_tushare_get_price_uses_index_asset_for_sh_index(monkeypatch):
    provider, dummy_ts = _provider_with_dummy_tushare(monkeypatch)

    df = provider.get_price(
        "000001.XSHG",
        start_date="2024-01-02",
        end_date="2024-01-02",
        fields=["close"],
    )

    assert list(df.columns) == ["close"]
    assert dummy_ts.pro_bar_calls[0]["ts_code"] == "000001.SH"
    assert dummy_ts.pro_bar_calls[0]["asset"] == "I"


@pytest.mark.unit
def test_tushare_get_price_keeps_sz_000001_as_stock(monkeypatch):
    provider, dummy_ts = _provider_with_dummy_tushare(monkeypatch)

    provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-02",
        fields=["close"],
        fq=None,
    )

    assert dummy_ts.pro_bar_calls[0]["ts_code"] == "000001.SZ"
    assert dummy_ts.pro_bar_calls[0]["asset"] == "E"


@pytest.mark.requires_network
def test_tushare_get_price_supports_jq_code():
    if not get_env("TUSHARE_TOKEN"):
        pytest.skip("缺少 TUSHARE_TOKEN")
    try:
        import tushare  # noqa: F401
    except ImportError:
        pytest.skip("未安装 tushare")

    from bullet_trade.data import api as data_api
    from bullet_trade.data.api import set_data_provider, get_price

    original_provider = data_api._provider
    original_auth_attempted = data_api._auth_attempted
    original_cache = data_api._security_info_cache

    try:
        set_data_provider("tushare")
        df = get_price(
            "600000.XSHG",
            start_date="2024-01-02",
            end_date="2024-01-10",
            frequency="1d",
            fields=["open", "high", "low", "close", "volume", "money"],
            fq="none",
        )
        assert not df.empty, "Tushare 返回空数据，请检查权限或代码转换"
        assert df.index.min().date() >= Date(2024, 1, 2)
        assert df.index.max().date() <= Date(2024, 1, 10)
        for col in ("open", "high", "low", "close", "volume", "money"):
            assert col in df.columns
    finally:
        data_api._provider = original_provider
        data_api._auth_attempted = original_auth_attempted
        data_api._security_info_cache = original_cache
