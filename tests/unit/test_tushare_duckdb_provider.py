"""
配置 tushare_duckdb_path 时，TushareProvider 用 tushare_duckdb 替换 tushare 模块的测试。
"""

from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.data.providers.tushare import TushareProvider
from bullet_trade.data.providers import tushare_duckdb


class DummyDuckdbModule:
    def __init__(self):
        self.pro_bar_calls = []
        self.db_path = None

    def set_db_path(self, path):
        self.db_path = path

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


def _cache_calls_through(provider):
    provider._cache.cached_call = lambda name, kwargs, fn, result_type=None: fn(kwargs)


@pytest.mark.unit
def test_ensure_ts_module_uses_duckdb_when_path_configured(monkeypatch):
    provider = TushareProvider({"cache_dir": None, "tushare_duckdb_path": "/tmp/test.duckdb"})

    captured = {}
    monkeypatch.setattr(tushare_duckdb, "set_db_path", lambda p: captured.setdefault("path", p))

    ts = provider._ensure_ts_module()

    assert ts is tushare_duckdb
    assert captured.get("path") == "/tmp/test.duckdb"


@pytest.mark.unit
def test_ensure_ts_module_uses_tushare_without_path():
    try:
        import tushare as real_tushare
    except ImportError:
        pytest.skip("未安装 tushare")

    provider = TushareProvider({"cache_dir": None})

    assert provider._ensure_ts_module() is real_tushare


@pytest.mark.unit
def test_auth_sets_global_token_on_module(monkeypatch):
    provider = TushareProvider({"cache_dir": None, "tushare_duckdb_path": "/tmp/test.duckdb"})
    provider._token = "TOKEN123"

    calls = {}

    def fake_pro_api(token):
        calls["token"] = token
        return SimpleNamespace()

    def fake_set_token(token):
        calls["set_token"] = token

    monkeypatch.setattr(
        provider,
        "_ensure_ts_module",
        lambda: SimpleNamespace(pro_api=fake_pro_api, set_token=fake_set_token),
    )

    provider.auth()

    assert calls["token"] == "TOKEN123"
    assert calls["set_token"] == "TOKEN123"


@pytest.mark.unit
def test_get_price_routes_pro_bar_through_duckdb_module_with_api(monkeypatch):
    provider = TushareProvider({"cache_dir": None, "tushare_duckdb_path": "/tmp/test.duckdb"})
    dummy = DummyDuckdbModule()
    monkeypatch.setattr(provider, "_ensure_ts_module", lambda: dummy)
    monkeypatch.setattr(provider, "_ensure_client", lambda: object())
    _cache_calls_through(provider)

    df = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-02",
        fields=["close"],
        fq=None,
    )

    assert list(df.columns) == ["close"]
    assert dummy.pro_bar_calls[0]["ts_code"] == "000001.SZ"
    assert "api" in dummy.pro_bar_calls[0]


@pytest.mark.unit
def test_tushare_duckdb_pro_bar_accepts_and_forwards_api(monkeypatch):
    class FakePro:
        def daily(self, **kwargs):
            return None

    captured = {}
    monkeypatch.setattr(tushare_duckdb, "_query_local", lambda *a, **k: None)
    monkeypatch.setattr(tushare_duckdb._ts, "pro_api", lambda *a, **k: FakePro())

    def fake_pro_bar(**kwargs):
        captured["api"] = kwargs.get("api")
        return pd.DataFrame({"a": [1]})

    monkeypatch.setattr(tushare_duckdb._ts, "pro_bar", fake_pro_bar)

    result = tushare_duckdb.pro_bar(
        ts_code="000001.SZ",
        asset="E",
        start_date="20260720",
        end_date="20260731",
        api="CUSTOM_API",
    )

    assert captured["api"] == "CUSTOM_API"
    assert not result.empty
