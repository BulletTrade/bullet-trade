import pytest

from bullet_trade.data.providers import miniqmt
from bullet_trade.data.providers.miniqmt import MiniQMTProvider


class FakeXtData:
    def get_stock_list_in_sector(self, sector):
        return ["000001.SZ", "000002.SZ"]

    def get_instrument_detail(self, code: str):
        if code == "000001.SZ":
            return None
        return {
            "InstrumentName": "测试证券",
            "InstrumentID": "000002",
            "OpenDate": "2020-01-01",
            "ExpireDate": None,
        }

    def get_instrument_type(self, code: str):
        if code == "000002.SZ":
            return {"etf": True}
        return {"stock": True}


@pytest.mark.unit
def test_miniqmt_get_all_securities_handles_missing_detail(monkeypatch):
    fake_xt = FakeXtData()
    monkeypatch.setattr(
        miniqmt.MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(lambda: fake_xt),
    )
    monkeypatch.delenv("DATA_CACHE_DIR", raising=False)

    provider = MiniQMTProvider({"cache_dir": None})
    monkeypatch.setattr(provider._cache, "cached_call", lambda name, kwargs, fn, result_type=None: fn(kwargs))

    df = provider.get_all_securities(types="stock", date=None)

    assert "000001.XSHE" in df.index
    assert "000002.XSHE" in df.index


@pytest.mark.unit
def test_miniqmt_get_all_securities_supports_fund_alias(monkeypatch):
    fake_xt = FakeXtData()
    monkeypatch.setattr(
        miniqmt.MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(lambda: fake_xt),
    )
    monkeypatch.delenv("DATA_CACHE_DIR", raising=False)

    provider = MiniQMTProvider({"cache_dir": None})
    monkeypatch.setattr(provider._cache, "cached_call", lambda name, kwargs, fn, result_type=None: fn(kwargs))

    df = provider.get_all_securities(types="fund", date=None)

    assert "000002.XSHE" in df.index
    assert df.loc["000002.XSHE", "type"] == "fund"


@pytest.mark.unit
def test_miniqmt_get_security_info_uses_instrument_type(monkeypatch):
    fake_xt = FakeXtData()
    monkeypatch.setattr(
        miniqmt.MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(lambda: fake_xt),
    )
    monkeypatch.delenv("DATA_CACHE_DIR", raising=False)

    provider = MiniQMTProvider({"cache_dir": None})
    info = provider.get_security_info("000002.XSHE")

    assert info["display_name"] == "测试证券"
    assert info["type"] == "etf"
    assert info["code"] == "000002.XSHE"
    assert info["qmt_code"] == "000002.SZ"
