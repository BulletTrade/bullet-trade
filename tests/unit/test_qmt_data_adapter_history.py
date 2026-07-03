import pandas as pd
import pytest

from bullet_trade.server.adapters.qmt import QmtDataAdapter


class _FakeProvider:
    def __init__(self) -> None:
        self.last_call = None
        self.last_trade_days_call = None
        self.last_ensure_cache_call = None

    def get_price(
        self,
        security,
        count=None,
        start_date=None,
        end_date=None,
        frequency=None,
        fq=None,
        fields=None,
        skip_paused=False,
        panel=True,
        fill_paused=True,
        pre_factor_ref_date=None,
    ):
        self.last_call = {
            "security": security,
            "count": count,
            "start_date": start_date,
            "end_date": end_date,
            "frequency": frequency,
            "fq": fq,
            "fields": fields,
            "skip_paused": skip_paused,
            "panel": panel,
            "fill_paused": fill_paused,
            "pre_factor_ref_date": pre_factor_ref_date,
        }
        return pd.DataFrame({"open": [1.0], "close": [2.0]})

    def get_trade_days(self, start_date=None, end_date=None, count=None):
        self.last_trade_days_call = {
            "start_date": start_date,
            "end_date": end_date,
            "count": count,
        }
        return [pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-03")]

    def ensure_cache(
        self,
        security,
        frequency,
        start=None,
        end=None,
        auto_download=True,
        count=None,
    ):
        self.last_ensure_cache_call = {
            "security": security,
            "frequency": frequency,
            "start": start,
            "end": end,
            "auto_download": auto_download,
            "count": count,
        }
        return {"requested": bool(auto_download), "security": security}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_qmt_data_adapter_get_history_passes_fields(monkeypatch):
    async def _run_now(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr("bullet_trade.server.adapters.qmt._run_in_qmt_executor", _run_now)
    fake_provider = _FakeProvider()
    monkeypatch.setattr(
        "bullet_trade.server.adapters.qmt.MiniQMTProvider", lambda _cfg: fake_provider
    )
    adapter = QmtDataAdapter()

    payload = {
        "security": "000001.XSHE",
        "count": 2,
        "start": "2025-01-01",
        "end": "2025-01-31",
        "frequency": "daily",
        "fq": "pre",
        "fields": ["open", "close"],
    }

    resp = await adapter.get_history(payload)

    assert fake_provider.last_call is not None
    assert fake_provider.last_call["security"] == "000001.XSHE"
    assert fake_provider.last_call["fields"] == ["open", "close"]
    assert fake_provider.last_call["frequency"] == "daily"
    assert resp["dtype"] == "dataframe"
    assert resp["columns"] == ["open", "close"]
    assert resp["records"] == [[1.0, 2.0]]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_qmt_data_adapter_get_history_forwards_resampled_frequency(monkeypatch):
    async def _run_now(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr("bullet_trade.server.adapters.qmt._run_in_qmt_executor", _run_now)
    fake_provider = _FakeProvider()
    monkeypatch.setattr(
        "bullet_trade.server.adapters.qmt.MiniQMTProvider", lambda _cfg: fake_provider
    )
    adapter = QmtDataAdapter()

    await adapter.get_history(
        {
            "security": "000001.XSHE",
            "start": "2026-05-14 09:30:00",
            "end": "2026-05-14 10:30:00",
            "frequency": "15m",
            "fq": "none",
            "fields": ["close"],
        }
    )

    assert fake_provider.last_call is not None
    assert fake_provider.last_call["frequency"] == "15m"
    assert fake_provider.last_call["start_date"] == "2026-05-14 09:30:00"
    assert fake_provider.last_call["end_date"] == "2026-05-14 10:30:00"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_qmt_data_adapter_get_trade_days_passes_count_and_fallback_keys(monkeypatch):
    async def _run_now(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr("bullet_trade.server.adapters.qmt._run_in_qmt_executor", _run_now)
    fake_provider = _FakeProvider()
    monkeypatch.setattr(
        "bullet_trade.server.adapters.qmt.MiniQMTProvider", lambda _cfg: fake_provider
    )
    adapter = QmtDataAdapter()

    resp = await adapter.get_trade_days(
        {
            "start_date": "2025-01-01",
            "end_date": "2025-01-31",
            "count": 5,
        }
    )

    assert fake_provider.last_trade_days_call == {
        "start_date": "2025-01-01",
        "end_date": "2025-01-31",
        "count": 5,
    }
    assert resp == {
        "dtype": "list",
        "values": ["2025-01-02 00:00:00", "2025-01-03 00:00:00"],
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_qmt_data_adapter_ensure_cache_passes_aliases(monkeypatch):
    async def _run_now(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr("bullet_trade.server.adapters.qmt._run_in_qmt_executor", _run_now)
    fake_provider = _FakeProvider()
    monkeypatch.setattr(
        "bullet_trade.server.adapters.qmt.MiniQMTProvider", lambda _cfg: fake_provider
    )
    adapter = QmtDataAdapter()

    resp = await adapter.ensure_cache(
        {
            "stockcode": "000001.XSHE",
            "period": "minute",
            "start_date": "2026-07-03",
            "end_time": "20260703150000",
            "auto_download": "false",
            "count": 10,
        }
    )

    assert fake_provider.last_ensure_cache_call == {
        "security": "000001.XSHE",
        "frequency": "minute",
        "start": "2026-07-03",
        "end": "20260703150000",
        "auto_download": False,
        "count": 10,
    }
    assert resp == {
        "dtype": "dict",
        "value": {"requested": False, "security": "000001.XSHE"},
    }
