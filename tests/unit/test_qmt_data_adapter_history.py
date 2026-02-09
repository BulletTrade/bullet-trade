import pandas as pd
import pytest

from bullet_trade.server.adapters.qmt import QmtDataAdapter


class _FakeProvider:
    def __init__(self) -> None:
        self.last_call = None

    def get_price(
        self,
        security,
        count=None,
        start_date=None,
        end_date=None,
        frequency=None,
        fq=None,
        fields=None,
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
            "pre_factor_ref_date": pre_factor_ref_date,
        }
        return pd.DataFrame({"open": [1.0], "close": [2.0]})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_qmt_data_adapter_get_history_passes_fields(monkeypatch):
    async def _run_now(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr("bullet_trade.server.adapters.qmt._run_in_qmt_executor", _run_now)
    fake_provider = _FakeProvider()
    monkeypatch.setattr("bullet_trade.server.adapters.qmt.MiniQMTProvider", lambda _cfg: fake_provider)
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
    assert resp["dtype"] == "dataframe"
    assert resp["columns"] == ["open", "close"]
    assert resp["records"] == [[1.0, 2.0]]
