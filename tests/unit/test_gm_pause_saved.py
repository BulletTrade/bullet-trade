"""GM 真实市场快照复用跨源停牌契约；含量额缺口与未来信息负例。"""

import json
from pathlib import Path

import pandas as pd
import pytest

from bullet_trade.data.providers.gm import GmDataProvider
from bullet_trade.integrations.gm.data_client import GmDataError
from tests.price_pause_contract import CASES, assert_pause_contract

DATA = json.loads((Path(__file__).parents[1] / "fixtures/gm_pause_20261008.json").read_text())


class RecordedMarket:
    def __init__(self, fault=None):
        self.fault = fault
        self.calls = []

    def query(self, method, **kwargs):
        self.calls.append((method, kwargs))
        key = json.dumps([method, kwargs], sort_keys=True, default=str)
        rows = json.loads(json.dumps(DATA["sdk"][key]))
        if self.fault and method == "history" and kwargs["frequency"] == "1d":
            rows[0][self.fault] += 100
        return rows


@pytest.mark.parametrize("case", CASES, ids=lambda c: c["id"])
def test_actual_gm_market_payload_satisfies_shared_pause_contract(case):
    p = GmDataProvider({"client": RecordedMarket()})
    assert_pause_contract(p.get_price(**case["request"]), case)


@pytest.mark.parametrize("fault", ["volume", "amount"])
def test_minute_gap_cannot_hide_missing_trades(fault):
    p = GmDataProvider({"client": RecordedMarket(fault)})
    case = next(c for c in CASES if c["id"] == "qdii_etf_partial_pause_minute_defaults")
    with pytest.raises(GmDataError, match="不守恒"):
        p.get_price(**case["request"])


def test_historical_intraday_request_cannot_use_later_daily_totals():
    sdk = RecordedMarket()
    p = GmDataProvider({"client": sdk})
    with pytest.raises(GmDataError, match="未来"):
        p._complete_zero_volume_day(
            "SHSE.513100", pd.Timestamp("2026-09-24"), pd.Timestamp("2026-09-24 10:30")
        )
    assert sdk.calls == []


def test_current_day_cannot_use_eventual_daily_totals(monkeypatch):
    from bullet_trade.data.providers import gm

    monkeypatch.setattr(gm, "_timestamp", lambda: pd.Timestamp("2026-09-24 10:30"))
    sdk = RecordedMarket()
    p = GmDataProvider({"client": sdk})
    with pytest.raises(GmDataError, match="未来"):
        p._complete_zero_volume_day(
            "SHSE.513100", pd.Timestamp("2026-09-24"), pd.Timestamp("2026-09-24 23:59")
        )
    assert sdk.calls == []
