from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.core.exceptions import FutureDataError
from bullet_trade.core.settings import set_option
from bullet_trade.data import api as data_api


class _DummyProvider:
    name = "dummy"

    def __init__(self):
        self.trade_days_calls = []
        self.price_calls = []
        self.auth_calls = 0

    def auth(self, user=None, pwd=None, host=None, port=None):
        self.auth_calls += 1

    def get_trade_days(self, start_date=None, end_date=None, count=None):
        self.trade_days_calls.append(
            {
                "start_date": start_date,
                "end_date": end_date,
                "count": count,
            }
        )
        return [pd.Timestamp("2026-02-12")]

    def get_price(self, **kwargs):
        self.price_calls.append(kwargs)
        return pd.DataFrame({"close": [1.0]})


@pytest.mark.unit
def test_get_trade_days_live_should_not_clip_end_date(monkeypatch):
    provider = _DummyProvider()
    context = SimpleNamespace(
        current_dt=datetime(2026, 2, 11, 23, 59, 0),
        run_params={"is_live": True},
    )

    monkeypatch.setattr(data_api, "_provider", provider, raising=False)
    monkeypatch.setattr(data_api, "_auth_attempted", False, raising=False)
    monkeypatch.setattr(data_api, "_current_context", context, raising=False)

    days = data_api.get_trade_days("2026-02-12", "2026-02-12")

    assert len(days) == 1
    assert provider.trade_days_calls, "provider.get_trade_days was not called"
    assert provider.trade_days_calls[0]["end_date"] == datetime(2026, 2, 12, 0, 0, 0)


@pytest.mark.unit
def test_get_trade_days_backtest_still_blocks_future_date(monkeypatch):
    provider = _DummyProvider()
    context = SimpleNamespace(
        current_dt=datetime(2026, 2, 11, 23, 59, 0),
        run_params={"is_live": False},
    )

    set_option("avoid_future_data", True)
    monkeypatch.setattr(data_api, "_provider", provider, raising=False)
    monkeypatch.setattr(data_api, "_auth_attempted", False, raising=False)
    monkeypatch.setattr(data_api, "_current_context", context, raising=False)

    with pytest.raises(FutureDataError):
        data_api.get_trade_days("2026-02-12", "2026-02-12")


@pytest.mark.unit
def test_get_price_live_should_not_apply_avoid_future_guard(monkeypatch):
    provider = _DummyProvider()
    context = SimpleNamespace(
        current_dt=datetime(2026, 2, 11, 9, 30, 0),
        run_params={"is_live": True},
    )

    set_option("avoid_future_data", True)
    monkeypatch.setattr(data_api, "_provider", provider, raising=False)
    monkeypatch.setattr(data_api, "_auth_attempted", False, raising=False)
    monkeypatch.setattr(data_api, "_current_context", context, raising=False)

    df = data_api.get_price(
        "000001.XSHE",
        end_date=datetime(2026, 2, 11, 10, 0, 0),
        frequency="minute",
        fields=["close"],
        count=1,
    )

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert provider.price_calls, "provider.get_price was not called"
    assert provider.price_calls[0]["end_date"] == context.current_dt
