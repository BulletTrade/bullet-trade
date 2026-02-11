from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.data import api as data_api


@pytest.mark.unit
def test_attribute_history_daily_excludes_current_day(monkeypatch):
    captured = {}
    context = SimpleNamespace(current_dt=datetime(2025, 9, 3, 10, 0, 0))

    def _fake_get_price(**kwargs):
        captured.update(kwargs)
        return pd.DataFrame({"close": [1.0]})

    monkeypatch.setattr(data_api, "_current_context", context, raising=False)
    monkeypatch.setattr(data_api, "get_price", _fake_get_price, raising=False)

    data_api.attribute_history("000001.XSHE", 5, "1d", ["close"], skip_paused=True)

    assert captured["frequency"] == "daily"
    assert captured["end_date"] == context.current_dt - timedelta(days=1)


@pytest.mark.unit
def test_attribute_history_minute_includes_current_minute(monkeypatch):
    captured = {}
    context = SimpleNamespace(current_dt=datetime(2025, 9, 3, 10, 0, 0))

    def _fake_get_price(**kwargs):
        captured.update(kwargs)
        return pd.DataFrame({"close": [1.0]})

    monkeypatch.setattr(data_api, "_current_context", context, raising=False)
    monkeypatch.setattr(data_api, "get_price", _fake_get_price, raising=False)

    data_api.attribute_history("000001.XSHE", 5, "1m", ["close"], skip_paused=True)

    assert captured["frequency"] == "minute"
    assert captured["end_date"] == context.current_dt + timedelta(minutes=1)
