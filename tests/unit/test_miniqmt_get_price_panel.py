"""miniqmt.get_price(panel=False) 应符合聚宽的多证券长表契约。"""
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from bullet_trade.data.providers.miniqmt import MiniQMTProvider


def _single_security_frame():
    df = pd.DataFrame(
        {"close": [1.0, 2.0], "volume": [100.0, 200.0]},
        index=pd.DatetimeIndex(["2026-11-05", "2026-11-06"]),
    )
    df.index.name = None
    return df


def _make_provider():
    provider = MiniQMTProvider({"cache_dir": None, "auto_download": False})
    provider._cache = SimpleNamespace(cached_call=lambda _kind, kwargs, fetch, **_extra: fetch(kwargs))
    return provider


def test_panel_false_multi_security_returns_time_code_long_frame():
    provider = _make_provider()
    with patch.object(MiniQMTProvider, "_get_price_single", return_value=_single_security_frame()):
        df = provider.get_price(
            ["510300.XSHG", "159915.XSHE"],
            end_date="2026-11-06",
            count=2,
            fields=["close", "volume"],
            panel=False,
        )

    assert "time" in df.columns
    assert "code" in df.columns
    assert set(df["code"]) == {"510300.XSHG", "159915.XSHE"}
    assert len(df) == 4
    assert df.pivot(index="time", columns="code", values="close").shape == (2, 2)


def test_panel_false_single_security_keeps_datetime_index():
    provider = _make_provider()
    with patch.object(MiniQMTProvider, "_get_price_single", return_value=_single_security_frame()):
        df = provider.get_price(
            "510300.XSHG",
            end_date="2026-11-06",
            count=2,
            fields=["close", "volume"],
            panel=False,
        )

    assert isinstance(df.index, pd.DatetimeIndex)
    assert "code" not in df.columns
    assert "time" not in df.columns
