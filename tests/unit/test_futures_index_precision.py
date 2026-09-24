"""合成期货指数保留来源精度，混合证券结果继续使用各自的价格归档规则。"""

import pandas as pd
import pytest

from bullet_trade.data.providers.jqdata import JQDataProvider


@pytest.fixture
def provider(monkeypatch):
    instance = JQDataProvider.__new__(JQDataProvider)
    resolved = []

    def decimals(code):
        resolved.append(code)
        return 3 if code == "510300.XSHG" else 2

    monkeypatch.setattr(instance, "_resolve_price_decimals", decimals)
    return instance, resolved


@pytest.mark.parametrize(
    "code",
    [
        "IF8888.CCFX", "RB8888.XSGE", "LH8888.XDCE", "SR8888.XZCE",
        "SC8888.XINE", "SI8888.GFEX", "lh8888.xdce",
    ],
)
def test_single_futures_index_preserves_source_prices(provider, code):
    instance, resolved = provider
    raw = pd.DataFrame(
        {
            "open": [100.12345678, 99.87654321],
            "close": [100.34567891, 99.65432198],
            "high_limit": [110.13579246, 109.86420864],
            "volume": [12.345678, 23.456789],
            "money": [1234.567891, 2345.678912],
        },
        index=pd.date_range("2024-01-02", periods=2),
    )
    before = raw.copy(deep=True)

    result = instance._round_price_result(raw, code)

    pd.testing.assert_frame_equal(result, before, check_exact=True)
    pd.testing.assert_frame_equal(raw, before, check_exact=True)
    assert resolved == []


def test_code_column_preserves_only_futures_index_rows(provider):
    instance, resolved = provider
    codes = ["IF8888.CCFX", "600000.XSHG", "510300.XSHG", "688888.XSHG", "IF2406.CCFX"]
    raw = pd.DataFrame(
        {
            "code": codes,
            "close": [100.12345678, 10.12345678, 3.12345678, 20.12345678, 3500.12345678],
            "avg": [100.87654321, 10.87654321, 3.87654321, 20.87654321, 3500.87654321],
            "volume": [11.12345, 22.12345, 33.12345, 44.12345, 55.12345],
        },
    )
    before = raw.copy(deep=True)
    expected = raw.copy(deep=True)
    expected["close"] = [100.12345678, 10.12, 3.123, 20.12, 3500.12]
    expected["avg"] = [100.87654321, 10.88, 3.877, 20.88, 3500.88]

    result = instance._round_price_result(raw, codes)

    pd.testing.assert_frame_equal(result, expected, check_exact=True)
    pd.testing.assert_frame_equal(raw, before, check_exact=True)
    assert resolved == codes[1:]


def test_multiindex_columns_preserve_index_and_round_other_securities(provider):
    instance, resolved = provider
    codes = ["IF8888.CCFX", "600000.XSHG", "510300.XSHG"]
    raw = pd.DataFrame(
        {
            ("close", codes[0]): [100.12345678],
            ("close", codes[1]): [10.12345678],
            ("close", codes[2]): [3.12345678],
            ("high", codes[0]): [101.98765432],
            ("high", codes[1]): [11.98765432],
            ("high", codes[2]): [4.98765432],
            ("money", codes[1]): [1234.567891],
        },
        index=pd.date_range("2024-01-02", periods=1),
    )
    before = raw.copy(deep=True)
    expected = raw.copy(deep=True)
    expected[("close", codes[1])] = [10.12]
    expected[("close", codes[2])] = [3.123]
    expected[("high", codes[1])] = [11.99]
    expected[("high", codes[2])] = [4.988]

    result = instance._round_price_result(raw, codes)

    pd.testing.assert_frame_equal(result, expected, check_exact=True)
    pd.testing.assert_frame_equal(raw, before, check_exact=True)
    assert resolved == codes[1:]


@pytest.mark.parametrize("code", ["600000.XSHG", "688888.XSHG", "IF2406.CCFX", "IF9999.CCFX"])
def test_single_non_index_keeps_existing_rounding(provider, code):
    instance, resolved = provider
    raw = pd.DataFrame({"close": [12.345678], "volume": [123.456789]})

    result = instance._round_price_result(raw, code)

    pd.testing.assert_frame_equal(
        result,
        pd.DataFrame({"close": [12.35], "volume": [123.456789]}),
        check_exact=True,
    )
    assert resolved == [code]
