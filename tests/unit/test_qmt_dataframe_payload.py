import json
from datetime import datetime

import pandas as pd
import pytest

from bullet_trade.data.providers.remote_qmt import _dataframe_from_payload
from bullet_trade.server.adapters.qmt import dataframe_to_payload


@pytest.mark.unit
def test_dataframe_to_payload_handles_datetime():
    df = pd.DataFrame(
        {
            "start_date": [pd.Timestamp("2025-01-01")],
            "end_date": [pd.NaT],
            "value": [1],
        }
    )
    payload = dataframe_to_payload(df)
    encoded = json.dumps(payload)
    assert "2025-01-01" in encoded


@pytest.mark.unit
def test_dataframe_to_payload_preserves_price_multiindex_metadata():
    df = pd.DataFrame(
        [[5.4, 5.5, 4107.5]],
        columns=pd.MultiIndex.from_tuples(
            [
                ("600635.XSHG", "open"),
                ("600635.XSHG", "close"),
                ("000001.XSHG", "close"),
            ]
        ),
    )

    payload = dataframe_to_payload(df)

    assert payload["column_tuples"] == [
        ["open", "600635.XSHG"],
        ["close", "600635.XSHG"],
        ["close", "000001.XSHG"],
    ]
    assert payload["column_index_names"] == ["field", "code"]
    assert payload["records"] == [[5.4, 5.5, 4107.5]]


@pytest.mark.unit
def test_dataframe_payload_round_trip_preserves_unnamed_datetime_index():
    """验证未命名交易日索引经 QMT wire 往返后不会丢失。

    Returns:
        None: pytest 断言失败时报告生产完成日线索引回归。
    """

    source = pd.DataFrame(
        {"close": [3.81, 3.82]},
        index=pd.DatetimeIndex(["2026-08-21", "2026-08-24"]),
    )

    payload = dataframe_to_payload(source)
    restored = _dataframe_from_payload(payload)

    assert payload["columns"] == ["index", "close"]
    assert payload["index_columns"] == ["index"]
    assert payload["index_names"] == [None]
    assert payload["index_type"] == "DatetimeIndex"
    assert payload["records"] == [
        ["2026-08-21T00:00:00", 3.81],
        ["2026-08-24T00:00:00", 3.82],
    ]
    pd.testing.assert_frame_equal(restored, source)


@pytest.mark.unit
def test_dataframe_payload_omits_default_range_index():
    """验证默认行号不进入 wire，也不会伪装成交易时间列。

    Returns:
        None: pytest 断言失败时报告默认 RangeIndex wire 回归。
    """

    source = pd.DataFrame({"close": [3.81, 3.82]})

    payload = dataframe_to_payload(source)
    restored = _dataframe_from_payload(payload)

    assert payload["columns"] == ["close"]
    assert payload["records"] == [[3.81], [3.82]]
    assert "index_columns" not in payload
    assert isinstance(restored.index, pd.RangeIndex)
    pd.testing.assert_frame_equal(restored, source)


@pytest.mark.unit
def test_dataframe_payload_named_range_index_stays_range_not_datetime():
    """验证带名称的 RangeIndex 可逆，但绝不会按名称 date 转成时间。

    Returns:
        None: pytest 断言失败时报告序号索引伪时间回归。
    """

    source = pd.DataFrame(
        {"close": [3.81, 3.82]},
        index=pd.RangeIndex(start=5, stop=7, name="date"),
    )

    payload = dataframe_to_payload(source)
    restored = _dataframe_from_payload(payload)

    assert payload["index_type"] == "RangeIndex"
    assert payload["index_range"] == {"start": 5, "stop": 7, "step": 1}
    assert isinstance(restored.index, pd.RangeIndex)
    assert restored.index.equals(source.index)
    pd.testing.assert_frame_equal(restored, source)


@pytest.mark.unit
def test_dataframe_payload_round_trip_preserves_datetime_and_multiindex_columns():
    """验证显式行索引与行情 MultiIndex 列可以同时无损恢复。

    Returns:
        None: pytest 断言失败时报告索引列与多级数据列冲突。
    """

    source = pd.DataFrame(
        [[5.4, 5.5], [5.6, 5.7]],
        index=pd.DatetimeIndex(["2026-08-21", "2026-08-24"]),
        columns=pd.MultiIndex.from_tuples(
            [("600635.XSHG", "open"), ("600635.XSHG", "close")]
        ),
    )

    payload = dataframe_to_payload(source)
    restored = _dataframe_from_payload(payload)

    assert payload["columns"][0] == "index"
    assert payload["column_tuples"] == [
        ["open", "600635.XSHG"],
        ["close", "600635.XSHG"],
    ]
    assert isinstance(restored.index, pd.DatetimeIndex)
    assert isinstance(restored.columns, pd.MultiIndex)
    assert restored.columns.names == ["field", "code"]
    expected = source.copy()
    expected.columns = expected.columns.swaplevel(0, 1)
    expected.columns.names = ["field", "code"]
    pd.testing.assert_frame_equal(restored, expected)
