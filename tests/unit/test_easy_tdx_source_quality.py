"""作者：BruceLee。

职责：保护通达信传输修复与本地数据来源质量标记的共同兼容合同。
输入：内存SDK格式K线、请求字段；输出：来源补值及坏字段标记断言。
上下游：EasyTdxProvider归一化到Bullet Data发布适配器；不构造客户端或访问网络。
配置约定：数值补零仅为历史返回兼容，质量标记不得在筛列或日期裁剪中丢失。
"""

import pytest

from bullet_trade.data.providers.easy_tdx import EasyTdxProvider


@pytest.mark.unit
@pytest.mark.parametrize("mutation", ["missing_volume", "invalid_money", "invalid_time"])
def test_normalized_bars_keep_quality_flags_after_filtering(mutation):
    """输入三种坏源情形，返回None；断言归一化和日期裁剪保留质量标记，无外部副作用。"""
    rows = [
        {
            "date": "2026-10-19", "open": 10, "high": 11, "low": 9,
            "close": 10, "vol": 100, "amount": 1000,
        },
        {
            "date": "2026-10-20", "open": 10, "high": 11, "low": 9,
            "close": 10, "vol": 100, "amount": 1000,
        },
    ]
    if mutation == "missing_volume":
        for row in rows:
            del row["vol"]
        flag, field = "synthetic_fields", "volume"
    elif mutation == "invalid_money":
        rows[0]["amount"] = "invalid"
        flag, field = "invalid_source_fields", "money"
    else:
        rows[0]["date"] = "invalid"
        flag, field = "invalid_source_fields", "time"

    fields = ["open", "high", "low", "close", "volume", "money"]
    frame = EasyTdxProvider._normalize_kline_frame(rows, fields)
    assert not frame.empty
    assert field in frame.attrs[flag]
    filtered = EasyTdxProvider._filter_by_date(
        frame, start_date="2026-10-19", end_date="2026-10-20"
    )
    assert not filtered.empty
    assert field in filtered.attrs[flag]
