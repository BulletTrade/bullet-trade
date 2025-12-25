"""
Tushare 数据源基础价格接口测试。
"""

from datetime import date as Date

import pytest

from bullet_trade.utils.env_loader import get_env


@pytest.mark.requires_network
def test_tushare_get_price_supports_jq_code():
    if not get_env("TUSHARE_TOKEN"):
        pytest.skip("缺少 TUSHARE_TOKEN")
    try:
        import tushare  # noqa: F401
    except ImportError:
        pytest.skip("未安装 tushare")

    from bullet_trade.data import api as data_api
    from bullet_trade.data.api import set_data_provider, get_price

    original_provider = data_api._provider
    original_auth_attempted = data_api._auth_attempted
    original_cache = data_api._security_info_cache

    try:
        set_data_provider("tushare")
        df = get_price(
            "600000.XSHG",
            start_date="2024-01-02",
            end_date="2024-01-10",
            frequency="1d",
            fields=["open", "high", "low", "close", "volume", "money"],
            fq="none",
        )
        assert not df.empty, "Tushare 返回空数据，请检查权限或代码转换"
        assert df.index.min().date() >= Date(2024, 1, 2)
        assert df.index.max().date() <= Date(2024, 1, 10)
        for col in ("open", "high", "low", "close", "volume", "money"):
            assert col in df.columns
    finally:
        data_api._provider = original_provider
        data_api._auth_attempted = original_auth_attempted
        data_api._security_info_cache = original_cache
