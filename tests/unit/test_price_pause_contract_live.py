"""显式联网的跨数据源停牌契约验收。

作者：BruceLee
职责：用固定聚宽基准检验用户指定的数据源；输入现有 provider 环境及 --live-providers。
输出：按场景列出的 pytest 通过/失败；上游仅 get_price，下游共用固定停牌断言。
环境：必须显式选择 requires_network，使用现有账号配置；不下单、不改生产配置。
"""

import pytest
from tests.price_pause_contract import CASES, assert_pause_contract

from bullet_trade.data.api import _create_provider

pytestmark = [pytest.mark.integration, pytest.mark.requires_network]


@pytest.fixture(scope="session")
def live_price_provider(provider_name):
    """输入既有命令行源名，返回已认证Provider；缺配置直接失败，结束关闭本测试连接。"""
    provider = _create_provider(provider_name)
    try:
        provider.auth()
        yield provider
    finally:
        connection = getattr(provider, "_connection", None)
        if connection is not None:
            connection.close()


@pytest.mark.parametrize("case", CASES, ids=[case["id"] for case in CASES])
def test_live_get_price_pause_contract(live_price_provider, case):
    """输入显式配置的源和固定参数，调用行情并断言；无返回，不跳过不支持或缺覆盖结果。"""
    result = live_price_provider.get_price(**case["request"])
    assert_pause_contract(result, case)
