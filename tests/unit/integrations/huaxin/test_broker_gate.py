"""验证第一阶段 HuaxinBroker 的启动与写操作硬门禁。"""

import pytest

from bullet_trade.broker.registry import create_broker, list_brokers
from bullet_trade.integrations.huaxin.broker import HuaxinBroker
from bullet_trade.integrations.huaxin.errors import (
    HUAXIN_CANCEL_DISABLED,
    HUAXIN_NATIVE_UNAVAILABLE,
    HUAXIN_TRADING_DISABLED,
    HuaxinNativeUnavailableError,
    HuaxinTradingDisabledError,
)


def test_huaxin_registry_construction_is_lazy_and_disconnected() -> None:
    """验证注册表构造华鑫对象时不执行 native preflight 或连接。"""

    broker = create_broker("huaxin", {"huaxin": {}})

    assert "huaxin" in list_brokers()
    assert isinstance(broker, HuaxinBroker)
    assert broker.is_connected() is False
    assert broker.doctor_report is None


def test_huaxin_preflight_without_bundle_is_stable_unavailable() -> None:
    """验证缺少 bundle 时在连接前返回稳定 native unavailable。"""

    broker = HuaxinBroker("test-placeholder")

    with pytest.raises(HuaxinNativeUnavailableError) as exc_info:
        broker.preflight()

    assert exc_info.value.code == HUAXIN_NATIVE_UNAVAILABLE
    assert exc_info.value.details["reason_code"] == "BRIDGE_BUNDLE_MISSING"
    assert broker.is_connected() is False


@pytest.mark.asyncio
async def test_huaxin_buy_and_cancel_are_disabled_before_native() -> None:
    """验证买入和撤单在默认门禁处失败且不会进入 native。"""

    broker = HuaxinBroker("test-placeholder")

    with pytest.raises(HuaxinTradingDisabledError) as buy_error:
        await broker.buy("000001.XSHE", 100, 10.0)
    with pytest.raises(HuaxinTradingDisabledError) as cancel_error:
        await broker.cancel_order("local-order")

    assert buy_error.value.code == HUAXIN_TRADING_DISABLED
    assert cancel_error.value.code == HUAXIN_CANCEL_DISABLED
