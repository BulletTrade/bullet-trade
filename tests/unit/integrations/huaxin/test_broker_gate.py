"""验证第一阶段 HuaxinBroker 的启动与写操作硬门禁。"""

from pathlib import Path
from types import SimpleNamespace

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


def test_huaxin_preflight_explicitly_loads_real_bundle(monkeypatch) -> None:
    """验证真实连接前 doctor 必须显式 dlopen，不能停在静态检查。"""

    calls = []
    report = SimpleNamespace(
        native_ready=True,
        offline_bridge_ready=False,
        reason_code="OK",
    )

    def _doctor(*, bundle_path, load):
        """记录 preflight doctor 参数并返回可用报告。

        Args:
            bundle_path: 测试 bundle 路径。
            load: 是否显式加载动态库。

        Returns:
            SimpleNamespace: 可用 doctor 报告。
        """

        calls.append((bundle_path, load))
        return report

    monkeypatch.setattr("bullet_trade.integrations.huaxin.broker.doctor", _doctor)
    broker = HuaxinBroker("acct", config={"bundle_path": "/secure/bundle"})

    broker.preflight()

    assert calls == [(Path("/secure/bundle"), True)]
    assert broker.doctor_report is report


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


def test_huaxin_trading_enabled_gate() -> None:
    """验证纯内存模式下交易硬门禁仅由 enable_trading 控制。"""
    disabled_broker = HuaxinBroker("acct", config={"enable_trading": False})
    with pytest.raises(HuaxinTradingDisabledError) as exc_info:
        disabled_broker._require_trading_enabled()
    assert exc_info.value.code == HUAXIN_TRADING_DISABLED

    enabled_broker = HuaxinBroker("acct", config={"enable_trading": True})
    # 开启交易后不再抛出异常
    enabled_broker._require_trading_enabled()
