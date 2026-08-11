"""验证券商注册表的兼容性和失效关闭行为。"""

from typing import Any, Dict, Mapping

import pytest

from bullet_trade.broker.base import BrokerBase
from bullet_trade.broker.registry import BrokerRegistry, create_broker, list_brokers
from bullet_trade.broker.simulator import SimulatorBroker


class _RegistryBroker(BrokerBase):
    """用于验证注册表构造合同的最小券商实现。"""

    def connect(self) -> bool:
        """标记测试券商已连接并返回成功。

        Returns:
            始终为 True。
        """

        self._connected = True
        return True

    def disconnect(self) -> bool:
        """标记测试券商已断开并返回成功。

        Returns:
            始终为 True。
        """

        self._connected = False
        return True

    def get_account_info(self) -> Dict[str, Any]:
        """返回空账户快照。

        Returns:
            仅包含账户标识的字典。
        """

        return {"account_id": self.account_id}

    def get_positions(self):
        """返回空持仓列表。

        Returns:
            空列表。
        """

        return []

    async def buy(self, security, amount, price=None, wait_timeout=None, remark=None, **kwargs):
        """返回固定测试买单标识。

        Args:
            security: 标的代码。
            amount: 委托数量。
            price: 可选价格。
            wait_timeout: 可选等待时间。
            remark: 可选备注。
            **kwargs: 其余订单参数。

        Returns:
            固定测试标识。
        """

        return "buy-test"

    async def sell(self, security, amount, price=None, wait_timeout=None, remark=None, **kwargs):
        """返回固定测试卖单标识。

        Args:
            security: 标的代码。
            amount: 委托数量。
            price: 可选价格。
            wait_timeout: 可选等待时间。
            remark: 可选备注。
            **kwargs: 其余订单参数。

        Returns:
            固定测试标识。
        """

        return "sell-test"

    async def cancel_order(self, order_id):
        """返回固定撤单结果。

        Args:
            order_id: 测试订单标识。

        Returns:
            始终为 True。
        """

        return True

    async def get_order_status(self, order_id):
        """返回测试订单状态。

        Args:
            order_id: 测试订单标识。

        Returns:
            包含订单标识的字典。
        """

        return {"order_id": order_id}


def _build_registry_broker(config: Mapping[str, Any]) -> BrokerBase:
    """从测试配置创建最小券商。

    Args:
        config: 含 ``account_id`` 的测试配置。

    Returns:
        新建的 ``_RegistryBroker``。
    """

    return _RegistryBroker(str(config.get("account_id") or "test"))


def test_registry_registers_name_and_alias_without_connecting() -> None:
    """验证注册和构造不会隐式连接券商。"""

    registry = BrokerRegistry()
    registry.register("custom", _build_registry_broker, aliases=("custom-alias",))

    broker = registry.create("CUSTOM-ALIAS", {"account_id": "demo"})

    assert broker.account_id == "demo"
    assert broker.is_connected() is False
    assert registry.names() == ("custom", "custom-alias")


def test_registry_rejects_conflicting_registration() -> None:
    """验证未显式允许覆盖时注册表拒绝名称冲突。"""

    registry = BrokerRegistry()
    registry.register("custom", _build_registry_broker)

    with pytest.raises(ValueError, match="已注册"):
        registry.register("custom", lambda config: _RegistryBroker("other"))


def test_registry_unknown_name_lists_available_choices() -> None:
    """验证未知券商错误包含确定性的可用名称。"""

    registry = BrokerRegistry()
    registry.register("custom", _build_registry_broker)

    with pytest.raises(ValueError, match="可用类型: custom"):
        registry.create("missing", {})


def test_process_registry_preserves_existing_simulator() -> None:
    """验证进程级注册表保持现有模拟器名称和默认配置语义。"""

    assert {"qmt", "qmt-remote", "simulator"}.issubset(set(list_brokers()))

    broker = create_broker("simulator", {"simulator": {"initial_cash": 123_456}})

    assert isinstance(broker, SimulatorBroker)
    assert broker.account_id == "simulator"
