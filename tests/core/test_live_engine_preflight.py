"""验证 LiveEngine 在策略初始化和连接前执行券商 preflight。"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from bullet_trade.broker.base import BrokerBase
from bullet_trade.core.live_engine import LiveEngine


class _RejectingPreflightBroker(BrokerBase):
    """在 preflight 阶段拒绝启动并记录是否曾连接的测试券商。"""

    def __init__(self) -> None:
        """创建未连接的测试券商。

        Returns:
            None。
        """

        super().__init__("preflight-test")
        self.connect_called = False

    def preflight(self) -> None:
        """用固定错误拒绝启动。

        Returns:
            本函数不会正常返回。

        Raises:
            RuntimeError: 每次调用都抛出。
        """

        raise RuntimeError("preflight rejected")

    def connect(self) -> bool:
        """记录连接调用。

        Returns:
            始终为 True。
        """

        self.connect_called = True
        self._connected = True
        return True

    def disconnect(self) -> bool:
        """清除连接状态。

        Returns:
            始终为 True。
        """

        self._connected = False
        return True

    def get_account_info(self) -> Dict[str, Any]:
        """返回空测试账户。

        Returns:
            空字典。
        """

        return {}

    def get_positions(self) -> List[Dict[str, Any]]:
        """返回空测试持仓。

        Returns:
            空列表。
        """

        return []

    async def buy(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        wait_timeout: Optional[float] = None,
        remark: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """返回测试买单标识。

        Args:
            security: 标的代码。
            amount: 委托数量。
            price: 可选价格。
            wait_timeout: 可选超时。
            remark: 可选备注。
            **kwargs: 其余订单参数。

        Returns:
            固定标识。
        """

        return "buy"

    async def sell(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        wait_timeout: Optional[float] = None,
        remark: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """返回测试卖单标识。

        Args:
            security: 标的代码。
            amount: 委托数量。
            price: 可选价格。
            wait_timeout: 可选超时。
            remark: 可选备注。
            **kwargs: 其余订单参数。

        Returns:
            固定标识。
        """

        return "sell"

    async def cancel_order(self, order_id: str) -> bool:
        """返回测试撤单结果。

        Args:
            order_id: 测试订单标识。

        Returns:
            始终为 True。
        """

        return True

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """返回测试订单状态。

        Args:
            order_id: 测试订单标识。

        Returns:
            含订单标识的字典。
        """

        return {"order_id": order_id}


@pytest.mark.asyncio
async def test_preflight_rejection_happens_before_initialize_and_connect(tmp_path: Path) -> None:
    """验证 preflight 失败时 initialize 与 connect 均不可达。

    Args:
        tmp_path: pytest 提供的临时目录。
    """

    marker = tmp_path / "initialize-called"
    strategy = tmp_path / "strategy.py"
    strategy.write_text(
        "def initialize(context):\n"
        f"    open({str(marker)!r}, 'w', encoding='utf-8').write('called')\n",
        encoding="utf-8",
    )
    broker = _RejectingPreflightBroker()
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: broker,
        live_config={"runtime_dir": str(tmp_path / "runtime")},
    )

    with pytest.raises(RuntimeError, match="preflight rejected"):
        await engine._bootstrap()

    assert marker.exists() is False
    assert broker.connect_called is False
