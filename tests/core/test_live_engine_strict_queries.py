"""LiveEngine 严格柜台查询合同测试。

作者: BruceLee
职责: 验证默认查询保持兼容降级，而资金操作可显式传播券商异常。
输入: 抛出连接错误的最小 broker 替身。
输出: 空快照兼容行为与 strict 异常传播断言。
上下游: 策略 get_orders/get_trades 到 LiveEngine 券商同步边界。
"""

from pathlib import Path

import pytest

from bullet_trade.core.live_engine import LiveEngine


class _FailingQueryBroker:
    """在订单和成交查询上抛出稳定连接错误的券商替身。"""

    def get_orders(self, **_kwargs):
        """模拟订单查询连接失败。

        Returns:
            无。

        Raises:
            ConnectionError: 每次调用均抛出。
        """
        raise ConnectionError("order-query-down")

    def get_trades(self):
        """模拟成交查询连接失败。

        Returns:
            无。

        Raises:
            ConnectionError: 每次调用均抛出。
        """
        raise ConnectionError("trade-query-down")


def _engine(tmp_path: Path) -> LiveEngine:
    """构造只用于查询合同的 LiveEngine。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        LiveEngine: 已注入失败券商的引擎。
    """
    strategy = tmp_path / "strategy.py"
    strategy.write_text("def initialize(context):\n    pass\n", encoding="utf-8")
    engine = LiveEngine(strategy_file=strategy)
    engine.broker = _FailingQueryBroker()
    return engine


def test_default_broker_queries_keep_empty_compatibility(tmp_path: Path) -> None:
    """验证旧策略默认仍把查询异常兼容为空快照。"""
    engine = _engine(tmp_path)
    assert engine.get_orders(from_broker=True) == {}
    assert engine.get_trades() == {}


def test_strict_broker_queries_propagate_transport_failure(tmp_path: Path) -> None:
    """验证资金操作启用 strict 后能区分连接失败与真实空结果。"""
    engine = _engine(tmp_path)
    with pytest.raises(ConnectionError, match="order-query-down"):
        engine.get_orders(from_broker=True, strict=True)
    with pytest.raises(ConnectionError, match="trade-query-down"):
        engine.get_trades(strict=True)


def test_external_broker_order_preserves_root_idempotency_identity(tmp_path: Path) -> None:
    """验证外部柜台快照根幂等键会进入策略可见的 Order.extra。"""

    engine = _engine(tmp_path)
    order = engine._build_broker_order_view(
        {
            "security": "511880.XSHG",
            "amount": 100,
            "filled": 0,
            "price": 100.701,
            "status": "open",
            "side": "SELL",
            "idempotency_key": "huaxin-dg14-20260817-o1",
        },
        "broker-order-1",
        None,
    )

    assert order is not None
    assert order.extra["idempotency_key"] == "huaxin-dg14-20260817-o1"
