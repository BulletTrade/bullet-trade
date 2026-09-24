"""
作者: BruceLee
文件职责:
    验证 order()/order_target()/order_target_value() 的方向与开平参数：
    默认调用完全复现原有股票语义，新增的 side/pindex/close_today 只扩展期货下单表达能力。

主要输入:
    bullet_trade.core.orders.order 的公开调用与返回的 Order 对象。

主要输出:
    pytest 断言结果，确认开平派生、方向归一化、非法取值拒绝与位置参数兼容。

上下游关系:
    上游是策略公开下单 API；下游是回测引擎的期货撮合分派与账本记账。

关键环境或配置约定:
    测试只在内存订单队列上运行，不访问行情、券商或外部数据源；
    每个用例前后清空全局队列，避免用例间串扰。
"""

from __future__ import annotations

import pytest

from bullet_trade.core.orders import (
    LimitOrderStyle,
    clear_order_queue,
    order,
    order_target,
    order_target_value,
)

LH = "LH2109.XDCE"
STOCK = "002714.XSHE"


@pytest.fixture(autouse=True)
def _clean_order_queue():
    """在每个用例前后清空全局订单队列。

    Returns:
        Iterator[None]: 用例执行期间不做任何事，退出时清空队列。
    """

    clear_order_queue()
    yield
    clear_order_queue()


def test_default_call_reproduces_stock_semantics() -> None:
    """默认参数下买入仍是 open/long，不改变既有股票行为。

    Args:
        无。

    Returns:
        None。
    """

    created = order(STOCK, 100)

    assert created is not None
    assert created.amount == 100
    assert created.is_buy is True
    assert created.action == "open"
    assert created.side == "long"
    assert created.pindex == 0
    assert created.close_today is False


def test_negative_amount_derives_close_action() -> None:
    """负数量派生平仓动作与卖出方向。

    Args:
        无。

    Returns:
        None。
    """

    created = order(STOCK, -300)

    assert created is not None
    assert created.amount == 300
    assert created.is_buy is False
    assert created.action == "close"


def test_short_side_is_normalized_and_kept() -> None:
    """期货开空按 side 记录方向，数量仍取绝对值。

    Args:
        无。

    Returns:
        None。
    """

    created = order(LH, 2, side="SHORT")

    assert created is not None
    assert created.side == "short"
    assert created.action == "open"
    assert created.amount == 2


def test_close_short_carries_side_and_priority() -> None:
    """平空订单同时保留方向、平仓优先级与平今标记。

    Args:
        无。

    Returns:
        None。
    """

    created = order(LH, -2, side="short", pindex=1, close_today=True)

    assert created is not None
    assert created.action == "close"
    assert created.side == "short"
    assert created.pindex == 1
    assert created.close_today is True


@pytest.mark.parametrize("side", ["buy", "sell", "", None])
def test_invalid_side_rejected(side: object) -> None:
    """方向取值非法时显式失败，不留半语义订单。

    Args:
        side: 非法方向输入。

    Returns:
        None。
    """

    with pytest.raises(ValueError):
        order(LH, 1, side=side)


@pytest.mark.parametrize("pindex", [2, -1])
def test_invalid_pindex_rejected(pindex: int) -> None:
    """平仓优先级只接受 0 或 1。

    Args:
        pindex: 非法优先级输入。

    Returns:
        None。
    """

    with pytest.raises(ValueError):
        order(LH, -1, pindex=pindex)


def test_positional_price_still_maps_to_limit_style() -> None:
    """第三个位置参数仍是委托价，未被新参数挤位。

    Args:
        无。

    Returns:
        None。
    """

    created = order(LH, 1, 27000.0)

    assert created is not None
    assert created.price == pytest.approx(27000.0)
    assert isinstance(created.style, LimitOrderStyle)
    assert created.side == "long"


def test_order_target_default_call_reproduces_stock_semantics() -> None:
    """目标手数默认仍是 long，既有股票调仓行为不变。

    Args:
        无。

    Returns:
        None。
    """

    created = order_target(STOCK, 100)

    assert created is not None
    assert created.side == "long"
    assert created.pindex == 0
    assert created.close_today is False
    assert created._is_target_amount is True
    assert created._target_amount == 100


def test_order_target_keeps_short_side_for_close() -> None:
    """期货清仓必须能表达方向，否则目标 0 手无法区分平多还是平空。

    Args:
        无。

    Returns:
        None。
    """

    created = order_target(LH, 0, side="SHORT", pindex=1, close_today=True)

    assert created is not None
    assert created.side == "short"
    assert created.pindex == 1
    assert created.close_today is True
    assert created._is_target_amount is True
    assert created._target_amount == 0


def test_order_target_value_keeps_side_and_budget() -> None:
    """目标价值订单保留方向与预算，手数换算留给引擎按保证金口径处理。

    Args:
        无。

    Returns:
        None。
    """

    created = order_target_value(LH, 200_000.0, side="short")

    assert created is not None
    assert created.side == "short"
    assert created._is_target_value is True
    assert created._target_value == pytest.approx(200_000.0)
    assert created.amount == 0


def test_target_orders_reject_invalid_side() -> None:
    """目标单的方向取值非法时显式失败，与 order() 一致。

    Args:
        无。

    Returns:
        None。
    """

    with pytest.raises(ValueError):
        order_target(LH, 1, side="buy")
    with pytest.raises(ValueError):
        order_target_value(LH, 1000.0, side="")
    with pytest.raises(ValueError):
        order_target(LH, 1, pindex=2)


def test_target_orders_positional_price_still_maps_to_limit_style() -> None:
    """第三个位置参数仍是委托价，未被新参数挤位。

    Args:
        无。

    Returns:
        None。
    """

    by_amount = order_target(LH, 1, 27000.0)
    by_value = order_target_value(LH, 200_000.0, 27000.0)

    assert by_amount is not None and by_value is not None
    assert isinstance(by_amount.style, LimitOrderStyle)
    assert isinstance(by_value.style, LimitOrderStyle)
    assert by_amount.price == pytest.approx(27000.0)
    assert by_value.price == pytest.approx(27000.0)
    assert by_amount.side == "long"
    assert by_value.side == "long"
