"""
作者: BruceLee
文件职责:
    验证成交统计在期货上把价差换算成金额时乘上合约乘数，与账本口径一致；
    股票乘数为 1，统计结果逐字不变。

主要输入:
    bullet_trade.core.analysis 的成交胜率与盈亏比计算函数，配合内存构造的成交列表。

主要输出:
    pytest 断言结果，确认期货回合的盈亏分类与盈亏比按乘数放大后的金额判定，
    以及非期货标的与规格缺失时退化为乘数 1。

上下游关系:
    上游是回测引擎产出的成交记录；下游是回测报告与摘要里的交易胜率、盈亏比。

关键环境或配置约定:
    测试只使用内存成交字典与内置合约规格表，不访问行情或任何外部数据源；
    所有期望值均可手工推导。
"""

from __future__ import annotations

from bullet_trade.core.analysis import (
    _compute_trade_profit_loss_ratio,
    _compute_trade_win_stats,
    _trade_multiplier,
)

LH = "LH2109.XDCE"
STOCK = "000001.XSHE"


def _trade(security: str, amount: int, price: float, commission: float = 0.0,
           tax: float = 0.0, time: str = "2021-04-06 09:30:00") -> dict:
    """构造一笔成交记录。

    Args:
        security: 标的代码。
        amount: 数量，正数为买入、负数为卖出。
        price: 成交价。
        commission: 手续费。
        tax: 印花税。
        time: 成交时间，用于排序。

    Returns:
        dict: 成交字典。
    """

    return {
        "security": security,
        "amount": amount,
        "price": price,
        "commission": commission,
        "tax": tax,
        "time": time,
    }


def test_multiplier_resolves_only_for_futures() -> None:
    """期货取合约乘数，股票与未知代码退化为 1。

    Args:
        无。

    Returns:
        None。
    """

    assert _trade_multiplier(LH) == 16.0
    assert _trade_multiplier(STOCK) == 1.0
    assert _trade_multiplier("") == 1.0


def test_futures_round_trip_win_needs_multiplier() -> None:
    """一个 tick 的价差要乘上乘数才够付手续费，否则会被误判为亏损。

    生猪档位 5 元、乘数 16：开 1 手 27000、平 27005，毛利 5×16=80 元，
    平仓手续费 9.9 元，净利 +70.1 元是赢。
    漏乘时毛利只有 5 元，减掉 9.9 元手续费变成 -4.9 元，被记成亏损。

    Args:
        无。

    Returns:
        None。
    """

    trades = [
        _trade(LH, 1, 27000.0, commission=9.9, time="2021-04-06 09:30:00"),
        _trade(LH, -1, 27005.0, commission=9.9, time="2021-04-07 09:30:00"),
    ]

    stats = _compute_trade_win_stats(trades)

    assert stats["交易盈利次数"] == 1
    assert stats["交易亏损次数"] == 0
    assert stats["交易胜率"] == 100.0


def test_futures_profit_loss_ratio_scales_with_multiplier() -> None:
    """盈亏比按金额汇总，漏乘会让手续费在比率里占错权重。

    两笔回合：赢 (27005-27000)×16-9.9 = +70.1，
    输 (26000-26100)×16-9.9 = -1609.9。
    盈亏比 = 70.1 / 1609.9 ≈ 0.0435。

    Args:
        无。

    Returns:
        None。
    """

    trades = [
        _trade(LH, 1, 27000.0, commission=9.9, time="2021-04-06 09:30:00"),
        _trade(LH, -1, 27005.0, commission=9.9, time="2021-04-07 09:30:00"),
        _trade(LH, 1, 26100.0, commission=9.9, time="2021-04-08 09:30:00"),
        _trade(LH, -1, 26000.0, commission=9.9, time="2021-04-09 09:30:00"),
    ]

    ratio = _compute_trade_profit_loss_ratio(trades)

    assert abs(ratio - 70.1 / 1609.9) < 1e-9


def test_stock_trade_stats_unchanged() -> None:
    """股票乘数为 1，胜率与盈亏比与改动前逐字一致。

    开 100 股 10.0、平 10.5，手续费 5：净利 (10.5-10)×100-5 = +45。

    Args:
        无。

    Returns:
        None。
    """

    trades = [
        _trade(STOCK, 100, 10.0, commission=2.0, time="2021-04-06 09:30:00"),
        _trade(STOCK, -100, 10.5, commission=3.0, tax=0.0, time="2021-04-07 09:30:00"),
    ]

    stats = _compute_trade_win_stats(trades)

    assert stats["交易盈利次数"] == 1
    assert stats["交易亏损次数"] == 0
    assert _compute_trade_profit_loss_ratio(trades) == float("inf")
