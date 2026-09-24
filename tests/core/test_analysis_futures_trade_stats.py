"""
作者: BruceLee
文件职责:
    验证成交统计在期货上把价差换算成金额时乘上合约乘数，与账本口径一致；
    股票乘数为 1，统计结果逐字不变。

主要输入:
    bullet_trade.core.analysis 的成交胜率与盈亏比计算函数，配合内存构造的成交列表。

主要输出:
    pytest 断言结果，确认期货回合的盈亏分类与盈亏比按乘数放大后的金额判定，
    以及旧格式兼容与完整期货成交的方向、费用、乘数和导出精度。

上下游关系:
    上游是回测引擎产出的成交记录；下游是回测报告与摘要里的交易胜率、盈亏比。

关键环境或配置约定:
    测试只使用内存成交字典与内置合约规格表，不访问行情或任何外部数据源；
    所有期望值均可手工推导。
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from bullet_trade.core.models import Trade
from bullet_trade.core.analysis import (
    _closed_trade_pnls,
    export_trades,
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
    """期货取已知合约乘数，股票与空代码为 1。

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


def _futures_trade(action, side, price, *, quantity=1, commission=10.0,
                   multiplier=16.0, security=LH, time=None):
    """按实际买卖符号构造具有完整语义的成交。"""
    buy = (action == "open") == (side == "long")
    return dict(security=security, amount=quantity if buy else -quantity,
                price=price, commission=commission, action=action, side=side,
                multiplier=multiplier, time=time)


def test_short_open_is_not_a_realized_win_and_losing_close_is_a_loss():
    trades = [_futures_trade("open", "short", 100)]
    assert _closed_trade_pnls(trades) == []
    assert _compute_trade_win_stats(trades)["交易盈利次数"] == 0
    trades.append(_futures_trade("close", "short", 110))
    assert _closed_trade_pnls(trades) == [-180.0]
    assert _compute_trade_win_stats(trades) == {
        "交易胜率": 0.0, "交易盈利次数": 0, "交易亏损次数": 1,
    }
    assert _compute_trade_profit_loss_ratio(trades) == 0.0


def test_same_contract_long_and_short_costs_are_independent():
    trades = [_futures_trade("open", "long", 100), _futures_trade("open", "short", 100),
              _futures_trade("close", "long", 105), _futures_trade("close", "short", 110)]
    assert _closed_trade_pnls(trades) == [60.0, -180.0]
    assert _compute_trade_win_stats(trades)["交易胜率"] == 50.0
    assert _compute_trade_profit_loss_ratio(trades) == pytest.approx(1 / 3)


def test_open_fees_are_allocated_across_partial_closes_and_new_lots():
    trades = [
        _futures_trade("open", "long", 100, quantity=2, commission=20),
        _futures_trade("close", "long", 101, commission=2),
        _futures_trade("open", "long", 110, commission=6),
        _futures_trade("close", "long", 104, quantity=2, commission=4),
    ]
    # 第一次分摊10元开仓费；剩余两手均价105，待分摊开仓费16元。
    assert _closed_trade_pnls(trades) == [4.0, -52.0]
    assert _compute_trade_profit_loss_ratio(trades) == pytest.approx(4 / 52)


def test_open_commission_can_turn_positive_price_change_into_loss():
    trades = [_futures_trade("open", "long", 100), _futures_trade("close", "long", 101)]
    assert _closed_trade_pnls(trades) == [-4.0]
    assert _compute_trade_win_stats(trades)["交易亏损次数"] == 1


def test_saved_multiplier_overrides_spec_lookup_and_supports_unknown_product():
    trades = [
        _futures_trade("open", "long", 100, multiplier=100, security="ZZ2109.XDCE"),
        _futures_trade("close", "long", 101, multiplier=100, security="ZZ2109.XDCE"),
    ]
    assert _closed_trade_pnls(trades) == [80.0]
    assert _trade_multiplier(LH, 100) == 100.0


@pytest.mark.parametrize("field,value", [
    ("action", None), ("action", "sell"), ("side", None), ("side", "buy"),
    ("multiplier", None), ("multiplier", 0), ("multiplier", float("nan")),
    ("multiplier", float("inf")), ("amount", -1), ("price", float("nan")),
])
def test_incomplete_or_invalid_futures_metadata_fails(field, value):
    trade = _futures_trade("open", "long", 100)
    trade[field] = value
    with pytest.raises(ValueError):
        _closed_trade_pnls([trade])


def test_close_cannot_exceed_known_side_position():
    with pytest.raises(ValueError, match="超过已知同向"):
        _closed_trade_pnls([_futures_trade("open", "long", 100),
                            _futures_trade("close", "short", 100)])


def test_legacy_futures_warn_and_reject_unidentifiable_short_records():
    with pytest.warns(RuntimeWarning, match="纯多头"):
        assert _closed_trade_pnls([_trade(LH, 1, 100), _trade(LH, -1, 101)]) == [16.0]
    with pytest.warns(RuntimeWarning, match="纯多头"):
        with pytest.raises(ValueError, match="超过已知同向"):
            _closed_trade_pnls([_trade(LH, -1, 100), _trade(LH, 1, 110)])
    with pytest.raises(ValueError, match="缺少可解析乘数"):
        _trade_multiplier("ZZ2109.XDCE")


def test_stock_open_fees_keep_existing_statistics():
    trades = [_trade(STOCK, 1, 100, commission=10), _trade(STOCK, -1, 105, commission=1)]
    assert _closed_trade_pnls(trades) == [4.0]


def test_trade_defaults_preserve_old_positional_constructor():
    trade = Trade("order", STOCK, 100, 10, datetime(2021, 1, 4), 1, 0, "trade")
    assert (trade.action, trade.side, trade.multiplier) == (None, None, None)


def test_export_reload_preserves_futures_facts_and_fee_precision(tmp_path):
    first = Trade("open", LH, -1, 100, datetime(2021, 1, 4), 0.12345,
                  action="open", side="short", multiplier=16)
    second = Trade("close", LH, 1, 110, datetime(2021, 1, 5), 0.23456,
                   action="close", side="short", multiplier=16)
    path = tmp_path / "trades.csv"
    export_trades({"trades": [first, second]}, str(path))
    frame = pd.read_csv(path)
    assert list(frame["side"]) == ["short", "short"]
    assert list(frame["action"]) == ["open", "close"]
    assert list(frame["multiplier"]) == [16, 16]
    assert list(frame["手续费"]) == [0.12345, 0.23456]
    assert list(frame["金额"]) == [1600, 1760]
    loaded = frame.to_dict("records")
    assert _closed_trade_pnls(loaded) == pytest.approx([-160.35801])
    assert _closed_trade_pnls(loaded) == _closed_trade_pnls([first, second])


def test_stock_export_retains_columns_and_fee_rounding(tmp_path):
    path = tmp_path / "stock_trades.csv"
    export_trades({"trades": [_trade(STOCK, 1, 100, commission=0.12345)]}, str(path))
    frame = pd.read_csv(path)
    assert list(frame.columns[:9]) == ["时间", "标的", "数量", "价格", "金额", "手续费", "印花税", "总费用", "方向"]
    assert list(frame.columns[9:]) == ["commission", "tax", "cost"]
    assert frame.iloc[0]["手续费"] == 0.12
    assert frame.iloc[0]["commission"] == pytest.approx(0.12345)


def test_legacy_and_new_futures_can_share_export_without_nan_metadata_errors(tmp_path):
    other = "CU2109.XSGE"
    trades = [_trade(LH, 1, 100), _trade(LH, -1, 101),
              _futures_trade("open", "short", 100, security=other),
              _futures_trade("close", "short", 110, security=other)]
    path = tmp_path / "mixed.csv"
    export_trades({"trades": trades}, str(path))
    loaded = pd.read_csv(path).to_dict("records")
    with pytest.warns(RuntimeWarning, match="纯多头"):
        assert sorted(_closed_trade_pnls(loaded)) == [-180.0, 16.0]
    export_trades({"trades": loaded}, str(path))
    with pytest.warns(RuntimeWarning, match="纯多头"):
        assert sorted(_closed_trade_pnls(pd.read_csv(path).to_dict("records"))) == [-180.0, 16.0]
