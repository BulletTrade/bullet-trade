"""使用合成成交核对公司行动、费用分配及不完整成本边界。"""

from copy import deepcopy
from datetime import datetime, timedelta

import pandas as pd
import pytest

from bullet_trade.core.analysis import _closed_trade_pnls, export_trades
from bullet_trade.core.equity_statistics import annotate_equity_trade_pnls
from bullet_trade.core.models import Trade


CODE = "000001.XSHE"
START = datetime(2024, 1, 2, 9, 30)


def trade(day, amount, price, commission=0.0, tax=0.0, **extra):
    return dict(security=CODE, amount=amount, price=price, commission=commission,
                tax=tax, time=START + timedelta(days=day), **extra)


def action(day, **extra):
    return dict(code=CODE, strategy_time=START + timedelta(days=day), **extra)


def test_partial_sales_allocate_cash_distributions_and_all_fees_after_split():
    # 总卖款1475 - 总买款1300 + 分派20 = 毛利195；全部费用7.75。
    trades = [trade(0, 100, 10, 2), trade(2, -25, 11, 1, .25),
              trade(3, 25, 12, .5), trade(5, -50, 6, 1), trade(6, -150, 6, 3)]
    events = [action(1, cash_in=20), action(4, old_amount=100, new_amount=200)]
    original = deepcopy(trades)

    annotate_equity_trade_pnls(trades, events)

    sells = [trades[i] for i in (1, 3, 4)]
    assert [t["realized_pnl_gross"] for t in sells] == pytest.approx([30, 41.25, 123.75])
    assert [t["realized_pnl_net"] for t in sells] == pytest.approx([28.25, 39.75, 119.25])
    assert [t["allocated_distributions"] for t in sells] == pytest.approx([5, 3.75, 11.25])
    assert sum(t["allocated_entry_fees"] for t in sells) == pytest.approx(2.5)
    assert sum(t["realized_pnl_net"] for t in sells) == pytest.approx(187.25)
    assert _closed_trade_pnls(trades) == pytest.approx([28.25, 39.75, 119.25])
    for before, after in zip(original, trades):
        assert all(after[key] == value for key, value in before.items())


def test_cash_is_attributed_before_same_time_sale_and_split_preserves_cost():
    trades = [trade(0, 100, 10, 5), trade(1, -10, 50, 1), trade(2, -30, 50, 3)]
    events = [action(1, old_amount=100, new_amount=40), action(1, cash_in=20)]
    annotate_equity_trade_pnls(trades, events)
    assert trades[1]["realized_pnl_gross"] == pytest.approx(255)
    assert trades[1]["realized_pnl_net"] == pytest.approx(252.75)
    assert trades[2]["realized_pnl_gross"] == pytest.approx(765)
    assert trades[2]["realized_pnl_net"] == pytest.approx(758.25)


def test_actual_net_cash_distribution_is_used_without_reapplying_tax():
    trades = [trade(0, 100, 10, 5), trade(2, -100, 10, 5)]
    annotate_equity_trade_pnls(trades, [action(1, cash_in=8, bonus_pre_tax=10, tax_rate_percent=20)])
    assert trades[1]["realized_pnl_gross"] == 8
    assert trades[1]["realized_pnl_net"] == -2


def test_full_close_then_reopen_does_not_reuse_distributions_or_fees():
    trades = [trade(0, 100, 10, 5), trade(2, -100, 11, 5),
              trade(3, 100, 10, 5), trade(4, -100, 10, 5)]
    annotate_equity_trade_pnls(trades, [action(1, cash_in=20)])
    assert _closed_trade_pnls(trades) == [110, -10]
    assert trades[3]["allocated_distributions"] == 0


def test_initial_holdings_do_not_borrow_cost_evidence_from_later_buys():
    trades = [trade(0, 100, 10, 2), trade(1, -50, 12, 1), trade(2, -150, 12, 3),
              trade(3, 100, 10, 2), trade(4, -100, 11, 2)]
    initial = [dict(security=CODE, amount=100, avg_cost=8)]
    annotate_equity_trade_pnls(trades, [], initial)
    for index in (1, 2):
        assert trades[index]["pnl_basis_status"] == "unknown_opening_basis"
        assert trades[index]["realized_pnl_gross"] is None
        assert trades[index]["realized_pnl_net"] is None
    assert trades[4]["realized_pnl_net"] == 96
    with pytest.warns(RuntimeWarning, match="证据不完整"):
        assert _closed_trade_pnls(trades) == [96]


def test_unrecorded_initial_quantity_cannot_be_assumed_flat_after_one_sale():
    trades = [trade(0, -50, 10), trade(1, 100, 10), trade(2, -50, 20)]
    annotate_equity_trade_pnls(trades, [])
    assert trades[0]["pnl_basis_status"] == "unknown_opening_basis"
    assert trades[2]["pnl_basis_status"] == "unknown_opening_basis"
    assert trades[2]["realized_pnl_net"] is None


def test_split_reveals_unknown_quantity_but_never_invents_opening_basis():
    trades = [trade(0, 100, 10), trade(2, -50, 10), trade(3, -100, 10),
              trade(4, 100, 10), trade(5, -100, 11)]
    annotate_equity_trade_pnls(trades, [action(1, old_amount=300, new_amount=150)])
    assert trades[1]["pnl_basis_status"] == "unknown_opening_basis"
    assert trades[2]["pnl_basis_status"] == "unknown_opening_basis"
    assert trades[4]["realized_pnl_net"] == 100


def test_distribution_with_no_opening_quantity_does_not_become_free_profit():
    trades = [trade(1, 100, 10), trade(2, -100, 10)]
    annotate_equity_trade_pnls(trades, [action(0, cash_in=20)])
    assert trades[1]["pnl_basis_status"] == "unknown_opening_basis"
    assert trades[1]["realized_pnl_net"] is None


def test_repeated_annotation_is_idempotent_and_refreshes_stale_fields():
    trades = [trade(0, 100, 10, 2), trade(2, -100, 11, 3), trade(3, 0, 0)]
    events = [action(1, cash_in=20)]
    annotate_equity_trade_pnls(trades, events)
    annotated = deepcopy(trades)
    annotate_equity_trade_pnls(trades, events)
    assert trades == annotated
    trades[1]["realized_pnl_net"] = 99999
    trades[2]["realized_pnl_net"] = 99999
    annotate_equity_trade_pnls(trades, [])
    assert trades[1]["realized_pnl_net"] == 95
    assert trades[2]["pnl_basis_status"] == "no_execution"
    assert trades[2]["realized_pnl_net"] is None


def test_trade_objects_and_event_date_fallback_use_same_evidence():
    trades = [Trade("buy", CODE, 100, 10, START, commission=2),
              Trade("sell", CODE, -100, 11, START+timedelta(days=2), commission=3)]
    events = [dict(code=CODE, strategy_time=None, event_date=START+timedelta(days=1), cash_in=5)]
    annotate_equity_trade_pnls((t for t in trades), (e for e in events))
    assert trades[1].realized_pnl_net == 100
    assert _closed_trade_pnls(trades) == [100]


def test_timeline_order_uses_times_but_preserves_same_time_execution_sequence():
    buy, sell = trade(0, 100, 10, 2), trade(0, -100, 11, 3)
    later = trade(1, -100, 11, 3)
    annotate_equity_trade_pnls([later, buy, sell], [])
    assert sell["realized_pnl_net"] == 95
    assert later["pnl_basis_status"] == "unknown_opening_basis"


def test_futures_annotations_and_explicit_short_pnl_are_unchanged():
    trades = [dict(security="IF2406.CCFX", amount=-1, price=100, action="open",
                   side="short", multiplier=300, commission=2, time=START),
              dict(security="IF2406.CCFX", amount=1, price=99, action="close",
                   side="short", multiplier=300, commission=3, time=START)]
    before = deepcopy(trades)
    annotate_equity_trade_pnls(trades, [dict(code="IF2406.CCFX", cash_in=999)])
    assert trades == before
    assert _closed_trade_pnls(trades) == [295]


@pytest.mark.parametrize("field", ["amount", "price", "commission", "tax"])
def test_nonfinite_equity_evidence_is_rejected(field):
    record = trade(0, 100, 10)
    record[field] = float("nan")
    with pytest.raises(ValueError, match="非有限"):
        annotate_equity_trade_pnls([record], [])


def test_incomplete_status_never_uses_a_stale_numeric_pnl():
    record = trade(0, -100, 10, pnl_basis_status="unknown_opening_basis", realized_pnl_net=1000)
    with pytest.warns(RuntimeWarning, match="证据不完整"):
        assert _closed_trade_pnls([record]) == []


def test_nan_status_from_legacy_tabular_record_uses_disclosed_legacy_path():
    records = [trade(0, 100, 10), trade(1, -100, 11, 2, pnl_basis_status=float("nan"))]
    with pytest.warns(RuntimeWarning, match="旧统计口径"):
        assert _closed_trade_pnls(records) == [98]


def test_mixed_new_open_and_legacy_close_cannot_create_zero_cost_profit():
    records = [trade(0, 100, 10, pnl_basis_status="open"), trade(1, -100, 11)]
    with pytest.warns(RuntimeWarning, match="混合新旧"):
        assert _closed_trade_pnls(records) == []


def test_export_preserves_raw_pnl_evidence_and_missing_initial_basis(tmp_path):
    records = [trade(0, 100, 10, .12345), trade(1, -100, 11, .23456),
               trade(2, -100, 11)]
    annotate_equity_trade_pnls(records, [])
    path = tmp_path / "trades.csv"
    export_trades({"trades": records}, str(path))
    exported = pd.read_csv(path)
    assert exported.loc[1, "realized_pnl_gross"] == 100
    assert exported.loc[1, "realized_pnl_net"] == pytest.approx(99.64199)
    assert exported.loc[1, "allocated_entry_fees"] == pytest.approx(.12345)
    assert exported.loc[2, "pnl_basis_status"] == "unknown_opening_basis"
    assert pd.isna(exported.loc[2, "realized_pnl_net"])


def test_engine_result_regeneration_keeps_trade_evidence_idempotent():
    from bullet_trade.core.engine import BacktestEngine

    engine = BacktestEngine(start_date="2024-01-02", end_date="2024-01-04", initial_cash=1000)
    engine.trades = [trade(0, 100, 10, 2), trade(2, -100, 11, 3)]
    engine.events = [action(1, cash_in=20)]
    engine.daily_records = [{"date": START+timedelta(days=2), "total_value": 1115}]
    first = deepcopy(engine._generate_results()["trades"])
    second = engine._generate_results()["trades"]
    assert second == first
    assert second[1]["realized_pnl_net"] == 115
