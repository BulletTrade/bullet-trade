from __future__ import annotations

from copy import deepcopy

import pytest

from bullet_trade.integrations.ths.normalization import normalize_account, normalize_rows


# Synthetic fixtures only; no account export or broker identifiers.
DAY = "2025-01-06"
POSITION = {"证券代码": "110086", "证券名称": "示例转债", "股票余额": "300",
            "可用余额": "200", "冻结数量": "100", "成本价": "100.125",
            "市价": "101.250", "市值": "30375.000", "交易市场": "上海Ａ股"}
ORDER = {"委托时间": "09:35:00", "证券代码": "110086", "操作": "买入",
         "委托数量": "10", "成交数量": "10", "撤消数量": "0",
         "合同编号": "00000001", "委托价格": "99.999",
         "成交均价": "99.999", "备注": "全部成交", "交易市场": "上海Ａ股"}
TRADE = {"成交时间": "09:35:02", "证券代码": "110086", "操作": "买入",
         "成交数量": "10", "成交金额": "999.990", "成交均价": "99.999",
         "合同编号": "00000001", "成交编号": "00000003",
         "交易市场": "上海Ａ股"}


def test_account_and_position_preserve_exact_money_and_share_counts():
    account = normalize_account({"资金余额": "10000.01", "可用金额": "10000.02",
                                 "冻结金额": "0.00", "股票市值": "30375.000"})
    assert account == {"cash_balance": "10000.01", "available_cash": "10000.02",
                       "frozen_cash": "0.00", "market_value": "30375.000"}
    assert normalize_rows("positions", [POSITION], DAY) == [{
        "security": "110086.XSHG", "name": "示例转债", "amount": 300,
        "closeable_amount": 200, "frozen_amount": 100, "avg_cost": "100.125",
        "current_price": "101.250", "market_value": "30375.000"}]
    negative_cost = normalize_rows("positions", [{**POSITION, "成本价": "-0.001"}], DAY)[0]
    assert negative_cost["avg_cost"] == "-0.001"


def test_orders_and_trades_keep_original_contract_and_fill_ids():
    orders = normalize_rows("orders", [ORDER, {**ORDER, "合同编号": "00000002"}], DAY)
    assert [row["order_id"] for row in orders] == ["00000001", "00000002"]
    assert orders[0]["status"] == "filled"
    assert orders[0]["is_buy"] is True
    assert orders[0]["order_time"] == "2025-01-06T09:35:00+08:00"
    trades = normalize_rows("trades", [TRADE], DAY)
    assert trades[0]["trade_id"] == "00000003"
    assert trades[0]["order_id"] == "00000001"
    assert trades[0]["deal_balance"] == "999.990"
    assert trades[0]["time"] == "2025-01-06T09:35:02+08:00"
    assert normalize_rows("trades", [{**TRADE, "成交日期": "20250106"}], DAY)[0]["time"] == trades[0]["time"]


def test_unknown_day_never_dates_time_only_orders_or_cancelable_rows():
    for kind in ("orders", "cancelable"):
        converted = normalize_rows(kind, [ORDER], None)[0]
        assert converted["order_time_raw"] == "09:35:00"
        assert "order_time" not in converted
    assert "order_time" not in normalize_rows(
        "cancelable", [{k: v for k, v in ORDER.items() if k != "委托时间"}], None)[0]
    assert normalize_rows("positions", [POSITION], None) == normalize_rows(
        "positions", [POSITION], DAY)
    assert normalize_rows("holdings", [POSITION], None) == normalize_rows(
        "holdings", [POSITION], DAY)


def test_unknown_day_uses_only_explicit_order_or_trade_dates():
    order = normalize_rows("orders", [{**ORDER, "委托日期": "20250103"}], None)[0]
    assert order["order_time"] == "2025-01-03T09:35:00+08:00"
    assert "order_time_raw" not in order
    full_order = normalize_rows(
        "cancelable", [{**ORDER, "委托时间": "2025-01-03 09:35:00"}], None)[0]
    assert full_order["order_time"] == "2025-01-03T09:35:00+08:00"
    trade = normalize_rows("trades", [{**TRADE, "成交日期": "2025-01-03"}], None)[0]
    assert trade["time"] == "2025-01-03T09:35:02+08:00"
    full_trade = normalize_rows(
        "trades", [{**TRADE, "成交时间": "2025-01-03 09:35:02"}], None)[0]
    assert full_trade["time"] == "2025-01-03T09:35:02+08:00"


def test_unknown_day_rejects_undated_nonempty_trade_and_invalid_order_time():
    assert normalize_rows("trades", [], None) == []
    with pytest.raises(ValueError, match="missing_成交日期"):
        normalize_rows("trades", [TRADE], None)
    with pytest.raises(ValueError, match="invalid_or_mismatched_委托时间"):
        normalize_rows("orders", [{**ORDER, "委托时间": "25:35:00"}], None)
    with pytest.raises(ValueError, match="invalid_or_mismatched_委托时间"):
        normalize_rows("orders", [{**ORDER, "委托日期": "20250103",
                                    "委托时间": "2025-01-04 09:35:00"}], None)


def test_explicit_day_keeps_date_conflict_rejection():
    with pytest.raises(ValueError, match="mismatched_委托日期"):
        normalize_rows("orders", [{**ORDER, "委托日期": "20250103"}], DAY)
    with pytest.raises(ValueError, match="mismatched_成交日期"):
        normalize_rows("trades", [{**TRADE, "成交日期": "20250103"}], DAY)


@pytest.mark.parametrize("trade_time", ["09:35:02", "2025-01-06 09:35:02"])
def test_ten_column_trade_uses_only_verified_security_mapping(trade_time):
    row = {"成交时间": trade_time, "证券代码": "518880", "证券名称": "黄金ETF华安",
           "操作": "买入", "成交数量": "100", "成交均价": "8.501",
           "成交金额": "850.100", "合同编号": "00000001",
           "成交编号": "00000003", "委托时间": "09:35:00"}
    assert len(row) == 10
    original = deepcopy(row)
    converted = normalize_rows("trades", [row], DAY,
                               verified_security_map={"518880": "518880.XSHG"})
    assert converted[0]["security"] == "518880.XSHG"
    assert converted[0]["time"] == "2025-01-06T09:35:02+08:00"
    assert converted[0]["price"] == "8.501"
    assert row == original


def test_missing_market_requires_exact_verified_mapping():
    row = {key: value for key, value in TRADE.items() if key != "交易市场"}
    for mapping in (None, {}, {"518880": "518880.XSHG"}):
        with pytest.raises(ValueError, match="missing_or_invalid_交易市场"):
            normalize_rows("trades", [row], DAY, verified_security_map=mapping)
    assert normalize_rows("trades", [{**row, "交易市场": " "}], DAY,
                          verified_security_map={"110086": "110086.XSHG"})[0]["security"] == "110086.XSHG"


def test_observed_market_must_agree_with_verified_mapping():
    mapping = {"110086": "110086.XSHG"}
    assert normalize_rows("trades", [TRADE], DAY,
                          verified_security_map=mapping)[0]["security"] == "110086.XSHG"
    with pytest.raises(ValueError, match="conflicting_verified_security_map"):
        normalize_rows("trades", [TRADE], DAY,
                       verified_security_map={"110086": "110086.XSHE"})
    with pytest.raises(ValueError, match="unknown_交易市场"):
        normalize_rows("trades", [{**TRADE, "交易市场": "未知"}], DAY,
                       verified_security_map=mapping)


@pytest.mark.parametrize("mapping", [
    {"51888": "51888.XSHG"},
    {518880: "518880.XSHG"},
    {"518880": "518881.XSHG"},
    {"518880": "518880.SH"},
    {"518880": "518880.xshg"},
    {"518880": "518880.XSHG "},
    {"518880": None},
    [("518880", "518880.XSHG")],
])
def test_invalid_verified_mapping_is_rejected(mapping):
    with pytest.raises(ValueError, match="invalid_verified_security_map"):
        normalize_rows("trades", [], DAY, verified_security_map=mapping)


def test_cancelable_and_partial_status_are_conservative():
    cancelable = normalize_rows("cancelable", [{k: v for k, v in ORDER.items()
                                                 if k not in {"委托时间", "撤消数量"}}], DAY)[0]
    assert cancelable["cancelable"] is True
    assert cancelable["status"] == "unknown"
    assert "cancelled" not in cancelable
    partial = normalize_rows("orders", [{**ORDER, "成交数量": "3",
                                          "备注": "部分成交"}], DAY)[0]
    assert partial["status"] == "filling"
    unknown = normalize_rows("orders", [{**ORDER, "成交数量": "3",
                                          "备注": "已报"}], DAY)[0]
    assert unknown["status"] == "unknown"
    assert normalize_rows("orders", [{**ORDER, "成交数量": "0",
                                      "撤消数量": "10", "备注": "全部撤单"}], DAY)[0]["status"] == "canceled"
    assert normalize_rows("orders", [{**ORDER, "成交数量": "3",
                                      "撤消数量": "7", "备注": "全部撤单"}], DAY)[0]["status"] == "partly_canceled"
    assert normalize_rows("orders", [{**ORDER, "成交数量": "3",
                                      "撤消数量": "7", "备注": "部分成交"}], DAY)[0]["status"] == "unknown"


def test_unfilled_order_is_open_only_with_consistent_zero_quantities():
    pending = {**ORDER, "操作": "卖出", "成交数量": "0", "撤消数量": "0",
               "备注": "未成交"}
    order = normalize_rows("orders", [pending], DAY)[0]
    assert order["status"] == "open"
    assert order["raw_status"] == "未成交"
    assert order["filled"] == 0
    assert order["cancelled"] == 0
    assert order["cancelable"] is False

    for row in (
        {**pending, "成交数量": "1"},
        {**pending, "撤消数量": "1"},
        {key: value for key, value in pending.items() if key != "撤消数量"},
        {**pending, "备注": "未知状态"},
    ):
        result = normalize_rows("orders", [row], DAY)[0]
        assert result["status"] == "unknown"
        assert result["cancelable"] is False


@pytest.mark.parametrize("kind,row", [
    ("positions", {**POSITION, "股票余额": ""}),
    ("positions", {**POSITION, "可用余额": "1.5"}),
    ("orders", {**ORDER, "合同编号": ""}),
    ("orders", {**ORDER, "委托数量": "9"}),
    ("trades", {**TRADE, "成交编号": ""}),
    ("trades", {**TRADE, "成交金额": ""}),
    ("trades", {**TRADE, "成交金额": "0"}),
    ("trades", {**TRADE, "成交均价": "0"}),
    ("trades", {k: v for k, v in TRADE.items() if k != "成交均价"}),
    ("trades", {**TRADE, "成交数量": "0.5"}),
    ("trades", {**TRADE, "交易市场": "未知"}),
    ("trades", {**TRADE, "成交日期": "2025-01-05"}),
    ("trades", {**TRADE, "成交日期": "20250105"}),
    ("trades", {**TRADE, "成交日期": ""}),
    ("trades", {**TRADE, "成交时间": "2025-01-06"}),
    ("trades", {**TRADE, "成交时间": "2025-01-05 09:35:02"}),
])
def test_invalid_required_facts_fail_whole_snapshot(kind, row):
    with pytest.raises(ValueError):
        normalize_rows(kind, [row], DAY)


def test_account_requires_observed_cash_and_valid_day():
    with pytest.raises(ValueError):
        normalize_account({"资金余额": "10000.01"})
    with pytest.raises(ValueError):
        normalize_account({"资金余额": "10000.01", "可用资金": "10",
                           "可用金额": "10"})
    with pytest.raises(ValueError):
        normalize_rows("positions", [], "2026-02-30")
