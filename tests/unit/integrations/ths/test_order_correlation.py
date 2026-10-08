"""Synthetic proof checks for the no-success-dialog order fallback."""

from dataclasses import replace
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from bullet_trade.integrations.ths.request_store import Request
from bullet_trade.integrations.ths.windows.native.order_correlation import (
    OrderCorrelationError, correlate_new_order,
)


DAY = "2025-01-06"
TZ = ZoneInfo("Asia/Shanghai")
SUBMITTED = datetime(2025, 1, 6, 9, 35, 0, 500000, tzinfo=TZ).timestamp()
OBSERVED = SUBMITTED + 12
ROW = {"合同编号": "NEW001", "证券代码": "518880", "交易市场": "上海Ａ股",
       "操作": "买入", "委托数量": "100", "成交数量": "0",
       "撤消数量": "0", "委托价格": "8.500", "委托时间": "09:35:01",
       "备注": "已报"}
OLD = {"合同编号": "OLD001"}


def request():
    return Request("synthetic-request", "synthetic-account", DAY, "key", "limit_buy",
                   {"security": "518880.XSHG", "quantity": 100, "price": "8.500"},
                   {"virtual_account_id": "synthetic-virtual"}, "submit_unknown",
                   OBSERVED + 100, None, None, SUBMITTED, SUBMITTED)


def correlate(*, before=None, after=None, req=None, **overrides):
    arguments = {"before_complete": True, "after_complete": True,
                 "confirmed_terms": True, "submitted_at": SUBMITTED,
                 "observed_at": OBSERVED}
    arguments.update(overrides)
    return correlate_new_order(req or request(),
                               [OLD] if before is None else before,
                               [OLD, ROW] if after is None else after,
                               **arguments)


def fails(code, **kwargs):
    with pytest.raises(OrderCorrelationError, match=f"^{code}$"):
        correlate(**kwargs)


def test_unique_exact_new_contract_with_verified_confirmation():
    assert correlate() == "NEW001"
    assert correlate(after=[OLD, {**ROW, "委托日期": "20250106"}]) == "NEW001"
    assert correlate(after=[OLD, {**ROW, "委托时间": "2025-01-06 09:35:01"}]) == "NEW001"


def test_complete_unfiltered_scope_and_confirmation_are_mandatory():
    fails("evidence_incomplete", before_complete=False)
    fails("evidence_incomplete", after_complete=False)
    fails("evidence_incomplete", after_complete=1)
    fails("confirmation_unverified", confirmed_terms=False)
    fails("confirmation_unverified", confirmed_terms="yes")


def test_contract_delta_must_be_unique_with_no_missing_old_contract():
    fails("new_contract_not_unique", after=[OLD])
    fails("new_contract_not_unique", after=[OLD, ROW, {**ROW, "合同编号": "NEW002"}])
    fails("contract_disappeared", after=[ROW])
    fails("contract_set_unverified", after=[OLD, ROW, dict(ROW)])
    fails("contract_set_unverified", before=[OLD, dict(OLD)])
    fails("rows_invalid", after=[OLD, "not-a-row"])
    fails("rows_invalid", before={"合同编号": "OLD001"})


@pytest.mark.parametrize("change", [
    {"证券代码": "000001"}, {"交易市场": "深圳Ａ股"},
    {"操作": "卖出"}, {"委托数量": "101"}, {"委托价格": "8.501"},
])
def test_new_row_must_match_complete_request_terms(change):
    with pytest.raises(OrderCorrelationError):
        correlate(after=[OLD, {**ROW, **change}])


@pytest.mark.parametrize("change,code", [
    ({"委托时间": ""}, "order_time_unverified"),
    ({"委托时间": "09:34:57"}, "order_time_unverified"),
    ({"委托时间": "09:35:20"}, "order_time_unverified"),
    ({"委托日期": "20250103"}, "date_mismatch"),
    ({"委托时间": "2025-01-03 09:35:01"}, "date_mismatch"),
    ({"委托时间": "25:35:01"}, "order_time_unverified"),
])
def test_order_time_and_explicit_day_must_bind_to_submission(change, code):
    fails(code, after=[OLD, {**ROW, **change}])


def test_epoch_window_cannot_cross_days_or_run_without_bound():
    fails("time_window_unverified", observed_at=SUBMITTED - 1)
    fails("time_window_unverified", observed_at=SUBMITTED + 61)
    fails("date_mismatch",
          submitted_at=datetime(2025, 1, 6, 23, 59, 59, tzinfo=TZ).timestamp(),
          observed_at=datetime(2025, 1, 7, 0, 0, 1, tzinfo=TZ).timestamp())
    fails("date_mismatch", req=replace(request(), trade_day="2025-01-03"))


@pytest.mark.parametrize("change,code", [
    ({"备注": "未知"}, "state_unverified"),
    ({"备注": "部分成交", "成交数量": "0"}, "state_unverified"),
    ({"备注": "全部成交", "成交数量": "99"}, "state_unverified"),
    ({"备注": "已报", "成交数量": "1"}, "state_unverified"),
    ({"撤消数量": "1"}, "quantity_unverified"),
    ({"成交数量": "101"}, "quantity_unverified"),
])
def test_status_and_quantity_conservation(change, code):
    fails(code, after=[OLD, {**ROW, **change}])


def test_valid_partial_and_full_fill_within_window():
    assert correlate(after=[OLD, {**ROW, "备注": "部分成交", "成交数量": "40"}]) == "NEW001"
    assert correlate(after=[OLD, {**ROW, "备注": "全部成交", "成交数量": "100"}]) == "NEW001"


def test_sell_requires_exact_sell_confirmation_terms():
    sell = replace(request(), kind="limit_sell")
    assert correlate(req=sell, after=[OLD, {**ROW, "操作": "卖出"}]) == "NEW001"
    fails("terms_mismatch", req=sell)
