"""Synthetic same-account durable ownership checks; no GUI or service I/O."""

from dataclasses import replace

import pytest

from bullet_trade.integrations.ths.owned_order_scope import bind_owned_order_days
from bullet_trade.integrations.ths.request_store import Request


DAY = "2025-01-06"
ROW = {"order_id": "SYN001", "security": "518880.XSHG", "amount": 100,
       "order_price": "8.500", "is_buy": True, "status": "canceled",
       "filled": 0}


def original(**changes):
    value = Request("origin-uuid", "synthetic-account", DAY, "key", "limit_buy",
                    {"security": "518880.XSHG", "quantity": 100,
                     "price": "8.5"}, {"virtual_account_id": "v1"},
                    "accepted", 9999999999.0, "SYN001", "native-proof:abc", 0, 0)
    return replace(value, **changes)


def bind(rows=None, owner=None, *, account="synthetic-account", day=DAY):
    accepted = original() if owner is None else owner
    return bind_owned_order_days([ROW] if rows is None else rows,
                                 account=account, trade_day=day,
                                 lookup=lambda a, d, c: accepted)


def test_exact_durable_owner_adds_scope_only_to_its_row():
    data = [ROW, {**ROW, "order_id": "OTHER001"}]
    seen = []
    def lookup(account, day, contract):
        seen.append((account, day, contract))
        return original() if contract == "SYN001" else None
    result = bind_owned_order_days(data, account="synthetic-account",
                                   trade_day=DAY, lookup=lookup)
    assert result[0]["trade_day"] == DAY
    assert result[0]["trade_day_source"] == "durable_accepted_request"
    assert result[0]["request_id"] == "origin-uuid"
    assert "trade_day" not in result[1]
    assert data[0] == ROW and result[0] is not data[0]
    assert seen == [("synthetic-account", DAY, "SYN001"),
                    ("synthetic-account", DAY, "OTHER001")]


@pytest.mark.parametrize("changes", [
    {"state": "submit_unknown"}, {"account": "other-account"},
    {"trade_day": "2025-01-03"}, {"broker_contract_no": "OTHER001"},
    {"kind": "cancel"}, {"evidence_ref": None},
    {"params": {"security": "518880.XSHG", "quantity": 100, "price": "8.501"}},
    {"params": {"security": "518880.XSHG", "quantity": 101, "price": "8.5"}},
])
def test_nonmatching_durable_request_cannot_supply_date(changes):
    assert "trade_day" not in bind(owner=original(**changes))[0]


@pytest.mark.parametrize("changed", [
    {"security": "518880.XSHE"}, {"amount": 101}, {"is_buy": False},
    {"order_price": "8.501"}, {"request_id": "other-uuid"},
    {"trade_day": "2025-01-03"},
    {"order_time": "2025-01-03T09:35:00+08:00"},
])
def test_row_terms_or_explicit_date_conflict_remains_unknown(changed):
    row = {**ROW, **changed}
    assert bind(rows=[row])[0] == row


def test_duplicate_contract_rows_are_not_attributed():
    rows = [ROW, dict(ROW)]
    assert bind(rows=rows) == rows
    assert all("trade_day" not in row for row in bind(rows=rows))


def test_lookup_error_or_ambiguous_result_leaves_unknown_date():
    def unavailable(*args):
        raise RuntimeError("synthetic lookup failure")
    assert "trade_day" not in bind_owned_order_days(
        [ROW], account="synthetic-account", trade_day=DAY,
        lookup=unavailable)[0]
    assert "trade_day" not in bind(owner=[original(), original()])[0]
    stale = {**ROW, "trade_day": DAY,
             "trade_day_source": "durable_accepted_request",
             "request_id": "origin-uuid"}
    refreshed = bind_owned_order_days(
        [stale], account="synthetic-account", trade_day=DAY,
        lookup=unavailable)[0]
    assert "trade_day" not in refreshed
    assert "trade_day_source" not in refreshed
    assert "request_id" not in refreshed


def test_conflicting_prior_owner_marker_is_not_rebound():
    row = {**ROW, "trade_day": DAY,
           "trade_day_source": "durable_accepted_request",
           "request_id": "other-uuid"}
    rebound = bind(rows=[row])[0]
    assert "trade_day" not in rebound
    assert "trade_day_source" not in rebound
    assert "request_id" not in rebound


def test_invalid_input_is_rejected():
    with pytest.raises(ValueError, match="owned_scope_input_invalid"):
        bind_owned_order_days(["not a row"], account="synthetic-account",
                              trade_day=DAY, lookup=lambda *args: None)
