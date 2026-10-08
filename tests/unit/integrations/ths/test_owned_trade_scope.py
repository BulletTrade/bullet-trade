"""Synthetic ownership evidence for undated native trade rows."""

from __future__ import annotations

from dataclasses import replace

import pytest

from bullet_trade.integrations.ths.normalization import normalize_rows
from bullet_trade.integrations.ths.owned_trade_scope import (
    OwnedTradeScopeError, normalize_owned_trades,
)
from bullet_trade.integrations.ths.request_store import Request


DAY = "2025-01-06"
ACCOUNT = "test-account"


def order(contract="ORDER-A", *, kind="limit_buy", quantity=10,
          price="10.000", security="600001.XSHG", **changes):
    request = Request(
        request_id=f"request-{contract}", account=ACCOUNT, trade_day=DAY,
        idempotency_key=f"key-{contract}", kind=kind,
        params={"security": security, "quantity": quantity, "price": price},
        origin={"subaccount_key": "parent:test-account"}, state="accepted",
        expires_at=100.0, broker_contract_no=contract, evidence_ref="receipt-test",
        created_at=1.0, updated_at=2.0,
    )
    return replace(request, **changes)


def trade(contract="ORDER-A", trade_id="FILL-A", *, quantity="4",
          price="9.990", side="买入", code="600001", **changes):
    row = {
        "成交时间": "09:35:02", "证券代码": code, "证券名称": "合成证券",
        "操作": side, "成交数量": quantity, "成交均价": price,
        "成交金额": "39.960", "合同编号": contract,
        "成交编号": trade_id, "委托时间": "09:35:00",
    }
    row.update(changes)
    return row


def lookup_for(*requests):
    owned = {request.broker_contract_no: request for request in requests}

    def lookup(account, day, contract):
        assert (account, day) == (ACCOUNT, DAY)
        return owned.get(contract)

    return lookup


def test_owned_undated_trade_uses_order_day_and_verified_market_without_mutation():
    raw = trade()
    result = normalize_owned_trades([raw], account=ACCOUNT, trade_day=DAY,
                                    lookup=lookup_for(order()))
    assert len(result) == 1
    assert result[0] == {
        "trade_id": "FILL-A", "order_id": "ORDER-A", "security": "600001.XSHG",
        "amount": 4, "deal_balance": "39.960",
        "time": "2025-01-06T09:35:02+08:00", "is_buy": True,
        "price": "9.990", "traded_price": "9.990",
        "trade_day": DAY, "day_source": "durable_accepted_request",
        "request_id": "request-ORDER-A",
    }
    assert "成交日期" not in raw and "交易市场" not in raw
    with pytest.raises(ValueError, match="missing_成交日期"):
        normalize_rows("trades", [{**raw, "交易市场": "上海"}], None)


def test_empty_and_valid_partial_fills():
    lookup = lookup_for(order())
    assert normalize_owned_trades([], account=ACCOUNT, trade_day=DAY, lookup=lookup) == []
    rows = [trade(quantity="4"), trade(trade_id="FILL-B", quantity="6")]
    assert [item["amount"] for item in normalize_owned_trades(
        rows, account=ACCOUNT, trade_day=DAY, lookup=lookup)] == [4, 6]


@pytest.mark.parametrize("change", [
    {"state": "submit_unknown"}, {"account": "other-account"},
    {"trade_day": "2025-01-07"}, {"broker_contract_no": "OTHER"},
    {"evidence_ref": None}, {"request_id": ""}, {"kind": "cancel"},
    {"params": {"security": "600001.XSHG", "quantity": 0, "price": "10.000"}},
])
def test_missing_or_invalid_durable_owner_rejects_whole_table(change):
    owner = order(**change)
    with pytest.raises(OwnedTradeScopeError):
        normalize_owned_trades([trade()], account=ACCOUNT, trade_day=DAY,
                               lookup=lambda *_: owner)


def test_any_unmapped_row_or_lookup_failure_rejects_whole_table():
    with pytest.raises(OwnedTradeScopeError, match="original_order_missing"):
        normalize_owned_trades([trade(), trade("OTHER", "FILL-B")],
                               account=ACCOUNT, trade_day=DAY, lookup=lookup_for(order()))

    def broken_lookup(*_):
        raise RuntimeError("store unavailable")

    with pytest.raises(OwnedTradeScopeError, match="original_order_lookup_failed"):
        normalize_owned_trades([trade()], account=ACCOUNT, trade_day=DAY,
                               lookup=broken_lookup)


@pytest.mark.parametrize("row", [
    trade(code="600002"), trade(side="卖出"), trade(quantity="11"),
    trade(quantity="0"), trade(price="10.001"),
    trade(**{"交易市场": "深圳"}),
    trade(**{"成交日期": "2025-01-07"}),
    trade(**{"成交时间": "2025-01-07 09:35:02"}),
    trade(**{"成交时间": "2025-01-06T16:00:00+00:00"}),
    trade(request_id="another-request"),
    trade(trade_day="2025-01-07"),
])
def test_conflicting_row_cannot_gain_durable_day(row):
    with pytest.raises(OwnedTradeScopeError):
        normalize_owned_trades([row], account=ACCOUNT, trade_day=DAY,
                               lookup=lookup_for(order()))


def test_explicit_matching_day_and_market_are_validated():
    row = trade(**{"成交日期": "20250106", "交易市场": "上海"})
    assert normalize_owned_trades([row], account=ACCOUNT, trade_day=DAY,
                                  lookup=lookup_for(order()))[0]["security"] == "600001.XSHG"


def test_sell_execution_price_must_respect_original_limit():
    owner = order(kind="limit_sell", price="10.000")
    good = trade(side="卖出", price="10.010")
    assert normalize_owned_trades([good], account=ACCOUNT, trade_day=DAY,
                                  lookup=lookup_for(owner))[0]["is_buy"] is False
    with pytest.raises(OwnedTradeScopeError, match="trade_terms_mismatch"):
        normalize_owned_trades([trade(side="卖出", price="9.990")], account=ACCOUNT,
                               trade_day=DAY, lookup=lookup_for(owner))


def test_duplicate_fill_id_and_cumulative_quantity_rejected():
    owners = lookup_for(order(), order("ORDER-B"))
    with pytest.raises(OwnedTradeScopeError, match="duplicate_trade_id"):
        normalize_owned_trades([trade(), trade("ORDER-B", "FILL-A")],
                               account=ACCOUNT, trade_day=DAY, lookup=owners)
    with pytest.raises(OwnedTradeScopeError, match="trade_quantity_exceeded"):
        normalize_owned_trades([trade(quantity="6"), trade(trade_id="FILL-B", quantity="5")],
                               account=ACCOUNT, trade_day=DAY, lookup=owners)


def test_input_and_preexisting_scope_require_verification():
    with pytest.raises(OwnedTradeScopeError):
        normalize_owned_trades(["not a row"], account=ACCOUNT, trade_day=DAY,
                               lookup=lookup_for(order()))
    with pytest.raises(OwnedTradeScopeError):
        normalize_owned_trades([trade(day_source="different")], account=ACCOUNT,
                               trade_day=DAY, lookup=lookup_for(order()))
