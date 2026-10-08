"""Normalize an undated trade row only with its durable accepted order.

The caller must independently establish that ``raw_rows`` is a complete,
unfiltered snapshot for ``account``. This module supplies row attribution,
not GUI page or snapshot coverage evidence.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
import re
from typing import Callable

from .normalization import normalize_rows
from .request_store import Request


class OwnedTradeScopeError(ValueError):
    """A row cannot be safely attributed to a durable accepted order."""


def _untrimmed_text(value: object) -> bool:
    return isinstance(value, str) and bool(value) and value == value.strip()


def _order_terms(original: Request, account: str, trade_day: str,
                 contract: str) -> tuple[str, int, Decimal, bool]:
    if (original.state != "accepted" or original.kind not in {"limit_buy", "limit_sell"}
            or original.account != account or original.trade_day != trade_day
            or original.broker_contract_no != contract
            or not _untrimmed_text(original.request_id)
            or not _untrimmed_text(original.evidence_ref)
            or not isinstance(original.params, dict)):
        raise OwnedTradeScopeError("original_order_unverified")
    security = original.params.get("security")
    quantity = original.params.get("quantity")
    if (not isinstance(security, str)
            or re.fullmatch(r"[0-9]{6}\.(?:XSHG|XSHE)", security) is None
            or type(quantity) is not int or quantity <= 0):
        raise OwnedTradeScopeError("original_terms_invalid")
    price_raw = original.params.get("price")
    if not isinstance(price_raw, str):
        raise OwnedTradeScopeError("original_terms_invalid")
    try:
        price = Decimal(price_raw)
    except InvalidOperation as exc:
        raise OwnedTradeScopeError("original_terms_invalid") from exc
    if not price.is_finite() or price <= 0:
        raise OwnedTradeScopeError("original_terms_invalid")
    return security, quantity, price, original.kind == "limit_buy"


def normalize_owned_trades(raw_rows: list[dict] | tuple[dict, ...], *,
                           account: str, trade_day: str,
                           lookup: Callable[[str, str, str], Request | None]) -> list[dict]:
    """Normalize every row using RequestStore.get_by_contract as ``lookup``.

    Any missing or conflicting attribution rejects the entire snapshot. An
    empty snapshot remains empty; this function does not prove completeness.
    """
    if (not isinstance(raw_rows, (list, tuple)) or not _untrimmed_text(account)
            or not callable(lookup)):
        raise OwnedTradeScopeError("owned_trade_input_invalid")
    try:
        if (not isinstance(trade_day, str)
                or date.fromisoformat(trade_day).isoformat() != trade_day):
            raise ValueError
    except ValueError as exc:
        raise OwnedTradeScopeError("owned_trade_input_invalid") from exc

    result: list[dict] = []
    seen_trade_ids: set[str] = set()
    filled_by_contract: dict[str, int] = defaultdict(int)
    owner_by_contract: dict[str, tuple] = {}
    for row in raw_rows:
        if type(row) is not dict:
            raise OwnedTradeScopeError("trade_row_invalid")
        contract = row.get("合同编号")
        if not _untrimmed_text(contract):
            raise OwnedTradeScopeError("trade_contract_invalid")
        try:
            original = lookup(account, trade_day, contract)
        except Exception as exc:
            raise OwnedTradeScopeError("original_order_lookup_failed") from exc
        if not isinstance(original, Request):
            raise OwnedTradeScopeError("original_order_missing")
        security, quantity, limit_price, is_buy = _order_terms(
            original, account, trade_day, contract)
        signature = (original.request_id, original.evidence_ref, security,
                     quantity, limit_price, is_buy)
        if contract in owner_by_contract and owner_by_contract[contract] != signature:
            raise OwnedTradeScopeError("original_order_changed")
        owner_by_contract[contract] = signature
        if (row.get("request_id") not in (None, original.request_id)
                or row.get("trade_day") not in (None, trade_day)
                or row.get("day_source") not in (None, "durable_accepted_request")):
            raise OwnedTradeScopeError("existing_trade_scope_conflict")
        try:
            converted = normalize_rows(
                "trades", [row], original.trade_day,
                verified_security_map={security[:6]: security})[0]
        except ValueError as exc:
            raise OwnedTradeScopeError("trade_row_unverified") from exc
        trade_id = converted["trade_id"]
        if trade_id in seen_trade_ids:
            raise OwnedTradeScopeError("duplicate_trade_id")
        seen_trade_ids.add(trade_id)
        if (converted["order_id"] != contract
                or converted["security"] != security
                or converted["is_buy"] is not is_buy):
            raise OwnedTradeScopeError("trade_terms_mismatch")
        amount = converted["amount"]
        price = Decimal(converted["price"])
        if (amount > quantity
                or (is_buy and price > limit_price)
                or (not is_buy and price < limit_price)):
            raise OwnedTradeScopeError("trade_terms_mismatch")
        filled_by_contract[contract] += amount
        if filled_by_contract[contract] > quantity:
            raise OwnedTradeScopeError("trade_quantity_exceeded")
        converted.update(trade_day=original.trade_day,
                         day_source="durable_accepted_request",
                         request_id=original.request_id)
        result.append(converted)
    return result
