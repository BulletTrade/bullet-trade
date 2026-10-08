"""Bind an undated broker order row to its durable accepted originating order.

This pure helper must run with the local RequestStore.get_by_contract lookup
before a qualified THS order snapshot is published. It never assigns one day
to a whole page or infers a date from a client caption or the local clock.
"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Callable
from zoneinfo import ZoneInfo

from .request_store import Request


_TZ = ZoneInfo("Asia/Shanghai")
_SOURCE = "durable_accepted_request"


def _explicit_order_day(row: dict) -> str | None:
    raw = row.get("order_time")
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError("order_time_invalid")
    try:
        observed = datetime.fromisoformat(raw)
        if observed.tzinfo is None:
            raise ValueError("timezone_missing")
        return observed.astimezone(_TZ).date().isoformat()
    except ValueError as exc:
        raise ValueError("order_time_invalid") from exc


def _same_terms(row: dict, original: Request) -> bool:
    params = original.params
    if (not isinstance(params, dict) or type(row.get("amount")) is not int
            or row["amount"] <= 0 or type(params.get("quantity")) is not int
            or params["quantity"] <= 0 or row["amount"] != params["quantity"]
            or row.get("security") != params.get("security")
            or row.get("is_buy") is not (original.kind == "limit_buy")):
        return False
    try:
        observed_price = Decimal(str(row["order_price"]))
        original_price = Decimal(params["price"])
    except (KeyError, InvalidOperation, TypeError, ValueError):
        return False
    return (observed_price.is_finite() and observed_price > 0
            and original_price.is_finite() and original_price > 0
            and observed_price == original_price)


def bind_owned_order_days(data: list[dict], *, account: str, trade_day: str,
                          lookup: Callable[[str, str, str], Request | None]) -> list[dict]:
    """Return copied rows; annotate only uniquely and exactly owned contracts.

    ``lookup`` must be RequestStore.get_by_contract for the same local account
    and day. A failed or ambiguous lookup leaves the row's date unknown.
    Existing explicit date conflicts never receive durable provenance.
    """
    if (not isinstance(data, list) or any(type(row) is not dict for row in data)
            or not isinstance(account, str) or not account
            or not isinstance(trade_day, str) or not callable(lookup)):
        raise ValueError("owned_scope_input_invalid")
    try:
        if date.fromisoformat(trade_day).isoformat() != trade_day:
            raise ValueError
    except ValueError as exc:
        raise ValueError("owned_scope_input_invalid") from exc
    counts = Counter(row.get("order_id") for row in data
                     if isinstance(row.get("order_id"), str))
    result = []
    for source_row in data:
        row = dict(source_row)
        previous_request_id = row.get("request_id")
        # Revalidate any prior durable annotation on every publication. A
        # failed lookup must not leave a stale scope marker in the new copy.
        if row.get("trade_day_source") == _SOURCE:
            row.pop("trade_day_source", None)
            row.pop("request_id", None)
            if row.get("trade_day") == trade_day:
                row.pop("trade_day", None)
        contract = row.get("order_id")
        if not isinstance(contract, str) or not contract or counts[contract] != 1:
            result.append(row)
            continue
        if (row.get("trade_day") not in (None, trade_day)
                or row.get("trade_day_source") not in (None, _SOURCE)):
            result.append(row)
            continue
        try:
            explicit_day = _explicit_order_day(row)
        except ValueError:
            result.append(row)
            continue
        if explicit_day is not None and explicit_day != trade_day:
            result.append(row)
            continue
        try:
            original = lookup(account, trade_day, contract)
        except Exception:
            original = None
        if (not isinstance(original, Request) or original.state != "accepted"
                or original.kind not in {"limit_buy", "limit_sell"}
                or original.account != account or original.trade_day != trade_day
                or original.broker_contract_no != contract
                or not isinstance(original.evidence_ref, str)
                or not original.evidence_ref
                or not isinstance(original.request_id, str)
                or not original.request_id
                or previous_request_id not in (None, original.request_id)
                or row.get("request_id") not in (None, original.request_id)
                or not _same_terms(row, original)):
            result.append(row)
            continue
        row.update(trade_day=original.trade_day,
                   trade_day_source=_SOURCE,
                   request_id=original.request_id)
        result.append(row)
    return result
