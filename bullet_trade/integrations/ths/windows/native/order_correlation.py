"""Correlate one GUI order with a unique new broker contract, without GUI I/O.

The caller must hold the single-account GUI actor, attest unfiltered complete
before/after order tables from that account, and verify this request's exact
confirmation before submission. Similar terms alone never establish identity.
This bounded fallback applies only when a success receipt is absent; it is not
a general order-discovery or historical reconciliation mechanism.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
import math
import re
from zoneinfo import ZoneInfo

from ...request_store import Request
from ...normalization import _security


_TZ = ZoneInfo("Asia/Shanghai")
_EARLY_SECONDS = 2
_MAX_OBSERVATION_SECONDS = 60
_OPEN_STATUSES = frozenset({"已报", "未成交"})
_PARTIAL_STATUS = "部分成交"
_FILLED_STATUS = "全部成交"


class OrderCorrelationError(ValueError):
    """Fixed code only; never includes account, security, contract, or row data."""


def _fail(code: str):
    raise OrderCorrelationError(code)


def _contracts(rows) -> dict[str, dict]:
    if not isinstance(rows, (list, tuple)):
        _fail("rows_invalid")
    result = {}
    for row in rows:
        if type(row) is not dict:
            _fail("rows_invalid")
        contract = row.get("合同编号")
        if (not isinstance(contract, str)
                or re.fullmatch(r"[0-9A-Za-z]+", contract) is None
                or contract in result):
            _fail("contract_set_unverified")
        result[contract] = row
    return result


def _positive_decimal(value) -> Decimal:
    if (not isinstance(value, str)
            or re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value) is None):
        _fail("terms_mismatch")
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        _fail("terms_mismatch")
    if not parsed.is_finite() or parsed <= 0:
        _fail("terms_mismatch")
    return parsed


def _nonnegative_int(value) -> int:
    if not isinstance(value, str) or re.fullmatch(r"[0-9]+", value) is None:
        _fail("quantity_unverified")
    return int(value)


def _row_datetime(row: dict, trading_day: date) -> datetime:
    raw = row.get("委托时间")
    if not isinstance(raw, str):
        _fail("order_time_unverified")
    try:
        if re.fullmatch(r"[0-9]{2}:[0-9]{2}:[0-9]{2}", raw):
            when = datetime.combine(trading_day, time.fromisoformat(raw), _TZ)
        elif re.fullmatch(
                r"[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}"
                r"(?:[+-][0-9]{2}:[0-9]{2})?", raw):
            when = datetime.fromisoformat(raw)
            when = (when.replace(tzinfo=_TZ) if when.tzinfo is None
                    else when.astimezone(_TZ))
        else:
            _fail("order_time_unverified")
    except ValueError as exc:
        raise OrderCorrelationError("order_time_unverified") from exc
    explicit = row.get("委托日期")
    if explicit is not None:
        if not isinstance(explicit, str):
            _fail("date_mismatch")
        try:
            if re.fullmatch(r"[0-9]{8}", explicit):
                explicit_day = datetime.strptime(explicit, "%Y%m%d").date()
            elif re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", explicit):
                explicit_day = date.fromisoformat(explicit)
            else:
                _fail("date_mismatch")
        except ValueError as exc:
            raise OrderCorrelationError("date_mismatch") from exc
        if explicit_day != trading_day or explicit_day != when.date():
            _fail("date_mismatch")
    if when.date() != trading_day:
        _fail("date_mismatch")
    return when


def correlate_new_order(request: Request, before_rows, after_rows, *,
                        before_complete: bool, after_complete: bool,
                        confirmed_terms: bool, submitted_at: float,
                        observed_at: float) -> str:
    """Return the sole new contract only for a tightly bound same-day delta.

    Completeness includes the caller's proof of same-account, unfiltered whole
    tables. Epochs must come from this submission and its subsequent snapshot;
    the observation is bounded to 60 seconds after submission. A competing
    identical external order inside that window cannot be excluded here and
    requires the actor's account exclusivity evidence at the call site.
    """
    if (not isinstance(request, Request)
            or request.kind not in {"limit_buy", "limit_sell"}
            or not isinstance(request.params, dict)):
        _fail("request_invalid")
    if before_complete is not True or after_complete is not True:
        _fail("evidence_incomplete")
    if confirmed_terms is not True:
        _fail("confirmation_unverified")
    if (type(submitted_at) not in (int, float)
            or type(observed_at) not in (int, float)
            or not math.isfinite(submitted_at) or not math.isfinite(observed_at)
            or not 0 <= observed_at - submitted_at <= _MAX_OBSERVATION_SECONDS):
        _fail("time_window_unverified")
    try:
        day = date.fromisoformat(request.trade_day)
        if day.isoformat() != request.trade_day:
            _fail("request_invalid")
        submitted = datetime.fromtimestamp(submitted_at, _TZ)
        observed = datetime.fromtimestamp(observed_at, _TZ)
    except (TypeError, ValueError, OverflowError) as exc:
        raise OrderCorrelationError("request_invalid") from exc
    if submitted.date() != day or observed.date() != day:
        _fail("date_mismatch")
    before = _contracts(before_rows)
    after = _contracts(after_rows)
    if not before.keys() <= after.keys():
        _fail("contract_disappeared")
    added = after.keys() - before.keys()
    if len(added) != 1:
        _fail("new_contract_not_unique")
    contract = next(iter(added))
    row = after[contract]
    try:
        security = _security(row)
        price = _positive_decimal(row.get("委托价格"))
        expected_price = _positive_decimal(request.params.get("price"))
    except ValueError as exc:
        raise OrderCorrelationError("terms_mismatch") from exc
    expected_side = "买入" if request.kind == "limit_buy" else "卖出"
    if (security != request.params.get("security")
            or row.get("操作") != expected_side or price != expected_price
            or type(request.params.get("quantity")) is not int
            or request.params["quantity"] <= 0):
        _fail("terms_mismatch")
    amount = _nonnegative_int(row.get("委托数量"))
    filled = _nonnegative_int(row.get("成交数量"))
    canceled = _nonnegative_int(row.get("撤消数量", "0"))
    if amount != request.params["quantity"] or filled + canceled > amount or canceled:
        _fail("quantity_unverified")
    status = row.get("备注")
    if (status in _OPEN_STATUSES and filled == 0):
        pass
    elif status == _PARTIAL_STATUS and 0 < filled < amount:
        pass
    elif status == _FILLED_STATUS and filled == amount:
        pass
    else:
        _fail("state_unverified")
    placed = _row_datetime(row, day)
    if not submitted - timedelta(seconds=_EARLY_SECONDS) <= placed <= observed:
        _fail("order_time_unverified")
    return contract
