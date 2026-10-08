"""Convert verified THS read-only query rows to BulletTrade broker fields.

This module only converts values. Snapshot completeness, account identity and
page attribution must be established before its functions are called.
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
import re
import math
from typing import Mapping
from zoneinfo import ZoneInfo


_SHANGHAI = {"上海", "上海A股", "上海Ａ股", "沪A", "沪Ａ", "SH", "XSHG"}
_SHENZHEN = {"深圳", "深圳A股", "深圳Ａ股", "深A", "深Ａ", "SZ", "XSHE"}
_TZ = ZoneInfo("Asia/Shanghai")


def broker_data(kind: str, data):
    """Validate the public contract; convert money to floats only at its boundary."""
    required = {
        "account": {"available_cash", "total_value"},
        "positions": {"security", "amount", "closeable_amount", "avg_cost", "current_price", "market_value"},
        "orders": {"order_id", "security", "amount", "filled", "order_price", "is_buy", "status"},
        "cancelable": {"order_id", "security", "amount", "filled", "is_buy", "status"},
        "trades": {"trade_id", "order_id", "security", "amount", "price", "deal_balance", "time", "is_buy"},
    }
    money = {"available_cash", "cash_balance", "frozen_cash", "market_value", "total_value",
             "avg_cost", "current_price", "order_price", "price", "traded_price", "deal_balance"}
    if kind not in required or not isinstance(data, dict if kind == "account" else list):
        raise ValueError("invalid_broker_data_shape")
    rows = [data] if kind == "account" else data
    result = []
    for row in rows:
        if not isinstance(row, dict) or not required[kind] <= row.keys():
            raise ValueError("required_broker_fields_missing")
        item = dict(row)
        for key in item.keys() & money:
            if isinstance(item[key], bool) or item[key] is None:
                raise ValueError("invalid_broker_money")
            value = float(item[key])
            if not math.isfinite(value):
                raise ValueError("nonfinite_broker_money")
            item[key] = value
        result.append(item)
    return result[0] if kind == "account" else result


def _text(row: Mapping, field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"missing_or_invalid_{field}")
    return value.strip()


def _optional_text(row: Mapping, field: str) -> str | None:
    value = row.get(field)
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError(f"invalid_{field}")
    return value.strip() or None


def _decimal(row: Mapping, field: str, *, required: bool = True,
             allow_negative: bool = False) -> str | None:
    raw = _text(row, field) if required else _optional_text(row, field)
    if raw is None:
        return None
    pattern = r"[+-]?[0-9]+(?:\.[0-9]+)?" if allow_negative else r"[+]?[0-9]+(?:\.[0-9]+)?"
    if re.fullmatch(pattern, raw) is None:
        raise ValueError(f"invalid_{field}")
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"invalid_{field}") from exc
    if not value.is_finite():
        raise ValueError(f"invalid_{field}")
    return raw.lstrip("+")


def _quantity(row: Mapping, field: str) -> int:
    raw = _decimal(row, field)
    assert raw is not None
    number = Decimal(raw)
    if number != number.to_integral_value():
        raise ValueError(f"non_integral_{field}")
    return int(number)


def _security(row: Mapping, verified_security_map: Mapping[str, str] | None = None) -> str:
    code = _text(row, "证券代码")
    if re.fullmatch(r"[0-9]{6}", code) is None:
        raise ValueError("invalid_证券代码")
    market = _optional_text(row, "交易市场")
    mapped = verified_security_map.get(code) if verified_security_map is not None else None
    if market is None:
        if mapped is None:
            raise ValueError("missing_or_invalid_交易市场")
        return mapped
    if market in _SHANGHAI:
        security = f"{code}.XSHG"
    elif market in _SHENZHEN:
        security = f"{code}.XSHE"
    else:
        raise ValueError("unknown_交易市场")
    if mapped is not None and mapped != security:
        raise ValueError("conflicting_verified_security_map")
    return security


def _validate_security_map(verified_security_map: Mapping[str, str] | None) -> None:
    if verified_security_map is None:
        return
    if not isinstance(verified_security_map, Mapping):
        raise ValueError("invalid_verified_security_map")
    for code, security in verified_security_map.items():
        if (not isinstance(code, str) or re.fullmatch(r"[0-9]{6}", code) is None
                or not isinstance(security, str)
                or security not in (f"{code}.XSHG", f"{code}.XSHE")):
            raise ValueError("invalid_verified_security_map")


def _day(trading_day: str) -> date:
    if not isinstance(trading_day, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", trading_day) is None:
        raise ValueError("invalid_trading_day")
    try:
        return date.fromisoformat(trading_day)
    except ValueError as exc:
        raise ValueError("invalid_trading_day") from exc


def _timestamp(row: Mapping, field: str, trading_day: date | None) -> str | None:
    raw = _text(row, field)
    date_field = "成交日期" if field == "成交时间" else "委托日期"
    explicit_date = _text(row, date_field) if date_field in row else None
    observed_date = None
    if explicit_date is not None:
        try:
            if re.fullmatch(r"\d{8}", explicit_date):
                observed_date = datetime.strptime(explicit_date, "%Y%m%d").date()
            elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", explicit_date):
                observed_date = date.fromisoformat(explicit_date)
            else:
                raise ValueError("invalid_date_format")
        except ValueError as exc:
            raise ValueError(f"invalid_{date_field}") from exc
        if trading_day is not None and observed_date != trading_day:
            raise ValueError(f"mismatched_{date_field}")
    try:
        if re.fullmatch(r"\d{2}:\d{2}:\d{2}", raw):
            clock = time.fromisoformat(raw)
            resolved_day = trading_day if trading_day is not None else observed_date
            if resolved_day is None:
                return None
            observed = datetime.combine(resolved_day, clock)
        elif re.fullmatch(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[+-]\d{2}:\d{2})?", raw):
            observed = datetime.fromisoformat(raw)
            if observed.tzinfo is not None:
                observed = observed.astimezone(_TZ).replace(tzinfo=None)
        else:
            raise ValueError("invalid_time_format")
        resolved_day = trading_day if trading_day is not None else observed_date
        if resolved_day is not None and observed.date() != resolved_day:
            raise ValueError("mismatched_trading_day")
        return observed.replace(tzinfo=_TZ).isoformat()
    except ValueError as exc:
        raise ValueError(f"invalid_or_mismatched_{field}") from exc


def _side(row: Mapping) -> bool:
    side = _text(row, "操作")
    if side == "买入":
        return True
    if side == "卖出":
        return False
    raise ValueError("unknown_操作")


def normalize_account(mapping: Mapping) -> dict:
    """Preserve observed money exactly; never infer total assets from cash."""
    if not isinstance(mapping, Mapping):
        raise ValueError("invalid_account")
    if "可用资金" in mapping and "可用金额" in mapping:
        raise ValueError("ambiguous_available_cash")
    available_field = "可用资金" if "可用资金" in mapping else "可用金额"
    cash_balance = _decimal(mapping, "资金余额")
    available = _decimal(mapping, available_field)
    result = {"cash_balance": cash_balance, "available_cash": available}
    for source, target in (("冻结资金", "frozen_cash"), ("冻结金额", "frozen_cash"),
                           ("股票市值", "market_value"), ("总资产", "total_value")):
        if source in mapping:
            if target in result:
                raise ValueError(f"ambiguous_{target}")
            result[target] = _decimal(mapping, source)
    return result


def _position(row: Mapping, verified_security_map: Mapping[str, str] | None = None) -> dict:
    amount = _quantity(row, "股票余额")
    closeable = _quantity(row, "可用余额")
    frozen = _quantity(row, "冻结数量")
    if closeable > amount or frozen > amount or closeable + frozen > amount:
        raise ValueError("inconsistent_position_quantity")
    result = {"security": _security(row, verified_security_map), "amount": amount,
              "closeable_amount": closeable, "frozen_amount": frozen}
    for source, target in (("证券名称", "name"),):
        value = _optional_text(row, source)
        if value is not None:
            result[target] = value
    cost = _decimal(row, "成本价", required=False, allow_negative=True)
    if cost is not None:
        result["avg_cost"] = cost
    for source, target in (("市价", "current_price"), ("市值", "market_value")):
        value = _decimal(row, source, required=False)
        if value is not None:
            result[target] = value
    return result


def _order(row: Mapping, trading_day: date | None, *, cancelable: bool,
           verified_security_map: Mapping[str, str] | None = None) -> dict:
    amount = _quantity(row, "委托数量")
    filled = _quantity(row, "成交数量")
    cancelled = _quantity(row, "撤消数量") if "撤消数量" in row else None
    if amount <= 0 or filled > amount or (cancelled is not None and filled + cancelled > amount):
        raise ValueError("inconsistent_order_quantity")
    raw_status = _optional_text(row, "备注")
    status = "unknown"
    if not cancelable and cancelled is not None:
        if raw_status == "未成交" and filled == 0 and cancelled == 0:
            status = "open"
        elif raw_status == "全部成交" and filled == amount and cancelled == 0:
            status = "filled"
        elif raw_status == "全部撤单" and cancelled > 0 and filled + cancelled == amount:
            status = "partly_canceled" if filled > 0 else "canceled"
        elif raw_status == "部分成交" and 0 < filled < amount and cancelled == 0:
            status = "filling"
    result = {"order_id": _text(row, "合同编号"),
              "security": _security(row, verified_security_map),
              "amount": amount, "filled": filled, "status": status,
              "raw_status": raw_status, "is_buy": _side(row),
              "cancelable": cancelable}
    if cancelled is not None:
        result["cancelled"] = cancelled
    if "委托时间" in row:
        order_time = _timestamp(row, "委托时间", trading_day)
        if order_time is None:
            result["order_time_raw"] = _text(row, "委托时间")
        else:
            result["order_time"] = order_time
    elif not cancelable:
        raise ValueError("missing_or_invalid_委托时间")
    price = _decimal(row, "委托价格", required=False)
    if price is not None:
        result["order_price"] = price
    traded_price = _decimal(row, "成交均价", required=False)
    if traded_price is not None:
        result["price"] = traded_price
    return result


def _trade(row: Mapping, trading_day: date | None,
           verified_security_map: Mapping[str, str] | None = None) -> dict:
    amount = _quantity(row, "成交数量")
    if amount <= 0:
        raise ValueError("invalid_成交数量")
    balance = _decimal(row, "成交金额")
    price = _decimal(row, "成交均价")
    if Decimal(balance) <= 0 or Decimal(price) <= 0:
        raise ValueError("nonpositive_trade_money_or_price")
    result = {"trade_id": _text(row, "成交编号"),
              "order_id": _text(row, "合同编号"),
              "security": _security(row, verified_security_map), "amount": amount,
              "deal_balance": balance,
              "time": _timestamp(row, "成交时间", trading_day),
              "is_buy": _side(row), "price": price, "traded_price": price}
    if result["time"] is None:
        raise ValueError("missing_成交日期")
    return result


def normalize_rows(kind: str, rows: list[Mapping], trading_day: str | None, *,
                   verified_security_map: Mapping[str, str] | None = None) -> list[dict]:
    """Convert one verified complete snapshot without dropping invalid rows."""
    day = _day(trading_day) if trading_day is not None else None
    _validate_security_map(verified_security_map)
    if kind not in {"positions", "holdings", "orders", "trades", "cancelable"}:
        raise ValueError("unknown_kind")
    if not isinstance(rows, list):
        raise ValueError("invalid_rows")
    converted = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("invalid_row")
        if kind in {"positions", "holdings"}:
            converted.append(_position(row, verified_security_map))
        elif kind in {"orders", "cancelable"}:
            converted.append(_order(row, day, cancelable=kind == "cancelable",
                                    verified_security_map=verified_security_map))
        else:
            converted.append(_trade(row, day, verified_security_map))
    return converted
