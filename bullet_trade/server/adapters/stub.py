from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

from .base import AccountRouter, AdapterBundle, RemoteBrokerAdapter, RemoteDataAdapter, AccountContext
from ..config import ServerConfig, AccountConfig
from . import register_adapter


def _first_present(*values: Any) -> Any:
    for value in values:
        if value in (None, ""):
            continue
        return value
    return None


def _as_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return int(default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


_DEFAULT_TRADE_DAYS = [
    "2026-03-26",
    "2026-03-27",
    "2026-03-30",
    "2026-03-31",
    "2026-04-01",
]
_DEFAULT_HISTORY_FIELDS = ["open", "close", "high", "low", "volume", "money"]
_DEFAULT_HISTORY_ROWS = {
    "1m": [
        {"open": 11.18, "close": 11.19, "high": 11.19, "low": 11.18, "volume": 1814.0, "money": 2029316.0},
        {"open": 11.19, "close": 11.20, "high": 11.20, "low": 11.19, "volume": 4008.0, "money": 4486960.0},
        {"open": 11.20, "close": 11.20, "high": 11.20, "low": 11.19, "volume": 1025.0, "money": 1147150.0},
        {"open": 11.19, "close": 11.19, "high": 11.20, "low": 11.19, "volume": 586.0, "money": 655831.0},
        {"open": 11.19, "close": 11.20, "high": 11.20, "low": 11.19, "volume": 1893.0, "money": 2118342.0},
    ],
    "1d": [
        {"open": 10.91, "close": 10.94, "high": 11.05, "low": 10.90, "volume": 827664.0, "money": 909199430.0},
        {"open": 10.91, "close": 11.02, "high": 11.05, "low": 10.83, "volume": 884129.0, "money": 972821270.0},
        {"open": 10.98, "close": 10.99, "high": 11.05, "low": 10.94, "volume": 632522.0, "money": 696208267.0},
        {"open": 11.00, "close": 11.08, "high": 11.18, "low": 10.99, "volume": 1164565.0, "money": 1294675716.0},
        {"open": 11.09, "close": 11.20, "high": 11.23, "low": 11.08, "volume": 532556.0, "money": 594385527.0},
    ],
}
_DEFAULT_SYMBOL_NAMES = {
    "000001.XSHE": "平安银行",
    "000002.XSHE": "万 科Ａ",
    "159915.XSHE": "创业板ETF易方达",
    "510050.XSHG": "上证50ETF华夏",
    "511880.XSHG": "银华日利ETF",
    "518880.XSHG": "黄金ETF华安",
    "600000.XSHG": "浦发银行",
    "601318.XSHG": "中国平安",
    "688001.XSHG": "华兴源创",
}
_RAW_STATUS_BY_STATUS = {
    "new": 48,
    "submitted": 49,
    "open": 50,
    "canceling": 51,
    "partly_canceling": 52,
    "partly_canceled": 53,
    "canceled": 54,
    "partly_filled": 55,
    "filled": 56,
    "rejected": 57,
}
_PRICE_TYPE_BY_STYLE = {
    "limit": 50,
    "market": 88,
}


def _dict_payload(value: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = {"dtype": "dict", "value": value or {}}
    if isinstance(value, dict):
        for key, item in value.items():
            if key not in payload and item is not None:
                payload[key] = item
    return payload


def _normalize_security(security: Any) -> str:
    return str(security or "").strip().upper()


def _stub_display_name(security: str) -> str:
    return _DEFAULT_SYMBOL_NAMES.get(security, f"Stub {security}" if security else "Stub Security")


def _infer_security_type(security: str) -> str:
    code = security.split(".", 1)[0]
    if code.startswith(("15", "16", "18", "50", "51")):
        return "etf"
    return "stock"


def _infer_security_subtype(security: str) -> str:
    sec_type = _infer_security_type(security)
    if sec_type == "etf":
        return "fund"
    return "equity"


def _to_qmt_code(security: str) -> str:
    if security.endswith(".XSHG"):
        return security.replace(".XSHG", ".SH")
    if security.endswith(".XSHE"):
        return security.replace(".XSHE", ".SZ")
    return security


def _normalize_status(status: Any) -> str:
    text = str(status or "open").strip().lower()
    if text == "cancelled":
        return "canceled"
    if text == "partially_filled":
        return "partly_filled"
    if text == "partially_canceled":
        return "partly_canceled"
    return text or "open"


def _raw_status_for(status: Any) -> int:
    return int(_RAW_STATUS_BY_STATUS.get(_normalize_status(status), 255))


def _price_type_for(style_type: str) -> int:
    return int(_PRICE_TYPE_BY_STYLE.get(str(style_type or "").lower(), 0))


class StubDataAdapter(RemoteDataAdapter):
    def __init__(self) -> None:
        self._ticks: Dict[str, Dict] = {}
        self._trade_days = list(_DEFAULT_TRADE_DAYS)
        self._history_rows = {key: [dict(row) for row in rows] for key, rows in _DEFAULT_HISTORY_ROWS.items()}

    def _tick_for(self, symbol: str) -> Dict[str, Any]:
        now = "2026-04-01 09:30:00"
        tick = {
            "sid": symbol,
            "symbol": symbol,
            "last_price": 10.0,
            "dt": now,
            "time": now,
            "volume": 1000,
            "provider": "stub",
        }
        tick.update(self._ticks.get(symbol, {}))
        tick["sid"] = tick.get("sid") or symbol
        tick["symbol"] = tick.get("symbol") or symbol
        tick["dt"] = tick.get("dt") or tick.get("time") or now
        tick["time"] = tick.get("time") or tick["dt"]
        tick["last_price"] = _as_float(tick.get("last_price"), 10.0)
        return tick

    def _live_snapshot_for(self, symbol: str) -> Dict[str, Any]:
        tick = self._tick_for(symbol)
        last_price = _as_float(tick.get("last_price"), 10.0)
        high_limit = _as_float(tick.get("high_limit"), round(last_price * 1.1, 3))
        low_limit = _as_float(tick.get("low_limit"), round(last_price * 0.9, 3))
        return {
            "last_price": last_price,
            "high_limit": high_limit,
            "low_limit": low_limit,
            "paused": bool(tick.get("paused", False)),
        }

    async def get_history(self, payload: Dict) -> Dict:
        frequency = str(payload.get("frequency") or payload.get("period") or "1d").lower()
        if frequency in {"1m", "1min", "minute", "min"}:
            history_key = "1m"
        else:
            history_key = "1d"
        fields = [str(item) for item in (payload.get("fields") or _DEFAULT_HISTORY_FIELDS)]
        rows = list(self._history_rows.get(history_key) or [])
        count = _as_int(payload.get("count"), 0)
        if count > 0:
            rows = rows[-count:]
        return {
            "dtype": "dataframe",
            "columns": fields,
            "records": [[row.get(field) for field in fields] for row in rows],
        }

    async def get_snapshot(self, payload: Dict) -> Dict:
        symbol = _normalize_security(payload.get("security"))
        tick = self._tick_for(symbol)
        return {
            "sid": tick["sid"],
            "last_price": tick["last_price"],
            "dt": tick["dt"],
        }

    async def get_live_current(self, payload: Dict) -> Dict:
        symbol = _normalize_security(payload.get("security"))
        return self._live_snapshot_for(symbol)

    async def get_trade_days(self, payload: Dict) -> Dict:
        count = _as_int(payload.get("count"), len(self._trade_days))
        values = self._trade_days[-count:] if count > 0 else list(self._trade_days)
        return {"dtype": "list", "values": values}

    async def get_security_info(self, payload: Dict) -> Dict:
        security = _normalize_security(payload.get("security"))
        sec_type = _infer_security_type(security)
        info = {
            "code": security,
            "display_name": _stub_display_name(security),
            "name": str(security).split(".", 1)[0] if security else "",
            "type": sec_type,
            "subtype": _infer_security_subtype(security),
            "qmt_code": _to_qmt_code(security),
            "start_date": "2005-04-08",
            "end_date": "2035-12-31",
            "parent": str(security).split(".", 1)[1] if "." in security else "",
        }
        return _dict_payload(info)

    async def ensure_cache(self, payload: Dict) -> Dict:
        return {"ok": True}

    async def get_current_tick(self, symbol: str) -> Optional[Dict]:
        return self._tick_for(_normalize_security(symbol))


class StubBrokerAdapter(RemoteBrokerAdapter):
    def __init__(self, router: AccountRouter):
        self.account_router = router
        self._orders: Dict[str, List[Dict]] = {}
        self._trades: Dict[str, List[Dict]] = {}
        self._positions: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._accounts: Dict[str, Dict[str, float]] = {}
        self._scenario_by_security: Dict[str, List[Dict[str, Any]]] = {}

    async def start(self) -> None:  # pragma: no cover - no-op
        for account in self.account_router.list_accounts():
            self._orders[account.config.key] = []
            self._trades[account.config.key] = []
            self._positions[account.config.key] = {}
            self._accounts[account.config.key] = {
                "available_cash": 1_000_000.0,
                "transferable_cash": 1_000_000.0,
                "frozen_cash": 0.0,
                "market_value": 0.0,
                "total_value": 1_000_000.0,
                "total_asset": 1_000_000.0,
            }

    async def stop(self) -> None:  # pragma: no cover - no-op
        return None

    def _orders_for(self, account: AccountContext) -> List[Dict]:
        return self._orders.setdefault(account.config.key, [])

    def _trades_for(self, account: AccountContext) -> List[Dict]:
        return self._trades.setdefault(account.config.key, [])

    def _positions_for(self, account: AccountContext) -> Dict[str, Dict[str, Any]]:
        return self._positions.setdefault(account.config.key, {})

    def _account_state_for(self, account: AccountContext) -> Dict[str, float]:
        return self._accounts.setdefault(
            account.config.key,
            {
                "available_cash": 1_000_000.0,
                "transferable_cash": 1_000_000.0,
                "frozen_cash": 0.0,
                "market_value": 0.0,
                "total_value": 1_000_000.0,
                "total_asset": 1_000_000.0,
            },
        )

    def _recalculate_account_totals(self, account: AccountContext) -> None:
        state = self._account_state_for(account)
        market_value = 0.0
        for row in self._positions_for(account).values():
            last_price = _as_float(_first_present(row.get("last_price"), row.get("current_price"), row.get("price")))
            amount = _as_int(_first_present(row.get("amount"), row.get("volume")))
            row.setdefault("name", _stub_display_name(_normalize_security(row.get("security"))))
            row.setdefault("current_price", last_price)
            row["market_value"] = row["position_value"] = round(last_price * amount, 2)
            market_value += row["market_value"]
        state["market_value"] = round(market_value, 2)
        state["transferable_cash"] = round(state.get("available_cash", 0.0), 2)
        state["total_value"] = round(state.get("available_cash", 0.0) + state.get("frozen_cash", 0.0) + market_value, 2)
        state["total_asset"] = state["total_value"]

    def _account_snapshot(self, account: AccountContext) -> Dict[str, Any]:
        state = dict(self._account_state_for(account))
        positions = [dict(row) for row in self._positions_for(account).values()]
        return {
            "account_id": account.config.account_id,
            "account_type": account.config.account_type,
            "available_cash": state.get("available_cash", 0.0),
            "transferable_cash": state.get("transferable_cash", 0.0),
            "frozen_cash": state.get("frozen_cash", 0.0),
            "market_value": state.get("market_value", 0.0),
            "positions": positions,
            "total_value": state.get("total_value", 0.0),
            "total_asset": state.get("total_asset", 0.0),
        }

    def _scenario_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        scenario = payload.get("stub_scenario")
        if isinstance(payload.get("meta"), dict) and isinstance(payload["meta"].get("stub_scenario"), dict):
            scenario = payload["meta"]["stub_scenario"]
        if scenario in (None, "", {}):
            security = str(payload.get("security") or "").strip().upper()
            queued = self._scenario_by_security.get(security) or []
            if queued:
                scenario = queued.pop(0)
                if not queued:
                    self._scenario_by_security.pop(security, None)
        if isinstance(scenario, str):
            return {"status": scenario}
        if isinstance(scenario, dict):
            return dict(scenario)
        return {}

    def _apply_terminal_execution(
        self,
        *,
        account: AccountContext,
        order: Dict[str, Any],
        status: str,
        filled: int,
        traded_price: float,
        commission: float,
        tax: float,
    ) -> None:
        status = _normalize_status(status)
        state = self._account_state_for(account)
        positions = self._positions_for(account)
        amount = _as_int(order.get("amount"))
        remaining = max(amount - filled, 0)
        deal_balance = round(traded_price * filled, 2)
        reserved_cash = _as_float(order.get("frozen_cash"))
        reserved_amount = _as_int(order.get("frozen_amount"))
        order["filled"] = filled
        order["traded_volume"] = filled
        order["status"] = status
        order["raw_status"] = _raw_status_for(status)
        order["remaining"] = remaining
        order["price"] = traded_price if filled > 0 else 0.0
        order["traded_price"] = traded_price if filled > 0 else None
        order["avg_price"] = traded_price if filled > 0 else None
        order["avg_cost"] = traded_price if filled > 0 else None
        order["deal_balance"] = deal_balance if filled > 0 else 0.0
        order["commission_fee"] = commission
        order["commission"] = commission
        order["tax"] = tax
        order["frozen_cash"] = 0.0
        order["frozen_amount"] = 0
        if order["side"] == "BUY":
            if reserved_cash > 0:
                state["frozen_cash"] = round(max(state.get("frozen_cash", 0.0) - reserved_cash, 0.0), 2)
                state["available_cash"] = round(
                    state.get("available_cash", 0.0) + reserved_cash - deal_balance - commission - tax,
                    2,
                )
            else:
                state["available_cash"] = round(state.get("available_cash", 0.0) - deal_balance - commission - tax, 2)
            if filled > 0:
                position = positions.get(order["security"]) or {
                    "security": order["security"],
                    "amount": 0,
                    "available_amount": 0,
                    "closeable_amount": 0,
                    "can_use_volume": 0,
                    "avg_cost": 0.0,
                    "last_price": traded_price,
                }
                old_amount = _as_int(position.get("amount"))
                old_cost = _as_float(position.get("avg_cost"))
                new_amount = old_amount + filled
                weighted_cost = traded_price if old_amount <= 0 else ((old_amount * old_cost) + deal_balance) / max(new_amount, 1)
                position.update(
                    {
                        "amount": new_amount,
                        "available_amount": _as_int(position.get("available_amount")),
                        "closeable_amount": _as_int(position.get("closeable_amount")),
                        "can_use_volume": _as_int(position.get("can_use_volume")),
                        "avg_cost": round(weighted_cost, 6),
                        "last_price": traded_price,
                        "current_price": traded_price,
                    }
                )
                positions[order["security"]] = position
        else:
            state["available_cash"] = round(state.get("available_cash", 0.0) + deal_balance - commission - tax, 2)
            position = positions.get(order["security"]) or {
                "security": order["security"],
                "amount": 0,
                "available_amount": 0,
                "closeable_amount": 0,
                "can_use_volume": 0,
                "avg_cost": traded_price,
                "last_price": traded_price,
            }
            new_amount = max(_as_int(position.get("amount")) - filled, 0)
            current_available = _as_int(position.get("available_amount"))
            current_frozen = _as_int(position.get("frozen_volume"))
            if reserved_amount > 0:
                new_available = min(current_available + max(reserved_amount - filled, 0), new_amount)
                new_frozen = max(current_frozen - reserved_amount, 0)
            else:
                new_available = min(current_available, new_amount)
                new_frozen = max(current_frozen - filled, 0)
            position.update(
                {
                    "amount": new_amount,
                    "available_amount": new_available,
                    "closeable_amount": new_available,
                    "can_use_volume": new_available,
                    "frozen_volume": new_frozen,
                    "last_price": traded_price,
                    "current_price": traded_price,
                }
            )
            positions[order["security"]] = position
        if filled > 0:
            self._trades_for(account).append(
                {
                    "trade_id": f"{order['order_id']}-{filled}",
                    "order_id": order["order_id"],
                    "security": order["security"],
                    "amount": filled,
                    "price": traded_price,
                    "traded_price": traded_price,
                    "trade_price": traded_price,
                    "deal_balance": deal_balance,
                    "commission_fee": commission,
                    "commission": commission,
                    "tax": tax,
                    "time": "2025-01-01 09:31:00",
                }
            )
        self._recalculate_account_totals(account)

    def _apply_open_reservation(self, *, account: AccountContext, order: Dict[str, Any]) -> None:
        state = self._account_state_for(account)
        positions = self._positions_for(account)
        if order["side"] == "BUY":
            freeze_cash = round(_as_float(order.get("order_price")) * _as_int(order.get("amount")), 2)
            state["available_cash"] = round(state.get("available_cash", 0.0) - freeze_cash, 2)
            state["frozen_cash"] = round(state.get("frozen_cash", 0.0) + freeze_cash, 2)
            order["frozen_cash"] = freeze_cash
        else:
            freeze_amount = _as_int(order.get("amount"))
            position = positions.get(order["security"]) or {
                "security": order["security"],
                "amount": 0,
                "available_amount": 0,
                "closeable_amount": 0,
                "can_use_volume": 0,
                "avg_cost": _as_float(order.get("order_price")),
                "last_price": _as_float(order.get("order_price")),
            }
            new_available = max(_as_int(position.get("available_amount")) - freeze_amount, 0)
            position.update(
                {
                    "available_amount": new_available,
                    "closeable_amount": new_available,
                    "can_use_volume": new_available,
                    "frozen_volume": _as_int(position.get("frozen_volume")) + freeze_amount,
                }
            )
            positions[order["security"]] = position
            order["frozen_amount"] = freeze_amount
        self._recalculate_account_totals(account)

    async def get_account_info(self, account: AccountContext, payload: Optional[Dict] = None) -> Dict:
        _ = payload
        self._recalculate_account_totals(account)
        return _dict_payload(self._account_snapshot(account))

    async def get_positions(self, account: AccountContext, payload: Optional[Dict] = None) -> List[Dict]:
        _ = payload
        self._recalculate_account_totals(account)
        return [dict(row) for row in self._positions_for(account).values()]

    async def list_orders(self, account: AccountContext, filters: Optional[Dict] = None) -> List[Dict]:
        rows = list(self._orders_for(account))
        if filters and filters.get("order_id"):
            rows = [row for row in rows if str(row.get("order_id")) == str(filters["order_id"])]
        if filters and filters.get("security"):
            rows = [row for row in rows if str(row.get("security")) == str(filters["security"])]
        return [dict(row) for row in rows]

    async def list_trades(self, account: AccountContext, filters: Optional[Dict] = None) -> List[Dict]:
        rows = list(self._trades_for(account))
        if filters and filters.get("order_id"):
            rows = [row for row in rows if str(row.get("order_id")) == str(filters["order_id"])]
        if filters and filters.get("security"):
            rows = [row for row in rows if str(row.get("security")) == str(filters["security"])]
        return [dict(row) for row in rows]

    async def get_order_status(self, account: AccountContext, order_id: Optional[str] = None, payload: Optional[Dict] = None) -> Dict:
        if not order_id and payload:
            order_id = payload.get("order_id")
        for order in self._orders_for(account):
            if order_id and order["order_id"] == order_id:
                return order
        return {}

    async def place_order(self, account: AccountContext, payload: Dict) -> Dict:
        scenario = self._scenario_payload(payload)
        side = str(payload.get("side") or "BUY").upper()
        style = payload.get("style") or {}
        style_type = "market" if bool(payload.get("market")) else str(style.get("type") or "limit").lower()
        order_price = _as_float(_first_present(style.get("price"), style.get("protect_price"), payload.get("price")))
        amount = _as_int(_first_present(payload.get("amount"), payload.get("volume")))
        status = _normalize_status(scenario.get("status") or "open")
        order_idx = len(self._orders_for(account)) + 1
        order = {
            "order_id": f"stub-{order_idx}",
            "security": payload.get("security"),
            "amount": amount,
            "filled": 0,
            "traded_volume": 0,
            "status": status,
            "side": side,
            "style_type": style_type,
            "style": style_type,
            "order_price": order_price,
            "price": 0.0,
            "order_type": 23,
            "is_buy": side == "BUY",
            "commission_fee": 0.0,
            "commission": 0.0,
            "tax": 0.0,
            "deal_balance": 0.0,
            "frozen_cash": 0.0,
            "frozen_amount": 0,
            "order_remark": payload.get("order_remark") or payload.get("remark") or "bullet-trade",
            "strategy_name": payload.get("strategy_name") or "bullet-trade",
            "status_msg": str(scenario.get("status_msg") or ""),
            "price_type": _price_type_for(style_type),
            "order_time": _as_int(scenario.get("order_time"), int(time.time())),
            "order_sysid": str(_as_int(scenario.get("order_sysid"), 70000 + order_idx)),
            "raw_status": _raw_status_for(status),
        }
        self._orders_for(account).append(order)
        if status in {"filled", "partly_canceled", "partly_filled", "rejected"}:
            filled_default = amount if status == "filled" else 0
            filled = min(_as_int(scenario.get("filled"), filled_default), amount)
            traded_price = _as_float(_first_present(scenario.get("traded_price"), scenario.get("price"), order_price))
            commission = _as_float(_first_present(scenario.get("commission_fee"), scenario.get("commission")), 0.0)
            tax = _as_float(scenario.get("tax"), 0.0)
            self._apply_terminal_execution(
                account=account,
                order=order,
                status=status,
                filled=filled,
                traded_price=traded_price,
                commission=commission,
                tax=tax,
            )
        elif bool(scenario.get("reserve_on_open")):
            self._apply_open_reservation(account=account, order=order)
        return order

    async def cancel_order(self, account: AccountContext, order_id: Optional[str] = None, payload: Optional[Dict] = None) -> Dict:
        if not order_id and payload:
            order_id = payload.get("order_id")
        orders = self._orders_for(account)
        for order in orders:
            if order_id and order["order_id"] == order_id:
                status = _normalize_status(order.get("status"))
                if status not in {"filled", "partly_canceled", "canceled", "rejected"}:
                    order["status"] = "canceled"
                    order["raw_status"] = _raw_status_for("canceled")
                    state = self._account_state_for(account)
                    if _as_float(order.get("frozen_cash")) > 0:
                        release_cash = _as_float(order.get("frozen_cash"))
                        state["available_cash"] = round(state.get("available_cash", 0.0) + release_cash, 2)
                        state["frozen_cash"] = round(max(state.get("frozen_cash", 0.0) - release_cash, 0.0), 2)
                        order["frozen_cash"] = 0.0
                    if _as_int(order.get("frozen_amount")) > 0:
                        position = self._positions_for(account).get(order["security"])
                        if position is not None:
                            release_amount = _as_int(order.get("frozen_amount"))
                            available = min(_as_int(position.get("available_amount")) + release_amount, _as_int(position.get("amount")))
                            position["available_amount"] = available
                            position["closeable_amount"] = available
                            position["can_use_volume"] = available
                            position["frozen_volume"] = max(_as_int(position.get("frozen_volume")) - release_amount, 0)
                        order["frozen_amount"] = 0
                    self._recalculate_account_totals(account)
                snapshot = dict(order)
                return {
                    "dtype": "dict",
                    "value": True,
                    "status": snapshot.get("status"),
                    "raw_status": snapshot.get("raw_status"),
                    "last_snapshot": snapshot,
                    "timed_out": False,
                }
        return {"dtype": "dict", "value": False, "timed_out": False}


def build_stub_bundle(config: ServerConfig, router: AccountRouter) -> AdapterBundle:
    if not router.list_accounts():
        router._accounts["default"] = AccountContext(AccountConfig(key="default", account_id="demo"))
    return AdapterBundle(data_adapter=StubDataAdapter(), broker_adapter=StubBrokerAdapter(router))


register_adapter("stub", build_stub_bundle)
