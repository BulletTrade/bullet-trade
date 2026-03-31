from __future__ import annotations

import asyncio
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


class StubDataAdapter(RemoteDataAdapter):
    def __init__(self) -> None:
        self._ticks: Dict[str, Dict] = {}

    async def get_history(self, payload: Dict) -> Dict:
        return {"dtype": "dataframe", "columns": ["datetime", "close"], "records": [["2025-01-01", 10.0]]}

    async def get_snapshot(self, payload: Dict) -> Dict:
        code = payload.get("security")
        return self._ticks.get(code, {"sid": code, "last_price": 10.0, "dt": "2025-01-01 09:30:00"})

    async def get_trade_days(self, payload: Dict) -> Dict:
        return {"values": ["2025-01-01"]}

    async def get_security_info(self, payload: Dict) -> Dict:
        security = payload.get("security")
        info = {
            "code": security,
            "display_name": f"Stub {security}",
            "name": str(security).split(".", 1)[0] if security else "",
            "type": "stock",
        }
        return {"dtype": "dict", "value": info, **info}

    async def ensure_cache(self, payload: Dict) -> Dict:
        return {"ok": True}

    async def get_current_tick(self, symbol: str) -> Optional[Dict]:
        return self._ticks.get(symbol)


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
            row["market_value"] = row["position_value"] = round(last_price * amount, 2)
            market_value += row["market_value"]
        state["market_value"] = round(market_value, 2)
        state["transferable_cash"] = round(state.get("available_cash", 0.0), 2)
        state["total_value"] = round(state.get("available_cash", 0.0) + state.get("frozen_cash", 0.0) + market_value, 2)
        state["total_asset"] = state["total_value"]

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
        order["remaining"] = remaining
        order["price"] = traded_price if filled > 0 else order.get("order_price")
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
        return {"value": dict(self._account_state_for(account))}

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
        style_type = str(style.get("type") or "limit").lower()
        order_price = _as_float(_first_present(style.get("price"), style.get("protect_price"), payload.get("price")))
        amount = _as_int(_first_present(payload.get("amount"), payload.get("volume")))
        order = {
            "order_id": f"stub-{len(self._orders_for(account)) + 1}",
            "security": payload.get("security"),
            "amount": amount,
            "filled": 0,
            "traded_volume": 0,
            "status": str(scenario.get("status") or "open"),
            "side": side,
            "style_type": style_type,
            "style": style_type,
            "order_price": order_price,
            "price": order_price,
            "commission_fee": 0.0,
            "commission": 0.0,
            "tax": 0.0,
            "deal_balance": 0.0,
            "frozen_cash": 0.0,
            "frozen_amount": 0,
            "order_remark": payload.get("order_remark") or payload.get("remark"),
        }
        self._orders_for(account).append(order)
        status = str(order["status"] or "open")
        if status in {"filled", "partly_canceled", "rejected"}:
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
                order["status"] = "cancelled"
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
        return {"value": True}


def build_stub_bundle(config: ServerConfig, router: AccountRouter) -> AdapterBundle:
    if not router.list_accounts():
        router._accounts["default"] = AccountContext(AccountConfig(key="default", account_id="demo"))
    return AdapterBundle(data_adapter=StubDataAdapter(), broker_adapter=StubBrokerAdapter(router))


register_adapter("stub", build_stub_bundle)
