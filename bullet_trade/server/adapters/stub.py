from __future__ import annotations

import asyncio
from typing import Dict, List, Optional

from .base import AccountRouter, AdapterBundle, RemoteBrokerAdapter, RemoteDataAdapter, AccountContext
from ..config import ServerConfig, AccountConfig
from . import register_adapter


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
        return {"value": {"symbol": payload.get("security")}}

    async def ensure_cache(self, payload: Dict) -> Dict:
        return {"ok": True}

    async def get_current_tick(self, symbol: str) -> Optional[Dict]:
        return self._ticks.get(symbol)


class StubBrokerAdapter(RemoteBrokerAdapter):
    def __init__(self, router: AccountRouter):
        self.account_router = router
        self._orders: Dict[str, List[Dict]] = {}

    async def start(self) -> None:  # pragma: no cover - no-op
        for account in self.account_router.list_accounts():
            self._orders[account.config.key] = []

    async def stop(self) -> None:  # pragma: no cover - no-op
        return None

    def _orders_for(self, account: AccountContext) -> List[Dict]:
        return self._orders.setdefault(account.config.key, [])

    async def get_account_info(self, account: AccountContext, payload: Optional[Dict] = None) -> Dict:
        return {"value": {"available_cash": 1_000_000, "total_value": 1_000_000}}

    async def get_positions(self, account: AccountContext, payload: Optional[Dict] = None) -> List[Dict]:
        return []

    async def list_orders(self, account: AccountContext, filters: Optional[Dict] = None) -> List[Dict]:
        return list(self._orders_for(account))

    async def list_trades(self, account: AccountContext, filters: Optional[Dict] = None) -> List[Dict]:
        return []

    async def get_order_status(self, account: AccountContext, order_id: Optional[str] = None, payload: Optional[Dict] = None) -> Dict:
        if not order_id and payload:
            order_id = payload.get("order_id")
        for order in self._orders_for(account):
            if order_id and order["order_id"] == order_id:
                return order
        return {}

    async def place_order(self, account: AccountContext, payload: Dict) -> Dict:
        order = {
            "order_id": f"stub-{len(self._orders_for(account)) + 1}",
            "security": payload.get("security"),
            "amount": payload.get("amount"),
            "status": "open",
        }
        self._orders_for(account).append(order)
        return order

    async def cancel_order(self, account: AccountContext, order_id: Optional[str] = None, payload: Optional[Dict] = None) -> Dict:
        if not order_id and payload:
            order_id = payload.get("order_id")
        orders = self._orders_for(account)
        for order in orders:
            if order_id and order["order_id"] == order_id:
                order["status"] = "cancelled"
        return {"value": True}


def build_stub_bundle(config: ServerConfig, router: AccountRouter) -> AdapterBundle:
    if not router.list_accounts():
        router._accounts["default"] = AccountContext(AccountConfig(key="default", account_id="demo"))
    return AdapterBundle(data_adapter=StubDataAdapter(), broker_adapter=StubBrokerAdapter(router))


register_adapter("stub", build_stub_bundle)
