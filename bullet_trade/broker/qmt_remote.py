from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, List, Optional

from .base import BrokerBase
from ..remote import RemoteQmtConnection
from ..core.globals import log


def _env(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(key, default)


class RemoteQmtBroker(BrokerBase):
    """
    使用 RemoteQmtConnection 与 bullet-trade server 交互的券商实现。
    """

    def __init__(self, account_id: str, account_type: str = "stock", config: Optional[Dict[str, Any]] = None):
        super().__init__(account_id, account_type)
        self.config = config or {}
        host = self.config.get("host") or _env("QMT_SERVER_HOST", "127.0.0.1")
        port = int(self.config.get("port") or _env("QMT_SERVER_PORT", 58620))
        token = self.config.get("token") or _env("QMT_SERVER_TOKEN")
        if not token:
            raise RuntimeError("缺少 QMT_SERVER_TOKEN")
        tls_cert = self.config.get("tls_cert") or _env("QMT_SERVER_TLS_CERT")
        tls_enabled = bool(tls_cert)
        self.account_key = self.config.get("account_key") or _env("QMT_SERVER_ACCOUNT_KEY")
        self.sub_account_id = self.config.get("sub_account_id") or _env("QMT_SERVER_SUB_ACCOUNT")
        self._connection = RemoteQmtConnection(host, port, token, tls_cert=tls_cert, tls_enabled=tls_enabled)
        self._last_warning: Optional[str] = None

    def connect(self) -> bool:
        self._connection.start()
        self._connected = True
        return True

    def disconnect(self) -> bool:
        self._connected = False
        self._connection.close()
        return True

    def get_account_info(self) -> Dict[str, Any]:
        payload = self._base_payload()
        resp = self._connection.request("broker.account", payload)
        return resp.get("value") or resp

    def get_positions(self) -> List[Dict[str, Any]]:
        payload = self._base_payload()
        resp = self._connection.request("broker.positions", payload)
        return resp or []

    def get_orders(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
        status: Optional[object] = None,
        from_broker: bool = False,
    ) -> List[Dict[str, Any]]:
        payload = self._base_payload()
        if order_id:
            payload["order_id"] = order_id
        if security:
            payload["security"] = security
        if status is not None:
            payload["status"] = getattr(status, "value", status)
        if from_broker:
            payload["from_broker"] = True
        resp = self._connection.request("broker.orders", payload)
        return resp or []

    def get_open_orders(self) -> List[Dict[str, Any]]:
        orders = self.get_orders()
        if not orders:
            return []
        open_states = {"new", "open", "filling", "canceling"}
        return [row for row in orders if str(row.get("status")) in open_states]

    def get_trades(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        payload = self._base_payload()
        if order_id:
            payload["order_id"] = order_id
        if security:
            payload["security"] = security
        resp = self._connection.request("broker.trades", payload)
        return resp or []

    async def buy(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        wait_timeout: Optional[float] = None,
        remark: Optional[str] = None,
        *,
        market: bool = False,
    ) -> str:
        return await self._place_order("BUY", security, amount, price, wait_timeout, remark, market)

    async def sell(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        wait_timeout: Optional[float] = None,
        remark: Optional[str] = None,
        *,
        market: bool = False,
    ) -> str:
        return await self._place_order("SELL", security, amount, price, wait_timeout, remark, market)

    async def cancel_order(self, order_id: str) -> bool:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, self._cancel_sync, order_id)
        value = result.get("value") if isinstance(result, dict) else None
        if isinstance(value, bool):
            ok = value
        elif value is None:
            ok = bool(result.get("success", True)) if isinstance(result, dict) else True
        else:
            ok = bool(value)
        if isinstance(result, dict) and result.get("timed_out"):
            status = result.get("status") or result.get("raw_status") or "unknown"
            log.warning(f"撤单等待超时: order_id={order_id}, status={status}")
        return ok

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._order_status_sync, order_id)

    def supports_orders_sync(self) -> bool:
        return True

    def supports_account_sync(self) -> bool:
        return True

    def sync_orders(self) -> List[Dict[str, Any]]:
        payload = self._base_payload()
        return self._connection.request("broker.orders", payload)

    def sync_account(self) -> Dict[str, Any]:
        return self.get_account_info()

    def _place_order(
        self,
        side: str,
        security: str,
        amount: int,
        price: Optional[float],
        wait_timeout: Optional[float],
        remark: Optional[str] = None,
        market: bool = False,
    ) -> asyncio.Future:
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(
            None, self._place_order_sync, side, security, amount, price, wait_timeout, remark, market
        )

    def _place_order_sync(
        self,
        side: str,
        security: str,
        amount: int,
        price: Optional[float],
        wait_timeout: Optional[float],
        remark: Optional[str] = None,
        market: bool = False,
    ) -> str:
        self._last_warning = None
        payload = self._base_payload()
        effective_market = bool(market or price is None)
        style = {"type": "market" if effective_market else "limit"}
        if price is None and effective_market:
            price = self._infer_price(security)
            if price is not None:
                style["protect_price"] = price
        if price is not None:
            if effective_market:
                style["protect_price"] = price
            else:
                style["price"] = price
        if price is None and not effective_market:
            raise ValueError("限价单缺少价格，请提供 price 或将 market 设为 True")
        if wait_timeout is not None:
            payload["wait_timeout"] = wait_timeout
        if effective_market:
            payload["market"] = True
        if remark:
            payload["order_remark"] = remark
        payload.update(
            {
                "security": security,
                "amount": amount,
                "side": side,
                "style": style,
            }
        )
        resp = self._connection.request("broker.place_order", payload)
        warning = None
        try:
            if isinstance(resp, dict):
                warning = resp.get("warning")
        except Exception:
            warning = None
        if warning:
            log.warning(warning)
            try:
                print(f"[远程警告] {warning}")
            except Exception:
                pass
            self._last_warning = str(warning)
        order_id = resp.get("order_id")
        if not order_id:
            raise RuntimeError(f"远程券商未返回 order_id: {resp}")
        return order_id

    def _infer_price(self, security: str) -> Optional[float]:
        """
        市价单时，尝试从远程数据接口取最新价并转换为限价单。
        """
        try:
            snap = self._connection.request("data.snapshot", {"security": security})
            last_price = snap.get("last_price") or snap.get("lastPrice")
            if last_price is None:
                # 回退到最近一条历史行情
                hist = self._connection.request("data.history", {"security": security, "count": 1, "frequency": "1m"})
                records = hist.get("records") or []
                if records:
                    last_price = records[-1][-1] if isinstance(records[-1], (list, tuple)) else None
            if last_price is None:
                return None
            return float(last_price)
        except Exception:
            return None

    def _cancel_sync(self, order_id: str) -> Dict:
        payload = self._base_payload()
        payload["order_id"] = order_id
        return self._connection.request("broker.cancel_order", payload)

    def _order_status_sync(self, order_id: str) -> Dict:
        payload = self._base_payload()
        payload["order_id"] = order_id
        return self._connection.request("broker.order_status", payload)

    def _base_payload(self) -> Dict[str, Any]:
        payload = {"account_key": self.account_key, "sub_account_id": self.sub_account_id}
        return payload
