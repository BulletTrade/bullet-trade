"""
聚宽远程辅助模块（短连接版）

使用方法：
1. 将本文件复制到聚宽研究环境根目录；
2. 在策略里：
   import bullet_trade_jq_remote_helper as bt
   bt.configure(host='你的IP', token='你的token', port=58620, account_key='main', sub_account_id='demo@main')
   acct = bt.get_account()
   oid = bt.order('000001.XSHE', amount=100, price=None, side='BUY', wait_timeout=10)
   bt.cancel_order(oid)

特点：
- 每次调用都会重新建立 TCP 连接，适合聚宽频繁重启。
- 自动补价：市价单会尝试读取 snapshot/1m K 线补全限价。
- 支持同步/异步：wait_timeout>0 时轮询订单状态，否则立即返回。
- 提供 account/positions/order_status/orders/cancel/order_value/order_target 等常见聚宽风格 API。
"""

import json
import os
import socket
import ssl
import struct
import time
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

_CLIENT: Optional["_ShortLivedClient"] = None
_DATA_CLIENT: Optional["RemoteDataClient"] = None
_BROKER_CLIENT: Optional["RemoteBrokerClient"] = None


def configure(
    host: str,
    token: str,
    *,
    port: int = 58620,
    account_key: Optional[str] = None,
    sub_account_id: Optional[str] = None,
    tls_cert: Optional[str] = None,
    retries: int = 2,
    retry_interval: float = 0.5,
    rpc_timeout: float = 60.0,
) -> None:
    """
    初始化远程访问参数；聚宽环境无法常驻进程，因此每次调用都会短连接访问。
    """
    global _CLIENT, _DATA_CLIENT, _BROKER_CLIENT
    _CLIENT = _ShortLivedClient(
        host,
        port,
        token,
        tls_cert=tls_cert,
        retries=retries,
        retry_interval=retry_interval,
        rpc_timeout=rpc_timeout,
    )
    _DATA_CLIENT = RemoteDataClient(_CLIENT)
    _BROKER_CLIENT = RemoteBrokerClient(
        _CLIENT,
        account_key=account_key,
        sub_account_id=sub_account_id,
    )


def get_data_client() -> "RemoteDataClient":
    if not _DATA_CLIENT:
        raise RuntimeError("尚未调用 configure() 初始化")
    return _DATA_CLIENT


def get_broker_client() -> "RemoteBrokerClient":
    if not _BROKER_CLIENT:
        raise RuntimeError("尚未调用 configure() 初始化")
    return _BROKER_CLIENT


# --------- 数据客户端 ----------
class RemoteDataClient:
    def __init__(self, client: "_ShortLivedClient") -> None:
        self._client = client

    def get_price(self, security: str, **kwargs) -> pd.DataFrame:
        payload = {"security": security}
        payload.update(kwargs)
        resp = self._client.request("data.history", payload)
        return _df_from_payload(resp)

    def get_trade_days(self, start: str, end: str) -> List[pd.Timestamp]:
        resp = self._client.request("data.trade_days", {"start": start, "end": end})
        values = resp.get("value") or resp.get("values") or []
        return [pd.to_datetime(v) for v in values]

    def get_snapshot(self, security: str) -> Dict[str, Any]:
        return self._client.request("data.snapshot", {"security": security})

    def get_last_price(self, security: str) -> Optional[float]:
        snap = self.get_snapshot(security)
        price = snap.get("last_price") or snap.get("lastPrice") or snap.get("price")
        if price is not None:
            try:
                return float(price)
            except Exception:
                return None
        hist = self._client.request("data.history", {"security": security, "count": 1, "frequency": "1m"})
        records = hist.get("records") or []
        if records and isinstance(records[-1], (list, tuple)) and len(records[-1]) >= 2:
            try:
                return float(records[-1][-1])
            except Exception:
                return None
        return None


# --------- 券商客户端 ----------
class RemoteOrder:
    def __init__(self, order_id: str, status: str, security: str, amount: int, price: Optional[float] = None):
        self.order_id = order_id
        self.status = status
        self.security = security
        self.amount = amount
        self.price = price


class RemotePosition:
    def __init__(
        self,
        security: str,
        amount: int,
        avg_cost: float,
        market_value: float,
        available: Optional[int] = None,
        frozen: Optional[int] = None,
        market: Optional[str] = None,
    ):
        self.security = security
        self.amount = amount
        self.avg_cost = avg_cost
        self.market_value = market_value
        self.available = available if available is not None else amount
        self.frozen = frozen if frozen is not None else 0
        self.market = market


class RemoteAccount:
    def __init__(self, available_cash: float, total_value: float):
        self.available_cash = available_cash
        self.total_value = total_value


class RemoteBrokerClient:
    def __init__(
        self,
        client: "_ShortLivedClient",
        *,
        account_key: Optional[str] = None,
        sub_account_id: Optional[str] = None,
    ) -> None:
        self._client = client
        self.account_key = account_key
        self.sub_account_id = sub_account_id
        self._data_client: Optional[RemoteDataClient] = None

    def bind_data_client(self, data_client: RemoteDataClient) -> None:
        self._data_client = data_client

    # ----- 聚宽风格入口 -----
    def order(self, security: str, amount: int, price: Optional[float] = None, side: Optional[str] = None, wait_timeout: float = 0) -> str:
        if amount == 0:
            return ""
        actual_side = side or ("BUY" if amount > 0 else "SELL")
        qty = abs(int(amount))
        order = self._place_order(security, qty, price, actual_side, wait_timeout=wait_timeout)
        return order.order_id

    def order_value(self, security: str, value: float, price: Optional[float] = None, wait_timeout: float = 0) -> str:
        if value == 0:
            return ""
        p = price or self._infer_price(security)
        if not p:
            raise RuntimeError("无法获取价格，无法按市值下单")
        qty = int(value / p)
        side = "BUY" if value > 0 else "SELL"
        order = self._place_order(security, abs(qty), price or p, side, wait_timeout=wait_timeout)
        return order.order_id

    def order_target(self, security: str, target: int, price: Optional[float] = None, wait_timeout: float = 0) -> str:
        current = self._current_amount(security)
        delta = target - current
        return self.order(security, delta, price=price, wait_timeout=wait_timeout)

    def order_target_value(self, security: str, target_value: float, price: Optional[float] = None, wait_timeout: float = 0) -> str:
        p = price or self._infer_price(security)
        if not p:
            raise RuntimeError("无法获取价格，无法按目标市值下单")
        target_amount = int(target_value / p)
        return self.order_target(security, target_amount, price=p, wait_timeout=wait_timeout)

    # ----- 基础接口 -----
    def get_account(self) -> RemoteAccount:
        payload = self._base_payload()
        value = self._client.request("broker.account", payload).get("value") or {}
        return RemoteAccount(
            available_cash=float(value.get("available_cash", 0.0)),
            total_value=float(value.get("total_value", 0.0)),
        )

    def get_positions(self) -> List[RemotePosition]:
        payload = self._base_payload()
        rows = self._client.request("broker.positions", payload)
        positions = []
        for row in rows or []:
            # 解析数量和可用数量
            amount = int(row.get("amount") or 0)
            # 优先读取 closeable_amount（服务端 QMT 返回的字段名）
            available = int(
                row.get("available")
                or row.get("closeable_amount")
                or row.get("can_sell_amount")
                or row.get("sellable")
                or row.get("can_use_amount")
                or row.get("current_amount")
                or row.get("qty")
                or row.get("volume")
                or row.get("position", 0)
            )
            # frozen 优先读取服务端返回值，如果没有则用 amount - available 计算
            frozen_raw = row.get("frozen") or row.get("lock_amount")
            frozen = int(frozen_raw) if frozen_raw is not None else (amount - available)
            positions.append(
                RemotePosition(
                    security=row.get("security"),
                    amount=amount,
                    avg_cost=float(row.get("avg_cost") or 0.0),
                    market_value=float(row.get("market_value") or 0.0),
                    available=available,
                    frozen=frozen,
                    market=row.get("market"),
                )
            )
        return positions

    def get_open_orders(self) -> List[Dict[str, Any]]:
        payload = self._base_payload()
        return self._client.request("broker.orders", payload) or []

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        payload = self._base_payload()
        payload["order_id"] = order_id
        return self._client.request("broker.order_status", payload)

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        payload = self._base_payload()
        payload["order_id"] = order_id
        return self._client.request("broker.cancel_order", payload)

    # ----- 内部 -----
    def _place_order(self, security: str, amount: int, price: Optional[float], side: str, wait_timeout: float) -> RemoteOrder:
        payload = self._base_payload()
        style = {"type": "limit"}
        market_flag = False
        if price is None:
            price = self._infer_price(security)
            market_flag = True
            if price is not None:
                style = {"type": "market", "protect_price": price}
            else:
                style = {"type": "market"}
        if price is not None and not market_flag:
            style["price"] = price
        payload.update(
            {
                "security": security,
                "side": side,
                "amount": amount,
                "style": style,
            }
        )
        resp = self._client.request("broker.place_order", payload)
        warning = None
        try:
            if isinstance(resp, dict):
                warning = resp.get("warning")
        except Exception:
            warning = None
        if warning:
            print(f"[远程警告] {warning}")
        order = RemoteOrder(
            order_id=resp.get("order_id"),
            status=resp.get("status", "submitted"),
            security=security,
            amount=amount,
            price=price,
        )
        if wait_timeout and order.order_id:
            self._wait_order(order.order_id, wait_timeout)
        return order

    def _wait_order(self, order_id: str, timeout: float) -> None:
        start = time.time()
        interval = 1.0
        while time.time() - start < timeout:
            try:
                status = self.get_order_status(order_id)
                st = str(status.get("status") or "").lower()
                if st in {"filled", "cancelled", "canceled", "rejected", "partly_canceled"}:
                    return
            except Exception:
                pass
            time.sleep(interval)

    def _current_amount(self, security: str) -> int:
        for pos in self.get_positions():
            if pos.security == security:
                return int(pos.amount)
        return 0

    def _infer_price(self, security: str) -> Optional[float]:
        if self._data_client:
            return self._data_client.get_last_price(security)
        return None

    def _base_payload(self) -> Dict[str, Any]:
        return {"account_key": self.account_key, "sub_account_id": self.sub_account_id}


# --------- TCP 客户端 ----------
class _ShortLivedClient:
    """
    简单的 TCP+JSON 客户端：每次请求都会重新连接、握手、发送请求并等待响应；失败会按配置重试。
    """

    def __init__(
        self,
        host: str,
        port: int,
        token: str,
        *,
        tls_cert: Optional[str] = None,
        retries: int = 2,
        retry_interval: float = 0.5,
        rpc_timeout: float = 30.0,
    ):
        self.host = host
        self.port = port
        self.token = token
        self.tls_cert = tls_cert
        self.retries = max(0, retries)
        self.retry_interval = max(0.1, float(retry_interval))
        self.rpc_timeout = max(5.0, float(rpc_timeout))

    def request(self, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        attempts = self.retries + 1
        for i in range(attempts):
            sock: Optional[socket.socket] = None
            try:
                sock = socket.create_connection((self.host, self.port), timeout=10)
                if self.tls_cert:
                    context = ssl.create_default_context(cafile=self.tls_cert)
                    sock = context.wrap_socket(sock, server_hostname=self.host)
                sock.settimeout(self.rpc_timeout)
                self._send(sock, {"type": "handshake", "protocol": 1, "token": self.token, "features": []})
                ack = self._recv(sock)
                if ack.get("type") != "handshake_ack":
                    raise RuntimeError("远程服务拒绝握手")
                req_id = str(id(payload) ^ int.from_bytes(os.urandom(4), "big"))
                self._send(sock, {"type": "request", "id": req_id, "action": action, "payload": payload})
                while True:
                    message = self._recv(sock)
                    msg_type = message.get("type")
                    if msg_type == "response" and message.get("id") == req_id:
                        return message.get("payload") or {}
                    if msg_type == "error":
                        raise RuntimeError(message.get("message", "server error"))
            except Exception as exc:
                last_error = exc
                time.sleep(self.retry_interval)
            finally:
                if sock:
                    try:
                        sock.close()
                    except Exception:
                        pass
        raise last_error or RuntimeError("远程请求失败")

    @staticmethod
    def _send(sock: socket.socket, message: Dict[str, Any]) -> None:
        body = json.dumps(message, ensure_ascii=False).encode("utf-8")
        header = struct.pack(">I", len(body))
        sock.sendall(header + body)

    @staticmethod
    def _recv(sock: socket.socket) -> Dict[str, Any]:
        header = _ShortLivedClient._read_exact(sock, 4)
        size = struct.unpack(">I", header)[0]
        payload = _ShortLivedClient._read_exact(sock, size)
        return json.loads(payload.decode("utf-8"))

    @staticmethod
    def _read_exact(sock: socket.socket, size: int) -> bytes:
        buf = b""
        while len(buf) < size:
            chunk = sock.recv(size - len(buf))
            if not chunk:
                raise RuntimeError("连接中断")
            buf += chunk
        return buf


# --------- 工具函数 ----------
def _df_from_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    if not payload or payload.get("dtype") != "dataframe":
        return pd.DataFrame()
    columns = payload.get("columns") or []
    records = payload.get("records") or []
    return pd.DataFrame(records, columns=columns)


# --------- 便捷函数（JQ 兼容） ----------
def order(security: str, amount: int, price: Optional[float] = None, side: Optional[str] = None, wait_timeout: float = 0) -> str:
    return get_broker_client().order(security, amount, price=price, side=side, wait_timeout=wait_timeout)


def order_value(security: str, value: float, price: Optional[float] = None, wait_timeout: float = 0) -> str:
    return get_broker_client().order_value(security, value, price=price, wait_timeout=wait_timeout)


def order_target(security: str, target: int, price: Optional[float] = None, wait_timeout: float = 0) -> str:
    return get_broker_client().order_target(security, target, price=price, wait_timeout=wait_timeout)


def order_target_value(security: str, target_value: float, price: Optional[float] = None, wait_timeout: float = 0) -> str:
    return get_broker_client().order_target_value(security, target_value, price=price, wait_timeout=wait_timeout)


def cancel_order(order_id: str) -> Dict[str, Any]:
    return get_broker_client().cancel_order(order_id)


def get_order_status(order_id: str) -> Dict[str, Any]:
    return get_broker_client().get_order_status(order_id)


def get_open_orders() -> List[Dict[str, Any]]:
    return get_broker_client().get_open_orders()


def get_account() -> RemoteAccount:
    return get_broker_client().get_account()


def get_positions() -> List[RemotePosition]:
    return get_broker_client().get_positions()


__all__ = [
    "configure",
    "get_data_client",
    "get_broker_client",
    "order",
    "order_value",
    "order_target",
    "order_target_value",
    "cancel_order",
    "get_order_status",
    "get_open_orders",
    "get_account",
    "get_positions",
    "RemoteAccount",
    "RemoteOrder",
    "RemotePosition",
    "RemoteDataClient",
    "RemoteBrokerClient",
]
