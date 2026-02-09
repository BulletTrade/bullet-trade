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
- 服务端统一处理：最小手数/步进取整、停牌检查、价格笼子、涨跌停校验、可卖数量检查。
- 支持同步/异步：wait_timeout>0 时轮询订单状态，否则立即返回。
- 提供 account/positions/order_status/orders/cancel/order_value/order_target 等常见聚宽风格 API。
"""

import json
import hashlib
import os
import socket
import ssl
import struct
import sys
import time
import traceback
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

_CLIENT: Optional["_ShortLivedClient"] = None
_DATA_CLIENT: Optional["RemoteDataClient"] = None
_BROKER_CLIENT: Optional["RemoteBrokerClient"] = None

# 全局调试开关
_DEBUG: bool = True
HELPER_PROTOCOL_VERSION: int = 1


def _log(level: str, msg: str, *args, **kwargs):
    """
    统一的日志输出函数。
    
    所有日志都通过此函数输出，受全局 _DEBUG 开关控制。
    输出到 stderr，避免干扰 stdout。
    """
    if not _DEBUG:
        return
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    formatted_msg = msg.format(*args, **kwargs) if args or kwargs else msg
    print(f"[{timestamp}] [{level}] {formatted_msg}", file=sys.stderr)


def _warn(msg: str, *args, **kwargs):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    formatted_msg = msg.format(*args, **kwargs) if args or kwargs else msg
    print(f"[{timestamp}] [WARN] {formatted_msg}", file=sys.stderr)


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
    debug: bool = True,
) -> None:
    """
    初始化远程访问参数；聚宽环境无法常驻进程，因此每次调用都会短连接访问。
    
    Args:
        host: 服务器主机名或 IP 地址
        token: 认证令牌
        port: 服务器端口，默认 58620
        account_key: 账户键，可选
        sub_account_id: 子账户 ID，可选
        tls_cert: TLS 证书文件路径，可选
        retries: 失败重试次数，默认 2
        retry_interval: 重试间隔（秒），默认 0.5
        rpc_timeout: RPC 超时时间（秒），默认 60.0
        debug: 是否启用调试日志，默认 True
    """
    global _CLIENT, _DATA_CLIENT, _BROKER_CLIENT, _DEBUG
    
    _DEBUG = debug
    _log("INFO", "初始化远程连接: host={}, port={}, retries={}, debug={}", host, port, retries, debug)
    
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
    
    _log("INFO", "初始化完成")


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
    """
    远程订单对象。
    
    属性：
    - order_id: 订单ID
    - status: 订单状态
    - security: 证券代码
    - amount: 请求数量
    - price: 请求价格
    - actual_amount: 服务端实际执行数量（可能因最小手数/步进取整而不同）
    - actual_price: 服务端实际委托价格（市价单会由服务端计算）
    - filled: 已成交数量
    - is_buy: 是否买入
    - order_remark: 订单备注
    - strategy_name: 策略标识
    """
    def __init__(
        self,
        order_id: str,
        status: str,
        security: str,
        amount: int,
        price: Optional[float] = None,
        actual_amount: Optional[int] = None,
        actual_price: Optional[float] = None,
        filled: int = 0,
        is_buy: Optional[bool] = None,
        order_remark: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ):
        self.order_id = order_id
        self.status = status
        self.security = security
        self.amount = amount
        self.price = price
        # 服务端返回的实际执行数量和价格
        self.actual_amount = actual_amount if actual_amount is not None else amount
        self.actual_price = actual_price if actual_price is not None else price
        self.filled = filled
        self.is_buy = is_buy
        self.order_remark = order_remark
        self.strategy_name = strategy_name


class RemoteTrade:
    """
    远程成交对象（聚宽风格）。
    """
    def __init__(
        self,
        trade_id: str,
        order_id: str,
        security: str,
        amount: int,
        price: float,
        time: pd.Timestamp,
        commission: float = 0.0,
        tax: float = 0.0,
    ):
        self.trade_id = trade_id
        self.order_id = order_id
        self.security = security
        self.amount = amount
        self.price = price
        self.time = time.to_pydatetime() if isinstance(time, pd.Timestamp) else time
        self.commission = commission
        self.tax = tax


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
        """
        按数量下单。
        
        :param security: 证券代码
        :param amount: 数量（正数买入，负数卖出；如果指定了 side 则取绝对值）
        :param price: 委托价格，None 时服务端自动使用市价单
        :param side: 方向 BUY/SELL，None 时根据 amount 正负判断
        :param wait_timeout: 等待超时秒数，0 表示异步返回
        :return: 订单 ID
        
        注意：服务端会自动处理最小手数/步进取整、停牌检查、价格笼子等。
        """
        if amount == 0:
            return ""
        actual_side = side or ("BUY" if amount > 0 else "SELL")
        qty = abs(int(amount))
        # 服务端会自动处理最小手数/步进取整
        order = self._place_order(security, qty, price, actual_side, wait_timeout=wait_timeout)
        return order.order_id

    def order_value(self, security: str, value: float, price: Optional[float] = None, wait_timeout: float = 0) -> str:
        """
        按市值下单。
        
        :param security: 证券代码
        :param value: 目标市值（正数买入，负数卖出）
        :param price: 委托价格，None 时服务端自动使用市价单
        :param wait_timeout: 等待超时秒数，0 表示异步返回
        :return: 订单 ID
        
        注意：服务端会自动处理最小手数/步进取整，实际成交市值可能与请求略有偏差。
        """
        if value == 0:
            return ""
        # 获取参考价格用于计算数量
        p = price or self._infer_price(security)
        if not p:
            raise RuntimeError("无法获取价格，无法按市值下单")
        # 计算大致数量，服务端会自动按最小手数/步进取整
        qty = int(abs(value) / p)
        side = "BUY" if value > 0 else "SELL"
        order = self._place_order(security, qty, price, side, wait_timeout=wait_timeout)
        return order.order_id

    def order_target(self, security: str, target: int, price: Optional[float] = None, wait_timeout: float = 0) -> str:
        """
        调仓到目标数量。
        
        :param security: 证券代码
        :param target: 目标持仓数量
        :param price: 委托价格，None 时服务端自动使用市价单
        :param wait_timeout: 等待超时秒数，0 表示异步返回
        :return: 订单 ID（如果不需要交易则返回空字符串）
        
        注意：建议 target 为 100 的整数倍，服务端会自动取整。
        """
        current = self._current_amount(security)
        delta = target - current
        if delta == 0:
            return ""
        return self.order(security, delta, price=price, wait_timeout=wait_timeout)

    def order_target_value(self, security: str, target_value: float, price: Optional[float] = None, wait_timeout: float = 0) -> str:
        """
        调仓到目标市值。
        
        :param security: 证券代码
        :param target_value: 目标持仓市值
        :param price: 委托价格，None 时服务端自动使用市价单
        :param wait_timeout: 等待超时秒数，0 表示异步返回
        :return: 订单 ID（如果不需要交易则返回空字符串）
        
        注意：服务端会自动处理最小手数/步进取整，实际市值可能与目标略有偏差。
        """
        p = price or self._infer_price(security)
        if not p:
            raise RuntimeError("无法获取价格，无法按目标市值下单")
        # 计算目标数量，服务端会自动按最小手数/步进取整
        target_amount = int(target_value / p)
        return self.order_target(security, target_amount, price=price, wait_timeout=wait_timeout)

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

    def get_orders(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
        status: Optional[object] = None,
        from_broker: bool = False,
    ) -> Dict[str, RemoteOrder]:
        payload = self._base_payload()
        if order_id:
            payload["order_id"] = order_id
        if security:
            payload["security"] = security
        if status is not None:
            payload["status"] = getattr(status, "value", status)
        if from_broker:
            payload["from_broker"] = True
        rows = self._client.request("broker.orders", payload) or []
        result: Dict[str, RemoteOrder] = {}
        for row in rows:
            order = self._build_order_snapshot(row)
            if not order:
                continue
            result[order.order_id] = order
        return result

    def get_open_orders(self) -> Dict[str, RemoteOrder]:
        orders = self.get_orders()
        if not orders:
            return {}
        open_states = {"new", "open", "filling", "canceling"}
        return {oid: order for oid, order in orders.items() if str(order.status) in open_states}

    def get_trades(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
    ) -> Dict[str, RemoteTrade]:
        payload = self._base_payload()
        if order_id:
            payload["order_id"] = order_id
        if security:
            payload["security"] = security
        rows = self._client.request("broker.trades", payload) or []
        result: Dict[str, RemoteTrade] = {}
        for row in rows:
            trade = self._build_trade_snapshot(row)
            if not trade:
                continue
            result[trade.trade_id] = trade
        return result

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        payload = self._base_payload()
        payload["order_id"] = order_id
        return self._client.request("broker.order_status", payload)

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        payload = self._base_payload()
        payload["order_id"] = order_id
        return self._client.request("broker.cancel_order", payload)

    def _build_order_snapshot(self, row: Dict[str, Any]) -> Optional[RemoteOrder]:
        if not isinstance(row, dict):
            return None
        order_id = row.get("order_id")
        if not order_id:
            return None
        amount = row.get("amount") or row.get("volume") or 0
        price = row.get("price")
        status = row.get("status") or row.get("state") or "open"
        filled = row.get("filled")
        if filled is None:
            filled = row.get("traded_volume") or 0
        is_buy = row.get("is_buy")
        order_remark = row.get("order_remark") or row.get("remark")
        strategy_name = row.get("strategy_name")
        return RemoteOrder(
            order_id=str(order_id),
            status=str(status),
            security=row.get("security"),
            amount=int(amount or 0),
            price=float(price) if price is not None else None,
            actual_amount=int(amount or 0),
            actual_price=float(price) if price is not None else None,
            filled=int(filled or 0),
            is_buy=bool(is_buy) if is_buy is not None else None,
            order_remark=str(order_remark) if order_remark is not None else None,
            strategy_name=str(strategy_name) if strategy_name is not None else None,
        )

    def _build_trade_snapshot(self, row: Dict[str, Any]) -> Optional[RemoteTrade]:
        if not isinstance(row, dict):
            return None
        trade_id = row.get("trade_id") or row.get("id") or row.get("trade_no")
        order_id = row.get("order_id") or row.get("entrust_id")
        security = row.get("security")
        if not trade_id and not order_id:
            return None
        amount = row.get("amount") or row.get("volume") or 0
        price = row.get("price") or 0.0
        raw_time = row.get("time") or row.get("trade_time")
        if isinstance(raw_time, pd.Timestamp):
            trade_time = raw_time
        elif raw_time:
            trade_time = pd.to_datetime(raw_time)
        else:
            trade_time = pd.Timestamp.now()
        if not trade_id:
            base = f"{order_id}-{trade_time}-{amount}-{price}"
            trade_id = hashlib.md5(base.encode("utf-8")).hexdigest()[:16]
        return RemoteTrade(
            trade_id=str(trade_id),
            order_id=str(order_id) if order_id is not None else "",
            security=str(security) if security else "",
            amount=int(amount or 0),
            price=float(price or 0.0),
            time=trade_time,
            commission=float(row.get("commission") or 0.0),
            tax=float(row.get("tax") or 0.0),
        )

    # ----- 内部 -----
    def _place_order(self, security: str, amount: int, price: Optional[float], side: str, wait_timeout: float) -> RemoteOrder:
        """
        发送下单请求到服务端。
        
        服务端会统一处理：
        - 最小手数/步进取整
        - 停牌检查
        - 市价单价格笼子计算
        - 限价单涨跌停校验
        - 卖出可卖数量检查
        """
        try:
            _log("INFO", "[下单] 准备下单: security={}, amount={}, price={}, side={}, wait_timeout={}", 
                 security, amount, price, side, wait_timeout)
            
            payload = self._base_payload()
            
            # 简化价格处理：price=None 表示市价单，服务端会自动计算价格笼子
            if price is None:
                style = {"type": "market"}
            else:
                style = {"type": "limit", "price": float(price)}
            
            payload.update({
                "security": security,
                "side": side,
                "amount": amount,
                "style": style,
            })
            
            _log("DEBUG", "[下单] 发送下单请求: payload={}", payload)
            resp = self._client.request("broker.place_order", payload)
            _log("DEBUG", "[下单] 收到下单响应: resp={}", resp)
            
            # 处理服务端警告
            warning = resp.get("warning") if isinstance(resp, dict) else None
            if warning:
                _log("WARN", "[远程警告] {}", warning)
            
            # 服务端返回实际执行的数量和价格（可能因取整/价格笼子而不同）
            actual_amount = resp.get("amount") if isinstance(resp, dict) else None
            actual_price = resp.get("price") if isinstance(resp, dict) else None
            
            # 检查订单 ID
            order_id = resp.get("order_id") if isinstance(resp, dict) else None
            if not order_id:
                error_msg = f"服务端未返回 order_id，响应: {resp}"
                _log("ERROR", "[下单错误] {}", error_msg)
                raise RuntimeError(error_msg)
            
            # 【逻辑变更】如果订单 ID 是 -1，说明 QMT 下单失败，抛出异常而非静默返回
            # 原因：-1 是 QMT 返回的错误码，表示下单失败，应该让调用方知道
            if order_id == "-1" or (isinstance(order_id, (int, float)) and order_id < 0):
                error_msg = f"下单失败，服务端返回错误订单号: {order_id}, 响应: {resp}"
                _log("ERROR", "[下单错误] {}", error_msg)
                raise RuntimeError(error_msg)
            
            # 如果服务端返回了不同的数量，提示用户
            if actual_amount is not None and actual_amount != amount:
                _log("INFO", "[下单] {} 数量已从 {} 调整为 {}（最小手数/步进取整）", 
                     security, amount, actual_amount)
            
            order = RemoteOrder(
                order_id=str(order_id),
                status=resp.get("status", "submitted") if isinstance(resp, dict) else "submitted",
                security=security,
                amount=amount,
                price=price,
                actual_amount=actual_amount,
                actual_price=actual_price,
            )
            
            _log("INFO", "[下单] 订单创建成功: order_id={}, status={}", order.order_id, order.status)
            
            if wait_timeout and order.order_id:
                _log("DEBUG", "[下单] 开始等待订单状态 (timeout={}s)", wait_timeout)
                self._wait_order(order.order_id, wait_timeout)
            
            return order
        except Exception as e:
            _log("ERROR", "[下单错误] 下单过程异常: security={}, amount={}, side={}, error={}", 
                 security, amount, side, e)
            _log("ERROR", "[下单错误] 堆栈:\n{}", traceback.format_exc())
            raise

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
    
    注意：每次 request() 调用都会建立新的 TCP 连接，连接后立即握手、发送请求、接收响应、关闭连接。
    这种设计适合聚宽环境频繁重启的场景，但会产生较多连接开销。
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
        """
        发送 RPC 请求（每次调用都会建立新的 TCP 连接）。
        
        Args:
            action: RPC 动作名称，如 "broker.place_order"
            payload: 请求载荷
            
        Returns:
            响应字典
            
        Raises:
            RuntimeError: 所有重试都失败后抛出最后一个异常
        """
        last_error: Optional[Exception] = None
        attempts = self.retries + 1
        request_start_time = time.time()
        
        _log("INFO", "[RPC] 开始请求: action={}, host={}, port={}, attempts={}", action, self.host, self.port, attempts)
        
        for attempt in range(1, attempts + 1):
            sock: Optional[socket.socket] = None
            connect_start_time = time.time()
            
            try:
                # ========== 1. 建立 TCP 连接 ==========
                _log("DEBUG", "[RPC] [尝试 {}/{}] 正在连接 TCP: {}:{}", attempt, attempts, self.host, self.port)
                
                try:
                    sock = socket.create_connection((self.host, self.port), timeout=10)
                    connect_duration = time.time() - connect_start_time
                    _log("DEBUG", "[RPC] [尝试 {}/{}] TCP 连接成功，耗时 {:.3f}s", attempt, attempts, connect_duration)
                except socket.gaierror as e:
                    # DNS 解析失败（这就是 "Name or service not known" 错误的来源）
                    error_msg = f"DNS 解析失败: host={self.host}, error={e}"
                    _log("ERROR", "[RPC] [尝试 {}/{}] {}", attempt, attempts, error_msg)
                    _log("ERROR", "[RPC] [尝试 {}/{}] 堆栈:\n{}", attempt, attempts, traceback.format_exc())
                    last_error = RuntimeError(error_msg)
                    if attempt < attempts:
                        _log("INFO", "[RPC] [尝试 {}/{}] {}s 后重试...", attempt, attempts, self.retry_interval)
                        time.sleep(self.retry_interval)
                    continue
                except socket.timeout as e:
                    error_msg = f"连接超时: host={self.host}, port={self.port}, timeout=10s"
                    _log("ERROR", "[RPC] [尝试 {}/{}] {}", attempt, attempts, error_msg)
                    _log("ERROR", "[RPC] [尝试 {}/{}] 堆栈:\n{}", attempt, attempts, traceback.format_exc())
                    last_error = RuntimeError(error_msg)
                    if attempt < attempts:
                        _log("INFO", "[RPC] [尝试 {}/{}] {}s 后重试...", attempt, attempts, self.retry_interval)
                        time.sleep(self.retry_interval)
                    continue
                except (ConnectionRefusedError, OSError) as e:
                    error_msg = f"连接被拒绝或网络错误: host={self.host}, port={self.port}, error={e}"
                    _log("ERROR", "[RPC] [尝试 {}/{}] {}", attempt, attempts, error_msg)
                    _log("ERROR", "[RPC] [尝试 {}/{}] 堆栈:\n{}", attempt, attempts, traceback.format_exc())
                    last_error = RuntimeError(error_msg)
                    if attempt < attempts:
                        _log("INFO", "[RPC] [尝试 {}/{}] {}s 后重试...", attempt, attempts, self.retry_interval)
                        time.sleep(self.retry_interval)
                    continue
                
                # ========== 2. TLS 握手（如果启用） ==========
                if self.tls_cert:
                    try:
                        _log("DEBUG", "[RPC] [尝试 {}/{}] 开始 TLS 握手", attempt, attempts)
                        context = ssl.create_default_context(cafile=self.tls_cert)
                        sock = context.wrap_socket(sock, server_hostname=self.host)
                        _log("DEBUG", "[RPC] [尝试 {}/{}] TLS 握手成功", attempt, attempts)
                    except Exception as e:
                        error_msg = f"TLS 握手失败: {e}"
                        _log("ERROR", "[RPC] [尝试 {}/{}] {}", attempt, attempts, error_msg)
                        _log("ERROR", "[RPC] [尝试 {}/{}] 堆栈:\n{}", attempt, attempts, traceback.format_exc())
                        last_error = RuntimeError(error_msg)
                        if attempt < attempts:
                            _log("INFO", "[RPC] [尝试 {}/{}] {}s 后重试...", attempt, attempts, self.retry_interval)
                            time.sleep(self.retry_interval)
                        continue
                
                sock.settimeout(self.rpc_timeout)
                
                # ========== 3. 应用层握手 ==========
                try:
                    _log("DEBUG", "[RPC] [尝试 {}/{}] 发送应用层握手", attempt, attempts)
                    handshake_msg = {
                        "type": "handshake",
                        "protocol": HELPER_PROTOCOL_VERSION,
                        "token": self.token,
                        "features": [],
                    }
                    self._send(sock, handshake_msg)
                    ack = self._recv(sock)
                    _log("DEBUG", "[RPC] [尝试 {}/{}] 收到握手响应: {}", attempt, attempts, ack.get("type"))
                    
                    if ack.get("type") != "handshake_ack":
                        raise RuntimeError(f"远程服务拒绝握手: {ack}")
                    server_protocol = ack.get("protocol")
                    server_protocol_value = None
                    if server_protocol is not None:
                        try:
                            server_protocol_value = int(server_protocol)
                        except (TypeError, ValueError):
                            server_protocol_value = None
                    if server_protocol_value is not None and server_protocol_value > HELPER_PROTOCOL_VERSION:
                        _warn(
                            "远程服务协议版本 {} 高于本地 helper 版本 {}，建议升级 helper",
                            server_protocol_value,
                            HELPER_PROTOCOL_VERSION,
                        )
                    _log("DEBUG", "[RPC] [尝试 {}/{}] 应用层握手成功", attempt, attempts)
                except Exception as e:
                    error_msg = f"应用层握手失败: {e}"
                    _log("ERROR", "[RPC] [尝试 {}/{}] {}", attempt, attempts, error_msg)
                    _log("ERROR", "[RPC] [尝试 {}/{}] 堆栈:\n{}", attempt, attempts, traceback.format_exc())
                    last_error = RuntimeError(error_msg)
                    if attempt < attempts:
                        _log("INFO", "[RPC] [尝试 {}/{}] {}s 后重试...", attempt, attempts, self.retry_interval)
                        time.sleep(self.retry_interval)
                    continue
                
                # ========== 4. 发送 RPC 请求 ==========
                req_id = str(id(payload) ^ int.from_bytes(os.urandom(4), "big"))
                request_msg = {"type": "request", "id": req_id, "action": action, "payload": payload}
                
                _log("DEBUG", "[RPC] [尝试 {}/{}] 发送 RPC 请求: action={}, req_id={}, payload_keys={}", 
                     attempt, attempts, action, req_id, list(payload.keys()) if isinstance(payload, dict) else "N/A")
                
                try:
                    self._send(sock, request_msg)
                    _log("DEBUG", "[RPC] [尝试 {}/{}] RPC 请求已发送", attempt, attempts)
                except Exception as e:
                    error_msg = f"发送 RPC 请求失败: {e}"
                    _log("ERROR", "[RPC] [尝试 {}/{}] {}", attempt, attempts, error_msg)
                    _log("ERROR", "[RPC] [尝试 {}/{}] 堆栈:\n{}", attempt, attempts, traceback.format_exc())
                    last_error = RuntimeError(error_msg)
                    if attempt < attempts:
                        _log("INFO", "[RPC] [尝试 {}/{}] {}s 后重试...", attempt, attempts, self.retry_interval)
                        time.sleep(self.retry_interval)
                    continue
                
                # ========== 5. 接收响应 ==========
                response_start_time = time.time()
                try:
                    _log("DEBUG", "[RPC] [尝试 {}/{}] 等待 RPC 响应 (timeout={}s)", attempt, attempts, self.rpc_timeout)
                    while True:
                        message = self._recv(sock)
                        msg_type = message.get("type")
                        _log("DEBUG", "[RPC] [尝试 {}/{}] 收到消息: type={}, id={}", 
                             attempt, attempts, msg_type, message.get("id"))
                        
                        if msg_type == "response" and message.get("id") == req_id:
                            response_duration = time.time() - response_start_time
                            response_payload = message.get("payload") or {}
                            _log("INFO", "[RPC] [尝试 {}/{}] RPC 请求成功: action={}, 耗时 {:.3f}s, response_keys={}", 
                                 attempt, attempts, action, response_duration, 
                                 list(response_payload.keys()) if isinstance(response_payload, dict) else "N/A")
                            return response_payload
                        
                        if msg_type == "error":
                            error_payload = message.get("payload") or {}
                            error_message = message.get("message", "server error")
                            _log("ERROR", "[RPC] [尝试 {}/{}] 服务器返回错误: message={}, payload={}", 
                                 attempt, attempts, error_message, error_payload)
                            raise RuntimeError(f"服务器错误: {error_message}")
                            
                except socket.timeout as e:
                    error_msg = f"接收响应超时: timeout={self.rpc_timeout}s"
                    _log("ERROR", "[RPC] [尝试 {}/{}] {}", attempt, attempts, error_msg)
                    _log("ERROR", "[RPC] [尝试 {}/{}] 堆栈:\n{}", attempt, attempts, traceback.format_exc())
                    last_error = RuntimeError(error_msg)
                    if attempt < attempts:
                        _log("INFO", "[RPC] [尝试 {}/{}] {}s 后重试...", attempt, attempts, self.retry_interval)
                        time.sleep(self.retry_interval)
                    continue
                except Exception as e:
                    error_msg = f"接收响应失败: {e}"
                    _log("ERROR", "[RPC] [尝试 {}/{}] {}", attempt, attempts, error_msg)
                    _log("ERROR", "[RPC] [尝试 {}/{}] 堆栈:\n{}", attempt, attempts, traceback.format_exc())
                    last_error = RuntimeError(error_msg)
                    if attempt < attempts:
                        _log("INFO", "[RPC] [尝试 {}/{}] {}s 后重试...", attempt, attempts, self.retry_interval)
                        time.sleep(self.retry_interval)
                    continue
                
            except Exception as exc:
                # 捕获所有其他未预期的异常
                error_msg = f"未预期的异常: {exc}"
                _log("ERROR", "[RPC] [尝试 {}/{}] {}", attempt, attempts, error_msg)
                _log("ERROR", "[RPC] [尝试 {}/{}] 堆栈:\n{}", attempt, attempts, traceback.format_exc())
                last_error = exc
                if attempt < attempts:
                    _log("INFO", "[RPC] [尝试 {}/{}] {}s 后重试...", attempt, attempts, self.retry_interval)
                    time.sleep(self.retry_interval)
            finally:
                # ========== 6. 关闭连接 ==========
                if sock:
                    try:
                        _log("DEBUG", "[RPC] [尝试 {}/{}] 关闭 TCP 连接", attempt, attempts)
                        sock.close()
                    except Exception as e:
                        _log("WARN", "[RPC] [尝试 {}/{}] 关闭连接时出错: {}", attempt, attempts, e)
        
        # 所有重试都失败
        total_duration = time.time() - request_start_time
        final_error_msg = f"远程请求失败（已重试 {attempts} 次，总耗时 {total_duration:.3f}s）: {last_error}"
        _log("ERROR", "[RPC] {}", final_error_msg)
        if last_error:
            _log("ERROR", "[RPC] 最后一次错误的堆栈:\n{}", traceback.format_exc())
        raise RuntimeError(final_error_msg)

    def _send(self, sock: socket.socket, message: Dict[str, Any]) -> None:
        """发送消息到服务器"""
        try:
            body = json.dumps(message, ensure_ascii=False).encode("utf-8")
            header = struct.pack(">I", len(body))
            sock.sendall(header + body)
            _log("DEBUG", "[RPC] 已发送消息: type={}, size={} bytes", message.get("type"), len(body))
        except Exception as e:
            _log("ERROR", "[RPC] 发送消息失败: {}, 堆栈:\n{}", e, traceback.format_exc())
            raise

    def _recv(self, sock: socket.socket) -> Dict[str, Any]:
        """从服务器接收消息"""
        try:
            header = self._read_exact(sock, 4)
            size = struct.unpack(">I", header)[0]
            _log("DEBUG", "[RPC] 收到消息头: size={} bytes", size)
            payload = self._read_exact(sock, size)
            message = json.loads(payload.decode("utf-8"))
            _log("DEBUG", "[RPC] 已解析消息: type={}", message.get("type"))
            return message
        except Exception as e:
            _log("ERROR", "[RPC] 接收消息失败: {}, 堆栈:\n{}", e, traceback.format_exc())
            raise

    def _read_exact(self, sock: socket.socket, size: int) -> bytes:
        """精确读取指定字节数"""
        buf = b""
        read_start = time.time()
        while len(buf) < size:
            remaining = size - len(buf)
            try:
                chunk = sock.recv(remaining)
                if not chunk:
                    raise RuntimeError(f"连接中断（已读取 {len(buf)}/{size} 字节）")
                buf += chunk
            except socket.timeout:
                elapsed = time.time() - read_start
                raise RuntimeError(f"读取超时（已读取 {len(buf)}/{size} 字节，耗时 {elapsed:.3f}s）")
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


def get_open_orders() -> Dict[str, RemoteOrder]:
    return get_broker_client().get_open_orders()


def get_orders(
    order_id: Optional[str] = None,
    security: Optional[str] = None,
    status: Optional[object] = None,
    from_broker: bool = False,
) -> Dict[str, RemoteOrder]:
    return get_broker_client().get_orders(
        order_id=order_id,
        security=security,
        status=status,
        from_broker=from_broker,
    )


def get_trades(
    order_id: Optional[str] = None,
    security: Optional[str] = None,
) -> Dict[str, RemoteTrade]:
    return get_broker_client().get_trades(order_id=order_id, security=security)


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
    "get_orders",
    "get_trades",
    "get_account",
    "get_positions",
    "RemoteAccount",
    "RemoteOrder",
    "RemoteTrade",
    "RemotePosition",
    "RemoteDataClient",
    "RemoteBrokerClient",
]
