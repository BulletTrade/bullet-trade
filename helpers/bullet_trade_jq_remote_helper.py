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
3. 如果希望聚宽模拟盘里尽量不改原策略下单代码，可在 process_initialize 里调用：
   bt.install_jq_compat(globals(), context=context, host='你的IP', token='你的token')

特点：
- 每次调用都会重新建立 TCP 连接，适合聚宽频繁重启。
- 服务端统一处理：最小手数/步进取整、停牌检查、价格笼子、涨跌停校验、可卖数量检查。
- 支持同步/异步：wait_timeout>0 时轮询订单状态，否则立即返回。
- 提供 account/positions/order_status/orders/cancel/order_value/order_target 等常见聚宽风格 API。
- install_jq_compat 在回测中不接管；在聚宽模拟盘中接管账户状态和同名下单函数，默认同步等待 16 秒。
"""

import ast
import hashlib
import json
import os
import socket
import ssl
import struct
import sys
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import pandas as pd

_CLIENT: Optional["_ShortLivedClient"] = None
_DATA_CLIENT: Optional["RemoteDataClient"] = None
_BROKER_CLIENT: Optional["RemoteBrokerClient"] = None

# 全局调试开关
_DEBUG: bool = True
HELPER_PROTOCOL_VERSION: int = 1
DEFAULT_RPC_TIMEOUT_SECONDS: float = 60.0
DEFAULT_HISTORY_RPC_TIMEOUT_SECONDS: float = 180.0
DEFAULT_PLACE_ORDER_TIMEOUT_MARGIN_SECONDS: float = 30.0
DEFAULT_JQ_COMPAT_WAIT_TIMEOUT_SECONDS: float = 16.0


class MarketOrderStyle:
    """聚宽风格市价单样式，可选保护价。"""

    def __init__(self, limit_price: Optional[float] = None):
        self.limit_price = limit_price


class LimitOrderStyle:
    """聚宽风格限价单样式。"""

    def __init__(self, limit_price: float):
        self.limit_price = limit_price
        self.price = limit_price


def _style_class_name(style: Any) -> str:
    return style.__class__.__name__ if style is not None else ""


def _is_order_style(value: Any) -> bool:
    name = _style_class_name(value)
    return bool(name and "OrderStyle" in name)


def _extract_style_price(style: Any) -> Optional[float]:
    for attr in ("limit_price", "price"):
        if hasattr(style, attr):
            value = getattr(style, attr)
            if value is not None:
                return float(value)
    return None


def _resolve_price_market(
    price: Optional[float] = None,
    style: Optional[Any] = None,
    market: Optional[bool] = None,
) -> Tuple[Optional[float], Optional[bool]]:
    """解析聚宽 style 和旧 helper price/market 语义。"""

    if style is None and _is_order_style(price):
        style = price
        price = None
    if style is None:
        return price, market

    name = _style_class_name(style)
    if name in ("StopMarketOrderStyle", "StopLimitOrderStyle"):
        raise NotImplementedError(f"{name} 暂不支持远程实盘接管")
    if "Stop" in name and "OrderStyle" in name:
        raise NotImplementedError(f"{name} 暂不支持远程实盘接管")
    style_price = _extract_style_price(style)
    effective_price = style_price if style_price is not None else price
    if "MarketOrderStyle" in name:
        return effective_price, True
    if "LimitOrderStyle" in name:
        if effective_price is None:
            raise ValueError("限价单缺少价格")
        return effective_price, False
    if "OrderStyle" in name:
        raise NotImplementedError(f"{name} 暂不支持远程实盘接管")
    return price, market


def _validate_jq_trade_scope(
    side: Optional[str] = None,
    pindex: int = 0,
    close_today: bool = False,
) -> None:
    if pindex not in (0, None):
        raise NotImplementedError("聚宽兼容接管第一版仅支持 pindex=0")
    if close_today:
        raise NotImplementedError("聚宽兼容接管第一版暂不支持 close_today=True")
    if side is None:
        return
    side_text = str(side).strip().lower()
    if side_text == "short":
        raise NotImplementedError("聚宽兼容接管第一版暂不支持 side='short'")


def _normalise_side(side: Optional[str], signed_value: float) -> str:
    if side is not None:
        side_text = str(side).strip().lower()
        if side_text == "short":
            raise NotImplementedError("聚宽兼容接管第一版暂不支持 side='short'")
        if side_text in ("buy", "b"):
            return "BUY"
        if side_text in ("sell", "s"):
            return "SELL"
    return "BUY" if signed_value > 0 else "SELL"


def _coerce_wait_timeout(value: Optional[float], default_wait_timeout: float) -> float:
    if value is None:
        return float(default_wait_timeout)
    return float(value)


def _now_ns() -> int:
    time_ns = getattr(time, "time_ns", None)
    if time_ns is not None:
        return int(time_ns())
    return int(time.time() * 1_000_000_000)


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
    rpc_timeout: float = DEFAULT_RPC_TIMEOUT_SECONDS,
    place_order_timeout_margin: float = DEFAULT_PLACE_ORDER_TIMEOUT_MARGIN_SECONDS,
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
        place_order_timeout_margin: 下单请求超时相对 wait_timeout 的安全余量，默认 30.0
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
        place_order_timeout_margin=place_order_timeout_margin,
    )
    _BROKER_CLIENT.bind_data_client(_DATA_CLIENT)
    
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
    - timed_out/async_tracking/last_snapshot: 新版 server 返回的等待超时追踪字段；旧 server 没有时保持默认值
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
        timed_out: bool = False,
        async_tracking: bool = False,
        last_snapshot: Optional[Dict[str, Any]] = None,
        raw_response: Optional[Dict[str, Any]] = None,
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
        self.timed_out = bool(timed_out)
        self.async_tracking = bool(async_tracking)
        self.last_snapshot = dict(last_snapshot or {})
        self.raw_response = dict(raw_response or {})


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
        place_order_timeout_margin: float = DEFAULT_PLACE_ORDER_TIMEOUT_MARGIN_SECONDS,
    ) -> None:
        self._client = client
        self.account_key = account_key
        self.sub_account_id = sub_account_id
        self._data_client: Optional[RemoteDataClient] = None
        self.place_order_timeout_margin = max(0.0, float(place_order_timeout_margin))

    def bind_data_client(self, data_client: RemoteDataClient) -> None:
        self._data_client = data_client

    # ----- 聚宽风格入口 -----
    def order(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        side: Optional[str] = None,
        wait_timeout: float = 0,
        *,
        style: Optional[Any] = None,
        market: Optional[bool] = None,
        remark: Optional[str] = None,
        order_remark: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> str:
        """
        按数量下单。
        
        :param security: 证券代码
        :param amount: 数量（正数买入，负数卖出；如果指定了 side 则取绝对值）
        :param price: 委托价格，None 时服务端自动使用市价单
        :param side: 方向 BUY/SELL，None 时根据 amount 正负判断
        :param wait_timeout: 等待超时秒数，0 表示异步返回
        :param market: True 表示市价单；price 同时传入时作为保护价。None 时保持旧行为
        :param remark/order_remark: 订单备注，透传到服务端/QMT
        :param idempotency_key: 幂等键；不传时 helper 会为本次短连接请求自动生成
        :return: 订单 ID
        
        注意：服务端会自动处理最小手数/步进取整、停牌检查、价格笼子等。
        """
        if amount == 0:
            return ""
        price, market = _resolve_price_market(price=price, style=style, market=market)
        actual_side = _normalise_side(side, amount)
        qty = abs(int(amount))
        # 服务端会自动处理最小手数/步进取整
        order = self._place_order(
            security,
            qty,
            price,
            actual_side,
            wait_timeout=wait_timeout,
            market=market,
            remark=remark,
            order_remark=order_remark,
            idempotency_key=idempotency_key,
        )
        return order.order_id

    def order_value(
        self,
        security: str,
        value: float,
        price: Optional[float] = None,
        wait_timeout: float = 0,
        *,
        style: Optional[Any] = None,
        side: Optional[str] = None,
        pindex: int = 0,
        close_today: bool = False,
        market: Optional[bool] = None,
        remark: Optional[str] = None,
        order_remark: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> str:
        """
        按市值下单。
        
        :param security: 证券代码
        :param value: 目标市值（正数买入，负数卖出）
        :param price: 委托价格，None 时服务端自动使用市价单
        :param wait_timeout: 等待超时秒数，0 表示异步返回
        :param market: True 表示市价单；price 同时传入时作为保护价。None 时保持旧行为
        :param remark/order_remark: 订单备注，透传到服务端/QMT
        :param idempotency_key: 幂等键；不传时 helper 会为本次短连接请求自动生成
        :return: 订单 ID
        
        注意：服务端会自动处理最小手数/步进取整，实际成交市值可能与请求略有偏差。
        """
        _validate_jq_trade_scope(side=side, pindex=pindex, close_today=close_today)
        if value == 0:
            return ""
        price, market = _resolve_price_market(price=price, style=style, market=market)
        # 获取参考价格用于计算数量
        p = price or self._infer_price(security)
        if not p:
            raise RuntimeError("无法获取价格，无法按市值下单")
        # 计算大致数量，服务端会自动按最小手数/步进取整
        qty = int(abs(value) / p)
        actual_side = _normalise_side(side, value)
        order = self._place_order(
            security,
            qty,
            price,
            actual_side,
            wait_timeout=wait_timeout,
            market=market,
            remark=remark,
            order_remark=order_remark,
            idempotency_key=idempotency_key,
        )
        return order.order_id

    def order_percent(
        self,
        security: str,
        percent: float,
        price: Optional[float] = None,
        wait_timeout: float = 0,
        *,
        style: Optional[Any] = None,
        side: Optional[str] = None,
        pindex: int = 0,
        close_today: bool = False,
        market: Optional[bool] = None,
        remark: Optional[str] = None,
        order_remark: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> str:
        """按当前远程账户总资产的一定比例下单。"""

        account = self.get_account()
        return self.order_value(
            security,
            float(account.total_value) * float(percent),
            price=price,
            wait_timeout=wait_timeout,
            style=style,
            side=side,
            pindex=pindex,
            close_today=close_today,
            market=market,
            remark=remark,
            order_remark=order_remark,
            idempotency_key=idempotency_key,
        )

    def order_target(
        self,
        security: str,
        target: int,
        price: Optional[float] = None,
        wait_timeout: float = 0,
        *,
        style: Optional[Any] = None,
        side: Optional[str] = None,
        pindex: int = 0,
        close_today: bool = False,
        market: Optional[bool] = None,
        remark: Optional[str] = None,
        order_remark: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> str:
        """
        调仓到目标数量。
        
        :param security: 证券代码
        :param target: 目标持仓数量
        :param price: 委托价格，None 时服务端自动使用市价单
        :param wait_timeout: 等待超时秒数，0 表示异步返回
        :param market: True 表示市价单；price 同时传入时作为保护价。None 时保持旧行为
        :param remark/order_remark: 订单备注，透传到服务端/QMT
        :param idempotency_key: 幂等键；不传时 helper 会为本次短连接请求自动生成
        :return: 订单 ID（如果不需要交易则返回空字符串）
        
        注意：建议 target 为 100 的整数倍，服务端会自动取整。
        """
        _validate_jq_trade_scope(side=side, pindex=pindex, close_today=close_today)
        price, market = _resolve_price_market(price=price, style=style, market=market)
        current = self._current_amount(security)
        delta = target - current
        if delta == 0:
            return ""
        return self.order(
            security,
            delta,
            price=price,
            side=side,
            wait_timeout=wait_timeout,
            style=style,
            market=market,
            remark=remark,
            order_remark=order_remark,
            idempotency_key=idempotency_key,
        )

    def order_target_value(
        self,
        security: str,
        target_value: Optional[float] = None,
        price: Optional[float] = None,
        wait_timeout: float = 0,
        *,
        value: Optional[float] = None,
        style: Optional[Any] = None,
        side: Optional[str] = None,
        pindex: int = 0,
        close_today: bool = False,
        market: Optional[bool] = None,
        remark: Optional[str] = None,
        order_remark: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> str:
        """
        调仓到目标市值。
        
        :param security: 证券代码
        :param target_value: 目标持仓市值
        :param price: 委托价格，None 时服务端自动使用市价单
        :param wait_timeout: 等待超时秒数，0 表示异步返回
        :param market: True 表示市价单；price 同时传入时作为保护价。None 时保持旧行为
        :param remark/order_remark: 订单备注，透传到服务端/QMT
        :param idempotency_key: 幂等键；不传时 helper 会为本次短连接请求自动生成
        :return: 订单 ID（如果不需要交易则返回空字符串）
        
        注意：服务端会自动处理最小手数/步进取整，实际市值可能与目标略有偏差。
        """
        _validate_jq_trade_scope(side=side, pindex=pindex, close_today=close_today)
        if target_value is None:
            if value is None:
                raise TypeError("order_target_value() missing required argument: 'target_value' or 'value'")
            target_value = value
        elif value is not None:
            raise TypeError("order_target_value() got both 'target_value' and 'value'")
        price, market = _resolve_price_market(price=price, style=style, market=market)
        p = price or self._infer_price(security)
        if not p:
            raise RuntimeError("无法获取价格，无法按目标市值下单")
        # 计算目标数量，服务端会自动按最小手数/步进取整
        target_amount = int(target_value / p)
        return self.order_target(
            security,
            target_amount,
            price=price,
            wait_timeout=wait_timeout,
            style=style,
            side=side,
            pindex=pindex,
            close_today=close_today,
            market=market,
            remark=remark,
            order_remark=order_remark,
            idempotency_key=idempotency_key,
        )

    def order_target_percent(
        self,
        security: str,
        percent: float,
        price: Optional[float] = None,
        wait_timeout: float = 0,
        *,
        style: Optional[Any] = None,
        side: Optional[str] = None,
        pindex: int = 0,
        close_today: bool = False,
        market: Optional[bool] = None,
        remark: Optional[str] = None,
        order_remark: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> str:
        """调仓到当前远程账户总资产的一定比例。"""

        account = self.get_account()
        return self.order_target_value(
            security,
            float(account.total_value) * float(percent),
            price=price,
            wait_timeout=wait_timeout,
            style=style,
            side=side,
            pindex=pindex,
            close_today=close_today,
            market=market,
            remark=remark,
            order_remark=order_remark,
            idempotency_key=idempotency_key,
        )

    # ----- 基础接口 -----
    def get_account(self) -> RemoteAccount:
        payload = self._base_payload()
        resp = self._client.request("broker.account", payload) or {}
        value = resp.get("value") or resp
        return RemoteAccount(
            available_cash=float(value.get("available_cash", 0.0)),
            total_value=float(value.get("total_value", value.get("total_asset", 0.0))),
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
        open_states = {"new", "submitted", "open", "filling", "canceling"}
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
            timed_out=bool(row.get("timed_out")),
            async_tracking=bool(row.get("async_tracking")),
            last_snapshot=row.get("last_snapshot") if isinstance(row.get("last_snapshot"), dict) else None,
            raw_response=dict(row),
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
    def _place_order(
        self,
        security: str,
        amount: int,
        price: Optional[float],
        side: str,
        wait_timeout: float,
        *,
        market: Optional[bool] = None,
        remark: Optional[str] = None,
        order_remark: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> RemoteOrder:
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
            
            effective_market = bool(price is None) if market is None else bool(market)
            if effective_market:
                style = {"type": "market"}
                if price is not None:
                    style["protect_price"] = float(price)
            else:
                if price is None:
                    raise ValueError("限价单缺少价格；请传入 price 或设置 market=True")
                style = {"type": "limit", "price": float(price)}
            
            payload.update({
                "security": security,
                "side": side,
                "amount": amount,
                "style": style,
                "idempotency_key": idempotency_key or self._make_idempotency_key(security, amount, side, style),
            })
            if wait_timeout is not None:
                payload["wait_timeout"] = wait_timeout
            if effective_market:
                payload["market"] = True
            effective_remark = order_remark if order_remark is not None else remark
            if effective_remark:
                payload["order_remark"] = effective_remark
            
            _log("DEBUG", "[下单] 发送下单请求: payload={}", payload)
            try:
                resp = self._request_place_order(
                    "broker.place_order",
                    payload,
                    timeout=self._resolve_place_order_rpc_timeout(wait_timeout),
                )
            except Exception as exc:
                if self._is_submit_unknown_timeout_error(exc):
                    error_msg = (
                        "下单请求超时，状态=submit_unknown，需要后续核对远端订单: "
                        f"security={security}, amount={amount}, side={side}, error={exc}"
                    )
                    _log("ERROR", "[下单错误] {}", error_msg)
                    raise RuntimeError(error_msg) from exc
                raise
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

            status = str(resp.get("status") or resp.get("order_status") or "").strip().lower()
            if status == "submit_unknown":
                error_msg = f"下单提交状态未知，需要后续核对远端订单: order_id={order_id}, 响应: {resp}"
                _log("ERROR", "[下单错误] {}", error_msg)
                raise RuntimeError(error_msg)
            if status in {"rejected", "canceled", "cancelled", "failed", "error"}:
                error_msg = f"下单失败，服务端返回终态失败: order_id={order_id}, status={status}, 响应: {resp}"
                _log("ERROR", "[下单错误] {}", error_msg)
                raise RuntimeError(error_msg)
            
            # 如果服务端返回了不同的数量，提示用户
            if actual_amount is not None and actual_amount != amount:
                _log("INFO", "[下单] {} 数量已从 {} 调整为 {}（最小手数/步进取整）", 
                     security, amount, actual_amount)
            
            order = RemoteOrder(
                order_id=str(order_id),
                status=status or "submitted",
                security=security,
                amount=amount,
                price=price,
                actual_amount=actual_amount,
                actual_price=actual_price,
                timed_out=bool(resp.get("timed_out")) if isinstance(resp, dict) else False,
                async_tracking=bool(resp.get("async_tracking")) if isinstance(resp, dict) else False,
                last_snapshot=resp.get("last_snapshot") if isinstance(resp.get("last_snapshot"), dict) else None,
                raw_response=dict(resp) if isinstance(resp, dict) else {},
            )
            
            if order.status in {"open", "submitted", "new", "filling"} or order.timed_out or order.async_tracking:
                _log("INFO", "[下单] 订单已提交，等待成交确认: order_id={}, status={}", order.order_id, order.status)
            else:
                _log("INFO", "[下单] 订单创建成功: order_id={}, status={}", order.order_id, order.status)
            
            if wait_timeout and order.order_id and not (order.timed_out or order.async_tracking):
                _log("DEBUG", "[下单] 开始等待订单状态 (timeout={}s)", wait_timeout)
                self._wait_order(order.order_id, wait_timeout)
            
            return order
        except Exception as e:
            _log("ERROR", "[下单错误] 下单过程异常: security={}, amount={}, side={}, error={}", 
                 security, amount, side, e)
            _log("ERROR", "[下单错误] 堆栈:\n{}", traceback.format_exc())
            raise

    def _resolve_place_order_rpc_timeout(self, wait_timeout: float) -> float:
        """解析下单 RPC 请求超时时间。

        Args:
            wait_timeout: 本次下单等待终态秒数。

        Returns:
            float: 单次 RPC 接收响应超时时间。
        """
        try:
            wait_seconds = float(wait_timeout or 0)
        except (TypeError, ValueError):
            wait_seconds = 0.0
        rpc_timeout = max(5.0, float(getattr(self._client, "rpc_timeout", DEFAULT_RPC_TIMEOUT_SECONDS)))
        if wait_seconds <= 0:
            return rpc_timeout
        return max(rpc_timeout, wait_seconds + self.place_order_timeout_margin)

    def _request_place_order(
        self,
        action: str,
        payload: Dict[str, Any],
        *,
        timeout: float,
    ) -> Dict[str, Any]:
        """发送下单请求，并兼容旧版 request 签名。

        Args:
            action: 远程 action 名称。
            payload: 请求载荷。
            timeout: 新版短连接客户端支持的单次请求超时。

        Returns:
            Dict[str, Any]: 远程响应。

        兼容性:
            部分外部用户或测试桩只实现 `request(action, payload)`，不接受
            `timeout` 关键字。此处只在签名不兼容时回退旧调用，避免破坏旧 helper
            使用方式；其他 TypeError 继续抛出。
        """

        try:
            return self._client.request(action, payload, timeout=timeout)
        except TypeError as exc:
            message = str(exc)
            if "timeout" not in message or "unexpected keyword" not in message:
                raise
            return self._client.request(action, payload)

    @staticmethod
    def _is_submit_unknown_timeout_error(exc: Exception) -> bool:
        """判断下单异常是否属于无订单号的提交状态未知。

        Args:
            exc: 下单请求阶段抛出的异常。

        Returns:
            bool: True 表示应映射为 submit_unknown 风险；False 表示保留原异常语义。
        """

        if isinstance(exc, TimeoutError):
            return True
        message = str(exc).lower()
        return "timeout" in message or "超时" in message

    def _wait_order(self, order_id: str, timeout: float) -> None:
        start = time.time()
        interval = 1.0
        while time.time() - start < timeout:
            try:
                status = self.get_order_status(order_id)
                st = str(status.get("status") or "").lower()
                if st in {
                    "filled",
                    "cancelled",
                    "canceled",
                    "rejected",
                    "partly_canceled",
                    "failed",
                    "error",
                }:
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

    def _make_idempotency_key(self, security: str, amount: int, side: str, style: Dict[str, Any]) -> str:
        raw = f"{security}|{amount}|{side}|{style}|{_now_ns()}|{os.urandom(8).hex()}"
        return "bt-helper-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


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
        rpc_timeout: float = DEFAULT_RPC_TIMEOUT_SECONDS,
    ):
        self.host = host
        self.port = port
        self.token = token
        self.tls_cert = tls_cert
        self.retries = max(0, retries)
        self.retry_interval = max(0.1, float(retry_interval))
        self.rpc_timeout = max(5.0, float(rpc_timeout))

    def _resolve_request_timeout(
        self,
        action: str,
        timeout: Optional[float],
    ) -> float:
        """计算一次短连接 RPC 请求的实际超时。

        Args:
            action: RPC 动作名称。
            timeout: 调用方显式指定的超时；为空时使用动作默认值。

        Returns:
            实际使用的超时秒数，且不会小于 5 秒。
        """

        if timeout is None:
            effective_timeout = self.rpc_timeout
            if action == "data.history":
                effective_timeout = max(
                    effective_timeout,
                    DEFAULT_HISTORY_RPC_TIMEOUT_SECONDS,
                )
        else:
            effective_timeout = float(timeout)
        return max(5.0, effective_timeout)

    def request(
        self,
        action: str,
        payload: Dict[str, Any],
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        发送 RPC 请求（每次调用都会建立新的 TCP 连接）。
        
        Args:
            action: RPC 动作名称，如 "broker.place_order"
            payload: 请求载荷
            timeout: 本次请求超时；不传时普通请求使用客户端默认 rpc_timeout，
                data.history 至少等待 180 秒。
            
        Returns:
            响应字典
            
        Raises:
            RuntimeError: 所有重试都失败后抛出最后一个异常
        """
        effective_timeout = self._resolve_request_timeout(action, timeout)
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
                
                sock.settimeout(effective_timeout)
                
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
                    _log("DEBUG", "[RPC] [尝试 {}/{}] 等待 RPC 响应 (timeout={}s)", attempt, attempts, effective_timeout)
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
                    error_msg = f"接收响应超时: timeout={effective_timeout}s"
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
_PRICE_FIELD_NAMES = {
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "amount",
    "avg",
    "price",
    "highlimit",
    "lowlimit",
    "paused",
    "preclose",
    "pre_close",
    "suspendflag",
    "suspend_flag",
    "openinterest",
    "open_interest",
    "settlementprice",
    "settelementprice",
}


def _price_field_tokens(values) -> Set[str]:
    return {str(value).replace(" ", "").replace("_", "").lower() for value in values}


def _normalise_price_multiindex_columns(columns: pd.MultiIndex) -> pd.MultiIndex:
    if columns.nlevels != 2:
        return columns
    level0 = _price_field_tokens(columns.get_level_values(0))
    level1 = _price_field_tokens(columns.get_level_values(1))
    if (level1 & _PRICE_FIELD_NAMES) and not (level0 & _PRICE_FIELD_NAMES):
        columns = columns.swaplevel(0, 1)
        columns.names = ["field", "code"]
    elif (level0 & _PRICE_FIELD_NAMES) and not (level1 & _PRICE_FIELD_NAMES):
        columns.names = ["field", "code"]
    return columns


def _multiindex_from_payload_columns(column_tuples, names) -> pd.MultiIndex:
    tuples = [tuple(items) for items in column_tuples]
    index = pd.MultiIndex.from_tuples(tuples)
    if names and len(names) == index.nlevels:
        index.names = list(names)
    return index


def _parse_legacy_tuple_columns(columns):
    parsed = []
    for column in columns:
        if not isinstance(column, str) or not column.startswith("("):
            return columns
        try:
            value = ast.literal_eval(column)
        except Exception:
            return columns
        if not isinstance(value, tuple) or len(value) != 2:
            return columns
        parsed.append(value)
    return pd.MultiIndex.from_tuples(parsed) if parsed else columns


def _df_from_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    if not payload or payload.get("dtype") != "dataframe":
        return pd.DataFrame()
    columns = payload.get("columns") or []
    column_tuples = payload.get("column_tuples") or None
    records = payload.get("records") or []
    if column_tuples:
        columns = _multiindex_from_payload_columns(column_tuples, payload.get("column_index_names"))
    else:
        columns = _parse_legacy_tuple_columns(columns)
    df = pd.DataFrame(records, columns=columns)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = _normalise_price_multiindex_columns(df.columns)
    return df


# --------- 聚宽策略零改兼容层 ----------
_JQ_COMPAT_STATE_KEY = "__bt_jq_compat_state__"
_JQ_COMPAT_FUNCTIONS = [
    "order",
    "order_value",
    "order_percent",
    "order_target",
    "order_target_value",
    "order_target_percent",
    "cancel_order",
    "get_open_orders",
    "get_orders",
    "get_trades",
]


class _RemoteJQPosition:
    """聚宽风格持仓对象，字段来自远程真实账户。"""

    def __init__(self, source: Optional[RemotePosition] = None, security: Optional[str] = None):
        if source is None:
            self.security = security or ""
            self.total_amount = 0
            self.closeable_amount = 0
            self.locked_amount = 0
            self.value = 0.0
            self.price = 0.0
            self.avg_cost = 0.0
            self.hold_cost = 0.0
            self.market = None
            return
        self.security = source.security
        self.total_amount = int(source.amount or 0)
        self.closeable_amount = int(source.available or 0)
        self.locked_amount = int(source.frozen or 0)
        self.value = float(source.market_value or 0.0)
        self.price = self.value / self.total_amount if self.total_amount else 0.0
        self.avg_cost = float(source.avg_cost or 0.0)
        self.hold_cost = self.avg_cost
        self.market = source.market


class _RemotePositionDict(dict):
    """不存在的持仓返回空仓位，兼容聚宽常见写法。"""

    def __missing__(self, key):
        return _RemoteJQPosition(security=str(key))


class _RemoteSnapshotCache:
    def __init__(self, broker: RemoteBrokerClient, ttl_seconds: float = 1.0):
        self.broker = broker
        self.ttl_seconds = max(0.0, float(ttl_seconds))
        self._snapshot: Optional[Dict[str, Any]] = None
        self._snapshot_at = 0.0

    def invalidate(self) -> None:
        self._snapshot = None
        self._snapshot_at = 0.0

    def snapshot(self) -> Dict[str, Any]:
        now = time.time()
        if self._snapshot is not None and now - self._snapshot_at <= self.ttl_seconds:
            return self._snapshot
        account = self.broker.get_account()
        raw_positions = self.broker.get_positions()
        positions = _RemotePositionDict()
        for pos in raw_positions:
            positions[pos.security] = _RemoteJQPosition(pos)
        positions_value = sum(float(pos.value or 0.0) for pos in positions.values())
        self._snapshot = {
            "account": account,
            "positions": positions,
            "positions_value": positions_value,
        }
        self._snapshot_at = now
        return self._snapshot


class _RemoteJQPortfolio:
    def __init__(self, cache: _RemoteSnapshotCache):
        self._cache = cache
        self.subportfolios = []

    @property
    def available_cash(self) -> float:
        return float(self._cache.snapshot()["account"].available_cash)

    @property
    def total_value(self) -> float:
        return float(self._cache.snapshot()["account"].total_value)

    @property
    def positions_value(self) -> float:
        return float(self._cache.snapshot()["positions_value"])

    @property
    def positions(self) -> _RemotePositionDict:
        return self._cache.snapshot()["positions"]


class _RemoteJQSubPortfolio(_RemoteJQPortfolio):
    @property
    def long_positions(self) -> _RemotePositionDict:
        return self.positions

    @property
    def short_positions(self) -> Dict[str, _RemoteJQPosition]:
        return {}

    @property
    def transferable_cash(self) -> float:
        return self.available_cash

    @property
    def locked_cash(self) -> float:
        return 0.0

    @property
    def type(self) -> str:
        return "stock"


def _run_type_from_context(context: Any) -> Optional[str]:
    run_params = getattr(context, "run_params", None)
    if isinstance(run_params, dict):
        return run_params.get("type")
    return getattr(run_params, "type", None)


def _restore_jq_compat(namespace: Dict[str, Any]) -> None:
    state = namespace.get(_JQ_COMPAT_STATE_KEY)
    if not isinstance(state, dict) or not state.get("installed"):
        return
    originals = state.get("originals") or {}
    for name, original in originals.items():
        if original is None:
            namespace.pop(name, None)
        else:
            namespace[name] = original
    state["installed"] = False


def _install_remote_context(context: Any, cache: _RemoteSnapshotCache) -> None:
    portfolio = _RemoteJQPortfolio(cache)
    subportfolio = _RemoteJQSubPortfolio(cache)
    portfolio.subportfolios = [subportfolio]
    setattr(context, "portfolio", portfolio)
    setattr(context, "subportfolios", [subportfolio])


def _extract_order_id(order_or_id: Any) -> str:
    if hasattr(order_or_id, "order_id"):
        return str(getattr(order_or_id, "order_id"))
    return str(order_or_id)


def _remote_order_result(
    order_id: str,
    security: str,
    amount: int,
    price: Optional[float],
    is_buy: bool,
) -> Optional[RemoteOrder]:
    if not order_id:
        return None
    return RemoteOrder(
        order_id=str(order_id),
        status="submitted",
        security=security,
        amount=abs(int(amount or 0)),
        price=float(price) if price is not None else None,
        is_buy=bool(is_buy),
    )


def _style_for_jq_mirror(
    style: Optional[Any],
    price: Optional[float],
    market: Optional[bool],
) -> Optional[Any]:
    if style is not None:
        return style
    if price is None:
        return None
    if market:
        return MarketOrderStyle(price)
    return LimitOrderStyle(price)


def _mirror_jq_order(
    original: Optional[Callable],
    args,
    kwargs,
) -> None:
    if not callable(original):
        return
    try:
        original(*args, **kwargs)
    except Exception as exc:
        _warn("聚宽镜像下单失败，仅影响聚宽页面展示，不影响远程真实订单: {}", exc)


def install_jq_compat(
    namespace: Dict[str, Any],
    *,
    context: Any,
    host: str,
    token: str,
    port: int = 58620,
    account_key: Optional[str] = None,
    sub_account_id: Optional[str] = None,
    mirror_jq_orders: bool = False,
    default_wait_timeout: float = DEFAULT_JQ_COMPAT_WAIT_TIMEOUT_SECONDS,
    tls_cert: Optional[str] = None,
    retries: int = 2,
    retry_interval: float = 0.5,
    rpc_timeout: float = DEFAULT_RPC_TIMEOUT_SECONDS,
    place_order_timeout_margin: float = DEFAULT_PLACE_ORDER_TIMEOUT_MARGIN_SECONDS,
    debug: bool = True,
) -> Dict[str, Any]:
    """安装聚宽模拟盘完全接管兼容层。

    回测环境不接管；仅在 `context.run_params.type == "sim_trade"` 时接管
    `context.portfolio`、`context.subportfolios` 和聚宽同名交易函数。
    """

    run_type = _run_type_from_context(context)
    state = namespace.get(_JQ_COMPAT_STATE_KEY)
    if not isinstance(state, dict):
        state = {
            "originals": {name: namespace.get(name) for name in _JQ_COMPAT_FUNCTIONS},
            "installed": False,
        }
        namespace[_JQ_COMPAT_STATE_KEY] = state

    if run_type in ("simple_backtest", "full_backtest"):
        _restore_jq_compat(namespace)
        _log("INFO", "聚宽兼容层检测到回测环境 {}，不接管远程交易", run_type)
        return {"enabled": False, "run_type": run_type, "reason": "backtest"}

    if run_type != "sim_trade":
        _restore_jq_compat(namespace)
        _warn("聚宽兼容层未识别运行环境 run_params.type={}，默认不接管远程交易", run_type)
        return {"enabled": False, "run_type": run_type, "reason": "unsupported_run_type"}

    configure(
        host=host,
        port=port,
        token=token,
        account_key=account_key,
        sub_account_id=sub_account_id,
        tls_cert=tls_cert,
        retries=retries,
        retry_interval=retry_interval,
        rpc_timeout=rpc_timeout,
        place_order_timeout_margin=place_order_timeout_margin,
        debug=debug,
    )
    broker = get_broker_client()
    cache = _RemoteSnapshotCache(broker)
    _install_remote_context(context, cache)
    originals = state.get("originals") or {}

    def compat_order(
        security: str,
        amount: int,
        style: Optional[Any] = None,
        side: str = "long",
        pindex: int = 0,
        close_today: bool = False,
        **kwargs,
    ) -> Optional[RemoteOrder]:
        _validate_jq_trade_scope(side=side, pindex=pindex, close_today=close_today)
        price = kwargs.pop("price", None)
        wait_timeout = _coerce_wait_timeout(kwargs.pop("wait_timeout", None), default_wait_timeout)
        market = kwargs.pop("market", None)
        remark = kwargs.pop("remark", None)
        order_remark = kwargs.pop("order_remark", None)
        idempotency_key = kwargs.pop("idempotency_key", None)
        price, market = _resolve_price_market(price=price, style=style, market=market)
        order_id = broker.order(
            security,
            amount,
            price=price,
            side=_normalise_side(side, amount),
            wait_timeout=wait_timeout,
            market=market,
            remark=remark,
            order_remark=order_remark,
            idempotency_key=idempotency_key,
        )
        cache.invalidate()
        if mirror_jq_orders and order_id:
            mirror_style = _style_for_jq_mirror(style, price, market)
            _mirror_jq_order(
                originals.get("order"),
                (security, amount, mirror_style),
                {"side": side, "pindex": pindex, "close_today": close_today},
            )
        return _remote_order_result(order_id, security, amount, price, amount > 0)

    def compat_order_value(
        security: str,
        value: float,
        style: Optional[Any] = None,
        side: str = "long",
        pindex: int = 0,
        close_today: bool = False,
        **kwargs,
    ) -> Optional[RemoteOrder]:
        _validate_jq_trade_scope(side=side, pindex=pindex, close_today=close_today)
        price = kwargs.pop("price", None)
        wait_timeout = _coerce_wait_timeout(kwargs.pop("wait_timeout", None), default_wait_timeout)
        market = kwargs.pop("market", None)
        price, market = _resolve_price_market(price=price, style=style, market=market)
        order_id = broker.order_value(
            security,
            value,
            price=price,
            wait_timeout=wait_timeout,
            style=style,
            side=side,
            pindex=pindex,
            close_today=close_today,
            market=market,
            remark=kwargs.pop("remark", None),
            order_remark=kwargs.pop("order_remark", None),
            idempotency_key=kwargs.pop("idempotency_key", None),
        )
        cache.invalidate()
        if mirror_jq_orders and order_id:
            mirror_style = _style_for_jq_mirror(style, price, market)
            _mirror_jq_order(
                originals.get("order_value"),
                (security, value, mirror_style),
                {"side": side, "pindex": pindex, "close_today": close_today},
            )
        return _remote_order_result(order_id, security, int(value), price, value > 0)

    def compat_order_percent(
        security: str,
        percent: float,
        style: Optional[Any] = None,
        side: str = "long",
        pindex: int = 0,
        close_today: bool = False,
        **kwargs,
    ) -> Optional[RemoteOrder]:
        _validate_jq_trade_scope(side=side, pindex=pindex, close_today=close_today)
        value = float(cache.snapshot()["account"].total_value) * float(percent)
        price = kwargs.pop("price", None)
        wait_timeout = _coerce_wait_timeout(kwargs.pop("wait_timeout", None), default_wait_timeout)
        market = kwargs.pop("market", None)
        price, market = _resolve_price_market(price=price, style=style, market=market)
        order_id = broker.order_value(
            security,
            value,
            price=price,
            wait_timeout=wait_timeout,
            style=style,
            side=side,
            pindex=pindex,
            close_today=close_today,
            market=market,
            remark=kwargs.pop("remark", None),
            order_remark=kwargs.pop("order_remark", None),
            idempotency_key=kwargs.pop("idempotency_key", None),
        )
        cache.invalidate()
        if mirror_jq_orders and order_id:
            mirror_style = _style_for_jq_mirror(style, price, market)
            _mirror_jq_order(
                originals.get("order_percent"),
                (security, percent, mirror_style),
                {"side": side, "pindex": pindex, "close_today": close_today},
            )
        return _remote_order_result(order_id, security, int(value), price, value > 0)

    def compat_order_target(
        security: str,
        amount: int,
        style: Optional[Any] = None,
        side: str = "long",
        pindex: int = 0,
        close_today: bool = False,
        **kwargs,
    ) -> Optional[RemoteOrder]:
        _validate_jq_trade_scope(side=side, pindex=pindex, close_today=close_today)
        price = kwargs.pop("price", None)
        wait_timeout = _coerce_wait_timeout(kwargs.pop("wait_timeout", None), default_wait_timeout)
        market = kwargs.pop("market", None)
        price, market = _resolve_price_market(price=price, style=style, market=market)
        current = broker._current_amount(security)
        order_id = broker.order_target(
            security,
            amount,
            price=price,
            wait_timeout=wait_timeout,
            style=style,
            side=side,
            pindex=pindex,
            close_today=close_today,
            market=market,
            remark=kwargs.pop("remark", None),
            order_remark=kwargs.pop("order_remark", None),
            idempotency_key=kwargs.pop("idempotency_key", None),
        )
        cache.invalidate()
        if mirror_jq_orders and order_id:
            mirror_style = _style_for_jq_mirror(style, price, market)
            _mirror_jq_order(
                originals.get("order_target"),
                (security, amount, mirror_style),
                {"side": side, "pindex": pindex, "close_today": close_today},
            )
        return _remote_order_result(order_id, security, amount - current, price, amount >= current)

    def compat_order_target_value(
        security: str,
        value: Optional[float] = None,
        style: Optional[Any] = None,
        side: str = "long",
        pindex: int = 0,
        close_today: bool = False,
        **kwargs,
    ) -> Optional[RemoteOrder]:
        _validate_jq_trade_scope(side=side, pindex=pindex, close_today=close_today)
        has_target_value = "target_value" in kwargs
        target_value = kwargs.pop("target_value", value)
        if has_target_value and value is not None:
            raise TypeError("order_target_value() got both 'value' and 'target_value'")
        if target_value is None:
            raise TypeError("order_target_value() missing required argument: 'value'")
        current_value = float(cache.snapshot()["positions"][security].value)
        price = kwargs.pop("price", None)
        wait_timeout = _coerce_wait_timeout(kwargs.pop("wait_timeout", None), default_wait_timeout)
        market = kwargs.pop("market", None)
        price, market = _resolve_price_market(price=price, style=style, market=market)
        order_id = broker.order_target_value(
            security,
            target_value,
            price=price,
            wait_timeout=wait_timeout,
            style=style,
            side=side,
            pindex=pindex,
            close_today=close_today,
            market=market,
            remark=kwargs.pop("remark", None),
            order_remark=kwargs.pop("order_remark", None),
            idempotency_key=kwargs.pop("idempotency_key", None),
        )
        cache.invalidate()
        if mirror_jq_orders and order_id:
            mirror_style = _style_for_jq_mirror(style, price, market)
            _mirror_jq_order(
                originals.get("order_target_value"),
                (security, target_value, mirror_style),
                {"side": side, "pindex": pindex, "close_today": close_today},
            )
        return _remote_order_result(
            order_id,
            security,
            int(float(target_value) - current_value),
            price,
            float(target_value) >= current_value,
        )

    def compat_order_target_percent(
        security: str,
        percent: float,
        style: Optional[Any] = None,
        side: str = "long",
        pindex: int = 0,
        close_today: bool = False,
        **kwargs,
    ) -> Optional[RemoteOrder]:
        _validate_jq_trade_scope(side=side, pindex=pindex, close_today=close_today)
        snapshot = cache.snapshot()
        target_value = float(snapshot["account"].total_value) * float(percent)
        current_value = float(snapshot["positions"][security].value)
        price = kwargs.pop("price", None)
        wait_timeout = _coerce_wait_timeout(kwargs.pop("wait_timeout", None), default_wait_timeout)
        market = kwargs.pop("market", None)
        price, market = _resolve_price_market(price=price, style=style, market=market)
        order_id = broker.order_target_value(
            security,
            target_value,
            price=price,
            wait_timeout=wait_timeout,
            style=style,
            side=side,
            pindex=pindex,
            close_today=close_today,
            market=market,
            remark=kwargs.pop("remark", None),
            order_remark=kwargs.pop("order_remark", None),
            idempotency_key=kwargs.pop("idempotency_key", None),
        )
        cache.invalidate()
        if mirror_jq_orders and order_id:
            mirror_style = _style_for_jq_mirror(style, price, market)
            _mirror_jq_order(
                originals.get("order_target_percent"),
                (security, percent, mirror_style),
                {"side": side, "pindex": pindex, "close_today": close_today},
            )
        return _remote_order_result(
            order_id,
            security,
            int(target_value - current_value),
            price,
            target_value >= current_value,
        )

    def compat_cancel_order(order_or_id: Any) -> Dict[str, Any]:
        result = broker.cancel_order(_extract_order_id(order_or_id))
        cache.invalidate()
        return result

    namespace.update(
        {
            "order": compat_order,
            "order_value": compat_order_value,
            "order_percent": compat_order_percent,
            "order_target": compat_order_target,
            "order_target_value": compat_order_target_value,
            "order_target_percent": compat_order_target_percent,
            "cancel_order": compat_cancel_order,
            "get_open_orders": lambda: broker.get_open_orders(),
            "get_orders": broker.get_orders,
            "get_trades": broker.get_trades,
        }
    )
    state.update(
        {
            "installed": True,
            "run_type": run_type,
            "cache": cache,
            "context": context,
            "mirror_jq_orders": bool(mirror_jq_orders),
            "default_wait_timeout": float(default_wait_timeout),
        }
    )
    _log("INFO", "聚宽模拟盘完全接管已启用: account_key={}, sub_account_id={}", account_key, sub_account_id)
    return {"enabled": True, "run_type": run_type}


# --------- 便捷函数（JQ 兼容） ----------
def order(
    security: str,
    amount: int,
    price: Optional[float] = None,
    side: Optional[str] = None,
    wait_timeout: float = 0,
    *,
    style: Optional[Any] = None,
    market: Optional[bool] = None,
    remark: Optional[str] = None,
    order_remark: Optional[str] = None,
    idempotency_key: Optional[str] = None,
) -> str:
    return get_broker_client().order(
        security,
        amount,
        price=price,
        side=side,
        wait_timeout=wait_timeout,
        style=style,
        market=market,
        remark=remark,
        order_remark=order_remark,
        idempotency_key=idempotency_key,
    )


def order_value(
    security: str,
    value: float,
    price: Optional[float] = None,
    wait_timeout: float = 0,
    *,
    style: Optional[Any] = None,
    side: Optional[str] = None,
    pindex: int = 0,
    close_today: bool = False,
    market: Optional[bool] = None,
    remark: Optional[str] = None,
    order_remark: Optional[str] = None,
    idempotency_key: Optional[str] = None,
) -> str:
    return get_broker_client().order_value(
        security,
        value,
        price=price,
        wait_timeout=wait_timeout,
        style=style,
        side=side,
        pindex=pindex,
        close_today=close_today,
        market=market,
        remark=remark,
        order_remark=order_remark,
        idempotency_key=idempotency_key,
    )


def order_percent(
    security: str,
    percent: float,
    price: Optional[float] = None,
    wait_timeout: float = 0,
    *,
    style: Optional[Any] = None,
    side: Optional[str] = None,
    pindex: int = 0,
    close_today: bool = False,
    market: Optional[bool] = None,
    remark: Optional[str] = None,
    order_remark: Optional[str] = None,
    idempotency_key: Optional[str] = None,
) -> str:
    return get_broker_client().order_percent(
        security,
        percent,
        price=price,
        wait_timeout=wait_timeout,
        style=style,
        side=side,
        pindex=pindex,
        close_today=close_today,
        market=market,
        remark=remark,
        order_remark=order_remark,
        idempotency_key=idempotency_key,
    )


def order_target(
    security: str,
    target: int,
    price: Optional[float] = None,
    wait_timeout: float = 0,
    *,
    style: Optional[Any] = None,
    side: Optional[str] = None,
    pindex: int = 0,
    close_today: bool = False,
    market: Optional[bool] = None,
    remark: Optional[str] = None,
    order_remark: Optional[str] = None,
    idempotency_key: Optional[str] = None,
) -> str:
    return get_broker_client().order_target(
        security,
        target,
        price=price,
        wait_timeout=wait_timeout,
        style=style,
        side=side,
        pindex=pindex,
        close_today=close_today,
        market=market,
        remark=remark,
        order_remark=order_remark,
        idempotency_key=idempotency_key,
    )


def order_target_value(
    security: str,
    target_value: Optional[float] = None,
    price: Optional[float] = None,
    wait_timeout: float = 0,
    *,
    value: Optional[float] = None,
    style: Optional[Any] = None,
    side: Optional[str] = None,
    pindex: int = 0,
    close_today: bool = False,
    market: Optional[bool] = None,
    remark: Optional[str] = None,
    order_remark: Optional[str] = None,
    idempotency_key: Optional[str] = None,
) -> str:
    return get_broker_client().order_target_value(
        security,
        target_value,
        price=price,
        wait_timeout=wait_timeout,
        value=value,
        style=style,
        side=side,
        pindex=pindex,
        close_today=close_today,
        market=market,
        remark=remark,
        order_remark=order_remark,
        idempotency_key=idempotency_key,
    )


def order_target_percent(
    security: str,
    percent: float,
    price: Optional[float] = None,
    wait_timeout: float = 0,
    *,
    style: Optional[Any] = None,
    side: Optional[str] = None,
    pindex: int = 0,
    close_today: bool = False,
    market: Optional[bool] = None,
    remark: Optional[str] = None,
    order_remark: Optional[str] = None,
    idempotency_key: Optional[str] = None,
) -> str:
    return get_broker_client().order_target_percent(
        security,
        percent,
        price=price,
        wait_timeout=wait_timeout,
        style=style,
        side=side,
        pindex=pindex,
        close_today=close_today,
        market=market,
        remark=remark,
        order_remark=order_remark,
        idempotency_key=idempotency_key,
    )


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
    "install_jq_compat",
    "get_data_client",
    "get_broker_client",
    "order",
    "order_value",
    "order_percent",
    "order_target",
    "order_target_value",
    "order_target_percent",
    "cancel_order",
    "get_order_status",
    "get_open_orders",
    "get_orders",
    "get_trades",
    "get_account",
    "get_positions",
    "MarketOrderStyle",
    "LimitOrderStyle",
    "RemoteAccount",
    "RemoteOrder",
    "RemoteTrade",
    "RemotePosition",
    "RemoteDataClient",
    "RemoteBrokerClient",
]
