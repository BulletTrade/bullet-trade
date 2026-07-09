from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, List, Optional

from .base import BrokerBase
from ..remote import RemoteQmtConnection
from ..core.globals import log


DEFAULT_REMOTE_RPC_TIMEOUT_SECONDS = 60.0
DEFAULT_PLACE_ORDER_TIMEOUT_MARGIN_SECONDS = 30.0
ORDER_EXTRA_PAYLOAD_KEYS = frozenset(
    {
        "signal_batch_id",
        "execution_batch_id",
        "strategy_name",
        "client_id",
        "request_id",
        "idempotency_key",
        "order_remark",
    }
)


def _env(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(key, default)


def _safe_positive_float(value: Any, default: float) -> float:
    """解析正数浮点配置。

    Args:
        value: 原始配置值。
        default: 解析失败或非正数时使用的默认值。

    Returns:
        float: 正数浮点值。
    """
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = float(default)
    if parsed <= 0:
        parsed = float(default)
    return parsed


def _safe_non_negative_float(value: Any, default: float) -> float:
    """解析非负浮点配置。

    Args:
        value: 原始配置值。
        default: 解析失败或为负数时使用的默认值。

    Returns:
        float: 非负浮点值。
    """
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = float(default)
    if parsed < 0:
        parsed = float(default)
    return parsed


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
        self.rpc_timeout = _safe_positive_float(
            self.config.get("rpc_timeout") or _env("QMT_SERVER_RPC_TIMEOUT"),
            DEFAULT_REMOTE_RPC_TIMEOUT_SECONDS,
        )
        place_order_timeout_margin = self.config.get("place_order_timeout_margin")
        if place_order_timeout_margin is None:
            place_order_timeout_margin = _env("QMT_PLACE_ORDER_TIMEOUT_MARGIN")
        self.place_order_timeout_margin = _safe_non_negative_float(
            place_order_timeout_margin,
            DEFAULT_PLACE_ORDER_TIMEOUT_MARGIN_SECONDS,
        )
        default_wait_timeout = self.config.get("wait_timeout")
        if default_wait_timeout is None:
            default_wait_timeout = self.config.get("trade_max_wait_time")
        if default_wait_timeout is None:
            default_wait_timeout = _env("TRADE_MAX_WAIT_TIME")
        self.default_wait_timeout = _safe_non_negative_float(default_wait_timeout, 0.0)
        self._warn_if_timeout_budget_is_risky()
        self.account_key = self.config.get("account_key") or _env("QMT_SERVER_ACCOUNT_KEY")
        self.sub_account_id = self.config.get("sub_account_id") or _env("QMT_SERVER_SUB_ACCOUNT")
        self._connection = RemoteQmtConnection(
            host,
            port,
            token,
            tls_cert=tls_cert,
            tls_enabled=tls_enabled,
            request_timeout=self.rpc_timeout,
        )
        self._last_warning: Optional[str] = None
        self._last_order_responses: Dict[str, Dict[str, Any]] = {}

    def _warn_if_timeout_budget_is_risky(self) -> None:
        """检查远程下单默认超时预算是否存在明显风险。

        Args:
            None。

        Returns:
            None。

        Side Effects:
            发现默认 RPC timeout 小于等于默认等待窗口时输出 warning；不抛异常，
            以保持开源用户旧配置和旧 server 组合的启动兼容性。
        """

        if self.default_wait_timeout <= 0:
            return
        required = self.default_wait_timeout + self.place_order_timeout_margin
        if self.rpc_timeout >= required:
            return
        log.warning(
            "QMT remote 下单超时配置风险: QMT_SERVER_RPC_TIMEOUT=%.1fs, "
            "TRADE_MAX_WAIT_TIME=%.1fs, QMT_PLACE_ORDER_TIMEOUT_MARGIN=%.1fs；"
            "下单时会临时使用至少 %.1fs 的请求超时，建议同步调整默认配置。",
            self.rpc_timeout,
            self.default_wait_timeout,
            self.place_order_timeout_margin,
            required,
        )

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
        open_states = {"new", "submitted", "open", "filling", "canceling"}
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
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        return await self._place_order("BUY", security, amount, price, wait_timeout, remark, market, extra)

    async def sell(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        wait_timeout: Optional[float] = None,
        remark: Optional[str] = None,
        *,
        market: bool = False,
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        return await self._place_order("SELL", security, amount, price, wait_timeout, remark, market, extra)

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
        """同步账户快照，兼容 LiveEngine 需要的现金+持仓联合视图。"""
        payload = dict(self.get_account_info() or {})
        try:
            payload["positions"] = self.get_positions()
        except Exception:
            payload.setdefault("positions", [])
        return payload

    def _place_order(
        self,
        side: str,
        security: str,
        amount: int,
        price: Optional[float],
        wait_timeout: Optional[float],
        remark: Optional[str] = None,
        market: bool = False,
        extra: Optional[Dict[str, Any]] = None,
    ) -> asyncio.Future:
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(
            None, self._place_order_sync, side, security, amount, price, wait_timeout, remark, market, extra
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
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        self._last_warning = None
        payload = self._base_payload()
        effective_market = bool(market or price is None)
        style = {"type": "market" if effective_market else "limit"}
        if price is not None:
            if effective_market:
                style["protect_price"] = price
            else:
                style["price"] = price
        if price is None and not effective_market:
            raise ValueError("限价单缺少价格，请提供 price 或将 market 设为 True")
        effective_wait_timeout = self._resolve_order_wait_timeout(wait_timeout)
        if effective_wait_timeout is not None:
            payload["wait_timeout"] = effective_wait_timeout
        if effective_market:
            payload["market"] = True
        if remark:
            payload["order_remark"] = remark
        self._merge_order_extra_payload(payload, extra)
        payload.update(
            {
                "security": security,
                "amount": amount,
                "side": side,
                "style": style,
            }
        )
        try:
            resp = self._connection.request(
                "broker.place_order",
                payload,
                timeout=self._resolve_place_order_rpc_timeout(effective_wait_timeout),
            )
        except TimeoutError as exc:
            raise RuntimeError(
                f"远程券商下单请求超时，状态=submit_unknown: side={side} security={security} amount={amount}"
            ) from exc
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
        status = str(resp.get("status") or resp.get("order_status") or "").strip().lower()
        if status == "submit_unknown":
            raise RuntimeError(f"远程券商下单提交状态未知: order_id={resp.get('order_id') or ''} response={resp}")
        order_id = resp.get("order_id")
        if not order_id:
            raise RuntimeError(f"远程券商未返回 order_id: {resp}")
        if status in {"rejected", "canceled", "cancelled", "failed", "error"}:
            raise RuntimeError(f"远程券商下单失败: order_id={order_id} status={status} response={resp}")
        self._last_order_responses[str(order_id)] = dict(resp)
        return str(order_id)

    def _merge_order_extra_payload(self, payload: Dict[str, Any], extra: Optional[Dict[str, Any]]) -> None:
        """把订单审计扩展字段白名单透传到远端下单 payload。"""

        if not isinstance(extra, dict):
            return
        for key in ORDER_EXTRA_PAYLOAD_KEYS:
            value = extra.get(key)
            if value in (None, ""):
                continue
            if key == "order_remark" and payload.get("order_remark"):
                continue
            payload[key] = value

    def get_last_order_response(self, order_id: str) -> Dict[str, Any]:
        """读取最近一次下单响应。

        Args:
            order_id: 远端订单号。

        Returns:
            Dict[str, Any]: 服务端下单响应副本；没有记录时返回空字典。
        """
        return dict(self._last_order_responses.get(str(order_id), {}) or {})

    def _resolve_order_wait_timeout(self, wait_timeout: Optional[float]) -> Optional[float]:
        """解析本次要传给服务端的订单等待窗口。

        Args:
            wait_timeout: 单笔下单参数；None 表示使用远程券商配置默认值。

        Returns:
            Optional[float]: 应放入请求 payload 的等待秒数；None 表示保持旧行为，
            由服务端自行读取默认配置。
        """

        if wait_timeout is not None:
            try:
                return max(0.0, float(wait_timeout))
            except (TypeError, ValueError):
                return 0.0
        if self.default_wait_timeout > 0:
            return self.default_wait_timeout
        return None

    def _resolve_place_order_rpc_timeout(self, wait_timeout: Optional[float]) -> float:
        """解析下单 RPC 请求超时。

        Args:
            wait_timeout: 本次订单等待终态窗口。

        Returns:
            float: 远程下单请求超时时间，保证大于等待窗口。
        """
        if wait_timeout is not None:
            try:
                wait_seconds = float(wait_timeout)
            except (TypeError, ValueError):
                wait_seconds = 0.0
        else:
            wait_seconds = self.default_wait_timeout
        if wait_seconds <= 0:
            return self.rpc_timeout
        return max(self.rpc_timeout, wait_seconds + self.place_order_timeout_margin)

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
