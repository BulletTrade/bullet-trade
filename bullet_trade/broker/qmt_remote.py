"""
作者: BruceLee
文件说明:
    远程 QMT/BulletTrade server 的统一 Broker 客户端适配。
    主要输入为标准 Broker 下单、撤单和查询调用，输出为通用订单号与账户事实。
    该层位于 LiveEngine 与 RemoteQmtConnection 之间，不感知底层券商厂商。
    交易写请求携带稳定幂等键，模糊结果只能通过只读对账收口。
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import uuid
from typing import Any, Dict, List, Optional

from ..core.globals import log
from ..remote import RemoteQmtConnection, RemoteSubmissionUnknownError
from .base import BrokerBase

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
        "market_type",
        "execution_claim_token",
        "execution_claim_generation",
        "gateway_id_snapshot",
        "sub_account_binding_id_snapshot",
        "backend_provider",
        "binding_version",
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


def _new_idempotency_key(operation: str) -> str:
    """为一次远程写操作生成不可复用的幂等键。

    Args:
        operation: `place_order` 或 `cancel_order` 等写操作名。

    Returns:
        str: 带操作前缀的随机幂等键。
    """

    return f"bt-{str(operation).strip() or 'write'}-{uuid.uuid4().hex}"


def _local_unknown_resolution(
    write_action: str,
    idempotency_key: str,
    *,
    order_id: Optional[str] = None,
    reason: str,
) -> Dict[str, Any]:
    """在解析服务也不可用时构造稳定的本地未知态。

    Args:
        write_action: 原始写 action。
        idempotency_key: 原始幂等键。
        order_id: 可选精确订单号。
        reason: 保持未知态的原因。

    Returns:
        Dict[str, Any]: fail-closed `submit_unknown` 结果。
    """

    stable_id = str(order_id or "").strip()
    if not stable_id:
        digest = hashlib.sha256(str(idempotency_key).encode("utf-8")).hexdigest()[:24]
        stable_id = f"submit_unknown:{digest}"
    return {
        "status": "submit_unknown",
        "submission_state": "submit_unknown",
        "write_action": write_action,
        "idempotency_key": idempotency_key,
        "order_id": stable_id,
        "stable_local_order_id": stable_id,
        "reason": reason,
    }


def _resolution_matches_place_request(
    resolution: Dict[str, Any], request_payload: Dict[str, Any], order_id: str
) -> bool:
    """验证只读解析返回的订单确实对应原下单，不信任裸 accepted 标记。

    Args:
        resolution: server 返回的 `resolve_submission` 响应。
        request_payload: 原始下单请求。
        order_id: 解析结果声明的精确订单号。

    Returns:
        bool: 原 action、幂等键、订单号、证券、方向和数量均一致时返回 True。
    """

    if str(resolution.get("write_action") or "") != "broker.place_order":
        return False
    key = str(request_payload.get("idempotency_key") or "")
    if not key or str(resolution.get("idempotency_key") or "") != key:
        return False
    row = resolution.get("resolved_result")
    if not isinstance(row, dict):
        return False
    if str(row.get("order_id") or "").strip() != str(order_id).strip():
        return False
    try:
        matches_amount = int(row.get("amount") or row.get("volume") or 0) == int(
            request_payload.get("amount") or request_payload.get("volume") or 0
        )
    except (TypeError, ValueError):
        return False
    row_key = str(row.get("idempotency_key") or "").strip()
    return (
        row_key == key
        and str(row.get("security") or "").strip()
        == str(request_payload.get("security") or "").strip()
        and str(row.get("side") or row.get("direction") or "").strip().upper()
        == str(request_payload.get("side") or "").strip().upper()
        and matches_amount
    )


def _resolution_matches_cancel_request(
    resolution: Dict[str, Any], request_payload: Dict[str, Any], expected_order_id: str
) -> bool:
    """验证撤单解析只确认目标订单已取消，不把成交或拒绝伪装为成功。

    Args:
        resolution: server 返回的 `resolve_submission` 响应。
        request_payload: 原始撤单请求。
        expected_order_id: 必须精确相等的目标订单号。

    Returns:
        bool: action/key/order_id 和取消终态都一致时返回 True。
    """

    if str(resolution.get("write_action") or "") != "broker.cancel_order":
        return False
    key = str(request_payload.get("idempotency_key") or "")
    if not key or str(resolution.get("idempotency_key") or "") != key:
        return False
    row = resolution.get("resolved_result")
    if not isinstance(row, dict):
        return False
    actual_order_id = str(
        row.get("order_id") or (row.get("last_snapshot") or {}).get("order_id") or ""
    )
    status = str(row.get("status") or (row.get("last_snapshot") or {}).get("status") or "").lower()
    return actual_order_id == expected_order_id and status in {
        "canceled",
        "cancelled",
        "partly_canceled",
        "partly_cancelled",
    }


def _is_confirmed_cancel_result(result: Any, expected_order_id: str) -> bool:
    """判断直接撤单响应是否已精确证明目标订单被取消。

    Args:
        result: broker.cancel_order 的响应。
        expected_order_id: 用户请求撤销的精确订单号。

    Returns:
        bool: 仅 exact ID 与 canceled/partly_canceled 状态组合返回 True。
    """

    if not isinstance(result, dict):
        return False
    row = result.get("last_snapshot") if isinstance(result.get("last_snapshot"), dict) else result
    actual_order_id = str(row.get("order_id") or result.get("order_id") or "")
    status = str(row.get("status") or row.get("order_status") or result.get("status") or "").lower()
    return actual_order_id == str(expected_order_id) and status in {
        "canceled",
        "cancelled",
        "partly_canceled",
        "partly_cancelled",
    }


class RemoteQmtBroker(BrokerBase):
    """
    使用 RemoteQmtConnection 与 bullet-trade server 交互的券商实现。
    """

    def __init__(
        self, account_id: str, account_type: str = "stock", config: Optional[Dict[str, Any]] = None
    ):
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
        info = dict(resp.get("value") or resp if isinstance(resp, dict) else {})
        if "positions" not in info:
            info["positions"] = self.get_positions()
        return info

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
        return await self._place_order(
            "BUY", security, amount, price, wait_timeout, remark, market, extra
        )

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
        return await self._place_order(
            "SELL", security, amount, price, wait_timeout, remark, market, extra
        )

    async def cancel_order(self, order_id: str) -> bool:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, self._cancel_sync, order_id)
        ok = _is_confirmed_cancel_result(result, order_id)
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
            None,
            self._place_order_sync,
            side,
            security,
            amount,
            price,
            wait_timeout,
            remark,
            market,
            extra,
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
        payload.setdefault("idempotency_key", _new_idempotency_key("place_order"))
        try:
            resp = self._connection.request(
                "broker.place_order",
                payload,
                timeout=self._resolve_place_order_rpc_timeout(effective_wait_timeout),
            )
        except RemoteSubmissionUnknownError as exc:
            resolution = self._try_resolve_submission(
                idempotency_key=exc.idempotency_key,
                write_action="broker.place_order",
                request_payload=payload,
            )
            return self._resolved_place_order_or_raise(payload, resolution, cause=exc)
        except TimeoutError as exc:
            resolution = self._try_resolve_submission(
                idempotency_key=str(payload["idempotency_key"]),
                write_action="broker.place_order",
                request_payload=payload,
            )
            return self._resolved_place_order_or_raise(payload, resolution, cause=exc)
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
        if status in {"submit_unknown", "reconciling"}:
            resolution = self._try_resolve_submission(
                idempotency_key=str(payload["idempotency_key"]),
                write_action="broker.place_order",
                request_payload=payload,
            )
            return self._resolved_place_order_or_raise(payload, resolution)
        order_id = resp.get("order_id")
        if not order_id:
            raise RuntimeError(f"远程券商未返回 order_id: {resp}")
        if status in {"rejected", "canceled", "cancelled", "failed", "error"}:
            raise RuntimeError(f"远程券商下单失败: order_id={order_id} status={status} response={resp}")
        self._last_order_responses[str(order_id)] = dict(resp)
        return str(order_id)

    def _merge_order_extra_payload(
        self, payload: Dict[str, Any], extra: Optional[Dict[str, Any]]
    ) -> None:
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

    def resolve_submission(
        self,
        idempotency_key: str,
        *,
        write_action: str,
        request_payload: Optional[Dict[str, Any]] = None,
        order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """用原幂等键只读解析一次模糊下单或撤单。

        Args:
            idempotency_key: 原始写请求幂等键。
            write_action: 原始 `broker.place_order` 或 `broker.cancel_order` action。
            request_payload: 原始写请求载荷。
            order_id: 可选的精确订单号。

        Returns:
            Dict[str, Any]: 服务端返回的提交解析结果。

        Side Effects:
            仅发送 `broker.resolve_submission` 只读请求，不重发原写 action。
        """

        resolver = getattr(self._connection, "resolve_submission", None)
        if callable(resolver):
            return resolver(
                idempotency_key,
                write_action=write_action,
                request_payload=request_payload,
                order_id=order_id,
                context=self._base_payload(),
                timeout=self.rpc_timeout,
            )
        payload = self._base_payload()
        payload.update(
            {
                "idempotency_key": str(idempotency_key),
                "write_action": write_action,
                "request_payload": dict(request_payload or {}),
            }
        )
        if order_id:
            payload["order_id"] = str(order_id)
        return self._connection.request(
            "broker.resolve_submission",
            payload,
            timeout=self.rpc_timeout,
        )

    def _try_resolve_submission(
        self,
        *,
        idempotency_key: str,
        write_action: str,
        request_payload: Dict[str, Any],
        order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """尝试只读解析模糊写，解析失败时保持 fail-closed。

        Args:
            idempotency_key: 原始幂等键。
            write_action: 原始写 action。
            request_payload: 原始写请求载荷。
            order_id: 可选精确订单号。

        Returns:
            Dict[str, Any]: 有证据的解析结果，或本地 `submit_unknown` 结果。
        """

        try:
            return self.resolve_submission(
                idempotency_key,
                write_action=write_action,
                request_payload=request_payload,
                order_id=order_id,
            )
        except Exception as exc:
            log.warning(
                "远程写结果解析失败，保持 submit_unknown: action=%s key=%s error=%s",
                write_action,
                idempotency_key,
                exc,
            )
            return _local_unknown_resolution(
                write_action,
                idempotency_key,
                order_id=order_id,
                reason="resolve_request_failed",
            )

    def _resolved_place_order_or_raise(
        self,
        request_payload: Dict[str, Any],
        resolution: Dict[str, Any],
        *,
        cause: Optional[BaseException] = None,
    ) -> str:
        """在只读解析确认下单后返回订单号，否则明确失败。

        Args:
            request_payload: 原始下单载荷。
            resolution: `broker.resolve_submission` 结果。
            cause: 可选的原始传输异常。

        Returns:
            str: 只读证据确认的订单号。

        Raises:
            RuntimeError: 解析明确拒绝时抛出。
            RemoteSubmissionUnknownError: 证据不足时抛出。
        """

        state = str(resolution.get("submission_state") or resolution.get("status") or "").lower()
        key = str(request_payload.get("idempotency_key") or "")
        order_id = str(
            resolution.get("order_id")
            or resolution.get("stable_local_order_id")
            or (resolution.get("resolved_result") or {}).get("order_id")
            or ""
        ).strip()
        if (
            state == "accepted"
            and order_id
            and not order_id.startswith("submit_unknown:")
            and _resolution_matches_place_request(resolution, request_payload, order_id)
        ):
            response = dict(resolution.get("resolved_result") or resolution)
            response.setdefault("order_id", order_id)
            response.setdefault("idempotency_key", key)
            self._last_order_responses[order_id] = response
            return order_id
        if state == "rejected":
            raise RuntimeError(f"远程券商下单已确认拒绝: response={resolution}") from cause
        error = RemoteSubmissionUnknownError(
            "broker.place_order",
            key,
            request_payload,
            message="read-only resolution remains inconclusive",
            resolution=resolution,
        )
        if cause is not None:
            raise error from cause
        raise error

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
                hist = self._connection.request(
                    "data.history", {"security": security, "count": 1, "frequency": "1m"}
                )
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
        payload["idempotency_key"] = _new_idempotency_key("cancel_order")
        try:
            result = self._connection.request("broker.cancel_order", payload)
        except RemoteSubmissionUnknownError as exc:
            resolution = self._try_resolve_submission(
                idempotency_key=exc.idempotency_key,
                write_action="broker.cancel_order",
                request_payload=payload,
                order_id=order_id,
            )
            return self._resolved_cancel_or_raise(payload, resolution, cause=exc)
        except TimeoutError as exc:
            resolution = self._try_resolve_submission(
                idempotency_key=str(payload["idempotency_key"]),
                write_action="broker.cancel_order",
                request_payload=payload,
                order_id=order_id,
            )
            return self._resolved_cancel_or_raise(payload, resolution, cause=exc)
        status = str(result.get("submission_state") or result.get("status") or "").lower()
        if status in {"submit_unknown", "reconciling"}:
            resolution = self._try_resolve_submission(
                idempotency_key=str(payload["idempotency_key"]),
                write_action="broker.cancel_order",
                request_payload=payload,
                order_id=order_id,
            )
            return self._resolved_cancel_or_raise(payload, resolution)
        if not _is_confirmed_cancel_result(result, order_id):
            resolution = self._try_resolve_submission(
                idempotency_key=str(payload["idempotency_key"]),
                write_action="broker.cancel_order",
                request_payload=payload,
                order_id=order_id,
            )
            return self._resolved_cancel_or_raise(payload, resolution)
        return result

    def _resolved_cancel_or_raise(
        self,
        request_payload: Dict[str, Any],
        resolution: Dict[str, Any],
        *,
        cause: Optional[BaseException] = None,
    ) -> Dict[str, Any]:
        """在只读证据确认撤单结果后返回，否则 fail-closed。

        Args:
            request_payload: 原始撤单载荷。
            resolution: `broker.resolve_submission` 结果。
            cause: 可选原始传输异常。

        Returns:
            Dict[str, Any]: 确认撤单成功的兼容响应。

        Raises:
            RuntimeError: 解析明确撤单失败时抛出。
            RemoteSubmissionUnknownError: 证据不足时抛出。
        """

        state = str(resolution.get("submission_state") or resolution.get("status") or "").lower()
        key = str(request_payload.get("idempotency_key") or "")
        expected_order_id = str(request_payload.get("order_id") or "").strip()
        if state == "accepted" and _resolution_matches_cancel_request(
            resolution, request_payload, expected_order_id
        ):
            result = dict(resolution.get("resolved_result") or {})
            if not result:
                result = {
                    "dtype": "dict",
                    "value": True,
                    "order_id": expected_order_id,
                    "status": "canceled",
                    "cancel_outcome": "cancelled",
                }
            result.setdefault("idempotency_key", key)
            return result
        if state == "rejected":
            raise RuntimeError(f"远程券商撤单已确认失败: response={resolution}") from cause
        error = RemoteSubmissionUnknownError(
            "broker.cancel_order",
            key,
            request_payload,
            message="read-only cancel resolution remains inconclusive",
            resolution=resolution,
        )
        if cause is not None:
            raise error from cause
        raise error

    def _order_status_sync(self, order_id: str) -> Dict:
        payload = self._base_payload()
        payload["order_id"] = order_id
        return self._connection.request("broker.order_status", payload)

    def _base_payload(self) -> Dict[str, Any]:
        payload = {"account_key": self.account_key, "sub_account_id": self.sub_account_id}
        return payload
