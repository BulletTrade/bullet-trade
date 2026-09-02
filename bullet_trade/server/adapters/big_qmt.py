from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

from bullet_trade.utils.env_loader import get_env, get_env_float

from ..config import ServerConfig
from . import register_adapter
from .base import (
    AccountContext,
    AccountRouter,
    AdapterBundle,
    RemoteBrokerAdapter,
    RemoteDataAdapter,
    mark_broker_call_started,
)
from .qmt import dataframe_to_payload, dict_payload

_DATA_ACTIONS = (
    "data.history",
    "data.snapshot",
    "data.current_tick",
    "data.live_current",
    "data.trade_days",
    "data.security_info",
    "data.ensure_cache",
    "data.get_all_securities",
    "data.get_index_stocks",
    "data.get_split_dividend",
    "data.subscribe",
    "data.unsubscribe",
    "data.unsubscribe_all",
)

_POLLING_SUBSCRIPTION_ACTIONS = {
    "data.subscribe",
    "data.unsubscribe",
    "data.unsubscribe_all",
}

_BROKER_READ_ACTIONS = (
    "broker.account",
    "broker.positions",
    "broker.orders",
    "broker.trades",
    "broker.order_status",
)

_ADMIN_ACTIONS = ("admin.health", "admin.print_account")

_BIG_QMT_PRE_BROKER_ERROR_CODES = frozenset(
    {
        "TRADING_DISABLED",
        "QMT_NOT_READY",
        "ACCOUNT_NOT_CONFIGURED",
        "BAD_REQUEST",
        "CANCEL_ORDER_DISABLED",
        "MISSING_ORDER_ID",
        "VIRTUAL_ACCOUNT_MISMATCH",
        "ORDER_NOT_CANCELABLE",
        "DANGEROUS_OPERATION_DISABLED",
        "GATEWAY_BUSY",
    }
)

_ORDER_STATUS_MAP = {
    0: "unknown",
    48: "open",
    49: "open",
    50: "open",
    51: "open",
    52: "partly_filled",
    53: "partly_filled",
    54: "partly_canceled",
    55: "cancelled",
    56: "filled",
    57: "rejected",
    86: "cancelled",
    255: "unknown",
}

_ORDER_CONFIRM_POLL_INTERVAL_SECONDS = 0.25
_ORDER_CONFIRM_MAX_CLOCK_SKEW_SECONDS = 60.0
_CANCEL_CONFIRM_TIMEOUT_SECONDS = 3.0

_BIG_QMT_MARKET_PRICE_TYPES = {
    "XSHG": {
        "opponent_best": 44,
        "home_best": 45,
        "five_level_ioc": 42,
        "five_level_to_limit": 43,
    },
    "XSHE": {
        "opponent_best": 44,
        "home_best": 45,
        "immediate_or_cancel": 46,
        "five_level_ioc": 47,
        "fill_or_kill": 48,
    },
}


@dataclass
class BigQmtGatewayConfig:
    base_url: str = "http://127.0.0.1:9000"
    password: Optional[str] = None
    secret: Optional[str] = None
    timeout_seconds: float = 10.0
    health_ttl_seconds: float = 15.0
    action_status: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class BigQmtGatewayError(RuntimeError):
    """保存大 QMT Gateway 错误及其券商调用边界事实。"""

    def __init__(
        self,
        message: str,
        *,
        code: str = "BIG_QMT_GATEWAY_ERROR",
        broker_called: Optional[bool] = None,
    ) -> None:
        """创建可向上传递的结构化 Gateway 错误。

        Args:
            message: 可读错误说明。
            code: 稳定错误码。
            broker_called: Gateway 是否已经调用 QMT ``passorder``；未知时为空。

        Returns:
            None: 初始化异常实例。
        """

        super().__init__(message)
        self.code = code
        self.broker_called = broker_called


def _resolve_gateway_broker_called(payload: Dict[str, Any], code: str) -> Optional[bool]:
    """从 Gateway 结构化错误解析是否已经调用 QMT 写接口。

    Args:
        payload: Gateway 返回的错误对象。
        code: 已解析的稳定错误码。

    Returns:
        Optional[bool]: 显式布尔值优先；仅稳定的调用前错误码推导 False，
        其余错误保持未知。
    """

    if isinstance(payload.get("broker_called"), bool):
        return bool(payload["broker_called"])
    if str(code or "") in _BIG_QMT_PRE_BROKER_ERROR_CODES:
        return False
    return None


def load_big_qmt_gateway_config(server_config: ServerConfig) -> BigQmtGatewayConfig:
    timeout = get_env_float(
        "BIG_QMT_GATEWAY_TIMEOUT_SECONDS",
        get_env_float("BIG_QMT_GATEWAY_TIMEOUT", 10.0),
    )
    cfg = BigQmtGatewayConfig(
        base_url=get_env("BIG_QMT_GATEWAY_URL", get_env("BIG_QMT_URL", "http://127.0.0.1:9000"))
        or "http://127.0.0.1:9000",
        password=get_env("BIG_QMT_GATEWAY_PASSWORD", get_env("BIG_QMT_GATEWAY_TOKEN")),
        secret=get_env("BIG_QMT_GATEWAY_SECRET"),
        timeout_seconds=max(0.1, float(timeout or 10.0)),
        health_ttl_seconds=max(
            1.0,
            float(get_env_float("BIG_QMT_GATEWAY_HEALTH_TTL_SECONDS", 15.0) or 15.0),
        ),
    )
    cfg.action_status = _build_action_status(server_config)
    return cfg


class BigQmtGatewayClient:
    def __init__(self, config: BigQmtGatewayConfig) -> None:
        self.config = config
        self._base_url = config.base_url.rstrip("/")
        self._last_health: Optional[Dict[str, Any]] = None
        self._last_health_at: Optional[float] = None
        self._last_error: Optional[str] = None
        self._last_success_at: Optional[float] = None
        self._last_failure_at: Optional[float] = None

    async def get(self, path: str) -> Any:
        return await self._run_blocking(self.request_json, path, None, "GET")

    async def post(self, path: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        return await self._run_blocking(self.request_json, path, payload or {}, "POST")

    async def post_first(
        self, paths: Iterable[str], payload: Optional[Dict[str, Any]] = None
    ) -> Any:
        last_error: Optional[BigQmtGatewayError] = None
        for path in paths:
            try:
                return await self.post(path, payload)
            except BigQmtGatewayError as exc:
                last_error = exc
                if exc.code not in {"HTTP_404", "NOT_FOUND", "NOT_IMPLEMENTED"}:
                    raise
        if last_error is not None:
            raise last_error
        raise BigQmtGatewayError("未配置 big QMT gateway path", code="NOT_IMPLEMENTED")

    async def health(self) -> Dict[str, Any]:
        value = await self.get("/health")
        if isinstance(value, dict):
            self._last_health = value
            self._last_health_at = time.time()
            return value
        return {"raw": value}

    def request_json(
        self,
        path: str,
        payload: Optional[Dict[str, Any]],
        method: str,
    ) -> Any:
        url = self._url(path)
        body = None
        headers = {
            "Accept": "application/json",
            "X-BulletTrade-Request-Id": f"bt-{uuid4().hex}",
        }
        if method != "GET":
            body = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self.config.password:
            headers["Authorization"] = f"Bearer {self.config.password}"
            headers["X-BulletTrade-Password"] = self.config.password
        if self.config.secret:
            headers["X-BulletTrade-Secret"] = self.config.secret

        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.config.timeout_seconds) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            self._record_failure(str(exc))
            raise self._http_error(exc) from exc
        except urllib.error.URLError as exc:
            self._record_failure(str(exc.reason))
            raise BigQmtGatewayError(
                f"big QMT gateway 不可用: {exc.reason}",
                code="BIG_QMT_GATEWAY_UNAVAILABLE",
            ) from exc
        except TimeoutError as exc:
            self._record_failure("timeout")
            raise BigQmtGatewayError(
                f"big QMT gateway 请求超时（>{self.config.timeout_seconds}s）",
                code="BIG_QMT_GATEWAY_TIMEOUT",
            ) from exc

        try:
            decoded = json.loads(raw.decode("utf-8")) if raw else {}
        except json.JSONDecodeError as exc:
            self._record_failure(f"invalid json: {exc}")
            raise BigQmtGatewayError(f"big QMT gateway 返回非 JSON 响应: {exc}") from exc

        try:
            result = self._unwrap_response(decoded)
        except BigQmtGatewayError as exc:
            self._record_failure(str(exc))
            raise
        self._record_success()
        return result

    def qmt_status(self) -> Dict[str, Any]:
        health = self._last_health or {}
        health_cache_age = (
            max(0.0, time.time() - self._last_health_at)
            if self._last_health_at is not None
            else None
        )
        health_cache_expired = (
            health_cache_age is None or health_cache_age > self.config.health_ttl_seconds
        )
        health_ready = health.get("ready")
        if health_ready is None:
            health_ready = health.get("process_alive")
        if self._last_error:
            ready = False
            state = "unavailable"
        elif health_cache_expired:
            ready = None
            state = "unknown"
        elif health_ready is None:
            ready = None
            state = "unknown"
        else:
            ready = bool(health_ready)
            state = "ready" if ready else "degraded"
        return {
            "backend_type": "big_qmt",
            "ready": ready,
            "state": state,
            "gateway_url": self.config.base_url,
            "trading_enabled": _health_bool(health, "trading_enabled"),
            "cancel_order_enabled": _health_bool(health, "cancel_order_enabled"),
            "last_error": self._last_error,
            "last_success_at": self._last_success_at,
            "last_failure_at": self._last_failure_at,
            "health_cache_age_seconds": health_cache_age,
            "health_cache_ttl_seconds": self.config.health_ttl_seconds,
            "health_cache_expired": health_cache_expired,
            "actions": self.config.action_status,
            "big_qmt_gateway": health,
        }

    async def _run_blocking(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: func(*args))

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return self._base_url + path

    def _http_error(self, exc: urllib.error.HTTPError) -> BigQmtGatewayError:
        message = str(exc)
        code = f"HTTP_{exc.code}"
        broker_called: Optional[bool] = None
        try:
            raw = exc.read()
            payload = json.loads(raw.decode("utf-8")) if raw else {}
            if isinstance(payload, dict):
                message = str(payload.get("message") or payload.get("error") or message)
                code = str(payload.get("code") or code)
                broker_called = _resolve_gateway_broker_called(payload, code)
        except Exception:
            pass
        return BigQmtGatewayError(message, code=code, broker_called=broker_called)

    def _unwrap_response(self, payload: Any) -> Any:
        if not isinstance(payload, dict):
            return payload
        ok = payload.get("ok")
        if ok is False:
            message = str(payload.get("message") or payload.get("error") or "big QMT gateway 请求失败")
            code = str(payload.get("code") or payload.get("error_code") or "BIG_QMT_GATEWAY_ERROR")
            broker_called = _resolve_gateway_broker_called(payload, code)
            raise BigQmtGatewayError(
                message,
                code=code,
                broker_called=broker_called,
            )
        if "data" in payload:
            return payload["data"]
        if "result" in payload:
            return payload["result"]
        if ok is True and "value" in payload and "dtype" not in payload:
            return payload["value"]
        return payload

    def _record_success(self) -> None:
        self._last_success_at = time.time()
        self._last_error = None

    def _record_failure(self, message: str) -> None:
        self._last_failure_at = time.time()
        self._last_error = message


class BigQmtDataAdapter(RemoteDataAdapter):
    def __init__(self, client: BigQmtGatewayClient) -> None:
        self.client = client

    def qmt_status(self) -> Dict[str, Any]:
        return self.client.qmt_status()

    async def refresh_backend_status(self) -> None:
        """刷新大 QMT helper 当前健康状态并保留受控失败信息。

        Args:
            None。

        Returns:
            None。

        Side Effects:
            向 helper 的 ``/health`` 发起一次只读请求，更新 sidecar 内存中的健康快照。
        """

        try:
            await self.client.health()
        except BigQmtGatewayError:
            pass

    async def get_history(self, payload: Dict) -> Dict:
        data = await self.client.post("/data/history", payload)
        return _as_dataframe_payload(data)

    async def get_snapshot(self, payload: Dict) -> Dict:
        security = payload.get("security")
        data = await self.client.post_first(("/data/snapshot", "/data/current_tick"), payload)
        return _normalize_snapshot_tick(_select_tick(data, security), security)

    async def get_live_current(self, payload: Dict) -> Dict:
        security = payload.get("security")
        data = await self.client.post_first(
            ("/data/live_current", "/data/current_tick", "/data/snapshot"), payload
        )
        return _normalize_live_current_tick(_select_tick(data, security), security)

    async def get_trade_days(self, payload: Dict) -> Dict:
        data = await self.client.post("/data/trade_days", payload)
        if isinstance(data, dict) and data.get("dtype") == "list":
            values = _extract_list(data, "values")
            return {
                "dtype": "list",
                "values": [_normalize_trade_day_value(item) for item in values],
            }
        values = _extract_list(data, "values")
        return {"dtype": "list", "values": [_normalize_trade_day_value(item) for item in values]}

    async def get_security_info(self, payload: Dict) -> Dict:
        data = await self.client.post("/data/security_info", payload)
        return data if _is_dict_payload(data) else dict_payload(_extract_dict(data))

    async def ensure_cache(self, payload: Dict) -> Dict:
        data = await self.client.post("/data/ensure_cache", payload)
        return data if _is_dict_payload(data) else {"dtype": "dict", "value": _extract_dict(data)}

    async def get_current_tick(self, symbol: str) -> Optional[Dict]:
        data = await self.client.post_first(
            ("/data/current_tick", "/data/snapshot"),
            {"security": symbol},
        )
        tick = _normalize_snapshot_tick(_select_tick(data, symbol), symbol)
        return tick or None

    async def get_all_securities(self, payload: Dict) -> Dict:
        data = await self.client.post("/data/all_securities", payload)
        return _as_dataframe_payload(data)

    async def get_index_stocks(self, payload: Dict) -> Dict:
        data = await self.client.post("/data/index_stocks", payload)
        if isinstance(data, dict) and "values" in data:
            return {"values": list(data.get("values") or [])}
        return {"values": _extract_list(data, "stocks")}

    async def get_split_dividend(self, payload: Dict) -> Dict:
        data = await self.client.post("/data/split_dividend", payload)
        if isinstance(data, dict) and "events" in data:
            return {"events": list(data.get("events") or [])}
        return {"events": _extract_list(data, "events")}


class BigQmtBrokerAdapter(RemoteBrokerAdapter):
    """通过大 QMT HTTP Gateway 提供账户查询、下单、撤单和订单对账。"""

    tracks_broker_call_boundary = True

    def __init__(
        self,
        config: ServerConfig,
        account_router: AccountRouter,
        client: BigQmtGatewayClient,
    ) -> None:
        self.config = config
        self.account_router = account_router
        self.client = client
        self._order_tag_overrides: Dict[str, Dict[str, Any]] = {}

    async def start(self) -> None:
        await self.refresh_backend_status()

    async def stop(self) -> None:
        return None

    def qmt_status(self) -> Dict[str, Any]:
        return self.client.qmt_status()

    async def refresh_backend_status(self) -> None:
        """刷新大 QMT helper 当前健康状态并保留受控失败信息。

        Args:
            None。

        Returns:
            None。

        Side Effects:
            向 helper 的 ``/health`` 发起一次只读请求，更新 sidecar 内存中的健康快照。
        """

        try:
            await self.client.health()
        except BigQmtGatewayError:
            pass

    async def get_account_info(self, account: AccountContext) -> Dict:
        data = await self.client.post("/account", self._account_payload(account))
        return data if _is_dict_payload(data) else dict_payload(_extract_dict(data))

    async def get_positions(self, account: AccountContext) -> List[Dict]:
        data = await self.client.post("/positions", self._account_payload(account))
        return [
            _normalize_position(item)
            for item in _extract_list(data, "positions")
            if isinstance(item, dict)
        ]

    async def list_orders(
        self, account: AccountContext, filters: Optional[Dict] = None
    ) -> List[Dict]:
        payload = _gateway_order_filters(filters or {})
        payload.update(self._account_payload(account))
        data = await self.client.post("/orders", payload)
        orders = [
            self._apply_local_order_tag(_normalize_order(item))
            for item in _extract_list(data, "orders")
            if isinstance(item, dict)
        ]
        return _filter_orders(orders, filters or {})

    async def list_trades(
        self, account: AccountContext, filters: Optional[Dict] = None
    ) -> List[Dict]:
        payload = _gateway_order_filters(filters or {})
        payload.update(self._account_payload(account))
        data = await self.client.post("/trades", payload)
        trades = [
            self._apply_local_order_tag(_normalize_trade(item))
            for item in _extract_list(data, "trades")
            if isinstance(item, dict)
        ]
        return _filter_trades(trades, filters or {})

    async def get_order_status(self, account: AccountContext, order_id: str) -> Dict:
        payload = self._account_payload(account)
        payload["order_id"] = order_id
        data = await self.client.post("/order_status", payload)
        return self._apply_local_order_tag(_normalize_order(_extract_dict(data)))

    async def place_order(self, account: AccountContext, payload: Dict) -> Dict:
        request = dict(payload or {})
        request.update(self._account_payload(account))
        _ensure_virtual_account_remark(request)
        _ensure_gateway_submission_identity(request)
        _apply_big_qmt_market_price_type(request)
        submitted_at = time.time()
        wait_timeout = _positive_float((payload or {}).get("wait_timeout"))
        known_order_ids = set()
        if wait_timeout > 0:
            known_order_ids = await self._snapshot_order_ids(account, request)
        mark_broker_call_started(request)
        data = await self.client.post("/place_order", request)
        order = _normalize_order(_extract_dict(data))
        if wait_timeout <= 0:
            return _submission_unknown_order(
                order,
                "big QMT 下单未配置确认等待窗口，不能仅凭 passorder 返回确认订单",
            )
        return await self._confirm_place_order_submission(
            account, request, order, wait_timeout, known_order_ids, submitted_at
        )

    async def _confirm_place_order_submission(
        self,
        account: AccountContext,
        request: Dict[str, Any],
        order: Dict[str, Any],
        wait_timeout: float,
        known_order_ids: Optional[set] = None,
        submitted_at: Optional[float] = None,
    ) -> Dict[str, Any]:
        deadline = time.monotonic() + wait_timeout
        last_snapshot: Optional[Dict[str, Any]] = None
        while True:
            matched = await self._find_submitted_order(
                account, request, order, known_order_ids or set(), submitted_at
            )
            if matched:
                last_snapshot = dict(matched)
                if not _order_has_order_id(matched) and time.monotonic() < deadline:
                    await asyncio.sleep(
                        min(
                            _ORDER_CONFIRM_POLL_INTERVAL_SECONDS,
                            max(0.0, deadline - time.monotonic()),
                        )
                    )
                    continue
                confirmed = dict(matched)
                self._remember_local_order_tag(confirmed.get("order_id"), request)
                confirmed = self._apply_local_order_tag(confirmed)
                for key in (
                    "order_ref",
                    "passorder_return",
                    "passorder_return_type",
                    "passorder_return_is_none",
                    "order_tag_recorded",
                    "order_tag_store",
                    "strategy_name",
                    "order_remark",
                    "remark",
                    "sub_account_id",
                    "virtual_account_id",
                ):
                    if key in order and not confirmed.get(key):
                        confirmed[key] = order.get(key)
                confirmed["last_snapshot"] = last_snapshot
                confirmed["timed_out"] = False
                confirmed["async_tracking"] = False
                confirmed["wait_timeout"] = wait_timeout
                return confirmed
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(
                min(_ORDER_CONFIRM_POLL_INTERVAL_SECONDS, max(0.0, deadline - time.monotonic()))
            )
        result = dict(order)
        result["status"] = "submit_unknown"
        result["submit_unknown"] = True
        result["timed_out"] = True
        result["async_tracking"] = True
        result["wait_timeout"] = wait_timeout
        result["last_snapshot"] = last_snapshot
        result["warning"] = (
            "big QMT passorder returned but no matching order was visible within %.3fs: "
            "security=%s amount=%s price=%s"
            % (
                wait_timeout,
                request.get("security") or request.get("stock") or request.get("stockcode"),
                request.get("amount") or request.get("volume"),
                _request_order_price(request),
            )
        )
        return result

    async def _find_submitted_order(
        self,
        account: AccountContext,
        request: Dict[str, Any],
        order: Dict[str, Any],
        known_order_ids: Optional[set] = None,
        submitted_at: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        known = {str(item) for item in (known_order_ids or set()) if str(item)}
        qmt_user_order_id = str(
            request.get("qmt_user_order_id") or order.get("qmt_user_order_id") or ""
        ).strip()
        order_id = str(order.get("order_id") or order.get("order_ref") or "").strip()
        if order_id and order_id != "0":
            filters = {"order_id": order_id}
            sub_account_id = _virtual_account_id(request)
            if sub_account_id:
                filters["sub_account_id"] = sub_account_id
            orders = await self.list_orders(account, filters)
            for item in orders:
                if _matches_confirmed_big_qmt_submission(
                    item, request, qmt_user_order_id, known, submitted_at
                ):
                    return item
        filters = {
            "security": request.get("security") or request.get("stock") or request.get("stockcode")
        }
        sub_account_id = _virtual_account_id(request)
        if sub_account_id:
            filters["sub_account_id"] = sub_account_id
        orders = await self.list_orders(account, filters)
        for item in orders:
            item_id = str(item.get("order_id") or "").strip()
            if item_id and item_id in known:
                continue
            if _matches_confirmed_big_qmt_submission(
                item, request, qmt_user_order_id, known, submitted_at
            ):
                return item
        return None

    async def _snapshot_order_ids(self, account: AccountContext, request: Dict[str, Any]) -> set:
        security = request.get("security") or request.get("stock") or request.get("stockcode")
        payload = self._account_payload(account)
        if security:
            payload["security"] = security
        try:
            data = await self.client.post("/orders", payload)
        except BigQmtGatewayError:
            return set()
        ids = set()
        for item in _extract_list(data, "orders"):
            if not isinstance(item, dict):
                continue
            normalized = _normalize_order(item)
            order_id = str(normalized.get("order_id") or "").strip()
            if order_id:
                ids.add(order_id)
        return ids

    def _remember_local_order_tag(self, order_id: Any, request: Dict[str, Any]) -> None:
        order_id_text = str(order_id or "").strip()
        if not order_id_text:
            return
        tag: Dict[str, Any] = {}
        sub_account_id = _virtual_account_id(request)
        if sub_account_id:
            tag["sub_account_id"] = sub_account_id
            tag["virtual_account_id"] = sub_account_id
        remark = str(request.get("order_remark") or request.get("remark") or "").strip()
        if remark:
            tag["order_remark"] = remark
            tag["remark"] = remark
        strategy_name = str(
            request.get("strategy_name") or request.get("strategyName") or ""
        ).strip()
        if strategy_name:
            tag["strategy_name"] = strategy_name
        if tag:
            self._order_tag_overrides[order_id_text] = tag

    def _apply_local_order_tag(self, item: Dict[str, Any]) -> Dict[str, Any]:
        order_id = str(item.get("order_id") or "").strip()
        tag = self._order_tag_overrides.get(order_id)
        if not tag:
            return item
        result = dict(item)
        for key, value in tag.items():
            if value not in (None, ""):
                result[key] = value
        return result

    async def cancel_order(self, account: AccountContext, order_id: str) -> Dict:
        payload = self._account_payload(account)
        payload["order_id"] = order_id
        data = await self.client.post("/cancel_order", payload)
        if isinstance(data, dict) and data.get("dtype") == "dict":
            return data
        value = _extract_dict(data)
        if "value" not in value and "success" in value:
            value["value"] = bool(value.get("success"))
        return dict_payload(value)

    async def cancel_order_request(
        self, account: AccountContext, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """撤销一个精确订单，并短暂轮询该订单的柜台终态。

        Args:
            account: Server 已解析的真实账户上下文。
            payload: 含精确 ``order_id`` 和幂等键的撤单请求。

        Returns:
            Dict[str, Any]: 已撤/部撤时返回成功，已成/已拒时返回明确失败；
            等待窗口内没有终态时返回 ``submit_unknown``，绝不重复发送撤单。

        Side Effects:
            仅调用一次 BigQMT helper ``/cancel_order``，随后只读查询同一订单号。
        """

        request = dict(payload or {})
        order_id = str(request.get("order_id") or "").strip()
        if not order_id:
            raise ValueError("缺少 order_id")
        request.update(self._account_payload(account))
        mark_broker_call_started(request)
        data = await self.client.post("/cancel_order", request)
        result = _extract_dict(data)
        result.setdefault("order_id", order_id)
        if result.get("success") is False or result.get("value") is False:
            result.setdefault("status", "rejected")
            result.setdefault("submission_state", "rejected")
            result.setdefault("cancel_outcome", "rejected")
            result["value"] = False
            return result

        deadline = time.monotonic() + _CANCEL_CONFIRM_TIMEOUT_SECONDS
        last_snapshot: Optional[Dict[str, Any]] = None
        while True:
            try:
                snapshot = await self.get_order_status(account, order_id)
            except BigQmtGatewayError:
                snapshot = {}
            actual_order_id = str(snapshot.get("order_id") or "").strip()
            if actual_order_id == order_id:
                last_snapshot = snapshot
                status = str(snapshot.get("status") or "").strip().lower()
                if status in {
                    "canceled",
                    "cancelled",
                    "partly_canceled",
                    "partly_cancelled",
                }:
                    result.update(
                        {
                            "order_id": order_id,
                            "status": status,
                            "submission_state": status,
                            "value": True,
                            "success": True,
                            "cancel_outcome": "cancelled",
                            "last_snapshot": snapshot,
                        }
                    )
                    return result
                if status in {"filled", "rejected", "failed", "error"}:
                    result.update(
                        {
                            "order_id": order_id,
                            "status": status,
                            "submission_state": status,
                            "value": False,
                            "success": False,
                            "cancel_outcome": "not_canceled_already_terminal",
                            "last_snapshot": snapshot,
                        }
                    )
                    return result
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(
                min(_ORDER_CONFIRM_POLL_INTERVAL_SECONDS, deadline - time.monotonic())
            )

        result.update(
            {
                "order_id": order_id,
                "status": "submit_unknown",
                "submission_state": "submit_unknown",
                "submit_unknown": True,
                "value": False,
                "success": False,
                "cancel_outcome": "unknown",
                "last_snapshot": last_snapshot,
            }
        )
        return result

    def _account_payload(self, account: AccountContext) -> Dict[str, Any]:
        return {
            "account_key": account.config.key or "default",
            "account_id": account.config.account_id,
            "account_type": account.config.account_type,
        }


def build_big_qmt_bundle(config: ServerConfig, router: AccountRouter) -> AdapterBundle:
    gateway_config = load_big_qmt_gateway_config(config)
    client = BigQmtGatewayClient(gateway_config)
    data_adapter = BigQmtDataAdapter(client) if config.enable_data else None
    broker_adapter = BigQmtBrokerAdapter(config, router, client) if config.enable_broker else None
    return AdapterBundle(data_adapter=data_adapter, broker_adapter=broker_adapter)


def _build_action_status(
    server_config: ServerConfig,
) -> Dict[str, Dict[str, Any]]:
    status: Dict[str, Dict[str, Any]] = {}
    for action in _DATA_ACTIONS:
        if not server_config.enable_data:
            status[action] = _status("unavailable", "data module disabled")
        elif action in _POLLING_SUBSCRIPTION_ACTIONS:
            status[action] = _status(
                "degraded",
                "uses server polling over get_current_tick; native big QMT tick callback is not MVP",
            )
        else:
            status[action] = _status("ready", "")
    for action in _BROKER_READ_ACTIONS:
        status[action] = _status(
            "ready" if server_config.enable_broker else "unavailable", "broker module disabled"
        )
    place_state = "ready" if server_config.enable_broker else "unavailable"
    place_reason = "" if place_state == "ready" else "broker module disabled"
    status["broker.place_order"] = _status(place_state, place_reason)
    cancel_state = "ready" if server_config.enable_broker else "unavailable"
    cancel_reason = "" if cancel_state == "ready" else "broker module disabled"
    status["broker.cancel_order"] = _status(cancel_state, cancel_reason)
    status["admin.health"] = _status("ready", "")
    status["admin.print_account"] = _status(
        "ready" if server_config.enable_broker else "unavailable",
        "broker module disabled",
    )
    return status


def _status(state: str, reason: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {"status": state}
    if state != "ready" and reason:
        result["reason"] = reason
    return result


def _health_bool(payload: Dict[str, Any], key: str) -> Optional[bool]:
    if key not in payload:
        return None
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    return bool(value)


def _is_dict_payload(value: Any) -> bool:
    return isinstance(value, dict) and value.get("dtype") == "dict" and "value" in value


def _as_dataframe_payload(value: Any) -> Dict:
    if isinstance(value, dict) and value.get("dtype") == "dataframe":
        return value
    if value is None or hasattr(value, "columns"):
        return dataframe_to_payload(value)
    if isinstance(value, dict) and "columns" in value and "records" in value:
        result = dict(value)
        result.setdefault("dtype", "dataframe")
        return result
    rows = _extract_list(value, "records")
    if not rows:
        return {"dtype": "dataframe", "columns": [], "records": []}
    if isinstance(rows[0], dict):
        columns = list(rows[0].keys())
        return {
            "dtype": "dataframe",
            "columns": columns,
            "records": [[row.get(col) for col in columns] for row in rows],
        }
    return {"dtype": "dataframe", "columns": [], "records": rows}


def _extract_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        if value.get("dtype") == "dict" and isinstance(value.get("value"), dict):
            return dict(value.get("value") or {})
        for key in ("value", "account", "order", "status", "data"):
            item = value.get(key)
            if isinstance(item, dict):
                return dict(item)
        return dict(value)
    return {"raw": value}


def _extract_list(value: Any, preferred_key: str) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, dict):
        for key in (preferred_key, "values", "items", "records", "data"):
            item = value.get(key)
            if isinstance(item, list):
                return list(item)
    return []


def _normalize_trade_day_value(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d 00:00:00")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d 00:00:00")

    text = str(value).strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        yyyy = digits[:4]
        mm = digits[4:6]
        dd = digits[6:8]
        try:
            datetime(int(yyyy), int(mm), int(dd))
            return f"{yyyy}-{mm}-{dd} 00:00:00"
        except ValueError:
            pass
    return text


def _select_tick(value: Any, security: Optional[str]) -> Dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        if "ticks" in value and isinstance(value["ticks"], dict):
            ticks = value["ticks"]
            if security and isinstance(ticks.get(security), dict):
                return dict(ticks[security])
            first = next((item for item in ticks.values() if isinstance(item, dict)), None)
            return dict(first or {})
        if security and isinstance(value.get(security), dict):
            return dict(value[security])
        if "value" in value and isinstance(value["value"], dict):
            return _select_tick(value["value"], security)
        return dict(value)
    return {}


def _first_present(mapping: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _as_float_or_none(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any, default: float = 0.0) -> float:
    parsed = _as_float_or_none(value)
    return default if parsed is None else parsed


def _normalize_snapshot_tick(tick: Dict[str, Any], security: Optional[str]) -> Dict:
    if not tick:
        return {}
    last_price = _as_float_or_none(_first_present(tick, "last_price", "lastPrice", "price", "last"))
    if last_price is None:
        return dict(tick)
    sid = (
        security
        or tick.get("sid")
        or tick.get("security")
        or tick.get("code")
        or tick.get("stock_code")
        or ""
    )
    dt = _first_present(tick, "dt", "timetag", "datetime", "time")
    value = {"sid": sid, "last_price": last_price, "dt": dt}
    _preserve_live_observation_fields(value, tick)
    return value


def _normalize_live_current_tick(tick: Dict[str, Any], security: Optional[str] = None) -> Dict:
    if not tick:
        return {}
    last_price = _as_float_or_none(_first_present(tick, "last_price", "lastPrice", "price", "last"))
    if last_price is None:
        return dict(tick)
    paused = tick.get("paused")
    if paused is None:
        open_int = _first_present(tick, "openInt", "stockStatus")
        try:
            paused = int(open_int) in (1, 17, 20)
        except (TypeError, ValueError):
            paused = False
    value = dict(tick)
    value.update(
        {
            "security": security or tick.get("security") or tick.get("sid") or tick.get("code"),
            "last_price": last_price,
            "high_limit": _as_float(
                _first_present(tick, "high_limit", "highLimit", "UpStopPrice", "up_stop_price")
            ),
            "low_limit": _as_float(
                _first_present(tick, "low_limit", "lowLimit", "DownStopPrice", "down_stop_price")
            ),
            "paused": bool(paused),
        }
    )
    _preserve_live_observation_fields(value, tick)
    return value


def _preserve_live_observation_fields(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    """把 helper 的来源、双时间、健康和一档盘口证据无损写入规范快照。

    Args:
        target: adapter 正在构造的规范快照。
        source: helper 返回的原始证券快照。

    Returns:
        None: 直接修改 target，不伪造缺失的源时间。
    """

    for name in (
        "source",
        "source_time",
        "received_time",
        "query_completed_time",
        "age_seconds",
        "event_stale",
        "feed_health",
        "bid_volume1",
        "ask_volume1",
    ):
        if name in source:
            target[name] = source[name]
    for scalar, array in (("bid_price1", "bidPrice"), ("ask_price1", "askPrice")):
        raw = source.get(scalar)
        levels = source.get(array)
        if raw in (None, "") and isinstance(levels, (list, tuple)) and levels:
            raw = levels[0]
        value = _as_float_or_none(raw)
        if value is not None and value > 0:
            target[scalar] = value


def _virtual_account_id(payload: Dict[str, Any]) -> str:
    value = (
        payload.get("sub_account_id")
        or payload.get("subAccountId")
        or payload.get("virtual_account_id")
        or payload.get("virtualAccountId")
        or payload.get("virtual_account")
        or ""
    )
    text = str(value or "").strip()
    if "@" in text:
        text = text.split("@", 1)[0].strip()
    return text


def _extract_virtual_account_from_remark(remark: Any) -> str:
    text = str(remark or "").strip()
    if not text:
        return ""
    tokens = [text]
    for separator in ("|", ";", ",", " "):
        expanded = []
        for token in tokens:
            expanded.extend(token.split(separator))
        tokens = expanded
    for token in tokens:
        item = token.strip()
        for prefix in ("sub:", "sub_account_id=", "virtual_account_id=", "sub=", "virtual="):
            if item.startswith(prefix):
                return item[len(prefix) :].strip()
    return ""


def _remark_matches_virtual_account(remark: Any, sub_account_id: str) -> bool:
    sub_account_id = str(sub_account_id or "").strip()
    if not sub_account_id:
        return True
    text = str(remark or "").strip()
    if not text:
        return False
    if text == sub_account_id:
        return True
    extracted = _extract_virtual_account_from_remark(text)
    if extracted and extracted == sub_account_id:
        return True
    return any(
        token in text
        for token in (
            f"sub:{sub_account_id}",
            f"sub_account_id={sub_account_id}",
            f"virtual_account_id={sub_account_id}",
            f"sub={sub_account_id}",
            f"virtual={sub_account_id}",
        )
    )


def _ensure_virtual_account_remark(payload: Dict[str, Any]) -> None:
    sub_account_id = _virtual_account_id(payload)
    if not sub_account_id:
        return
    remark = str(payload.get("order_remark") or payload.get("remark") or "").strip()
    if _remark_matches_virtual_account(remark, sub_account_id):
        payload["order_remark"] = remark
        return
    encoded = f"sub:{sub_account_id}"
    if remark:
        encoded = f"{encoded}|{remark}"
    payload["order_remark"] = encoded
    payload.setdefault("remark", encoded)


def _ensure_gateway_submission_identity(payload: Dict[str, Any]) -> None:
    """为 Big QMT 下单生成可回查且不泄漏原幂等键的强关联标识。

    Args:
        payload: 即将发往 gateway 的下单请求，会原地补充 `qmt_user_order_id`。

    Returns:
        None。

    Side Effects:
        使用原幂等键的 SHA-256 截断摘要生成稳定标识，禁止依赖证券/价格/数量等经济字段。
    """

    if str(payload.get("qmt_user_order_id") or "").strip():
        return
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    if not idempotency_key:
        # 直接调用 adapter 的离线/兼容入口不经过 ServerApplication；仍生成一次性强键，
        # 但生产远程写必须由服务端提供可持久化的原 idempotency_key。
        idempotency_key = f"big-qmt-direct-{uuid4().hex}"
        payload["idempotency_key"] = idempotency_key
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:28]
    payload["qmt_user_order_id"] = f"BT-{digest}"


def _apply_big_qmt_market_price_type(payload: Dict[str, Any]) -> None:
    """把公共沪深市价类型翻译为 BigQMT 原生 ``pr_type``。

    Args:
        payload: 即将发往 helper 的订单载荷；可含公共 ``style`` 和
            ``market_type``，也可已经显式携带数字 ``pr_type``。

    Returns:
        None。

    Raises:
        ValueError: 市价类型不适用于当前沪深市场时抛出，避免静默退化成限价 11。

    Side Effects:
        仅在市价意图且尚无数字原生类型时原地写入 ``pr_type``。
    """

    style = payload.get("style")
    style = style if isinstance(style, dict) else {}
    if payload.get("pr_type") not in (None, "") or payload.get("prType") not in (None, ""):
        return
    if style.get("pr_type") not in (None, "") or style.get("prType") not in (None, ""):
        return
    style_type = str(style.get("type") or "").strip().lower()
    market_type = str(style.get("market_type") or payload.get("market_type") or "").strip().lower()
    if style_type not in {"market", *set(_all_big_qmt_market_types())} and not payload.get(
        "market"
    ):
        return
    if style_type in _all_big_qmt_market_types() and not market_type:
        market_type = style_type
    if not market_type:
        market_type = "opponent_best"

    security = str(
        payload.get("security") or payload.get("stock") or payload.get("stockcode") or ""
    ).strip()
    exchange = _big_qmt_exchange_suffix(security)
    pr_type = _BIG_QMT_MARKET_PRICE_TYPES.get(exchange, {}).get(market_type)
    if pr_type is None:
        raise ValueError(
            "BigQMT 市价类型不适用于当前交易所: " f"security={security or '-'} market_type={market_type or '-'}"
        )
    payload["market_type"] = market_type
    payload["pr_type"] = pr_type


def _all_big_qmt_market_types() -> frozenset:
    """返回 BigQMT 沪深映射支持的公共市价类型集合。

    Returns:
        frozenset: 沪深映射表中所有 canonical 市价类型。
    """

    return frozenset(
        market_type
        for exchange_types in _BIG_QMT_MARKET_PRICE_TYPES.values()
        for market_type in exchange_types
    )


def _big_qmt_exchange_suffix(security: str) -> str:
    """把常用沪深证券后缀规范为 BigQMT 映射键。

    Args:
        security: 带 ``XSHG/XSHE``、``SH/SZ`` 或 ``SSE/SZSE`` 后缀的证券代码。

    Returns:
        str: ``XSHG``、``XSHE`` 或无法识别时的空字符串。
    """

    suffix = str(security or "").rsplit(".", 1)[-1].upper()
    if suffix in {"XSHG", "SH", "SSE"}:
        return "XSHG"
    if suffix in {"XSHE", "SZ", "SZSE"}:
        return "XSHE"
    return ""


def _positive_float(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    if parsed <= 0:
        return 0.0
    return parsed


def _request_order_price(payload: Dict[str, Any]) -> Optional[Any]:
    price = payload.get("price")
    style = payload.get("style")
    if price in (None, "") and isinstance(style, dict):
        price = style.get("price")
    if price in (None, "") and isinstance(style, dict):
        price = style.get("protect_price")
    return price


def _float_close(left: Any, right: Any) -> bool:
    try:
        left_value = float(left)
        right_value = float(right)
    except (TypeError, ValueError):
        return False
    return abs(left_value - right_value) <= max(0.01, abs(right_value) * 0.000001)


def _order_has_order_id(order: Dict[str, Any]) -> bool:
    return bool(str(order.get("order_id") or order.get("order_ref") or "").strip())


def _order_matches_qmt_user_order_id(order: Dict[str, Any], qmt_user_order_id: str) -> bool:
    if not qmt_user_order_id:
        return False
    candidates = [
        order.get("qmt_user_order_id"),
        order.get("order_remark"),
        order.get("remark"),
        order.get("m_strRemark"),
        order.get("m_strUserOrderId"),
    ]
    raw = order.get("raw")
    if isinstance(raw, dict):
        candidates.extend(
            [
                raw.get("qmt_user_order_id"),
                raw.get("m_strRemark"),
                raw.get("m_strUserOrderId"),
            ]
        )
    return any(str(item or "").strip() == qmt_user_order_id for item in candidates)


def _matches_confirmed_big_qmt_submission(
    order: Dict[str, Any],
    request: Dict[str, Any],
    qmt_user_order_id: str,
    known_order_ids: set,
    submitted_at: Optional[float],
) -> bool:
    """确认 Big QMT 订单具备强身份、方向和安全时间证据。

    Args:
        order: 当前轮询得到的归一化订单。
        request: 原始下单请求。
        qmt_user_order_id: 下单前生成并透传的强关联标识。
        known_order_ids: 下单前已经存在的订单号集合。
        submitted_at: 请求发送前的 wall-clock 时间戳。

    Returns:
        bool: 订单号新出现、强标识、证券/方向/数量和时间证据全部一致时返回 True。
    """

    order_id = str(order.get("order_id") or "").strip()
    if not order_id or order_id in known_order_ids:
        return False
    if not _has_strong_submission_identity(order, request, qmt_user_order_id):
        return False
    if not _order_matches_place_request(order, request):
        return False
    return _order_has_safe_submission_time(order, submitted_at)


def _has_strong_submission_identity(
    order: Dict[str, Any], request: Dict[str, Any], qmt_user_order_id: str
) -> bool:
    """确认回报回显了强客户键，或完全相同的显式订单备注。

    Args:
        order: 当前轮询得到的归一化订单。
        request: 原始下单请求。
        qmt_user_order_id: 服务端由幂等键导出的客户标识。

    Returns:
        bool: 客户键精确相同，或明确携带的订单备注完全相同时返回 True。
    """

    if qmt_user_order_id and _order_matches_qmt_user_order_id(order, qmt_user_order_id):
        return True
    request_remark = str(request.get("order_remark") or request.get("remark") or "").strip()
    order_remark = str(order.get("order_remark") or order.get("remark") or "").strip()
    return bool(request_remark and order_remark and request_remark == order_remark)


def _order_has_safe_submission_time(order: Dict[str, Any], submitted_at: Optional[float]) -> bool:
    """校验订单回报时间没有早于本次下单开始，拒绝旧委托误认。

    Args:
        order: 归一化订单事实。
        submitted_at: 请求发送前的 wall-clock 时间戳。

    Returns:
        bool: 找到可解析订单时间，且它没有早于请求前允许的 QMT 时钟偏差时返回 True。
    """

    if submitted_at is None:
        return False
    for key in ("order_time", "insert_time", "datetime", "time", "m_strInsertTime"):
        value = order.get(key)
        parsed = _parse_order_timestamp(value)
        if parsed is not None:
            return parsed >= submitted_at - _ORDER_CONFIRM_MAX_CLOCK_SKEW_SECONDS
    return False


def _parse_order_timestamp(value: Any) -> Optional[float]:
    """解析 gateway 订单时间为 Unix 秒，无法解析则返回 None。

    Args:
        value: epoch 秒/毫秒或 ISO 格式的订单时间。

    Returns:
        Optional[float]: Unix 秒时间戳；格式不可信时为 None。
    """

    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = 0.0
    if number > 100000000000:
        return number / 1000.0
    if number > 1000000000:
        return number
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def _order_matches_place_request(order: Dict[str, Any], request: Dict[str, Any]) -> bool:
    """核对订单的证券、方向、数量和限价经济字段。

    Args:
        order: 已通过强订单标识和新订单号检查的柜台订单。
        request: 原始 BigQMT 下单请求。

    Returns:
        bool: 核心经济字段一致时返回 True；市价保护价不与柜台生成价强行比较。
    """

    security = request.get("security") or request.get("stock") or request.get("stockcode")
    if security and order.get("security") != security:
        return False
    request_side = str(request.get("side") or "").strip().upper()
    order_side = str(order.get("side") or order.get("direction") or "").strip().upper()
    if not request_side or request_side != order_side:
        return False
    amount = request.get("amount") or request.get("volume")
    if amount not in (None, ""):
        try:
            if int(order.get("amount") or 0) != int(amount):
                return False
        except (TypeError, ValueError):
            return False
    if not _request_is_market_order(request):
        price = _request_order_price(request)
        order_price = order.get("order_price")
        if order_price in (None, ""):
            order_price = order.get("price")
        if price not in (None, "") and order_price not in (None, "", 0, 0.0):
            if not _float_close(order_price, price):
                return False
    return True


def _request_is_market_order(request: Dict[str, Any]) -> bool:
    """判断请求是否表达公共或原生市价意图。

    Args:
        request: 原始下单请求。

    Returns:
        bool: style、market 标记或原生非 11 价格类型表明市价意图时返回 True。
    """

    style = request.get("style")
    style = style if isinstance(style, dict) else {}
    style_type = str(style.get("type") or "").strip().lower()
    if style_type == "market" or style_type in _all_big_qmt_market_types():
        return True
    if bool(request.get("market")):
        return True
    pr_type = request.get("pr_type")
    if pr_type in (None, ""):
        pr_type = request.get("prType")
    try:
        return pr_type not in (None, "") and int(pr_type) != 11
    except (TypeError, ValueError):
        return False


def _submission_unknown_order(order: Dict[str, Any], warning: str) -> Dict[str, Any]:
    """将未获强确认的 Big QMT 回应转换为 fail-closed 未知态。

    Args:
        order: gateway 初始回包的归一化订单。
        warning: 未确认原因。

    Returns:
        Dict[str, Any]: 保留最小回包字段且标记 `submit_unknown` 的结果。
    """

    result = dict(order)
    result.update(
        {
            "status": "submit_unknown",
            "submission_state": "submit_unknown",
            "submit_unknown": True,
            "timed_out": True,
            "async_tracking": True,
            "warning": warning,
        }
    )
    return result


def _normalize_position(row: Dict[str, Any]) -> Dict[str, Any]:
    """规范化大 QMT 持仓字段并过滤非法行情与成本数值。

    Args:
        row: helper 返回的持仓字典，可包含嵌套 raw 原始字段。

    Returns:
        Dict[str, Any]: 证券身份、数量、平均成本、现价和市值口径统一的持仓。
    """

    item = dict(row)
    raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
    security = item.get("security") or _security_from_qmt_fields(item)
    if security:
        item["security"] = security
    if "closeable_amount" not in item:
        item["closeable_amount"] = item.get("available") or item.get("m_nCanUseVolume")
    if "amount" not in item:
        item["amount"] = item.get("volume") or item.get("m_nVolume")
    avg_cost = None
    zero_cost = None
    for key in ("m_dAvgOpenPrice", "avg_cost", "cost_basis", "open_price", "m_dOpenPrice"):
        for source in (item, raw):
            value = _as_float_or_none(source.get(key))
            if value is not None and math.isfinite(value) and value >= 0.0:
                if value > 0.0:
                    avg_cost = value
                    break
                zero_cost = value
        if avg_cost is not None:
            break
    if avg_cost is None:
        avg_cost = zero_cost
    item["avg_cost"] = float(avg_cost or 0.0)
    item["cost_basis"] = float(avg_cost or 0.0)
    current_price = None
    for key in ("current_price", "price", "last_price", "m_dLastPrice"):
        for source in (item, raw):
            value = _as_float_or_none(source.get(key))
            if value is not None and math.isfinite(value) and value >= 0.0:
                current_price = value
                break
        if current_price is not None:
            break
    if current_price is not None:
        item["current_price"] = current_price
    market_value = None
    for key in ("market_value", "m_dMarketValue"):
        for source in (item, raw):
            value = _as_float_or_none(source.get(key))
            if value is not None and math.isfinite(value) and value >= 0.0:
                market_value = value
                break
        if market_value is not None:
            break
    if market_value is not None:
        item["market_value"] = market_value
    return item


def _normalize_order(row: Dict[str, Any]) -> Dict[str, Any]:
    """规范化 BigQMT 委托字段，并提升真机 raw 中的方向和委托时间。

    Args:
        row: helper 返回的订单字典，可包含嵌套 ``raw`` 原始字段。

    Returns:
        Dict[str, Any]: 可用于订单确认、撤单确认和公共查询的规范订单。
    """

    item = dict(row)
    raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
    raw_status = item.get("raw_status")
    if raw_status is None:
        raw_status = (
            item.get("m_nOrderStatus") or item.get("order_status") or raw.get("m_nOrderStatus")
        )
    raw_int = _to_int(raw_status)
    if raw_int is not None:
        item["raw_status"] = raw_int
        item.setdefault("status", _ORDER_STATUS_MAP.get(raw_int, "unknown"))
    if "security" not in item:
        security = _security_from_qmt_fields(item)
        if security:
            item["security"] = security
    item.setdefault(
        "order_id",
        item.get("order_sys_id") or item.get("m_strOrderSysID") or item.get("entrust_no"),
    )
    item.setdefault("filled", item.get("traded") or item.get("m_nTradedVolume"))
    item.setdefault("amount", item.get("volume") or item.get("m_nVolume"))
    item.setdefault("price", item.get("order_price") or item.get("m_dLimitPrice"))
    side = _normalize_big_qmt_order_side(
        item.get("side") or item.get("direction") or item.get("m_nOpType") or raw.get("m_nOpType")
    )
    if side:
        item["side"] = side
        item.setdefault("direction", side)
    order_time = _big_qmt_order_timestamp(item, raw)
    if order_time is not None:
        item.setdefault("order_time", order_time)
    qmt_user_order_id = (
        item.get("qmt_user_order_id")
        or item.get("m_strUserOrderId")
        or raw.get("qmt_user_order_id")
        or raw.get("m_strUserOrderId")
        or raw.get("m_strRemark")
    )
    if qmt_user_order_id:
        item.setdefault("qmt_user_order_id", qmt_user_order_id)
    item.setdefault(
        "order_remark",
        item.get("remark")
        or item.get("m_strRemark")
        or item.get("m_strUserOrderId")
        or raw.get("m_strRemark")
        or raw.get("m_strUserOrderId"),
    )
    if item.get("order_remark") and "remark" not in item:
        item["remark"] = item.get("order_remark")
    sub_account_id = item.get("sub_account_id") or item.get("virtual_account_id")
    if not sub_account_id:
        sub_account_id = _extract_virtual_account_from_remark(
            item.get("order_remark") or item.get("remark")
        )
    if sub_account_id:
        item.setdefault("sub_account_id", sub_account_id)
        item.setdefault("virtual_account_id", sub_account_id)
    return item


def _normalize_big_qmt_order_side(value: Any) -> str:
    """把 QMT 操作类型或文本方向规范为 BUY/SELL。

    Args:
        value: ``m_nOpType`` 数字或买卖方向文本。

    Returns:
        str: ``BUY``、``SELL`` 或无法识别时的空字符串。
    """

    text = str(value or "").strip().upper()
    if text in {"23", "BUY", "B", "买", "买入"}:
        return "BUY"
    if text in {"24", "SELL", "S", "卖", "卖出"}:
        return "SELL"
    return ""


def _big_qmt_order_timestamp(item: Dict[str, Any], raw: Dict[str, Any]) -> Optional[float]:
    """从顶层或 raw 的 QMT 日期时间字段解析委托时间。

    Args:
        item: helper 的规范字段候选。
        raw: helper 保留的 QMT 原始订单字段。

    Returns:
        Optional[float]: 本地时区 Unix 秒；缺失或格式无效时为 None。
    """

    for source in (item, raw):
        date_value = source.get("m_strInsertDate") or source.get("insert_date")
        time_value = source.get("m_strInsertTime") or source.get("insert_time")
        if date_value not in (None, "") and time_value not in (None, ""):
            date_text = "".join(char for char in str(date_value) if char.isdigit())[:8]
            time_text = "".join(char for char in str(time_value) if char.isdigit())[:6]
            if len(date_text) == 8 and len(time_text) == 6:
                try:
                    return datetime.strptime(date_text + time_text, "%Y%m%d%H%M%S").timestamp()
                except ValueError:
                    pass
    for source in (item, raw):
        for key in ("order_time", "datetime", "time"):
            parsed = _parse_order_timestamp(source.get(key))
            if parsed is not None:
                return parsed
    return None


def _normalize_trade(row: Dict[str, Any]) -> Dict[str, Any]:
    item = dict(row)
    if "security" not in item:
        security = _security_from_qmt_fields(item)
        if security:
            item["security"] = security
    item.setdefault("trade_id", item.get("m_strTradeID") or item.get("deal_id"))
    item.setdefault("order_id", item.get("m_strOrderSysID") or item.get("order_sys_id"))
    item.setdefault("amount", item.get("volume") or item.get("m_nVolume"))
    item.setdefault("price", item.get("trade_price") or item.get("m_dTradePrice"))
    item.setdefault(
        "order_remark",
        item.get("remark") or item.get("m_strRemark") or item.get("m_strUserOrderId"),
    )
    if item.get("order_remark") and "remark" not in item:
        item["remark"] = item.get("order_remark")
    sub_account_id = item.get("sub_account_id") or item.get("virtual_account_id")
    if not sub_account_id:
        sub_account_id = _extract_virtual_account_from_remark(
            item.get("order_remark") or item.get("remark")
        )
    if sub_account_id:
        item.setdefault("sub_account_id", sub_account_id)
        item.setdefault("virtual_account_id", sub_account_id)
    return item


def _filter_orders(orders: List[Dict], filters: Dict) -> List[Dict]:
    order_id = filters.get("order_id")
    security = filters.get("security")
    status = filters.get("status")
    sub_account_id = _virtual_account_id(filters)
    if order_id:
        orders = [item for item in orders if str(item.get("order_id")) == str(order_id)]
    if security:
        orders = [item for item in orders if item.get("security") == security]
    if status is not None:
        status_value = getattr(status, "value", status)
        orders = [item for item in orders if str(item.get("status")) == str(status_value)]
    if sub_account_id:
        orders = [
            item
            for item in orders
            if str(item.get("sub_account_id") or item.get("virtual_account_id") or "")
            == sub_account_id
            or _remark_matches_virtual_account(
                item.get("order_remark") or item.get("remark"), sub_account_id
            )
        ]
    return orders


def _gateway_order_filters(filters: Dict) -> Dict[str, Any]:
    result = dict(filters or {})
    for key in (
        "sub_account_id",
        "subAccountId",
        "virtual_account_id",
        "virtualAccountId",
        "virtual_account",
    ):
        result.pop(key, None)
    return result


def _filter_trades(trades: List[Dict], filters: Dict) -> List[Dict]:
    order_id = filters.get("order_id")
    security = filters.get("security")
    sub_account_id = _virtual_account_id(filters)
    if order_id:
        trades = [item for item in trades if str(item.get("order_id")) == str(order_id)]
    if security:
        trades = [item for item in trades if item.get("security") == security]
    if sub_account_id:
        trades = [
            item
            for item in trades
            if str(item.get("sub_account_id") or item.get("virtual_account_id") or "")
            == sub_account_id
            or _remark_matches_virtual_account(
                item.get("order_remark") or item.get("remark"), sub_account_id
            )
        ]
    return trades


def _security_from_qmt_fields(item: Dict[str, Any]) -> Optional[str]:
    code = item.get("stock_code") or item.get("m_strInstrumentID") or item.get("instrument_id")
    exchange = item.get("exchange") or item.get("m_strExchangeID") or item.get("exchange_id")
    if not code:
        return None
    code_text = str(code)
    if "." in code_text:
        return code_text
    exchange_text = str(exchange or "").upper()
    if exchange_text in {"SZ", "XSHE", "SZE"}:
        return f"{code_text}.XSHE"
    if exchange_text in {"SH", "SSE", "XSHG"}:
        return f"{code_text}.XSHG"
    return code_text


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


register_adapter("big_qmt", build_big_qmt_bundle)
register_adapter("big-qmt", build_big_qmt_bundle)
