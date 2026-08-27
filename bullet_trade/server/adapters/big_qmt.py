from __future__ import annotations

import asyncio
import hashlib
import json
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


@dataclass
class BigQmtGatewayConfig:
    base_url: str = "http://127.0.0.1:9000"
    password: Optional[str] = None
    secret: Optional[str] = None
    timeout_seconds: float = 10.0
    health_ttl_seconds: float = 15.0
    action_status: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class BigQmtGatewayError(RuntimeError):
    def __init__(self, message: str, *, code: str = "BIG_QMT_GATEWAY_ERROR") -> None:
        super().__init__(message)
        self.code = code


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
        try:
            raw = exc.read()
            payload = json.loads(raw.decode("utf-8")) if raw else {}
            if isinstance(payload, dict):
                message = str(payload.get("message") or payload.get("error") or message)
                code = str(payload.get("code") or code)
        except Exception:
            pass
        return BigQmtGatewayError(message, code=code)

    def _unwrap_response(self, payload: Any) -> Any:
        if not isinstance(payload, dict):
            return payload
        ok = payload.get("ok")
        if ok is False:
            message = str(payload.get("message") or payload.get("error") or "big QMT gateway 请求失败")
            code = str(payload.get("code") or payload.get("error_code") or "BIG_QMT_GATEWAY_ERROR")
            raise BigQmtGatewayError(message, code=code)
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
        try:
            await self.client.health()
        except BigQmtGatewayError:
            pass

    async def stop(self) -> None:
        return None

    def qmt_status(self) -> Dict[str, Any]:
        return self.client.qmt_status()

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
        payload = self._account_payload(account)
        payload.update(_gateway_order_filters(filters or {}))
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
        payload = self._account_payload(account)
        payload.update(_gateway_order_filters(filters or {}))
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
        request = self._account_payload(account)
        request.update(payload or {})
        _ensure_virtual_account_remark(request)
        _ensure_gateway_submission_identity(request)
        submitted_at = time.time()
        wait_timeout = _positive_float((payload or {}).get("wait_timeout"))
        known_order_ids = set()
        if wait_timeout > 0:
            known_order_ids = await self._snapshot_order_ids(account, request)
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
        bool: 找到可解析订单时间且它不早于请求前五秒时返回 True。
    """

    if submitted_at is None:
        return False
    for key in ("order_time", "insert_time", "datetime", "time", "m_strInsertTime"):
        value = order.get(key)
        parsed = _parse_order_timestamp(value)
        if parsed is not None:
            return parsed >= submitted_at - 5.0
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
    price = _request_order_price(request)
    order_price = order.get("order_price")
    if order_price in (None, ""):
        order_price = order.get("price")
    if price not in (None, "") and order_price not in (None, "", 0, 0.0):
        if not _float_close(order_price, price):
            return False
    sub_account_id = _virtual_account_id(request)
    if sub_account_id:
        row_sub = str(order.get("sub_account_id") or order.get("virtual_account_id") or "")
        if row_sub != sub_account_id and not _remark_matches_virtual_account(
            order.get("order_remark") or order.get("remark"),
            sub_account_id,
        ):
            return False
    remark = request.get("order_remark") or request.get("remark")
    if remark and not (
        order.get("order_remark") == remark
        or order.get("remark") == remark
        or (
            bool(sub_account_id)
            and _remark_matches_virtual_account(
                order.get("order_remark") or order.get("remark"), sub_account_id
            )
        )
    ):
        return False
    return True


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
    item = dict(row)
    security = item.get("security") or _security_from_qmt_fields(item)
    if security:
        item["security"] = security
    if "closeable_amount" not in item:
        item["closeable_amount"] = item.get("available") or item.get("m_nCanUseVolume")
    if "amount" not in item:
        item["amount"] = item.get("volume") or item.get("m_nVolume")
    if "cost_basis" not in item:
        item["cost_basis"] = item.get("avg_cost") or item.get("m_dOpenPrice")
    return item


def _normalize_order(row: Dict[str, Any]) -> Dict[str, Any]:
    item = dict(row)
    raw_status = item.get("raw_status")
    if raw_status is None:
        raw_status = item.get("m_nOrderStatus") or item.get("order_status")
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
