"""
作者: BruceLee

文件职责: 把 HuaxinBroker 与独立 XMD L1 backend 暴露为通用远程 server adapter。
主要输入: ServerConfig、账户路由、外部 HUAXIN_* 私密 Trader 配置和显式 HUAXIN_XMD_*。
主要输出: Trader 查询/写入结果，以及仅来自 huaxin_xmd_l1 新鲜缓存的实时快照。
上游关系: ``bullet-trade server --server-type huaxin`` 的 broker/data 独立模块开关。
下游关系: HuaxinBroker 与 Python 3.7 XMD sidecar backend；两个 readiness 互不替代。
关键配置: 非回环监听必须 TLS+固定 token+allowlist；华鑫写入必须携带显式执行价，
server 不使用历史 K 线或其他 provider 补价。
"""

from __future__ import annotations

import asyncio
import functools
import ipaddress
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Set

from bullet_trade.integrations.huaxin.broker import HuaxinBroker
from bullet_trade.integrations.huaxin.errors import (
    HUAXIN_MARKET_ORDER_DISABLED,
    HUAXIN_NATIVE_UNAVAILABLE,
    HuaxinNativeUnavailableError,
    HuaxinTradingDisabledError,
)
from bullet_trade.integrations.huaxin.xmd_backend import (
    DEFAULT_MAX_AGE_SECONDS,
    HUAXIN_XMD_SOURCE,
    Python37XmdBackend,
    XmdBackend,
    XmdBackendError,
    _validate_tcp_front,
)
from bullet_trade.utils.env_loader import (
    get_broker_config,
    get_env,
    get_env_bool,
    get_env_float,
    get_env_int,
)

from ..config import ServerConfig
from . import register_adapter
from .base import AccountContext, AccountRouter, AdapterBundle, RemoteBrokerAdapter

log = logging.getLogger(__name__)

_RECOVERY_POLL_SECONDS = 5.0
_RECOVERABLE_XMD_START_CODES = frozenset(
    {"front_disconnected", "login_failed", "sidecar_ready_timeout"}
)
_RECOVERABLE_XMD_RUNTIME_CODES = frozenset(
    {
        "backend_not_running",
        "front_disconnected",
        "login_failed",
        "sidecar_response_timeout",
        "sidecar_write_failed",
    }
)


def _is_loopback_listener(value: str) -> bool:
    """判断监听地址是否严格限制在本机回环接口。

    Args:
        value: ServerConfig.listen 地址。

    Returns:
        bool: localhost 或 IP loopback 时为 True。
    """

    text = str(value or "").strip().lower()
    if text == "localhost":
        return True
    try:
        return bool(ipaddress.ip_address(text).is_loopback)
    except ValueError:
        return False


def _load_huaxin_broker_config() -> Dict[str, Any]:
    """从统一 env loader 与私密环境变量装配 Trader 会话配置。

    Returns:
        Dict[str, Any]: 只保存在进程内、不会写日志的 HuaxinBroker 配置。

    Side Effects:
        仅读取当前进程环境；不读取 secret 文件内容、不联网。
    """

    config = dict((get_broker_config().get("huaxin") or {}))
    text_fields = {
        "flow_path": "HUAXIN_FLOW_PATH",
        "trade_front": "HUAXIN_TRADE_FRONT",
        "login_account": "HUAXIN_LOGIN_ACCOUNT",
        "password": "HUAXIN_PASSWORD",
        "terminal_info": "HUAXIN_TERMINAL_INFO",
        "mac_address": "HUAXIN_MAC_ADDRESS",
        "department_id": "HUAXIN_DEPARTMENT_ID",
        "dynamic_password": "HUAXIN_DYNAMIC_PASSWORD",
        "user_product_info": "HUAXIN_USER_PRODUCT_INFO",
        "interface_product_info": "HUAXIN_INTERFACE_PRODUCT_INFO",
        "interface_address": "HUAXIN_INTERFACE_ADDRESS",
        "login_account_type": "HUAXIN_LOGIN_ACCOUNT_TYPE",
        "trade_comm_mode": "HUAXIN_TRADE_COMM_MODE",
        "private_topic": "HUAXIN_PRIVATE_TOPIC",
        "public_topic": "HUAXIN_PUBLIC_TOPIC",
        "investor_id": "HUAXIN_INVESTOR_ID",
        "shareholder_id": "HUAXIN_SHAREHOLDER_ID",
        "business_unit_id": "HUAXIN_BUSINESS_UNIT_ID",
    }
    for key, env_name in text_fields.items():
        value = get_env(env_name)
        if value not in (None, ""):
            config[key] = value
    config["encrypt"] = get_env_bool("HUAXIN_ENCRYPT", bool(config.get("encrypt", False)))
    config["queue_capacity"] = get_env_int("HUAXIN_EVENT_QUEUE_CAPACITY", 1024)
    config["drain_max_events"] = get_env_int("HUAXIN_DRAIN_MAX_EVENTS", 256)
    config["connect_timeout"] = get_env_float("HUAXIN_CONNECT_TIMEOUT", 30.0)
    config["query_timeout"] = get_env_float("HUAXIN_QUERY_TIMEOUT", 10.0)
    config["write_response_timeout"] = get_env_float("HUAXIN_WRITE_RESPONSE_TIMEOUT", 3.0)
    return config


def _validate_huaxin_server_config(config: ServerConfig, broker_config: Mapping[str, Any]) -> None:
    """在创建 adapter 前执行 Trader/XMD、网络和幂等安全门禁。

    Args:
        config: 通用远程服务配置。
        broker_config: 华鑫 Trader 门禁配置；data-only 模式可为空。

    Returns:
        None。

    Raises:
        ValueError: 模块配置、账户或外网安全条件不完整时抛出。
    """

    if not config.enable_data and not config.enable_broker:
        raise ValueError("Huaxin server 至少必须启用 broker 或 XMD data 模块")
    if not _is_loopback_listener(config.listen):
        if not config.tls.enabled or not config.tls.cert_path or not config.tls.key_path:
            raise ValueError("非回环 Huaxin server 必须启用 TLS 证书和私钥")
        if config.generated_token or not str(config.token or "").strip():
            raise ValueError("非回环 Huaxin server 必须配置固定 token，禁止临时随机 token")
        if not config.allowlist:
            raise ValueError("非回环 Huaxin server 必须配置来源 IP allowlist")
    if config.enable_broker:
        if not config.accounts:
            raise ValueError("启用 Huaxin broker 时至少需要一个账户配置")
        required_login_fields = (
            ("mac_address", "HUAXIN_MAC_ADDRESS", 20),
            ("user_product_info", "HUAXIN_USER_PRODUCT_INFO", 10),
        )
        for config_key, env_name, max_bytes in required_login_fields:
            text = str(broker_config.get(config_key) or "")
            if not text.strip():
                raise ValueError(f"Huaxin server 必须配置 {env_name}")
            try:
                encoded = text.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ValueError(f"Huaxin server {env_name} 必须可编码为 UTF-8") from exc
            if len(encoded) > max_bytes:
                raise ValueError(f"Huaxin server {env_name} UTF-8 长度不能超过 {max_bytes} 字节")
    if config.enable_data:
        backend = str(config.huaxin_xmd_backend or "").strip().lower()
        if backend != "python37_sidecar":
            raise ValueError("启用 Huaxin data 时 HUAXIN_XMD_BACKEND 必须为 python37_sidecar")
        required_xmd_fields = (
            (config.huaxin_xmd_python, "HUAXIN_XMD_PYTHON"),
            (config.huaxin_xmd_sdk_dir, "HUAXIN_XMD_SDK_DIR"),
            (config.huaxin_xmd_front, "HUAXIN_XMD_FRONT"),
        )
        for value, env_name in required_xmd_fields:
            if not str(value or "").strip():
                raise ValueError(f"启用 Huaxin data 时必须配置 {env_name}")
        try:
            _validate_tcp_front(str(config.huaxin_xmd_front or ""))
        except XmdBackendError as exc:
            raise ValueError("HUAXIN_XMD_FRONT 必须为 tcp://host:port") from exc
        positive_fields = (
            (config.huaxin_xmd_max_age_seconds, "HUAXIN_XMD_MAX_AGE_SECONDS"),
            (config.huaxin_xmd_connect_timeout, "HUAXIN_XMD_CONNECT_TIMEOUT"),
            (config.huaxin_xmd_command_timeout, "HUAXIN_XMD_COMMAND_TIMEOUT"),
            (config.huaxin_xmd_snapshot_timeout, "HUAXIN_XMD_SNAPSHOT_TIMEOUT"),
        )
        for numeric_value, env_name in positive_fields:
            try:
                parsed = float(numeric_value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"{env_name} 必须为正数") from exc
            if not math.isfinite(parsed) or parsed <= 0:
                raise ValueError(f"{env_name} 必须为正数")
            if env_name == "HUAXIN_XMD_MAX_AGE_SECONDS" and parsed > DEFAULT_MAX_AGE_SECONDS:
                raise ValueError(f"{env_name} 不能超过默认安全上限 {DEFAULT_MAX_AGE_SECONDS:g} 秒")


class HuaxinBrokerAdapter(RemoteBrokerAdapter):
    """在独立单线程 executor 中串行调用同步 Trader runtime。

    单线程可以避免阻塞 asyncio server，并保证同一进程内的 query/drain/write 顺序不会
    交错；每个账户仍由独立 HuaxinBroker/NativeRuntime 管理实际 SDK 会话。
    """

    requires_explicit_execution_price = True

    def __init__(
        self,
        config: ServerConfig,
        account_router: AccountRouter,
        *,
        broker_config: Optional[Mapping[str, Any]] = None,
        broker_factory: Callable[..., HuaxinBroker] = HuaxinBroker,
    ) -> None:
        """保存服务配置并创建华鑫专用 executor。

        Args:
            config: 通用远程服务配置。
            account_router: 父账户路由器。
            broker_config: 可注入的私密 Trader 配置；默认从环境读取。
            broker_factory: 测试可注入的 HuaxinBroker 等价工厂。

        Returns:
            None。

        Side Effects:
            创建一个尚未执行任务的单线程池，不连接 Trader。
        """

        self.config = config
        self.account_router = account_router
        self._broker_config = dict(broker_config or _load_huaxin_broker_config())
        self._broker_factory = broker_factory
        self._brokers: Dict[str, HuaxinBroker] = {}
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="huaxin-trader")
        self._stopped = False
        self._state = "starting"
        self._last_error_reason: Optional[str] = None
        self._watchdog_task: Optional[asyncio.Task] = None
        self._watchdog_stop: Optional[asyncio.Event] = None
        self._watchdog_interval_seconds = _RECOVERY_POLL_SECONDS
        self._ever_ready = False
        self._rebuild_required = False
        self._reconnect_count = 0

    async def start(self) -> None:
        """依次创建并连接全部配置账户。

        Returns:
            None。

        Raises:
            Exception: 任一账户未达到只读查询 readiness 时原样抛出并清理已连接账户。

        Side Effects:
            在专用线程中 dlopen、启动 Trader 会话并把 broker handle 挂到路由器。
        """

        self._state = "starting"
        try:
            await self._connect_all()
        except Exception as exc:
            await self._disconnect_all()
            self._state = "unavailable"
            self._last_error_reason = type(exc).__name__
            raise
        if await self._brokers_are_ready():
            self._state = "ready"
            self._ever_ready = True
        else:
            self._state = "degraded"
            self._last_error_reason = "trader_readiness_pending"
        self._watchdog_stop = asyncio.Event()
        self._watchdog_task = asyncio.create_task(
            self._watchdog_loop(),
            name="huaxin-trader-recovery",
        )

    async def stop(self) -> None:
        """幂等断开全部 Trader 会话并关闭专用 executor。

        Returns:
            None。

        Side Effects:
            尽力调用每个 broker.disconnect，并停止接收新线程任务。
        """

        if self._stopped:
            return
        self._stopped = True
        self._state = "stopped"
        if self._watchdog_stop is not None:
            self._watchdog_stop.set()
        task = self._watchdog_task
        self._watchdog_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await self._disconnect_all()
        self._executor.shutdown(wait=False)

    async def get_account_info(self, account: AccountContext) -> Dict[str, Any]:
        """查询账户资金并保持远程 dict payload 兼容。

        Args:
            account: 已路由父账户上下文。

        Returns:
            Dict[str, Any]: ``{"dtype":"dict","value":...}``。
        """

        info = await self._run_ready(self._broker_for(account).get_account_info)
        return {"dtype": "dict", "value": info or {}}

    async def get_positions(self, account: AccountContext) -> List[Dict[str, Any]]:
        """查询账户持仓。

        Args:
            account: 已路由父账户上下文。

        Returns:
            List[Dict[str, Any]]: 规范化持仓列表。
        """

        return list(await self._run_ready(self._broker_for(account).get_positions) or [])

    async def list_orders(
        self, account: AccountContext, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """查询并过滤当日委托。

        Args:
            account: 已路由父账户上下文。
            filters: 可选 order_id/security/status 条件。

        Returns:
            List[Dict[str, Any]]: 委托列表。
        """

        body = dict(filters or {})
        broker = self._broker_for(account)
        return list(
            await self._run_ready(
                broker.get_orders,
                order_id=body.get("order_id"),
                security=body.get("security"),
                status=body.get("status"),
                from_broker=True,
            )
            or []
        )

    async def list_trades(
        self, account: AccountContext, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """查询并过滤当日成交。

        Args:
            account: 已路由父账户上下文。
            filters: 可选 order_id/security 条件。

        Returns:
            List[Dict[str, Any]]: 成交列表。
        """

        body = dict(filters or {})
        broker = self._broker_for(account)
        return list(
            await self._run_ready(
                broker.get_trades,
                order_id=body.get("order_id"),
                security=body.get("security"),
            )
            or []
        )

    async def get_order_status(self, account: AccountContext, order_id: str) -> Dict[str, Any]:
        """查询一个精确订单的当前状态。

        Args:
            account: 已路由父账户上下文。
            order_id: 稳定本地或柜台订单号。

        Returns:
            Dict[str, Any]: 找到时返回订单，否则为空字典。
        """

        rows = await self.list_orders(account, {"order_id": order_id})
        return rows[0] if rows else {}

    async def place_order(self, account: AccountContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        """透传显式限价或沪深白名单原生市价及持久幂等元数据。

        Args:
            account: 已路由父账户上下文。
            payload: 通用 broker.place_order 请求。

        Returns:
            Dict[str, Any]: accepted、rejected 或 submit_unknown 响应。
        """

        style = payload.get("style") or {"type": "limit"}
        if not isinstance(style, Mapping):
            raise ValueError("Huaxin server style 必须为对象")
        style_type = str(style.get("type") or "limit").strip().lower()
        canonical_market_types = {
            "home_best",
            "opponent_best",
            "five_level_ioc",
            "five_level_to_limit",
            "immediate_or_cancel",
            "fill_or_kill",
        }
        if style_type == "limit":
            market = False
            market_type = ""
        elif style_type == "market":
            market = True
            market_type = (
                str(style.get("market_type") or payload.get("market_type") or "").strip().lower()
            )
        elif style_type in canonical_market_types:
            market = True
            market_type = style_type
        else:
            raise HuaxinTradingDisabledError(
                HUAXIN_MARKET_ORDER_DISABLED,
                "Huaxin server 不支持当前订单风格",
                {
                    "style_type": style_type,
                    "supported_styles": ["limit", "market"],
                },
            )
        if market and not market_type:
            raise HuaxinTradingDisabledError(
                HUAXIN_MARKET_ORDER_DISABLED,
                "Huaxin server 原生市价单必须显式指定 market_type",
                {"supported_market_types": sorted(canonical_market_types)},
            )
        price = style.get("price") if not market else style.get("protect_price")
        if market and price in (None, ""):
            price = style.get("limit_price")
        if market and price in (None, ""):
            price = style.get("price")
        if price in (None, ""):
            price = payload.get("price")
        amount = payload.get("amount")
        if amount is None:
            amount = payload.get("volume")
        direction = str(payload.get("side") or "BUY").strip().lower()
        broker = self._broker_for(account)
        extra = dict(payload)
        if market_type:
            extra["market_type"] = market_type
        return dict(
            await self._run_ready(
                broker.submit_order,
                direction,
                str(payload.get("security") or ""),
                amount,
                price,
                market=market,
                extra=extra,
                wait_timeout=payload.get("wait_timeout"),
            )
            or {}
        )

    async def cancel_order(self, account: AccountContext, order_id: str) -> Dict[str, Any]:
        """兼容旧 server 调度的精确订单撤单入口。

        Args:
            account: 已路由父账户上下文。
            order_id: 精确柜台或已缓存稳定本地订单号。

        Returns:
            Dict[str, Any]: 明确已撤、拒绝或 submit_unknown。
        """

        return await self.cancel_order_request(account, {"order_id": order_id})

    async def cancel_order_request(
        self, account: AccountContext, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """保留 idempotency_key 和 TORA provider extension 的撤单入口。

        Args:
            account: 已路由父账户上下文。
            payload: 含 order_id、幂等键和可选精确 TORA 身份的完整请求。

        Returns:
            Dict[str, Any]: 明确已撤、拒绝或 submit_unknown。
        """

        order_id = str(payload.get("order_id") or "").strip()
        if not order_id:
            raise ValueError("缺少 order_id")
        broker = self._broker_for(account)
        return dict(
            await self._run_ready(
                broker.submit_cancel_order,
                order_id,
                dict(payload),
                wait_timeout=payload.get("wait_timeout"),
            )
            or {}
        )

    def backend_status(self) -> Dict[str, Any]:
        """聚合全部账户的脱敏 Trader readiness 与 action 状态。

        Returns:
            Dict[str, Any]: 不含账户 secret、前置地址或 TerminalInfo 的 health 快照。
        """

        accounts = {key: broker.health_snapshot() for key, broker in sorted(self._brokers.items())}
        native_query_ready = bool(accounts) and all(
            bool(item.get("ready_for_queries")) for item in accounts.values()
        )
        baseline_query_ready = bool(accounts) and all(
            bool(item.get("baseline_query_ready")) for item in accounts.values()
        )
        query_ready = self._state == "ready" and native_query_ready and baseline_query_ready
        order_ready = query_ready and all(
            bool(item.get("ready_for_new_orders"))
            and bool(item.get("trading_enabled"))
            and bool(item.get("order_ref_allocator_ready"))
            and bool(item.get("security_order_constraints_ready"))
            for item in accounts.values()
        )
        cancel_ready = query_ready and all(
            bool(item.get("ready_for_cancel"))
            and bool(item.get("cancel_order_enabled"))
            and bool(item.get("order_identity_ready"))
            for item in accounts.values()
        )
        state = "ready" if query_ready else self._state
        if state not in {"ready", "starting", "degraded", "reconnecting", "stopped", "unavailable"}:
            state = "degraded" if native_query_ready else "unavailable"
        query_action_status = "ready" if query_ready else state
        reason = self._last_error_reason
        if not query_ready:
            reason = reason or (
                "four_baseline_queries_not_completed"
                if native_query_ready
                else "native_query_not_ready"
            )
        return {
            "backend_type": "huaxin",
            "component": "trader",
            "ready": query_ready,
            "state": state,
            "reason": reason,
            "reconnect_count": self._reconnect_count,
            "accounts": accounts,
            "actions": {
                "broker.account": {"status": query_action_status},
                "broker.positions": {"status": query_action_status},
                "broker.orders": {"status": query_action_status},
                "broker.trades": {"status": query_action_status},
                "broker.place_order": {"status": "ready" if order_ready else "unavailable"},
                "broker.cancel_order": {"status": "ready" if cancel_ready else "unavailable"},
            },
        }

    def _broker_for(self, account: AccountContext) -> HuaxinBroker:
        """取得当前账户已连接的 HuaxinBroker。

        Args:
            account: 已路由父账户上下文。

        Returns:
            HuaxinBroker: 已连接 broker。

        Raises:
            RuntimeError: adapter 尚未启动或账户未连接时抛出。
        """

        key = account.config.key or "default"
        broker = self._brokers.get(key)
        if broker is None:
            raise RuntimeError(f"Huaxin broker 账户尚未连接: {key}")
        return broker

    async def _connect_all(self) -> None:
        """按账户配置创建并连接一套新的 Broker 映射。

        Returns:
            None。

        Side Effects:
            在唯一 Trader executor 中创建会话，并把新 handle 原子挂到路由器。
        """

        for ctx in self.account_router.list_accounts():
            key = ctx.config.key or "default"
            account_config = dict(self._broker_config)
            account_config["account_id"] = ctx.config.account_id
            account_config["account_type"] = ctx.config.account_type
            broker = self._broker_factory(
                account_id=ctx.config.account_id,
                account_type=ctx.config.account_type,
                config=account_config,
            )
            self._brokers[key] = broker
            await self._run(broker.connect)
            await self.account_router.attach_handle(key, broker)

    async def _brokers_are_ready(self) -> bool:
        """在线程安全边界内检查全部 Trader 查询与基线 readiness。

        Returns:
            bool: 每个配置账户都完成 native 查询 readiness 和四项基线时为 True。
        """

        return bool(await self._run(self._brokers_are_ready_sync))

    def _brokers_are_ready_sync(self) -> bool:
        """在 Trader executor 内同步读取全部 Broker health。

        Returns:
            bool: 至少有一个 Broker 且全部查询与基线已就绪时为 True。
        """

        if not self._brokers:
            return False
        for broker in self._brokers.values():
            health = dict(broker.health_snapshot() or {})
            if not bool(health.get("ready_for_queries")):
                return False
            if not bool(health.get("baseline_query_ready")):
                return False
        return True

    def _refresh_broker_baselines_sync(self) -> None:
        """对已登录但基线不完整的 Broker 重试四项只读查询。

        Returns:
            None。

        Side Effects:
            只调用资金、持仓、订单和成交查询，不调用任何交易写接口。
        """

        for broker in self._brokers.values():
            health = dict(broker.health_snapshot() or {})
            if not bool(health.get("ready_for_queries")):
                continue
            completed = set(health.get("baseline_queries_completed") or ())
            operations = (
                ("account", broker.get_account_info),
                ("positions", broker.get_positions),
                ("orders", broker.get_orders),
                ("trades", broker.get_trades),
            )
            for name, operation in operations:
                if name in completed:
                    continue
                operation()

    async def _watchdog_loop(self) -> None:
        """持续观察 Trader health，并在 ready 后失活时重建完整 Broker。

        Returns:
            None。

        Side Effects:
            只执行连接、断开和基线查询；绝不调用下单或撤单。
        """

        assert self._watchdog_stop is not None
        while not self._stopped and not self._watchdog_stop.is_set():
            try:
                ready = await self._brokers_are_ready()
                if not ready:
                    try:
                        await self._run(self._refresh_broker_baselines_sync)
                    except Exception:
                        pass
                    ready = await self._brokers_are_ready()
                if ready:
                    if self._state != "ready":
                        log.info("华鑫 Trader 已恢复 ready，远程策略可继续查询和交易")
                    self._state = "ready"
                    self._last_error_reason = None
                    self._ever_ready = True
                    self._rebuild_required = False
                else:
                    if self._state == "ready":
                        self._state = "degraded"
                        self._last_error_reason = "trader_runtime_lost"
                        self._rebuild_required = True
                        log.warning("华鑫 Trader 运行中失去 readiness，开始原进程恢复")
                    if self._rebuild_required:
                        self._state = "reconnecting"
                        try:
                            await self._rebuild_all()
                            ready = await self._brokers_are_ready()
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            self._state = "degraded"
                            self._last_error_reason = type(exc).__name__
                            log.warning("华鑫 Trader 本轮重连未就绪: %s", exc)
                        else:
                            self._rebuild_required = False
                            if ready:
                                self._reconnect_count += 1
                                self._state = "ready"
                                self._last_error_reason = None
                                self._ever_ready = True
                                log.info("华鑫 Trader 新会话与基线查询已恢复")
                            else:
                                self._state = "degraded"
                                self._last_error_reason = "trader_readiness_pending"
                    elif self._state not in {"starting", "stopped"}:
                        self._state = "degraded"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._state = "degraded"
                self._last_error_reason = type(exc).__name__
                if self._ever_ready:
                    self._rebuild_required = True
                log.warning("华鑫 Trader watchdog 检查失败: %s", exc)
            try:
                await asyncio.wait_for(
                    self._watchdog_stop.wait(),
                    timeout=max(0.01, float(self._watchdog_interval_seconds)),
                )
            except asyncio.TimeoutError:
                pass

    async def _rebuild_all(self) -> None:
        """在唯一 executor 中释放旧 Broker 并创建全新会话。

        Returns:
            None。

        Raises:
            Exception: 新会话创建或连接失败时原样抛出。

        Side Effects:
            重新提交登录与 TerminalInfo，并由 Broker.connect 完成基线查询和 MaxOrderRef 初始化。
        """

        await self._disconnect_all()
        try:
            await self._connect_all()
        except Exception:
            await self._disconnect_all()
            raise

    def _require_ready(self) -> None:
        """在业务调用进入 executor 前执行快速 readiness 门禁。

        Returns:
            None。

        Raises:
            HuaxinNativeUnavailableError: adapter 正在启动、降级、重连或已经停止时抛出。
        """

        if self._state != "ready" or self._stopped:
            raise HuaxinNativeUnavailableError(
                HUAXIN_NATIVE_UNAVAILABLE,
                "华鑫 Trader 正在恢复连接，请稍后重试",
                {"runtime_state": self._state},
            )

    async def _run_ready(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """执行一次只允许在 ready 状态进入的 Trader 业务调用。

        Args:
            func: 同步 Broker 查询或写方法。
            *args: 位置参数。
            **kwargs: 关键字参数。

        Returns:
            Any: Broker 方法返回值。

        Raises:
            HuaxinNativeUnavailableError: 调用前或调用中发现 native 会话不可用时抛出。
        """

        self._require_ready()
        try:
            return await self._run(func, *args, **kwargs)
        except HuaxinNativeUnavailableError as exc:
            self._state = "degraded"
            self._last_error_reason = str(exc.code)
            self._rebuild_required = True
            raise

    async def _run(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """在华鑫专用单线程 executor 中执行同步 SDK 调用。

        Args:
            func: 同步函数。
            *args: 位置参数。
            **kwargs: 关键字参数。

        Returns:
            Any: 同步函数结果。
        """

        loop = asyncio.get_running_loop()
        call = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(self._executor, call)

    async def _disconnect_all(self) -> None:
        """尽力断开当前已创建的全部 broker。

        Returns:
            None。

        Side Effects:
            串行调用 disconnect 并清空本地 broker 映射。
        """

        for broker in list(self._brokers.values()):
            try:
                await self._run(broker.disconnect)
            except Exception:
                pass
        self._brokers.clear()


class HuaxinDataAdapter:
    """把只读 XMD L1 backend 暴露为 server 当前时点数据接口。

    该 adapter 不实现 history/trade_days/security_info，也不会持有 Trader。所有返回值
    必须同时通过 backend 与 adapter 两层 source、证券身份、时间和盘口校验。
    """

    authoritative_realtime_only = True
    requires_explicit_execution_price = True

    def __init__(
        self,
        config: ServerConfig,
        *,
        backend_factory: Callable[..., XmdBackend] = Python37XmdBackend,
    ) -> None:
        """创建尚未启动的 XMD backend 与专用单线程 executor。

        Args:
            config: 已通过华鑫 data 配置门禁的服务配置。
            backend_factory: 测试可注入的 XmdBackend 工厂。

        Returns:
            None。

        Side Effects:
            创建一个尚未执行任务的线程池；不创建 sidecar、不连接行情。
        """

        self.config = config
        self._max_age_seconds = float(config.huaxin_xmd_max_age_seconds)
        self._snapshot_timeout = float(config.huaxin_xmd_snapshot_timeout)
        self._backend_factory = backend_factory
        self._backend = self._build_backend()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="huaxin-xmd")
        self._started = False
        self._stopped = False
        self._state = "starting"
        self._last_error_reason: Optional[str] = None
        self._subscriptions: Set[str] = set()
        self._watchdog_task: Optional[asyncio.Task] = None
        self._watchdog_stop: Optional[asyncio.Event] = None
        self._watchdog_interval_seconds = _RECOVERY_POLL_SECONDS
        self._reconnect_count = 0

    async def start(self) -> None:
        """在专用线程启动 sidecar 并等待 XMD 登录就绪。

        Returns:
            None。

        Raises:
            XmdBackendError: sidecar、SDK 或登录未就绪时原样抛出。

        Side Effects:
            只建立 XMD 行情会话，不创建 Trader、不订阅证券、不写业务数据。
        """

        if self._started and not self._stopped:
            return
        self._state = "starting"
        try:
            await self._run(self._backend.start)
        except XmdBackendError as exc:
            if exc.code not in _RECOVERABLE_XMD_START_CODES:
                self._state = "unavailable"
                raise
            self._state = "degraded"
            self._last_error_reason = exc.code
            log.warning("华鑫 XMD 当前未开放，Server 将保持存活并自动重连: %s", exc)
        else:
            self._state = "ready"
            self._last_error_reason = None
        self._started = True
        self._watchdog_stop = asyncio.Event()
        self._watchdog_task = asyncio.create_task(
            self._watchdog_loop(),
            name="huaxin-xmd-recovery",
        )

    async def stop(self) -> None:
        """幂等停止 XMD backend 并关闭专用 executor。

        Returns:
            None。

        Side Effects:
            最佳努力停止本 adapter 创建的 sidecar；不影响 Trader adapter。
        """

        if self._stopped:
            return
        self._stopped = True
        self._state = "stopped"
        if self._watchdog_stop is not None:
            self._watchdog_stop.set()
        task = self._watchdog_task
        self._watchdog_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        try:
            await self._run(self._backend.stop)
        finally:
            self._started = False
            self._executor.shutdown(wait=False)

    async def get_snapshot(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """返回目标证券的新鲜华鑫 L1 快照。

        Args:
            payload: 含 security/stock/stockcode 的远程 data.snapshot 请求。

        Returns:
            Dict[str, Any]: ``source=huaxin_xmd_l1`` 的 canonical 快照。

        Raises:
            ValueError: 请求缺少标准证券代码时抛出。
            XmdBackendError: 订阅、快照或时效门禁失败时抛出。
        """

        return await self._read_snapshot(self._security_from_payload(payload))

    async def get_live_current(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """返回与 data.snapshot 相同来源、相同时效合同的当前行情。

        Args:
            payload: 含标准证券代码的远程 data.live_current 请求。

        Returns:
            Dict[str, Any]: 新鲜华鑫 L1 快照。
        """

        return await self.get_snapshot(payload)

    async def get_current_tick(self, security: str) -> Dict[str, Any]:
        """兼容 server tick manager 读取一个标准证券快照。

        Args:
            security: ``.XSHG/.XSHE`` 标准证券代码。

        Returns:
            Dict[str, Any]: 新鲜华鑫 L1 快照。
        """

        return await self._read_snapshot(str(security or ""))

    async def current_tick(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """兼容 app 对 ``data.current_tick`` 的 payload 调度入口。

        Args:
            payload: 含标准证券代码的远程请求。

        Returns:
            Dict[str, Any]: 新鲜华鑫 L1 快照。
        """

        return await self.get_snapshot(payload)

    def backend_status(self) -> Dict[str, Any]:
        """返回独立于 Trader 的 XMD L1 readiness 与 action 状态。

        Returns:
            Dict[str, Any]: 不包含 Python/SDK 路径或前置地址的脱敏 health。
        """

        health = dict(self._backend.health() or {})
        ready = self._state == "ready" and bool(health.get("ready"))
        state = "ready" if ready else self._state
        if state not in {"ready", "starting", "degraded", "reconnecting", "stopped", "unavailable"}:
            state = str(health.get("state") or "unavailable")
        action_state = "ready" if ready else state
        reason = self._last_error_reason or health.get("last_error_code")
        return {
            "backend_type": "huaxin",
            "component": "xmd_l1",
            "ready": ready,
            "state": state,
            "reason": reason,
            "reconnect_count": self._reconnect_count,
            "source": HUAXIN_XMD_SOURCE,
            "xmd_l1": health,
            "actions": {
                "data.snapshot": {"status": action_state, "reason": reason},
                "data.live_current": {"status": action_state, "reason": reason},
                "data.current_tick": {"status": action_state, "reason": reason},
                "data.history": {
                    "status": "unsupported",
                    "reason": "Huaxin XMD 不提供历史行情",
                },
            },
        }

    async def _read_snapshot(self, security: str) -> Dict[str, Any]:
        """确认订阅后读取并二次校验一条新鲜快照。

        Args:
            security: 标准证券代码。

        Returns:
            Dict[str, Any]: 通过二次校验的快照副本。

        Raises:
            XmdBackendError: adapter 未启动、订阅或快照校验失败时抛出。
        """

        self._require_ready()
        canonical = str(security or "").strip().upper()
        if not canonical:
            raise ValueError("缺少 security")
        try:
            await self._run(self._backend.subscribe, canonical)
            self._subscriptions.add(canonical)
            snapshot = await self._run(
                self._backend.get_latest,
                canonical,
                self._snapshot_timeout,
            )
            return self._validate_snapshot(canonical, snapshot)
        except XmdBackendError as exc:
            if exc.code in _RECOVERABLE_XMD_RUNTIME_CODES:
                self._state = "degraded"
                self._last_error_reason = exc.code
            raise

    def _validate_snapshot(
        self,
        security: str,
        snapshot: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """在 server adapter 边界再次验证来源、时效、证券和一档盘口。

        Args:
            security: 请求的标准证券代码。
            snapshot: backend 返回的候选快照。

        Returns:
            Dict[str, Any]: 重新计算 age_seconds 的快照副本。

        Raises:
            XmdBackendError: 任一身份、时间或价格条件不满足时抛出。
        """

        value = dict(snapshot or {})
        if value.get("source") != HUAXIN_XMD_SOURCE:
            raise XmdBackendError("snapshot_source_invalid", "实时快照不是华鑫 XMD L1 来源")
        if str(value.get("security") or "").strip().upper() != security:
            raise XmdBackendError("snapshot_security_mismatch", "实时快照证券与请求不一致")
        try:
            source_time = datetime.fromisoformat(str(value["source_time"]))
            received_time = datetime.fromisoformat(str(value["received_time"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise XmdBackendError("snapshot_time_invalid", "实时快照缺少有效来源时间") from exc
        if source_time.tzinfo is None or received_time.tzinfo is None:
            raise XmdBackendError("snapshot_time_invalid", "实时快照时间必须包含时区")
        now = time.time()
        if source_time.timestamp() - now > 1.0 or received_time.timestamp() - now > 1.0:
            raise XmdBackendError("snapshot_time_in_future", "实时快照时间明显晚于本机时间")
        age_seconds = max(
            0.0,
            now - source_time.timestamp(),
            now - received_time.timestamp(),
        )
        if age_seconds > self._max_age_seconds:
            raise XmdBackendError("snapshot_stale", "华鑫 XMD 快照超过允许时效")
        try:
            last_price = float(value["last_price"])
            bid_price1 = float(value["bid_price1"])
            ask_price1 = float(value["ask_price1"])
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise XmdBackendError("snapshot_price_invalid", "实时快照缺少有效一档价格") from exc
        prices = (last_price, bid_price1, ask_price1)
        if any(not math.isfinite(item) or item <= 0 for item in prices):
            raise XmdBackendError("snapshot_price_invalid", "实时快照一档价格必须为正有限数")
        if bid_price1 > ask_price1:
            raise XmdBackendError("snapshot_spread_invalid", "实时快照买一价不能高于卖一价")
        value["age_seconds"] = age_seconds
        return value

    @staticmethod
    def _security_from_payload(payload: Mapping[str, Any]) -> str:
        """从远程 data 请求读取标准证券代码。

        Args:
            payload: 远程请求对象。

        Returns:
            str: 去空白并大写的证券代码。

        Raises:
            ValueError: 未提供 security/stock/stockcode 时抛出。
        """

        security = payload.get("security") or payload.get("stock") or payload.get("stockcode")
        text = str(security or "").strip().upper()
        if not text:
            raise ValueError("缺少 security")
        return text

    def _build_backend(self) -> XmdBackend:
        """按当前 ServerConfig 创建一个尚未启动的 XMD backend。

        Returns:
            XmdBackend: 使用当前生产或仿真 env 前置的新 backend。

        Side Effects:
            只构造 Python 对象；不创建 sidecar、不连接网络。
        """

        return self._backend_factory(
            python_path=str(self.config.huaxin_xmd_python or ""),
            sdk_dir=str(self.config.huaxin_xmd_sdk_dir or ""),
            front=str(self.config.huaxin_xmd_front or ""),
            max_age_seconds=self._max_age_seconds,
            connect_timeout=float(self.config.huaxin_xmd_connect_timeout),
            command_timeout=float(self.config.huaxin_xmd_command_timeout),
        )

    def _require_ready(self) -> None:
        """在行情业务调用进入 executor 前执行快速 readiness 门禁。

        Returns:
            None。

        Raises:
            XmdBackendError: adapter 尚未启动、正在恢复或已经停止时抛出。
        """

        if not self._started:
            raise XmdBackendError("xmd_not_started", "华鑫 XMD data adapter 尚未启动")
        if self._stopped or self._state == "stopped":
            raise XmdBackendError("xmd_stopped", "华鑫 XMD data adapter 已停止")
        if self._state != "ready":
            raise XmdBackendError("xmd_reconnecting", "华鑫 XMD 正在恢复连接，请稍后重试")

    def _backend_is_ready_sync(self) -> bool:
        """在 XMD executor 中读取当前 backend transport/login health。

        Returns:
            bool: sidecar 存活、登录成功且未 degraded 时为 True。
        """

        return bool(dict(self._backend.health() or {}).get("ready"))

    def _verify_subscriptions_sync(self) -> None:
        """在 XMD executor 中恢复订阅并验证每个目标已有新鲜快照。

        Returns:
            None。

        Raises:
            XmdBackendError: 订阅、来源、证券、盘口或 freshness 未通过时抛出。

        Side Effects:
            只调用 subscribe/get_latest，不会触碰 Trader 或任何交易写接口。
        """

        for security in sorted(self._subscriptions):
            self._backend.subscribe(security)
            snapshot = self._backend.get_latest(security, self._snapshot_timeout)
            self._validate_snapshot(security, snapshot)

    def _rebuild_backend_sync(self) -> None:
        """在 XMD executor 中停止旧 sidecar 并创建、启动、恢复新 backend。

        Returns:
            None。

        Raises:
            XmdBackendError: 新 sidecar 启动、订阅或新鲜快照验证失败时抛出。

        Side Effects:
            最佳努力停止旧 sidecar，只创建一套新的只读 XMD 会话。
        """

        try:
            self._backend.stop()
        except Exception:
            pass
        self._backend = self._build_backend()
        self._backend.start()
        self._verify_subscriptions_sync()

    async def _watchdog_loop(self) -> None:
        """持续观察 XMD health，并在断线后重建 sidecar 和恢复订阅。

        Returns:
            None。

        Side Effects:
            仅操作只读 XMD backend；不存在订单、撤单或跨 provider 回退。
        """

        assert self._watchdog_stop is not None
        while not self._stopped and not self._watchdog_stop.is_set():
            try:
                transport_ready = bool(await self._run(self._backend_is_ready_sync))
                if transport_ready:
                    try:
                        await self._run(self._verify_subscriptions_sync)
                    except XmdBackendError as exc:
                        self._state = "reconnecting"
                        self._last_error_reason = exc.code
                    else:
                        if self._state != "ready":
                            log.info("华鑫 XMD 会话、订阅与新鲜快照已恢复")
                        self._state = "ready"
                        self._last_error_reason = None
                else:
                    self._state = "reconnecting"
                    try:
                        await self._run(self._rebuild_backend_sync)
                    except asyncio.CancelledError:
                        raise
                    except XmdBackendError as exc:
                        self._state = "degraded"
                        self._last_error_reason = exc.code
                        log.warning("华鑫 XMD 本轮重连未就绪: %s", exc)
                    except Exception as exc:
                        self._state = "degraded"
                        self._last_error_reason = type(exc).__name__
                        log.warning("华鑫 XMD 本轮重连异常: %s", exc)
                    else:
                        self._reconnect_count += 1
                        self._state = "ready"
                        self._last_error_reason = None
                        log.info("华鑫 XMD sidecar 已重建并恢复订阅")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._state = "degraded"
                self._last_error_reason = type(exc).__name__
                log.warning("华鑫 XMD watchdog 检查失败: %s", exc)
            try:
                await asyncio.wait_for(
                    self._watchdog_stop.wait(),
                    timeout=max(0.01, float(self._watchdog_interval_seconds)),
                )
            except asyncio.TimeoutError:
                pass

    async def _run(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """在 XMD 专用单线程 executor 中执行同步 backend 调用。

        Args:
            func: 同步 backend 函数。
            *args: 位置参数。
            **kwargs: 关键字参数。

        Returns:
            Any: backend 调用结果。
        """

        loop = asyncio.get_running_loop()
        call = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(self._executor, call)


def build_huaxin_bundle(config: ServerConfig, router: AccountRouter) -> AdapterBundle:
    """校验安全边界并按显式开关构造 Trader/XMD adapter bundle。

    Args:
        config: 通用远程服务配置。
        router: 父账户路由器。

    Returns:
        AdapterBundle: Trader 与 XMD 分别按 enable_broker/enable_data 创建。
    """

    broker_config = _load_huaxin_broker_config() if config.enable_broker else {}
    _validate_huaxin_server_config(config, broker_config)
    data_adapter = HuaxinDataAdapter(config) if config.enable_data else None
    broker_adapter = (
        HuaxinBrokerAdapter(config, router, broker_config=broker_config)
        if config.enable_broker
        else None
    )
    return AdapterBundle(
        data_adapter=data_adapter,
        broker_adapter=broker_adapter,
        broker_writes_require_persistent_idempotency=False,
        broker_writes_require_idempotency_key=True,
    )


register_adapter("huaxin", build_huaxin_bundle, aliases=("huaxin-tora",))


__all__ = ["HuaxinBrokerAdapter", "HuaxinDataAdapter", "build_huaxin_bundle"]
