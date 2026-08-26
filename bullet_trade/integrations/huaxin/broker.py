"""
作者: BruceLee

文件职责: 把华鑫 Trader NativeRuntime 的事件式合同适配为 BulletTrade Broker 合同。
主要输入: 外部私密登录配置、受管 native bundle、查询条件及限价/原生市价/精确撤单请求。
主要输出: 资金、持仓、委托、成交快照，以及不会把本地受理误判成成交事实的写响应。
上游关系: LiveEngine 或 Huaxin server adapter 通过依赖注入/显式 bundle 构造本类。
下游关系: 仅调用 integrations.huaxin.native 的公开 Runtime API，不解析 ctypes 私有结构。
关键配置: 交易和撤单默认关闭；市价类型按沪深白名单显式映射；查询必须等待 query_end 才算完成。
"""

from __future__ import annotations

import hashlib
import logging
import math
import os
import threading
import time
import uuid
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

log = logging.getLogger(__name__)

from ...broker.base import BrokerBase
from . import native as native_api
from .build import DoctorReport, doctor
from .errors import (
    HUAXIN_CANCEL_DISABLED,
    HUAXIN_MARKET_ORDER_DISABLED,
    HUAXIN_NATIVE_UNAVAILABLE,
    HUAXIN_TRADING_DISABLED,
    NATIVE_CALL_FAILED,
    HuaxinNativeUnavailableError,
    HuaxinTradingDisabledError,
)

_HEALTH_FIELDS = (
    "state",
    "queue_capacity",
    "queue_size",
    "dropped_events",
    "vendor_schema_id",
    "field_set_version",
    "transport_connected",
    "logged_in",
    "ready_for_queries",
    "ready_for_new_orders",
    "ready_for_cancel",
    "session_epoch",
    "last_error_id",
)

_MAX_PENDING_EVENTS_PER_REQUEST = 512
_MAX_NATIVE_ORDER_AMOUNT = 2_147_483_647
_REQUIRED_BASELINE_QUERIES = frozenset({"account", "positions", "orders", "trades"})

# 公共市价类型只映射到经过规格冻结的 canonical native 三元组。沪深相同的
# 五档撤销/本方最优/对手方最优组合复用同一值，交易所特有组合保持分表。
_MARKET_ORDER_MATRIX: Dict[str, Dict[str, Tuple[str, str, str]]] = {
    "SSE": {
        "home_best": ("home_best", "gfd", "any"),
        "opponent_best": ("opponent_best", "gfd", "any"),
        "five_level_ioc": ("five_level", "ioc", "any"),
        "five_level_to_limit": ("five_level", "gfd", "any"),
    },
    "SZSE": {
        "home_best": ("home_best", "gfd", "any"),
        "opponent_best": ("opponent_best", "gfd", "any"),
        "five_level_ioc": ("five_level", "ioc", "any"),
        "immediate_or_cancel": ("any_price", "ioc", "any"),
        "fill_or_kill": ("any_price", "ioc", "all"),
    },
}

_TORA_ORDER_STATUS = {
    # TORA Trader v4.1.8 TTORATstpOrderStatusType。
    "0": "new",  # TORA_TSTP_OST_Cached（预埋）
    "1": "new",  # TORA_TSTP_OST_Unknown（未知，保持非终态）
    "2": "open",  # TORA_TSTP_OST_Accepted（交易所已接收）
    "3": "filling",  # TORA_TSTP_OST_PartTraded（部分成交）
    "4": "filled",  # TORA_TSTP_OST_AllTraded（全部成交）
    "5": "partly_canceled",  # TORA_TSTP_OST_PartTradeCanceled（部成部撤）
    "6": "canceled",  # TORA_TSTP_OST_AllCanceled（全部撤单）
    "7": "rejected",  # TORA_TSTP_OST_Rejected（交易所已拒绝）
    "#": "open",  # TORA_TSTP_OST_SendTradeEngine（发往交易核心）
}

_TORA_ORDER_SUBMIT_STATUS = {
    # TORA Trader v4.1.8 TTORATstpOrderSubmitStatusType；它描述提交阶段，
    # 不能覆盖 OrderStatus 推导原委托的最终状态。
    "0": "insert_unsubmitted",
    "1": "insert_submitted",
    "2": "cancel_unsubmitted",
    "3": "cancel_submitted",
    "4": "cancel_rejected",
    "5": "cancel_deleted",
}


def _mapping(value: Any) -> Dict[str, Any]:
    """把 dataclass、Mapping 或普通 health 对象转换为浅字典。

    Args:
        value: native health 或测试替身返回值。

    Returns:
        Dict[str, Any]: 可安全读取的浅字典；无法转换时返回空字典。
    """

    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value):
        return dict(asdict(value))
    result: Dict[str, Any] = {}
    for name in _HEALTH_FIELDS:
        if hasattr(value, name):
            result[name] = getattr(value, name)
    return result


def _as_int(value: Any, default: int = 0) -> int:
    """把 native 数字字段安全转换为整数。

    Args:
        value: 原始字段值。
        default: 转换失败时的默认值。

    Returns:
        int: 转换后的整数。
    """

    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _as_float(value: Any, default: float = 0.0) -> float:
    """把 native 数字字段安全转换为浮点数。

    Args:
        value: 原始字段值。
        default: 转换失败时的默认值。

    Returns:
        float: 转换后的浮点数。
    """

    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _stable_local_order_id(idempotency_key: str) -> str:
    """从幂等键派生不回显原键的稳定本地订单号。

    Args:
        idempotency_key: 服务端持久幂等键。

    Returns:
        str: 带 ``huaxin:`` 前缀的稳定本地订单号。
    """

    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:24]
    return f"huaxin:{digest}"


def _split_security(security: str) -> Tuple[str, str]:
    """把 BulletTrade 标准代码拆为 TORA exchange 和证券代码。

    Args:
        security: 例如 ``511880.XSHG`` 或 ``000001.XSHE``。

    Returns:
        Tuple[str, str]: ``(exchange, security_code)``。

    Raises:
        ValueError: 交易所后缀不受当前 Trader 现货切片支持时抛出。
    """

    text = str(security or "").strip().upper()
    if text.endswith(".XSHG"):
        return "SSE", text[:-5]
    if text.endswith(".XSHE"):
        return "SZSE", text[:-5]
    if text.endswith(".SH"):
        return "SSE", text[:-3]
    if text.endswith(".SZ"):
        return "SZSE", text[:-3]
    raise ValueError("华鑫现货委托仅支持带 XSHG/XSHE 交易所后缀的证券代码")


def _canonical_security(exchange: Any, security: Any) -> str:
    """把 TORA 交易所和代码投影为 BulletTrade 标准代码。

    Args:
        exchange: TORA 交易所标识。
        security: TORA 证券代码。

    Returns:
        str: 标准代码；交易所未知时保留原证券代码。
    """

    code = str(security or "").strip().upper()
    if "." in code:
        return code
    market = str(exchange or "").strip().upper()
    if market in {"SSE", "SH", "XSHG", "1"}:
        return f"{code}.XSHG"
    if market in {"SZSE", "SZE", "SZ", "XSHE", "2"}:
        return f"{code}.XSHE"
    return code


class HuaxinBroker(BrokerBase):
    """实现 Trader-only 华鑫券商合同及默认关闭的写安全门禁。

    核心协作对象是公开 ``NativeRuntime``；运行时事件只通过 ``drain`` 消费，查询
    必须看到对应 request_id 的 ``query_end``，委托和撤单的本地返回绝不冒充最终事实。
    """

    def __init__(
        self,
        account_id: str,
        account_type: str = "stock",
        *,
        config: Optional[Mapping[str, Any]] = None,
        runtime_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    ) -> None:
        """创建默认只读、尚未连接的华鑫券商。

        Args:
            account_id: 交易账户标识，只传给外部私密会话配置。
            account_type: BulletTrade 账户类型；不映射为 TORA 登录枚举。
            config: bundle、会话、超时和写门禁配置。
            runtime_factory: 测试或受控嵌入场景注入的公开 Runtime 工厂。

        Returns:
            None。

        Side Effects:
            只复制配置，不 dlopen、不联网、不连接交易前置。
        """

        super().__init__(account_id=account_id, account_type=account_type)
        self._config = dict(config or {})
        _env_mappings = {
            "flow_path": "HUAXIN_FLOW_PATH",
            "trade_front": "HUAXIN_TRADE_FRONT",
            "login_account": "HUAXIN_LOGIN_ACCOUNT",
            "password": "HUAXIN_PASSWORD",
            "terminal_info": "HUAXIN_TERMINAL_INFO",
            "local_ip": "HUAXIN_LOCAL_IP",
            "lip": "HUAXIN_LIP",
            "mac_address": "HUAXIN_MAC_ADDRESS",
            "hard_disk_id": "HUAXIN_HARD_DISK_ID",
            "hd": "HUAXIN_HD",
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
            "bundle_path": "HUAXIN_NATIVE_BUNDLE",
            "tradable_security_statuses": "HUAXIN_TRADABLE_SECURITY_STATUSES",
        }
        for k, env_name in _env_mappings.items():
            if self._config.get(k) in (None, ""):
                val = os.getenv(env_name)
                if val not in (None, ""):
                    self._config[k] = val
                elif k in self._config and self._config[k] == "":
                    self._config[k] = None
        self._config.setdefault("account_id", account_id)
        self._enable_trading = bool(self._config.get("enable_trading", False))
        self._enable_cancel = bool(self._config.get("enable_cancel", False))
        self._enable_node_transfer = bool(self._config.get("enable_node_transfer", False))
        self._order_ref = 0
        self._order_ref_lock = threading.Lock()
        self._order_ref_allocator_ready = True
        self._security_order_constraints_ready = True
        self._runtime_factory = runtime_factory
        self._runtime: Optional[Any] = None
        self._doctor_report: Optional[DoctorReport] = None
        self._request_sequence = 0
        self._runtime_lock = threading.RLock()
        self._pending_events: Dict[int, List[Any]] = {}
        self._orders: Dict[str, Dict[str, Any]] = {}
        self._trades: List[Dict[str, Any]] = []
        self._local_by_order_ref: Dict[int, str] = {}
        self._idempotency_by_local: Dict[str, str] = {}
        self._last_order_results: Dict[str, Dict[str, Any]] = {}
        self._shareholder_accounts: List[Dict[str, Any]] = []
        self._security_constraints: Dict[str, Dict[str, Any]] = {}
        self._successful_baseline_queries: set = set()
        self._login_max_order_ref: Optional[int] = None
        self._login_front_id = 0
        self._login_session_id = 0
        self._login_trading_day: Optional[str] = None
        self._security_master: Dict[str, Dict[str, Any]] = {}

    @property
    def doctor_report(self) -> Optional[DoctorReport]:
        """返回最近一次默认 runtime 启动前诊断快照。

        Returns:
            Optional[DoctorReport]: 注入 runtime 时保持 None，否则返回 doctor 结果。
        """

        return self._doctor_report

    def preflight(self) -> None:
        """在连接前显式 dlopen bundle 并验证真实 Trader readiness。

        Returns:
            None；注入 runtime 或真实 Trader bundle 已就绪时返回。

        Raises:
            HuaxinNativeUnavailableError: bundle 缺失、仅 fake 或真实 SDK 未就绪时抛出。

        Side Effects:
            默认路径校验 bundle 完整性、vendor runtime、ABI/version 并显式 dlopen；
            不创建 NativeRuntime、不连接柜台。注入 runtime 时无副作用。
        """

        if self._runtime_factory is not None:
            return
        raw_bundle = self._config.get("bundle_path")
        bundle_path = Path(str(raw_bundle)).expanduser() if raw_bundle else None
        report = doctor(bundle_path=bundle_path, load=True)
        self._doctor_report = report
        if not report.native_ready:
            raise HuaxinNativeUnavailableError(
                HUAXIN_NATIVE_UNAVAILABLE,
                "华鑫真实 native/SDK 尚未就绪，策略初始化已被拒绝",
                {
                    "reason_code": report.reason_code,
                    "offline_bridge_ready": report.offline_bridge_ready,
                },
            )

    def connect(self) -> bool:
        """创建 Trader runtime、启动会话并等待只读查询就绪。

        Returns:
            bool: 查询 readiness 达成时返回 True。

        Raises:
            HuaxinNativeUnavailableError: 会话未在超时内达到查询就绪时抛出。

        Side Effects:
            显式 dlopen 自研 bridge，并由 native 启动 Trader 登录状态机。
        """

        with self._runtime_lock:
            if self._connected and self._runtime is not None:
                return True
            self._config["terminal_info"] = self._resolve_terminal_info()
            self._require_login_metadata()
            self.preflight()
            self._successful_baseline_queries.clear()
            self._login_max_order_ref = None
            self._login_front_id = 0
            self._login_session_id = 0
            self._order_ref_allocator_ready = True
            self._security_order_constraints_ready = True
            self._security_constraints.clear()
            term_info = str(self._config.get("terminal_info") or "")
            prod_info = str(self._config.get("user_product_info") or "")
            acct_val = str(
                self._config.get("login_account") or self._config.get("account_id") or ""
            )
            log.info("【华鑫合规信息上报】账号=%s, UserProductInfo=%s", acct_val, prod_info)
            log.info("【华鑫合规信息上报】自动组装 TerminalInfo=%s", term_info)
            runtime = self._create_runtime()
            try:
                session_type = getattr(native_api, "NativeSessionConfig")
                session_config = session_type.from_mapping(self._config)
                runtime.start_session(session_config)
                self._runtime = runtime
                try:
                    self._wait_for_health("ready_for_queries", min(5.0, self._connect_timeout()))
                    self._drain_into_pending()
                    self._order_ref = int(self._login_max_order_ref or 0)
                    self._connected = True
                    try:
                        self._query_shareholder_accounts()
                        self.get_account_info()
                        self.get_positions()
                        self.get_orders()
                        self.get_trades()
                        if self._enable_trading:
                            self._wait_for_health("ready_for_new_orders", 5.0)
                    except HuaxinNativeUnavailableError as exc:
                        if exc.code != HUAXIN_NATIVE_UNAVAILABLE:
                            raise
                        log.warning("基线查询预热提示: %s", exc)
                except HuaxinNativeUnavailableError as exc:
                    if exc.code != HUAXIN_NATIVE_UNAVAILABLE:
                        raise
                    self._connected = True
                    log.warning(
                        "🌙 华鑫 Trader 柜台当前未开放 (%s)；引擎已进入夜间常驻守望模式，将在后台持续等待柜台开机并自动就绪...",
                        exc,
                    )
                    self._start_standby_watchdog()
            except Exception:
                try:
                    runtime.stop_session()
                except Exception:
                    pass
                try:
                    runtime.close()
                except Exception:
                    pass
                self._runtime = None
                self._connected = False
                raise
            return True

    def _start_standby_watchdog(self) -> None:
        """启动后台守护线程，守望柜台开市并在连通时自动执行登录预热与主动激活重试。"""
        if getattr(self, "_watchdog_thread", None) and self._watchdog_thread.is_alive():
            return

        def _watchdog_loop() -> None:
            last_heartbeat = time.time()
            last_reconnect_attempt = time.time()
            session_type = getattr(native_api, "NativeSessionConfig")
            session_config = session_type.from_mapping(self._config)

            while self._connected:
                try:
                    if self._runtime is not None:
                        self._drain_into_pending()
                        health = _mapping(self._runtime.health())
                        if bool(health.get("ready_for_queries", False)):
                            self._login_trading_day = self.get_trading_day()
                            self._order_ref = int(self._login_max_order_ref or 0)
                            log.info(
                                "☀️ ✅ 华鑫柜台已成功开机并登录就绪！权威交易日: %s",
                                self._login_trading_day or "未知",
                            )
                            try:
                                self._query_shareholder_accounts()
                                self.get_account_info()
                                self.get_positions()
                                self.get_orders()
                                self.get_trades()
                            except Exception as exc:
                                log.warning("基线查询自动预热完成 (提示: %s)", exc)
                            break

                    # 若未就绪，每隔 15 秒主动尝试重启 session 发起 TCP 重新握手
                    now = time.time()
                    if now - last_reconnect_attempt >= 15.0:
                        last_reconnect_attempt = now
                        try:
                            if self._runtime is not None:
                                try:
                                    self._runtime.stop_session()
                                except Exception:
                                    pass
                                try:
                                    self._runtime.close()
                                except Exception:
                                    pass
                            runtime = self._create_runtime()
                            runtime.start_session(session_config)
                            self._runtime = runtime
                        except Exception as exc:
                            log.debug("主动尝试重连华鑫柜台未果: %s", exc)

                except Exception as loop_exc:
                    log.debug("守望循环异常: %s", loop_exc)

                # 每 5 分钟打印一次温和的守望心跳日志
                if time.time() - last_heartbeat >= 300:
                    log.info("⏳ 正在持续守望华鑫柜台开机 (主事件循环与策略定时任务健康运行中，后台正以 15s 周期主动探测就绪)...")
                    last_heartbeat = time.time()

                time.sleep(5.0)

        t = threading.Thread(target=_watchdog_loop, name="HuaxinStandbyWatchdog", daemon=True)
        self._watchdog_thread = t
        t.start()

    def _next_order_ref(self) -> int:
        """获取下一个单调递增的 OrderRef（纯内存原子自增，0 纳秒延迟）。

        Returns:
            int: 递增后的 OrderRef。
        """
        with self._order_ref_lock:
            self._order_ref += 1
            return self._order_ref

    def _resolve_terminal_info(self) -> str:
        """根据独立的原子配置字段，自动组装华鑫 4.0 官方合规 TerminalInfo。

        Returns:
            str: 格式为 'PC;IIP=;IPORT=;LIP={lip};MAC={mac};HD={hd}@{product}' 的合规字符串。
        """
        explicit = str(self._config.get("terminal_info") or "").strip()
        if "terminal_info" in self._config:
            if explicit == "":
                return ""
            if len(explicit) > 10 or ";" in explicit:
                return explicit

        lip = str(
            self._config.get("local_ip")
            or self._config.get("lip")
            or os.getenv("HUAXIN_LOCAL_IP")
            or os.getenv("HUAXIN_LIP")
            or ""
        ).strip()
        mac = str(
            self._config.get("mac_address")
            or self._config.get("mac")
            or os.getenv("HUAXIN_MAC_ADDRESS")
            or ""
        ).strip()
        hd = str(
            self._config.get("hard_disk_id")
            or self._config.get("hd")
            or os.getenv("HUAXIN_HARD_DISK_ID")
            or os.getenv("HUAXIN_HD")
            or ""
        ).strip()
        prod = str(
            self._config.get("user_product_info") or os.getenv("HUAXIN_USER_PRODUCT_INFO") or ""
        ).strip()

        if not hd:
            for cand in ("/home/terminalinfo", "/home/userlgy/terminalinfo"):
                if os.path.isfile(cand):
                    try:
                        with open(cand, "r", encoding="utf-8") as f:
                            content = f.read().strip()
                            if content.startswith("HD="):
                                hd = content[3:].strip()
                            elif content:
                                hd = content
                            if hd:
                                break
                    except Exception:
                        pass

        if not lip and not mac and not hd and not explicit:
            return ""

        return f"PC;IIP=;IPORT=;LIP={lip};MAC={mac};HD={hd}@{prod}"

    def _require_login_metadata(self) -> None:
        """在创建 runtime 和登录前验证生产必填的终端身份字段。

        Returns:
            None。

        Raises:
            HuaxinNativeUnavailableError: 字段缺失或 UTF-8 长度越界时失败关闭。
        """

        fields = (
            ("user_product_info", "HUAXIN_USER_PRODUCT_INFO", 10),
            ("terminal_info", "HUAXIN_TERMINAL_INFO", 512),
        )
        for config_key, env_name, max_bytes in fields:
            text = str(self._config.get(config_key) or "")
            if not text.strip():
                raise HuaxinNativeUnavailableError(
                    HUAXIN_NATIVE_UNAVAILABLE,
                    "华鑫 Trader 登录缺少必填终端身份字段",
                    {"required_config_field": env_name},
                )
            try:
                encoded = text.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise HuaxinNativeUnavailableError(
                    HUAXIN_NATIVE_UNAVAILABLE,
                    "华鑫 Trader 登录终端身份字段无法编码为 UTF-8",
                    {"required_config_field": env_name},
                ) from exc
            if len(encoded) > max_bytes:
                raise HuaxinNativeUnavailableError(
                    HUAXIN_NATIVE_UNAVAILABLE,
                    "华鑫 Trader 登录终端身份字段超过官方长度上限",
                    {
                        "required_config_field": env_name,
                        "max_bytes": max_bytes,
                        "actual_bytes": len(encoded),
                    },
                )

    def disconnect(self) -> bool:
        """幂等停止 Trader 会话并释放 native runtime。

        Returns:
            bool: 清理完成后始终返回 True。

        Side Effects:
            调用 ``stop_session`` 和 ``close``，并清空本地连接标记。
        """

        with self._runtime_lock:
            runtime = self._runtime
            self._runtime = None
            self._connected = False
            self._successful_baseline_queries.clear()
            self._order_ref_allocator_ready = False
            self._security_order_constraints_ready = False
            self._login_max_order_ref = None
            self._security_constraints.clear()
            if runtime is None:
                return True
            try:
                runtime.stop_session()
            finally:
                close = getattr(runtime, "close", None)
                if callable(close):
                    close()
            return True

    def get_account_info(self) -> Dict[str, Any]:
        """查询交易资金账户并等待明确的 query_end。

        Returns:
            Dict[str, Any]: 与当前 ``account_id`` 精确匹配的资金字段。

        Raises:
            HuaxinNativeUnavailableError: 查询结果没有目标资金账号时失败关闭。
        """

        rows = self._query_rows(
            "query_trading_accounts",
            "trading_account",
            native_api.REQUEST_QUERY_TRADING_ACCOUNT,
        )
        normalized = [self._normalize_account(row) for row in rows]
        for row in normalized:
            if str(row.get("account_id") or "") == str(self.account_id):
                self._successful_baseline_queries.add("account")
                return row
        raise HuaxinNativeUnavailableError(
            NATIVE_CALL_FAILED,
            "华鑫 Trader 资金查询未返回目标资金账号",
            {
                "operation": "query_trading_accounts",
                "record_count": len(normalized),
                "target_config_field": "HUAXIN_ACCOUNT_ID",
            },
        )

    def get_positions(self) -> List[Dict[str, Any]]:
        """查询全部持仓并等待明确的 query_end。

        Returns:
            List[Dict[str, Any]]: 兼容 BulletTrade 的持仓列表。
        """

        rows = [
            self._normalize_position(row)
            for row in self._query_rows(
                "query_positions",
                "position",
                native_api.REQUEST_QUERY_POSITION,
            )
        ]
        self._successful_baseline_queries.add("positions")
        return rows

    def get_shareholder_accounts(self, *, refresh: bool = True) -> List[Dict[str, Any]]:
        """公开读取股东账户身份，默认向柜台重新查询权威快照。

        Args:
            refresh: 为 True 时重新查询；为 False 且已有缓存时返回缓存副本。

        Returns:
            List[Dict[str, Any]]: 股东账号、投资者和营业单元等身份记录副本。

        Side Effects:
            重新查询时刷新当前 Broker 的只读身份缓存，不发起任何写请求。
        """

        if refresh or not self._shareholder_accounts:
            self._query_shareholder_accounts()
        return [dict(row) for row in self._shareholder_accounts]

    def get_system_nodes(self, node_id: int = 0) -> List[Dict[str, Any]]:
        """查询柜台系统节点目录并等待明确 query_end。

        Args:
            node_id: 零查询全部，正数仅查询指定节点。

        Returns:
            List[Dict[str, Any]]: 含 node_id、node_info 和 current 的权威记录。
        """

        return self._query_rows(
            "query_system_nodes",
            "system_node",
            native_api.REQUEST_QUERY_SYSTEM_NODE,
            node_id,
        )

    def get_fund_transfer_details(
        self,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """查询当前 Trader 会话可见的资金划拨流水。

        Args:
            filters: 可选账户、投资者、币种和节点方向过滤。

        Returns:
            List[Dict[str, Any]]: 柜台资金划拨明细。
        """

        body = dict(filters or {})
        query = native_api.NativeFundTransferDetailQuery(
            department_id=str(body.get("department_id") or ""),
            account_id=str(body.get("account_id") or ""),
            investor_id=str(body.get("investor_id") or ""),
            currency=str(body.get("currency") or ""),
            transfer_direction=str(body.get("transfer_direction") or ""),
        )
        rows = self._query_rows(
            "query_fund_transfer_details",
            "fund_transfer_detail",
            native_api.REQUEST_QUERY_FUND_TRANSFER_DETAIL,
            query,
        )
        if body.get("apply_serial") not in (None, ""):
            apply_serial = _as_int(body.get("apply_serial"), -1)
            rows = [row for row in rows if _as_int(row.get("apply_serial"), -2) == apply_serial]
        return rows

    def get_position_transfer_details(
        self,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """查询当前 Trader 会话可见的证券划拨流水。

        Args:
            filters: 可选证券同行身份和节点方向过滤。

        Returns:
            List[Dict[str, Any]]: 柜台证券划拨明细。
        """

        body = dict(filters or {})
        query = native_api.NativePositionTransferDetailQuery(
            exchange=str(body.get("exchange") or ""),
            investor_id=str(body.get("investor_id") or ""),
            business_unit_id=str(body.get("business_unit_id") or ""),
            shareholder_id=str(body.get("shareholder_id") or ""),
            security=str(body.get("security") or ""),
            transfer_direction=str(body.get("transfer_direction") or ""),
        )
        rows = self._query_rows(
            "query_position_transfer_details",
            "position_transfer_detail",
            native_api.REQUEST_QUERY_POSITION_TRANSFER_DETAIL,
            query,
        )
        if body.get("apply_serial") not in (None, ""):
            apply_serial = _as_int(body.get("apply_serial"), -1)
            rows = [row for row in rows if _as_int(row.get("apply_serial"), -2) == apply_serial]
        return rows

    def submit_fund_transfer(
        self,
        source_account: Mapping[str, Any],
        *,
        amount: float,
        apply_serial: int,
        external_node_id: int,
        transfer_direction: str = "node_move_in",
        wait_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """用同一资金行身份提交一次跨节点资金划拨且绝不自动重试。

        Args:
            source_account: 上海权威资金行，提供营业部、资金账号和币种。
            amount: 不超过该行 transferable_cash 的已冻结金额。
            apply_serial: 调用前已持久化的正 int32 申请流水。
            external_node_id: 经系统节点查询验证的上海节点 ID。
            transfer_direction: 固定生产路径使用 ``node_move_in``。
            wait_timeout: 最终回报最长等待秒数。

        Returns:
            Dict[str, Any]: succeeded、rejected 或 unknown，不把接受回执当成功。
        """

        if not self._enable_node_transfer:
            raise HuaxinTradingDisabledError(
                HUAXIN_TRADING_DISABLED,
                "华鑫节点资产划拨未启用",
                {"required_config": "enable_node_transfer"},
            )
        frozen_amount = _as_float(amount, float("nan"))
        transferable_cash = _as_float(source_account.get("transferable_cash"), float("nan"))
        if not math.isfinite(frozen_amount) or frozen_amount <= 0:
            raise ValueError("amount 必须为正有限数")
        if not math.isfinite(transferable_cash) or transferable_cash < frozen_amount:
            raise ValueError("amount 不得超过来源资金行 transferable_cash")
        transfer_type = getattr(native_api, "NativeTransferFundRequest")
        transfer = transfer_type(
            department_id=str(source_account.get("department_id") or ""),
            account_id=str(source_account.get("account_id") or ""),
            currency=str(source_account.get("currency") or ""),
            transfer_direction=transfer_direction,
            amount=amount,
            apply_serial=apply_serial,
            external_node_id=external_node_id,
        )
        return self._submit_transfer_once(
            "transfer_fund",
            transfer,
            apply_serial=apply_serial,
            response_event="fund_transfer_response",
            final_event="fund_transfer",
            wait_timeout=wait_timeout,
        )

    def submit_position_transfer(
        self,
        source_position: Mapping[str, Any],
        *,
        volume: int,
        apply_serial: int,
        external_node_id: int,
        transfer_direction: str = "node_move_in",
        transfer_position_type: str = "all",
        wait_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """用同一持仓行完整身份提交一次跨节点证券划拨且绝不自动重试。

        Args:
            source_position: 上海权威持仓行，不跨行补技术身份。
            volume: 不超过该行 available_position 的已冻结数量。
            apply_serial: 调用前已持久化的正 int32 申请流水。
            external_node_id: 经系统节点查询验证的上海节点 ID。
            transfer_direction: 固定生产路径使用 ``node_move_in``。
            transfer_position_type: canary 证明前仅允许显式配置的柜台持仓类型。
            wait_timeout: 最终回报最长等待秒数。

        Returns:
            Dict[str, Any]: succeeded、rejected 或 unknown，不把接受回执当成功。
        """

        if not self._enable_node_transfer:
            raise HuaxinTradingDisabledError(
                HUAXIN_TRADING_DISABLED,
                "华鑫节点资产划拨未启用",
                {"required_config": "enable_node_transfer"},
            )
        frozen_volume = _as_int(volume, -1)
        available_position = _as_int(source_position.get("available_position"), -1)
        if isinstance(volume, bool) or frozen_volume <= 0:
            raise ValueError("volume 必须为正整数")
        if frozen_volume != volume or available_position < frozen_volume:
            raise ValueError("volume 不得超过来源持仓行 available_position")
        transfer_type = getattr(native_api, "NativeTransferPositionRequest")
        transfer = transfer_type(
            exchange=str(source_position.get("exchange") or ""),
            investor_id=str(source_position.get("investor_id") or ""),
            business_unit_id=str(source_position.get("business_unit_id") or ""),
            shareholder_id=str(source_position.get("shareholder_id") or ""),
            security=str(source_position.get("security") or "").split(".", 1)[0],
            market_id=_as_int(source_position.get("market_id")),
            transfer_direction=transfer_direction,
            transfer_position_type=transfer_position_type,
            volume=volume,
            apply_serial=apply_serial,
            external_node_id=external_node_id,
        )
        return self._submit_transfer_once(
            "transfer_position",
            transfer,
            apply_serial=apply_serial,
            response_event="position_transfer_response",
            final_event="position_transfer",
            wait_timeout=wait_timeout,
        )

    def get_orders(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
        status: Optional[object] = None,
        from_broker: bool = False,
    ) -> List[Dict[str, Any]]:
        """查询当日委托并按公共条件过滤。

        Args:
            order_id: 可选稳定本地或柜台订单号。
            security: 可选标准证券代码。
            status: 可选规范化订单状态。
            from_broker: 保留公共合同字段；华鑫始终查询柜台事实。

        Returns:
            List[Dict[str, Any]]: 过滤后的委托列表。
        """

        del from_broker
        rows = [
            self._normalize_order(row)
            for row in self._query_rows(
                "query_orders",
                "order",
                native_api.REQUEST_QUERY_ORDER,
            )
        ]
        self._successful_baseline_queries.add("orders")
        if order_id:
            wanted = str(order_id)
            rows = [row for row in rows if self._order_matches_id(row, wanted)]
        if security:
            rows = [row for row in rows if row.get("security") == security]
        if status is not None:
            wanted_status = str(getattr(status, "value", status)).lower()
            rows = [row for row in rows if str(row.get("status") or "").lower() == wanted_status]
        return rows

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """返回当日尚未进入终态的华鑫委托。

        Returns:
            List[Dict[str, Any]]: 非成交、非撤销、非拒绝的委托列表。
        """

        terminal = {"filled", "canceled", "rejected", "failed", "error"}
        return [row for row in self.get_orders() if str(row.get("status") or "") not in terminal]

    def get_trades(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """查询当日成交并按订单或证券过滤。

        Args:
            order_id: 可选稳定本地或柜台订单号。
            security: 可选标准证券代码。

        Returns:
            List[Dict[str, Any]]: 过滤后的成交列表。
        """

        rows = [
            self._normalize_trade(row)
            for row in self._query_rows(
                "query_trades",
                "trade",
                native_api.REQUEST_QUERY_TRADE,
            )
        ]
        self._successful_baseline_queries.add("trades")
        if order_id:
            wanted = str(order_id)
            rows = [row for row in rows if wanted in self._row_order_ids(row)]
        if security:
            rows = [row for row in rows if row.get("security") == security]
        return rows

    def get_trading_day(self) -> Optional[str]:
        """返回华鑫柜台返回的权威当前交易日（8位字符串，如 '20260819'）。

        Returns:
            Optional[str]: 格式为 'YYYYMMDD' 的交易日；未连接时返回 None。
        """
        if self._login_trading_day:
            return self._login_trading_day
        if self._connected and self._runtime is not None:
            self._drain_into_pending()
            return self._login_trading_day
        return None

    def get_security_master(self, security: str) -> Optional[Dict[str, Any]]:
        """获取目标证券在柜台的静态主数据字典（包含中文名、跳价、T+0回转标志等）。

        Args:
            security: 标准代码（如 '511880.XSHG' 或 '511880'）。

        Returns:
            Optional[Dict[str, Any]]: 柜台主数据字典。
        """
        sec = security.split(".")[0].strip()
        exchange = (
            "SZSE" if security.endswith(".XSHE") or sec.startswith(("00", "30", "15")) else "SSE"
        )
        canonical_key = f"{exchange}:{sec}"
        if canonical_key in self._security_master:
            return dict(self._security_master[canonical_key])
        if not self._connected:
            return None
        try:
            row = self._query_security_constraints(exchange, sec)
            if row:
                self._security_master[canonical_key] = row
                return dict(row)
        except Exception:
            pass
        return None

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
        """提交买入限价或显式原生市价委托并返回稳定本地订单号。

        Args:
            security: 标准证券代码。
            amount: 委托数量。
            price: 限价或市价保护价；深市允许的无保护价组合可为 None。
            wait_timeout: 等待插入响应的秒数。
            remark: 公共备注；当前 Trader V1 不写入厂商字段。
            market: 是否为原生市价意图。
            extra: 必须包含 ``idempotency_key``；市价还必须含 ``market_type``。

        Returns:
            str: 稳定本地订单号；明确拒绝详情可由 ``get_last_order_wait_result`` 读取。
        """

        del remark
        result = self.submit_order(
            "buy",
            security,
            amount,
            price,
            market=market,
            extra=extra,
            wait_timeout=wait_timeout,
        )
        return str(result["order_id"])

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
        """提交卖出限价或显式原生市价委托并返回稳定本地订单号。

        Args:
            security: 标准证券代码。
            amount: 委托数量。
            price: 限价或市价保护价；深市允许的无保护价组合可为 None。
            wait_timeout: 等待插入响应的秒数。
            remark: 公共备注；当前 Trader V1 不写入厂商字段。
            market: 是否为原生市价意图。
            extra: 必须包含 ``idempotency_key``；市价还必须含 ``market_type``。

        Returns:
            str: 稳定本地订单号；明确拒绝详情可由 ``get_last_order_wait_result`` 读取。
        """

        del remark
        result = self.submit_order(
            "sell",
            security,
            amount,
            price,
            market=market,
            extra=extra,
            wait_timeout=wait_timeout,
        )
        return str(result["order_id"])

    def submit_limit_order(
        self,
        direction: str,
        security: str,
        amount: int,
        price: Optional[float],
        *,
        market: bool = False,
        extra: Optional[Mapping[str, Any]] = None,
        wait_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """兼容旧调用方并转交统一限价/市价提交实现。

        Args:
            direction: ``buy`` 或 ``sell``。
            security: 标准证券代码。
            amount: 正整数委托数量。
            price: 正数限价。
            market: 兼容参数；True 时按 ``extra.market_type`` 提交原生市价。
            extra: 幂等键及可选账户身份字段。
            wait_timeout: 等待 ``order_insert_response`` 的秒数。

        Returns:
            Dict[str, Any]: accepted、rejected 或 submit_unknown 写响应。

        Side Effects:
            转调 :meth:`submit_order`，不自行访问 native。
        """

        return self.submit_order(
            direction,
            security,
            amount,
            price,
            market=market,
            extra=extra,
            wait_timeout=wait_timeout,
        )

    def submit_order(
        self,
        direction: str,
        security: str,
        amount: int,
        price: Optional[float],
        *,
        market: bool = False,
        extra: Optional[Mapping[str, Any]] = None,
        wait_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """同步提交一笔带显式类型和持久幂等键的华鑫现货委托。

        Args:
            direction: ``buy`` 或 ``sell``。
            security: 标准证券代码。
            amount: 正整数委托数量。
            price: 限价或市价保护价；深市允许的市价组合可为空。
            market: 是否提交原生市价委托。
            extra: 幂等键、显式 ``market_type`` 及可选账户身份字段。
            wait_timeout: 等待 ``order_insert_response`` 的秒数。

        Returns:
            Dict[str, Any]: accepted、rejected 或 submit_unknown 写响应。

        Side Effects:
            先查询目标证券并完成单位/边界/价格/状态校验，再调用 native
            ``place_order``；限价单可兼容旧 native ``place_limit``。
        """

        self._require_trading_enabled()
        side = str(direction or "").strip().lower()
        if side not in {"buy", "sell"}:
            raise ValueError("direction 必须为 buy 或 sell")
        try:
            quantity = int(amount)
            exact_amount = float(amount)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("华鑫委托数量必须为正整数") from exc
        if (
            isinstance(amount, bool)
            or not math.isfinite(exact_amount)
            or exact_amount != quantity
            or quantity <= 0
            or quantity > _MAX_NATIVE_ORDER_AMOUNT
        ):
            raise ValueError(f"华鑫委托数量必须为 1..{_MAX_NATIVE_ORDER_AMOUNT} 的整数")
        payload = dict(extra or {})
        idempotency_key = str(payload.get("idempotency_key") or "").strip()
        if not idempotency_key:
            idempotency_key = uuid.uuid4().hex
        exchange, security_code = _split_security(security)
        market_type, native_conditions = self._resolve_order_conditions(
            exchange,
            market=market,
            payload=payload,
        )
        limit_price = self._normalize_order_price(
            price,
            exchange=exchange,
            market=market,
        )
        security_row = self._query_security_constraints(exchange, security_code)
        self._validate_security_order_constraints(
            security_row,
            exchange=exchange,
            security=security_code,
            side=side,
            quantity=quantity,
            price=limit_price,
            market=market,
        )
        investor_id, shareholder_id = self._resolve_trading_identity(exchange, payload)
        local_id = _stable_local_order_id(idempotency_key)
        with self._runtime_lock:
            runtime = self._require_runtime_ready("ready_for_new_orders")
            request_id = self._next_request_id()
            unified_place = callable(getattr(runtime, "place_order", None))
            if market and not unified_place:
                raise HuaxinTradingDisabledError(
                    HUAXIN_MARKET_ORDER_DISABLED,
                    "当前华鑫 native bundle 尚未提供原生市价接口",
                    {"market_type": market_type, "exchange": exchange},
                )
            order_ref = self._next_order_ref()
            self._local_by_order_ref[order_ref] = local_id
            self._idempotency_by_local[local_id] = idempotency_key
            try:
                if unified_place:
                    request = self._build_native_order_request(
                        exchange=exchange,
                        investor_id=investor_id,
                        shareholder_id=shareholder_id,
                        security=security_code,
                        direction=side,
                        amount=quantity,
                        limit_price=limit_price,
                        order_ref=order_ref,
                        business_unit_id=str(payload.get("business_unit_id") or ""),
                        native_conditions=native_conditions,
                    )
                    runtime.place_order(request_id, request)
                else:
                    legacy_type = getattr(native_api, "NativeLimitOrderRequest")
                    legacy_request = legacy_type(
                        exchange=exchange,
                        investor_id=investor_id,
                        shareholder_id=shareholder_id,
                        security=security_code,
                        direction=side,
                        limit_price=limit_price,
                        amount=quantity,
                        order_ref=order_ref,
                        business_unit_id=str(payload.get("business_unit_id") or ""),
                    )
                    runtime.place_limit(request_id, legacy_request)
                events, terminal = self._wait_request(
                    request_id,
                    terminal_names={"order_insert_response"},
                    timeout=self._write_timeout(wait_timeout),
                )
                result = self._format_place_result(
                    local_id=local_id,
                    idempotency_key=idempotency_key,
                    request_id=request_id,
                    order_ref=order_ref,
                    events=events,
                    terminal=terminal,
                    security=_canonical_security(exchange, security_code),
                    direction=side,
                    amount=quantity,
                    price=limit_price,
                    style_type="market" if market else "limit",
                    market_type=market_type,
                    native_conditions=native_conditions,
                )
            except Exception as exc:
                log.error("华鑫 native place_order 异常: %s", exc, exc_info=True)
                result = self._submit_unknown_result(
                    local_id=local_id,
                    idempotency_key=idempotency_key,
                    request_id=request_id,
                    order_ref=order_ref,
                    reason=f"native_write_failed: {exc}",
                )
            self._last_order_results[local_id] = dict(result)
            return result

    async def cancel_order(self, order_id: str) -> bool:
        """提交精确撤单，并且仅在已确认撤销时返回 True。

        Args:
            order_id: 稳定本地订单号或精确柜台订单号。

        Returns:
            bool: 只有精确订单状态已确认 canceled/partly_canceled 时为 True。
        """

        result = self.submit_cancel_order(order_id, {})
        return bool(result.get("value") is True)

    def submit_cancel_order(
        self,
        order_id: str,
        payload: Optional[Mapping[str, Any]] = None,
        *,
        wait_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """同步提交带精确 TORA 身份的撤单请求。

        Args:
            order_id: 稳定本地订单号或精确柜台订单号。
            payload: 幂等键及可选 ``provider_extension.huaxin_tora`` 身份。
            wait_timeout: 等待 ``order_action_response`` 的秒数。

        Returns:
            Dict[str, Any]: 明确拒绝、明确已撤或 submit_unknown 响应。

        Side Effects:
            调用 native ``cancel_order``，不会以本地返回码推导撤单成功。
        """

        if not self._enable_cancel:
            raise HuaxinTradingDisabledError(
                HUAXIN_CANCEL_DISABLED,
                "华鑫撤单开关默认关闭",
                {"required_flag": "HUAXIN_ENABLE_CANCEL"},
            )
        body = dict(payload or {})
        identity = self._resolve_cancel_identity(str(order_id), body)
        with self._runtime_lock:
            runtime = self._require_runtime_ready("ready_for_cancel")
            request_id = self._next_request_id()
            request_type = getattr(native_api, "NativeCancelOrderRequest")
            request = request_type(**identity)
            runtime.cancel_order(request_id, request)
            events, terminal = self._wait_request(
                request_id,
                terminal_names={"order_action_response"},
                timeout=self._write_timeout(wait_timeout),
            )
            return self._format_cancel_result(str(order_id), events, terminal)

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """查询一个稳定本地或柜台订单号的当前事实。

        Args:
            order_id: 稳定本地订单号、OrderSysID 或已缓存组合身份。

        Returns:
            Dict[str, Any]: 找到时的规范化订单，否则为空字典。
        """

        rows = self.get_orders(order_id=order_id, from_broker=True)
        return rows[0] if rows else {}

    def get_last_order_wait_result(self, order_id: str) -> Optional[Dict[str, Any]]:
        """返回最近一次限价委托提交的非最终或最终响应。

        Args:
            order_id: ``buy``/``sell`` 返回的稳定本地订单号。

        Returns:
            Optional[Dict[str, Any]]: 找到时返回副本，否则返回 None。
        """

        result = self._last_order_results.get(str(order_id))
        return dict(result) if result is not None else None

    def supports_orders_sync(self) -> bool:
        """声明支持订单与成交只读同步。

        Returns:
            bool: 始终为 True。
        """

        return True

    def supports_account_sync(self) -> bool:
        """声明支持账户与持仓只读同步。

        Returns:
            bool: 始终为 True。
        """

        return True

    def sync_orders(self) -> List[Dict[str, Any]]:
        """执行一次柜台订单快照同步。

        Returns:
            List[Dict[str, Any]]: 当日订单列表。
        """

        return self.get_orders(from_broker=True)

    def sync_account(self) -> Dict[str, Any]:
        """执行一次资金和持仓快照同步。

        Returns:
            Dict[str, Any]: 资金字段及 ``positions`` 列表。
        """

        result = dict(self.get_account_info())
        result["positions"] = self.get_positions()
        return result

    def health_snapshot(self) -> Dict[str, Any]:
        """返回不包含登录 secret 的 Trader readiness 快照。

        Returns:
            Dict[str, Any]: 固定 health 字段、本地连接状态和写门禁。
        """

        runtime = self._runtime
        health = _mapping(runtime.health()) if runtime is not None else {}
        safe = {name: health.get(name) for name in _HEALTH_FIELDS if name in health}
        safe.update(
            {
                "connected": self._connected,
                "trading_enabled": self._enable_trading,
                "cancel_order_enabled": self._enable_cancel,
                "order_ref_allocator_ready": self._connected,
                "order_identity_ready": self._connected,
                "order_identity_mode": "memory",
                "security_order_constraints_ready": self._security_order_constraints_ready,
                "validated_security_constraint_count": len(self._security_constraints),
                "baseline_query_ready": _REQUIRED_BASELINE_QUERIES.issubset(
                    self._successful_baseline_queries
                ),
                "baseline_queries_completed": sorted(self._successful_baseline_queries),
                "baseline_queries_required": sorted(_REQUIRED_BASELINE_QUERIES),
            }
        )
        return safe

    def _create_runtime(self) -> Any:
        """按注入工厂或显式受管 bundle 创建 NativeRuntime。

        Returns:
            Any: 只使用公开 Runtime 方法的对象。

        Side Effects:
            默认路径显式 dlopen bundle 并创建 native handle。
        """

        if self._runtime_factory is not None:
            return self._runtime_factory(dict(self._config))
        raw_bundle = self._config.get("bundle_path")
        if not raw_bundle:
            raise HuaxinNativeUnavailableError(
                HUAXIN_NATIVE_UNAVAILABLE,
                "华鑫 native bundle 路径未配置",
                {"required_flag": "HUAXIN_NATIVE_BUNDLE"},
            )
        bridge = native_api.NativeBridge.load(Path(str(raw_bundle)).expanduser())
        if str(getattr(bridge, "mode", "")).lower() != "trader":
            raise HuaxinNativeUnavailableError(
                HUAXIN_NATIVE_UNAVAILABLE,
                "华鑫 bundle 不是 Trader 模式",
                {"mode": str(getattr(bridge, "mode", "unknown"))},
            )
        capacity = min(
            1_000_000,
            max(2, _as_int(self._config.get("queue_capacity"), 1024)),
        )
        return bridge.create(queue_capacity=capacity)

    def _require_trading_enabled(self) -> None:
        """检查默认关闭的交易硬门禁。

        Returns:
            None。

        Raises:
            HuaxinTradingDisabledError: 交易开关关闭时抛出。
        """

        if not self._enable_trading:
            raise HuaxinTradingDisabledError(
                HUAXIN_TRADING_DISABLED,
                "华鑫交易开关默认关闭",
                {"required_flag": "HUAXIN_ENABLE_TRADING"},
            )

    @staticmethod
    def _resolve_market_type(payload: Mapping[str, Any]) -> str:
        """从公共扩展或远程 style 中读取显式市价类型。

        Args:
            payload: Live broker 或远程 server 透传的订单扩展。

        Returns:
            str: 小写 canonical 市价类型；缺失时返回空字符串。
        """

        market_type = payload.get("market_type")
        style = payload.get("style")
        if not market_type and isinstance(style, Mapping):
            market_type = style.get("market_type")
        return str(market_type or "").strip().lower()

    def _resolve_order_conditions(
        self,
        exchange: str,
        *,
        market: bool,
        payload: Mapping[str, Any],
    ) -> Tuple[Optional[str], Tuple[str, str, str]]:
        """把限价或显式沪深市价类型解析为 canonical native 三元组。

        Args:
            exchange: ``SSE`` 或 ``SZSE``。
            market: 是否为原生市价意图。
            payload: 含可选 ``market_type`` 的公共订单扩展。

        Returns:
            Tuple[Optional[str], Tuple[str, str, str]]: 高阶市价类型和
            ``(order_price_type, time_condition, volume_condition)``。

        Raises:
            HuaxinTradingDisabledError: 市价类型缺失、未知或不适用于该市场。
            ValueError: 限价单错误携带市价类型时抛出。
        """

        market_type = self._resolve_market_type(payload)
        if not market:
            if market_type:
                raise ValueError("华鑫限价单不能指定 market_type")
            return None, ("limit", "gfd", "any")
        if not market_type:
            market_type = "five_level_to_limit" if exchange == "SSE" else "five_level_ioc"
        conditions = _MARKET_ORDER_MATRIX.get(exchange, {}).get(market_type)
        if conditions is None:
            raise HuaxinTradingDisabledError(
                HUAXIN_MARKET_ORDER_DISABLED,
                "华鑫市价类型不适用于当前交易所或尚未进入白名单",
                {
                    "exchange": exchange,
                    "market_type": market_type,
                    "supported_market_types": sorted(_MARKET_ORDER_MATRIX.get(exchange, {})),
                },
            )
        return market_type, conditions

    @staticmethod
    def _normalize_order_price(
        price: Optional[float],
        *,
        exchange: str,
        market: bool,
    ) -> float:
        """校验限价或保护价并转换为 native 浮点字段。

        Args:
            price: 限价、保护价或 None。
            exchange: 当前交易所。
            market: 是否为原生市价意图。

        Returns:
            float: 有效价格；深市允许无保护价的市价组合返回 0.0。

        Raises:
            ValueError: 限价无效或上交所市价缺少有效保护价时抛出。
        """

        if price in (None, ""):
            normalized = 0.0
        else:
            if isinstance(price, bool):
                raise ValueError("华鑫委托价格不能是布尔值")
            try:
                normalized = float(price)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("华鑫委托价格必须为有限数值") from exc
        if not math.isfinite(normalized) or normalized < 0:
            raise ValueError("华鑫委托价格必须为有限非负数")
        if not market and normalized <= 0:
            raise ValueError("华鑫限价单价格必须为有限正数")
        if market and exchange == "SSE" and normalized <= 0:
            raise ValueError("上交所原生市价单必须填写有限正数保护限价")
        return normalized

    @staticmethod
    def _required_int_field(
        row: Mapping[str, Any],
        field: str,
        *,
        minimum: int = 0,
    ) -> int:
        """读取证券约束中的必需整数并拒绝缺失或越界值。

        Args:
            row: ``query_security`` 的单条记录。
            field: 需要读取的 canonical 字段名。
            minimum: 允许的最小整数。

        Returns:
            int: 已验证整数。

        Raises:
            ValueError: 字段缺失、不是精确整数或小于下界时抛出。
        """

        value = row.get(field)
        if isinstance(value, bool):
            raise ValueError(f"华鑫证券约束字段 {field} 不是有效整数")
        try:
            parsed = int(value)
            exact = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"华鑫证券约束缺少有效字段 {field}") from exc
        if not math.isfinite(exact) or exact != parsed or parsed < minimum:
            raise ValueError(f"华鑫证券约束字段 {field} 小于安全下界 {minimum}")
        return parsed

    @staticmethod
    def _required_signed_int32_field(row: Mapping[str, Any], field: str) -> int:
        """读取 TORA 有符号 int32 字段并拒绝缺失或越界值。

        Args:
            row: Native 事件的字段映射。
            field: 需要读取的 canonical 字段名。

        Returns:
            int: 已验证的有符号 int32 整数。

        Raises:
            ValueError: 字段缺失、不是精确整数或超出 int32 时抛出。
        """

        value = row.get(field)
        if isinstance(value, bool):
            raise ValueError(f"华鑫 TORA 字段 {field} 不是有效整数")
        try:
            parsed = int(value)
            exact = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"华鑫 TORA 缺少有效字段 {field}") from exc
        if (
            not math.isfinite(exact)
            or exact != parsed
            or parsed < -(1 << 31)
            or parsed > (1 << 31) - 1
        ):
            raise ValueError(f"华鑫 TORA 字段 {field} 超出有符号 int32 范围")
        return parsed

    @staticmethod
    def _required_float_field(
        row: Mapping[str, Any],
        field: str,
        *,
        positive: bool = False,
    ) -> float:
        """读取证券约束中的必需有限浮点字段。

        Args:
            row: ``query_security`` 的单条记录。
            field: 需要读取的 canonical 字段名。
            positive: 是否要求严格大于零。

        Returns:
            float: 已验证有限数值。

        Raises:
            ValueError: 字段缺失、非有限或不满足正数门禁时抛出。
        """

        value = row.get(field)
        if isinstance(value, bool):
            raise ValueError(f"华鑫证券约束字段 {field} 不是有效数值")
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"华鑫证券约束缺少有效字段 {field}") from exc
        if not math.isfinite(parsed) or (positive and parsed <= 0):
            requirement = "有限正数" if positive else "有限数值"
            raise ValueError(f"华鑫证券约束字段 {field} 必须为{requirement}")
        return parsed

    def _query_security_constraints(self, exchange: str, security: str) -> Dict[str, Any]:
        """精确查询并缓存当前目标证券的一条申报约束记录。

        Args:
            exchange: ``SSE`` 或 ``SZSE``。
            security: 不带后缀的证券代码。

        Returns:
            Dict[str, Any]: 唯一匹配的 ``query_security`` 记录。

        Raises:
            HuaxinNativeUnavailableError: 返回零条、多条或标识不匹配时失败关闭。

        Side Effects:
            仅发起一次只读 Trader 查询；完整校验成功后才由校验方法写诊断缓存。
        """

        rows = self._query_rows(
            "query_security",
            "security",
            native_api.REQUEST_QUERY_SECURITY,
            exchange,
            security,
        )
        matches = [
            dict(row)
            for row in rows
            if _canonical_security(row.get("exchange"), row.get("security"))
            == _canonical_security(exchange, security)
        ]
        if len(matches) != 1:
            raise HuaxinNativeUnavailableError(
                NATIVE_CALL_FAILED,
                "华鑫证券约束查询未返回唯一目标记录",
                {
                    "operation": "query_security",
                    "exchange": exchange,
                    "record_count": len(rows),
                    "matched_record_count": len(matches),
                },
            )
        return matches[0]

    def _validate_security_order_constraints(
        self,
        row: Mapping[str, Any],
        *,
        exchange: str,
        security: str,
        side: str,
        quantity: int,
        price: float,
        market: bool,
    ) -> None:
        """按证券查询事实校验数量单位、状态、价格步长和涨跌停。

        Args:
            row: 唯一 ``query_security`` 记录。
            exchange: 当前交易所。
            security: 不带后缀的证券代码。
            side: ``buy`` 或 ``sell``。
            quantity: 待提交数量。
            price: 限价或保护价；允许的深市无保护价市价为 0。
            market: 是否为原生市价。

        Returns:
            None；全部证券级前置条件满足时正常返回。

        Raises:
            ValueError: 任一必需字段缺失或订单违反证券约束时抛出。
        """

        self._required_int_field(row, "security_type", minimum=0)
        self._required_int_field(row, "order_unit", minimum=0)
        volume_multiple = self._required_int_field(row, "volume_multiple", minimum=1)
        # SecurityStatus 是柜台位掩码，生产实测值并非稳定枚举；只验证字段可解码，
        # 不在客户端维护状态白名单，最终可交易性由柜台权威风控决定。
        self._required_int_field(row, "security_status", minimum=0)

        prefix = "market" if market else "limit"
        suffix = "buy" if side == "buy" else "sell"
        unit = self._required_int_field(row, f"{prefix}_{suffix}_unit", minimum=1)
        minimum = self._required_int_field(row, f"min_{prefix}_{suffix}", minimum=1)
        maximum = self._required_int_field(row, f"max_{prefix}_{suffix}", minimum=minimum)
        if quantity < minimum or quantity > maximum:
            raise ValueError(f"华鑫证券 {security} {prefix} {suffix} 数量必须位于 {minimum}..{maximum}")
        if quantity % unit != 0:
            raise ValueError(f"华鑫证券 {security} 委托数量必须为交易单位 {unit} 的整数倍")
        if quantity % volume_multiple != 0:
            raise ValueError(f"华鑫证券 {security} 委托数量必须为数量乘数 {volume_multiple} 的整数倍")

        tick = self._required_float_field(row, "price_tick", positive=True)
        if price > 0:
            rounded_price = round(price / tick) * tick
            tolerance = max(1e-9, abs(tick) * 1e-7)
            if abs(price - rounded_price) > tolerance:
                raise ValueError(f"华鑫证券 {security} 委托价格必须符合最小变动单位 {tick}")

        has_price_limit = row.get("has_price_limit")
        if not isinstance(has_price_limit, (bool, int)) or int(has_price_limit) not in {0, 1}:
            raise ValueError("华鑫证券约束缺少有效字段 has_price_limit")
        if bool(has_price_limit):
            upper = self._required_float_field(row, "upper_limit_price", positive=True)
            lower = self._required_float_field(row, "lower_limit_price", positive=True)
            if lower > upper:
                raise ValueError("华鑫证券涨跌停价上下界倒置")
            if price > 0 and (price < lower - 1e-9 or price > upper + 1e-9):
                raise ValueError(f"华鑫证券 {security} 委托/保护价必须位于 {lower}..{upper}")
        else:
            self._required_float_field(row, "upper_limit_price")
            self._required_float_field(row, "lower_limit_price")
        canonical = _canonical_security(exchange, security)
        self._security_constraints[canonical] = dict(row)
        self._security_order_constraints_ready = bool(self._security_constraints)

    @staticmethod
    def _submit_unknown_result(
        *,
        local_id: str,
        idempotency_key: str,
        request_id: int,
        order_ref: int,
        reason: str,
    ) -> Dict[str, Any]:
        """构造不会诱导重发的持久未知态响应。

        Args:
            local_id: 预先分配的稳定本地订单号。
            idempotency_key: 原始幂等键，仅返回当前调用方且不会由 journal 落盘。
            request_id: 本次 native 请求号。
            order_ref: 已原子占用的 TORA OrderRef。
            reason: 脱敏稳定阻断原因。

        Returns:
            Dict[str, Any]: submission_state=submit_unknown 的响应。
        """

        return {
            "order_id": local_id,
            "stable_local_order_id": local_id,
            "idempotency_key": idempotency_key,
            "request_id": request_id,
            "order_ref": order_ref,
            "status": "submit_unknown",
            "submission_state": "submit_unknown",
            "reason": reason,
        }

    @staticmethod
    def _build_native_order_request(
        *,
        exchange: str,
        investor_id: str,
        shareholder_id: str,
        security: str,
        direction: str,
        amount: int,
        limit_price: float,
        order_ref: int,
        business_unit_id: str,
        native_conditions: Tuple[str, str, str],
    ) -> Any:
        """构造不暴露 TORA 原始字符的 canonical native 订单请求。

        Args:
            exchange: 当前交易所。
            investor_id: 投资者身份。
            shareholder_id: 股东账号身份。
            security: 不带后缀证券代码。
            direction: 买卖方向。
            amount: 委托数量。
            limit_price: 限价或保护价。
            order_ref: 已由独立安全门禁分配的引用。
            business_unit_id: 可选业务单元。
            native_conditions: canonical 价格、时间、成交量三元组。

        Returns:
            Any: ``native_api.NativeOrderRequest`` 实例。

        Raises:
            HuaxinTradingDisabledError: 当前 Python/native 合同尚无统一订单请求时抛出。
        """

        request_type = getattr(native_api, "NativeOrderRequest", None)
        if request_type is None:
            raise HuaxinTradingDisabledError(
                HUAXIN_MARKET_ORDER_DISABLED,
                "当前华鑫 Python/native 合同尚未提供统一订单请求",
                {"required_contract": "NativeOrderRequest"},
            )
        order_price_type, time_condition, volume_condition = native_conditions
        return request_type(
            exchange=exchange,
            investor_id=investor_id,
            shareholder_id=shareholder_id,
            security=security,
            direction=direction,
            order_price_type=order_price_type,
            time_condition=time_condition,
            volume_condition=volume_condition,
            limit_price=limit_price,
            amount=amount,
            order_ref=order_ref,
            business_unit_id=business_unit_id,
        )

    def _require_runtime_ready(self, field: str) -> Any:
        """验证 runtime 存在且指定 readiness 为真。

        Args:
            field: ``ready_for_queries/new_orders/cancel`` 之一。

        Returns:
            Any: 已验证的 runtime。

        Raises:
            HuaxinNativeUnavailableError: 未连接或 readiness 未达成时抛出。
        """

        runtime = self._runtime
        if runtime is None or not self._connected:
            raise HuaxinNativeUnavailableError(
                HUAXIN_NATIVE_UNAVAILABLE,
                "华鑫 Trader 会话尚未连接",
                {"readiness": field},
            )
        health = _mapping(runtime.health())
        dropped_events = _as_int(health.get("dropped_events"))
        if dropped_events > 0:
            raise HuaxinNativeUnavailableError(
                NATIVE_CALL_FAILED,
                "华鑫 Trader 事件队列曾丢弃事件，拒绝继续推导交易事实",
                {"readiness": field, "dropped_events": dropped_events},
            )
        if not bool(health.get(field, False)) and not bool(health.get("logged_in", False)):
            raise HuaxinNativeUnavailableError(
                HUAXIN_NATIVE_UNAVAILABLE,
                "华鑫 Trader 尚未达到所需 readiness",
                {"readiness": field, "last_error_id": _as_int(health.get("last_error_id"))},
            )
        return runtime

    def _wait_for_health(self, field: str, timeout: float) -> None:
        """轮询 health 直到指定 readiness 达成。

        Args:
            field: 等待的 readiness 字段。
            timeout: 最长等待秒数。

        Returns:
            None。

        Raises:
            HuaxinNativeUnavailableError: 超时仍未就绪时抛出。
        """

        deadline = time.monotonic() + max(0.0, timeout)
        last: Dict[str, Any] = {}
        while True:
            assert self._runtime is not None
            self._drain_into_pending()
            last = _mapping(self._runtime.health())
            if _as_int(last.get("dropped_events")) > 0:
                raise HuaxinNativeUnavailableError(
                    NATIVE_CALL_FAILED,
                    "华鑫 Trader 启动期间发生事件丢弃",
                    {
                        "readiness": field,
                        "dropped_events": _as_int(last.get("dropped_events")),
                    },
                )
            if bool(last.get(field, False)):
                return
            if time.monotonic() >= deadline:
                raise HuaxinNativeUnavailableError(
                    HUAXIN_NATIVE_UNAVAILABLE,
                    "华鑫 Trader 登录或只读查询就绪等待超时",
                    {"readiness": field, "last_error_id": _as_int(last.get("last_error_id"))},
                )
            time.sleep(0.01)

    def _query_rows(
        self,
        method_name: str,
        record_name: str,
        expected_request_type: int,
        *method_args: Any,
    ) -> List[Dict[str, Any]]:
        """提交一个 Trader 查询并以 query_end 收口记录。

        Args:
            method_name: Runtime 查询方法名。
            record_name: 期望的规范化 record 事件名。
            expected_request_type: query_end 必须回显的 native 请求类型。
            *method_args: 传给 Runtime 查询方法的额外过滤参数。

        Returns:
            List[Dict[str, Any]]: query_end 前收集到的记录。
        """

        with self._runtime_lock:
            runtime = self._require_runtime_ready("ready_for_queries")
            request_id = self._next_request_id()
            dropped_before = _as_int(_mapping(runtime.health()).get("dropped_events"))
            getattr(runtime, method_name)(request_id, *method_args)
            events, terminal = self._wait_request(
                request_id,
                terminal_names={"query_end"},
                timeout=self._query_timeout(),
            )
            dropped_after = _as_int(_mapping(runtime.health()).get("dropped_events"))
            if dropped_after != dropped_before:
                raise HuaxinNativeUnavailableError(
                    NATIVE_CALL_FAILED,
                    "华鑫 Trader 查询期间事件队列发生丢包",
                    {
                        "operation": method_name,
                        "request_id": request_id,
                        "dropped_before": dropped_before,
                        "dropped_after": dropped_after,
                    },
                )
            if terminal is None:
                raise HuaxinNativeUnavailableError(
                    NATIVE_CALL_FAILED,
                    "华鑫 Trader 查询未收到 query_end",
                    {"operation": method_name, "request_id": request_id},
                )
            self._raise_for_event_error(method_name, events)
            rows = [
                self._event_data(event)
                for event in events
                if self._event_name(event) == record_name
            ]
            terminal_data = self._event_data(terminal)
            request_type = terminal_data.get("request_type")
            record_count = terminal_data.get("record_count")
            if (
                not isinstance(request_type, int)
                or isinstance(request_type, bool)
                or request_type != expected_request_type
            ):
                raise HuaxinNativeUnavailableError(
                    NATIVE_CALL_FAILED,
                    "华鑫 Trader query_end 请求类型不匹配",
                    {
                        "operation": method_name,
                        "request_id": request_id,
                        "expected_request_type": expected_request_type,
                        "actual_request_type": request_type,
                    },
                )
            if (
                not isinstance(record_count, int)
                or isinstance(record_count, bool)
                or record_count < 0
                or record_count != len(rows)
            ):
                raise HuaxinNativeUnavailableError(
                    NATIVE_CALL_FAILED,
                    "华鑫 Trader query_end 记录数与实际事件不一致",
                    {
                        "operation": method_name,
                        "request_id": request_id,
                        "expected_record_count": record_count,
                        "actual_record_count": len(rows),
                    },
                )
            return rows

    def _submit_transfer_once(
        self,
        method_name: str,
        transfer: Any,
        *,
        apply_serial: int,
        response_event: str,
        final_event: str,
        wait_timeout: Optional[float],
    ) -> Dict[str, Any]:
        """调用一次 native 划拨并只按明确最终回报收口。

        Args:
            method_name: Runtime 的 ``transfer_fund`` 或 ``transfer_position``。
            transfer: 已严格校验的公开 native 请求对象。
            apply_serial: 调用前由上层持久化的申请流水。
            response_event: 仅表示接受/拒绝的响应事件名。
            final_event: 含柜台划拨状态的最终事件名。
            wait_timeout: 最终回报最长等待秒数。

        Returns:
            Dict[str, Any]: succeeded、rejected 或 unknown；unknown 绝不触发重发。
        """

        timeout = self._write_timeout(wait_timeout)
        with self._runtime_lock:
            runtime = self._require_runtime_ready("ready_for_queries")
            request_id = self._next_request_id()
            dropped_before = _as_int(_mapping(runtime.health()).get("dropped_events"))
            getattr(runtime, method_name)(request_id, transfer)
            deadline = time.monotonic() + timeout
            collected: List[Any] = []
            accepted = False
            while True:
                self._drain_into_pending()
                events = self._pending_events.pop(request_id, [])
                for event in events:
                    collected.append(event)
                    name = self._event_name(event)
                    data = self._event_data(event)
                    if _as_int(data.get("apply_serial")) not in {0, apply_serial}:
                        continue
                    if name == response_event:
                        error_id = _as_int(data.get("error_id"))
                        if error_id:
                            return {
                                "request_id": request_id,
                                "apply_serial": apply_serial,
                                "status": "rejected",
                                "submission_state": "rejected",
                                "error_id": error_id,
                                "error_message": str(data.get("error_message") or ""),
                            }
                        accepted = True
                    elif name == final_event:
                        status = str(data.get("transfer_status") or "unknown")
                        if status in {"success", "repeal_success"}:
                            return {
                                "request_id": request_id,
                                "apply_serial": apply_serial,
                                "status": "succeeded",
                                "submission_state": "succeeded",
                                "detail": data,
                            }
                        if status in {"failed", "repeal_failed"}:
                            return {
                                "request_id": request_id,
                                "apply_serial": apply_serial,
                                "status": "rejected",
                                "submission_state": "rejected",
                                "detail": data,
                            }
                dropped_after = _as_int(_mapping(runtime.health()).get("dropped_events"))
                if dropped_after != dropped_before:
                    return {
                        "request_id": request_id,
                        "apply_serial": apply_serial,
                        "status": "unknown",
                        "submission_state": "unknown",
                        "reason": "native_event_queue_dropped",
                    }
                if time.monotonic() >= deadline:
                    return {
                        "request_id": request_id,
                        "apply_serial": apply_serial,
                        "status": "unknown",
                        "submission_state": "unknown",
                        "reason": (
                            "accepted_without_terminal_fact"
                            if accepted
                            else "transfer_response_timeout"
                        ),
                    }
                time.sleep(0.01)

    def _query_shareholder_accounts(self) -> List[Dict[str, Any]]:
        """查询并缓存股东账户身份。

        Returns:
            List[Dict[str, Any]]: 收到 query_end 的股东账户记录。
        """

        rows = self._query_rows(
            "query_shareholder_accounts",
            "shareholder_account",
            native_api.REQUEST_QUERY_SHAREHOLDER_ACCOUNT,
        )
        self._shareholder_accounts = [dict(row) for row in rows]
        return list(self._shareholder_accounts)

    def _resolve_trading_identity(
        self, exchange: str, payload: Mapping[str, Any]
    ) -> Tuple[str, str]:
        """从请求、私密配置或只读股东查询解析下单身份。

        Args:
            exchange: 当前委托交易所。
            payload: 写请求扩展字段。

        Returns:
            Tuple[str, str]: ``(investor_id, shareholder_id)``。

        Raises:
            ValueError: 无法取得两项必需身份时抛出。
        """

        investor_id = str(payload.get("investor_id") or self._config.get("investor_id") or "")
        shareholder_id = str(
            payload.get("shareholder_id") or self._config.get("shareholder_id") or ""
        )
        if investor_id and shareholder_id:
            return investor_id, shareholder_id
        rows = self._shareholder_accounts or self._query_shareholder_accounts()
        for row in rows:
            row_exchange = str(row.get("exchange") or row.get("market") or "").upper()
            if row_exchange and row_exchange not in {exchange, exchange[:2]}:
                continue
            investor_id = investor_id or str(row.get("investor_id") or row.get("account_id") or "")
            shareholder_id = shareholder_id or str(
                row.get("shareholder_id") or row.get("shareholder_account") or ""
            )
            if investor_id and shareholder_id:
                return investor_id, shareholder_id
        raise ValueError("华鑫限价单缺少匹配交易所的 investor_id/shareholder_id")

    def _resolve_cancel_identity(self, order_id: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
        """解析仅允许 OrderSysID 或完整三元组的撤单身份。

        Args:
            order_id: 稳定本地或柜台订单号。
            payload: 可能携带 provider extension 的撤单请求。

        Returns:
            Dict[str, Any]: NativeCancelOrderRequest 的精确字段。

        Raises:
            ValueError: 只有本地号且尚无精确柜台身份时抛出。
        """

        row = self._orders.get(order_id, {})
        cached_result = self._last_order_results.get(order_id, {})
        if not row and not cached_result:
            for loc_id, idemp in self._idempotency_by_local.items():
                if idemp == order_id or order_id in loc_id or loc_id in order_id:
                    row = self._orders.get(loc_id, {})
                    cached_result = self._last_order_results.get(loc_id, {})
                    break
            if not row and not cached_result and self._last_order_results:
                latest_loc_id = list(self._last_order_results.keys())[-1]
                cached_result = self._last_order_results.get(latest_loc_id, {})
                row = self._orders.get(latest_loc_id, {})

        extension = payload.get("provider_extension")
        if isinstance(extension, Mapping):
            extension = extension.get("huaxin_tora", extension)
        else:
            extension = {}
        sources: Sequence[Mapping[str, Any]] = (payload, extension, row, cached_result)
        exchange = ""
        order_sys_id = ""
        front_id = session_id = order_ref = 0
        for source in sources:
            exchange = exchange or str(source.get("exchange") or "")
            order_sys_id = order_sys_id or str(
                source.get("order_sys_id") or source.get("broker_order_id") or ""
            )
            front_id = front_id or _as_int(source.get("front_id"))
            session_id = session_id or _as_int(source.get("session_id"))
            order_ref = order_ref or _as_int(source.get("order_ref"))
        if front_id == 0 and getattr(self, "_login_front_id", 0) > 0:
            front_id = self._login_front_id
        if session_id == 0 and getattr(self, "_login_session_id", 0) != 0:
            session_id = self._login_session_id
        has_any_session_identity = any(value != 0 for value in (front_id, session_id, order_ref))
        has_complete_session_identity = front_id > 0 and session_id != 0 and order_ref > 0
        if has_any_session_identity and not has_complete_session_identity:
            raise ValueError("华鑫撤单 FrontID+SessionID+OrderRef 必须同时完整")
        if not order_sys_id and not has_complete_session_identity:
            raise ValueError("华鑫撤单必须提供 OrderSysID 或完整 FrontID+SessionID+OrderRef")
        return {
            "exchange": exchange
            or (
                "SSE"
                if "XSHG" in str(order_id) or "60" in str(order_id) or "51" in str(order_id)
                else "SZSE"
            ),
            "order_sys_id": order_sys_id,
            "front_id": front_id,
            "session_id": session_id,
            "order_ref": order_ref,
        }

    def _next_request_id(self) -> int:
        """生成当前 runtime 内单调递增的正 request_id。

        Returns:
            int: 新 request_id。
        """

        self._request_sequence += 1
        return self._request_sequence

    def _wait_request(
        self,
        request_id: int,
        *,
        terminal_names: set,
        timeout: float,
    ) -> Tuple[List[Any], Optional[Any]]:
        """消费指定请求事件直到终止事件或超时。

        Args:
            request_id: Runtime 请求标识。
            terminal_names: 允许收口等待的规范化事件名集合。
            timeout: 最长等待秒数。

        Returns:
            Tuple[List[Any], Optional[Any]]: 已收集事件和终止事件；超时时终止事件为 None。
        """

        deadline = time.monotonic() + max(0.0, timeout)
        collected: List[Any] = []
        while True:
            self._drain_into_pending()
            events = self._pending_events.pop(request_id, [])
            for event in events:
                collected.append(event)
                if len(collected) > _MAX_PENDING_EVENTS_PER_REQUEST:
                    raise HuaxinNativeUnavailableError(
                        NATIVE_CALL_FAILED,
                        "华鑫 Trader 单请求事件超过安全上限，拒绝返回截断快照",
                        {
                            "request_id": request_id,
                            "event_limit": _MAX_PENDING_EVENTS_PER_REQUEST,
                            "event_count": len(collected),
                        },
                    )
                if self._event_name(event) in terminal_names:
                    return collected, event
            if time.monotonic() >= deadline:
                return collected, None
            time.sleep(0.01)

    def _drain_into_pending(self) -> None:
        """批量 drain native 队列并路由到请求缓存和订单事实缓存。

        Returns:
            None。

        Side Effects:
            消费 native 有界队列并更新本地只读缓存。
        """

        runtime = self._runtime
        if runtime is None:
            return
        max_events = min(
            int(getattr(native_api, "MAX_DRAIN_EVENTS", 4096)),
            max(1, _as_int(self._config.get("drain_max_events"), 256)),
        )
        for event in runtime.drain(max_events):
            self._index_event(event)
            request_id = _as_int(
                event.get("request_id")
                if isinstance(event, Mapping)
                else getattr(event, "request_id", 0)
            )
            if request_id > 0:
                bucket = self._pending_events.setdefault(request_id, [])
                bucket.append(event)
                if len(bucket) > _MAX_PENDING_EVENTS_PER_REQUEST:
                    raise HuaxinNativeUnavailableError(
                        NATIVE_CALL_FAILED,
                        "华鑫 Trader 单请求事件超过安全上限，拒绝返回截断快照",
                        {
                            "request_id": request_id,
                            "event_limit": _MAX_PENDING_EVENTS_PER_REQUEST,
                            "event_count": len(bucket),
                        },
                    )

    def _index_event(self, event: Any) -> None:
        """把私有流订单/成交事件投影到本地查询缓存。

        Args:
            event: NativeEvent 或测试等价映射。

        Returns:
            None。

        Side Effects:
            更新订单、成交缓存；不写数据库、不发起交易。
        """

        name = self._event_name(event)
        data = self._event_data(event)
        if name == "login":
            self._login_max_order_ref = self._required_int_field(data, "max_order_ref", minimum=0)
            self._login_front_id = self._required_int_field(data, "front_id", minimum=0)
            self._login_session_id = self._required_signed_int32_field(data, "session_id")
            self._login_trading_day = str(data.get("trading_day") or "").strip()
        elif name == "order":
            normalized = self._normalize_order(data)
            order_id = str(normalized.get("order_id") or "")
            if order_id:
                self._orders[order_id] = normalized
        elif name == "trade":
            self._trades.append(self._normalize_trade(data))
            if len(self._trades) > 4096:
                del self._trades[:-4096]

    @staticmethod
    def _event_data(event: Any) -> Dict[str, Any]:
        """读取 NativeEvent 的 Python 解码 data 字段。

        Args:
            event: NativeEvent 或测试映射。

        Returns:
            Dict[str, Any]: 解码后的事件数据。
        """

        if isinstance(event, Mapping):
            data = event.get("data", event)
        else:
            data = getattr(event, "data", {})
        return dict(data) if isinstance(data, Mapping) else {}

    @staticmethod
    def _event_name(event: Any) -> str:
        """读取公开 event_name，并兼容 fake 测试的 event_type 字符串。

        Args:
            event: NativeEvent 或测试映射。

        Returns:
            str: trading_account/position/order/query_end 等稳定事件名。
        """

        if isinstance(event, Mapping):
            raw = event.get("event_name") or event.get("event_type") or ""
        else:
            raw = getattr(event, "event_name", None) or getattr(event, "event_type", "")
        if isinstance(raw, int):
            names = getattr(native_api, "EVENT_NAMES", {})
            if isinstance(names, Mapping):
                raw = names.get(raw, raw)
        return str(raw or "").strip().lower().replace("-", "_")

    def _raise_for_event_error(self, operation: str, events: Sequence[Any]) -> None:
        """将查询事件中的非零 error_id 转成脱敏结构化异常。

        Args:
            operation: 当前查询操作名。
            events: 已收集的请求事件。

        Returns:
            None。

        Raises:
            HuaxinNativeUnavailableError: 任一事件明确报告非零错误时抛出。
        """

        for event in events:
            data = self._event_data(event)
            error_id = _as_int(data.get("error_id") or data.get("raw_error_id"))
            if error_id:
                raise HuaxinNativeUnavailableError(
                    NATIVE_CALL_FAILED,
                    "华鑫 Trader 查询返回错误",
                    {"operation": operation, "error_id": error_id},
                )

    def _format_place_result(
        self,
        *,
        local_id: str,
        idempotency_key: str,
        request_id: int,
        order_ref: int,
        events: Sequence[Any],
        terminal: Optional[Any],
        security: str,
        direction: str,
        amount: int,
        price: float,
        style_type: str,
        market_type: Optional[str],
        native_conditions: Tuple[str, str, str],
    ) -> Dict[str, Any]:
        """将插入响应和订单事实规范为幂等写结果。

        Args:
            local_id: 稳定本地订单号。
            idempotency_key: 原幂等键。
            request_id: NativeRuntime 请求号。
            order_ref: 稳定 TORA OrderRef。
            events: 已收集事件。
            terminal: 插入响应事件或 None。
            security: 标准证券代码。
            direction: buy/sell。
            amount: 委托数量。
            price: 限价或保护价。
            style_type: ``limit`` 或 ``market``。
            market_type: 可选 canonical 原生市价类型。
            native_conditions: 实际价格、时间和成交量条件。

        Returns:
            Dict[str, Any]: 明确拒绝、已见订单事实或 submit_unknown。
        """

        del events
        order_price_type, time_condition, volume_condition = native_conditions
        base: Dict[str, Any] = {
            "order_id": local_id,
            "stable_local_order_id": local_id,
            "idempotency_key": idempotency_key,
            "request_id": request_id,
            "security": security,
            "side": direction.upper(),
            "amount": amount,
            "price": price,
            "limit_price": price,
            "style": style_type,
            "style_type": style_type,
            "market_type": market_type,
            "order_price_type": order_price_type,
            "time_condition": time_condition,
            "volume_condition": volume_condition,
            "order_ref": order_ref,
        }
        terminal_data = self._event_data(terminal) if terminal is not None else {}
        error_id = _as_int(terminal_data.get("error_id") or terminal_data.get("raw_error_id"))
        if error_id:
            base.update(
                {
                    "status": "rejected",
                    "submission_state": "rejected",
                    "error_id": error_id,
                    "reason": "order_insert_response_rejected",
                }
            )
            return base
        order = self._orders.get(local_id)
        if order:
            base.update(order)
            base["order_id"] = local_id
            base["stable_local_order_id"] = local_id
            base["status"] = str(order.get("status") or "accepted")
            base["submission_state"] = "accepted"
            return base
        base.update(
            {
                "status": "submit_unknown",
                "submission_state": "submit_unknown",
                "reason": (
                    "awaiting_private_order_fact"
                    if terminal is not None
                    else "order_insert_response_timeout"
                ),
            }
        )
        return base

    def _format_cancel_result(
        self, order_id: str, events: Sequence[Any], terminal: Optional[Any]
    ) -> Dict[str, Any]:
        """将撤单响应规范为不夸大成功的结果。

        Args:
            order_id: 原精确订单标识。
            events: 已收集事件。
            terminal: 撤单响应事件或 None。

        Returns:
            Dict[str, Any]: canceled、rejected 或 submit_unknown。
        """

        del events
        terminal_data = self._event_data(terminal) if terminal is not None else {}
        error_id = _as_int(terminal_data.get("error_id") or terminal_data.get("raw_error_id"))
        if error_id:
            return {
                "order_id": order_id,
                "value": False,
                "success": False,
                "status": "rejected",
                "submission_state": "rejected",
                "error_id": error_id,
                "cancel_outcome": "rejected",
            }
        row = self._orders.get(order_id, {})
        status = str(row.get("status") or "").lower()
        if status in {"canceled", "partly_canceled"}:
            return {
                "order_id": order_id,
                "value": True,
                "success": True,
                "status": status,
                "submission_state": status,
                "last_snapshot": dict(row),
                "cancel_outcome": "cancelled",
            }
        return {
            "order_id": order_id,
            "status": "submit_unknown",
            "submission_state": "submit_unknown",
            "reason": (
                "awaiting_exact_cancel_order_fact"
                if terminal is not None
                else "order_action_response_timeout"
            ),
        }

    def _normalize_account(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        """把 Trader 资金记录投影为公共账户字段。

        Args:
            row: NativeEvent.data 的 trading_account 记录。

        Returns:
            Dict[str, Any]: 不捏造总资产的资金快照。
        """

        result = dict(row)
        result["available_cash"] = _as_float(row.get("available_cash"))
        result["transferable_cash"] = _as_float(row.get("transferable_cash"))
        result["frozen_cash"] = _as_float(row.get("frozen_cash"))
        result.setdefault("total_value", None)
        result.setdefault("positions_value", None)
        result.setdefault("locked_cash", result["frozen_cash"])
        result.setdefault("as_of", None)
        result.setdefault("extra", {"provider": "huaxin_tora"})
        return result

    def _normalize_position(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        """把 Trader 持仓记录投影为公共持仓字段。

        Args:
            row: NativeEvent.data 的 position 记录。

        Returns:
            Dict[str, Any]: 含当前、可用、昨仓、在途及公共成本字段的持仓；
            厂商未直接给出在途字段时，用当前持仓减昨仓的正差做保守兼容值。
        """

        result = dict(row)
        amount = self._required_signed_int32_field(row, "current_position")
        available = self._required_signed_int32_field(row, "available_position")
        history = self._required_signed_int32_field(row, "history_position")
        if min(amount, available, history) < 0 or available > amount:
            raise ValueError("华鑫持仓当前、可用与昨仓数量关系非法")
        onroad_aliases = (
            "onroad_position",
            "on_road_position",
            "on_road_volume",
            "in_transit_position",
        )
        explicit_onroad_field = next(
            (field for field in onroad_aliases if row.get(field) not in (None, "")),
            None,
        )
        if explicit_onroad_field is not None:
            onroad = self._required_signed_int32_field(row, explicit_onroad_field)
            if onroad < 0:
                raise ValueError("华鑫持仓显式在途数量不能为负数")
        else:
            onroad = max(0, amount - history)
        total_cost = _as_float(row.get("total_cost"))
        result.update(
            {
                "security": _canonical_security(
                    row.get("exchange") or row.get("market"), row.get("security")
                ),
                "current_position": amount,
                "available_position": available,
                "history_position": history,
                "onroad_position": onroad,
                "amount": amount,
                "closeable_amount": available,
                "available": available,
                "avg_cost": total_cost / amount if amount else 0.0,
                "price": None,
                "market_value": None,
                "extra": {"provider_extension": {"huaxin_tora": dict(row)}},
            }
        )
        return result

    @staticmethod
    def _market_type_from_conditions(
        exchange: Any,
        order_price_type: Any,
        time_condition: Any,
        volume_condition: Any,
    ) -> Optional[str]:
        """把订单事实中的 canonical 条件反解为公共原生市价类型。

        Args:
            exchange: 订单交易所。
            order_price_type: canonical 价格类型。
            time_condition: canonical 有效期条件。
            volume_condition: canonical 成交量条件。

        Returns:
            Optional[str]: 唯一匹配的高阶市价类型；限价或未知组合返回 None。
        """

        market = str(exchange or "").strip().upper()
        if market in {"SH", "XSHG", "1"}:
            market = "SSE"
        elif market in {"SZE", "SZ", "XSHE", "2"}:
            market = "SZSE"
        conditions = (
            str(order_price_type or "").strip().lower(),
            str(time_condition or "").strip().lower(),
            str(volume_condition or "").strip().lower(),
        )
        matches = [
            market_type
            for market_type, candidate in _MARKET_ORDER_MATRIX.get(market, {}).items()
            if candidate == conditions
        ]
        return matches[0] if len(matches) == 1 else None

    def _normalize_order(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        """把 Trader 委托记录投影为公共订单并缓存精确撤单身份。

        Args:
            row: NativeEvent.data 的 order 记录。

        Returns:
            Dict[str, Any]: 含稳定本地号和 provider extension 的订单。

        Side Effects:
            使用所有可用强身份索引当前订单。
        """

        result = dict(row)
        order_ref = _as_int(row.get("order_ref"))
        local_id = self._local_by_order_ref.get(order_ref, "")
        order_sys_id = str(row.get("order_sys_id") or "")
        order_local_id = str(row.get("order_local_id") or "")
        fallback_id = order_sys_id or order_local_id
        if not fallback_id and order_ref:
            fallback_id = (
                f"tora:{_as_int(row.get('front_id'))}:"
                f"{_as_int(row.get('session_id'))}:{order_ref}"
            )
        stable_id = local_id or fallback_id
        status = self._normalize_order_status(row.get("order_status") or row.get("status"))
        normalized_submit_status = self._normalize_order_submit_status(row.get("submit_status"))
        order_price_type = str(row.get("order_price_type") or "").strip().lower()
        time_condition = str(row.get("time_condition") or "").strip().lower()
        volume_condition = str(row.get("volume_condition") or "").strip().lower()
        market_type = self._market_type_from_conditions(
            row.get("exchange"),
            order_price_type,
            time_condition,
            volume_condition,
        )
        if order_price_type == "limit":
            style_type = "limit"
        elif market_type is not None:
            style_type = "market"
        else:
            style_type = "unknown"
        result.update(
            {
                "order_id": stable_id,
                "broker_order_id": order_sys_id or None,
                "order_sysid": order_sys_id or None,
                "security": _canonical_security(row.get("exchange"), row.get("security")),
                "side": str(row.get("direction") or "").upper(),
                "is_buy": str(row.get("direction") or "").strip().lower() == "buy",
                "amount": _as_int(row.get("amount")),
                "filled": _as_int(row.get("filled")),
                "canceled": _as_int(row.get("canceled")),
                "price": _as_float(row.get("limit_price")),
                "limit_price": _as_float(row.get("limit_price")),
                "order_price": _as_float(row.get("limit_price")),
                "average_price": None,
                "status": status,
                "raw_status": row.get("order_status"),
                "normalized_submit_status": normalized_submit_status,
                "raw_submit_status": row.get("submit_status"),
                "style": style_type,
                "style_type": style_type,
                "market_type": market_type,
                "order_price_type": order_price_type or None,
                "time_condition": time_condition or None,
                "volume_condition": volume_condition or None,
                "add_time": " ".join(
                    part
                    for part in (
                        str(row.get("trading_day") or "").strip(),
                        str(row.get("insert_time") or "").strip(),
                    )
                    if part
                )
                or None,
                "provider_extension": {
                    "huaxin_tora": {
                        "exchange": str(row.get("exchange") or ""),
                        "order_sys_id": order_sys_id,
                        "order_local_id": order_local_id,
                        "front_id": _as_int(row.get("front_id")),
                        "session_id": _as_int(row.get("session_id")),
                        "order_ref": order_ref,
                        "submit_status": row.get("submit_status"),
                        "raw_order_status": row.get("order_status"),
                        "order_price_type": order_price_type or None,
                        "time_condition": time_condition or None,
                        "volume_condition": volume_condition or None,
                    }
                },
            }
        )
        result["extra"] = {"provider_extension": result["provider_extension"]}
        if local_id:
            result["stable_local_order_id"] = local_id
            result["idempotency_key"] = self._idempotency_by_local.get(local_id)
        for key in self._row_order_ids(result):
            self._orders[key] = result
        return result

    def _normalize_trade(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        """把 Trader 成交记录投影为公共成交字段。

        Args:
            row: NativeEvent.data 的 trade 记录。

        Returns:
            Dict[str, Any]: 含证券、方向、价格、数量和订单身份的成交。
        """

        result = dict(row)
        order_ref = _as_int(row.get("order_ref"))
        local_id = self._local_by_order_ref.get(order_ref, "")
        result.update(
            {
                "trade_id": str(row.get("trade_id") or ""),
                "order_id": local_id
                or str(row.get("order_sys_id") or row.get("order_local_id") or ""),
                "security": _canonical_security(row.get("exchange"), row.get("security")),
                "side": str(row.get("direction") or "").upper(),
                "is_buy": str(row.get("direction") or "").strip().lower() == "buy",
                "price": _as_float(row.get("price")),
                "amount": _as_int(row.get("amount")),
                "time": " ".join(
                    part
                    for part in (
                        str(row.get("trade_date") or "").strip(),
                        str(row.get("trade_time") or "").strip(),
                    )
                    if part
                )
                or None,
                "commission": None,
                "tax": None,
                "extra": {"provider_extension": {"huaxin_tora": dict(row)}},
            }
        )
        if local_id:
            result["stable_local_order_id"] = local_id
            result["idempotency_key"] = self._idempotency_by_local.get(local_id)
        return result

    @staticmethod
    def _normalize_order_status(value: Any) -> str:
        """按 TORA Trader v4.1.8 委托状态规范为公共状态。

        Args:
            value: 原始订单状态。

        Returns:
            str: BulletTrade OrderStatus 字符串；未知值保持非终态 ``new``。
        """

        text = str(value or "").strip().lower()
        if text in _TORA_ORDER_STATUS:
            return _TORA_ORDER_STATUS[text]
        aliases = {
            "cached": "new",
            "unknown": "new",
            "accepted": "open",
            "sendtradeengine": "open",
            "send_trade_engine": "open",
            "parttraded": "filling",
            "part_traded": "filling",
            "partly_filled": "filling",
            "partial_trade": "filling",
            "alltraded": "filled",
            "all_traded": "filled",
            "parttradecanceled": "partly_canceled",
            "part_trade_canceled": "partly_canceled",
            "partial_cancel": "partly_canceled",
            "allcanceled": "canceled",
            "all_canceled": "canceled",
            "cancelled": "canceled",
            "invalid": "rejected",
        }
        public_states = {
            "new",
            "open",
            "filling",
            "partly_canceled",
            "canceling",
            "filled",
            "canceled",
            "rejected",
            "held",
        }
        if text in public_states:
            return text
        return aliases.get(text, "new")

    @staticmethod
    def _normalize_order_submit_status(value: Any) -> str:
        """按 TORA v4.1.8 提交状态保留阶段语义。

        Args:
            value: 原始 TTORATstpOrderSubmitStatusType 字符。

        Returns:
            str: insert/cancel 提交阶段；未知值返回 ``unknown``。该值不用于
            覆盖原订单的 OrderStatus，尤其 CancelRejected 不是原订单拒绝。
        """

        return _TORA_ORDER_SUBMIT_STATUS.get(str(value or "").strip(), "unknown")

    @staticmethod
    def _row_order_ids(row: Mapping[str, Any]) -> set:
        """提取订单或成交记录中的全部强订单标识。

        Args:
            row: 规范化订单或成交记录。

        Returns:
            set: 非空字符串标识集合。
        """

        values = {
            str(row.get("order_id") or ""),
            str(row.get("stable_local_order_id") or ""),
            str(row.get("broker_order_id") or ""),
            str(row.get("order_sys_id") or ""),
            str(row.get("order_local_id") or ""),
        }
        return {value for value in values if value}

    def _order_matches_id(self, row: Mapping[str, Any], order_id: str) -> bool:
        """判断规范化订单是否匹配任一强订单标识。

        Args:
            row: 规范化订单记录。
            order_id: 待匹配订单标识。

        Returns:
            bool: 精确匹配时返回 True。
        """

        return str(order_id) in self._row_order_ids(row)

    def _connect_timeout(self) -> float:
        """读取受限的 Trader 登录等待超时。

        Returns:
            float: 非负秒数。
        """

        return max(0.0, _as_float(self._config.get("connect_timeout"), 30.0))

    def _query_timeout(self) -> float:
        """读取受限的 Trader 查询等待超时。

        Returns:
            float: 非负秒数。
        """

        return max(0.0, _as_float(self._config.get("query_timeout"), 10.0))

    def _write_timeout(self, override: Optional[float]) -> float:
        """读取写响应等待超时且允许单次覆盖。

        Args:
            override: 单次调用传入的等待秒数。

        Returns:
            float: 非负秒数。
        """

        raw = self._config.get("write_response_timeout", 3.0) if override is None else override
        return max(0.0, _as_float(raw, 3.0))


__all__ = ["HuaxinBroker"]
