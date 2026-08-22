"""
作者: BruceLee
文件职责: 在独立 Python 3.7 进程中把华鑫 TORA XMD L1 行情转换为安全 JSONL。
主要输入: 显式 SDK 目录，以及 stdin 中带 request_id 的 subscribe/unsubscribe/health/stop 命令。
主要输出: stdout 中的 ready/response/tick/error JSON 事件。
上游关系: BulletTrade 行情网关以子进程方式启动本模块并消费 JSONL；普通 import 不启动 SDK。
下游关系: 仅显式导入 SDK 目录中的 xmdapi，不导入 Trader，也不包含任何交易调用。
关键环境或配置: 由父进程通过固定 argv 显式传入 L1 TCP 前置；空域登录；不配置 flow、不写文件；launcher
负责设置 PYTHONDONTWRITEBYTECODE=1。SDK 回调只复制标量到有界队列，输出和 Release
必须由主线程完成。
"""

import argparse
import importlib
import json
import math
import os
import queue
import re
import select
import sys
import threading
import time
from typing import IO, Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit

DEFAULT_QUEUE_CAPACITY = 1024
STDIN_POLL_SECONDS = 0.10

_COMMANDS = frozenset(("subscribe", "unsubscribe", "health", "stop"))
_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


class XmdSidecarError(RuntimeError):
    """表示 sidecar 的稳定、可脱敏报告错误。"""

    def __init__(self, code: str, message: str) -> None:
        """保存稳定错误码和可公开消息。

        参数:
            code: 稳定错误码。
            message: 不包含凭据或 SDK 对象的公开消息。
        返回:
            无。
        """

        super().__init__(message)
        self.code = str(code)
        self.message = str(message)


def _validate_tcp_front(value: str) -> str:
    """校验父进程显式传入的 TCP 行情前置。

    参数:
        value: 生产或仿真配置中的 ``tcp://host:port``。
    返回:
        去除首尾空白后的原始地址。
    异常:
        XmdSidecarError: 地址结构不完整或包含凭据时抛出。
    """

    text = str(value or "").strip()
    try:
        parsed = urlsplit(text)
        port = parsed.port
    except ValueError as exc:
        raise XmdSidecarError("front_invalid", "华鑫 XMD 前置必须为 tcp://host:port") from exc
    if (
        parsed.scheme.lower() != "tcp"
        or not parsed.hostname
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in ("", "/")
    ):
        raise XmdSidecarError("front_invalid", "华鑫 XMD 前置必须为 tcp://host:port")
    return text


def _text(value: Any) -> str:
    """把 SDK 文本标量转换为去除 NUL 的字符串。

    参数:
        value: bytes、str 或其他标量。
    返回:
        清理后的字符串。
    """

    if isinstance(value, bytes):
        return value.decode("utf-8", "replace").rstrip("\x00")
    return str(value or "").rstrip("\x00")


def _integer(value: Any) -> int:
    """把 SDK 数值标量安全转换为整数。

    参数:
        value: 原始 SDK 字段。
    返回:
        整数；转换失败时返回零。
    """

    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return 0


def _number(value: Any) -> float:
    """把 SDK 数值标量安全转换为浮点数。

    参数:
        value: 原始 SDK 字段。
    返回:
        浮点数；转换失败时返回零。
    """

    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _is_within(path: str, directory: str) -> bool:
    """判断真实文件路径是否位于显式 SDK 目录内。

    参数:
        path: 待检查文件路径。
        directory: 显式 SDK 根目录。
    返回:
        位于目录内时为 True。
    """

    try:
        return os.path.commonpath((os.path.realpath(path), directory)) == directory
    except (AttributeError, TypeError, ValueError):
        return False


def load_xmdapi(sdk_dir: str) -> Any:
    """只从显式 SDK 目录加载 xmdapi 及其 native 扩展。

    参数:
        sdk_dir: 同时包含 xmdapi.py 和 _xmdapi 扩展的绝对目录。
    返回:
        已完成来源校验的 xmdapi 模块。
    异常:
        XmdSidecarError: 目录无效、模块缺失或模块来源越界时抛出。
    副作用:
        显式执行一次 Python/native 模块导入；不创建 XMD API 实例。
    """

    root = os.path.realpath(os.path.abspath(str(sdk_dir or "")))
    if not os.path.isabs(str(sdk_dir or "")) or not os.path.isdir(root):
        raise XmdSidecarError("sdk_dir_invalid", "华鑫 XMD SDK 目录必须是已存在的绝对目录")

    existing = sys.modules.get("xmdapi")
    if existing is not None:
        origin = getattr(existing, "__file__", "")
        if not origin or not _is_within(origin, root):
            raise XmdSidecarError("sdk_origin_mismatch", "已加载的 xmdapi 不来自显式 SDK 目录")
        module = existing
    else:
        sys.path.insert(0, root)
        try:
            module = importlib.import_module("xmdapi")
        except Exception as exc:
            raise XmdSidecarError("sdk_import_failed", "无法从显式目录加载华鑫 XMD SDK") from exc
        finally:
            if sys.path and sys.path[0] == root:
                del sys.path[0]

    origin = getattr(module, "__file__", "")
    native_module = sys.modules.get("_xmdapi")
    native_origin = getattr(native_module, "__file__", "") if native_module is not None else ""
    if not origin or not _is_within(origin, root):
        raise XmdSidecarError("sdk_origin_mismatch", "xmdapi 不来自显式 SDK 目录")
    if not native_origin or not _is_within(native_origin, root):
        raise XmdSidecarError("native_origin_mismatch", "_xmdapi 不来自显式 SDK 目录")
    return module


def _normalise_security(value: Any, exchange_value: Any) -> Tuple[str, str, str]:
    """校验父进程提供的六位证券代码和显式交易所。

    参数:
        value: 例如 511880 或 000001 的六位证券代码。
        exchange_value: SSE 或 SZSE。
    返回:
        canonical、六位证券代码和 SSE/SZSE 三元组。
    异常:
        XmdSidecarError: 代码或交易所非法时抛出。
    """

    code = _text(value).strip()
    exchange = _text(exchange_value).strip().upper()
    if exchange not in ("SSE", "SZSE") or len(code) != 6 or not code.isdigit():
        raise XmdSidecarError("security_invalid", "证券代码必须为六位数字且交易所为 SSE/SZSE")
    canonical = code + (".XSHG" if exchange == "SSE" else ".XSHE")
    return canonical, code, exchange


def _request_id(value: Any) -> str:
    """校验父进程用于响应关联的公开请求号。

    参数:
        value: JSON 命令中的 request_id。
    返回:
        已校验的请求号。
    异常:
        XmdSidecarError: 请求号为空、过长或含不安全字符时抛出。
    """

    text = _text(value).strip()
    if not _REQUEST_ID_PATTERN.match(text):
        raise XmdSidecarError("request_id_invalid", "request_id 格式非法")
    return text


def _exchange_name(value: Any, xmdapi: Any) -> str:
    """把 TORA ExchangeID 标量转换为 SSE 或 SZSE。

    参数:
        value: 回调中的 ExchangeID。
        xmdapi: 已显式加载的 SDK 模块。
    返回:
        SSE、SZSE 或空字符串。
    """

    if value == getattr(xmdapi, "TORA_TSTP_EXD_SSE", object()):
        return "SSE"
    if value == getattr(xmdapi, "TORA_TSTP_EXD_SZSE", object()):
        return "SZSE"
    text = _text(value).strip().upper()
    if text in ("1", "SSE", "SH", "XSHG"):
        return "SSE"
    if text in ("2", "SZSE", "SZE", "SZ", "XSHE"):
        return "SZSE"
    return ""


class _JsonlWriter:
    """只允许主线程向目标流写入单行 JSON 对象。"""

    def __init__(self, stream: IO[str]) -> None:
        """记录输出流，实际写入由 emit 执行。

        参数:
            stream: 文本输出流。
        返回:
            无。
        """

        self._stream = stream
        self._owner_thread = threading.get_ident()

    def emit(self, payload: Mapping[str, Any]) -> None:
        """以紧凑 JSONL 输出一个公开事件。

        参数:
            payload: 仅含可 JSON 序列化标量的事件映射。
        返回:
            无。
        异常:
            RuntimeError: 非创建线程尝试输出时抛出。
        """

        if threading.get_ident() != self._owner_thread:
            raise RuntimeError("华鑫 XMD JSONL 只能由主线程输出")
        line = json.dumps(dict(payload), ensure_ascii=False, separators=(",", ":"))
        self._stream.write(line + "\n")
        self._stream.flush()


def _make_spi_class(xmdapi: Any) -> Any:
    """按运行时 SDK 基类构造只复制标量的 SPI 类型。

    参数:
        xmdapi: 已显式加载或测试注入的 SDK 模块。
    返回:
        继承 CTORATstpXMdSpi 的内部回调类型。
    """

    class QueueingSpi(xmdapi.CTORATstpXMdSpi):
        """把 native 回调深拷为 Python 标量事件并放入有界队列。"""

        def __init__(self, enqueue: Any) -> None:
            """保存线程安全入队函数。

            参数:
                enqueue: 接受内部事件字典的可调用对象。
            返回:
                无。
            """

            xmdapi.CTORATstpXMdSpi.__init__(self)
            self._enqueue = enqueue

        def OnFrontConnected(self) -> None:
            """把连接成功回调转换为无 SDK 对象的内部事件。"""

            self._enqueue({"kind": "front_connected"})

        def OnFrontDisconnected(self, reason: Any) -> None:
            """复制断开原因并通知主线程。

            参数:
                reason: SDK 断开原因码。
            返回:
                无。
            """

            self._enqueue({"kind": "front_disconnected", "reason": _integer(reason)})

        def OnRspUserLogin(self, login: Any, response: Any, request_id: Any) -> None:
            """只复制登录结果，不在回调线程订阅。

            参数:
                login: SDK 登录结果对象；不跨线程保存。
                response: SDK 响应对象。
                request_id: 登录请求号。
            返回:
                无。
            """

            del login
            error_id = _integer(getattr(response, "ErrorID", -1)) if response else -1
            self._enqueue(
                {
                    "kind": "login_response",
                    "request_id": _integer(request_id),
                    "error_id": error_id,
                }
            )

        def OnRspSubMarketData(self, security: Any, response: Any) -> None:
            """复制订阅响应的证券、交易所和错误码。

            参数:
                security: SDK 订阅证券对象。
                response: SDK 响应对象。
            返回:
                无。
            """

            self._enqueue(
                {
                    "kind": "subscription_response",
                    "action": "subscribe",
                    "security_code": _text(getattr(security, "SecurityID", "")),
                    "exchange_raw": _text(getattr(security, "ExchangeID", "")),
                    "error_id": _integer(getattr(response, "ErrorID", -1)) if response else -1,
                }
            )

        def OnRspUnSubMarketData(self, security: Any, response: Any) -> None:
            """复制解除订阅响应的证券、交易所和错误码。

            参数:
                security: SDK 订阅证券对象。
                response: SDK 响应对象。
            返回:
                无。
            """

            self._enqueue(
                {
                    "kind": "subscription_response",
                    "action": "unsubscribe",
                    "security_code": _text(getattr(security, "SecurityID", "")),
                    "exchange_raw": _text(getattr(security, "ExchangeID", "")),
                    "error_id": _integer(getattr(response, "ErrorID", -1)) if response else -1,
                }
            )

        def OnRtnMarketData(self, market_data: Any) -> None:
            """深拷一条 L1 快照的公开标量并记录本机接收纳秒。

            参数:
                market_data: SDK 行情对象，仅在当前回调栈读取。
            返回:
                无。
            """

            self._enqueue(
                {
                    "kind": "tick",
                    "security_code": _text(getattr(market_data, "SecurityID", "")),
                    "exchange_raw": _text(getattr(market_data, "ExchangeID", "")),
                    "TradingDay": _text(getattr(market_data, "TradingDay", "")),
                    "UpdateTime": _text(getattr(market_data, "UpdateTime", "")),
                    "Millisec": _integer(getattr(market_data, "UpdateMillisec", 0)),
                    "Last": _number(getattr(market_data, "LastPrice", 0.0)),
                    "Bid1": _number(getattr(market_data, "BidPrice1", 0.0)),
                    "Ask1": _number(getattr(market_data, "AskPrice1", 0.0)),
                    "BidVolume1": _integer(getattr(market_data, "BidVolume1", 0)),
                    "AskVolume1": _integer(getattr(market_data, "AskVolume1", 0)),
                    "UpperLimit": _number(getattr(market_data, "UpperLimitPrice", 0.0)),
                    "LowerLimit": _number(getattr(market_data, "LowerLimitPrice", 0.0)),
                    "Volume": _integer(getattr(market_data, "Volume", 0)),
                    "Turnover": _number(getattr(market_data, "Turnover", 0.0)),
                    "receive_ns": time.time_ns(),
                }
            )

    return QueueingSpi


class XmdJsonlSidecar:
    """管理一个显式前置、空域登录的华鑫 XMD JSONL 生命周期。"""

    def __init__(
        self,
        sdk_dir: str,
        front: str,
        output_stream: IO[str],
        xmdapi_module: Optional[Any] = None,
        queue_capacity: int = DEFAULT_QUEUE_CAPACITY,
    ) -> None:
        """建立尚未启动且不触碰 SDK 的 sidecar 对象。

        参数:
            sdk_dir: 显式 XMD SDK 目录；测试注入模块时仍保留来源说明。
            front: 父进程显式传入的生产或仿真 TCP 前置。
            output_stream: JSONL 文本输出流。
            xmdapi_module: 纯测试使用的 fake SDK；生产必须留空。
            queue_capacity: native 回调事件队列容量。
        返回:
            无。
        """

        if int(queue_capacity) <= 0:
            raise ValueError("queue_capacity 必须大于零")
        self._sdk_dir = str(sdk_dir)
        self._front = _validate_tcp_front(front)
        self._xmdapi = xmdapi_module
        self._writer = _JsonlWriter(output_stream)
        self._events = queue.Queue(maxsize=int(queue_capacity))
        self._queue_capacity = int(queue_capacity)
        self._drop_lock = threading.Lock()
        self._dropped_events = 0
        self._reported_drops = 0
        self._api = None
        self._spi = None
        self._main_thread_id = None  # type: Optional[int]
        self._api_version = ""
        self._running = False
        self._connected = False
        self._logged_in = False
        self._released = False
        self._stop_requested = False
        self._stop_request_id = None  # type: Optional[str]
        self._subscriptions = {}  # type: Dict[str, Dict[str, Any]]

    @property
    def stop_requested(self) -> bool:
        """返回主循环是否已经收到 stop 请求。

        返回:
            已请求停止时为 True。
        """

        return self._stop_requested

    def _assert_main_thread(self) -> None:
        """阻止 SDK 控制和 JSON 输出离开主线程。

        返回:
            无。
        异常:
            RuntimeError: 当前线程不是 sidecar 主线程时抛出。
        """

        if self._main_thread_id is None or threading.get_ident() != self._main_thread_id:
            raise RuntimeError("华鑫 XMD 控制调用必须位于主线程")

    def _enqueue_callback(self, event: Dict[str, Any]) -> None:
        """由 SDK 回调线程非阻塞写入有界内部队列。

        参数:
            event: 已深拷且只含标量的内部事件。
        返回:
            无；队列满时只累计丢弃计数。
        """

        try:
            self._events.put_nowait(dict(event))
        except queue.Full:
            with self._drop_lock:
                self._dropped_events += 1

    def _emit_error(self, code: str, message: str, request_id: Optional[str] = None) -> None:
        """输出不包含原始异常或输入内容的稳定错误事件。

        参数:
            code: 稳定错误码。
            message: 可公开中文消息。
            request_id: 可选父进程请求号。
        返回:
            无。
        """

        payload = {"type": "error", "code": str(code), "message": str(message)}
        if request_id:
            payload["request_id"] = str(request_id)
        self._writer.emit(payload)

    def _health_fields(self) -> Dict[str, Any]:
        """构造当前 transport/login/queue 健康字段。

        返回:
            仅包含公开布尔值和计数的字段映射。
        """

        return {
            "running": self._running,
            "connected": self._connected,
            "logged_in": self._logged_in,
            "released": self._released,
            "queue_capacity": self._queue_capacity,
            "queue_size": self._events.qsize(),
            "dropped_events": self._dropped_events,
        }

    def start(self) -> None:
        """显式创建 XMD API、注册固定 TCP 前置并异步 Init。

        返回:
            无。
        异常:
            XmdSidecarError: SDK 导入或创建失败时抛出。
        副作用:
            创建只读 XMD 会话；不订阅证券、不调用 Trader、不写文件。
        """

        if self._running:
            raise XmdSidecarError("already_running", "华鑫 XMD sidecar 已经启动")
        self._main_thread_id = threading.get_ident()
        if self._xmdapi is None:
            self._xmdapi = load_xmdapi(self._sdk_dir)
        try:
            self._api_version = _text(self._xmdapi.CTORATstpXMdApi_GetApiVersion())
            self._api = self._xmdapi.CTORATstpXMdApi_CreateTstpXMdApi()
            if self._api is None:
                raise XmdSidecarError("api_create_failed", "华鑫 XMD API 创建失败")
            spi_class = _make_spi_class(self._xmdapi)
            self._spi = spi_class(self._enqueue_callback)
            self._api.RegisterSpi(self._spi)
            self._api.RegisterFront(self._front)
            self._running = True
            self._api.Init()
            self.drain_events()
        except XmdSidecarError:
            self._release_api()
            raise
        except Exception as exc:
            self._release_api()
            raise XmdSidecarError("sdk_start_failed", "华鑫 XMD SDK 启动失败") from exc

    def _release_api(self) -> None:
        """仅在主线程释放已创建的 XMD API。

        返回:
            无。
        副作用:
            调用一次 SDK Release，并清除 Python 持有的 API/SPI 引用。
        """

        self._assert_main_thread()
        api = self._api
        self._api = None
        self._spi = None
        if api is not None:
            api.Release()
            self._released = True
        self._running = False

    def _exchange_constant(self, exchange: str) -> Any:
        """返回 SSE/SZSE 对应的 SDK ExchangeID 常量。

        参数:
            exchange: SSE 或 SZSE。
        返回:
            SDK 交易所常量。
        异常:
            XmdSidecarError: SDK 缺少所需常量时抛出。
        """

        name = "TORA_TSTP_EXD_SSE" if exchange == "SSE" else "TORA_TSTP_EXD_SZSE"
        if not hasattr(self._xmdapi, name):
            raise XmdSidecarError("exchange_constant_missing", "XMD SDK 缺少现货交易所常量")
        return getattr(self._xmdapi, name)

    def _canonical_from_event(self, event: Mapping[str, Any]) -> Tuple[str, str, str]:
        """从回调标量恢复内部 canonical、六位代码和交易所。

        参数:
            event: subscription 或 tick 内部事件。
        返回:
            canonical、六位证券代码和 SSE/SZSE。
        """

        code = _text(event.get("security_code", "")).strip().upper()
        exchange = _exchange_name(event.get("exchange_raw", ""), self._xmdapi)
        if not exchange:
            matches = [item for item in self._subscriptions.values() if item.get("code") == code]
            if len(matches) == 1:
                exchange = _text(matches[0].get("exchange"))
        if exchange not in ("SSE", "SZSE") or len(code) != 6:
            return code, code, exchange
        canonical = code + (".XSHG" if exchange == "SSE" else ".XSHE")
        return canonical, code, exchange

    def _send_subscription(self, canonical: str, action: str, request_id: Optional[str]) -> None:
        """由主线程向 SDK 发送订阅或解除订阅请求。

        参数:
            canonical: 标准证券代码。
            action: subscribe 或 unsubscribe。
            request_id: 父进程请求号；停止清理时为 None。
        返回:
            无。
        """

        self._assert_main_thread()
        item = self._subscriptions[canonical]
        exchange_constant = self._exchange_constant(item["exchange"])
        securities = [item["code"].encode("ascii")]
        item["pending"] = action
        item["pending_request_id"] = request_id
        if action == "subscribe":
            result = self._api.SubscribeMarketData(securities, exchange_constant)
        else:
            result = self._api.UnSubscribeMarketData(securities, exchange_constant)
        accepted = _integer(result) == 0
        if not accepted:
            item["pending"] = ""
            item["pending_request_id"] = None
            if request_id:
                self._writer.emit(
                    {
                        "type": "response",
                        "request_id": request_id,
                        "op": action,
                        "ok": False,
                        "security": item["code"],
                        "exchange": item["exchange"],
                        "active": bool(item["active"]),
                    }
                )
            self._emit_error(
                "subscription_request_failed",
                "XMD SDK 拒绝了订阅状态变更请求",
                request_id,
            )
            return
        if action == "unsubscribe":
            item["active"] = False
            item["pending"] = ""
            item["pending_request_id"] = None
            if request_id:
                self._writer.emit(
                    {
                        "type": "response",
                        "request_id": request_id,
                        "op": "unsubscribe",
                        "ok": True,
                        "security": item["code"],
                        "exchange": item["exchange"],
                        "active": False,
                    }
                )

    def _subscribe_desired(self) -> None:
        """登录成功后发送所有尚未生效的目标订阅。

        返回:
            无。
        """

        for canonical, item in sorted(self._subscriptions.items()):
            if item["desired"] and not item["active"] and item["pending"] == "waiting_login":
                self._send_subscription(canonical, "subscribe", item.get("pending_request_id"))

    def _fail_pending_subscriptions(self, code: str) -> None:
        """在登录或连接失败时结束所有待处理父进程请求。

        参数:
            code: 写入 response 的稳定失败码。
        返回:
            无。
        """

        for item in self._subscriptions.values():
            request_id = item.get("pending_request_id")
            action = item.get("pending")
            if request_id and action:
                self._writer.emit(
                    {
                        "type": "response",
                        "request_id": request_id,
                        "op": "subscribe" if action == "waiting_login" else action,
                        "ok": False,
                        "security": item["code"],
                        "exchange": item["exchange"],
                        "active": False,
                        "code": str(code),
                    }
                )
            item["active"] = False
            item["pending"] = ""
            item["pending_request_id"] = None

    def _handle_internal_event(self, event: Mapping[str, Any]) -> None:
        """在主线程消费一条回调标量并更新/输出状态。

        参数:
            event: callback queue 中的内部事件。
        返回:
            无。
        """

        kind = event.get("kind")
        if kind == "front_connected":
            self._connected = True
            request = self._xmdapi.CTORATstpReqUserLoginField()
            result = self._api.ReqUserLogin(request, 1)
            if _integer(result) != 0:
                self._emit_error("login_request_failed", "XMD SDK 拒绝空域登录请求")
            return
        if kind == "front_disconnected":
            self._connected = False
            self._logged_in = False
            self._fail_pending_subscriptions("front_disconnected")
            self._emit_error("front_disconnected", "华鑫 XMD 行情前置连接已断开")
            return
        if kind == "login_response":
            self._logged_in = _integer(event.get("error_id")) == 0
            if self._logged_in:
                ready = {
                    "type": "ready",
                    "api_version": self._api_version,
                }
                ready.update(self._health_fields())
                self._writer.emit(ready)
                self._subscribe_desired()
            else:
                self._fail_pending_subscriptions("login_failed")
                self._emit_error("login_failed", "华鑫 XMD 空域登录失败")
            return
        if kind == "subscription_response":
            self._handle_subscription_response(event)
            return
        if kind == "tick":
            self._handle_tick(event)

    def _handle_subscription_response(self, event: Mapping[str, Any]) -> None:
        """在主线程投影订阅应答并更新 active 状态。

        参数:
            event: 已深拷的订阅应答内部事件。
        返回:
            无。
        """

        canonical, code, exchange = self._canonical_from_event(event)
        item = self._subscriptions.get(canonical)
        action = _text(event.get("action"))
        success = _integer(event.get("error_id")) == 0
        request_id = item.get("pending_request_id") if item is not None else None
        if item is not None:
            item["pending"] = ""
            item["pending_request_id"] = None
            if success:
                item["active"] = action == "subscribe"
        if request_id:
            self._writer.emit(
                {
                    "type": "response",
                    "request_id": request_id,
                    "op": action,
                    "ok": success,
                    "security": code,
                    "exchange": exchange,
                    "active": bool(item and item["active"]),
                }
            )
        if not success:
            self._emit_error(
                "subscription_response_failed",
                "华鑫 XMD 订阅状态变更失败",
                request_id,
            )

    def _handle_tick(self, event: Mapping[str, Any]) -> None:
        """在主线程确认实际订阅并把内部行情标量投影为公开 tick JSON。

        参数:
            event: 已深拷的行情内部事件。
        返回:
            无。

        说明:
            部分 v1.0.5 柜台在 ``SubscribeMarketData`` 返回成功后不发送可关联的
            ``OnRspSubMarketData``，但会立即推送目标证券行情。首个匹配 tick 是订阅
            已实际生效的更强证据，因此仅用它完成仍处于 pending 的 subscribe；行情
            新鲜度仍由 parent backend 独立校验，收盘旧 tick 不会被当作可用价格。
        """

        canonical, code, exchange = self._canonical_from_event(event)
        if exchange not in ("SSE", "SZSE"):
            self._emit_error("tick_exchange_unknown", "华鑫 XMD 行情交易所无法识别")
            return
        item = self._subscriptions.get(canonical)
        if item is not None and item["desired"] and item["pending"] == "subscribe":
            request_id = item.get("pending_request_id")
            item["active"] = True
            item["pending"] = ""
            item["pending_request_id"] = None
            if request_id:
                self._writer.emit(
                    {
                        "type": "response",
                        "request_id": request_id,
                        "op": "subscribe",
                        "ok": True,
                        "security": code,
                        "exchange": exchange,
                        "active": True,
                    }
                )
        payload = {
            "type": "tick",
            "security": code,
            "exchange": exchange,
            "TradingDay": _text(event.get("TradingDay")),
            "UpdateTime": _text(event.get("UpdateTime")),
            "Millisec": _integer(event.get("Millisec")),
            "Last": _number(event.get("Last")),
            "Bid1": _number(event.get("Bid1")),
            "Ask1": _number(event.get("Ask1")),
            "BidVolume1": _integer(event.get("BidVolume1")),
            "AskVolume1": _integer(event.get("AskVolume1")),
            "UpperLimit": _number(event.get("UpperLimit")),
            "LowerLimit": _number(event.get("LowerLimit")),
            "Volume": _integer(event.get("Volume")),
            "Turnover": _number(event.get("Turnover")),
            "receive_ns": _integer(event.get("receive_ns")),
        }
        self._writer.emit(payload)

    def drain_events(self) -> int:
        """由主线程排空当前回调队列并输出 JSONL。

        返回:
            本轮处理的内部事件数。
        """

        self._assert_main_thread()
        processed = 0
        while True:
            try:
                event = self._events.get_nowait()
            except queue.Empty:
                break
            self._handle_internal_event(event)
            processed += 1
        with self._drop_lock:
            dropped = self._dropped_events
        if dropped > self._reported_drops:
            self._reported_drops = dropped
            self._emit_error("callback_queue_overflow", "华鑫 XMD 回调队列发生丢弃")
        return processed

    def handle_command(self, command: Mapping[str, Any]) -> None:
        """在主线程处理一个已解析的白名单 JSON 命令。

        参数:
            command: 必须含 op 和 request_id 的 JSON 对象。
        返回:
            无。
        """

        self._assert_main_thread()
        raw_request_id = _text(command.get("request_id")).strip()
        try:
            request_id = _request_id(raw_request_id)
        except XmdSidecarError as exc:
            self._emit_error(exc.code, exc.message)
            return
        name = _text(command.get("op")).strip().lower()
        if name not in _COMMANDS:
            self._emit_error(
                "command_invalid",
                "只允许 subscribe/unsubscribe/health/stop 命令",
                request_id,
            )
            return
        allowed_fields = {"op", "request_id"}
        if name in ("subscribe", "unsubscribe"):
            allowed_fields.update(("security", "exchange"))
        if set(command) != allowed_fields:
            self._emit_error("command_fields_invalid", "JSON 命令字段与固定协议不一致", request_id)
            return
        if name == "health":
            response = {
                "type": "response",
                "request_id": request_id,
                "op": "health",
                "ok": True,
                "api_version": self._api_version,
            }
            response.update(self._health_fields())
            self._writer.emit(response)
            return
        if name == "stop":
            self._stop_request_id = request_id
            self._stop_requested = True
            return
        try:
            canonical, code, exchange = _normalise_security(
                command.get("security"), command.get("exchange")
            )
        except XmdSidecarError as exc:
            self._emit_error(exc.code, exc.message, request_id)
            return
        item = self._subscriptions.setdefault(
            canonical,
            {
                "code": code,
                "exchange": exchange,
                "desired": False,
                "active": False,
                "pending": "",
                "pending_request_id": None,
            },
        )
        if item["pending"]:
            self._writer.emit(
                {
                    "type": "response",
                    "request_id": request_id,
                    "op": name,
                    "ok": False,
                    "security": code,
                    "exchange": exchange,
                    "active": bool(item["active"]),
                    "code": "subscription_busy",
                }
            )
            return
        if name == "subscribe":
            item["desired"] = True
            if item["active"]:
                self._writer.emit(
                    {
                        "type": "response",
                        "request_id": request_id,
                        "op": "subscribe",
                        "ok": True,
                        "security": code,
                        "exchange": exchange,
                        "active": True,
                    }
                )
            elif self._logged_in:
                self._send_subscription(canonical, "subscribe", request_id)
            else:
                item["pending"] = "waiting_login"
                item["pending_request_id"] = request_id
            return
        item["desired"] = False
        if (item["active"] or item["pending"] == "subscribe") and self._logged_in:
            self._send_subscription(canonical, "unsubscribe", request_id)
        else:
            self._writer.emit(
                {
                    "type": "response",
                    "request_id": request_id,
                    "op": "unsubscribe",
                    "ok": True,
                    "security": code,
                    "exchange": exchange,
                    "active": False,
                }
            )

    def handle_json_line(self, line: str) -> None:
        """解析一行 stdin JSON 并执行白名单命令。

        参数:
            line: 单行 JSON 文本。
        返回:
            无。
        """

        try:
            payload = json.loads(line)
        except (TypeError, ValueError):
            self._emit_error("json_invalid", "stdin 必须提供单行 JSON 对象")
            return
        if not isinstance(payload, dict):
            self._emit_error("json_invalid", "stdin JSON 顶层必须是对象")
            return
        self.handle_command(payload)

    def stop(self) -> None:
        """由主线程停止订阅并唯一一次 Release XMD API。

        返回:
            无。
        副作用:
            对已生效订阅发送最佳努力解除请求，然后调用 SDK Release。
        """

        self._assert_main_thread()
        if not self._running:
            return
        if self._logged_in:
            for canonical, item in sorted(self._subscriptions.items()):
                if item["active"] or item["pending"] == "subscribe":
                    self._send_subscription(canonical, "unsubscribe", None)
        self.drain_events()
        self._release_api()
        self._connected = False
        self._logged_in = False
        if self._stop_request_id:
            self._writer.emit(
                {
                    "type": "response",
                    "request_id": self._stop_request_id,
                    "op": "stop",
                    "ok": True,
                    "released": self._released,
                }
            )
            self._stop_request_id = None


def run_sidecar(
    sdk_dir: str,
    front: str,
    input_stream: IO[str],
    output_stream: IO[str],
    xmdapi_module: Optional[Any] = None,
) -> int:
    """运行 stdin/stdout JSONL 主循环直至 stop 或 EOF。

    参数:
        sdk_dir: 显式 XMD SDK 目录。
        front: 父进程显式传入的生产或仿真 TCP 前置。
        input_stream: JSONL 命令输入流。
        output_stream: JSONL 事件输出流。
        xmdapi_module: 仅供纯 fake 单测注入。
    返回:
        正常停止为零，启动失败为二。
    """

    sidecar = XmdJsonlSidecar(
        sdk_dir=sdk_dir,
        front=front,
        output_stream=output_stream,
        xmdapi_module=xmdapi_module,
    )
    try:
        sidecar.start()
    except XmdSidecarError as exc:
        sidecar._writer.emit({"type": "error", "code": exc.code, "message": exc.message})
        return 2

    try:
        while not sidecar.stop_requested:
            sidecar.drain_events()
            readable, _, _ = select.select((input_stream,), (), (), STDIN_POLL_SECONDS)
            if not readable:
                continue
            line = input_stream.readline()
            if line == "":
                break
            sidecar.handle_json_line(line)
    finally:
        sidecar.stop()
    return 0


def _protected_cli_stream() -> IO[str]:
    """保留 JSONL stdout 并抑制厂商 SDK 的非 JSON 原生日志。

    返回:
        指向原始 stdout 的独立行缓冲文本流。
    副作用:
        将进程 fd 1 和 fd 2 重定向到 /dev/null；JSON 写入保留的 fd 副本。
    """

    report_fd = os.dup(sys.stdout.fileno())
    null_fd = os.open(os.devnull, os.O_WRONLY)
    os.dup2(null_fd, 1)
    os.dup2(null_fd, 2)
    os.close(null_fd)
    return os.fdopen(report_fd, "w", 1)


def main(argv: Optional[List[str]] = None) -> int:
    """解析最小 CLI 并启动华鑫 XMD JSONL sidecar。

    参数:
        argv: 可选参数列表；默认读取 sys.argv。
    返回:
        进程退出码。
    """

    parser = argparse.ArgumentParser(description="华鑫 XMD L1 JSONL sidecar")
    parser.add_argument("--sdk-dir", required=True, help="包含 xmdapi.py 与 _xmdapi 的绝对目录")
    parser.add_argument("--front", required=True, help="当前环境的 tcp://host:port 行情前置")
    args = parser.parse_args(argv)
    output_stream = _protected_cli_stream()
    try:
        return run_sidecar(args.sdk_dir, args.front, sys.stdin, output_stream)
    finally:
        output_stream.close()


if __name__ == "__main__":
    raise SystemExit(main())
