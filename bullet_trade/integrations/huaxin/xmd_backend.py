"""
作者: BruceLee

文件职责: 在 BulletTrade 主进程中管理华鑫 Python 3.7 XMD JSONL sidecar。
主要输入: 显式 Python 3.7 解释器、XMD SDK 目录、环境配置的 L1 前置和订阅证券。
主要输出: 仅来自 ``huaxin_xmd_l1`` 的新鲜 L1 快照与脱敏模块健康状态。
上游关系: Huaxin server 数据 adapter；普通 import 不创建进程、不加载厂商 SDK。
下游关系: ``xmd_sidecar.py`` 的 stdin/stdout JSONL 白名单协议，不触碰 Trader 写接口。
关键配置: 子进程使用固定 argv 且 ``shell=False``；只继承必要运行库环境变量；任何
协议、来源、证券、时间或盘口校验失败都 fail closed。
"""

from __future__ import annotations

import json
import math
import os
import subprocess
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Protocol, Set, TextIO, Tuple
from urllib.parse import urlsplit

HUAXIN_XMD_SOURCE = "huaxin_xmd_l1"
DEFAULT_MAX_AGE_SECONDS = 30.0
_SHANGHAI_TZ = timezone(timedelta(hours=8))
_ALLOWED_ENVIRONMENT = ("PATH", "LD_LIBRARY_PATH", "LANG", "LC_ALL", "TZ")


class XmdBackendError(RuntimeError):
    """表示 XMD parent/backend 的稳定、可脱敏错误。"""

    def __init__(self, code: str, message: str) -> None:
        """保存稳定错误码和不含敏感值的公开消息。

        Args:
            code: 稳定错误码。
            message: 可安全进入 health 或日志的中文消息。

        Returns:
            None。
        """

        super().__init__(message)
        self.code = str(code)
        self.message = str(message)


class XmdBackend(Protocol):
    """定义 server 数据 adapter 所需的可替换 XMD backend 合同。"""

    def start(self) -> None:
        """启动只读行情 backend，并等待登录就绪。"""

    def stop(self) -> None:
        """幂等停止行情 backend。"""

    def subscribe(self, security: str) -> Dict[str, Any]:
        """订阅一个标准证券并返回确认回执。"""

    def unsubscribe(self, security: str) -> Dict[str, Any]:
        """退订一个标准证券并返回确认回执。"""

    def get_latest(self, security: str, wait_timeout: float = 0.0) -> Dict[str, Any]:
        """返回指定证券的最新新鲜快照。"""

    def health(self) -> Dict[str, Any]:
        """返回不含路径或凭据的模块健康状态。"""


def _validate_tcp_front(value: str) -> str:
    """校验并返回显式 TCP 行情前置地址。

    Args:
        value: 生产或仿真 env 提供的 ``tcp://host:port``。

    Returns:
        str: 去除首尾空白后的原始前置地址。

    Raises:
        XmdBackendError: 地址缺少 TCP scheme、主机或合法端口时抛出。
    """

    text = str(value or "").strip()
    try:
        parsed = urlsplit(text)
        port = parsed.port
    except ValueError as exc:
        raise XmdBackendError("front_invalid", "华鑫 XMD 前置必须为 tcp://host:port") from exc
    if (
        parsed.scheme.lower() != "tcp"
        or not parsed.hostname
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise XmdBackendError("front_invalid", "华鑫 XMD 前置必须为 tcp://host:port")
    return text


def _normalise_security(security: str) -> Tuple[str, str, str]:
    """把标准证券代码拆为 canonical、六位代码和 XMD 交易所。

    Args:
        security: ``511880.XSHG`` 或 ``000001.XSHE`` 格式的标准代码。

    Returns:
        Tuple[str, str, str]: canonical、六位代码和 SSE/SZSE。

    Raises:
        XmdBackendError: 代码或后缀不满足严格现货格式时抛出。
    """

    text = str(security or "").strip().upper()
    if text.endswith(".XSHG"):
        code, exchange = text[:-5], "SSE"
    elif text.endswith(".XSHE"):
        code, exchange = text[:-5], "SZSE"
    else:
        raise XmdBackendError("security_invalid", "华鑫 XMD 只接受显式 .XSHG/.XSHE 代码")
    if len(code) != 6 or not code.isdigit():
        raise XmdBackendError("security_invalid", "华鑫 XMD 证券代码必须为六位数字")
    return text, code, exchange


def _parse_source_timestamp(payload: Mapping[str, Any]) -> float:
    """从 XMD TradingDay/UpdateTime/Millisec 构造北京时间 epoch。

    Args:
        payload: sidecar tick 事件。

    Returns:
        float: 带东八区解释的 Unix 时间戳。

    Raises:
        XmdBackendError: 日期、时间或毫秒缺失/非法时抛出。
    """

    trading_day = str(payload.get("TradingDay") or "").strip()
    update_time = str(payload.get("UpdateTime") or "").strip()
    try:
        millisec = int(payload.get("Millisec"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise XmdBackendError("source_time_invalid", "华鑫 XMD 毫秒字段非法") from exc
    if millisec < 0 or millisec > 999:
        raise XmdBackendError("source_time_invalid", "华鑫 XMD 毫秒字段超出范围")
    try:
        parsed = datetime.strptime(
            f"{trading_day} {update_time}",
            "%Y%m%d %H:%M:%S",
        ).replace(tzinfo=_SHANGHAI_TZ, microsecond=millisec * 1000)
    except ValueError as exc:
        raise XmdBackendError("source_time_invalid", "华鑫 XMD 交易所时间格式非法") from exc
    return parsed.timestamp()


def _finite_number(payload: Mapping[str, Any], field: str) -> float:
    """读取一个必须为有限数值的 XMD 字段。

    Args:
        payload: sidecar tick 事件。
        field: 待读取字段名。

    Returns:
        float: 有限浮点值。

    Raises:
        XmdBackendError: 字段不是有限数值时抛出。
    """

    try:
        value = float(payload.get(field))
    except (TypeError, ValueError, OverflowError) as exc:
        raise XmdBackendError("tick_numeric_invalid", f"华鑫 XMD {field} 不是有效数值") from exc
    if not math.isfinite(value):
        raise XmdBackendError("tick_numeric_invalid", f"华鑫 XMD {field} 不是有限数值")
    return value


def _non_negative_integer(payload: Mapping[str, Any], field: str) -> int:
    """读取一个必须为非负整数的 XMD 字段。

    Args:
        payload: sidecar tick 事件。
        field: 待读取字段名。

    Returns:
        int: 非负整数。

    Raises:
        XmdBackendError: 字段非法或小于零时抛出。
    """

    try:
        value = int(payload.get(field))
    except (TypeError, ValueError, OverflowError) as exc:
        raise XmdBackendError("tick_numeric_invalid", f"华鑫 XMD {field} 不是有效整数") from exc
    if value < 0:
        raise XmdBackendError("tick_numeric_invalid", f"华鑫 XMD {field} 不能小于零")
    return value


def _normalise_tick(payload: Mapping[str, Any], now: Optional[float] = None) -> Dict[str, Any]:
    """把 sidecar tick 严格归一为 BulletTrade 新鲜度快照。

    Args:
        payload: ``type=tick`` 的 JSON 对象。
        now: 测试可注入的当前 epoch；默认读取系统时间。

    Returns:
        Dict[str, Any]: 带 source/source_time/received_time/盘口的 canonical 快照。

    Raises:
        XmdBackendError: 来源身份、时间、证券或盘口任一不合法时抛出。
    """

    if str(payload.get("type") or "") != "tick":
        raise XmdBackendError("tick_type_invalid", "华鑫 XMD parent 只接受 tick 事件")
    code = str(payload.get("security") or "").strip()
    exchange = str(payload.get("exchange") or "").strip().upper()
    canonical, expected_code, expected_exchange = _normalise_security(
        code + (".XSHG" if exchange == "SSE" else ".XSHE") if exchange in {"SSE", "SZSE"} else ""
    )
    if code != expected_code or exchange != expected_exchange:
        raise XmdBackendError("tick_identity_invalid", "华鑫 XMD 行情证券或交易所身份不一致")

    source_timestamp = _parse_source_timestamp(payload)
    try:
        receive_ns = int(payload.get("receive_ns"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise XmdBackendError("received_time_invalid", "华鑫 XMD 接收时间非法") from exc
    if receive_ns <= 0:
        raise XmdBackendError("received_time_invalid", "华鑫 XMD 接收时间必须大于零")
    received_timestamp = receive_ns / 1_000_000_000.0
    current = time.time() if now is None else float(now)
    if source_timestamp - current > 1.0 or received_timestamp - current > 1.0:
        raise XmdBackendError("tick_time_in_future", "华鑫 XMD 行情时间明显晚于本机时间")

    last_price = _finite_number(payload, "Last")
    bid_price1 = _finite_number(payload, "Bid1")
    ask_price1 = _finite_number(payload, "Ask1")
    if last_price <= 0 or bid_price1 <= 0 or ask_price1 <= 0:
        raise XmdBackendError("tick_price_invalid", "华鑫 XMD 最新价和买卖一价必须为正")
    if bid_price1 > ask_price1:
        raise XmdBackendError("tick_spread_invalid", "华鑫 XMD 买一价不能高于卖一价")

    age_seconds = max(
        0.0,
        current - source_timestamp,
        current - received_timestamp,
    )
    high_limit = _finite_number(payload, "UpperLimit")
    low_limit = _finite_number(payload, "LowerLimit")
    return {
        "security": canonical,
        "sid": canonical,
        "raw_security": code,
        "exchange": "XSHG" if exchange == "SSE" else "XSHE",
        "last_price": last_price,
        "price": last_price,
        "bid_price1": bid_price1,
        "ask_price1": ask_price1,
        "bid_volume1": _non_negative_integer(payload, "BidVolume1"),
        "ask_volume1": _non_negative_integer(payload, "AskVolume1"),
        "high_limit": high_limit if high_limit > 0 else None,
        "low_limit": low_limit if low_limit > 0 else None,
        "volume": _non_negative_integer(payload, "Volume"),
        "turnover": _finite_number(payload, "Turnover"),
        "trading_day": str(payload.get("TradingDay") or ""),
        "update_time": str(payload.get("UpdateTime") or ""),
        "update_millisec": int(payload.get("Millisec")),
        "source_time": datetime.fromtimestamp(source_timestamp, _SHANGHAI_TZ).isoformat(),
        "received_time": datetime.fromtimestamp(received_timestamp, _SHANGHAI_TZ).isoformat(),
        "age_seconds": age_seconds,
        "source": HUAXIN_XMD_SOURCE,
        "provider": HUAXIN_XMD_SOURCE,
    }


class Python37XmdBackend:
    """通过固定 JSONL 子进程提供只读华鑫 L1 快照缓存。"""

    def __init__(
        self,
        *,
        python_path: str,
        sdk_dir: str,
        front: str,
        max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS,
        connect_timeout: float = 15.0,
        command_timeout: float = 5.0,
        sidecar_path: Optional[str] = None,
    ) -> None:
        """保存显式路径和时效门禁，但不启动任何进程。

        Args:
            python_path: Python 3.7 可执行文件绝对路径。
            sdk_dir: XMD SDK 绝对目录。
            front: 当前生产或仿真环境的 L1 TCP 前置。
            max_age_seconds: 快照最大允许年龄。
            connect_timeout: 等待 sidecar 登录 ready 的秒数。
            command_timeout: 等待订阅/退订回执的秒数。
            sidecar_path: 测试可注入的 sidecar 绝对路径。

        Returns:
            None。

        Raises:
            XmdBackendError: 配置值不满足固定安全边界时抛出。
        """

        self.python_path = self._absolute_file(python_path, "python_path")
        self.sdk_dir = self._absolute_directory(sdk_dir, "sdk_dir")
        self.sidecar_path = self._absolute_file(
            sidecar_path or str(Path(__file__).with_name("xmd_sidecar.py")),
            "sidecar_path",
        )
        self.front = _validate_tcp_front(front)
        self.max_age_seconds = self._positive_float(max_age_seconds, "max_age_seconds")
        self.connect_timeout = self._positive_float(connect_timeout, "connect_timeout")
        self.command_timeout = self._positive_float(command_timeout, "command_timeout")
        self._process: Optional[subprocess.Popen] = None
        self._stdin: Optional[TextIO] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._condition = threading.Condition()
        self._write_lock = threading.Lock()
        self._ready = False
        self._running = False
        self._degraded = False
        self._api_version = ""
        self._pending: Dict[str, Optional[Dict[str, Any]]] = {}
        self._latest: Dict[str, Dict[str, Any]] = {}
        self._subscriptions: Set[str] = set()
        self._last_error_code: Optional[str] = None
        self._last_error_message: Optional[str] = None
        self._invalid_events = 0
        self._last_event_time: Optional[str] = None

    @staticmethod
    def _absolute_file(value: str, field: str) -> str:
        """校验一个必须存在的绝对普通文件路径。

        Args:
            value: 待校验路径。
            field: 用于稳定错误消息的字段名。

        Returns:
            str: 解析后的绝对真实路径。

        Raises:
            XmdBackendError: 路径不是绝对已存在文件时抛出。
        """

        text = str(value or "").strip()
        if not os.path.isabs(text):
            raise XmdBackendError("path_invalid", f"华鑫 XMD {field} 必须为绝对路径")
        resolved = os.path.realpath(text)
        if not os.path.isfile(resolved):
            raise XmdBackendError("path_invalid", f"华鑫 XMD {field} 文件不存在")
        return resolved

    @staticmethod
    def _absolute_directory(value: str, field: str) -> str:
        """校验一个必须存在的绝对目录路径。

        Args:
            value: 待校验路径。
            field: 用于稳定错误消息的字段名。

        Returns:
            str: 解析后的绝对真实路径。

        Raises:
            XmdBackendError: 路径不是绝对已存在目录时抛出。
        """

        text = str(value or "").strip()
        if not os.path.isabs(text):
            raise XmdBackendError("path_invalid", f"华鑫 XMD {field} 必须为绝对路径")
        resolved = os.path.realpath(text)
        if not os.path.isdir(resolved):
            raise XmdBackendError("path_invalid", f"华鑫 XMD {field} 目录不存在")
        return resolved

    @staticmethod
    def _positive_float(value: Any, field: str) -> float:
        """把配置转换为严格正有限浮点数。

        Args:
            value: 原始配置值。
            field: 用于稳定错误消息的字段名。

        Returns:
            float: 正有限浮点值。

        Raises:
            XmdBackendError: 值非法、非有限或不大于零时抛出。
        """

        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise XmdBackendError("config_invalid", f"华鑫 XMD {field} 必须为正数") from exc
        if not math.isfinite(parsed) or parsed <= 0:
            raise XmdBackendError("config_invalid", f"华鑫 XMD {field} 必须为正数")
        return parsed

    def start(self) -> None:
        """以固定 argv 启动 sidecar，并等待登录 ready。

        Returns:
            None。

        Raises:
            XmdBackendError: 子进程创建失败、登录超时或协议错误时抛出。

        Side Effects:
            创建一个只读 Python 3.7 子进程和一个 stdout reader 线程。
        """

        with self._condition:
            if self._running:
                return
            self._ready = False
            self._degraded = False
            self._last_error_code = None
            self._last_error_message = None
        environment = {key: os.environ[key] for key in _ALLOWED_ENVIRONMENT if key in os.environ}
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        environment["PYTHONUNBUFFERED"] = "1"
        argv = [
            self.python_path,
            self.sidecar_path,
            "--sdk-dir",
            self.sdk_dir,
            "--front",
            self.front,
        ]
        try:
            process = subprocess.Popen(
                argv,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                errors="strict",
                bufsize=1,
                shell=False,
                cwd=os.path.dirname(self.sidecar_path),
                env=environment,
            )
        except OSError as exc:
            raise XmdBackendError("sidecar_start_failed", "华鑫 XMD sidecar 无法启动") from exc
        if process.stdin is None or process.stdout is None:
            process.kill()
            raise XmdBackendError("sidecar_pipe_failed", "华鑫 XMD sidecar 管道创建失败")
        with self._condition:
            self._process = process
            self._stdin = process.stdin
            self._running = True
            self._reader_thread = threading.Thread(
                target=self._read_stdout,
                args=(process.stdout,),
                name="huaxin-xmd-jsonl-reader",
                daemon=True,
            )
            self._reader_thread.start()
            deadline = time.monotonic() + self.connect_timeout
            while not self._ready and self._running:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(remaining)
            ready = self._ready and self._running
            error_code = self._last_error_code
            error_message = self._last_error_message
        if ready:
            return
        self.stop()
        if error_code:
            raise XmdBackendError(error_code, error_message or "华鑫 XMD sidecar 启动失败")
        raise XmdBackendError("sidecar_ready_timeout", "等待华鑫 XMD 登录就绪超时")

    def stop(self) -> None:
        """幂等请求 sidecar 停止，并有界回收子进程和 reader 线程。

        Returns:
            None。

        Side Effects:
            最佳努力发送 stop；超时后只终止本对象创建的子进程。
        """

        with self._condition:
            process = self._process
            running = self._running
        if process is None:
            return
        if running and process.poll() is None:
            try:
                self._request("stop", timeout=min(self.command_timeout, 2.0))
            except XmdBackendError:
                pass
        stdin = self._stdin
        if stdin is not None:
            try:
                stdin.close()
            except OSError:
                pass
        try:
            process.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1.0)
        reader = self._reader_thread
        if reader is not None and reader is not threading.current_thread():
            reader.join(timeout=1.0)
        with self._condition:
            self._running = False
            self._ready = False
            self._process = None
            self._stdin = None
            self._reader_thread = None
            self._pending.clear()
            self._subscriptions.clear()
            self._condition.notify_all()

    def subscribe(self, security: str) -> Dict[str, Any]:
        """订阅一个标准证券，并等待厂商逐项成功回执。

        Args:
            security: 标准证券代码。

        Returns:
            Dict[str, Any]: sidecar 的订阅确认响应。

        Raises:
            XmdBackendError: backend 未就绪、回执失败或超时时抛出。
        """

        canonical, code, exchange = _normalise_security(security)
        with self._condition:
            if canonical in self._subscriptions:
                return {
                    "type": "response",
                    "op": "subscribe",
                    "ok": True,
                    "security": code,
                    "exchange": exchange,
                    "active": True,
                }
        response = self._request(
            "subscribe",
            security=code,
            exchange=exchange,
        )
        if not bool(response.get("ok")) or not bool(response.get("active")):
            raise XmdBackendError("subscription_failed", "华鑫 XMD 订阅未得到成功确认")
        with self._condition:
            self._subscriptions.add(canonical)
        return response

    def unsubscribe(self, security: str) -> Dict[str, Any]:
        """退订一个标准证券，并等待厂商逐项成功回执。

        Args:
            security: 标准证券代码。

        Returns:
            Dict[str, Any]: sidecar 的退订确认响应。

        Raises:
            XmdBackendError: backend 未就绪、回执失败或超时时抛出。
        """

        canonical, code, exchange = _normalise_security(security)
        with self._condition:
            if canonical not in self._subscriptions:
                return {
                    "type": "response",
                    "op": "unsubscribe",
                    "ok": True,
                    "security": code,
                    "exchange": exchange,
                    "active": False,
                }
        response = self._request(
            "unsubscribe",
            security=code,
            exchange=exchange,
        )
        if not bool(response.get("ok")) or bool(response.get("active")):
            raise XmdBackendError("unsubscription_failed", "华鑫 XMD 退订未得到成功确认")
        with self._condition:
            self._subscriptions.discard(canonical)
        return response

    def get_latest(self, security: str, wait_timeout: float = 0.0) -> Dict[str, Any]:
        """返回指定证券的新鲜 L1 快照，不允许历史或跨源回退。

        Args:
            security: 标准证券代码。
            wait_timeout: 无缓存时等待首条 tick 的最大秒数。

        Returns:
            Dict[str, Any]: 重新计算 age_seconds 后的新鲜快照副本。

        Raises:
            XmdBackendError: 无快照、来源异常或快照过期时抛出。
        """

        canonical, _, _ = _normalise_security(security)
        deadline = time.monotonic() + max(0.0, float(wait_timeout))
        with self._condition:
            while canonical not in self._latest and self._running:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(remaining)
            snapshot = dict(self._latest.get(canonical) or {})
        if not snapshot:
            raise XmdBackendError("snapshot_unavailable", "华鑫 XMD 尚无目标证券快照")
        if snapshot.get("source") != HUAXIN_XMD_SOURCE:
            raise XmdBackendError("snapshot_source_invalid", "华鑫 XMD 缓存来源标记非法")
        try:
            source_timestamp = datetime.fromisoformat(str(snapshot["source_time"])).timestamp()
            received_timestamp = datetime.fromisoformat(str(snapshot["received_time"])).timestamp()
        except (KeyError, TypeError, ValueError) as exc:
            raise XmdBackendError("snapshot_time_invalid", "华鑫 XMD 缓存缺少有效时间") from exc
        current = time.time()
        age_seconds = max(0.0, current - source_timestamp, current - received_timestamp)
        snapshot["age_seconds"] = age_seconds
        if age_seconds > self.max_age_seconds:
            raise XmdBackendError("snapshot_stale", "华鑫 XMD 快照超过允许时效")
        return snapshot

    def health(self) -> Dict[str, Any]:
        """返回不包含前置、路径和凭据的 parent/backend 健康快照。

        Returns:
            Dict[str, Any]: 连接、登录、订阅、缓存和错误计数。
        """

        with self._condition:
            process_alive = bool(
                self._process is not None and self._process.poll() is None and self._running
            )
            ready = process_alive and self._ready and not self._degraded
            return {
                "backend": "python37_sidecar",
                "source": HUAXIN_XMD_SOURCE,
                "process_alive": process_alive,
                "connected": bool(self._ready and process_alive),
                "logged_in": bool(self._ready and process_alive),
                "ready": ready,
                "state": "ready" if ready else ("degraded" if process_alive else "unavailable"),
                "api_version": self._api_version,
                "subscriptions": len(self._subscriptions),
                "cached_symbols": len(self._latest),
                "invalid_events": self._invalid_events,
                "last_event_time": self._last_event_time,
                "last_error_code": self._last_error_code,
                "last_error_message": self._last_error_message,
            }

    def _request(
        self,
        op: str,
        *,
        timeout: Optional[float] = None,
        security: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> Dict[str, Any]:
        """发送一条固定字段 JSONL 命令并等待相同 request_id 响应。

        Args:
            op: subscribe、unsubscribe、health 或 stop。
            timeout: 可选单次等待秒数。
            security: 订阅命令的六位代码。
            exchange: 订阅命令的 SSE/SZSE。

        Returns:
            Dict[str, Any]: sidecar response 对象。

        Raises:
            XmdBackendError: backend 未运行、管道失败、响应错误或超时时抛出。
        """

        request_id = uuid.uuid4().hex
        command: Dict[str, Any] = {"op": op, "request_id": request_id}
        if op in {"subscribe", "unsubscribe"}:
            command["security"] = security
            command["exchange"] = exchange
        with self._condition:
            if not self._running or self._stdin is None:
                raise XmdBackendError("backend_not_running", "华鑫 XMD backend 尚未运行")
            self._pending[request_id] = None
            stream = self._stdin
        line = json.dumps(command, ensure_ascii=False, separators=(",", ":")) + "\n"
        try:
            with self._write_lock:
                stream.write(line)
                stream.flush()
        except (BrokenPipeError, OSError, ValueError) as exc:
            with self._condition:
                self._pending.pop(request_id, None)
            raise XmdBackendError("sidecar_write_failed", "华鑫 XMD sidecar 命令写入失败") from exc

        wait_seconds = self.command_timeout if timeout is None else max(0.01, float(timeout))
        deadline = time.monotonic() + wait_seconds
        with self._condition:
            while self._pending.get(request_id) is None and self._running:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(remaining)
            response = self._pending.pop(request_id, None)
        if response is None:
            raise XmdBackendError("sidecar_response_timeout", "等待华鑫 XMD 命令响应超时")
        if response.get("type") == "error":
            raise XmdBackendError(
                str(response.get("code") or "sidecar_error"),
                str(response.get("message") or "华鑫 XMD sidecar 返回错误"),
            )
        return response

    def _read_stdout(self, stream: TextIO) -> None:
        """持续读取 sidecar stdout，并把 JSON 事件转入线程安全状态。

        Args:
            stream: Popen stdout 文本流。

        Returns:
            None。

        Side Effects:
            更新 readiness、pending response 和最新快照缓存；不执行用户 callback。
        """

        try:
            for line in stream:
                try:
                    payload = json.loads(line)
                    if not isinstance(payload, dict):
                        raise ValueError("JSON 顶层不是对象")
                    self._handle_message(payload)
                except (UnicodeError, ValueError, XmdBackendError) as exc:
                    code = (
                        exc.code if isinstance(exc, XmdBackendError) else "sidecar_protocol_invalid"
                    )
                    message = (
                        exc.message if isinstance(exc, XmdBackendError) else "华鑫 XMD sidecar 输出协议非法"
                    )
                    with self._condition:
                        self._invalid_events += 1
                        self._degraded = True
                        self._last_error_code = code
                        self._last_error_message = message
                        self._condition.notify_all()
        finally:
            try:
                stream.close()
            except OSError:
                pass
            with self._condition:
                self._running = False
                self._ready = False
                self._condition.notify_all()

    def _handle_message(self, payload: Mapping[str, Any]) -> None:
        """校验并消费一个 ready/response/tick/error JSON 对象。

        Args:
            payload: sidecar stdout 中的一条 JSON 对象。

        Returns:
            None。

        Raises:
            XmdBackendError: 事件类型或字段不满足冻结协议时抛出。
        """

        event_type = str(payload.get("type") or "")
        if event_type == "ready":
            if not bool(payload.get("running")) or not bool(payload.get("logged_in")):
                raise XmdBackendError("sidecar_ready_invalid", "华鑫 XMD ready 未证明登录成功")
            with self._condition:
                self._api_version = str(payload.get("api_version") or "")
                self._ready = True
                self._condition.notify_all()
            return
        if event_type == "response":
            request_id = str(payload.get("request_id") or "")
            with self._condition:
                if request_id not in self._pending:
                    raise XmdBackendError(
                        "response_request_unknown",
                        "华鑫 XMD response 无法关联已发送请求",
                    )
                self._pending[request_id] = dict(payload)
                self._condition.notify_all()
            return
        if event_type == "tick":
            snapshot = _normalise_tick(payload)
            with self._condition:
                self._latest[snapshot["security"]] = snapshot
                self._last_event_time = snapshot["received_time"]
                self._condition.notify_all()
            return
        if event_type == "error":
            code = str(payload.get("code") or "sidecar_error")
            message = str(payload.get("message") or "华鑫 XMD sidecar 返回错误")
            request_id = str(payload.get("request_id") or "")
            with self._condition:
                self._last_error_code = code
                self._last_error_message = message
                if code in {"callback_queue_overflow", "front_disconnected", "login_failed"}:
                    self._degraded = True
                if request_id in self._pending:
                    self._pending[request_id] = dict(payload)
                self._condition.notify_all()
            return
        raise XmdBackendError("event_type_invalid", "华鑫 XMD sidecar 返回未知事件类型")


__all__ = [
    "DEFAULT_MAX_AGE_SECONDS",
    "HUAXIN_XMD_SOURCE",
    "Python37XmdBackend",
    "XmdBackend",
    "XmdBackendError",
]
