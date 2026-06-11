"""
作者: BruceLee
日期: 2026-06-09
文件说明:
    QMT 可用性保护器。
    主要输入为 QMT broker/data 调用结果、环境变量中的退避参数和服务端重连探针事件。
    主要输出为 readiness 快照、受控错误码和下一次探测时间。
    上游由 QMT server adapter、session 握手和 health 入口调用；下游保护 xtquant/xtdata。
    关键约定: 普通请求不负责无限重连；只有受控探针或 cooldown 后的有限尝试能触碰 QMT。
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


class QmtGuardState:
    """QMT availability guard 的状态常量。

    职责:
        用字符串常量表达 QMT 当前可用性，避免在协议和日志中暴露 enum 细节。
    核心协作对象:
        QmtAvailabilityGuard、server health、session handshake。
    关键状态:
        READY 表示请求可进入 QMT；COOLDOWN/CONNECTING 表示请求应快速失败。
    """

    READY = "ready"
    CONNECTING = "connecting"
    COOLDOWN = "cooldown"
    UNAVAILABLE = "unavailable"
    DISABLED = "disabled"


@dataclass
class QmtGuardConfig:
    """QMT guard 配置。

    职责:
        保存重试退避、探测间隔和保护阈值。
    核心协作对象:
        QmtAvailabilityGuard。
    关键状态:
        initial_delay_seconds 为首次失败后的 cooldown；max_delay_seconds 为退避上限。
    """

    initial_delay_seconds: float = 5.0
    max_delay_seconds: float = 300.0
    backoff_multiplier: float = 2.0
    ready_poll_seconds: float = 5.0
    tcp_pressure_threshold: int = 12000


class QmtGuardError(RuntimeError):
    """QMT guard 受控错误。

    职责:
        将 QMT 不可用状态转换为可机器识别的协议错误。
    核心协作对象:
        ClientSession、QmtDataAdapter、QmtBrokerAdapter。
    关键状态:
        code 为协议错误码；state/retry_after_seconds 便于客户端和日志判断恢复时机。
    """

    def __init__(
        self,
        message: str,
        *,
        code: str,
        state: str,
        retry_after_seconds: Optional[float] = None,
    ) -> None:
        """初始化 QMT 受控错误。

        Args:
            message: 对人可读的错误信息。
            code: 对客户端稳定的错误码。
            state: 当前 QMT guard 状态。
            retry_after_seconds: 距离下次探测的秒数，未知时为 None。

        Returns:
            None。

        Side Effects:
            初始化 RuntimeError 的 message，并保存协议字段。
        """

        super().__init__(message)
        self.code = code
        self.state = state
        self.retry_after_seconds = retry_after_seconds


class QmtUnavailableError(QmtGuardError):
    """QMT 当前不可用错误。

    职责:
        表达 QMT 已知不可用或处于 cooldown。
    核心协作对象:
        server session 错误映射和 adapter 请求保护。
    关键状态:
        code 固定为 QMT_UNAVAILABLE。
    """

    def __init__(
        self,
        message: str,
        *,
        state: str = QmtGuardState.UNAVAILABLE,
        retry_after_seconds: Optional[float] = None,
    ) -> None:
        """初始化 QMT 不可用错误。

        Args:
            message: 对人可读的错误信息。
            state: 当前 guard 状态。
            retry_after_seconds: 距离下次探测的秒数。

        Returns:
            None。
        """

        super().__init__(
            message,
            code="QMT_UNAVAILABLE",
            state=state,
            retry_after_seconds=retry_after_seconds,
        )


class QmtReconnectingError(QmtGuardError):
    """QMT 正在重连错误。

    职责:
        表达已有单飞探针正在连接 QMT，普通请求不应再触发连接。
    核心协作对象:
        server session 错误映射和 adapter 请求保护。
    关键状态:
        code 固定为 QMT_RECONNECTING。
    """

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: Optional[float] = None,
    ) -> None:
        """初始化 QMT 重连中错误。

        Args:
            message: 对人可读的错误信息。
            retry_after_seconds: 距离下次探测的秒数。

        Returns:
            None。
        """

        super().__init__(
            message,
            code="QMT_RECONNECTING",
            state=QmtGuardState.CONNECTING,
            retry_after_seconds=retry_after_seconds,
        )


def _read_float_env(name: str, default: float) -> float:
    """读取浮点数环境变量。

    Args:
        name: 环境变量名称。
        default: 缺失或非法时使用的默认值。

    Returns:
        float: 解析后的浮点数。
    """

    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _read_int_env(name: str, default: int) -> int:
    """读取整数环境变量。

    Args:
        name: 环境变量名称。
        default: 缺失或非法时使用的默认值。

    Returns:
        int: 解析后的整数。
    """

    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def load_qmt_guard_config() -> QmtGuardConfig:
    """从环境变量加载 QMT guard 配置。

    Args:
        None。

    Returns:
        QmtGuardConfig: 完整的 guard 配置。

    Environment:
        QMT_GUARD_INITIAL_DELAY_SECONDS、QMT_GUARD_MAX_DELAY_SECONDS、
        QMT_GUARD_BACKOFF_MULTIPLIER、QMT_GUARD_READY_POLL_SECONDS、
        QMT_GUARD_TCP_PRESSURE_THRESHOLD。
    """

    initial = max(0.0, _read_float_env("QMT_GUARD_INITIAL_DELAY_SECONDS", 5.0))
    max_delay = max(initial, _read_float_env("QMT_GUARD_MAX_DELAY_SECONDS", 300.0))
    multiplier = max(1.0, _read_float_env("QMT_GUARD_BACKOFF_MULTIPLIER", 2.0))
    ready_poll = max(0.5, _read_float_env("QMT_GUARD_READY_POLL_SECONDS", 5.0))
    threshold = max(1, _read_int_env("QMT_GUARD_TCP_PRESSURE_THRESHOLD", 12000))
    return QmtGuardConfig(
        initial_delay_seconds=initial,
        max_delay_seconds=max_delay,
        backoff_multiplier=multiplier,
        ready_poll_seconds=ready_poll,
        tcp_pressure_threshold=threshold,
    )


def is_qmt_connectivity_error(exc: BaseException) -> bool:
    """判断异常是否像 QMT/xtdata 连接类故障。

    Args:
        exc: 待判断的异常对象。

    Returns:
        bool: True 表示应视为 QMT 可用性故障；False 表示更可能是业务参数或数据错误。
    """

    text = str(exc).lower()
    exc_name = type(exc).__name__.lower()
    module = getattr(type(exc), "__module__", "").lower()
    if "xtquant" in module or "xtdata" in module:
        return True
    markers = (
        "xtquant",
        "xtdata",
        "winerror 10061",
        "winerror 10060",
        "connection refused",
        "connection reset",
        "connection aborted",
        "connection timed out",
        "timed out",
        "timeout",
        "broken pipe",
        "not connected",
        "no connection",
        "无法连接",
        "连接失败",
        "连接超时",
        "连接被拒绝",
        "连接已断开",
        "qmt 服务",
        "qmt服务",
        "服务不可用",
        "服务异常",
        "socket",
        "synsent",
    )
    if any(marker in text for marker in markers):
        return True
    return "connection" in exc_name or "timeout" in exc_name


class QmtAvailabilityGuard:
    """QMT 可用性保护器。

    职责:
        集中管理 QMT readiness、cooldown、单飞探针和受控错误。
    核心协作对象:
        QmtBrokerAdapter、QmtDataAdapter、ServerApplication、ClientSession。
    关键状态:
        state 表示当前 readiness；next_probe_at 控制下一次允许触碰 QMT 的时间。
    """

    def __init__(
        self,
        *,
        config: Optional[QmtGuardConfig] = None,
        name: str = "qmt",
        initial_state: str = QmtGuardState.READY,
    ) -> None:
        """初始化 QMT 可用性保护器。

        Args:
            config: guard 配置，None 时使用环境变量默认值。
            name: 日志和 health 中显示的 guard 名称。
            initial_state: 初始状态，server 启动探针前通常会重置为 unavailable。

        Returns:
            None。
        """

        self.config = config or load_qmt_guard_config()
        self.name = name
        self.state = initial_state
        self.failure_count = 0
        self.last_error: Optional[str] = None
        self.last_error_type: Optional[str] = None
        self.last_error_at: Optional[float] = None
        self.next_probe_at = 0.0
        self._current_delay = max(0.0, self.config.initial_delay_seconds)
        self._probe_lock = asyncio.Lock()

    @property
    def ready(self) -> bool:
        """返回 QMT 是否 ready。

        Args:
            None。

        Returns:
            bool: 当前状态是否为 ready。
        """

        return self.state == QmtGuardState.READY

    @property
    def connecting(self) -> bool:
        """返回 QMT 是否正在重连。

        Args:
            None。

        Returns:
            bool: 当前状态是否为 connecting。
        """

        return self.state == QmtGuardState.CONNECTING

    def seconds_until_probe(self, now: Optional[float] = None) -> float:
        """计算距离下一次探测的秒数。

        Args:
            now: 当前 monotonic 时间，None 时自动读取。

        Returns:
            float: 剩余秒数，已到期时为 0。
        """

        current = time.monotonic() if now is None else now
        return max(0.0, self.next_probe_at - current)

    def is_probe_due(self, now: Optional[float] = None) -> bool:
        """判断是否允许开始下一次探针。

        Args:
            now: 当前 monotonic 时间，None 时自动读取。

        Returns:
            bool: cooldown 是否已经到期且当前不是 ready/disabled。
        """

        if self.state in (QmtGuardState.READY, QmtGuardState.DISABLED):
            return False
        return self.seconds_until_probe(now) <= 0

    def schedule_probe_now(self, reason: str) -> None:
        """立即安排下一次探针。

        Args:
            reason: 状态变更原因，用于 health 展示。

        Returns:
            None。

        Side Effects:
            将状态置为 unavailable，并把 next_probe_at 设为当前时间。
        """

        self.state = QmtGuardState.UNAVAILABLE
        self.last_error = reason
        self.last_error_type = "RuntimeError"
        self.last_error_at = time.time()
        self.next_probe_at = time.monotonic()

    def mark_ready(self) -> None:
        """标记 QMT 已恢复 ready。

        Args:
            None。

        Returns:
            None。

        Side Effects:
            清空失败计数和最近错误，并重置退避间隔。
        """

        self.state = QmtGuardState.READY
        self.failure_count = 0
        self.last_error = None
        self.last_error_type = None
        self.last_error_at = None
        self.next_probe_at = 0.0
        self._current_delay = max(0.0, self.config.initial_delay_seconds)

    def mark_disabled(self, reason: str = "QMT 功能未启用") -> None:
        """标记 QMT guard 为 disabled。

        Args:
            reason: 禁用原因。

        Returns:
            None。
        """

        self.state = QmtGuardState.DISABLED
        self.last_error = reason
        self.last_error_type = "Disabled"
        self.last_error_at = time.time()
        self.next_probe_at = 0.0

    def mark_failure(self, exc: BaseException, *, delay: Optional[float] = None) -> None:
        """记录一次 QMT 失败并进入 cooldown。

        Args:
            exc: 触发失败的异常。
            delay: 覆盖本次 cooldown 秒数，None 时使用当前退避值。

        Returns:
            None。

        Side Effects:
            更新状态、失败次数、最近错误和下一次探针时间。
        """

        self.failure_count += 1
        self.last_error = str(exc)
        self.last_error_type = type(exc).__name__
        self.last_error_at = time.time()
        cooldown = self._current_delay if delay is None else max(0.0, float(delay))
        self.next_probe_at = time.monotonic() + cooldown
        self.state = QmtGuardState.COOLDOWN
        self._current_delay = min(
            self.config.max_delay_seconds,
            max(
                self.config.initial_delay_seconds,
                self._current_delay * self.config.backoff_multiplier,
            ),
        )

    def ensure_ready(self) -> None:
        """确保当前 QMT ready，否则抛出受控错误。

        Args:
            None。

        Returns:
            None。

        Raises:
            QmtGuardError: QMT 当前不可用或正在重连。
        """

        if self.state == QmtGuardState.READY:
            return
        raise self.build_error()

    def build_error(self) -> QmtGuardError:
        """构造当前状态对应的受控错误。

        Args:
            None。

        Returns:
            QmtGuardError: 当前状态对应的错误对象。
        """

        retry_after = self.seconds_until_probe()
        if self.state == QmtGuardState.CONNECTING:
            return QmtReconnectingError("QMT 正在重连，请稍后重试", retry_after_seconds=retry_after)
        message = "QMT 当前不可用"
        if self.last_error:
            message = f"{message}: {self.last_error}"
        if retry_after > 0:
            message = f"{message}，约 {retry_after:.1f}s 后重试"
        return QmtUnavailableError(
            message,
            state=self.state,
            retry_after_seconds=retry_after,
        )

    async def acquire_probe(self) -> bool:
        """尝试获取单飞探针权限。

        Args:
            None。

        Returns:
            bool: True 表示调用方获得探针权限；False 表示当前不应探测。

        Side Effects:
            成功时把状态置为 connecting，并持有内部锁，调用方必须 release_probe。
        """

        if not self.is_probe_due():
            return False
        if self._probe_lock.locked():
            return False
        await self._probe_lock.acquire()
        if not self.is_probe_due():
            self._probe_lock.release()
            return False
        self.state = QmtGuardState.CONNECTING
        return True

    def release_probe(self) -> None:
        """释放单飞探针权限。

        Args:
            None。

        Returns:
            None。

        Side Effects:
            如果当前持有探针锁，则释放它。
        """

        if self._probe_lock.locked():
            self._probe_lock.release()

    def snapshot(self) -> Dict[str, Any]:
        """返回 health 使用的 QMT guard 快照。

        Args:
            None。

        Returns:
            Dict[str, Any]: 包含 readiness、错误、重试时间和配置阈值的快照。
        """

        retry_after = self.seconds_until_probe()
        next_probe = None
        if self.next_probe_at > 0:
            next_probe = time.time() + retry_after
        return {
            "name": self.name,
            "ready": self.state == QmtGuardState.READY,
            "state": self.state,
            "failure_count": self.failure_count,
            "last_error": self.last_error,
            "last_error_type": self.last_error_type,
            "last_error_at": self.last_error_at,
            "retry_after_seconds": retry_after,
            "next_probe_at": next_probe,
            "tcp_pressure_threshold": self.config.tcp_pressure_threshold,
        }
