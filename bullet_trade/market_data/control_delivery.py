"""
作者: BruceLee

文件职责: 将行情队列的 gap/degraded 控制批次按显式 ACK 合同可靠交给下游 sink。
主要输入: BoundedMarketEventQueue 中的稳定 MarketEventControlDelivery 与 vendor-neutral sink。
主要输出: 单步投递结果、精确 ACK 后的队列释放动作和有界诊断指标快照。
上游关系: 由 native drain、server writer 或本地 Feed 适配层主动调用单步 dispatcher。
下游关系: sink 可连接 EventBus、RealtimeMarketDataFeed 或远程 writer，但本模块不联网。
关键配置约定: 本模块不启动后台线程、不消费普通数据；调用方必须先循环投递控制直到
NO_CONTROL，任一失败立即停止本轮，之后才可调用 queue.drain_data；队列同时拒绝控制
未 ACK 时的普通数据 drain。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from threading import Lock, RLock
from typing import Optional, Protocol

from .queue import BoundedMarketEventQueue, MarketEventControlAckError, MarketEventControlDelivery


class MarketControlDispatchOutcome(str, Enum):
    """枚举一次控制投递调用的稳定终态。

    由 ``ReliableMarketControlDispatcher`` 返回；只有 ``ACKED`` 表示当前 delivery 已从
    队列释放，失败终态均要求调用方停止普通数据 drain 并稍后重试。
    """

    NO_CONTROL = "no_control"
    ACKED = "acked"
    DISPATCH_IN_PROGRESS = "dispatch_in_progress"
    SINK_NOT_ACKNOWLEDGED = "sink_not_acknowledged"
    SINK_ACK_MISMATCH = "sink_ack_mismatch"
    SINK_PROTOCOL_ERROR = "sink_protocol_error"
    SINK_EXCEPTION = "sink_exception"
    QUEUE_ACK_ERROR = "queue_ack_error"


@dataclass(frozen=True)
class MarketControlSinkAck:
    """保存 sink 对一个精确 delivery 的显式确认。

    sink 必须回显收到的 delivery ID；``acknowledged=False`` 只表示本次未确认，dispatcher
    不会据此释放队列。detail 只能包含已脱敏且有界的诊断文本。
    """

    MAX_DELIVERY_ID_LENGTH = 256
    MAX_DETAIL_LENGTH = 512

    delivery_id: str
    acknowledged: bool
    detail: Optional[str] = None

    def __post_init__(self) -> None:
        """校验 ACK 标识、布尔值和诊断文本长度。

        Args:
            无；输入来自 dataclass 字段。

        Returns:
            None。

        Raises:
            ValueError: delivery ID、acknowledged 或 detail 不符合有界合同时抛出。

        Side Effects:
            规范化 delivery ID 与可选 detail；不访问队列或 sink。
        """
        if not isinstance(self.delivery_id, str) or not self.delivery_id.strip():
            raise ValueError("delivery_id 必须是非空字符串")
        delivery_id = self.delivery_id.strip()
        if len(delivery_id) > self.MAX_DELIVERY_ID_LENGTH:
            raise ValueError("delivery_id 超过固定长度上限")
        if not isinstance(self.acknowledged, bool):
            raise ValueError("acknowledged 必须是 bool")
        detail = self.detail
        if detail is not None:
            if not isinstance(detail, str):
                raise ValueError("detail 必须是字符串或 None")
            detail = detail.strip()
            if len(detail) > self.MAX_DETAIL_LENGTH:
                raise ValueError("detail 超过固定长度上限")
            if not detail:
                detail = None
        object.__setattr__(self, "delivery_id", delivery_id)
        object.__setattr__(self, "detail", detail)


class MarketControlSink(Protocol):
    """定义 vendor-neutral 控制批次 sink 合同。

    实现方可同步发布到 Feed、EventBus 或网络 writer；必须在全部事件已可靠接受后才返回
    acknowledged=True，且不得把部分成功伪装成 ACK。
    """

    def publish_control(
        self,
        delivery: MarketEventControlDelivery,
    ) -> MarketControlSinkAck:
        """同步发布一个不可变控制 delivery 并返回显式确认。

        Args:
            delivery: queue.take_control 返回的稳定、未 ACK 控制批次。

        Returns:
            MarketControlSinkAck: 回显精确 delivery ID 的确认或未确认结果。

        Raises:
            Exception: sink 失败时可抛出；dispatcher 会转为有界诊断并保留 delivery。

        Side Effects:
            由具体 sink 定义；实现方不得直接调用 queue.ack_control。
        """
        ...


@dataclass(frozen=True)
class MarketControlDispatchResult:
    """保存一次单步可靠控制投递的不可变结果。

    结果明确区分空队列、成功 ACK、sink 未确认、协议错误和异常；调用方只能在持续取得
    ``NO_CONTROL`` 后进入普通数据 drain 阶段。
    """

    outcome: MarketControlDispatchOutcome
    delivery_id: Optional[str]
    event_count: int
    retry: bool
    queue_acknowledged: bool
    retained_for_retry: bool
    diagnostic_code: Optional[str] = None
    diagnostic_message: Optional[str] = None


@dataclass(frozen=True)
class MarketControlDispatcherMetrics:
    """暴露 dispatcher 的有界累计计数和当前 delivery 诊断。

    指标不保存事件或异常对象，仅保留一个当前 delivery ID、最后终态和截断后的脱敏诊断。
    """

    dispatch_call_count: int
    dispatch_in_progress_count: int
    no_control_count: int
    delivery_attempt_count: int
    delivery_retry_count: int
    acknowledged_count: int
    sink_not_acknowledged_count: int
    sink_ack_mismatch_count: int
    sink_protocol_error_count: int
    sink_exception_count: int
    queue_ack_error_count: int
    inflight_delivery_id: Optional[str]
    last_outcome: Optional[MarketControlDispatchOutcome]
    last_diagnostic_code: Optional[str]
    last_diagnostic_message: Optional[str]


class ReliableMarketControlDispatcher:
    """按 at-least-once 语义同步投递队列控制批次。

    dispatcher 以非阻塞 single-flight gate 串行执行
    ``take_control → sink.publish_control → ack_control``；只有 sink 返回
    acknowledged=True 且 delivery ID 精确匹配时才 ACK 队列。重入或并发调用立即返回
    ``DISPATCH_IN_PROGRESS``，不会再次调用 sink 或 ACK。对象不创建线程、不联网、
    不加载 SDK，也不持有历史事件。调用方应循环调用 ``dispatch_control_once``，直至
    ``NO_CONTROL``；任何其他失败终态都必须停止本轮普通数据 drain。
    """

    MAX_DIAGNOSTIC_MESSAGE_LIMIT = 4096

    def __init__(
        self,
        queue: BoundedMarketEventQueue,
        sink: MarketControlSink,
        max_diagnostic_message_length: int = 256,
    ) -> None:
        """绑定一个有界队列和同步 sink。

        Args:
            queue: 提供 take_control/ack_control 合同的有界市场事件队列。
            sink: 实现 publish_control 的 vendor-neutral 同步 sink。
            max_diagnostic_message_length: 最后诊断文本的固定截断长度，范围 1 到 4096。

        Returns:
            None。

        Raises:
            TypeError: queue 类型不正确或 sink 没有可调用 publish_control 时抛出。
            ValueError: 诊断长度不是允许范围内的整数时抛出。

        Side Effects:
            仅创建进程内锁和零值计数器；不 take、publish、ACK 或启动后台任务。
        """
        if not isinstance(queue, BoundedMarketEventQueue):
            raise TypeError("queue 必须是 BoundedMarketEventQueue")
        if not callable(getattr(sink, "publish_control", None)):
            raise TypeError("sink 必须实现可调用的 publish_control")
        if (
            isinstance(max_diagnostic_message_length, bool)
            or not isinstance(max_diagnostic_message_length, int)
            or not 1 <= max_diagnostic_message_length <= self.MAX_DIAGNOSTIC_MESSAGE_LIMIT
        ):
            raise ValueError("max_diagnostic_message_length 必须是 1 到 4096 的整数")
        self._queue = queue
        self._sink = sink
        self._max_diagnostic_message_length = max_diagnostic_message_length
        self._lock = RLock()
        self._dispatch_gate = Lock()
        self._dispatch_call_count = 0
        self._dispatch_in_progress_count = 0
        self._no_control_count = 0
        self._delivery_attempt_count = 0
        self._delivery_retry_count = 0
        self._acknowledged_count = 0
        self._sink_not_acknowledged_count = 0
        self._sink_ack_mismatch_count = 0
        self._sink_protocol_error_count = 0
        self._sink_exception_count = 0
        self._queue_ack_error_count = 0
        self._inflight_delivery_id: Optional[str] = None
        self._inflight_event_count = 0
        self._last_outcome: Optional[MarketControlDispatchOutcome] = None
        self._last_diagnostic_code: Optional[str] = None
        self._last_diagnostic_message: Optional[str] = None

    def dispatch_control_once(self) -> MarketControlDispatchResult:
        """尝试同步发布当前一个控制 delivery。

        Args:
            无。

        Returns:
            MarketControlDispatchResult: 明确说明是否空队列、成功 ACK 或保留待重试。

        Side Effects:
            非空时调用 sink 一次；仅 exact acknowledged ACK 才调用 queue.ack_control。
            sink 未确认、ID 不匹配、协议错误或异常均保留当前 delivery 供下次重试。
        """
        if not self._dispatch_gate.acquire(blocking=False):
            with self._lock:
                self._dispatch_call_count += 1
                self._dispatch_in_progress_count += 1
                return self._result_locked(
                    MarketControlDispatchOutcome.DISPATCH_IN_PROGRESS,
                    delivery=None,
                    retry=False,
                    queue_acknowledged=False,
                    retained_for_retry=True,
                    diagnostic_code="CONTROL_DISPATCH_IN_PROGRESS",
                    delivery_id_override=self._inflight_delivery_id,
                    event_count_override=self._inflight_event_count,
                )

        try:
            with self._lock:
                self._dispatch_call_count += 1
            delivery = self._queue.take_control()
            if delivery is None:
                with self._lock:
                    self._no_control_count += 1
                    self._inflight_delivery_id = None
                    self._inflight_event_count = 0
                    return self._result_locked(
                        MarketControlDispatchOutcome.NO_CONTROL,
                        delivery=None,
                        retry=False,
                        queue_acknowledged=False,
                        retained_for_retry=False,
                    )

            with self._lock:
                retry = self._inflight_delivery_id == delivery.delivery_id
                if retry:
                    self._delivery_retry_count += 1
                self._inflight_delivery_id = delivery.delivery_id
                self._inflight_event_count = len(delivery.events)
                self._delivery_attempt_count += 1

            try:
                sink_ack = self._sink.publish_control(delivery)
            except Exception as exc:  # noqa: BLE001 - sink 边界必须保留 delivery 并转诊断
                with self._lock:
                    self._sink_exception_count += 1
                    return self._result_locked(
                        MarketControlDispatchOutcome.SINK_EXCEPTION,
                        delivery=delivery,
                        retry=retry,
                        queue_acknowledged=False,
                        retained_for_retry=True,
                        diagnostic_code="SINK_PUBLISH_EXCEPTION",
                        diagnostic_message=type(exc).__name__,
                    )

            if not isinstance(sink_ack, MarketControlSinkAck):
                with self._lock:
                    self._sink_protocol_error_count += 1
                    return self._result_locked(
                        MarketControlDispatchOutcome.SINK_PROTOCOL_ERROR,
                        delivery=delivery,
                        retry=retry,
                        queue_acknowledged=False,
                        retained_for_retry=True,
                        diagnostic_code="SINK_ACK_TYPE_INVALID",
                        diagnostic_message=type(sink_ack).__name__,
                    )

            if not sink_ack.acknowledged:
                with self._lock:
                    self._sink_not_acknowledged_count += 1
                    return self._result_locked(
                        MarketControlDispatchOutcome.SINK_NOT_ACKNOWLEDGED,
                        delivery=delivery,
                        retry=retry,
                        queue_acknowledged=False,
                        retained_for_retry=True,
                        diagnostic_code="SINK_NOT_ACKNOWLEDGED",
                        diagnostic_message=sink_ack.detail,
                    )

            if sink_ack.delivery_id != delivery.delivery_id:
                with self._lock:
                    self._sink_ack_mismatch_count += 1
                    return self._result_locked(
                        MarketControlDispatchOutcome.SINK_ACK_MISMATCH,
                        delivery=delivery,
                        retry=retry,
                        queue_acknowledged=False,
                        retained_for_retry=True,
                        diagnostic_code="SINK_ACK_DELIVERY_ID_MISMATCH",
                        diagnostic_message=(
                            f"expected={delivery.delivery_id}, " f"actual={sink_ack.delivery_id}"
                        ),
                    )

            try:
                self._queue.ack_control(delivery.delivery_id)
            except MarketEventControlAckError as exc:
                with self._lock:
                    self._queue_ack_error_count += 1
                    return self._result_locked(
                        MarketControlDispatchOutcome.QUEUE_ACK_ERROR,
                        delivery=delivery,
                        retry=retry,
                        queue_acknowledged=False,
                        retained_for_retry=True,
                        diagnostic_code=exc.code,
                        diagnostic_message=type(exc).__name__,
                    )

            with self._lock:
                self._acknowledged_count += 1
                self._inflight_delivery_id = None
                self._inflight_event_count = 0
                return self._result_locked(
                    MarketControlDispatchOutcome.ACKED,
                    delivery=delivery,
                    retry=retry,
                    queue_acknowledged=True,
                    retained_for_retry=False,
                )
        finally:
            self._dispatch_gate.release()

    def metrics(self) -> MarketControlDispatcherMetrics:
        """返回累计尝试、失败类型和当前 delivery 的不可变快照。

        Args:
            无。

        Returns:
            MarketControlDispatcherMetrics: 与同一锁时点一致的有界诊断指标。

        Side Effects:
            无；不触发 take、sink publish 或 ACK。
        """
        with self._lock:
            return MarketControlDispatcherMetrics(
                dispatch_call_count=self._dispatch_call_count,
                dispatch_in_progress_count=self._dispatch_in_progress_count,
                no_control_count=self._no_control_count,
                delivery_attempt_count=self._delivery_attempt_count,
                delivery_retry_count=self._delivery_retry_count,
                acknowledged_count=self._acknowledged_count,
                sink_not_acknowledged_count=self._sink_not_acknowledged_count,
                sink_ack_mismatch_count=self._sink_ack_mismatch_count,
                sink_protocol_error_count=self._sink_protocol_error_count,
                sink_exception_count=self._sink_exception_count,
                queue_ack_error_count=self._queue_ack_error_count,
                inflight_delivery_id=self._inflight_delivery_id,
                last_outcome=self._last_outcome,
                last_diagnostic_code=self._last_diagnostic_code,
                last_diagnostic_message=self._last_diagnostic_message,
            )

    def _result_locked(
        self,
        outcome: MarketControlDispatchOutcome,
        *,
        delivery: Optional[MarketEventControlDelivery],
        retry: bool,
        queue_acknowledged: bool,
        retained_for_retry: bool,
        diagnostic_code: Optional[str] = None,
        diagnostic_message: Optional[str] = None,
        delivery_id_override: Optional[str] = None,
        event_count_override: int = 0,
    ) -> MarketControlDispatchResult:
        """更新最后诊断并构造单步不可变结果。

        Args:
            outcome: 本次稳定终态。
            delivery: 本次尝试的 delivery；空队列时为 None。
            retry: 是否重取了当前未 ACK delivery。
            queue_acknowledged: 是否已精确调用并完成 queue ACK。
            retained_for_retry: 当前 delivery 是否应视为仍待重试。
            diagnostic_code: 可选稳定诊断码。
            diagnostic_message: 可选已脱敏诊断文本；写入前按构造上限截断。
            delivery_id_override: delivery 不可用时用于诊断的当前稳定 ID。
            event_count_override: delivery 不可用时用于诊断的当前事件数。

        Returns:
            MarketControlDispatchResult: 与更新后指标一致的不可变结果。

        Side Effects:
            更新最后 outcome/code/message；不调用外部对象。
        """
        bounded_message = self._bounded_diagnostic(diagnostic_message)
        self._last_outcome = outcome
        self._last_diagnostic_code = diagnostic_code
        self._last_diagnostic_message = bounded_message
        return MarketControlDispatchResult(
            outcome=outcome,
            delivery_id=(delivery.delivery_id if delivery is not None else delivery_id_override),
            event_count=(len(delivery.events) if delivery is not None else event_count_override),
            retry=retry,
            queue_acknowledged=queue_acknowledged,
            retained_for_retry=retained_for_retry,
            diagnostic_code=diagnostic_code,
            diagnostic_message=bounded_message,
        )

    def _bounded_diagnostic(self, message: Optional[str]) -> Optional[str]:
        """把最后诊断规范为固定长度文本。

        Args:
            message: 可选诊断文本。

        Returns:
            Optional[str]: None、原短文本或按构造上限截断的文本。

        Side Effects:
            无。
        """
        if message is None:
            return None
        normalized = str(message).strip()
        if not normalized:
            return None
        return normalized[: self._max_diagnostic_message_length]


__all__ = [
    "MarketControlDispatchOutcome",
    "MarketControlDispatchResult",
    "MarketControlDispatcherMetrics",
    "MarketControlSink",
    "MarketControlSinkAck",
    "ReliableMarketControlDispatcher",
]
