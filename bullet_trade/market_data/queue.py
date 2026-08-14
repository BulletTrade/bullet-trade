"""
作者: BruceLee

文件职责: 提供线程安全、内存有界且显式暴露行情损失边界的通用市场事件队列。
主要输入: 已完成字段校验的 MarketEvent，以及数据队列容量和可选单调时钟提供器。
主要输出: 确定性的入队结果、控制事件优先的 drain 批次和不可变背压指标快照。
上游关系: 由 realtime feed、native bridge drain 线程或远程 market-data writer 投递事件。
下游关系: 供 EventBus、网络 writer、health 与完整 L2 门禁消费数据和 gap/degraded 控制事件。
关键配置约定: 普通数据按事件数硬限界；仅 L1/L2 快照和 IOPV 可按
Provider/epoch/stream/channel/证券/事件类型合并；控制路径按真实通道 scope
有界聚合，超出容量立即 fail closed，且不自动恢复连续性。
"""

from __future__ import annotations

import secrets
from collections import OrderedDict
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
from threading import RLock
from types import MappingProxyType
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from .models import (
    ConnectionStateEvent,
    MarketDataLevel,
    MarketEvent,
    MarketEventType,
    SequenceGapEvent,
    SourceSequence,
)


class QueuePutOutcome(str, Enum):
    """枚举非阻塞行情入队请求的确定性结果。

    由 ``BoundedMarketEventQueue`` 生成并交给上游 feed 判断数据是新增、合并还是拒绝；
    枚举本身无可变状态。
    """

    ENQUEUED = "enqueued"
    CONTROL_ENQUEUED = "control_enqueued"
    COALESCED = "coalesced"
    OVERFLOW = "overflow"


class MarketEventControlCapacityError(RuntimeError):
    """表示新的连续性 scope 无法进入已满的队外控制路径。

    与 ``BoundedMarketEventQueue`` 协作，在普通数据已丢失且无法为新 scope
    保留 gap/degraded 槽时立即中止上游快速路径；异常不含业务 payload 或凭据。
    """

    def __init__(self, scope_key: Tuple[Any, ...], control_scope_capacity: int) -> None:
        """保存被拒绝的脱敏 scope 和配置容量。

        Args:
            scope_key: Provider、能力、级别、epoch、stream、channel 和市场组成的键。
            control_scope_capacity: 队外控制路径可同时保留的 scope 数。

        Returns:
            None。

        Side Effects:
            初始化异常消息与可诊断属性；不修改队列。
        """
        self.scope_key = scope_key
        self.control_scope_capacity = control_scope_capacity
        super().__init__(
            "MARKET_EVENT_CONTROL_CAPACITY_EXHAUSTED: "
            f"scope={scope_key!r}, capacity={control_scope_capacity}"
        )


class MarketEventControlAckError(RuntimeError):
    """表示控制投递 ACK 缺失、过期或与当前 in-flight 批次不匹配。

    与 ``take_control``/``ack_control`` 协作，阻止 writer 用错误 ID 释放尚未可靠发送的
    gap/degraded 控制事件；异常只保存稳定错误码和批次 ID，不含行情 payload。
    """

    def __init__(
        self,
        code: str,
        delivery_id: str,
        expected_delivery_id: Optional[str],
    ) -> None:
        """保存 ACK 失败的稳定诊断信息。

        Args:
            code: ``NO_CONTROL_DELIVERY_IN_FLIGHT`` 或
                ``CONTROL_DELIVERY_ID_MISMATCH``。
            delivery_id: 调用方尝试确认的 delivery ID。
            expected_delivery_id: 当前 in-flight delivery ID；没有批次时为 None。

        Returns:
            None。

        Side Effects:
            初始化异常消息和只读语义属性；不释放任何控制事件。
        """
        self.code = code
        self.delivery_id = delivery_id
        self.expected_delivery_id = expected_delivery_id
        super().__init__(
            f"{code}: delivery_id={delivery_id!r}, "
            f"expected_delivery_id={expected_delivery_id!r}"
        )


class MarketEventControlDrainError(RuntimeError):
    """表示兼容 drain 可能破坏可靠控制投递合同而被拒绝。

    与 ``drain``/``drain_control`` 协作；默认禁止 pending fire-and-forget，且无论是否
    开启兼容模式都禁止绕过 ACK 清除 in-flight delivery。
    """

    def __init__(self, code: str) -> None:
        """保存 drain 失败的稳定错误码。

        Args:
            code: ``LEGACY_CONTROL_DRAIN_DISABLED`` 或
                ``RELIABLE_CONTROL_DELIVERY_IN_FLIGHT``。

        Returns:
            None。

        Side Effects:
            初始化异常消息和诊断属性；不清除任何控制或普通数据。
        """
        self.code = code
        super().__init__(code)


class MarketEventRecoveryAuthorizationError(RuntimeError):
    """表示连续性恢复授权的 scope、ID 或生命周期不合法。

    与 ``authorize_continuity_recovery`` 协作，阻止未降级 scope、冲突 recovery ID
    或已经持有一次性授权的 scope 被模糊覆盖；异常不含行情 payload。
    """

    def __init__(
        self,
        code: str,
        scope_key: Tuple[Any, ...],
        recovery_id: str,
    ) -> None:
        """保存授权失败的稳定诊断信息。

        Args:
            code: 授权失败的稳定错误码。
            scope_key: Provider、能力、级别、epoch、stream、channel 和市场组成的键。
            recovery_id: 调用方请求使用的脱敏恢复 ID。

        Returns:
            None。

        Side Effects:
            初始化异常消息和属性；不新增、覆盖或消费授权。
        """
        self.code = code
        self.scope_key = scope_key
        self.recovery_id = recovery_id
        super().__init__(f"{code}: scope={scope_key!r}, recovery_id={recovery_id!r}")


@dataclass(frozen=True)
class QueuePutResult:
    """记录一次入队操作的结果快照。

    与 ``QueuePutOutcome`` 和有界队列协作，保存接收状态、操作后水位及累计降级状态；
    实例不可变，不持有队列引用。
    """

    outcome: QueuePutOutcome
    accepted: bool
    data_depth: int
    control_depth: int
    degraded: bool
    ever_degraded: bool
    active_degraded: bool


@dataclass(frozen=True)
class MarketEventControlDelivery:
    """保存一次必须显式 ACK 的不可变控制事件投递批次。

    由 ``BoundedMarketEventQueue.take_control`` 创建并交给 network writer 或 EventBus；
    ``delivery_id`` 在未 ACK 重取时保持稳定，``events`` 只包含 gap/status 控制事件。
    """

    delivery_id: str
    events: Tuple[MarketEvent, ...]
    scope_count: int

    def __post_init__(self) -> None:
        """校验 ID、事件类型和 scope 数并冻结事件序列。

        Args:
            无；输入来自 dataclass 字段。

        Returns:
            None。

        Raises:
            ValueError: delivery ID、事件集合或 scope 数不合法时抛出。

        Side Effects:
            将 ``events`` 规范化为不可变元组。
        """
        delivery_id = self.delivery_id.strip()
        events = tuple(self.events)
        if not delivery_id:
            raise ValueError("delivery_id 不能为空")
        if not events:
            raise ValueError("control delivery 必须包含事件")
        if isinstance(self.scope_count, bool) or not isinstance(self.scope_count, int):
            raise ValueError("scope_count 必须是正整数")
        if self.scope_count <= 0:
            raise ValueError("scope_count 必须是正整数")
        if any(not _is_control_event(event) for event in events):
            raise ValueError("control delivery 只能包含 gap/status 事件")
        object.__setattr__(self, "delivery_id", delivery_id)
        object.__setattr__(self, "events", events)


@dataclass(frozen=True)
class MarketEventQueueMetrics:
    """暴露有界行情队列的累计计数与当前水位。

    由队列在同一把锁下创建，供 health、监控和完整 L2 门禁读取；关键状态包括容量、
    水位、合并/溢出计数、损失边界数量和单调 degraded 标志。
    """

    capacity: int
    control_capacity: int
    data_depth: int
    control_depth: int
    high_watermark: int
    enqueued_count: int
    drained_count: int
    coalesced_count: int
    overflow_count: int
    loss_boundary_count: int
    control_emitted_count: int
    control_scope_capacity: int
    control_scope_depth: int
    control_overflow_count: int
    control_pending_depth: int
    control_inflight_depth: int
    control_outstanding_depth: int
    control_pending_scope_depth: int
    control_inflight_scope_depth: int
    control_outstanding_scope_depth: int
    control_outstanding_capacity: int
    control_outstanding_scope_capacity: int
    control_received_count: int
    control_delivery_count: int
    control_retry_count: int
    control_ack_count: int
    control_ack_error_count: int
    control_delivery_inflight: bool
    degraded: bool
    ever_degraded: bool
    active_degraded: bool
    active_degraded_scope_depth: int
    active_degraded_scope_capacity: int
    recovery_authorization_depth: int
    recovery_authorization_capacity: int
    recovery_authorization_count: int
    recovery_completed_count: int
    recovery_rejected_count: int
    overflow_by_event_type: Mapping[str, int]

    def __post_init__(self) -> None:
        """冻结逐事件类型溢出计数，防止 health 调用方改写指标。

        Args:
            无；输入来自 dataclass 字段。

        Returns:
            None。

        Side Effects:
            将 ``overflow_by_event_type`` 替换为独立只读副本。
        """
        object.__setattr__(
            self,
            "overflow_by_event_type",
            MappingProxyType(dict(self.overflow_by_event_type)),
        )


@dataclass(frozen=True)
class MarketEventDrainBatch:
    """保存一次原子 drain 取得的控制事件和普通数据事件。

    由有界队列创建并交给下游 EventBus 或网络 writer；关键状态按不可变元组分区保存，
    ``events`` 始终先暴露控制边界，再暴露普通数据。
    """

    control_events: Tuple[MarketEvent, ...]
    data_events: Tuple[MarketEvent, ...]

    @property
    def events(self) -> Tuple[MarketEvent, ...]:
        """按控制事件优先顺序返回本批全部事件。

        Args:
            无。

        Returns:
            Tuple[MarketEvent, ...]: control 在前、普通数据在后的不可变事件元组。

        Side Effects:
            无。
        """
        return self.control_events + self.data_events


@dataclass(frozen=True)
class _LossPoint:
    """保存单个被拒绝事件的有界连续性证据。

    与 ``MarketEvent`` 和损失聚合器协作，只复制身份、通道、时间与原始序列；关键状态
    不包含完整业务 payload，避免控制路径随被拒绝数据无限增长。
    """

    provider: str
    capability_key: str
    event_type: MarketEventType
    level: MarketDataLevel
    security: Optional[str]
    raw_security_code: Optional[str]
    exchange: str
    session_epoch: str
    stream_id: Optional[str]
    channel_id: Optional[str]
    source_sequence: SourceSequence
    gateway_received_at: Optional[datetime]
    queue_loss_index: int

    @classmethod
    def from_event(cls, event: MarketEvent, queue_loss_index: int) -> "_LossPoint":
        """从被拒绝事件复制构造最小 loss-boundary 记录。

        Args:
            event: 因数据队列已满而未入队的事件。
            queue_loss_index: 队列生命周期内严格递增的溢出计数。

        Returns:
            _LossPoint: 不保留完整业务 payload 的损失点。

        Side Effects:
            无；只复制事件的身份、通道、时间与原始序列引用。
        """
        source_sequence = event.source_sequence
        if not isinstance(source_sequence, SourceSequence):
            source_sequence = SourceSequence(components=source_sequence)
        return cls(
            provider=event.provider,
            capability_key=event.capability_key,
            event_type=event.event_type,
            level=event.level,
            security=event.security,
            raw_security_code=event.raw_security_code,
            exchange=event.exchange,
            session_epoch=event.session_epoch,
            stream_id=event.stream_id,
            channel_id=event.channel_id,
            source_sequence=source_sequence,
            gateway_received_at=event.gateway_received_at,
            queue_loss_index=queue_loss_index,
        )

    @property
    def scope_key(self) -> Tuple[Any, ...]:
        """返回判断两次损失是否属于相同连续性作用域的键。

        Args:
            无。

        Returns:
            Tuple[Any, ...]: Provider、能力、级别、epoch、stream、channel 和市场。

        Side Effects:
            无。
        """
        return (
            self.provider,
            self.capability_key,
            self.level.value,
            self.session_epoch,
            self.stream_id,
            self.channel_id,
            self.exchange,
        )

    def as_payload(self) -> Mapping[str, Any]:
        """生成可嵌入控制事件的明确损失点映射。

        Args:
            无。

        Returns:
            Mapping[str, Any]: 包含队列序号、通道作用域和原始序列的只读映射。

        Side Effects:
            无。
        """
        return MappingProxyType(
            {
                "queue_loss_index": self.queue_loss_index,
                "provider": self.provider,
                "capability_key": self.capability_key,
                "event_type": self.event_type.value,
                "level": self.level.value,
                "security": self.security,
                "raw_security": self.raw_security_code,
                "exchange": self.exchange,
                "session_epoch": self.session_epoch,
                "stream_id": self.stream_id,
                "channel_id": self.channel_id,
                "source_sequence": dict(self.source_sequence.components),
                "gateway_received_at": self.gateway_received_at,
            }
        )


@dataclass
class _LossAccumulator:
    """在固定控制槽内聚合尚未送达的首末损失边界。

    由有界队列在锁内维护并最终转换为 gap/degraded 控制事件；关键状态仅保存首末损失点、
    计数、事件类型统计和多证券标志，不保存全部丢失事件。
    """

    first: _LossPoint
    last: _LossPoint
    loss_count: int
    first_gap: Optional[_LossPoint]
    last_gap: Optional[_LossPoint]
    gap_loss_count: int
    multiple_securities: bool
    multiple_gap_securities: bool
    event_type_counts: Dict[str, int]

    @classmethod
    def start(cls, point: _LossPoint) -> "_LossAccumulator":
        """以第一个溢出点创建一段待发布控制边界。

        Args:
            point: 当前控制窗口内第一个被拒绝的事件损失点。

        Returns:
            _LossAccumulator: 初始计数为一的可变聚合器。

        Side Effects:
            无。
        """
        gap_point = point if point.event_type in _GAP_REQUIRED_EVENT_TYPES else None
        return cls(
            first=point,
            last=point,
            loss_count=1,
            first_gap=gap_point,
            last_gap=gap_point,
            gap_loss_count=1 if gap_point is not None else 0,
            multiple_securities=False,
            multiple_gap_securities=False,
            event_type_counts={point.event_type.value: 1},
        )

    def add(self, point: _LossPoint) -> None:
        """把后续溢出点并入同一个有界首末边界窗口。

        Args:
            point: 新的被拒绝事件损失点。

        Returns:
            None。

        Side Effects:
            更新末端、累计计数、事件类型计数和多证券标记；不保存中间事件。
        """
        if point.scope_key != self.first.scope_key:
            raise ValueError("loss point 不属于当前控制 scope")
        if (point.security, point.raw_security_code) != (
            self.first.security,
            self.first.raw_security_code,
        ):
            self.multiple_securities = True
        self.last = point
        self.loss_count += 1
        event_type = point.event_type.value
        self.event_type_counts[event_type] = self.event_type_counts.get(event_type, 0) + 1
        if point.event_type not in _GAP_REQUIRED_EVENT_TYPES:
            return
        if self.first_gap is None:
            self.first_gap = point
        elif (point.security, point.raw_security_code) != (
            self.first_gap.security,
            self.first_gap.raw_security_code,
        ):
            self.multiple_gap_securities = True
        self.last_gap = point
        self.gap_loss_count += 1


@dataclass
class _ControlScopeAccumulator:
    """在一个有界 scope 槽内保存来源控制状态和本地 overflow 证据。

    由队列在锁内按 provider/capability/level/epoch/stream/channel/exchange 维护；同一窗口
    最终最多生成一个 gap 和一个 status，重复来源控制通过首末证据有界聚合。
    """

    loss: Optional[_LossAccumulator] = None
    source_gap_first: Optional[SequenceGapEvent] = None
    source_gap_last: Optional[SequenceGapEvent] = None
    source_gap_count: int = 0
    source_gap_multiple_securities: bool = False
    source_status_first: Optional[ConnectionStateEvent] = None
    source_status_last: Optional[ConnectionStateEvent] = None
    source_status_count: int = 0
    source_status_multiple_securities: bool = False

    def add_loss(self, point: _LossPoint) -> None:
        """把普通数据 overflow 并入当前 scope 的固定首末窗口。

        Args:
            point: 本次未能进入普通数据区的最小损失证据。

        Returns:
            None。

        Side Effects:
            首次创建或继续更新 ``loss`` 聚合器，不保留中间业务事件。
        """
        if self.loss is None:
            self.loss = _LossAccumulator.start(point)
            return
        self.loss.add(point)

    def add_source_control(self, event: MarketEvent) -> None:
        """把来源 gap/status 控制事件有界聚合到当前 scope。

        Args:
            event: 已通过模型校验的 SequenceGapEvent 或 ConnectionStateEvent。

        Returns:
            None。

        Raises:
            TypeError: event 不是受支持的控制事件时抛出。

        Side Effects:
            保存对应类型的首末不可变事件、累计数量和单调多证券标志；不占普通数据容量。
        """
        if isinstance(event, SequenceGapEvent):
            if self.source_gap_first is None:
                self.source_gap_first = event
            elif (event.security, event.raw_security_code) != (
                self.source_gap_first.security,
                self.source_gap_first.raw_security_code,
            ):
                self.source_gap_multiple_securities = True
            self.source_gap_last = event
            self.source_gap_count += 1
            return
        if isinstance(event, ConnectionStateEvent):
            if self.source_status_first is None:
                self.source_status_first = event
            elif (event.security, event.raw_security_code) != (
                self.source_status_first.security,
                self.source_status_first.raw_security_code,
            ):
                self.source_status_multiple_securities = True
            self.source_status_last = event
            self.source_status_count += 1
            return
        raise TypeError("source control 必须是 SequenceGapEvent 或 ConnectionStateEvent")

    @property
    def event_count(self) -> int:
        """返回当前 scope 最终会生成的一或两个控制事件。

        Args:
            无。

        Returns:
            int: gap 与 status 的存在数量，范围为 1 到 2。

        Side Effects:
            无。
        """
        has_gap = self.source_gap_first is not None or (
            self.loss is not None and self.loss.first_gap is not None
        )
        has_status = self.source_status_first is not None or self.loss is not None
        return int(has_gap) + int(has_status)

    @property
    def has_loss_boundary(self) -> bool:
        """判断当前控制窗口是否包含必须可靠 ACK 的损失边界。

        Args:
            无。

        Returns:
            bool: 存在来源 gap 或任意普通数据 queue loss 时返回 True。

        Side Effects:
            无。
        """
        return self.source_gap_first is not None or self.loss is not None


_COALESCIBLE_EVENT_TYPES = frozenset(
    {
        MarketEventType.SNAPSHOT_L1,
        MarketEventType.SNAPSHOT_L2,
        MarketEventType.IOPV,
    }
)

_GAP_REQUIRED_EVENT_TYPES = frozenset(
    {
        MarketEventType.TRANSACTION,
        MarketEventType.ORDER_DETAIL,
        MarketEventType.CONSOLIDATED_TICK,
    }
)

_HEALTHY_CONNECTION_STATES = frozenset({"connected", "ready"})


def _is_control_event(event: MarketEvent) -> bool:
    """判断事件是否属于必须走队外可靠路径的控制类型。

    Args:
        event: 待分类的市场事件。

    Returns:
        bool: SequenceGapEvent 或 ConnectionStateEvent 返回 True，否则返回 False。

    Side Effects:
        无。
    """
    return isinstance(event, (SequenceGapEvent, ConnectionStateEvent))


class BoundedMarketEventQueue:
    """管理普通行情容量、快照合并和队外 gap/degraded 控制边界。

    上游 feed 或 native bridge 调用非阻塞入队，下游 EventBus、writer 与 health 消费 drain
    批次和指标；关键状态包括受 ``RLock`` 保护的数据区、有界 per-scope 损失聚合器、累计指标及
    只会从正常转为降级的连续性标志，以及至多一个必须精确 ACK 的 in-flight 控制批次。
    """

    CONTROL_EVENTS_PER_SCOPE = 2
    MAX_RECOVERY_ID_LENGTH = 128

    def __init__(
        self,
        capacity: int,
        now_provider: Optional[Callable[[], datetime]] = None,
        control_scope_capacity: Optional[int] = None,
        allow_legacy_control_drain: bool = False,
    ) -> None:
        """创建空的线程安全有界行情队列。

        Args:
            capacity: 普通数据区最多保存的事件数量，必须为正整数。
            now_provider: 控制事件缺少来源时间时使用的时钟；默认 ``datetime.now``。
            control_scope_capacity: 队外路径可同时保留的连续性 scope 数；
                默认与普通数据容量相同。
            allow_legacy_control_drain: 是否允许在没有 in-flight delivery 时通过
                ``drain``/``drain_control`` fire-and-forget 清除 pending 控制；默认关闭。

        Returns:
            None。

        Raises:
            ValueError: 容量不是正整数或兼容开关不是 bool 时抛出。

        Side Effects:
            创建进程内锁、空数据区和累计指标；不启动线程、不联网也不执行交易。
        """
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("capacity 必须是正整数")
        if control_scope_capacity is None:
            control_scope_capacity = capacity
        if (
            isinstance(control_scope_capacity, bool)
            or not isinstance(control_scope_capacity, int)
            or control_scope_capacity <= 0
        ):
            raise ValueError("control_scope_capacity 必须是正整数")
        if not isinstance(allow_legacy_control_drain, bool):
            raise ValueError("allow_legacy_control_drain 必须是 bool")
        self._capacity = capacity
        self._control_scope_capacity = control_scope_capacity
        self._allow_legacy_control_drain = allow_legacy_control_drain
        self._now = now_provider or datetime.now
        self._lock = RLock()
        self._data: "OrderedDict[Tuple[Any, ...], MarketEvent]" = OrderedDict()
        self._serial = 0
        self._pending_controls: "OrderedDict[Tuple[Any, ...], _ControlScopeAccumulator]" = (
            OrderedDict()
        )
        self._inflight_control: Optional[MarketEventControlDelivery] = None
        self._inflight_loss_scopes: Tuple[Tuple[Any, ...], ...] = ()
        self._delivery_serial = 0
        self._delivery_incarnation = secrets.token_hex(16)
        self._active_degraded_scopes: "OrderedDict[Tuple[Any, ...], None]" = OrderedDict()
        self._active_degraded_scope_capacity = control_scope_capacity * 2
        self._recovery_authorizations: "OrderedDict[Tuple[Any, ...], str]" = OrderedDict()
        self._unacked_loss_scopes: "OrderedDict[Tuple[Any, ...], None]" = OrderedDict()
        self._high_watermark = 0
        self._enqueued_count = 0
        self._drained_count = 0
        self._coalesced_count = 0
        self._overflow_count = 0
        self._loss_boundary_count = 0
        self._control_emitted_count = 0
        self._control_overflow_count = 0
        self._control_received_count = 0
        self._control_delivery_count = 0
        self._control_retry_count = 0
        self._control_ack_count = 0
        self._control_ack_error_count = 0
        self._recovery_authorization_count = 0
        self._recovery_completed_count = 0
        self._recovery_rejected_count = 0
        self._ever_degraded = False
        self._overflow_by_event_type: Dict[str, int] = {}

    @property
    def capacity(self) -> int:
        """返回普通数据区的硬容量。

        Args:
            无。

        Returns:
            int: 构造时配置的最大事件数。

        Side Effects:
            无。
        """
        return self._capacity

    @property
    def control_capacity(self) -> int:
        """返回单个 pending 或 in-flight 窗口的最大逻辑事件槽数。

        Args:
            无。

        Returns:
            int: 每个 scope 最多一个 gap 和一个 status 事件的窗口上限。

        Side Effects:
            无。
        """
        return self._control_scope_capacity * self.CONTROL_EVENTS_PER_SCOPE

    def authorize_continuity_recovery(
        self,
        *,
        provider: str,
        capability_key: str,
        level: MarketDataLevel,
        session_epoch: str,
        stream_id: Optional[str],
        channel_id: Optional[str],
        exchange: str,
        recovery_id: str,
    ) -> None:
        """为一个 active degraded scope 登记一次性连续性恢复授权。

        Args:
            provider: 降级 lineage 的行情 Provider。
            capability_key: 降级 lineage 的原子能力键。
            level: 降级 lineage 的行情级别。
            session_epoch: 降级 lineage 的连接会话 epoch。
            stream_id: 降级 lineage 的 stream ID；scope 本身为 None 时传 None。
            channel_id: 降级 lineage 的 channel ID；scope 本身为 None 时传 None。
            exchange: 降级 lineage 的交易所。
            recovery_id: 上游在完成重传或建立新基线后生成的非空一次性 ID。

        Returns:
            None；相同 scope 和 recovery ID 的未消费重试为幂等操作。

        Raises:
            ValueError: scope 字段、level 或 recovery_id 不合法时抛出。
            MarketEventRecoveryAuthorizationError: scope 未处于 active degraded、仍有
                未 ACK 损失边界，或已有不同 recovery ID 授权时抛出。

        Side Effects:
            在有界授权表中登记 recovery ID；只有随后完全匹配且明确确认的连接状态
            才会消费它并清除 active degraded，普通布尔状态不能创建授权。
        """
        normalized_provider = self._normalize_required_scope_string(provider, "provider")
        normalized_capability = self._normalize_required_scope_string(
            capability_key,
            "capability_key",
        )
        normalized_epoch = self._normalize_required_scope_string(
            session_epoch,
            "session_epoch",
        )
        normalized_stream = self._normalize_optional_scope_string(stream_id, "stream_id")
        normalized_channel = self._normalize_optional_scope_string(channel_id, "channel_id")
        normalized_exchange = self._normalize_required_scope_string(exchange, "exchange")
        normalized_recovery_id = self._normalize_recovery_id(recovery_id)
        try:
            normalized_level = MarketDataLevel(level)
        except (TypeError, ValueError) as exc:
            raise ValueError("level 必须是合法 MarketDataLevel") from exc
        scope_key = (
            normalized_provider,
            normalized_capability,
            normalized_level.value,
            normalized_epoch,
            normalized_stream,
            normalized_channel,
            normalized_exchange,
        )
        with self._lock:
            if scope_key not in self._active_degraded_scopes:
                self._recovery_rejected_count += 1
                raise MarketEventRecoveryAuthorizationError(
                    "RECOVERY_SCOPE_NOT_ACTIVE_DEGRADED",
                    scope_key,
                    normalized_recovery_id,
                )
            if scope_key in self._unacked_loss_scopes:
                self._recovery_rejected_count += 1
                raise MarketEventRecoveryAuthorizationError(
                    "RECOVERY_LOSS_BOUNDARY_UNACKED",
                    scope_key,
                    normalized_recovery_id,
                )
            existing_id = self._recovery_authorizations.get(scope_key)
            if existing_id is not None:
                if existing_id == normalized_recovery_id:
                    return
                self._recovery_rejected_count += 1
                raise MarketEventRecoveryAuthorizationError(
                    "RECOVERY_AUTHORIZATION_CONFLICT",
                    scope_key,
                    normalized_recovery_id,
                )
            self._recovery_authorizations[scope_key] = normalized_recovery_id
            self._recovery_authorization_count += 1

    def __len__(self) -> int:
        """返回当前普通数据事件数量。

        Args:
            无。

        Returns:
            int: 受锁保护的数据区深度，不含队外控制事件。

        Side Effects:
            无。
        """
        with self._lock:
            return len(self._data)

    def put_nowait(self, event: MarketEvent) -> QueuePutResult:
        """非阻塞入队；来源控制事件始终绕过普通数据容量。

        Args:
            event: 已完成模型校验且生命周期独立的类型化市场事件。

        Returns:
            QueuePutResult: control_enqueued、enqueued、coalesced 或 overflow 的确定结果。

        Raises:
            TypeError: event 不是 MarketEvent 时抛出。
            ValueError: 可合并事件缺少证券身份时抛出，避免跨证券错误覆盖。
            MarketEventControlCapacityError: pending 控制 scope 窗口已满时抛出。

        Side Effects:
            控制事件只更新队外 pending 窗口；普通事件可能追加或合并数据区，溢出时更新
            固定大小 loss boundary。所有路径都不等待消费者，且不会自动清除连续性降级。
        """
        if not isinstance(event, MarketEvent):
            raise TypeError("event 必须是 MarketEvent")
        with self._lock:
            if _is_control_event(event):
                self._record_source_control_locked(event)
                return self._result_locked(QueuePutOutcome.CONTROL_ENQUEUED, accepted=True)
            event = self._normalize_data_for_active_degradation_locked(event)
            token = self._coalesce_token(event)
            if token is not None and token in self._data:
                del self._data[token]
                self._data[token] = event
                self._coalesced_count += 1
                return self._result_locked(QueuePutOutcome.COALESCED, accepted=True)
            if len(self._data) < self._capacity:
                if token is None:
                    self._serial += 1
                    token = ("event", self._serial)
                self._data[token] = event
                self._enqueued_count += 1
                self._high_watermark = max(self._high_watermark, len(self._data))
                return self._result_locked(QueuePutOutcome.ENQUEUED, accepted=True)
            self._record_overflow_locked(event)
            return self._result_locked(QueuePutOutcome.OVERFLOW, accepted=False)

    def take_control(self) -> Optional[MarketEventControlDelivery]:
        """取得一个必须显式 ACK 的 at-least-once 控制投递批次。

        Args:
            无。

        Returns:
            Optional[MarketEventControlDelivery]: 没有控制状态时返回 None；否则返回不可变
            delivery。未 ACK 前重复调用返回同一 delivery ID 和同一事件元组。

        Side Effects:
            首次取得时把当前 pending 窗口冻结为唯一 in-flight 批次；后续来源控制和
            overflow 进入独立的下一 pending 窗口，不会修改当前 delivery。
        """
        with self._lock:
            if self._inflight_control is not None:
                self._control_retry_count += 1
                return self._inflight_control
            if not self._pending_controls:
                return None
            events = self._build_pending_control_events_locked()
            scope_count = len(self._pending_controls)
            loss_scopes = tuple(
                scope_key
                for scope_key, accumulator in self._pending_controls.items()
                if accumulator.has_loss_boundary
            )
            self._pending_controls.clear()
            self._delivery_serial += 1
            delivery = MarketEventControlDelivery(
                delivery_id=(
                    f"control-delivery-{self._delivery_incarnation}-"
                    f"{self._delivery_serial:016d}"
                ),
                events=events,
                scope_count=scope_count,
            )
            self._inflight_control = delivery
            self._inflight_loss_scopes = loss_scopes
            self._control_delivery_count += 1
            self._control_emitted_count += len(events)
            return delivery

    def ack_control(self, delivery_id: str) -> None:
        """仅用当前精确 delivery ID 释放已可靠发送的 in-flight 控制批次。

        Args:
            delivery_id: ``take_control`` 返回且由下游确认发送成功的稳定 ID。

        Returns:
            None。

        Raises:
            ValueError: delivery_id 不是非空字符串时抛出。
            MarketEventControlAckError: 当前无 in-flight 批次或 ID 不匹配时抛出。

        Side Effects:
            精确匹配时释放当前 in-flight 批次并增加 ACK 计数；不会触碰随后到达的
            pending 控制窗口。错误 ACK 不修改控制状态。
        """
        if not isinstance(delivery_id, str) or not delivery_id.strip():
            raise ValueError("delivery_id 必须是非空字符串")
        normalized_id = delivery_id.strip()
        with self._lock:
            delivery = self._inflight_control
            if delivery is None:
                self._control_ack_error_count += 1
                raise MarketEventControlAckError(
                    "NO_CONTROL_DELIVERY_IN_FLIGHT",
                    normalized_id,
                    None,
                )
            if delivery.delivery_id != normalized_id:
                self._control_ack_error_count += 1
                raise MarketEventControlAckError(
                    "CONTROL_DELIVERY_ID_MISMATCH",
                    normalized_id,
                    delivery.delivery_id,
                )
            self._inflight_control = None
            for scope_key in self._inflight_loss_scopes:
                pending = self._pending_controls.get(scope_key)
                if pending is None or not pending.has_loss_boundary:
                    self._unacked_loss_scopes.pop(scope_key, None)
            self._inflight_loss_scopes = ()
            self._control_ack_count += 1

    def drain(self, max_data_items: Optional[int] = None) -> MarketEventDrainBatch:
        """按显式兼容开关取得 pending 控制状态及有界普通数据。

        Args:
            max_data_items: 本次最多取出的普通事件数；None 表示取完，零表示只取控制事件。

        Returns:
            MarketEventDrainBatch: 控制事件与普通事件分区保存，``events`` 属性控制优先。

        Raises:
            ValueError: max_data_items 不是非负整数或 None 时抛出。
            MarketEventControlDrainError: 存在 in-flight delivery，或 pending 控制存在但
                构造时没有显式开启 legacy fire-and-forget 时抛出。

        Side Effects:
            仅在显式 legacy 开关开启且没有 in-flight 时清空 pending 控制和已返回数据；
            失败时保持控制与数据不变。生产 writer 应使用 take/ack 合同。
        """
        self._validate_max_items(max_data_items)
        with self._lock:
            controls = self._drain_control_unreliable_locked()
            data_events = self._drain_data_locked(max_data_items)
            return MarketEventDrainBatch(control_events=controls, data_events=data_events)

    def drain_control(self) -> Tuple[MarketEvent, ...]:
        """按显式兼容开关 fire-and-forget 取出 pending 控制事件。

        Args:
            无。

        Returns:
            Tuple[MarketEvent, ...]: 没有控制时为空，否则为 pending 控制事件。

        Raises:
            MarketEventControlDrainError: 存在 in-flight delivery，或 pending 控制存在但
                构造时没有显式开启 legacy fire-and-forget 时抛出。

        Side Effects:
            显式 legacy 开关开启时清空 pending；永不隐式确认 in-flight delivery。
            生产 writer 必须改用 ``take_control`` 和 ``ack_control``。
        """
        with self._lock:
            return self._drain_control_unreliable_locked()

    def drain_data(self, max_items: Optional[int] = None) -> Tuple[MarketEvent, ...]:
        """按到达顺序取出普通数据，不隐式确认或清除任何控制边界。

        Args:
            max_items: 最多取出的普通事件数；None 表示取完，零表示不取。

        Returns:
            Tuple[MarketEvent, ...]: 按实际队列顺序排列的数据事件。

        Raises:
            ValueError: max_items 不是非负整数或 None 时抛出。

        Side Effects:
            从普通数据区移除已返回事件并增加 drained_count。
        """
        self._validate_max_items(max_items)
        with self._lock:
            return self._drain_data_locked(max_items)

    def metrics(self) -> MarketEventQueueMetrics:
        """返回当前水位、可靠控制深度及累计/active degraded 状态快照。

        Args:
            无。

        Returns:
            MarketEventQueueMetrics: 与同一锁时点一致的不可变指标。

        Side Effects:
            无；不会 drain 或恢复连续性。
        """
        with self._lock:
            pending_depth = self._pending_control_depth_locked()
            inflight_depth = self._inflight_control_depth_locked()
            pending_scope_depth = len(self._pending_controls)
            inflight_scope_depth = self._inflight_control_scope_depth_locked()
            return MarketEventQueueMetrics(
                capacity=self._capacity,
                control_capacity=self.control_capacity,
                data_depth=len(self._data),
                control_depth=max(pending_depth, inflight_depth),
                high_watermark=self._high_watermark,
                enqueued_count=self._enqueued_count,
                drained_count=self._drained_count,
                coalesced_count=self._coalesced_count,
                overflow_count=self._overflow_count,
                loss_boundary_count=self._loss_boundary_count,
                control_emitted_count=self._control_emitted_count,
                control_scope_capacity=self._control_scope_capacity,
                control_scope_depth=max(pending_scope_depth, inflight_scope_depth),
                control_overflow_count=self._control_overflow_count,
                control_pending_depth=pending_depth,
                control_inflight_depth=inflight_depth,
                control_outstanding_depth=pending_depth + inflight_depth,
                control_pending_scope_depth=pending_scope_depth,
                control_inflight_scope_depth=inflight_scope_depth,
                control_outstanding_scope_depth=pending_scope_depth + inflight_scope_depth,
                control_outstanding_capacity=self.control_capacity * 2,
                control_outstanding_scope_capacity=self._control_scope_capacity * 2,
                control_received_count=self._control_received_count,
                control_delivery_count=self._control_delivery_count,
                control_retry_count=self._control_retry_count,
                control_ack_count=self._control_ack_count,
                control_ack_error_count=self._control_ack_error_count,
                control_delivery_inflight=self._inflight_control is not None,
                degraded=self._ever_degraded,
                ever_degraded=self._ever_degraded,
                active_degraded=bool(self._active_degraded_scopes),
                active_degraded_scope_depth=len(self._active_degraded_scopes),
                active_degraded_scope_capacity=self._active_degraded_scope_capacity,
                recovery_authorization_depth=len(self._recovery_authorizations),
                recovery_authorization_capacity=self._active_degraded_scope_capacity,
                recovery_authorization_count=self._recovery_authorization_count,
                recovery_completed_count=self._recovery_completed_count,
                recovery_rejected_count=self._recovery_rejected_count,
                overflow_by_event_type=self._overflow_by_event_type,
            )

    def _coalesce_token(self, event: MarketEvent) -> Optional[Tuple[Any, ...]]:
        """为允许合并的快照/IOPV 生成不会跨来源或 epoch 覆盖的稳定键。

        Args:
            event: 待入队事件。

        Returns:
            Optional[Tuple[Any, ...]]: 可合并 token；逐笔、状态和控制事件返回 None。

        Raises:
            ValueError: 允许合并的事件缺少证券代码时抛出。

        Side Effects:
            无。
        """
        if event.event_type not in _COALESCIBLE_EVENT_TYPES:
            return None
        if event.security is None:
            raise ValueError("快照/IOPV 合并要求明确 security")
        return (
            "coalesce",
            event.provider,
            event.capability_key,
            event.level.value,
            event.event_type.value,
            event.exchange,
            event.session_epoch,
            event.stream_id,
            event.channel_id,
            event.security,
        )

    def _record_overflow_locked(self, event: MarketEvent) -> None:
        """在数据锁内累计一次溢出并更新固定大小的控制边界。

        Args:
            event: 因普通数据区已满而未被接收的事件。

        Returns:
            None。

        Side Effects:
            增加 overflow、事件类型计数和累计 degraded；仅保留每个 scope 当前窗口的
            首末损失点，并激活有界 degraded lineage。新 scope 超过容量时立即抛错。
        """
        self._overflow_count += 1
        self._ever_degraded = True
        event_type = event.event_type.value
        self._overflow_by_event_type[event_type] = (
            self._overflow_by_event_type.get(event_type, 0) + 1
        )
        point = _LossPoint.from_event(event, self._overflow_count)
        self._ensure_active_degraded_capacity_locked(point.scope_key)
        accumulator = self._get_or_create_pending_scope_locked(point.scope_key)
        if accumulator.loss is None:
            self._loss_boundary_count += 1
            self._unacked_loss_scopes[point.scope_key] = None
        accumulator.add_loss(point)
        self._activate_degraded_scope_locked(
            point.scope_key,
            invalidate_authorization=True,
        )

    def _record_source_control_locked(self, event: MarketEvent) -> None:
        """把来源 gap/status 放入队外 pending 窗口并阻止隐式连续性恢复。

        Args:
            event: public ``put_nowait`` 收到的来源控制事件。

        Returns:
            None。

        Raises:
            TypeError: event 不是 SequenceGapEvent 或 ConnectionStateEvent 时抛出。
            MarketEventControlCapacityError: 新 scope 超过 pending 窗口上限时抛出。

        Side Effects:
            更新来源控制计数、per-scope 首末状态和全局 degraded 标志；不修改普通数据区。
        """
        if not _is_control_event(event):
            raise TypeError("source control 必须是 gap/status 事件")
        scope_key = self._control_scope_key(event)
        if self._source_control_requires_degradation_locked(event, scope_key):
            self._ensure_active_degraded_capacity_locked(scope_key)
        accumulator = self._get_or_create_pending_scope_locked(scope_key)
        normalized_event = self._normalize_source_control_locked(event, scope_key)
        accumulator.add_source_control(normalized_event)
        if isinstance(normalized_event, SequenceGapEvent):
            self._unacked_loss_scopes[scope_key] = None
        self._control_received_count += 1

    def _get_or_create_pending_scope_locked(
        self,
        scope_key: Tuple[Any, ...],
    ) -> _ControlScopeAccumulator:
        """取得 pending scope 槽，容量不足时立即 fail closed。

        Args:
            scope_key: Provider、能力、级别、epoch、stream、channel 和市场组成的键。

        Returns:
            _ControlScopeAccumulator: 现有或刚创建的有界 scope 聚合器。

        Raises:
            MarketEventControlCapacityError: pending scope 数已达到配置上限时抛出。

        Side Effects:
            可能创建一个空聚合器；容量失败时增加 control_overflow_count。
        """
        accumulator = self._pending_controls.get(scope_key)
        if accumulator is not None:
            return accumulator
        if len(self._pending_controls) >= self._control_scope_capacity:
            self._control_overflow_count += 1
            self._ever_degraded = True
            raise MarketEventControlCapacityError(scope_key, self._control_scope_capacity)
        accumulator = _ControlScopeAccumulator()
        self._pending_controls[scope_key] = accumulator
        return accumulator

    @staticmethod
    def _control_scope_key(event: MarketEvent) -> Tuple[Any, ...]:
        """从控制事件生成与 loss boundary 一致的连续性 scope 键。

        Args:
            event: SequenceGapEvent 或 ConnectionStateEvent。

        Returns:
            Tuple[Any, ...]: Provider、能力、级别、epoch、stream、channel 和市场。

        Side Effects:
            无。
        """
        return (
            event.provider,
            event.capability_key,
            event.level.value,
            event.session_epoch,
            event.stream_id,
            event.channel_id,
            event.exchange,
        )

    def _normalize_data_for_active_degradation_locked(self, event: MarketEvent) -> MarketEvent:
        """阻止 active degraded lineage 的普通数据重新声称连续或完整。

        Args:
            event: 已完成模型校验且不是 gap/status 的普通市场事件。

        Returns:
            MarketEvent: scope 健康时返回原事件；active degraded 时返回
            completeness=false、continuous=false 的不可变副本。

        Side Effects:
            无；不会消费恢复授权或改变 degraded 状态，仅规范化即将入队的事件。
        """
        scope_key = self._control_scope_key(event)
        if scope_key not in self._active_degraded_scopes:
            return event
        payload = dict(event.payload)
        payload["continuous"] = False
        payload["continuity_degraded"] = True
        payload["recovery_required"] = True
        return replace(event, payload=payload, completeness=False)

    def _normalize_source_control_locked(
        self,
        event: MarketEvent,
        scope_key: Tuple[Any, ...],
    ) -> MarketEvent:
        """规范化来源控制，仅允许预授权且完全匹配的状态清除 active degraded。

        Args:
            event: 已校验的来源 gap/status 事件。
            scope_key: 当前事件的连续性 scope。

        Returns:
            MarketEvent: 原不可变事件、授权恢复副本或 continuous=false 的降级副本。

        Side Effects:
            gap/degraded 会激活 scope 并使既有授权失效；完全匹配的恢复状态一次性消费
            recovery ID 并清除 active scope。累计 ever_degraded 永不回退。
        """
        payload = dict(event.payload)
        if isinstance(event, SequenceGapEvent):
            payload["state"] = "degraded"
            payload["continuous"] = False
            self._ever_degraded = True
            self._activate_degraded_scope_locked(
                scope_key,
                invalidate_authorization=True,
            )
            if payload == dict(event.payload) and event.completeness is False:
                return event
            return replace(event, payload=payload, completeness=False)

        state = str(payload.get("state", "")).strip().lower()
        continuous = payload.get("continuous")
        if continuous is False and state == "degraded":
            payload["state"] = "degraded"
            payload["continuous"] = False
            self._ever_degraded = True
            self._activate_degraded_scope_locked(
                scope_key,
                invalidate_authorization=True,
            )
            if payload == dict(event.payload) and event.completeness is False:
                return event
            return replace(event, payload=payload, completeness=False)

        if state not in _HEALTHY_CONNECTION_STATES or continuous is not True:
            payload["reported_state"] = state or None
            payload["state"] = "degraded"
            payload["continuous"] = False
            payload["recovery_blocked"] = True
            if state not in _HEALTHY_CONNECTION_STATES:
                payload["reason"] = "unhealthy_connection_state"
            else:
                payload["reason"] = "healthy_state_without_continuity_evidence"
            self._ever_degraded = True
            self._activate_degraded_scope_locked(
                scope_key,
                invalidate_authorization=True,
            )
            return replace(event, payload=payload, completeness=False)

        if scope_key in self._active_degraded_scopes:
            if self._is_matching_recovery_locked(event, scope_key):
                self._recovery_authorizations.pop(scope_key, None)
                self._active_degraded_scopes.pop(scope_key, None)
                self._recovery_completed_count += 1
                payload["recovery_authorized"] = True
                return replace(event, payload=payload)
            payload["state"] = "degraded"
            payload["continuous"] = False
            payload["recovery_blocked"] = True
            payload["reported_state"] = state or None
            payload["reason"] = "continuity_recovery_not_authorized"
            self._ever_degraded = True
            self._recovery_rejected_count += 1
            return replace(event, payload=payload, completeness=False)
        return event

    def _source_control_requires_degradation_locked(
        self,
        event: MarketEvent,
        scope_key: Tuple[Any, ...],
    ) -> bool:
        """判断来源控制是否会新增或维持 active degraded lineage。

        Args:
            event: 已通过 public 类型检查的 gap/status 事件。
            scope_key: 当前事件的精确连续性 scope。

        Returns:
            bool: gap、明确降级、缺少连续性证据或未授权恢复返回 True。

        Side Effects:
            无；仅用于在创建 pending 槽前预检 active 状态容量。
        """
        if isinstance(event, SequenceGapEvent):
            return True
        payload = event.payload
        state = str(payload.get("state", "")).strip().lower()
        continuous = payload.get("continuous")
        if state not in _HEALTHY_CONNECTION_STATES or continuous is not True:
            return True
        if scope_key in self._active_degraded_scopes:
            return not self._is_matching_recovery_locked(event, scope_key)
        return False

    def _is_matching_recovery_locked(
        self,
        event: MarketEvent,
        scope_key: Tuple[Any, ...],
    ) -> bool:
        """判断连接状态是否完整匹配当前 scope 的一次性恢复授权。

        Args:
            event: 待校验的来源连接状态事件。
            scope_key: 当前事件的精确连续性 scope。

        Returns:
            bool: 仅 state 为 connected/ready、continuous=true、recovery_confirmed=true
            且 recovery_id 与预授权完全一致时返回 True。

        Side Effects:
            无；不消费授权也不修改 active degraded 状态。
        """
        if not isinstance(event, ConnectionStateEvent):
            return False
        authorized_id = self._recovery_authorizations.get(scope_key)
        if authorized_id is None:
            return False
        payload = event.payload
        state = str(payload.get("state", "")).strip().lower()
        return (
            state in _HEALTHY_CONNECTION_STATES
            and payload.get("continuous") is True
            and payload.get("recovery_confirmed") is True
            and payload.get("recovery_id") == authorized_id
        )

    def _ensure_active_degraded_capacity_locked(self, scope_key: Tuple[Any, ...]) -> None:
        """预检新的 active degraded scope 是否仍有固定容量。

        Args:
            scope_key: 将被激活的精确连续性 scope。

        Returns:
            None。

        Raises:
            MarketEventControlCapacityError: active degraded scope 容量已满时抛出。

        Side Effects:
            容量不足时增加 control_overflow_count 并设置 ever_degraded；不淘汰旧 scope。
        """
        if scope_key in self._active_degraded_scopes:
            return
        if len(self._active_degraded_scopes) >= self._active_degraded_scope_capacity:
            self._control_overflow_count += 1
            self._ever_degraded = True
            raise MarketEventControlCapacityError(
                scope_key,
                self._active_degraded_scope_capacity,
            )

    def _activate_degraded_scope_locked(
        self,
        scope_key: Tuple[Any, ...],
        *,
        invalidate_authorization: bool,
    ) -> None:
        """激活有界 degraded lineage，并按需使旧恢复授权失效。

        Args:
            scope_key: 已发生 gap、overflow 或明确降级的精确连续性 scope。
            invalidate_authorization: 是否删除早于本次新降级证据登记的 recovery ID。

        Returns:
            None。

        Raises:
            MarketEventControlCapacityError: 新 scope 超过 active 状态容量时抛出。

        Side Effects:
            新增 active scope；新 gap/overflow/降级状态可删除同 scope 的陈旧授权。
        """
        self._ensure_active_degraded_capacity_locked(scope_key)
        self._active_degraded_scopes[scope_key] = None
        if invalidate_authorization:
            self._recovery_authorizations.pop(scope_key, None)

    def _build_pending_control_events_locked(self) -> Tuple[MarketEvent, ...]:
        """在锁内把 pending scope 状态冻结为 gap/status 事件元组。

        Args:
            无。

        Returns:
            Tuple[MarketEvent, ...]: 按 scope 首次到达顺序输出，每个 scope 的 gap 在
            status 之前。

        Side Effects:
            无；调用方负责在 delivery 创建或兼容 drain 后清空 pending 窗口。
        """
        if not self._pending_controls:
            return ()
        controls: List[MarketEvent] = []
        for accumulator in self._pending_controls.values():
            gap = self._build_scope_gap_event_locked(accumulator)
            status = self._build_scope_status_event_locked(accumulator)
            if gap is not None:
                controls.append(gap)
            if status is not None:
                controls.append(status)
        return tuple(controls)

    def _drain_control_unreliable_locked(self) -> Tuple[MarketEvent, ...]:
        """仅在显式兼容模式下以 fire-and-forget 语义清空 pending 控制。

        Args:
            无。

        Returns:
            Tuple[MarketEvent, ...]: 没有控制时为空，否则为 pending 控制事件元组。

        Raises:
            MarketEventControlDrainError: 存在 in-flight delivery 或 pending 未获 legacy
            开关授权时抛出。

        Side Effects:
            显式兼容模式下清空 pending 并增加 emitted 计数；永不释放 in-flight。
        """
        if self._inflight_control is not None:
            raise MarketEventControlDrainError("RELIABLE_CONTROL_DELIVERY_IN_FLIGHT")
        if not self._pending_controls:
            return ()
        if not self._allow_legacy_control_drain:
            raise MarketEventControlDrainError("LEGACY_CONTROL_DRAIN_DISABLED")
        pending_events = self._build_pending_control_events_locked()
        self._pending_controls.clear()
        self._control_emitted_count += len(pending_events)
        return pending_events

    def _build_scope_gap_event_locked(
        self,
        accumulator: _ControlScopeAccumulator,
    ) -> Optional[SequenceGapEvent]:
        """把来源 gap 与同 scope queue overflow 合并为一个有界 gap 事件。

        Args:
            accumulator: 当前 scope 的来源控制和本地 loss 聚合状态。

        Returns:
            Optional[SequenceGapEvent]: 没有逐笔/source gap 时返回 None，否则返回完整
            provenance 的不可变 gap。

        Side Effects:
            无。
        """
        source_gap = self._build_source_gap_event_locked(accumulator)
        queue_gap = None
        if (
            accumulator.loss is not None
            and accumulator.loss.first_gap is not None
            and accumulator.loss.last_gap is not None
        ):
            queue_gap = self._build_gap_event_locked(accumulator.loss)
        if source_gap is None:
            return queue_gap
        if queue_gap is None:
            return source_gap
        payload = dict(source_gap.payload)
        payload["queue_overflow_boundary"] = dict(queue_gap.payload)
        payload["continuous"] = False
        payload["state"] = "degraded"
        security, raw_security, multiple_securities = self._merge_control_security_identity(
            source_gap,
            queue_gap,
        )
        payload["multiple_securities"] = multiple_securities
        return replace(
            source_gap,
            payload=payload,
            security=security,
            raw_security_code=raw_security,
            completeness=False,
        )

    def _build_scope_status_event_locked(
        self,
        accumulator: _ControlScopeAccumulator,
    ) -> Optional[ConnectionStateEvent]:
        """把来源 status 与同 scope queue overflow 合并为一个有界状态事件。

        Args:
            accumulator: 当前 scope 的来源控制和本地 loss 聚合状态。

        Returns:
            Optional[ConnectionStateEvent]: 没有任何 status/loss 时返回 None，否则返回
            不会自动声明连续性恢复的不可变状态事件。

        Side Effects:
            无。
        """
        source_status = self._build_source_status_event_locked(accumulator)
        if source_status is not None:
            source_status = self._align_status_with_final_active_scope_locked(source_status)
        queue_status = None
        if accumulator.loss is not None:
            queue_status = self._build_degraded_event_locked(accumulator.loss)
        if source_status is None:
            return queue_status
        if queue_status is None:
            return source_status
        payload = dict(source_status.payload)
        payload["queue_overflow_boundary"] = dict(queue_status.payload)
        payload["continuous"] = False
        payload["state"] = "degraded"
        security, raw_security, multiple_securities = self._merge_control_security_identity(
            source_status,
            queue_status,
        )
        payload["multiple_securities"] = multiple_securities
        return replace(
            source_status,
            payload=payload,
            security=security,
            raw_security_code=raw_security,
            completeness=False,
        )

    def _align_status_with_final_active_scope_locked(
        self,
        status: ConnectionStateEvent,
    ) -> ConnectionStateEvent:
        """按冻结 delivery 时的最终 active scope 收紧较早健康状态。

        Args:
            status: 当前 scope 来源状态窗口构造出的最后状态。

        Returns:
            ConnectionStateEvent: scope 最终健康时返回原状态；若后到 gap/loss 已重新激活
            scope，则返回移除 recovery_authorized 且 continuous=false 的降级副本。

        Side Effects:
            无；只读取锁内 active scope，避免同窗口 recovery→gap 输出假恢复。
        """
        scope_key = self._control_scope_key(status)
        if scope_key not in self._active_degraded_scopes:
            return status
        payload = dict(status.payload)
        already_degraded = (
            payload.get("continuous") is False
            and str(payload.get("state", "")).strip().lower() == "degraded"
            and "recovery_authorized" not in payload
        )
        if already_degraded:
            return status
        reported_state = str(payload.get("state", "")).strip().lower()
        payload.pop("recovery_authorized", None)
        payload["reported_state"] = reported_state or None
        payload["state"] = "degraded"
        payload["continuous"] = False
        payload["recovery_invalidated"] = True
        payload["reason"] = "loss_boundary_after_recovery"
        return replace(status, payload=payload, completeness=False)

    @staticmethod
    def _merge_control_security_identity(
        source_control: MarketEvent,
        queue_control: MarketEvent,
    ) -> Tuple[Optional[str], Optional[str], bool]:
        """统一来源控制与 queue loss 控制的顶层证券身份。

        Args:
            source_control: 作为合并主体的来源 gap 或 status。
            queue_control: 同 scope 的 queue overflow gap 或 degraded status。

        Returns:
            Tuple[Optional[str], Optional[str], bool]: 合并后的标准证券、原始证券和
            multiple_securities；任一侧已多证券或两侧身份不同即清空顶层证券。

        Side Effects:
            无；不修改两个不可变输入事件。
        """
        source_identity = (source_control.security, source_control.raw_security_code)
        queue_identity = (queue_control.security, queue_control.raw_security_code)
        multiple_securities = (
            bool(source_control.payload.get("multiple_securities", False))
            or bool(queue_control.payload.get("multiple_securities", False))
            or source_identity != queue_identity
        )
        if multiple_securities:
            return None, None, True
        return source_control.security, source_control.raw_security_code, False

    def _build_source_gap_event_locked(
        self,
        accumulator: _ControlScopeAccumulator,
    ) -> Optional[SequenceGapEvent]:
        """把同 scope 重复来源 gap 有界折叠为首末证据事件。

        Args:
            accumulator: 含零个或多个来源 gap 的 scope 聚合器。

        Returns:
            Optional[SequenceGapEvent]: 无来源 gap 时返回 None；单个事件原样返回；多个
            事件返回保留首个 payload 和末端 provenance 的聚合副本。

        Side Effects:
            无。
        """
        first = accumulator.source_gap_first
        last = accumulator.source_gap_last
        if first is None or last is None:
            return None
        if accumulator.source_gap_count == 1:
            return first
        payload = dict(first.payload)
        payload["source_control_window"] = {
            "count": accumulator.source_gap_count,
            "first": self._control_event_evidence(first),
            "last": self._control_event_evidence(last),
        }
        multiple_securities = accumulator.source_gap_multiple_securities
        payload["multiple_securities"] = multiple_securities
        return replace(
            first,
            payload=payload,
            security=None if multiple_securities else first.security,
            raw_security_code=None if multiple_securities else first.raw_security_code,
            gateway_received_at=last.gateway_received_at,
            source_sequence=last.source_sequence,
            completeness=False,
        )

    def _build_source_status_event_locked(
        self,
        accumulator: _ControlScopeAccumulator,
    ) -> Optional[ConnectionStateEvent]:
        """把同 scope 重复来源 status 有界折叠为最新状态及首末证据。

        Args:
            accumulator: 含零个或多个来源 status 的 scope 聚合器。

        Returns:
            Optional[ConnectionStateEvent]: 无来源状态时返回 None；单个事件原样返回；
            多个事件返回以最新状态为主体的聚合副本。

        Side Effects:
            无。
        """
        first = accumulator.source_status_first
        last = accumulator.source_status_last
        if first is None or last is None:
            return None
        if accumulator.source_status_count == 1:
            return last
        payload = dict(last.payload)
        payload["source_control_window"] = {
            "count": accumulator.source_status_count,
            "first": self._control_event_evidence(first),
            "last": self._control_event_evidence(last),
        }
        multiple_securities = accumulator.source_status_multiple_securities
        payload["multiple_securities"] = multiple_securities
        return replace(
            last,
            payload=payload,
            security=None if multiple_securities else last.security,
            raw_security_code=None if multiple_securities else last.raw_security_code,
            completeness=False,
        )

    @staticmethod
    def _control_event_evidence(event: MarketEvent) -> Mapping[str, Any]:
        """生成来源控制首末窗口使用的有界完整 provenance 证据。

        Args:
            event: 已冻结的来源 gap/status 事件。

        Returns:
            Mapping[str, Any]: 含身份、scope、序列、时间和 payload 的诊断映射。

        Side Effects:
            无；返回新建普通映射，最终由 MarketEvent 再次深冻结。
        """
        source_sequence = event.source_sequence
        if isinstance(source_sequence, SourceSequence):
            sequence_components = dict(source_sequence.components)
        else:
            sequence_components = dict(source_sequence)
        return {
            "provider": event.provider,
            "capability_key": event.capability_key,
            "event_type": event.event_type.value,
            "level": event.level.value,
            "exchange": event.exchange,
            "session_epoch": event.session_epoch,
            "stream_id": event.stream_id,
            "channel_id": event.channel_id,
            "security": event.security,
            "raw_security": event.raw_security_code,
            "source_sequence": sequence_components,
            "gateway_received_at": event.gateway_received_at,
            "payload": dict(event.payload),
        }

    def _build_gap_event_locked(self, accumulator: _LossAccumulator) -> SequenceGapEvent:
        """为 L1/L2 损失窗口构造显式首末边界 SequenceGapEvent。

        Args:
            accumulator: 至少含一个 L1/L2 损失点的待发聚合窗口。

        Returns:
            SequenceGapEvent: 不声明恢复、可穿过独立控制通道的缺口事件。

        Side Effects:
            无。
        """
        first = accumulator.first_gap
        last = accumulator.last_gap
        if first is None or last is None:
            raise RuntimeError("gap accumulator 缺少 L1/L2 loss point")
        sequence = last.source_sequence
        if not sequence:
            sequence = SourceSequence(
                components={"queue_loss_index": last.queue_loss_index},
                ordering_scope="queue_arrival",
            )
        security = None if accumulator.multiple_gap_securities else first.security
        raw_security = None if accumulator.multiple_gap_securities else first.raw_security_code
        payload = {
            "state": "degraded",
            "reason": "queue_overflow",
            "continuous": False,
            "loss_count": accumulator.gap_loss_count,
            "total_overflow_count": self._overflow_count,
            "first_lost": first.as_payload(),
            "last_lost": last.as_payload(),
            "multiple_scopes": False,
            "multiple_securities": accumulator.multiple_gap_securities,
            "queue_capacity": self._capacity,
            "queue_high_watermark": self._high_watermark,
        }
        return SequenceGapEvent(
            provider=first.provider,
            capability_key=first.capability_key,
            event_type=MarketEventType.STREAM_GAP,
            level=first.level,
            exchange=first.exchange,
            session_epoch=first.session_epoch,
            payload=payload,
            security=security,
            raw_security_code=raw_security,
            gateway_received_at=last.gateway_received_at or self._now(),
            stream_id=first.stream_id,
            channel_id=first.channel_id,
            source_sequence=sequence,
            raw_type="queue_overflow",
            completeness=False,
        )

    def _build_degraded_event_locked(self, accumulator: _LossAccumulator) -> ConnectionStateEvent:
        """为所有事件类型的损失构造独立 degraded 状态控制事件。

        Args:
            accumulator: 当前尚未送达的完整损失窗口。

        Returns:
            ConnectionStateEvent: 含首末边界、累计数量且 continuous=false 的状态事件。

        Side Effects:
            无。
        """
        first = accumulator.first
        last = accumulator.last
        security = None if accumulator.multiple_securities else first.security
        raw_security = None if accumulator.multiple_securities else first.raw_security_code
        payload = {
            "state": "degraded",
            "reason": "queue_overflow",
            "continuous": False,
            "loss_count": accumulator.loss_count,
            "total_overflow_count": self._overflow_count,
            "first_lost": first.as_payload(),
            "last_lost": last.as_payload(),
            "multiple_scopes": False,
            "multiple_securities": accumulator.multiple_securities,
            "event_type_counts": dict(sorted(accumulator.event_type_counts.items())),
            "queue_capacity": self._capacity,
            "queue_high_watermark": self._high_watermark,
        }
        return ConnectionStateEvent(
            provider=first.provider,
            capability_key=first.capability_key,
            event_type=MarketEventType.STREAM_STATUS,
            level=first.level,
            exchange=first.exchange,
            session_epoch=first.session_epoch,
            payload=payload,
            security=security,
            raw_security_code=raw_security,
            gateway_received_at=last.gateway_received_at or self._now(),
            stream_id=first.stream_id,
            channel_id=first.channel_id,
            raw_type="queue_overflow",
            completeness=False,
        )

    def _drain_data_locked(self, max_items: Optional[int]) -> Tuple[MarketEvent, ...]:
        """在锁内按当前队列顺序取出有界数量的普通数据事件。

        Args:
            max_items: 已校验的非负上限或 None。

        Returns:
            Tuple[MarketEvent, ...]: 本次移除的数据事件。

        Side Effects:
            从 OrderedDict 头部移除事件并增加 drained_count。
        """
        count = len(self._data) if max_items is None else min(max_items, len(self._data))
        drained = []
        for _ in range(count):
            _, event = self._data.popitem(last=False)
            drained.append(event)
        self._drained_count += len(drained)
        return tuple(drained)

    def _pending_control_depth_locked(self) -> int:
        """返回当前下一 pending 窗口会生成的控制事件数量。

        Args:
            无。

        Returns:
            int: 各 pending scope 的 gap/status 数之和。

        Side Effects:
            无。
        """
        return sum(accumulator.event_count for accumulator in self._pending_controls.values())

    def _inflight_control_depth_locked(self) -> int:
        """返回当前未 ACK delivery 中的控制事件数。

        Args:
            无。

        Returns:
            int: 没有 in-flight 时为零，否则为不可变 delivery 事件数。

        Side Effects:
            无。
        """
        if self._inflight_control is None:
            return 0
        return len(self._inflight_control.events)

    def _inflight_control_scope_depth_locked(self) -> int:
        """返回当前未 ACK delivery 中的连续性 scope 数。

        Args:
            无。

        Returns:
            int: 没有 in-flight 时为零，否则为 delivery 冻结的 scope 数。

        Side Effects:
            无。
        """
        if self._inflight_control is None:
            return 0
        return self._inflight_control.scope_count

    def _control_depth_locked(self) -> int:
        """返回单窗口最大控制深度，兼容既有 health 容量合同。

        Args:
            无。

        Returns:
            int: pending 与 in-flight 两个分别有界窗口的较大事件数；总 outstanding
            深度由 ``MarketEventQueueMetrics.control_outstanding_depth`` 单独暴露。

        Side Effects:
            无。
        """
        return max(
            self._pending_control_depth_locked(),
            self._inflight_control_depth_locked(),
        )

    def _result_locked(self, outcome: QueuePutOutcome, accepted: bool) -> QueuePutResult:
        """在同一锁时点构造入队结果。

        Args:
            outcome: 本次入队的稳定结果枚举。
            accepted: 事件是否已进入普通数据区或完成合法合并。

        Returns:
            QueuePutResult: 包含操作后水位与连续性状态的不可变结果。

        Side Effects:
            无。
        """
        return QueuePutResult(
            outcome=outcome,
            accepted=accepted,
            data_depth=len(self._data),
            control_depth=self._control_depth_locked(),
            degraded=self._ever_degraded,
            ever_degraded=self._ever_degraded,
            active_degraded=bool(self._active_degraded_scopes),
        )

    @staticmethod
    def _normalize_required_scope_string(value: str, label: str) -> str:
        """校验并规范化恢复授权的必填 scope 字符串。

        Args:
            value: 调用方提供的 scope 字段值。
            label: 参数错误时使用的稳定字段名。

        Returns:
            str: 去除首尾空白后的非空字符串。

        Raises:
            ValueError: value 不是字符串或去空后为空时抛出。

        Side Effects:
            无。
        """
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{label} 必须是非空字符串")
        return value.strip()

    @staticmethod
    def _normalize_optional_scope_string(
        value: Optional[str],
        label: str,
    ) -> Optional[str]:
        """校验并规范化恢复授权的可选 scope 字符串。

        Args:
            value: None 或调用方提供的 scope 字段值。
            label: 参数错误时使用的稳定字段名。

        Returns:
            Optional[str]: None 或去除首尾空白后的非空字符串。

        Raises:
            ValueError: 非 None 值不是非空字符串时抛出。

        Side Effects:
            无。
        """
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{label} 必须是非空字符串或 None")
        return value.strip()

    @classmethod
    def _normalize_recovery_id(cls, recovery_id: str) -> str:
        """校验一次性 recovery ID 的类型、非空和有界长度。

        Args:
            recovery_id: 上游完成重传或新基线后生成的脱敏 ID。

        Returns:
            str: 去除首尾空白后的 recovery ID。

        Raises:
            ValueError: recovery_id 不是非空字符串或超过固定长度时抛出。

        Side Effects:
            无。
        """
        if not isinstance(recovery_id, str) or not recovery_id.strip():
            raise ValueError("recovery_id 必须是非空字符串")
        normalized = recovery_id.strip()
        if len(normalized) > cls.MAX_RECOVERY_ID_LENGTH:
            raise ValueError(f"recovery_id 长度不能超过 {cls.MAX_RECOVERY_ID_LENGTH} 个字符")
        return normalized

    @staticmethod
    def _validate_max_items(max_items: Optional[int]) -> None:
        """校验 drain 数量上限，允许零表示只消费控制或不消费数据。

        Args:
            max_items: 用户传入的可选数量上限。

        Returns:
            None。

        Raises:
            ValueError: 值不是 None 或非负整数时抛出。

        Side Effects:
            无。
        """
        if max_items is None:
            return
        if isinstance(max_items, bool) or not isinstance(max_items, int) or max_items < 0:
            raise ValueError("max_items 必须是非负整数或 None")


__all__ = [
    "BoundedMarketEventQueue",
    "MarketEventControlAckError",
    "MarketEventControlCapacityError",
    "MarketEventControlDelivery",
    "MarketEventControlDrainError",
    "MarketEventDrainBatch",
    "MarketEventQueueMetrics",
    "MarketEventRecoveryAuthorizationError",
    "QueuePutOutcome",
    "QueuePutResult",
]
