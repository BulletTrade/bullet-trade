"""
作者: BruceLee

文件职责: 提供线程安全、内存有界且显式暴露行情损失边界的通用市场事件队列。
主要输入: 已完成字段校验的 MarketEvent，以及数据队列容量和可选单调时钟提供器。
主要输出: 确定性的入队结果、控制事件优先的 drain 批次和不可变背压指标快照。
上游关系: 由 realtime feed、native bridge drain 线程或远程 market-data writer 投递事件。
下游关系: 供 EventBus、网络 writer、health 与完整 L2 门禁消费数据和 gap/degraded 控制事件。
关键配置约定: 普通数据按事件数硬限界；仅 L1/L2 快照和 IOPV 可按
Provider/epoch/stream/channel/证券/事件类型合并；控制路径固定为队外聚合槽且不自动恢复连续性。
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from threading import RLock
from types import MappingProxyType
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

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
    COALESCED = "coalesced"
    OVERFLOW = "overflow"


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
    degraded: bool
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
            source_sequence=event.source_sequence,
            gateway_received_at=event.gateway_received_at,
            queue_loss_index=queue_loss_index,
        )

    @property
    def scope_key(self) -> Tuple[Any, ...]:
        """返回判断两次损失是否属于相同连续性作用域的键。

        Args:
            无。

        Returns:
            Tuple[Any, ...]: Provider、能力、级别、市场、epoch、stream、channel 和证券。

        Side Effects:
            无。
        """
        return (
            self.provider,
            self.capability_key,
            self.level,
            self.exchange,
            self.session_epoch,
            self.stream_id,
            self.channel_id,
            self.security,
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
    计数、事件类型统计和多作用域标志，不保存全部丢失事件。
    """

    first: _LossPoint
    last: _LossPoint
    loss_count: int
    first_gap: Optional[_LossPoint]
    last_gap: Optional[_LossPoint]
    gap_loss_count: int
    multiple_scopes: bool
    multiple_gap_scopes: bool
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
            multiple_scopes=False,
            multiple_gap_scopes=False,
            event_type_counts={point.event_type.value: 1},
        )

    def add(self, point: _LossPoint) -> None:
        """把后续溢出点并入同一个有界首末边界窗口。

        Args:
            point: 新的被拒绝事件损失点。

        Returns:
            None。

        Side Effects:
            更新末端、累计计数、事件类型计数和多作用域标记；不保存中间事件。
        """
        if point.scope_key != self.first.scope_key:
            self.multiple_scopes = True
        self.last = point
        self.loss_count += 1
        event_type = point.event_type.value
        self.event_type_counts[event_type] = self.event_type_counts.get(event_type, 0) + 1
        if point.event_type not in _GAP_REQUIRED_EVENT_TYPES:
            return
        if self.first_gap is None:
            self.first_gap = point
        elif point.scope_key != self.first_gap.scope_key:
            self.multiple_gap_scopes = True
        self.last_gap = point
        self.gap_loss_count += 1


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


class BoundedMarketEventQueue:
    """管理普通行情容量、快照合并和队外 gap/degraded 控制边界。

    上游 feed 或 native bridge 调用非阻塞入队，下游 EventBus、writer 与 health 消费 drain
    批次和指标；关键状态包括受 ``RLock`` 保护的数据区、固定大小损失聚合器、累计指标及
    只会从正常转为降级的连续性标志。
    """

    CONTROL_CAPACITY = 2

    def __init__(
        self,
        capacity: int,
        now_provider: Optional[Callable[[], datetime]] = None,
    ) -> None:
        """创建空的线程安全有界行情队列。

        Args:
            capacity: 普通数据区最多保存的事件数量，必须为正整数。
            now_provider: 控制事件缺少来源时间时使用的时钟；默认 ``datetime.now``。

        Returns:
            None。

        Raises:
            ValueError: capacity 不是正整数时抛出。

        Side Effects:
            创建进程内锁、空数据区和累计指标；不启动线程、不联网也不执行交易。
        """
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("capacity 必须是正整数")
        self._capacity = capacity
        self._now = now_provider or datetime.now
        self._lock = RLock()
        self._data: "OrderedDict[Tuple[Any, ...], MarketEvent]" = OrderedDict()
        self._serial = 0
        self._pending_loss: Optional[_LossAccumulator] = None
        self._high_watermark = 0
        self._enqueued_count = 0
        self._drained_count = 0
        self._coalesced_count = 0
        self._overflow_count = 0
        self._loss_boundary_count = 0
        self._control_emitted_count = 0
        self._degraded = False
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
        """返回队外控制路径的固定逻辑槽数。

        Args:
            无。

        Returns:
            int: 最多一个 gap 和一个 degraded 状态事件，共两个槽。

        Side Effects:
            无。
        """
        return self.CONTROL_CAPACITY

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
        """非阻塞入队；容量不足时记录边界并通过队外控制路径降级。

        Args:
            event: 已完成模型校验且生命周期独立的类型化市场事件。

        Returns:
            QueuePutResult: enqueued、coalesced 或 overflow 的确定性结果。

        Raises:
            TypeError: event 不是 MarketEvent 时抛出。
            ValueError: 可合并事件缺少证券身份时抛出，避免跨证券错误覆盖。

        Side Effects:
            可能追加或替换普通数据、更新指标；溢出时更新固定大小的 loss boundary，
            永不等待消费者腾空且永不自动清除 degraded 状态。
        """
        if not isinstance(event, MarketEvent):
            raise TypeError("event 必须是 MarketEvent")
        with self._lock:
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

    def drain(self, max_data_items: Optional[int] = None) -> MarketEventDrainBatch:
        """原子取得全部待发控制事件及有界数量的普通数据事件。

        Args:
            max_data_items: 本次最多取出的普通事件数；None 表示取完，零表示只取控制事件。

        Returns:
            MarketEventDrainBatch: 控制事件与普通事件分区保存，``events`` 属性控制优先。

        Raises:
            ValueError: max_data_items 不是非负整数或 None 时抛出。

        Side Effects:
            清除已返回的待发控制边界和普通数据；累计 degraded 状态保持，不伪造连续性恢复。
        """
        self._validate_max_items(max_data_items)
        with self._lock:
            controls = self._drain_control_locked()
            data_events = self._drain_data_locked(max_data_items)
            return MarketEventDrainBatch(control_events=controls, data_events=data_events)

    def drain_control(self) -> Tuple[MarketEvent, ...]:
        """独立取出 gap/degraded 控制事件，不受普通数据容量和水位影响。

        Args:
            无。

        Returns:
            Tuple[MarketEvent, ...]: 固定顺序的 gap（若适用）和 degraded 状态事件。

        Side Effects:
            清除当前待发控制窗口；累计 overflow 指标与 degraded 状态保持。
        """
        with self._lock:
            return self._drain_control_locked()

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
        """返回当前水位、累计合并/溢出和单调 degraded 状态快照。

        Args:
            无。

        Returns:
            MarketEventQueueMetrics: 与同一锁时点一致的不可变指标。

        Side Effects:
            无；不会 drain 或恢复连续性。
        """
        with self._lock:
            return MarketEventQueueMetrics(
                capacity=self._capacity,
                control_capacity=self.CONTROL_CAPACITY,
                data_depth=len(self._data),
                control_depth=self._control_depth_locked(),
                high_watermark=self._high_watermark,
                enqueued_count=self._enqueued_count,
                drained_count=self._drained_count,
                coalesced_count=self._coalesced_count,
                overflow_count=self._overflow_count,
                loss_boundary_count=self._loss_boundary_count,
                control_emitted_count=self._control_emitted_count,
                degraded=self._degraded,
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
            增加 overflow、事件类型计数并永久标记 degraded；仅保留当前窗口首末损失点。
        """
        self._overflow_count += 1
        self._degraded = True
        event_type = event.event_type.value
        self._overflow_by_event_type[event_type] = (
            self._overflow_by_event_type.get(event_type, 0) + 1
        )
        point = _LossPoint.from_event(event, self._overflow_count)
        if self._pending_loss is None:
            self._pending_loss = _LossAccumulator.start(point)
            self._loss_boundary_count += 1
        else:
            self._pending_loss.add(point)

    def _drain_control_locked(self) -> Tuple[MarketEvent, ...]:
        """在锁内把当前聚合边界转换成至多两个控制事件并清空窗口。

        Args:
            无。

        Returns:
            Tuple[MarketEvent, ...]: gap 在前、degraded 状态在后的控制事件。

        Side Effects:
            清除当前 pending loss 并增加 control_emitted_count；不恢复 degraded。
        """
        accumulator = self._pending_loss
        if accumulator is None:
            return ()
        controls = []
        if accumulator.first_gap is not None and accumulator.last_gap is not None:
            controls.append(self._build_gap_event_locked(accumulator))
        controls.append(self._build_degraded_event_locked(accumulator))
        self._pending_loss = None
        self._control_emitted_count += len(controls)
        return tuple(controls)

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
        multiple = accumulator.multiple_gap_scopes
        sequence = last.source_sequence
        if multiple or not sequence:
            sequence = SourceSequence(
                components={"queue_loss_index": last.queue_loss_index},
                ordering_scope="queue_arrival",
            )
        payload = {
            "state": "degraded",
            "reason": "queue_overflow",
            "continuous": False,
            "loss_count": accumulator.gap_loss_count,
            "total_overflow_count": self._overflow_count,
            "first_lost": first.as_payload(),
            "last_lost": last.as_payload(),
            "multiple_scopes": multiple,
            "queue_capacity": self._capacity,
            "queue_high_watermark": self._high_watermark,
        }
        return SequenceGapEvent(
            provider=first.provider,
            capability_key=first.capability_key,
            event_type=MarketEventType.STREAM_GAP,
            level=first.level,
            exchange=first.exchange if not multiple else "MULTI",
            session_epoch=first.session_epoch if not multiple else "multiple-epochs",
            payload=payload,
            security=first.security if not multiple else None,
            raw_security_code=first.raw_security_code if not multiple else None,
            gateway_received_at=last.gateway_received_at or self._now(),
            stream_id=(first.stream_id or "queue-overflow") if not multiple else "multiple-streams",
            channel_id=(first.channel_id or "market-data") if not multiple else "multiple-channels",
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
        multiple = accumulator.multiple_scopes
        payload = {
            "state": "degraded",
            "reason": "queue_overflow",
            "continuous": False,
            "loss_count": accumulator.loss_count,
            "total_overflow_count": self._overflow_count,
            "first_lost": first.as_payload(),
            "last_lost": last.as_payload(),
            "multiple_scopes": multiple,
            "event_type_counts": dict(sorted(accumulator.event_type_counts.items())),
            "queue_capacity": self._capacity,
            "queue_high_watermark": self._high_watermark,
        }
        return ConnectionStateEvent(
            provider=first.provider,
            capability_key=first.capability_key,
            event_type=MarketEventType.STREAM_STATUS,
            level=first.level,
            exchange=first.exchange if not multiple else "MULTI",
            session_epoch=first.session_epoch if not multiple else "multiple-epochs",
            payload=payload,
            gateway_received_at=last.gateway_received_at or self._now(),
            stream_id=(first.stream_id or "queue-overflow") if not multiple else "multiple-streams",
            channel_id=(first.channel_id or "market-data") if not multiple else "multiple-channels",
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

    def _control_depth_locked(self) -> int:
        """返回当前聚合窗口实际会生成的控制事件数量。

        Args:
            无。

        Returns:
            int: 无损失为零，仅非逐笔损失为一，含逐笔损失为二。

        Side Effects:
            无。
        """
        if self._pending_loss is None:
            return 0
        return 2 if self._pending_loss.first_gap is not None else 1

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
            degraded=self._degraded,
        )

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
    "MarketEventDrainBatch",
    "MarketEventQueueMetrics",
    "QueuePutOutcome",
    "QueuePutResult",
]
