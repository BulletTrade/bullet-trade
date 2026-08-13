"""
作者: BruceLee

文件职责: 验证通用行情队列的有界内存、事件顺序、快照合并和显式损失控制合同。
主要输入: 脱敏合成 L2 快照、IOPV、逐笔成交与市场状态事件。
主要输出: 入队结果、FIFO drain、SequenceGapEvent、degraded 状态和确定性指标断言。
上游关系: 覆盖 bullet_trade.market_data.queue 的公开离线接口。
下游关系: 为 realtime feed、native drain 和远程 market writer 的背压接入提供回归门禁。
关键配置约定: 测试不联网、不加载厂商 SDK、不启动服务且不执行任何交易动作。
"""

from datetime import datetime, timedelta
from threading import Barrier, Thread
from typing import List

import pytest

from bullet_trade.market_data.models import (
    CompatibilityTickEvent,
    ConnectionStateEvent,
    ConsolidatedTickEvent,
    DepthSnapshotEvent,
    IopvEvent,
    MarketDataLevel,
    MarketEvent,
    MarketEventType,
    MarketStatusEvent,
    OrderDetailEvent,
    SequenceGapEvent,
    SourceSequence,
    TransactionEvent,
)
from bullet_trade.market_data.queue import BoundedMarketEventQueue, QueuePutOutcome

pytestmark = pytest.mark.unit

_BASE_TIME = datetime(2026, 8, 13, 9, 30, 0)


def _transaction(
    sequence: int,
    *,
    stream_id: str = "transaction-stream",
    channel_id: str = "channel-1",
    security: str = "600000.XSHG",
) -> TransactionEvent:
    """构造带明确通道序列的脱敏逐笔成交事件。

    Args:
        sequence: 同一测试通道内的 MainSeq。
        stream_id: 事件所属 stream ID。
        channel_id: 事件所属 channel ID。
        security: 标准证券代码。

    Returns:
        TransactionEvent: 可用于验证无损入队或 loss boundary 的 L2 事件。

    Side Effects:
        无。
    """
    raw_security = security.split(".", 1)[0]
    return TransactionEvent(
        provider="mock",
        capability_key="realtime.stream.transaction",
        event_type=MarketEventType.TRANSACTION,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch="epoch-1",
        security=security,
        raw_security_code=raw_security,
        stream_id=stream_id,
        channel_id=channel_id,
        source_sequence=SourceSequence(components={"MainSeq": sequence}),
        payload={"price": 10.0, "sequence": sequence},
        gateway_received_at=_BASE_TIME + timedelta(microseconds=sequence),
    )


def _loss_sensitive_l2_event(
    event_type: MarketEventType,
    sequence: int,
) -> MarketEvent:
    """构造必须通过 gap 暴露队列损失的三类 L2 逐笔事件。

    Args:
        event_type: transaction、order_detail 或 consolidated_tick。
        sequence: 写入原始 MainSeq 的序号。

    Returns:
        MarketEvent: 对应具体类型、带完整连续性作用域的逐笔事件。

    Raises:
        ValueError: event_type 不属于三类无损逐笔事件时抛出。

    Side Effects:
        无。
    """
    event_classes = {
        MarketEventType.TRANSACTION: TransactionEvent,
        MarketEventType.ORDER_DETAIL: OrderDetailEvent,
        MarketEventType.CONSOLIDATED_TICK: ConsolidatedTickEvent,
    }
    capabilities = {
        MarketEventType.TRANSACTION: "realtime.stream.transaction",
        MarketEventType.ORDER_DETAIL: "realtime.stream.order_detail",
        MarketEventType.CONSOLIDATED_TICK: "realtime.stream.consolidated_tick",
    }
    try:
        event_class = event_classes[event_type]
        capability_key = capabilities[event_type]
    except KeyError as exc:
        raise ValueError("event_type 必须是无损 L2 逐笔事件") from exc
    return event_class(
        provider="mock",
        capability_key=capability_key,
        event_type=event_type,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch="epoch-1",
        security="600000.XSHG",
        raw_security_code="600000",
        stream_id="loss-sensitive-stream",
        channel_id="channel-1",
        source_sequence=SourceSequence(components={"MainSeq": sequence}),
        payload={"price": 10.0, "sequence": sequence},
        gateway_received_at=_BASE_TIME + timedelta(microseconds=sequence),
    )


def _snapshot(price: float) -> DepthSnapshotEvent:
    """构造同一证券、epoch 和通道的 L2 最新快照。

    Args:
        price: 写入 canonical payload 的最新价。

    Returns:
        DepthSnapshotEvent: 可按证券和事件类型合并的快照事件。

    Side Effects:
        无。
    """
    return DepthSnapshotEvent(
        provider="mock",
        capability_key="realtime.snapshot.l2",
        event_type=MarketEventType.SNAPSHOT_L2,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch="epoch-1",
        security="600000.XSHG",
        raw_security_code="600000",
        stream_id="snapshot-stream",
        channel_id="channel-1",
        payload={"last_price": price},
        gateway_received_at=_BASE_TIME + timedelta(milliseconds=int(price * 10)),
    )


def _iopv(value: float) -> IopvEvent:
    """构造同一证券的独立 IOPV 事件。

    Args:
        value: IOPV 数值。

    Returns:
        IopvEvent: 允许按证券和事件类型保留最新值的事件。

    Side Effects:
        无。
    """
    return IopvEvent(
        provider="mock",
        capability_key="realtime.stream.iopv",
        event_type=MarketEventType.IOPV,
        level=MarketDataLevel.L1,
        exchange="XSHG",
        session_epoch="epoch-1",
        security="510300.XSHG",
        raw_security_code="510300",
        payload={"iopv": value},
        gateway_received_at=_BASE_TIME + timedelta(milliseconds=int(value * 10)),
    )


def _market_status(state: str) -> MarketStatusEvent:
    """构造不带厂商序列的市场状态事件。

    Args:
        state: 脱敏市场状态文本。

    Returns:
        MarketStatusEvent: 不允许按快照规则相互覆盖的 L1 状态事件。

    Side Effects:
        无。
    """
    return MarketStatusEvent(
        provider="mock",
        capability_key="realtime.stream.market_status",
        event_type=MarketEventType.MARKET_STATUS,
        level=MarketDataLevel.L1,
        exchange="XSHG",
        session_epoch="epoch-1",
        payload={"state": state},
        gateway_received_at=_BASE_TIME,
    )


def _compatibility_tick(price: float) -> CompatibilityTickEvent:
    """构造旧策略使用的兼容 tick 投影事件。

    Args:
        price: 写入兼容 payload 的最新价。

    Returns:
        CompatibilityTickEvent: 不属于规范合并白名单的兼容事件。

    Side Effects:
        无。
    """
    return CompatibilityTickEvent(
        provider="mock",
        capability_key="realtime.stream.tick_compat",
        event_type=MarketEventType.TICK_COMPAT,
        level=MarketDataLevel.TICK_COMPAT,
        exchange="XSHG",
        session_epoch="epoch-1",
        security="600000.XSHG",
        raw_security_code="600000",
        payload={"last_price": price},
        gateway_received_at=_BASE_TIME,
    )


def test_snapshot_coalesce_keeps_latest_arrival_order() -> None:
    """验证同证券快照替换旧值并移动到最新到达位置，不越过先到逐笔。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        仅创建和 drain 本地内存队列。
    """
    queue = BoundedMarketEventQueue(capacity=3)
    first_snapshot = _snapshot(10.1)
    transaction = _transaction(1)
    latest_snapshot = _snapshot(10.2)

    assert queue.put_nowait(first_snapshot).outcome is QueuePutOutcome.ENQUEUED
    assert queue.put_nowait(transaction).outcome is QueuePutOutcome.ENQUEUED
    assert queue.put_nowait(latest_snapshot).outcome is QueuePutOutcome.COALESCED

    batch = queue.drain()
    metrics = queue.metrics()

    assert batch.control_events == ()
    assert batch.data_events == (transaction, latest_snapshot)
    assert metrics.enqueued_count == 2
    assert metrics.coalesced_count == 1
    assert metrics.drained_count == 2
    assert metrics.high_watermark == 2
    assert metrics.overflow_count == 0


def test_iopv_coalesces_but_non_snapshot_status_does_not() -> None:
    """验证 IOPV 可保留最新值，而普通状态事件在满队列时产生显式损失。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        仅操作本地有界队列并读取生成的控制事件。
    """
    iopv_queue = BoundedMarketEventQueue(capacity=1)
    first_iopv = _iopv(4.01)
    latest_iopv = _iopv(4.02)
    assert iopv_queue.put_nowait(first_iopv).accepted is True
    assert iopv_queue.put_nowait(latest_iopv).outcome is QueuePutOutcome.COALESCED
    assert iopv_queue.drain_data() == (latest_iopv,)

    status_queue = BoundedMarketEventQueue(capacity=1)
    first_status = _market_status("open")
    lost_status = _market_status("halted")
    assert status_queue.put_nowait(first_status).accepted is True
    result = status_queue.put_nowait(lost_status)
    controls = status_queue.drain_control()

    assert result.outcome is QueuePutOutcome.OVERFLOW
    assert status_queue.drain_data() == (first_status,)
    assert tuple(event.event_type for event in controls) == (MarketEventType.STREAM_STATUS,)
    assert controls[0].payload["last_lost"]["queue_loss_index"] == 1
    assert status_queue.metrics().coalesced_count == 0


def test_compatibility_tick_does_not_expand_snapshot_coalesce_whitelist() -> None:
    """验证兼容 tick 不被按快照合并，队列满时明确进入损失边界。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        仅向本地容量一队列写入两个兼容事件并读取降级控制事件。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    first_tick = _compatibility_tick(10.1)
    lost_tick = _compatibility_tick(10.2)

    assert queue.put_nowait(first_tick).accepted is True
    result = queue.put_nowait(lost_tick)
    controls = queue.drain_control()

    assert result.outcome is QueuePutOutcome.OVERFLOW
    assert queue.drain_data() == (first_tick,)
    assert tuple(event.event_type for event in controls) == (MarketEventType.STREAM_STATUS,)
    assert controls[0].payload["continuous"] is False
    assert queue.metrics().coalesced_count == 0


@pytest.mark.parametrize(
    "event_type",
    (
        MarketEventType.TRANSACTION,
        MarketEventType.ORDER_DETAIL,
        MarketEventType.CONSOLIDATED_TICK,
    ),
)
def test_loss_sensitive_l2_overflow_uses_out_of_band_gap_and_degraded_events(
    event_type: MarketEventType,
) -> None:
    """验证三类逐笔在普通数据已满时均通过队外控制路径暴露损失。

    Args:
        event_type: 当前参数化验证的无损 L2 事件类型。

    Returns:
        None。

    Side Effects:
        仅在本地记录一次 loss boundary 并 drain 控制/数据分区。
    """
    queue = BoundedMarketEventQueue(capacity=1, now_provider=lambda: _BASE_TIME)
    retained = _snapshot(10.1)
    lost = _loss_sensitive_l2_event(event_type, 2)
    assert queue.put_nowait(retained).accepted is True

    result = queue.put_nowait(lost)
    before_drain = queue.metrics()
    controls = queue.drain_control()

    assert result.outcome is QueuePutOutcome.OVERFLOW
    assert result.accepted is False
    assert before_drain.data_depth == 1
    assert before_drain.control_depth == 2
    assert before_drain.control_capacity == 2
    assert len(queue) == 1
    assert len(controls) == 2
    gap, degraded = controls
    assert isinstance(gap, SequenceGapEvent)
    assert isinstance(degraded, ConnectionStateEvent)
    assert gap.payload["reason"] == "queue_overflow"
    assert gap.payload["continuous"] is False
    assert gap.payload["loss_count"] == 1
    assert gap.payload["first_lost"]["event_type"] == event_type.value
    assert gap.payload["first_lost"]["source_sequence"]["MainSeq"] == 2
    assert gap.payload["last_lost"]["source_sequence"]["MainSeq"] == 2
    assert degraded.payload["state"] == "degraded"
    assert queue.drain_data() == (retained,)


def test_repeated_overflow_aggregates_exact_first_last_boundary_in_fixed_slots() -> None:
    """验证持续过载只占固定控制槽，同时保留精确首末序列和累计丢失数。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        在满队列上模拟三次逐笔溢出并清除一次控制窗口。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_transaction(1))
    for sequence in (2, 3, 4):
        assert queue.put_nowait(_transaction(sequence)).accepted is False

    metrics = queue.metrics()
    controls = queue.drain_control()
    gap = controls[0]

    assert metrics.data_depth == 1
    assert metrics.control_depth == 2
    assert metrics.overflow_count == 3
    assert metrics.loss_boundary_count == 1
    assert metrics.overflow_by_event_type == {"transaction": 3}
    assert gap.payload["loss_count"] == 3
    assert gap.payload["first_lost"]["source_sequence"]["MainSeq"] == 2
    assert gap.payload["last_lost"]["source_sequence"]["MainSeq"] == 4
    assert gap.source_sequence["MainSeq"] == 4

    assert queue.put_nowait(_transaction(5)).accepted is False
    assert queue.metrics().loss_boundary_count == 2
    assert queue.metrics().control_depth == 2


def test_multiple_loss_scopes_are_marked_without_fabricating_single_stream_order() -> None:
    """验证多个通道同时丢失时控制事件显式标记聚合，而不伪造跨通道总序。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        在满队列上生成两个不同 stream/channel 的损失点并 drain 控制事件。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_transaction(1))
    queue.put_nowait(_transaction(2, stream_id="stream-a", channel_id="channel-a"))
    queue.put_nowait(_transaction(9, stream_id="stream-b", channel_id="channel-b"))

    gap, degraded = queue.drain_control()

    assert gap.payload["multiple_scopes"] is True
    assert gap.stream_id == "multiple-streams"
    assert gap.channel_id == "multiple-channels"
    assert gap.payload["first_lost"]["stream_id"] == "stream-a"
    assert gap.payload["last_lost"]["stream_id"] == "stream-b"
    assert gap.source_sequence.ordering_scope == "queue_arrival"
    assert gap.source_sequence["queue_loss_index"] == 2
    assert degraded.payload["multiple_scopes"] is True
    assert degraded.payload["loss_count"] == 2


def test_drain_does_not_fabricate_continuity_recovery() -> None:
    """验证腾空数据和控制事件后 degraded 仍单调保持，新数据不会伪装补齐缺口。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        产生一次溢出、完整 drain 后再次投递本地事件。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_transaction(1))
    queue.put_nowait(_transaction(2))

    batch = queue.drain()
    assert tuple(event.event_type for event in batch.control_events) == (
        MarketEventType.STREAM_GAP,
        MarketEventType.STREAM_STATUS,
    )
    assert batch.data_events[0].source_sequence["MainSeq"] == 1
    assert batch.events == batch.control_events + batch.data_events
    assert queue.metrics().data_depth == 0
    assert queue.metrics().control_depth == 0
    assert queue.metrics().degraded is True

    assert queue.put_nowait(_transaction(3)).accepted is True
    assert queue.drain_control() == ()
    assert queue.metrics().degraded is True


def test_concurrent_producers_keep_hard_capacity_and_exact_metrics() -> None:
    """验证多个生产线程并发入队时容量、水位和总计数保持确定。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        启动四个短生命周期本地线程向同一内存队列投递合成逐笔事件。
    """
    worker_count = 4
    events_per_worker = 50
    capacity = 32
    barrier = Barrier(worker_count)
    queue = BoundedMarketEventQueue(capacity=capacity)
    workers: List[Thread] = []

    def _produce(worker_index: int) -> None:
        """等待并发起点后投递当前 worker 的唯一序列事件。

        Args:
            worker_index: 用于生成不重复 MainSeq 的线程编号。

        Returns:
            None。

        Side Effects:
            等待本地 Barrier，并对共享 BoundedMarketEventQueue 调用 put_nowait。
        """
        barrier.wait()
        start = worker_index * events_per_worker
        for offset in range(events_per_worker):
            queue.put_nowait(_transaction(start + offset + 1))

    for worker_index in range(worker_count):
        worker = Thread(target=_produce, args=(worker_index,))
        workers.append(worker)
        worker.start()
    for worker in workers:
        worker.join(timeout=5)

    metrics = queue.metrics()
    total_events = worker_count * events_per_worker

    assert all(not worker.is_alive() for worker in workers)
    assert len(queue) == capacity
    assert metrics.data_depth == capacity
    assert metrics.high_watermark == capacity
    assert metrics.enqueued_count == capacity
    assert metrics.overflow_count == total_events - capacity
    assert metrics.enqueued_count + metrics.overflow_count == total_events
    assert metrics.control_depth == 2
    assert metrics.loss_boundary_count == 1


def test_capacity_and_drain_limits_fail_closed() -> None:
    """验证非法容量与 drain 上限被同步拒绝，不进入模糊无界行为。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        仅构造本地对象并触发参数校验异常。
    """
    with pytest.raises(ValueError, match="capacity"):
        BoundedMarketEventQueue(capacity=0)

    queue = BoundedMarketEventQueue(capacity=1)
    with pytest.raises(ValueError, match="max_items"):
        queue.drain_data(-1)
    with pytest.raises(ValueError, match="max_items"):
        queue.drain(max_data_items=True)
