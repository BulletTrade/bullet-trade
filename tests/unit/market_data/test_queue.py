"""
作者: BruceLee

文件职责: 验证通用行情队列的有界内存、事件顺序、快照合并和显式损失控制合同。
主要输入: 脱敏合成 L2 快照、IOPV、逐笔成交与市场状态事件。
主要输出: 入队结果、FIFO drain、SequenceGapEvent、degraded 状态和确定性指标断言。
上游关系: 覆盖 bullet_trade.market_data.queue 的公开离线接口。
下游关系: 为 realtime feed、native drain 和远程 market writer 的背压接入提供回归门禁。
关键配置约定: 测试不联网、不加载厂商 SDK、不启动服务且不执行任何交易动作。
"""

from dataclasses import replace
from datetime import datetime, timedelta
from threading import Barrier, Thread
from typing import List, Optional, Tuple

import pytest

from bullet_trade.market_data.capability import (
    CapabilityDeclaration,
    CapabilityManifest,
    CapabilityReadiness,
    CapabilitySupport,
    ProviderLocation,
)
from bullet_trade.market_data.feed import MockRealtimeMarketDataFeed
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
from bullet_trade.market_data.queue import (
    BoundedMarketEventQueue,
    MarketEventControlAckError,
    MarketEventControlCapacityError,
    MarketEventControlDrainError,
    MarketEventRecoveryAuthorizationError,
    QueuePutOutcome,
)

pytestmark = pytest.mark.unit

_BASE_TIME = datetime(2026, 8, 13, 9, 30, 0)


def _transaction(
    sequence: int,
    *,
    stream_id: str = "transaction-stream",
    channel_id: str = "channel-1",
    security: str = "600000.XSHG",
    session_epoch: str = "epoch-1",
    provider: str = "mock",
) -> TransactionEvent:
    """构造带明确通道序列的脱敏逐笔成交事件。

    Args:
        sequence: 同一测试通道内的 MainSeq。
        stream_id: 事件所属 stream ID。
        channel_id: 事件所属 channel ID。
        security: 标准证券代码。
        session_epoch: 事件所属的连接会话 epoch。
        provider: 事件所属的实时数据后端。

    Returns:
        TransactionEvent: 可用于验证无损入队或 loss boundary 的 L2 事件。

    Side Effects:
        无。
    """
    raw_security = security.split(".", 1)[0]
    return TransactionEvent(
        provider=provider,
        capability_key="realtime.stream.transaction",
        event_type=MarketEventType.TRANSACTION,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch=session_epoch,
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
    *,
    session_epoch: str = "epoch-1",
    stream_id: str = "loss-sensitive-stream",
    channel_id: str = "channel-1",
    security: str = "600000.XSHG",
) -> MarketEvent:
    """构造必须通过 gap 暴露队列损失的三类 L2 逐笔事件。

    Args:
        event_type: transaction、order_detail 或 consolidated_tick。
        sequence: 写入原始 MainSeq 的序号。
        session_epoch: 事件所属的连接会话 epoch。
        stream_id: 事件所属 stream ID。
        channel_id: 事件所属 channel ID。
        security: 标准证券代码。

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
        session_epoch=session_epoch,
        security=security,
        raw_security_code=security.split(".", 1)[0],
        stream_id=stream_id,
        channel_id=channel_id,
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


def _source_gap(
    sequence: int,
    *,
    provider: str = "source",
    session_epoch: str = "source-epoch-1",
    stream_id: str = "source-stream",
    channel_id: str = "source-channel",
    security: str = "600000.XSHG",
) -> SequenceGapEvent:
    """构造应通过可靠队外路径发布的来源缺口事件。

    Args:
        sequence: 写入来源 MainSeq 和 boundary ID 的序号。
        provider: 来源后端名称。
        session_epoch: 来源连接 epoch。
        stream_id: 来源 stream ID。
        channel_id: 来源 channel ID。
        security: 标准证券代码。

    Returns:
        SequenceGapEvent: 带完整 per-scope provenance 的不可变控制事件。

    Side Effects:
        无。
    """
    return SequenceGapEvent(
        provider=provider,
        capability_key="realtime.stream.transaction",
        event_type=MarketEventType.STREAM_GAP,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch=session_epoch,
        security=security,
        raw_security_code=security.split(".", 1)[0],
        stream_id=stream_id,
        channel_id=channel_id,
        source_sequence=SourceSequence(components={"MainSeq": sequence}),
        payload={
            "state": "degraded",
            "continuous": False,
            "reason": "source_gap",
            "loss_boundary_id": f"source-gap-{sequence}",
        },
        gateway_received_at=_BASE_TIME + timedelta(microseconds=sequence),
        completeness=False,
    )


def _source_status(
    sequence: int,
    *,
    continuous: Optional[bool],
    state: str,
    provider: str = "source",
    session_epoch: str = "source-epoch-1",
    stream_id: str = "source-stream",
    channel_id: str = "source-channel",
    security: str = "600000.XSHG",
    recovery_confirmed: Optional[bool] = None,
    recovery_id: Optional[str] = None,
) -> ConnectionStateEvent:
    """构造来源连接/连续性状态控制事件。

    Args:
        sequence: 写入诊断 payload 和接收时间的稳定序号。
        continuous: 来源声称的逐笔连续性状态；None 表示省略该证据。
        state: 来源连接状态文本。
        provider: 来源后端名称。
        session_epoch: 来源连接 epoch。
        stream_id: 来源 stream ID。
        channel_id: 来源 channel ID。
        security: 标准证券代码。
        recovery_confirmed: 可选的显式恢复确认布尔值。
        recovery_id: 可选的一次性恢复 ID。

    Returns:
        ConnectionStateEvent: 可用于验证降级保持和可靠 ACK 的状态事件。

    Side Effects:
        无。
    """
    payload = {
        "state": state,
        "reason": "source_status",
        "status_sequence": sequence,
    }
    if continuous is not None:
        payload["continuous"] = continuous
    if recovery_confirmed is not None:
        payload["recovery_confirmed"] = recovery_confirmed
    if recovery_id is not None:
        payload["recovery_id"] = recovery_id
    return ConnectionStateEvent(
        provider=provider,
        capability_key="realtime.stream.transaction",
        event_type=MarketEventType.STREAM_STATUS,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch=session_epoch,
        security=security,
        raw_security_code=security.split(".", 1)[0],
        stream_id=stream_id,
        channel_id=channel_id,
        payload=payload,
        gateway_received_at=_BASE_TIME + timedelta(microseconds=sequence),
        completeness=continuous is True,
    )


def _take_and_ack_controls(queue: BoundedMarketEventQueue) -> Tuple[MarketEvent, ...]:
    """通过可靠 take/ACK 合同取得当前控制窗口。

    Args:
        queue: 已含至少一个 pending 控制事件的本地有界队列。

    Returns:
        Tuple[MarketEvent, ...]: 当前不可变 delivery 中的控制事件。

    Side Effects:
        创建并精确 ACK 一个 delivery，使下一 pending 窗口可继续投递。
    """
    delivery = queue.take_control()
    assert delivery is not None
    events = delivery.events
    queue.ack_control(delivery.delivery_id)
    return events


def _authorize_source_recovery(
    queue: BoundedMarketEventQueue,
    recovery_id: str,
) -> None:
    """为默认来源 transaction scope 登记一次性恢复授权。

    Args:
        queue: 已包含 active degraded 默认来源 scope 的队列。
        recovery_id: 测试使用的脱敏一次性恢复 ID。

    Returns:
        None。

    Side Effects:
        调用队列公开授权 API；不联网、不启动 feed，也不执行交易。
    """
    queue.authorize_continuity_recovery(
        provider="source",
        capability_key="realtime.stream.transaction",
        level=MarketDataLevel.L2,
        session_epoch="source-epoch-1",
        stream_id="source-stream",
        channel_id="source-channel",
        exchange="XSHG",
        recovery_id=recovery_id,
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
    controls = _take_and_ack_controls(status_queue)

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
    controls = _take_and_ack_controls(queue)

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
    controls = _take_and_ack_controls(queue)

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
    controls = _take_and_ack_controls(queue)
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


def test_multiple_securities_in_one_scope_keep_real_control_identity() -> None:
    """验证同一通道多证券可有界聚合，但控制事件仍保留真实 epoch 和通道。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        在满队列上生成同 stream/channel 不同证券的损失并 drain 控制事件。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_transaction(1))
    queue.put_nowait(_transaction(2, security="600000.XSHG"))
    queue.put_nowait(_transaction(9, security="600001.XSHG"))

    gap, degraded = _take_and_ack_controls(queue)

    assert gap.provider == "mock"
    assert gap.capability_key == "realtime.stream.transaction"
    assert gap.level is MarketDataLevel.L2
    assert gap.session_epoch == "epoch-1"
    assert gap.stream_id == "transaction-stream"
    assert gap.channel_id == "channel-1"
    assert gap.exchange == "XSHG"
    assert gap.security is None
    assert gap.payload["multiple_scopes"] is False
    assert gap.payload["multiple_securities"] is True
    assert gap.payload["first_lost"]["security"] == "600000.XSHG"
    assert gap.payload["last_lost"]["security"] == "600001.XSHG"
    assert gap.source_sequence["MainSeq"] == 9
    assert degraded.session_epoch == "epoch-1"
    assert degraded.stream_id == "transaction-stream"
    assert degraded.channel_id == "channel-1"
    assert degraded.security is None
    assert degraded.payload["multiple_scopes"] is False
    assert degraded.payload["multiple_securities"] is True
    assert degraded.payload["loss_count"] == 2


def test_different_provider_capability_and_epoch_emit_independent_controls() -> None:
    """验证跨 provider、capability 或 epoch 的损失分别发布真实身份。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        在容量一的本地队列上产生四个控制 scope 并一次 drain。
    """
    queue = BoundedMarketEventQueue(capacity=1, control_scope_capacity=4)
    queue.put_nowait(_transaction(1))
    losses = (
        _loss_sensitive_l2_event(
            MarketEventType.TRANSACTION,
            2,
            session_epoch="epoch-1",
            stream_id="transaction-stream",
        ),
        _loss_sensitive_l2_event(
            MarketEventType.ORDER_DETAIL,
            3,
            session_epoch="epoch-1",
            stream_id="order-detail-stream",
        ),
        _loss_sensitive_l2_event(
            MarketEventType.TRANSACTION,
            4,
            session_epoch="epoch-2",
            stream_id="transaction-stream",
        ),
        _transaction(
            5,
            provider="secondary",
            session_epoch="epoch-1",
            stream_id="transaction-stream",
        ),
    )
    for event in losses:
        assert queue.put_nowait(event).outcome is QueuePutOutcome.OVERFLOW

    controls = _take_and_ack_controls(queue)
    gaps = controls[::2]
    degraded_events = controls[1::2]
    expected_identities = tuple(
        (
            event.provider,
            event.capability_key,
            event.session_epoch,
            event.stream_id,
            event.channel_id,
        )
        for event in losses
    )
    gap_identities = tuple(
        (
            event.provider,
            event.capability_key,
            event.session_epoch,
            event.stream_id,
            event.channel_id,
        )
        for event in gaps
    )
    degraded_identities = tuple(
        (
            event.provider,
            event.capability_key,
            event.session_epoch,
            event.stream_id,
            event.channel_id,
        )
        for event in degraded_events
    )

    assert len(controls) == 8
    assert gap_identities == expected_identities
    assert degraded_identities == expected_identities
    assert all(event.exchange == "XSHG" for event in controls)
    assert all(event.payload["multiple_scopes"] is False for event in controls)
    assert queue.metrics().control_scope_depth == 0


def test_control_scope_capacity_exhaustion_fails_closed() -> None:
    """验证队外 scope 容量耗尽时明确抛错，不把新 epoch 损失静默并入旧事件。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        使本地控制路径达到配置上限并读取累计指标。
    """
    queue = BoundedMarketEventQueue(capacity=1, control_scope_capacity=1)
    queue.put_nowait(_transaction(1))
    assert queue.put_nowait(_transaction(2, session_epoch="epoch-1")).accepted is False

    with pytest.raises(MarketEventControlCapacityError) as exc_info:
        queue.put_nowait(_transaction(3, session_epoch="epoch-2"))

    metrics = queue.metrics()
    assert exc_info.value.control_scope_capacity == 1
    assert exc_info.value.scope_key[3] == "epoch-2"
    assert metrics.overflow_count == 2
    assert metrics.control_overflow_count == 1
    assert metrics.control_scope_depth == 1
    assert metrics.control_depth == 2
    assert tuple(event.session_epoch for event in _take_and_ack_controls(queue)) == (
        "epoch-1",
        "epoch-1",
    )


def test_public_source_controls_bypass_full_data_capacity_with_provenance() -> None:
    """验证来源 gap/status 在普通数据已满时仍进入可靠队外控制窗口。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        填满本地数据区，投递两个脱敏来源控制事件并创建 in-flight delivery。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    retained = _transaction(1)
    gap = _source_gap(10)
    status = _source_status(11, continuous=False, state="degraded")
    assert queue.put_nowait(retained).accepted is True

    gap_result = queue.put_nowait(gap)
    status_result = queue.put_nowait(status)
    before_take = queue.metrics()
    delivery = queue.take_control()

    assert gap_result.outcome is QueuePutOutcome.CONTROL_ENQUEUED
    assert status_result.outcome is QueuePutOutcome.CONTROL_ENQUEUED
    assert len(queue) == 1
    assert before_take.control_pending_depth == 2
    assert before_take.control_received_count == 2
    assert delivery is not None
    assert delivery.events == (gap, status)
    assert delivery.scope_count == 1
    assert tuple(
        (
            event.provider,
            event.capability_key,
            event.level,
            event.session_epoch,
            event.stream_id,
            event.channel_id,
            event.exchange,
        )
        for event in delivery.events
    ) == (
        (
            "source",
            "realtime.stream.transaction",
            MarketDataLevel.L2,
            "source-epoch-1",
            "source-stream",
            "source-channel",
            "XSHG",
        ),
        (
            "source",
            "realtime.stream.transaction",
            MarketDataLevel.L2,
            "source-epoch-1",
            "source-stream",
            "source-channel",
            "XSHG",
        ),
    )
    after_take = queue.metrics()
    assert after_take.control_pending_depth == 0
    assert after_take.control_inflight_depth == 2
    assert after_take.control_outstanding_depth == 2
    assert after_take.control_delivery_inflight is True
    assert queue.drain_data() == (retained,)


def test_source_gap_a_b_a_keeps_monotonic_multiple_securities() -> None:
    """验证来源 gap 的证券身份 A→B→A 后多证券标志不会回退。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        在单个本地 scope 中聚合三个脱敏来源 gap 并可靠 ACK。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_source_gap(1, security="600000.XSHG"))
    queue.put_nowait(_source_gap(2, security="600001.XSHG"))
    queue.put_nowait(_source_gap(3, security="600000.XSHG"))

    (gap,) = _take_and_ack_controls(queue)

    assert gap.security is None
    assert gap.raw_security_code is None
    assert gap.payload["multiple_securities"] is True
    assert gap.payload["source_control_window"]["first"]["security"] == "600000.XSHG"
    assert gap.payload["source_control_window"]["last"]["security"] == "600000.XSHG"
    assert gap.payload["source_control_window"]["count"] == 3


def test_source_status_a_b_a_keeps_monotonic_multiple_securities() -> None:
    """验证来源 status 的证券身份 A→B→A 后多证券标志不会回退。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        在单个本地 scope 中聚合三个脱敏降级状态并可靠 ACK。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_source_status(1, continuous=False, state="degraded", security="600000.XSHG"))
    queue.put_nowait(_source_status(2, continuous=False, state="degraded", security="600001.XSHG"))
    queue.put_nowait(_source_status(3, continuous=False, state="degraded", security="600000.XSHG"))

    (status,) = _take_and_ack_controls(queue)

    assert status.security is None
    assert status.raw_security_code is None
    assert status.payload["multiple_securities"] is True
    assert status.payload["source_control_window"]["first"]["security"] == "600000.XSHG"
    assert status.payload["source_control_window"]["last"]["security"] == "600000.XSHG"
    assert status.payload["source_control_window"]["count"] == 3


def test_active_degraded_lineage_forces_later_data_incomplete() -> None:
    """验证 active degraded scope 的普通数据不能重新声称连续或完整。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        激活默认来源 scope，投递带 continuous=true 的合成逐笔并 drain 数据。
    """
    queue = BoundedMarketEventQueue(capacity=2)
    queue.put_nowait(_source_gap(10))
    _take_and_ack_controls(queue)
    claimed_complete = _transaction(
        11,
        provider="source",
        session_epoch="source-epoch-1",
        stream_id="source-stream",
        channel_id="source-channel",
    )
    claimed_complete = replace(
        claimed_complete,
        payload={**dict(claimed_complete.payload), "continuous": True},
        completeness=True,
    )

    result = queue.put_nowait(claimed_complete)
    (stored,) = queue.drain_data()
    metrics = queue.metrics()

    assert result.accepted is True
    assert result.ever_degraded is True
    assert result.active_degraded is True
    assert stored is not claimed_complete
    assert stored.completeness is False
    assert stored.payload["continuous"] is False
    assert stored.payload["continuity_degraded"] is True
    assert stored.payload["recovery_required"] is True
    assert metrics.degraded is True
    assert metrics.ever_degraded is True
    assert metrics.active_degraded is True
    assert metrics.active_degraded_scope_depth == 1


def test_degraded_connected_status_needs_all_recovery_evidence() -> None:
    """验证 degraded lineage 的 connected/ready 缺字段或普通布尔都不能恢复。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        先激活来源 scope，再依次投递缺 continuous 和未预授权 recovery 状态。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_source_status(20, continuous=False, state="degraded"))
    _take_and_ack_controls(queue)

    queue.put_nowait(_source_status(21, continuous=None, state="connected"))
    (missing_continuity,) = _take_and_ack_controls(queue)
    assert missing_continuity.payload["continuous"] is False
    assert missing_continuity.payload["state"] == "degraded"
    assert missing_continuity.payload["recovery_blocked"] is True

    queue.put_nowait(
        _source_status(
            22,
            continuous=True,
            state="ready",
            recovery_confirmed=True,
            recovery_id="not-authorized",
        )
    )
    (plain_boolean_claim,) = _take_and_ack_controls(queue)
    assert plain_boolean_claim.payload["continuous"] is False
    assert plain_boolean_claim.payload["state"] == "degraded"
    assert plain_boolean_claim.payload["recovery_blocked"] is True
    assert queue.metrics().active_degraded is True


def test_recovery_authorization_is_exact_bounded_and_one_shot() -> None:
    """验证恢复必须先授权、精确匹配 scope/ID，并在成功后一次性消费。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        激活来源 scope，模拟错误 ID、正确恢复和未重新授权的 ID 重放。
    """
    queue = BoundedMarketEventQueue(capacity=2)
    with pytest.raises(MarketEventRecoveryAuthorizationError) as inactive_scope:
        _authorize_source_recovery(queue, "recovery-1")
    assert inactive_scope.value.code == "RECOVERY_SCOPE_NOT_ACTIVE_DEGRADED"

    queue.put_nowait(_source_gap(30))
    _take_and_ack_controls(queue)
    _authorize_source_recovery(queue, "recovery-1")
    _authorize_source_recovery(queue, "recovery-1")
    with pytest.raises(MarketEventRecoveryAuthorizationError) as conflict:
        _authorize_source_recovery(queue, "recovery-2")
    assert conflict.value.code == "RECOVERY_AUTHORIZATION_CONFLICT"

    queue.put_nowait(
        _source_status(
            31,
            continuous=True,
            state="connected",
            recovery_confirmed=True,
            recovery_id="wrong-id",
        )
    )
    (wrong_id_status,) = _take_and_ack_controls(queue)
    assert wrong_id_status.payload["continuous"] is False
    assert queue.metrics().recovery_authorization_depth == 1
    assert queue.metrics().active_degraded is True

    queue.put_nowait(
        _source_status(
            32,
            continuous=True,
            state="connected",
            recovery_confirmed=True,
            recovery_id="recovery-1",
        )
    )
    (recovered_status,) = _take_and_ack_controls(queue)
    recovered_metrics = queue.metrics()
    assert recovered_status.payload["continuous"] is True
    assert recovered_status.payload["recovery_authorized"] is True
    assert recovered_metrics.degraded is True
    assert recovered_metrics.ever_degraded is True
    assert recovered_metrics.active_degraded is False
    assert recovered_metrics.active_degraded_scope_depth == 0
    assert recovered_metrics.recovery_authorization_depth == 0
    assert recovered_metrics.recovery_authorization_count == 1
    assert recovered_metrics.recovery_completed_count == 1

    healthy_data = _transaction(
        33,
        provider="source",
        session_epoch="source-epoch-1",
        stream_id="source-stream",
        channel_id="source-channel",
    )
    queue.put_nowait(healthy_data)
    assert queue.drain_data() == (healthy_data,)

    queue.put_nowait(_source_gap(34))
    _take_and_ack_controls(queue)
    queue.put_nowait(
        _source_status(
            35,
            continuous=True,
            state="connected",
            recovery_confirmed=True,
            recovery_id="recovery-1",
        )
    )
    (replayed_status,) = _take_and_ack_controls(queue)
    assert replayed_status.payload["continuous"] is False
    assert queue.metrics().active_degraded is True


@pytest.mark.parametrize("healthy_state", ("connected", "ready"))
def test_only_allowlisted_healthy_states_can_complete_recovery(healthy_state: str) -> None:
    """验证 connected/ready 均可在完整授权证据下完成一次恢复。

    Args:
        healthy_state: 当前参数化验证的明确健康连接状态。

    Returns:
        None。

    Side Effects:
        ACK 一个来源 gap，登记一次性授权并可靠投递健康恢复状态。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_source_gap(36))
    _take_and_ack_controls(queue)
    _authorize_source_recovery(queue, f"recovery-{healthy_state}")

    queue.put_nowait(
        _source_status(
            37,
            continuous=True,
            state=healthy_state,
            recovery_confirmed=True,
            recovery_id=f"recovery-{healthy_state}",
        )
    )
    (recovered_status,) = _take_and_ack_controls(queue)
    metrics = queue.metrics()

    assert recovered_status.payload["state"] == healthy_state
    assert recovered_status.payload["continuous"] is True
    assert recovered_status.payload["recovery_authorized"] is True
    assert metrics.active_degraded is False
    assert metrics.recovery_completed_count == 1


@pytest.mark.parametrize(
    "unhealthy_state",
    (
        "disconnected",
        "reconnecting",
        "auth_failed",
        "unavailable",
        "not_ready",
        "error",
        "",
        "future_unknown_state",
    ),
)
def test_unhealthy_or_unknown_state_never_claims_or_completes_recovery(
    unhealthy_state: str,
) -> None:
    """验证负态、空态和未知态无论布尔声称如何都只输出 degraded。

    Args:
        unhealthy_state: 当前参数化验证的负态、空态或未来未知状态。

    Returns:
        None。

    Side Effects:
        分别在新 scope 和已授权 active scope 投递 continuous=true 状态并可靠 ACK。
    """
    fresh_queue = BoundedMarketEventQueue(capacity=1)
    fresh_queue.put_nowait(
        _source_status(
            38,
            continuous=True,
            state=unhealthy_state,
            recovery_confirmed=True,
            recovery_id="untrusted-recovery",
        )
    )
    (fresh_status,) = _take_and_ack_controls(fresh_queue)
    assert fresh_status.payload["state"] == "degraded"
    assert fresh_status.payload["continuous"] is False
    assert fresh_status.payload["reason"] == "unhealthy_connection_state"
    assert "recovery_authorized" not in fresh_status.payload
    assert fresh_queue.metrics().active_degraded is True

    active_queue = BoundedMarketEventQueue(capacity=1)
    active_queue.put_nowait(_source_gap(39))
    _take_and_ack_controls(active_queue)
    _authorize_source_recovery(active_queue, "exact-but-unhealthy")
    active_queue.put_nowait(
        _source_status(
            40,
            continuous=True,
            state=unhealthy_state,
            recovery_confirmed=True,
            recovery_id="exact-but-unhealthy",
        )
    )
    (active_status,) = _take_and_ack_controls(active_queue)
    metrics = active_queue.metrics()

    assert active_status.payload["state"] == "degraded"
    assert active_status.payload["continuous"] is False
    assert active_status.payload["reason"] == "unhealthy_connection_state"
    assert "recovery_authorized" not in active_status.payload
    assert metrics.active_degraded is True
    assert metrics.recovery_authorization_depth == 0
    assert metrics.recovery_completed_count == 0


def test_active_degraded_scope_capacity_exhaustion_fails_closed() -> None:
    """验证 ACK 不会遗忘 active lineage，状态容量耗尽时也不淘汰旧 scope。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        依次激活两个不同 epoch 并拒绝第三个 active degraded scope。
    """
    queue = BoundedMarketEventQueue(capacity=1, control_scope_capacity=1)
    for sequence, epoch in ((40, "epoch-a"), (41, "epoch-b")):
        queue.put_nowait(_source_gap(sequence, session_epoch=epoch))
        _take_and_ack_controls(queue)

    with pytest.raises(MarketEventControlCapacityError) as exc_info:
        queue.put_nowait(_source_gap(42, session_epoch="epoch-c"))

    metrics = queue.metrics()
    assert exc_info.value.scope_key[3] == "epoch-c"
    assert exc_info.value.control_scope_capacity == 2
    assert metrics.active_degraded_scope_depth == 2
    assert metrics.active_degraded_scope_capacity == 2
    assert metrics.control_pending_depth == 0
    assert metrics.control_overflow_count == 1


def test_take_control_retries_same_delivery_until_exact_ack() -> None:
    """验证发送失败不 ACK 时重取同一不可变批次，错误和旧 ACK 均不释放。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        创建一个来源 gap delivery，模拟两次发送失败、错误 ACK 和最终精确 ACK。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_source_gap(20))

    first = queue.take_control()
    retry = queue.take_control()
    assert first is not None
    assert retry is first
    assert retry.delivery_id == first.delivery_id
    assert retry.events == first.events

    with pytest.raises(MarketEventControlAckError) as mismatch:
        queue.ack_control("control-delivery-wrong")
    assert mismatch.value.code == "CONTROL_DELIVERY_ID_MISMATCH"
    assert queue.take_control() is first

    queue.ack_control(first.delivery_id)
    assert queue.take_control() is None
    with pytest.raises(MarketEventControlAckError) as old_ack:
        queue.ack_control(first.delivery_id)
    assert old_ack.value.code == "NO_CONTROL_DELIVERY_IN_FLIGHT"

    metrics = queue.metrics()
    assert metrics.control_retry_count == 2
    assert metrics.control_delivery_count == 1
    assert metrics.control_ack_count == 1
    assert metrics.control_ack_error_count == 2
    assert metrics.control_outstanding_depth == 0


def test_delivery_id_nonce_blocks_cross_instance_aba_and_guessed_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证每队列实例 nonce 阻止相同 serial 的跨实例旧 ACK 和猜测 ID。

    Args:
        monkeypatch: pytest 提供的局部属性替换工具。

    Returns:
        None。

    Side Effects:
        临时替换本模块 secrets nonce 生成器，创建两个纯内存队列并精确 ACK。
    """
    nonces = iter(("incarnation-a", "incarnation-b"))
    monkeypatch.setattr(
        "bullet_trade.market_data.queue.secrets.token_hex",
        lambda _: next(nonces),
    )
    first_queue = BoundedMarketEventQueue(capacity=1)
    second_queue = BoundedMarketEventQueue(capacity=1)
    first_queue.put_nowait(_source_gap(50))
    second_queue.put_nowait(_source_gap(51))
    first_delivery = first_queue.take_control()
    second_delivery = second_queue.take_control()
    assert first_delivery is not None
    assert second_delivery is not None
    assert first_delivery.delivery_id.endswith("0000000000000001")
    assert second_delivery.delivery_id.endswith("0000000000000001")
    assert first_delivery.delivery_id != second_delivery.delivery_id

    with pytest.raises(MarketEventControlAckError) as cross_instance_ack:
        second_queue.ack_control(first_delivery.delivery_id)
    assert cross_instance_ack.value.code == "CONTROL_DELIVERY_ID_MISMATCH"
    with pytest.raises(MarketEventControlAckError) as guessed_ack:
        second_queue.ack_control("control-delivery-0000000000000001")
    assert guessed_ack.value.code == "CONTROL_DELIVERY_ID_MISMATCH"
    assert second_queue.take_control() is second_delivery

    first_queue.ack_control(first_delivery.delivery_id)
    second_queue.ack_control(second_delivery.delivery_id)


def test_legacy_control_drain_defaults_fail_closed_and_never_clears_inflight() -> None:
    """验证默认禁用 pending fire-and-forget，显式兼容也不能绕过 in-flight ACK。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        对默认与显式兼容队列分别触发受控 drain 异常并最终可靠 ACK。
    """
    default_queue = BoundedMarketEventQueue(capacity=2)
    retained = _transaction(1)
    default_queue.put_nowait(retained)
    default_queue.put_nowait(_source_gap(60))

    with pytest.raises(MarketEventControlDrainError) as control_drain_error:
        default_queue.drain_control()
    assert control_drain_error.value.code == "LEGACY_CONTROL_DRAIN_DISABLED"
    with pytest.raises(MarketEventControlDrainError) as combined_drain_error:
        default_queue.drain()
    assert combined_drain_error.value.code == "LEGACY_CONTROL_DRAIN_DISABLED"
    assert default_queue.metrics().data_depth == 1
    assert default_queue.metrics().control_pending_depth == 1
    _take_and_ack_controls(default_queue)
    assert default_queue.drain_data() == (retained,)

    legacy_queue = BoundedMarketEventQueue(
        capacity=1,
        allow_legacy_control_drain=True,
    )
    legacy_queue.put_nowait(_source_gap(61))
    delivery = legacy_queue.take_control()
    assert delivery is not None
    with pytest.raises(MarketEventControlDrainError) as inflight_control_error:
        legacy_queue.drain_control()
    assert inflight_control_error.value.code == "RELIABLE_CONTROL_DELIVERY_IN_FLIGHT"
    with pytest.raises(MarketEventControlDrainError) as inflight_combined_error:
        legacy_queue.drain()
    assert inflight_combined_error.value.code == "RELIABLE_CONTROL_DELIVERY_IN_FLIGHT"
    assert legacy_queue.take_control() is delivery
    legacy_queue.ack_control(delivery.delivery_id)

    legacy_queue.put_nowait(_source_gap(62))
    assert legacy_queue.drain_control() == (_source_gap(62),)


def test_new_controls_wait_in_next_window_while_delivery_is_inflight() -> None:
    """验证 in-flight 期间新控制进入下一窗口且不会改变待重试批次。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        依次创建并 ACK 两个同 scope 来源 gap delivery。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    first_gap = _source_gap(30)
    second_gap = _source_gap(31)
    queue.put_nowait(first_gap)
    first_delivery = queue.take_control()
    assert first_delivery is not None

    queue.put_nowait(second_gap)
    while_inflight = queue.metrics()
    assert while_inflight.control_inflight_depth == 1
    assert while_inflight.control_pending_depth == 1
    assert while_inflight.control_outstanding_depth == 2
    assert queue.take_control() is first_delivery
    assert first_delivery.events == (first_gap,)

    queue.ack_control(first_delivery.delivery_id)
    second_delivery = queue.take_control()
    assert second_delivery is not None
    assert second_delivery.delivery_id != first_delivery.delivery_id
    assert second_delivery.events == (second_gap,)
    queue.ack_control(second_delivery.delivery_id)
    assert queue.take_control() is None


def test_control_inflight_and_pending_windows_are_bounded_fail_closed() -> None:
    """验证单一 in-flight 加单一 pending 窗口有界，新 pending scope 超限立即失败。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        建立一个 in-flight scope、一个下一窗口 scope，并拒绝第三个来源 scope。
    """
    queue = BoundedMarketEventQueue(capacity=1, control_scope_capacity=1)
    queue.put_nowait(_source_gap(40, session_epoch="epoch-a"))
    first_delivery = queue.take_control()
    assert first_delivery is not None
    queue.put_nowait(_source_gap(41, session_epoch="epoch-b"))

    with pytest.raises(MarketEventControlCapacityError) as exc_info:
        queue.put_nowait(_source_gap(42, session_epoch="epoch-c"))

    metrics = queue.metrics()
    assert exc_info.value.scope_key[3] == "epoch-c"
    assert metrics.control_inflight_scope_depth == 1
    assert metrics.control_pending_scope_depth == 1
    assert metrics.control_outstanding_scope_depth == 2
    assert metrics.control_outstanding_scope_capacity == 2
    assert metrics.control_overflow_count == 1

    queue.ack_control(first_delivery.delivery_id)
    second_delivery = queue.take_control()
    assert second_delivery is not None
    assert second_delivery.events[0].session_epoch == "epoch-b"


def test_degraded_scope_rejects_automatic_continuity_recovery_status() -> None:
    """验证同 scope 后续 continuous=true 只形成被阻断的 degraded 状态。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        可靠投递一次 degraded 状态后，再投递来源声称恢复的状态并检查不可变副本。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_source_status(50, continuous=False, state="degraded"))
    degraded_delivery = queue.take_control()
    assert degraded_delivery is not None
    queue.ack_control(degraded_delivery.delivery_id)

    claimed_recovery = _source_status(51, continuous=True, state="connected")
    queue.put_nowait(claimed_recovery)
    blocked_delivery = queue.take_control()
    assert blocked_delivery is not None
    blocked_status = blocked_delivery.events[0]

    assert blocked_status is not claimed_recovery
    assert blocked_status.provider == claimed_recovery.provider
    assert blocked_status.session_epoch == claimed_recovery.session_epoch
    assert blocked_status.stream_id == claimed_recovery.stream_id
    assert blocked_status.channel_id == claimed_recovery.channel_id
    assert blocked_status.payload["continuous"] is False
    assert blocked_status.payload["state"] == "degraded"
    assert blocked_status.payload["recovery_blocked"] is True
    assert blocked_status.completeness is False


def test_source_status_and_queue_overflow_share_scope_without_losing_evidence() -> None:
    """验证普通 overflow 不吞来源控制状态，两类证据在同 scope 有界合并。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        在满数据区制造逐笔 overflow，再经 public put_nowait 投递同 scope 来源状态。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_transaction(1))
    queue.put_nowait(_transaction(2))
    source_status = _source_status(
        60,
        continuous=False,
        state="degraded",
        provider="mock",
        session_epoch="epoch-1",
        stream_id="transaction-stream",
        channel_id="channel-1",
    )
    assert queue.put_nowait(source_status).outcome is QueuePutOutcome.CONTROL_ENQUEUED

    delivery = queue.take_control()
    assert delivery is not None
    gap, status = delivery.events
    assert isinstance(gap, SequenceGapEvent)
    assert isinstance(status, ConnectionStateEvent)
    assert status.provider == "mock"
    assert status.session_epoch == "epoch-1"
    assert status.stream_id == "transaction-stream"
    assert status.channel_id == "channel-1"
    assert status.payload["status_sequence"] == 60
    assert status.payload["queue_overflow_boundary"]["reason"] == "queue_overflow"
    assert status.payload["queue_overflow_boundary"]["first_lost"]["queue_loss_index"] == 1
    assert queue.metrics().overflow_count == 1


def test_source_and_queue_loss_merge_clears_conflicting_security_identity() -> None:
    """验证 source=A 与同 scope queue loss=B 合并后 gap/status 顶层不误指向 A。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        填满本地数据区，合并跨证券来源控制与 queue overflow，并可靠 ACK delivery。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_snapshot(10.1))
    queue.put_nowait(_source_gap(70, security="600000.XSHG"))
    queue.put_nowait(
        _source_status(
            71,
            continuous=False,
            state="degraded",
            security="600000.XSHG",
        )
    )
    lost = _transaction(
        72,
        provider="source",
        session_epoch="source-epoch-1",
        stream_id="source-stream",
        channel_id="source-channel",
        security="600001.XSHG",
    )
    assert queue.put_nowait(lost).outcome is QueuePutOutcome.OVERFLOW

    gap, status = _take_and_ack_controls(queue)

    for control in (gap, status):
        assert control.security is None
        assert control.raw_security_code is None
        assert control.payload["multiple_securities"] is True
        assert control.payload["queue_overflow_boundary"]["multiple_securities"] is False
        assert control.payload["queue_overflow_boundary"]["first_lost"]["security"] == (
            "600001.XSHG"
        )


def test_recovery_authorization_waits_for_loss_boundary_exact_ack() -> None:
    """验证 pending/in-flight queue loss 未精确 ACK 时恢复授权始终 fail closed。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        制造同 scope queue loss，分别在 pending、in-flight 和 ACK 后申请脱敏恢复授权。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_snapshot(10.1))
    lost = _transaction(
        80,
        provider="source",
        session_epoch="source-epoch-1",
        stream_id="source-stream",
        channel_id="source-channel",
    )
    assert queue.put_nowait(lost).outcome is QueuePutOutcome.OVERFLOW

    with pytest.raises(MarketEventRecoveryAuthorizationError) as pending_error:
        _authorize_source_recovery(queue, "loss-recovery-1")
    assert pending_error.value.code == "RECOVERY_LOSS_BOUNDARY_UNACKED"

    loss_delivery = queue.take_control()
    assert loss_delivery is not None
    with pytest.raises(MarketEventRecoveryAuthorizationError) as inflight_error:
        _authorize_source_recovery(queue, "loss-recovery-1")
    assert inflight_error.value.code == "RECOVERY_LOSS_BOUNDARY_UNACKED"
    queue.ack_control(loss_delivery.delivery_id)

    _authorize_source_recovery(queue, "loss-recovery-1")
    queue.put_nowait(
        _source_status(
            81,
            continuous=True,
            state="ready",
            recovery_confirmed=True,
            recovery_id="loss-recovery-1",
        )
    )
    (recovered_status,) = _take_and_ack_controls(queue)
    metrics = queue.metrics()

    assert recovered_status.payload["state"] == "ready"
    assert recovered_status.payload["continuous"] is True
    assert recovered_status.payload["recovery_authorized"] is True
    assert metrics.active_degraded is False
    assert metrics.recovery_completed_count == 1


def test_later_gap_invalidates_earlier_recovery_in_same_pending_window() -> None:
    """验证同窗口 exact recovery 后到新 gap 时 delivery 末状态不会假恢复。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        ACK 旧 gap 后授权恢复，在同一 pending 窗口依次写入恢复状态和新 gap。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_source_gap(90))
    _take_and_ack_controls(queue)
    _authorize_source_recovery(queue, "recovery-before-new-gap")

    queue.put_nowait(
        _source_status(
            91,
            continuous=True,
            state="ready",
            recovery_confirmed=True,
            recovery_id="recovery-before-new-gap",
        )
    )
    assert queue.metrics().active_degraded is False
    queue.put_nowait(_source_gap(92))
    assert queue.metrics().active_degraded is True

    delivery = queue.take_control()
    assert delivery is not None
    gap, final_status = delivery.events

    assert isinstance(gap, SequenceGapEvent)
    assert isinstance(final_status, ConnectionStateEvent)
    assert final_status.payload["state"] == "degraded"
    assert final_status.payload["continuous"] is False
    assert final_status.payload["recovery_invalidated"] is True
    assert final_status.payload["reason"] == "loss_boundary_after_recovery"
    assert "recovery_authorized" not in final_status.payload
    assert queue.metrics().active_degraded is True
    queue.ack_control(delivery.delivery_id)


def test_queue_controls_keep_identity_shape_accepted_by_feed_gate() -> None:
    """验证队列生成的 gap/degraded 保留当前 Feed 可接收的完整身份。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        连接无 SDK Mock Feed，将本地队列控制事件投递到内存 callback。
    """
    capability_id = "realtime.stream.transaction"
    manifest = CapabilityManifest(
        provider="mock",
        manifest_version="queue-control-v1",
        location=ProviderLocation.LOCAL,
        capabilities={
            capability_id: CapabilityDeclaration(
                capability_id=capability_id,
                semantic_class=capability_id,
                support=CapabilitySupport.SUPPORTED,
                readiness=CapabilityReadiness.UNAVAILABLE,
                markets=("XSHG",),
                asset_types=("stock",),
                continuous=True,
            )
        },
    )
    feed = MockRealtimeMarketDataFeed(
        manifest,
        negotiated_event_types=(MarketEventType.TRANSACTION,),
    )
    feed.connect()
    session_epoch = feed.health().session_epoch
    assert session_epoch is not None

    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_transaction(1, session_epoch=session_epoch))
    queue.put_nowait(
        _transaction(
            2,
            session_epoch=session_epoch,
            stream_id="feed-stream",
            channel_id="feed-channel",
        )
    )
    controls = _take_and_ack_controls(queue)
    delivered: List[MarketEvent] = []
    feed.set_market_event_callback(delivered.append)

    for control in controls:
        assert control.session_epoch == session_epoch
        assert control.capability_key == capability_id
        assert control.stream_id == "feed-stream"
        assert control.channel_id == "feed-channel"
        assert feed.publish_event(control) is True

    assert tuple(event.event_type for event in delivered) == (
        MarketEventType.STREAM_GAP,
        MarketEventType.STREAM_STATUS,
    )
    assert tuple(
        (
            event.provider,
            event.capability_key,
            event.level,
            event.session_epoch,
            event.stream_id,
            event.channel_id,
        )
        for event in delivered
    ) == tuple(
        (
            event.provider,
            event.capability_key,
            event.level,
            event.session_epoch,
            event.stream_id,
            event.channel_id,
        )
        for event in controls
    )


def test_drain_does_not_fabricate_continuity_recovery() -> None:
    """验证腾空数据和控制事件后 degraded 仍单调保持，新数据不会伪装补齐缺口。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        产生一次溢出、完整 drain 后再次投递本地事件。
    """
    queue = BoundedMarketEventQueue(capacity=1, allow_legacy_control_drain=True)
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
    with pytest.raises(ValueError, match="control_scope_capacity"):
        BoundedMarketEventQueue(capacity=1, control_scope_capacity=0)
    with pytest.raises(ValueError, match="control_scope_capacity"):
        BoundedMarketEventQueue(capacity=1, control_scope_capacity=True)

    queue = BoundedMarketEventQueue(capacity=1)
    with pytest.raises(ValueError, match="max_items"):
        queue.drain_data(-1)
    with pytest.raises(ValueError, match="max_items"):
        queue.drain(max_data_items=True)
