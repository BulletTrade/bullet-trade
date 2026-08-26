"""
作者: BruceLee

文件职责: 验证可靠行情控制 dispatcher 的精确 ACK、失败重试、有界诊断和 Feed 接线。
主要输入: 脱敏合成逐笔、queue overflow 控制 delivery 与纯内存 sink。
主要输出: dispatcher 终态/指标、稳定 delivery ID、Feed gap/degraded health 断言。
上游关系: 覆盖 bullet_trade.market_data.control_delivery 的独立同步接口。
下游关系: 为 native drain、EventBus 和远程 market writer 的生产接线提供离线门禁。
关键配置约定: 测试不联网、不加载 SDK、不启动后台线程且不执行任何交易动作；普通数据
只在 dispatcher 连续返回 NO_CONTROL 后 drain。
"""

from datetime import datetime, timedelta
from threading import Event, Thread
from typing import Any, List, Optional, Sequence

import pytest

from bullet_trade.market_data.capability import (
    CapabilityDeclaration,
    CapabilityManifest,
    CapabilityReadiness,
    CapabilitySupport,
    ProviderLocation,
)
from bullet_trade.market_data.control_delivery import (
    MarketControlDispatchOutcome,
    MarketControlDispatchResult,
    MarketControlSinkAck,
    ReliableMarketControlDispatcher,
)
from bullet_trade.market_data.feed import MockRealtimeMarketDataFeed
from bullet_trade.market_data.models import (
    MarketDataLevel,
    MarketEvent,
    MarketEventType,
    MarketSubscriptionSpec,
    SequenceGapEvent,
    SourceSequence,
    SubscriptionSelector,
    TransactionEvent,
)
from bullet_trade.market_data.queue import (
    BoundedMarketEventQueue,
    MarketEventControlDelivery,
    QueuePutOutcome,
)

pytestmark = pytest.mark.unit

_BASE_TIME = datetime(2026, 8, 14, 9, 30, 0)


def _transaction(
    sequence: int,
    *,
    session_epoch: str = "epoch-1",
    security: str = "600000.XSHG",
) -> TransactionEvent:
    """构造带精确 mock Provider/通道 lineage 的脱敏逐笔成交。

    Args:
        sequence: 写入 MainSeq 和接收时间的稳定序号。
        session_epoch: 事件所属 Feed/队列会话 epoch。
        security: 标准证券代码。

    Returns:
        TransactionEvent: 可用于 Feed 发布或 queue overflow 的不可变 L2 事件。

    Side Effects:
        无。
    """
    return TransactionEvent(
        provider="mock",
        capability_key="realtime.stream.transaction",
        event_type=MarketEventType.TRANSACTION,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch=session_epoch,
        security=security,
        raw_security_code=security.split(".", 1)[0],
        stream_id="transaction-stream",
        channel_id="channel-1",
        source_sequence=SourceSequence(components={"MainSeq": sequence, "SubSeq": 1}),
        payload={"price": 10.0, "sequence": sequence},
        gateway_received_at=_BASE_TIME + timedelta(microseconds=sequence),
    )


def _source_gap(sequence: int, *, session_epoch: str = "epoch-1") -> SequenceGapEvent:
    """构造可直接进入队外控制窗口的脱敏来源 gap。

    Args:
        sequence: 写入 MainSeq 和 loss boundary ID 的稳定序号。
        session_epoch: gap 所属会话 epoch。

    Returns:
        SequenceGapEvent: 带完整 scope 和显式 boundary ID 的控制事件。

    Side Effects:
        无。
    """
    return SequenceGapEvent(
        provider="mock",
        capability_key="realtime.stream.transaction",
        event_type=MarketEventType.STREAM_GAP,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch=session_epoch,
        security="600000.XSHG",
        raw_security_code="600000",
        stream_id="transaction-stream",
        channel_id="channel-1",
        source_sequence=SourceSequence(components={"MainSeq": sequence, "SubSeq": 1}),
        payload={
            "state": "degraded",
            "continuous": False,
            "reason": "source_gap",
            "loss_boundary_id": f"source-gap-{sequence}",
        },
        gateway_received_at=_BASE_TIME + timedelta(microseconds=sequence),
        completeness=False,
    )


def _feed() -> MockRealtimeMarketDataFeed:
    """创建只声明逐笔成交能力且无网络的 Mock Feed。

    Args:
        无。

    Returns:
        MockRealtimeMarketDataFeed: 尚未连接、可订阅 transaction 的本地 Feed。

    Side Effects:
        仅创建内存对象；不 connect、不启动线程。
    """
    capability_id = "realtime.stream.transaction"
    manifest = CapabilityManifest(
        provider="mock",
        manifest_version="control-dispatch-v1",
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
    return MockRealtimeMarketDataFeed(
        manifest,
        negotiated_event_types=(MarketEventType.TRANSACTION,),
    )


class _ScriptedSink:
    """按预设动作返回异常、非法 ACK、未确认、错 ID 或精确 ACK。"""

    def __init__(self, actions: Sequence[str]) -> None:
        """保存固定动作序列和实际收到的 delivery ID。

        Args:
            actions: 每次 publish_control 依次执行的动作名称。

        Returns:
            None。

        Side Effects:
            创建有界测试列表；不访问队列。
        """
        self._actions = tuple(actions)
        self.delivery_ids: List[str] = []

    def publish_control(self, delivery: MarketEventControlDelivery) -> Any:
        """执行当前脚本动作并返回对应 ACK 或异常。

        Args:
            delivery: dispatcher 当前稳定控制批次。

        Returns:
            Any: ACK 动作返回 MarketControlSinkAck，invalid 动作返回错误类型。

        Raises:
            RuntimeError: 当前动作是 exception 时抛出。
            AssertionError: 调用次数超过预设脚本时抛出。

        Side Effects:
            追加一个 delivery ID 到测试观测列表。
        """
        attempt_index = len(self.delivery_ids)
        assert attempt_index < len(self._actions)
        action = self._actions[attempt_index]
        self.delivery_ids.append(delivery.delivery_id)
        if action == "exception":
            raise RuntimeError("synthetic sink failure")
        if action == "invalid":
            return object()
        if action == "unconfirmed":
            return MarketControlSinkAck(
                delivery_id=delivery.delivery_id,
                acknowledged=False,
                detail="temporary sink failure",
            )
        if action == "mismatch":
            return MarketControlSinkAck(
                delivery_id="control-delivery-wrong-incarnation-0000000000000001",
                acknowledged=True,
            )
        if action == "ack":
            return MarketControlSinkAck(
                delivery_id=delivery.delivery_id,
                acknowledged=True,
            )
        raise AssertionError(f"未知 sink 测试动作: {action}")


class _FeedRetrySink:
    """把每次 delivery 发布到 Mock Feed，首次模拟 ACK 丢失、随后确认。"""

    def __init__(self, feed: MockRealtimeMarketDataFeed) -> None:
        """绑定本地 Mock Feed 并初始化尝试记录。

        Args:
            feed: 已连接且已确认 transaction 订阅的 Mock Feed。

        Returns:
            None。

        Side Effects:
            仅保存 Feed 引用和空 ID 列表。
        """
        self._feed = feed
        self.delivery_ids: List[str] = []

    def publish_control(
        self,
        delivery: MarketEventControlDelivery,
    ) -> MarketControlSinkAck:
        """同步发布全部控制事件，并从第二次开始返回 exact ACK。

        Args:
            delivery: dispatcher 当前稳定控制批次。

        Returns:
            MarketControlSinkAck: 第一次 acknowledged=False，重试时为 True。

        Raises:
            Exception: Feed 拒绝任何事件时原样传播，dispatcher 将保留 delivery。

        Side Effects:
            调用 Mock Feed publish_event，并记录当前 delivery ID。
        """
        self.delivery_ids.append(delivery.delivery_id)
        for event in delivery.events:
            if not self._feed.publish_event(event):
                raise RuntimeError("mock feed 未接受控制事件")
        acknowledged = len(self.delivery_ids) >= 2
        return MarketControlSinkAck(
            delivery_id=delivery.delivery_id,
            acknowledged=acknowledged,
            detail=None if acknowledged else "synthetic ack loss",
        )


class _ReentrantSink:
    """在首次 sink 回调中同步重入同一 dispatcher 的测试 sink。"""

    def __init__(self) -> None:
        """初始化尚未绑定的 dispatcher 和有界调用记录。

        Args:
            无。

        Returns:
            None。

        Side Effects:
            创建空测试状态；不访问队列。
        """
        self.dispatcher: Optional[ReliableMarketControlDispatcher] = None
        self.delivery_ids: List[str] = []
        self.reentrant_results: List[MarketControlDispatchResult] = []

    def publish_control(
        self,
        delivery: MarketEventControlDelivery,
    ) -> MarketControlSinkAck:
        """记录 delivery，并同步重入 dispatcher 后确认外层 delivery。

        Args:
            delivery: 外层 dispatcher 正在投递的稳定控制批次。

        Returns:
            MarketControlSinkAck: 对外层 delivery 的精确确认。

        Raises:
            RuntimeError: 测试未先绑定 dispatcher 时抛出。

        Side Effects:
            同线程调用一次 dispatch_control_once 并保存其受控结果。
        """
        if self.dispatcher is None:
            raise RuntimeError("reentrant sink 尚未绑定 dispatcher")
        self.delivery_ids.append(delivery.delivery_id)
        self.reentrant_results.append(self.dispatcher.dispatch_control_once())
        return MarketControlSinkAck(
            delivery_id=delivery.delivery_id,
            acknowledged=True,
        )


class _BlockingAckSink:
    """用 Event 暂停外层调用以确定性触发跨线程并发的测试 sink。"""

    def __init__(self) -> None:
        """创建进入与释放 Event 以及有界调用记录。

        Args:
            无。

        Returns:
            None。

        Side Effects:
            仅创建进程内同步原语；不启动线程。
        """
        self.entered = Event()
        self.release = Event()
        self.delivery_ids: List[str] = []

    def publish_control(
        self,
        delivery: MarketEventControlDelivery,
    ) -> MarketControlSinkAck:
        """通知测试线程已进入 sink，等待显式释放后返回精确 ACK。

        Args:
            delivery: 外层 dispatcher 正在投递的稳定控制批次。

        Returns:
            MarketControlSinkAck: 对当前 delivery 的精确确认。

        Raises:
            RuntimeError: 测试线程两秒内没有显式释放 sink 时抛出。

        Side Effects:
            设置 entered Event，并阻塞等待 release Event；不使用 sleep。
        """
        self.delivery_ids.append(delivery.delivery_id)
        self.entered.set()
        if not self.release.wait(timeout=2.0):
            raise RuntimeError("测试未及时释放 blocking sink")
        return MarketControlSinkAck(
            delivery_id=delivery.delivery_id,
            acknowledged=True,
        )


def _dispatch_in_worker(
    dispatcher: ReliableMarketControlDispatcher,
    results: List[MarketControlDispatchResult],
) -> None:
    """在线程中执行一次 dispatcher 并保存结果。

    Args:
        dispatcher: 待调用的可靠控制 dispatcher。
        results: 保存唯一返回结果的测试列表。

    Returns:
        None。

    Side Effects:
        调用一次 dispatch_control_once，并向 results 追加一个结果。
    """
    results.append(dispatcher.dispatch_control_once())


def test_dispatcher_retries_same_delivery_for_every_non_exact_ack() -> None:
    """验证异常、非法返回、未确认和错 ID 都不释放当前 delivery。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        对一个来源 gap 执行五次纯内存 sink 尝试并在最后精确 ACK。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_source_gap(1))
    sink = _ScriptedSink(("exception", "invalid", "unconfirmed", "mismatch", "ack"))
    dispatcher = ReliableMarketControlDispatcher(
        queue,
        sink,
        max_diagnostic_message_length=8,
    )

    outcomes = tuple(dispatcher.dispatch_control_once() for _ in range(5))
    empty = dispatcher.dispatch_control_once()
    metrics = dispatcher.metrics()

    assert tuple(result.outcome for result in outcomes) == (
        MarketControlDispatchOutcome.SINK_EXCEPTION,
        MarketControlDispatchOutcome.SINK_PROTOCOL_ERROR,
        MarketControlDispatchOutcome.SINK_NOT_ACKNOWLEDGED,
        MarketControlDispatchOutcome.SINK_ACK_MISMATCH,
        MarketControlDispatchOutcome.ACKED,
    )
    assert outcomes[0].retry is False
    assert all(result.retry is True for result in outcomes[1:])
    assert len(set(sink.delivery_ids)) == 1
    assert all(result.delivery_id == sink.delivery_ids[0] for result in outcomes)
    assert all(result.retained_for_retry for result in outcomes[:-1])
    assert outcomes[-1].queue_acknowledged is True
    assert empty.outcome is MarketControlDispatchOutcome.NO_CONTROL
    assert metrics.dispatch_call_count == 6
    assert metrics.no_control_count == 1
    assert metrics.delivery_attempt_count == 5
    assert metrics.delivery_retry_count == 4
    assert metrics.acknowledged_count == 1
    assert metrics.sink_exception_count == 1
    assert metrics.sink_protocol_error_count == 1
    assert metrics.sink_not_acknowledged_count == 1
    assert metrics.sink_ack_mismatch_count == 1
    assert metrics.inflight_delivery_id is None
    assert metrics.last_outcome is MarketControlDispatchOutcome.NO_CONTROL


def test_same_thread_sink_reentry_returns_dispatch_in_progress() -> None:
    """验证 sink 同线程重入不会递归调用 sink 或重复 ACK。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        通过重入 sink 投递并 ACK 一个来源 gap。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_source_gap(2))
    sink = _ReentrantSink()
    dispatcher = ReliableMarketControlDispatcher(queue, sink)
    sink.dispatcher = dispatcher

    outer = dispatcher.dispatch_control_once()
    metrics = dispatcher.metrics()

    assert outer.outcome is MarketControlDispatchOutcome.ACKED
    assert len(sink.delivery_ids) == 1
    assert len(sink.reentrant_results) == 1
    inner = sink.reentrant_results[0]
    assert inner.outcome is MarketControlDispatchOutcome.DISPATCH_IN_PROGRESS
    assert inner.delivery_id == outer.delivery_id
    assert inner.event_count == outer.event_count
    assert inner.queue_acknowledged is False
    assert inner.retained_for_retry is True
    assert metrics.dispatch_call_count == 2
    assert metrics.dispatch_in_progress_count == 1
    assert metrics.delivery_attempt_count == 1
    assert metrics.acknowledged_count == 1
    assert queue.metrics().control_outstanding_depth == 0


def test_cross_thread_dispatch_returns_in_progress_without_waiting_for_sink() -> None:
    """验证 sink 阻塞时并发调用立即受控返回且不形成互等死锁。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        启动一个本地线程并用 Event 确定性协调一次并发控制投递。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_source_gap(3))
    sink = _BlockingAckSink()
    dispatcher = ReliableMarketControlDispatcher(queue, sink)
    worker_results: List[MarketControlDispatchResult] = []
    worker = Thread(
        target=_dispatch_in_worker,
        args=(dispatcher, worker_results),
        daemon=True,
    )
    worker.start()
    try:
        assert sink.entered.wait(timeout=2.0)
        concurrent = dispatcher.dispatch_control_once()
        active_metrics = dispatcher.metrics()
    finally:
        sink.release.set()
        worker.join(timeout=2.0)

    assert not worker.is_alive()
    assert len(worker_results) == 1
    outer = worker_results[0]
    assert outer.outcome is MarketControlDispatchOutcome.ACKED
    assert concurrent.outcome is MarketControlDispatchOutcome.DISPATCH_IN_PROGRESS
    assert concurrent.delivery_id == outer.delivery_id
    assert concurrent.event_count == outer.event_count
    assert concurrent.queue_acknowledged is False
    assert concurrent.retained_for_retry is True
    assert active_metrics.dispatch_call_count == 2
    assert active_metrics.dispatch_in_progress_count == 1
    assert active_metrics.delivery_attempt_count == 1
    assert active_metrics.acknowledged_count == 0
    final_metrics = dispatcher.metrics()
    assert final_metrics.acknowledged_count == 1
    assert final_metrics.inflight_delivery_id is None
    assert sink.delivery_ids == [outer.delivery_id]
    assert queue.metrics().control_outstanding_depth == 0


def test_old_delivery_ack_does_not_clear_new_pending_window() -> None:
    """验证 in-flight 期间新控制进入下一窗口，旧 exact ACK 只释放旧批次。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        首次 sink 未确认后写入新 gap，再依次 ACK 两个 delivery。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    queue.put_nowait(_source_gap(10))
    sink = _ScriptedSink(("unconfirmed", "ack", "ack"))
    dispatcher = ReliableMarketControlDispatcher(queue, sink)

    first_attempt = dispatcher.dispatch_control_once()
    queue.put_nowait(_source_gap(11))
    old_ack = dispatcher.dispatch_control_once()
    after_old_ack = queue.metrics()
    new_ack = dispatcher.dispatch_control_once()

    assert first_attempt.delivery_id == old_ack.delivery_id
    assert old_ack.outcome is MarketControlDispatchOutcome.ACKED
    assert after_old_ack.control_inflight_depth == 0
    assert after_old_ack.control_pending_depth == 1
    assert new_ack.outcome is MarketControlDispatchOutcome.ACKED
    assert new_ack.delivery_id != old_ack.delivery_id
    assert dispatcher.dispatch_control_once().outcome is MarketControlDispatchOutcome.NO_CONTROL


def test_overflow_control_retries_through_mock_feed_before_data_drain() -> None:
    """验证 overflow 控制经真实 Mock Feed 重试、幂等 gap 计数并先于普通数据 drain。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        连接纯内存 Feed、订阅逐笔、制造 queue overflow 并模拟一次 ACK 丢失。
    """
    feed = _feed()
    feed.connect()
    session_epoch = feed.health().session_epoch
    assert session_epoch is not None
    feed.subscribe(
        MarketSubscriptionSpec(
            request_id="control-dispatch-transaction",
            selector=SubscriptionSelector.SYMBOLS,
            symbols=("600000.XSHG",),
            level=MarketDataLevel.L2,
            event_types=(MarketEventType.TRANSACTION,),
        )
    )
    retained = _transaction(20, session_epoch=session_epoch)
    lost = _transaction(21, session_epoch=session_epoch)
    assert feed.publish_event(retained) is True
    assert (
        feed.health().capability_readiness["realtime.stream.transaction"]
        is CapabilityReadiness.READY
    )

    queue = BoundedMarketEventQueue(capacity=1)
    assert queue.put_nowait(retained).outcome is QueuePutOutcome.ENQUEUED
    assert queue.put_nowait(lost).outcome is QueuePutOutcome.OVERFLOW
    observed: List[MarketEvent] = []
    feed.set_market_event_callback(observed.append)
    sink = _FeedRetrySink(feed)
    dispatcher = ReliableMarketControlDispatcher(queue, sink)

    first = dispatcher.dispatch_control_once()
    after_first = feed.health()
    assert first.outcome is MarketControlDispatchOutcome.SINK_NOT_ACKNOWLEDGED
    assert first.retained_for_retry is True
    assert queue.metrics().data_depth == 1
    assert queue.metrics().control_delivery_inflight is True
    assert after_first.gap_count == 1

    second = dispatcher.dispatch_control_once()
    empty = dispatcher.dispatch_control_once()
    after_ack = feed.health()
    assert second.outcome is MarketControlDispatchOutcome.ACKED
    assert second.retry is True
    assert second.delivery_id == first.delivery_id
    assert sink.delivery_ids == [first.delivery_id, first.delivery_id]
    assert empty.outcome is MarketControlDispatchOutcome.NO_CONTROL
    assert after_ack.gap_count == 1
    assert (
        after_ack.capability_readiness["realtime.stream.transaction"]
        is CapabilityReadiness.DEGRADED
    )
    assert "market_stream_continuity_degraded" in after_ack.reasons
    assert tuple(event.event_type for event in observed) == (
        MarketEventType.STREAM_GAP,
        MarketEventType.STREAM_STATUS,
        MarketEventType.STREAM_GAP,
        MarketEventType.STREAM_STATUS,
    )
    assert all(event.provider == "mock" for event in observed)
    assert all(event.session_epoch == session_epoch for event in observed)
    assert all(event.stream_id == "transaction-stream" for event in observed)
    assert all(event.channel_id == "channel-1" for event in observed)
    assert all(event.security == "600000.XSHG" for event in observed)
    assert queue.metrics().control_outstanding_depth == 0
    assert queue.drain_data() == (retained,)


def test_constructor_and_sink_ack_validation_fail_closed() -> None:
    """验证非法 dispatcher 配置和无界/模糊 sink ACK 被同步拒绝。

    Args:
        无。

    Returns:
        None。

    Side Effects:
        仅构造本地对象并触发参数校验异常。
    """
    queue = BoundedMarketEventQueue(capacity=1)
    sink = _ScriptedSink(("ack",))

    with pytest.raises(TypeError, match="queue"):
        ReliableMarketControlDispatcher(object(), sink)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="sink"):
        ReliableMarketControlDispatcher(queue, object())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="max_diagnostic_message_length"):
        ReliableMarketControlDispatcher(queue, sink, max_diagnostic_message_length=0)
    with pytest.raises(ValueError, match="delivery_id"):
        MarketControlSinkAck(delivery_id="", acknowledged=True)
    with pytest.raises(ValueError, match="acknowledged"):
        MarketControlSinkAck(delivery_id="delivery", acknowledged=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="detail"):
        MarketControlSinkAck(
            delivery_id="delivery",
            acknowledged=False,
            detail="x" * 513,
        )
