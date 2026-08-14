"""
作者: BruceLee

文件职责: 离线验证实时行情 health、单调时钟 freshness、暂停窗口与队列降级合同。
主要输入: 合成 manifest、显式更新 policy、可控单调时钟、订阅和市场事件。
主要输出: stale/unavailable 具名错误、分层 readiness、时间证据和队列指标断言。
上游关系: 覆盖 market_data.health、MockRealtimeMarketDataFeed 与有界事件队列接线。
下游关系: 为策略门禁、远程 health 和 Huaxin adapter 后续接入提供回归基线。
关键配置约定: 测试不读取本机交易日历、不联网；午休/闭市/停牌只由注入 policy 表达。
"""

from dataclasses import replace
from datetime import datetime, timedelta
from threading import Event, Thread
from typing import List, Optional

import pytest

import bullet_trade.market_data as market_data
from bullet_trade.market_data.capability import (
    CapabilityDeclaration,
    CapabilityManifest,
    CapabilityReadiness,
    CapabilityRequest,
    CapabilitySupport,
    DataCapabilityNotReadyError,
    DataSourceRouter,
    ProviderLocation,
    RouteRule,
)
from bullet_trade.market_data.feed import (
    MockRealtimeMarketDataFeed,
    RealtimeDataUnavailableError,
    StaleMarketDataError,
)
from bullet_trade.market_data.health import (
    MarketFreshnessError,
    MarketFreshnessTracker,
    MarketUpdateExpectation,
)
from bullet_trade.market_data.models import (
    CompatibilityTickEvent,
    ConnectionStateEvent,
    DepthSnapshotEvent,
    MarketDataLevel,
    MarketEvent,
    MarketEventType,
    MarketStatusEvent,
    MarketSubscriptionSpec,
    QuoteSnapshotEvent,
    SequenceGapEvent,
    SourceSequence,
    SubscriptionSelector,
    TransactionEvent,
)
from bullet_trade.market_data.queue import BoundedMarketEventQueue, MarketEventControlCapacityError

pytestmark = pytest.mark.unit


def test_health_contract_is_exported_from_market_data_package() -> None:
    """
    验证时效、健康与 stale 合同可从稳定包级入口导入。

    Returns:
        None: 全部公开类型都映射到实际实现时返回。
    """
    assert market_data.FeedEventTimes is not None
    assert market_data.MarketFreshnessTracker is MarketFreshnessTracker
    assert market_data.MarketUpdateExpectation is MarketUpdateExpectation
    assert market_data.StaleMarketDataError is StaleMarketDataError
    assert market_data.CurrentTickDiagnostic is not None
    assert market_data.MarketSnapshotDiagnostic is not None
    assert market_data.MarketFreshnessSnapshot is not None


class _FakeMonotonicClock:
    """提供不依赖系统时间、可由测试显式推进的单调时钟。"""

    def __init__(self) -> None:
        """初始化从零开始的测试时钟。"""
        self.value = 0.0

    def __call__(self) -> float:
        """
        返回当前测试时钟值。

        Returns:
            float: 当前单调秒数。
        """
        return self.value

    def advance(self, seconds: float) -> None:
        """
        将测试时钟向前推进指定秒数。

        Args:
            seconds: 非负推进秒数。

        Returns:
            None: 时钟更新后返回。
        """
        if seconds < 0:
            raise ValueError("seconds 不能为负数")
        self.value += seconds


class _MutableUpdatePolicy:
    """让测试显式切换连续交易、午休和累计暂停秒数。"""

    def __init__(self) -> None:
        """初始化为连续交易且无暂停时间。"""
        self.expected = True
        self.market_state = "continuous_trading"
        self.paused_seconds = 0.0
        self.effective_source_age_seconds: Optional[float] = 0.0

    def __call__(
        self,
        security: str,
        level: MarketDataLevel,
        event: MarketEvent,
        raw_age_seconds: float,
    ) -> MarketUpdateExpectation:
        """
        返回测试当前显式设置的市场更新窗口。

        Args:
            security: 当前证券代码。
            level: 当前行情级别。
            event: 最近一次受控 gateway 事件。
            raw_age_seconds: 当前单调原始 age。

        Returns:
            MarketUpdateExpectation: 当前 expected/state/paused/source age 组合。
        """
        del security, level, event, raw_age_seconds
        return MarketUpdateExpectation(
            expected=self.expected,
            market_state=self.market_state,
            paused_seconds=self.paused_seconds,
            effective_source_age_seconds=self.effective_source_age_seconds,
        )


def _manifest() -> CapabilityManifest:
    """
    构造覆盖 tick、L1、L2 和逐笔成交的离线能力清单。

    Returns:
        CapabilityManifest: 初始 readiness 为 unavailable 的 Mock manifest。
    """
    capabilities = {}
    for capability_id in (
        "realtime.stream.tick_compat",
        "realtime.snapshot.l1",
        "realtime.snapshot.l2",
        "realtime.stream.transaction",
    ):
        capabilities[capability_id] = CapabilityDeclaration(
            capability_id=capability_id,
            semantic_class=capability_id,
            support=CapabilitySupport.SUPPORTED,
            readiness=CapabilityReadiness.UNAVAILABLE,
            markets=("XSHG",),
            asset_types=("stock",),
            continuous=capability_id.endswith("transaction"),
            metadata={"full_market": True},
        )
    return CapabilityManifest(
        provider="mock",
        manifest_version="health-v1",
        location=ProviderLocation.LOCAL,
        capabilities=capabilities,
    )


def _feed(
    clock: _FakeMonotonicClock,
    policy: Optional[_MutableUpdatePolicy],
    event_queue: Optional[BoundedMarketEventQueue] = None,
) -> MockRealtimeMarketDataFeed:
    """
    创建使用显式 policy、测试时钟和可选队列的 Mock Feed。

    Args:
        clock: 可控单调时钟。
        policy: 显式更新 policy；None 用于验证 fail-closed。
        event_queue: 可选共享有界事件队列。

    Returns:
        MockRealtimeMarketDataFeed: 尚未连接的离线 Feed。
    """
    return MockRealtimeMarketDataFeed(
        _manifest(),
        event_queue=event_queue,
        stale_after_seconds={level: 3.0 for level in MarketDataLevel},
        update_expectation_policy=policy,
        monotonic_clock=clock,
    )


def _event(
    feed: MockRealtimeMarketDataFeed,
    event_type: MarketEventType,
    level: MarketDataLevel,
    *,
    price: float,
) -> MarketEvent:
    """
    构造当前 Mock epoch 的证券行情事件。

    Args:
        feed: 已连接并拥有 session epoch 的 Feed。
        event_type: 实际事件类型。
        level: 精确行情级别。
        price: 写入 payload 的测试价格。

    Returns:
        MarketEvent: 带 gateway/client/exchange 时间的不可变事件。
    """
    capability = {
        MarketEventType.TICK_COMPAT: "realtime.stream.tick_compat",
        MarketEventType.SNAPSHOT_L1: "realtime.snapshot.l1",
        MarketEventType.SNAPSHOT_L2: "realtime.snapshot.l2",
        MarketEventType.TRANSACTION: "realtime.stream.transaction",
    }[event_type]
    now = datetime(2026, 8, 13, 9, 30, 0)
    event_model = {
        MarketEventType.TICK_COMPAT: CompatibilityTickEvent,
        MarketEventType.SNAPSHOT_L1: QuoteSnapshotEvent,
        MarketEventType.SNAPSHOT_L2: DepthSnapshotEvent,
        MarketEventType.TRANSACTION: TransactionEvent,
    }[event_type]
    sequence_fields = (
        {
            "stream_id": "transaction",
            "channel_id": "channel-1",
            "source_sequence": SourceSequence({"MainSeq": 1, "SubSeq": 1}),
        }
        if event_type is MarketEventType.TRANSACTION
        else {}
    )
    return event_model(
        provider="mock",
        capability_key=capability,
        event_type=event_type,
        level=level,
        exchange="XSHG",
        session_epoch=feed.health().session_epoch or "missing",
        payload={"last_price": price},
        security="600000.XSHG",
        raw_security_code="600000",
        asset_type="stock",
        gateway_received_at=now,
        client_received_at=now,
        exchange_time=now,
        **sequence_fields,
    )


def _subscribe(
    feed: MockRealtimeMarketDataFeed,
    request_id: str,
    level: MarketDataLevel,
    event_type: MarketEventType,
) -> None:
    """
    为测试证券创建并确认一个精确订阅 lease。

    Args:
        feed: 已连接 Mock Feed。
        request_id: 本次测试唯一请求 ID。
        level: 精确行情级别。
        event_type: 实际事件类型。

    Returns:
        None: 回执确认后返回。
    """
    receipt = feed.subscribe(
        MarketSubscriptionSpec(
            request_id=request_id,
            selector="symbols",
            symbols=("600000.XSHG",),
            level=level,
            event_types=(event_type,),
        )
    )
    assert receipt.confirmed


def test_monotonic_age_excludes_explicit_lunch_pause() -> None:
    """验证午休累计暂停时间从 raw age 扣除，恢复后只累计有效更新窗口。"""
    clock = _FakeMonotonicClock()
    policy = _MutableUpdatePolicy()
    tracker = MarketFreshnessTracker(
        {level: 3.0 for level in MarketDataLevel},
        expectation_policy=policy,
        monotonic_clock=clock,
    )
    event = MarketEvent(
        provider="mock",
        capability_key="realtime.snapshot.l1",
        event_type="snapshot_l1",
        level="l1",
        exchange="XSHG",
        session_epoch="epoch-1",
        payload={"last_price": 10.0},
        security="600000.XSHG",
    )
    tracker.record(event)
    clock.advance(7200)
    policy.expected = False
    policy.market_state = "lunch_break"
    policy.paused_seconds = 7200
    assert tracker.evaluate("600000.XSHG", MarketDataLevel.L1).stale is False

    clock.advance(2)
    policy.expected = True
    policy.market_state = "continuous_trading"
    assert tracker.evaluate("600000.XSHG", MarketDataLevel.L1).effective_age_seconds == 2
    clock.advance(2)
    assert tracker.evaluate("600000.XSHG", MarketDataLevel.L1).stale is True


def test_lunch_pause_does_not_revive_snapshot_already_stale_before_pause() -> None:
    """
    验证 expected=false 只暂停继续增龄，不能清除午休前已经形成的 stale。

    Returns:
        None: 有效 age 仍超过阈值且 stale 保持 True 时返回。
    """
    clock = _FakeMonotonicClock()
    policy = _MutableUpdatePolicy()
    tracker = MarketFreshnessTracker(
        {level: 3.0 for level in MarketDataLevel},
        expectation_policy=policy,
        monotonic_clock=clock,
    )
    event = MarketEvent(
        provider="mock",
        capability_key="realtime.snapshot.l1",
        event_type="snapshot_l1",
        level="l1",
        exchange="XSHG",
        session_epoch="epoch-1",
        payload={"last_price": 10.0},
        security="600000.XSHG",
    )
    tracker.record(event)
    clock.advance(10)
    policy.expected = False
    policy.market_state = "lunch_break"
    policy.paused_seconds = 0

    decision = tracker.evaluate("600000.XSHG", MarketDataLevel.L1)

    assert decision.effective_age_seconds == 10
    assert decision.gateway_stale is True
    assert decision.stale is True


def test_lunch_market_state_is_visible_from_tick_and_snapshot_diagnostics() -> None:
    """
    验证午休已有快照不假 stale，且两类诊断入口显式保留 lunch_break。

    Returns:
        None: 策略读取成功且 tick/L1 freshness 均公开午休状态时返回。
    """
    clock = _FakeMonotonicClock()
    policy = _MutableUpdatePolicy()
    feed = _feed(clock, policy)
    feed.connect()
    _subscribe(feed, "lunch-tick", MarketDataLevel.TICK_COMPAT, MarketEventType.TICK_COMPAT)
    _subscribe(feed, "lunch-l1", MarketDataLevel.L1, MarketEventType.SNAPSHOT_L1)
    feed.publish_event(
        _event(feed, MarketEventType.TICK_COMPAT, MarketDataLevel.TICK_COMPAT, price=10.0)
    )
    feed.publish_event(_event(feed, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, price=10.0))
    clock.advance(7200)
    policy.expected = False
    policy.market_state = "lunch_break"
    policy.paused_seconds = 7200
    policy.effective_source_age_seconds = 0.0

    assert feed.get_current_tick("600000.XSHG")["last_price"] == 10.0
    assert feed.get_market_snapshot("600000.XSHG", MarketDataLevel.L1).payload["last_price"] == 10.0
    tick_diagnostic = feed.diagnose_current_tick("600000.XSHG")
    snapshot_diagnostic = feed.diagnose_market_snapshot("600000.XSHG", MarketDataLevel.L1)

    assert tick_diagnostic.freshness.market_state == "lunch_break"
    assert snapshot_diagnostic.freshness.market_state == "lunch_break"
    assert tick_diagnostic.freshness.stale is False
    assert snapshot_diagnostic.freshness.stale is False


def test_gateway_age_starts_at_ingress_before_queue_delay() -> None:
    """
    验证 bridge/队列等待时间计入 gateway age，而不是在 publish 时重新归零。

    Returns:
        None: 延迟发布立即触发 stale 且 raw age 等于完整等待时长时返回。
    """
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    _subscribe(feed, "ingress-l1", MarketDataLevel.L1, MarketEventType.SNAPSHOT_L1)
    event = _event(feed, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, price=10.0)
    ingress = feed.capture_gateway_ingress(event)
    clock.advance(4)

    assert feed.publish_event(event, gateway_ingress=ingress)
    with pytest.raises(StaleMarketDataError) as exc_info:
        feed.get_market_snapshot("600000.XSHG", MarketDataLevel.L1)

    assert exc_info.value.decision.raw_age_seconds == 4
    assert exc_info.value.decision.gateway_stale is True


def test_exchange_lag_can_stale_first_delivery_with_fresh_gateway_age() -> None:
    """
    验证显式 calendar/status owner 可让刚到 gateway 的旧交易所快照立即 stale。

    Returns:
        None: source age 与三类时间保留且 source stale 生效时返回。
    """
    clock = _FakeMonotonicClock()
    policy = _MutableUpdatePolicy()
    policy.effective_source_age_seconds = 10.0
    feed = _feed(clock, policy)
    feed.connect()
    _subscribe(feed, "source-lag", MarketDataLevel.L1, MarketEventType.SNAPSHOT_L1)
    event = _event(feed, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, price=10.0)
    assert feed.publish_event(event)

    with pytest.raises(StaleMarketDataError) as exc_info:
        feed.get_market_snapshot("600000.XSHG", MarketDataLevel.L1)

    decision = exc_info.value.decision
    assert decision.raw_age_seconds == 0
    assert decision.effective_source_age_seconds == 10
    assert decision.source_stale is True
    assert decision.last_exchange_time == event.exchange_time
    assert decision.last_gateway_received_at == event.gateway_received_at
    assert decision.last_client_received_at == event.client_received_at


def test_strategy_read_is_fail_closed_and_diagnostic_allow_stale_is_explicit() -> None:
    """验证未连接/无订阅/policy 未配置与 stale 使用独立错误和诊断入口。"""
    clock = _FakeMonotonicClock()
    no_policy_feed = _feed(clock, None)
    with pytest.raises(RealtimeDataUnavailableError, match="NOT_CONNECTED"):
        no_policy_feed.get_current_tick("600000.XSHG")
    no_policy_feed.connect()
    with pytest.raises(RealtimeDataUnavailableError, match="SUBSCRIPTION_NOT_CONFIRMED"):
        no_policy_feed.get_current_tick("600000.XSHG")
    _subscribe(
        no_policy_feed,
        "tick-no-policy",
        MarketDataLevel.TICK_COMPAT,
        MarketEventType.TICK_COMPAT,
    )
    no_policy_feed.publish_event(
        _event(
            no_policy_feed,
            MarketEventType.TICK_COMPAT,
            MarketDataLevel.TICK_COMPAT,
            price=10.0,
        )
    )
    with pytest.raises(RealtimeDataUnavailableError, match="UPDATE_POLICY_UNAVAILABLE"):
        no_policy_feed.get_current_tick("600000.XSHG")
    assert (
        no_policy_feed.diagnose_current_tick("600000.XSHG", allow_stale=True).tick["last_price"]
        == 10.0
    )
    assert (
        no_policy_feed.health().capability_readiness["realtime.stream.tick_compat"]
        is CapabilityReadiness.UNAVAILABLE
    )

    policy = _MutableUpdatePolicy()
    stale_feed = _feed(clock, policy)
    stale_feed.connect()
    _subscribe(
        stale_feed,
        "tick-stale",
        MarketDataLevel.TICK_COMPAT,
        MarketEventType.TICK_COMPAT,
    )
    stale_feed.publish_event(
        _event(
            stale_feed,
            MarketEventType.TICK_COMPAT,
            MarketDataLevel.TICK_COMPAT,
            price=10.1,
        )
    )
    clock.advance(4)
    with pytest.raises(StaleMarketDataError):
        stale_feed.get_current_tick("600000.XSHG")
    assert (
        stale_feed.diagnose_current_tick("600000.XSHG", allow_stale=True).tick["last_price"] == 10.1
    )


def test_l1_and_l2_are_exact_and_reconnect_clears_old_epoch_cache() -> None:
    """验证 L1 不冒充 L2，且重连后旧 epoch 快照不能满足当前读取。"""
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    _subscribe(feed, "l1", MarketDataLevel.L1, MarketEventType.SNAPSHOT_L1)
    _subscribe(feed, "l2", MarketDataLevel.L2, MarketEventType.SNAPSHOT_L2)
    feed.publish_event(_event(feed, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, price=10.0))
    with pytest.raises(RealtimeDataUnavailableError, match="NOT_RECEIVED"):
        feed.get_market_snapshot("600000.XSHG", MarketDataLevel.L2)
    assert feed.get_market_snapshot("600000.XSHG", MarketDataLevel.L1).level is MarketDataLevel.L1

    feed.disconnect()
    feed.connect()
    health = feed.health()
    assert health.reconnect_count == 1
    assert health.active_subscriptions
    assert health.last_gateway_received_at is None
    with pytest.raises(RealtimeDataUnavailableError, match="NOT_RECEIVED"):
        feed.get_market_snapshot("600000.XSHG", MarketDataLevel.L1)


def test_health_reports_event_times_receipts_and_queue_loss_metrics() -> None:
    """验证 health 暴露分层时间、active receipt、水位、合并、溢出和 gap 降级。"""
    clock = _FakeMonotonicClock()
    queue = BoundedMarketEventQueue(capacity=1)
    feed = _feed(clock, _MutableUpdatePolicy(), event_queue=queue)
    feed.connect()
    _subscribe(feed, "l2-health", MarketDataLevel.L2, MarketEventType.SNAPSHOT_L2)
    snapshot = _event(feed, MarketEventType.SNAPSHOT_L2, MarketDataLevel.L2, price=10.0)
    assert feed.publish_event(snapshot)
    queue.put_nowait(snapshot)
    queue.put_nowait(_event(feed, MarketEventType.SNAPSHOT_L2, MarketDataLevel.L2, price=10.1))
    queue.put_nowait(_event(feed, MarketEventType.TRANSACTION, MarketDataLevel.L2, price=10.1))

    health = feed.health()
    assert len(health.active_subscriptions) == 1
    assert health.last_gateway_received_at == snapshot.gateway_received_at
    assert health.capability_event_times["realtime.snapshot.l2"].gateway_age_seconds == 0
    assert health.module_event_times["l2"].last_exchange_time == snapshot.exchange_time
    assert health.queue_depth == 1
    assert health.queue_control_depth == 2
    assert health.queue_control_capacity == 2
    assert health.queue_control_scope_depth == 1
    assert health.queue_control_scope_capacity == 1
    assert health.queue_control_overflow_count == 0
    assert health.queue_high_watermark == 1
    assert health.queue_coalesced_count == 1
    assert health.queue_overflow_count == 1
    assert health.queue_loss_boundary_count == 1
    assert health.gap_count == 0
    assert health.queue_degraded is True
    assert health.queue_overflow_by_event_type == {"transaction": 1}
    assert (
        health.capability_readiness["realtime.stream.transaction"]
        is CapabilityReadiness.UNAVAILABLE
    )
    assert health.module_readiness["l2"] is CapabilityReadiness.READY


def test_control_capacity_exhaustion_is_hard_unavailable_in_health() -> None:
    """验证控制 scope 耗尽被 health 明确暴露并使受影响能力不可用。

    Returns:
        None: 控制容量、溢出计数、原因和能力状态全部符合 fail-closed 合同时返回。

    Side Effects:
        填满纯内存数据及控制队列，并捕获预期的具名容量异常。
    """
    clock = _FakeMonotonicClock()
    queue = BoundedMarketEventQueue(capacity=1, control_scope_capacity=1)
    feed = _feed(clock, _MutableUpdatePolicy(), event_queue=queue)
    feed.connect()
    _subscribe(feed, "transaction", MarketDataLevel.L2, MarketEventType.TRANSACTION)
    transaction = _event(feed, MarketEventType.TRANSACTION, MarketDataLevel.L2, price=10.0)
    assert feed.publish_event(transaction)
    assert queue.put_nowait(transaction).accepted is True
    assert queue.put_nowait(replace(transaction, stream_id="overflow-one")).accepted is False

    with pytest.raises(MarketEventControlCapacityError):
        queue.put_nowait(
            replace(
                transaction,
                session_epoch="queue-other-epoch",
                stream_id="overflow-two",
            )
        )

    health = feed.health()
    assert health.queue_control_depth == 2
    assert health.queue_control_capacity == 2
    assert health.queue_control_scope_depth == 1
    assert health.queue_control_scope_capacity == 1
    assert health.queue_control_overflow_count == 1
    assert "market_event_control_capacity_exhausted" in health.reasons
    assert (
        health.capability_readiness["realtime.stream.transaction"]
        is CapabilityReadiness.UNAVAILABLE
    )
    assert health.module_readiness["l2"] is CapabilityReadiness.UNAVAILABLE


def test_native_controls_bypass_subscription_and_do_not_refresh_data_age() -> None:
    """
    验证原生 gap/status 不要求数据 lease、不占满载数据槽且不冒充最近数据时间。

    Returns:
        None: 控制 callback、真实 gap 计数和数据时间隔离全部成立时返回。
    """
    clock = _FakeMonotonicClock()
    queue = BoundedMarketEventQueue(capacity=1)
    feed = _feed(clock, _MutableUpdatePolicy(), event_queue=queue)
    feed.connect()
    queued = _event(feed, MarketEventType.SNAPSHOT_L2, MarketDataLevel.L2, price=10.0)
    queue.put_nowait(queued)
    observed: List[MarketEvent] = []
    feed.set_market_event_callback(observed.append)
    event_time = datetime(2026, 8, 13, 9, 30, 1)
    common = dict(
        provider="mock",
        capability_key="realtime.stream.transaction",
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch=feed.health().session_epoch or "missing",
        stream_id="transaction",
        channel_id="channel-1",
        security="600000.XSHG",
        gateway_received_at=event_time,
        client_received_at=event_time,
        source_sequence=SourceSequence({"MainSeq": 2, "SubSeq": 1}),
    )
    gap = SequenceGapEvent(
        event_type=MarketEventType.STREAM_GAP,
        payload={
            "state": "degraded",
            "continuous": False,
            "reason": "source_gap",
            "loss_boundary_id": "gap-1",
        },
        **common,
    )
    status = ConnectionStateEvent(
        event_type=MarketEventType.STREAM_STATUS,
        payload={"state": "degraded", "continuous": False, "reason": "source_gap"},
        **common,
    )

    assert feed.publish_event(gap)
    assert feed.publish_event(status)
    health = feed.health()

    assert observed == [gap, status]
    assert health.gap_count == 1
    assert health.queue_depth == 1
    assert health.queue_overflow_count == 0
    assert "realtime.stream.transaction" not in health.capability_event_times
    assert (
        health.capability_readiness["realtime.stream.transaction"]
        is CapabilityReadiness.UNAVAILABLE
    )


def test_source_gap_degrades_ready_capability_and_manifest_health_agree() -> None:
    """
    验证首个逐笔事件就绪后，精确 source gap 同时降级 public manifest 与 health。

    Returns:
        None: 两个公共视图均为 degraded 且重复 boundary 不重复计数时返回。
    """
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    _subscribe(feed, "transaction", MarketDataLevel.L2, MarketEventType.TRANSACTION)
    transaction = _event(feed, MarketEventType.TRANSACTION, MarketDataLevel.L2, price=10.0)
    assert feed.publish_event(transaction)
    assert feed.manifest.get("realtime.stream.transaction").readiness is CapabilityReadiness.READY
    event_time = datetime(2026, 8, 13, 9, 30, 1)
    gap = SequenceGapEvent(
        provider="mock",
        capability_key="realtime.stream.transaction",
        event_type=MarketEventType.STREAM_GAP,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch=feed.health().session_epoch or "missing",
        payload={
            "state": "degraded",
            "continuous": False,
            "loss_boundary_id": "gap-ready-1",
        },
        stream_id="transaction",
        channel_id="channel-1",
        security="600000.XSHG",
        gateway_received_at=event_time,
        client_received_at=event_time,
        source_sequence=SourceSequence({"MainSeq": 3, "SubSeq": 1}),
    )

    assert feed.publish_event(gap)
    assert feed.publish_event(
        replace(
            gap,
            gateway_received_at=event_time + timedelta(milliseconds=1),
            client_received_at=event_time + timedelta(milliseconds=1),
        )
    )
    health = feed.health()

    assert health.gap_count == 1
    assert (
        health.capability_readiness["realtime.stream.transaction"] is CapabilityReadiness.DEGRADED
    )
    assert (
        feed.manifest.get("realtime.stream.transaction").readiness is CapabilityReadiness.DEGRADED
    )


def test_manifest_health_and_router_supplier_follow_fresh_then_stale_state() -> None:
    """
    验证 manifest、health 与 Router 每次 resolve 都消费同一动态 freshness 真相。

    Returns:
        None: fresh 可路由、推进时钟后全部 stale 且不换源时返回。
    """
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    _subscribe(feed, "router-l1", MarketDataLevel.L1, MarketEventType.SNAPSHOT_L1)
    assert feed.publish_event(
        _event(feed, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, price=10.0)
    )
    capability_id = "realtime.snapshot.l1"
    router = DataSourceRouter()
    router.register_provider(
        feed.manifest,
        feed,
        manifest_supplier=lambda: feed.manifest,
    )
    router.set_route(RouteRule(capability_id, "mock", rule_id="dynamic-health"))

    assert router.resolve(CapabilityRequest(capability_id)).provider == "mock"
    assert feed.health().capability_readiness[capability_id] is CapabilityReadiness.READY
    clock.advance(4)

    assert feed.manifest.get(capability_id).readiness is CapabilityReadiness.STALE
    assert feed.health().capability_readiness[capability_id] is CapabilityReadiness.STALE
    with pytest.raises(DataCapabilityNotReadyError) as exc_info:
        router.resolve(CapabilityRequest(capability_id))
    assert exc_info.value.provider == "mock"
    assert exc_info.value.readiness is CapabilityReadiness.STALE


def test_event_identity_mismatch_is_rejected_before_health_mutation() -> None:
    """
    验证 event type、capability 与 level 错配在任何缓存和 health 修改前受控拒绝。

    Returns:
        None: 错配事件失败且最近事件时间仍为空时返回。
    """
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    now = datetime(2026, 8, 13, 9, 30, 0)
    mismatch = MarketEvent(
        provider="mock",
        capability_key="realtime.snapshot.l2",
        event_type=MarketEventType.SNAPSHOT_L1,
        level=MarketDataLevel.L1,
        exchange="XSHG",
        session_epoch=feed.health().session_epoch or "missing",
        payload={"last_price": 10.0},
        security="600000.XSHG",
        gateway_received_at=now,
        client_received_at=now,
        exchange_time=now,
    )

    with pytest.raises(RuntimeError, match="EVENT_CAPABILITY_MISMATCH"):
        feed.publish_event(mismatch)

    assert feed.health().last_gateway_received_at is None
    assert feed.health().capability_event_times == {}


def test_reconnect_waits_for_inflight_callback_and_rejects_old_epoch_event() -> None:
    """
    验证 epoch 切换等待旧 callback 完成，且旧 ingress/event 不能在新 epoch 投递。

    Returns:
        None: callback 仅观察旧 epoch，重连完成后旧事件受控拒绝时返回。
    """
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    _subscribe(feed, "epoch-l1", MarketDataLevel.L1, MarketEventType.SNAPSHOT_L1)
    old_epoch = feed.health().session_epoch
    event = _event(feed, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, price=10.0)
    old_ingress = feed.capture_gateway_ingress(event)
    callback_entered = Event()
    callback_release = Event()
    reconnect_done = Event()
    callback_epochs: List[Optional[str]] = []

    def callback(_event_value: MarketEvent) -> None:
        """
        阻塞旧 callback 以确定性制造与重连并发。

        Args:
            _event_value: 当前旧 epoch 事件；测试只验证调度边界。

        Returns:
            None: 主测试允许 callback 退出后返回。
        """
        del _event_value
        callback_epochs.append(feed.health().session_epoch)
        callback_entered.set()
        assert callback_release.wait(2)

    def reconnect() -> None:
        """
        在另一线程执行断开和新 epoch 连接。

        Returns:
            None: 新 epoch 建立并设置完成信号后返回。
        """
        feed.disconnect()
        feed.connect()
        reconnect_done.set()

    feed.set_market_event_callback(callback)
    publish_thread = Thread(target=feed.publish_event, args=(event,))
    publish_thread.start()
    assert callback_entered.wait(2)
    reconnect_thread = Thread(target=reconnect)
    reconnect_thread.start()
    assert reconnect_done.wait(0.05) is False
    callback_release.set()
    publish_thread.join(2)
    reconnect_thread.join(2)

    assert publish_thread.is_alive() is False
    assert reconnect_thread.is_alive() is False
    assert callback_epochs == [old_epoch]
    assert feed.health().session_epoch != old_epoch
    with pytest.raises(RuntimeError, match="EVENT_SESSION_EPOCH_MISMATCH"):
        feed.publish_event(event, gateway_ingress=old_ingress)


def test_missing_gateway_time_is_rejected_before_health_mutation() -> None:
    """
    验证受控入口不接受缺失 gateway_received_at 的实时事件。

    Returns:
        None: 事件受控拒绝且 health 没有伪造时间证据时返回。
    """
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    _subscribe(feed, "missing-gateway", MarketDataLevel.L1, MarketEventType.SNAPSHOT_L1)
    event = MarketEvent(
        provider="mock",
        capability_key="realtime.snapshot.l1",
        event_type=MarketEventType.SNAPSHOT_L1,
        level=MarketDataLevel.L1,
        exchange="XSHG",
        session_epoch=feed.health().session_epoch or "missing",
        payload={"last_price": 10.0},
        security="600000.XSHG",
        raw_security_code="600000",
        client_received_at=datetime(2026, 8, 13, 9, 30, 0),
        exchange_time=datetime(2026, 8, 13, 9, 30, 0),
    )

    with pytest.raises(RuntimeError, match="EVENT_GATEWAY_RECEIVED_AT_REQUIRED"):
        feed.publish_event(event)

    assert feed.health().last_gateway_received_at is None
    assert feed.health().capability_event_times == {}


def test_gateway_ingress_mark_cannot_be_reused_for_different_payload() -> None:
    """
    验证 ingress mark 绑定完整不可变事件，不能挪给同 scope 的另一载荷。

    Returns:
        None: payload 漂移在缓存和 health 修改前受控拒绝时返回。
    """
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    _subscribe(feed, "mark-binding", MarketDataLevel.L1, MarketEventType.SNAPSHOT_L1)
    original = _event(feed, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, price=10.0)
    ingress = feed.capture_gateway_ingress(original)
    changed = replace(original, payload={"last_price": 10.1})

    with pytest.raises(RuntimeError, match="GATEWAY_INGRESS_EVENT_MISMATCH"):
        feed.publish_event(changed, gateway_ingress=ingress)

    assert feed.health().capability_event_times == {}


def test_missing_exchange_time_is_unavailable_but_diagnostic_can_read() -> None:
    """
    验证缺失交易所源时间不能被 fresh gateway age 冒充为可交易快照。

    Returns:
        None: 公共 readiness 与策略读取 fail-closed，显式诊断仍可查看缓存时返回。
    """
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    _subscribe(feed, "missing-exchange", MarketDataLevel.L1, MarketEventType.SNAPSHOT_L1)
    event = replace(
        _event(feed, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, price=10.0),
        exchange_time=None,
    )
    assert feed.publish_event(event)

    assert feed.manifest.get("realtime.snapshot.l1").readiness is CapabilityReadiness.UNAVAILABLE
    assert (
        feed.health().capability_readiness["realtime.snapshot.l1"]
        is CapabilityReadiness.UNAVAILABLE
    )
    with pytest.raises(RealtimeDataUnavailableError, match="EXCHANGE_TIME_UNVERIFIED"):
        feed.get_market_snapshot("600000.XSHG", MarketDataLevel.L1)
    assert (
        feed.diagnose_market_snapshot("600000.XSHG", MarketDataLevel.L1, allow_stale=True).event
        is event
    )


def test_policy_failure_is_a_stable_unavailable_error_in_all_public_views() -> None:
    """
    验证 calendar/status owner 异常不会泄漏原始 RuntimeError 或保持 ready。

    Returns:
        None: manifest、health 和策略读取统一 fail-closed 为 unavailable 时返回。
    """

    def failing_policy(
        security: str,
        level: MarketDataLevel,
        event: MarketEvent,
        raw_age_seconds: float,
    ) -> MarketUpdateExpectation:
        """
        模拟不可用的 calendar/status owner。

        Args:
            security: 当前证券代码。
            level: 当前行情级别。
            event: 当前快照事件。
            raw_age_seconds: 当前 gateway 单调 age。

        Returns:
            MarketUpdateExpectation: 本测试永不返回。

        Raises:
            RuntimeError: 固定模拟外部 policy 故障。
        """
        del security, level, event, raw_age_seconds
        raise RuntimeError("calendar down")

    clock = _FakeMonotonicClock()
    feed = MockRealtimeMarketDataFeed(
        _manifest(),
        stale_after_seconds={level: 3.0 for level in MarketDataLevel},
        update_expectation_policy=failing_policy,
        monotonic_clock=clock,
    )
    feed.connect()
    _subscribe(feed, "policy-failure", MarketDataLevel.L1, MarketEventType.SNAPSHOT_L1)
    feed.publish_event(_event(feed, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, price=10.0))

    assert feed.manifest.get("realtime.snapshot.l1").readiness is CapabilityReadiness.UNAVAILABLE
    assert (
        feed.health().capability_readiness["realtime.snapshot.l1"]
        is CapabilityReadiness.UNAVAILABLE
    )
    with pytest.raises(RealtimeDataUnavailableError, match="UPDATE_POLICY_UNAVAILABLE"):
        feed.get_market_snapshot("600000.XSHG", MarketDataLevel.L1)


def test_unsubscribed_gap_scope_does_not_degrade_new_symbol_scope() -> None:
    """
    验证退订 A 后其旧逐笔 gap 不会污染新订阅并已收首帧的 B。

    Returns:
        None: B 的 capability 恢复 ready 且历史 gap 计数仍保留时返回。
    """
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    receipt_a = feed.subscribe(
        MarketSubscriptionSpec(
            request_id="transaction-a",
            selector=SubscriptionSelector.SYMBOLS,
            symbols=("600000.XSHG",),
            level=MarketDataLevel.L2,
            event_types=(MarketEventType.TRANSACTION,),
        )
    )
    transaction_a = _event(feed, MarketEventType.TRANSACTION, MarketDataLevel.L2, price=10.0)
    assert feed.publish_event(transaction_a)
    event_time = datetime(2026, 8, 13, 9, 30, 1)
    gap_a = SequenceGapEvent(
        provider="mock",
        capability_key="realtime.stream.transaction",
        event_type=MarketEventType.STREAM_GAP,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch=feed.health().session_epoch or "missing",
        payload={"continuous": False, "loss_boundary_id": "a-gap"},
        stream_id="transaction",
        channel_id="channel-1",
        security="600000.XSHG",
        gateway_received_at=event_time,
        client_received_at=event_time,
        source_sequence=SourceSequence({"MainSeq": 7, "SubSeq": 1}),
    )
    assert feed.publish_event(gap_a)
    assert (
        feed.manifest.get("realtime.stream.transaction").readiness is CapabilityReadiness.DEGRADED
    )

    feed.unsubscribe(receipt_a.subscription_id)
    feed.subscribe(
        MarketSubscriptionSpec(
            request_id="transaction-b",
            selector=SubscriptionSelector.SYMBOLS,
            symbols=("600001.XSHG",),
            level=MarketDataLevel.L2,
            event_types=(MarketEventType.TRANSACTION,),
        )
    )
    transaction_b = replace(
        transaction_a,
        security="600001.XSHG",
        raw_security_code="600001",
        payload={"last_price": 11.0},
    )
    assert feed.publish_event(transaction_b)
    health = feed.health()

    assert health.gap_count == 1
    assert health.capability_readiness["realtime.stream.transaction"] is CapabilityReadiness.READY


def test_shared_capability_does_not_make_unsubscribed_l2_module_ready() -> None:
    """
    验证仅收到 L1 的共享 market-status 能力不会抬高 L2 模块状态。

    Returns:
        None: L1 ready 而未订阅 L2 保持 unavailable 时返回。
    """
    capability_id = "realtime.stream.market_status"
    base_manifest = _manifest()
    capabilities = dict(base_manifest.capabilities)
    capabilities[capability_id] = CapabilityDeclaration(
        capability_id=capability_id,
        semantic_class=capability_id,
        support=CapabilitySupport.SUPPORTED,
        readiness=CapabilityReadiness.UNAVAILABLE,
        markets=("XSHG",),
        asset_types=("stock",),
    )
    clock = _FakeMonotonicClock()
    feed = MockRealtimeMarketDataFeed(
        replace(base_manifest, capabilities=capabilities),
        stale_after_seconds={level: 3.0 for level in MarketDataLevel},
        update_expectation_policy=_MutableUpdatePolicy(),
        monotonic_clock=clock,
    )
    feed.connect()
    _subscribe(feed, "status-l1", MarketDataLevel.L1, MarketEventType.MARKET_STATUS)
    event_time = datetime(2026, 8, 13, 9, 30, 0)
    event = MarketStatusEvent(
        provider="mock",
        capability_key=capability_id,
        event_type=MarketEventType.MARKET_STATUS,
        level=MarketDataLevel.L1,
        exchange="XSHG",
        session_epoch=feed.health().session_epoch or "missing",
        payload={"state": "continuous_trading"},
        security="600000.XSHG",
        gateway_received_at=event_time,
        client_received_at=event_time,
        exchange_time=event_time,
    )
    assert feed.publish_event(event)
    health = feed.health()

    assert health.module_readiness["l1"] is CapabilityReadiness.READY
    assert health.module_readiness["l2"] is CapabilityReadiness.UNAVAILABLE


def test_unrelated_high_rate_events_do_not_starve_snapshot_read_or_health() -> None:
    """
    验证 policy 执行期间持续发布 B 不会让 A 的读取和 health 全局重试耗尽。

    Returns:
        None: A 读取与 capability health 均稳定 ready 时返回。
    """
    clock = _FakeMonotonicClock()
    feed_holder: List[MockRealtimeMarketDataFeed] = []
    event_holder: List[MarketEvent] = []

    def publishing_policy(
        security: str,
        level: MarketDataLevel,
        event: MarketEvent,
        raw_age_seconds: float,
    ) -> MarketUpdateExpectation:
        """
        每次 freshness 评估都同步发布一个无关证券 B 的新事件。

        Args:
            security: 当前评估证券。
            level: 当前行情级别。
            event: 当前证券最近事件。
            raw_age_seconds: 当前 gateway 单调 age。

        Returns:
            MarketUpdateExpectation: 连续交易且 source age 已验证的结果。
        """
        del security, level, event, raw_age_seconds
        if feed_holder and event_holder:
            feed_holder[0].publish_event(event_holder[0])
        return MarketUpdateExpectation(
            expected=True,
            market_state="continuous_trading",
            effective_source_age_seconds=0.0,
        )

    feed = MockRealtimeMarketDataFeed(
        _manifest(),
        stale_after_seconds={level: 3.0 for level in MarketDataLevel},
        update_expectation_policy=publishing_policy,
        monotonic_clock=clock,
    )
    feed_holder.append(feed)
    feed.connect()
    feed.subscribe(
        MarketSubscriptionSpec(
            request_id="high-rate-l1",
            selector=SubscriptionSelector.SYMBOLS,
            symbols=("600000.XSHG", "600001.XSHG"),
            level=MarketDataLevel.L1,
            event_types=(MarketEventType.SNAPSHOT_L1,),
        )
    )
    event_a = _event(feed, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, price=10.0)
    event_b = replace(
        event_a,
        security="600001.XSHG",
        raw_security_code="600001",
        payload={"last_price": 11.0},
    )
    event_holder.append(event_b)
    assert feed.publish_event(event_a)
    assert feed.publish_event(event_b)

    assert feed.get_market_snapshot("600000.XSHG", MarketDataLevel.L1) is event_a
    assert feed.health().capability_readiness["realtime.snapshot.l1"] is CapabilityReadiness.READY


def test_runtime_snapshot_does_not_mix_old_readiness_with_new_event_times() -> None:
    """
    验证并发新帧不会让一次 health 混合旧 stale 决策和新 age=0 时间。

    Returns:
        None: 首次 health 保持同代旧证据，下一次完整切换到新帧时返回。
    """
    clock = _FakeMonotonicClock()
    policy_entered = Event()
    policy_release = Event()
    blocking = {"enabled": False}

    def blocking_policy(
        security: str,
        level: MarketDataLevel,
        event: MarketEvent,
        raw_age_seconds: float,
    ) -> MarketUpdateExpectation:
        """
        在 health 已冻结 marks 后阻塞，制造同证券新帧并发。

        Args:
            security: 当前证券代码。
            level: 当前行情级别。
            event: 冻结 snapshot 中的事件。
            raw_age_seconds: 冻结事件的 gateway age。

        Returns:
            MarketUpdateExpectation: 连续交易且 source age 已验证的结果。
        """
        del security, level, event
        if blocking["enabled"]:
            policy_entered.set()
            assert policy_release.wait(2)
        return MarketUpdateExpectation(
            expected=True,
            market_state="continuous_trading",
            effective_source_age_seconds=raw_age_seconds,
        )

    feed = MockRealtimeMarketDataFeed(
        _manifest(),
        stale_after_seconds={level: 3.0 for level in MarketDataLevel},
        update_expectation_policy=blocking_policy,
        monotonic_clock=clock,
    )
    feed.connect()
    _subscribe(feed, "coherent-health", MarketDataLevel.L1, MarketEventType.SNAPSHOT_L1)
    old_event = _event(feed, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, price=10.0)
    assert feed.publish_event(old_event)
    clock.advance(4)
    results: List[market_data.FeedHealth] = []

    def read_health() -> None:
        """
        在线程中读取一次应保持同代的 health。

        Returns:
            None: health 追加到结果列表后返回。
        """
        results.append(feed.health())

    blocking["enabled"] = True
    reader = Thread(target=read_health)
    reader.start()
    assert policy_entered.wait(2)
    new_time = datetime(2026, 8, 13, 9, 30, 1)
    new_event = replace(
        old_event,
        payload={"last_price": 11.0},
        gateway_received_at=new_time,
        client_received_at=new_time,
        exchange_time=new_time,
    )
    assert feed.publish_event(new_event)
    blocking["enabled"] = False
    policy_release.set()
    reader.join(2)

    assert reader.is_alive() is False
    assert len(results) == 1
    old_health = results[0]
    assert old_health.capability_readiness["realtime.snapshot.l1"] is CapabilityReadiness.STALE
    assert (
        old_health.capability_event_times["realtime.snapshot.l1"].last_gateway_received_at
        == old_event.gateway_received_at
    )
    assert old_health.capability_event_times["realtime.snapshot.l1"].gateway_age_seconds == 4
    new_health = feed.health()
    assert new_health.capability_readiness["realtime.snapshot.l1"] is CapabilityReadiness.READY
    assert (
        new_health.capability_event_times["realtime.snapshot.l1"].last_gateway_received_at
        == new_event.gateway_received_at
    )


def test_late_old_snapshot_cannot_roll_back_cache_or_recent_times() -> None:
    """
    验证先到 gateway 的旧帧晚发布时不会覆盖已发布的新帧。

    Returns:
        None: 旧 ingress 被拒绝，缓存和最近时间继续指向新帧时返回。
    """
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    _subscribe(feed, "out-of-order", MarketDataLevel.L1, MarketEventType.SNAPSHOT_L1)
    old_event = _event(feed, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, price=10.0)
    old_ingress = feed.capture_gateway_ingress(old_event)
    clock.advance(1)
    new_time = datetime(2026, 8, 13, 9, 30, 1)
    new_event = replace(
        old_event,
        payload={"last_price": 11.0},
        gateway_received_at=new_time,
        client_received_at=new_time,
        exchange_time=new_time,
    )
    new_ingress = feed.capture_gateway_ingress(new_event)
    assert feed.publish_event(new_event, gateway_ingress=new_ingress)

    with pytest.raises(MarketFreshnessError, match="OUT_OF_ORDER_GATEWAY_INGRESS"):
        feed.publish_event(old_event, gateway_ingress=old_ingress)

    assert feed.get_market_snapshot("600000.XSHG", MarketDataLevel.L1).payload["last_price"] == 11.0
    assert feed.health().last_gateway_received_at == new_time


def test_callback_cannot_reenter_lifecycle_and_leak_old_epoch_event() -> None:
    """
    验证 callback 内重连受控拒绝，后续 typed callback 不会落入新 epoch。

    Returns:
        None: 生命周期保持旧 epoch 且两个 callback 观察同一 epoch 时返回。
    """
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    _subscribe(feed, "callback-tick", MarketDataLevel.TICK_COMPAT, MarketEventType.TICK_COMPAT)
    original_epoch = feed.health().session_epoch
    lifecycle_errors: List[str] = []
    typed_epochs: List[tuple] = []

    def tick_callback(_tick: object) -> None:
        """
        在 tick callback 内尝试非法断开。

        Args:
            _tick: 当前兼容 tick；测试不读取载荷。

        Returns:
            None: 具名生命周期错误被测试捕获后返回。
        """
        del _tick
        try:
            feed.disconnect()
        except RuntimeError as exc:
            lifecycle_errors.append(str(exc))

    def market_callback(event: MarketEvent) -> None:
        """
        记录 typed callback 事件 epoch 和当前 Feed epoch。

        Args:
            event: 当前兼容 tick 的 typed 事件。

        Returns:
            None: epoch 对追加完成后返回。
        """
        typed_epochs.append((event.session_epoch, feed.health().session_epoch))

    feed.set_tick_callback(tick_callback)
    feed.set_market_event_callback(market_callback)
    event = _event(feed, MarketEventType.TICK_COMPAT, MarketDataLevel.TICK_COMPAT, price=10.0)
    assert feed.publish_event(event)

    assert lifecycle_errors == ["LIFECYCLE_CHANGE_DURING_CALLBACK"]
    assert typed_epochs == [(original_epoch, original_epoch)]
    assert feed.health().session_epoch == original_epoch


def test_generic_transaction_cannot_bypass_typed_sequence_contract() -> None:
    """
    验证 generic MarketEvent 不能绕过逐笔 stream/channel/sequence 的 typed 约束。

    Returns:
        None: 非 typed 逐笔在 readiness 和 callback 修改前受控拒绝时返回。
    """
    clock = _FakeMonotonicClock()
    feed = _feed(clock, _MutableUpdatePolicy())
    feed.connect()
    _subscribe(feed, "typed-transaction", MarketDataLevel.L2, MarketEventType.TRANSACTION)
    event_time = datetime(2026, 8, 13, 9, 30, 0)
    generic = MarketEvent(
        provider="mock",
        capability_key="realtime.stream.transaction",
        event_type=MarketEventType.TRANSACTION,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch=feed.health().session_epoch or "missing",
        payload={"price": 10.0},
        security="600000.XSHG",
        raw_security_code="600000",
        gateway_received_at=event_time,
        client_received_at=event_time,
        exchange_time=event_time,
    )

    with pytest.raises(RuntimeError, match="DATA_EVENT_REQUIRES_TYPED_MODEL"):
        feed.publish_event(generic)

    assert (
        feed.health().capability_readiness["realtime.stream.transaction"]
        is CapabilityReadiness.UNAVAILABLE
    )


def test_l2_gap_on_shared_capability_does_not_degrade_l1_module() -> None:
    """
    验证 L1/L2 双活共享 capability 中仅 L2 gap 只降级 L2 模块。

    Returns:
        None: public capability 保守 degraded，而 L1/L2 模块分别 ready/degraded 时返回。
    """
    capability_id = "realtime.stream.market_status"
    base_manifest = _manifest()
    capabilities = dict(base_manifest.capabilities)
    capabilities[capability_id] = CapabilityDeclaration(
        capability_id=capability_id,
        semantic_class=capability_id,
        support=CapabilitySupport.SUPPORTED,
        readiness=CapabilityReadiness.UNAVAILABLE,
        markets=("XSHG",),
        asset_types=("stock",),
    )
    clock = _FakeMonotonicClock()
    feed = MockRealtimeMarketDataFeed(
        replace(base_manifest, capabilities=capabilities),
        stale_after_seconds={level: 3.0 for level in MarketDataLevel},
        update_expectation_policy=_MutableUpdatePolicy(),
        monotonic_clock=clock,
    )
    feed.connect()
    _subscribe(feed, "status-shared-l1", MarketDataLevel.L1, MarketEventType.MARKET_STATUS)
    _subscribe(feed, "status-shared-l2", MarketDataLevel.L2, MarketEventType.MARKET_STATUS)
    event_time = datetime(2026, 8, 13, 9, 30, 0)
    common = dict(
        provider="mock",
        capability_key=capability_id,
        event_type=MarketEventType.MARKET_STATUS,
        exchange="XSHG",
        session_epoch=feed.health().session_epoch or "missing",
        payload={"state": "continuous_trading"},
        security="600000.XSHG",
        gateway_received_at=event_time,
        client_received_at=event_time,
        exchange_time=event_time,
    )
    assert feed.publish_event(MarketStatusEvent(level=MarketDataLevel.L1, **common))
    assert feed.publish_event(MarketStatusEvent(level=MarketDataLevel.L2, **common))
    gap = SequenceGapEvent(
        provider="mock",
        capability_key=capability_id,
        event_type=MarketEventType.STREAM_GAP,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch=feed.health().session_epoch or "missing",
        payload={"continuous": False, "loss_boundary_id": "status-l2-gap"},
        stream_id="market-status",
        channel_id="channel-l2",
        security="600000.XSHG",
        gateway_received_at=event_time,
        client_received_at=event_time,
        source_sequence=SourceSequence({"MainSeq": 9, "SubSeq": 1}),
    )
    assert feed.publish_event(gap)
    health = feed.health()

    assert health.capability_readiness[capability_id] is CapabilityReadiness.DEGRADED
    assert health.module_readiness["l1"] is CapabilityReadiness.READY
    assert health.module_readiness["l2"] is CapabilityReadiness.DEGRADED


def test_shared_capability_module_with_first_frame_is_ready_independently() -> None:
    """
    验证 L1/L2 双订阅仅收到 L1 首帧时，L1 不被缺帧的 L2 连带阻断。

    Returns:
        None: public capability 保守 unavailable，L1/L2 分别 ready/unavailable 时返回。
    """
    capability_id = "realtime.stream.market_status"
    base_manifest = _manifest()
    capabilities = dict(base_manifest.capabilities)
    capabilities[capability_id] = CapabilityDeclaration(
        capability_id=capability_id,
        semantic_class=capability_id,
        support=CapabilitySupport.SUPPORTED,
        readiness=CapabilityReadiness.UNAVAILABLE,
        markets=("XSHG",),
        asset_types=("stock",),
    )
    clock = _FakeMonotonicClock()
    feed = MockRealtimeMarketDataFeed(
        replace(base_manifest, capabilities=capabilities),
        stale_after_seconds={level: 3.0 for level in MarketDataLevel},
        update_expectation_policy=_MutableUpdatePolicy(),
        monotonic_clock=clock,
    )
    feed.connect()
    _subscribe(feed, "status-first-l1", MarketDataLevel.L1, MarketEventType.MARKET_STATUS)
    _subscribe(feed, "status-missing-l2", MarketDataLevel.L2, MarketEventType.MARKET_STATUS)
    event_time = datetime(2026, 8, 13, 9, 30, 0)
    assert feed.publish_event(
        MarketStatusEvent(
            provider="mock",
            capability_key=capability_id,
            event_type=MarketEventType.MARKET_STATUS,
            level=MarketDataLevel.L1,
            exchange="XSHG",
            session_epoch=feed.health().session_epoch or "missing",
            payload={"state": "continuous_trading"},
            security="600000.XSHG",
            gateway_received_at=event_time,
            client_received_at=event_time,
            exchange_time=event_time,
        )
    )
    health = feed.health()

    assert health.capability_readiness[capability_id] is CapabilityReadiness.UNAVAILABLE
    assert health.module_readiness["l1"] is CapabilityReadiness.READY
    assert health.module_readiness["l2"] is CapabilityReadiness.UNAVAILABLE
