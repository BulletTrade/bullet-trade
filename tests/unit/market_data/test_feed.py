"""
作者: BruceLee

文件职责: 验证无 SDK Mock 实时 Feed 的生命周期、订阅、门禁、callback 和缓存合同。
主要输入: 合成 CapabilityManifest、订阅 spec 与 typed/tick 事件。
主要输出: FeedHealth、逐项 receipt、callback 过滤和受控错误断言。
上游关系: 覆盖 bullet_trade.market_data.feed 的公共接口。
下游关系: 为未来 LiveEngine 与 Huaxin adapter 接入提供纯 Python 合同门禁。
关键配置约定: Mock 立即确认合法订阅；全市场 L2 默认关闭；测试不联网不加载 SDK。
"""

from datetime import datetime
from typing import Dict, List

import pytest

from bullet_trade.market_data import (
    CapabilityDeclaration,
    CapabilityManifest,
    CapabilityReadiness,
    CapabilitySupport,
    DepthSnapshotEvent,
    MarketDataLevel,
    MarketEvent,
    MarketEventType,
    MarketSubscriptionSpec,
    MockRealtimeMarketDataFeed,
    ProviderLocation,
    SubscriptionConflictError,
    SubscriptionSelector,
    SubscriptionState,
)
from bullet_trade.market_data.health import MarketUpdateExpectation

pytestmark = pytest.mark.unit


def _continuous_update_policy(
    security: str,
    level: MarketDataLevel,
    event: MarketEvent,
    raw_age_seconds: float,
) -> MarketUpdateExpectation:
    """
    为既有 Feed 合同测试声明持续交易中的显式更新窗口。

    Args:
        security: 当前标准证券代码。
        level: 当前精确行情级别。
        event: 最近一次受控 gateway 事件。
        raw_age_seconds: 事件到当前的单调时钟原始 age。

    Returns:
        MarketUpdateExpectation: 始终预期继续更新且无暂停时长的测试 policy。
    """
    del security, level, raw_age_seconds
    return MarketUpdateExpectation(
        expected=True,
        market_state="continuous_trading",
        effective_source_age_seconds=0.0 if event.exchange_time is not None else None,
    )


def _capability(
    capability_id: str,
    *,
    support: CapabilitySupport = CapabilitySupport.SUPPORTED,
    continuous: bool = False,
    full_market: bool = False,
) -> CapabilityDeclaration:
    """
    构造 Mock Feed 的实时 capability 声明。

    Args:
        capability_id: 原子实时能力 ID。
        support: 静态支持状态。
        continuous: 是否能满足逐笔连续性要求。
        full_market: 是否通过全市场门禁。

    Returns:
        CapabilityDeclaration: 初始 unavailable 的测试声明。
    """
    readiness = CapabilityReadiness.UNAVAILABLE
    return CapabilityDeclaration(
        capability_id=capability_id,
        semantic_class=capability_id,
        support=support,
        readiness=readiness,
        markets=("XSHG", "XSHE"),
        asset_types=("stock",),
        continuous=continuous,
        metadata={"full_market": full_market},
    )


def _feed(l2_full_market: bool = False) -> MockRealtimeMarketDataFeed:
    """
    创建尚未 connect 的能力完备 Mock Feed。

    Args:
        l2_full_market: 是否为 L2 快照和逐笔开放全市场门禁。

    Returns:
        MockRealtimeMarketDataFeed: 可由测试控制生命周期的实例。
    """
    declarations = {
        "realtime.stream.tick_compat": _capability("realtime.stream.tick_compat"),
        "realtime.snapshot.l1": _capability("realtime.snapshot.l1", full_market=True),
        "realtime.snapshot.l2": _capability(
            "realtime.snapshot.l2", continuous=True, full_market=l2_full_market
        ),
        "realtime.stream.transaction": _capability(
            "realtime.stream.transaction", continuous=True, full_market=l2_full_market
        ),
        "realtime.stream.order_detail": _capability(
            "realtime.stream.order_detail",
            support=CapabilitySupport.UNSUPPORTED,
            continuous=True,
            full_market=l2_full_market,
        ),
        "realtime.stream.market_status": _capability(
            "realtime.stream.market_status", full_market=True
        ),
    }
    manifest = CapabilityManifest(
        provider="mock",
        manifest_version="mock-v1",
        location=ProviderLocation.LOCAL,
        capabilities=declarations,
    )
    return MockRealtimeMarketDataFeed(
        manifest,
        limits={"max_symbols": 100},
        update_expectation_policy=_continuous_update_policy,
    )


def test_mock_feed_lifecycle_separates_support_and_readiness() -> None:
    """验证 connect/disconnect 只更新 readiness，静态 support 保持不变。"""
    feed = _feed()
    before = feed.health()
    assert before.connected is False
    assert before.capability_readiness["realtime.snapshot.l2"] is CapabilityReadiness.UNAVAILABLE

    feed.connect()
    first_epoch = feed.health().session_epoch
    assert feed.manifest.get("realtime.snapshot.l2").support is CapabilitySupport.SUPPORTED
    assert feed.manifest.get("realtime.snapshot.l2").readiness is CapabilityReadiness.UNAVAILABLE
    feed.subscribe(
        MarketSubscriptionSpec(
            request_id="lifecycle-l2",
            selector="symbols",
            symbols=("600000.XSHG",),
            level="l2",
            event_types=("snapshot_l2",),
        )
    )
    now = datetime.now()
    feed.publish_event(
        DepthSnapshotEvent(
            provider="mock",
            capability_key="realtime.snapshot.l2",
            event_type="snapshot_l2",
            level="l2",
            exchange="XSHG",
            session_epoch=first_epoch,
            security="600000.XSHG",
            raw_security_code="600000",
            payload={"last_price": 10.0},
            gateway_received_at=now,
            client_received_at=now,
            exchange_time=now,
        )
    )
    assert feed.manifest.get("realtime.snapshot.l2").readiness is CapabilityReadiness.READY

    feed.disconnect()
    assert feed.manifest.get("realtime.snapshot.l2").support is CapabilitySupport.SUPPORTED
    assert feed.manifest.get("realtime.snapshot.l2").readiness is CapabilityReadiness.UNAVAILABLE
    feed.connect()
    assert feed.health().session_epoch != first_epoch
    assert feed.health().reconnect_count == 1


def test_partial_subscription_is_idempotent_and_conflict_is_rejected() -> None:
    """验证 supported/unsupported 混合回执、幂等重放和 request 冲突。"""
    feed = _feed()
    feed.connect()
    spec = MarketSubscriptionSpec(
        request_id="partial-request",
        selector="symbols",
        symbols=("600000.XSHG",),
        level="l2",
        event_types=("snapshot_l2", "order_detail"),
    )

    receipt = feed.subscribe(spec)

    assert receipt.state is SubscriptionState.PARTIAL
    assert len(receipt.confirmed) == 1
    assert receipt.rejected[0].code == "UNSUPPORTED_EVENT_TYPE"
    assert feed.subscribe(spec).subscription_id == receipt.subscription_id

    canceled = feed.unsubscribe(receipt.subscription_id)
    assert canceled.state is SubscriptionState.CANCELED
    assert canceled.rejected[0].code == "UNSUPPORTED_EVENT_TYPE"

    conflict = MarketSubscriptionSpec(
        request_id=spec.request_id,
        selector="symbols",
        symbols=("600000.XSHG",),
        level="l2",
        event_types=("transaction",),
    )
    with pytest.raises(SubscriptionConflictError):
        feed.subscribe(conflict)


def test_full_market_gate_rejects_l2_and_wildcard_only_expands_allowed_events() -> None:
    """验证全市场 L2 不降级，L1 '*' 只回执协商且获准的实际事件。"""
    feed = _feed(l2_full_market=False)
    feed.connect()
    rejected = feed.subscribe(
        MarketSubscriptionSpec(
            request_id="full-l2",
            selector="markets",
            markets=("XSHG",),
            level="l2",
            event_types=("snapshot_l2",),
        )
    )
    assert rejected.state is SubscriptionState.REJECTED
    assert rejected.rejected[0].code == "FULL_MARKET_CAPABILITY_UNAVAILABLE"

    allowed = feed.subscribe(
        MarketSubscriptionSpec(
            request_id="all-l1",
            selector="all",
            level="l1",
            event_types=("*",),
        )
    )
    assert allowed.state is SubscriptionState.CONFIRMED
    assert set(allowed.actual_event_types) == {
        MarketEventType.MARKET_STATUS,
        MarketEventType.SNAPSHOT_L1,
    }
    assert allowed.effective_markets == ("XSHE", "XSHG")


def test_callbacks_and_snapshot_cache_only_receive_confirmed_scope() -> None:
    """验证未订阅证券不投递，已确认 tick/typed 快照可由兼容 API 读取。"""
    feed = _feed()
    feed.connect()
    tick_events: List[Dict[str, object]] = []
    typed_events: List[MarketEvent] = []
    feed.set_tick_callback(lambda tick: tick_events.append(dict(tick)))
    feed.set_market_event_callback(typed_events.append)
    feed.subscribe(
        MarketSubscriptionSpec(
            request_id="tick-subscription",
            selector="symbols",
            symbols=("600000.XSHG",),
            level="tick_compat",
            event_types=("tick_compat",),
        )
    )

    tick_time = datetime.now()
    assert (
        feed.publish_tick(
            "000001.XSHE",
            "XSHE",
            {"sid": "000001.XSHE", "last_price": 9.9},
            received_at=tick_time,
            exchange_time=tick_time,
        )
        is False
    )
    assert (
        feed.publish_tick(
            "600000.XSHG",
            "XSHG",
            {"sid": "600000.XSHG", "last_price": 10.1},
            received_at=tick_time,
            exchange_time=tick_time,
        )
        is True
    )
    assert tick_events == [{"sid": "600000.XSHG", "last_price": 10.1}]
    assert len(typed_events) == 1
    assert feed.get_current_tick("600000.XSHG")["last_price"] == 10.1

    feed.subscribe(
        MarketSubscriptionSpec(
            request_id="snapshot-subscription",
            selector="symbols",
            symbols=("600000.XSHG",),
            level="l2",
            event_types=("snapshot_l2",),
        )
    )
    now = datetime.now()
    snapshot = DepthSnapshotEvent(
        provider="mock",
        capability_key="realtime.snapshot.l2",
        event_type="snapshot_l2",
        level="l2",
        exchange="XSHG",
        session_epoch=feed.health().session_epoch,
        security="600000.XSHG",
        raw_security_code="600000",
        asset_type="stock",
        payload={"last_price": 10.2, "bid": [10.1]},
        gateway_received_at=now,
        client_received_at=now,
        exchange_time=now,
    )
    assert feed.publish_event(snapshot) is True
    assert feed.get_market_snapshot("600000.XSHG", MarketDataLevel.L2) is snapshot


def test_unsubscribe_all_filter_and_reconnect_preserve_only_active_leases() -> None:
    """验证过滤退订只取消目标 lease，重连仅恢复剩余 active lease 的新 epoch。"""
    feed = _feed()
    feed.connect()
    l1 = feed.subscribe(
        MarketSubscriptionSpec(
            request_id="l1-lease",
            selector=SubscriptionSelector.SYMBOLS,
            symbols=("600000.XSHG",),
            level=MarketDataLevel.L1,
            event_types=(MarketEventType.SNAPSHOT_L1,),
        )
    )
    l2 = feed.subscribe(
        MarketSubscriptionSpec(
            request_id="l2-lease",
            selector=SubscriptionSelector.SYMBOLS,
            symbols=("600000.XSHG",),
            level=MarketDataLevel.L2,
            event_types=(MarketEventType.TRANSACTION,),
            require_continuity=True,
        )
    )

    canceled = feed.unsubscribe_all(level=MarketDataLevel.L2)

    assert tuple(item.subscription_id for item in canceled) == (l2.subscription_id,)
    assert set(feed.health().active_subscriptions) == {l1.subscription_id}
    old_epoch = feed.health().session_epoch
    feed.disconnect()
    feed.connect()
    health = feed.health()
    assert health.session_epoch != old_epoch
    assert health.active_subscriptions[l1.subscription_id].session_epoch == health.session_epoch
