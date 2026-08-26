"""
作者: BruceLee

文件职责: 验证 MockRealtimeMarketDataFeed 经通用 coordinator 生成 receipt/health。
主要输入: 延迟或拒绝的内存 adapter、部分/full 订阅与重连 epoch。
主要输出: pending/confirmed/canceled 回执、health 一致性和底层动作顺序断言。
上游关系: 覆盖 feed.py 对 subscription_runtime.py 的最小端到端接线。
下游关系: 保证未来远程/厂商 adapter 不能以本地 accepted 伪造已确认。
关键配置约定: 全部测试纯内存，不联网、不加载 SDK、不交易。
"""

from datetime import datetime
from threading import Event, RLock, Thread
from typing import Dict, List, Tuple

import pytest

from bullet_trade.market_data import (
    CapabilityDeclaration,
    CapabilityManifest,
    CapabilityReadiness,
    CapabilitySupport,
    DepthSnapshotEvent,
    MarketControlDispatcherMetrics,
    MarketControlDispatchOutcome,
    MarketControlDispatchResult,
    MarketControlSink,
    MarketControlSinkAck,
    MarketDataLevel,
    MarketEventControlAckError,
    MarketEventControlDelivery,
    MarketEventControlDrainError,
    MarketEventRecoveryAuthorizationError,
    MarketEventType,
    MarketSubscriptionSpec,
    MockRealtimeMarketDataFeed,
    ProviderLocation,
    ReliableMarketControlDispatcher,
    SubscriptionCapacityError,
    SubscriptionItemState,
    SubscriptionState,
)
from bullet_trade.market_data.subscription_runtime import (
    AdapterSubscriptionResponse,
    AdapterSubscriptionResponseCallback,
    AdapterSubscriptionResponseOutcome,
    AdapterSubscriptionSubmitResult,
    InMemorySubscriptionActionAdapter,
    SubscriptionActionAdapter,
)
from bullet_trade.market_data.subscriptions import (
    AdapterSubscriptionAction,
    AdapterSubscriptionOperation,
    AdapterSubscriptionScope,
    StaleSubscriptionActionError,
)

pytestmark = pytest.mark.unit


class _ThreadCallbackAdapter(SubscriptionActionAdapter):
    """在独立线程同步完成 callback，用于确定性暴露 Feed 锁反转。"""

    def __init__(self) -> None:
        """
        初始化线程安全动作记录。

        Returns:
            None: 空动作列表和互斥锁初始化完成后返回。
        """
        self._lock = RLock()
        self._submitted: List[AdapterSubscriptionAction] = []

    @property
    def submitted_actions(self) -> Tuple[AdapterSubscriptionAction, ...]:
        """
        返回全部已提交动作的不可变快照。

        Returns:
            Tuple[AdapterSubscriptionAction, ...]: 按提交顺序排列的动作。
        """
        with self._lock:
            return tuple(self._submitted)

    def submit(
        self,
        action: AdapterSubscriptionAction,
        callback: AdapterSubscriptionResponseCallback,
    ) -> AdapterSubscriptionSubmitResult:
        """
        在另一线程调用 confirmed callback，并等待其真实返回后才接受提交。

        Args:
            action: coordinator 已 claim 的精确动作。
            callback: 需要在另一线程执行的状态回调。

        Returns:
            AdapterSubscriptionSubmitResult: callback 无异常完成后的本地 accepted。

        Raises:
            RuntimeError: callback 被 Feed 锁阻塞或自身抛出异常时 fail closed。
        """
        completed = Event()
        errors: List[Exception] = []

        def invoke_callback() -> None:
            """
            在线程边界内执行精确 callback 并记录完成或异常。

            Returns:
                None: callback 结果已写入共享测试状态后返回。
            """
            try:
                callback(
                    AdapterSubscriptionResponse(
                        action=action,
                        outcome=AdapterSubscriptionResponseOutcome.CONFIRMED,
                    )
                )
            except Exception as exc:  # pragma: no cover - 断言失败时仅用于传回主线程
                errors.append(exc)
            finally:
                completed.set()

        with self._lock:
            self._submitted.append(action)
        worker = Thread(target=invoke_callback, daemon=True)
        worker.start()
        if not completed.wait(timeout=2.0):
            raise RuntimeError("threaded subscription callback blocked")
        worker.join(timeout=2.0)
        if errors:
            raise RuntimeError("threaded subscription callback failed") from errors[0]
        return AdapterSubscriptionSubmitResult(accepted=True)


def _feed(
    adapter: SubscriptionActionAdapter,
    *,
    max_subscription_records: int = 1024,
    max_subscription_items_per_request: int = 4096,
) -> MockRealtimeMarketDataFeed:
    """
    构造允许沪深 full-market L1/L2 快照的离线 Feed。

    Args:
        adapter: 测试控制的内存订阅 adapter。
        max_subscription_records: Feed 本地请求与墓碑总数硬上限。
        max_subscription_items_per_request: 单请求展开 items 硬上限。

    Returns:
        MockRealtimeMarketDataFeed: 尚未连接的线程安全 Feed。
    """
    declarations = {}
    for capability_id in (
        "realtime.snapshot.l1",
        "realtime.snapshot.l2",
        "realtime.stream.transaction",
    ):
        declarations[capability_id] = CapabilityDeclaration(
            capability_id=capability_id,
            semantic_class=capability_id,
            support=CapabilitySupport.SUPPORTED,
            readiness=CapabilityReadiness.UNAVAILABLE,
            markets=("XSHG", "XSHE"),
            asset_types=("stock",),
            continuous=capability_id == "realtime.stream.transaction",
            metadata={"full_market": True},
        )
    return MockRealtimeMarketDataFeed(
        CapabilityManifest(
            provider="mock",
            manifest_version="subscription-runtime-v1",
            location=ProviderLocation.LOCAL,
            capabilities=declarations,
        ),
        subscription_adapter=adapter,
        max_subscription_records=max_subscription_records,
        max_subscription_items_per_request=max_subscription_items_per_request,
    )


def _symbol_spec(request_id: str = "symbol-l2") -> MarketSubscriptionSpec:
    """
    构造单证券 L2 快照订阅。

    Args:
        request_id: 用于幂等与冲突检查的请求 ID。

    Returns:
        MarketSubscriptionSpec: 600000.XSHG 的规范化 spec。
    """
    return MarketSubscriptionSpec(
        request_id=request_id,
        selector="symbols",
        symbols=("600000.XSHG",),
        level="l2",
        event_types=("snapshot_l2",),
    )


def test_feed_accepted_receipt_and_health_remain_pending_until_callback() -> None:
    """验证 Feed 的 public receipt 与 health 共同反映 sent 而非伪 confirmed。"""
    adapter = InMemorySubscriptionActionAdapter(auto_respond=False)
    feed = _feed(adapter)
    feed.connect()
    spec = _symbol_spec()

    pending = feed.subscribe(spec)

    assert pending.state is SubscriptionState.PENDING
    assert pending.items[0].state is SubscriptionItemState.SENT
    assert pending.confirmed == ()
    assert feed.health().active_subscriptions[pending.subscription_id] == pending
    assert (
        feed.health().capability_readiness["realtime.snapshot.l2"]
        is CapabilityReadiness.UNAVAILABLE
    )

    adapter.respond_next()

    confirmed = feed.subscribe(spec)
    assert confirmed.state is SubscriptionState.CONFIRMED
    assert feed.health().active_subscriptions[confirmed.subscription_id] == confirmed


def test_feed_rejected_callback_stays_rejected_and_not_deliverable() -> None:
    """验证已接受请求的真实拒绝回调进入逐项 rejected 而非 confirmed。"""

    def reject(action: AdapterSubscriptionAction) -> AdapterSubscriptionResponse:
        """
        为单个订阅动作同步产生离线拒绝回调。

        Args:
            action: coordinator 已 claim 的精确动作。

        Returns:
            AdapterSubscriptionResponse: 稳定错误码的 rejected 回调。
        """
        return AdapterSubscriptionResponse(
            action=action,
            outcome=AdapterSubscriptionResponseOutcome.REJECTED,
            code="FAKE_PERMISSION_DENIED",
        )

    adapter = InMemorySubscriptionActionAdapter(response_factory=reject)
    feed = _feed(adapter)
    feed.connect()

    receipt = feed.subscribe(_symbol_spec())

    assert receipt.state is SubscriptionState.REJECTED
    assert receipt.rejected[0].code == "FAKE_PERMISSION_DENIED"
    assert feed.health().active_subscriptions[receipt.subscription_id] == receipt


def test_feed_unsubscribe_is_pending_until_unsubscribe_callback() -> None:
    """验证退订本地 accepted 时不冒充 confirmed-empty，重复调用保持幂等。"""
    adapter = InMemorySubscriptionActionAdapter(auto_respond=False)
    feed = _feed(adapter)
    feed.connect()
    subscribed = feed.subscribe(_symbol_spec())
    adapter.respond_next()

    pending = feed.unsubscribe(subscribed.subscription_id)

    assert pending.state is SubscriptionState.PENDING
    assert pending.items[0].state is SubscriptionItemState.PENDING
    assert feed.unsubscribe(subscribed.subscription_id) == pending
    assert subscribed.subscription_id not in feed.health().active_subscriptions
    action = adapter.pending_actions[0]
    assert action.operation is AdapterSubscriptionOperation.UNSUBSCRIBE

    adapter.respond_next()

    canceled = feed.unsubscribe(subscribed.subscription_id)
    assert canceled.state is SubscriptionState.CANCELED
    assert canceled.items[0].state is SubscriptionItemState.CANCELED


def test_feed_reconnect_sets_pending_and_old_epoch_callback_fails_closed() -> None:
    """验证重连立即撤销旧 confirmed 证据，且迟到旧 callback 不能污染新回执。"""
    adapter = InMemorySubscriptionActionAdapter(auto_respond=False)
    feed = _feed(adapter)
    feed.connect()
    spec = _symbol_spec()
    first = feed.subscribe(spec)
    old_action = adapter.pending_actions[0]
    adapter.respond(old_action.action_id)
    assert feed.subscribe(spec).state is SubscriptionState.CONFIRMED
    old_epoch = first.session_epoch

    feed.disconnect()
    disconnected = feed.health().active_subscriptions[first.subscription_id]
    assert disconnected.state is SubscriptionState.PENDING
    assert disconnected.session_epoch.startswith("mock-disconnected-")
    with pytest.raises(StaleSubscriptionActionError):
        feed._subscription_coordinator.handle_adapter_response(  # pylint: disable=protected-access
            AdapterSubscriptionResponse(
                action=old_action,
                outcome=AdapterSubscriptionResponseOutcome.CONFIRMED,
            )
        )
    assert (
        feed.health().active_subscriptions[first.subscription_id].state is SubscriptionState.PENDING
    )

    feed.connect()

    restored = feed.subscribe(spec)
    assert restored.session_epoch != old_epoch
    assert restored.state is SubscriptionState.PENDING
    new_action = adapter.pending_actions[0]
    assert new_action.session_epoch == restored.session_epoch

    with pytest.raises(StaleSubscriptionActionError):
        feed._subscription_coordinator.handle_adapter_response(  # pylint: disable=protected-access
            AdapterSubscriptionResponse(
                action=old_action,
                outcome=AdapterSubscriptionResponseOutcome.CONFIRMED,
            )
        )

    adapter.respond(new_action.action_id)
    assert feed.subscribe(spec).state is SubscriptionState.CONFIRMED


def test_feed_canceled_while_disconnected_is_not_restored() -> None:
    """验证断线期间移除 desired 的 lease 在新 epoch 变 canceled 且不会重订阅。"""
    adapter = InMemorySubscriptionActionAdapter(auto_respond=False)
    feed = _feed(adapter)
    feed.connect()
    subscribed = feed.subscribe(_symbol_spec())
    old_action = adapter.pending_actions[0]

    feed.disconnect()
    disconnected_cancel = feed.unsubscribe(subscribed.subscription_id)
    assert disconnected_cancel.state is SubscriptionState.CANCELED
    feed.connect()

    canceled = feed.unsubscribe(subscribed.subscription_id)
    assert canceled.state is SubscriptionState.CANCELED
    assert len(adapter.submitted_actions) == 1
    assert subscribed.subscription_id not in feed.health().active_subscriptions
    with pytest.raises(StaleSubscriptionActionError):
        adapter.respond(old_action.action_id)


def test_feed_adapter_submit_exception_stays_uncertain_then_retries_after_reconcile() -> None:
    """验证 adapter pump 异常不丢 lease，对账确定未生效后以新 action 幂等恢复。"""
    calls = {"count": 0}

    def fail_once(action: AdapterSubscriptionAction) -> AdapterSubscriptionResponse:
        """
        首次提交模拟结果不确定异常，第二次产生真实 confirmed callback。

        Args:
            action: coordinator 已 claim 的精确订阅动作。

        Returns:
            AdapterSubscriptionResponse: 第二次提交的确认回调。

        Raises:
            RuntimeError: 首次模拟 adapter 调用结果不确定。
        """
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("offline submit result unknown")
        return AdapterSubscriptionResponse(
            action=action,
            outcome=AdapterSubscriptionResponseOutcome.CONFIRMED,
        )

    adapter = InMemorySubscriptionActionAdapter(response_factory=fail_once)
    feed = _feed(adapter)
    feed.connect()
    spec = _symbol_spec("submit-exception")

    uncertain = feed.subscribe(spec)

    assert uncertain.state is SubscriptionState.PENDING
    assert uncertain.items[0].code == "ACK_RESULT_UNCERTAIN"
    assert feed.subscribe(spec) == uncertain
    assert len(adapter.submitted_actions) == 1

    feed.reconcile_subscription_action(
        adapter.submitted_actions[0].action_id,
        applied=False,
        reason="offline_query_not_applied",
    )

    recovered = feed.subscribe(spec)
    assert recovered.subscription_id == uncertain.subscription_id
    assert recovered.state is SubscriptionState.CONFIRMED
    assert len(adapter.submitted_actions) == 2


def test_feed_full_removal_waits_for_partial_materialization_and_full_ack() -> None:
    """验证 Feed 的 full 退订回执覆盖先 partial 后 full-unsubscribe 的两阶段闭环。"""
    adapter = InMemorySubscriptionActionAdapter(auto_respond=False)
    feed = _feed(adapter)
    feed.connect()
    full = feed.subscribe(
        MarketSubscriptionSpec(
            request_id="full-xshg",
            selector="markets",
            markets=("XSHG",),
            level=MarketDataLevel.L2,
            event_types=(MarketEventType.SNAPSHOT_L2,),
        )
    )
    adapter.respond_next()
    partial = feed.subscribe(_symbol_spec("partial-xshg"))
    assert partial.state is SubscriptionState.CONFIRMED
    assert len(adapter.submitted_actions) == 1

    canceling_full = feed.unsubscribe(full.subscription_id)

    assert canceling_full.state is SubscriptionState.PENDING
    materialize = adapter.pending_actions[0]
    assert materialize.operation is AdapterSubscriptionOperation.SUBSCRIBE
    assert materialize.scope.symbol == "600000.XSHG"

    adapter.respond_next()
    remove_full = adapter.pending_actions[0]
    assert remove_full.operation is AdapterSubscriptionOperation.UNSUBSCRIBE
    assert remove_full.scope.symbol is None
    assert feed.unsubscribe(full.subscription_id).state is SubscriptionState.PENDING

    adapter.respond_next()
    assert feed.unsubscribe(full.subscription_id).state is SubscriptionState.CANCELED
    assert feed.subscribe(_symbol_spec("partial-xshg")).state is SubscriptionState.CONFIRMED


def test_feed_threaded_callbacks_never_run_under_feed_lock() -> None:
    """验证连接恢复、订阅和退订的跨线程 callback 均不会与 Feed 锁互锁。"""
    adapter = _ThreadCallbackAdapter()
    feed = _feed(adapter)
    feed.connect()

    first = feed.subscribe(_symbol_spec("thread-first"))
    second = feed.subscribe(
        MarketSubscriptionSpec(
            request_id="thread-second",
            selector="symbols",
            symbols=("600001.XSHG",),
            level="l2",
            event_types=("snapshot_l2",),
        )
    )

    assert first.state is SubscriptionState.CONFIRMED
    assert second.state is SubscriptionState.CONFIRMED
    feed.disconnect()
    feed.connect()
    assert feed.subscribe(_symbol_spec("thread-first")).state is SubscriptionState.CONFIRMED
    assert feed.unsubscribe(first.subscription_id).state is SubscriptionState.CANCELED
    canceled = feed.unsubscribe_all()
    assert tuple(item.subscription_id for item in canceled) == (second.subscription_id,)
    assert canceled[0].state is SubscriptionState.CANCELED


def test_feed_all_selector_exposes_per_market_partial_and_filters_delivery() -> None:
    """验证 ALL 按实际市场确认，沪市确认深市拒绝时公开 partial 且只投递沪市。"""
    adapter = InMemorySubscriptionActionAdapter(auto_respond=False)
    feed = _feed(adapter)
    feed.connect()
    spec = MarketSubscriptionSpec(
        request_id="all-l2-snapshot",
        selector="all",
        level="l2",
        event_types=("snapshot_l2",),
    )

    pending = feed.subscribe(spec)
    actions = adapter.pending_actions
    actions_by_market = {action.scope.market: action for action in actions}

    assert pending.state is SubscriptionState.PENDING
    assert set(actions_by_market) == {"XSHG", "XSHE"}
    adapter.respond(actions_by_market["XSHG"].action_id)
    adapter.respond(
        actions_by_market["XSHE"].action_id,
        outcome=AdapterSubscriptionResponseOutcome.REJECTED,
        code="FAKE_XSHE_DENIED",
        reason="offline market permission",
    )

    partial = feed.subscribe(spec)
    assert partial.state is SubscriptionState.PARTIAL
    assert tuple(item.scope for item in partial.confirmed) == ("XSHG",)
    assert tuple(item.scope for item in partial.rejected) == ("XSHE",)
    assert partial.rejected[0].code == "FAKE_XSHE_DENIED"
    now = datetime.now()
    epoch = feed.health().session_epoch
    assert epoch is not None
    assert (
        feed.publish_event(
            DepthSnapshotEvent(
                provider="mock",
                capability_key="realtime.snapshot.l2",
                event_type="snapshot_l2",
                level="l2",
                exchange="XSHG",
                session_epoch=epoch,
                security="600000.XSHG",
                raw_security_code="600000",
                asset_type="stock",
                payload={"last_price": 10.0},
                gateway_received_at=now,
                client_received_at=now,
                exchange_time=now,
            )
        )
        is True
    )
    assert (
        feed.publish_event(
            DepthSnapshotEvent(
                provider="mock",
                capability_key="realtime.snapshot.l2",
                event_type="snapshot_l2",
                level="l2",
                exchange="XSHE",
                session_epoch=epoch,
                security="000001.XSHE",
                raw_security_code="000001",
                asset_type="stock",
                payload={"last_price": 11.0},
                gateway_received_at=now,
                client_received_at=now,
                exchange_time=now,
            )
        )
        is False
    )


def test_feed_retries_rejected_symbol_and_full_scope_only_when_explicit() -> None:
    """验证普通与 full 拒绝都保持门闩，只有显式 retry_failed 才产生新动作。"""
    attempts: Dict[AdapterSubscriptionScope, int] = {}

    def reject_once(action: AdapterSubscriptionAction) -> AdapterSubscriptionResponse:
        """
        对每个精确 scope 首次拒绝、显式重试后的第二次确认。

        Args:
            action: coordinator 当前 claim 的订阅动作。

        Returns:
            AdapterSubscriptionResponse: 首次 rejected、后续 confirmed 的 fake callback。
        """
        attempts[action.scope] = attempts.get(action.scope, 0) + 1
        if attempts[action.scope] == 1:
            return AdapterSubscriptionResponse(
                action=action,
                outcome=AdapterSubscriptionResponseOutcome.REJECTED,
                code="FAKE_RETRY_REQUIRED",
                reason="offline retry gate",
            )
        return AdapterSubscriptionResponse(
            action=action,
            outcome=AdapterSubscriptionResponseOutcome.CONFIRMED,
        )

    adapter = InMemorySubscriptionActionAdapter(response_factory=reject_once)
    feed = _feed(adapter)
    feed.connect()
    specs = (
        _symbol_spec("retry-symbol"),
        MarketSubscriptionSpec(
            request_id="retry-full",
            selector="markets",
            markets=("XSHG",),
            level="l2",
            event_types=("snapshot_l2",),
        ),
    )

    for spec in specs:
        rejected = feed.subscribe(spec)
        submitted_before_retry = len(adapter.submitted_actions)
        rejected_scope = adapter.submitted_actions[-1].scope

        assert rejected.state is SubscriptionState.REJECTED
        assert rejected.rejected[0].code == "FAKE_RETRY_REQUIRED"
        assert feed.subscribe(spec) == rejected
        assert len(adapter.submitted_actions) == submitted_before_retry

        confirmed = feed.retry_failed(rejected.subscription_id)

        assert confirmed.state is SubscriptionState.CONFIRMED
        assert len(adapter.submitted_actions) > submitted_before_retry
        assert attempts[rejected_scope] == 2


def test_feed_local_tombstone_and_item_caps_are_hard_and_close_reclaims_capacity() -> None:
    """验证本地拒绝墓碑与逐项展开均有硬上限，幂等读取和显式 close 安全回收。"""
    adapter = InMemorySubscriptionActionAdapter()
    feed = _feed(
        adapter,
        max_subscription_records=2,
        max_subscription_items_per_request=2,
    )
    feed.connect()

    def unsupported(request_id: str) -> MarketSubscriptionSpec:
        """
        构造不进入 manager 的单项本地拒绝请求。

        Args:
            request_id: 唯一幂等请求标识。

        Returns:
            MarketSubscriptionSpec: 缺少 capability 声明的 order_detail 订阅。
        """
        return MarketSubscriptionSpec(
            request_id=request_id,
            selector="symbols",
            symbols=("600000.XSHG",),
            level="l2",
            event_types=("order_detail",),
        )

    first = feed.subscribe(unsupported("unsupported-1"))
    feed.subscribe(unsupported("unsupported-2"))

    assert first.state is SubscriptionState.REJECTED
    assert feed.subscribe(unsupported("unsupported-1")) == first
    with pytest.raises(SubscriptionCapacityError, match="SUBSCRIPTION_RECORD_LIMIT"):
        feed.subscribe(unsupported("unsupported-3"))

    feed.close_subscription_session()

    assert feed.health().active_subscriptions == {}
    third = feed.subscribe(unsupported("unsupported-3"))
    assert third.state is SubscriptionState.REJECTED
    with pytest.raises(SubscriptionCapacityError, match="SUBSCRIPTION_ITEM_LIMIT"):
        feed.subscribe(
            MarketSubscriptionSpec(
                request_id="oversized-items",
                selector="symbols",
                symbols=("600000.XSHG", "600001.XSHG", "600002.XSHG"),
                level="l2",
                event_types=("snapshot_l2",),
            )
        )
    accepted = feed.subscribe(
        MarketSubscriptionSpec(
            request_id="within-item-cap",
            selector="symbols",
            symbols=("600000.XSHG", "600001.XSHG"),
            level="l2",
            event_types=("snapshot_l2",),
        )
    )
    assert accepted.state is SubscriptionState.CONFIRMED


def test_market_data_package_root_exports_reliable_control_contracts() -> None:
    """验证订阅运行时旁路不会遗漏可靠控制通道的稳定包根导出。"""
    exported = (
        MarketControlDispatchOutcome,
        MarketControlDispatchResult,
        MarketControlDispatcherMetrics,
        MarketControlSink,
        MarketControlSinkAck,
        ReliableMarketControlDispatcher,
        MarketEventControlAckError,
        MarketEventControlDrainError,
        MarketEventControlDelivery,
        MarketEventRecoveryAuthorizationError,
    )
    assert all(item.__name__ for item in exported)
