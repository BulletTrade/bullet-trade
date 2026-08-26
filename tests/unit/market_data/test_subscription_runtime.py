"""
作者: BruceLee

文件职责: 验证通用订阅 coordinator 从租约规划到真实 callback 的离线闭环。
主要输入: 内存 fake adapter、full/partial scope、延迟 ACK 和会话 epoch。
主要输出: desired/sent/confirmed、union/refcount、对账和恢复动作断言。
上游关系: 覆盖 bullet_trade.market_data.subscription_runtime 公共契约。
下游关系: 为 Mock Feed、远程 server 和未来厂商 adapter 提供同一安全门禁。
关键配置约定: 全部测试不联网、不加载 SDK、不执行交易。
"""

from typing import List

import pytest

from bullet_trade.market_data.models import MarketEventType
from bullet_trade.market_data.subscription_runtime import (
    AdapterSubscriptionResponse,
    AdapterSubscriptionResponseOutcome,
    InMemorySubscriptionActionAdapter,
    SubscriptionActionCoordinator,
)
from bullet_trade.market_data.subscriptions import (
    AdapterSubscriptionOperation,
    AdapterSubscriptionScope,
    StaleSubscriptionActionError,
    SubscriptionLeaseManager,
)

pytestmark = pytest.mark.unit


def _scope(symbol: str = "600000.XSHG") -> AdapterSubscriptionScope:
    """
    构造 L2 快照的 full 或单证券 adapter scope。

    Args:
        symbol: 标准证券代码；空字符串表示 full scope。

    Returns:
        AdapterSubscriptionScope: 可直接用于租约状态机的作用域。
    """
    return AdapterSubscriptionScope(
        module="l2",
        event_type=MarketEventType.SNAPSHOT_L2,
        market="XSHG",
        symbol=symbol or None,
    )


def _add(
    coordinator: SubscriptionActionCoordinator,
    session_id: str,
    subscription_id: str,
    scope: AdapterSubscriptionScope,
) -> None:
    """
    向 coordinator 注册单 scope 租约并立即调度。

    Args:
        coordinator: 待驱动的订阅协调器。
        session_id: 独立客户端会话 ID。
        subscription_id: 稳定租约 ID。
        scope: 该租约需要的底层 scope。

    Returns:
        None: 租约注册和调度完成后返回。
    """
    coordinator.add_lease(
        session_id=session_id,
        subscription_id=subscription_id,
        request_id=f"request-{subscription_id}",
        payload_fingerprint=f"fingerprint-{subscription_id}",
        scopes=(scope,),
    )


def test_local_acceptance_remains_sent_until_real_callback_confirms() -> None:
    """验证 fake 本地 accepted 不会被 coordinator 冒充为 confirmed。"""
    adapter = InMemorySubscriptionActionAdapter(auto_respond=False)
    coordinator = SubscriptionActionCoordinator(
        SubscriptionLeaseManager("epoch-1"),
        adapter,
    )
    scope = _scope()

    _add(coordinator, "session-a", "lease-a", scope)

    pending = coordinator.snapshot()
    assert pending.desired == frozenset({scope})
    assert pending.sent == frozenset({scope})
    assert pending.confirmed == frozenset()
    assert pending.pending_subscribe == frozenset({scope})

    adapter.respond_next()

    confirmed = coordinator.snapshot()
    assert confirmed.confirmed == frozenset({scope})
    assert confirmed.pending_subscribe == frozenset()


def test_cross_session_union_and_refcount_submit_each_scope_only_once() -> None:
    """验证两个 session 共享一个 scope 时只订阅一次且最后一个退订才下发。"""
    adapter = InMemorySubscriptionActionAdapter()
    coordinator = SubscriptionActionCoordinator(
        SubscriptionLeaseManager("epoch-1"),
        adapter,
    )
    scope = _scope()

    _add(coordinator, "session-a", "lease-a", scope)
    _add(coordinator, "session-b", "lease-b", scope)

    assert [action.operation for action in adapter.submitted_actions] == [
        AdapterSubscriptionOperation.SUBSCRIBE
    ]
    assert coordinator.snapshot().refcounts[scope] == 2

    coordinator.remove_lease("session-a", "lease-a")
    assert len(adapter.submitted_actions) == 1
    assert coordinator.snapshot().confirmed == frozenset({scope})

    coordinator.remove_lease("session-b", "lease-b")
    assert [action.operation for action in adapter.submitted_actions] == [
        AdapterSubscriptionOperation.SUBSCRIBE,
        AdapterSubscriptionOperation.UNSUBSCRIBE,
    ]
    assert coordinator.snapshot().confirmed == frozenset()


def test_partial_scope_removal_only_unsubscribes_removed_scope() -> None:
    """验证多 scope lease 的部分退订保留其余 confirmed 意图。"""
    adapter = InMemorySubscriptionActionAdapter()
    coordinator = SubscriptionActionCoordinator(
        SubscriptionLeaseManager("epoch-1"),
        adapter,
    )
    first = _scope("600000.XSHG")
    second = _scope("600001.XSHG")
    coordinator.add_lease(
        session_id="session-a",
        subscription_id="lease-a",
        request_id="request-lease-a",
        payload_fingerprint="fingerprint-lease-a",
        scopes=(first, second),
    )

    updated = coordinator.remove_scopes(
        "session-a",
        "lease-a",
        (first,),
    )

    assert updated.active_scopes == (second,)
    assert coordinator.snapshot().confirmed == frozenset({second})
    unsubscribe_actions = tuple(
        action
        for action in adapter.submitted_actions
        if action.operation is AdapterSubscriptionOperation.UNSUBSCRIBE
    )
    assert tuple(action.scope for action in unsubscribe_actions) == (first,)


def test_full_removal_materializes_partial_before_unsubscribing_full() -> None:
    """验证 full+partial 重叠时先确认 partial，再安全退订 full。"""
    adapter = InMemorySubscriptionActionAdapter(auto_respond=False)
    coordinator = SubscriptionActionCoordinator(
        SubscriptionLeaseManager("epoch-1"),
        adapter,
    )
    full = _scope("")
    partial = _scope("600000.XSHG")

    _add(coordinator, "session-a", "full-lease", full)
    adapter.respond_next()
    _add(coordinator, "session-b", "partial-lease", partial)
    assert len(adapter.submitted_actions) == 1

    coordinator.remove_lease("session-a", "full-lease")

    materialize = adapter.pending_actions
    assert len(materialize) == 1
    assert materialize[0].operation is AdapterSubscriptionOperation.SUBSCRIBE
    assert materialize[0].scope == partial
    assert full in coordinator.snapshot().confirmed

    adapter.respond_next()

    remove_full = adapter.pending_actions
    assert len(remove_full) == 1
    assert remove_full[0].operation is AdapterSubscriptionOperation.UNSUBSCRIBE
    assert remove_full[0].scope == full
    assert partial in coordinator.snapshot().confirmed

    adapter.respond_next()
    assert coordinator.snapshot().confirmed == frozenset({partial})


def test_ack_timeout_becomes_uncertain_and_late_callback_reconciles() -> None:
    """验证 accepted 后超时不重发，迟到 callback 通过 reconcile 落地。"""
    clock: List[float] = [10.0]
    adapter = InMemorySubscriptionActionAdapter(auto_respond=False)
    coordinator = SubscriptionActionCoordinator(
        SubscriptionLeaseManager("epoch-1"),
        adapter,
        ack_timeout_seconds=2.0,
        monotonic_clock=lambda: clock[0],
    )
    scope = _scope()
    _add(coordinator, "session-a", "lease-a", scope)

    clock[0] = 12.1
    expired = coordinator.expire_ack_timeouts()

    assert len(expired) == 1
    assert coordinator.snapshot().uncertain_subscribe == frozenset({scope})
    assert len(adapter.submitted_actions) == 1

    adapter.respond_next()

    snapshot = coordinator.snapshot()
    assert snapshot.uncertain_subscribe == frozenset()
    assert snapshot.confirmed == frozenset({scope})
    assert len(adapter.submitted_actions) == 1


def test_late_rejection_after_ack_timeout_requires_explicit_retry() -> None:
    """验证超时后的迟到拒绝保留厂商失败门闩，显式 retry 前绝不再次提交。"""
    clock: List[float] = [10.0]
    adapter = InMemorySubscriptionActionAdapter(auto_respond=False)
    coordinator = SubscriptionActionCoordinator(
        SubscriptionLeaseManager("epoch-1"),
        adapter,
        ack_timeout_seconds=2.0,
        monotonic_clock=lambda: clock[0],
    )
    scope = _scope()
    _add(coordinator, "session-a", "lease-a", scope)
    original = adapter.pending_actions[0]
    clock[0] = 12.1
    coordinator.expire_ack_timeouts()

    adapter.respond(
        original.action_id,
        outcome=AdapterSubscriptionResponseOutcome.REJECTED,
        code="VENDOR_PERMISSION_DENIED",
        reason="offline permission",
    )

    rejected = coordinator.snapshot()
    assert rejected.uncertain_subscribe == frozenset()
    assert rejected.confirmed == frozenset()
    assert rejected.failures[0].code == "VENDOR_PERMISSION_DENIED"
    assert rejected.failures[0].reason == "offline permission"
    assert coordinator.pump() == ()
    assert len(adapter.submitted_actions) == 1
    coordinator.handle_adapter_response(
        AdapterSubscriptionResponse(
            action=original,
            outcome=AdapterSubscriptionResponseOutcome.REJECTED,
            code="VENDOR_PERMISSION_DENIED",
            reason="offline permission",
        )
    )
    assert len(adapter.submitted_actions) == 1

    coordinator.retry_failed(AdapterSubscriptionOperation.SUBSCRIBE, scope)

    assert len(adapter.submitted_actions) == 2
    retry = adapter.pending_actions[0]
    assert retry.action_id != original.action_id
    adapter.respond(retry.action_id)
    assert coordinator.snapshot().confirmed == frozenset({scope})


def test_rejected_callback_is_not_confirmed_and_does_not_spin() -> None:
    """验证本地接受后的明确 rejected callback 保留失败门闩且不无界重试。"""

    def reject(action: object) -> AdapterSubscriptionResponse:
        """
        为任意 fake 动作生成稳定拒绝回调。

        Args:
            action: fake adapter 传入的底层动作。

        Returns:
            AdapterSubscriptionResponse: 对应动作的拒绝结果。
        """
        assert hasattr(action, "action_id")
        return AdapterSubscriptionResponse(
            action=action,  # type: ignore[arg-type]
            outcome=AdapterSubscriptionResponseOutcome.REJECTED,
            code="FAKE_DENIED",
            reason="offline_policy",
        )

    adapter = InMemorySubscriptionActionAdapter(response_factory=reject)
    coordinator = SubscriptionActionCoordinator(
        SubscriptionLeaseManager("epoch-1"),
        adapter,
    )
    scope = _scope()

    _add(coordinator, "session-a", "lease-a", scope)

    snapshot = coordinator.snapshot()
    assert snapshot.confirmed == frozenset()
    assert snapshot.failures[0].code == "FAKE_DENIED"
    assert len(adapter.submitted_actions) == 1


def test_new_epoch_replays_current_union_once_and_rejects_old_callback() -> None:
    """验证重连只恢复当前 union，旧 epoch callback fail closed。"""
    adapter = InMemorySubscriptionActionAdapter(auto_respond=False)
    coordinator = SubscriptionActionCoordinator(
        SubscriptionLeaseManager("epoch-1"),
        adapter,
    )
    scope = _scope()
    _add(coordinator, "session-a", "lease-a", scope)
    old_action = adapter.pending_actions[0]

    assert coordinator.begin_session_epoch("epoch-2") is True
    assert coordinator.snapshot().confirmed == frozenset()
    assert len(adapter.pending_actions) == 2

    with pytest.raises(StaleSubscriptionActionError):
        adapter.respond(old_action.action_id)

    new_action = adapter.pending_actions[0]
    assert new_action.session_epoch == "epoch-2"
    adapter.respond(new_action.action_id)
    assert coordinator.snapshot().confirmed == frozenset({scope})

    assert coordinator.begin_session_epoch("epoch-2") is False
    assert len(adapter.submitted_actions) == 2


def test_removed_during_disconnect_is_not_restored_in_new_epoch() -> None:
    """验证断线期间取消的 lease 不会被旧恢复任务重建。"""
    adapter = InMemorySubscriptionActionAdapter(auto_respond=False)
    coordinator = SubscriptionActionCoordinator(
        SubscriptionLeaseManager("epoch-1"),
        adapter,
    )
    scope = _scope()
    _add(coordinator, "session-a", "lease-a", scope)
    first_action = adapter.pending_actions[0]

    coordinator.remove_lease("session-a", "lease-a")
    coordinator.begin_session_epoch("epoch-2")

    snapshot = coordinator.snapshot()
    assert snapshot.desired == frozenset()
    assert snapshot.sent == frozenset()
    assert snapshot.confirmed == frozenset()
    assert len(adapter.submitted_actions) == 1
    with pytest.raises(StaleSubscriptionActionError):
        adapter.respond(first_action.action_id)
