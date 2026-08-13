"""
作者: BruceLee

文件职责: 验证实时行情 session lease、adapter union/refcount 和确认状态机。
主要输入: 脱敏的 full/partial 市场作用域、合成 session 与底层成功/失败回执。
主要输出: desired/sent/confirmed、引用计数、安全转换动作和重连恢复断言。
上游关系: 覆盖 bullet_trade.market_data.subscriptions 纯 Python 公共合同。
下游关系: 为未来远程 server、Huaxin adapter 和 Feed receipt 接入提供回归门禁。
关键配置约定: 全部测试离线运行，不联网、不加载厂商 SDK、不执行交易。
"""

from typing import Optional, Sequence, Tuple

import pytest

from bullet_trade.market_data.models import MarketEventType
from bullet_trade.market_data.subscriptions import (
    AdapterSubscriptionAction,
    AdapterSubscriptionOperation,
    AdapterSubscriptionScope,
    StaleSubscriptionActionError,
    SubscriptionLeaseConflictError,
    SubscriptionLeaseManager,
    SubscriptionLeaseNotFoundError,
)

pytestmark = pytest.mark.unit


def _scope(
    symbol: Optional[str],
    event_type: MarketEventType = MarketEventType.TRANSACTION,
    module: str = "l2",
    market: str = "XSHG",
) -> AdapterSubscriptionScope:
    """
    构造一个脱敏 full 或 partial adapter 作用域。

    Args:
        symbol: 标准证券代码；None 表示当前市场 full scope。
        event_type: 已展开的实际行情事件类型。
        module: 底层行情模块标识。
        market: 标准市场代码。

    Returns:
        AdapterSubscriptionScope: 可供状态机合并计数的不可变作用域。
    """
    return AdapterSubscriptionScope(
        module=module,
        event_type=event_type,
        market=market,
        symbol=symbol,
    )


def _take_single(
    manager: SubscriptionLeaseManager,
    operation: AdapterSubscriptionOperation,
) -> AdapterSubscriptionAction:
    """
    取出并验证状态机此轮只规划了一个指定动作。

    Args:
        manager: 待验证的订阅租约状态机。
        operation: 期望的 subscribe 或 unsubscribe 类型。

    Returns:
        AdapterSubscriptionAction: 唯一的已发送待回执动作。
    """
    actions = manager.take_actions()
    assert len(actions) == 1
    assert actions[0].operation is operation
    return actions[0]


def _confirm_actions(
    manager: SubscriptionLeaseManager,
    actions: Sequence[AdapterSubscriptionAction],
) -> None:
    """
    逐项确认一批已发送的底层动作。

    Args:
        manager: 动作所属的订阅租约状态机。
        actions: 由同一状态机 take_actions 生成的动作序列。

    Returns:
        None: 所有动作幂等确认后返回。
    """
    for action in actions:
        manager.confirm_action(action)


def _add_lease(
    manager: SubscriptionLeaseManager,
    session_id: str,
    subscription_id: str,
    scopes: Sequence[AdapterSubscriptionScope],
    request_id: Optional[str] = None,
    fingerprint: Optional[str] = None,
) -> None:
    """
    为测试状态机新增一个带稳定请求和指纹的 session lease。

    Args:
        manager: 待增加租约的状态机。
        session_id: 脱敏 session 标识。
        subscription_id: 服务端订阅 ID。
        scopes: 租约已展开的 adapter 作用域。
        request_id: 可选幂等请求 ID；默认由 subscription_id 生成。
        fingerprint: 可选语义指纹；默认由 subscription_id 生成。

    Returns:
        None: 租约添加完成后返回。
    """
    manager.add_lease(
        session_id=session_id,
        subscription_id=subscription_id,
        request_id=request_id or f"request-{subscription_id}",
        payload_fingerprint=fingerprint or f"fingerprint-{subscription_id}",
        scopes=scopes,
    )


def test_duplicate_leases_share_one_adapter_subscription_and_refcount() -> None:
    """
    验证跨 session 重复 partial lease 只订阅一次，最后一个引用移除才退订。

    Returns:
        None: union/refcount、幂等回执和最终空集合断言全部通过后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    partial = _scope("600000.XSHG")
    _add_lease(manager, "session-a", "sub-a", (partial,))
    _add_lease(manager, "session-b", "sub-b", (partial,))

    subscribe = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert subscribe.scope == partial
    assert manager.take_actions() == ()
    manager.confirm_action(subscribe)
    manager.confirm_action(subscribe)

    snapshot = manager.snapshot()
    assert snapshot.refcounts[partial] == 2
    assert snapshot.desired == frozenset({partial})
    assert snapshot.sent == snapshot.confirmed == frozenset({partial})

    manager.remove_lease("session-a", "sub-a")
    assert manager.take_actions() == ()
    assert manager.snapshot().refcounts[partial] == 1

    manager.remove_lease("session-b", "sub-b")
    unsubscribe = _take_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)
    assert unsubscribe.scope == partial
    manager.confirm_action(unsubscribe)
    assert manager.snapshot().desired == frozenset()
    assert manager.snapshot().sent == manager.snapshot().confirmed == frozenset()


def test_same_request_is_idempotent_but_changed_payload_conflicts() -> None:
    """
    验证同 session/request/fingerprint 返回原租约，载荷改变时 fail closed。

    Returns:
        None: 原 subscription ID 保留且冲突异常断言通过后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    first_scope = _scope("600000.XSHG")
    first = manager.add_lease(
        "session-a",
        "subscription-original",
        "request-stable",
        "fingerprint-a",
        (first_scope,),
    )
    replay = manager.add_lease(
        "session-a",
        "subscription-retry-placeholder",
        "request-stable",
        "fingerprint-a",
        (first_scope,),
    )

    assert replay is first
    assert replay.subscription_id == "subscription-original"
    with pytest.raises(SubscriptionLeaseConflictError, match="REQUEST_CONFLICT"):
        manager.add_lease(
            "session-a",
            "subscription-conflict",
            "request-stable",
            "fingerprint-b",
            (_scope("600001.XSHG"),),
        )


def test_full_removal_materializes_remaining_partial_before_unsubscribe() -> None:
    """
    验证 full+partial 重叠时不重复订阅，退 full 前必须先确认物化 partial。

    Returns:
        None: 两阶段 full→partial 动作顺序与最终集合断言通过后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    full = _scope(None)
    partial = _scope("600000.XSHG")
    _add_lease(manager, "session-full", "full-lease", (full,))
    full_subscribe = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    manager.confirm_action(full_subscribe)

    _add_lease(manager, "session-partial", "partial-lease", (partial,))
    assert manager.take_actions() == ()
    assert manager.is_lease_confirmed("session-partial", "partial-lease") is True
    snapshot = manager.snapshot()
    assert snapshot.desired == frozenset({full, partial})
    assert snapshot.effective_desired == frozenset({full})

    manager.remove_lease("session-full", "full-lease")
    materialize = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert materialize.scope == partial
    assert materialize.reason == "materialize_partial_before_full_unsubscribe"
    assert manager.snapshot().confirmed == frozenset({full})
    assert manager.take_actions() == ()

    manager.confirm_action(materialize)
    remove_full = _take_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)
    assert remove_full.scope == full
    assert remove_full.reason == "remove_full_after_partial_materialized"
    manager.confirm_action(remove_full)
    assert manager.snapshot().confirmed == frozenset({partial})
    assert manager.is_lease_confirmed("session-partial", "partial-lease") is True


def test_full_promotion_waits_for_confirmation_before_releasing_partial() -> None:
    """
    验证 partial→full 时也先确认 full，再释放被覆盖的底层 partial。

    Returns:
        None: 升级 full 时无可避免窗口且 partial 逻辑租约仍 confirmed 后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    partial = _scope("600000.XSHG")
    full = _scope(None)
    _add_lease(manager, "session-partial", "partial-lease", (partial,))
    partial_subscribe = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    manager.confirm_action(partial_subscribe)

    _add_lease(manager, "session-full", "full-lease", (full,))
    promote = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert promote.scope == full
    assert promote.reason == "confirm_full_before_partial_unsubscribe"
    assert manager.take_actions() == ()

    manager.confirm_action(promote)
    remove_partial = _take_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)
    assert remove_partial.scope == partial
    assert remove_partial.reason == "remove_partial_after_full_confirmed"
    manager.confirm_action(remove_partial)
    assert manager.snapshot().confirmed == frozenset({full})
    assert manager.is_lease_confirmed("session-partial", "partial-lease") is True


def test_partial_and_filtered_unsubscribe_preserve_other_sessions() -> None:
    """
    验证租约内部分退订和 session 级 unsubscribe_all 不破坏其他客户端引用。

    Returns:
        None: 局部作用域、过滤清空与共享 refcount 断言通过后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    first = _scope("600000.XSHG")
    second = _scope("600001.XSHG")
    snapshot_scope = _scope(
        "600002.XSHG",
        event_type=MarketEventType.SNAPSHOT_L2,
    )
    _add_lease(manager, "session-a", "multi-lease", (first, second, snapshot_scope))
    _add_lease(manager, "session-b", "shared-lease", (first,))
    _confirm_actions(manager, manager.take_actions())

    updated = manager.remove_scopes("session-a", "multi-lease", (first, second))
    assert updated.active_scopes == (snapshot_scope,)
    actions = manager.take_actions()
    assert len(actions) == 1
    assert actions[0].operation is AdapterSubscriptionOperation.UNSUBSCRIBE
    assert actions[0].scope == second
    manager.confirm_action(actions[0])
    assert manager.snapshot().refcounts[first] == 1
    assert first in manager.snapshot().confirmed

    changed = manager.remove_all(
        "session-a",
        module="l2",
        event_types=(MarketEventType.SNAPSHOT_L2,),
    )
    assert tuple(item.subscription_id for item in changed) == ("multi-lease",)
    remove_snapshot = _take_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)
    assert remove_snapshot.scope == snapshot_scope
    manager.confirm_action(remove_snapshot)
    assert manager.snapshot().confirmed == frozenset({first})

    manager.remove_all("session-b")
    remove_shared = _take_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)
    assert remove_shared.scope == first
    manager.confirm_action(remove_shared)
    assert manager.snapshot().confirmed == frozenset()


def test_unsubscribe_rejects_unknown_scope_instead_of_clearing_all() -> None:
    """
    验证部分退订必须明确命中租约作用域，不会将缺失目标解释为全退。

    Returns:
        None: 受控 not-found 异常与原 desired 不变断言通过后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    existing = _scope("600000.XSHG")
    _add_lease(manager, "session-a", "lease-a", (existing,))

    with pytest.raises(SubscriptionLeaseNotFoundError, match="SCOPE_NOT_FOUND"):
        manager.remove_scopes(
            "session-a",
            "lease-a",
            (_scope("600001.XSHG"),),
        )
    assert manager.snapshot().desired == frozenset({existing})


def test_rejected_materialization_keeps_full_until_explicit_retry_succeeds() -> None:
    """
    验证 partial 物化失败后 full 保持 confirmed，仅显式重试成功才允许退 full。

    Returns:
        None: 失败门闩、无界重试防护和覆盖安全断言通过后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    full = _scope(None)
    partial = _scope("600000.XSHG")
    _add_lease(manager, "session-full", "full-lease", (full,))
    manager.confirm_action(_take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE))
    _add_lease(manager, "session-partial", "partial-lease", (partial,))
    manager.remove_lease("session-full", "full-lease")

    rejected = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    manager.reject_action(rejected, "VENDOR_PERMISSION_DENIED", "mock rejection")
    snapshot = manager.snapshot()
    assert snapshot.confirmed == frozenset({full})
    assert snapshot.sent == frozenset({full, partial})
    assert snapshot.failures[0].code == "VENDOR_PERMISSION_DENIED"
    assert manager.take_actions() == ()

    manager.retry_failed(AdapterSubscriptionOperation.SUBSCRIBE, partial)
    retried = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert retried.action_id != rejected.action_id
    manager.confirm_action(retried)
    remove_full = _take_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)
    assert remove_full.scope == full


def test_reconnect_replays_effective_desired_once_and_ignores_canceled_lease() -> None:
    """
    验证新 epoch 只恢复当前有效的最小 adapter union，同 epoch 不重复发送。

    Returns:
        None: epoch 清空三集合、幂等恢复、断线期退订与迟到回执断言通过后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    full = _scope(None)
    partial = _scope("600000.XSHG")
    _add_lease(manager, "session-full", "full-lease", (full,))
    _add_lease(manager, "session-partial", "partial-lease", (partial,))
    initial = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert initial.scope == full
    manager.confirm_action(initial)

    assert manager.begin_session_epoch("epoch-2") is True
    assert manager.snapshot().sent == manager.snapshot().confirmed == frozenset()
    replay = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert replay.scope == full
    assert manager.take_actions() == ()
    assert manager.begin_session_epoch("epoch-2") is False
    manager.confirm_action(replay)
    assert manager.is_lease_confirmed("session-partial", "partial-lease") is True

    assert manager.begin_session_epoch("epoch-3") is True
    manager.remove_all("session-full")
    manager.remove_all("session-partial")
    assert manager.take_actions() == ()
    assert manager.snapshot().desired == frozenset()
    with pytest.raises(StaleSubscriptionActionError, match="STALE_SUBSCRIPTION_ACTION"):
        manager.confirm_action(replay)


def test_reconnect_with_remaining_partial_restores_partial_not_old_full() -> None:
    """
    验证断线期取消 full lease 后，新 epoch 直接恢复仍有效 partial 而非旧 full。

    Returns:
        None: 新 epoch effective desired 和唯一恢复动作断言通过后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    full = _scope(None)
    partial = _scope("600000.XSHG")
    _add_lease(manager, "session-full", "full-lease", (full,))
    _add_lease(manager, "session-partial", "partial-lease", (partial,))
    manager.confirm_action(_take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE))

    manager.begin_session_epoch("epoch-2")
    manager.remove_lease("session-full", "full-lease")
    restore = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert restore.scope == partial
    assert full not in manager.snapshot().sent
    manager.confirm_action(restore)
    assert manager.snapshot().confirmed == frozenset({partial})
