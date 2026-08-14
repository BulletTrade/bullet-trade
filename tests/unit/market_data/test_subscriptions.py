"""
作者: BruceLee

文件职责: 验证实时行情 session lease、adapter union/refcount 和确认状态机。
主要输入: 脱敏的 full/partial 市场作用域、合成 session 与底层成功/失败回执。
主要输出: desired/sent/confirmed、引用计数、安全转换动作和重连恢复断言。
上游关系: 覆盖 bullet_trade.market_data.subscriptions 纯 Python 公共合同。
下游关系: 为未来远程 server、Huaxin adapter 和 Feed receipt 接入提供回归门禁。
关键配置约定: 全部测试离线运行，不联网、不加载厂商 SDK、不执行交易。
"""

from dataclasses import replace
from threading import Barrier, Event, Thread
from typing import List, Optional, Sequence

import pytest

from bullet_trade.market_data.models import MarketEventType
from bullet_trade.market_data.subscriptions import (
    AdapterSubscriptionAction,
    AdapterSubscriptionOperation,
    AdapterSubscriptionScope,
    StaleSubscriptionActionError,
    SubscriptionLeaseConflictError,
    SubscriptionLeaseError,
    SubscriptionLeaseManager,
    SubscriptionLeaseNotFoundError,
    SubscriptionTransitionError,
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


def _plan_single(
    manager: SubscriptionLeaseManager,
    operation: AdapterSubscriptionOperation,
) -> AdapterSubscriptionAction:
    """
    取出并验证状态机此轮只规划了一个未 claim 的指定动作。

    Args:
        manager: 待验证的订阅租约状态机。
        operation: 期望的 subscribe 或 unsubscribe 类型。

    Returns:
        AdapterSubscriptionAction: 唯一的未发送计划。
    """
    actions = manager.take_actions()
    assert len(actions) == 1
    assert actions[0].operation is operation
    return actions[0]


def _take_single(
    manager: SubscriptionLeaseManager,
    operation: AdapterSubscriptionOperation,
) -> AdapterSubscriptionAction:
    """
    规划并原子 claim 此轮唯一的指定动作。

    Args:
        manager: 待验证的订阅租约状态机。
        operation: 期望的 subscribe 或 unsubscribe 类型。

    Returns:
        AdapterSubscriptionAction: 已进入 inflight、可立即交给 SDK 的动作。
    """
    action = _plan_single(manager, operation)
    manager.claim_action(action)
    return action


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
        manager.claim_action(action)
        manager.confirm_action(action)


def _drive_success(manager: SubscriptionLeaseManager, max_rounds: int = 8) -> None:
    """
    将当前所有可规划动作按 claim→成功回执推进至稳定状态。

    Args:
        manager: 待收敛的订阅租约状态机。
        max_rounds: 防止测试缺陷造成无限循环的最大轮数。

    Returns:
        None: 状态机无剩余计划时返回。

    Raises:
        AssertionError: 超过最大轮数仍有计划时抛出。
    """
    for _ in range(max_rounds):
        actions = manager.take_actions()
        if not actions:
            return
        _confirm_actions(manager, actions)
    raise AssertionError("订阅状态机未在有界轮数内收敛")


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
    manager.claim_action(actions[0])
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
    assert snapshot.sent == frozenset({full})
    assert snapshot.failures[0].code == "VENDOR_PERMISSION_DENIED"
    assert manager.take_actions() == ()

    manager.retry_failed(AdapterSubscriptionOperation.SUBSCRIBE, partial)
    retried = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert retried.action_id != rejected.action_id
    manager.confirm_action(retried)
    remove_full = _take_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)
    assert remove_full.scope == full


def test_rejected_full_degrades_to_partial_without_poisoning_other_lease() -> None:
    """
    验证 full 订阅被拒绝后，同组其他 session 的 partial 仍可独立确认。

    Returns:
        None: partial 降级、full 显式重试与无窗口切回全部通过后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    full = _scope(None)
    partial = _scope("600000.XSHG")
    _add_lease(manager, "session-full", "full-lease", (full,))
    _add_lease(manager, "session-partial", "partial-lease", (partial,))

    rejected_full = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert rejected_full.scope == full
    manager.reject_action(rejected_full, "FULL_PERMISSION_DENIED")
    degraded = manager.snapshot()
    assert degraded.desired == frozenset({full, partial})
    assert degraded.effective_desired == frozenset({partial})
    assert degraded.sent == degraded.confirmed == frozenset()
    assert manager.is_lease_confirmed("session-full", "full-lease") is False

    establish_partial = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert establish_partial.scope == partial
    manager.confirm_action(establish_partial)
    assert manager.is_lease_confirmed("session-partial", "partial-lease") is True
    assert manager.is_lease_confirmed("session-full", "full-lease") is False
    assert manager.take_actions() == ()

    manager.retry_failed(AdapterSubscriptionOperation.SUBSCRIBE, full)
    restore_full = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert restore_full.scope == full
    manager.confirm_action(restore_full)
    remove_partial = _take_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)
    assert remove_partial.scope == partial
    assert full in manager.snapshot().stable_confirmed
    manager.confirm_action(remove_partial)
    assert manager.snapshot().confirmed == frozenset({full})
    assert manager.is_lease_confirmed("session-full", "full-lease") is True
    assert manager.is_lease_confirmed("session-partial", "partial-lease") is True


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


def test_take_only_plans_and_intent_reversal_prevents_racing_send() -> None:
    """
    验证 dispatcher 取计划后、claim 前发生意图反转时旧动作绝不能进入 sent。

    Returns:
        None: Event/Barrier 确定性竞态与 stale revision 断言通过后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    scope = _scope("600000.XSHG")
    _add_lease(manager, "session-a", "lease-a", (scope,))
    start = Barrier(2)
    planned = Event()
    release_claim = Event()
    actions: List[AdapterSubscriptionAction] = []
    errors: List[BaseException] = []

    def dispatch() -> None:
        """
        在线程内先取计划，等待主线程反转意图后再尝试 claim。

        Returns:
            None: claim 成功或异常被记录后返回。
        """
        try:
            start.wait()
            action = _plan_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
            actions.append(action)
            planned.set()
            release_claim.wait()
            manager.claim_action(action)
        except BaseException as exc:  # pragma: no branch - 竞态结果统一回传主线程
            errors.append(exc)

    worker = Thread(target=dispatch, name="subscription-plan-race")
    worker.start()
    start.wait()
    assert planned.wait(timeout=2.0) is True
    before = manager.snapshot()
    assert before.planned_subscribe == frozenset({scope})
    assert before.sent == before.pending_subscribe == frozenset()

    manager.remove_lease("session-a", "lease-a")
    release_claim.set()
    worker.join(timeout=2.0)

    assert worker.is_alive() is False
    assert len(actions) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], StaleSubscriptionActionError)
    assert "STALE_SUBSCRIPTION_REVISION" in str(errors[0])
    after = manager.snapshot()
    assert after.desired == after.sent == after.confirmed == frozenset()
    assert after.pending_subscribe == after.planned_subscribe == frozenset()
    assert manager.take_actions() == ()


def test_epoch_change_prevents_unclaimed_plan_from_being_sent() -> None:
    """
    验证 plan 与 claim 之间切换 session epoch 会拒绝旧动作且不污染新 epoch。

    Returns:
        None: 旧计划被拒绝、新 epoch 仅重新规划当前 desired 一次后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    scope = _scope("600000.XSHG")
    _add_lease(manager, "session-a", "lease-a", (scope,))
    old_plan = _plan_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)

    manager.begin_session_epoch("epoch-2")
    with pytest.raises(StaleSubscriptionActionError, match="STALE_SUBSCRIPTION_ACTION"):
        manager.claim_action(old_plan)

    snapshot = manager.snapshot()
    assert snapshot.sent == snapshot.pending_subscribe == frozenset()
    new_plan = _plan_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert new_plan.session_epoch == "epoch-2"
    assert new_plan.action_id != old_plan.action_id


@pytest.mark.parametrize("first_result", ("confirm", "timeout"))
def test_claimed_ack_and_timeout_race_has_one_linearized_owner(first_result: str) -> None:
    """
    验证 claimed 动作的 ACK 与 timeout 诊断竞态只会线性化一个结果。

    Args:
        first_result: 由 Event 明确安排先落锁的 confirm 或 timeout 分支。

    Returns:
        None: 两种顺序均无 inflight/uncertain/completed 双态并收敛后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    scope = _scope("600000.XSHG")
    _add_lease(manager, "session-a", "lease-a", (scope,))
    action = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    start = Barrier(3)
    first_done = Event()
    successes = [False, False]
    errors: List[Optional[BaseException]] = [None, None]

    def confirm_worker() -> None:
        """
        按测试指定顺序提交 SDK 成功 ACK。

        Returns:
            None: 成功标记或精确异常写入固定结果槽后返回。
        """
        start.wait()
        if first_result == "timeout":
            first_done.wait()
        try:
            manager.confirm_action(action)
            successes[0] = True
        except BaseException as exc:  # pragma: no branch - 异常类型由主线程断言
            errors[0] = exc
        finally:
            if first_result == "confirm":
                first_done.set()

    def timeout_worker() -> None:
        """
        按测试指定顺序将同一 SDK 动作标记为 ACK 不确定。

        Returns:
            None: 成功标记或精确异常写入固定结果槽后返回。
        """
        start.wait()
        if first_result == "confirm":
            first_done.wait()
        try:
            manager.mark_action_uncertain(action, reason="mock ack timeout")
            successes[1] = True
        except BaseException as exc:  # pragma: no branch - 异常类型由主线程断言
            errors[1] = exc
        finally:
            if first_result == "timeout":
                first_done.set()

    confirm_thread = Thread(target=confirm_worker, name=f"subscription-{first_result}-confirm")
    timeout_thread = Thread(target=timeout_worker, name=f"subscription-{first_result}-timeout")
    confirm_thread.start()
    timeout_thread.start()
    start.wait()
    confirm_thread.join(timeout=2.0)
    timeout_thread.join(timeout=2.0)
    assert confirm_thread.is_alive() is False
    assert timeout_thread.is_alive() is False

    raced = manager.snapshot()
    assert raced.pending_subscribe == frozenset()
    if first_result == "confirm":
        assert successes == [True, False]
        assert errors[0] is None
        assert isinstance(errors[1], SubscriptionTransitionError)
        assert "ALREADY_CONFIRMED" in str(errors[1])
        assert raced.confirmed == frozenset({scope})
        assert raced.uncertain_subscribe == frozenset()
        manager.confirm_action(action)
    else:
        assert successes == [False, True]
        assert isinstance(errors[0], SubscriptionTransitionError)
        assert "UNCERTAIN_REQUIRES_RECONCILE" in str(errors[0])
        assert errors[1] is None
        assert raced.confirmed == frozenset()
        assert raced.uncertain_subscribe == frozenset({scope})
        manager.reconcile_action(action, applied=True, reason="mock query proves applied")
        manager.reconcile_action(action, applied=True, reason="mock query proves applied")

    final = manager.snapshot()
    assert final.pending_subscribe == final.uncertain_subscribe == frozenset()
    assert final.sent == final.confirmed == frozenset({scope})


def test_unclaimed_or_forged_plan_cannot_accept_vendor_result() -> None:
    """
    验证底层 confirm/reject 只接受精确 claimed inflight，不接受纯计划或伪造内容。

    Returns:
        None: 未 claim 回执与同 ID 篡改动作均被 fail closed 后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    scope = _scope("600000.XSHG")
    _add_lease(manager, "session-a", "lease-a", (scope,))
    action = _plan_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)

    with pytest.raises(SubscriptionTransitionError, match="NOT_INFLIGHT"):
        manager.confirm_action(action)
    with pytest.raises(SubscriptionTransitionError, match="NOT_INFLIGHT"):
        manager.reject_action(action, "MOCK_REJECT")
    forged = replace(action, reason="forged-reason")
    with pytest.raises(SubscriptionTransitionError, match="NOT_EXACT_PLAN"):
        manager.claim_action(forged)

    manager.claim_action(action)
    manager.confirm_action(action)
    forged_completed = replace(action, scope=_scope("600001.XSHG"))
    with pytest.raises(SubscriptionTransitionError, match="COMPLETED_ACTION_MISMATCH"):
        manager.confirm_action(forged_completed)


def test_action_revision_and_completed_history_limit_reject_bool_values() -> None:
    """
    验证 Python bool 不会被当作 generation 或完成历史容量的整数接受。

    Returns:
        None: 两个公开构造边界均以 ValueError fail closed 后返回。
    """
    with pytest.raises(ValueError, match="completed_history_limit"):
        SubscriptionLeaseManager("epoch-1", completed_history_limit=True)

    manager = SubscriptionLeaseManager("epoch-1")
    scope = _scope("600000.XSHG")
    _add_lease(manager, "session-a", "lease-a", (scope,))
    action = _plan_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    with pytest.raises(ValueError, match="desired_revision"):
        replace(action, desired_revision=True)


def test_repeated_reject_requires_exact_action_code_and_reason() -> None:
    """
    验证 rejected 幂等只接受完全相同的动作、错误码和脱敏原因。

    Returns:
        None: 精确重复成功且冲突重复被拒绝后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    scope = _scope("600000.XSHG")
    _add_lease(manager, "session-a", "lease-a", (scope,))
    action = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)

    manager.reject_action(action, "MOCK_REJECT", "same reason")
    manager.reject_action(action, "MOCK_REJECT", "same reason")
    with pytest.raises(SubscriptionTransitionError, match="COMPLETED_RESULT_MISMATCH"):
        manager.reject_action(action, "OTHER_REJECT", "same reason")
    with pytest.raises(SubscriptionTransitionError, match="COMPLETED_RESULT_MISMATCH"):
        manager.reject_action(action, "MOCK_REJECT", "changed reason")
    with pytest.raises(SubscriptionTransitionError, match="COMPLETED_RESULT_MISMATCH"):
        manager.confirm_action(action)


def test_claimed_partial_unsubscribe_is_not_a_safe_cover_for_full_removal() -> None:
    """
    验证 partial 退订已 claim 后意图反转时，不会再并发退掉唯一 full 覆盖。

    Returns:
        None: 两个覆盖不会同时在途退订，最终 desired partial 无窗口收敛后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    partial = _scope("600000.XSHG")
    full = _scope(None)
    _add_lease(manager, "session-partial", "partial-lease", (partial,))
    _drive_success(manager)
    _add_lease(manager, "session-full", "full-lease", (full,))

    promote = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert promote.scope == full
    manager.confirm_action(promote)
    remove_partial = _take_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)
    assert remove_partial.scope == partial

    manager.remove_lease("session-full", "full-lease")
    assert manager.take_actions() == ()
    assert manager.snapshot().pending_unsubscribe == frozenset({partial})
    assert manager.snapshot().confirmed == frozenset({partial, full})

    manager.confirm_action(remove_partial)
    restore_partial = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert restore_partial.scope == partial
    assert full in manager.snapshot().confirmed
    manager.confirm_action(restore_partial)
    remove_full = _take_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)
    assert remove_full.scope == full
    manager.confirm_action(remove_full)
    assert manager.snapshot().confirmed == frozenset({partial})


def test_new_lease_is_not_confirmed_by_scope_with_claimed_unsubscribe() -> None:
    """
    验证新 lease 不会借用一个已 claim 退订的旧 confirmed scope 冒充 ready。

    Returns:
        None: ACK 前保持未确认，退订生效并补偿订阅后才恢复 confirmed。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    scope = _scope("600000.XSHG")
    _add_lease(manager, "session-old", "lease-old", (scope,))
    _drive_success(manager)
    manager.remove_lease("session-old", "lease-old")
    unsubscribe = _take_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)

    _add_lease(manager, "session-new", "lease-new", (scope,))
    assert scope in manager.snapshot().confirmed
    assert scope not in manager.snapshot().stable_confirmed
    assert manager.is_lease_confirmed("session-new", "lease-new") is False
    assert manager.take_actions() == ()

    manager.confirm_action(unsubscribe)
    assert manager.is_lease_confirmed("session-new", "lease-new") is False
    _drive_success(manager)
    assert manager.is_lease_confirmed("session-new", "lease-new") is True


def test_new_lease_is_not_confirmed_by_uncertain_unsubscribe() -> None:
    """
    验证退订 ACK timeout 未对账前，新 lease readiness 使用稳定覆盖而非旧事实。

    Returns:
        None: uncertain 阶段保持未确认，证明退订未生效后才恢复 confirmed。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    scope = _scope("600000.XSHG")
    _add_lease(manager, "session-old", "lease-old", (scope,))
    _drive_success(manager)
    manager.remove_lease("session-old", "lease-old")
    unsubscribe = _take_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)
    _add_lease(manager, "session-new", "lease-new", (scope,))

    manager.mark_action_uncertain(unsubscribe, reason="mock ack timeout")
    assert scope not in manager.snapshot().stable_confirmed
    assert manager.is_lease_confirmed("session-new", "lease-new") is False
    manager.reconcile_action(
        unsubscribe,
        applied=False,
        reason="mock query proves not applied",
    )
    assert manager.is_lease_confirmed("session-new", "lease-new") is True
    assert manager.take_actions() == ()


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    tuple(
        (field_name, invalid_value)
        for field_name in (
            "completed_history_limit",
            "max_leases_per_session",
            "max_total_leases",
            "max_requests_per_session",
            "max_total_requests",
            "max_scopes_per_lease",
            "max_scope_references_per_session",
            "max_total_scope_references",
            "max_retained_adapter_scopes",
        )
        for invalid_value in (True, 0, 1.5)
    ),
)
def test_manager_capacity_limits_require_positive_non_bool_integers(
    field_name: str,
    invalid_value: int,
) -> None:
    """
    验证完成历史和 lease/request 容量参数拒绝 bool、零与非整数。

    Args:
        field_name: 待覆盖的构造参数名。
        invalid_value: 应被拒绝的非法容量值。

    Returns:
        None: 构造器以带字段名的 ValueError fail closed 后返回。
    """
    with pytest.raises(ValueError, match=field_name):
        SubscriptionLeaseManager("epoch-1", **{field_name: invalid_value})


def test_session_and_global_lease_request_limits_preserve_idempotent_replay() -> None:
    """
    验证硬容量阻止唯一 ID 内存增长，但已登记请求在上限处仍可幂等重放。

    Returns:
        None: 单 session/global 的 lease/request 四类门禁均返回稳定错误后返回。
    """
    scope = _scope("600000.XSHG")
    session_lease_limited = SubscriptionLeaseManager(
        "epoch-1",
        max_leases_per_session=1,
        max_total_leases=10,
        max_requests_per_session=10,
        max_total_requests=10,
    )
    original = session_lease_limited.add_lease(
        "session-a",
        "lease-a",
        "request-a",
        "fingerprint-a",
        (scope,),
    )
    replay = session_lease_limited.add_lease(
        "session-a",
        "ignored-new-id",
        "request-a",
        "fingerprint-a",
        (scope,),
    )
    assert replay is original
    with pytest.raises(SubscriptionLeaseError, match="SESSION_LEASE_LIMIT"):
        _add_lease(session_lease_limited, "session-a", "lease-b", (scope,))

    session_request_limited = SubscriptionLeaseManager(
        "epoch-1",
        max_leases_per_session=3,
        max_total_leases=10,
        max_requests_per_session=1,
        max_total_requests=10,
    )
    _add_lease(session_request_limited, "session-a", "lease-a", (scope,))
    with pytest.raises(SubscriptionLeaseError, match="SESSION_REQUEST_LIMIT"):
        _add_lease(session_request_limited, "session-a", "lease-b", (scope,))

    global_lease_limited = SubscriptionLeaseManager(
        "epoch-1",
        max_leases_per_session=3,
        max_total_leases=1,
        max_requests_per_session=3,
        max_total_requests=10,
    )
    _add_lease(global_lease_limited, "session-a", "lease-a", (scope,))
    with pytest.raises(SubscriptionLeaseError, match="GLOBAL_LEASE_LIMIT"):
        _add_lease(global_lease_limited, "session-b", "lease-b", (scope,))

    global_request_limited = SubscriptionLeaseManager(
        "epoch-1",
        max_leases_per_session=3,
        max_total_leases=3,
        max_requests_per_session=3,
        max_total_requests=1,
    )
    _add_lease(global_request_limited, "session-a", "lease-a", (scope,))
    with pytest.raises(SubscriptionLeaseError, match="GLOBAL_REQUEST_LIMIT"):
        _add_lease(global_request_limited, "session-b", "lease-b", (scope,))


def test_scope_reference_limits_block_single_and_accumulated_memory_growth() -> None:
    """
    验证单 lease、单 session 与全局 scope 引用硬上限均在持久化前生效。

    Returns:
        None: 三层 scope 容量门禁分别返回稳定错误且未污染快照后返回。
    """
    first = _scope("600000.XSHG")
    second = _scope("600001.XSHG")
    third = _scope("600002.XSHG")
    single_lease_limited = SubscriptionLeaseManager(
        "epoch-1",
        max_scopes_per_lease=1,
    )
    with pytest.raises(SubscriptionLeaseError, match="LEASE_SCOPE_LIMIT"):
        _add_lease(single_lease_limited, "session-a", "lease-a", (first, second))
    assert single_lease_limited.snapshot().leases == ()

    session_scope_limited = SubscriptionLeaseManager(
        "epoch-1",
        max_leases_per_session=4,
        max_requests_per_session=4,
        max_scopes_per_lease=2,
        max_scope_references_per_session=2,
        max_total_scope_references=10,
    )
    _add_lease(session_scope_limited, "session-a", "lease-a", (first,))
    _add_lease(session_scope_limited, "session-a", "lease-b", (second,))
    session_scope_limited.remove_lease("session-a", "lease-a")
    with pytest.raises(SubscriptionLeaseError, match="SESSION_SCOPE_REFERENCE_LIMIT"):
        _add_lease(session_scope_limited, "session-a", "lease-c", (third,))
    assert len(session_scope_limited.snapshot().leases) == 2

    global_scope_limited = SubscriptionLeaseManager(
        "epoch-1",
        max_total_leases=4,
        max_total_requests=4,
        max_scopes_per_lease=2,
        max_scope_references_per_session=3,
        max_total_scope_references=2,
    )
    _add_lease(global_scope_limited, "session-a", "lease-a", (first,))
    _add_lease(global_scope_limited, "session-b", "lease-b", (second,))
    with pytest.raises(SubscriptionLeaseError, match="GLOBAL_SCOPE_REFERENCE_LIMIT"):
        _add_lease(global_scope_limited, "session-c", "lease-c", (third,))
    assert len(global_scope_limited.snapshot().leases) == 2


def test_close_session_cleans_tombstones_and_defines_idempotency_boundary() -> None:
    """
    验证 canceled 墓碑在 session 存活期保留幂等，显式 close 后释放容量。

    Returns:
        None: close 前重放原墓碑、close 后可注册新请求且快照无泄漏后返回。
    """
    manager = SubscriptionLeaseManager(
        "epoch-1",
        max_leases_per_session=1,
        max_total_leases=1,
        max_requests_per_session=1,
        max_total_requests=1,
    )
    scope = _scope("600000.XSHG")
    original = manager.add_lease(
        "session-a",
        "lease-a",
        "request-a",
        "fingerprint-a",
        (scope,),
    )
    manager.remove_lease("session-a", "lease-a")
    replay = manager.add_lease(
        "session-a",
        "ignored-new-id",
        "request-a",
        "fingerprint-a",
        (scope,),
    )
    assert replay.subscription_id == original.subscription_id
    assert replay.is_active is False
    with pytest.raises(SubscriptionLeaseError, match="SESSION_LEASE_LIMIT"):
        _add_lease(manager, "session-a", "lease-b", (scope,))

    removed = manager.close_session("session-a")
    assert tuple(item.subscription_id for item in removed) == ("lease-a",)
    assert manager.snapshot().leases == ()
    replacement = manager.add_lease(
        "session-a",
        "lease-b",
        "request-b",
        "fingerprint-b",
        (scope,),
    )
    assert replacement.subscription_id == "lease-b"


def test_close_active_session_updates_union_refcount_and_invalidates_old_plan() -> None:
    """
    验证 close active session 只移除其引用，并使关闭前未 claim 计划失效。

    Returns:
        None: shared refcount 保留、独占 desired 删除和 stale plan 断言通过后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    shared = _scope("600000.XSHG")
    exclusive = _scope("600001.XSHG")
    _add_lease(manager, "session-a", "shared-a", (shared,))
    _add_lease(manager, "session-b", "shared-b", (shared,))
    _drive_success(manager)
    _add_lease(manager, "session-a", "exclusive-a", (exclusive,))
    old_plan = _plan_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    assert old_plan.scope == exclusive

    removed = manager.close_session("session-a")
    assert tuple(item.subscription_id for item in removed) == ("exclusive-a", "shared-a")
    with pytest.raises(StaleSubscriptionActionError, match="STALE_SUBSCRIPTION_REVISION"):
        manager.claim_action(old_plan)
    snapshot = manager.snapshot()
    assert tuple(item.subscription_id for item in snapshot.leases) == ("shared-b",)
    assert snapshot.desired == frozenset({shared})
    assert snapshot.refcounts == {shared: 1}
    assert snapshot.planned_subscribe == frozenset()
    assert manager.take_actions() == ()


def test_rejected_subscribe_close_churn_releases_residual_adapter_scope() -> None:
    """
    验证明确拒绝的 subscribe 在意图关闭后不会按唯一 scope 累积。

    Returns:
        None: 多轮唯一 session/scope 均在容量为一时释放 sent/failure 后返回。
    """
    manager = SubscriptionLeaseManager(
        "epoch-1",
        completed_history_limit=2,
        max_leases_per_session=1,
        max_total_leases=1,
        max_requests_per_session=1,
        max_total_requests=1,
        max_scopes_per_lease=1,
        max_scope_references_per_session=1,
        max_total_scope_references=1,
        max_retained_adapter_scopes=1,
    )

    for index in range(20):
        scope = _scope(f"{600000 + index:06d}.XSHG")
        session_id = f"session-{index}"
        subscription_id = f"lease-{index}"
        _add_lease(manager, session_id, subscription_id, (scope,))
        action = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
        rejected = manager.reject_action(action, "VENDOR_PERMISSION_DENIED")
        assert rejected.sent == frozenset()
        assert tuple(item.action.scope for item in rejected.failures) == (scope,)

        manager.close_session(session_id)
        closed = manager.snapshot()
        assert closed.leases == ()
        assert closed.desired == frozenset()
        assert closed.sent == frozenset()
        assert closed.confirmed == frozenset()
        assert closed.failures == ()


def test_uncertain_close_churn_retains_capacity_until_reconcile() -> None:
    """
    验证 close 不删除可能已发的 uncertain scope，并在对账前占用硬容量。

    Returns:
        None: 同 scope 可复用、新 scope fail closed，对账后新 scope 可注册时返回。
    """
    manager = SubscriptionLeaseManager(
        "epoch-1",
        max_leases_per_session=1,
        max_total_leases=1,
        max_requests_per_session=1,
        max_total_requests=1,
        max_scopes_per_lease=1,
        max_scope_references_per_session=1,
        max_total_scope_references=1,
        max_retained_adapter_scopes=1,
    )
    first = _scope("600000.XSHG")
    second = _scope("600001.XSHG")
    _add_lease(manager, "session-a", "lease-a", (first,))
    action = _take_single(manager, AdapterSubscriptionOperation.SUBSCRIBE)
    manager.mark_action_uncertain(action, reason="mock ack timeout")
    manager.close_session("session-a")

    uncertain = manager.snapshot()
    assert uncertain.leases == ()
    assert uncertain.uncertain_subscribe == frozenset({first})
    _add_lease(manager, "session-reuse", "lease-reuse", (first,))
    manager.close_session("session-reuse")
    with pytest.raises(SubscriptionLeaseError, match="RETAINED_ADAPTER_SCOPE_LIMIT"):
        _add_lease(manager, "session-b", "lease-b", (second,))
    assert manager.snapshot().leases == ()

    manager.reconcile_action(action, applied=False, reason="mock query proves not applied")
    _add_lease(manager, "session-b", "lease-b", (second,))
    assert manager.snapshot().desired == frozenset({second})


def test_confirmed_close_retains_capacity_until_unsubscribe_or_same_scope_reuse() -> None:
    """
    验证会话关闭后未完成退订的 confirmed ghost 不能被新 scope 绕过容量。

    Returns:
        None: 新 scope 被拒绝、同 scope 复用并使旧退订计划失效后返回。
    """
    manager = SubscriptionLeaseManager(
        "epoch-1",
        max_total_leases=1,
        max_total_requests=1,
        max_total_scope_references=1,
        max_retained_adapter_scopes=1,
    )
    first = _scope("600000.XSHG")
    second = _scope("600001.XSHG")
    _add_lease(manager, "session-a", "lease-a", (first,))
    _drive_success(manager)
    manager.close_session("session-a")
    unsubscribe = _plan_single(manager, AdapterSubscriptionOperation.UNSUBSCRIBE)
    assert unsubscribe.scope == first

    with pytest.raises(SubscriptionLeaseError, match="RETAINED_ADAPTER_SCOPE_LIMIT"):
        _add_lease(manager, "session-b", "lease-b", (second,))
    _add_lease(manager, "session-reuse", "lease-reuse", (first,))
    assert manager.take_actions() == ()
    with pytest.raises(StaleSubscriptionActionError, match="STALE_SUBSCRIPTION_REVISION"):
        manager.claim_action(unsubscribe)


@pytest.mark.parametrize(
    ("operation", "desired_present", "applied"),
    tuple(
        (operation, desired_present, applied)
        for operation in (
            AdapterSubscriptionOperation.SUBSCRIBE,
            AdapterSubscriptionOperation.UNSUBSCRIBE,
        )
        for desired_present in (False, True)
        for applied in (False, True)
    ),
)
def test_uncertain_timeout_reconcile_converges_all_intent_quadrants(
    operation: AdapterSubscriptionOperation,
    desired_present: bool,
    applied: bool,
) -> None:
    """
    验证订阅/退订 ACK timeout 在当前 desired 有/无及生效/未生效下均可收敛。

    Args:
        operation: 被标记 uncertain 的 subscribe 或 unsubscribe。
        desired_present: timeout 对账时当前逻辑意图是否仍需要该 scope。
        applied: 对账是否证明原动作已经在 SDK 生效。

    Returns:
        None: 对账后补偿动作收敛到最新 desired/confirmed 一致状态后返回。
    """
    manager = SubscriptionLeaseManager("epoch-1")
    scope = _scope("600000.XSHG")
    _add_lease(manager, "session-a", "lease-a", (scope,))
    if operation is AdapterSubscriptionOperation.SUBSCRIBE:
        action = _take_single(manager, operation)
        if not desired_present:
            manager.remove_lease("session-a", "lease-a")
    else:
        _drive_success(manager)
        manager.remove_lease("session-a", "lease-a")
        action = _take_single(manager, operation)
        if desired_present:
            _add_lease(manager, "session-b", "lease-b", (scope,))

    manager.mark_action_uncertain(action, reason="mock ack timeout")
    uncertain = manager.snapshot()
    expected_uncertain = frozenset({scope})
    if operation is AdapterSubscriptionOperation.SUBSCRIBE:
        assert uncertain.uncertain_subscribe == expected_uncertain
    else:
        assert uncertain.uncertain_unsubscribe == expected_uncertain
    with pytest.raises(SubscriptionTransitionError, match="UNCERTAIN_REQUIRES_RECONCILE"):
        manager.confirm_action(action)
    with pytest.raises(SubscriptionTransitionError, match="UNCERTAIN_REQUIRES_RECONCILE"):
        manager.reject_action(action, "LATE_REJECT")

    manager.reconcile_action(action, applied=applied, reason="mock query evidence")
    manager.reconcile_action(action, applied=applied, reason="mock query evidence")
    _drive_success(manager)

    expected = frozenset({scope}) if desired_present else frozenset()
    final = manager.snapshot()
    assert final.desired == expected
    assert final.effective_desired == expected
    assert final.sent == expected
    assert final.confirmed == expected
    assert final.planned_subscribe == final.planned_unsubscribe == frozenset()
    assert final.pending_subscribe == final.pending_unsubscribe == frozenset()
    assert final.uncertain_subscribe == final.uncertain_unsubscribe == frozenset()
