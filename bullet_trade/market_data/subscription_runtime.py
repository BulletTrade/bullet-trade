"""
作者: BruceLee

文件职责: 串联订阅租约状态机、底层 adapter 本地接受与异步回调确认。
主要输入: AdapterSubscriptionAction、adapter 本地提交结果和真实确认/拒绝回调。
主要输出: 可对账的 desired/sent/confirmed 快照、ACK 超时与重连恢复动作。
上游关系: Feed 或远程会话将已展开 scope 注册为 session lease。
下游关系: 具体 SDK adapter 实现 submit；Feed receipt/health 共用状态快照。
关键配置约定: 本模块不联网、不加载厂商 SDK、不交易；本地 accepted
只表示请求已交给 adapter，绝不代表厂商 callback confirmed。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from threading import RLock
from time import monotonic
from typing import Callable, Dict, Optional, Sequence, Tuple

from .subscriptions import (
    AdapterSubscriptionAction,
    AdapterSubscriptionOperation,
    AdapterSubscriptionScope,
    SessionSubscriptionLease,
    SubscriptionLeaseManager,
    SubscriptionLeaseNotFoundError,
    SubscriptionLeaseSnapshot,
    SubscriptionTransitionError,
)

SubscriptionMonotonicClock = Callable[[], float]
SubscriptionStateCallback = Callable[[SubscriptionLeaseSnapshot], None]


class SubscriptionDispatchError(RuntimeError):
    """表示 adapter 提交合同、调度轮次或 ACK 对账违反受控边界。"""


class AdapterSubscriptionResponseOutcome(str, Enum):
    """表示 adapter 回调已明确确认或拒绝一个底层动作。"""

    CONFIRMED = "confirmed"
    REJECTED = "rejected"


@dataclass(frozen=True)
class AdapterSubscriptionSubmitResult:
    """保存 adapter 同步本地提交结果，不承载厂商确认语义。"""

    accepted: bool
    code: Optional[str] = None
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        """
        校验本地拒绝必须提供稳定错误码。

        Returns:
            None: 合同合法时返回。

        Raises:
            ValueError: accepted 不是 bool 或本地拒绝缺少 code 时抛出。
        """
        if not isinstance(self.accepted, bool):
            raise ValueError("accepted 必须为 bool")
        normalized_code = str(self.code).strip() if self.code is not None else None
        normalized_reason = str(self.reason).strip() if self.reason is not None else None
        if not self.accepted and not normalized_code:
            raise ValueError("adapter 本地拒绝必须提供 code")
        object.__setattr__(self, "code", normalized_code or None)
        object.__setattr__(self, "reason", normalized_reason or None)


@dataclass(frozen=True)
class AdapterSubscriptionResponse:
    """保存 adapter 通过厂商回调产生的精确动作结果。"""

    action: AdapterSubscriptionAction
    outcome: AdapterSubscriptionResponseOutcome
    code: Optional[str] = None
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        """
        校验动作、回调结果与拒绝错误码。

        Returns:
            None: 字段规范化完成后返回。

        Raises:
            ValueError: action 类型错误或 rejected 缺少 code 时抛出。
        """
        if not isinstance(self.action, AdapterSubscriptionAction):
            raise ValueError("action 必须为 AdapterSubscriptionAction")
        try:
            outcome = AdapterSubscriptionResponseOutcome(self.outcome)
        except ValueError as exc:
            raise ValueError("outcome 必须为 confirmed 或 rejected") from exc
        normalized_code = str(self.code).strip() if self.code is not None else None
        normalized_reason = str(self.reason).strip() if self.reason is not None else None
        if outcome is AdapterSubscriptionResponseOutcome.REJECTED and not normalized_code:
            raise ValueError("rejected callback 必须提供 code")
        if outcome is AdapterSubscriptionResponseOutcome.CONFIRMED and normalized_code:
            raise ValueError("confirmed callback 不得携带 code")
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "code", normalized_code or None)
        object.__setattr__(self, "reason", normalized_reason or None)


AdapterSubscriptionResponseCallback = Callable[[AdapterSubscriptionResponse], None]


class SubscriptionActionAdapter(ABC):
    """定义只提交底层订阅动作的 vendor-neutral adapter 契约。"""

    @abstractmethod
    def submit(
        self,
        action: AdapterSubscriptionAction,
        callback: AdapterSubscriptionResponseCallback,
    ) -> AdapterSubscriptionSubmitResult:
        """
        将已 claim 动作交给底层，并在厂商回调时调用 callback。

        Args:
            action: 已由调度器 claim 的精确订阅或退订动作。
            callback: 将实际确认/拒绝结果传回调度器的函数。

        Returns:
            AdapterSubscriptionSubmitResult: 仅表示本地是否接受了请求。

        Raises:
            Exception: 结果不确定的底层异常由调度器转为 uncertain。
        """
        raise NotImplementedError


@dataclass(frozen=True)
class _PendingFakeResponse:
    """保存离线 fake adapter 尚未触发的动作与回调。"""

    action: AdapterSubscriptionAction
    callback: AdapterSubscriptionResponseCallback


class InMemorySubscriptionActionAdapter(SubscriptionActionAdapter):
    """提供可即时确认、延迟或按动作拒绝的无 SDK fake adapter。"""

    def __init__(
        self,
        auto_respond: bool = True,
        response_factory: Optional[
            Callable[[AdapterSubscriptionAction], Optional[AdapterSubscriptionResponse]]
        ] = None,
    ) -> None:
        """
        初始化内存 adapter 的响应策略和可观测动作记录。

        Args:
            auto_respond: True 时默认同步产生 confirmed callback。
            response_factory: 可选逐动作回调工厂；返回 None 表示延迟。

        Returns:
            None: fake adapter 初始化完成后返回。
        """
        if not isinstance(auto_respond, bool):
            raise ValueError("auto_respond 必须为 bool")
        self._lock = RLock()
        self._auto_respond = auto_respond
        self._response_factory = response_factory
        self._submitted = []  # type: list[AdapterSubscriptionAction]
        self._pending: Dict[str, _PendingFakeResponse] = {}

    @property
    def submitted_actions(self) -> Tuple[AdapterSubscriptionAction, ...]:
        """
        返回按本地提交顺序记录的全部动作。

        Returns:
            Tuple[AdapterSubscriptionAction, ...]: 不可变动作快照。
        """
        with self._lock:
            return tuple(self._submitted)

    @property
    def pending_actions(self) -> Tuple[AdapterSubscriptionAction, ...]:
        """
        返回尚未产生 callback 的离线动作。

        Returns:
            Tuple[AdapterSubscriptionAction, ...]: 按提交顺序排列的待响应动作。
        """
        with self._lock:
            pending_ids = set(self._pending)
            return tuple(action for action in self._submitted if action.action_id in pending_ids)

    def submit(
        self,
        action: AdapterSubscriptionAction,
        callback: AdapterSubscriptionResponseCallback,
    ) -> AdapterSubscriptionSubmitResult:
        """
        记录动作，并按注入策略立即回调或留待手工响应。

        Args:
            action: 已 claim 的精确动作。
            callback: 协调器的结果回调。

        Returns:
            AdapterSubscriptionSubmitResult: 始终表示 fake 本地已接受。

        Raises:
            SubscriptionDispatchError: 响应工厂返回了其他动作时抛出。
        """
        if not isinstance(action, AdapterSubscriptionAction):
            raise ValueError("action 必须为 AdapterSubscriptionAction")
        with self._lock:
            self._submitted.append(action)
            response = (
                self._response_factory(action)
                if self._response_factory is not None
                else (
                    AdapterSubscriptionResponse(
                        action=action,
                        outcome=AdapterSubscriptionResponseOutcome.CONFIRMED,
                    )
                    if self._auto_respond
                    else None
                )
            )
            if response is not None and response.action != action:
                raise SubscriptionDispatchError("FAKE_ADAPTER_RESPONSE_ACTION_MISMATCH")
            if response is None:
                self._pending[action.action_id] = _PendingFakeResponse(action, callback)
                return AdapterSubscriptionSubmitResult(accepted=True)
        callback(response)
        return AdapterSubscriptionSubmitResult(accepted=True)

    def respond(
        self,
        action_id: str,
        outcome: AdapterSubscriptionResponseOutcome = AdapterSubscriptionResponseOutcome.CONFIRMED,
        code: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> AdapterSubscriptionResponse:
        """
        为一个延迟动作触发精确 callback。

        Args:
            action_id: pending_actions 中的稳定动作 ID。
            outcome: confirmed 或 rejected。
            code: rejected 时必填的稳定错误码。
            reason: 可选脱敏原因。

        Returns:
            AdapterSubscriptionResponse: 已交给协调器的回调对象。

        Raises:
            SubscriptionLeaseNotFoundError: action_id 不在延迟队列时抛出。
        """
        normalized_id = str(action_id).strip()
        with self._lock:
            pending = self._pending.pop(normalized_id, None)
        if pending is None:
            raise SubscriptionLeaseNotFoundError("SUBSCRIPTION_FAKE_ACTION_NOT_PENDING")
        response = AdapterSubscriptionResponse(
            action=pending.action,
            outcome=outcome,
            code=code,
            reason=reason,
        )
        pending.callback(response)
        return response

    def respond_next(
        self,
        outcome: AdapterSubscriptionResponseOutcome = AdapterSubscriptionResponseOutcome.CONFIRMED,
        code: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> AdapterSubscriptionResponse:
        """
        按提交顺序响应最早的延迟动作。

        Args:
            outcome: confirmed 或 rejected。
            code: rejected 时必填的错误码。
            reason: 可选脱敏原因。

        Returns:
            AdapterSubscriptionResponse: 已触发的回调。

        Raises:
            SubscriptionLeaseNotFoundError: 当前没有 pending 动作时抛出。
        """
        actions = self.pending_actions
        if not actions:
            raise SubscriptionLeaseNotFoundError("SUBSCRIPTION_FAKE_ACTION_NOT_PENDING")
        return self.respond(actions[0].action_id, outcome=outcome, code=code, reason=reason)


class SubscriptionActionCoordinator:
    """将租约 planner 、adapter submit、异步 callback 和超时对账串成闭环。"""

    def __init__(
        self,
        manager: SubscriptionLeaseManager,
        adapter: SubscriptionActionAdapter,
        ack_timeout_seconds: float = 5.0,
        monotonic_clock: SubscriptionMonotonicClock = monotonic,
        state_callback: Optional[SubscriptionStateCallback] = None,
        max_dispatch_rounds: int = 64,
    ) -> None:
        """
        初始化带有明确 ACK 时限的订阅动作协调器。

        Args:
            manager: 保存 lease 与 desired/sent/confirmed 的状态机。
            adapter: 只负责提交动作并产生 callback 的底层适配器。
            ack_timeout_seconds: 本地 accepted 后等待 callback 的正数秒数。
            monotonic_clock: 用于 ACK deadline 的可注入单调时钟。
            state_callback: 每次状态落地后可选通知 Feed 刷新 receipt。
            max_dispatch_rounds: full/partial 多阶段补偿的单次轮次上限。

        Returns:
            None: 协调器初始化完成后返回。
        """
        if not isinstance(manager, SubscriptionLeaseManager):
            raise ValueError("manager 必须为 SubscriptionLeaseManager")
        if not isinstance(adapter, SubscriptionActionAdapter):
            raise ValueError("adapter 必须为 SubscriptionActionAdapter")
        if isinstance(ack_timeout_seconds, bool) or ack_timeout_seconds <= 0:
            raise ValueError("ack_timeout_seconds 必须为正数")
        if isinstance(max_dispatch_rounds, bool) or not isinstance(max_dispatch_rounds, int):
            raise ValueError("max_dispatch_rounds 必须为正数整数")
        if max_dispatch_rounds <= 0:
            raise ValueError("max_dispatch_rounds 必须为正数整数")
        self._lock = RLock()
        self._manager = manager
        self._adapter = adapter
        self._ack_timeout_seconds = float(ack_timeout_seconds)
        self._clock = monotonic_clock
        self._state_callback = state_callback
        self._max_dispatch_rounds = max_dispatch_rounds
        self._waiting: Dict[str, Tuple[AdapterSubscriptionAction, float]] = {}
        self._uncertain: Dict[str, AdapterSubscriptionAction] = {}
        self._dispatching = False
        self._redispatch_requested = False

    @property
    def manager(self) -> SubscriptionLeaseManager:
        """
        返回协调器独占驱动的租约状态机。

        Returns:
            SubscriptionLeaseManager: 当前 manager 引用。
        """
        return self._manager

    def snapshot(self) -> SubscriptionLeaseSnapshot:
        """
        返回当前租约与 adapter 集合快照。

        Returns:
            SubscriptionLeaseSnapshot: manager 的不可变快照。
        """
        return self._manager.snapshot()

    def add_lease(
        self,
        session_id: str,
        subscription_id: str,
        request_id: str,
        payload_fingerprint: str,
        scopes: Sequence[AdapterSubscriptionScope],
        dispatch: bool = True,
    ) -> SessionSubscriptionLease:
        """
        注册一个 session lease，并可选立即驱动底层 union 动作。

        Args:
            session_id: 客户端会话标识。
            subscription_id: 稳定租约 ID。
            request_id: 客户端幂等请求 ID。
            payload_fingerprint: 规范化 spec 指纹。
            scopes: 已展开的底层 adapter scopes。
            dispatch: True 时在注册后立即 pump。

        Returns:
            SessionSubscriptionLease: 新建或幂等返回的租约。
        """
        with self._lock:
            lease = self._manager.add_lease(
                session_id,
                subscription_id,
                request_id,
                payload_fingerprint,
                scopes,
            )
        self._notify_state()
        if dispatch:
            self.pump()
        return lease

    def remove_lease(
        self,
        session_id: str,
        subscription_id: str,
        dispatch: bool = True,
    ) -> SessionSubscriptionLease:
        """
        移除明确 lease 的全部意图，并可选驱动安全退订。

        Args:
            session_id: 租约所属会话。
            subscription_id: 必须精确命中的租约 ID。
            dispatch: True 时立即规划 adapter 补偿。

        Returns:
            SessionSubscriptionLease: active_scopes 已置空的租约。
        """
        with self._lock:
            lease = self._manager.remove_lease(session_id, subscription_id)
        self._notify_state()
        if dispatch:
            self.pump()
        return lease

    def remove_scopes(
        self,
        session_id: str,
        subscription_id: str,
        scopes: Sequence[AdapterSubscriptionScope],
        dispatch: bool = True,
    ) -> SessionSubscriptionLease:
        """
        从明确 lease 中移除精确 scopes，并按剩余 union 驱动安全部分退订。

        Args:
            session_id: 租约所属客户端会话。
            subscription_id: 需要部分修改的稳定租约 ID。
            scopes: 必须完整命中该 lease 当前 active_scopes 的移除集合。
            dispatch: True 时立即规划必要的 adapter 退订或覆盖转换。

        Returns:
            SessionSubscriptionLease: 保留未移除 active scopes 的更新后租约。
        """
        with self._lock:
            lease = self._manager.remove_scopes(session_id, subscription_id, scopes)
        self._notify_state()
        if dispatch:
            self.pump()
        return lease

    def close_session(
        self,
        session_id: str,
        dispatch: bool = True,
    ) -> Tuple[SessionSubscriptionLease, ...]:
        """
        显式清理一个永久关闭 session 的 lease/request 墓碑并驱动剩余 union。

        Args:
            session_id: 已确认不会恢复的客户端会话 ID。
            dispatch: True 时立即规划清理后必要的 adapter 补偿。

        Returns:
            Tuple[SessionSubscriptionLease, ...]: 被 manager 删除的全部租约墓碑。
        """
        with self._lock:
            leases = self._manager.close_session(session_id)
        self._notify_state()
        if dispatch:
            self.pump()
        return leases

    def retry_failed(
        self,
        operation: AdapterSubscriptionOperation,
        scope: AdapterSubscriptionScope,
        dispatch: bool = True,
    ) -> SubscriptionLeaseSnapshot:
        """
        显式清除一个精确失败门闩，并可选驱动新的安全 action ID。

        Args:
            operation: 需要重试的 subscribe 或 unsubscribe。
            scope: 当前 failure 对应的精确 adapter scope。
            dispatch: True 时清除门闩后立即 pump；False 只修改状态。

        Returns:
            SubscriptionLeaseSnapshot: 重试请求处理后的最终状态快照。
        """
        with self._lock:
            self._manager.retry_failed(operation, scope)
        self._notify_state()
        if dispatch:
            self.pump()
        return self._manager.snapshot()

    def begin_session_epoch(self, session_epoch: str, dispatch: bool = True) -> bool:
        """
        切换 adapter epoch，保留最新 desired lease 并丢弃旧确认证据。

        Args:
            session_epoch: 新连接/登录世代标识。
            dispatch: True 时只按当前 union 恢复一次。

        Returns:
            bool: epoch 实际改变时为 True。
        """
        with self._lock:
            changed = bool(self._manager.begin_session_epoch(session_epoch))
            if changed:
                self._waiting.clear()
                self._uncertain.clear()
        self._notify_state()
        if dispatch:
            self.pump()
        return changed

    def pump(self) -> Tuple[AdapterSubscriptionAction, ...]:
        """
        将 planner 动作按 claim→submit 顺序驱动到无新动作或等待 callback。

        Returns:
            Tuple[AdapterSubscriptionAction, ...]: 本次实际 claim 并调用 adapter 的动作。

        Raises:
            SubscriptionDispatchError: 多阶段转换超出有界轮次时抛出。
        """
        with self._lock:
            if self._dispatching:
                self._redispatch_requested = True
                return ()
            self._dispatching = True
            self._redispatch_requested = False
        dispatched = []  # type: list[AdapterSubscriptionAction]
        rerun = False
        try:
            for _round in range(self._max_dispatch_rounds):
                actions = self._manager.take_actions()
                if not actions:
                    break
                for action in actions:
                    with self._lock:
                        self._manager.claim_action(action)
                        self._waiting[action.action_id] = (
                            action,
                            self._clock() + self._ack_timeout_seconds,
                        )
                    dispatched.append(action)
                    self._submit_claimed_action(action)
            else:
                if self._manager.take_actions():
                    raise SubscriptionDispatchError("SUBSCRIPTION_DISPATCH_ROUND_LIMIT")
        finally:
            with self._lock:
                self._dispatching = False
                rerun = self._redispatch_requested
                self._redispatch_requested = False
            self._notify_state()
        if rerun:
            dispatched.extend(self.pump())
        return tuple(dispatched)

    def handle_adapter_response(self, response: AdapterSubscriptionResponse) -> None:
        """
        将真实 adapter callback 落入 confirm/reject 或 uncertain reconcile。

        Args:
            response: 带完整 action 的精确确认或拒绝回调。

        Returns:
            None: 结果落地并驱动后续补偿后返回。

        Raises:
            SubscriptionTransitionError: 旧 epoch、伪造动作或冲突重复回调时抛出。
        """
        if not isinstance(response, AdapterSubscriptionResponse):
            raise ValueError("response 必须为 AdapterSubscriptionResponse")
        action = response.action
        with self._lock:
            uncertain = self._uncertain.get(action.action_id)
            if uncertain is not None and uncertain != action:
                raise SubscriptionTransitionError("SUBSCRIPTION_UNCERTAIN_ACTION_MISMATCH")
            if uncertain is not None:
                if response.outcome is AdapterSubscriptionResponseOutcome.CONFIRMED:
                    self._manager.reconcile_action(
                        action,
                        applied=True,
                        reason=response.reason,
                    )
                else:
                    self._manager.reject_uncertain_action(
                        action,
                        code=response.code or "ADAPTER_REJECTED",
                        reason=response.reason,
                    )
                self._uncertain.pop(action.action_id, None)
            elif response.outcome is AdapterSubscriptionResponseOutcome.CONFIRMED:
                self._manager.confirm_action(action)
            else:
                self._manager.reject_action(
                    action,
                    code=response.code or "ADAPTER_REJECTED",
                    reason=response.reason,
                )
            self._waiting.pop(action.action_id, None)
        self._notify_state()
        self.pump()

    def expire_ack_timeouts(
        self, now: Optional[float] = None
    ) -> Tuple[AdapterSubscriptionAction, ...]:
        """
        将到期且未回调的已接受动作转为 uncertain，等待对账。

        Args:
            now: 可选单调时钟当前值；缺省时调用注入 clock。

        Returns:
            Tuple[AdapterSubscriptionAction, ...]: 本次转入 uncertain 的动作。
        """
        current = self._clock() if now is None else float(now)
        expired = []  # type: list[AdapterSubscriptionAction]
        with self._lock:
            due = tuple(
                action for action, deadline in self._waiting.values() if deadline <= current
            )
            for action in due:
                waiting = self._waiting.get(action.action_id)
                if waiting is None or waiting[0] != action:
                    continue
                self._manager.mark_action_uncertain(action, code="ACK_TIMEOUT")
                self._waiting.pop(action.action_id, None)
                self._uncertain[action.action_id] = action
                expired.append(action)
        if expired:
            self._notify_state()
        return tuple(expired)

    def reconcile_action(
        self,
        action_id: str,
        applied: bool,
        reason: Optional[str] = None,
    ) -> SubscriptionLeaseSnapshot:
        """
        使用查询或可信基线对账一个 uncertain 动作。

        Args:
            action_id: expire_ack_timeouts 产生的不确定动作 ID。
            applied: True 表示底层已生效，False 表示确定未生效。
            reason: 可选脱敏对账证据。

        Returns:
            SubscriptionLeaseSnapshot: 对账后的 manager 快照。

        Raises:
            SubscriptionLeaseNotFoundError: action_id 不是当前 uncertain 动作时抛出。
        """
        normalized_id = str(action_id).strip()
        with self._lock:
            action = self._uncertain.get(normalized_id)
            if action is None:
                raise SubscriptionLeaseNotFoundError("SUBSCRIPTION_UNCERTAIN_ACTION_NOT_FOUND")
            snapshot = self._manager.reconcile_action(action, applied=applied, reason=reason)
            self._uncertain.pop(normalized_id, None)
        self._notify_state()
        self.pump()
        return snapshot

    def _submit_claimed_action(self, action: AdapterSubscriptionAction) -> None:
        """
        提交单个已 claim 动作，区分本地拒绝、回调和不确定异常。

        Args:
            action: 已放入 waiting deadline 的精确动作。

        Returns:
            None: 提交结果已受控落地后返回。
        """
        try:
            result = self._adapter.submit(action, self.handle_adapter_response)
        except Exception as exc:
            with self._lock:
                waiting = self._waiting.pop(action.action_id, None)
                if waiting is not None:
                    self._manager.mark_action_uncertain(
                        action,
                        code="ADAPTER_SUBMIT_UNCERTAIN",
                        reason=type(exc).__name__,
                    )
                    self._uncertain[action.action_id] = action
            if waiting is not None:
                return
            raise SubscriptionDispatchError("ADAPTER_RAISED_AFTER_CALLBACK") from exc
        if not isinstance(result, AdapterSubscriptionSubmitResult):
            raise SubscriptionDispatchError("ADAPTER_SUBMIT_RESULT_INVALID")
        if result.accepted:
            return
        with self._lock:
            waiting = self._waiting.pop(action.action_id, None)
            if waiting is None:
                raise SubscriptionDispatchError("ADAPTER_REJECTED_AFTER_CALLBACK")
            self._manager.reject_action(
                action,
                code=result.code or "ADAPTER_LOCAL_REJECTED",
                reason=result.reason,
            )

    def _notify_state(self) -> None:
        """
        在不持有协调器锁时将不可变快照通知上层。

        Returns:
            None: 无 callback 或 callback 完成后返回。
        """
        callback = self._state_callback
        if callback is not None:
            callback(self._manager.snapshot())


__all__ = [
    "AdapterSubscriptionResponse",
    "AdapterSubscriptionResponseCallback",
    "AdapterSubscriptionResponseOutcome",
    "AdapterSubscriptionSubmitResult",
    "InMemorySubscriptionActionAdapter",
    "SubscriptionActionAdapter",
    "SubscriptionActionCoordinator",
    "SubscriptionDispatchError",
    "SubscriptionMonotonicClock",
    "SubscriptionStateCallback",
]
