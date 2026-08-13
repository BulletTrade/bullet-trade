"""
作者: BruceLee

文件职责: 管理实时行情的 session 租约、adapter union/refcount 与确认状态机。
主要输入: 已展开的模块、事件类型、市场及证券作用域，以及底层订阅/退订回执。
主要输出: 不重复的底层订阅动作、desired/sent/confirmed 快照和租约确认结果。
上游关系: 由实时 Feed、远程服务或厂商 adapter 将 MarketSubscriptionSpec 展开后调用。
下游关系: 将动作交给具体 SDK，并为 receipt、health、重连恢复和离线测试提供状态。
关键配置约定: symbol=None 表示该模块/事件/市场的 full scope；新 epoch 只清空
sent/confirmed，不清空有效租约；本模块不联网、不加载 SDK，也不执行交易。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from threading import RLock
from types import MappingProxyType
from typing import Dict, FrozenSet, Mapping, Optional, Sequence, Set, Tuple

from .models import MarketEventType


class SubscriptionLeaseError(RuntimeError):
    """表示订阅租约、底层动作或状态转换失败。"""


class SubscriptionLeaseConflictError(SubscriptionLeaseError):
    """表示 request ID、subscription ID 或载荷指纹发生幂等冲突。"""


class SubscriptionLeaseNotFoundError(SubscriptionLeaseError):
    """表示要退订或重试的明确租约/作用域不存在。"""


class SubscriptionTransitionError(SubscriptionLeaseError):
    """表示底层回执与当前已发送状态不一致。"""


class StaleSubscriptionActionError(SubscriptionTransitionError):
    """表示旧 session epoch 的迟到回执被当前状态机拒绝。"""


class AdapterSubscriptionOperation(str, Enum):
    """表示 adapter 需要执行的底层订阅或退订操作。"""

    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"


def _normalize_identifier(value: str, label: str) -> str:
    """
    规范化状态机中的必填标识符。

    Args:
        value: 待去除首尾空白的字符串。
        label: 非法时用于错误信息的字段名。

    Returns:
        str: 非空的规范化标识符。

    Raises:
        ValueError: 输入为空或只包含空白时抛出。
    """
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{label} 不能为空")
    return normalized


@dataclass(frozen=True)
class AdapterSubscriptionScope:
    """表示 adapter 级可合并计数的一个精确底层订阅作用域。"""

    module: str
    event_type: MarketEventType
    market: str
    symbol: Optional[str] = None

    def __post_init__(self) -> None:
        """
        规范化模块、事件、市场和可选证券代码。

        Returns:
            None: 字段规范化完成后返回。

        Raises:
            ValueError: 必填字段为空、事件为通配符或 symbol 与 market 冲突时抛出。
        """
        module = _normalize_identifier(self.module, "module").lower()
        market = _normalize_identifier(self.market, "market").upper()
        try:
            event_type = MarketEventType(self.event_type)
        except ValueError as exc:
            raise ValueError("event_type 包含未知枚举值") from exc
        if event_type is MarketEventType.ALL:
            raise ValueError("adapter scope 必须使用已展开的实际事件类型")
        symbol = None
        if self.symbol is not None:
            symbol = _normalize_identifier(self.symbol, "symbol").upper()
            if "." in symbol and symbol.rsplit(".", 1)[-1] != market:
                raise ValueError("symbol 后缀与 market 不一致")
        object.__setattr__(self, "module", module)
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(self, "market", market)
        object.__setattr__(self, "symbol", symbol)

    @property
    def is_full(self) -> bool:
        """
        返回当前作用域是否代表整个市场。

        Returns:
            bool: symbol 为 ``None`` 时为 True。
        """
        return self.symbol is None

    @property
    def group_key(self) -> Tuple[str, MarketEventType, str]:
        """
        返回用于 full/partial 覆盖判定的分组键。

        Returns:
            Tuple[str, MarketEventType, str]: 模块、事件类型和市场。
        """
        return (self.module, self.event_type, self.market)

    def covers(self, other: "AdapterSubscriptionScope") -> bool:
        """
        判断当前底层作用域是否完整覆盖另一作用域。

        Args:
            other: 待判定的目标作用域。

        Returns:
            bool: 同组 full 可覆盖任意 partial，partial 仅覆盖自身。
        """
        if self.group_key != other.group_key:
            return False
        return self.is_full or self.symbol == other.symbol

    def overlaps(self, other: "AdapterSubscriptionScope") -> bool:
        """
        判断两个作用域是否在同组中覆盖共同数据。

        Args:
            other: 待比较的 full 或 partial 作用域。

        Returns:
            bool: 同组且至少一方为 full，或两方 symbol 相同时为 True。
        """
        if self.group_key != other.group_key:
            return False
        return self.is_full or other.is_full or self.symbol == other.symbol


def _scope_sort_key(scope: AdapterSubscriptionScope) -> Tuple[str, str, str, str]:
    """
    为底层作用域生成可重现的排序键。

    Args:
        scope: 需要排序的 adapter 作用域。

    Returns:
        Tuple[str, str, str, str]: 按模块、事件、市场和 full/symbol 排序的键。
    """
    return (scope.module, scope.event_type.value, scope.market, scope.symbol or "")


def _normalize_scopes(
    scopes: Sequence[AdapterSubscriptionScope],
) -> Tuple[AdapterSubscriptionScope, ...]:
    """
    验证、去重并稳定排序租约作用域。

    Args:
        scopes: 一个订阅租约已展开的底层作用域。

    Returns:
        Tuple[AdapterSubscriptionScope, ...]: 去重后的稳定元组。

    Raises:
        ValueError: 列表为空或存在非 AdapterSubscriptionScope 值时抛出。
    """
    normalized: Set[AdapterSubscriptionScope] = set()
    for scope in scopes:
        if not isinstance(scope, AdapterSubscriptionScope):
            raise ValueError("scopes 必须全部为 AdapterSubscriptionScope")
        normalized.add(scope)
    if not normalized:
        raise ValueError("scopes 不能为空")
    return tuple(sorted(normalized, key=_scope_sort_key))


@dataclass(frozen=True)
class SessionSubscriptionLease:
    """保存一个客户端 session 中稳定且可部分退订的订阅租约。"""

    session_id: str
    subscription_id: str
    request_id: str
    payload_fingerprint: str
    initial_scopes: Tuple[AdapterSubscriptionScope, ...]
    active_scopes: Tuple[AdapterSubscriptionScope, ...]

    def __post_init__(self) -> None:
        """
        规范化租约标识并确保有效作用域是初始作用域的子集。

        Returns:
            None: 租约字段验证完成后返回。

        Raises:
            ValueError: 标识为空、初始作用域为空或有效作用域越界时抛出。
        """
        initial_scopes = _normalize_scopes(self.initial_scopes)
        active_scopes = tuple(sorted(set(self.active_scopes), key=_scope_sort_key))
        if not set(active_scopes).issubset(initial_scopes):
            raise ValueError("active_scopes 必须是 initial_scopes 的子集")
        object.__setattr__(self, "session_id", _normalize_identifier(self.session_id, "session_id"))
        object.__setattr__(
            self,
            "subscription_id",
            _normalize_identifier(self.subscription_id, "subscription_id"),
        )
        object.__setattr__(self, "request_id", _normalize_identifier(self.request_id, "request_id"))
        object.__setattr__(
            self,
            "payload_fingerprint",
            _normalize_identifier(self.payload_fingerprint, "payload_fingerprint"),
        )
        object.__setattr__(self, "initial_scopes", initial_scopes)
        object.__setattr__(self, "active_scopes", active_scopes)

    @property
    def is_active(self) -> bool:
        """
        返回租约是否仍含有至少一个有效作用域。

        Returns:
            bool: active_scopes 非空时为 True。
        """
        return bool(self.active_scopes)


@dataclass(frozen=True)
class AdapterSubscriptionAction:
    """表示已从状态机取出、必须等待底层逐项回执的单个动作。"""

    action_id: str
    session_epoch: str
    operation: AdapterSubscriptionOperation
    scope: AdapterSubscriptionScope
    reason: str


@dataclass(frozen=True)
class AdapterSubscriptionFailure:
    """保存当前 epoch 内已发送但被拒绝的底层动作。"""

    action: AdapterSubscriptionAction
    code: str
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        """
        验证拒绝错误码并规范化可选原因。

        Returns:
            None: 失败信息规范化完成后返回。

        Raises:
            ValueError: code 为空时抛出。
        """
        object.__setattr__(self, "code", _normalize_identifier(self.code, "code"))
        if self.reason is not None:
            object.__setattr__(self, "reason", str(self.reason).strip() or None)


@dataclass(frozen=True)
class SubscriptionLeaseSnapshot:
    """提供当前租约、引用计数与 adapter 三集合的不可变诊断快照。"""

    session_epoch: str
    leases: Tuple[SessionSubscriptionLease, ...]
    desired: FrozenSet[AdapterSubscriptionScope]
    effective_desired: FrozenSet[AdapterSubscriptionScope]
    sent: FrozenSet[AdapterSubscriptionScope]
    confirmed: FrozenSet[AdapterSubscriptionScope]
    pending_subscribe: FrozenSet[AdapterSubscriptionScope]
    pending_unsubscribe: FrozenSet[AdapterSubscriptionScope]
    refcounts: Mapping[AdapterSubscriptionScope, int] = field(default_factory=dict)
    failures: Tuple[AdapterSubscriptionFailure, ...] = ()

    def __post_init__(self) -> None:
        """
        复制并冻结快照的引用计数映射。

        Returns:
            None: refcounts 转换为只读映射后返回。
        """
        object.__setattr__(self, "refcounts", MappingProxyType(dict(self.refcounts)))


class SubscriptionLeaseManager:
    """
    在线程安全边界内管理 session lease 和 adapter 级覆盖安全订阅转换。

    状态机不直接调用厂商 SDK。调用方从 ``take_actions`` 原子取出动作，
    执行后通过 ``confirm_action`` 或 ``reject_action`` 送回逐项回执。
    """

    def __init__(self, session_epoch: str) -> None:
        """
        初始化一个空租约集合和当前 adapter session epoch。

        Args:
            session_epoch: 当前 adapter 登录/连接世代的稳定标识。

        Returns:
            None: 状态机初始化完成后返回。
        """
        self._lock = RLock()
        self._session_epoch = _normalize_identifier(session_epoch, "session_epoch")
        self._leases: Dict[Tuple[str, str], SessionSubscriptionLease] = {}
        self._request_index: Dict[
            Tuple[str, str], Tuple[str, str, Tuple[AdapterSubscriptionScope, ...]]
        ] = {}
        self._sent: Set[AdapterSubscriptionScope] = set()
        self._confirmed: Set[AdapterSubscriptionScope] = set()
        self._inflight: Dict[str, AdapterSubscriptionAction] = {}
        self._completed: Dict[str, str] = {}
        self._failures: Dict[
            Tuple[AdapterSubscriptionOperation, AdapterSubscriptionScope],
            AdapterSubscriptionFailure,
        ] = {}
        self._action_sequence = 0

    @property
    def session_epoch(self) -> str:
        """
        返回当前 adapter session epoch。

        Returns:
            str: 构造时或最近一次 begin_session_epoch 设置的值。
        """
        with self._lock:
            return self._session_epoch

    def add_lease(
        self,
        session_id: str,
        subscription_id: str,
        request_id: str,
        payload_fingerprint: str,
        scopes: Sequence[AdapterSubscriptionScope],
    ) -> SessionSubscriptionLease:
        """
        新增租约，或对相同 session/request/fingerprint 幂等返回原租约。

        Args:
            session_id: 客户端或策略 session 标识。
            subscription_id: 服务端分配的订阅租约 ID。
            request_id: 客户端稳定请求 ID。
            payload_fingerprint: 已规范化 MarketSubscriptionSpec 的语义指纹。
            scopes: 通配事件和 selector 已展开后的精确作用域。

        Returns:
            SessionSubscriptionLease: 新建或原有的稳定租约。

        Raises:
            SubscriptionLeaseConflictError: 同 request ID 载荷改变或 subscription ID 重用时抛出。
        """
        normalized_session = _normalize_identifier(session_id, "session_id")
        normalized_subscription = _normalize_identifier(subscription_id, "subscription_id")
        normalized_request = _normalize_identifier(request_id, "request_id")
        normalized_fingerprint = _normalize_identifier(payload_fingerprint, "payload_fingerprint")
        normalized_scopes = _normalize_scopes(scopes)
        request_key = (normalized_session, normalized_request)
        lease_key = (normalized_session, normalized_subscription)
        with self._lock:
            indexed = self._request_index.get(request_key)
            if indexed is not None:
                old_fingerprint, old_subscription, old_scopes = indexed
                if old_fingerprint != normalized_fingerprint or old_scopes != normalized_scopes:
                    raise SubscriptionLeaseConflictError(
                        "SUBSCRIPTION_REQUEST_CONFLICT: 同 request_id 的语义已改变"
                    )
                return self._leases[(normalized_session, old_subscription)]
            if lease_key in self._leases:
                raise SubscriptionLeaseConflictError(
                    "SUBSCRIPTION_ID_CONFLICT: subscription_id 已被其他请求使用"
                )
            lease = SessionSubscriptionLease(
                session_id=normalized_session,
                subscription_id=normalized_subscription,
                request_id=normalized_request,
                payload_fingerprint=normalized_fingerprint,
                initial_scopes=normalized_scopes,
                active_scopes=normalized_scopes,
            )
            self._leases[lease_key] = lease
            self._request_index[request_key] = (
                normalized_fingerprint,
                normalized_subscription,
                normalized_scopes,
            )
            return lease

    def get_lease(self, session_id: str, subscription_id: str) -> SessionSubscriptionLease:
        """
        按 session 和 subscription ID 读取当前租约，包括已取消墓碑。

        Args:
            session_id: 租约所属 session ID。
            subscription_id: 租约 ID。

        Returns:
            SessionSubscriptionLease: 当前不可变租约快照。

        Raises:
            SubscriptionLeaseNotFoundError: 租约不存在时抛出。
        """
        key = (
            _normalize_identifier(session_id, "session_id"),
            _normalize_identifier(subscription_id, "subscription_id"),
        )
        with self._lock:
            try:
                return self._leases[key]
            except KeyError as exc:
                raise SubscriptionLeaseNotFoundError(
                    f"SUBSCRIPTION_NOT_FOUND: session={key[0]}, subscription={key[1]}"
                ) from exc

    def remove_scopes(
        self,
        session_id: str,
        subscription_id: str,
        scopes: Sequence[AdapterSubscriptionScope],
    ) -> SessionSubscriptionLease:
        """
        从一个明确租约中部分移除指定作用域。

        Args:
            session_id: 租约所属 session ID。
            subscription_id: 需要修改的租约 ID。
            scopes: 必须完整命中当前 active_scopes 的退订集合。

        Returns:
            SessionSubscriptionLease: 部分退订后的租约；全部移除时 is_active=False。

        Raises:
            SubscriptionLeaseNotFoundError: 租约不存在、已取消或作用域未命中时抛出。
        """
        normalized = set(_normalize_scopes(scopes))
        with self._lock:
            lease = self.get_lease(session_id, subscription_id)
            active = set(lease.active_scopes)
            if not active:
                raise SubscriptionLeaseNotFoundError("SUBSCRIPTION_ALREADY_CANCELED")
            missing = normalized - active
            if missing:
                raise SubscriptionLeaseNotFoundError("SUBSCRIPTION_SCOPE_NOT_FOUND: 退订作用域不属于当前租约")
            updated = replace(
                lease,
                active_scopes=tuple(sorted(active - normalized, key=_scope_sort_key)),
            )
            self._leases[(lease.session_id, lease.subscription_id)] = updated
            return updated

    def remove_lease(self, session_id: str, subscription_id: str) -> SessionSubscriptionLease:
        """
        取消一个明确 subscription ID 的全部作用域。

        Args:
            session_id: 租约所属 session ID。
            subscription_id: 需要取消的租约 ID。

        Returns:
            SessionSubscriptionLease: active_scopes 为空的墓碑租约。

        Raises:
            SubscriptionLeaseNotFoundError: 租约不存在或已取消时抛出。
        """
        with self._lock:
            lease = self.get_lease(session_id, subscription_id)
            if not lease.active_scopes:
                raise SubscriptionLeaseNotFoundError("SUBSCRIPTION_ALREADY_CANCELED")
            updated = replace(lease, active_scopes=())
            self._leases[(lease.session_id, lease.subscription_id)] = updated
            return updated

    def remove_all(
        self,
        session_id: str,
        module: Optional[str] = None,
        event_types: Optional[Sequence[MarketEventType]] = None,
        market: Optional[str] = None,
    ) -> Tuple[SessionSubscriptionLease, ...]:
        """
        清空当前 session 的全部或指定模块/事件/市场范围租约。

        Args:
            session_id: 只允许影响该 session 的标识。
            module: 可选模块过滤器，例如 l1 或 l2。
            event_types: 可选已展开事件类型过滤器。
            market: 可选标准市场过滤器。

        Returns:
            Tuple[SessionSubscriptionLease, ...]: 所有发生变更的租约，按 subscription ID 排序。

        Raises:
            ValueError: event_types 为空或含通配符时抛出。
        """
        normalized_session = _normalize_identifier(session_id, "session_id")
        normalized_module = (
            _normalize_identifier(module, "module").lower() if module is not None else None
        )
        normalized_market = (
            _normalize_identifier(market, "market").upper() if market is not None else None
        )
        normalized_events: Optional[Set[MarketEventType]] = None
        if event_types is not None:
            normalized_events = {MarketEventType(item) for item in event_types}
            if not normalized_events or MarketEventType.ALL in normalized_events:
                raise ValueError("event_types 过滤器必须为非空的实际事件集合")
        with self._lock:
            updated_leases = []
            for key, lease in sorted(self._leases.items()):
                if lease.session_id != normalized_session or not lease.active_scopes:
                    continue
                removed = {
                    scope
                    for scope in lease.active_scopes
                    if (normalized_module is None or scope.module == normalized_module)
                    and (normalized_events is None or scope.event_type in normalized_events)
                    and (normalized_market is None or scope.market == normalized_market)
                }
                if not removed:
                    continue
                remaining = set(lease.active_scopes) - removed
                updated = replace(
                    lease,
                    active_scopes=tuple(sorted(remaining, key=_scope_sort_key)),
                )
                self._leases[key] = updated
                updated_leases.append(updated)
            return tuple(sorted(updated_leases, key=lambda item: item.subscription_id))

    def begin_session_epoch(self, session_epoch: str) -> bool:
        """
        进入新 adapter epoch，保留 desired leases 并清空旧发送/确认证据。

        Args:
            session_epoch: 重连、重新登录或日切后的新世代标识。

        Returns:
            bool: epoch 真正变更时为 True，重复传入当前值时为 False。

        Side Effects:
            清空 sent/confirmed、待回执动作和当前 epoch 失败，但不恢复已取消租约。
        """
        normalized = _normalize_identifier(session_epoch, "session_epoch")
        with self._lock:
            if normalized == self._session_epoch:
                return False
            self._session_epoch = normalized
            self._sent.clear()
            self._confirmed.clear()
            self._inflight.clear()
            self._completed.clear()
            self._failures.clear()
            return True

    def take_actions(self) -> Tuple[AdapterSubscriptionAction, ...]:
        """
        原子规划并标记一批底层动作为已发送。

        Returns:
            Tuple[AdapterSubscriptionAction, ...]: 先 subscribe、后 unsubscribe 的稳定动作序列。

        Notes:
            只有当新目标作用域已 confirmed 并能覆盖仍有效租约时，才生成会降低
            旧覆盖范围的 unsubscribe，因此 full 转 partial 至少分两轮执行。
        """
        with self._lock:
            desired = self._desired_scopes()
            effective = self._effective_desired(desired)
            actions = []
            for scope in sorted(effective, key=_scope_sort_key):
                failure_key = (AdapterSubscriptionOperation.SUBSCRIBE, scope)
                if scope in self._sent or failure_key in self._failures:
                    continue
                action = self._new_action(
                    AdapterSubscriptionOperation.SUBSCRIBE,
                    scope,
                    self._subscribe_reason(scope),
                )
                self._sent.add(scope)
                self._inflight[action.action_id] = action
                actions.append(action)

            for scope in sorted(self._confirmed - effective, key=_scope_sort_key):
                failure_key = (AdapterSubscriptionOperation.UNSUBSCRIBE, scope)
                if failure_key in self._failures or self._has_inflight(
                    AdapterSubscriptionOperation.UNSUBSCRIBE, scope
                ):
                    continue
                if not self._can_remove_without_gap(scope, desired):
                    continue
                action = self._new_action(
                    AdapterSubscriptionOperation.UNSUBSCRIBE,
                    scope,
                    self._unsubscribe_reason(scope, desired),
                )
                self._inflight[action.action_id] = action
                actions.append(action)
            return tuple(actions)

    def confirm_action(self, action: AdapterSubscriptionAction) -> SubscriptionLeaseSnapshot:
        """
        幂等确认一个已发送的底层订阅或退订动作。

        Args:
            action: ``take_actions`` 返回且底层已逐项确认的动作。

        Returns:
            SubscriptionLeaseSnapshot: 应用确认后的完整状态快照。

        Raises:
            StaleSubscriptionActionError: action 属于旧 epoch 时抛出。
            SubscriptionTransitionError: action 未发送或先前已被拒绝时抛出。
        """
        with self._lock:
            if self._completed.get(action.action_id) == "confirmed":
                return self.snapshot()
            self._require_inflight_action(action)
            if action.operation is AdapterSubscriptionOperation.SUBSCRIBE:
                self._sent.add(action.scope)
                self._confirmed.add(action.scope)
            else:
                self._sent.discard(action.scope)
                self._confirmed.discard(action.scope)
            self._inflight.pop(action.action_id, None)
            self._failures.pop((action.operation, action.scope), None)
            self._completed[action.action_id] = "confirmed"
            return self.snapshot()

    def reject_action(
        self,
        action: AdapterSubscriptionAction,
        code: str,
        reason: Optional[str] = None,
    ) -> SubscriptionLeaseSnapshot:
        """
        幂等记录底层动作的逐项拒绝，且不在当前 epoch 无界重试。

        Args:
            action: ``take_actions`` 返回且底层明确拒绝的动作。
            code: 稳定拒绝错误码。
            reason: 可选脱敏原因。

        Returns:
            SubscriptionLeaseSnapshot: 包含失败明细且保留安全覆盖的状态快照。

        Raises:
            StaleSubscriptionActionError: action 属于旧 epoch 时抛出。
            SubscriptionTransitionError: action 未发送或先前已确认时抛出。
        """
        with self._lock:
            if self._completed.get(action.action_id) == "rejected":
                return self.snapshot()
            self._require_inflight_action(action)
            failure = AdapterSubscriptionFailure(action=action, code=code, reason=reason)
            self._inflight.pop(action.action_id, None)
            self._failures[(action.operation, action.scope)] = failure
            self._completed[action.action_id] = "rejected"
            return self.snapshot()

    def retry_failed(
        self,
        operation: AdapterSubscriptionOperation,
        scope: AdapterSubscriptionScope,
    ) -> None:
        """
        由上层显式允许当前 epoch 重试一个已拒绝动作。

        Args:
            operation: 需要重试的 subscribe 或 unsubscribe。
            scope: 被拒绝的精确 adapter 作用域。

        Returns:
            None: 失败门闩清除后返回，下次 take_actions 生成新 action ID。

        Raises:
            SubscriptionLeaseNotFoundError: 当前没有对应失败时抛出。
        """
        normalized_operation = AdapterSubscriptionOperation(operation)
        failure_key = (normalized_operation, scope)
        with self._lock:
            if failure_key not in self._failures:
                raise SubscriptionLeaseNotFoundError("SUBSCRIPTION_FAILURE_NOT_FOUND")
            self._failures.pop(failure_key)
            if (
                normalized_operation is AdapterSubscriptionOperation.SUBSCRIBE
                and scope not in self._confirmed
            ):
                self._sent.discard(scope)

    def is_lease_confirmed(self, session_id: str, subscription_id: str) -> bool:
        """
        判断一个有效租约的所有作用域是否已被 adapter 确认覆盖。

        Args:
            session_id: 租约所属 session ID。
            subscription_id: 租约 ID。

        Returns:
            bool: 租约非空且每个 partial/full 意图都被 confirmed scope 覆盖时为 True。
        """
        with self._lock:
            lease = self.get_lease(session_id, subscription_id)
            return bool(lease.active_scopes) and all(
                any(confirmed.covers(scope) for confirmed in self._confirmed)
                for scope in lease.active_scopes
            )

    def snapshot(self) -> SubscriptionLeaseSnapshot:
        """
        返回租约、desired/sent/confirmed、引用计数和待回执动作的只读快照。

        Returns:
            SubscriptionLeaseSnapshot: 可供 receipt、health 和测试消费的不可变快照。
        """
        with self._lock:
            desired = self._desired_scopes()
            inflight_actions = tuple(self._inflight.values())
            pending_subscribe = {
                action.scope
                for action in inflight_actions
                if action.operation is AdapterSubscriptionOperation.SUBSCRIBE
            }
            pending_unsubscribe = {
                action.scope
                for action in inflight_actions
                if action.operation is AdapterSubscriptionOperation.UNSUBSCRIBE
            }
            return SubscriptionLeaseSnapshot(
                session_epoch=self._session_epoch,
                leases=tuple(
                    sorted(
                        self._leases.values(),
                        key=lambda item: (item.session_id, item.subscription_id),
                    )
                ),
                desired=frozenset(desired),
                effective_desired=frozenset(self._effective_desired(desired)),
                sent=frozenset(self._sent),
                confirmed=frozenset(self._confirmed),
                pending_subscribe=frozenset(pending_subscribe),
                pending_unsubscribe=frozenset(pending_unsubscribe),
                refcounts=self._refcounts(),
                failures=tuple(
                    sorted(
                        self._failures.values(),
                        key=lambda item: (
                            item.action.operation.value,
                            _scope_sort_key(item.action.scope),
                        ),
                    )
                ),
            )

    def _desired_scopes(self) -> Set[AdapterSubscriptionScope]:
        """
        从全部有效 session leases 聚合 adapter 逻辑 desired 作用域。

        Returns:
            Set[AdapterSubscriptionScope]: 包含 full/partial 重叠意图的并集。
        """
        return {scope for lease in self._leases.values() for scope in lease.active_scopes}

    def _refcounts(self) -> Dict[AdapterSubscriptionScope, int]:
        """
        计算每个逻辑作用域被多少个 session lease 引用。

        Returns:
            Dict[AdapterSubscriptionScope, int]: 仅包含引用计数大于零的映射。
        """
        counts: Dict[AdapterSubscriptionScope, int] = {}
        for lease in self._leases.values():
            for scope in lease.active_scopes:
                counts[scope] = counts.get(scope, 0) + 1
        return counts

    def _effective_desired(
        self, desired: Set[AdapterSubscriptionScope]
    ) -> Set[AdapterSubscriptionScope]:
        """
        将逻辑 desired 压缩为不会向 SDK 重复订阅的 adapter 目标并集。

        Args:
            desired: 从全部有效租约直接聚合的 full/partial 集合。

        Returns:
            Set[AdapterSubscriptionScope]: 同组存在 full 时抑制 partial 的最小底层目标集。
        """
        full_groups = {scope.group_key for scope in desired if scope.is_full}
        return {scope for scope in desired if scope.is_full or scope.group_key not in full_groups}

    def _new_action(
        self,
        operation: AdapterSubscriptionOperation,
        scope: AdapterSubscriptionScope,
        reason: str,
    ) -> AdapterSubscriptionAction:
        """
        分配当前 epoch 内单调的底层动作 ID。

        Args:
            operation: subscribe 或 unsubscribe。
            scope: 动作针对的精确作用域。
            reason: 可审计的规划原因。

        Returns:
            AdapterSubscriptionAction: 尚未获得回执的新动作。
        """
        self._action_sequence += 1
        return AdapterSubscriptionAction(
            action_id=f"{self._session_epoch}:{self._action_sequence}",
            session_epoch=self._session_epoch,
            operation=operation,
            scope=scope,
            reason=reason,
        )

    def _has_inflight(
        self,
        operation: AdapterSubscriptionOperation,
        scope: AdapterSubscriptionScope,
    ) -> bool:
        """
        判断同一操作和作用域是否已等待底层回执。

        Args:
            operation: 需要查询的动作类型。
            scope: 需要查询的精确作用域。

        Returns:
            bool: 有相同待回执动作时为 True。
        """
        return any(
            action.operation is operation and action.scope == scope
            for action in self._inflight.values()
        )

    def _subscribe_reason(self, scope: AdapterSubscriptionScope) -> str:
        """
        为新订阅动作生成 full/partial 转换可审计原因。

        Args:
            scope: 即将发送的目标作用域。

        Returns:
            str: 区分物化 partial、提升 full 或普通建立的稳定原因。
        """
        same_group = {
            current for current in self._confirmed if current.group_key == scope.group_key
        }
        if not scope.is_full and any(current.is_full for current in same_group):
            return "materialize_partial_before_full_unsubscribe"
        if scope.is_full and any(not current.is_full for current in same_group):
            return "confirm_full_before_partial_unsubscribe"
        return "establish_desired_scope"

    def _unsubscribe_reason(
        self,
        scope: AdapterSubscriptionScope,
        desired: Set[AdapterSubscriptionScope],
    ) -> str:
        """
        为安全退订动作生成可审计的覆盖转换原因。

        Args:
            scope: 即将退订的旧底层作用域。
            desired: 当前仍有效的逻辑租约并集。

        Returns:
            str: full/partial 转换或普通释放的稳定原因。
        """
        overlapping = {item for item in desired if scope.overlaps(item)}
        if scope.is_full and any(not item.is_full for item in overlapping):
            return "remove_full_after_partial_materialized"
        if not scope.is_full and any(item.is_full for item in overlapping):
            return "remove_partial_after_full_confirmed"
        return "release_unreferenced_scope"

    def _can_remove_without_gap(
        self,
        scope: AdapterSubscriptionScope,
        desired: Set[AdapterSubscriptionScope],
    ) -> bool:
        """
        校验退订旧作用域不会破坏仍有效租约的已确认覆盖。

        Args:
            scope: 候选退订的 confirmed 作用域。
            desired: 当前逻辑 desired full/partial 作用域。

        Returns:
            bool: 所有与旧作用域重叠的意图都已由其他 confirmed scope 覆盖时为 True。
        """
        alternatives = self._confirmed - {scope}
        overlapping = {item for item in desired if scope.overlaps(item)}
        for item in overlapping:
            coverage_point = scope if item.is_full and not scope.is_full else item
            if not any(candidate.covers(coverage_point) for candidate in alternatives):
                return False
        return True

    def _require_inflight_action(self, action: AdapterSubscriptionAction) -> None:
        """
        验证回执动作属于当前 epoch 且与已发送记录逐字段一致。

        Args:
            action: 待确认或拒绝的动作。

        Returns:
            None: 动作合法时返回。

        Raises:
            StaleSubscriptionActionError: action epoch 已过期时抛出。
            SubscriptionTransitionError: action 未在 inflight 中、内容不同或已以反向结果完成时抛出。
        """
        if action.session_epoch != self._session_epoch:
            raise StaleSubscriptionActionError(
                f"STALE_SUBSCRIPTION_ACTION: expected={self._session_epoch}, "
                f"actual={action.session_epoch}"
            )
        completed = self._completed.get(action.action_id)
        if completed is not None:
            raise SubscriptionTransitionError(f"SUBSCRIPTION_ACTION_ALREADY_{completed.upper()}")
        current = self._inflight.get(action.action_id)
        if current is None or current != action:
            raise SubscriptionTransitionError("SUBSCRIPTION_ACTION_NOT_INFLIGHT")


__all__ = [
    "AdapterSubscriptionAction",
    "AdapterSubscriptionFailure",
    "AdapterSubscriptionOperation",
    "AdapterSubscriptionScope",
    "SessionSubscriptionLease",
    "StaleSubscriptionActionError",
    "SubscriptionLeaseConflictError",
    "SubscriptionLeaseError",
    "SubscriptionLeaseManager",
    "SubscriptionLeaseNotFoundError",
    "SubscriptionLeaseSnapshot",
    "SubscriptionTransitionError",
]
