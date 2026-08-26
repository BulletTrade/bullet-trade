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

from collections import OrderedDict
from dataclasses import dataclass, field, replace
from enum import Enum
from threading import RLock
from types import MappingProxyType
from typing import Dict, FrozenSet, Mapping, Optional
from typing import OrderedDict as OrderedDictType
from typing import Sequence, Set, Tuple

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


def _normalize_nonnegative_int(value: int, label: str) -> int:
    """
    校验并返回非布尔的非负整数。

    Args:
        value: 待校验的整数值。
        label: 非法时用于错误信息的字段名。

    Returns:
        int: 原始非负整数。

    Raises:
        ValueError: 值为 bool、非 int 或小于零时抛出。
    """
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} 必须为非负整数")
    return value


def _normalize_positive_int(value: int, label: str) -> int:
    """
    校验并返回非布尔的正整数。

    Args:
        value: 待校验的整数值。
        label: 非法时用于错误信息的字段名。

    Returns:
        int: 原始正整数。

    Raises:
        ValueError: 值为 bool、非 int 或不大于零时抛出。
    """
    normalized = _normalize_nonnegative_int(value, label)
    if normalized == 0:
        raise ValueError(f"{label} 必须为正整数")
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
    """表示带期望版本、须先 claim 再发送的单个 adapter 动作。"""

    action_id: str
    session_epoch: str
    desired_revision: int
    operation: AdapterSubscriptionOperation
    scope: AdapterSubscriptionScope
    reason: str

    def __post_init__(self) -> None:
        """
        规范化动作标识、世代、操作类型、作用域和审计原因。

        Returns:
            None: 动作字段校验完成后返回。

        Raises:
            ValueError: 标识为空、revision 非法或 scope 类型错误时抛出。
        """
        if not isinstance(self.scope, AdapterSubscriptionScope):
            raise ValueError("scope 必须为 AdapterSubscriptionScope")
        object.__setattr__(self, "action_id", _normalize_identifier(self.action_id, "action_id"))
        object.__setattr__(
            self,
            "session_epoch",
            _normalize_identifier(self.session_epoch, "session_epoch"),
        )
        object.__setattr__(
            self,
            "desired_revision",
            _normalize_nonnegative_int(self.desired_revision, "desired_revision"),
        )
        object.__setattr__(
            self,
            "operation",
            AdapterSubscriptionOperation(self.operation),
        )
        object.__setattr__(self, "reason", _normalize_identifier(self.reason, "reason"))


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
class _UncertainSubscriptionAction:
    """保存已 claim 且 ACK 结果不确定、必须对账的动作。"""

    action: AdapterSubscriptionAction
    code: str
    reason: Optional[str] = None


@dataclass(frozen=True)
class _CompletedSubscriptionAction:
    """保存有界幂等历史中的精确动作、结果和可选诊断。"""

    action: AdapterSubscriptionAction
    outcome: str
    code: Optional[str] = None
    reason: Optional[str] = None


@dataclass(frozen=True)
class SubscriptionLeaseSnapshot:
    """提供当前租约、引用计数与 adapter 三集合的不可变诊断快照。"""

    session_epoch: str
    desired_revision: int
    leases: Tuple[SessionSubscriptionLease, ...]
    desired: FrozenSet[AdapterSubscriptionScope]
    effective_desired: FrozenSet[AdapterSubscriptionScope]
    sent: FrozenSet[AdapterSubscriptionScope]
    confirmed: FrozenSet[AdapterSubscriptionScope]
    stable_confirmed: FrozenSet[AdapterSubscriptionScope]
    planned_subscribe: FrozenSet[AdapterSubscriptionScope]
    planned_unsubscribe: FrozenSet[AdapterSubscriptionScope]
    pending_subscribe: FrozenSet[AdapterSubscriptionScope]
    pending_unsubscribe: FrozenSet[AdapterSubscriptionScope]
    uncertain_subscribe: FrozenSet[AdapterSubscriptionScope]
    uncertain_unsubscribe: FrozenSet[AdapterSubscriptionScope]
    refcounts: Mapping[AdapterSubscriptionScope, int] = field(default_factory=dict)
    failures: Tuple[AdapterSubscriptionFailure, ...] = ()

    def __post_init__(self) -> None:
        """
        复制并冻结快照的引用计数映射。

        Returns:
            None: refcounts 转换为只读映射后返回。
        """
        object.__setattr__(
            self,
            "desired_revision",
            _normalize_nonnegative_int(self.desired_revision, "desired_revision"),
        )
        object.__setattr__(self, "refcounts", MappingProxyType(dict(self.refcounts)))


class SubscriptionLeaseManager:
    """
    在线程安全边界内管理 session lease 和 adapter 级覆盖安全订阅转换。

    状态机不直接调用厂商 SDK。调用方从 ``take_actions`` 取得纯计划，发送前
    必须 ``claim_action``，随后通过 confirm/reject 或 uncertain/reconcile 闭环。
    """

    def __init__(
        self,
        session_epoch: str,
        completed_history_limit: int = 4096,
        max_leases_per_session: int = 1024,
        max_total_leases: int = 16384,
        max_requests_per_session: int = 1024,
        max_total_requests: int = 16384,
        max_scopes_per_lease: int = 4096,
        max_scope_references_per_session: int = 8192,
        max_total_scope_references: int = 65536,
        max_retained_adapter_scopes: int = 65536,
    ) -> None:
        """
        初始化一个空租约集合和当前 adapter session epoch。

        Args:
            session_epoch: 当前 adapter 登录/连接世代的稳定标识。
            completed_history_limit: 当前 epoch 内保留的幂等完成记录上限。
            max_leases_per_session: 单个 session 可保留的有效 lease 与墓碑总数。
            max_total_leases: manager 可保留的全部 session lease 与墓碑总数。
            max_requests_per_session: 单个 session 可保留的幂等 request 索引上限。
            max_total_requests: manager 可保留的全部幂等 request 索引上限。
            max_scopes_per_lease: 单个 lease 可持久化的展开 scope 数量上限。
            max_scope_references_per_session: 单 session 全部 lease 的 scope 引用上限。
            max_total_scope_references: manager 全部 lease 的 scope 引用总上限。
            max_retained_adapter_scopes: desired 与已发送/待对账等适配器状态
                可保留的唯一 scope 全局上限。

        Returns:
            None: 状态机初始化完成后返回。
        """
        self._lock = RLock()
        self._session_epoch = _normalize_identifier(session_epoch, "session_epoch")
        self._desired_revision = 0
        self._completed_history_limit = _normalize_positive_int(
            completed_history_limit,
            "completed_history_limit",
        )
        self._max_leases_per_session = _normalize_positive_int(
            max_leases_per_session,
            "max_leases_per_session",
        )
        self._max_total_leases = _normalize_positive_int(
            max_total_leases,
            "max_total_leases",
        )
        self._max_requests_per_session = _normalize_positive_int(
            max_requests_per_session,
            "max_requests_per_session",
        )
        self._max_total_requests = _normalize_positive_int(
            max_total_requests,
            "max_total_requests",
        )
        self._max_scopes_per_lease = _normalize_positive_int(
            max_scopes_per_lease,
            "max_scopes_per_lease",
        )
        self._max_scope_references_per_session = _normalize_positive_int(
            max_scope_references_per_session,
            "max_scope_references_per_session",
        )
        self._max_total_scope_references = _normalize_positive_int(
            max_total_scope_references,
            "max_total_scope_references",
        )
        self._max_retained_adapter_scopes = _normalize_positive_int(
            max_retained_adapter_scopes,
            "max_retained_adapter_scopes",
        )
        self._leases: Dict[Tuple[str, str], SessionSubscriptionLease] = {}
        self._request_index: Dict[
            Tuple[str, str], Tuple[str, str, Tuple[AdapterSubscriptionScope, ...]]
        ] = {}
        self._sent: Set[AdapterSubscriptionScope] = set()
        self._confirmed: Set[AdapterSubscriptionScope] = set()
        self._planned: Dict[str, AdapterSubscriptionAction] = {}
        self._inflight: Dict[str, AdapterSubscriptionAction] = {}
        self._uncertain: Dict[str, _UncertainSubscriptionAction] = {}
        self._completed: OrderedDictType[str, _CompletedSubscriptionAction] = OrderedDict()
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

    @property
    def desired_revision(self) -> int:
        """
        返回当前有效租约意图的单调修订号。

        Returns:
            int: 每次租约作用域实际改变后递增的修订号。
        """
        with self._lock:
            return self._desired_revision

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
        if len(scopes) > self._max_scopes_per_lease:
            raise SubscriptionLeaseError("SUBSCRIPTION_LEASE_SCOPE_LIMIT")
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
            self._require_registration_capacity(
                normalized_session,
                new_scopes=normalized_scopes,
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
            self._advance_desired_revision()
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
            self._advance_desired_revision()
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
            self._advance_desired_revision()
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
            if updated_leases:
                self._advance_desired_revision()
            return tuple(sorted(updated_leases, key=lambda item: item.subscription_id))

    def close_session(self, session_id: str) -> Tuple[SessionSubscriptionLease, ...]:
        """
        关闭 session，原子移除其有效意图、lease 墓碑与幂等 request 索引。

        Args:
            session_id: 已由上层连接生命周期判定为永久关闭的 session 标识。

        Returns:
            Tuple[SessionSubscriptionLease, ...]: 被清理的全部 lease，按 ID 排序。

        Notes:
            上层只能在 session binding 不会再恢复时调用。本方法保留 adapter 级
            claimed/uncertain 动作，因为它们可能已经发送；若存在 active scope，
            revision 会推进，随后由 planner 按其他 session 的最新 union 补偿。
            close 后该 session 的旧 request 幂等墓碑不再保留。
        """
        normalized_session = _normalize_identifier(session_id, "session_id")
        with self._lock:
            lease_keys = [key for key in self._leases if key[0] == normalized_session]
            removed = tuple(
                sorted(
                    (self._leases[key] for key in lease_keys),
                    key=lambda item: item.subscription_id,
                )
            )
            had_active = any(lease.active_scopes for lease in removed)
            for key in lease_keys:
                self._leases.pop(key, None)
            request_keys = [key for key in self._request_index if key[0] == normalized_session]
            for key in request_keys:
                self._request_index.pop(key, None)
            if had_active:
                self._advance_desired_revision()
            else:
                self._purge_obsolete_subscribe_state()
            return removed

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
            self._planned.clear()
            self._inflight.clear()
            self._uncertain.clear()
            self._completed.clear()
            self._failures.clear()
            return True

    def take_actions(self) -> Tuple[AdapterSubscriptionAction, ...]:
        """
        原子规划一批底层动作，但不把任何动作标记为已发送。

        Returns:
            Tuple[AdapterSubscriptionAction, ...]: 当前仍有效、先订阅后退订的稳定计划。

        Notes:
            dispatcher 必须对每个返回值先调用 ``claim_action``，claim 成功后立即
            调用 SDK；未 claim 的计划不属于 sent/inflight，也不能接受底层回执。
            只有当新目标作用域已 confirmed 并能覆盖仍有效租约时，才生成会降低
            旧覆盖范围的 unsubscribe，因此 full 转 partial 至少分两轮执行。
        """
        with self._lock:
            desired = self._desired_scopes()
            effective = self._effective_desired(desired)
            for action_id, action in tuple(self._planned.items()):
                if not self._is_action_currently_needed(
                    action,
                    desired,
                    excluded_action_id=action_id,
                ):
                    self._planned.pop(action_id, None)

            for scope in sorted(effective, key=_scope_sort_key):
                failure_key = (AdapterSubscriptionOperation.SUBSCRIBE, scope)
                if (
                    scope in self._sent
                    or failure_key in self._failures
                    or self._has_active_action(AdapterSubscriptionOperation.SUBSCRIBE, scope)
                ):
                    continue
                self._require_retained_adapter_scope_capacity((scope,))
                action = self._new_action(
                    AdapterSubscriptionOperation.SUBSCRIBE,
                    scope,
                    self._subscribe_reason(scope),
                )
                self._planned[action.action_id] = action

            for scope in sorted(self._confirmed - effective, key=_scope_sort_key):
                failure_key = (AdapterSubscriptionOperation.UNSUBSCRIBE, scope)
                if failure_key in self._failures or self._has_active_action(
                    AdapterSubscriptionOperation.UNSUBSCRIBE,
                    scope,
                ):
                    continue
                if not self._can_remove_without_gap(scope, desired):
                    continue
                self._require_retained_adapter_scope_capacity((scope,))
                action = self._new_action(
                    AdapterSubscriptionOperation.UNSUBSCRIBE,
                    scope,
                    self._unsubscribe_reason(scope, desired),
                )
                self._planned[action.action_id] = action
            return tuple(
                sorted(
                    self._planned.values(),
                    key=lambda item: (
                        0 if item.operation is AdapterSubscriptionOperation.SUBSCRIBE else 1,
                        _scope_sort_key(item.scope),
                        item.action_id,
                    ),
                )
            )

    def claim_action(self, action: AdapterSubscriptionAction) -> SubscriptionLeaseSnapshot:
        """
        在调用 SDK 前原子认领一个仍必要的精确计划。

        Args:
            action: ``take_actions`` 返回的完整动作对象。

        Returns:
            SubscriptionLeaseSnapshot: 动作进入 inflight 后的状态快照。

        Raises:
            StaleSubscriptionActionError: epoch 或 desired revision 已变化时抛出。
            SubscriptionTransitionError: 动作非精确计划、已被认领或不再必要时抛出。

        Notes:
            dispatcher 在本方法成功返回后必须立即调用对应 SDK。claim 之后发生的
            意图变化按“动作可能已发送”处理，等待回执或 uncertain 对账后再补偿。
        """
        if not isinstance(action, AdapterSubscriptionAction):
            raise SubscriptionTransitionError("SUBSCRIPTION_ACTION_INVALID_TYPE")
        with self._lock:
            self._require_current_action_generation(action)
            planned = self._planned.get(action.action_id)
            if planned is None or planned != action:
                raise SubscriptionTransitionError("SUBSCRIPTION_ACTION_NOT_EXACT_PLAN")
            desired = self._desired_scopes()
            if not self._is_action_currently_needed(
                action,
                desired,
                excluded_action_id=action.action_id,
            ):
                self._planned.pop(action.action_id, None)
                raise SubscriptionTransitionError("SUBSCRIPTION_ACTION_NO_LONGER_NEEDED")
            self._require_retained_adapter_scope_capacity((action.scope,))
            self._planned.pop(action.action_id, None)
            self._inflight[action.action_id] = action
            if action.operation is AdapterSubscriptionOperation.SUBSCRIBE:
                self._sent.add(action.scope)
            return self.snapshot()

    def confirm_action(self, action: AdapterSubscriptionAction) -> SubscriptionLeaseSnapshot:
        """
        幂等确认一个已发送的底层订阅或退订动作。

        Args:
            action: ``take_actions`` 返回且底层已逐项确认的动作。

        Returns:
            SubscriptionLeaseSnapshot: 应用确认后的完整状态快照。

        Raises:
            StaleSubscriptionActionError: action 属于旧 epoch 时抛出。
            SubscriptionTransitionError: action 未 claim、内容被伪造或先前结果不一致时抛出。
        """
        with self._lock:
            self._require_action_epoch(action)
            completed = self._completed.get(action.action_id)
            if completed is not None:
                self._require_completed_match(action, completed, "confirmed")
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
            self._remember_completed(action, outcome="confirmed")
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
            normalized_code = _normalize_identifier(code, "code")
            normalized_reason = str(reason).strip() or None if reason is not None else None
            self._require_action_epoch(action)
            completed = self._completed.get(action.action_id)
            if completed is not None:
                self._require_completed_match(
                    action,
                    completed,
                    "rejected",
                    code=normalized_code,
                    reason=normalized_reason,
                )
                return self.snapshot()
            self._require_inflight_action(action)
            failure = AdapterSubscriptionFailure(
                action=action,
                code=normalized_code,
                reason=normalized_reason,
            )
            self._inflight.pop(action.action_id, None)
            if action.operation is AdapterSubscriptionOperation.SUBSCRIBE:
                self._sent.discard(action.scope)
            self._failures[(action.operation, action.scope)] = failure
            self._remember_completed(
                action,
                outcome="rejected",
                code=failure.code,
                reason=failure.reason,
            )
            self._purge_obsolete_subscribe_state()
            return self.snapshot()

    def mark_action_uncertain(
        self,
        action: AdapterSubscriptionAction,
        code: str = "ACK_TIMEOUT",
        reason: Optional[str] = None,
    ) -> SubscriptionLeaseSnapshot:
        """
        将已 claim 但 ACK 结果未知的动作转入必须对账的 uncertain 状态。

        Args:
            action: 已由 ``claim_action`` 认领并可能写入 SDK 的精确动作。
            code: 稳定的不确定状态码，默认表示 ACK 超时。
            reason: 可选脱敏诊断原因。

        Returns:
            SubscriptionLeaseSnapshot: 包含 uncertain scope 的状态快照。

        Raises:
            StaleSubscriptionActionError: action 属于旧 epoch 时抛出。
            SubscriptionTransitionError: action 未 claim、内容不同或重复诊断不一致时抛出。
        """
        normalized_code = _normalize_identifier(code, "code")
        normalized_reason = str(reason).strip() or None if reason is not None else None
        with self._lock:
            self._require_action_epoch(action)
            existing = self._uncertain.get(action.action_id)
            if existing is not None:
                if (
                    existing.action != action
                    or existing.code != normalized_code
                    or existing.reason != normalized_reason
                ):
                    raise SubscriptionTransitionError("SUBSCRIPTION_UNCERTAIN_RESULT_MISMATCH")
                return self.snapshot()
            self._require_inflight_action(action)
            self._inflight.pop(action.action_id, None)
            self._uncertain[action.action_id] = _UncertainSubscriptionAction(
                action=action,
                code=normalized_code,
                reason=normalized_reason,
            )
            return self.snapshot()

    def reject_uncertain_action(
        self,
        action: AdapterSubscriptionAction,
        code: str,
        reason: Optional[str] = None,
    ) -> SubscriptionLeaseSnapshot:
        """
        将 uncertain 动作的迟到明确拒绝落为失败门闩，而不是按未生效自动重试。

        Args:
            action: 已进入 uncertain 且与迟到 callback 完全一致的动作。
            code: 厂商 callback 返回的稳定拒绝错误码。
            reason: 可选脱敏厂商原因；精确重复必须保持一致。

        Returns:
            SubscriptionLeaseSnapshot: 保留 vendor 拒绝和失败门闩的状态快照。

        Raises:
            StaleSubscriptionActionError: action 属于旧 epoch 时抛出。
            SubscriptionTransitionError: action 不在 uncertain、被伪造或重复结果冲突时抛出。

        Notes:
            明确 rejected 与查询证明 ``applied=False`` 语义不同：前者必须等待
            上层显式 ``retry_failed``，不得因 desired 仍存在而自动生成新动作。
        """
        normalized_code = _normalize_identifier(code, "code")
        normalized_reason = str(reason).strip() or None if reason is not None else None
        with self._lock:
            self._require_action_epoch(action)
            completed = self._completed.get(action.action_id)
            if completed is not None:
                self._require_completed_match(
                    action,
                    completed,
                    "rejected",
                    code=normalized_code,
                    reason=normalized_reason,
                )
                return self.snapshot()
            uncertain = self._uncertain.get(action.action_id)
            if uncertain is None or uncertain.action != action:
                raise SubscriptionTransitionError("SUBSCRIPTION_ACTION_NOT_UNCERTAIN")
            failure = AdapterSubscriptionFailure(
                action=action,
                code=normalized_code,
                reason=normalized_reason,
            )
            self._uncertain.pop(action.action_id, None)
            if action.operation is AdapterSubscriptionOperation.SUBSCRIBE:
                self._sent.discard(action.scope)
            self._failures[(action.operation, action.scope)] = failure
            self._remember_completed(
                action,
                outcome="rejected",
                code=failure.code,
                reason=failure.reason,
            )
            self._purge_obsolete_subscribe_state()
            return self.snapshot()

    def reconcile_action(
        self,
        action: AdapterSubscriptionAction,
        applied: bool,
        reason: Optional[str] = None,
    ) -> SubscriptionLeaseSnapshot:
        """
        根据 SDK 查询或可信回调解析 uncertain 动作是否实际生效。

        Args:
            action: 已转入 uncertain 的精确动作。
            applied: True 表示动作已生效，False 表示确定未生效。
            reason: 可选脱敏对账证据摘要；重复调用必须保持一致。

        Returns:
            SubscriptionLeaseSnapshot: 对账落地后的快照，后续 take_actions 可规划补偿。

        Raises:
            StaleSubscriptionActionError: action 属于旧 epoch 时抛出。
            SubscriptionTransitionError: action 不在 uncertain、被伪造或结果冲突时抛出。

        Notes:
            本方法不根据旧计划猜当前意图。subscribe/unsubscribe 的 applied 与
            not-applied 结果落地后，planner 始终以最新 desired 生成必要补偿。
        """
        if not isinstance(applied, bool):
            raise ValueError("applied 必须为 bool")
        normalized_reason = str(reason).strip() or None if reason is not None else None
        outcome = "reconciled_applied" if applied else "reconciled_not_applied"
        with self._lock:
            self._require_action_epoch(action)
            completed = self._completed.get(action.action_id)
            if completed is not None:
                self._require_completed_match(
                    action,
                    completed,
                    outcome,
                    code=completed.code,
                    reason=normalized_reason,
                )
                return self.snapshot()
            uncertain = self._uncertain.get(action.action_id)
            if uncertain is None or uncertain.action != action:
                raise SubscriptionTransitionError("SUBSCRIPTION_ACTION_NOT_UNCERTAIN")
            self._uncertain.pop(action.action_id, None)
            if action.operation is AdapterSubscriptionOperation.SUBSCRIBE:
                if applied:
                    self._sent.add(action.scope)
                    self._confirmed.add(action.scope)
                else:
                    self._sent.discard(action.scope)
                    self._confirmed.discard(action.scope)
            elif applied:
                self._sent.discard(action.scope)
                self._confirmed.discard(action.scope)
            self._failures.pop((action.operation, action.scope), None)
            self._remember_completed(
                action,
                outcome=outcome,
                code=uncertain.code,
                reason=normalized_reason,
            )
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
            stable_confirmed = self._stable_confirmed_scopes()
            return bool(lease.active_scopes) and all(
                any(confirmed.covers(scope) for confirmed in stable_confirmed)
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
            planned_actions = tuple(self._planned.values())
            inflight_actions = tuple(self._inflight.values())
            uncertain_actions = tuple(item.action for item in self._uncertain.values())
            planned_subscribe = {
                action.scope
                for action in planned_actions
                if action.operation is AdapterSubscriptionOperation.SUBSCRIBE
            }
            planned_unsubscribe = {
                action.scope
                for action in planned_actions
                if action.operation is AdapterSubscriptionOperation.UNSUBSCRIBE
            }
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
            uncertain_subscribe = {
                action.scope
                for action in uncertain_actions
                if action.operation is AdapterSubscriptionOperation.SUBSCRIBE
            }
            uncertain_unsubscribe = {
                action.scope
                for action in uncertain_actions
                if action.operation is AdapterSubscriptionOperation.UNSUBSCRIBE
            }
            return SubscriptionLeaseSnapshot(
                session_epoch=self._session_epoch,
                desired_revision=self._desired_revision,
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
                stable_confirmed=frozenset(self._stable_confirmed_scopes()),
                planned_subscribe=frozenset(planned_subscribe),
                planned_unsubscribe=frozenset(planned_unsubscribe),
                pending_subscribe=frozenset(pending_subscribe),
                pending_unsubscribe=frozenset(pending_unsubscribe),
                uncertain_subscribe=frozenset(uncertain_subscribe),
                uncertain_unsubscribe=frozenset(uncertain_unsubscribe),
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

    def _require_registration_capacity(
        self,
        session_id: str,
        new_scopes: Sequence[AdapterSubscriptionScope],
    ) -> None:
        """
        在注册新 lease/request 前校验单 session 与 manager 的硬容量上限。

        Args:
            session_id: 即将新增记录的规范化 session ID。
            new_scopes: 新 lease 去重规范化后的 scope 序列。

        Returns:
            None: lease/request/scope 引用与适配器保留状态均有余量时返回。

        Raises:
            SubscriptionLeaseError: 任一容量已满时抛出。

        Notes:
            已存在 request 的幂等重放在调用本方法前返回，因此达到上限时仍可
            查询原 subscription；唯一 request/session 只能在硬上限内消耗内存。
        """
        new_scope_count = len(new_scopes)
        session_lease_count = sum(1 for key in self._leases if key[0] == session_id)
        session_request_count = sum(1 for key in self._request_index if key[0] == session_id)
        session_scope_references = sum(
            len(lease.initial_scopes)
            for lease in self._leases.values()
            if lease.session_id == session_id
        )
        total_scope_references = sum(len(lease.initial_scopes) for lease in self._leases.values())
        if session_lease_count >= self._max_leases_per_session:
            raise SubscriptionLeaseError("SUBSCRIPTION_SESSION_LEASE_LIMIT")
        if len(self._leases) >= self._max_total_leases:
            raise SubscriptionLeaseError("SUBSCRIPTION_GLOBAL_LEASE_LIMIT")
        if session_request_count >= self._max_requests_per_session:
            raise SubscriptionLeaseError("SUBSCRIPTION_SESSION_REQUEST_LIMIT")
        if len(self._request_index) >= self._max_total_requests:
            raise SubscriptionLeaseError("SUBSCRIPTION_GLOBAL_REQUEST_LIMIT")
        if session_scope_references + new_scope_count > self._max_scope_references_per_session:
            raise SubscriptionLeaseError("SUBSCRIPTION_SESSION_SCOPE_REFERENCE_LIMIT")
        if total_scope_references + new_scope_count > self._max_total_scope_references:
            raise SubscriptionLeaseError("SUBSCRIPTION_GLOBAL_SCOPE_REFERENCE_LIMIT")
        self._require_retained_adapter_scope_capacity(new_scopes)

    def _advance_desired_revision(self) -> None:
        """
        推进租约意图修订并作废全部尚未 claim 的旧计划。

        Returns:
            None: revision 递增且 planned 清空后返回。

        Notes:
            已 claim 的 inflight/uncertain 动作可能已经写入 SDK，不能因意图变化
            擅自撤销；它们必须等待回执或对账，再按新 revision 生成补偿动作。
        """
        self._desired_revision += 1
        self._planned.clear()
        self._purge_obsolete_subscribe_state()

    def _retained_adapter_scopes(self) -> Set[AdapterSubscriptionScope]:
        """
        聚合会被租约或适配器过渡状态保留的唯一 scope。

        Returns:
            Set[AdapterSubscriptionScope]: desired、已发送、已确认、活动动作与失败
            门闩的 scope 并集。

        Notes:
            completed 历史由独立条数上限约束。本并集把 inflight/uncertain
            视为已占用，使 close_session 不能释放它们的资源责任。
        """
        retained = self._desired_scopes() | self._sent | self._confirmed
        retained.update(action.scope for action in self._planned.values())
        retained.update(action.scope for action in self._inflight.values())
        retained.update(item.action.scope for item in self._uncertain.values())
        retained.update(scope for _, scope in self._failures)
        return retained

    def _require_retained_adapter_scope_capacity(
        self,
        additional_scopes: Sequence[AdapterSubscriptionScope],
    ) -> None:
        """
        在持久化新意图或动作前校验适配器 scope 全局硬上限。

        Args:
            additional_scopes: 本次状态转换可能新保留的 scope 序列。

        Returns:
            None: 当前并集加本次 scope 不超过配置上限时返回。

        Raises:
            SubscriptionLeaseError: 唯一 scope 并集超过硬上限时 fail closed。
        """
        retained = self._retained_adapter_scopes()
        retained.update(additional_scopes)
        if len(retained) > self._max_retained_adapter_scopes:
            raise SubscriptionLeaseError("SUBSCRIPTION_RETAINED_ADAPTER_SCOPE_LIMIT")

    def _purge_obsolete_subscribe_state(self) -> None:
        """
        清理已确定未生效且已无当前意图的 subscribe 残留状态。

        Returns:
            None: 可证明不再需要的失败门闩与未确认 sent 标记被删除后返回。

        Notes:
            inflight/uncertain 可能已进入 SDK，confirmed 仍是底层事实，两者
            都不在此自动删除。unsubscribe 拒绝也意味底层仍订阅，
            必须保留到重试、对账或新 epoch。
        """
        desired = self._desired_scopes()
        active_scopes = {action.scope for action in self._inflight.values()}
        active_scopes.update(item.action.scope for item in self._uncertain.values())
        for failure_key in tuple(self._failures):
            operation, scope = failure_key
            if operation is not AdapterSubscriptionOperation.SUBSCRIBE:
                continue
            if scope in desired or scope in self._confirmed or scope in active_scopes:
                continue
            self._failures.pop(failure_key, None)
            self._sent.discard(scope)

        subscribe_failures = {
            scope
            for operation, scope in self._failures
            if operation is AdapterSubscriptionOperation.SUBSCRIBE
        }
        for scope in tuple(self._sent):
            if (
                scope in desired
                or scope in self._confirmed
                or scope in active_scopes
                or scope in subscribe_failures
            ):
                continue
            self._sent.discard(scope)

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
        将逻辑 desired 压缩为覆盖安全且失败隔离的 adapter 目标并集。

        Args:
            desired: 从全部有效租约直接聚合的 full/partial 集合。

        Returns:
            Set[AdapterSubscriptionScope]: 通常由 full 压缩 partial；full 明确拒绝时
            降级为同组仍有租约的 partial 集合。

        Notes:
            full 订阅失败不应毒化其他 session 的 partial lease。失败门闩
            仍保留在原 full receipt 上；只有上层显式 retry_failed 后才会
            恢复 full 目标，随后按先确认 full、再退 partial 的顺序转换。
        """
        full_groups = {scope.group_key for scope in desired if scope.is_full}
        rejected_full_groups = {
            scope.group_key
            for operation, scope in self._failures
            if operation is AdapterSubscriptionOperation.SUBSCRIBE
            and scope.is_full
            and scope in desired
            and scope not in self._confirmed
            and not self._has_active_action(AdapterSubscriptionOperation.SUBSCRIBE, scope)
        }
        return {
            scope
            for scope in desired
            if (
                scope.is_full
                and scope.group_key not in rejected_full_groups
                or not scope.is_full
                and (scope.group_key not in full_groups or scope.group_key in rejected_full_groups)
            )
        }

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
            desired_revision=self._desired_revision,
            operation=operation,
            scope=scope,
            reason=reason,
        )

    def _has_active_action(
        self,
        operation: AdapterSubscriptionOperation,
        scope: AdapterSubscriptionScope,
        excluded_action_id: Optional[str] = None,
    ) -> bool:
        """
        判断同一操作和作用域是否已有计划、待回执或 uncertain 动作。

        Args:
            operation: 需要查询的动作类型。
            scope: 需要查询的精确作用域。
            excluded_action_id: 校验某个已有计划本身时需要排除的 action ID。

        Returns:
            bool: 存在另一个相同作用域活动动作时为 True。
        """
        actions = (
            tuple(self._planned.values())
            + tuple(self._inflight.values())
            + tuple(item.action for item in self._uncertain.values())
        )
        return any(
            action.action_id != excluded_action_id
            and action.operation is operation
            and action.scope == scope
            for action in actions
        )

    def _is_action_currently_needed(
        self,
        action: AdapterSubscriptionAction,
        desired: Set[AdapterSubscriptionScope],
        excluded_action_id: Optional[str] = None,
    ) -> bool:
        """
        校验计划在当前 epoch/revision、三集合与覆盖关系下仍有必要。

        Args:
            action: 待校验的完整计划。
            desired: 当前全部有效租约聚合出的逻辑意图。
            excluded_action_id: 检查计划自身时从活动动作查重中排除的 ID。

        Returns:
            bool: 动作当前仍可安全 claim 时为 True。
        """
        if (
            action.session_epoch != self._session_epoch
            or action.desired_revision != self._desired_revision
        ):
            return False
        effective = self._effective_desired(desired)
        failure_key = (action.operation, action.scope)
        if failure_key in self._failures or self._has_active_action(
            action.operation,
            action.scope,
            excluded_action_id=excluded_action_id,
        ):
            return False
        if action.operation is AdapterSubscriptionOperation.SUBSCRIBE:
            return (
                action.scope in effective
                and action.scope not in self._sent
                and action.reason == self._subscribe_reason(action.scope)
            )
        return (
            action.scope in self._confirmed - effective
            and action.reason == self._unsubscribe_reason(action.scope, desired)
            and self._can_remove_without_gap(
                action.scope,
                desired,
                excluded_action_id=excluded_action_id,
            )
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
        excluded_action_id: Optional[str] = None,
    ) -> bool:
        """
        校验退订旧作用域不会破坏仍有效租约的已确认覆盖。

        Args:
            scope: 候选退订的 confirmed 作用域。
            desired: 当前逻辑 desired full/partial 作用域。
            excluded_action_id: claim 某退订计划时只排除该计划自身的 ID。

        Returns:
            bool: 所有与旧作用域重叠的意图都已由其他 confirmed scope 覆盖时为 True。
        """
        alternatives = self._stable_confirmed_scopes(excluded_action_id=excluded_action_id) - {
            scope
        }
        overlapping = {item for item in desired if scope.overlaps(item)}
        for item in overlapping:
            coverage_point = scope if item.is_full and not scope.is_full else item
            if not any(candidate.covers(coverage_point) for candidate in alternatives):
                return False
        return True

    def _stable_confirmed_scopes(
        self,
        excluded_action_id: Optional[str] = None,
    ) -> Set[AdapterSubscriptionScope]:
        """
        返回未被 planned/inflight/uncertain 退订威胁的稳定确认覆盖。

        Args:
            excluded_action_id: 校验某个候选退订自身时可排除的 action ID。

        Returns:
            Set[AdapterSubscriptionScope]: 可安全用于 lease readiness 的 confirmed 集合。

        Notes:
            原始 ``_confirmed`` 保留 SDK 最近已确认事实；本视图更保守，避免新
            lease 借用一个可能马上被退掉的旧覆盖而被误报为 confirmed。
        """
        pending_removals = {
            action.scope
            for action in tuple(self._planned.values()) + tuple(self._inflight.values())
            if action.action_id != excluded_action_id
            and action.operation is AdapterSubscriptionOperation.UNSUBSCRIBE
        }
        pending_removals.update(
            item.action.scope
            for item in self._uncertain.values()
            if item.action.action_id != excluded_action_id
            and item.action.operation is AdapterSubscriptionOperation.UNSUBSCRIBE
        )
        return self._confirmed - pending_removals

    def _require_current_action_generation(self, action: AdapterSubscriptionAction) -> None:
        """
        要求待 claim 动作属于当前 epoch 和当前 desired revision。

        Args:
            action: 待认领的计划。

        Returns:
            None: 两类 generation 均匹配时返回。

        Raises:
            StaleSubscriptionActionError: epoch 或 revision 已过期时抛出。
        """
        self._require_action_epoch(action)
        if action.desired_revision != self._desired_revision:
            raise StaleSubscriptionActionError(
                f"STALE_SUBSCRIPTION_REVISION: expected={self._desired_revision}, "
                f"actual={action.desired_revision}"
            )

    def _require_action_epoch(self, action: AdapterSubscriptionAction) -> None:
        """
        要求动作属于当前 adapter session epoch。

        Args:
            action: 待校验的计划、inflight 或 uncertain 动作。

        Returns:
            None: epoch 匹配时返回。

        Raises:
            StaleSubscriptionActionError: 动作属于旧 epoch 时抛出。
        """
        if action.session_epoch != self._session_epoch:
            raise StaleSubscriptionActionError(
                f"STALE_SUBSCRIPTION_ACTION: expected={self._session_epoch}, "
                f"actual={action.session_epoch}"
            )

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
        self._require_action_epoch(action)
        completed = self._completed.get(action.action_id)
        if completed is not None:
            if completed.action != action:
                raise SubscriptionTransitionError("SUBSCRIPTION_COMPLETED_ACTION_MISMATCH")
            raise SubscriptionTransitionError(
                f"SUBSCRIPTION_ACTION_ALREADY_{completed.outcome.upper()}"
            )
        current = self._inflight.get(action.action_id)
        if current is None or current != action:
            if action.action_id in self._uncertain:
                raise SubscriptionTransitionError(
                    "SUBSCRIPTION_ACTION_UNCERTAIN_REQUIRES_RECONCILE"
                )
            raise SubscriptionTransitionError("SUBSCRIPTION_ACTION_NOT_INFLIGHT")

    def _require_completed_match(
        self,
        action: AdapterSubscriptionAction,
        completed: _CompletedSubscriptionAction,
        outcome: str,
        code: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        """
        验证重复结果与有界历史中的完整动作和结果逐字段一致。

        Args:
            action: 本次重复提交的动作。
            completed: action ID 对应的已完成记录。
            outcome: 本次期望重复的结果类型。
            code: reject/reconcile 使用的可选稳定码。
            reason: 可选脱敏原因，重复调用必须一致。

        Returns:
            None: 动作与结果全部一致时允许幂等返回。

        Raises:
            SubscriptionTransitionError: action ID 被伪造或结果信息冲突时抛出。
        """
        if completed.action != action:
            raise SubscriptionTransitionError("SUBSCRIPTION_COMPLETED_ACTION_MISMATCH")
        if completed.outcome != outcome or completed.code != code or completed.reason != reason:
            raise SubscriptionTransitionError("SUBSCRIPTION_COMPLETED_RESULT_MISMATCH")

    def _remember_completed(
        self,
        action: AdapterSubscriptionAction,
        outcome: str,
        code: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        """
        在有界历史中保存精确动作结果并淘汰最旧记录。

        Args:
            action: 已完成的精确 claimed/uncertain 动作。
            outcome: confirmed、rejected 或 reconciled 结果类型。
            code: 可选稳定结果码。
            reason: 可选脱敏原因。

        Returns:
            None: 完成记录写入且历史容量受控后返回。
        """
        self._completed[action.action_id] = _CompletedSubscriptionAction(
            action=action,
            outcome=outcome,
            code=code,
            reason=reason,
        )
        self._completed.move_to_end(action.action_id)
        while len(self._completed) > self._completed_history_limit:
            self._completed.popitem(last=False)


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
