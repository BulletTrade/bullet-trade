"""
作者: BruceLee

文件职责: 定义实时行情订阅、逐项回执、类型化市场事件和健康状态的离线数据模型。
主要输入: 规范化证券/市场 selector、行情级别、事件类型、Provider 回执和事件字段。
主要输出: 稳定指纹的 MarketSubscriptionSpec、不可变 Receipt、MarketEvent 与 FeedHealth。
上游关系: 由 RealtimeMarketDataFeed、远程协议适配层和未来 Huaxin native bridge 构造。
下游关系: 供 LiveEngine、策略 callback、健康检查、协议序列化和单元测试消费。
关键配置约定: selector 三选一；通配事件只可单独出现；指纹不包含传输 request_id。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple, Type, TypeVar

from .capability import CapabilityReadiness


class SubscriptionSelector(str, Enum):
    """表示订阅按证券、按市场或按全部获准范围选择。"""

    SYMBOLS = "symbols"
    MARKETS = "markets"
    ALL = "all"


class MarketDataLevel(str, Enum):
    """表示兼容 tick、L1 或 L2 行情层级。"""

    TICK_COMPAT = "tick_compat"
    L1 = "l1"
    L2 = "l2"


class MarketEventType(str, Enum):
    """表示 V1 可独立订阅的标准实时市场事件类型。"""

    ALL = "*"
    TICK_COMPAT = "tick_compat"
    SNAPSHOT_L1 = "snapshot_l1"
    SNAPSHOT_L2 = "snapshot_l2"
    TRANSACTION = "transaction"
    ORDER_DETAIL = "order_detail"
    CONSOLIDATED_TICK = "consolidated_tick"
    IOPV = "iopv"
    SECURITY_STATUS = "security_status"
    MARKET_STATUS = "market_status"


class SubscriptionItemState(str, Enum):
    """表示单个作用域与事件类型的订阅进度。"""

    REQUESTED = "requested"
    SENT = "sent"
    PENDING = "pending"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    CANCELED = "canceled"


class SubscriptionState(str, Enum):
    """表示一个订阅 lease 的整体状态。"""

    PENDING = "pending"
    CONFIRMED = "confirmed"
    PARTIAL = "partial"
    REJECTED = "rejected"
    CANCELED = "canceled"


class FieldProfile(str, Enum):
    """表示 typed 市场事件携带的字段保真层级。"""

    CANONICAL = "canonical"
    CANONICAL_WITH_VENDOR = "canonical_with_vendor"
    CANONICAL_WITH_RAW = "canonical_with_raw"


EnumType = TypeVar("EnumType", bound=Enum)


def _normalize_strings(values: Sequence[str], label: str) -> Tuple[str, ...]:
    """
    将字符串序列去空、去重并稳定排序。

    Args:
        values: 需要规范化的输入序列。
        label: 发生非法空字符串时用于错误信息的字段名。

    Returns:
        Tuple[str, ...]: 可稳定比较与序列化的字符串元组。

    Raises:
        ValueError: 输入中包含空字符串时抛出。
    """
    normalized = []
    for value in values:
        item = str(value).strip()
        if not item:
            raise ValueError(f"{label} 不能包含空字符串")
        normalized.append(item)
    return tuple(sorted(set(normalized)))


def _normalize_enums(
    values: Sequence[EnumType], enum_type: Type[EnumType], label: str
) -> Tuple[EnumType, ...]:
    """
    将字符串或枚举输入转换成去重、稳定排序的目标枚举元组。

    Args:
        values: 字符串或目标枚举值序列。
        enum_type: 需要转换到的枚举类型。
        label: 转换失败时用于错误信息的字段名。

    Returns:
        Tuple[EnumType, ...]: 按枚举值排序的唯一枚举元组。

    Raises:
        ValueError: 输入不是目标枚举的合法值时抛出。
    """
    try:
        normalized = {enum_type(item) for item in values}
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} 包含未知枚举值") from exc
    return tuple(sorted(normalized, key=lambda item: str(item.value)))


@dataclass(frozen=True)
class MarketSubscriptionSpec:
    """描述一个 session lease 的规范化部分或全市场订阅意图。"""

    request_id: str
    selector: SubscriptionSelector
    level: MarketDataLevel
    event_types: Tuple[MarketEventType, ...]
    symbols: Tuple[str, ...] = ()
    markets: Tuple[str, ...] = ()
    asset_types: Tuple[str, ...] = ()
    require_continuity: bool = False
    schema_version: str = "1"

    def __post_init__(self) -> None:
        """
        规范化字段并校验 selector、作用域和通配事件互斥条件。

        输入参数来自 dataclass 字段；本方法无返回值，非法订阅会抛出 ValueError。
        """
        request_id = self.request_id.strip()
        schema_version = self.schema_version.strip()
        if not request_id:
            raise ValueError("request_id 不能为空")
        if not schema_version:
            raise ValueError("schema_version 不能为空")
        try:
            selector = SubscriptionSelector(self.selector)
            level = MarketDataLevel(self.level)
        except ValueError as exc:
            raise ValueError("selector 或 level 包含未知枚举值") from exc
        symbols = _normalize_strings(self.symbols, "symbols")
        markets = _normalize_strings(self.markets, "markets")
        asset_types = _normalize_strings(self.asset_types, "asset_types")
        event_types = _normalize_enums(self.event_types, MarketEventType, "event_types")
        if not event_types:
            raise ValueError("event_types 不能为空")
        if MarketEventType.ALL in event_types and event_types != (MarketEventType.ALL,):
            raise ValueError("event_types='*' 必须单独使用")
        if selector is SubscriptionSelector.SYMBOLS:
            if not symbols or markets:
                raise ValueError("selector=symbols 必须只提供非空 symbols")
        elif selector is SubscriptionSelector.MARKETS:
            if not markets or symbols:
                raise ValueError("selector=markets 必须只提供非空 markets")
        elif symbols or markets:
            raise ValueError("selector=all 不得同时提供 symbols 或 markets")
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "selector", selector)
        object.__setattr__(self, "level", level)
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "markets", markets)
        object.__setattr__(self, "asset_types", asset_types)
        object.__setattr__(self, "event_types", event_types)

    def canonical_payload(self) -> Mapping[str, Any]:
        """
        返回不含 request_id 的稳定语义载荷。

        Returns:
            Mapping[str, Any]: 可直接 JSON 编码并计算指纹的规范化字段。
        """
        return {
            "schema_version": self.schema_version,
            "selector": self.selector.value,
            "symbols": list(self.symbols),
            "markets": list(self.markets),
            "asset_types": list(self.asset_types),
            "level": self.level.value,
            "event_types": [item.value for item in self.event_types],
            "require_continuity": self.require_continuity,
        }

    @property
    def fingerprint(self) -> str:
        """
        计算与输入顺序和 request_id 无关的 SHA-256 语义指纹。

        Returns:
            str: 小写十六进制 SHA-256 字符串。
        """
        payload = json.dumps(
            self.canonical_payload(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def scope_items(self) -> Tuple[str, ...]:
        """
        展开回执逐项状态所使用的规范化作用域键。

        Returns:
            Tuple[str, ...]: symbols、markets 或单个通配符作用域。
        """
        if self.selector is SubscriptionSelector.SYMBOLS:
            return self.symbols
        if self.selector is SubscriptionSelector.MARKETS:
            return self.markets
        return ("*",)


@dataclass(frozen=True)
class SubscriptionItemResult:
    """记录一个作用域和事件类型从 requested 到终态的确定结果。"""

    selector: SubscriptionSelector
    scope: str
    level: MarketDataLevel
    event_type: MarketEventType
    state: SubscriptionItemState
    code: Optional[str] = None
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        """
        规范化单项回执并校验拒绝原因。

        输入参数来自 dataclass 字段；本方法无返回值，非法状态会抛出 ValueError。
        """
        scope = self.scope.strip()
        if not scope:
            raise ValueError("scope 不能为空")
        try:
            selector = SubscriptionSelector(self.selector)
            level = MarketDataLevel(self.level)
            event_type = MarketEventType(self.event_type)
            state = SubscriptionItemState(self.state)
        except ValueError as exc:
            raise ValueError("单项回执包含未知枚举值") from exc
        if event_type is MarketEventType.ALL:
            raise ValueError("回执必须列出实际事件类型，不能保留 '*' 通配符")
        if state is SubscriptionItemState.REJECTED and not self.code:
            raise ValueError("rejected 单项必须提供 code")
        object.__setattr__(self, "selector", selector)
        object.__setattr__(self, "scope", scope)
        object.__setattr__(self, "level", level)
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(self, "state", state)


def _derive_receipt_state(items: Sequence[SubscriptionItemResult]) -> SubscriptionState:
    """
    从逐项状态确定订阅整体状态。

    Args:
        items: 同一个订阅 lease 的逐项回执。

    Returns:
        SubscriptionState: pending、confirmed、partial、rejected 或 canceled。

    Raises:
        ValueError: items 为空时抛出。
    """
    if not items:
        raise ValueError("receipt items 不能为空")
    states = {item.state for item in items}
    if states == {SubscriptionItemState.CONFIRMED}:
        return SubscriptionState.CONFIRMED
    if states == {SubscriptionItemState.REJECTED}:
        return SubscriptionState.REJECTED
    if states == {SubscriptionItemState.CANCELED}:
        return SubscriptionState.CANCELED
    if SubscriptionItemState.CANCELED in states and states.issubset(
        {SubscriptionItemState.CANCELED, SubscriptionItemState.REJECTED}
    ):
        return SubscriptionState.CANCELED
    if states.issubset(
        {
            SubscriptionItemState.REQUESTED,
            SubscriptionItemState.SENT,
            SubscriptionItemState.PENDING,
        }
    ):
        return SubscriptionState.PENDING
    return SubscriptionState.PARTIAL


@dataclass(frozen=True)
class MarketSubscriptionReceipt:
    """保存实际事件展开、逐项确认和 session epoch 的版本化订阅回执。"""

    subscription_id: str
    request_id: str
    payload_fingerprint: str
    effective_scope: SubscriptionSelector
    session_epoch: str
    items: Tuple[SubscriptionItemResult, ...]
    state: SubscriptionState
    actual_event_types: Tuple[MarketEventType, ...]
    effective_symbols: Tuple[str, ...] = ()
    effective_markets: Tuple[str, ...] = ()
    limits: Mapping[str, Any] = field(default_factory=dict)
    schema_version: str = "1"

    def __post_init__(self) -> None:
        """
        校验回执一致性并冻结列表、状态和 limits。

        输入参数来自 dataclass 字段；本方法无返回值，不一致时抛出 ValueError。
        """
        subscription_id = self.subscription_id.strip()
        request_id = self.request_id.strip()
        payload_fingerprint = self.payload_fingerprint.strip()
        session_epoch = self.session_epoch.strip()
        schema_version = self.schema_version.strip()
        if not all(
            (subscription_id, request_id, payload_fingerprint, session_epoch, schema_version)
        ):
            raise ValueError("receipt 标识、指纹、epoch 和版本均不能为空")
        try:
            effective_scope = SubscriptionSelector(self.effective_scope)
            state = SubscriptionState(self.state)
        except ValueError as exc:
            raise ValueError("receipt 包含未知整体枚举值") from exc
        items = tuple(self.items)
        derived_state = _derive_receipt_state(items)
        if state is not derived_state:
            raise ValueError(
                f"receipt state 与逐项状态不一致: state={state.value}, " f"derived={derived_state.value}"
            )
        actual_event_types = _normalize_enums(
            self.actual_event_types, MarketEventType, "actual_event_types"
        )
        if not actual_event_types or MarketEventType.ALL in actual_event_types:
            raise ValueError("actual_event_types 必须列出非通配的实际事件")
        item_event_types = {item.event_type for item in items}
        if set(actual_event_types) != item_event_types:
            raise ValueError("actual_event_types 与逐项回执事件集合不一致")
        effective_symbols = _normalize_strings(self.effective_symbols, "effective_symbols")
        effective_markets = _normalize_strings(self.effective_markets, "effective_markets")
        if effective_scope is SubscriptionSelector.SYMBOLS and not effective_symbols:
            raise ValueError("symbols receipt 必须包含 effective_symbols")
        if effective_scope is SubscriptionSelector.MARKETS and not effective_markets:
            raise ValueError("markets receipt 必须包含 effective_markets")
        object.__setattr__(self, "subscription_id", subscription_id)
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "payload_fingerprint", payload_fingerprint)
        object.__setattr__(self, "session_epoch", session_epoch)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "effective_scope", effective_scope)
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "items", items)
        object.__setattr__(self, "actual_event_types", actual_event_types)
        object.__setattr__(self, "effective_symbols", effective_symbols)
        object.__setattr__(self, "effective_markets", effective_markets)
        object.__setattr__(self, "limits", MappingProxyType(dict(self.limits)))

    @classmethod
    def from_items(
        cls,
        subscription_id: str,
        spec: MarketSubscriptionSpec,
        session_epoch: str,
        items: Sequence[SubscriptionItemResult],
        actual_event_types: Sequence[MarketEventType],
        effective_symbols: Sequence[str] = (),
        effective_markets: Sequence[str] = (),
        limits: Optional[Mapping[str, Any]] = None,
    ) -> "MarketSubscriptionReceipt":
        """
        从规范化 spec 和逐项结果构造状态一致的回执。

        Args:
            subscription_id: Feed 分配的稳定订阅 ID。
            spec: 原始语义订阅请求。
            session_epoch: 当前连接会话 epoch。
            items: 逐作用域和事件类型的结果。
            actual_event_types: 通配展开后的实际事件类型。
            effective_symbols: 实际证券范围。
            effective_markets: 实际市场范围。
            limits: Feed 对本次订阅公布的脱敏限制。

        Returns:
            MarketSubscriptionReceipt: 不可变且状态自洽的订阅回执。
        """
        normalized_items = tuple(items)
        return cls(
            subscription_id=subscription_id,
            request_id=spec.request_id,
            payload_fingerprint=spec.fingerprint,
            effective_scope=spec.selector,
            session_epoch=session_epoch,
            items=normalized_items,
            state=_derive_receipt_state(normalized_items),
            actual_event_types=tuple(actual_event_types),
            effective_symbols=tuple(effective_symbols),
            effective_markets=tuple(effective_markets),
            limits=limits or {},
            schema_version=spec.schema_version,
        )

    @property
    def requested(self) -> Tuple[SubscriptionItemResult, ...]:
        """
        返回全部 requested 明细。

        Returns:
            Tuple[SubscriptionItemResult, ...]: 本 lease 的全部逐项请求。
        """
        return self.items

    @property
    def sent(self) -> Tuple[SubscriptionItemResult, ...]:
        """
        返回已进入 Provider 流程而非本地直接拒绝的明细。

        Returns:
            Tuple[SubscriptionItemResult, ...]: sent、pending、confirmed 或 canceled 项。
        """
        return tuple(
            item
            for item in self.items
            if item.state
            in {
                SubscriptionItemState.SENT,
                SubscriptionItemState.PENDING,
                SubscriptionItemState.CONFIRMED,
                SubscriptionItemState.CANCELED,
            }
        )

    @property
    def confirmed(self) -> Tuple[SubscriptionItemResult, ...]:
        """
        返回 Provider 已确认的明细。

        Returns:
            Tuple[SubscriptionItemResult, ...]: 状态为 confirmed 的项目。
        """
        return tuple(item for item in self.items if item.state is SubscriptionItemState.CONFIRMED)

    @property
    def pending(self) -> Tuple[SubscriptionItemResult, ...]:
        """
        返回尚未确认的明细。

        Returns:
            Tuple[SubscriptionItemResult, ...]: requested、sent 或 pending 项。
        """
        pending_states = {
            SubscriptionItemState.REQUESTED,
            SubscriptionItemState.SENT,
            SubscriptionItemState.PENDING,
        }
        return tuple(item for item in self.items if item.state in pending_states)

    @property
    def rejected(self) -> Tuple[SubscriptionItemResult, ...]:
        """
        返回被明确拒绝的明细。

        Returns:
            Tuple[SubscriptionItemResult, ...]: 状态为 rejected 的项目。
        """
        return tuple(item for item in self.items if item.state is SubscriptionItemState.REJECTED)

    def as_canceled(self) -> "MarketSubscriptionReceipt":
        """
        将已有 lease 的所有逐项状态复制为 canceled。

        Returns:
            MarketSubscriptionReceipt: 保留订阅 ID 和作用域的 canceled 回执。
        """
        canceled_items = tuple(
            item
            if item.state is SubscriptionItemState.REJECTED
            else replace(item, state=SubscriptionItemState.CANCELED, code=None, reason=None)
            for item in self.items
        )
        return replace(self, items=canceled_items, state=SubscriptionState.CANCELED)


@dataclass(frozen=True)
class MarketEvent:
    """保存 canonical、Provider 扩展和顺序来源的版本化 typed 市场事件。"""

    provider: str
    capability_key: str
    event_type: MarketEventType
    level: MarketDataLevel
    exchange: str
    session_epoch: str
    payload: Mapping[str, Any]
    security: Optional[str] = None
    raw_security_code: Optional[str] = None
    asset_type: Optional[str] = None
    schema_version: str = "1"
    field_set_version: str = "1"
    field_profile: FieldProfile = FieldProfile.CANONICAL
    route_rule: Optional[str] = None
    trading_day: Optional[date] = None
    trading_day_source: Optional[str] = None
    exchange_time: Optional[datetime] = None
    gateway_received_at: Optional[datetime] = None
    client_received_at: Optional[datetime] = None
    stream_id: Optional[str] = None
    channel_id: Optional[str] = None
    source_sequence: Mapping[str, Any] = field(default_factory=dict)
    raw_type: Optional[str] = None
    provider_extension: Mapping[str, Any] = field(default_factory=dict)
    raw_profile: Mapping[str, Any] = field(default_factory=dict)
    field_presence: Tuple[str, ...] = ()
    completeness: bool = True
    missing_fields: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """
        校验 typed 事件标识并冻结所有载荷和字段集合。

        输入参数来自 dataclass 字段；本方法无返回值，非法事件会抛出 ValueError。
        """
        provider = self.provider.strip()
        capability_key = self.capability_key.strip()
        exchange = self.exchange.strip()
        session_epoch = self.session_epoch.strip()
        schema_version = self.schema_version.strip()
        field_set_version = self.field_set_version.strip()
        if not all(
            (provider, capability_key, exchange, session_epoch, schema_version, field_set_version)
        ):
            raise ValueError("市场事件的 provider/capability/exchange/epoch/版本不能为空")
        try:
            event_type = MarketEventType(self.event_type)
            level = MarketDataLevel(self.level)
            field_profile = FieldProfile(self.field_profile)
        except ValueError as exc:
            raise ValueError("市场事件包含未知 event_type 或 field_profile") from exc
        if event_type is MarketEventType.ALL:
            raise ValueError("实际市场事件不能使用 '*' 通配符")
        field_presence = _normalize_strings(self.field_presence, "field_presence")
        missing_fields = _normalize_strings(self.missing_fields, "missing_fields")
        if self.completeness and missing_fields:
            raise ValueError("completeness=true 时 missing_fields 必须为空")
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "capability_key", capability_key)
        object.__setattr__(self, "exchange", exchange)
        object.__setattr__(self, "session_epoch", session_epoch)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "field_set_version", field_set_version)
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(self, "level", level)
        object.__setattr__(self, "field_profile", field_profile)
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))
        object.__setattr__(self, "source_sequence", MappingProxyType(dict(self.source_sequence)))
        object.__setattr__(
            self, "provider_extension", MappingProxyType(dict(self.provider_extension))
        )
        object.__setattr__(self, "raw_profile", MappingProxyType(dict(self.raw_profile)))
        object.__setattr__(self, "field_presence", field_presence)
        object.__setattr__(self, "missing_fields", missing_fields)


@dataclass(frozen=True)
class FeedHealth:
    """暴露 Feed 连接、能力、订阅、时效和基础队列指标的脱敏健康快照。"""

    provider: str
    connected: bool
    manifest_version: str
    session_epoch: Optional[str]
    active_subscriptions: Mapping[str, MarketSubscriptionReceipt]
    capability_readiness: Mapping[str, CapabilityReadiness]
    reconnect_count: int = 0
    last_gateway_received_at: Optional[datetime] = None
    last_client_received_at: Optional[datetime] = None
    last_exchange_time: Optional[datetime] = None
    queue_depth: int = 0
    queue_capacity: int = 0
    gap_count: int = 0
    reasons: Tuple[str, ...] = ()
    schema_version: str = "1"

    def __post_init__(self) -> None:
        """
        校验健康计数并冻结能力和订阅映射。

        输入参数来自 dataclass 字段；本方法无返回值，负数计数会抛出 ValueError。
        """
        provider = self.provider.strip()
        manifest_version = self.manifest_version.strip()
        schema_version = self.schema_version.strip()
        if not provider or not manifest_version or not schema_version:
            raise ValueError("health provider、manifest_version 和 schema_version 不能为空")
        if min(self.reconnect_count, self.queue_depth, self.queue_capacity, self.gap_count) < 0:
            raise ValueError("health 计数不能为负数")
        if self.queue_capacity and self.queue_depth > self.queue_capacity:
            raise ValueError("queue_depth 不能大于 queue_capacity")
        readiness: Dict[str, CapabilityReadiness] = {}
        for capability_id, state in self.capability_readiness.items():
            readiness[capability_id] = CapabilityReadiness(state)
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "manifest_version", manifest_version)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(
            self, "active_subscriptions", MappingProxyType(dict(self.active_subscriptions))
        )
        object.__setattr__(self, "capability_readiness", MappingProxyType(readiness))
        object.__setattr__(self, "reasons", _normalize_strings(self.reasons, "reasons"))


__all__ = [
    "FeedHealth",
    "FieldProfile",
    "MarketDataLevel",
    "MarketEvent",
    "MarketEventType",
    "MarketSubscriptionReceipt",
    "MarketSubscriptionSpec",
    "SubscriptionItemResult",
    "SubscriptionItemState",
    "SubscriptionSelector",
    "SubscriptionState",
]
