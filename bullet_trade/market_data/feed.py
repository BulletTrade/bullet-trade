"""
作者: BruceLee

文件职责: 定义独立实时行情 Feed 契约，并提供无需厂商 SDK 的确定性 Mock 实现。
主要输入: CapabilityManifest、MarketSubscriptionSpec、typed MarketEvent 和 callback。
主要输出: 生命周期状态、精确订阅回执、兼容 tick、分级快照与 FeedHealth。
上游关系: 未来由 LiveEngine、远程客户端或 Huaxin native bridge 创建和驱动 Feed。
下游关系: 策略 tick/market callback、DataSourceRouter、健康门禁和离线合同测试。
关键配置约定: Mock 不联网；通配符只展开协商且 ready 的事件；全市场必须显式门禁。
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, replace
from datetime import datetime
from threading import Condition, RLock
from types import MappingProxyType
from typing import AbstractSet, Any, Callable, Dict, Mapping, Optional, Sequence, Set, Tuple, Type

from .capability import (
    CapabilityDeclaration,
    CapabilityManifest,
    CapabilityReadiness,
    CapabilitySupport,
)
from .health import (
    FreshnessDecision,
    FreshnessRecordNotFoundError,
    GatewayIngressMark,
    MarketFreshnessError,
    MarketFreshnessTracker,
    MarketUpdateExpectationPolicy,
    MonotonicClock,
)
from .models import (
    CompatibilityTickEvent,
    ConnectionStateEvent,
    ConsolidatedTickEvent,
    DepthSnapshotEvent,
    FeedEventTimes,
    FeedHealth,
    IopvEvent,
    MarketDataLevel,
    MarketEvent,
    MarketEventType,
    MarketStatusEvent,
    MarketSubscriptionReceipt,
    MarketSubscriptionSpec,
    OrderDetailEvent,
    QuoteSnapshotEvent,
    SecurityStatusEvent,
    SequenceGapEvent,
    SubscriptionItemResult,
    SubscriptionSelector,
    SubscriptionState,
    TransactionEvent,
)
from .queue import BoundedMarketEventQueue
from .subscription_receipts import FeedSubscriptionItemPlan, SubscriptionReceiptProjector
from .subscription_runtime import (
    InMemorySubscriptionActionAdapter,
    SubscriptionActionAdapter,
    SubscriptionActionCoordinator,
)
from .subscriptions import SubscriptionLeaseManager, SubscriptionLeaseSnapshot

TickCallback = Callable[[Mapping[str, Any]], None]
MarketEventCallback = Callable[[MarketEvent], None]


_CONTROL_EVENT_TYPES = frozenset({MarketEventType.STREAM_GAP, MarketEventType.STREAM_STATUS})
_MAX_SEEN_GAP_BOUNDARIES = 4096
_FEED_READINESS_PRIORITY: Mapping[CapabilityReadiness, int] = MappingProxyType(
    {
        CapabilityReadiness.READY: 0,
        CapabilityReadiness.DEGRADED: 1,
        CapabilityReadiness.STALE: 2,
        CapabilityReadiness.UNAVAILABLE: 3,
        CapabilityReadiness.UNAUTHORIZED: 4,
    }
)


@dataclass(frozen=True)
class _EffectiveFeedState:
    """保存一次线性化读取产生的 manifest、health 与队列共同状态。"""

    manifest: CapabilityManifest
    readiness: Mapping[str, CapabilityReadiness]
    readiness_reasons: Mapping[str, Optional[str]]
    module_readiness: Mapping[str, CapabilityReadiness]
    latest_times: Optional[FeedEventTimes]
    capability_times: Mapping[str, FeedEventTimes]
    module_times: Mapping[str, FeedEventTimes]
    active_subscriptions: Mapping[str, MarketSubscriptionReceipt]
    connected: bool
    session_epoch: Optional[str]
    reconnect_count: int
    source_gap_count: int
    queue_depth: int
    queue_control_depth: int
    queue_capacity: int
    queue_control_capacity: int
    queue_control_scope_depth: int
    queue_control_scope_capacity: int
    queue_control_overflow_count: int
    queue_high_watermark: int
    queue_overflow_count: int
    queue_coalesced_count: int
    queue_loss_boundary_count: int
    queue_degraded: bool
    queue_overflow_by_event_type: Mapping[str, int]
    reasons: Tuple[str, ...]


class MarketDataFeedError(RuntimeError):
    """实时行情 Feed 生命周期、订阅或数据读取失败的公共基类。"""


class FeedNotConnectedError(MarketDataFeedError):
    """表示调用要求已连接 Feed，但当前连接尚未 ready。"""


class RealtimeDataUnavailableError(MarketDataFeedError):
    """表示当前没有已确认且可返回的实时 tick 或快照。"""


class StaleMarketDataError(MarketDataFeedError):
    """表示策略读取命中了已存在但超过显式更新窗口阈值的行情。"""

    def __init__(self, decision: FreshnessDecision) -> None:
        """
        使用可诊断的单调时钟 freshness 证据初始化异常。

        Args:
            decision: 包含证券、级别、有效 age、阈值与市场状态的失效决策。

        Returns:
            None: 异常消息与 decision 保存完成后返回。
        """
        self.decision = decision
        source_age = (
            "unknown"
            if decision.effective_source_age_seconds is None
            else f"{decision.effective_source_age_seconds:.6f}"
        )
        super().__init__(
            "STALE_MARKET_DATA: "
            f"security={decision.security}, level={decision.level.value}, "
            f"effective_age={decision.effective_age_seconds:.6f}, "
            f"source_age={source_age}, "
            f"threshold={decision.stale_after_seconds:.6f}, "
            f"market_state={decision.market_state}, "
            f"exchange_time={decision.last_exchange_time}, "
            f"gateway_received_at={decision.last_gateway_received_at}, "
            f"client_received_at={decision.last_client_received_at}"
        )


@dataclass(frozen=True)
class MarketSnapshotDiagnostic:
    """绑定原始 typed 快照与同一次读取使用的显式 freshness 决策。"""

    event: MarketEvent
    freshness: FreshnessDecision

    def __post_init__(self) -> None:
        """
        校验诊断结果的事件、证券和级别保持一致。

        Returns:
            None: 事件与 freshness 身份一致时返回。

        Raises:
            ValueError: 类型、证券或级别不一致时抛出。
        """
        if not isinstance(self.event, MarketEvent):
            raise ValueError("diagnostic event 必须是 MarketEvent")
        if not isinstance(self.freshness, FreshnessDecision):
            raise ValueError("diagnostic freshness 必须是 FreshnessDecision")
        if (
            self.event.security != self.freshness.security
            or self.event.level is not self.freshness.level
        ):
            raise ValueError("diagnostic event 与 freshness scope 不一致")
        if self.event.event_type not in {
            MarketEventType.SNAPSHOT_L1,
            MarketEventType.SNAPSHOT_L2,
        }:
            raise ValueError("snapshot diagnostic 只能包含 L1/L2 快照事件")


@dataclass(frozen=True)
class CurrentTickDiagnostic:
    """绑定兼容 tick 副本与同一次读取使用的显式 freshness 决策。"""

    tick: Mapping[str, Any]
    freshness: FreshnessDecision

    def __post_init__(self) -> None:
        """
        冻结 tick 副本并校验 freshness 类型。

        Returns:
            None: tick 与 freshness 转为稳定只读诊断结果后返回。

        Raises:
            ValueError: tick 不是映射或 freshness 类型错误时抛出。
        """
        if not isinstance(self.tick, Mapping):
            raise ValueError("diagnostic tick 必须是 Mapping")
        if not isinstance(self.freshness, FreshnessDecision):
            raise ValueError("diagnostic freshness 必须是 FreshnessDecision")
        if self.freshness.level is not MarketDataLevel.TICK_COMPAT:
            raise ValueError("tick diagnostic freshness 必须是 tick_compat")
        object.__setattr__(self, "tick", MappingProxyType(dict(self.tick)))


class SubscriptionConflictError(MarketDataFeedError):
    """表示相同 request_id 被重用于不同语义指纹。"""


class SubscriptionNotFoundError(MarketDataFeedError):
    """表示退订目标不是当前或已取消的明确 subscription ID。"""


class SubscriptionCapacityError(MarketDataFeedError):
    """表示 Feed 的本地订阅墓碑或单请求逐项数量达到硬上限。"""


def _normalize_positive_limit(value: int, field_name: str) -> int:
    """
    校验 Feed 本地订阅容量参数为非布尔正整数。

    Args:
        value: 待校验的容量值。
        field_name: 用于稳定错误消息的参数名。

    Returns:
        int: 已校验的原始容量值。

    Raises:
        ValueError: value 不是非布尔正整数时抛出。
    """
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} 必须为正数整数")
    return value


_EVENT_CAPABILITY: Mapping[MarketEventType, str] = MappingProxyType(
    {
        MarketEventType.TICK_COMPAT: "realtime.stream.tick_compat",
        MarketEventType.SNAPSHOT_L1: "realtime.snapshot.l1",
        MarketEventType.SNAPSHOT_L2: "realtime.snapshot.l2",
        MarketEventType.TRANSACTION: "realtime.stream.transaction",
        MarketEventType.ORDER_DETAIL: "realtime.stream.order_detail",
        MarketEventType.CONSOLIDATED_TICK: "realtime.stream.consolidated_tick",
        MarketEventType.IOPV: "realtime.stream.iopv",
        MarketEventType.SECURITY_STATUS: "realtime.stream.security_status",
        MarketEventType.MARKET_STATUS: "realtime.stream.market_status",
    }
)

_EVENT_LEVELS: Mapping[MarketEventType, Tuple[MarketDataLevel, ...]] = MappingProxyType(
    {
        MarketEventType.TICK_COMPAT: (MarketDataLevel.TICK_COMPAT,),
        MarketEventType.SNAPSHOT_L1: (MarketDataLevel.L1,),
        MarketEventType.SNAPSHOT_L2: (MarketDataLevel.L2,),
        MarketEventType.TRANSACTION: (MarketDataLevel.L2,),
        MarketEventType.ORDER_DETAIL: (MarketDataLevel.L2,),
        MarketEventType.CONSOLIDATED_TICK: (MarketDataLevel.L2,),
        MarketEventType.IOPV: (MarketDataLevel.L1, MarketDataLevel.L2),
        MarketEventType.SECURITY_STATUS: (MarketDataLevel.L1, MarketDataLevel.L2),
        MarketEventType.MARKET_STATUS: (MarketDataLevel.L1, MarketDataLevel.L2),
    }
)

_EVENT_MODELS: Mapping[MarketEventType, Type[MarketEvent]] = MappingProxyType(
    {
        MarketEventType.TICK_COMPAT: CompatibilityTickEvent,
        MarketEventType.SNAPSHOT_L1: QuoteSnapshotEvent,
        MarketEventType.SNAPSHOT_L2: DepthSnapshotEvent,
        MarketEventType.TRANSACTION: TransactionEvent,
        MarketEventType.ORDER_DETAIL: OrderDetailEvent,
        MarketEventType.CONSOLIDATED_TICK: ConsolidatedTickEvent,
        MarketEventType.IOPV: IopvEvent,
        MarketEventType.SECURITY_STATUS: SecurityStatusEvent,
        MarketEventType.MARKET_STATUS: MarketStatusEvent,
    }
)


class RealtimeMarketDataFeed(ABC):
    """定义与历史 DataProvider 分离的实时行情生命周期和订阅合同。"""

    @property
    @abstractmethod
    def manifest(self) -> CapabilityManifest:
        """
        返回当前版本化能力清单。

        Returns:
            CapabilityManifest: 同时包含静态 support 和动态 readiness 的快照。
        """
        raise NotImplementedError

    @abstractmethod
    def connect(self) -> None:
        """
        建立实时 Feed 会话并更新能力 readiness。

        Returns:
            None: 连接成功后返回；失败应抛出具名 Feed 错误。
        """
        raise NotImplementedError

    @abstractmethod
    def disconnect(self) -> None:
        """
        断开 Feed 并停止事件投递，但不得隐式执行任何交易写操作。

        Returns:
            None: 已断开时允许幂等返回。
        """
        raise NotImplementedError

    @abstractmethod
    def health(self) -> FeedHealth:
        """
        获取连接、能力、订阅和时效的不可变健康快照。

        Returns:
            FeedHealth: 不包含 secret 的运行时健康状态。
        """
        raise NotImplementedError

    @abstractmethod
    def get_current_tick(self, security: str) -> Mapping[str, Any]:
        """
        获取一个证券的最新兼容 tick。

        Args:
            security: 标准证券代码。

        Returns:
            Mapping[str, Any]: 兼容旧策略的 tick 字段副本。
        """
        raise NotImplementedError

    @abstractmethod
    def get_market_snapshot(self, security: str, level: MarketDataLevel) -> MarketEvent:
        """
        按 L1/L2 级别读取最新 typed 快照。

        Args:
            security: 标准证券代码。
            level: 明确的 L1 或 L2 级别。

        Returns:
            MarketEvent: 保留 Provider 和字段保真信息的最新事件。
        """
        raise NotImplementedError

    @abstractmethod
    def diagnose_current_tick(
        self, security: str, *, allow_stale: bool = False
    ) -> CurrentTickDiagnostic:
        """
        通过独立诊断入口读取兼容 tick，并显式决定是否容忍 stale。

        Args:
            security: 标准证券代码。
            allow_stale: 仅供诊断使用；True 时允许返回已有 stale 数据。

        Returns:
            CurrentTickDiagnostic: 兼容 tick 副本及同次读取的显式 market state/age。
        """
        raise NotImplementedError

    @abstractmethod
    def diagnose_market_snapshot(
        self,
        security: str,
        level: MarketDataLevel,
        *,
        allow_stale: bool = False,
    ) -> MarketSnapshotDiagnostic:
        """
        通过独立诊断入口读取精确 L1/L2 快照。

        Args:
            security: 标准证券代码。
            level: 明确的 L1 或 L2 级别。
            allow_stale: 仅供诊断使用；True 时允许返回已有 stale 数据。

        Returns:
            MarketSnapshotDiagnostic: 原始缓存事件及同次读取的显式 market state/age。
        """
        raise NotImplementedError

    @abstractmethod
    def subscribe(self, spec: MarketSubscriptionSpec) -> MarketSubscriptionReceipt:
        """
        创建或幂等重放一个 session 订阅 lease。

        Args:
            spec: 规范化部分/全市场订阅意图。

        Returns:
            MarketSubscriptionReceipt: 实际展开和逐项确认结果。
        """
        raise NotImplementedError

    @abstractmethod
    def unsubscribe(self, subscription_id: str) -> MarketSubscriptionReceipt:
        """
        仅取消指定的明确 subscription ID。

        Args:
            subscription_id: 需要取消的订阅 lease ID。

        Returns:
            MarketSubscriptionReceipt: canceled 状态回执。
        """
        raise NotImplementedError

    @abstractmethod
    def unsubscribe_all(
        self,
        level: Optional[MarketDataLevel] = None,
        event_types: Optional[Sequence[MarketEventType]] = None,
    ) -> Tuple[MarketSubscriptionReceipt, ...]:
        """
        取消当前 Feed session 的全部或指定 level/event 范围订阅。

        Args:
            level: 可选的精确行情级别过滤器。
            event_types: 可选的实际事件类型过滤器。

        Returns:
            Tuple[MarketSubscriptionReceipt, ...]: 每个被取消 lease 的回执。
        """
        raise NotImplementedError

    @abstractmethod
    def set_tick_callback(self, callback: Optional[TickCallback]) -> None:
        """
        设置或清除单参数兼容 tick callback。

        Args:
            callback: 接收不可变 tick mapping 的可选函数。

        Returns:
            None: callback 绑定完成后返回。
        """
        raise NotImplementedError

    @abstractmethod
    def set_market_event_callback(self, callback: Optional[MarketEventCallback]) -> None:
        """
        设置或清除类型化 MarketEvent callback。

        Args:
            callback: 接收一个 MarketEvent 的可选函数。

        Returns:
            None: callback 绑定完成后返回。
        """
        raise NotImplementedError


class MockRealtimeMarketDataFeed(RealtimeMarketDataFeed):
    """提供无网络、无 SDK 且可控确认订阅的线程安全合同测试 Feed。"""

    def __init__(
        self,
        manifest: CapabilityManifest,
        negotiated_event_types: Optional[Sequence[MarketEventType]] = None,
        limits: Optional[Mapping[str, Any]] = None,
        event_queue: Optional[BoundedMarketEventQueue] = None,
        stale_after_seconds: Optional[Mapping[MarketDataLevel, float]] = None,
        update_expectation_policy: Optional[MarketUpdateExpectationPolicy] = None,
        monotonic_clock: Optional[MonotonicClock] = None,
        module_capabilities: Optional[Mapping[str, Sequence[str]]] = None,
        subscription_adapter: Optional[SubscriptionActionAdapter] = None,
        subscription_ack_timeout_seconds: float = 5.0,
        max_subscription_records: int = 1024,
        max_subscription_items_per_request: int = 4096,
    ) -> None:
        """
        初始化断开状态的 Mock Feed。

        Args:
            manifest: Mock Provider 的静态能力和初始元数据。
            negotiated_event_types: 模拟 handshake 与账户共同获准的事件；默认由 manifest 推导。
            limits: 回执和 health 可公开的脱敏订阅限制。
            event_queue: 可选的共享离线有界队列；health 读取其水位和损失指标。
            stale_after_seconds: tick/L1/L2 分级失效阈值；默认均为 3 秒。
            update_expectation_policy: 显式交易时段/停牌 policy；未配置时读取 fail-closed。
            monotonic_clock: 记录 gateway 到达 age 的可注入单调时钟。
            module_capabilities: 可选模块到原子 capability ID 的显式归属。
            subscription_adapter: 可选离线 adapter；默认使用同步 callback 确认的内存实现。
            subscription_ack_timeout_seconds: 本地 accepted 后等待实际 callback 的正数秒数。
            max_subscription_records: 当前 Feed session 可保留的请求、回执和墓碑硬上限。
            max_subscription_items_per_request: 单请求展开的公开逐项状态硬上限。
        """
        self._lock = RLock()
        self._subscription_condition = Condition(self._lock)
        self._delivery_lock = RLock()
        self._callback_dispatch_active = False
        self._static_manifest = manifest.with_supported_readiness(
            CapabilityReadiness.UNAVAILABLE, "mock_disconnected"
        )
        self._admission_manifest = manifest.with_supported_readiness(
            CapabilityReadiness.UNAVAILABLE, "mock_disconnected"
        )
        self._connected = False
        self._ever_connected = False
        self._epoch_counter = 0
        self._session_epoch: Optional[str] = None
        self._reconnect_count = 0
        self._state_revision = 0
        self._source_gap_count = 0
        self._source_degraded_scopes: Dict[Tuple[str, ...], str] = {}
        self._seen_gap_boundaries: OrderedDict[Tuple[Any, ...], None] = OrderedDict()
        self._event_evidence: Set[Tuple[str, MarketDataLevel, str, Optional[str]]] = set()
        self._limits = MappingProxyType(dict(limits or {}))
        self._event_queue = event_queue
        thresholds = stale_after_seconds or {
            MarketDataLevel.TICK_COMPAT: 3.0,
            MarketDataLevel.L1: 3.0,
            MarketDataLevel.L2: 3.0,
        }
        self._freshness = MarketFreshnessTracker(
            thresholds,
            expectation_policy=update_expectation_policy,
            monotonic_clock=monotonic_clock,
        )
        if module_capabilities is None:
            derived_modules: Dict[str, Set[str]] = {level.value: set() for level in MarketDataLevel}
            for event_type, capability_id in _EVENT_CAPABILITY.items():
                declaration = manifest.get(capability_id)
                if declaration is None or declaration.support is CapabilitySupport.UNSUPPORTED:
                    continue
                for level in _EVENT_LEVELS[event_type]:
                    derived_modules[level.value].add(capability_id)
            normalized_modules = {
                module: tuple(sorted(capability_ids))
                for module, capability_ids in derived_modules.items()
            }
        else:
            normalized_modules = {
                str(module)
                .strip()
                .lower(): tuple(
                    sorted({str(item).strip() for item in capability_ids if str(item).strip()})
                )
                for module, capability_ids in module_capabilities.items()
            }
            if any(not module for module in normalized_modules):
                raise ValueError("module_capabilities 不能包含空模块名")
        self._module_capabilities = MappingProxyType(normalized_modules)
        if negotiated_event_types is None:
            derived = []
            for event_type, capability_id in _EVENT_CAPABILITY.items():
                declaration = manifest.get(capability_id)
                if (
                    declaration is not None
                    and declaration.support is not CapabilitySupport.UNSUPPORTED
                ):
                    derived.append(event_type)
            self._negotiated_event_types = tuple(sorted(set(derived), key=lambda item: item.value))
        else:
            normalized = {MarketEventType(item) for item in negotiated_event_types}
            if MarketEventType.ALL in normalized:
                raise ValueError("negotiated_event_types 不能包含 '*' 通配符")
            self._negotiated_event_types = tuple(sorted(normalized, key=lambda item: item.value))
        self._request_index: Dict[str, Tuple[str, str]] = {}
        self._specs: Dict[str, MarketSubscriptionSpec] = {}
        self._receipts: Dict[str, MarketSubscriptionReceipt] = {}
        self._subscription_item_plans: Dict[str, Tuple[FeedSubscriptionItemPlan, ...]] = {}
        self._active_subscription_ids: Dict[str, None] = {}
        self._subscription_request_inflight: Dict[str, str] = {}
        self._subscription_unsubscribe_inflight: Set[str] = set()
        self._subscription_retry_inflight: Set[str] = set()
        self._subscription_session_closing = False
        self._subscription_lifecycle_revision = 0
        self._max_subscription_records = _normalize_positive_limit(
            max_subscription_records,
            "max_subscription_records",
        )
        self._max_subscription_items_per_request = _normalize_positive_limit(
            max_subscription_items_per_request,
            "max_subscription_items_per_request",
        )
        self._subscription_receipts = SubscriptionReceiptProjector(
            self._subscription_failure,
            self._subscription_event_markets,
        )
        runtime_adapter = subscription_adapter or InMemorySubscriptionActionAdapter()
        subscription_manager = SubscriptionLeaseManager("mock-session-0")
        self._subscription_coordinator = SubscriptionActionCoordinator(
            subscription_manager,
            runtime_adapter,
            ack_timeout_seconds=subscription_ack_timeout_seconds,
            state_callback=self._on_subscription_state_change,
        )
        self._tick_callback: Optional[TickCallback] = None
        self._market_event_callback: Optional[MarketEventCallback] = None
        self._tick_cache: Dict[str, Mapping[str, Any]] = {}
        self._snapshot_cache: Dict[Tuple[str, MarketDataLevel], MarketEvent] = {}

    @property
    def manifest(self) -> CapabilityManifest:
        """
        返回当前 Mock 能力清单。

        Returns:
            CapabilityManifest: 受锁保护的不可变 manifest 快照。
        """
        return self._build_effective_state().manifest

    def connect(self) -> None:
        """
        幂等连接 Mock，创建新 session epoch 并仅开放订阅准入。

        Returns:
            None: 生命周期更新完成后返回。
        """
        with self._delivery_lock:
            if self._callback_dispatch_active:
                raise MarketDataFeedError("LIFECYCLE_CHANGE_DURING_CALLBACK")
            with self._lock:
                if self._connected:
                    return
                if self._ever_connected:
                    self._reconnect_count += 1
                self._ever_connected = True
                self._connected = True
                self._epoch_counter += 1
                self._session_epoch = f"mock-session-{self._epoch_counter}"
                self._admission_manifest = self._static_manifest.with_supported_readiness(
                    CapabilityReadiness.READY, None
                )
                self._tick_cache.clear()
                self._snapshot_cache.clear()
                self._event_evidence.clear()
                self._seen_gap_boundaries.clear()
                self._freshness.reset()
                self._subscription_lifecycle_revision += 1
                lifecycle_revision = self._subscription_lifecycle_revision
                session_epoch = self._session_epoch
                self._state_revision += 1
            self._subscription_coordinator.begin_session_epoch(
                session_epoch,
                dispatch=False,
            )
            with self._lock:
                dispatch_current = (
                    self._connected
                    and self._session_epoch == session_epoch
                    and self._subscription_lifecycle_revision == lifecycle_revision
                )
            if dispatch_current:
                self._subscription_coordinator.pump()

    def disconnect(self) -> None:
        """
        幂等断开 Mock，保留 desired lease 但将动态能力标为 unavailable。

        Returns:
            None: 生命周期更新完成后返回。
        """
        with self._delivery_lock:
            if self._callback_dispatch_active:
                raise MarketDataFeedError("LIFECYCLE_CHANGE_DURING_CALLBACK")
            with self._lock:
                if not self._connected:
                    return
                self._connected = False
                self._session_epoch = f"mock-disconnected-{self._epoch_counter}"
                self._admission_manifest = self._static_manifest.with_supported_readiness(
                    CapabilityReadiness.UNAVAILABLE, "mock_disconnected"
                )
                self._subscription_lifecycle_revision += 1
                session_epoch = self._session_epoch
                self._state_revision += 1
            self._subscription_coordinator.begin_session_epoch(
                session_epoch,
                dispatch=False,
            )

    def health(self) -> FeedHealth:
        """
        获取 Mock Feed 当前健康快照。

        Returns:
            FeedHealth: 包含动态能力和当前 active receipts 的不可变状态。
        """
        state = self._build_effective_state()
        return FeedHealth(
            provider=state.manifest.provider,
            connected=state.connected,
            manifest_version=state.manifest.manifest_version,
            session_epoch=state.session_epoch,
            active_subscriptions=state.active_subscriptions,
            capability_readiness=state.readiness,
            module_readiness=state.module_readiness,
            capability_event_times=state.capability_times,
            module_event_times=state.module_times,
            reconnect_count=state.reconnect_count,
            last_gateway_received_at=(
                state.latest_times.last_gateway_received_at if state.latest_times else None
            ),
            last_client_received_at=(
                state.latest_times.last_client_received_at if state.latest_times else None
            ),
            last_exchange_time=(
                state.latest_times.last_exchange_time if state.latest_times else None
            ),
            queue_depth=state.queue_depth,
            queue_control_depth=state.queue_control_depth,
            queue_capacity=state.queue_capacity,
            queue_control_capacity=state.queue_control_capacity,
            queue_control_scope_depth=state.queue_control_scope_depth,
            queue_control_scope_capacity=state.queue_control_scope_capacity,
            queue_control_overflow_count=state.queue_control_overflow_count,
            queue_high_watermark=state.queue_high_watermark,
            queue_overflow_count=state.queue_overflow_count,
            queue_coalesced_count=state.queue_coalesced_count,
            queue_loss_boundary_count=state.queue_loss_boundary_count,
            queue_degraded=state.queue_degraded,
            queue_overflow_by_event_type=state.queue_overflow_by_event_type,
            gap_count=state.source_gap_count,
            reasons=state.reasons,
        )

    def _build_effective_state(self) -> _EffectiveFeedState:
        """
        从连接、当前 epoch 回执、首帧、freshness、gap 和队列构造唯一运行态。

        Returns:
            _EffectiveFeedState: 同时供 public manifest 与 health 使用的不可变快照。

        Raises:
            MarketDataFeedError: 状态持续变化且无法取得一致快照时 fail-closed 抛出。

        Side Effects:
            在 Feed 锁外执行显式更新 policy，避免慢 calendar/status owner 阻塞 callback。
        """
        for _attempt in range(5):
            with self._lock:
                revision = self._state_revision
                connected = self._connected
                session_epoch = self._session_epoch
                reconnect_count = self._reconnect_count
                static_manifest = self._static_manifest
                active = {
                    subscription_id: self._receipts[subscription_id]
                    for subscription_id in self._active_subscription_ids
                }
                event_evidence = frozenset(self._event_evidence)
                degraded_scopes = dict(self._source_degraded_scopes)
                source_gap_count = self._source_gap_count
                module_capabilities = dict(self._module_capabilities)

            freshness_snapshot = self._freshness.runtime_snapshot()
            freshness_readiness = freshness_snapshot.capability_readiness
            latest_times = freshness_snapshot.latest_times
            capability_times = freshness_snapshot.capability_times
            module_times = freshness_snapshot.module_times
            freshness_error = freshness_snapshot.failure_reason

            if self._event_queue is None:
                queue_depth = 0
                queue_control_depth = 0
                queue_capacity = 0
                queue_control_capacity = 0
                queue_control_scope_depth = 0
                queue_control_scope_capacity = 0
                queue_control_overflow_count = 0
                queue_high_watermark = 0
                queue_overflow_count = 0
                queue_coalesced_count = 0
                queue_loss_boundary_count = 0
                queue_degraded = False
                queue_overflow_by_event_type: Mapping[str, int] = {}
            else:
                queue_metrics = self._event_queue.metrics()
                queue_depth = queue_metrics.data_depth
                queue_control_depth = queue_metrics.control_depth
                queue_capacity = queue_metrics.capacity
                queue_control_capacity = queue_metrics.control_capacity
                queue_control_scope_depth = queue_metrics.control_scope_depth
                queue_control_scope_capacity = queue_metrics.control_scope_capacity
                queue_control_overflow_count = queue_metrics.control_overflow_count
                queue_high_watermark = queue_metrics.high_watermark
                queue_overflow_count = queue_metrics.overflow_count
                queue_coalesced_count = queue_metrics.coalesced_count
                queue_loss_boundary_count = queue_metrics.loss_boundary_count
                queue_degraded = queue_metrics.degraded
                queue_overflow_by_event_type = queue_metrics.overflow_by_event_type

            with self._lock:
                if revision != self._state_revision:
                    continue

            confirmed_capabilities = self._confirmed_capabilities(active)
            missing_first_event = self._missing_first_event_capabilities(
                active,
                event_evidence,
            )
            active_degraded_scopes = {
                scope: reason
                for scope, reason in degraded_scopes.items()
                if self._control_scope_has_confirmed_coverage(scope, active)
            }
            degraded_by_capability: Dict[str, str] = {}
            for scope, reason in active_degraded_scopes.items():
                degraded_by_capability.setdefault(scope[1], reason)
            readiness: Dict[str, CapabilityReadiness] = {}
            readiness_reasons: Dict[str, Optional[str]] = {}
            snapshot_capabilities = {
                _EVENT_CAPABILITY[MarketEventType.TICK_COMPAT],
                _EVENT_CAPABILITY[MarketEventType.SNAPSHOT_L1],
                _EVENT_CAPABILITY[MarketEventType.SNAPSHOT_L2],
            }
            for capability_id, declaration in static_manifest.capabilities.items():
                if declaration.support is CapabilitySupport.UNSUPPORTED:
                    readiness[capability_id] = CapabilityReadiness.UNAVAILABLE
                    readiness_reasons[capability_id] = declaration.reason or "unsupported"
                elif not connected:
                    readiness[capability_id] = CapabilityReadiness.UNAVAILABLE
                    readiness_reasons[capability_id] = "feed_disconnected"
                elif capability_id not in confirmed_capabilities:
                    readiness[capability_id] = CapabilityReadiness.UNAVAILABLE
                    readiness_reasons[capability_id] = "subscription_not_confirmed"
                elif capability_id in missing_first_event:
                    readiness[capability_id] = CapabilityReadiness.UNAVAILABLE
                    readiness_reasons[capability_id] = "current_epoch_event_not_received"
                elif capability_id not in capability_times:
                    readiness[capability_id] = CapabilityReadiness.UNAVAILABLE
                    readiness_reasons[capability_id] = "current_epoch_event_not_received"
                elif capability_id in snapshot_capabilities:
                    state = freshness_readiness.get(capability_id, CapabilityReadiness.UNAVAILABLE)
                    readiness[capability_id] = state
                    readiness_reasons[capability_id] = (
                        freshness_error
                        if freshness_error is not None
                        else self._freshness_reason(state)
                    )
                else:
                    readiness[capability_id] = CapabilityReadiness.READY
                    readiness_reasons[capability_id] = "current_epoch_event_received"

            queue_degraded_capabilities: Set[str] = set()
            for raw_event_type in queue_overflow_by_event_type:
                event_type = MarketEventType(raw_event_type)
                queue_capability_id = _EVENT_CAPABILITY.get(event_type)
                if queue_capability_id is not None:
                    queue_degraded_capabilities.add(queue_capability_id)
                if queue_capability_id is not None:
                    if (
                        queue_control_overflow_count
                        and queue_capability_id in confirmed_capabilities
                    ):
                        readiness[queue_capability_id] = CapabilityReadiness.UNAVAILABLE
                        readiness_reasons[
                            queue_capability_id
                        ] = "market_event_control_capacity_exhausted"
                    elif readiness.get(queue_capability_id) is CapabilityReadiness.READY:
                        readiness[queue_capability_id] = CapabilityReadiness.DEGRADED
                        readiness_reasons[queue_capability_id] = "market_event_queue_overflow"

            active_modules = self._active_module_capabilities(active, module_capabilities)
            module_readiness: Dict[str, CapabilityReadiness] = {}
            for module, capability_ids in active_modules.items():
                states = []
                for capability_id in capability_ids:
                    module_declaration = static_manifest.get(capability_id)
                    items = self._confirmed_items_for_module_capability(
                        active, module, capability_id
                    )
                    if (
                        module_declaration is None
                        or module_declaration.support is CapabilitySupport.UNSUPPORTED
                    ):
                        state = CapabilityReadiness.UNAVAILABLE
                    elif not connected or not items:
                        state = CapabilityReadiness.UNAVAILABLE
                    elif any(
                        not self._item_has_event_evidence(item, capability_id, event_evidence)
                        for item in items
                    ):
                        state = CapabilityReadiness.UNAVAILABLE
                    elif capability_id in snapshot_capabilities:
                        state = freshness_readiness.get(
                            capability_id, CapabilityReadiness.UNAVAILABLE
                        )
                    else:
                        state = CapabilityReadiness.READY
                    if capability_id in queue_degraded_capabilities:
                        if queue_control_overflow_count:
                            state = CapabilityReadiness.UNAVAILABLE
                        elif state is CapabilityReadiness.READY:
                            state = CapabilityReadiness.DEGRADED
                    states.append(state)
                module_readiness[module] = (
                    max(states, key=lambda state: _FEED_READINESS_PRIORITY[state])
                    if states
                    else CapabilityReadiness.UNAVAILABLE
                )
            for scope in active_degraded_scopes:
                capability_id = scope[1]
                module = scope[2]
                if (
                    capability_id in active_modules.get(module, ())
                    and module_readiness.get(module) is CapabilityReadiness.READY
                ):
                    module_readiness[module] = CapabilityReadiness.DEGRADED

            for capability_id, reason in degraded_by_capability.items():
                if readiness.get(capability_id) is CapabilityReadiness.READY:
                    readiness[capability_id] = CapabilityReadiness.DEGRADED
                    readiness_reasons[capability_id] = reason

            effective_manifest = static_manifest
            for capability_id, state in readiness.items():
                effective_manifest = effective_manifest.with_readiness(
                    capability_id, state, readiness_reasons[capability_id]
                )
            reasons = [] if connected else ["mock_disconnected"]
            if not self._freshness.policy_configured:
                reasons.append("market_update_policy_not_configured")
            if freshness_error is not None:
                reasons.append(freshness_error)
            if queue_degraded:
                reasons.append("market_event_queue_degraded")
            if queue_control_overflow_count:
                reasons.append("market_event_control_capacity_exhausted")
            if active_degraded_scopes:
                reasons.append("market_stream_continuity_degraded")
            return _EffectiveFeedState(
                manifest=effective_manifest,
                readiness=MappingProxyType(readiness),
                readiness_reasons=MappingProxyType(readiness_reasons),
                module_readiness=module_readiness,
                latest_times=latest_times,
                capability_times=capability_times,
                module_times=module_times,
                active_subscriptions=MappingProxyType(active),
                connected=connected,
                session_epoch=session_epoch,
                reconnect_count=reconnect_count,
                source_gap_count=source_gap_count,
                queue_depth=queue_depth,
                queue_control_depth=queue_control_depth,
                queue_capacity=queue_capacity,
                queue_control_capacity=queue_control_capacity,
                queue_control_scope_depth=queue_control_scope_depth,
                queue_control_scope_capacity=queue_control_scope_capacity,
                queue_control_overflow_count=queue_control_overflow_count,
                queue_high_watermark=queue_high_watermark,
                queue_overflow_count=queue_overflow_count,
                queue_coalesced_count=queue_coalesced_count,
                queue_loss_boundary_count=queue_loss_boundary_count,
                queue_degraded=queue_degraded,
                queue_overflow_by_event_type=queue_overflow_by_event_type,
                reasons=tuple(reasons),
            )
        raise MarketDataFeedError("FEED_RUNTIME_SNAPSHOT_UNSTABLE")

    @staticmethod
    def _confirmed_capabilities(
        active_subscriptions: Mapping[str, MarketSubscriptionReceipt],
    ) -> Set[str]:
        """
        从当前 epoch 的 active receipts 提取实际 confirmed 原子能力。

        Args:
            active_subscriptions: 当前 active subscription ID 到回执的映射。

        Returns:
            Set[str]: 至少存在一个 confirmed item 的原子能力集合。
        """
        capabilities: Set[str] = set()
        for receipt in active_subscriptions.values():
            for item in receipt.confirmed:
                capability_id = _EVENT_CAPABILITY.get(item.event_type)
                if capability_id is not None:
                    capabilities.add(capability_id)
        return capabilities

    @staticmethod
    def _missing_first_event_capabilities(
        active_subscriptions: Mapping[str, MarketSubscriptionReceipt],
        event_evidence: AbstractSet[Tuple[str, MarketDataLevel, str, Optional[str]]],
    ) -> Set[str]:
        """
        找出当前确认 scope 尚未收到本 epoch 首帧的原子能力。

        Args:
            active_subscriptions: 当前 active 回执映射。
            event_evidence: 当前 epoch 已接收事件的能力、级别、市场和证券证据。

        Returns:
            Set[str]: 至少一个 confirmed scope 尚无首帧的能力集合。
        """
        missing: Set[str] = set()
        for receipt in active_subscriptions.values():
            for item in receipt.confirmed:
                capability_id = _EVENT_CAPABILITY.get(item.event_type)
                if capability_id is None:
                    continue
                if not MockRealtimeMarketDataFeed._item_has_event_evidence(
                    item, capability_id, event_evidence
                ):
                    missing.add(capability_id)
        return missing

    @staticmethod
    def _item_has_event_evidence(
        item: SubscriptionItemResult,
        capability_id: str,
        event_evidence: AbstractSet[Tuple[str, MarketDataLevel, str, Optional[str]]],
    ) -> bool:
        """
        判断一个 confirmed item 是否拥有当前 epoch 的匹配首帧证据。

        Args:
            item: 已确认的单项订阅回执。
            capability_id: item 对应的原子能力 ID。
            event_evidence: 当前 epoch 的能力、级别、市场和证券证据。

        Returns:
            bool: 存在同能力/级别且 selector 覆盖的事件时为 True。
        """
        return any(
            evidence_capability == capability_id
            and evidence_level is item.level
            and MockRealtimeMarketDataFeed._selector_covers_scope(
                item,
                evidence_exchange,
                evidence_security,
            )
            for (
                evidence_capability,
                evidence_level,
                evidence_exchange,
                evidence_security,
            ) in event_evidence
        )

    @staticmethod
    def _confirmed_items_for_module_capability(
        active_subscriptions: Mapping[str, MarketSubscriptionReceipt],
        module: str,
        capability_id: str,
    ) -> Tuple[SubscriptionItemResult, ...]:
        """
        提取模块内同一 capability 的 confirmed items。

        Args:
            active_subscriptions: 当前 active 回执映射。
            module: tick_compat、L1、L2 或调用方自定义模块名。
            capability_id: 需要聚合的原子能力 ID。

        Returns:
            Tuple[SubscriptionItemResult, ...]: 标准级别模块只含同级 items；自定义模块保守含全部。
        """
        level_modules = {level.value for level in MarketDataLevel}
        items = []
        for receipt in active_subscriptions.values():
            for item in receipt.confirmed:
                if _EVENT_CAPABILITY.get(item.event_type) != capability_id:
                    continue
                if module in level_modules and item.level.value != module:
                    continue
                items.append(item)
        return tuple(items)

    @staticmethod
    def _active_module_capabilities(
        active_subscriptions: Mapping[str, MarketSubscriptionReceipt],
        module_capabilities: Mapping[str, Tuple[str, ...]],
    ) -> Mapping[str, Tuple[str, ...]]:
        """
        按 confirmed item 的精确 level 生成模块能力集合。

        Args:
            active_subscriptions: 当前 active 回执映射。
            module_capabilities: 配置的模块到原子 capability 归属。

        Returns:
            Mapping[str, Tuple[str, ...]]: 不把 L1 的共享 capability 冒充为 L2 活跃能力。
        """
        active_by_level: Dict[str, Set[str]] = {level.value: set() for level in MarketDataLevel}
        active_any: Set[str] = set()
        for receipt in active_subscriptions.values():
            for item in receipt.confirmed:
                capability_id = _EVENT_CAPABILITY.get(item.event_type)
                if capability_id is None:
                    continue
                active_by_level[item.level.value].add(capability_id)
                active_any.add(capability_id)
        return {
            module: tuple(
                capability_id
                for capability_id in capability_ids
                if capability_id
                in (active_by_level[module] if module in active_by_level else active_any)
            )
            for module, capability_ids in module_capabilities.items()
        }

    @staticmethod
    def _selector_covers_scope(
        item: SubscriptionItemResult,
        exchange: str,
        security: Optional[str],
    ) -> bool:
        """
        判断一个 confirmed item 是否覆盖事件或 continuity scope。

        Args:
            item: 已确认的单项订阅回执。
            exchange: 标准交易所代码；空值表示 Provider 级控制边界。
            security: 可选标准证券代码；None 表示市场或通道级事件。

        Returns:
            bool: selector 覆盖该证券、市场或更宽控制 scope 时为 True。
        """
        if item.selector is SubscriptionSelector.ALL:
            return not exchange or item.scope in {"*", exchange}
        if item.selector is SubscriptionSelector.SYMBOLS:
            if security is not None:
                return item.scope == security
            return not exchange or item.scope.rsplit(".", 1)[-1] == exchange
        if item.selector is SubscriptionSelector.MARKETS:
            return not exchange or item.scope == exchange
        return False

    @staticmethod
    def _control_scope_has_confirmed_coverage(
        scope: Tuple[str, ...],
        active_subscriptions: Mapping[str, MarketSubscriptionReceipt],
    ) -> bool:
        """
        判断历史或当前 epoch 的 continuity scope 是否仍被 active lease 覆盖。

        Args:
            scope: Provider、能力、级别、epoch、通道、市场和证券组成的 scope。
            active_subscriptions: 当前 active 回执映射。

        Returns:
            bool: 存在相同 capability/level 且 selector 覆盖该 scope 时为 True。
        """
        capability_id = scope[1]
        level = MarketDataLevel(scope[2])
        exchange = scope[6]
        security = scope[7] or None
        for receipt in active_subscriptions.values():
            for item in receipt.confirmed:
                if (
                    _EVENT_CAPABILITY.get(item.event_type) == capability_id
                    and item.level is level
                    and MockRealtimeMarketDataFeed._selector_covers_scope(item, exchange, security)
                ):
                    return True
        return False

    @staticmethod
    def _event_evidence_has_confirmed_coverage(
        evidence: Tuple[str, MarketDataLevel, str, Optional[str]],
        active_subscriptions: Mapping[str, MarketSubscriptionReceipt],
    ) -> bool:
        """
        判断当前 epoch 的首帧证据是否仍被 active lease 覆盖。

        Args:
            evidence: 能力、级别、市场和证券组成的首帧证据。
            active_subscriptions: 当前 active 回执映射。

        Returns:
            bool: 至少一个同能力/级别 confirmed selector 覆盖证据时为 True。
        """
        capability_id, level, exchange, security = evidence
        for receipt in active_subscriptions.values():
            for item in receipt.confirmed:
                if (
                    _EVENT_CAPABILITY.get(item.event_type) == capability_id
                    and item.level is level
                    and MockRealtimeMarketDataFeed._selector_covers_scope(item, exchange, security)
                ):
                    return True
        return False

    @staticmethod
    def _freshness_reason(state: CapabilityReadiness) -> str:
        """
        将 freshness readiness 转成稳定、可运维断言的原因。

        Args:
            state: tracker 聚合出的动态 readiness。

        Returns:
            str: 与 ready、stale 或 unavailable 对应的稳定原因。
        """
        return {
            CapabilityReadiness.READY: "fresh",
            CapabilityReadiness.STALE: "stale_threshold_exceeded",
            CapabilityReadiness.UNAVAILABLE: "market_update_policy_unavailable",
            CapabilityReadiness.DEGRADED: "freshness_degraded",
            CapabilityReadiness.UNAUTHORIZED: "freshness_unauthorized",
        }[state]

    def get_current_tick(self, security: str) -> Mapping[str, Any]:
        """
        返回已投递到确认 lease 的最新兼容 tick 副本。

        Args:
            security: 标准证券代码。

        Returns:
            Mapping[str, Any]: 最新 tick 的只读副本。

        Raises:
            FeedNotConnectedError: Feed 未连接时抛出。
            RealtimeDataUnavailableError: 该证券尚无兼容 tick 时抛出。
        """
        return self._read_current_tick(security, allow_stale=False).tick

    def diagnose_current_tick(
        self, security: str, *, allow_stale: bool = False
    ) -> CurrentTickDiagnostic:
        """
        通过独立诊断入口读取 tick，只有显式参数才能返回 stale 缓存。

        Args:
            security: 标准证券代码。
            allow_stale: 是否仅为诊断目的容忍 stale 或 policy 未配置。

        Returns:
            CurrentTickDiagnostic: 最新 tick 与同次读取的显式 market state/age。
        """
        return self._read_current_tick(security, allow_stale=allow_stale)

    def get_market_snapshot(self, security: str, level: MarketDataLevel) -> MarketEvent:
        """
        返回已投递到确认 lease 的最新 L1/L2 typed 快照。

        Args:
            security: 标准证券代码。
            level: L1 或 L2；tick_compat 不属于 typed snapshot。

        Returns:
            MarketEvent: 最新 typed 快照。

        Raises:
            FeedNotConnectedError: Feed 未连接时抛出。
            RealtimeDataUnavailableError: 尚无对应快照时抛出。
            ValueError: level 为 tick_compat 时抛出。
        """
        return self._read_market_snapshot(security, level, allow_stale=False).event

    def diagnose_market_snapshot(
        self,
        security: str,
        level: MarketDataLevel,
        *,
        allow_stale: bool = False,
    ) -> MarketSnapshotDiagnostic:
        """
        通过独立诊断入口读取精确快照，只有显式参数才能返回 stale 缓存。

        Args:
            security: 标准证券代码。
            level: 明确的 L1 或 L2，不允许兼容 tick。
            allow_stale: 是否仅为诊断目的容忍 stale 或 policy 未配置。

        Returns:
            MarketSnapshotDiagnostic: 精确级别缓存与显式 market state/age，不执行 fallback。
        """
        return self._read_market_snapshot(security, level, allow_stale=allow_stale)

    def _read_current_tick(self, security: str, *, allow_stale: bool) -> CurrentTickDiagnostic:
        """
        执行兼容 tick 的连接、订阅、存在性和 freshness 四层门禁。

        Args:
            security: 标准证券代码。
            allow_stale: 仅由独立诊断入口传入的显式 stale 容忍开关。

        Returns:
            CurrentTickDiagnostic: 通过门禁的只读 tick 与 freshness 决策。

        Raises:
            RealtimeDataUnavailableError: 未连接、无确认订阅、无缓存或 policy 未配置时抛出。
            StaleMarketDataError: 策略读取命中 stale 缓存时抛出。
        """
        normalized_security = str(security).strip().upper()
        if not normalized_security:
            raise ValueError("security 不能为空")
        for _attempt in range(5):
            with self._lock:
                self._require_realtime_read_connected()
                self._require_confirmed_read_subscription(
                    normalized_security,
                    MarketDataLevel.TICK_COMPAT,
                    MarketEventType.TICK_COMPAT,
                )
                tick = self._tick_cache.get(normalized_security)
                if tick is None:
                    raise RealtimeDataUnavailableError(
                        f"REALTIME_TICK_NO_SNAPSHOT: security={normalized_security}"
                    )
                revision = self._state_revision
            freshness_error: Optional[MarketDataFeedError] = None
            freshness_decision: Optional[FreshnessDecision] = None
            try:
                freshness_decision = self._require_freshness(
                    normalized_security,
                    MarketDataLevel.TICK_COMPAT,
                    allow_stale=allow_stale,
                )
            except (RealtimeDataUnavailableError, StaleMarketDataError) as exc:
                freshness_error = exc
            with self._lock:
                if (
                    revision != self._state_revision
                    or self._tick_cache.get(normalized_security) is not tick
                ):
                    continue
            if freshness_error is not None:
                raise freshness_error
            if freshness_decision is None:
                raise RealtimeDataUnavailableError("REALTIME_FRESHNESS_DECISION_MISSING")
            return CurrentTickDiagnostic(tick=tick, freshness=freshness_decision)
        raise RealtimeDataUnavailableError("REALTIME_READ_STATE_UNSTABLE")

    def _read_market_snapshot(
        self,
        security: str,
        level: MarketDataLevel,
        *,
        allow_stale: bool,
    ) -> MarketSnapshotDiagnostic:
        """
        执行 typed 快照的精确级别读取与 freshness 门禁。

        Args:
            security: 标准证券代码。
            level: 明确的 L1 或 L2。
            allow_stale: 仅由独立诊断入口传入的显式 stale 容忍开关。

        Returns:
            MarketSnapshotDiagnostic: 通过门禁的同级事件及同次 freshness 决策。

        Raises:
            RealtimeDataUnavailableError: 未连接、无确认订阅、无快照或 policy 未配置时抛出。
            StaleMarketDataError: 策略读取命中 stale 快照时抛出。
            ValueError: level 为 tick_compat 时抛出。
        """
        normalized_security = str(security).strip().upper()
        normalized_level = MarketDataLevel(level)
        if not normalized_security:
            raise ValueError("security 不能为空")
        if normalized_level is MarketDataLevel.TICK_COMPAT:
            raise ValueError("typed snapshot level 必须是 l1 或 l2")
        event_type = (
            MarketEventType.SNAPSHOT_L1
            if normalized_level is MarketDataLevel.L1
            else MarketEventType.SNAPSHOT_L2
        )
        for _attempt in range(5):
            with self._lock:
                self._require_realtime_read_connected()
                self._require_confirmed_read_subscription(
                    normalized_security, normalized_level, event_type
                )
                event = self._snapshot_cache.get((normalized_security, normalized_level))
                if event is None:
                    raise RealtimeDataUnavailableError(
                        "REALTIME_SNAPSHOT_NOT_RECEIVED: "
                        f"security={normalized_security}, level={normalized_level.value}"
                    )
                revision = self._state_revision
            freshness_error: Optional[MarketDataFeedError] = None
            freshness_decision: Optional[FreshnessDecision] = None
            try:
                freshness_decision = self._require_freshness(
                    normalized_security,
                    normalized_level,
                    allow_stale=allow_stale,
                )
            except (RealtimeDataUnavailableError, StaleMarketDataError) as exc:
                freshness_error = exc
            with self._lock:
                if (
                    revision != self._state_revision
                    or self._snapshot_cache.get((normalized_security, normalized_level))
                    is not event
                ):
                    continue
            if freshness_error is not None:
                raise freshness_error
            if freshness_decision is None:
                raise RealtimeDataUnavailableError("REALTIME_FRESHNESS_DECISION_MISSING")
            return MarketSnapshotDiagnostic(event=event, freshness=freshness_decision)
        raise RealtimeDataUnavailableError("REALTIME_READ_STATE_UNSTABLE")

    def _require_realtime_read_connected(self) -> None:
        """
        将策略读取的未连接状态转换为具名 realtime unavailable 语义。

        Returns:
            None: Feed 已连接时返回。

        Raises:
            RealtimeDataUnavailableError: Feed 尚未连接或已经断开时抛出。
        """
        if not self._connected:
            raise RealtimeDataUnavailableError("REALTIME_DATA_FEED_NOT_CONNECTED")

    def _require_confirmed_read_subscription(
        self,
        security: str,
        level: MarketDataLevel,
        event_type: MarketEventType,
    ) -> None:
        """
        要求当前 session 存在覆盖证券和精确事件级别的 confirmed lease。

        Args:
            security: 已规范化的标准证券代码。
            level: 精确行情级别。
            event_type: tick、L1 快照或 L2 快照实际事件类型。

        Returns:
            None: 找到确认项时返回。

        Raises:
            RealtimeDataUnavailableError: 未找到仍 active 的确认订阅时抛出。
        """
        if self._has_confirmed_read_subscription_locked(security, level, event_type):
            return
        raise RealtimeDataUnavailableError(
            "REALTIME_SUBSCRIPTION_NOT_CONFIRMED: "
            f"security={security}, level={level.value}, event_type={event_type.value}"
        )

    def _has_confirmed_read_subscription_locked(
        self,
        security: str,
        level: MarketDataLevel,
        event_type: MarketEventType,
    ) -> bool:
        """
        判断当前 active receipts 是否覆盖精确证券、级别和快照事件。

        Args:
            security: 已规范化证券代码。
            level: tick_compat、L1 或 L2 精确级别。
            event_type: 对应 tick 或快照事件类型。

        Returns:
            bool: 至少一个 confirmed selector 覆盖当前读取 scope 时为 True。
        """
        exchange = security.rsplit(".", 1)[-1] if "." in security else ""
        for subscription_id in self._active_subscription_ids:
            spec = self._specs[subscription_id]
            if spec.level is not level:
                continue
            receipt = self._receipts[subscription_id]
            for item in receipt.confirmed:
                if item.event_type is not event_type:
                    continue
                if item.selector is SubscriptionSelector.ALL and item.scope in {
                    "*",
                    exchange,
                }:
                    return True
                if item.selector is SubscriptionSelector.SYMBOLS and item.scope == security:
                    return True
                if item.selector is SubscriptionSelector.MARKETS and item.scope == exchange:
                    return True
        return False

    def _purge_uncovered_snapshot_state_locked(self) -> None:
        """
        清除已不再被任何 active lease 覆盖的缓存与原 ingress 时效证据。

        Returns:
            None: tick、L1/L2 缓存和 tracker scope 保持一致后返回。

        Side Effects:
            只删除失去覆盖的 scope，不重采样仍有效 scope 的 gateway 单调时间。
        """
        for security in tuple(self._tick_cache):
            if not self._has_confirmed_read_subscription_locked(
                security,
                MarketDataLevel.TICK_COMPAT,
                MarketEventType.TICK_COMPAT,
            ):
                self._tick_cache.pop(security, None)
        for security, level in tuple(self._snapshot_cache):
            event_type = (
                MarketEventType.SNAPSHOT_L1
                if level is MarketDataLevel.L1
                else MarketEventType.SNAPSHOT_L2
            )
            if not self._has_confirmed_read_subscription_locked(
                security,
                level,
                event_type,
            ):
                self._snapshot_cache.pop((security, level), None)
        retained_keys = frozenset(
            {(security, MarketDataLevel.TICK_COMPAT) for security in self._tick_cache}
            | set(self._snapshot_cache)
        )
        self._freshness.retain_snapshot_keys(retained_keys)
        active = {
            subscription_id: self._receipts[subscription_id]
            for subscription_id in self._active_subscription_ids
        }
        self._event_evidence = {
            evidence
            for evidence in self._event_evidence
            if self._event_evidence_has_confirmed_coverage(evidence, active)
        }
        self._source_degraded_scopes = {
            scope: reason
            for scope, reason in self._source_degraded_scopes.items()
            if self._control_scope_has_confirmed_coverage(scope, active)
        }

    def _require_freshness(
        self,
        security: str,
        level: MarketDataLevel,
        *,
        allow_stale: bool,
    ) -> FreshnessDecision:
        """
        执行显式 policy freshness 判定并阻断策略的 stale/未知读取。

        Args:
            security: 已规范化证券代码。
            level: 精确行情级别。
            allow_stale: 诊断入口显式传入的容忍开关。

        Returns:
            FreshnessDecision: 可供诊断记录的完整判定。

        Raises:
            RealtimeDataUnavailableError: 无记录或 policy 未配置且未显式诊断容忍时抛出。
            StaleMarketDataError: 数据 stale 且未显式诊断容忍时抛出。
        """
        try:
            decision = self._freshness.evaluate(security, level)
        except FreshnessRecordNotFoundError as exc:
            raise RealtimeDataUnavailableError(
                f"REALTIME_FRESHNESS_RECORD_UNAVAILABLE: security={security}, level={level.value}"
            ) from exc
        except MarketFreshnessError as exc:
            raise RealtimeDataUnavailableError(
                "REALTIME_UPDATE_POLICY_UNAVAILABLE: "
                f"security={security}, level={level.value}, cause={type(exc).__name__}"
            ) from exc
        if decision.market_state == "policy_unconfigured" and not allow_stale:
            raise RealtimeDataUnavailableError(
                "REALTIME_UPDATE_POLICY_UNAVAILABLE: " f"security={security}, level={level.value}"
            )
        if not decision.source_time_verified and not allow_stale:
            raise RealtimeDataUnavailableError(
                "REALTIME_EXCHANGE_TIME_UNVERIFIED: "
                f"security={security}, level={level.value}, "
                f"exchange_time={decision.last_exchange_time}"
            )
        if decision.stale and not allow_stale:
            raise StaleMarketDataError(decision)
        return decision

    def subscribe(self, spec: MarketSubscriptionSpec) -> MarketSubscriptionReceipt:
        """
        评估 Mock 订阅并经通用 coordinator 等待实际 callback，保持请求幂等。

        Args:
            spec: 已规范化的部分、市场或全部范围订阅。

        Returns:
            MarketSubscriptionReceipt: 通配展开和逐项结果。

        Raises:
            FeedNotConnectedError: Feed 未连接时抛出。
            SubscriptionConflictError: request_id 复用于不同 fingerprint 时抛出。
            RealtimeDataUnavailableError: 通配符没有任何获准实际事件时抛出。
        """
        if not isinstance(spec, MarketSubscriptionSpec):
            raise ValueError("spec 必须为 MarketSubscriptionSpec")
        with self._subscription_condition:
            while self._subscription_session_closing:
                self._subscription_condition.wait()
            while spec.request_id in self._subscription_request_inflight:
                inflight_fingerprint = self._subscription_request_inflight[spec.request_id]
                if inflight_fingerprint != spec.fingerprint:
                    raise SubscriptionConflictError(
                        "SUBSCRIPTION_REQUEST_CONFLICT: "
                        f"request_id={spec.request_id}, previous={inflight_fingerprint}, "
                        f"current={spec.fingerprint}"
                    )
                self._subscription_condition.wait()
            self._require_connected()
            previous = self._request_index.get(spec.request_id)
            if previous is not None:
                previous_fingerprint, previous_subscription_id = previous
                if previous_fingerprint != spec.fingerprint:
                    raise SubscriptionConflictError(
                        "SUBSCRIPTION_REQUEST_CONFLICT: "
                        f"request_id={spec.request_id}, previous={previous_fingerprint}, "
                        f"current={spec.fingerprint}"
                    )
                return self._receipts[previous_subscription_id]
            if len(self._request_index) >= self._max_subscription_records:
                raise SubscriptionCapacityError("SUBSCRIPTION_RECORD_LIMIT")
            actual_event_types = self._expand_event_types(spec)
            estimated_items = self._subscription_receipts.estimate_plan_count(
                spec,
                actual_event_types,
            )
            if estimated_items > self._max_subscription_items_per_request:
                raise SubscriptionCapacityError("SUBSCRIPTION_ITEM_LIMIT")
            effective_symbols, effective_markets = self._effective_scope(spec, actual_event_types)
            plans = self._subscription_receipts.build_plans(spec, actual_event_types)
            token = hashlib.sha256(
                f"{spec.request_id}:{spec.fingerprint}".encode("utf-8")
            ).hexdigest()[:20]
            subscription_id = f"mock-{token}"
            initial_items = self._subscription_receipts.initial_items(plans)
            session_epoch = self._require_session_epoch()
            receipt = MarketSubscriptionReceipt.from_items(
                subscription_id=subscription_id,
                spec=spec,
                session_epoch=session_epoch,
                items=initial_items,
                actual_event_types=actual_event_types,
                effective_symbols=effective_symbols,
                effective_markets=effective_markets,
                limits=self._limits,
            )
            self._request_index[spec.request_id] = (spec.fingerprint, subscription_id)
            self._specs[subscription_id] = spec
            self._receipts[subscription_id] = receipt
            self._subscription_item_plans[subscription_id] = plans
            if receipt.confirmed or receipt.pending:
                self._active_subscription_ids[subscription_id] = None
            adapter_scopes = self._subscription_receipts.unique_adapter_scopes(plans)
            lifecycle_revision = self._subscription_lifecycle_revision
            self._subscription_request_inflight[spec.request_id] = spec.fingerprint
            self._state_revision += 1

        registered = False
        try:
            if adapter_scopes:
                with self._delivery_lock:
                    self._subscription_coordinator.add_lease(
                        session_id="mock-feed",
                        subscription_id=subscription_id,
                        request_id=spec.request_id,
                        payload_fingerprint=spec.fingerprint,
                        scopes=adapter_scopes,
                        dispatch=False,
                    )
                    registered = True
                    with self._lock:
                        dispatch_current = (
                            self._connected
                            and self._session_epoch == session_epoch
                            and self._subscription_lifecycle_revision == lifecycle_revision
                        )
                    if dispatch_current:
                        self._subscription_coordinator.pump()
                snapshot = self._subscription_coordinator.snapshot()
                with self._lock:
                    self._refresh_subscription_receipts_locked(snapshot)
                    self._state_revision += 1
            with self._lock:
                return self._receipts[subscription_id]
        except Exception:
            if not registered:
                with self._lock:
                    self._request_index.pop(spec.request_id, None)
                    self._specs.pop(subscription_id, None)
                    self._receipts.pop(subscription_id, None)
                    self._subscription_item_plans.pop(subscription_id, None)
                    self._active_subscription_ids.pop(subscription_id, None)
                    self._state_revision += 1
            raise
        finally:
            with self._subscription_condition:
                self._subscription_request_inflight.pop(spec.request_id, None)
                self._subscription_condition.notify_all()

    def unsubscribe(self, subscription_id: str) -> MarketSubscriptionReceipt:
        """
        取消明确订阅 ID，重复取消时返回同一个 canceled 回执。

        Args:
            subscription_id: 需要取消的唯一 lease ID。

        Returns:
            MarketSubscriptionReceipt: canceled 状态回执。

        Raises:
            SubscriptionNotFoundError: ID 从未创建时抛出。
        """
        normalized_id = subscription_id.strip()
        if not normalized_id:
            raise ValueError("subscription_id 不能为空")
        with self._subscription_condition:
            while self._subscription_session_closing:
                self._subscription_condition.wait()
            while normalized_id in self._subscription_unsubscribe_inflight:
                self._subscription_condition.wait()
            receipt = self._receipts.get(normalized_id)
            if receipt is None:
                raise SubscriptionNotFoundError(
                    f"SUBSCRIPTION_NOT_FOUND: subscription_id={normalized_id}"
                )
            if normalized_id not in self._active_subscription_ids:
                return receipt
            self._subscription_unsubscribe_inflight.add(normalized_id)
            self._active_subscription_ids.pop(normalized_id, None)
            has_adapter_scopes = any(
                plan.adapter_scopes for plan in self._subscription_item_plans[normalized_id]
            )
            session_epoch = self._session_epoch
            lifecycle_revision = self._subscription_lifecycle_revision
            self._state_revision += 1

        removed = False
        try:
            if has_adapter_scopes:
                with self._delivery_lock:
                    self._subscription_coordinator.remove_lease(
                        "mock-feed",
                        normalized_id,
                        dispatch=False,
                    )
                    removed = True
                    with self._lock:
                        dispatch_current = (
                            self._connected
                            and self._session_epoch == session_epoch
                            and self._subscription_lifecycle_revision == lifecycle_revision
                        )
                    if dispatch_current:
                        self._subscription_coordinator.pump()
                snapshot = self._subscription_coordinator.snapshot()
                with self._lock:
                    self._refresh_subscription_receipts_locked(snapshot)
            with self._lock:
                self._purge_uncovered_snapshot_state_locked()
                self._state_revision += 1
                return self._receipts[normalized_id]
        except Exception:
            if has_adapter_scopes and not removed:
                with self._lock:
                    self._active_subscription_ids[normalized_id] = None
                    self._state_revision += 1
            raise
        finally:
            with self._subscription_condition:
                self._subscription_unsubscribe_inflight.discard(normalized_id)
                self._subscription_condition.notify_all()

    def unsubscribe_all(
        self,
        level: Optional[MarketDataLevel] = None,
        event_types: Optional[Sequence[MarketEventType]] = None,
    ) -> Tuple[MarketSubscriptionReceipt, ...]:
        """
        取消当前 session 全部或匹配 level/event 的 active leases。

        Args:
            level: 可选的精确 level 过滤器。
            event_types: 可选实际事件集合；包含 '*' 等价于全部事件。

        Returns:
            Tuple[MarketSubscriptionReceipt, ...]: 按 subscription ID 排序的 canceled 回执。
        """
        normalized_level = MarketDataLevel(level) if level is not None else None
        normalized_events = (
            {MarketEventType(item) for item in event_types} if event_types is not None else None
        )
        if normalized_events and MarketEventType.ALL in normalized_events:
            normalized_events = None
        with self._lock:
            selected = []
            for subscription_id in sorted(self._active_subscription_ids):
                spec = self._specs[subscription_id]
                receipt = self._receipts[subscription_id]
                if normalized_level is not None and spec.level is not normalized_level:
                    continue
                if normalized_events is not None and not normalized_events.intersection(
                    receipt.actual_event_types
                ):
                    continue
                selected.append(subscription_id)
        return tuple(self.unsubscribe(subscription_id) for subscription_id in selected)

    def retry_failed(self, subscription_id: str) -> MarketSubscriptionReceipt:
        """
        显式重试一个订阅回执当前 epoch 内被 adapter 明确拒绝的动作。

        Args:
            subscription_id: 需要解除失败门闩并重试的明确订阅 ID。

        Returns:
            MarketSubscriptionReceipt: 重试调度和同步 fake callback 后的最新回执。

        Raises:
            FeedNotConnectedError: 当前 Feed 已断开或生命周期已改变时抛出。
            SubscriptionNotFoundError: 订阅不存在或当前没有可重试失败时抛出。

        Notes:
            本入口只响应调用方显式动作；普通幂等 ``subscribe`` 不会清除失败门闩。
            coordinator 与 adapter 调用均发生在 Feed ``_lock`` 外。
        """
        normalized_id = str(subscription_id).strip()
        if not normalized_id:
            raise ValueError("subscription_id 不能为空")
        waited_for_retry = False
        with self._subscription_condition:
            while self._subscription_session_closing:
                self._subscription_condition.wait()
            while normalized_id in self._subscription_retry_inflight:
                waited_for_retry = True
                self._subscription_condition.wait()
            receipt = self._receipts.get(normalized_id)
            if receipt is None:
                raise SubscriptionNotFoundError(
                    f"SUBSCRIPTION_NOT_FOUND: subscription_id={normalized_id}"
                )
            if waited_for_retry and not receipt.rejected:
                return receipt
            self._require_connected()
            plan_scopes = frozenset(
                scope
                for plan in self._subscription_item_plans[normalized_id]
                for scope in plan.adapter_scopes
            )
            if not plan_scopes:
                raise SubscriptionNotFoundError(
                    f"SUBSCRIPTION_FAILURE_NOT_FOUND: subscription_id={normalized_id}"
                )
            session_epoch = self._require_session_epoch()
            lifecycle_revision = self._subscription_lifecycle_revision
            self._subscription_retry_inflight.add(normalized_id)

        try:
            with self._delivery_lock:
                with self._lock:
                    retry_current = (
                        self._connected
                        and self._session_epoch == session_epoch
                        and self._subscription_lifecycle_revision == lifecycle_revision
                    )
                if not retry_current:
                    raise FeedNotConnectedError("SUBSCRIPTION_RETRY_EPOCH_CHANGED")
                snapshot = self._subscription_coordinator.snapshot()
                failures = tuple(
                    failure for failure in snapshot.failures if failure.action.scope in plan_scopes
                )
                if not failures:
                    raise SubscriptionNotFoundError(
                        f"SUBSCRIPTION_FAILURE_NOT_FOUND: subscription_id={normalized_id}"
                    )
                for failure in failures:
                    self._subscription_coordinator.retry_failed(
                        failure.action.operation,
                        failure.action.scope,
                        dispatch=False,
                    )
                self._subscription_coordinator.pump()
            snapshot = self._subscription_coordinator.snapshot()
            with self._lock:
                self._refresh_subscription_receipts_locked(snapshot)
                self._state_revision += 1
                return self._receipts[normalized_id]
        finally:
            with self._subscription_condition:
                self._subscription_retry_inflight.discard(normalized_id)
                self._subscription_condition.notify_all()

    def close_subscription_session(self) -> None:
        """
        永久关闭 Mock Feed 的逻辑订阅 session 并释放全部本地墓碑容量。

        Returns:
            None: request/spec/receipt/plan 墓碑和 active 索引均已清理后返回。

        Notes:
            该操作不同于 ``disconnect``：disconnect 保留 desired 供重连恢复；本方法
            删除全部 desired，并在仍连接时通过同一 coordinator 安全退订当前 union。
            已发送但结果未知的动作仍由状态机等待 callback/对账，不会被伪装为成功。
        """
        with self._subscription_condition:
            while self._subscription_session_closing:
                self._subscription_condition.wait()
            self._subscription_session_closing = True
            while (
                self._subscription_request_inflight
                or self._subscription_unsubscribe_inflight
                or self._subscription_retry_inflight
            ):
                self._subscription_condition.wait()
        try:
            with self._delivery_lock:
                with self._lock:
                    session_epoch = self._session_epoch
                    self._subscription_lifecycle_revision += 1
                    lifecycle_revision = self._subscription_lifecycle_revision
                    self._request_index.clear()
                    self._specs.clear()
                    self._receipts.clear()
                    self._subscription_item_plans.clear()
                    self._active_subscription_ids.clear()
                    self._purge_uncovered_snapshot_state_locked()
                    self._state_revision += 1
                self._subscription_coordinator.close_session("mock-feed", dispatch=False)
                with self._lock:
                    dispatch_current = (
                        self._connected
                        and self._session_epoch == session_epoch
                        and self._subscription_lifecycle_revision == lifecycle_revision
                    )
                if dispatch_current:
                    self._subscription_coordinator.pump()
        finally:
            with self._subscription_condition:
                self._subscription_session_closing = False
                self._subscription_condition.notify_all()

    def expire_subscription_ack_timeouts(
        self,
        now: Optional[float] = None,
    ) -> Tuple[str, ...]:
        """
        将已超过 ACK 时限的订阅控制动作转为 uncertain 并刷新公开 receipt。

        Args:
            now: 可选单调时钟当前值；缺省时使用 coordinator 注入时钟。

        Returns:
            Tuple[str, ...]: 本次转为 uncertain 的稳定 action IDs。

        Notes:
            本方法只推进离线控制状态，不联网、不加载 SDK，也不自动猜测应用结果。
        """
        expired = self._subscription_coordinator.expire_ack_timeouts(now=now)
        snapshot = self._subscription_coordinator.snapshot()
        with self._lock:
            self._refresh_subscription_receipts_locked(snapshot)
            if expired:
                self._state_revision += 1
        return tuple(action.action_id for action in expired)

    def reconcile_subscription_action(
        self,
        action_id: str,
        applied: bool,
        reason: Optional[str] = None,
    ) -> None:
        """
        用离线查询证据对账 uncertain 控制动作并刷新全部受影响 lease 回执。

        Args:
            action_id: ACK 超时或 adapter 异常产生的 uncertain action ID。
            applied: True 表示底层动作已生效，False 表示确定未生效。
            reason: 可选脱敏对账证据。

        Returns:
            None: 对账及必要补偿完成后返回；调用方可按原 request 幂等读取回执。

        Notes:
            本方法只用于 fake/离线合同测试；真实 adapter 应由自身查询结果调用
            coordinator，并不得根据超时自动猜测 applied。
        """
        with self._delivery_lock:
            self._subscription_coordinator.reconcile_action(
                action_id,
                applied=applied,
                reason=reason,
            )

    def set_tick_callback(self, callback: Optional[TickCallback]) -> None:
        """
        设置或清除兼容 tick callback。

        Args:
            callback: 接收单个 tick mapping 的可选函数。

        Returns:
            None: callback 引用替换完成后返回。
        """
        with self._lock:
            self._tick_callback = callback

    def set_market_event_callback(self, callback: Optional[MarketEventCallback]) -> None:
        """
        设置或清除 typed MarketEvent callback。

        Args:
            callback: 接收单个 MarketEvent 的可选函数。

        Returns:
            None: callback 引用替换完成后返回。
        """
        with self._lock:
            self._market_event_callback = callback

    def capture_gateway_ingress(self, event: MarketEvent) -> GatewayIngressMark:
        """
        在事件首次进入受控 Feed 边界时绑定当前 epoch 和单调时间。

        Args:
            event: 尚未经过下游队列等待的原始 typed 事件。

        Returns:
            GatewayIngressMark: 可随事件穿过 bridge/队列并在发布时复验的标记。

        Raises:
            FeedNotConnectedError: Feed 未连接时抛出。
            MarketDataFeedError: Provider、epoch 或事件身份不一致时抛出。
        """
        ingress = self._freshness.capture_gateway_ingress(event)
        with self._lock:
            self._require_connected()
            self._validate_event_identity_locked(event)
            return ingress

    def publish_event(
        self,
        event: MarketEvent,
        *,
        gateway_ingress: Optional[GatewayIngressMark] = None,
    ) -> bool:
        """
        向匹配且已确认的 lease 投递一个 typed 事件并更新缓存。

        Args:
            event: Provider、epoch、级别和事件类型均明确的市场事件。
            gateway_ingress: 可选的首次 gateway ingress 标记；未提供时当前发布调用即视为直接 ingress。

        Returns:
            bool: 至少一个确认 lease 匹配并实际投递时为 True。

        Raises:
            FeedNotConnectedError: Feed 未连接时抛出。
            MarketDataFeedError: Provider 或 session epoch 不匹配时抛出。
        """
        ingress = gateway_ingress or self.capture_gateway_ingress(event)
        with self._delivery_lock:
            if self._callback_dispatch_active:
                raise MarketDataFeedError("REENTRANT_EVENT_PUBLISH_DURING_CALLBACK")
            with self._lock:
                self._require_connected()
                self._validate_event_identity_locked(event)
                if not ingress.matches(event):
                    raise MarketDataFeedError("GATEWAY_INGRESS_EVENT_MISMATCH")
                is_control = event.event_type in _CONTROL_EVENT_TYPES
                if not is_control and not self._has_confirmed_match(event):
                    return False
                observed_event = (
                    event
                    if event.client_received_at is not None
                    else replace(event, client_received_at=datetime.now().astimezone())
                )
                if is_control:
                    self._apply_control_event_locked(observed_event)
                else:
                    self._freshness.record(observed_event, ingress)
                    self._event_evidence.add(
                        (
                            observed_event.capability_key,
                            observed_event.level,
                            observed_event.exchange,
                            observed_event.security,
                        )
                    )
                    if (
                        observed_event.event_type is MarketEventType.TICK_COMPAT
                        and observed_event.security
                    ):
                        self._tick_cache[observed_event.security] = MappingProxyType(
                            dict(observed_event.payload)
                        )
                    if (
                        observed_event.event_type
                        in {
                            MarketEventType.SNAPSHOT_L1,
                            MarketEventType.SNAPSHOT_L2,
                        }
                        and observed_event.security
                    ):
                        self._snapshot_cache[
                            (observed_event.security, observed_event.level)
                        ] = observed_event
                tick_callback = (
                    self._tick_callback
                    if observed_event.event_type is MarketEventType.TICK_COMPAT
                    else None
                )
                market_callback = self._market_event_callback
                tick_payload = MappingProxyType(dict(observed_event.payload))
            self._callback_dispatch_active = True
            try:
                if tick_callback is not None:
                    tick_callback(tick_payload)
                if market_callback is not None:
                    market_callback(observed_event)
            finally:
                self._callback_dispatch_active = False
        return True

    def _validate_event_identity_locked(self, event: MarketEvent) -> None:
        """
        在任何缓存、health 或 callback 修改前校验事件完整身份。

        Args:
            event: 待接收的 typed 市场事件。

        Returns:
            None: Provider、epoch、类型、能力和级别完全一致时返回。

        Raises:
            MarketDataFeedError: 任一身份字段不匹配时抛出。
        """
        if event.provider != self._static_manifest.provider:
            raise MarketDataFeedError(
                f"EVENT_PROVIDER_MISMATCH: expected={self._static_manifest.provider}, "
                f"actual={event.provider}"
            )
        if event.gateway_received_at is None:
            raise MarketDataFeedError("EVENT_GATEWAY_RECEIVED_AT_REQUIRED")
        if event.session_epoch != self._session_epoch:
            raise MarketDataFeedError(
                f"EVENT_SESSION_EPOCH_MISMATCH: expected={self._session_epoch}, "
                f"actual={event.session_epoch}"
            )
        if event.event_type in _CONTROL_EVENT_TYPES:
            expected_class = (
                SequenceGapEvent
                if event.event_type is MarketEventType.STREAM_GAP
                else ConnectionStateEvent
            )
            if not isinstance(event, expected_class):
                raise MarketDataFeedError("CONTROL_EVENT_REQUIRES_TYPED_MODEL")
            target_events = tuple(
                event_type
                for event_type, capability_id in _EVENT_CAPABILITY.items()
                if capability_id == event.capability_key
            )
            if not target_events:
                raise MarketDataFeedError(
                    f"CONTROL_CAPABILITY_UNKNOWN: capability={event.capability_key}"
                )
            declaration = self._static_manifest.get(event.capability_key)
            if declaration is None or declaration.support is CapabilitySupport.UNSUPPORTED:
                raise MarketDataFeedError(
                    f"CONTROL_CAPABILITY_NOT_SUPPORTED: capability={event.capability_key}"
                )
            allowed_levels = {
                level for event_type in target_events for level in _EVENT_LEVELS[event_type]
            }
            if event.level not in allowed_levels:
                raise MarketDataFeedError(
                    "CONTROL_EVENT_LEVEL_MISMATCH: "
                    f"capability={event.capability_key}, level={event.level.value}"
                )
            return
        expected_capability = _EVENT_CAPABILITY.get(event.event_type)
        if expected_capability is None:
            raise MarketDataFeedError(
                f"EVENT_TYPE_NOT_SUPPORTED: event_type={event.event_type.value}"
            )
        if event.capability_key != expected_capability:
            raise MarketDataFeedError(
                "EVENT_CAPABILITY_MISMATCH: "
                f"event_type={event.event_type.value}, expected={expected_capability}, "
                f"actual={event.capability_key}"
            )
        if event.level not in _EVENT_LEVELS[event.event_type]:
            raise MarketDataFeedError(
                "EVENT_LEVEL_MISMATCH: "
                f"event_type={event.event_type.value}, level={event.level.value}"
            )
        expected_model = _EVENT_MODELS[event.event_type]
        if not isinstance(event, expected_model):
            raise MarketDataFeedError(
                "DATA_EVENT_REQUIRES_TYPED_MODEL: "
                f"event_type={event.event_type.value}, expected={expected_model.__name__}"
            )

    def _apply_control_event_locked(self, event: MarketEvent) -> None:
        """
        将原生 gap/status 写入独立健康状态，不要求普通数据订阅且不刷新数据 age。

        Args:
            event: 已验证 provider、epoch 与目标 capability 的 typed 控制事件。

        Returns:
            None: gap 计数和对应 continuity scope 状态更新后返回。

        Side Effects:
            同一 loss boundary 幂等计数；恢复必须由显式 recovery_confirmed 状态事件证明。
        """
        scope = self._control_scope(event)
        active = {
            subscription_id: self._receipts[subscription_id]
            for subscription_id in self._active_subscription_ids
        }
        covered = self._control_scope_has_confirmed_coverage(scope, active)
        if event.event_type is MarketEventType.STREAM_GAP:
            boundary = self._gap_boundary_key(event, scope)
            if boundary not in self._seen_gap_boundaries:
                self._seen_gap_boundaries[boundary] = None
                self._source_gap_count += 1
                while len(self._seen_gap_boundaries) > _MAX_SEEN_GAP_BOUNDARIES:
                    self._seen_gap_boundaries.popitem(last=False)
            if covered:
                self._source_degraded_scopes[scope] = str(
                    event.payload.get("reason") or "source_sequence_gap"
                )
            return
        continuous = event.payload.get("continuous")
        state = str(event.payload.get("state") or "").strip().lower()
        if continuous is False or state in {"degraded", "disconnected", "error"}:
            if covered:
                self._source_degraded_scopes[scope] = str(
                    event.payload.get("reason") or "source_stream_degraded"
                )
            return
        if (
            continuous is True
            and event.payload.get("recovery_confirmed") is True
            and state in {"ready", "connected", "continuous"}
        ):
            for degraded_scope in tuple(self._source_degraded_scopes):
                if self._same_control_lineage(degraded_scope, scope):
                    self._source_degraded_scopes.pop(degraded_scope, None)

    @staticmethod
    def _control_scope(event: MarketEvent) -> Tuple[str, ...]:
        """
        构造不伪造跨 provider/epoch/stream/channel 顺序的 continuity scope。

        Args:
            event: 已验证的 gap 或连接状态事件。

        Returns:
            Tuple[str, ...]: Provider、能力、级别、epoch、通道、市场与证券的稳定 scope。
        """
        return (
            event.provider,
            event.capability_key,
            event.level.value,
            event.session_epoch,
            event.stream_id or "",
            event.channel_id or "",
            event.exchange,
            event.security or "",
        )

    @staticmethod
    def _same_control_lineage(left: Tuple[str, ...], right: Tuple[str, ...]) -> bool:
        """
        判断两个 continuity scope 是否仅 session epoch 不同但属于同一通道谱系。

        Args:
            left: 已降级的历史或当前 scope。
            right: 已证明恢复的当前 scope。

        Returns:
            bool: Provider、能力、stream、channel、市场和证券均一致时为 True。
        """
        return left[:3] == right[:3] and left[4:] == right[4:]

    @staticmethod
    def _gap_boundary_key(event: MarketEvent, scope: Tuple[str, ...]) -> Tuple[Any, ...]:
        """
        为重复送达的同一 gap boundary 生成进程内稳定幂等键。

        Args:
            event: 当前 SequenceGapEvent。
            scope: 已规范化 continuity scope。

        Returns:
            Tuple[Any, ...]: 含显式 ID 或首末边界与原始序列的不可变键。
        """
        explicit_id = event.payload.get("loss_boundary_id")
        if explicit_id is not None:
            return (scope, str(explicit_id))
        sequence_items = repr(tuple(event.source_sequence.items()))
        return (
            scope,
            repr(event.payload.get("first_lost")),
            repr(event.payload.get("last_lost")),
            sequence_items,
        )

    def publish_tick(
        self,
        security: str,
        exchange: str,
        payload: Mapping[str, Any],
        received_at: Optional[datetime] = None,
        exchange_time: Optional[datetime] = None,
    ) -> bool:
        """
        构造并投递一个兼容 tick，便于无 SDK 测试旧 callback 路径。

        Args:
            security: 标准证券代码。
            exchange: 标准交易所代码。
            payload: 兼容 tick 字段。
            received_at: 可选的网关接收时间；默认使用当前时间。
            exchange_time: 可选且不可伪造的交易所时间；缺失时策略读取保持 unavailable。

        Returns:
            bool: 是否存在匹配且确认的 tick lease。
        """
        now = received_at or datetime.now()
        event = CompatibilityTickEvent(
            provider=self._static_manifest.provider,
            capability_key=_EVENT_CAPABILITY[MarketEventType.TICK_COMPAT],
            event_type=MarketEventType.TICK_COMPAT,
            level=MarketDataLevel.TICK_COMPAT,
            exchange=exchange,
            session_epoch=self._require_session_epoch(),
            payload=payload,
            security=security,
            raw_security_code=security.split(".", 1)[0],
            gateway_received_at=now,
            client_received_at=now,
            exchange_time=exchange_time,
        )
        return self.publish_event(event)

    def _require_connected(self) -> None:
        """
        校验 Mock Feed 当前已连接。

        Returns:
            None: 已连接时返回。

        Raises:
            FeedNotConnectedError: 未连接时抛出。
        """
        if not self._connected:
            raise FeedNotConnectedError("MARKET_DATA_FEED_NOT_CONNECTED")

    def _require_session_epoch(self) -> str:
        """
        返回当前 session epoch。

        Returns:
            str: 已连接会话的稳定 epoch。

        Raises:
            FeedNotConnectedError: 未连接或 epoch 尚未创建时抛出。
        """
        with self._lock:
            self._require_connected()
            if self._session_epoch is None:
                raise FeedNotConnectedError("MARKET_DATA_SESSION_EPOCH_MISSING")
            return self._session_epoch

    def _on_subscription_state_change(
        self,
        snapshot: SubscriptionLeaseSnapshot,
    ) -> None:
        """
        使用 coordinator 提供的同一个 manager 快照原子刷新 receipt 与 health 来源。

        Args:
            snapshot: 一次状态转换完成后的不可变租约快照。

        Returns:
            None: 全部非终态 receipt 与 Feed revision 更新后返回。
        """
        with self._lock:
            self._refresh_subscription_receipts_locked(snapshot)
            self._state_revision += 1

    def _refresh_subscription_receipts_locked(
        self,
        snapshot: SubscriptionLeaseSnapshot,
    ) -> None:
        """
        从单一 manager 快照重建全部 active 或退订过渡中的公开回执。

        Args:
            snapshot: receipt 与 health 必须共同消费的线性化状态快照。

        Returns:
            None: self._receipts 已与 snapshot 对齐后返回。

        Notes:
            调用方必须持有 Feed 锁。终态 canceled 和纯本地 rejected 墓碑保留原 epoch。
        """
        if self._session_epoch is not None and snapshot.session_epoch != self._session_epoch:
            return
        for subscription_id, previous in tuple(self._receipts.items()):
            plans = self._subscription_item_plans[subscription_id]
            active = subscription_id in self._active_subscription_ids
            if not active and previous.state is SubscriptionState.CANCELED:
                continue
            if not active and all(not plan.adapter_scopes for plan in plans):
                continue
            spec = self._specs[subscription_id]
            self._receipts[subscription_id] = self._subscription_receipts.project_receipt(
                previous,
                spec,
                plans,
                snapshot,
                active=active,
                limits=self._limits,
            )

    def _subscription_event_markets(
        self,
        event_type: MarketEventType,
    ) -> Tuple[str, ...]:
        """
        从当前准入 manifest 返回一个实际事件的标准市场。

        Args:
            event_type: 需要展开 all selector 的实际事件类型。

        Returns:
            Tuple[str, ...]: 能力未声明时为空，否则为稳定市场元组。
        """
        declaration = self._event_declaration(event_type)
        return tuple(declaration.markets) if declaration is not None else ()

    def _expand_event_types(self, spec: MarketSubscriptionSpec) -> Tuple[MarketEventType, ...]:
        """
        将 '*' 只展开为协商、授权且满足精确 scope/level 门禁的实际事件。

        Args:
            spec: 当前订阅请求。

        Returns:
            Tuple[MarketEventType, ...]: 不包含通配符的实际事件集合。

        Raises:
            RealtimeDataUnavailableError: 通配符没有任何获准事件时抛出。
        """
        if spec.event_types != (MarketEventType.ALL,):
            return spec.event_types
        expanded = []
        for event_type in self._negotiated_event_types:
            if all(
                self._subscription_failure(spec, scope, event_type) is None
                for scope in spec.scope_items()
            ):
                expanded.append(event_type)
        if not expanded:
            raise RealtimeDataUnavailableError("NO_NEGOTIATED_EVENT_TYPES_FOR_SUBSCRIPTION")
        return tuple(sorted(expanded, key=lambda item: item.value))

    def _effective_scope(
        self,
        spec: MarketSubscriptionSpec,
        event_types: Sequence[MarketEventType],
    ) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
        """
        计算回执需要公开的实际证券和市场范围。

        Args:
            spec: 当前订阅请求。
            event_types: 通配展开后的实际事件类型。

        Returns:
            Tuple[Tuple[str, ...], Tuple[str, ...]]: 实际证券与市场元组。
        """
        if spec.selector is SubscriptionSelector.SYMBOLS:
            return spec.symbols, ()
        if spec.selector is SubscriptionSelector.MARKETS:
            return (), spec.markets
        markets: Set[str] = set()
        for event_type in event_types:
            declaration = self._event_declaration(event_type)
            if declaration is not None:
                markets.update(declaration.markets)
        return (), tuple(sorted(markets))

    def _event_declaration(self, event_type: MarketEventType) -> Optional[CapabilityDeclaration]:
        """
        读取一个实际事件对应的 capability 声明。

        Args:
            event_type: 非通配 MarketEventType。

        Returns:
            Optional[CapabilityDeclaration]: manifest 已声明时返回，否则为 None。
        """
        capability_id = _EVENT_CAPABILITY.get(event_type)
        if capability_id is None:
            return None
        return self._admission_manifest.get(capability_id)

    def _subscription_failure(
        self,
        spec: MarketSubscriptionSpec,
        scope: str,
        event_type: MarketEventType,
    ) -> Optional[Tuple[str, str]]:
        """
        返回精确订阅项的静态、协商、作用域或 readiness 失败。

        Args:
            spec: 当前订阅请求。
            scope: 单个证券、市场或 '*' 作用域。
            event_type: 实际事件类型。

        Returns:
            Optional[Tuple[str, str]]: 失败 code/reason；可确认时为 None。
        """
        declaration = self._event_declaration(event_type)
        if declaration is None or declaration.support is CapabilitySupport.UNSUPPORTED:
            return "UNSUPPORTED_EVENT_TYPE", event_type.value
        if event_type not in self._negotiated_event_types:
            return "EVENT_TYPE_NOT_NEGOTIATED", event_type.value
        allowed_levels = _EVENT_LEVELS[event_type]
        if spec.level not in allowed_levels:
            return "EVENT_LEVEL_MISMATCH", f"event={event_type.value}, level={spec.level.value}"
        if declaration.readiness is not CapabilityReadiness.READY:
            return (
                "CAPABILITY_NOT_READY",
                f"capability={declaration.capability_id}, readiness={declaration.readiness.value}",
            )
        if spec.require_continuity and not declaration.continuous:
            return "CONTINUITY_UNAVAILABLE", declaration.capability_id
        if spec.asset_types and declaration.asset_types:
            unsupported_assets = set(spec.asset_types).difference(declaration.asset_types)
            if unsupported_assets:
                return "ASSET_TYPE_UNSUPPORTED", ",".join(sorted(unsupported_assets))
        if spec.selector is SubscriptionSelector.MARKETS:
            if declaration.markets and scope not in declaration.markets:
                return "MARKET_UNSUPPORTED", scope
        if spec.selector is SubscriptionSelector.SYMBOLS:
            symbol_market = scope.rsplit(".", 1)[-1] if "." in scope else ""
            if not symbol_market:
                return "SYMBOL_MARKET_UNRESOLVED", scope
            if declaration.markets and symbol_market not in declaration.markets:
                return "MARKET_UNSUPPORTED", symbol_market
        if spec.selector in {SubscriptionSelector.MARKETS, SubscriptionSelector.ALL}:
            if not bool(declaration.metadata.get("full_market", False)):
                return "FULL_MARKET_CAPABILITY_UNAVAILABLE", declaration.capability_id
        return None

    def _has_confirmed_match(self, event: MarketEvent) -> bool:
        """
        判断事件是否至少匹配一个 active 且 confirmed 的 lease。

        Args:
            event: 待投递的 typed 市场事件。

        Returns:
            bool: 存在精确 selector/level/event 匹配时为 True。
        """
        for subscription_id in self._active_subscription_ids:
            spec = self._specs[subscription_id]
            receipt = self._receipts[subscription_id]
            if spec.level is not event.level:
                continue
            if spec.asset_types and (
                event.asset_type is None or event.asset_type not in spec.asset_types
            ):
                continue
            for item in receipt.confirmed:
                if item.event_type is not event.event_type:
                    continue
                if item.selector is SubscriptionSelector.ALL and item.scope in {
                    "*",
                    event.exchange,
                }:
                    return True
                if item.selector is SubscriptionSelector.MARKETS and item.scope == event.exchange:
                    return True
                if item.selector is SubscriptionSelector.SYMBOLS and item.scope == event.security:
                    return True
        return False


__all__ = [
    "CurrentTickDiagnostic",
    "FeedNotConnectedError",
    "MarketDataFeedError",
    "MarketEventCallback",
    "MarketSnapshotDiagnostic",
    "MockRealtimeMarketDataFeed",
    "RealtimeDataUnavailableError",
    "RealtimeMarketDataFeed",
    "StaleMarketDataError",
    "SubscriptionCapacityError",
    "SubscriptionConflictError",
    "SubscriptionNotFoundError",
    "TickCallback",
]
