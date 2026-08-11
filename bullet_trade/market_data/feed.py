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
from dataclasses import replace
from datetime import datetime
from threading import RLock
from types import MappingProxyType
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Set, Tuple

from .capability import (
    CapabilityDeclaration,
    CapabilityManifest,
    CapabilityReadiness,
    CapabilitySupport,
)
from .models import (
    FeedHealth,
    MarketDataLevel,
    MarketEvent,
    MarketEventType,
    MarketSubscriptionReceipt,
    MarketSubscriptionSpec,
    SubscriptionItemResult,
    SubscriptionItemState,
    SubscriptionSelector,
)

TickCallback = Callable[[Mapping[str, Any]], None]
MarketEventCallback = Callable[[MarketEvent], None]


class MarketDataFeedError(RuntimeError):
    """实时行情 Feed 生命周期、订阅或数据读取失败的公共基类。"""


class FeedNotConnectedError(MarketDataFeedError):
    """表示调用要求已连接 Feed，但当前连接尚未 ready。"""


class RealtimeDataUnavailableError(MarketDataFeedError):
    """表示当前没有已确认且可返回的实时 tick 或快照。"""


class SubscriptionConflictError(MarketDataFeedError):
    """表示相同 request_id 被重用于不同语义指纹。"""


class SubscriptionNotFoundError(MarketDataFeedError):
    """表示退订目标不是当前或已取消的明确 subscription ID。"""


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
    """提供无网络、无 SDK 且立即确认订阅的线程安全合同测试 Feed。"""

    def __init__(
        self,
        manifest: CapabilityManifest,
        negotiated_event_types: Optional[Sequence[MarketEventType]] = None,
        limits: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        初始化断开状态的 Mock Feed。

        Args:
            manifest: Mock Provider 的静态能力和初始元数据。
            negotiated_event_types: 模拟 handshake 与账户共同获准的事件；默认由 manifest 推导。
            limits: 回执和 health 可公开的脱敏订阅限制。
        """
        self._lock = RLock()
        self._manifest = manifest.with_supported_readiness(
            CapabilityReadiness.UNAVAILABLE, "mock_disconnected"
        )
        self._connected = False
        self._ever_connected = False
        self._epoch_counter = 0
        self._session_epoch: Optional[str] = None
        self._reconnect_count = 0
        self._limits = MappingProxyType(dict(limits or {}))
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
        self._active_subscription_ids: Dict[str, None] = {}
        self._tick_callback: Optional[TickCallback] = None
        self._market_event_callback: Optional[MarketEventCallback] = None
        self._tick_cache: Dict[str, Mapping[str, Any]] = {}
        self._snapshot_cache: Dict[Tuple[str, MarketDataLevel], MarketEvent] = {}
        self._last_gateway_received_at: Optional[datetime] = None
        self._last_client_received_at: Optional[datetime] = None
        self._last_exchange_time: Optional[datetime] = None

    @property
    def manifest(self) -> CapabilityManifest:
        """
        返回当前 Mock 能力清单。

        Returns:
            CapabilityManifest: 受锁保护的不可变 manifest 快照。
        """
        with self._lock:
            return self._manifest

    def connect(self) -> None:
        """
        幂等连接 Mock，创建新 session epoch 并将静态支持能力标为 ready。

        Returns:
            None: 生命周期更新完成后返回。
        """
        with self._lock:
            if self._connected:
                return
            if self._ever_connected:
                self._reconnect_count += 1
            self._ever_connected = True
            self._connected = True
            self._epoch_counter += 1
            self._session_epoch = f"mock-session-{self._epoch_counter}"
            self._manifest = self._manifest.with_supported_readiness(
                CapabilityReadiness.READY, None
            )
            for subscription_id in tuple(self._active_subscription_ids):
                receipt = self._receipts[subscription_id]
                self._receipts[subscription_id] = replace(
                    receipt, session_epoch=self._session_epoch
                )

    def disconnect(self) -> None:
        """
        幂等断开 Mock，保留 desired lease 但将动态能力标为 unavailable。

        Returns:
            None: 生命周期更新完成后返回。
        """
        with self._lock:
            if not self._connected:
                return
            self._connected = False
            self._manifest = self._manifest.with_supported_readiness(
                CapabilityReadiness.UNAVAILABLE, "mock_disconnected"
            )

    def health(self) -> FeedHealth:
        """
        获取 Mock Feed 当前健康快照。

        Returns:
            FeedHealth: 包含动态能力和当前 active receipts 的不可变状态。
        """
        with self._lock:
            readiness = {
                capability_id: declaration.readiness
                for capability_id, declaration in self._manifest.capabilities.items()
            }
            active = {
                subscription_id: self._receipts[subscription_id]
                for subscription_id in self._active_subscription_ids
            }
            reasons = () if self._connected else ("mock_disconnected",)
            return FeedHealth(
                provider=self._manifest.provider,
                connected=self._connected,
                manifest_version=self._manifest.manifest_version,
                session_epoch=self._session_epoch,
                active_subscriptions=active,
                capability_readiness=readiness,
                reconnect_count=self._reconnect_count,
                last_gateway_received_at=self._last_gateway_received_at,
                last_client_received_at=self._last_client_received_at,
                last_exchange_time=self._last_exchange_time,
                reasons=reasons,
            )

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
        normalized_security = security.strip()
        if not normalized_security:
            raise ValueError("security 不能为空")
        with self._lock:
            self._require_connected()
            tick = self._tick_cache.get(normalized_security)
            if tick is None:
                raise RealtimeDataUnavailableError(
                    f"REALTIME_TICK_UNAVAILABLE: security={normalized_security}"
                )
            return MappingProxyType(dict(tick))

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
        normalized_security = security.strip()
        normalized_level = MarketDataLevel(level)
        if not normalized_security:
            raise ValueError("security 不能为空")
        if normalized_level is MarketDataLevel.TICK_COMPAT:
            raise ValueError("typed snapshot level 必须是 l1 或 l2")
        with self._lock:
            self._require_connected()
            event = self._snapshot_cache.get((normalized_security, normalized_level))
            if event is None:
                raise RealtimeDataUnavailableError(
                    "REALTIME_SNAPSHOT_UNAVAILABLE: "
                    f"security={normalized_security}, level={normalized_level.value}"
                )
            return event

    def subscribe(self, spec: MarketSubscriptionSpec) -> MarketSubscriptionReceipt:
        """
        立即评估并确认或拒绝 Mock 订阅，保持 request_id + fingerprint 幂等。

        Args:
            spec: 已规范化的部分、市场或全部范围订阅。

        Returns:
            MarketSubscriptionReceipt: 通配展开和逐项结果。

        Raises:
            FeedNotConnectedError: Feed 未连接时抛出。
            SubscriptionConflictError: request_id 复用于不同 fingerprint 时抛出。
            RealtimeDataUnavailableError: 通配符没有任何获准实际事件时抛出。
        """
        with self._lock:
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
            actual_event_types = self._expand_event_types(spec)
            effective_symbols, effective_markets = self._effective_scope(spec, actual_event_types)
            items = []
            for scope in spec.scope_items():
                for event_type in actual_event_types:
                    failure = self._subscription_failure(spec, scope, event_type)
                    if failure is None:
                        item = SubscriptionItemResult(
                            selector=spec.selector,
                            scope=scope,
                            level=spec.level,
                            event_type=event_type,
                            state=SubscriptionItemState.CONFIRMED,
                        )
                    else:
                        code, reason = failure
                        item = SubscriptionItemResult(
                            selector=spec.selector,
                            scope=scope,
                            level=spec.level,
                            event_type=event_type,
                            state=SubscriptionItemState.REJECTED,
                            code=code,
                            reason=reason,
                        )
                    items.append(item)
            token = hashlib.sha256(
                f"{spec.request_id}:{spec.fingerprint}".encode("utf-8")
            ).hexdigest()[:20]
            subscription_id = f"mock-{token}"
            receipt = MarketSubscriptionReceipt.from_items(
                subscription_id=subscription_id,
                spec=spec,
                session_epoch=self._require_session_epoch(),
                items=items,
                actual_event_types=actual_event_types,
                effective_symbols=effective_symbols,
                effective_markets=effective_markets,
                limits=self._limits,
            )
            self._request_index[spec.request_id] = (spec.fingerprint, subscription_id)
            self._specs[subscription_id] = spec
            self._receipts[subscription_id] = receipt
            if receipt.confirmed or receipt.pending:
                self._active_subscription_ids[subscription_id] = None
            return receipt

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
        with self._lock:
            receipt = self._receipts.get(normalized_id)
            if receipt is None:
                raise SubscriptionNotFoundError(
                    f"SUBSCRIPTION_NOT_FOUND: subscription_id={normalized_id}"
                )
            if normalized_id not in self._active_subscription_ids:
                return receipt
            canceled = receipt.as_canceled()
            self._receipts[normalized_id] = canceled
            self._active_subscription_ids.pop(normalized_id, None)
            return canceled

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

    def publish_event(self, event: MarketEvent) -> bool:
        """
        向匹配且已确认的 lease 投递一个 typed 事件并更新缓存。

        Args:
            event: Provider、epoch、级别和事件类型均明确的市场事件。

        Returns:
            bool: 至少一个确认 lease 匹配并实际投递时为 True。

        Raises:
            FeedNotConnectedError: Feed 未连接时抛出。
            MarketDataFeedError: Provider 或 session epoch 不匹配时抛出。
        """
        with self._lock:
            self._require_connected()
            if event.provider != self._manifest.provider:
                raise MarketDataFeedError(
                    f"EVENT_PROVIDER_MISMATCH: expected={self._manifest.provider}, "
                    f"actual={event.provider}"
                )
            if event.session_epoch != self._require_session_epoch():
                raise MarketDataFeedError(
                    f"EVENT_SESSION_EPOCH_MISMATCH: expected={self._session_epoch}, "
                    f"actual={event.session_epoch}"
                )
            if not self._has_confirmed_match(event):
                return False
            if event.event_type is MarketEventType.TICK_COMPAT and event.security:
                self._tick_cache[event.security] = MappingProxyType(dict(event.payload))
            if (
                event.event_type
                in {
                    MarketEventType.SNAPSHOT_L1,
                    MarketEventType.SNAPSHOT_L2,
                }
                and event.security
            ):
                self._snapshot_cache[(event.security, event.level)] = event
            self._last_gateway_received_at = event.gateway_received_at
            self._last_client_received_at = event.client_received_at or datetime.now()
            self._last_exchange_time = event.exchange_time
            tick_callback = (
                self._tick_callback if event.event_type is MarketEventType.TICK_COMPAT else None
            )
            market_callback = self._market_event_callback
            tick_payload = MappingProxyType(dict(event.payload))
        if tick_callback is not None:
            tick_callback(tick_payload)
        if market_callback is not None:
            market_callback(event)
        return True

    def publish_tick(
        self,
        security: str,
        exchange: str,
        payload: Mapping[str, Any],
        received_at: Optional[datetime] = None,
    ) -> bool:
        """
        构造并投递一个兼容 tick，便于无 SDK 测试旧 callback 路径。

        Args:
            security: 标准证券代码。
            exchange: 标准交易所代码。
            payload: 兼容 tick 字段。
            received_at: 可选的网关接收时间；默认使用当前时间。

        Returns:
            bool: 是否存在匹配且确认的 tick lease。
        """
        now = received_at or datetime.now()
        event = MarketEvent(
            provider=self.manifest.provider,
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
        return self._manifest.get(capability_id)

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
                if item.selector is SubscriptionSelector.ALL:
                    return True
                if item.selector is SubscriptionSelector.MARKETS and item.scope == event.exchange:
                    return True
                if item.selector is SubscriptionSelector.SYMBOLS and item.scope == event.security:
                    return True
        return False


__all__ = [
    "FeedNotConnectedError",
    "MarketDataFeedError",
    "MarketEventCallback",
    "MockRealtimeMarketDataFeed",
    "RealtimeDataUnavailableError",
    "RealtimeMarketDataFeed",
    "SubscriptionConflictError",
    "SubscriptionNotFoundError",
    "TickCallback",
]
