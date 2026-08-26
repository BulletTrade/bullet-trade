"""
作者: BruceLee

文件职责: 使用显式市场更新 policy 和 gateway 到达时的单调时钟判定行情时效。
主要输入: 已验证 MarketEvent、分级 stale 阈值、单调时钟和日历/停牌 policy 回调。
主要输出: 每证券/级别的 FreshnessDecision，以及模块/原子能力最近事件时间。
上游关系: 由 realtime feed 在 callback 首次进入受控路径时记录事件。
下游关系: 供实时读取门禁、FeedHealth、远程 health 和离线合同测试消费。
关键配置约定: policy 必须显式说明是否预期更新；午休/闭市/停牌的累计
暂停时长由 calendar/status owner 提供；本模块不从本机日期或交易所时间猜测时段。
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, replace
from datetime import datetime
from threading import RLock
from types import MappingProxyType
from typing import Callable, Dict, FrozenSet, Mapping, Optional, Tuple

from .capability import CapabilityReadiness
from .models import FeedEventTimes, MarketDataLevel, MarketEvent, MarketEventType


class MarketFreshnessError(RuntimeError):
    """表示行情时效记录、policy 或单调时钟合同失败。"""


class FreshnessRecordNotFoundError(MarketFreshnessError):
    """表示某证券和行情级别尚无 gateway 到达记录。"""


class MarketUpdatePolicyError(MarketFreshnessError):
    """表示显式市场更新 policy 返回了无法安全解释的结果。"""


@dataclass(frozen=True)
class MarketUpdateExpectation:
    """表示 calendar/status owner 给出的更新窗口和扣除暂停后的 source age。"""

    expected: bool
    market_state: str
    paused_seconds: float = 0.0
    source_stale: bool = False
    effective_source_age_seconds: Optional[float] = None
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        """
        校验市场状态、累计暂停时长与交易所源时效证据。

        Returns:
            None: policy 结果规范化完成后返回。

        Raises:
            ValueError: bool、状态、暂停秒数或交易所源 age 非法时抛出。
        """
        if not isinstance(self.expected, bool):
            raise ValueError("expected 必须是 bool")
        if not isinstance(self.source_stale, bool):
            raise ValueError("source_stale 必须是 bool")
        market_state = str(self.market_state).strip().lower()
        if not market_state:
            raise ValueError("market_state 不能为空")
        if (
            isinstance(self.paused_seconds, bool)
            or self.paused_seconds < 0
            or not math.isfinite(self.paused_seconds)
        ):
            raise ValueError("paused_seconds 必须是有限非负数")
        object.__setattr__(self, "market_state", market_state)
        object.__setattr__(self, "paused_seconds", float(self.paused_seconds))
        if self.effective_source_age_seconds is not None:
            if (
                isinstance(self.effective_source_age_seconds, bool)
                or self.effective_source_age_seconds < 0
                or not math.isfinite(self.effective_source_age_seconds)
            ):
                raise ValueError("effective_source_age_seconds 必须是有限非负数")
            object.__setattr__(
                self,
                "effective_source_age_seconds",
                float(self.effective_source_age_seconds),
            )
        if self.source_stale and self.effective_source_age_seconds is None:
            raise ValueError("source_stale=true 时必须提供 effective_source_age_seconds")
        if self.reason is not None:
            object.__setattr__(self, "reason", str(self.reason).strip() or None)


@dataclass(frozen=True)
class FreshnessDecision:
    """保存某个证券/级别一次时效判定的完整单调时钟证据。"""

    security: str
    level: MarketDataLevel
    capability_id: str
    expected_update: bool
    market_state: str
    raw_age_seconds: float
    paused_seconds: float
    effective_age_seconds: float
    stale_after_seconds: float
    gateway_stale: bool
    source_time_verified: bool
    source_stale: bool
    stale: bool
    effective_source_age_seconds: Optional[float] = None
    last_gateway_received_at: Optional[datetime] = None
    last_client_received_at: Optional[datetime] = None
    last_exchange_time: Optional[datetime] = None
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        """
        校验时效决策的标识、枚举与数值边界。

        Returns:
            None: 决策字段规范化完成后返回。

        Raises:
            ValueError: 标识为空或 age/阈值非法时抛出。
        """
        security = str(self.security).strip().upper()
        capability_id = str(self.capability_id).strip()
        market_state = str(self.market_state).strip().lower()
        if not security or not capability_id or not market_state:
            raise ValueError("freshness security、capability_id 和 market_state 不能为空")
        try:
            level = MarketDataLevel(self.level)
        except ValueError as exc:
            raise ValueError("freshness level 包含未知枚举") from exc
        values = (
            self.raw_age_seconds,
            self.paused_seconds,
            self.effective_age_seconds,
            self.stale_after_seconds,
        )
        if any(value < 0 or not math.isfinite(value) for value in values):
            raise ValueError("freshness age 和阈值必须是有限非负数")
        if not all(
            isinstance(value, bool)
            for value in (
                self.expected_update,
                self.gateway_stale,
                self.source_time_verified,
                self.source_stale,
                self.stale,
            )
        ):
            raise ValueError("freshness bool 标志必须是 bool")
        if self.effective_source_age_seconds is not None and (
            isinstance(self.effective_source_age_seconds, bool)
            or self.effective_source_age_seconds < 0
            or not math.isfinite(self.effective_source_age_seconds)
        ):
            raise ValueError("effective_source_age_seconds 必须是有限非负数")
        if self.source_stale and self.effective_source_age_seconds is None:
            raise ValueError("source_stale=true 时必须提供 effective_source_age_seconds")
        if self.stale != (self.gateway_stale or self.source_stale):
            raise ValueError("stale 必须等于 gateway_stale 或 source_stale")
        object.__setattr__(self, "security", security)
        object.__setattr__(self, "level", level)
        object.__setattr__(self, "capability_id", capability_id)
        object.__setattr__(self, "market_state", market_state)


@dataclass(frozen=True)
class MarketFreshnessSnapshot:
    """保存同一代 ingress marks 派生的 readiness、时间和完整决策。"""

    capability_readiness: Mapping[str, CapabilityReadiness]
    decisions: Tuple[FreshnessDecision, ...]
    latest_times: Optional[FeedEventTimes]
    capability_times: Mapping[str, FeedEventTimes]
    module_times: Mapping[str, FeedEventTimes]
    failure_reason: Optional[str] = None

    def __post_init__(self) -> None:
        """
        冻结 snapshot 映射并规范化可选失败原因。

        Returns:
            None: 所有映射复制为只读快照后返回。

        Raises:
            ValueError: readiness、decision 或 event-times 类型非法时抛出。
        """
        readiness = {
            str(capability_id).strip(): CapabilityReadiness(state)
            for capability_id, state in self.capability_readiness.items()
        }
        if any(not capability_id for capability_id in readiness):
            raise ValueError("capability_readiness 不能包含空 capability")
        decisions = tuple(self.decisions)
        if any(not isinstance(decision, FreshnessDecision) for decision in decisions):
            raise ValueError("decisions 必须包含 FreshnessDecision")
        capability_times = dict(self.capability_times)
        module_times = dict(self.module_times)
        if any(
            not isinstance(times, FeedEventTimes)
            for times in tuple(capability_times.values()) + tuple(module_times.values())
        ):
            raise ValueError("snapshot event-times 必须包含 FeedEventTimes")
        object.__setattr__(self, "capability_readiness", MappingProxyType(readiness))
        object.__setattr__(self, "decisions", decisions)
        object.__setattr__(
            self,
            "capability_times",
            MappingProxyType(capability_times),
        )
        object.__setattr__(self, "module_times", MappingProxyType(module_times))
        if self.failure_reason is not None:
            object.__setattr__(
                self,
                "failure_reason",
                str(self.failure_reason).strip() or None,
            )


MarketUpdateExpectationPolicy = Callable[
    [str, MarketDataLevel, MarketEvent, float], MarketUpdateExpectation
]
MonotonicClock = Callable[[], float]


@dataclass(frozen=True)
class GatewayIngressMark:
    """绑定一个事件身份与其首次进入受控 gateway 路径的单调时间。"""

    provider: str
    capability_id: str
    event_type: MarketEventType
    level: MarketDataLevel
    session_epoch: str
    security: Optional[str]
    stream_id: Optional[str]
    channel_id: Optional[str]
    gateway_monotonic: float
    event: MarketEvent

    def __post_init__(self) -> None:
        """
        校验 ingress 身份字段和单调时间。

        Returns:
            None: 字段规范化完成后返回。

        Raises:
            ValueError: 身份字段为空、枚举未知或单调时间非法时抛出。
        """
        provider = str(self.provider).strip()
        capability_id = str(self.capability_id).strip()
        session_epoch = str(self.session_epoch).strip()
        if not provider or not capability_id or not session_epoch:
            raise ValueError("ingress provider、capability_id 和 session_epoch 不能为空")
        if not math.isfinite(self.gateway_monotonic):
            raise ValueError("gateway_monotonic 必须是有限数")
        if not isinstance(self.event, MarketEvent):
            raise ValueError("ingress event 必须是 MarketEvent")
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "capability_id", capability_id)
        object.__setattr__(self, "event_type", MarketEventType(self.event_type))
        object.__setattr__(self, "level", MarketDataLevel(self.level))
        object.__setattr__(self, "session_epoch", session_epoch)
        object.__setattr__(self, "gateway_monotonic", float(self.gateway_monotonic))

    def matches(self, event: MarketEvent) -> bool:
        """
        判断该 mark 是否属于同一个不可变事件身份。

        Args:
            event: 可能补充 client 时间但来源身份不得变化的事件。

        Returns:
            bool: Provider、能力、类型、级别、epoch 与通道身份全部相同时为 True。
        """
        comparable_event = (
            replace(event, client_received_at=None)
            if self.event.client_received_at is None
            else event
        )
        return self.event == comparable_event and (
            self.provider == event.provider
            and self.capability_id == event.capability_key
            and self.event_type is event.event_type
            and self.level is event.level
            and self.session_epoch == event.session_epoch
            and self.security == event.security
            and self.stream_id == event.stream_id
            and self.channel_id == event.channel_id
        )


@dataclass(frozen=True)
class _FreshnessMark:
    """保存事件首次进入受控 gateway 路径时的单调时钟标记。"""

    event: MarketEvent
    module: str
    gateway_monotonic: float

    def as_event_times(self, now_monotonic: float) -> FeedEventTimes:
        """
        将内部单调时钟标记转换为可对外诊断的时间快照。

        Args:
            now_monotonic: 同一次 health 读取的当前单调时钟值。

        Returns:
            FeedEventTimes: 保留 gateway/client/exchange 时间与 gateway age 的快照。

        Raises:
            MarketFreshnessError: 单调时钟倒退时抛出。
        """
        age = now_monotonic - self.gateway_monotonic
        if age < 0:
            raise MarketFreshnessError("MONOTONIC_CLOCK_MOVED_BACKWARDS")
        return FeedEventTimes(
            last_gateway_received_at=self.event.gateway_received_at,
            last_client_received_at=self.event.client_received_at,
            last_exchange_time=self.event.exchange_time,
            gateway_age_seconds=age,
        )


class MarketFreshnessTracker:
    """
    记录快照到达时刻，并在读取时通过显式 policy 计算暂停后的 stale age。

    记录和读取受 ``RLock`` 保护；policy 在锁外执行，避免 calendar owner 阻塞
    callback 记录。
    """

    def __init__(
        self,
        stale_after_seconds: Mapping[MarketDataLevel, float],
        expectation_policy: Optional[MarketUpdateExpectationPolicy] = None,
        monotonic_clock: Optional[MonotonicClock] = None,
    ) -> None:
        """
        初始化分级 stale 阈值、显式更新 policy 和可测试单调时钟。

        Args:
            stale_after_seconds: tick_compat/L1/L2 各自的正数 stale 阈值。
            expectation_policy: 可选 calendar/status policy；未提供时明确标记 policy 未配置并不猜测 stale。
            monotonic_clock: 用于记录 gateway age 的时钟；默认 ``time.monotonic``。

        Returns:
            None: 空 tracker 创建完成后返回。

        Raises:
            ValueError: 阈值缺失、非正数或非有限值时抛出。
        """
        thresholds: Dict[MarketDataLevel, float] = {}
        for raw_level, raw_threshold in stale_after_seconds.items():
            level = MarketDataLevel(raw_level)
            if (
                isinstance(raw_threshold, bool)
                or raw_threshold <= 0
                or not math.isfinite(raw_threshold)
            ):
                raise ValueError("stale_after_seconds 必须是有限正数")
            thresholds[level] = float(raw_threshold)
        missing = set(MarketDataLevel) - set(thresholds)
        if missing:
            raise ValueError(
                "stale_after_seconds 缺少级别: " + ",".join(sorted(level.value for level in missing))
            )
        self._thresholds = thresholds
        self._policy = expectation_policy
        self._clock = monotonic_clock or time.monotonic
        self._lock = RLock()
        self._snapshot_marks: Dict[Tuple[str, MarketDataLevel], _FreshnessMark] = {}
        self._capability_marks: Dict[str, _FreshnessMark] = {}
        self._module_marks: Dict[str, _FreshnessMark] = {}
        self._latest_mark: Optional[_FreshnessMark] = None
        self._decisions: Dict[Tuple[str, MarketDataLevel], FreshnessDecision] = {}

    @property
    def policy_configured(self) -> bool:
        """
        返回是否已注入显式市场更新窗口 policy。

        Returns:
            bool: 已配置 callback 时为 True；未配置且必须 fail-closed 时为 False。
        """
        return self._policy is not None

    def capture_gateway_ingress(self, event: MarketEvent) -> GatewayIngressMark:
        """
        在厂商 callback 首次进入受控 gateway 路径时立即取得单调时间。

        Args:
            event: 已完成来源身份和 epoch 绑定的不可变市场事件。

        Returns:
            GatewayIngressMark: 可跨后续有界队列与 drain 传递的身份绑定时间标记。

        Raises:
            MarketFreshnessError: 单调时钟返回非有限值时抛出。
        """
        now = float(self._clock())
        if not math.isfinite(now):
            raise MarketFreshnessError("MONOTONIC_CLOCK_INVALID")
        return GatewayIngressMark(
            provider=event.provider,
            capability_id=event.capability_key,
            event_type=event.event_type,
            level=event.level,
            session_epoch=event.session_epoch,
            security=event.security,
            stream_id=event.stream_id,
            channel_id=event.channel_id,
            gateway_monotonic=now,
            event=event,
        )

    def record(
        self,
        event: MarketEvent,
        gateway_ingress: Optional[GatewayIngressMark] = None,
    ) -> None:
        """
        在事件首次进入 Feed 受控路径时记录单调时钟和最近时间。

        Args:
            event: 已拥有 provider/capability/level/epoch 的市场事件。
            gateway_ingress: callback 首次进入 gateway 时取得的可选绑定标记；未提供时当前调用即视为 ingress。

        Returns:
            None: 全局、模块、能力与可选证券快照标记更新后返回。

        Raises:
            MarketFreshnessError: 单调时钟非法、mark 不匹配或快照 ingress 倒序时抛出。
        """
        ingress = gateway_ingress or self.capture_gateway_ingress(event)
        if not ingress.matches(event):
            raise MarketFreshnessError("GATEWAY_INGRESS_EVENT_MISMATCH")
        module = event.level.value
        mark = _FreshnessMark(
            event=event,
            module=module,
            gateway_monotonic=ingress.gateway_monotonic,
        )
        with self._lock:
            snapshot_key: Optional[Tuple[str, MarketDataLevel]] = None
            if event.security is not None and event.event_type in {
                MarketEventType.TICK_COMPAT,
                MarketEventType.SNAPSHOT_L1,
                MarketEventType.SNAPSHOT_L2,
            }:
                snapshot_key = (event.security, event.level)
                current_snapshot = self._snapshot_marks.get(snapshot_key)
                if (
                    current_snapshot is not None
                    and current_snapshot.gateway_monotonic > mark.gateway_monotonic
                ):
                    raise MarketFreshnessError("OUT_OF_ORDER_GATEWAY_INGRESS")
            if (
                self._latest_mark is None
                or self._latest_mark.gateway_monotonic <= mark.gateway_monotonic
            ):
                self._latest_mark = mark
            current_capability = self._capability_marks.get(event.capability_key)
            if (
                current_capability is None
                or current_capability.gateway_monotonic <= mark.gateway_monotonic
            ):
                self._capability_marks[event.capability_key] = mark
            current_module = self._module_marks.get(module)
            if current_module is None or current_module.gateway_monotonic <= mark.gateway_monotonic:
                self._module_marks[module] = mark
            if snapshot_key is not None:
                self._snapshot_marks[snapshot_key] = mark
                self._decisions.pop(snapshot_key, None)

    def reset(self) -> None:
        """
        清空旧 session epoch 的全部到达标记和时效决策。

        Returns:
            None: 重连/日切后不再使用旧快照证据时返回。
        """
        with self._lock:
            self._snapshot_marks.clear()
            self._capability_marks.clear()
            self._module_marks.clear()
            self._latest_mark = None
            self._decisions.clear()

    def retain_snapshot_keys(
        self,
        retained_keys: FrozenSet[Tuple[str, MarketDataLevel]],
    ) -> None:
        """
        移除已不再被任何 active lease 覆盖的快照时效证据。

        Args:
            retained_keys: 当前仍有确认订阅和缓存的证券/精确级别键集合。

        Returns:
            None: 失效 scope、聚合能力时间、模块时间和决策同步清理后返回。

        Side Effects:
            不重采样任何保留 mark，因而不会把订阅拓扑变化伪装成新的 gateway ingress。
        """
        normalized_keys = frozenset(
            (str(security).strip().upper(), MarketDataLevel(level))
            for security, level in retained_keys
        )
        with self._lock:
            removed = {
                key: mark
                for key, mark in self._snapshot_marks.items()
                if key not in normalized_keys
            }
            if not removed:
                return
            removed_ids = {id(mark) for mark in removed.values()}
            for key in removed:
                self._snapshot_marks.pop(key, None)
                self._decisions.pop(key, None)
            affected_capabilities = {mark.event.capability_key for mark in removed.values()}
            for capability_id in affected_capabilities:
                current = self._capability_marks.get(capability_id)
                if current is None or id(current) not in removed_ids:
                    continue
                candidates = [
                    mark
                    for mark in self._snapshot_marks.values()
                    if mark.event.capability_key == capability_id
                ]
                if candidates:
                    self._capability_marks[capability_id] = max(
                        candidates, key=lambda item: item.gateway_monotonic
                    )
                else:
                    self._capability_marks.pop(capability_id, None)
            for module, current in tuple(self._module_marks.items()):
                if id(current) not in removed_ids:
                    continue
                candidates = [
                    mark for mark in self._capability_marks.values() if mark.module == module
                ]
                if candidates:
                    self._module_marks[module] = max(
                        candidates, key=lambda item: item.gateway_monotonic
                    )
                else:
                    self._module_marks.pop(module, None)
            if self._latest_mark is not None and id(self._latest_mark) in removed_ids:
                self._latest_mark = (
                    max(
                        self._capability_marks.values(),
                        key=lambda item: item.gateway_monotonic,
                    )
                    if self._capability_marks
                    else None
                )

    def evaluate(self, security: str, level: MarketDataLevel) -> FreshnessDecision:
        """
        按单调 gateway age、累计暂停时长和分级阈值评估一个快照。

        Args:
            security: 标准证券代码。
            level: tick_compat、L1 或 L2 精确级别。

        Returns:
            FreshnessDecision: 包含 raw/effective age、market state 和 stale 结果的决策。

        Raises:
            FreshnessRecordNotFoundError: 尚无该证券/级别到达记录时抛出。
            MarketUpdatePolicyError: policy 返回错误类型或暂停时长超过 raw age 时抛出。
        """
        normalized_security = str(security).strip().upper()
        normalized_level = MarketDataLevel(level)
        if not normalized_security:
            raise ValueError("security 不能为空")
        key = (normalized_security, normalized_level)
        with self._lock:
            mark = self._snapshot_marks.get(key)
        if mark is None:
            raise FreshnessRecordNotFoundError(
                f"FRESHNESS_RECORD_NOT_FOUND: security={normalized_security}, "
                f"level={normalized_level.value}"
            )
        now = float(self._clock())
        decision = self._evaluate_mark(normalized_security, normalized_level, mark, now)
        with self._lock:
            if self._snapshot_marks.get(key) is mark:
                self._decisions[key] = decision
        return decision

    def _evaluate_mark(
        self,
        security: str,
        level: MarketDataLevel,
        mark: _FreshnessMark,
        now: float,
    ) -> FreshnessDecision:
        """
        使用冻结 mark 与同一单调时点计算一次 freshness 决策。

        Args:
            security: 已规范化证券代码。
            level: 已规范化精确行情级别。
            mark: 某次 tracker snapshot 冻结的 ingress mark。
            now: 复制 marks 后取得的同一单调时点。

        Returns:
            FreshnessDecision: 不读取后续 mark 的完整时效证据。

        Raises:
            MarketFreshnessError: 单调时钟无效、倒退或 policy 合同失败时抛出。
        """
        raw_age = now - mark.gateway_monotonic
        if not math.isfinite(now) or raw_age < 0:
            raise MarketFreshnessError("MONOTONIC_CLOCK_MOVED_BACKWARDS_OR_INVALID")
        expectation = self._evaluate_policy(security, level, mark.event, raw_age)
        if expectation.paused_seconds > raw_age:
            raise MarketUpdatePolicyError(
                "MARKET_UPDATE_POLICY_INVALID: paused_seconds 超过 gateway raw age"
            )
        effective_age = raw_age - expectation.paused_seconds
        threshold = self._thresholds[level]
        gateway_stale = effective_age > threshold
        source_time_verified = (
            mark.event.exchange_time is not None
            and expectation.effective_source_age_seconds is not None
        )
        source_stale = expectation.source_stale or (
            expectation.effective_source_age_seconds is not None
            and expectation.effective_source_age_seconds > threshold
        )
        reason = expectation.reason
        if reason is None and source_stale:
            reason = "exchange_time_stale"
        elif reason is None and gateway_stale:
            reason = "gateway_age_stale"
        decision = FreshnessDecision(
            security=security,
            level=level,
            capability_id=mark.event.capability_key,
            expected_update=expectation.expected,
            market_state=expectation.market_state,
            raw_age_seconds=raw_age,
            paused_seconds=expectation.paused_seconds,
            effective_age_seconds=effective_age,
            stale_after_seconds=threshold,
            gateway_stale=gateway_stale,
            source_time_verified=source_time_verified,
            source_stale=source_stale,
            stale=gateway_stale or source_stale,
            effective_source_age_seconds=expectation.effective_source_age_seconds,
            last_gateway_received_at=mark.event.gateway_received_at,
            last_client_received_at=mark.event.client_received_at,
            last_exchange_time=mark.event.exchange_time,
            reason=reason,
        )
        return decision

    def evaluate_all(self) -> Tuple[FreshnessDecision, ...]:
        """
        使用同一 policy 逐项刷新当前全部证券/级别的时效决策。

        Returns:
            Tuple[FreshnessDecision, ...]: 按证券和级别排序的决策快照。
        """
        snapshot = self.runtime_snapshot()
        if snapshot.failure_reason is not None:
            raise MarketFreshnessError(snapshot.failure_reason)
        return snapshot.decisions

    def stale_capabilities(self) -> FrozenSet[str]:
        """
        返回当前 policy 评估后至少有一个快照 stale 的原子能力。

        Returns:
            FrozenSet[str]: 可用于 FeedHealth readiness 派生的 capability ID 集合。
        """
        return frozenset(
            decision.capability_id for decision in self.evaluate_all() if decision.stale
        )

    def readiness_by_capability(self) -> Mapping[str, CapabilityReadiness]:
        """
        将已有快照的显式 freshness 决策聚合为 capability readiness。

        Returns:
            Mapping[str, CapabilityReadiness]: stale 为 stale；policy 未配置为
            unavailable；午休、闭市、停牌等显式暂停窗口与 fresh 数据为 ready。

        Side Effects:
            执行注入的 expectation policy 并刷新内部诊断决策，不修改事件记录。
        """
        return self.runtime_snapshot().capability_readiness

    @staticmethod
    def _aggregate_decision_readiness(
        decisions: Tuple[FreshnessDecision, ...],
    ) -> Mapping[str, CapabilityReadiness]:
        """
        将同代 freshness decisions 保守聚合为 capability readiness。

        Args:
            decisions: 同一 marks snapshot 派生的完整决策。

        Returns:
            Mapping[str, CapabilityReadiness]: 每个 capability 的最严重状态。
        """
        readiness: Dict[str, CapabilityReadiness] = {}
        priority = {
            CapabilityReadiness.READY: 0,
            CapabilityReadiness.STALE: 1,
            CapabilityReadiness.UNAVAILABLE: 2,
        }
        for decision in decisions:
            if decision.market_state == "policy_unconfigured" or not decision.source_time_verified:
                state = CapabilityReadiness.UNAVAILABLE
            elif decision.stale:
                state = CapabilityReadiness.STALE
            else:
                state = CapabilityReadiness.READY
            previous = readiness.get(decision.capability_id)
            if previous is None or priority[state] > priority[previous]:
                readiness[decision.capability_id] = state
        return readiness

    def runtime_snapshot(self) -> MarketFreshnessSnapshot:
        """
        用一次冻结 marks 计算 readiness、三类时间和全部决策。

        Returns:
            MarketFreshnessSnapshot: 不混用 callback 期间新旧事件的同代运行快照。

        Side Effects:
            在 tracker 锁外执行注入 policy；若对应 mark 未变化则刷新诊断 decision 缓存。
        """
        with self._lock:
            snapshot_marks = dict(self._snapshot_marks)
            latest = self._latest_mark
            capability_marks = dict(self._capability_marks)
            module_marks = dict(self._module_marks)
        now = float(self._clock())
        if not math.isfinite(now):
            return MarketFreshnessSnapshot(
                capability_readiness={},
                decisions=(),
                latest_times=None,
                capability_times={},
                module_times={},
                failure_reason="freshness_clock_failed:MONOTONIC_CLOCK_INVALID",
            )
        try:
            latest_times = latest.as_event_times(now) if latest is not None else None
            capability_times = {
                capability_id: mark.as_event_times(now)
                for capability_id, mark in capability_marks.items()
            }
            module_times = {
                module: mark.as_event_times(now) for module, mark in module_marks.items()
            }
        except MarketFreshnessError as exc:
            return MarketFreshnessSnapshot(
                capability_readiness={},
                decisions=(),
                latest_times=None,
                capability_times={},
                module_times={},
                failure_reason=f"freshness_clock_failed:{type(exc).__name__}",
            )
        try:
            decisions = tuple(
                self._evaluate_mark(security, level, mark, now)
                for (security, level), mark in sorted(
                    snapshot_marks.items(),
                    key=lambda item: (item[0][0], item[0][1].value),
                )
            )
            readiness = self._aggregate_decision_readiness(decisions)
            failure_reason = None
        except MarketFreshnessError as exc:
            decisions = ()
            readiness = {}
            failure_reason = f"freshness_policy_failed:{type(exc).__name__}"
        if decisions:
            decision_by_key = {
                (decision.security, decision.level): decision for decision in decisions
            }
            with self._lock:
                for key, mark in snapshot_marks.items():
                    if self._snapshot_marks.get(key) is mark:
                        decision = decision_by_key.get(key)
                        if decision is not None:
                            self._decisions[key] = decision
        return MarketFreshnessSnapshot(
            capability_readiness=readiness,
            decisions=decisions,
            latest_times=latest_times,
            capability_times=capability_times,
            module_times=module_times,
            failure_reason=failure_reason,
        )

    def event_times(
        self,
    ) -> Tuple[
        Optional[FeedEventTimes],
        Mapping[str, FeedEventTimes],
        Mapping[str, FeedEventTimes],
    ]:
        """
        生成全局、原子能力和模块级最近事件时间快照。

        Returns:
            Tuple[Optional[FeedEventTimes], Mapping[str, FeedEventTimes], Mapping[str, FeedEventTimes]]:
            全局最近时间、能力映射和模块映射；尚无事件时全局值为 None。
        """
        with self._lock:
            latest = self._latest_mark
            capability_marks = dict(self._capability_marks)
            module_marks = dict(self._module_marks)
        now = float(self._clock())
        if not math.isfinite(now):
            raise MarketFreshnessError("MONOTONIC_CLOCK_INVALID")
        latest_times = latest.as_event_times(now) if latest is not None else None
        capability_times = {
            capability_id: mark.as_event_times(now)
            for capability_id, mark in capability_marks.items()
        }
        module_times = {module: mark.as_event_times(now) for module, mark in module_marks.items()}
        return latest_times, capability_times, module_times

    def _evaluate_policy(
        self,
        security: str,
        level: MarketDataLevel,
        event: MarketEvent,
        raw_age_seconds: float,
    ) -> MarketUpdateExpectation:
        """
        执行显式 update policy，或在未配置时返回不猜测更新窗口的诊断结果。

        Args:
            security: 标准证券代码。
            level: 当前快照级别。
            event: 对应最近事件。
            raw_age_seconds: 自 gateway 到达标记起的原始单调 age。

        Returns:
            MarketUpdateExpectation: 是否预期更新、市场状态和累计暂停时长。

        Raises:
            MarketUpdatePolicyError: policy 返回值不是 MarketUpdateExpectation 时抛出。
        """
        if self._policy is None:
            return MarketUpdateExpectation(
                expected=False,
                market_state="policy_unconfigured",
                reason="market_update_policy_not_configured",
            )
        try:
            result = self._policy(security, level, event, raw_age_seconds)
        except Exception as exc:
            raise MarketUpdatePolicyError(
                "MARKET_UPDATE_POLICY_FAILED: " f"cause={type(exc).__name__}"
            ) from exc
        if not isinstance(result, MarketUpdateExpectation):
            raise MarketUpdatePolicyError(
                "MARKET_UPDATE_POLICY_INVALID: 必须返回 MarketUpdateExpectation"
            )
        return result


_READINESS_PRIORITY: Mapping[CapabilityReadiness, int] = {
    CapabilityReadiness.READY: 0,
    CapabilityReadiness.DEGRADED: 1,
    CapabilityReadiness.STALE: 2,
    CapabilityReadiness.UNAVAILABLE: 3,
    CapabilityReadiness.UNAUTHORIZED: 4,
}


def aggregate_module_readiness(
    capability_readiness: Mapping[str, CapabilityReadiness],
    module_capabilities: Mapping[str, Tuple[str, ...]],
) -> Mapping[str, CapabilityReadiness]:
    """
    将显式模块内的原子 capability readiness 按最严重状态聚合。

    Args:
        capability_readiness: 当前 manifest/stale/队列派生后的原子能力状态。
        module_capabilities: 模块名到已明确属于该模块能力 ID 的映射。

    Returns:
        Mapping[str, CapabilityReadiness]: 每个模块的只读聚合状态；无已声明能力时为 unavailable。
    """
    result: Dict[str, CapabilityReadiness] = {}
    for raw_module, capability_ids in module_capabilities.items():
        module = str(raw_module).strip().lower()
        if not module:
            raise ValueError("module_capabilities 不能包含空模块名")
        states = [
            CapabilityReadiness(capability_readiness[capability_id])
            for capability_id in capability_ids
            if capability_id in capability_readiness
        ]
        result[module] = (
            max(states, key=lambda state: _READINESS_PRIORITY[state])
            if states
            else CapabilityReadiness.UNAVAILABLE
        )
    return result


__all__ = [
    "FreshnessDecision",
    "FreshnessRecordNotFoundError",
    "GatewayIngressMark",
    "MarketFreshnessError",
    "MarketFreshnessSnapshot",
    "MarketFreshnessTracker",
    "MarketUpdateExpectation",
    "MarketUpdateExpectationPolicy",
    "MarketUpdatePolicyError",
    "MonotonicClock",
    "aggregate_module_readiness",
]
