"""
作者: BruceLee

文件职责: 将公开订阅 spec/item 稳定映射到 adapter scopes 并投影实时回执。
主要输入: MarketSubscriptionSpec、本地准入结果、事件市场和租约快照。
主要输出: AdapterSubscriptionScope 绑定和 requested/sent/pending/confirmed/rejected/canceled 回执。
上游关系: Feed 注入自身 capability 准入与 event-to-market 解析函数。
下游关系: Feed public receipt 与 health.active_subscriptions 共用同一投影结果。
关键配置约定: 本模块是纯函数式离线投影，不联网、不加载 SDK、不交易。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import AbstractSet, Any, Callable, Mapping, Optional, Sequence, Tuple

from .models import (
    MarketDataLevel,
    MarketEventType,
    MarketSubscriptionReceipt,
    MarketSubscriptionSpec,
    SubscriptionItemResult,
    SubscriptionItemState,
    SubscriptionSelector,
)
from .subscriptions import (
    AdapterSubscriptionOperation,
    AdapterSubscriptionScope,
    SubscriptionLeaseSnapshot,
)

SubscriptionFailureResolver = Callable[
    [MarketSubscriptionSpec, str, MarketEventType], Optional[Tuple[str, str]]
]
SubscriptionEventMarketsResolver = Callable[[MarketEventType], Sequence[str]]


@dataclass(frozen=True)
class FeedSubscriptionItemPlan:
    """保存一个公开 receipt item 到一个或多个 adapter scope 的稳定映射。"""

    selector: SubscriptionSelector
    scope: str
    level: MarketDataLevel
    event_type: MarketEventType
    adapter_scopes: Tuple[AdapterSubscriptionScope, ...] = ()
    rejection_code: Optional[str] = None
    rejection_reason: Optional[str] = None


class SubscriptionReceiptProjector:
    """集中实现 spec 展开、scope 绑定和同快照 receipt 投影。"""

    def __init__(
        self,
        failure_resolver: SubscriptionFailureResolver,
        event_markets_resolver: SubscriptionEventMarketsResolver,
    ) -> None:
        """
        初始化动态 capability 准入与事件市场解析器。

        Args:
            failure_resolver: 对单个 spec/scope/event 返回本地拒绝或 None。
            event_markets_resolver: 返回一个事件允许的标准市场序列。

        Returns:
            None: 投影器初始化完成后返回。
        """
        if not callable(failure_resolver) or not callable(event_markets_resolver):
            raise ValueError("subscription receipt resolvers 必须可调用")
        self._failure_resolver = failure_resolver
        self._event_markets_resolver = event_markets_resolver

    def build_plans(
        self,
        spec: MarketSubscriptionSpec,
        event_types: Sequence[MarketEventType],
    ) -> Tuple[FeedSubscriptionItemPlan, ...]:
        """
        将规范化 spec 展开为公开逐项状态与底层 scopes 的稳定映射。

        Args:
            spec: 已完成 selector/level/event 规范化的订阅请求。
            event_types: 通配展开后的实际事件类型。

        Returns:
            Tuple[FeedSubscriptionItemPlan, ...]: 按 scope、event 输入语义排序的计划。
        """
        if spec.selector is SubscriptionSelector.ALL:
            return self._build_all_scope_plans(spec, event_types)
        plans = []
        for scope in spec.scope_items():
            for event_type in event_types:
                failure = self._failure_resolver(spec, scope, event_type)
                adapter_scopes: Tuple[AdapterSubscriptionScope, ...] = ()
                if failure is None:
                    adapter_scopes = self._adapter_scopes_for_item(spec, scope, event_type)
                    if not adapter_scopes:
                        failure = (
                            "ADAPTER_SCOPE_UNAVAILABLE",
                            f"selector={spec.selector.value}, event={event_type.value}",
                        )
                plans.append(
                    FeedSubscriptionItemPlan(
                        selector=spec.selector,
                        scope=scope,
                        level=spec.level,
                        event_type=event_type,
                        adapter_scopes=adapter_scopes,
                        rejection_code=failure[0] if failure is not None else None,
                        rejection_reason=failure[1] if failure is not None else None,
                    )
                )
        return tuple(plans)

    def estimate_plan_count(
        self,
        spec: MarketSubscriptionSpec,
        event_types: Sequence[MarketEventType],
    ) -> int:
        """
        在分配 item plans 前计算公开逐项数量，用于 Feed 的硬容量门禁。

        Args:
            spec: 已规范化的订阅请求。
            event_types: 通配展开后的实际事件类型。

        Returns:
            int: 本地拒绝或逐市场展开后至少为一的精确 item 数量。
        """
        if spec.selector is not SubscriptionSelector.ALL:
            return len(spec.scope_items()) * len(event_types)
        count = 0
        for event_type in event_types:
            failure = self._failure_resolver(spec, "*", event_type)
            markets = tuple(self._event_markets_resolver(event_type))
            count += 1 if failure is not None or not markets else len(set(markets))
        return count

    def initial_items(
        self,
        plans: Sequence[FeedSubscriptionItemPlan],
    ) -> Tuple[SubscriptionItemResult, ...]:
        """
        为 coordinator 调度前的 plans 构造 requested 或本地 rejected 项。

        Args:
            plans: 已完成准入和 adapter scope 展开的计划。

        Returns:
            Tuple[SubscriptionItemResult, ...]: 不包含任何伪造 confirmed 状态的初始项。
        """
        return tuple(self._item_before_dispatch(plan) for plan in plans)

    def unique_adapter_scopes(
        self,
        plans: Sequence[FeedSubscriptionItemPlan],
    ) -> Tuple[AdapterSubscriptionScope, ...]:
        """
        聚合 plans 中全部合法 adapter scopes 并稳定排序。

        Args:
            plans: 公开 item 绑定计划。

        Returns:
            Tuple[AdapterSubscriptionScope, ...]: 去重的底层作用域。
        """
        return tuple(
            sorted(
                {scope for plan in plans for scope in plan.adapter_scopes},
                key=self._adapter_scope_sort_key,
            )
        )

    def project_receipt(
        self,
        previous: MarketSubscriptionReceipt,
        spec: MarketSubscriptionSpec,
        plans: Sequence[FeedSubscriptionItemPlan],
        snapshot: SubscriptionLeaseSnapshot,
        *,
        active: bool,
        limits: Mapping[str, Any],
    ) -> MarketSubscriptionReceipt:
        """
        使用单一 manager 快照重建一个 active 或退订过渡 receipt。

        Args:
            previous: 保留 actual events/effective scope 的上一版回执。
            spec: 原始规范化订阅请求。
            plans: 公开 items 与 adapter scopes 绑定。
            snapshot: desired/sent/confirmed 的线性化快照。
            active: 当前 session lease 是否仍保留 desired。
            limits: Feed 可公开的脱敏限制。

        Returns:
            MarketSubscriptionReceipt: 与 snapshot 状态一致的不可变回执。
        """
        items = tuple(self._runtime_item_result(plan, snapshot, active=active) for plan in plans)
        return MarketSubscriptionReceipt.from_items(
            subscription_id=previous.subscription_id,
            spec=spec,
            session_epoch=snapshot.session_epoch,
            items=items,
            actual_event_types=previous.actual_event_types,
            effective_symbols=previous.effective_symbols,
            effective_markets=previous.effective_markets,
            limits=limits,
        )

    def _adapter_scopes_for_item(
        self,
        spec: MarketSubscriptionSpec,
        scope: str,
        event_type: MarketEventType,
    ) -> Tuple[AdapterSubscriptionScope, ...]:
        """
        把一个公开 selector item 转换为模块、事件、市场、证券四元组。

        Args:
            spec: 当前规范化订阅请求。
            scope: symbol、market 或通配符公开 scope。
            event_type: 已展开的实际事件类型。

        Returns:
            Tuple[AdapterSubscriptionScope, ...]: 去重稳定排序的底层 scopes。
        """
        module = spec.level.value
        if spec.selector is SubscriptionSelector.SYMBOLS:
            market = scope.rsplit(".", 1)[-1] if "." in scope else ""
            if not market:
                return ()
            return (
                AdapterSubscriptionScope(
                    module=module,
                    event_type=event_type,
                    market=market,
                    symbol=scope,
                ),
            )
        if spec.selector is SubscriptionSelector.MARKETS:
            return (
                AdapterSubscriptionScope(
                    module=module,
                    event_type=event_type,
                    market=scope,
                ),
            )
        markets = tuple(self._event_markets_resolver(event_type)) if scope == "*" else (scope,)
        return tuple(
            AdapterSubscriptionScope(
                module=module,
                event_type=event_type,
                market=market,
            )
            for market in sorted(set(markets))
        )

    def _build_all_scope_plans(
        self,
        spec: MarketSubscriptionSpec,
        event_types: Sequence[MarketEventType],
    ) -> Tuple[FeedSubscriptionItemPlan, ...]:
        """
        将 selector=ALL 按实际 adapter 市场拆成可独立确认或拒绝的公开 items。

        Args:
            spec: selector 已规范化为 ALL 的订阅请求。
            event_types: 通配展开后的实际事件类型。

        Returns:
            Tuple[FeedSubscriptionItemPlan, ...]: 每个事件/市场一个计划；全局准入失败
            或市场未知时保留单个 ``scope='*'`` 拒绝项。
        """
        plans = []
        for event_type in event_types:
            failure = self._failure_resolver(spec, "*", event_type)
            markets = tuple(sorted(set(self._event_markets_resolver(event_type))))
            if failure is not None or not markets:
                code, reason = failure or (
                    "ADAPTER_SCOPE_UNAVAILABLE",
                    f"selector=all, event={event_type.value}",
                )
                plans.append(
                    FeedSubscriptionItemPlan(
                        selector=spec.selector,
                        scope="*",
                        level=spec.level,
                        event_type=event_type,
                        rejection_code=code,
                        rejection_reason=reason,
                    )
                )
                continue
            for market in markets:
                plans.append(
                    FeedSubscriptionItemPlan(
                        selector=spec.selector,
                        scope=market,
                        level=spec.level,
                        event_type=event_type,
                        adapter_scopes=self._adapter_scopes_for_item(
                            spec,
                            market,
                            event_type,
                        ),
                    )
                )
        return tuple(plans)

    def _runtime_item_result(
        self,
        plan: FeedSubscriptionItemPlan,
        snapshot: SubscriptionLeaseSnapshot,
        *,
        active: bool,
    ) -> SubscriptionItemResult:
        """
        将一个公开 item plan 映射为当前 confirmed/pending/rejected/canceled 状态。

        Args:
            plan: 公开 item 与底层 scopes 的稳定绑定。
            snapshot: coordinator 的单一状态快照。
            active: 当前 session lease 是否仍保留 desired 意图。

        Returns:
            SubscriptionItemResult: 可直接构造整体 receipt 的逐项结果。
        """
        if plan.rejection_code is not None:
            return SubscriptionItemResult(
                selector=plan.selector,
                scope=plan.scope,
                level=plan.level,
                event_type=plan.event_type,
                state=SubscriptionItemState.REJECTED,
                code=plan.rejection_code,
                reason=plan.rejection_reason,
            )
        if active:
            state, code, reason = self._active_item_state(plan, snapshot)
        else:
            state, code, reason = self._canceled_item_state(plan, snapshot)
        return SubscriptionItemResult(
            selector=plan.selector,
            scope=plan.scope,
            level=plan.level,
            event_type=plan.event_type,
            state=state,
            code=code,
            reason=reason,
        )

    def _active_item_state(
        self,
        plan: FeedSubscriptionItemPlan,
        snapshot: SubscriptionLeaseSnapshot,
    ) -> Tuple[SubscriptionItemState, Optional[str], Optional[str]]:
        """
        计算仍有 desired lease 的一个 item 在 adapter 状态机中的公开状态。

        Args:
            plan: 至少包含一个 adapter scope 的有效 item plan。
            snapshot: desired/sent/confirmed 与失败集合快照。

        Returns:
            Tuple[SubscriptionItemState, Optional[str], Optional[str]]: 状态、错误码和原因。
        """
        failures = tuple(
            failure
            for failure in snapshot.failures
            if failure.action.operation is AdapterSubscriptionOperation.SUBSCRIBE
            and failure.action.scope in plan.adapter_scopes
        )
        if failures:
            failure = sorted(
                failures,
                key=lambda item: self._adapter_scope_sort_key(item.action.scope),
            )[0]
            return SubscriptionItemState.REJECTED, failure.code, failure.reason
        if self._all_scopes_covered(plan.adapter_scopes, snapshot.stable_confirmed):
            return SubscriptionItemState.CONFIRMED, None, None
        if self._any_scope_covered(plan.adapter_scopes, snapshot.uncertain_subscribe):
            return (
                SubscriptionItemState.PENDING,
                "ACK_RESULT_UNCERTAIN",
                "adapter callback timeout requires reconciliation",
            )
        if self._all_scopes_covered(plan.adapter_scopes, snapshot.sent):
            return SubscriptionItemState.SENT, None, None
        if self._any_scope_covered(plan.adapter_scopes, snapshot.planned_subscribe):
            return SubscriptionItemState.REQUESTED, None, None
        return SubscriptionItemState.PENDING, None, None

    def _canceled_item_state(
        self,
        plan: FeedSubscriptionItemPlan,
        snapshot: SubscriptionLeaseSnapshot,
    ) -> Tuple[SubscriptionItemState, Optional[str], Optional[str]]:
        """
        区分退订控制尚在途、明确失败与已达到 confirmed-empty 的 item。

        Args:
            plan: 原 lease 的公开 item 与底层 scopes。
            snapshot: 移除 desired 后的 adapter 过渡快照。

        Returns:
            Tuple[SubscriptionItemState, Optional[str], Optional[str]]:
                pending/rejected/canceled 结果。
        """
        relevant = tuple(scope for scope in plan.adapter_scopes if scope not in snapshot.desired)
        if not relevant:
            return SubscriptionItemState.CANCELED, None, None
        failures = tuple(
            failure
            for failure in snapshot.failures
            if any(failure.action.scope.overlaps(scope) for scope in relevant)
        )
        if failures:
            failure = sorted(
                failures,
                key=lambda item: (
                    item.action.operation.value,
                    self._adapter_scope_sort_key(item.action.scope),
                ),
            )[0]
            return (
                SubscriptionItemState.REJECTED,
                f"CANCEL_TRANSITION_{failure.code}",
                failure.reason,
            )
        retained = (
            set(snapshot.sent)
            | set(snapshot.confirmed)
            | set(snapshot.planned_subscribe)
            | set(snapshot.planned_unsubscribe)
            | set(snapshot.pending_subscribe)
            | set(snapshot.pending_unsubscribe)
            | set(snapshot.uncertain_subscribe)
            | set(snapshot.uncertain_unsubscribe)
        )
        if any(scope in retained for scope in relevant):
            return SubscriptionItemState.PENDING, None, "unsubscribe confirmation pending"
        return SubscriptionItemState.CANCELED, None, None

    @staticmethod
    def _item_before_dispatch(plan: FeedSubscriptionItemPlan) -> SubscriptionItemResult:
        """
        为 coordinator 调度前的 plan 构造 requested 或本地 rejected 回执项。

        Args:
            plan: 已完成本地准入和 adapter scope 展开的 item plan。

        Returns:
            SubscriptionItemResult: 不包含任何伪造 confirmed 状态的初始项。
        """
        state = (
            SubscriptionItemState.REJECTED
            if plan.rejection_code is not None
            else SubscriptionItemState.REQUESTED
        )
        return SubscriptionItemResult(
            selector=plan.selector,
            scope=plan.scope,
            level=plan.level,
            event_type=plan.event_type,
            state=state,
            code=plan.rejection_code,
            reason=plan.rejection_reason,
        )

    @staticmethod
    def _all_scopes_covered(
        targets: Sequence[AdapterSubscriptionScope],
        candidates: AbstractSet[AdapterSubscriptionScope],
    ) -> bool:
        """
        判断每个目标 scope 是否均被某个 confirmed/sent 候选覆盖。

        Args:
            targets: 公开 item 所需的全部底层 scopes。
            candidates: full/partial 覆盖候选集合。

        Returns:
            bool: 所有目标均有覆盖时为 True。
        """
        return bool(targets) and all(
            any(candidate.covers(target) for candidate in candidates) for target in targets
        )

    @staticmethod
    def _any_scope_covered(
        targets: Sequence[AdapterSubscriptionScope],
        candidates: AbstractSet[AdapterSubscriptionScope],
    ) -> bool:
        """
        判断至少一个目标 scope 是否被候选集合覆盖。

        Args:
            targets: 公开 item 所需的全部底层 scopes。
            candidates: planned/pending/uncertain 候选集合。

        Returns:
            bool: 至少存在一项覆盖时为 True。
        """
        return any(candidate.covers(target) for target in targets for candidate in candidates)

    @staticmethod
    def _adapter_scope_sort_key(
        scope: AdapterSubscriptionScope,
    ) -> Tuple[str, str, str, str]:
        """
        为 adapter scope 生成稳定排序键。

        Args:
            scope: 待排序的 full 或 partial 作用域。

        Returns:
            Tuple[str, str, str, str]: 模块、事件、市场和证券排序键。
        """
        return (
            scope.module,
            scope.event_type.value,
            scope.market,
            scope.symbol or "",
        )


__all__ = [
    "FeedSubscriptionItemPlan",
    "SubscriptionEventMarketsResolver",
    "SubscriptionFailureResolver",
    "SubscriptionReceiptProjector",
]
