"""
作者: BruceLee

文件职责: 定义市场数据原子能力、策略能力画像、唯一来源路由与离线预检合同。
主要输入: Provider 能力清单、显式路由规则、策略所需 capability 及一次具体查询请求。
主要输出: 不可变的 CapabilityManifest、RouteDecision 和 StrategyCapabilityPreflight。
上游关系: 由实时 Feed、历史/静态 Provider、Broker adapter 和部署配置提供能力声明。
下游关系: 供 LiveEngine、远程协议、策略初始化门禁和数据 API 在后续集成中调用。
关键配置约定: 路由必须显式配置；fallback 只由精确请求的静态 unsupported 触发。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from threading import RLock
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

_REQUEST_MODES = frozenset({"live", "backtest", "replay"})
_REQUEST_TIME_DOMAINS = frozenset({"realtime", "historical", "as_of", "current"})
_DECLARATION_TIME_DOMAINS = _REQUEST_TIME_DOMAINS.union({"any"})


def _infer_time_domain(capability_id: str) -> str:
    """从原子能力命名空间推导默认数据时间域。

    Args:
        capability_id: 已去除首尾空格的原子能力 ID。

    Returns:
        str: ``realtime``、``historical``、``current`` 或 ``as_of``。

    Side Effects:
        无；未知命名空间保守归类为 ``as_of``，不作通配。
    """

    if capability_id.startswith("realtime."):
        return "realtime"
    if capability_id.startswith("history."):
        return "historical"
    if capability_id.startswith("broker."):
        return "current"
    return "as_of"


def _normalize_runtime_modes(values: Sequence[str]) -> Tuple[str, ...]:
    """规范化 Provider 声明的可用运行模式。

    Args:
        values: 声明允许的 live/backtest/replay 序列。

    Returns:
        Tuple[str, ...]: 去重并稳定排序的运行模式。

    Raises:
        ValueError: 序列为空或包含未知模式时抛出。
    """

    normalized = tuple(sorted({str(value).strip().lower() for value in values}))
    if not normalized or any(value not in _REQUEST_MODES for value in normalized):
        raise ValueError("runtime_modes 只能包含 live、backtest 和 replay")
    return normalized


class CapabilitySupport(str, Enum):
    """表示 Provider 对原子能力的静态支持级别。"""

    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    CONDITIONAL = "conditional"


class CapabilityReadiness(str, Enum):
    """表示原子能力在当前运行时的动态就绪状态。"""

    READY = "ready"
    UNAVAILABLE = "unavailable"
    STALE = "stale"
    DEGRADED = "degraded"
    UNAUTHORIZED = "unauthorized"


class ProviderLocation(str, Enum):
    """表示能力所有者位于当前进程本地还是远程服务。"""

    LOCAL = "local"
    REMOTE = "remote"


class StrategyCapabilityProfile(str, Enum):
    """表示策略预置能力画像；画像可以继续收窄或增加原子能力。"""

    EXECUTION_ONLY = "execution_only"
    REALTIME_MICROSTRUCTURE_SIGNAL = "realtime_microstructure_signal"
    RESEARCH_LIVE = "research_live"


class DataCapabilityError(RuntimeError):
    """数据能力声明、路由、就绪或执行失败的公共基类。"""


class DataCapabilityUnavailableError(DataCapabilityError):
    """表示指定原子能力没有可用且语义相符的唯一 owner。"""

    def __init__(self, capability_id: str, reason: str) -> None:
        """
        初始化能力不可用错误。

        Args:
            capability_id: 不能解析 owner 的原子能力 ID。
            reason: 可供运维和测试断言的失败原因。
        """
        self.capability_id = capability_id
        self.reason = reason
        super().__init__(f"数据能力不可用: capability_id={capability_id}, reason={reason}")


class DataCapabilityNotReadyError(DataCapabilityError):
    """表示 owner 静态支持能力，但当前 readiness 不能满足调用。"""

    def __init__(
        self,
        capability_id: str,
        provider: str,
        readiness: CapabilityReadiness,
        reason: Optional[str] = None,
    ) -> None:
        """
        初始化能力未就绪错误。

        Args:
            capability_id: 当前请求的原子能力 ID。
            provider: 已被路由规则选中的 Provider 名称。
            readiness: Provider 报告的动态就绪状态。
            reason: Provider 给出的可选补充原因。
        """
        self.capability_id = capability_id
        self.provider = provider
        self.readiness = readiness
        self.reason = reason
        detail = f", reason={reason}" if reason else ""
        super().__init__(
            "数据能力未就绪: "
            f"capability_id={capability_id}, provider={provider}, "
            f"readiness={readiness.value}{detail}"
        )


class DataCapabilityRouteError(DataCapabilityError):
    """表示显式路由规则本身缺失、歧义或跨越了语义边界。"""


class DataCapabilityExecutionError(DataCapabilityError):
    """表示固定 RouteDecision 后，唯一 owner 的实际调用发生运行时错误。"""

    def __init__(self, decision: "RouteDecision", operation: str, cause: BaseException) -> None:
        """
        初始化路由执行错误并保留原始异常。

        Args:
            decision: 执行前已经固定的唯一 RouteDecision。
            operation: 在 owner 上调用的方法名称。
            cause: Provider 方法抛出的原始异常。
        """
        self.decision = decision
        self.operation = operation
        self.cause = cause
        super().__init__(
            "数据能力执行失败且未切换来源: "
            f"provider={decision.provider}, capability_id={decision.capability_id}, "
            f"operation={operation}, cause={cause}"
        )


def _normalize_strings(values: Sequence[str], label: str) -> Tuple[str, ...]:
    """
    将字符串集合去空、去重并稳定排序。

    Args:
        values: 需要规范化的字符串序列。
        label: 输入为空字符串时用于错误信息的字段名称。

    Returns:
        Tuple[str, ...]: 可稳定序列化和比较的字符串元组。

    Raises:
        ValueError: 任一输入为空字符串时抛出。
    """
    normalized = []
    for value in values:
        item = str(value).strip()
        if not item:
            raise ValueError(f"{label} 不能包含空字符串")
        normalized.append(item)
    return tuple(sorted(set(normalized)))


@dataclass(frozen=True)
class CapabilityRequest:
    """描述一次精确的原子能力请求及其语义和作用域约束。"""

    capability_id: str
    semantic_class: Optional[str] = None
    mode: str = "live"
    time_domain: Optional[str] = None
    as_of: Optional[Any] = None
    market: Optional[str] = None
    asset_type: Optional[str] = None
    frequency: Optional[str] = None
    adjustment: Optional[str] = None
    fields: Tuple[str, ...] = ()
    require_continuity: bool = False

    def __post_init__(self) -> None:
        """
        校验能力请求并规范化字段集合。

        输入参数来自 dataclass 字段；本方法无返回值，会在字段非法时抛出 ValueError。
        """
        capability_id = self.capability_id.strip()
        if not capability_id:
            raise ValueError("capability_id 不能为空")
        object.__setattr__(self, "capability_id", capability_id)
        if self.semantic_class is not None:
            semantic_class = self.semantic_class.strip()
            if not semantic_class:
                raise ValueError("semantic_class 不能是空字符串")
            object.__setattr__(self, "semantic_class", semantic_class)
        mode = self.mode.strip().lower()
        if mode not in _REQUEST_MODES:
            raise ValueError(f"mode 必须为 live、backtest 或 replay，实际为: {self.mode!r}")
        object.__setattr__(self, "mode", mode)
        raw_time_domain = self.time_domain
        time_domain = (
            _infer_time_domain(capability_id)
            if raw_time_domain is None
            else str(raw_time_domain).strip().lower()
        )
        if time_domain not in _REQUEST_TIME_DOMAINS:
            raise ValueError(
                "time_domain 必须为 realtime、historical、as_of 或 current，" f"实际为: {raw_time_domain!r}"
            )
        object.__setattr__(self, "time_domain", time_domain)
        if self.as_of is not None:
            as_of = (
                self.as_of.isoformat()
                if hasattr(self.as_of, "isoformat")
                else str(self.as_of).strip()
            )
            if not as_of:
                raise ValueError("as_of 不能是空字符串")
            object.__setattr__(self, "as_of", as_of)
        for field_name in ("market", "asset_type", "frequency", "adjustment"):
            value = getattr(self, field_name)
            if value is None:
                continue
            normalized = str(value).strip()
            if not normalized:
                raise ValueError(f"{field_name} 不能是空字符串")
            object.__setattr__(self, field_name, normalized)
        object.__setattr__(self, "fields", _normalize_strings(self.fields, "fields"))


@dataclass(frozen=True)
class CapabilityDeclaration:
    """声明某个 Provider 对一个原子能力的静态支持和动态就绪状态。"""

    capability_id: str
    semantic_class: str
    support: CapabilitySupport
    readiness: CapabilityReadiness = CapabilityReadiness.UNAVAILABLE
    time_domain: Optional[str] = None
    runtime_modes: Tuple[str, ...] = ("live", "backtest", "replay")
    markets: Tuple[str, ...] = ()
    asset_types: Tuple[str, ...] = ()
    frequencies: Tuple[str, ...] = ()
    adjustments: Tuple[str, ...] = ()
    fields: Tuple[str, ...] = ()
    field_set_version: Optional[str] = None
    max_staleness_ms: Optional[int] = None
    continuous: bool = False
    reason: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """
        校验声明的一致性并冻结所有集合和元数据。

        输入参数来自 dataclass 字段；本方法无返回值。静态 unsupported 不能同时标记 ready。
        """
        capability_id = self.capability_id.strip()
        semantic_class = self.semantic_class.strip()
        time_domain = (
            _infer_time_domain(capability_id)
            if self.time_domain is None
            else str(self.time_domain).strip().lower()
        )
        if not capability_id:
            raise ValueError("capability_id 不能为空")
        if not semantic_class:
            raise ValueError("semantic_class 不能为空")
        if time_domain not in _DECLARATION_TIME_DOMAINS:
            raise ValueError(
                "time_domain 必须为 realtime、historical、as_of、current 或 any，"
                f"实际为: {self.time_domain!r}"
            )
        try:
            support = CapabilitySupport(self.support)
            readiness = CapabilityReadiness(self.readiness)
        except (TypeError, ValueError) as exc:
            raise ValueError("support 或 readiness 包含未知枚举值") from exc
        if support is CapabilitySupport.UNSUPPORTED and readiness is CapabilityReadiness.READY:
            raise ValueError("unsupported capability 不能标记为 ready")
        if self.max_staleness_ms is not None and self.max_staleness_ms < 0:
            raise ValueError("max_staleness_ms 不能小于 0")
        object.__setattr__(self, "capability_id", capability_id)
        object.__setattr__(self, "semantic_class", semantic_class)
        object.__setattr__(self, "time_domain", time_domain)
        object.__setattr__(self, "runtime_modes", _normalize_runtime_modes(self.runtime_modes))
        object.__setattr__(self, "support", support)
        object.__setattr__(self, "readiness", readiness)
        object.__setattr__(self, "markets", _normalize_strings(self.markets, "markets"))
        object.__setattr__(self, "asset_types", _normalize_strings(self.asset_types, "asset_types"))
        object.__setattr__(self, "frequencies", _normalize_strings(self.frequencies, "frequencies"))
        object.__setattr__(self, "adjustments", _normalize_strings(self.adjustments, "adjustments"))
        object.__setattr__(self, "fields", _normalize_strings(self.fields, "fields"))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def matches(self, request: CapabilityRequest) -> Tuple[bool, str]:
        """
        判断声明的静态作用域是否精确覆盖请求。

        Args:
            request: 包含市场、资产、频率、复权、字段和连续性要求的精确请求。

        Returns:
            Tuple[bool, str]: 第一项表示是否匹配，第二项说明匹配或拒绝原因。
        """
        if request.capability_id != self.capability_id:
            return False, "capability_id_mismatch"
        if request.semantic_class and request.semantic_class != self.semantic_class:
            return False, "semantic_class_mismatch"
        if request.mode not in self.runtime_modes:
            return False, "runtime_mode_unsupported"
        if self.time_domain != "any" and request.time_domain != self.time_domain:
            return False, "time_domain_unsupported"
        if request.market and self.markets and request.market not in self.markets:
            return False, "market_unsupported"
        if request.asset_type and self.asset_types and request.asset_type not in self.asset_types:
            return False, "asset_type_unsupported"
        if request.frequency and self.frequencies and request.frequency not in self.frequencies:
            return False, "frequency_unsupported"
        if request.adjustment and self.adjustments and request.adjustment not in self.adjustments:
            return False, "adjustment_unsupported"
        if request.fields and self.fields and not set(request.fields).issubset(self.fields):
            return False, "field_set_unsupported"
        if request.require_continuity and not self.continuous:
            return False, "continuity_unsupported"
        return True, "matched"


@dataclass(frozen=True)
class CapabilityManifest:
    """保存单个 Provider 的版本化原子能力清单和运行位置。"""

    provider: str
    manifest_version: str
    location: ProviderLocation
    capabilities: Mapping[str, CapabilityDeclaration]
    provider_version: Optional[str] = None
    build_id: Optional[str] = None

    def __post_init__(self) -> None:
        """
        校验 Provider 身份、版本和 capability 键值一致性并冻结映射。

        输入参数来自 dataclass 字段；本方法无返回值，声明键不一致时抛出 ValueError。
        """
        provider = self.provider.strip()
        manifest_version = self.manifest_version.strip()
        if not provider:
            raise ValueError("provider 不能为空")
        if not manifest_version:
            raise ValueError("manifest_version 不能为空")
        try:
            location = ProviderLocation(self.location)
        except (TypeError, ValueError) as exc:
            raise ValueError("location 必须为 local 或 remote") from exc
        for field_name in ("provider_version", "build_id"):
            value = getattr(self, field_name)
            if value is not None and not str(value).strip():
                raise ValueError(f"{field_name} 不能是空字符串")
        copied: Dict[str, CapabilityDeclaration] = {}
        for capability_id, declaration in self.capabilities.items():
            if capability_id != declaration.capability_id:
                raise ValueError(
                    f"capabilities 键与声明不一致: key={capability_id}, "
                    f"declaration={declaration.capability_id}"
                )
            if capability_id in copied:
                raise ValueError(f"重复 capability_id: {capability_id}")
            copied[capability_id] = declaration
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "manifest_version", manifest_version)
        object.__setattr__(self, "location", location)
        if self.provider_version is not None:
            object.__setattr__(self, "provider_version", str(self.provider_version).strip())
        if self.build_id is not None:
            object.__setattr__(self, "build_id", str(self.build_id).strip())
        object.__setattr__(self, "capabilities", MappingProxyType(copied))

    def get(self, capability_id: str) -> Optional[CapabilityDeclaration]:
        """
        按原子能力 ID 读取声明。

        Args:
            capability_id: 需要读取的原子能力 ID。

        Returns:
            Optional[CapabilityDeclaration]: 已声明时返回声明，否则返回 None。
        """
        return self.capabilities.get(capability_id)

    def with_readiness(
        self,
        capability_id: str,
        readiness: CapabilityReadiness,
        reason: Optional[str] = None,
    ) -> "CapabilityManifest":
        """
        复制清单并只更新一个原子能力的动态 readiness。

        Args:
            capability_id: 需要更新的原子能力 ID。
            readiness: 新的动态就绪状态。
            reason: 可选的状态原因。

        Returns:
            CapabilityManifest: 保持 Provider 和清单版本不变的新清单。

        Raises:
            KeyError: capability_id 尚未声明时抛出。
        """
        declaration = self.capabilities.get(capability_id)
        if declaration is None:
            raise KeyError(capability_id)
        capabilities = dict(self.capabilities)
        capabilities[capability_id] = replace(declaration, readiness=readiness, reason=reason)
        return replace(self, capabilities=capabilities)

    def with_supported_readiness(
        self,
        readiness: CapabilityReadiness,
        reason: Optional[str] = None,
    ) -> "CapabilityManifest":
        """
        复制清单并批量更新全部非 unsupported 能力的 readiness。

        Args:
            readiness: 新的动态就绪状态。
            reason: 可选的统一状态原因。

        Returns:
            CapabilityManifest: 更新后的不可变清单。
        """
        capabilities = {
            capability_id: (
                declaration
                if declaration.support is CapabilitySupport.UNSUPPORTED
                else replace(declaration, readiness=readiness, reason=reason)
            )
            for capability_id, declaration in self.capabilities.items()
        }
        return replace(self, capabilities=capabilities)


@dataclass(frozen=True)
class RouteRule:
    """定义一个原子能力的唯一 primary 和显式有序 fallback。"""

    capability_id: str
    primary: str
    fallbacks: Tuple[str, ...] = ()
    rule_id: str = "default"

    def __post_init__(self) -> None:
        """
        校验路由规则并冻结 Provider 顺序。

        输入参数来自 dataclass 字段；本方法无返回值。重复或循环 Provider 会被拒绝。
        """
        capability_id = self.capability_id.strip()
        primary = self.primary.strip()
        rule_id = self.rule_id.strip()
        if not capability_id or not primary or not rule_id:
            raise ValueError("capability_id、primary 和 rule_id 均不能为空")
        normalized = tuple(str(item).strip() for item in self.fallbacks)
        if any(not item for item in normalized):
            raise ValueError("fallbacks 不能包含空 Provider")
        if primary in normalized:
            raise ValueError("primary 不能同时出现在 fallbacks")
        if len(set(normalized)) != len(normalized):
            raise ValueError("fallbacks 不能重复")
        object.__setattr__(self, "capability_id", capability_id)
        object.__setattr__(self, "primary", primary)
        object.__setattr__(self, "fallbacks", normalized)
        object.__setattr__(self, "rule_id", rule_id)


@dataclass(frozen=True)
class RouteDecision:
    """记录调用前固定的唯一能力 owner 和完整路由来源证明。"""

    capability_id: str
    provider: str
    location: ProviderLocation
    semantic_class: str
    manifest_version: str
    rule_id: str
    support: CapabilitySupport
    readiness: CapabilityReadiness
    reason: str
    fallback_from: Tuple[str, ...] = ()
    provider_version: Optional[str] = None
    build_id: Optional[str] = None
    time_domain: str = "as_of"
    runtime_modes: Tuple[str, ...] = ()
    request_mode: str = "live"
    request_time_domain: str = "as_of"
    request_as_of: Optional[str] = None
    request_market: Optional[str] = None
    request_asset_type: Optional[str] = None
    request_frequency: Optional[str] = None
    request_adjustment: Optional[str] = None
    request_fields: Tuple[str, ...] = ()
    request_require_continuity: bool = False


_PROFILE_DEFAULTS: Mapping[
    StrategyCapabilityProfile, Tuple[Tuple[str, ...], Tuple[str, ...]]
] = MappingProxyType(
    {
        StrategyCapabilityProfile.EXECUTION_ONLY: (
            ("broker.query",),
            ("broker.order.limit", "broker.cancel", "realtime.snapshot.l1"),
        ),
        StrategyCapabilityProfile.REALTIME_MICROSTRUCTURE_SIGNAL: (
            (
                "realtime.snapshot.l2",
                "realtime.stream.transaction",
                "realtime.stream.order_detail",
                "calendar.trade_days",
            ),
            ("reference.security_master", "realtime.stream.iopv"),
        ),
        StrategyCapabilityProfile.RESEARCH_LIVE: (
            (
                "realtime.snapshot.l1",
                "history.bars",
                "calendar.trade_days",
                "reference.security_master",
            ),
            (
                "history.ticks",
                "reference.corporate_actions",
                "fundamentals",
                "classification.industry",
                "classification.concept",
            ),
        ),
    }
)


@dataclass(frozen=True)
class StrategyCapabilityRequirements:
    """描述一个策略画像真正必需和可选的最小原子能力集合。"""

    profile: StrategyCapabilityProfile
    required: Tuple[str, ...]
    optional: Tuple[str, ...] = ()
    schema_version: str = "1"

    def __post_init__(self) -> None:
        """
        规范化策略能力集合并拒绝 required/optional 重叠。

        输入参数来自 dataclass 字段；本方法无返回值，集合重叠或版本为空时抛出 ValueError。
        """
        schema_version = self.schema_version.strip()
        if not schema_version:
            raise ValueError("schema_version 不能为空")
        required = _normalize_strings(self.required, "required")
        optional = _normalize_strings(self.optional, "optional")
        overlap = set(required).intersection(optional)
        if overlap:
            raise ValueError(f"required 与 optional 不能重叠: {sorted(overlap)}")
        object.__setattr__(self, "required", required)
        object.__setattr__(self, "optional", optional)
        object.__setattr__(self, "schema_version", schema_version)

    @classmethod
    def for_profile(
        cls,
        profile: StrategyCapabilityProfile,
        add_required: Sequence[str] = (),
        add_optional: Sequence[str] = (),
        remove_required: Sequence[str] = (),
        schema_version: str = "1",
    ) -> "StrategyCapabilityRequirements":
        """
        从预置画像创建可收窄、可扩展的策略能力要求。

        Args:
            profile: execution_only、realtime_microstructure_signal 或 research_live。
            add_required: 在画像基础上新增的必需原子能力。
            add_optional: 在画像基础上新增的可选原子能力。
            remove_required: 调用方明确从预置画像移除的必需能力。
            schema_version: 能力声明格式版本。

        Returns:
            StrategyCapabilityRequirements: 规范化后的不可变能力要求。
        """
        base_required, base_optional = _PROFILE_DEFAULTS[profile]
        required = set(base_required)
        required.difference_update(_normalize_strings(remove_required, "remove_required"))
        required.update(_normalize_strings(add_required, "add_required"))
        optional = set(base_optional)
        optional.update(_normalize_strings(add_optional, "add_optional"))
        optional.difference_update(required)
        return cls(
            profile=profile,
            required=tuple(required),
            optional=tuple(optional),
            schema_version=schema_version,
        )


@dataclass(frozen=True)
class StrategyCapabilityPreflight:
    """保存策略必需能力的固定路由，以及可选能力未就绪的诊断。"""

    profile: StrategyCapabilityProfile
    required_decisions: Mapping[str, RouteDecision]
    optional_decisions: Mapping[str, RouteDecision]
    optional_errors: Mapping[str, DataCapabilityError]

    def __post_init__(self) -> None:
        """
        冻结预检结果映射，防止启动后无声改变 RouteDecision。

        输入参数来自 dataclass 字段；本方法无返回值。
        """
        object.__setattr__(
            self, "required_decisions", MappingProxyType(dict(self.required_decisions))
        )
        object.__setattr__(
            self, "optional_decisions", MappingProxyType(dict(self.optional_decisions))
        )
        object.__setattr__(self, "optional_errors", MappingProxyType(dict(self.optional_errors)))


class DataSourceRouter:
    """按显式路由规则为每次原子能力请求固定唯一 owner。"""

    def __init__(self) -> None:
        """
        初始化空 Router。

        Router 初始不含 Provider 或规则；调用方必须先注册 manifest/owner 并设置显式路由。
        """
        self._lock = RLock()
        self._providers: Dict[str, Tuple[CapabilityManifest, Any]] = {}
        self._routes: Dict[str, RouteRule] = {}

    def register_provider(
        self, manifest: CapabilityManifest, owner: Any, replace_existing: bool = False
    ) -> None:
        """
        注册 Provider 能力清单和实际调用对象。

        Args:
            manifest: Provider 的版本化能力清单。
            owner: RouteDecision 固定后实际接收方法调用的对象。
            replace_existing: 是否允许替换同名 Provider。

        Raises:
            DataCapabilityRouteError: Provider 已存在且未允许替换时抛出。
        """
        with self._lock:
            if manifest.provider in self._providers and not replace_existing:
                raise DataCapabilityRouteError(f"Provider 已注册: {manifest.provider}")
            self._providers[manifest.provider] = (manifest, owner)

    def update_manifest(self, manifest: CapabilityManifest) -> None:
        """
        原子替换已注册 Provider 的运行时能力清单。

        Args:
            manifest: Provider 名称必须与已有注册一致的新清单。

        Raises:
            DataCapabilityRouteError: Provider 尚未注册时抛出。
        """
        with self._lock:
            registration = self._providers.get(manifest.provider)
            if registration is None:
                raise DataCapabilityRouteError(f"Provider 尚未注册: {manifest.provider}")
            self._providers[manifest.provider] = (manifest, registration[1])

    def set_route(self, rule: RouteRule) -> None:
        """
        设置并验证一个原子能力的显式唯一路由规则。

        Args:
            rule: 包含 primary、同语义 fallbacks 和 rule_id 的规则。

        Raises:
            DataCapabilityRouteError: Provider/声明缺失或 semantic class 不一致时抛出。
        """
        with self._lock:
            providers = (rule.primary,) + rule.fallbacks
            declarations = []
            for provider in providers:
                registration = self._providers.get(provider)
                if registration is None:
                    raise DataCapabilityRouteError(f"路由引用未注册 Provider: {provider}")
                declaration = registration[0].get(rule.capability_id)
                if declaration is None:
                    raise DataCapabilityRouteError(
                        f"Provider 未显式声明 capability: provider={provider}, "
                        f"capability_id={rule.capability_id}"
                    )
                declarations.append(declaration)
            semantic_classes = {item.semantic_class for item in declarations}
            if len(semantic_classes) != 1:
                raise DataCapabilityRouteError(
                    f"fallback semantic class 不一致: capability_id={rule.capability_id}, "
                    f"semantic_classes={sorted(semantic_classes)}"
                )
            self._routes[rule.capability_id] = rule

    def resolve(self, request: CapabilityRequest) -> RouteDecision:
        """
        在调用前按 support、scope 和 readiness 固定唯一 RouteDecision。

        Args:
            request: 当前调用的精确原子能力及语义约束。

        Returns:
            RouteDecision: 可在本次调用和诊断中复用的不可变路由决定。

        Raises:
            DataCapabilityUnavailableError: 无规则或全部候选静态 unsupported 时抛出。
            DataCapabilityNotReadyError: 候选静态支持但当前未 ready 时抛出。
            DataCapabilityRouteError: 路由配置或 semantic class 不一致时抛出。
        """
        with self._lock:
            rule = self._routes.get(request.capability_id)
            if rule is None:
                raise DataCapabilityUnavailableError(request.capability_id, "route_not_configured")
            providers = (rule.primary,) + rule.fallbacks
            primary_registration = self._providers.get(rule.primary)
            if primary_registration is None:
                raise DataCapabilityRouteError(f"路由引用未注册 Provider: {rule.primary}")
            primary_declaration = primary_registration[0].get(request.capability_id)
            if primary_declaration is None:
                raise DataCapabilityRouteError(
                    f"Provider 未显式声明 capability: provider={rule.primary}, "
                    f"capability_id={request.capability_id}"
                )
            route_semantic_class = primary_declaration.semantic_class
            for provider in providers:
                validation_registration = self._providers.get(provider)
                if validation_registration is None:
                    raise DataCapabilityRouteError(f"路由引用未注册 Provider: {provider}")
                validation_declaration = validation_registration[0].get(request.capability_id)
                if validation_declaration is None:
                    raise DataCapabilityRouteError(
                        f"Provider 未显式声明 capability: provider={provider}, "
                        f"capability_id={request.capability_id}"
                    )
                if validation_declaration.semantic_class != route_semantic_class:
                    raise DataCapabilityRouteError(
                        "运行时 manifest 与路由 semantic class 不一致: "
                        f"provider={provider}, declared={validation_declaration.semantic_class}, "
                        f"route={route_semantic_class}"
                    )
            unsupported: List[str] = []
            for provider in providers:
                registration = self._providers.get(provider)
                if registration is None:
                    raise DataCapabilityRouteError(f"路由引用未注册 Provider: {provider}")
                manifest, _owner = registration
                declaration = manifest.get(request.capability_id)
                if declaration is None:
                    raise DataCapabilityRouteError(
                        f"Provider 未显式声明 capability: provider={provider}, "
                        f"capability_id={request.capability_id}"
                    )
                if request.semantic_class and request.semantic_class != declaration.semantic_class:
                    raise DataCapabilityRouteError(
                        f"请求 semantic class 不一致: requested={request.semantic_class}, "
                        f"declared={declaration.semantic_class}"
                    )
                matches, match_reason = declaration.matches(request)
                if declaration.support is CapabilitySupport.UNSUPPORTED or not matches:
                    unsupported.append(provider)
                    continue
                if declaration.readiness is not CapabilityReadiness.READY:
                    raise DataCapabilityNotReadyError(
                        capability_id=request.capability_id,
                        provider=provider,
                        readiness=declaration.readiness,
                        reason=declaration.reason or match_reason,
                    )
                is_fallback = provider != rule.primary
                return RouteDecision(
                    capability_id=request.capability_id,
                    provider=provider,
                    location=manifest.location,
                    semantic_class=declaration.semantic_class,
                    manifest_version=manifest.manifest_version,
                    rule_id=rule.rule_id,
                    support=declaration.support,
                    readiness=declaration.readiness,
                    reason=(
                        "fallback_after_explicit_unsupported" if is_fallback else "primary_ready"
                    ),
                    fallback_from=tuple(unsupported),
                    provider_version=manifest.provider_version,
                    build_id=manifest.build_id,
                    time_domain=str(declaration.time_domain),
                    runtime_modes=declaration.runtime_modes,
                    request_mode=request.mode,
                    request_time_domain=str(request.time_domain),
                    request_as_of=request.as_of,
                    request_market=request.market,
                    request_asset_type=request.asset_type,
                    request_frequency=request.frequency,
                    request_adjustment=request.adjustment,
                    request_fields=request.fields,
                    request_require_continuity=request.require_continuity,
                )
            raise DataCapabilityUnavailableError(
                request.capability_id,
                f"all_candidates_explicitly_unsupported:{','.join(unsupported)}",
            )

    def execute(
        self,
        request: CapabilityRequest,
        operation: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        固定一次路由并且只调用该 owner，运行时错误绝不切换 Provider。

        Args:
            request: 当前调用的精确原子能力请求。
            operation: 在唯一 owner 上调用的方法名。
            *args: 传给 Provider 方法的位置参数。
            **kwargs: 传给 Provider 方法的关键字参数。

        Returns:
            Any: 唯一 owner 方法的原始返回值，包括空集合或 None。

        Raises:
            DataCapabilityExecutionError: owner 方法缺失或执行抛错时包装后抛出。
            DataCapabilityError: 路由或 readiness 预检失败时直接抛出。
        """
        decision = self.resolve(request)
        with self._lock:
            owner = self._providers[decision.provider][1]
        try:
            target = getattr(owner, operation)
            return target(*args, **kwargs)
        except Exception as exc:
            raise DataCapabilityExecutionError(decision, operation, exc) from exc

    def preflight(
        self, requirements: StrategyCapabilityRequirements
    ) -> StrategyCapabilityPreflight:
        """
        为策略必需能力固定路由，并把可选能力失败保留为诊断而不阻塞。

        Args:
            requirements: 三类画像之一及其收窄/扩展后的原子能力集合。

        Returns:
            StrategyCapabilityPreflight: 必需和可选能力的不可变预检结果。

        Raises:
            DataCapabilityError: 任一 required capability 无法解析或未 ready 时抛出。
        """
        required_decisions = {
            capability_id: self.resolve(CapabilityRequest(capability_id=capability_id))
            for capability_id in requirements.required
        }
        optional_decisions: Dict[str, RouteDecision] = {}
        optional_errors: Dict[str, DataCapabilityError] = {}
        for capability_id in requirements.optional:
            try:
                optional_decisions[capability_id] = self.resolve(
                    CapabilityRequest(capability_id=capability_id)
                )
            except DataCapabilityError as exc:
                optional_errors[capability_id] = exc
        return StrategyCapabilityPreflight(
            profile=requirements.profile,
            required_decisions=required_decisions,
            optional_decisions=optional_decisions,
            optional_errors=optional_errors,
        )


__all__ = [
    "CapabilityDeclaration",
    "CapabilityManifest",
    "CapabilityReadiness",
    "CapabilityRequest",
    "CapabilitySupport",
    "DataCapabilityError",
    "DataCapabilityExecutionError",
    "DataCapabilityNotReadyError",
    "DataCapabilityRouteError",
    "DataCapabilityUnavailableError",
    "DataSourceRouter",
    "ProviderLocation",
    "RouteDecision",
    "RouteRule",
    "StrategyCapabilityPreflight",
    "StrategyCapabilityProfile",
    "StrategyCapabilityRequirements",
]
