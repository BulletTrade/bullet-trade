"""
作者: BruceLee

文件职责: 验证能力 support/readiness、唯一 owner、严格 fallback 和策略画像离线合同。
主要输入: 合成 CapabilityManifest、RouteRule、CapabilityRequest 与测试 owner。
主要输出: Router 决策、预检结果和具名失败断言。
上游关系: 覆盖 bullet_trade.market_data.capability 的公共接口。
下游关系: 为未来 LiveEngine 和 Huaxin 集成提供不换源的回归门禁。
关键配置约定: 全部测试离线执行，不加载 SDK、网络或真实账户。
"""

from threading import Event, Thread
from typing import Any, Mapping, Optional

import pytest

from bullet_trade.market_data import (
    CapabilityDeclaration,
    CapabilityManifest,
    CapabilityReadiness,
    CapabilityRequest,
    CapabilitySupport,
    DataCapabilityExecutionError,
    DataCapabilityNotReadyError,
    DataCapabilityRouteError,
    DataCapabilityUnavailableError,
    DataSourceRouter,
    ProviderLocation,
    RouteRule,
    StrategyCapabilityProfile,
    StrategyCapabilityRequirements,
)

pytestmark = pytest.mark.unit


class _Owner:
    """记录 Router 是否错误调用备用来源的最小测试 owner。"""

    def __init__(self, result: Any = None, error: Optional[BaseException] = None) -> None:
        """
        初始化测试 owner。

        Args:
            result: fetch 正常返回值。
            error: fetch 需要抛出的可选异常。
        """
        self.result = result
        self.error = error
        self.calls = 0

    def fetch(self) -> Any:
        """
        记录一次调用并返回或抛出预设结果。

        Returns:
            Any: 初始化时传入的业务结果。

        Raises:
            BaseException: 初始化时传入 error 时原样抛出。
        """
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.result


def _declaration(
    capability_id: str,
    support: Any = CapabilitySupport.SUPPORTED,
    readiness: Any = CapabilityReadiness.READY,
    semantic_class: str = "same-semantic",
    **kwargs: Any,
) -> CapabilityDeclaration:
    """
    构造测试用原子能力声明。

    Args:
        capability_id: 原子能力 ID。
        support: 静态支持状态。
        readiness: 动态就绪状态。
        semantic_class: 路由语义类。
        **kwargs: 传给 CapabilityDeclaration 的其他字段。

    Returns:
        CapabilityDeclaration: 可注册到测试 manifest 的声明。
    """
    return CapabilityDeclaration(
        capability_id=capability_id,
        semantic_class=semantic_class,
        support=support,
        readiness=readiness,
        **kwargs,
    )


def _manifest(
    provider: str,
    declarations: Mapping[str, CapabilityDeclaration],
    provider_version: Optional[str] = None,
    build_id: Optional[str] = None,
    location: Any = ProviderLocation.LOCAL,
) -> CapabilityManifest:
    """
    构造测试用本地 Provider manifest。

    Args:
        provider: Provider 名称。
        declarations: capability ID 到声明的映射。
        provider_version: 可选 Provider 版本。
        build_id: 可选构建标识。
        location: Provider 本地/远程位置枚举或 JSON 字符串。

    Returns:
        CapabilityManifest: 版本固定为测试值的清单。
    """
    return CapabilityManifest(
        provider=provider,
        manifest_version="test-v1",
        location=location,
        capabilities=declarations,
        provider_version=provider_version,
        build_id=build_id,
    )


def test_supported_but_not_ready_never_uses_fallback() -> None:
    """验证 primary 静态支持但断线时 fail closed，不切换 ready fallback。"""
    capability_id = "realtime.snapshot.l2"
    primary_owner = _Owner()
    fallback_owner = _Owner()
    router = DataSourceRouter()
    router.register_provider(
        _manifest(
            "primary",
            {capability_id: _declaration(capability_id, readiness=CapabilityReadiness.UNAVAILABLE)},
        ),
        primary_owner,
    )
    router.register_provider(
        _manifest("fallback", {capability_id: _declaration(capability_id)}), fallback_owner
    )
    router.set_route(RouteRule(capability_id, "primary", ("fallback",), "route-l2"))

    with pytest.raises(DataCapabilityNotReadyError) as exc_info:
        router.resolve(CapabilityRequest(capability_id))

    assert exc_info.value.provider == "primary"
    assert primary_owner.calls == 0
    assert fallback_owner.calls == 0


def test_explicit_unsupported_uses_same_semantic_fallback() -> None:
    """验证精确请求被 primary 显式 unsupported 后才选择同语义 fallback。"""
    capability_id = "history.bars"
    router = DataSourceRouter()
    router.register_provider(
        _manifest(
            "primary",
            {
                capability_id: _declaration(
                    capability_id,
                    support=CapabilitySupport.UNSUPPORTED,
                    readiness=CapabilityReadiness.UNAVAILABLE,
                )
            },
        ),
        _Owner(),
    )
    router.register_provider(
        _manifest("fallback", {capability_id: _declaration(capability_id)}), _Owner()
    )
    router.set_route(RouteRule(capability_id, "primary", ("fallback",), "bars-fallback"))

    decision = router.resolve(CapabilityRequest(capability_id))

    assert decision.provider == "fallback"
    assert decision.fallback_from == ("primary",)
    assert decision.reason == "fallback_after_explicit_unsupported"
    assert decision.rule_id == "bars-fallback"


def test_runtime_error_and_empty_result_never_hop_source() -> None:
    """验证运行时异常与真实空结果都不会触发备用 Provider。"""
    capability_id = "history.bars"
    primary_owner = _Owner(result=[])
    fallback_owner = _Owner(result=["wrong-source"])
    router = DataSourceRouter()
    declaration = _declaration(capability_id)
    router.register_provider(_manifest("primary", {capability_id: declaration}), primary_owner)
    router.register_provider(_manifest("fallback", {capability_id: declaration}), fallback_owner)
    router.set_route(RouteRule(capability_id, "primary", ("fallback",)))

    assert router.execute(CapabilityRequest(capability_id), "fetch") == []
    assert primary_owner.calls == 1
    assert fallback_owner.calls == 0

    primary_owner.error = TimeoutError("provider timeout")
    with pytest.raises(DataCapabilityExecutionError) as exc_info:
        router.execute(CapabilityRequest(capability_id), "fetch")
    assert isinstance(exc_info.value.cause, TimeoutError)
    assert primary_owner.calls == 2
    assert fallback_owner.calls == 0


def test_semantic_mismatch_is_rejected_at_config_and_after_manifest_update() -> None:
    """验证路由配置和运行时 manifest 漂移都不能跨越 semantic class。"""
    capability_id = "realtime.snapshot.l2"
    router = DataSourceRouter()
    router.register_provider(
        _manifest("primary", {capability_id: _declaration(capability_id, semantic_class="l2")}),
        _Owner(),
    )
    router.register_provider(
        _manifest("fallback", {capability_id: _declaration(capability_id, semantic_class="bars")}),
        _Owner(),
    )
    with pytest.raises(DataCapabilityRouteError):
        router.set_route(RouteRule(capability_id, "primary", ("fallback",)))

    router.update_manifest(
        _manifest("fallback", {capability_id: _declaration(capability_id, semantic_class="l2")})
    )
    router.set_route(RouteRule(capability_id, "primary", ("fallback",)))
    router.update_manifest(
        _manifest(
            "fallback", {capability_id: _declaration(capability_id, semantic_class="changed")}
        )
    )
    with pytest.raises(DataCapabilityRouteError):
        router.resolve(CapabilityRequest(capability_id))


def test_strategy_profiles_and_optional_preflight_are_role_scoped() -> None:
    """验证三画像不把无关能力混入 required，optional 缺失只形成诊断。"""
    execution = StrategyCapabilityRequirements.for_profile(StrategyCapabilityProfile.EXECUTION_ONLY)
    microstructure = StrategyCapabilityRequirements.for_profile(
        StrategyCapabilityProfile.REALTIME_MICROSTRUCTURE_SIGNAL
    )
    research = StrategyCapabilityRequirements.for_profile(StrategyCapabilityProfile.RESEARCH_LIVE)

    assert "history.bars" not in execution.required
    assert "realtime.stream.transaction" in microstructure.required
    assert "realtime.stream.order_detail" in microstructure.required
    assert "history.bars" in research.required
    assert "reference.security_master" in research.required

    router = DataSourceRouter()
    required_id = "broker.query"
    optional_id = "realtime.snapshot.l1"
    router.register_provider(
        _manifest(
            "owner",
            {
                required_id: _declaration(required_id),
                optional_id: _declaration(
                    optional_id,
                    support=CapabilitySupport.UNSUPPORTED,
                    readiness=CapabilityReadiness.UNAVAILABLE,
                ),
            },
        ),
        _Owner(),
    )
    router.set_route(RouteRule(required_id, "owner"))
    router.set_route(RouteRule(optional_id, "owner"))
    requirements = StrategyCapabilityRequirements(
        profile=StrategyCapabilityProfile.EXECUTION_ONLY,
        required=(required_id,),
        optional=(optional_id,),
    )

    preflight = router.preflight(requirements)

    assert preflight.required_decisions[required_id].provider == "owner"
    assert optional_id in preflight.optional_errors


def test_runtime_mode_and_time_domain_are_independent_and_fail_closed() -> None:
    """验证运行模式与数据时间域分别参与路由且未知值受控拒绝。"""
    capability_id = "history.bars"
    router = DataSourceRouter()
    router.register_provider(
        _manifest(
            "history",
            {
                capability_id: _declaration(
                    capability_id,
                    time_domain="historical",
                    runtime_modes=("backtest", "replay"),
                )
            },
        ),
        _Owner(),
    )
    router.set_route(RouteRule(capability_id, "history"))

    decision = router.resolve(
        CapabilityRequest(
            capability_id,
            mode="backtest",
            time_domain="historical",
            as_of="2026-08-08T15:00:00+08:00",
        )
    )

    assert decision.request_mode == "backtest"
    assert decision.runtime_modes == ("backtest", "replay")
    assert decision.time_domain == "historical"
    assert decision.request_time_domain == "historical"
    assert decision.request_as_of == "2026-08-08T15:00:00+08:00"
    with pytest.raises(DataCapabilityUnavailableError) as exc_info:
        router.resolve(CapabilityRequest(capability_id, mode="live"))
    assert "explicitly_unsupported" in str(exc_info.value)
    with pytest.raises(DataCapabilityUnavailableError):
        router.resolve(
            CapabilityRequest(
                capability_id,
                mode="backtest",
                time_domain="realtime",
            )
        )
    with pytest.raises(ValueError, match="live、backtest 或 replay"):
        CapabilityRequest(capability_id, mode="nonsense")
    with pytest.raises(ValueError, match="realtime、historical、as_of 或 current"):
        CapabilityRequest(capability_id, time_domain="unspecified")

    assert CapabilityRequest("history.bars").time_domain == "historical"
    assert CapabilityRequest("realtime.snapshot.l1").time_domain == "realtime"
    assert _declaration("custom.reference").time_domain == "as_of"


def test_route_decision_records_provider_and_request_scope_provenance() -> None:
    """验证决策携带 Provider 版本、构建号与精确请求作用域。"""
    capability_id = "realtime.snapshot.l2"
    declaration = _declaration(
        capability_id,
        time_domain="realtime",
        markets=("XSHG",),
        asset_types=("stock",),
        frequencies=("tick",),
        fields=("last_price", "bid_prices"),
        continuous=True,
    )
    router = DataSourceRouter()
    router.register_provider(
        _manifest(
            "huaxin",
            {capability_id: declaration},
            provider_version="4.0.8",
            build_id="bridge-test",
        ),
        _Owner(),
    )
    router.set_route(RouteRule(capability_id, "huaxin", rule_id="l2-live"))
    request = CapabilityRequest(
        capability_id,
        mode="live",
        market="XSHG",
        asset_type="stock",
        frequency="tick",
        fields=("bid_prices", "last_price"),
        require_continuity=True,
    )

    decision = router.resolve(request)

    assert decision.provider_version == "4.0.8"
    assert decision.build_id == "bridge-test"
    assert decision.location is ProviderLocation.LOCAL
    assert decision.manifest_version == "test-v1"
    assert decision.semantic_class == "same-semantic"
    assert decision.support is CapabilitySupport.SUPPORTED
    assert decision.readiness is CapabilityReadiness.READY
    assert decision.rule_id == "l2-live"
    assert decision.time_domain == "realtime"
    assert decision.request_time_domain == "realtime"
    assert decision.request_market == "XSHG"
    assert decision.request_asset_type == "stock"
    assert decision.request_frequency == "tick"
    assert decision.request_fields == ("bid_prices", "last_price")
    assert decision.request_require_continuity is True


def test_dynamic_manifest_supplier_rechecks_readiness_without_runtime_fallback() -> None:
    """
    验证每次 resolve 都读取显式 supplier，primary 运行时失效后绝不切备用来源。

    Returns:
        None: 首次 ready、随后 stale 且两个 owner 均未执行时返回。
    """
    capability_id = "realtime.snapshot.l1"
    current = {"manifest": _manifest("primary", {capability_id: _declaration(capability_id)})}
    primary = _Owner()
    fallback = _Owner()
    router = DataSourceRouter()
    router.register_provider(
        current["manifest"],
        primary,
        manifest_supplier=lambda: current["manifest"],
    )
    router.register_provider(
        _manifest("fallback", {capability_id: _declaration(capability_id)}),
        fallback,
    )
    router.set_route(RouteRule(capability_id, "primary", ("fallback",)))

    assert router.resolve(CapabilityRequest(capability_id)).provider == "primary"
    current["manifest"] = current["manifest"].with_readiness(
        capability_id,
        CapabilityReadiness.STALE,
        "stale_threshold_exceeded",
    )

    with pytest.raises(DataCapabilityNotReadyError) as exc_info:
        router.resolve(CapabilityRequest(capability_id))
    assert exc_info.value.provider == "primary"
    assert exc_info.value.readiness is CapabilityReadiness.STALE
    assert primary.calls == 0
    assert fallback.calls == 0


@pytest.mark.parametrize("failure", ["exception", "provider_mismatch"])
def test_dynamic_manifest_supplier_failure_is_fail_closed(failure: str) -> None:
    """
    验证 supplier 异常或 Provider 身份漂移都形成路由错误而不是静态 fail-open。

    Args:
        failure: 需要模拟的 supplier 失败类型。

    Returns:
        None: 两类失败均抛出 DataCapabilityRouteError 时返回。
    """
    capability_id = "realtime.snapshot.l1"
    registered = _manifest("primary", {capability_id: _declaration(capability_id)})

    def supplier() -> CapabilityManifest:
        """
        按参数返回错误身份 manifest 或抛出测试异常。

        Returns:
            CapabilityManifest: provider_mismatch 分支返回错误身份清单。

        Raises:
            RuntimeError: exception 分支模拟运行态 health 获取失败。
        """
        if failure == "exception":
            raise RuntimeError("health unavailable")
        return _manifest("other", {capability_id: _declaration(capability_id)})

    router = DataSourceRouter()
    router.register_provider(registered, _Owner(), manifest_supplier=supplier)
    router.set_route(RouteRule(capability_id, "primary"))

    with pytest.raises(DataCapabilityRouteError):
        router.resolve(CapabilityRequest(capability_id))


def test_unused_fallback_supplier_is_not_called_when_primary_is_ready() -> None:
    """
    验证 primary 已就绪时不会调用无关 fallback 的动态 health supplier。

    Returns:
        None: fallback supplier 即使会失败也不影响 primary 决策时返回。
    """
    capability_id = "realtime.snapshot.l1"
    router = DataSourceRouter()
    primary_manifest = _manifest("primary", {capability_id: _declaration(capability_id)})
    fallback_manifest = _manifest("fallback", {capability_id: _declaration(capability_id)})

    def failing_supplier() -> CapabilityManifest:
        """
        模拟不应在 primary ready 路径调用的 fallback health。

        Returns:
            CapabilityManifest: 本测试不会正常返回。

        Raises:
            RuntimeError: 一旦被错误调用就抛出。
        """
        raise RuntimeError("unused fallback unavailable")

    router.register_provider(primary_manifest, _Owner())
    router.register_provider(
        fallback_manifest,
        _Owner(),
        manifest_supplier=failing_supplier,
    )
    router.set_route(RouteRule(capability_id, "primary", ("fallback",)))

    assert router.resolve(CapabilityRequest(capability_id)).provider == "primary"


def test_dynamic_supplier_cannot_turn_static_support_into_runtime_fallback() -> None:
    """
    验证 supplier 只能更新 readiness，不能把 supported 改成 unsupported 触发换源。

    Returns:
        None: 静态 support 漂移形成 RouteError 且 fallback 未执行时返回。
    """
    capability_id = "realtime.snapshot.l1"
    primary_manifest = _manifest("primary", {capability_id: _declaration(capability_id)})
    drifted_manifest = _manifest(
        "primary",
        {
            capability_id: _declaration(
                capability_id,
                support=CapabilitySupport.UNSUPPORTED,
                readiness=CapabilityReadiness.UNAVAILABLE,
            )
        },
    )
    fallback = _Owner()
    router = DataSourceRouter()
    router.register_provider(
        primary_manifest,
        _Owner(),
        manifest_supplier=lambda: drifted_manifest,
    )
    router.register_provider(
        _manifest("fallback", {capability_id: _declaration(capability_id)}),
        fallback,
    )
    router.set_route(RouteRule(capability_id, "primary", ("fallback",)))

    with pytest.raises(DataCapabilityRouteError, match="静态能力合同"):
        router.resolve(CapabilityRequest(capability_id))
    assert fallback.calls == 0


def test_dynamic_supplier_cannot_promote_static_unsupported_capability() -> None:
    """
    验证 supplier 不能把静态 unsupported 反向提升为 supported/ready。

    Returns:
        None: 对称 support 漂移统一形成受控 RouteError 时返回。
    """
    capability_id = "realtime.snapshot.l1"
    static_manifest = _manifest(
        "primary",
        {
            capability_id: _declaration(
                capability_id,
                support=CapabilitySupport.UNSUPPORTED,
                readiness=CapabilityReadiness.UNAVAILABLE,
            )
        },
    )
    dynamic_manifest = _manifest(
        "primary",
        {capability_id: _declaration(capability_id)},
    )
    router = DataSourceRouter()
    router.register_provider(
        static_manifest,
        _Owner(),
        manifest_supplier=lambda: dynamic_manifest,
    )
    router.set_route(RouteRule(capability_id, "primary"))

    with pytest.raises(DataCapabilityRouteError, match="静态能力合同"):
        router.resolve(CapabilityRequest(capability_id))


def test_dynamic_manifest_supplier_runs_outside_router_global_lock() -> None:
    """
    验证慢 supplier 不持有 Router 全局锁，允许并行原子更新 manifest。

    Returns:
        None: updater 在 supplier 返回前完成且 resolve 正常固定来源时返回。
    """
    capability_id = "realtime.snapshot.l1"
    manifest = _manifest("primary", {capability_id: _declaration(capability_id)})
    supplier_entered = Event()
    manifest_updated = Event()
    router = DataSourceRouter()

    def supplier() -> CapabilityManifest:
        """
        等待另一线程完成 Router 更新后返回动态快照。

        Returns:
            CapabilityManifest: 与注册静态合同一致的 manifest。

        Raises:
            RuntimeError: Router 锁错误覆盖 supplier 调用时超时抛出。
        """
        supplier_entered.set()
        if not manifest_updated.wait(1):
            raise RuntimeError("router lock held during supplier")
        return manifest

    def update_manifest() -> None:
        """
        在 supplier 已开始后尝试获取 Router 锁并更新 manifest。

        Returns:
            None: 更新完成并释放等待者后返回。
        """
        assert supplier_entered.wait(1)
        router.update_manifest(manifest)
        manifest_updated.set()

    router.register_provider(manifest, _Owner(), manifest_supplier=supplier)
    router.set_route(RouteRule(capability_id, "primary"))
    updater = Thread(target=update_manifest)
    updater.start()

    decision = router.resolve(CapabilityRequest(capability_id))
    updater.join(2)

    assert updater.is_alive() is False
    assert decision.provider == "primary"


def test_market_and_asset_scope_selects_the_only_matching_owner() -> None:
    """验证不匹配的主来源不会截获另一市场和资产类型请求。"""
    capability_id = "history.bars"
    router = DataSourceRouter()
    router.register_provider(
        _manifest(
            "stock-sh",
            {
                capability_id: _declaration(
                    capability_id,
                    markets=("XSHG",),
                    asset_types=("stock",),
                )
            },
        ),
        _Owner(),
    )
    router.register_provider(
        _manifest(
            "fund-sz",
            {
                capability_id: _declaration(
                    capability_id,
                    markets=("XSHE",),
                    asset_types=("fund",),
                )
            },
        ),
        _Owner(),
    )
    router.set_route(RouteRule(capability_id, "stock-sh", ("fund-sz",)))

    decision = router.resolve(CapabilityRequest(capability_id, market="XSHE", asset_type="fund"))

    assert decision.provider == "fund-sz"
    assert decision.fallback_from == ("stock-sh",)
    assert decision.request_market == "XSHE"
    assert decision.request_asset_type == "fund"


def test_json_enum_strings_are_normalized_and_unknown_values_are_rejected() -> None:
    """验证 JSON manifest 常见字符串枚举被确定转换，未知值不延迟到 resolve 崩溃。"""
    capability_id = "realtime.snapshot.l1"
    declaration = _declaration(
        capability_id,
        support="supported",
        readiness="ready",
    )
    manifest = _manifest(
        "remote-feed",
        {capability_id: declaration},
        location="remote",
    )
    router = DataSourceRouter()
    router.register_provider(manifest, _Owner())
    router.set_route(RouteRule(capability_id, "remote-feed"))

    decision = router.resolve(CapabilityRequest(capability_id))

    assert decision.support is CapabilitySupport.SUPPORTED
    assert decision.readiness is CapabilityReadiness.READY
    assert decision.location is ProviderLocation.REMOTE
    with pytest.raises(ValueError, match="support 或 readiness"):
        _declaration(capability_id, support="mystery")
    with pytest.raises(ValueError, match="location 必须"):
        _manifest("bad", {capability_id: declaration}, location="somewhere")
