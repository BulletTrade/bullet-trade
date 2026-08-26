"""验证服务端 adapter 注册表的兼容性和冲突保护。"""

import pytest

from bullet_trade.server.adapters import REGISTRY, get_adapter, list_adapters, register_adapter


def _builder(config, router):
    """返回测试 builder 的输入，证明注册阶段不会执行它。

    Args:
        config: 测试服务配置。
        router: 测试账户路由器。

    Returns:
        由两个输入组成的元组。
    """

    return config, router


@pytest.fixture
def isolated_registry():
    """临时隔离全局 adapter 注册表并在测试后恢复。

    Yields:
        可直接修改的全局注册表。

    Side Effects:
        测试期间清空并在结束后恢复进程级 ``REGISTRY``。
    """

    snapshot = dict(REGISTRY)
    REGISTRY.clear()
    try:
        yield REGISTRY
    finally:
        REGISTRY.clear()
        REGISTRY.update(snapshot)


def test_register_adapter_supports_normalized_alias_without_execution(
    isolated_registry,
) -> None:
    """验证名称规范化、别名和延迟 builder 合同。"""

    del isolated_registry
    register_adapter("Custom", _builder, aliases=("custom-alias",))

    assert get_adapter(" CUSTOM-ALIAS ") is _builder
    assert list_adapters() == ("custom", "custom-alias")


def test_register_adapter_rejects_conflicting_builder(isolated_registry) -> None:
    """验证默认拒绝同一名称被不同 builder 静默覆盖。"""

    del isolated_registry
    register_adapter("custom", _builder)

    with pytest.raises(ValueError, match="已注册"):
        register_adapter("custom", lambda config, router: None)


def test_get_adapter_unknown_name_lists_available(isolated_registry) -> None:
    """验证未注册错误提供确定性的可用名称。"""

    del isolated_registry
    register_adapter("custom", _builder)

    with pytest.raises(KeyError, match="可用类型: custom"):
        get_adapter("missing")
