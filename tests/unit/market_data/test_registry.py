"""
作者: BruceLee

文件职责: 验证实时 Feed 注册表延迟工厂、名称规范化和受控失败合同。
主要输入: 计数 factory、MockRealtimeMarketDataFeed 和不同大小写名称。
主要输出: 注册名称、创建次数、重复注册和未知名称错误断言。
上游关系: 覆盖 bullet_trade.market_data.registry 的公共接口。
下游关系: 为未来 CLI/LiveEngine 无硬编码创建 backend 提供回归门禁。
关键配置约定: register/get/names 不能执行 factory 或加载任何 SDK。
"""

import pytest

from bullet_trade.market_data import (
    CapabilityManifest,
    MarketDataFeedAlreadyRegisteredError,
    MarketDataFeedRegistry,
    MockRealtimeMarketDataFeed,
    ProviderLocation,
    UnknownMarketDataFeedError,
)

pytestmark = pytest.mark.unit


class _CountingFactory:
    """记录注册表是否只在 create 时实例化 Feed。"""

    def __init__(self) -> None:
        """
        初始化调用计数。

        Returns:
            None: count 初始为零。
        """
        self.count = 0

    def __call__(self) -> MockRealtimeMarketDataFeed:
        """
        创建一个空能力 Mock Feed 并累计调用次数。

        Returns:
            MockRealtimeMarketDataFeed: 尚未 connect 的独立实例。
        """
        self.count += 1
        manifest = CapabilityManifest(
            provider="mock",
            manifest_version="registry-test-v1",
            location=ProviderLocation.LOCAL,
            capabilities={},
        )
        return MockRealtimeMarketDataFeed(manifest)


def test_registry_is_lazy_and_normalizes_names() -> None:
    """验证列举和读取不执行 factory，create 才创建实例。"""
    registry = MarketDataFeedRegistry()
    factory = _CountingFactory()

    registry.register(" Mock ", factory, metadata={"native": False})

    assert registry.names() == ("mock",)
    assert registry.get("MOCK").metadata["native"] is False
    assert factory.count == 0
    feed = registry.create("mock")
    assert isinstance(feed, MockRealtimeMarketDataFeed)
    assert factory.count == 1


def test_registry_duplicate_and_unknown_names_fail_explicitly() -> None:
    """验证重复注册和未知创建都使用具名受控错误。"""
    registry = MarketDataFeedRegistry()
    registry.register("mock", _CountingFactory())

    with pytest.raises(MarketDataFeedAlreadyRegisteredError):
        registry.register("MOCK", _CountingFactory())
    with pytest.raises(UnknownMarketDataFeedError):
        registry.create("missing")
    with pytest.raises(UnknownMarketDataFeedError):
        registry.unregister("missing")
