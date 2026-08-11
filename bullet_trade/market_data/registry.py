"""
作者: BruceLee

文件职责: 提供实时行情 Feed 的延迟工厂注册、查询、创建和受控未知名称错误。
主要输入: 规范化 Feed 名称、无副作用 factory、公开 metadata 和创建参数。
主要输出: RealtimeMarketDataFeed 实例、已注册名称及不可变注册信息。
上游关系: 由内置 backend、未来 Huaxin integration 或应用启动代码显式注册工厂。
下游关系: 供 LiveEngine、CLI 配置解析和测试按名称创建 Feed，避免硬编码分支。
关键配置约定: 注册本身不得加载 SDK、连接网络或创建 Feed；只有 create 调用 factory。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from types import MappingProxyType
from typing import Any, Callable, Dict, Mapping, Tuple

from .feed import RealtimeMarketDataFeed

FeedFactory = Callable[..., RealtimeMarketDataFeed]


class MarketDataFeedRegistryError(RuntimeError):
    """实时行情 Feed 注册表操作失败的公共基类。"""


class MarketDataFeedAlreadyRegisteredError(MarketDataFeedRegistryError):
    """表示同名 Feed 已注册且调用方未明确允许替换。"""


class UnknownMarketDataFeedError(MarketDataFeedRegistryError):
    """表示请求创建或读取的 Feed 名称未注册。"""


def _normalize_name(name: str) -> str:
    """
    规范化 Feed 名称。

    Args:
        name: 配置或注册方提供的名称。

    Returns:
        str: 去除首尾空格并转换为小写的名称。

    Raises:
        ValueError: 名称为空时抛出。
    """
    normalized = name.strip().lower()
    if not normalized:
        raise ValueError("Feed 名称不能为空")
    return normalized


@dataclass(frozen=True)
class MarketDataFeedRegistration:
    """保存一个延迟 Feed factory 及其无敏感公开元数据。"""

    name: str
    factory: FeedFactory
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """
        规范化名称、校验 factory 并冻结 metadata。

        输入参数来自 dataclass 字段；本方法无返回值，factory 不可调用时抛出 TypeError。
        """
        if not callable(self.factory):
            raise TypeError("Feed factory 必须可调用")
        object.__setattr__(self, "name", _normalize_name(self.name))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


class MarketDataFeedRegistry:
    """线程安全保存 Feed 延迟工厂，注册或列举名称时不触发 backend 初始化。"""

    def __init__(self) -> None:
        """
        初始化空注册表。

        Returns:
            None: 注册表初始不包含任何工厂。
        """
        self._lock = RLock()
        self._registrations: Dict[str, MarketDataFeedRegistration] = {}

    def register(
        self,
        name: str,
        factory: FeedFactory,
        metadata: Mapping[str, Any] = MappingProxyType({}),
        replace_existing: bool = False,
    ) -> None:
        """
        注册一个不立即执行的 Feed factory。

        Args:
            name: 配置使用的稳定 Feed 名称。
            factory: create 时才调用的工厂。
            metadata: 可安全展示的 backend 元数据。
            replace_existing: 是否明确允许替换同名注册。

        Returns:
            None: 注册完成后返回。

        Raises:
            MarketDataFeedAlreadyRegisteredError: 同名已存在且不允许替换时抛出。
        """
        registration = MarketDataFeedRegistration(name=name, factory=factory, metadata=metadata)
        with self._lock:
            if registration.name in self._registrations and not replace_existing:
                raise MarketDataFeedAlreadyRegisteredError(
                    f"MARKET_DATA_FEED_ALREADY_REGISTERED: {registration.name}"
                )
            self._registrations[registration.name] = registration

    def unregister(self, name: str) -> MarketDataFeedRegistration:
        """
        移除并返回一个已注册 Feed，不创建实例。

        Args:
            name: 需要移除的 Feed 名称。

        Returns:
            MarketDataFeedRegistration: 被移除的不可变注册信息。

        Raises:
            UnknownMarketDataFeedError: 名称不存在时抛出。
        """
        normalized = _normalize_name(name)
        with self._lock:
            try:
                return self._registrations.pop(normalized)
            except KeyError as exc:
                raise UnknownMarketDataFeedError(f"UNKNOWN_MARKET_DATA_FEED: {normalized}") from exc

    def get(self, name: str) -> MarketDataFeedRegistration:
        """
        读取一个不可变注册信息，不执行 factory。

        Args:
            name: 需要读取的 Feed 名称。

        Returns:
            MarketDataFeedRegistration: 对应的延迟工厂与元数据。

        Raises:
            UnknownMarketDataFeedError: 名称不存在时抛出。
        """
        normalized = _normalize_name(name)
        with self._lock:
            registration = self._registrations.get(normalized)
            if registration is None:
                raise UnknownMarketDataFeedError(f"UNKNOWN_MARKET_DATA_FEED: {normalized}")
            return registration

    def create(self, name: str, *args: Any, **kwargs: Any) -> RealtimeMarketDataFeed:
        """
        调用已注册 factory 创建一个实时 Feed。

        Args:
            name: 已注册 Feed 名称。
            *args: 传给 factory 的位置参数。
            **kwargs: 传给 factory 的关键字参数。

        Returns:
            RealtimeMarketDataFeed: factory 创建的独立实例。

        Raises:
            UnknownMarketDataFeedError: 名称不存在时抛出。
            TypeError: factory 返回值不满足 RealtimeMarketDataFeed 契约时抛出。
        """
        registration = self.get(name)
        feed = registration.factory(*args, **kwargs)
        if not isinstance(feed, RealtimeMarketDataFeed):
            raise TypeError(
                f"Feed factory 返回了错误类型: name={registration.name}, " f"type={type(feed).__name__}"
            )
        return feed

    def names(self) -> Tuple[str, ...]:
        """
        返回所有已注册名称且不执行任何 factory。

        Returns:
            Tuple[str, ...]: 稳定排序的 Feed 名称。
        """
        with self._lock:
            return tuple(sorted(self._registrations))


default_market_data_feed_registry = MarketDataFeedRegistry()


__all__ = [
    "FeedFactory",
    "MarketDataFeedAlreadyRegisteredError",
    "MarketDataFeedRegistration",
    "MarketDataFeedRegistry",
    "MarketDataFeedRegistryError",
    "UnknownMarketDataFeedError",
    "default_market_data_feed_registry",
]
