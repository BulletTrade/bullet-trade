"""
作者: BruceLee

文件职责: 为 BulletTrade 实盘引擎提供稳定、可扩展的券商构造注册表。
主要输入: 券商名称、环境配置映射和返回 ``BrokerBase`` 的构造函数。
主要输出: 已注册券商实例，或包含可用名称的确定性配置错误。
上游关系: ``LiveEngine`` 和未来服务端适配器通过本模块选择券商实现。
下游关系: QMT、远程 QMT、模拟器以及后续第一方华鑫适配器。
关键配置: 注册表本身不读取环境变量，也不会连接券商或加载厂商 SDK。
"""

from __future__ import annotations

from threading import RLock
from typing import Any, Callable, Dict, Mapping, Tuple

from .base import BrokerBase

BrokerBuilder = Callable[[Mapping[str, Any]], BrokerBase]


class BrokerRegistry:
    """维护规范化券商名称到构造函数的映射。

    核心协作对象是 ``LiveEngine`` 与各券商工厂。注册和读取由可重入锁保护，
    以避免测试、插件导入或服务启动期间发生部分可见的注册状态。
    """

    def __init__(self) -> None:
        """创建空注册表。

        Returns:
            None。

        Side Effects:
            创建仅属于当前实例的锁和构造函数映射，不执行任何外部 I/O。
        """

        self._builders: Dict[str, BrokerBuilder] = {}
        self._lock = RLock()

    @staticmethod
    def _normalize_name(name: str) -> str:
        """规范化券商注册名称。

        Args:
            name: 用户配置或代码传入的券商名称。

        Returns:
            去除首尾空白并转为小写的名称。

        Raises:
            ValueError: 名称为空或不是字符串时抛出。
        """

        if not isinstance(name, str) or not name.strip():
            raise ValueError("券商名称不能为空")
        return name.strip().lower()

    def register(
        self,
        name: str,
        builder: BrokerBuilder,
        *,
        aliases: Tuple[str, ...] = (),
        replace: bool = False,
    ) -> None:
        """注册券商构造函数及其别名。

        Args:
            name: 券商主名称。
            builder: 接收完整券商配置并返回 ``BrokerBase`` 的构造函数。
            aliases: 与主名称共享同一构造函数的别名。
            replace: 是否允许覆盖已有且不同的构造函数。

        Returns:
            None。

        Raises:
            TypeError: ``builder`` 不可调用时抛出。
            ValueError: 名称无效或试图覆盖已有注册且 ``replace`` 为 False 时抛出。

        Side Effects:
            原子更新当前进程内的注册表；不会创建券商实例。
        """

        if not callable(builder):
            raise TypeError("券商 builder 必须可调用")
        names = tuple(self._normalize_name(item) for item in (name, *aliases))
        if len(set(names)) != len(names):
            raise ValueError("券商主名称与别名不能重复")
        with self._lock:
            conflicts = [
                item
                for item in names
                if item in self._builders and self._builders[item] is not builder
            ]
            if conflicts and not replace:
                raise ValueError(f"券商名称已注册: {', '.join(sorted(conflicts))}")
            for item in names:
                self._builders[item] = builder

    def get(self, name: str) -> BrokerBuilder:
        """读取指定券商的构造函数。

        Args:
            name: 券商主名称或别名。

        Returns:
            已注册的构造函数。

        Raises:
            ValueError: 券商名称无效或尚未注册时抛出。
        """

        normalized = self._normalize_name(name)
        with self._lock:
            builder = self._builders.get(normalized)
            available = tuple(sorted(self._builders))
        if builder is None:
            choices = ", ".join(available) or "无"
            raise ValueError(f"未知券商类型: {normalized}；可用类型: {choices}")
        return builder

    def create(self, name: str, config: Mapping[str, Any]) -> BrokerBase:
        """用指定配置创建券商实例。

        Args:
            name: 券商主名称或别名。
            config: ``get_broker_config`` 产生的完整只读配置映射。

        Returns:
            构造完成但尚未连接的券商实例。

        Raises:
            TypeError: 配置不是映射或 builder 返回值不是 ``BrokerBase`` 时抛出。
            ValueError: 券商名称不存在或具体工厂拒绝配置时抛出。

        Side Effects:
            调用已注册 builder；注册表自身不连接网络，具体 builder 也应只构造对象。
        """

        if not isinstance(config, Mapping):
            raise TypeError("券商配置必须是映射")
        broker = self.get(name)(config)
        if not isinstance(broker, BrokerBase):
            raise TypeError(f"券商 builder 返回了无效类型: {type(broker).__name__}")
        return broker

    def names(self) -> Tuple[str, ...]:
        """返回当前所有已注册名称。

        Returns:
            按字典序排列的不可变名称元组。
        """

        with self._lock:
            return tuple(sorted(self._builders))


def _build_qmt(config: Mapping[str, Any]) -> BrokerBase:
    """根据完整配置构造本地 QMT 券商。

    Args:
        config: 完整券商配置，使用其中的 ``qmt`` 子映射。

    Returns:
        尚未连接的 ``QmtBroker``。

    Raises:
        RuntimeError: 未配置 QMT 账户时抛出。
    """

    from .qmt import QmtBroker

    qmt_config = dict(config.get("qmt") or {})
    account_id = qmt_config.get("account_id")
    if not account_id:
        raise RuntimeError("缺少 QMT_ACCOUNT_ID，请在 .env.live 中配置")
    return QmtBroker(
        account_id=account_id,
        account_type=qmt_config.get("account_type", "stock"),
        data_path=qmt_config.get("data_path"),
        session_id=qmt_config.get("session_id"),
        auto_subscribe=qmt_config.get("auto_subscribe"),
    )


def _build_remote_qmt(config: Mapping[str, Any]) -> BrokerBase:
    """根据完整配置构造远程 QMT 券商。

    Args:
        config: 完整券商配置，使用其中的 ``qmt-remote`` 子映射。

    Returns:
        尚未连接的 ``RemoteQmtBroker``。
    """

    from .qmt_remote import RemoteQmtBroker

    remote_config = dict(config.get("qmt-remote") or {})
    return RemoteQmtBroker(
        account_id=(
            remote_config.get("account_id") or remote_config.get("account_key") or "remote"
        ),
        account_type=remote_config.get("account_type", "stock"),
        config=remote_config,
    )


def _build_simulator(config: Mapping[str, Any]) -> BrokerBase:
    """根据完整配置构造内置模拟券商。

    Args:
        config: 完整券商配置，使用其中的 ``simulator`` 子映射。

    Returns:
        尚未连接的 ``SimulatorBroker``。
    """

    from .simulator import SimulatorBroker

    simulator_config = dict(config.get("simulator") or {})
    return SimulatorBroker(
        account_id=simulator_config.get("account_id", "simulator"),
        account_type=simulator_config.get("account_type", "stock"),
        initial_cash=simulator_config.get("initial_cash", 1_000_000),
    )


def _build_huaxin(config: Mapping[str, Any]) -> BrokerBase:
    """根据完整配置延迟构造第一方华鑫券商。

    Args:
        config: 完整券商配置，使用其中的 ``huaxin`` 子映射。

    Returns:
        尚未 preflight、默认禁止交易与撤单的 ``HuaxinBroker``。

    Side Effects:
        仅延迟导入并构造 Python 对象，不 dlopen、编译、联网或加载厂商 SDK。
    """

    from ..integrations.huaxin.broker import HuaxinBroker

    huaxin_config = dict(config.get("huaxin") or {})
    return HuaxinBroker(
        account_id=huaxin_config.get("account_id") or "huaxin-unconfigured",
        account_type=huaxin_config.get("account_type", "stock"),
        config=huaxin_config,
    )


BROKER_REGISTRY = BrokerRegistry()
BROKER_REGISTRY.register("qmt", _build_qmt)
BROKER_REGISTRY.register("qmt-remote", _build_remote_qmt)
BROKER_REGISTRY.register("simulator", _build_simulator)
BROKER_REGISTRY.register("huaxin", _build_huaxin)


def register_broker(
    name: str,
    builder: BrokerBuilder,
    *,
    aliases: Tuple[str, ...] = (),
    replace: bool = False,
) -> None:
    """向进程级注册表注册券商实现。

    Args:
        name: 券商主名称。
        builder: 券商构造函数。
        aliases: 可选别名。
        replace: 是否允许覆盖已有注册。

    Returns:
        None。

    Side Effects:
        更新进程级 ``BROKER_REGISTRY``。
    """

    BROKER_REGISTRY.register(name, builder, aliases=aliases, replace=replace)


def create_broker(name: str, config: Mapping[str, Any]) -> BrokerBase:
    """通过进程级注册表创建券商实例。

    Args:
        name: 券商名称。
        config: 完整券商配置映射。

    Returns:
        构造完成但尚未连接的券商实例。
    """

    return BROKER_REGISTRY.create(name, config)


def list_brokers() -> Tuple[str, ...]:
    """列出进程级注册表中的全部券商名称。

    Returns:
        按字典序排列的名称元组。
    """

    return BROKER_REGISTRY.names()


__all__ = [
    "BROKER_REGISTRY",
    "BrokerBuilder",
    "BrokerRegistry",
    "create_broker",
    "list_brokers",
    "register_broker",
]
