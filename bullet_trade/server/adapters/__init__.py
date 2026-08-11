"""
作者: BruceLee

文件职责: 管理远程服务 adapter 的进程级注册、查询和兼容别名。
主要输入: server-type 名称及返回 ``AdapterBundle`` 的 builder。
主要输出: 稳定的 adapter builder，或确定性的未注册/冲突错误。
上游关系: ``bullet-trade server`` 命令与第一方 adapter 模块。
下游关系: QMT、Big QMT、stub 以及后续华鑫服务端 adapter。
关键配置: 注册过程不创建监听、不连接数据源或券商，也不加载厂商 SDK。
"""

from __future__ import annotations

from threading import RLock
from typing import Dict, Tuple

from .base import AdapterBuilder, AdapterBundle

REGISTRY: Dict[str, AdapterBuilder] = {}
_REGISTRY_LOCK = RLock()


def _normalize_server_type(server_type: str) -> str:
    """规范化服务端 adapter 名称。

    Args:
        server_type: 配置或 CLI 传入的 adapter 名称。

    Returns:
        去除首尾空白并转为小写的名称。

    Raises:
        ValueError: 名称为空或不是字符串时抛出。
    """

    if not isinstance(server_type, str) or not server_type.strip():
        raise ValueError("server-type 不能为空")
    return server_type.strip().lower()


def register_adapter(
    server_type: str,
    builder: AdapterBuilder,
    *,
    aliases: Tuple[str, ...] = (),
    replace: bool = False,
) -> None:
    """注册服务端 adapter builder 及其别名。

    Args:
        server_type: adapter 主名称。
        builder: 接收 server 配置和账户路由器的构造函数。
        aliases: 可选兼容别名。
        replace: 是否允许覆盖已有且不同的 builder。

    Returns:
        None。

    Raises:
        TypeError: builder 不可调用时抛出。
        ValueError: 名称无效、别名重复或未允许的注册冲突时抛出。

    Side Effects:
        原子更新进程级 ``REGISTRY``；不会执行 builder。
    """

    if not callable(builder):
        raise TypeError("adapter builder 必须可调用")
    names = tuple(_normalize_server_type(item) for item in (server_type, *aliases))
    if len(set(names)) != len(names):
        raise ValueError("server-type 主名称与别名不能重复")
    with _REGISTRY_LOCK:
        conflicts = [
            item for item in names if item in REGISTRY and REGISTRY[item] is not builder
        ]
        if conflicts and not replace:
            raise ValueError(f"server-type 已注册: {', '.join(sorted(conflicts))}")
        for item in names:
            REGISTRY[item] = builder


def get_adapter(server_type: str) -> AdapterBuilder:
    """读取指定服务类型的 adapter builder。

    Args:
        server_type: adapter 主名称或别名。

    Returns:
        已注册的 builder。

    Raises:
        KeyError: 指定名称尚未注册时抛出，错误中包含可用名称。
    """

    normalized = _normalize_server_type(server_type)
    with _REGISTRY_LOCK:
        builder = REGISTRY.get(normalized)
        available = tuple(sorted(REGISTRY))
    if builder is None:
        choices = ", ".join(available) or "无"
        raise KeyError(f"未注册的 server-type: {normalized}；可用类型: {choices}")
    return builder


def list_adapters() -> Tuple[str, ...]:
    """列出当前进程全部已注册 server-type。

    Returns:
        按字典序排列的不可变名称元组。
    """

    with _REGISTRY_LOCK:
        return tuple(sorted(REGISTRY))


__all__ = [
    "AdapterBundle",
    "get_adapter",
    "list_adapters",
    "register_adapter",
]
