"""
运行时辅助：持有当前引擎实例并提供即时撮合入口
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Any, Optional

_current_engine: Optional[Any] = None


def set_current_engine(engine: object) -> None:
    """注册当前运行的引擎实例"""
    global _current_engine
    _current_engine = engine


def get_current_engine() -> Optional[object]:
    """获取当前引擎，如果不存在则返回 None"""
    return _current_engine


def process_orders_now() -> None:
    """立即处理订单队列。

    Args:
        无。

    Returns:
        None。当前引擎不存在时静默返回，其他模式保持原有即时处理语义。

    Raises:
        RuntimeError: 严格 checkpoint 引擎禁止在状态落盘前即时产生 broker 副作用。
    """
    engine = get_current_engine()
    if engine is None:
        return
    if getattr(engine, "defer_order_processing", False):
        raise RuntimeError(
            "严格 checkpoint 模式禁止 process_orders_now() 绕过状态持久化"
        )

    try:
        result = engine._process_orders(engine.context.current_dt)
        if inspect.isawaitable(result):
            loop = getattr(engine, "_loop", None)
            if loop and loop.is_running():
                fut = asyncio.run_coroutine_threadsafe(result, loop)
                fut.result()
            else:
                asyncio.run(result)
    except Exception:
        raise
