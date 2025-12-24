"""
BulletTrade - 专业的量化交易系统
完整的本地策略回测系统，兼容聚宽API，支持多数据源和实盘交易
"""

from importlib import import_module
from typing import Any, TYPE_CHECKING

import warnings

try:
    import matplotlib
    # 设置非交互式后端，避免程序挂起
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt  # noqa: F401
except Exception as exc:  # pragma: no cover - 兼容运行环境缺少 GUI 依赖的情况
    warnings.warn(f"Matplotlib 初始化失败，将跳过图形相关功能：{exc!r}")
    matplotlib = None  # type: ignore
    plt = None  # type: ignore

# 加载环境变量
from .utils.env_loader import load_env
import os
load_env(os.getenv("ENV_FILE", ".env"), verbose=True)

# 配置中文字体显示
# from .utils.font_config import setup_chinese_fonts
# setup_chinese_fonts()

# 导入数据模块
from .data.api import *  # noqa: F401,F403

# 导入核心模块
from .core.models import (
    Context, Portfolio, SubPortfolio, Position,
    Trade, Order, OrderStatus, OrderStyle, SecurityUnitData
)
from .core.globals import g, log
from .core.settings import (
    set_benchmark, set_order_cost, set_commission, set_universe, set_slippage, set_option,
    OrderCost, PerTrade, FixedSlippage, PriceRelatedSlippage, StepRelatedSlippage
)
from .core.orders import (
    order, order_value, order_target, order_target_value, cancel_order, cancel_all_orders,
    MarketOrderStyle, LimitOrderStyle
)
from .core.scheduler import (
    run_daily, run_weekly, run_monthly, unschedule_all
)
from .core.engine import create_backtest, BacktestEngine

from .__version__ import __version__

_ANALYSIS_EXPORTS = (
    'plot_results',
    'plot_positions',
    'calculate_metrics',
    'print_metrics',
    'export_trades',
    'generate_report',
)

if TYPE_CHECKING:  # pragma: no cover - 仅用于类型提示
    from .core import analysis as _analysis_module  # noqa: F401,F811

__all__ = [
    # 数据模型
    'Context', 'Portfolio', 'SubPortfolio', 'Position',
    'Trade', 'Order', 'OrderStatus', 'OrderStyle', 'SecurityUnitData',
    # 全局对象
    'g', 'log',
    # 设置函数
    'set_benchmark', 'set_order_cost', 'set_commission', 'set_universe', 'set_slippage', 'set_option',
    'OrderCost', 'PerTrade', 'FixedSlippage', 'PriceRelatedSlippage', 'StepRelatedSlippage',
    # 订单函数
    'order', 'order_value', 'order_target', 'order_target_value', 'cancel_order', 'cancel_all_orders',
    'MarketOrderStyle', 'LimitOrderStyle',
    # 调度函数
    'run_daily', 'run_weekly', 'run_monthly', 'unschedule_all',
    # 回测引擎
    'create_backtest', 'BacktestEngine',
    # 版本
    '__version__',
] + list(_ANALYSIS_EXPORTS)


def __getattr__(name: str) -> Any:
    """延迟加载分析绘图相关函数，避免在未安装 matplotlib 时导入失败"""
    if name in _ANALYSIS_EXPORTS:
        module = import_module('.core.analysis', __name__)
        attr = getattr(module, name)
        globals()[name] = attr
        return attr
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """让内建 dir() 同样展示延迟导出的接口"""
    return sorted(set(globals().keys()) | set(_ANALYSIS_EXPORTS))
