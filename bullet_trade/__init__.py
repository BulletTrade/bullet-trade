"""作者：BruceLee。

职责：公开聚宽兼容的数据、策略和回测入口，按需提供分析绘图接口。
输入：模块导入和显式导出访问；输出：标准API对象及延迟绘图导出。
上下游：Provider、核心模型/调度/引擎和分析模块；数据导入不初始化字体管理器。
约定：保留Agg非交互后端及原plt/分析接口，绘图库失败保持警告/None兼容。
"""

from importlib import import_module
from typing import Any, TYPE_CHECKING

import warnings

try:
    import matplotlib
    # 设置非交互式后端，避免程序挂起
    matplotlib.use('Agg')
except Exception as exc:  # pragma: no cover - 兼容运行环境缺少 GUI 依赖的情况
    warnings.warn(f"Matplotlib 初始化失败，将跳过图形相关功能：{exc!r}")
    matplotlib = None  # type: ignore
    plt = None  # type: ignore

# 加载环境变量
from .utils.env_loader import load_env
load_env()

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
    """输入导出名称，返回并缓存原绘图模块/分析对象；显式请求才导入，未知名称抛AttributeError。"""
    if name == 'plt':
        try:
            attr = import_module('matplotlib.pyplot')
        except Exception as exc:  # 兼容此前入口的可选绘图库加载失败行为。
            warnings.warn(f"Matplotlib 初始化失败，将跳过图形相关功能：{exc!r}")
            attr = None
        globals()[name] = attr
        return attr
    if name in _ANALYSIS_EXPORTS:
        module = import_module('.core.analysis', __name__)
        attr = getattr(module, name)
        globals()[name] = attr
        return attr
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """无输入，返回包含原plt和延迟分析接口的名称列表；仅列举不触发绘图导入。"""
    return sorted(set(globals().keys()) | set(_ANALYSIS_EXPORTS) | {'plt'})
