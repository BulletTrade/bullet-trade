"""
聚宽策略回测系统 - 核心模块

提供回测所需的所有核心对象和功能
"""

from .models import (
    Context, Portfolio, SubPortfolio, Position, 
    Trade, Order, OrderStatus, OrderStyle,
    SecurityUnitData
)
from .globals import g, log
from .notifications import send_msg, set_message_handler
from .settings import (
    set_benchmark, set_order_cost, set_commission, set_universe, set_slippage,
    set_option, OrderCost, PerTrade, FixedSlippage, PriceRelatedSlippage, StepRelatedSlippage
)
from .orders import (
    order, order_value, order_target, order_target_value
)
from .scheduler import (
    run_daily, run_weekly, run_monthly, unschedule_all
)
from .exceptions import (
    FutureDataError, UserError
)
from .risk_control import (
    RiskController, RiskStats,
    get_global_risk_controller, reset_global_risk_controller
)
from .security_id import (
    SECURITY_ID_SCHEME_VERSION,
    JoinQuantSecurityIdAdapter,
    QmtSecurityIdAdapter,
    SecurityCodeSource,
    SecurityCodeTarget,
    SecurityExchange,
    SecurityId,
    SecurityIdErrorCode,
    SecurityIdValidationError,
    ThsSecurityIdAdapter,
    format_security_id,
    parse_security_id,
)
# 注意：为避免导入环，核心包初始化阶段不导入 engine/optimizer
# 需要时请从具体模块导入：
# from bullet_trade.core.engine import create_backtest, BacktestEngine
# from bullet_trade.core.optimizer import run_param_grid

__all__ = [
    # 数据模型
    'Context', 'Portfolio', 'SubPortfolio', 'Position',
    'Trade', 'Order', 'OrderStatus', 'OrderStyle',
    'SecurityUnitData',
    # 全局对象
    'g', 'log', 'send_msg', 'set_message_handler',
    # 设置函数
    'set_benchmark', 'set_order_cost', 'set_commission', 'set_universe', 'set_slippage',
    'set_option', 'OrderCost', 'PerTrade', 'FixedSlippage', 'PriceRelatedSlippage', 'StepRelatedSlippage',
    # 订单函数
    'order', 'order_value', 'order_target', 'order_target_value',
    # 调度函数
    'run_daily', 'run_weekly', 'run_monthly', 'unschedule_all',
    # 异常
    'FutureDataError', 'UserError',
    # 风控
    'RiskController', 'RiskStats',
    'get_global_risk_controller', 'reset_global_risk_controller',
    # 证券身份契约
    'SECURITY_ID_SCHEME_VERSION',
    'SecurityCodeSource', 'SecurityCodeTarget', 'SecurityExchange', 'SecurityId',
    'SecurityIdErrorCode', 'SecurityIdValidationError',
    'JoinQuantSecurityIdAdapter', 'QmtSecurityIdAdapter', 'ThsSecurityIdAdapter',
    'format_security_id', 'parse_security_id',
]
