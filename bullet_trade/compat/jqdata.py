"""
jqdata 兼容模块

此模块提供与聚宽 jqdata 完全兼容的API接口，
使得聚宽策略可以使用 `from jqdata import *` 而无需修改代码。
"""

"""兼容层导出：核心 API + 策略辅助工具。"""

# 从 bullet_trade 核心模块导入所有API
from bullet_trade.core.api import *  # noqa: F401,F403
from bullet_trade.core.orders import MarketOrderStyle, LimitOrderStyle  # 显式导出市价/限价样式

# 策略辅助：打印持仓/打印DataFrame（供策略便捷使用）
from bullet_trade.utils.strategy_helpers import (
    print_portfolio_info,
    prettytable_print_df,
)

# 常用模块：与聚宽环境一致，from jqdata import * 后可直接使用
import datetime as _datetime
import math as _math
import random as _random
import time as _time
import numpy as _np
import pandas as _pd

# 暴露到模块命名空间
datetime = _datetime
math = _math
random = _random
time = _time
np = _np
pd = _pd

# 导出所有API，确保 from jqdata import * 可用
__all__ = [
    # 全局对象
    'g', 'log', 'send_msg', 'set_message_handler',
    
    # 订单相关
    'order', 'order_target', 'order_value', 'order_target_value', 'cancel_order', 'cancel_all_orders',
    'get_open_orders', 'get_orders', 'get_trades',
    'MarketOrderStyle', 'LimitOrderStyle',
    'FixedSlippage', 'PriceRelatedSlippage', 'StepRelatedSlippage',
    'OrderCost', 'PerTrade',
    
    # 调度器
    'run_daily', 'run_weekly', 'run_monthly', 'unschedule_all',
    
    # 设置相关
    'set_benchmark', 'set_option', 'set_slippage', 'set_order_cost', 'set_commission', 'set_universe',
    
    # 数据API
    'get_price', 'history', 'attribute_history', 'get_bars', 'get_ticks', 'get_current_tick',
    'get_current_data', 'get_extras', 'get_fundamentals', 'get_fundamentals_continuously',
    'get_trade_days', 'get_trade_day', 'get_all_securities', 'get_security_info', 'get_fund_info',
    'get_index_stocks', 'get_index_weights',
    'get_industry_stocks', 'get_industry', 'get_concept_stocks', 'get_concept',
    'get_margincash_stocks', 'get_marginsec_stocks',
    'get_dominant_future', 'get_future_contracts',
    'get_billboard_list', 'get_locked_shares',
    'get_split_dividend',
    'set_data_provider', 'get_data_provider',
    # Live 能力声明
    'require_data_capabilities',
    # 研究文件读写
    'read_file', 'write_file',
    # Tick 订阅与快照（占位/最小实现）
    'subscribe', 'unsubscribe', 'unsubscribe_all',

    # 数据模型
    'Context', 'Portfolio', 'SubPortfolio', 'Position',
    'Order', 'Trade', 'OrderStatus', 'OrderStyle',
    'SecurityUnitData',

    # 策略辅助工具
    'print_portfolio_info',
    'prettytable_print_df',

    # 常用模块
    'datetime', 'math', 'random', 'time', 'np', 'pd',
]
