"""
回测引擎

实现策略回测的核心逻辑
"""

import importlib.util
import inspect as _inspect
import sys
import re
from typing import Dict, Any, Callable, Optional, List, Sequence, Tuple
from datetime import datetime, timedelta, time as Time
import pandas as pd
import numpy as np
import time
from decimal import Decimal, ROUND_HALF_UP
# from pathlib import Path  # removed: use local import in _setup_log_file
# Optional import: jqdatasdk for fallback in strategy wrapper
# try:
#     import jqdatasdk as jq
# except Exception:
jq = None

from .models import Context, Portfolio, Position, Trade, OrderStatus
from .globals import g, log, reset_globals
from .settings import get_settings, reset_settings, OrderCost, FixedSlippage, PriceRelatedSlippage, StepRelatedSlippage
from .orders import get_order_queue, clear_order_queue, MarketOrderStyle, LimitOrderStyle
from .scheduler import (
    generate_daily_schedule,
    get_market_periods,
    get_tasks,
    unschedule_all,
)
from ..data.api import (
    set_current_context,
    get_price as api_get_price,
    get_trade_days as api_get_trade_days,
    get_data_provider,
    get_security_info,
)
from .runtime import set_current_engine
from . import pricing
from ..utils.env_loader import get_live_trade_config

_BASE_MARKET_SLIPPAGE = 0.00246
_DEFAULT_MARKET_BUY_PERCENT = _BASE_MARKET_SLIPPAGE / 2
_DEFAULT_MARKET_SELL_PERCENT = -_BASE_MARKET_SLIPPAGE / 2

PRE_MARKET_OFFSET = timedelta(minutes=30)


class BacktestEngine:
    """回测引擎"""
    
    def __init__(
        self,
        strategy_file: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: str = 'day',
        initial_cash: float = 100000,
        benchmark: Optional[str] = None,
        log_file: Optional[str] = None,
        extras: Optional[Dict[str, Any]] = None,
        initial_positions: Optional[List[Dict[str, Any]]] = None,
        algorithm_id: Optional[str] = None,
        # 新增：支持直接传递策略函数（用于测试框架）
        initialize: Optional[Callable] = None,
        handle_data: Optional[Callable] = None,
        before_trading_start: Optional[Callable] = None,
        after_trading_end: Optional[Callable] = None,
        process_initialize: Optional[Callable] = None,
    ):
        """
        初始化回测引擎
        
        Args:
            strategy_file: 策略文件路径（与函数参数二选一）
            start_date: 回测开始日期 'YYYY-MM-DD'（可在run()中指定）
            end_date: 回测结束日期 'YYYY-MM-DD'（可在run()中指定）
            frequency: 回测频率 ('day' or 'minute')
            initial_cash: 初始资金（现金部分）
            benchmark: 基准标的
            log_file: 日志文件路径，默认为None（不写文件）
            extras: 额外参数字典，将注入到 g.extras 和 context.run_params
            initial_positions: 初始持仓列表 [{'security': str, 'amount': int, 'avg_cost': float?}]
            algorithm_id: 算法ID（可选），将记录在结果 meta
            # 函数参数（与strategy_file二选一，用于测试框架）
            initialize: 策略初始化函数
            handle_data: 每个bar调用的函数
            before_trading_start: 盘前调用的函数
            after_trading_end: 盘后调用的函数
            process_initialize: 实盘初始化函数
        """
        self.strategy_file = strategy_file
        self.start_date = pd.to_datetime(start_date) if start_date else None
        self.end_date = pd.to_datetime(end_date) if end_date else None
        self.frequency = frequency
        self.initial_cash = initial_cash
        self.log_file = log_file
        self.file_handler = None  # 文件处理器
        
        # 策略函数：支持直接传递（测试框架）或从文件加载
        self.initialize_func: Optional[Callable] = initialize
        self.handle_data_func: Optional[Callable] = handle_data
        self.before_trading_start_func: Optional[Callable] = before_trading_start
        self.after_trading_end_func: Optional[Callable] = after_trading_end
        self.process_initialize_func: Optional[Callable] = process_initialize
        
        self.context: Optional[Context] = None
        self.daily_records = []  # 每日记录
        self.trades = []  # 所有交易记录
        self.events = []  # 事件记录（分红/拆分）
        self._processed_dividend_keys = set()  # 已处理的分红事件键（避免重复处理）
        self.benchmark_data = None  # 基准数据
        # 新增：每日持仓快照记录
        self.daily_positions = []
        # 新增：用户自定义参数与初始持仓
        self.extras = extras or None
        self.initial_positions = initial_positions or None
        self.algorithm_id = algorithm_id
        # 新增：收益计算基准（首次总资产）
        self.start_total_value: Optional[float] = None
        # 新增：回测运行耗时（秒）
        self.runtime_seconds: Optional[float] = None
        market_cfg = get_live_trade_config()
        self._market_buy_percent = float(market_cfg.get('market_buy_price_percent', _DEFAULT_MARKET_BUY_PERCENT))
        self._market_sell_percent = float(market_cfg.get('market_sell_price_percent', _DEFAULT_MARKET_SELL_PERCENT))

    def load_strategy(self):
        """加载策略文件或使用传入的函数"""
        
        # 如果已经通过构造函数传入了策略函数，不需要从文件加载
        if self.initialize_func is not None:
            log.info("使用直接传入的策略函数")
            # 重置全局状态
            reset_globals()
            reset_settings()
            unschedule_all()
            # 注入全局到策略所在模块，使其具备与文件加载一致的环境（打印/数据API/时间库等）
            try:
                mod = _inspect.getmodule(self.initialize_func)
                if mod is not None:
                    self._inject_globals(mod)
            except Exception as _inj_err:
                log.debug(f"函数策略全局注入失败: {_inj_err}")
            return
        
        # 否则从文件加载
        if not self.strategy_file:
            raise ValueError("必须提供 strategy_file 或 initialize 函数")
            
        log.info(f"加载策略文件: {self.strategy_file}")
        
        # 重置全局状态
        reset_globals()
        reset_settings()
        unschedule_all()
        
        try:
            # 动态加载策略模块
            spec = importlib.util.spec_from_file_location("strategy", self.strategy_file)
            if spec and spec.loader:
                strategy_module = importlib.util.module_from_spec(spec)
                sys.modules["strategy"] = strategy_module
                
                # 注入全局变量和函数
                self._inject_globals(strategy_module)
                
                # 加载模块
                spec.loader.exec_module(strategy_module)
                
                # 获取策略函数
                self.initialize_func = getattr(strategy_module, 'initialize', None)
                self.handle_data_func = getattr(strategy_module, 'handle_data', None)
                self.before_trading_start_func = getattr(strategy_module, 'before_trading_start', None)
                self.after_trading_end_func = getattr(strategy_module, 'after_trading_end', None)
                
                log.info("策略文件加载成功")
            else:
                raise ValueError(f"无法加载策略文件: {self.strategy_file}")
                
        except Exception as e:
            log.error(f"加载策略失败: {e}")
            raise
    
    def _inject_globals(self, module):
        """向策略模块注入全局变量和函数"""
        from .globals import g, log
        from .settings import (
            set_benchmark, set_order_cost, set_commission, set_universe, set_slippage, set_option,
            OrderCost, PerTrade, FixedSlippage, PriceRelatedSlippage, StepRelatedSlippage
        )
        from .api import (
            subscribe as _subscribe,
            unsubscribe as _unsubscribe,
            unsubscribe_all as _unsubscribe_all,
            get_current_tick as _get_current_tick,
        )
        from .orders import (
            order,
            order_value,
            order_target,
            order_target_value,
            cancel_order,
            cancel_all_orders,
        )
        from .scheduler import run_daily, run_weekly, run_monthly, unschedule_all
        from ..data import api as wrapped_api
        from .notifications import send_msg as _send_msg, set_message_handler as _set_message_handler
        from ..utils.strategy_helpers import (
            print_portfolio_info,
            prettytable_print_df,
        )
        from ..research.io import read_file as _read_file, write_file as _write_file
        import math as _math
        import datetime as _datetime
        import time as _time
        import random as _random
        try:
            import talib as _talib  # type: ignore
        except Exception:  # talib 可能未安装
            class _TalibProxy:
                def __getattr__(self, name):
                    raise ImportError(
                        "TA-Lib 未安装。请先 `pip install TA-Lib` 并确保本机已安装对应的C库。"
                    )
            _talib = _TalibProxy()
        
        # 注入全局对象
        module.g = g
        module.log = log
        module.send_msg = _send_msg
        module.set_message_handler = _set_message_handler
        
        # 注入设置函数
        module.set_benchmark = set_benchmark
        module.set_order_cost = set_order_cost
        module.set_commission = set_commission
        module.set_universe = set_universe
        module.set_slippage = set_slippage
        module.set_option = set_option
        module.OrderCost = OrderCost
        module.PerTrade = PerTrade
        module.FixedSlippage = FixedSlippage
        module.PriceRelatedSlippage = PriceRelatedSlippage
        module.StepRelatedSlippage = StepRelatedSlippage
        # 研究文件读写
        module.read_file = _read_file
        module.write_file = _write_file

        # Tick 订阅 API
        module.subscribe = _subscribe
        module.unsubscribe = _unsubscribe
        module.unsubscribe_all = _unsubscribe_all
        module.get_current_tick = _get_current_tick

        # 注入订单函数
        module.order = order
        module.order_value = order_value
        module.order_target = order_target
        module.order_target_value = order_target_value
        
        # 注入调度函数
        module.run_daily = run_daily
        module.run_weekly = run_weekly
        module.run_monthly = run_monthly
        module.unschedule_all = unschedule_all

        # 注入 record 函数（模拟聚宽，用于自定义绘图数据）
        def record(**kwargs):
            dt = self.context.current_dt
            # 初始化容器
            if not hasattr(g, 'record_series') or g.record_series is None:
                g.record_series = {}
            for key, value in kwargs.items():
                series = g.record_series.get(key)
                if series is None:
                    g.record_series[key] = []
                g.record_series[key].append((dt, value))
        module.record = record
        
        # 注入包装过的数据函数（支持真实价格和未来数据检测）
        module.get_price = wrapped_api.get_price
        module.attribute_history = wrapped_api.attribute_history
        module.get_current_data = wrapped_api.get_current_data
        module.get_trade_days = wrapped_api.get_trade_days
        module.get_all_securities = wrapped_api.get_all_securities
        module.get_index_stocks = wrapped_api.get_index_stocks
        module.get_security_info = wrapped_api.get_security_info
        
        # 注入numpy和pandas
        module.np = np
        module.pd = pd
        # 注入标准库与常用模块
        module.math = _math
        module.datetime = _datetime
        module.time = _time
        module.random = _random
        module.talib = _talib
        # 注入常用打印/报告辅助函数
        module.print_portfolio_info = print_portfolio_info
        module.prettytable_print_df = prettytable_print_df
        
        # 注册 jqdata 兼容模块：使用真正的 ModuleType 并补齐常见导出
        import types as _types
        jq_mod = _types.ModuleType('jqdata')
        # 全局对象与常用工具，保证 from jqdata import * 可获取 g/log 等
        jq_mod.g = g
        jq_mod.log = log
        jq_mod.send_msg = _send_msg
        jq_mod.set_message_handler = _set_message_handler
        jq_mod.record = record
        jq_mod.np = np
        jq_mod.pd = pd
        jq_mod.math = _math
        jq_mod.datetime = _datetime
        jq_mod.time = _time
        jq_mod.random = _random
        jq_mod.talib = _talib
        jq_mod.print_portfolio_info = print_portfolio_info
        jq_mod.prettytable_print_df = prettytable_print_df
        # 数据 API（包装层）
        jq_mod.get_price = wrapped_api.get_price
        jq_mod.attribute_history = wrapped_api.attribute_history
        jq_mod.get_current_data = wrapped_api.get_current_data
        jq_mod.get_trade_days = wrapped_api.get_trade_days
        jq_mod.get_all_securities = wrapped_api.get_all_securities
        jq_mod.get_index_stocks = wrapped_api.get_index_stocks
        jq_mod.get_security_info = wrapped_api.get_security_info
        # 设置/参数 API
        jq_mod.set_benchmark = set_benchmark
        jq_mod.set_order_cost = set_order_cost
        jq_mod.set_slippage = set_slippage
        jq_mod.set_option = set_option
        jq_mod.OrderCost = OrderCost
        jq_mod.FixedSlippage = FixedSlippage
        # 研究文件读写
        jq_mod.read_file = _read_file
        jq_mod.write_file = _write_file
        # 订单 API（保持从 jqdata 导入的兼容写法）
        jq_mod.order = order
        jq_mod.order_value = order_value
        jq_mod.order_target = order_target
        jq_mod.order_target_value = order_target_value
        jq_mod.cancel_order = cancel_order
        jq_mod.cancel_all_orders = cancel_all_orders
        jq_mod.MarketOrderStyle = MarketOrderStyle
        jq_mod.LimitOrderStyle = LimitOrderStyle
        # 调度 API
        jq_mod.run_daily = run_daily
        jq_mod.run_weekly = run_weekly
        jq_mod.run_monthly = run_monthly
        jq_mod.unschedule_all = unschedule_all
        # Tick 订阅 API（聚宽兼容写法）
        jq_mod.subscribe = _subscribe
        jq_mod.unsubscribe = _unsubscribe
        jq_mod.unsubscribe_all = _unsubscribe_all
        jq_mod.get_current_tick = _get_current_tick
        # 注入并注册
        sys.modules['jqdata'] = jq_mod
        module.jqdata = jq_mod
        # 如有需要，可在此处同步注册到 jqdatasdk：保持禁用以避免误用
        # sys.modules['jqdatasdk'] = jq_mod
    
    def run(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        capital_base: Optional[float] = None,
        frequency: Optional[str] = None,
        benchmark: Optional[str] = None,
    ):
        """
        运行回测
        
        Args:
            start_date: 回测开始日期（可选，优先使用此参数）
            end_date: 回测结束日期（可选，优先使用此参数）
            capital_base: 初始资金（可选，优先使用此参数）
            frequency: 回测频率（可选，优先使用此参数）
            benchmark: 基准标的（可选，优先使用此参数）
        
        Returns:
            Dict[str, Any]: 回测结果
        """
        # 优先使用传入的参数，否则使用构造函数的参数
        if start_date:
            self.start_date = pd.to_datetime(start_date)
        if end_date:
            self.end_date = pd.to_datetime(end_date)
        if capital_base:
            self.initial_cash = capital_base
        if frequency:
            self.frequency = frequency
        if benchmark is not None:  # 允许传入 None 来不设置基准
            # benchmark 会在load_strategy后通过 set_benchmark 设置
            pass
            
        # 验证必需参数
        if self.start_date is None or self.end_date is None:
            raise ValueError("必须提供 start_date 和 end_date")
            
        # 设置日志文件（如果指定）
        if self.log_file:
            self._setup_log_file()
        
        t0_run = time.time()
        
        log.info("=" * 60)
        log.info(f"开始回测: {self.start_date.strftime('%Y-%m-%d')} 至 {self.end_date.strftime('%Y-%m-%d')}")
        log.info(f"初始资金: {self.initial_cash:,.2f}")
        log.info(f"回测频率: {self.frequency}")
        if self.log_file:
            log.info(f"日志文件: {self.log_file}")
        log.info("=" * 60)
        
        # 设置回测频率到 settings（供 scheduler 使用）
        from .settings import set_option
        set_option('backtest_frequency', self.frequency)
        set_option('backtest_start_date', self.start_date.date())
        
        # 加载策略
        self.load_strategy()

        # 注意：load_strategy 内部会调用 reset_settings()，需要重新注入频率配置
        set_option('backtest_frequency', self.frequency)
        set_option('backtest_start_date', self.start_date.date())
        
        # 初始化上下文
        self.context = Context(
            portfolio=Portfolio(
                total_value=self.initial_cash,
                available_cash=self.initial_cash,
                starting_cash=self.initial_cash
            ),
            current_dt=self.start_date
        )
        
        # 设置当前上下文（用于数据API）
        set_current_context(self.context)
        # 注册当前引擎实例（供即时撮合使用）
        try:
            set_current_engine(self)
        except Exception:
            pass
        
        # 新增：注入运行参数与初始持仓
        try:
            # 注入 extras 到全局 g，供策略访问
            if self.extras is not None:
                g.extras = self.extras
            # 注入运行参数到 context
            self.context.run_params = {
                'algorithm_id': self.algorithm_id,
                'start_date': self.start_date.strftime('%Y-%m-%d'),
                'end_date': self.end_date.strftime('%Y-%m-%d'),
                'frequency': self.frequency,
                'initial_cash': self.initial_cash,
                'extras': self.extras,
                'initial_positions': self.initial_positions,
            }
            # 应用初始持仓（不消耗现金，视为已有持仓）
            self._apply_initial_positions()
            # 设置收益基准为首次总资产（现金+持仓）
            self.start_total_value = float(self.context.portfolio.available_cash + self.context.portfolio.positions_value + self.context.portfolio.locked_cash)
        except Exception as e:
            log.warning(f"初始化参数/持仓注入失败: {e}")
        
        # 调用initialize
        if self.initialize_func:
            log.info("调用策略初始化函数...")
            try:
                self.initialize_func(self.context)
                log.info("策略初始化完成")
                # 根据 extras 覆盖 g.xxx（在策略初始化后执行）
                try:
                    if self.extras:
                        applied = 0
                        for k, v in self.extras.items():
                            try:
                                setattr(g, k, v)
                                applied += 1
                            except Exception:
                                pass
                        log.info(f"已根据 extras 覆盖 g 参数: {applied} 项")
                except Exception as ex:
                    log.warning(f"extras 覆盖 g 参数失败: {ex}")
                # 避免重复：若 before_market_open/market_open 已通过调度注册，则取消直接调用
                try:
                    tasks = get_tasks()
                    if self.before_trading_start_func and any(t.func is self.before_trading_start_func for t in tasks):
                        log.debug("检测到 before_market_open 已由调度注册，取消重复直接调用")
                        self.before_trading_start_func = None
                    if self.handle_data_func and any(t.func is self.handle_data_func for t in tasks):
                        log.debug("检测到 market_open 已由调度注册，取消重复直接调用")
                        self.handle_data_func = None
                except Exception as ex:
                    log.warning(f"调度重复检查失败: {ex}")
            except Exception as e:
                log.error(f"策略初始化失败: {e}")
                import traceback
                log.error(traceback.format_exc())
                raise
        
        # 获取交易日列表（直接使用Provider，避免上下文限制导致只取到起始日）
        provider = get_data_provider()
        trade_days = provider.get_trade_days(
            start_date=self.start_date,
            end_date=self.end_date
        )
        trade_days = [pd.to_datetime(d) for d in trade_days]
        
        log.info(f"交易日数量: {len(trade_days)}")
        
        # 如果没有交易日，提前返回
        if not trade_days:
            log.warning(f"⚠️  回测区间 {self.start_date} 至 {self.end_date} 内没有交易日")
            log.warning("可能的原因：")
            log.warning("  1. 回测日期范围过短（如只有一天且恰好是周末或节假日）")
            log.warning("  2. 回测日期超出数据源的可用范围")
            log.warning("  3. 回测日期为未来日期")
            log.warning("建议：请检查回测日期范围，或选择包含有效交易日的日期区间")
            
            # 返回空结果
            return self._generate_empty_results()
        
        # 获取回测第一天的前一个交易日（用于初始化 previous_date）
        first_day_previous_date = None
        if trade_days:
            try:
                # 获取第一个交易日之前的1个交易日（直接使用Provider）
                provider = get_data_provider()
                before_first_day = provider.get_trade_days(
                    end_date=trade_days[0],
                    count=2  # 包括第一天本身和前一天
                )
                if len(before_first_day) >= 2:
                    first_day_previous_date = pd.to_datetime(before_first_day[0]).date()
                    log.info(f"回测第一天的前一交易日: {first_day_previous_date}")
            except Exception as e:
                log.warning(f"获取回测第一天的前一交易日失败: {e}")
        
        # 获取基准数据
        settings = get_settings()
        if settings.benchmark:
            self._load_benchmark_data(settings.benchmark)
        
        # 逐日回测
        for i, trade_day in enumerate(trade_days):
            # 更新前一个时间点
            self.context.previous_dt = self.context.current_dt if i > 0 else None
            
            # 更新前一个交易日（date 类型）
            if i > 0:
                self.context.previous_date = trade_days[i-1].date()
            else:
                # 第一天使用回测开始前的交易日
                self.context.previous_date = first_day_previous_date
            
            # 更新当前时间
            self.context.current_dt = trade_day

            # 新交易日开始：释放 T+1 锁定
            try:
                self._rollover_tplus_for_new_day()
            except Exception as ex:
                log.debug(f"T+1 解锁失败: {ex}")

            log.info(f"\n{'=' * 60}")
            log.info(f"交易日: {trade_day.strftime('%Y-%m-%d')} ({i+1}/{len(trade_days)})")
            if self.context.previous_date:
                log.debug(f"前一交易日: {self.context.previous_date}")

            market_periods = get_market_periods()

            # 执行当日调度（open/close等）
            self._run_trading_day(trade_day, market_periods)

            # 先更新收盘价与持仓市值，再记录每日数据，避免“日志总值≠CSV总值”的错位
            self._update_positions()

            # 记录每日数据（使用最新的收盘价与持仓市值）
            self._record_daily()

            # 新增：记录每日持仓快照（已是收盘价）
            self._record_daily_positions()
        
        log.info("\n" + "=" * 60)
        log.info("回测完成")
        log.info("=" * 60)
        
        # 清理日志文件处理器
        if self.log_file:
            self._cleanup_log_file()
        
        # 记录运行耗时
        try:
            self.runtime_seconds = float(time.time() - t0_run)
            log.info(f"本次回测耗时: {self.runtime_seconds:.3f} 秒")
        except Exception:
            pass
        
        return self._generate_results()
    
    def _setup_log_file(self):
        """设置日志文件处理器，级别由 LOG_FILE_LEVEL 环境变量控制"""
        try:
            import logging
            from pathlib import Path
            
            # 确保日志目录存在
            log_path = Path(self.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 读取文件日志级别配置（LOG_FILE_LEVEL，未设置则跟随 LOG_LEVEL）
            try:
                from bullet_trade.utils.env_loader import get_system_config
                sys_cfg = get_system_config() or {}
                file_level_name = str(sys_cfg.get('log_file_level', 'INFO')).upper()
                file_level = getattr(logging, file_level_name, logging.INFO)
            except Exception:
                file_level = logging.INFO
            
            # 创建文件处理器
            self.file_handler = logging.FileHandler(
                self.log_file, 
                mode='w', 
                encoding='utf-8'
            )
            self.file_handler.setLevel(file_level)
            
            # 设置日志格式
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            self.file_handler.setFormatter(formatter)
            
            # 确保 logger 主体级别不高于文件级别，否则消息会被过滤
            if log.logger.level > file_level:
                log.logger.setLevel(file_level)
            
            # 添加到 jq_strategy logger
            log.logger.addHandler(self.file_handler)
            
            # 同步到 bullet_trade logger，确保 miniqmt 等模块的日志也写入文件
            try:
                std_logger = logging.getLogger("bullet_trade")
                std_logger.addHandler(self.file_handler)
                if std_logger.level > file_level:
                    std_logger.setLevel(file_level)
            except Exception:
                pass
            
            log.info(f"日志将同时输出到文件: {self.log_file} (级别: {file_level_name})")
            
        except Exception as e:
            log.warning(f"设置日志文件失败: {e}")
    
    def _cleanup_log_file(self):
        """清理日志文件处理器"""
        try:
            if self.file_handler:
                self.file_handler.flush()
                self.file_handler.close()
                log.logger.removeHandler(self.file_handler)
                # 同时从 bullet_trade logger 移除
                try:
                    import logging
                    std_logger = logging.getLogger("bullet_trade")
                    std_logger.removeHandler(self.file_handler)
                except Exception:
                    pass
                self.file_handler = None
                log.info(f"日志已保存至: {self.log_file}")
        except Exception as e:
            log.warning(f"清理日志文件处理器失败: {e}")
    
    def _update_current_time(self, current_dt: datetime, previous_dt: Optional[datetime]):
        """更新上下文当前时间并同步日志、引擎状态。"""
        self.context.previous_dt = previous_dt
        self.context.current_dt = current_dt
        set_current_context(self.context)
        try:
            set_current_engine(self)
        except Exception:
            pass
        log.set_strategy_time(current_dt)

    @staticmethod
    def _is_trading_time(current_dt: datetime, market_periods: Sequence[Tuple[Time, Time]]) -> bool:
        """判断当前时间是否处于交易时段。"""
        current_time = current_dt.time()
        for start, end in market_periods:
            if start <= current_time <= end:
                return True
        return False

    def _run_trading_day(self, trade_day: datetime, market_periods: Sequence[Tuple[Time, Time]]):
        """执行单个交易日的调度与撮合流程。"""
        if not market_periods:
            raise ValueError("交易时段配置不能为空")

        previous_trade_day = getattr(self.context, "previous_date", None)
        schedule_map = generate_daily_schedule(trade_day, previous_trade_day, market_periods)

        day_date = trade_day.date()
        open_dt = datetime.combine(day_date, market_periods[0][0])
        close_dt = datetime.combine(day_date, market_periods[-1][1])
        pre_open_dt = open_dt - PRE_MARKET_OFFSET

        timeline_set = set(schedule_map.keys())
        timeline_set.add(pre_open_dt)
        timeline_set.add(open_dt)
        timeline_set.add(close_dt)

        timeline = sorted(timeline_set)
        previous_event_dt = self.context.previous_dt
        dividends_applied = False

        for current_dt in timeline:
            self._update_current_time(current_dt, previous_event_dt)
            previous_event_dt = current_dt

            if not dividends_applied:
                try:
                    self._apply_dividends_for_day(trade_day)
                except Exception as e:
                    log.warning(f"盘前分红处理失败: {e}")
                dividends_applied = True

            tasks = schedule_map.get(current_dt, [])
            for task in tasks:
                try:
                    log.debug(f"执行定时任务: {task.func.__name__}")
                    task.func(self.context)
                except Exception as e:
                    log.error(f"定时任务执行失败 {task.func.__name__}: {e}")
                    import traceback
                    log.error(traceback.format_exc())

            if current_dt == pre_open_dt and self.before_trading_start_func:
                try:
                    self.before_trading_start_func(self.context)
                except Exception as e:
                    log.error(f"盘前函数执行失败: {e}")
                    import traceback
                    log.error(traceback.format_exc())

            if current_dt == open_dt and self.handle_data_func:
                try:
                    from ..data.api import get_current_data

                    data = get_current_data()
                    self.handle_data_func(self.context, data)
                except Exception as e:
                    log.error(f"交易函数执行失败: {e}")
                    import traceback
                    log.error(traceback.format_exc())

            if self._is_trading_time(current_dt, market_periods):
                self._process_orders(current_dt)

            if current_dt == close_dt and self.after_trading_end_func:
                try:
                    self.after_trading_end_func(self.context)
                except Exception as e:
                    log.error(f"盘后函数执行失败: {e}")
                    import traceback
                    log.error(traceback.format_exc())

    def _apply_dividends_for_day(self, trade_day: datetime):
        """开盘前处理当日生效的权益变动（分红、送转/拆分），同步持仓与现金口径。
        
        注意：如果除权日当天停牌，延迟到复牌日再处理（与聚宽行为一致）。
        """
        portfolio = self.context.portfolio
        if not portfolio.positions:
            return

        # 税务口径：股票默认按 20% 预提，公募基金类免税（可由配置覆盖）
        settings = get_settings()
        tax_rate = settings.options.get('dividend_tax_rate', 0.20)

        # 取当日权益变动事件；若除权除息日期落在非交易日或停牌日，则顺延到后续首个交易日盘前处理
        # 注意：需要往前多查几天，以包含因停牌而延迟的分红事件
        current_date = trade_day.date()
        prev_date = getattr(self.context, 'previous_date', None)
        # 往前查 10 天，以覆盖可能因停牌延迟的分红事件
        query_start = current_date - timedelta(days=10)

        for code, pos in list(portfolio.positions.items()):
            try:
                corp_actions = self._load_corporate_actions(code, start_date=query_start, end_date=current_date)
                if not corp_actions:
                    continue

                for action in corp_actions:
                    eff_date = action.get('date')
                    split_ratio = float(action.get('scale_factor', 1.0) or 1.0)
                    gross_div = float(action.get('bonus_pre_tax', 0.0) or 0.0)
                    
                    # 生成事件唯一键，用于避免重复处理
                    event_key = f"{code}_{eff_date}_{split_ratio}_{gross_div}"
                    if event_key in self._processed_dividend_keys:
                        log.debug(f"{code} 分红/拆分事件已处理过，跳过: {event_key}")
                        continue
                    
                    # 记录完整的分红/拆分事件信息
                    log.debug(
                        f"{code} 分红/拆分事件: 除权日={eff_date}, 当前日={current_date}, "
                        f"拆分比例={split_ratio:.4f}, 派息={gross_div:.4f}, "
                        f"当前持仓={pos.total_amount}股, 当前价格={pos.price:.4f}"
                    )
                    
                    if not self._is_action_effective_today(action, current_date):
                        log.debug(f"{code} 分红/拆分事件: 除权日 {eff_date} 未到当日 {current_date}，跳过")
                        continue
                    
                    # 检查除权日当天是否停牌：如果停牌则延迟到复牌日处理
                    # 使用直接调用 provider 的方式绕过 avoid_future_data 限制
                    is_paused = self._is_security_paused_on_date(code, eff_date)
                    log.debug(f"{code} 停牌检测结果: 除权日 {eff_date} 停牌={is_paused}")
                    
                    if is_paused:
                        if eff_date == current_date:
                            # 除权日就是今天，今天停牌，延迟处理
                            log.info(f"{code} 在除权日 {current_date} 停牌，除权事件（拆分={split_ratio:.4f}，派息={gross_div:.4f}）延迟到复牌日处理")
                            continue
                        else:
                            # 除权日已过去且停牌，今天是复牌后的第一个处理机会
                            log.info(f"{code} 除权日 {eff_date} 停牌，今日 {current_date} 执行延迟的除权事件")

                    sec_type = action.get('security_type', 'stock')
                    # 内部口径：统一为 split_ratio/gross_div/base_lot
                    split_ratio = float(action.get('scale_factor', 1.0) or 1.0)
                    gross_div = float(action.get('bonus_pre_tax', 0.0) or 0.0)
                    base_lot = int(action.get('per_base', 10) or 10)

                    # 记录事件前股数用于现金红利口径（若同日既有拆分又有现金红利，按拆分前股数计算）
                    pre_event_amount = pos.total_amount

                    # 送转/拆分调整持仓与成本
                    if split_ratio and abs(split_ratio - 1.0) > 1e-9:
                        self._apply_split(
                            code=code,
                            pos=pos,
                            split_ratio=split_ratio,
                            eff_date=eff_date,
                            portfolio=portfolio,
                        )
                    # 现金红利（基金类免税；股票按税率扣减）
                    if gross_div > 0:
                        self._apply_cash_dividend(
                            code=code,
                            pos=pos,
                            gross_div=gross_div,
                            base_lot=base_lot,
                            tax_rate=tax_rate,
                            sec_type=sec_type,
                            split_ratio=split_ratio,
                            pre_event_amount=pre_event_amount,
                            eff_date=eff_date,
                            portfolio=portfolio,
                        )
                    
                    # 记录已处理的分红事件，避免重复处理
                    self._processed_dividend_keys.add(event_key)
                    log.debug(f"{code} 分红/拆分事件已处理: {event_key}")
            except Exception as e:
                log.debug(f"处理分红失败 {code}: {e}")

        # 更新账户价值
        portfolio.update_value()
    
    def _get_order_cost_config(self, security: str) -> OrderCost:
        info = get_security_info(security)
        category = self._infer_security_category(security, info)
        settings = get_settings()
        type_hint = str(info.get('type') or category).lower()
        order_cost = settings.order_cost_overrides.get(f"{category}_{security}")
        if not order_cost:
            order_cost = settings.order_cost_overrides.get(f"{type_hint}_{security}")
        if not order_cost:
            order_cost = settings.order_cost.get(category)
        if not order_cost and category != 'stock':
            order_cost = settings.order_cost.get('stock')
        if not order_cost:
            order_cost = OrderCost()
        return order_cost

    @staticmethod
    def _infer_security_category(security: str, info: Dict[str, Any]) -> str:
        """推断证券分类，用于确定价格精度、交易规则和费用。
        
        分类来源优先级（由 get_security_info + security_overrides.json 决定）：
        1. by_code 显式指定的 category（最高优先级）
        2. by_prefix 前缀映射的 category
        3. 数据源返回的 type/subtype
        4. 默认为 stock
        
        Returns:
            str: 'money_market_fund' | 'fund' | 'stock'
        """
        # get_security_info 已合并 security_overrides.json 的配置（by_code > by_prefix）
        # 所以 info.get('category') 已经是最终结果
        explicit = str(info.get('category') or '').lower()
        if explicit in ('money_market_fund', 'fund', 'stock', 'futures'):
            return explicit

        # 兼容数据源返回的 type/subtype 字段
        subtype = str(info.get('subtype') or '').lower()
        primary = str(info.get('type') or '').lower()

        if subtype in ('mmf', 'money_market_fund'):
            return 'money_market_fund'
        if primary in ('fund', 'etf'):
            # jqdatasdk 返回 type='etf'，统一归类为 fund
            return 'fund'
        if primary == 'futures':
            return 'futures'
        if primary == 'stock':
            return 'stock'

        # 默认视为股票（security_overrides.json 的 by_prefix 已在 get_security_info 中处理）
        return 'stock'

    def _calc_trade_price_with_default_slippage(self, price: float, is_buy: bool, security: str, info: Optional[Dict[str, Any]] = None, category: Optional[str] = None) -> float:
        """未显式配置滑点时，按品类采用默认值；现金类不应用滑点。"""
        if info is None:
            info = get_security_info(security)
        if category is None:
            category = self._infer_security_category(security, info)
        # 可由 overrides 指定精确值
        ratio = info.get('slippage')
        if not isinstance(ratio, (int, float)):
            if category == 'money_market_fund':
                ratio = 0.0
            else:
                ratio = 0.00246  # 默认值 24.6bps（常见设置），可通过配置覆盖
        if ratio <= 0:
            return price
        half = float(ratio) / 2.0
        return price * (1 + half) if is_buy else price * (1 - half)

    def _select_slippage_config(self, security: str, category: str, info: Dict[str, Any]) -> Optional[Any]:
        """按聚宽语义解析滑点配置优先级。"""
        settings = get_settings()
        sl_map = getattr(settings, 'slippage_map', {}) or {}
        if not sl_map:
            return None
        keys = []
        code = security
        type_hint = str(info.get('type') or '').lower()
        subtype = str(info.get('subtype') or '').lower()

        if category == 'stock':
            keys.append(f'stock_{code}')
            keys.append('stock')
        elif category == 'fund' or subtype in ('fund', 'index'):
            keys.append(f'fund_{code}')
            keys.append('fund')
        elif category == 'money_market_fund' or subtype in ('mmf', 'money_market_fund'):
            return None

        if type_hint == 'index' and category != 'stock':
            keys.append(f'fund_{code}')
            keys.append('fund')

        if type_hint == 'futures' or category == 'futures':
            keys.append(f'futures_{code}')
            try:
                tag = re.sub(r'\\d+.*', '', code.split('.')[0])
                if tag:
                    keys.append(f'futures_{tag}')
            except Exception:
                pass
            keys.append('futures')

        keys.append(code)
        keys.append(category)
        keys.append('all')

        seen = set()
        for key in keys:
            if key in seen:
                continue
            seen.add(key)
            cfg = sl_map.get(key)
            if cfg is not None:
                return cfg
        return None

    def _apply_slippage_config(self, config: Any, price: float, is_buy: bool, security: str, category: str, info: Dict[str, Any]) -> float:
        """根据配置计算滑点后的价格。"""
        try:
            if isinstance(config, PriceRelatedSlippage):
                half = float(config.ratio) / 2.0
                return price * (1 + half) if is_buy else price * (1 - half)
            if isinstance(config, StepRelatedSlippage):
                tick = self._tick_step_for_security(security, info=info, category=category)
                half = float(config.steps) * tick / 2.0
                return price + half if is_buy else price - half
            if isinstance(config, FixedSlippage):
                half = float(config.value) / 2.0
                return price + half if is_buy else price - half
            # 兜底：若实现了 calculate_slippage 接口
            if hasattr(config, 'calculate_slippage'):
                return config.calculate_slippage(price, is_buy)
        except Exception as exc:
            log.debug(f"应用滑点失败 {security}: {exc}")
        return price

    def _apply_slippage_price(self, price: float, is_buy: bool, security: str) -> float:
        """统一处理滑点选择与回退。"""
        info = get_security_info(security)
        category = self._infer_security_category(security, info)
        subtype = str(info.get('subtype') or '').lower()
        if category == 'money_market_fund' or subtype in ('mmf', 'money_market_fund'):
            return price

        settings = get_settings()
        cfg = self._select_slippage_config(security, category, info)
        if cfg:
            return self._apply_slippage_config(cfg, price, is_buy, security, category, info)
        if settings.slippage:
            return self._apply_slippage_config(settings.slippage, price, is_buy, security, category, info)
        return self._calc_trade_price_with_default_slippage(price, is_buy, security, info=info, category=category)

    @staticmethod
    def _round_half_up(value: float, decimals: int) -> float:
        q = Decimal(10) ** -decimals
        return float(Decimal(str(value)).quantize(q, rounding=ROUND_HALF_UP))

    def _round_to_tick(self, price: float, security: str, *, is_buy: Optional[bool] = None) -> float:
        """按最小报价单位处理价格：
        - 股票: 0.01
        - 基金/ETF/货基: 0.001
        方向规则：is_buy=True 向上取整，is_buy=False 向下取整，is_buy=None 四舍五入到最近档位。
        """
        step = self._tick_step_for_security(security)
        if step <= 0:
            return price
        import math
        ticks = price / step
        if is_buy is True:
            ticks_rounded = math.ceil(ticks - 1e-12)
        elif is_buy is False:
            ticks_rounded = math.floor(ticks + 1e-12)
        else:
            # 四舍五入到最近 tick
            ticks_rounded = math.floor(ticks + 0.5)
        return round(ticks_rounded * step, 3 if step == 0.001 else 2)

    def _tick_step_for_security(self, security: str, info: Optional[Dict[str, Any]] = None, category: Optional[str] = None) -> float:
        """返回标的对应的最小报价步长。"""
        if info is None:
            info = get_security_info(security)
        if category is None:
            category = self._infer_security_category(security, info)
        tick_decimals = info.get('tick_decimals')
        tick_size = info.get('tick_size')
        try:
            if isinstance(tick_size, (int, float)) and tick_size > 0:
                return float(tick_size)
            if isinstance(tick_decimals, (int, float)) and tick_decimals >= 0:
                return float(round(10 ** (-int(tick_decimals)), 6))
        except Exception:
            pass
        return 0.01 if category == 'stock' else 0.001

    def _rollover_tplus_for_new_day(self) -> None:
        """在新交易日开始时释放 T+1 锁定的当日买入量。"""
        if not self.context or not self.context.portfolio or not self.context.portfolio.positions:
            return
        for code, pos in list(self.context.portfolio.positions.items()):
            try:
                info = get_security_info(code)
                tplus = self._infer_tplus_from_info(info)
                if tplus == 1:
                    add = getattr(pos, 'today_buy_t1', 0) or 0
                    if add > 0:
                        pos.closeable_amount += add
                        pos.today_buy_t1 = 0
            except Exception:
                continue

    def _apply_split(
        self,
        *,
        code: str,
        pos: "Position",
        split_ratio: float,
        eff_date: "date",
        portfolio: "Portfolio",
    ) -> None:
        """应用送转/拆分：调整股数、可卖数量、持仓成本与价格，使持仓市值保持不变。"""
        if not split_ratio or abs(split_ratio - 1.0) <= 1e-9:
            return
        positions_value_before = portfolio.positions_value
        cash_before = portfolio.available_cash
        old_amount = pos.total_amount
        old_price = pos.price
        old_avg_cost = pos.avg_cost
        old_pos_value = old_amount * pos.price

        new_amount = int(round(old_amount * split_ratio))
        pos.total_amount = new_amount
        pos.closeable_amount = int(round(pos.closeable_amount * split_ratio))
        if pos.avg_cost > 0:
            pos.avg_cost = pos.avg_cost / split_ratio
            pos.acc_avg_cost = pos.avg_cost
        if pos.price > 0:
            pos.price = pos.price / split_ratio
        pos.value = pos.total_amount * pos.price

        new_pos_value = pos.total_amount * pos.price
        positions_value_after = positions_value_before - old_pos_value + new_pos_value
        cash_after = cash_before
        total_value_before = cash_before + positions_value_before
        total_value_after = cash_after + positions_value_after

        log.info(
            f"{code} 拆分/送转: 生效日={eff_date}, 比例={split_ratio:.4f}, "
            f"股数 {old_amount} -> {new_amount}, "
            f"价格 {old_price:.4f} -> {pos.price:.4f}, "
            f"成本 {old_avg_cost:.4f} -> {pos.avg_cost:.4f}, "
            f"市值 {old_pos_value:.2f} -> {new_pos_value:.2f}"
        )
        self.events.append(
            {
                "event_type": "拆分/送转",
                "strategy_time": self.context.current_dt,
                "code": code,
                "event_date": eff_date,
                "per_base": None,
                "bonus_pre_tax": None,
                "net_bonus": None,
                "tax_rate_percent": None,
                "cash_in": None,
                "scale_factor": split_ratio,
                "old_amount": int(old_amount),
                "new_amount": int(new_amount),
                "cash_before": float(cash_before),
                "cash_after": float(cash_after),
                "positions_value_before": float(positions_value_before),
                "positions_value_after": float(positions_value_after),
                "total_value_before": float(total_value_before),
                "total_value_after": float(total_value_after),
            }
        )

    @staticmethod
    def _infer_tplus_from_info(info):
        if not isinstance(info, dict):
            return 0
        raw = info.get('tplus')
        if raw is not None:
            try:
                return int(raw)
            except Exception:
                pass
        sec_type = str(info.get('type') or '').lower()
        return 1 if sec_type == 'stock' else 0

    def _apply_cash_dividend(
        self,
        *,
        code: str,
        pos: "Position",
        gross_div: float,
        base_lot: int,
        tax_rate: float,
        sec_type: str,
        split_ratio: float,
        pre_event_amount: int,
        eff_date: "date",
        portfolio: "Portfolio",
    ) -> None:
        """应用现金分红：按口径入账现金，并按税前派息冲减平均成本。"""
        if gross_div <= 0:
            return
        if sec_type in ("fund", "etf", "lof", "fja", "fjb"):
            net_bonus = gross_div
            tax_msg = "免税"
        else:
            net_bonus = gross_div * (1.0 - tax_rate)
            tax_msg = f"税率 {tax_rate*100:.0f}%"

        positions_value_before = portfolio.positions_value
        cash_before = portfolio.available_cash

        use_pre = bool(split_ratio and abs(split_ratio - 1.0) > 1e-9)
        amount_for_dividend = pre_event_amount if use_pre else pos.total_amount
        cash_in = (amount_for_dividend / base_lot) * net_bonus if base_lot else 0.0
        cash_in = self._round_half_up(cash_in, 2)
        if cash_in <= 0:
            return

        portfolio.available_cash += cash_in
        cash_after = cash_before + cash_in
        positions_value_after = positions_value_before
        total_value_before = cash_before + positions_value_before
        total_value_after = cash_after + positions_value_after

        log.info(
            f"{code} 现金分红: 生效 {eff_date}, 每{base_lot}派{gross_div:.4f} (净{net_bonus:.4f}, {tax_msg})，入账 {cash_in:.2f}"
        )
        self.events.append(
            {
                "event_type": "现金分红",
                "strategy_time": self.context.current_dt,
                "code": code,
                "event_date": eff_date,
                "per_base": int(base_lot),
                "bonus_pre_tax": float(gross_div),
                "net_bonus": float(net_bonus),
                "tax_rate_percent": 0 if "免税" in tax_msg else int(round(tax_rate * 100)),
                "cash_in": float(cash_in),
                "scale_factor": None,
                "old_amount": None,
                "new_amount": None,
                "cash_before": float(cash_before),
                "cash_after": float(cash_after),
                "positions_value_before": float(positions_value_before),
                "positions_value_after": float(positions_value_after),
                "total_value_before": float(total_value_before),
                "total_value_after": float(total_value_after),
            }
        )

        # 平均成本冲减（按税前派息口径；如当日发生拆分需折算至新股数）
        try:
            reduce_per_old_share = (gross_div / base_lot) if base_lot else 0.0
            if reduce_per_old_share > 0 and pos.avg_cost > 0:
                factor = split_ratio if (split_ratio and abs(split_ratio - 1.0) > 1e-9) else 1.0
                per_new_share_reduce = reduce_per_old_share / factor
                pos.avg_cost = max(0.0, pos.avg_cost - per_new_share_reduce)
                pos.acc_avg_cost = pos.avg_cost
        except Exception:
            pass

    def _resolve_base_exec_price(self, security: str, current_dt: datetime, fq_mode: str) -> Optional[float]:
        """根据时间窗口解析撮合的基准价。
        - 09:25-09:30: 当日未复权开盘价
        - 09:31-15:00: 当前分钟未复权收盘价
        - 其他时段: 未复权日收盘价
        返回 None 表示无法获取。
        """
        try:
            t = current_dt.time() if isinstance(current_dt, datetime) else None
            if t and (Time(9, 25) <= t < Time(9, 31)):
                # # 优先尝试 09:30 分钟价（若存在），否则回退到日开
                # try:
                #     dfm = api_get_price(
                #         security=security,
                #         end_date=current_dt,
                #         frequency='minute',
                #         fields=['close'],
                #         count=1,
                #         fq=fq_mode
                #     )
                #     if not dfm.empty:
                #         rowm = dfm.iloc[-1]
                #         if isinstance(dfm.index, pd.DatetimeIndex):
                #             last_ts = dfm.index[-1]
                #             # 若最后一行时间早于当前时间，仍可作为近似基准
                #             if last_ts <= pd.Timestamp(current_dt):
                #                 val = float(rowm.get('close') or 0.0)
                #                 if val > 0:
                #                     return val
                # except Exception:
                #     pass
                dfp = api_get_price(
                    security=security,
                    end_date=current_dt,
                    frequency='daily',
                    fields=['open'],
                    count=1,
                    fq=fq_mode
                )
                if not dfp.empty:
                    rowp = dfp.iloc[-1]
                    return float(rowp.get('open') or 0.0)
            elif t and (Time(9, 31) <= t < Time(15, 0)):
                dfp = api_get_price(
                    security=security,
                    end_date=current_dt,
                    frequency='minute',
                    fields=['close'],
                    count=1,
                    fq=fq_mode
                )
                if not dfp.empty:
                    rowp = dfp.iloc[-1]
                    return float(rowp.get('close') or 0.0)
            else:
                dfp = api_get_price(
                    security=security,
                    end_date=current_dt,
                    frequency='daily',
                    fields=['close'],
                    count=1,
                    fq=fq_mode
                )
                if not dfp.empty:
                    rowp = dfp.iloc[-1]
                    return float(rowp.get('close') or 0.0)
        except Exception:
            return None
        return None

    def _load_corporate_actions(self, code: str, start_date: datetime.date, end_date: datetime.date) -> List[Dict[str, Any]]:
        """加载标的在区间内的分红/拆分等权益事件。失败时返回空列表。"""
        try:
            from ..data.api import get_split_dividend

            events = get_split_dividend(code, start_date=start_date, end_date=end_date)
            return events or []
        except Exception:
            return []

    @staticmethod
    def _is_action_effective_today(action: Dict[str, Any], current_date: datetime.date) -> bool:
        """判断事件在 current_date 是否应当生效（包含等于）。"""
        ev_date = action.get("date")
        if ev_date is None:
            return False
        return ev_date <= current_date

    def _is_security_paused_on_date(self, security: str, check_date: datetime.date) -> bool:
        """检查标的在指定日期是否停牌。
        
        用于判断除权日当天是否停牌，如果停牌则需延迟到复牌日处理。
        
        兼容 JQData 和 QMT 两种数据源：
        - JQData: 停牌日有数据，paused=1, volume=0
        - QMT: 停牌日无数据，返回前一天数据
        
        注意：此函数直接调用 provider.get_price，绕过 avoid_future_data 限制，
        因为停牌状态是元数据，不是策略交易信号。
        """
        try:
            from datetime import timedelta
            from bullet_trade.data import api as data_api
            
            # 直接调用 provider 的 get_price，绕过 api 层的 avoid_future_data 检查
            # 这是因为停牌判断是元数据，不应受回测模式限制
            provider = data_api._provider
            
            # 获取 check_date 前后的数据
            start = check_date - timedelta(days=5)
            end = check_date + timedelta(days=1)  # 多取一天确保包含 check_date
            
            df = provider.get_price(
                security=security,
                start_date=datetime.combine(start, Time(0, 0)),
                end_date=datetime.combine(end, Time(15, 0)),
                frequency='daily',
                fields=['volume', 'paused'],
                fq='none',
                skip_paused=False  # 重要：不跳过停牌日，否则无法判断
            )
            
            if df.empty:
                log.debug(f"{security} 在 {check_date} 附近无数据")
                return False  # 无法判断，不阻断
            
            # 获取 df 中所有日期
            dates_in_df = []
            for idx in df.index:
                if hasattr(idx, 'date'):
                    dates_in_df.append(idx.date())
                elif hasattr(idx, 'to_pydatetime'):
                    dates_in_df.append(idx.to_pydatetime().date())
            dates_in_df = sorted(dates_in_df)
            
            log.debug(f"{security} 停牌检测: check_date={check_date}, df日期={dates_in_df[-5:] if len(dates_in_df) > 5 else dates_in_df}")
            
            # 检查 check_date 是否在数据中
            if check_date not in dates_in_df:
                # 数据中没有 check_date（QMT行为），判定为停牌
                prev_day = check_date - timedelta(days=1)
                if prev_day in dates_in_df or (dates_in_df and dates_in_df[-1] >= prev_day):
                    log.debug(f"{security} 在 {check_date} 无数据（数据范围正常），判定为停牌")
                    return True
                else:
                    log.debug(f"{security} 在 {check_date} 无数据，数据范围不足，无法判断")
                    return False
            
            # check_date 在数据中（JQData行为），检查 paused/volume 字段
            for idx in df.index:
                idx_date = idx.date() if hasattr(idx, 'date') else idx.to_pydatetime().date()
                if idx_date == check_date:
                    row = df.loc[idx]
                    # 优先使用 paused 字段
                    if 'paused' in row and pd.notna(row['paused']):
                        is_paused = bool(row['paused'])
                        log.debug(f"{security} 在 {check_date} paused={row['paused']}，停牌={is_paused}")
                        return is_paused
                    # 其次用成交量判断
                    if 'volume' in row:
                        is_paused = float(row['volume'] or 0) == 0
                        log.debug(f"{security} 在 {check_date} volume={row['volume']}，停牌={is_paused}")
                        return is_paused
                    break
            
            return False
        except Exception as e:
            log.debug(f"检查 {security} 停牌状态失败: {e}")
            return False  # 获取失败时不阻断处理

    def _process_orders(self, current_dt: datetime):
        """处理订单队列"""
        orders = get_order_queue()
        if not orders:
            return
        
        log.info(f"处理 {len(orders)} 个订单")
        
        settings = get_settings()
        
        # 获取当前行情数据容器（延迟加载）
        from ..data.api import get_current_data
        current_data = get_current_data()
        
        # 目标类订单预处理：若同一标的存在目标订单，取消其之前未完成订单，避免重复与超额
        try:
            last_target_index: Dict[str, int] = {}
            for idx, o in enumerate(orders):
                is_target = (hasattr(o, '_is_target_amount') and getattr(o, '_is_target_amount')) or \
                            (hasattr(o, '_is_target_value') and getattr(o, '_is_target_value'))
                if is_target:
                    last_target_index[o.security] = idx
            if last_target_index:
                new_orders = []
                for idx, o in enumerate(orders):
                    if o.security in last_target_index and idx < last_target_index[o.security]:
                        # 取消旧订单
                        if o.status == OrderStatus.open:
                            o.status = OrderStatus.canceled
                            log.info(f"因目标下单，取消未完成订单: {o.security}, 订单ID {o.order_id}")
                        continue
                    new_orders.append(o)
                orders = new_orders
        except Exception as ex:
            log.debug(f"目标订单预处理失败: {ex}")
        use_real_price = bool(settings.options.get('use_real_price'))
        fq_mode = 'pre' if use_real_price else 'none'

        for order in orders:
            try:
                # 获取当前价格
                if order.security not in current_data:
                    log.warning(f"无法获取 {order.security} 的行情数据")
                    order.status = OrderStatus.rejected
                    continue
                
                security_data = current_data[order.security]
                try:
                    sec_info = get_security_info(order.security)
                except Exception:
                    sec_info = {}
                security_category = self._infer_security_category(order.security, sec_info)
                if security_data.paused:
                    log.warning(f"{order.security} 停牌，订单取消")
                    order.status = OrderStatus.canceled
                    continue
                
                # 解析执行价基准（封装逻辑便于维护与测试）
                current_dt = self.context.current_dt
                base_exec_price = self._resolve_base_exec_price(order.security, current_dt, fq_mode)

                current_price = float(base_exec_price) if base_exec_price and base_exec_price > 0 else security_data.last_price
                if current_price <= 0:
                    log.warning(f"{order.security} 价格无效: {current_price}")
                    order.status = OrderStatus.rejected
                    continue
                
                # 计算下单数量（普通/目标/价值）——使用 current_price 作为金额换算基准
                amount = self._calculate_order_amount(order, current_price)
                if amount == 0:
                    log.debug(f"{order.security} 无需交易")
                    order.status = OrderStatus.canceled
                    continue
                
                # 先按方向取绝对值，后续统一为正数处理
                is_buy = (amount > 0)
                intended_amount = abs(amount)
                
                style_obj = getattr(order, "style", None)
                trade_price: Optional[float] = None
                limit_price: Optional[float] = None
                if isinstance(style_obj, LimitOrderStyle):
                    limit_price = float(style_obj.price)
                elif isinstance(style_obj, MarketOrderStyle):
                    if style_obj.limit_price is not None:
                        limit_price = float(style_obj.limit_price)
                    else:
                        try:
                            percent = pricing.resolve_market_percent(
                                style_obj,
                                is_buy,
                                self._market_buy_percent,
                                self._market_sell_percent,
                            )
                            limit_price = pricing.compute_market_protect_price(
                                order.security,
                                security_data.last_price,
                                getattr(security_data, "high_limit", None),
                                getattr(security_data, "low_limit", None),
                                percent,
                                is_buy,
                            )
                        except Exception as exc:
                            log.debug(f"未能计算市价保护价 {order.security}: {exc}")

                if trade_price is None:
                    info = get_security_info(order.security)
                    category = self._infer_security_category(order.security, info)
                    if category == 'money_market_fund':
                        trade_price = current_price
                    else:
                        trade_price = self._apply_slippage_price(current_price, is_buy, order.security)

                trade_price = self._round_to_tick(trade_price, order.security, is_buy=None)
                
                # 根据证券分类确定价格精度：stock=2位小数，fund/money_market_fund=3位小数
                price_decimals = 2 if security_category == 'stock' else 3

                if limit_price is not None:
                    if is_buy and trade_price - limit_price > 1e-9:
                        log.info(f"{order.security} 撮合价 {trade_price:.{price_decimals}f} 超出买入限价 {limit_price:.{price_decimals}f}，取消订单")
                        order.status = OrderStatus.canceled
                        continue
                    if (not is_buy) and limit_price - trade_price > 1e-9:
                        log.info(f"{order.security} 撮合价 {trade_price:.{price_decimals}f} 低于卖出限价 {limit_price:.{price_decimals}f}，取消订单")
                        order.status = OrderStatus.canceled
                        continue
                if isinstance(style_obj, MarketOrderStyle) and limit_price is not None and security_category == 'stock':
                    # 市价单的保护价仅用于限价保护与资金检查，成交价仍按滑点撮合
                    pass

                # 买入前资金检查：按“可下单量上限”缩量 + 一手取整 + 最小申报量
                final_amount = intended_amount
                min_trade_size = 100  # A股一手
                min_order_amount = 100
                
                # 资金检查价格：若存在限价/保护价则按限价锁资，否则按撮合价
                fund_check_price = limit_price if limit_price is not None else trade_price
                
                # 费用参数
                order_cost_config = self._get_order_cost_config(order.security)
                if order_cost_config:
                    open_comm_rate = order_cost_config.open_commission
                    open_tax_rate = order_cost_config.open_tax
                    close_comm_rate = order_cost_config.close_commission
                    close_tax_rate = order_cost_config.close_tax
                    min_commission = order_cost_config.min_commission
                else:
                    open_comm_rate = 0.0003
                    open_tax_rate = 0.0
                    close_comm_rate = 0.0003
                    close_tax_rate = 0.001
                    min_commission = 5.0
                
                if is_buy:
                    # 买入：计算可下单量上限，考虑最小佣金与锁定资金
                    available_for_buy = max(0.0, self.context.portfolio.available_cash - self.context.portfolio.locked_cash)
                    effective_cash = max(0.0, available_for_buy - min_commission)
                    denom = fund_check_price * (1.0 + open_comm_rate + open_tax_rate)
                    aval_amount = int(effective_cash // denom) if denom > 0 else 0
                    if aval_amount <= 0:
                        log.warning(f"{order.security} 资金不足，最小费用后可用现金为 {effective_cash:.2f}")
                        order.status = OrderStatus.rejected
                        continue
                    if aval_amount < final_amount:
                        log.info(f"{order.security} 缩量至可下单上限: {aval_amount}")
                        final_amount = aval_amount
                else:
                    # 卖出：缩量为可卖出数量
                    pos = self.context.portfolio.positions.get(order.security)
                    if not pos or pos.closeable_amount <= 0:
                        log.warning(f"{order.security} 无可卖持仓")
                        order.status = OrderStatus.rejected
                        continue
                    if final_amount > pos.closeable_amount:
                        log.info(f"{order.security} 可卖出不足，缩量为 {pos.closeable_amount}")
                        final_amount = pos.closeable_amount
                
                if is_buy:
                    # 买入：一手取整 + 最小申报量
                    final_amount = (final_amount // min_trade_size) * min_trade_size
                    if final_amount < min_trade_size:
                        log.debug(f"{order.security} 买入数量 {final_amount} 不足最小申报量({min_order_amount})")
                        order.status = OrderStatus.canceled
                        continue
                else:
                    # 如果剩余的股数超过一手，但卖出订单不足一手，则取消卖出订单（如果剩余股数不足一手，则允许碎股卖出）
                    if pos.closeable_amount >= min_trade_size:
                        final_amount = (final_amount // min_trade_size) * min_trade_size
                        if final_amount < min_trade_size:
                            log.debug(f"{order.security} 卖出数量 {final_amount} 不足最小申报量({min_order_amount})")
                            order.status = OrderStatus.canceled
                            continue
                    else:
                        log.debug(f"{order.security} 可卖持仓 {final_amount} 不足一手，允许碎股卖出")
                        final_amount = final_amount
                
                trade_amount = final_amount
                trade_value = trade_price * trade_amount
                
                # 计算交易费用（含最小佣金）
                if is_buy:
                    commission = max(trade_value * open_comm_rate, min_commission)
                    tax = trade_value * open_tax_rate
                else:
                    commission = max(trade_value * close_comm_rate, min_commission)
                    tax = trade_value * close_tax_rate
                # 金额类按“分”四舍五入
                commission = self._round_half_up(commission, 2)
                tax = self._round_half_up(tax, 2)
                total_cost = self._round_half_up(trade_value + commission + tax, 2)
                
                if is_buy:
                    # 委托时锁定资金（含费用）
                    self.context.portfolio.locked_cash += total_cost
                    if total_cost > (self.context.portfolio.available_cash):
                        # 双重保障：若仍不足则拒绝并回滚锁定
                        log.warning(f"{order.security} 资金不足: 需要 {total_cost:.2f}, 可用 {self.context.portfolio.available_cash:.2f}")
                        self.context.portfolio.locked_cash -= total_cost
                        order.status = OrderStatus.rejected
                        continue
                    # 扣除资金并释放锁定
                    self.context.portfolio.available_cash -= total_cost
                    self.context.portfolio.locked_cash -= total_cost
                    
                    # 更新持仓
                    if order.security not in self.context.portfolio.positions:
                        self.context.portfolio.positions[order.security] = Position(security=order.security)
                    position = self.context.portfolio.positions[order.security]
                    position.update_position(trade_amount, trade_price)
                    # T+ 规则：若 tplus=1，将当日买入计入锁定，并从可卖数量中抵消同额增量
                    tplus = self._infer_tplus_from_info(info)
                    if tplus == 1:
                        # today_buy_t1 仅记录当日买入；抵消 update_position 对 closeable 的递增
                        position.today_buy_t1 = getattr(position, 'today_buy_t1', 0) + trade_amount
                        position.closeable_amount = max(0, position.closeable_amount - trade_amount)
                    position.update_price(current_price)
                    log.info(f"买入 {order.security}: {trade_amount} 股, 委托价 {fund_check_price:.{price_decimals}f}, 成交价 {trade_price:.{price_decimals}f}, 费用 {commission+tax:.2f}")
                else:
                    # 卖出：检查并执行
                    position = self.context.portfolio.positions[order.security]
                    # 增加资金（卖出释放资金，不需锁定）
                    self.context.portfolio.available_cash += self._round_half_up((trade_value - commission - tax), 2)
                    
                    # 更新持仓
                    position.update_position(-trade_amount, trade_price)
                    if position.total_amount == 0:
                        del self.context.portfolio.positions[order.security]
                    else:
                        position.update_price(current_price)
                    log.info(f"卖出 {order.security}: {trade_amount} 股, 成交价 {trade_price:.{price_decimals}f}, 费用 {commission+tax:.2f}")
                
                # 记录交易
                trade = Trade(
                    order_id=order.order_id,
                    security=order.security,
                    amount=trade_amount if is_buy else -trade_amount,
                    price=trade_price,
                    time=current_dt,
                    commission=commission,
                    tax=tax
                )
                self.trades.append(trade)
                
                # 标记订单完成
                order.price = trade_price
                order.amount = trade_amount if is_buy else -trade_amount
                order.status = OrderStatus.filled
                order.filled = trade_amount
                
            except Exception as e:
                log.error(f"处理订单失败: {order.security}, 错误: {e}")
                order.status = OrderStatus.rejected
        
        # 清空订单队列
        clear_order_queue()
        
        # 更新账户价值
        self.context.portfolio.update_value()
    
    def _calculate_order_amount(self, order, current_price: float) -> int:
        """计算订单实际数量"""
        # 普通订单
        if not hasattr(order, '_target_amount') and not hasattr(order, '_target_value') and not hasattr(order, '_is_target_amount') and not hasattr(order, '_is_target_value'):
            return order.amount if order.is_buy else -order.amount
        
        # 目标数量订单
        if hasattr(order, '_is_target_amount') and order._is_target_amount:
            target = order._target_amount
            current = 0
            if order.security in self.context.portfolio.positions:
                current = self.context.portfolio.positions[order.security].total_amount
            return target - current
        
        # 目标价值订单
        if hasattr(order, '_is_target_value') and order._is_target_value:
            target_value = order._target_value
            current_value = 0
            if order.security in self.context.portfolio.positions:
                position = self.context.portfolio.positions[order.security]
                current_value = position.total_amount * current_price
            
            diff_value = target_value - current_value
            return int(diff_value / current_price)
        
        # 按价值订单
        if hasattr(order, '_target_value'):
            return int(order._target_value / current_price) if order.is_buy else -int(order._target_value / current_price)
        
        return order.amount if order.is_buy else -order.amount
    
    def _update_positions(self):
        """更新持仓价格（使用收盘价）"""
        if not self.context.portfolio.positions:
            return
        
        securities = list(self.context.portfolio.positions.keys())
        try:
            # 获取多个标的的收盘价
            # 返回的DataFrame: index=时间, columns=标的代码（如果只有一个字段）
            df = api_get_price(
                security=securities,
                end_date=self.context.current_dt,
                frequency='daily',
                fields=['close'],
                count=1,
                fq='none'
            )
            
            if not df.empty:
                long_df = None
                if ('code' in df.columns) and ('close' in df.columns):
                    long_df = df
                elif isinstance(df.columns, pd.MultiIndex):
                    try:
                        level1 = list(df.columns.get_level_values(1))
                        if ('code' in level1) and ('close' in level1):
                            long_df = df.copy()
                            long_df.columns = long_df.columns.get_level_values(1)
                    except Exception:
                        long_df = None
                # 取最后一行（最新数据）
                last_row = df.iloc[-1]
                
                # 更新每个标的的价格
                for security in securities:
                    try:
                        close_price = None
                        if long_df is not None:
                            # 长表：列包含 time/code/close
                            if 'code' not in long_df.columns or 'close' not in long_df.columns:
                                log.error(f"{security} 长表行情列缺失，列={list(long_df.columns)}")
                                continue
                            sub = long_df[long_df['code'] == security]
                            if sub.empty:
                                log.error(f"{security} 无法在长表行情中找到 close 价格，列={list(long_df.columns)}")
                                continue
                            if 'time' in sub.columns:
                                sub = sub.sort_values('time')
                            close_price = sub.iloc[-1].get('close')
                        else:
                            # 对于单个字段，列名就是标的代码
                            # 对于多个字段，列名是MultiIndex
                            if security in df.columns:
                                close_price = last_row[security]
                            elif ('close', security) in df.columns:
                                # MultiIndex格式
                                close_price = last_row[('close', security)]
                            else:
                                log.error(f"{security} 无法匹配收盘价列，列={list(df.columns)}")
                                continue
                        
                        if pd.notna(close_price) and close_price > 0:
                            self.context.portfolio.positions[security].update_price(float(close_price))
                    except Exception as e:
                        log.debug(f"更新{security}价格失败: {e}")
        except Exception as e:
            log.debug(f"更新持仓价格失败: {e}")
        
        self.context.portfolio.update_value()
    
    def _record_daily(self):
        """记录每日数据"""
        portfolio = self.context.portfolio
        
        record = {
            'date': self.context.current_dt,
            'total_value': portfolio.total_value,
            'cash': portfolio.available_cash,
            'positions_value': portfolio.positions_value,
            'returns': portfolio.total_value - (self.start_total_value if self.start_total_value is not None else self.initial_cash),
            'returns_pct': (portfolio.total_value / (self.start_total_value if self.start_total_value is not None else self.initial_cash) - 1) * 100,
        }
        
        self.daily_records.append(record)
        
        log.info(f"账户总值: {portfolio.total_value:,.2f}, 现金: {portfolio.available_cash:,.2f}, 持仓市值: {portfolio.positions_value:,.2f}")
        log.info(f"累计收益率: {record['returns_pct']:.2f}%")

    # 新增：记录每日持仓快照（在更新收盘价后调用）
    def _record_daily_positions(self):
        portfolio = self.context.portfolio
        current_dt = self.context.current_dt
        for code, pos in portfolio.positions.items():
            self.daily_positions.append({
                'date': current_dt,
                'code': code,
                'amount': pos.total_amount,
                'closeable_amount': pos.closeable_amount,
                'avg_cost': pos.avg_cost,
                'acc_avg_cost': pos.acc_avg_cost,
                'price': pos.price,
                'value': pos.value,
            })


    def _load_benchmark_data(self, benchmark: str):
        """
        加载基准数据
        
        注意：直接使用原始 jq.get_price，不经过包装函数，
        因为基准数据是预加载整个回测期间的数据，不需要未来数据检测
        """
        try:
            log.info(f"加载基准数据: {benchmark}")
            
            # 通过当前数据提供者直接获取（不经过包装，避免未来数据检测），一次性加载全区间收盘价
            provider = get_data_provider()
            self.benchmark_data = provider.get_price(
                security=benchmark,
                start_date=self.start_date,
                end_date=self.end_date,
                frequency='daily',
                fields=['close']
            )
        except Exception as e:
            log.warning(f"加载基准数据失败: {e}")
    
    def _generate_empty_results(self) -> Dict[str, Any]:
        """生成空回测结果（没有交易日的情况）"""
        results = {
            'summary': {
                '策略收益': '0.00%',
                '策略年化收益': '0.00%',
                '最大回撤': '0.00%',
                '夏普比率': '0.00',
                '日胜率': '0.00%',
                '交易胜率': '0.00%',
                '交易天数': 0,
                '初始资金': f'{self.initial_cash:,.2f}',
                '最终资金': f'{self.initial_cash:,.2f}',
            },
            'daily_records': pd.DataFrame(columns=['date', 'total_value', 'cash', 'position_value', 'daily_returns']).set_index('date'),
            'trades': [],
            'events': [],
            'daily_positions': pd.DataFrame(columns=['date','code','amount','closeable_amount','avg_cost','acc_avg_cost','price','value']),
            'custom_plot': None,
            'meta': {
                'strategy_file': self.strategy_file,
                'start_date': self.start_date.strftime('%Y-%m-%d'),
                'end_date': self.end_date.strftime('%Y-%m-%d'),
                'algorithm_id': self.algorithm_id,
                'extras': self.extras,
                'runtime_seconds': getattr(self, 'runtime_seconds', 0.0),
                'initial_total_value': float(self.initial_cash),
                'final_total_value': float(self.initial_cash),
            }
        }
        
        # 打印摘要
        log.info("\n" + "=" * 60)
        log.info("回测结果摘要（无交易日）")
        log.info("=" * 60)
        for key, value in results['summary'].items():
            log.info(f"{key}: {value}")
        log.info("=" * 60)
        
        return results
    
    def _generate_results(self) -> Dict[str, Any]:
        """生成回测结果"""
        df = pd.DataFrame(self.daily_records)
        
        # 检查是否有数据
        if df.empty or 'date' not in df.columns:
            log.warning("没有回测数据，返回空结果")
            return self._generate_empty_results()
        
        df.set_index('date', inplace=True)
        
        # 计算日收益率
        df['daily_returns'] = df['total_value'].pct_change()
        
        # 基本统计
        total_returns = (df['total_value'].iloc[-1] / self.initial_cash - 1) * 100
        trading_days = len(df)
        years = trading_days / 250
        annual_returns = (pow(df['total_value'].iloc[-1] / self.initial_cash, 1/years) - 1) * 100 if years > 0 else 0
        
        # 最大回撤
        cummax = df['total_value'].expanding().max()
        drawdown = (df['total_value'] - cummax) / cummax * 100
        max_drawdown = drawdown.min()
        
        # 夏普比率（假设无风险利率为3%）
        risk_free_rate = 0.03 / 250
        excess_returns = df['daily_returns'] - risk_free_rate
        sharpe_ratio = np.sqrt(250) * excess_returns.mean() / excess_returns.std() if excess_returns.std() > 0 else 0
        
        # 日胜率（仅作为摘要展示，完整指标在 analysis.calculate_metrics）
        winning_days = (df['daily_returns'] > 0).sum()
        win_rate_daily = winning_days / trading_days * 100 if trading_days > 0 else 0

        # 交易胜率（基于卖出回合）
        try:
            from .analysis import _compute_trade_win_stats
            trade_stats = _compute_trade_win_stats(self.trades)
        except Exception:
            trade_stats = {'交易胜率': 0.0}

        # 构建自定义绘图数据（优先使用 g.custom_plot，其次聚合 record() 收集的序列）
        custom_plot_obj = getattr(g, 'custom_plot', None)
        if custom_plot_obj is None and hasattr(g, 'record_series') and g.record_series:
            try:
                series_map = {}
                for key, items in g.record_series.items():
                    # 按日期聚合最后一次记录
                    by_date = {}
                    for dt, val in items:
                        if isinstance(val, (int, float, np.number)):
                            by_date[dt] = float(val)
                    if by_date:
                        series_map[key] = pd.Series(by_date)
                if series_map:
                    custom_plot_obj = pd.DataFrame(series_map).sort_index()
            except Exception as e:
                log.warning(f'构建自定义图数据失败: {e}')
        
        results = {
            'summary': {
                '策略收益': f'{total_returns:.2f}%',
                '策略年化收益': f'{annual_returns:.2f}%',
                '最大回撤': f'{max_drawdown:.2f}%',
                '夏普比率': f'{sharpe_ratio:.2f}',
                '日胜率': f'{win_rate_daily:.2f}%',
                '交易胜率': f"{float(trade_stats.get('交易胜率', 0.0)):.2f}%",
                '交易天数': trading_days,
                '初始资金': f'{(self.start_total_value if self.start_total_value is not None else self.initial_cash):,.2f}',
                '最终资金': f'{df["total_value"].iloc[-1]:,.2f}',
            },
            'daily_records': df,
            'trades': self.trades,
            'events': self.events,
            'daily_positions': pd.DataFrame(self.daily_positions) if self.daily_positions else pd.DataFrame(columns=['date','code','amount','closeable_amount','avg_cost','acc_avg_cost','price','value']),
            'custom_plot': custom_plot_obj,
            'meta': {
                'strategy_file': self.strategy_file,
                'start_date': self.start_date.strftime('%Y-%m-%d'),
                'end_date': self.end_date.strftime('%Y-%m-%d'),
                'algorithm_id': self.algorithm_id,
                'extras': self.extras,
                'runtime_seconds': self.runtime_seconds,
                'initial_total_value': float(self.start_total_value if self.start_total_value is not None else self.initial_cash),
                'final_total_value': float(df['total_value'].iloc[-1] if len(df) > 0 else 0.0),
            }
        }
        
        # 打印摘要
        log.info("\n" + "=" * 60)
        log.info("回测结果摘要")
        log.info("=" * 60)
        for key, value in results['summary'].items():
            log.info(f"{key}: {value}")
        log.info("=" * 60)
        
        return results


def create_backtest(
    strategy_file: str,
    start_date: str,
    end_date: str,
    frequency: str = 'day',
    initial_cash: float = 100000,
    benchmark: Optional[str] = None,
    log_file: Optional[str] = None,
    extras: Optional[Dict[str, Any]] = None,
    initial_positions: Optional[List[Dict[str, Any]]] = None,
    algorithm_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    创建并运行回测
    
    Args:
        strategy_file: 策略文件路径
        start_date: 回测开始日期 'YYYY-MM-DD'
        end_date: 回测结束日期 'YYYY-MM-DD'
        frequency: 回测频率 ('day' or 'minute')
        initial_cash: 初始资金
        benchmark: 基准标的
        log_file: 日志文件路径，默认为None（只在控制台显示，不写文件）
        extras: 额外参数字典
        initial_positions: 初始持仓列表
        algorithm_id: 算法ID（可选）
    """
    engine = BacktestEngine(
        strategy_file=strategy_file,
        start_date=start_date,
        end_date=end_date,
        frequency=frequency,
        initial_cash=initial_cash,
        benchmark=benchmark,
        log_file=log_file,
        extras=extras,
        initial_positions=initial_positions,
        algorithm_id=algorithm_id,
    )
    
    return engine.run()


__all__ = ['BacktestEngine', 'create_backtest']


# 新增：应用初始持仓（辅助方法）
def _safe_last_close(df: pd.DataFrame, code: str) -> Optional[float]:
    try:
        if df is None or df.empty:
            return None
        last_row = df.iloc[-1]
        # 支持三种列结构：['close'], [code], [('close', code)]
        if 'close' in df.columns:
            val = last_row['close']
        elif code in df.columns:
            val = last_row[code]
        elif ('close', code) in df.columns:
            val = last_row[('close', code)]
        else:
            return None
        return float(val) if pd.notna(val) else None
    except Exception:
        return None

# 将方法挂到类中
BacktestEngine._safe_last_close = staticmethod(_safe_last_close)

def _apply_initial_positions(self):
    """根据 initial_positions 注入初始持仓，不消耗现金。"""
    try:
        if not self.initial_positions:
            return
        provider = get_data_provider()
        portfolio = self.context.portfolio
        for item in self.initial_positions:
            try:
                code = str(item.get('security'))
                amount = int(item.get('amount') or 0)
                avg_cost = item.get('avg_cost')
                if amount <= 0 or not code:
                    continue
                # 获取起始日前的/当日收盘价作为估值价格
                df = provider.get_price(
                    security=code,
                    end_date=self.start_date,
                    frequency='daily',
                    fields=['close'],
                    count=1,
                    fq='pre'
                )
                close_price = self._safe_last_close(df, code)
                if close_price is None:
                    close_price = float(avg_cost) if avg_cost is not None else 0.0
                # 平均成本
                ac = float(avg_cost) if avg_cost is not None else float(close_price or 0.0)
                # 创建或更新持仓
                if code not in portfolio.positions:
                    portfolio.positions[code] = Position(security=code)
                pos = portfolio.positions[code]
                pos.total_amount = amount
                pos.closeable_amount = amount
                pos.avg_cost = ac
                pos.acc_avg_cost = ac
                pos.price = float(close_price or ac)
                pos.value = pos.total_amount * pos.price
            except Exception as ie:
                log.debug(f"初始化持仓失败 {item}: {ie}")
        # 更新账户总值（现金 + 持仓）
        portfolio.update_value()
    except Exception as e:
        log.warning(f"应用初始持仓失败: {e}")

# 绑定到类
BacktestEngine._apply_initial_positions = _apply_initial_positions
