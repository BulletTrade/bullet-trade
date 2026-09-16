"""
回测引擎

实现策略回测的核心逻辑
"""

import importlib.util
import inspect as _inspect
import re
import sys
import time
from dataclasses import dataclass, replace
from datetime import date, datetime
from datetime import time as Time
from datetime import timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

# from pathlib import Path  # removed: use local import in _setup_log_file
# Optional import: jqdatasdk for fallback in strategy wrapper
# try:
#     import jqdatasdk as jq
# except Exception:
jq = None

from ..core.exceptions import FutureDataError
from ..data.api import get_data_provider
from ..data.api import get_extras as _data_api_get_extras
from ..data.api import get_price as _data_api_get_price
from ..data.api import get_security_info, set_current_context
from ..data.api import internal_replay_prefetch
from ..data.tick_replay import (
    TickDataMissingError,
    TickDayStream,
    TickSnapshot,
    load_tick_day,
)
from .futures_account import (
    LONG,
    SHORT,
    ContractSpecError,
    ContractSpecTable,
    FuturesAccount,
    InsufficientMarginError,
    OverCloseError,
    is_futures_security,
)
from .globals import g, log, reset_globals
from .models import (
    CompatOrderStatus,
    Context,
    Order,
    OrderStatus,
    OrderStyle,
    Portfolio,
    Position,
    SecurityUnitData,
    SubPortfolio,
    Trade,
)
from .orders import LimitOrderStyle, MarketOrderStyle, clear_order_queue, get_order_queue
from .scheduler import (
    generate_daily_schedule,
    get_market_periods,
    get_tasks,
    get_trade_calendar,
    set_trade_calendar,
    unschedule_all,
)
from .settings import (
    FixedSlippage,
    OrderCost,
    PriceRelatedSlippage,
    StepRelatedSlippage,
    get_settings,
    get_subportfolio_configs,
    reset_settings,
)


def api_get_price(*args: Any, **kwargs: Any) -> pd.DataFrame:
    """
    引擎级别的 get_price 包装，用于捕获 FutureDataError 并以 warning 记录。
    """
    try:
        return _data_api_get_price(*args, **kwargs)
    except FutureDataError as exc:
        log.warning("avoid_future_data 拦截未来数据: %s", exc)
        return pd.DataFrame()


from ..utils.env_loader import get_live_trade_config
from . import pricing
from .runtime import set_current_engine

_BASE_MARKET_SLIPPAGE = 0.00246
_DEFAULT_MARKET_BUY_PERCENT = _BASE_MARKET_SLIPPAGE / 2
_DEFAULT_MARKET_SELL_PERCENT = -_BASE_MARKET_SLIPPAGE / 2

PRE_MARKET_OFFSET = timedelta(minutes=30)

_TICK_BACKTEST_FREQUENCIES: Set[str] = {"tick", "ticks"}


def _is_tick_frequency(frequency: Any) -> bool:
    """判断运行频率是否为 tick 回放。

    Args:
        frequency: 运行频率字符串。

    Returns:
        bool: 归一化后落在 tick 频率集合时为 True。
    """

    return str(frequency or "").strip().lower() in _TICK_BACKTEST_FREQUENCIES


@dataclass
class _DayRunState:
    """单个交易日回放过程中的可变状态。

    Attributes:
        trade_day: 当前交易日（含时间的调度基准）。
        previous_event_dt: 上一个已执行事件的时刻。
        dividends_applied: 当日权益变动是否已处理，保证只跑一次。
    """

    trade_day: datetime
    previous_event_dt: Optional[datetime] = None
    dividends_applied: bool = False


def _iter_security_code_candidates(security: Optional[str]) -> List[str]:
    """生成证券代码兼容候选，优先保留原始输入。"""
    if not security:
        return []
    if "." not in security:
        return [security]

    code, suffix = security.split(".", 1)
    suffix = suffix.upper()
    candidates = [security]
    if suffix == "SH":
        candidates.append(f"{code}.XSHG")
    elif suffix == "XSHG":
        candidates.append(f"{code}.SH")
    elif suffix == "SZ":
        candidates.append(f"{code}.XSHE")
    elif suffix == "XSHE":
        candidates.append(f"{code}.SZ")
    return list(dict.fromkeys(candidates))


class BacktestEngine:
    """回测引擎"""

    def __init__(
        self,
        strategy_file: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: str = "day",
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
        handle_tick: Optional[Callable] = None,
        process_initialize: Optional[Callable] = None,
        data_session_config: Optional[Dict[str, Any]] = None,
    ):
        """
        初始化回测引擎

        Args:
            strategy_file: 策略文件路径（与函数参数二选一）
            start_date: 回测开始日期 'YYYY-MM-DD'（可在run()中指定）
            end_date: 回测结束日期 'YYYY-MM-DD'（可在run()中指定）
            frequency: 回测频率 ('day'、'minute' 或 'tick')
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
            handle_tick: tick 频率下每笔快照调用的函数
            process_initialize: 实盘初始化函数
            data_session_config: 回测数据会话配置，仅用于回测内的临时性能优化
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
        self.orders: Dict[str, Order] = {}  # 当日订单快照
        self.events = []  # 事件记录（分红/拆分）
        self._processed_dividend_keys = set()  # 已处理的分红事件键（避免重复处理）
        self._split_price_adjustments: Dict[Tuple[str, date], Dict[str, float]] = {}
        self.benchmark_data = None  # 基准数据
        # 新增：每日持仓快照记录
        self.daily_positions = []
        # 新增：用户自定义参数与初始持仓
        self.extras = extras or None
        self.initial_positions = initial_positions or None
        self.algorithm_id = algorithm_id
        self.data_session_config = data_session_config or None
        # 新增：收益计算基准（首次总资产）
        self.start_total_value: Optional[float] = None
        # 新增：回测运行耗时（秒）
        self.runtime_seconds: Optional[float] = None
        self.run_started_at: Optional[str] = None
        self.run_finished_at: Optional[str] = None
        self._benchmark_base_price: Optional[float] = None
        self._trade_calendar: Dict[date, Dict[str, Any]] = {}
        self._trade_seq = 0
        market_cfg = get_live_trade_config()
        self._market_buy_percent = float(
            market_cfg.get("market_buy_price_percent", _DEFAULT_MARKET_BUY_PERCENT)
        )
        self._market_sell_percent = float(
            market_cfg.get("market_sell_price_percent", _DEFAULT_MARKET_SELL_PERCENT)
        )
        # 期货合约规格表：规格按合约代码分层解析（显式注册 > 配置文件 > 远端 > 内置兜底），
        # 保证金率由 set_option('futures_margin_rate') 与 set_option('futures_margin_rate.<品种>') 提供
        self._futures_spec_table = ContractSpecTable(enable_remote=True)
        self._futures_cost_warned = False

        # tick 回放状态：订阅集合、当日事件流、最新快照
        self.handle_tick_func: Optional[Callable] = handle_tick
        self._tick_subscriptions: Set[str] = set()
        self._tick_streams: Dict[str, TickDayStream] = {}
        self._tick_snapshots: Dict[str, TickSnapshot] = {}
        self._tick_day: Optional[date] = None

    @staticmethod
    def _amount_from_value(value: float, price: float) -> int:
        """把市值按价格换算为股数，并修正浮点数贴近整数时的截断误差。

        Args:
            value: 需要换算的市值，非正数按 0 股处理。
            price: 换算价格，非正数按 0 股处理。

        Returns:
            int: 不超过目标市值的股数；若浮点计算结果极接近整数，则返回该整数。
        """

        if value <= 0 or price <= 0:
            return 0
        raw_amount = float(value) / float(price)
        nearest_amount = round(raw_amount)
        tolerance = max(1e-9, abs(raw_amount) * 1e-12)
        if abs(raw_amount - nearest_amount) <= tolerance:
            return int(nearest_amount)
        return int(raw_amount)

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
                self.initialize_func = getattr(strategy_module, "initialize", None)
                self.handle_data_func = getattr(strategy_module, "handle_data", None)
                self.before_trading_start_func = getattr(
                    strategy_module, "before_trading_start", None
                )
                self.after_trading_end_func = getattr(strategy_module, "after_trading_end", None)
                self.process_initialize_func = getattr(strategy_module, "process_initialize", None)
                self.handle_tick_func = (
                    getattr(strategy_module, "handle_tick", None) or self.handle_tick_func
                )

                log.info("策略文件加载成功")
            else:
                raise ValueError(f"无法加载策略文件: {self.strategy_file}")

        except Exception as e:
            log.error(f"加载策略失败: {e}")
            raise

    @staticmethod
    def _is_daily_backtest_frequency(frequency: Any) -> bool:
        """判断回测频率是否属于日频写法。"""
        normalized = str(frequency or "day").strip().lower()
        return normalized in {"day", "daily", "1d", "1day", "d"}

    def _warn_every_bar_minute_semantics(self, tasks: Sequence[Any]) -> None:
        """在日频回测注册 every_bar 时提示分钟级触发语义。"""
        if not self._is_daily_backtest_frequency(self.frequency):
            return
        has_every_bar = any(
            str(getattr(task, "time", "")).strip().lower() == "every_bar" for task in tasks
        )
        if not has_every_bar:
            return
        log.warning(
            '检测到 run_daily(..., time="every_bar")，为保持回测与实盘一致，'
            "当前回测会按交易时段每分钟触发。"
            '如只希望每天执行一次，请改用 time="open" 或具体时间；'
            '如希望语义更直白，可使用 time="every_minute"。'
        )

    def _inject_globals(self, module):
        """向策略模块注入全局变量和函数"""
        import datetime as _datetime
        import math as _math
        import random as _random
        import time as _time

        from ..data import api as wrapped_api
        from ..research.io import read_file as _read_file
        from ..research.io import write_file as _write_file
        from ..utils.strategy_helpers import prettytable_print_df, print_portfolio_info
        from .api import get_current_tick as _get_current_tick
        from .api import get_open_orders as _get_open_orders
        from .api import get_orders as _get_orders
        from .api import get_trades as _get_trades
        from .api import require_data_capabilities as _require_data_capabilities
        from .api import subscribe as _subscribe
        from .api import unsubscribe as _unsubscribe
        from .api import unsubscribe_all as _unsubscribe_all
        from .globals import g, log
        from .notifications import send_msg as _send_msg
        from .notifications import set_message_handler as _set_message_handler
        from .orders import (
            LimitOrderStyle,
            MarketOrderStyle,
            cancel_all_orders,
            cancel_order,
            order,
            order_target,
            order_target_value,
            order_value,
        )
        from .scheduler import run_daily, run_monthly, run_weekly, unschedule_all
        from .settings import (
            FixedSlippage,
            OrderCost,
            PerTrade,
            PriceRelatedSlippage,
            StepRelatedSlippage,
            SubPortfolioConfig,
            set_benchmark,
            set_commission,
            set_option,
            set_order_cost,
            set_slippage,
            set_subportfolios,
            set_universe,
        )

        try:
            import talib as _talib  # type: ignore
        except Exception:  # talib 可能未安装

            class _TalibProxy:
                def __getattr__(self, name):
                    raise ImportError("TA-Lib 未安装。请先 `pip install TA-Lib` 并确保本机已安装对应的C库。")

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
        module.set_subportfolios = set_subportfolios
        module.SubPortfolioConfig = SubPortfolioConfig
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
        module.require_data_capabilities = _require_data_capabilities

        # 注入订单函数
        module.order = order
        module.order_value = order_value
        module.order_target = order_target
        module.order_target_value = order_target_value
        module.MarketOrderStyle = MarketOrderStyle
        module.LimitOrderStyle = LimitOrderStyle
        module.get_open_orders = _get_open_orders
        module.get_orders = _get_orders
        module.get_trades = _get_trades
        module.cancel_order = cancel_order
        module.cancel_all_orders = cancel_all_orders

        # 注入调度函数
        module.run_daily = run_daily
        module.run_weekly = run_weekly
        module.run_monthly = run_monthly
        module.unschedule_all = unschedule_all

        # 注入数据模型与订单状态别名（策略常直接引用这些名字做判断）
        module.OrderStatus = CompatOrderStatus
        module.OrderStyle = OrderStyle
        module.Context = Context
        module.Portfolio = Portfolio
        module.SubPortfolio = SubPortfolio
        module.Position = Position
        module.Order = Order
        module.Trade = Trade
        module.SecurityUnitData = SecurityUnitData

        # 注入 record 函数（模拟聚宽，用于自定义绘图数据）
        def record(**kwargs):
            dt = self.context.current_dt
            # 初始化容器
            if not hasattr(g, "record_series") or g.record_series is None:
                g.record_series = {}
            for key, value in kwargs.items():
                series = g.record_series.get(key)
                if series is None:
                    g.record_series[key] = []
                g.record_series[key].append((dt, value))

        module.record = record

        # 注入包装过的数据函数（支持真实价格和未来数据检测）
        module.get_price = wrapped_api.get_price
        module.history = wrapped_api.history
        module.attribute_history = wrapped_api.attribute_history
        module.get_bars = wrapped_api.get_bars
        module.get_ticks = wrapped_api.get_ticks
        module.get_current_tick = wrapped_api.get_current_tick
        module.get_current_data = wrapped_api.get_current_data
        module.get_extras = wrapped_api.get_extras
        module.get_fundamentals = wrapped_api.get_fundamentals
        module.get_fundamentals_continuously = wrapped_api.get_fundamentals_continuously
        module.get_trade_days = wrapped_api.get_trade_days
        module.get_trade_day = wrapped_api.get_trade_day
        module.get_all_securities = wrapped_api.get_all_securities
        module.get_security_info = wrapped_api.get_security_info
        module.get_fund_info = wrapped_api.get_fund_info
        module.get_index_stocks = wrapped_api.get_index_stocks
        module.get_index_weights = wrapped_api.get_index_weights
        module.get_industry_stocks = wrapped_api.get_industry_stocks
        module.get_industry = wrapped_api.get_industry
        module.get_concept_stocks = wrapped_api.get_concept_stocks
        module.get_concept = wrapped_api.get_concept
        module.get_margincash_stocks = wrapped_api.get_margincash_stocks
        module.get_marginsec_stocks = wrapped_api.get_marginsec_stocks
        module.get_dominant_future = wrapped_api.get_dominant_future
        module.get_future_contracts = wrapped_api.get_future_contracts
        module.get_billboard_list = wrapped_api.get_billboard_list
        module.get_locked_shares = wrapped_api.get_locked_shares
        module.get_split_dividend = wrapped_api.get_split_dividend
        module.set_data_provider = wrapped_api.set_data_provider
        module.get_data_provider = wrapped_api.get_data_provider

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

        jq_mod = _types.ModuleType("jqdata")
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
        jq_mod.history = wrapped_api.history
        jq_mod.attribute_history = wrapped_api.attribute_history
        jq_mod.get_bars = wrapped_api.get_bars
        jq_mod.get_ticks = wrapped_api.get_ticks
        jq_mod.get_current_data = wrapped_api.get_current_data
        jq_mod.get_extras = wrapped_api.get_extras
        jq_mod.get_fundamentals = wrapped_api.get_fundamentals
        jq_mod.get_fundamentals_continuously = wrapped_api.get_fundamentals_continuously
        jq_mod.get_trade_days = wrapped_api.get_trade_days
        jq_mod.get_trade_day = wrapped_api.get_trade_day
        jq_mod.get_all_securities = wrapped_api.get_all_securities
        jq_mod.get_security_info = wrapped_api.get_security_info
        jq_mod.get_fund_info = wrapped_api.get_fund_info
        jq_mod.get_index_stocks = wrapped_api.get_index_stocks
        jq_mod.get_index_weights = wrapped_api.get_index_weights
        jq_mod.get_industry_stocks = wrapped_api.get_industry_stocks
        jq_mod.get_industry = wrapped_api.get_industry
        jq_mod.get_concept_stocks = wrapped_api.get_concept_stocks
        jq_mod.get_concept = wrapped_api.get_concept
        jq_mod.get_margincash_stocks = wrapped_api.get_margincash_stocks
        jq_mod.get_marginsec_stocks = wrapped_api.get_marginsec_stocks
        jq_mod.get_dominant_future = wrapped_api.get_dominant_future
        jq_mod.get_future_contracts = wrapped_api.get_future_contracts
        jq_mod.get_billboard_list = wrapped_api.get_billboard_list
        jq_mod.get_locked_shares = wrapped_api.get_locked_shares
        jq_mod.get_split_dividend = wrapped_api.get_split_dividend
        jq_mod.set_data_provider = wrapped_api.set_data_provider
        jq_mod.get_data_provider = wrapped_api.get_data_provider
        # 设置/参数 API
        jq_mod.set_benchmark = set_benchmark
        jq_mod.set_order_cost = set_order_cost
        jq_mod.set_commission = set_commission
        jq_mod.set_universe = set_universe
        jq_mod.set_slippage = set_slippage
        jq_mod.set_option = set_option
        jq_mod.set_subportfolios = set_subportfolios
        jq_mod.SubPortfolioConfig = SubPortfolioConfig
        jq_mod.OrderCost = OrderCost
        jq_mod.PerTrade = PerTrade
        jq_mod.FixedSlippage = FixedSlippage
        jq_mod.PriceRelatedSlippage = PriceRelatedSlippage
        jq_mod.StepRelatedSlippage = StepRelatedSlippage
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
        jq_mod.get_open_orders = _get_open_orders
        jq_mod.get_orders = _get_orders
        jq_mod.get_trades = _get_trades
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
        jq_mod.require_data_capabilities = _require_data_capabilities
        # 数据模型与订单状态别名
        jq_mod.OrderStatus = CompatOrderStatus
        jq_mod.OrderStyle = OrderStyle
        jq_mod.Context = Context
        jq_mod.Portfolio = Portfolio
        jq_mod.SubPortfolio = SubPortfolio
        jq_mod.Position = Position
        jq_mod.Order = Order
        jq_mod.Trade = Trade
        jq_mod.SecurityUnitData = SecurityUnitData
        # 注入并注册
        sys.modules["jqdata"] = jq_mod
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
        运行回测，并在单次运行内激活可选回测数据会话。

        Args:
            start_date: 回测开始日期（可选，优先使用此参数）
            end_date: 回测结束日期（可选，优先使用此参数）
            capital_base: 初始资金（可选，优先使用此参数）
            frequency: 回测频率（可选，优先使用此参数）
            benchmark: 基准标的（可选，优先使用此参数）

        Returns:
            Dict[str, Any]: 回测结果

        Side Effects:
            启用回测数据会话时，会在 finally 中关闭会话并恢复上下文。
        """
        from ..data.backtest_session import (
            create_backtest_data_session,
            reset_current_backtest_data_session,
            set_current_backtest_data_session,
        )

        if start_date:
            self.start_date = pd.to_datetime(start_date)
        if end_date:
            self.end_date = pd.to_datetime(end_date)
        if capital_base:
            self.initial_cash = capital_base
        if frequency:
            self.frequency = frequency

        provider_name = None
        try:
            provider_name = getattr(get_data_provider(), "name", None)
        except Exception:
            provider_name = None
        data_session = create_backtest_data_session(
            overrides=self.data_session_config,
            start_date=self.start_date,
            end_date=self.end_date,
            frequency=self.frequency,
            provider_name=provider_name,
        )
        token = set_current_backtest_data_session(data_session)
        try:
            results = self._run_impl(
                start_date=None,
                end_date=None,
                capital_base=None,
                frequency=None,
                benchmark=benchmark,
            )
            if data_session.config.enabled:
                results["backtest_data_session"] = data_session.to_manifest()
            return results
        finally:
            try:
                data_session.close()
            finally:
                reset_current_backtest_data_session(token)

    def _run_impl(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        capital_base: Optional[float] = None,
        frequency: Optional[str] = None,
        benchmark: Optional[str] = None,
    ):
        """
        执行回测主体流程。

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
        self.run_started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.run_finished_at = None
        self.runtime_seconds = None
        self._benchmark_base_price = None

        log.info("=" * 60)
        log.info(
            f"开始回测: {self.start_date.strftime('%Y-%m-%d')} 至 {self.end_date.strftime('%Y-%m-%d')}"
        )
        log.info(f"初始资金: {self.initial_cash:,.2f}")
        log.info(f"回测频率: {self.frequency}")
        if self.log_file:
            log.info(f"日志文件: {self.log_file}")
        log.info("=" * 60)

        # 设置回测频率到 settings（供 scheduler 使用）
        from .settings import set_option

        set_option("backtest_frequency", self.frequency)

        # 加载策略
        self.load_strategy()

        # 注意：load_strategy 内部会调用 reset_settings()，需要重新注入频率配置
        set_option("backtest_frequency", self.frequency)

        # 初始化上下文
        self.context = Context(
            portfolio=Portfolio(
                total_value=self.initial_cash,
                available_cash=self.initial_cash,
                starting_cash=self.initial_cash,
            ),
            current_dt=self.start_date,
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
                "algorithm_id": self.algorithm_id,
                "start_date": self.start_date.strftime("%Y-%m-%d"),
                "end_date": self.end_date.strftime("%Y-%m-%d"),
                "frequency": self.frequency,
                "initial_cash": self.initial_cash,
                "extras": self.extras,
                "initial_positions": self.initial_positions,
            }
            # 应用初始持仓（不消耗现金，视为已有持仓）
            self._apply_initial_positions()
            # 设置收益基准为首次总资产（现金+持仓）
            self.start_total_value = float(
                self.context.portfolio.available_cash
                + self.context.portfolio.positions_value
                + self.context.portfolio.locked_cash
            )
        except Exception as e:
            log.warning(f"初始化参数/持仓注入失败: {e}")

        # 调用 initialize
        if self.initialize_func:
            log.info("调用策略初始化函数...")
            try:
                self.initialize_func(self.context)
                log.info("策略初始化完成")
            except Exception as e:
                log.error(f"策略初始化失败: {e}")
                import traceback

                log.error(traceback.format_exc())
                raise

        # 策略可能在 initialize 中声明子账户，需在 Portfolio 建好后补建视图
        self._apply_subportfolio_config()

        # 根据 extras 覆盖 g.xxx（在策略初始化后、process_initialize 前执行）
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

        # 调用 process_initialize，使回测与 live 保持一致
        if self.process_initialize_func:
            log.info("调用策略进程初始化函数...")
            try:
                self.process_initialize_func(self.context)
                log.info("策略进程初始化完成")
            except Exception as e:
                log.error(f"策略进程初始化失败: {e}")
                import traceback

                log.error(traceback.format_exc())
                raise

        # 避免重复：若 before_market_open/market_open 已通过调度注册，则取消直接调用
        try:
            tasks = get_tasks()
            self._warn_every_bar_minute_semantics(tasks)
            if self.before_trading_start_func and any(
                t.func is self.before_trading_start_func for t in tasks
            ):
                log.debug("检测到 before_market_open 已由调度注册，取消重复直接调用")
                self.before_trading_start_func = None
            if self.handle_data_func and any(t.func is self.handle_data_func for t in tasks):
                log.debug("检测到 market_open 已由调度注册，取消重复直接调用")
                self.handle_data_func = None
        except Exception as ex:
            log.warning(f"调度重复检查失败: {ex}")

        # 获取交易日列表（直接使用Provider，避免上下文限制导致只取到起始日）
        provider = get_data_provider()
        trade_days = provider.get_trade_days(start_date=self.start_date, end_date=self.end_date)
        trade_days = [pd.to_datetime(d) for d in trade_days]
        calendar_days: List[date] = [d.date() for d in trade_days]
        try:
            extra_days = provider.get_trade_days(
                end_date=trade_days[0] if trade_days else None, count=60
            )
            if extra_days is None:
                extra_days = []
            elif isinstance(extra_days, np.ndarray):
                extra_days = extra_days.tolist()
            else:
                extra_days = list(extra_days)
            calendar_days.extend(pd.to_datetime(d).date() for d in extra_days)
        except Exception as exc:
            log.debug(f"扩展交易日序列失败: {exc}")
        if trade_days:
            start_day = pd.to_datetime(self.start_date or trade_days[0]).date()
            set_trade_calendar(calendar_days, start_day)
            self._trade_calendar = get_trade_calendar()

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
                    end_date=trade_days[0], count=2  # 包括第一天本身和前一天
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
                self.context.previous_date = trade_days[i - 1].date()
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

            try:
                self._on_futures_day_start()
            except Exception as ex:
                log.debug(f"期货日内口径复位失败: {ex}")

            log.info(f"\n{'=' * 60}")
            log.info(f"交易日: {trade_day.strftime('%Y-%m-%d')} ({i+1}/{len(trade_days)})")
            if self.context.previous_date:
                log.debug(f"前一交易日: {self.context.previous_date}")

            market_periods = get_market_periods()

            # 执行当日调度（open/close等）
            self._run_trading_day(trade_day, market_periods)

            # 先更新收盘价与持仓市值，再记录每日数据，避免“日志总值≠CSV总值”的错位
            self._update_positions()

            # 期货日终结算：盯市入现金、保证金结转到结算价、到期合约了结
            try:
                self._settle_futures_day(trade_day)
            except Exception as ex:
                log.warning(f"期货日终结算失败: {ex}")

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
            self.run_finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
                file_level_name = str(sys_cfg.get("log_file_level", "INFO")).upper()
                file_level = getattr(logging, file_level_name, logging.INFO)
            except Exception:
                file_level = logging.INFO

            # 创建文件处理器
            self.file_handler = logging.FileHandler(self.log_file, mode="w", encoding="utf-8")
            self.file_handler.setLevel(file_level)

            # 设置日志格式
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
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
            from ..data.backtest_session import get_current_backtest_data_session

            data_session = get_current_backtest_data_session()
            if data_session is not None:
                data_session.advance_bar(current_dt)
        except Exception:
            pass
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

        resolver = lambda _ref=None: market_periods
        schedule_map = generate_daily_schedule(
            trade_day,
            trade_calendar=self._trade_calendar,
            market_periods_resolver=resolver,
        )

        day_date = trade_day.date()
        open_dt = datetime.combine(day_date, market_periods[0][0])
        close_dt = datetime.combine(day_date, market_periods[-1][1])
        pre_open_dt = open_dt - PRE_MARKET_OFFSET

        timeline_set = set(schedule_map.keys())
        timeline_set.add(pre_open_dt)
        timeline_set.add(open_dt)
        timeline_set.add(close_dt)

        timeline = sorted(timeline_set)
        state = _DayRunState(trade_day=trade_day, previous_event_dt=self.context.previous_dt)
        anchors = {
            "schedule_map": schedule_map,
            "pre_open_dt": pre_open_dt,
            "open_dt": open_dt,
            "close_dt": close_dt,
            "market_periods": market_periods,
        }

        if self.is_tick_backtest():
            self._run_tick_timeline(timeline=timeline, state=state, **anchors)
            return

        for current_dt in timeline:
            self._execute_time_point(current_dt, state=state, **anchors)

    def _execute_time_point(
        self,
        current_dt: datetime,
        *,
        schedule_map: Dict[datetime, List[Any]],
        pre_open_dt: datetime,
        open_dt: datetime,
        close_dt: datetime,
        market_periods: Sequence[Tuple[Time, Time]],
        state: _DayRunState,
        tick_snapshot: Optional[TickSnapshot] = None,
    ) -> None:
        """执行时间轴上的单个时刻。

        Args:
            current_dt: 本时刻的回放时间。
            schedule_map: 当日调度任务映射。
            pre_open_dt: 盘前回调时刻。
            open_dt: 开盘时刻。
            close_dt: 收盘时刻。
            market_periods: 当日交易时段。
            state: 当日回放状态，跨时刻携带时钟与分红标记。
            tick_snapshot: 本时刻投递的 tick 快照；非 tick 事件时为 None。
        """

        self._update_current_time(current_dt, state.previous_event_dt)
        state.previous_event_dt = current_dt

        if not state.dividends_applied:
            try:
                self._apply_dividends_for_day(state.trade_day)
            except Exception as e:
                log.warning(f"盘前分红处理失败: {e}")
            state.dividends_applied = True

        tasks = schedule_map.get(current_dt, [])
        for task in tasks:
            try:
                log.debug(f"执行定时任务: {task.func.__name__}")
                task.func(self.context)
            except Exception as e:
                log.error(f"定时任务执行失败 {task.func.__name__}: {e}")
                import traceback

                log.error(traceback.format_exc())

        if current_dt == pre_open_dt and tick_snapshot is None and self.before_trading_start_func:
            try:
                self.before_trading_start_func(self.context)
            except Exception as e:
                log.error(f"盘前函数执行失败: {e}")
                import traceback

                log.error(traceback.format_exc())

        # 首笔 tick 可能正好落在开盘时刻，开盘回调只在调度时刻执行一次
        if current_dt == open_dt and tick_snapshot is None and self.handle_data_func:
            try:
                from ..data.api import get_current_data

                data = get_current_data()
                self.handle_data_func(self.context, data)
            except Exception as e:
                log.error(f"交易函数执行失败: {e}")
                import traceback

                log.error(traceback.format_exc())

        if tick_snapshot is not None and self.handle_tick_func:
            try:
                self.handle_tick_func(self.context, tick_snapshot)
            except Exception as e:
                log.error(f"逐笔函数执行失败: {e}")
                import traceback

                log.error(traceback.format_exc())

        # tick 事件自带成交时点，不依赖分钟级交易时段判定
        if tick_snapshot is not None or self._is_trading_time(current_dt, market_periods):
            self._process_orders(current_dt)

        if current_dt == close_dt and tick_snapshot is None and self.after_trading_end_func:
            try:
                self.after_trading_end_func(self.context)
            except Exception as e:
                log.error(f"盘后函数执行失败: {e}")
                import traceback

                log.error(traceback.format_exc())

    def is_tick_backtest(self) -> bool:
        """返回当前回测是否以 tick 频率运行。

        Returns:
            bool: frequency 归一化后属于 tick 频率时为 True。
        """

        return _is_tick_frequency(self.frequency)

    def _run_tick_timeline(
        self,
        *,
        timeline: List[datetime],
        schedule_map: Dict[datetime, List[Any]],
        pre_open_dt: datetime,
        open_dt: datetime,
        close_dt: datetime,
        market_periods: Sequence[Tuple[Time, Time]],
        state: _DayRunState,
    ) -> None:
        """按 min(调度时刻, 下一笔 tick) 合并推进单个交易日。

        Args:
            timeline: 已排序的调度时刻列表。
            schedule_map: 当日调度任务映射。
            pre_open_dt: 盘前回调时刻。
            open_dt: 开盘时刻。
            close_dt: 收盘时刻。
            market_periods: 当日交易时段。
            state: 当日回放状态。

        Raises:
            TickDataMissingError: 已订阅标的当日取不到 tick。
        """

        anchors = {
            "schedule_map": schedule_map,
            "pre_open_dt": pre_open_dt,
            "open_dt": open_dt,
            "close_dt": close_dt,
            "market_periods": market_periods,
        }
        day_date = state.trade_day.date()
        self._prepare_tick_day(day_date)

        index = 0
        while index < len(timeline):
            next_point = timeline[index]
            tick_code, tick_dt = self._peek_next_tick()
            if tick_dt is not None and tick_dt < next_point:
                self._consume_tick_event(tick_code, anchors=anchors, state=state)
                continue
            self._execute_time_point(next_point, state=state, **anchors)
            self._sync_tick_subscriptions(day_date, next_point)
            index += 1

        # 调度时刻用尽后仍有 tick：说明交易时段配置盖不住行情时段，消费完并提示
        tick_code, tick_dt = self._peek_next_tick()
        if tick_dt is not None:
            log.warning(
                f"{day_date.isoformat()} 存在晚于收盘调度时刻 {close_dt} 的 tick，"
                "请检查交易时段配置"
            )
            while tick_code is not None:
                self._consume_tick_event(tick_code, anchors=anchors, state=state)
                tick_code, _ = self._peek_next_tick()

    def _consume_tick_event(
        self,
        tick_code: str,
        *,
        anchors: Dict[str, Any],
        state: _DayRunState,
    ) -> None:
        """消费指定标的的下一笔 tick 并投递给策略。

        Args:
            tick_code: 事件所属标的代码。
            anchors: 当日调度锚点集合。
            state: 当日回放状态。
        """

        stream = self._tick_streams[tick_code]
        snapshot = stream.advance()
        if snapshot is None:
            return
        self._tick_snapshots[tick_code] = snapshot
        self._execute_time_point(snapshot.datetime, state=state, tick_snapshot=snapshot, **anchors)
        self._sync_tick_subscriptions(state.trade_day.date(), snapshot.datetime)

    def _prepare_tick_day(self, day: date) -> None:
        """为指定交易日预取全部已订阅标的的 tick 事件流。

        Args:
            day: 自然日；数据源单次查询上限为 24 小时，因此按日预取。

        Raises:
            TickDataMissingError: 任一已订阅标的当日缺数据。
        """

        if self._tick_day == day and self._tick_streams:
            return
        self._tick_day = day
        self._tick_streams.clear()
        self._tick_snapshots.clear()
        for code in sorted(self._tick_subscriptions):
            self._load_tick_stream(code, day)

    def _load_tick_stream(
        self, code: str, day: date, cutoff_dt: Optional[datetime] = None
    ) -> TickDayStream:
        """加载单个标的的单日 tick 事件流。

        Args:
            code: 标的代码。
            day: 自然日。
            cutoff_dt: 盘中新增订阅时的起投时刻，早于该时刻的 tick 不补投。

        Returns:
            TickDayStream: 已按源时间戳稳定排序的事件流。

        Raises:
            TickDataMissingError: 当日无数据或加载失败，错误信息含代码与日期。
        """

        try:
            # 整日预取是引擎内部行为，取数窗口覆盖回放时钟之后属预期，
            # 策略仍只能看到已推进到的 tick，因此这里暂停未来数据守卫
            with internal_replay_prefetch():
                stream = load_tick_day(code, day)
        except TickDataMissingError:
            raise
        except Exception as exc:
            raise TickDataMissingError(
                f"{code} 在 {day.isoformat()} 的 tick 数据加载失败: {exc}"
            ) from exc
        if stream is None or len(stream) == 0:
            raise TickDataMissingError(f"{code} 在 {day.isoformat()} 缺少 tick 数据")
        if cutoff_dt is not None:
            stream.skip_until_dt(cutoff_dt)
        self._tick_streams[code] = stream
        return stream

    def _sync_tick_subscriptions(self, day: date, current_dt: datetime) -> None:
        """让订阅变更在回放时钟生效时点立即起效。

        Args:
            day: 当前交易日。
            current_dt: 当前回放时刻，作为新增订阅的起投时间。

        Raises:
            TickDataMissingError: 新增订阅的标的当日缺数据。
        """

        for code in sorted(self._tick_subscriptions - set(self._tick_streams)):
            self._load_tick_stream(code, day, cutoff_dt=current_dt)
        for code in set(self._tick_streams) - self._tick_subscriptions:
            self._tick_streams.pop(code, None)
            self._tick_snapshots.pop(code, None)

    def _peek_next_tick(self) -> Tuple[Optional[str], Optional[datetime]]:
        """返回所有活跃事件流中最早的一笔 tick。

        Returns:
            Tuple[Optional[str], Optional[datetime]]: (标的代码, 回放时刻)；
                全部消费完时为 (None, None)。
        """

        best_code: Optional[str] = None
        best_time: Optional[float] = None
        best_dt: Optional[datetime] = None
        for code, stream in self._tick_streams.items():
            raw_time = stream.next_time
            if raw_time is None:
                continue
            # 用原始 float 时间戳比较，避免 datetime 量化后误判先后
            if best_time is None or raw_time < best_time:
                best_code = code
                best_time = raw_time
                best_dt = stream.next_dt
        return best_code, best_dt

    def get_current_tick_snapshot(self, security: str) -> Optional[TickSnapshot]:
        """返回指定标的在回放时钟下最近可见的一笔 tick。

        Args:
            security: 标的代码。

        Returns:
            Optional[TickSnapshot]: 尚无可视 tick 或非 tick 回测时为 None。
        """

        if not self.is_tick_backtest():
            return None
        return self._tick_snapshots.get(security)

    def register_backtest_tick_subscription(self, security: str) -> None:
        """登记回测 tick 订阅，投递自当前回放时点起生效。

        Args:
            security: 标的代码。
        """

        if security:
            self._tick_subscriptions.add(security)

    def unregister_backtest_tick_subscription(self, security: str) -> None:
        """注销单个标的的回测 tick 订阅。

        Args:
            security: 标的代码。
        """

        self._tick_subscriptions.discard(security)
        self._tick_streams.pop(security, None)
        self._tick_snapshots.pop(security, None)

    def clear_backtest_tick_subscriptions(self) -> None:
        """注销全部回测 tick 订阅并停止投递。"""

        self._tick_subscriptions.clear()
        self._tick_streams.clear()
        self._tick_snapshots.clear()

    def _apply_dividends_for_day(self, trade_day: datetime):
        """开盘前处理当日生效的权益变动（分红、送转/拆分），同步持仓与现金口径。

        注意：如果除权日当天停牌，延迟到复牌日再处理（与聚宽行为一致）。
        """
        portfolio = self.context.portfolio
        if not portfolio.positions:
            return

        # 税务口径：股票默认按 20% 预提，公募基金类免税（可由配置覆盖）
        settings = get_settings()
        tax_rate = settings.options.get("dividend_tax_rate", 0.20)

        # 取当日权益变动事件；若除权除息日期落在非交易日或停牌日，则顺延到后续首个交易日盘前处理
        # 注意：需要往前多查几天，以包含因停牌而延迟的分红事件
        current_date = trade_day.date()
        # 往前查 10 天，以覆盖可能因停牌延迟的分红事件
        query_start = current_date - timedelta(days=10)

        for code, pos in list(portfolio.positions.items()):
            try:
                corp_actions = self._load_corporate_actions(
                    code, start_date=query_start, end_date=current_date
                )
                if not corp_actions:
                    continue

                for action in corp_actions:
                    eff_date = action.get("date")
                    split_ratio = float(action.get("scale_factor", 1.0) or 1.0)
                    gross_div = float(action.get("bonus_pre_tax", 0.0) or 0.0)

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

                    if self._position_not_eligible_for_action(pos, eff_date):
                        log.info(
                            f"{code} 分红/拆分事件跳过: 除权日={eff_date}, "
                            f"当前持仓建仓日={pos.buy_time.date() if pos.buy_time else None}, "
                            "持仓不早于除权日，不能参与本次权益事件"
                        )
                        continue

                    # 检查除权日当天是否停牌：如果停牌则延迟到复牌日处理
                    # 使用直接调用 provider 的方式绕过 avoid_future_data 限制
                    is_paused = self._is_security_paused_on_date(code, eff_date)
                    log.debug(f"{code} 停牌检测结果: 除权日 {eff_date} 停牌={is_paused}")

                    if is_paused:
                        if eff_date == current_date:
                            # 除权日就是今天，今天停牌，延迟处理
                            log.info(
                                f"{code} 在除权日 {current_date} 停牌，除权事件（拆分={split_ratio:.4f}，派息={gross_div:.4f}）延迟到复牌日处理"
                            )
                            continue
                        else:
                            # 除权日已过去且停牌，今天是复牌后的第一个处理机会
                            log.info(f"{code} 除权日 {eff_date} 停牌，今日 {current_date} 执行延迟的除权事件")

                    sec_type = action.get("security_type", "stock")
                    # 内部口径：统一为 split_ratio/gross_div/base_lot
                    split_ratio = float(action.get("scale_factor", 1.0) or 1.0)
                    gross_div = float(action.get("bonus_pre_tax", 0.0) or 0.0)
                    base_lot = int(action.get("per_base", 10) or 10)

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
        if category != "futures" and is_futures_security(security):
            # 数据源未分类的合约按代码后缀归入期货，避免套用股票印花税
            category = "futures"
        settings = get_settings()
        type_hint = str(info.get("type") or category).lower()
        order_cost = settings.order_cost_overrides.get(f"{category}_{security}")
        if not order_cost:
            order_cost = settings.order_cost_overrides.get(f"{type_hint}_{security}")
        if not order_cost:
            order_cost = settings.order_cost.get(category)
        if not order_cost and category != "stock":
            order_cost = settings.order_cost.get("stock")
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
        explicit = str(info.get("category") or "").lower()
        if explicit in ("money_market_fund", "fund", "stock", "futures"):
            return explicit

        # 兼容数据源返回的 type/subtype 字段
        subtype = str(info.get("subtype") or "").lower()
        primary = str(info.get("type") or "").lower()

        if subtype in ("mmf", "money_market_fund"):
            return "money_market_fund"
        if primary in ("fund", "etf"):
            # jqdatasdk 返回 type='etf'，统一归类为 fund
            return "fund"
        if primary == "futures":
            return "futures"
        if primary == "stock":
            return "stock"

        # 默认视为股票（security_overrides.json 的 by_prefix 已在 get_security_info 中处理）
        return "stock"

    def _calc_trade_price_with_default_slippage(
        self,
        price: float,
        is_buy: bool,
        security: str,
        info: Optional[Dict[str, Any]] = None,
        category: Optional[str] = None,
    ) -> float:
        """未显式配置滑点时，按品类采用默认值；现金类不应用滑点。"""
        if info is None:
            info = get_security_info(security)
        if category is None:
            category = self._infer_security_category(security, info)
        # 可由 overrides 指定精确值
        ratio = info.get("slippage")
        if not isinstance(ratio, (int, float)):
            if category == "money_market_fund":
                ratio = 0.0
            else:
                ratio = 0.00246  # 默认值 24.6bps（常见设置），可通过配置覆盖
        if ratio <= 0:
            return price
        half = float(ratio) / 2.0
        return price * (1 + half) if is_buy else price * (1 - half)

    def _select_slippage_config(
        self, security: str, category: str, info: Dict[str, Any]
    ) -> Optional[Any]:
        """按聚宽语义解析滑点配置优先级。"""
        settings = get_settings()
        sl_map = getattr(settings, "slippage_map", {}) or {}
        if not sl_map:
            return None
        keys = []
        code = security
        type_hint = str(info.get("type") or "").lower()
        subtype = str(info.get("subtype") or "").lower()

        if category == "stock":
            keys.append(f"stock_{code}")
            keys.append("stock")
        elif category == "fund" or subtype in ("fund", "index"):
            keys.append(f"fund_{code}")
            keys.append("fund")
        elif category == "money_market_fund" or subtype in ("mmf", "money_market_fund"):
            return None

        if type_hint == "index" and category != "stock":
            keys.append(f"fund_{code}")
            keys.append("fund")

        if type_hint == "futures" or category == "futures":
            keys.append(f"futures_{code}")
            try:
                tag = re.sub(r"\\d+.*", "", code.split(".")[0])
                if tag:
                    keys.append(f"futures_{tag}")
            except Exception:
                pass
            keys.append("futures")

        keys.append(code)
        keys.append(category)
        keys.append("all")

        seen = set()
        for key in keys:
            if key in seen:
                continue
            seen.add(key)
            cfg = sl_map.get(key)
            if cfg is not None:
                return cfg
        return None

    def _apply_slippage_config(
        self,
        config: Any,
        price: float,
        is_buy: bool,
        security: str,
        category: str,
        info: Dict[str, Any],
    ) -> float:
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
            if hasattr(config, "calculate_slippage"):
                return config.calculate_slippage(price, is_buy)
        except Exception as exc:
            log.debug(f"应用滑点失败 {security}: {exc}")
        return price

    def _apply_slippage_price(self, price: float, is_buy: bool, security: str) -> float:
        """统一处理滑点选择与回退。"""
        info = get_security_info(security)
        category = self._infer_security_category(security, info)
        subtype = str(info.get("subtype") or "").lower()
        if category == "money_market_fund" or subtype in ("mmf", "money_market_fund"):
            return price

        settings = get_settings()
        cfg = self._select_slippage_config(security, category, info)
        if cfg:
            return self._apply_slippage_config(cfg, price, is_buy, security, category, info)
        if settings.slippage:
            return self._apply_slippage_config(
                settings.slippage, price, is_buy, security, category, info
            )
        return self._calc_trade_price_with_default_slippage(
            price, is_buy, security, info=info, category=category
        )

    @staticmethod
    def _round_half_up(value: float, decimals: int) -> float:
        q = Decimal(10) ** -decimals
        return float(Decimal(str(value)).quantize(q, rounding=ROUND_HALF_UP))

    def _round_to_tick(
        self, price: float, security: str, *, is_buy: Optional[bool] = None
    ) -> float:
        """按最小报价单位处理价格：
        - 股票: 0.01
        - 基金/ETF/货基: 0.001
        - 期货: 取合约规格表的最小变动价位，规格缺失时不归档直接返回原价
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
        # 小数位由步长推导：写死 2 位会把 0.005 这类档位压平成非法价格
        decimals = max(0, -Decimal(str(step)).as_tuple().exponent)
        return round(ticks_rounded * step, decimals)

    def _tick_step_for_security(
        self, security: str, info: Optional[Dict[str, Any]] = None, category: Optional[str] = None
    ) -> float:
        """返回标的对应的最小报价步长。"""
        if is_futures_security(security):
            # 行情信息在缺省口径下会给期货返回股票默认档位，规格表优先
            try:
                current_dt = getattr(self.context, "current_dt", None)
                spec_day = current_dt.date() if current_dt is not None else None
                spec_tick = self._futures_spec_table.tick_size(security, spec_day)
                if spec_tick and spec_tick > 0:
                    return float(spec_tick)
            except ContractSpecError as exc:
                log.debug(f"{security} 合约规格表无最小变动价位: {exc}")
            info = info if info is not None else get_security_info(security)
            try:
                tick_size = info.get("tick_size")
                if isinstance(tick_size, (int, float)) and tick_size > 0:
                    return float(tick_size)
            except Exception:
                pass
            # 步长未知时不归档，避免用错误档位改动成交价
            return 0.0
        if info is None:
            info = get_security_info(security)
        if category is None:
            category = self._infer_security_category(security, info)
        tick_decimals = info.get("tick_decimals")
        tick_size = info.get("tick_size")
        try:
            if isinstance(tick_size, (int, float)) and tick_size > 0:
                return float(tick_size)
            if isinstance(tick_decimals, (int, float)) and tick_decimals >= 0:
                return float(round(10 ** (-int(tick_decimals)), 6))
        except Exception:
            pass
        return 0.01 if category == "stock" else 0.001

    def _rollover_tplus_for_new_day(self) -> None:
        """在新交易日开始时释放 T+1 锁定的当日买入量。"""
        if not self.context or not self.context.portfolio or not self.context.portfolio.positions:
            return
        for code, pos in list(self.context.portfolio.positions.items()):
            try:
                info = get_security_info(code)
                tplus = self._infer_tplus_from_info(info)
                if tplus == 1:
                    add = getattr(pos, "today_buy_t1", 0) or 0
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
        process_date = (
            self.context.current_dt.date()
            if self.context is not None and self.context.current_dt is not None
            else eff_date
        )
        self._split_price_adjustments[(code, process_date)] = {
            "split_ratio": float(split_ratio),
            "adjusted_price": float(pos.price),
        }

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

    def _normalize_split_day_close_price(self, security: str, close_price: float) -> float:
        """将拆分生效日的收盘价统一到当前持仓份额口径。

        Args:
            security: 当前持仓标的代码。
            close_price: 行情源返回的原始收盘价。

        Returns:
            float: 可直接用于当前持仓股数估值的收盘价。

        说明:
            部分数据源在拆分/送转生效日，尤其叠加停牌时，会返回拆分前旧价。
            此时引擎已经调整了持仓股数，如果直接用旧价估值会制造假回撤。
            本函数只在当日确实执行过拆分时介入；正常交易日保持原价。
        """
        raw_close = float(close_price)
        if self.context is None or self.context.current_dt is None:
            return raw_close
        current_date = self.context.current_dt.date()
        adjustment = self._split_price_adjustments.get((security, current_date))
        if not adjustment:
            return raw_close

        split_ratio = float(adjustment.get("split_ratio") or 1.0)
        adjusted_price = float(adjustment.get("adjusted_price") or 0.0)
        if split_ratio <= 0 or adjusted_price <= 0 or abs(split_ratio - 1.0) <= 1e-9:
            return raw_close

        if abs(raw_close / adjusted_price - 1.0) <= 0.2:
            return raw_close

        converted_price = raw_close / split_ratio
        if converted_price > 0 and abs(converted_price / adjusted_price - 1.0) <= 0.2:
            log.info(
                f"{security} 拆分日收盘价口径修正: "
                f"原始close={raw_close:.4f}, 拆分比例={split_ratio:.6f}, "
                f"修正后close={converted_price:.4f}"
            )
            return converted_price

        log.warning(
            f"{security} 拆分日收盘价疑似与持仓份额不同口径: "
            f"原始close={raw_close:.4f}, 拆分后持仓价={adjusted_price:.4f}, "
            f"拆分比例={split_ratio:.6f}; 保留拆分后持仓价估值"
        )
        return adjusted_price

    @staticmethod
    def _infer_tplus_from_info(info):
        if not isinstance(info, dict):
            return 0
        raw = info.get("tplus")
        if raw is not None:
            try:
                return int(raw)
            except Exception:
                pass
        sec_type = str(info.get("type") or "").lower()
        return 1 if sec_type == "stock" else 0

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

    @staticmethod
    def _positive_price_or_none(value: Any) -> Optional[float]:
        """把行情取值归一化为可用的正价格。

        合约退市后行情返回的是 NaN 行，而 NaN 在布尔上下文里为真，
        `value or 0.0` 会把 NaN 原样放行，因此必须显式判别。

        Args:
            value: 行情字段原始取值。

        Returns:
            Optional[float]: 大于 0 的有限价格；NaN、None、非正数一律返回 None。
        """

        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if number != number or number <= 0:
            return None
        return number

    def _resolve_base_exec_price(
        self, security: str, current_dt: datetime, fq_mode: str
    ) -> Optional[float]:
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
                    frequency="daily",
                    fields=["open"],
                    count=1,
                    fq=fq_mode,
                )
                if not dfp.empty:
                    rowp = dfp.iloc[-1]
                    return self._positive_price_or_none(rowp.get("open"))
            elif t and (Time(9, 31) <= t < Time(15, 0)):
                dfp = api_get_price(
                    security=security,
                    end_date=current_dt,
                    frequency="minute",
                    fields=["close"],
                    count=1,
                    fq=fq_mode,
                )
                if not dfp.empty:
                    rowp = dfp.iloc[-1]
                    return self._positive_price_or_none(rowp.get("close"))
            else:
                dfp = api_get_price(
                    security=security,
                    end_date=current_dt,
                    frequency="daily",
                    fields=["close"],
                    count=1,
                    fq=fq_mode,
                )
                if not dfp.empty:
                    rowp = dfp.iloc[-1]
                    return self._positive_price_or_none(rowp.get("close"))
        except Exception:
            return None
        return None

    def _load_corporate_actions(
        self, code: str, start_date: datetime.date, end_date: datetime.date
    ) -> List[Dict[str, Any]]:
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

    @staticmethod
    def _position_not_eligible_for_action(pos: "Position", eff_date: datetime.date) -> bool:
        """判断当前整轮持仓是否没有资格参与权益事件。

        参数:
            pos: 当前持仓对象，使用 buy_time 表示本轮持仓首次建仓时间。
            eff_date: 权益事件除权日。

        返回值:
            如果本轮持仓首次建仓日未知、等于或晚于除权日，返回 True；否则返回 False。

        说明:
            回测可能在除权日之后数日仍能查到该权益事件。如果该标的是除权日之后才买入，
            持仓已经按除权后的价格成交，不能再补做拆分或派息，否则会把份额/现金重复计算。
            除权日当天买入也不能参与本次权益事件；只有跨除权日一直持有的仓位才按原逻辑处理。
            如果持仓没有 buy_time，无法证明其在除权日前已持有，按保守口径跳过，避免制造虚增。
        """

        if eff_date is None:
            return False
        if pos.buy_time is None:
            return True
        return pos.buy_time.date() >= eff_date

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
            provider = data_api.get_data_provider()

            # 获取 check_date 前后的数据
            start = check_date - timedelta(days=5)
            end = check_date + timedelta(days=1)  # 多取一天确保包含 check_date

            df = provider.get_price(
                security=security,
                start_date=datetime.combine(start, Time(0, 0)),
                end_date=datetime.combine(end, Time(15, 0)),
                frequency="daily",
                fields=["volume", "paused"],
                fq="none",
                skip_paused=False,  # 重要：不跳过停牌日，否则无法判断
            )

            if df.empty:
                log.debug(f"{security} 在 {check_date} 附近无数据")
                return False  # 无法判断，不阻断

            # 获取 df 中所有日期
            dates_in_df = []
            for idx in df.index:
                if hasattr(idx, "date"):
                    dates_in_df.append(idx.date())
                elif hasattr(idx, "to_pydatetime"):
                    dates_in_df.append(idx.to_pydatetime().date())
            dates_in_df = sorted(dates_in_df)

            log.debug(
                f"{security} 停牌检测: check_date={check_date}, df日期={dates_in_df[-5:] if len(dates_in_df) > 5 else dates_in_df}"
            )

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
                idx_date = idx.date() if hasattr(idx, "date") else idx.to_pydatetime().date()
                if idx_date == check_date:
                    row = df.loc[idx]
                    # 优先使用 paused 字段
                    if "paused" in row and pd.notna(row["paused"]):
                        is_paused = bool(row["paused"])
                        log.debug(
                            f"{security} 在 {check_date} paused={row['paused']}，停牌={is_paused}"
                        )
                        return is_paused
                    # 其次用成交量判断
                    if "volume" in row:
                        is_paused = float(row["volume"] or 0) == 0
                        log.debug(
                            f"{security} 在 {check_date} volume={row['volume']}，停牌={is_paused}"
                        )
                        return is_paused
                    break

            return False
        except Exception as e:
            log.debug(f"检查 {security} 停牌状态失败: {e}")
            return False  # 获取失败时不阻断处理

    def _futures_margin_rate(self) -> Optional[float]:
        """读取策略配置的期货保证金率。

        Args:
            无。

        Returns:
            Optional[float]: set_option('futures_margin_rate') 的值；未配置时为 None。
        """

        value = get_settings().options.get("futures_margin_rate")
        return None if value is None else float(value)

    def _futures_margin_rate_by_product(self) -> Dict[str, float]:
        """读取按品种配置的期货保证金率。

        set_option('futures_margin_rate.T', 0.03) 这类点号键只写入 options 字典，
        需要在此展开，否则品种级设置永远被全局设置盖掉。

        Args:
            无。

        Returns:
            Dict[str, float]: 品种代码到保证金率的映射，非法条目被跳过。
        """

        prefix = "futures_margin_rate."
        result: Dict[str, float] = {}
        for key, value in (get_settings().options or {}).items():
            if not isinstance(key, str) or not key.startswith(prefix):
                continue
            product = key[len(prefix) :].strip().upper()
            if not product or not product.isalpha():
                log.debug(f"忽略无法识别的保证金率配置项: {key}")
                continue
            try:
                rate = float(value)
            except (TypeError, ValueError):
                log.debug(f"忽略非数值的保证金率配置项: {key}={value!r}")
                continue
            if rate <= 0:
                log.debug(f"忽略非正的保证金率配置项: {key}={rate}")
                continue
            result[product] = rate
        return result

    def _ensure_futures_account(self) -> FuturesAccount:
        """取得期货账本，并把组合现金同步为其可用资金。

        组合的 available_cash 是含股票与期货的唯一权威现金口径，
        账本现金只在撮合与结算期间作为工作副本，操作结束后回写。

        Args:
            无。

        Returns:
            FuturesAccount: 与组合现金对齐的期货账本。
        """

        portfolio = self.context.portfolio
        account = portfolio.futures_account
        if account is None:
            account = FuturesAccount(
                cash=portfolio.available_cash,
                starting_cash=portfolio.starting_cash,
                spec_table=self._futures_spec_table,
            )
            portfolio.futures_account = account
        account.spec_table = self._futures_spec_table
        self._futures_spec_table.set_margin_rate(self._futures_margin_rate())
        self._futures_spec_table.set_margin_rate_by_product(self._futures_margin_rate_by_product())
        account.cash = portfolio.available_cash
        account.starting_cash = portfolio.starting_cash
        return account

    def _futures_contract_end_date(self, security: str) -> Optional[date]:
        """解析合约最后交易日，用于到期了结。

        Args:
            security: 期货合约代码。

        Returns:
            Optional[date]: 最后交易日；数据源未提供或解析失败时为 None。
        """

        try:
            info = get_security_info(security)
        except Exception as exc:
            log.debug(f"{security} 合约信息获取失败: {exc}")
            return None
        getter = getattr(info, "get", None)
        raw = getter("end_date") if callable(getter) else getattr(info, "end_date", None)
        if raw is None:
            return None
        if isinstance(raw, datetime):
            return raw.date()
        if isinstance(raw, date):
            return raw
        try:
            parsed = pd.to_datetime(raw)
        except Exception:
            return None
        return None if pd.isna(parsed) else parsed.date()

    def _calculate_futures_commission(
        self,
        cost_config: OrderCost,
        action: str,
        lots: int,
        notional_per_lot: float,
        today_lots: int,
    ) -> float:
        """按成交金额计算期货手续费；期货没有印花税。

        Args:
            cost_config: 期货费用配置。
            action: 'open' 或 'close'。
            lots: 成交手数。
            notional_per_lot: 单手成交金额（成交价×合约乘数）。
            today_lots: 平仓中按平今费率计费的手数，开仓传 0。

        Returns:
            float: 四舍五入到分并满足最低佣金的手续费。
        """

        if action == "open":
            commission = notional_per_lot * lots * cost_config.open_commission
        else:
            yesterday_lots = max(0, lots - today_lots)
            commission = notional_per_lot * (
                yesterday_lots * cost_config.close_commission
                + today_lots * cost_config.close_today_commission
            )
        commission = self._round_half_up(commission, 2)
        if cost_config.min_commission > 0:
            commission = max(commission, self._round_half_up(cost_config.min_commission, 2))
        return commission

    @staticmethod
    def _split_futures_close_lots(position, lots: int, pindex: int, close_today: bool) -> int:
        """计算本次平仓中按平今费率计费的手数。

        Args:
            position: 被平的期货持仓。
            lots: 平仓手数。
            pindex: 0 表示先平昨仓，1 表示先平今仓。
            close_today: True 表示整笔按平今计费。

        Returns:
            int: 今仓手数。
        """

        if close_today:
            return lots
        if pindex == 1:
            return min(lots, position.today_amount)
        return max(0, lots - position.yesterday_amount)

    def _execute_futures_fill(
        self,
        order: Order,
        lots: int,
        action: str,
        side: str,
        is_buy: bool,
        trade_price: float,
        fund_check_price: float,
        current_dt: datetime,
    ) -> None:
        """按期货口径撮合一笔订单。

        与股票路径的差异：不做一手取整与最小申报量约束，按保证金而非全额锁资，
        允许无持仓开空，超量平仓与合约规格缺失一律拒单，且不收印花税。

        Args:
            order: 待撮合订单。
            lots: 意图手数，正数。
            action: 'open' 或 'close'。
            side: 'long' 或 'short'。
            is_buy: 买卖方向；开多/平空为买，开空/平多为卖。
            trade_price: 撮合成交价。
            fund_check_price: 用于锁资的价格（限价优先）。
            current_dt: 当前回放时刻。

        Returns:
            None: 结果写入订单状态、账本、组合现金与成交记录。
        """

        portfolio = self.context.portfolio
        account = self._ensure_futures_account()
        security = order.security
        if not isinstance(getattr(order, "extra", None), dict):
            order.extra = {}
        extra = order.extra
        extra["order_price"] = float(fund_check_price)
        extra.setdefault("requested_order_price", float(fund_check_price))

        cost_config = self._get_order_cost_config(security)
        if (
            not self._futures_cost_warned
            and cost_config.open_commission <= 0
            and cost_config.close_commission <= 0
        ):
            self._futures_cost_warned = True
            log.warning(
                "未配置期货手续费（set_order_cost(..., type='futures')），本次回测按零成本撮合"
            )

        try:
            trade_day = current_dt.date()
            multiplier = account.spec_table.multiplier(security, trade_day)
            margin_rate = account.spec_table.margin_rate(security, trade_day)
        except ContractSpecError as exc:
            log.error(f"{security} 期货合约规格缺失，订单拒绝: {exc}")
            order.status = OrderStatus.rejected
            extra["rejection_reason"] = "futures_contract_spec_missing"
            return

        lots = int(lots)
        if lots <= 0:
            order.status = OrderStatus.canceled
            return

        notional_per_lot = float(trade_price) * multiplier
        commission = 0.0
        # 开仓口径下订单均价即成交价，平仓分支会改写成平仓前的持仓开仓价
        avg_cost = float(trade_price)
        end_date = self._futures_contract_end_date(security)

        if action == "open":
            if end_date is not None and current_dt.date() > end_date:
                log.warning(
                    f"{security} 已过最后交易日 {end_date}，开仓订单拒绝"
                )
                order.status = OrderStatus.rejected
                extra["rejection_reason"] = "futures_contract_expired"
                return
            margin_per_lot = float(fund_check_price) * multiplier * margin_rate
            unit_cost = margin_per_lot + notional_per_lot * cost_config.open_commission
            if unit_cost <= 0:
                log.error(f"{security} 单手保证金与手续费非正数，无法撮合")
                order.status = OrderStatus.rejected
                extra["rejection_reason"] = "invalid_futures_unit_cost"
                return
            affordable = int(account.cash // unit_cost)
            if affordable < lots:
                log.info(f"{security} 保证金不足，手数由 {lots} 缩至 {affordable}")
                lots = affordable
            # 最低佣金会抬高单手成本，闭式解后再做有限次修正
            for _ in range(3):
                if lots <= 0:
                    break
                commission = self._calculate_futures_commission(
                    cost_config, "open", lots, notional_per_lot, 0
                )
                if lots * margin_per_lot + commission <= account.cash + 1e-9:
                    break
                lots -= 1
            if lots <= 0:
                log.warning(
                    f"{security} 可用资金 {account.cash:.2f} 不足以支付单手保证金，订单拒绝"
                )
                order.status = OrderStatus.rejected
                extra["rejection_reason"] = "insufficient_margin"
                return
            commission = self._calculate_futures_commission(
                cost_config, "open", lots, notional_per_lot, 0
            )
            try:
                account.open(
                    security,
                    side,
                    lots,
                    float(trade_price),
                    commission=commission,
                    trade_time=current_dt,
                    end_date=end_date,
                )
            except (InsufficientMarginError, ContractSpecError) as exc:
                log.warning(f"{security} 开仓失败，订单拒绝: {exc}")
                order.status = OrderStatus.rejected
                extra["rejection_reason"] = "insufficient_margin"
                return
        else:
            position = account.get_position(security, side)
            if position is None or position.amount <= 0:
                log.warning(f"{security} 无 {side} 持仓，平仓订单拒绝")
                order.status = OrderStatus.rejected
                extra["rejection_reason"] = "no_futures_position"
                return
            if lots > position.amount:
                log.warning(
                    f"{security} {side} 平仓 {lots} 手超过持仓 {position.amount} 手，订单拒绝"
                )
                order.status = OrderStatus.rejected
                extra["rejection_reason"] = "over_close"
                return
            if order.close_today and lots > position.today_amount:
                log.warning(
                    f"{security} 平今 {lots} 手超过今仓 {position.today_amount} 手，订单拒绝"
                )
                order.status = OrderStatus.rejected
                extra["rejection_reason"] = "close_today_exceeds_today_lots"
                return
            today_lots = self._split_futures_close_lots(
                position, lots, int(getattr(order, "pindex", 0)), bool(order.close_today)
            )
            commission = self._calculate_futures_commission(
                cost_config, "close", lots, notional_per_lot, today_lots
            )
            avg_cost = float(position.open_price)
            try:
                account.close(
                    security,
                    side,
                    lots,
                    float(trade_price),
                    commission=commission,
                    trade_time=current_dt,
                    close_today=bool(order.close_today),
                )
            except (OverCloseError, ContractSpecError) as exc:
                log.warning(f"{security} 平仓失败，订单拒绝: {exc}")
                order.status = OrderStatus.rejected
                extra["rejection_reason"] = "over_close"
                return

        portfolio.available_cash = account.cash

        self._trade_seq += 1
        self.trades.append(
            Trade(
                order_id=order.order_id,
                security=security,
                amount=lots if is_buy else -lots,
                price=float(trade_price),
                time=current_dt,
                commission=commission,
                tax=0.0,
                trade_id=f"T{self._trade_seq:08d}",
            )
        )

        order.price = float(trade_price)
        order.amount = lots
        order.filled = lots
        order.is_buy = is_buy
        order.action = action
        order.status = OrderStatus.filled
        order.avg_cost = avg_cost
        order.commission = float(commission)
        extra["fill_price"] = float(trade_price)
        extra["commission"] = commission
        extra["multiplier"] = multiplier
        portfolio.update_value()
        log.info(
            f"期货{'开仓' if action == 'open' else '平仓'}{'多' if side == LONG else '空'} "
            f"{security}: {lots} 手, 成交价 {trade_price}, 手续费 {commission:.2f}, "
            f"占用保证金 {account.margin:.2f}, 权益 {portfolio.total_value:.2f}"
        )

    def _mark_futures_positions(self) -> None:
        """按当日收盘价标记期货持仓，供缺结算价时仍有可解释的估值。

        Args:
            无。

        Returns:
            None: 结果写入账本各持仓的 last_price。
        """

        account = getattr(self.context.portfolio, "futures_account", None)
        if account is None or not account.positions:
            return
        prices: Dict[str, float] = {}
        for position in account.iter_positions():
            try:
                df = api_get_price(
                    security=position.security,
                    end_date=self.context.current_dt,
                    frequency="daily",
                    fields=["close"],
                    count=1,
                    fq="none",
                )
                close_price = self._extract_single_close(df, position.security)
                if close_price is not None and close_price > 0:
                    prices[position.security] = close_price
            except Exception as exc:
                log.debug(f"{position.security} 期货收盘价标记失败: {exc}")
        account.mark_prices(prices)

    @staticmethod
    def _extract_single_close(df: pd.DataFrame, security: str) -> Optional[float]:
        """从单行行情结果中取出收盘价。

        Args:
            df: get_price 返回的最后一根日线。
            security: 标的代码，用于匹配多级列。

        Returns:
            Optional[float]: 有效收盘价；缺失或非正数时为 None。
        """

        if df is None or df.empty:
            return None
        row = df.iloc[-1]
        candidates: List[Any] = []
        if "close" in df.columns:
            candidates.append(row["close"])
        if security in df.columns:
            candidates.append(row[security])
        if isinstance(df.columns, pd.MultiIndex) and ("close", security) in df.columns:
            candidates.append(row[("close", security)])
        for value in candidates:
            try:
                if pd.notna(value) and float(value) > 0:
                    return float(value)
            except (TypeError, ValueError):
                continue
        return None

    def _fetch_futures_settlement_prices(
        self, securities: Sequence[str], trade_day: datetime
    ) -> Dict[str, float]:
        """取当日结算价；缺值不做任何替代，由账本记录为 missing_settlement。

        Args:
            securities: 需要结算价的合约列表。
            trade_day: 结算日。

        Returns:
            Dict[str, float]: 合约到结算价的映射，缺值不出现在结果中。
        """

        if not securities:
            return {}
        day = trade_day.date() if isinstance(trade_day, datetime) else trade_day
        try:
            df = _data_api_get_extras(
                info="futures_sett_price",
                security_list=list(securities),
                start_date=day,
                end_date=day,
                df=True,
            )
        except Exception as exc:
            log.warning(f"获取期货结算价失败: {exc}")
            return {}
        prices: Dict[str, float] = {}
        if not isinstance(df, pd.DataFrame) or df.empty:
            return prices
        row = df.iloc[-1]
        for security in securities:
            value: Any = None
            if security in df.columns:
                value = row[security]
            elif isinstance(df.columns, pd.MultiIndex):
                for column in df.columns:
                    if security in tuple(column):
                        value = row[column]
                        break
            try:
                if value is not None and pd.notna(value) and float(value) > 0:
                    prices[security] = float(value)
            except (TypeError, ValueError):
                continue
        return prices

    def _settle_futures_day(self, trade_day: datetime) -> None:
        """日终结算：盯市变动入现金、保证金结转到结算价、到期合约了结。

        Args:
            trade_day: 当前交易日。

        Returns:
            None: 结果写入账本、组合现金与每日权益。
        """

        portfolio = self.context.portfolio
        account = portfolio.futures_account
        if account is None or not account.positions:
            return
        account.cash = portfolio.available_cash
        securities = [position.security for position in account.iter_positions()]
        settlement_prices = self._fetch_futures_settlement_prices(securities, trade_day)
        result = account.settle_day(settlement_prices, day=trade_day.date())
        portfolio.available_cash = account.cash
        if result.missing_settlement:
            log.warning(
                f"以下合约缺当日结算价，保持上一基准不做替代估值: "
                f"{', '.join(result.missing_settlement)}"
            )
        for record in result.delivered:
            log.info(
                f"合约到期了结: {record[0]} {record[1]} {record[2]} 手 "
                f"@ 结算价 {record[3]}, 盯市 {record[4]:.2f}"
            )
        portfolio.update_value()
        log.debug(
            f"期货日终结算: 盯市 {result.variation_margin:.2f}, "
            f"结算后保证金 {result.margin:.2f}"
        )

    def _on_futures_day_start(self) -> None:
        """新交易日复位期货当日累计口径。

        Args:
            无。

        Returns:
            None: 结果写入账本当日字段。
        """

        account = getattr(self.context.portfolio, "futures_account", None)
        if account is None:
            return
        account.cash = self.context.portfolio.available_cash
        account.on_day_start()

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
                is_target = (
                    hasattr(o, "_is_target_amount") and getattr(o, "_is_target_amount")
                ) or (hasattr(o, "_is_target_value") and getattr(o, "_is_target_value"))
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
        use_real_price = bool(settings.options.get("use_real_price"))
        fq_mode = "pre" if use_real_price else "none"

        for order in orders:
            try:
                self._register_order(order)
                tick_snapshot = (
                    self.get_current_tick_snapshot(order.security)
                    if self.is_tick_backtest()
                    else None
                )
                # 获取当前价格
                if order.security in current_data:
                    security_data = current_data[order.security]
                elif tick_snapshot is not None:
                    # tick 回放下部分合约不在当日行情容器里，用回放快照合成最小行情
                    security_data = SecurityUnitData(
                        security=order.security,
                        last_price=float(tick_snapshot.current),
                        paused=False,
                        source="tick_replay",
                        source_time=tick_snapshot.datetime,
                        bid_price1=tick_snapshot.b1_p or None,
                        ask_price1=tick_snapshot.a1_p or None,
                        bid_volume1=tick_snapshot.b1_v or None,
                        ask_volume1=tick_snapshot.a1_v or None,
                    )
                else:
                    log.warning(f"无法获取 {order.security} 的行情数据")
                    order.status = OrderStatus.rejected
                    continue
                if tick_snapshot is not None:
                    # 撮合基准已强制取当前 tick，市价单保护价也必须同源，
                    # 否则 bar 价与 tick 价的差距会让市价单被误判越界而取消
                    security_data = replace(
                        security_data, last_price=float(tick_snapshot.current)
                    )
                try:
                    sec_info = get_security_info(order.security)
                except Exception:
                    sec_info = {}
                security_category = self._infer_security_category(order.security, sec_info)
                # 数据源可能不给合约分类，代码本身的交易所后缀作为兜底判据
                is_futures_order = security_category == "futures" or is_futures_security(
                    order.security
                )
                if security_data.paused:
                    log.warning(f"{order.security} 停牌，订单取消")
                    order.status = OrderStatus.canceled
                    continue

                # 解析执行价基准（封装逻辑便于维护与测试）
                current_dt = self.context.current_dt
                # 期货无复权概念，动态复权口径会篡改撮合基准价
                exec_fq_mode = "none" if is_futures_order else fq_mode
                if tick_snapshot is not None:
                    # tick 回放的成交价基准就是当前这笔快照，不能再退回 bar 价
                    base_exec_price = float(tick_snapshot.current)
                else:
                    base_exec_price = self._resolve_base_exec_price(
                        order.security, current_dt, exec_fq_mode
                    )

                current_price = (
                    float(base_exec_price)
                    if base_exec_price and base_exec_price > 0
                    else security_data.last_price
                )
                if not current_price > 0:
                    # NaN 同样落在这里：NaN <= 0 为假，不能用来判别价格有效性
                    log.warning(f"{order.security} 价格无效: {current_price}")
                    order.status = OrderStatus.rejected
                    continue

                # 计算下单数量（普通/目标/价值）——使用 current_price 作为金额换算基准
                try:
                    amount = self._calculate_order_amount(order, current_price)
                except ContractSpecError as exc:
                    log.error(f"{order.security} 期货合约规格缺失，订单拒绝: {exc}")
                    order.status = OrderStatus.rejected
                    if not isinstance(getattr(order, "extra", None), dict):
                        order.extra = {}
                    order.extra["rejection_reason"] = "futures_contract_spec_missing"
                    continue
                if amount == 0:
                    log.debug(f"{order.security} 无需交易")
                    order.status = OrderStatus.canceled
                    continue

                # 先按方向取绝对值，后续统一为正数处理
                is_buy = amount > 0
                intended_amount = abs(amount)
                futures_action = "open" if amount > 0 else "close"
                futures_side = str(getattr(order, "side", LONG) or LONG).lower()
                if is_futures_order:
                    # 期货：数量符号表示开/平，买卖方向由开平与多空共同决定
                    if futures_side not in (LONG, SHORT):
                        log.warning(
                            f"{order.security} 期货订单方向非法: {futures_side}，订单拒绝"
                        )
                        order.status = OrderStatus.rejected
                        continue
                    is_buy = (futures_action == "open") == (futures_side == LONG)

                # 根据证券分类确定价格精度：stock=2位小数，fund/money_market_fund=3位小数
                price_decimals = 2 if security_category == "stock" else 3
                price_reference = (
                    float(security_data.last_price)
                    if getattr(security_data, "last_price", 0.0) and security_data.last_price > 0
                    else current_price
                )

                style_obj = getattr(order, "style", None)
                trade_price: Optional[float] = None
                limit_price: Optional[float] = None
                price_label = "限价"
                if isinstance(style_obj, LimitOrderStyle):
                    limit_price = float(style_obj.price)
                elif isinstance(style_obj, MarketOrderStyle):
                    price_label = "保护价"
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

                if limit_price is not None:
                    requested_limit_price = float(limit_price)
                    try:
                        limit_price = pricing.clamp_price_to_trade_bounds(
                            order.security,
                            requested_limit_price,
                            price_reference,
                            getattr(security_data, "high_limit", None),
                            getattr(security_data, "low_limit", None),
                            is_buy,
                        )
                        if abs(limit_price - requested_limit_price) > 1e-9:
                            action_label = "买入" if is_buy else "卖出"
                            log.info(
                                f"{order.security} {action_label}{price_label} "
                                f"{requested_limit_price:.{price_decimals}f} 超出涨跌停/价格笼子，"
                                f"调整为 {limit_price:.{price_decimals}f}"
                            )
                    except Exception as exc:
                        log.debug(f"{order.security} {price_label}边界裁剪失败: {exc}")

                if trade_price is None:
                    info = get_security_info(order.security)
                    category = self._infer_security_category(order.security, info)
                    if category == "money_market_fund":
                        trade_price = current_price
                    else:
                        trade_price = self._apply_slippage_price(
                            current_price, is_buy, order.security
                        )

                trade_price = self._round_to_tick(trade_price, order.security, is_buy=None)

                if limit_price is not None:
                    if is_buy and current_price - limit_price > 1e-9:
                        log.info(
                            f"{order.security} 当前价 {current_price:.{price_decimals}f} "
                            f"高于买入{price_label} {limit_price:.{price_decimals}f}，订单未成交"
                        )
                        order.status = OrderStatus.canceled
                        continue
                    if (not is_buy) and limit_price - current_price > 1e-9:
                        log.info(
                            f"{order.security} 当前价 {current_price:.{price_decimals}f} "
                            f"低于卖出{price_label} {limit_price:.{price_decimals}f}，订单未成交"
                        )
                        order.status = OrderStatus.canceled
                        continue
                    if is_buy:
                        trade_price = min(trade_price, limit_price)
                    else:
                        trade_price = max(trade_price, limit_price)

                try:
                    trade_price = pricing.clamp_price_to_trade_bounds(
                        order.security,
                        trade_price,
                        price_reference,
                        getattr(security_data, "high_limit", None),
                        getattr(security_data, "low_limit", None),
                        is_buy,
                    )
                except Exception as exc:
                    log.debug(f"{order.security} 成交价边界裁剪失败: {exc}")
                trade_price = self._round_to_tick(trade_price, order.security, is_buy=None)

                if is_futures_order:
                    # 期货不走 A股一手取整/印花税/可卖持仓口径，单独撮合
                    self._execute_futures_fill(
                        order=order,
                        lots=intended_amount,
                        action=futures_action,
                        side=futures_side,
                        is_buy=is_buy,
                        trade_price=float(trade_price),
                        fund_check_price=float(
                            limit_price if limit_price is not None else trade_price
                        ),
                        current_dt=current_dt,
                    )
                    continue

                # 买入前资金检查：按“可下单量上限”缩量 + 一手取整 + 最小申报量
                final_amount = intended_amount
                min_trade_size = 100  # A股一手
                min_order_amount = 100

                # 资金检查价格：若存在限价/保护价则按限价锁资，否则按撮合价
                fund_check_price = limit_price if limit_price is not None else trade_price
                try:
                    extra = getattr(order, "extra", None)
                    if extra is None:
                        order.extra = {}
                        extra = order.extra
                    if fund_check_price is not None:
                        extra["order_price"] = float(fund_check_price)
                        extra.setdefault("requested_order_price", float(fund_check_price))
                except Exception:
                    pass

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
                    available_for_buy = max(
                        0.0,
                        self.context.portfolio.available_cash - self.context.portfolio.locked_cash,
                    )
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
                        log.debug(
                            f"{order.security} 买入数量 {final_amount} 不足最小申报量({min_order_amount})"
                        )
                        order.status = OrderStatus.canceled
                        continue
                else:
                    # 如果剩余的股数超过一手，但卖出订单不足一手，则取消卖出订单（如果剩余股数不足一手，则允许碎股卖出）
                    if pos.closeable_amount >= min_trade_size:
                        final_amount = (final_amount // min_trade_size) * min_trade_size
                        if final_amount < min_trade_size:
                            log.debug(
                                f"{order.security} 卖出数量 {final_amount} 不足最小申报量({min_order_amount})"
                            )
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
                # 买入口径下订单均价即成交价，卖出分支会改写成卖出前的持仓成本
                avg_cost = float(trade_price)

                if is_buy:
                    # 委托时锁定资金（含费用）
                    self.context.portfolio.locked_cash += total_cost
                    if total_cost > (self.context.portfolio.available_cash):
                        # 双重保障：若仍不足则拒绝并回滚锁定
                        log.warning(
                            f"{order.security} 资金不足: 需要 {total_cost:.2f}, 可用 {self.context.portfolio.available_cash:.2f}"
                        )
                        self.context.portfolio.locked_cash -= total_cost
                        order.status = OrderStatus.rejected
                        continue
                    # 扣除资金并释放锁定
                    self.context.portfolio.available_cash -= total_cost
                    self.context.portfolio.locked_cash -= total_cost

                    # 更新持仓
                    if order.security not in self.context.portfolio.positions:
                        self.context.portfolio.positions[order.security] = Position(
                            security=order.security
                        )
                    position = self.context.portfolio.positions[order.security]
                    prev_amount = int(getattr(position, "total_amount", 0) or 0)
                    position.update_position(trade_amount, trade_price)
                    if prev_amount <= 0:
                        position.buy_time = current_dt
                    position.last_buy_time = current_dt
                    # T+ 规则：若 tplus=1，将当日买入计入锁定，并从可卖数量中抵消同额增量
                    tplus = self._infer_tplus_from_info(info)
                    if tplus == 1:
                        # today_buy_t1 仅记录当日买入；抵消 update_position 对 closeable 的递增
                        position.today_buy_t1 = getattr(position, "today_buy_t1", 0) + trade_amount
                        position.closeable_amount = max(0, position.closeable_amount - trade_amount)
                    position.update_price(current_price)
                    log.info(
                        f"买入 {order.security}: {trade_amount} 股, 委托价 {fund_check_price:.{price_decimals}f}, 成交价 {trade_price:.{price_decimals}f}, 费用 {commission+tax:.2f}"
                    )
                else:
                    # 卖出：检查并执行
                    position = self.context.portfolio.positions[order.security]
                    avg_cost = float(getattr(position, "avg_cost", 0.0) or 0.0)
                    # 增加资金（卖出释放资金，不需锁定）
                    self.context.portfolio.available_cash += self._round_half_up(
                        (trade_value - commission - tax), 2
                    )

                    # 更新持仓
                    position.update_position(-trade_amount, trade_price)
                    if position.total_amount == 0:
                        del self.context.portfolio.positions[order.security]
                    else:
                        position.update_price(current_price)
                    log.info(
                        f"卖出 {order.security}: {trade_amount} 股, 成交价 {trade_price:.{price_decimals}f}, 费用 {commission+tax:.2f}"
                    )

                # 记录交易
                self._trade_seq += 1
                trade_id = f"T{self._trade_seq:08d}"
                trade = Trade(
                    order_id=order.order_id,
                    security=order.security,
                    amount=trade_amount if is_buy else -trade_amount,
                    price=trade_price,
                    time=current_dt,
                    commission=commission,
                    tax=tax,
                    trade_id=trade_id,
                )
                self.trades.append(trade)

                # 标记订单完成
                order.price = trade_price
                order.amount = trade_amount if is_buy else -trade_amount
                order.status = OrderStatus.filled
                order.filled = trade_amount
                order.avg_cost = avg_cost
                order.commission = float(commission + tax)
                try:
                    extra = getattr(order, "extra", None)
                    if extra is None:
                        order.extra = {}
                        extra = order.extra
                    extra["fill_price"] = float(trade_price)
                except Exception:
                    pass

            except Exception as e:
                log.error(f"处理订单失败: {order.security}, 错误: {e}")
                order.status = OrderStatus.rejected

        # 清空订单队列
        clear_order_queue()

        # 更新账户价值
        self.context.portfolio.update_value()

    def _register_order(self, order: Order) -> None:
        if not order:
            return
        oid = str(getattr(order, "order_id", "") or "")
        if not oid:
            return
        if oid not in self.orders:
            # 创建订单时写入的是真实墙钟时间，回测下须改写成回放时刻；
            # 只在首次登记时改写，避免跨时刻未成交订单的下单时间被后移
            current_dt = getattr(self.context, "current_dt", None)
            if current_dt is not None:
                order.add_time = current_dt
            self.orders[oid] = order

    def _normalize_status(self, status: Optional[object]) -> Optional[str]:
        if status is None:
            return None
        if isinstance(status, OrderStatus):
            return status.value
        try:
            return OrderStatus(str(status)).value
        except Exception:
            return None

    def _status_value(self, status: object) -> str:
        if isinstance(status, OrderStatus):
            return status.value
        return str(status)

    def get_orders(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
        status: Optional[object] = None,
        from_broker: bool = False,
    ) -> Dict[str, Order]:
        _ = from_broker
        # 先登记队列中的订单，避免遗漏未撮合订单
        for queued in list(get_order_queue() or []):
            self._register_order(queued)

        if not self.orders:
            return {}
        status_val = self._normalize_status(status)
        if status is not None and status_val is None:
            return {}
        target_id = str(order_id) if order_id is not None else None
        result: Dict[str, Order] = {}
        for oid, order in self.orders.items():
            if target_id and oid != target_id:
                continue
            if security and order.security != security:
                continue
            if status_val is not None and self._status_value(order.status) != status_val:
                continue
            result[oid] = order
        return result

    def get_open_orders(self) -> Dict[str, Order]:
        open_states = {
            OrderStatus.new.value,
            "submitted",
            OrderStatus.open.value,
            OrderStatus.filling.value,
            OrderStatus.canceling.value,
        }
        orders = self.get_orders()
        if not orders:
            return {}
        return {
            oid: order
            for oid, order in orders.items()
            if self._status_value(order.status) in open_states
        }

    def get_trades(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
    ) -> Dict[str, Trade]:
        if not self.trades:
            return {}
        target_id = str(order_id) if order_id is not None else None
        result: Dict[str, Trade] = {}
        for trade in self.trades:
            if target_id and trade.order_id != target_id:
                continue
            if security and trade.security != security:
                continue
            trade_id = trade.trade_id or ""
            if not trade_id:
                self._trade_seq += 1
                trade_id = f"T{self._trade_seq:08d}"
                trade.trade_id = trade_id
            result[trade_id] = trade
        return result

    def _current_held_amount(self, security: str, side: str) -> int:
        """取得目标单口径下的当前持仓数量。

        期货持仓不在组合的 positions 字典里，必须按方向从期货账本读取，
        否则每次目标单都被当成从零开仓，换月时旧合约永远平不掉。

        Args:
            security: 标的代码。
            side: 期货持仓方向，'long' 或 'short'；股票忽略该参数。

        Returns:
            int: 当前持有数量，无持仓时为 0。
        """

        portfolio = self.context.portfolio
        if is_futures_security(security):
            account = portfolio.futures_account
            if account is None:
                return 0
            position = account.get_position(security, side)
            return int(position.amount) if position is not None else 0
        position = portfolio.positions.get(security)
        return int(position.total_amount) if position is not None else 0

    def _lots_from_margin_value(
        self, security: str, value: float, price: float, day: Optional[date] = None
    ) -> int:
        """把保证金预算换算成期货手数。

        期货目标价值的口径是占用保证金而非合约全额：
        手数 = int(预算 / 价格 / 保证金率 / 合约乘数)。
        向下取整是刻意的，预算不足一手时为 0，不做四舍五入补整。

        Args:
            security: 合约代码。
            value: 保证金预算。
            price: 用于换算的价格。
            day: 交易日，用于命中保证金率的生效日期分段。

        Returns:
            int: 目标手数，非负。

        Raises:
            ContractSpecError: 合约乘数或保证金率缺失。
        """

        if not price or price <= 0:
            return 0
        resolved_day = day
        if resolved_day is None:
            current_dt = getattr(self.context, "current_dt", None)
            resolved_day = current_dt.date() if current_dt is not None else None
        spec_table = self._futures_spec_table
        multiplier = spec_table.multiplier(security, resolved_day)
        margin_rate = spec_table.margin_rate(security, resolved_day)
        if multiplier <= 0 or margin_rate <= 0:
            return 0
        # 除法顺序与目标价值口径的定义一致，先合并成单手保证金会改变截断结果
        return int(float(value) / float(price) / margin_rate / multiplier)

    def _calculate_order_amount(self, order, current_price: float) -> int:
        """计算订单实际数量"""
        # 普通订单
        if (
            not hasattr(order, "_target_amount")
            and not hasattr(order, "_target_value")
            and not hasattr(order, "_is_target_amount")
            and not hasattr(order, "_is_target_value")
        ):
            if is_futures_security(order.security):
                # 期货：符号表示开/平，买卖方向另由 side 决定
                return order.amount if order.action == "open" else -order.amount
            return order.amount if order.is_buy else -order.amount

        # 目标数量订单
        if hasattr(order, "_is_target_amount") and order._is_target_amount:
            side = str(getattr(order, "side", LONG) or LONG).lower()
            return int(order._target_amount) - self._current_held_amount(order.security, side)

        # 目标价值订单
        if hasattr(order, "_is_target_value") and order._is_target_value:
            side = str(getattr(order, "side", LONG) or LONG).lower()
            target_value = float(order._target_value)
            if is_futures_security(order.security):
                target_amount = self._lots_from_margin_value(
                    order.security, target_value, current_price
                )
            else:
                target_amount = self._amount_from_value(target_value, current_price)
            return target_amount - self._current_held_amount(order.security, side)

        # 按价值订单
        if hasattr(order, "_target_value"):
            amount = self._amount_from_value(float(order._target_value), current_price)
            return amount if order.is_buy else -amount

        return order.amount if order.is_buy else -order.amount

    def _update_positions(self):
        """更新持仓价格（使用收盘价）"""
        portfolio = self.context.portfolio
        if portfolio.positions:
            for security in list(portfolio.positions.keys()):
                try:
                    df = api_get_price(
                        security=security,
                        end_date=self.context.current_dt,
                        frequency="daily",
                        fields=["close"],
                        count=1,
                        fq="none",
                    )
                    if df.empty:
                        continue

                    last_row = df.iloc[-1]
                    close_price = None
                    if "close" in df.columns:
                        close_price = last_row["close"]
                    elif security in df.columns:
                        close_price = last_row[security]
                    elif ("close", security) in df.columns:
                        close_price = last_row[("close", security)]
                    else:
                        log.error(f"{security} 无法匹配收盘价列，列={list(df.columns)}")
                        continue

                    if pd.notna(close_price) and close_price > 0:
                        price = self._normalize_split_day_close_price(
                            security, float(close_price)
                        )
                        portfolio.positions[security].update_price(price)
                except Exception as e:
                    log.debug(f"更新{security}价格失败: {e}")

        self._mark_futures_positions()
        portfolio.update_value()

    def _record_daily(self):
        """记录每日数据"""
        portfolio = self.context.portfolio
        base_total_value = (
            self.start_total_value if self.start_total_value is not None else self.initial_cash
        )

        record = {
            "date": self.context.current_dt,
            "total_value": portfolio.total_value,
            "cash": portfolio.available_cash,
            "positions_value": portfolio.positions_value,
            "returns": portfolio.total_value - base_total_value,
            "returns_pct": (portfolio.total_value / base_total_value - 1) * 100,
        }

        futures_account = portfolio.futures_account
        if futures_account is not None:
            record["futures_margin"] = futures_account.margin
            record["futures_pnl"] = futures_account.mark_to_market_pnl
            record["futures_lots"] = sum(p.amount for p in futures_account.iter_positions())

        benchmark_price = self._resolve_benchmark_close(self.context.current_dt)
        if benchmark_price is not None and benchmark_price > 0:
            if self._benchmark_base_price is None:
                self._benchmark_base_price = benchmark_price
            if self._benchmark_base_price and self._benchmark_base_price > 0:
                benchmark_value = base_total_value * benchmark_price / self._benchmark_base_price
                benchmark_returns_pct = (benchmark_value / base_total_value - 1) * 100
                record["benchmark_price"] = benchmark_price
                record["benchmark_value"] = benchmark_value
                record["benchmark_returns_pct"] = benchmark_returns_pct
                record["excess_returns_pct"] = record["returns_pct"] - benchmark_returns_pct

        self.daily_records.append(record)

        log.info(
            f"账户总值: {portfolio.total_value:,.2f}, 现金: {portfolio.available_cash:,.2f}, 持仓市值: {portfolio.positions_value:,.2f}"
        )
        log.info(f"累计收益率: {record['returns_pct']:.2f}%")
        if "benchmark_returns_pct" in record:
            log.info(
                f"基准收益率: {record['benchmark_returns_pct']:.2f}%, "
                f"累计超额收益: {record['excess_returns_pct']:.2f}%"
            )

    # 新增：记录每日持仓快照（在更新收盘价后调用）
    def _record_daily_positions(self):
        portfolio = self.context.portfolio
        current_dt = self.context.current_dt
        for code, pos in portfolio.positions.items():
            self.daily_positions.append(
                {
                    "date": current_dt,
                    "code": code,
                    "amount": pos.total_amount,
                    "closeable_amount": pos.closeable_amount,
                    "avg_cost": pos.avg_cost,
                    "acc_avg_cost": pos.acc_avg_cost,
                    "price": pos.price,
                    "value": pos.value,
                }
            )

        futures_account = portfolio.futures_account
        if futures_account is None:
            return
        for position in futures_account.iter_positions():
            self.daily_positions.append(
                {
                    "date": current_dt,
                    "code": position.security,
                    "amount": position.amount,
                    "closeable_amount": position.closeable_amount,
                    "avg_cost": position.open_price,
                    "acc_avg_cost": position.prev_settlement,
                    "price": position.last_price,
                    "value": position.margin_held,
                    "side": position.side,
                    "today_amount": position.today_amount,
                    "yesterday_amount": position.yesterday_amount,
                }
            )

    def _load_benchmark_data(self, benchmark: str):
        """
        加载基准数据

        注意：直接使用原始 jq.get_price，不经过包装函数，
        因为基准数据是预加载整个回测期间的数据，不需要未来数据检测
        """
        provider = get_data_provider()
        last_error: Optional[Exception] = None
        for candidate in _iter_security_code_candidates(benchmark):
            try:
                if candidate == benchmark:
                    log.info(f"加载基准数据: {candidate}")
                else:
                    log.info(f"加载基准数据兼容尝试: {benchmark} -> {candidate}")

                # 通过当前数据提供者直接获取（不经过包装，避免未来数据检测），一次性加载全区间收盘价
                data = provider.get_price(
                    security=candidate,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    frequency="daily",
                    fields=["close"],
                )
                if data is None or len(data) == 0:
                    last_error = ValueError(f"标的{candidate}返回空数据")
                    continue
                self.benchmark_data = data
                if candidate != benchmark:
                    log.info(f"基准代码兼容成功: {benchmark} -> {candidate}")
                return
            except Exception as e:
                last_error = e

        if last_error is not None:
            log.warning(f"加载基准数据失败: {last_error}")

    def _resolve_benchmark_close(self, current_dt: datetime) -> Optional[float]:
        if self.benchmark_data is None:
            return None
        try:
            df = self.benchmark_data
            if df is None or len(df) == 0:
                return None
            work_df = df.copy()
            if not isinstance(work_df.index, pd.DatetimeIndex):
                work_df.index = pd.to_datetime(work_df.index)
            target_dt = pd.Timestamp(current_dt).normalize()
            eligible = work_df.loc[work_df.index <= target_dt]
            if eligible.empty:
                return None
            row = eligible.iloc[-1]
            if isinstance(row, pd.Series):
                if "close" in row.index:
                    price = row["close"]
                elif len(row) == 1:
                    price = row.iloc[0]
                else:
                    price = None
                    for key, value in row.items():
                        if isinstance(key, tuple) and key and str(key[0]).lower() == "close":
                            price = value
                            break
                    if price is None:
                        for key, value in row.items():
                            if str(key).lower() == "close":
                                price = value
                                break
                    if price is None:
                        price = row.iloc[-1]
            else:
                price = row
            if price is None or pd.isna(price):
                return None
            return float(price)
        except Exception as exc:
            log.debug(f"解析 benchmark 收盘价失败: {exc}")
            return None

    def _generate_empty_results(self) -> Dict[str, Any]:
        """生成空回测结果（没有交易日的情况）"""
        results = {
            "summary": {
                "策略收益": "0.00%",
                "策略年化收益": "0.00%",
                "最大回撤": "0.00%",
                "夏普比率": "0.00",
                "日胜率": "0.00%",
                "交易胜率": "0.00%",
                "交易天数": 0,
                "初始资金": f"{self.initial_cash:,.2f}",
                "最终资金": f"{self.initial_cash:,.2f}",
            },
            "daily_records": pd.DataFrame(
                columns=["date", "total_value", "cash", "position_value", "daily_returns"]
            ).set_index("date"),
            "trades": [],
            "events": [],
            "daily_positions": pd.DataFrame(
                columns=[
                    "date",
                    "code",
                    "amount",
                    "closeable_amount",
                    "avg_cost",
                    "acc_avg_cost",
                    "price",
                    "value",
                ]
            ),
            "custom_plot": None,
            "meta": {
                "strategy_file": self.strategy_file,
                "start_date": self.start_date.strftime("%Y-%m-%d"),
                "end_date": self.end_date.strftime("%Y-%m-%d"),
                "algorithm_id": self.algorithm_id,
                "extras": self.extras,
                "runtime_seconds": getattr(self, "runtime_seconds", 0.0),
                "run_started_at": self.run_started_at,
                "run_finished_at": self.run_finished_at,
                "benchmark": get_settings().benchmark,
                "initial_total_value": float(self.initial_cash),
                "final_total_value": float(self.initial_cash),
            },
        }

        # 打印摘要
        log.info("\n" + "=" * 60)
        log.info("回测结果摘要（无交易日）")
        log.info("=" * 60)
        for key, value in results["summary"].items():
            log.info(f"{key}: {value}")
        log.info("=" * 60)

        return results

    def _generate_results(self) -> Dict[str, Any]:
        """生成回测结果"""
        df = pd.DataFrame(self.daily_records)

        # 检查是否有数据
        if df.empty or "date" not in df.columns:
            log.warning("没有回测数据，返回空结果")
            return self._generate_empty_results()

        df.set_index("date", inplace=True)

        # 计算日收益率
        df["daily_returns"] = df["total_value"].pct_change()

        # 基本统计
        total_returns = (df["total_value"].iloc[-1] / self.initial_cash - 1) * 100
        trading_days = len(df)
        years = trading_days / 250
        annual_returns = (
            (pow(df["total_value"].iloc[-1] / self.initial_cash, 1 / years) - 1) * 100
            if years > 0
            else 0
        )

        # 最大回撤
        cummax = df["total_value"].expanding().max()
        drawdown = (df["total_value"] - cummax) / cummax * 100
        max_drawdown = drawdown.min()

        # 夏普比率（假设无风险利率为3%）
        risk_free_rate = 0.03 / 250
        excess_returns = df["daily_returns"] - risk_free_rate
        sharpe_ratio = (
            np.sqrt(250) * excess_returns.mean() / excess_returns.std()
            if excess_returns.std() > 0
            else 0
        )

        # 日胜率（仅作为摘要展示，完整指标在 analysis.calculate_metrics）
        winning_days = (df["daily_returns"] > 0).sum()
        win_rate_daily = winning_days / trading_days * 100 if trading_days > 0 else 0

        # 交易胜率（基于卖出回合）
        try:
            from .analysis import _compute_trade_win_stats

            trade_stats = _compute_trade_win_stats(self.trades)
        except Exception:
            trade_stats = {"交易胜率": 0.0}

        # 构建自定义绘图数据（优先使用 g.custom_plot，其次聚合 record() 收集的序列）
        custom_plot_obj = getattr(g, "custom_plot", None)
        if custom_plot_obj is None and hasattr(g, "record_series") and g.record_series:
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
                log.warning(f"构建自定义图数据失败: {e}")

        results = {
            "summary": {
                "策略收益": f"{total_returns:.2f}%",
                "策略年化收益": f"{annual_returns:.2f}%",
                "最大回撤": f"{max_drawdown:.2f}%",
                "夏普比率": f"{sharpe_ratio:.2f}",
                "日胜率": f"{win_rate_daily:.2f}%",
                "交易胜率": f"{float(trade_stats.get('交易胜率', 0.0)):.2f}%",
                "交易天数": trading_days,
                "初始资金": f"{(self.start_total_value if self.start_total_value is not None else self.initial_cash):,.2f}",
                "最终资金": f'{df["total_value"].iloc[-1]:,.2f}',
            },
            "daily_records": df,
            "trades": self.trades,
            "events": self.events,
            "daily_positions": pd.DataFrame(self.daily_positions)
            if self.daily_positions
            else pd.DataFrame(
                columns=[
                    "date",
                    "code",
                    "amount",
                    "closeable_amount",
                    "avg_cost",
                    "acc_avg_cost",
                    "price",
                    "value",
                ]
            ),
            "custom_plot": custom_plot_obj,
            "meta": {
                "strategy_file": self.strategy_file,
                "start_date": self.start_date.strftime("%Y-%m-%d"),
                "end_date": self.end_date.strftime("%Y-%m-%d"),
                "algorithm_id": self.algorithm_id,
                "extras": self.extras,
                "runtime_seconds": self.runtime_seconds,
                "run_started_at": self.run_started_at,
                "run_finished_at": self.run_finished_at,
                "benchmark": get_settings().benchmark,
                "initial_total_value": float(
                    self.start_total_value
                    if self.start_total_value is not None
                    else self.initial_cash
                ),
                "final_total_value": float(df["total_value"].iloc[-1] if len(df) > 0 else 0.0),
            },
        }

        # 打印摘要
        log.info("\n" + "=" * 60)
        log.info("回测结果摘要")
        log.info("=" * 60)
        for key, value in results["summary"].items():
            log.info(f"{key}: {value}")
        log.info("=" * 60)

        return results


def create_backtest(
    strategy_file: str,
    start_date: str,
    end_date: str,
    frequency: str = "day",
    initial_cash: float = 100000,
    benchmark: Optional[str] = None,
    log_file: Optional[str] = None,
    extras: Optional[Dict[str, Any]] = None,
    initial_positions: Optional[List[Dict[str, Any]]] = None,
    algorithm_id: Optional[str] = None,
    data_session_config: Optional[Dict[str, Any]] = None,
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
        data_session_config: 回测数据会话配置（可选）
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
        data_session_config=data_session_config,
    )

    return engine.run()


__all__ = ["BacktestEngine", "create_backtest"]


# 新增：应用初始持仓（辅助方法）
def _safe_last_close(df: pd.DataFrame, code: str) -> Optional[float]:
    try:
        if df is None or df.empty:
            return None
        last_row = df.iloc[-1]
        # 支持三种列结构：['close'], [code], [('close', code)]
        if "close" in df.columns:
            val = last_row["close"]
        elif code in df.columns:
            val = last_row[code]
        elif ("close", code) in df.columns:
            val = last_row[("close", code)]
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
                code = str(item.get("security"))
                amount = int(item.get("amount") or 0)
                avg_cost = item.get("avg_cost")
                if amount <= 0 or not code:
                    continue
                # 获取起始日前的/当日收盘价作为估值价格
                df = provider.get_price(
                    security=code,
                    end_date=self.start_date,
                    frequency="daily",
                    fields=["close"],
                    count=1,
                    fq="pre",
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
                raw_buy_time = item.get("buy_time") or item.get("init_time")
                if raw_buy_time is not None:
                    try:
                        pos.buy_time = pd.to_datetime(raw_buy_time).to_pydatetime()
                    except Exception:
                        pos.buy_time = None
                if pos.buy_time is None:
                    pos.buy_time = pd.to_datetime(self.start_date).to_pydatetime()
                pos.last_buy_time = pos.buy_time
            except Exception as ie:
                log.debug(f"初始化持仓失败 {item}: {ie}")
        # 更新账户总值（现金 + 持仓）
        portfolio.update_value()
    except Exception as e:
        log.warning(f"应用初始持仓失败: {e}")


# 绑定到类
BacktestEngine._apply_initial_positions = _apply_initial_positions


def _apply_subportfolio_config(self):
    """按策略声明的子账户配置重建 portfolio.subportfolios 视图。

    Returns:
        None: 未声明子账户时保持默认 stock 子账户不变。
    """

    configs = get_subportfolio_configs()
    if not configs:
        return

    portfolio = self.context.portfolio
    rebuilt: Dict[str, SubPortfolio] = {}
    declared_cash = 0.0
    for index, config in enumerate(configs):
        account_type = str(config.type or "stock")
        key = account_type if account_type not in rebuilt else f"{account_type}_{index}"
        previous = portfolio.subportfolios.get(account_type)
        cash = float(config.cash or 0.0)
        declared_cash += cash
        rebuilt[key] = SubPortfolio(
            type=account_type,
            available_cash=cash,
            transferable_cash=cash,
            total_value=cash,
            positions=dict(previous.positions) if previous is not None else {},
        )

    portfolio.subportfolios = rebuilt
    portfolio.update_value()

    # 现金池仍以 portfolio.available_cash 为准，子账户只是视图，不做拆分
    if abs(declared_cash - float(portfolio.available_cash)) > 1e-6:
        log.warning(
            "子账户声明现金合计 %.2f 与组合可用资金 %.2f 不一致，"
            "回测仍以组合现金池为准，不按子账户拆分资金",
            declared_cash,
            float(portfolio.available_cash),
        )


# 绑定到类
BacktestEngine._apply_subportfolio_config = _apply_subportfolio_config
