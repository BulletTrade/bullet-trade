"""
异步回测引擎

在 BacktestEngine 基础上添加事件驱动支持
- 集成 EventLoop
- 集成 EventBus
- 集成 AsyncScheduler
- 发布事件
- 保持向后兼容
"""

import asyncio
import functools
from typing import Dict, Any, Optional, Callable, Sequence, Tuple
from datetime import datetime, time as Time
import pandas as pd
import time as python_time

from .engine import BacktestEngine, PRE_MARKET_OFFSET
from .event_loop import EventLoop
from .event_bus import EventBus, EventPriority
from .async_scheduler import AsyncScheduler, OverlapStrategy
from .events import (
    BacktestStartEvent,
    BacktestEndEvent,
    TradingDayStartEvent,
    TradingDayEndEvent,
    BeforeTradingStartEvent,
    MarketOpenEvent,
    MarketCloseEvent,
    AfterTradingEndEvent,
)
from .globals import g, log
from .settings import get_settings, set_option
from .orders import get_order_queue
from .scheduler import generate_daily_schedule, get_market_periods
from ..data.api import set_current_context, get_data_provider


class AsyncBacktestEngine(BacktestEngine):
    """
    异步回测引擎
    
    在 BacktestEngine 基础上添加事件驱动支持：
    - 使用 AsyncScheduler 替代同步 scheduler
    - 发布事件到 EventBus
    - 支持异步策略函数
    - 保持向后兼容（同步策略照常运行）
    
    Example:
        >>> engine = AsyncBacktestEngine(initialize=my_init, handle_data=my_handle)
        >>> results = await engine.run_async(
        ...     start_date='2024-01-01',
        ...     end_date='2024-12-31',
        ...     capital_base=100000
        ... )
    """
    
    def __init__(self, *args, use_uvloop: bool = True, **kwargs):
        """
        初始化异步回测引擎
        
        Args:
            *args: 传递给 BacktestEngine 的参数
            use_uvloop: 是否使用 uvloop（仅 macOS/Linux）
            **kwargs: 传递给 BacktestEngine 的关键字参数
        """
        super().__init__(*args, **kwargs)
        
        # 事件驱动组件
        self.event_loop: Optional[EventLoop] = None
        self.event_bus: Optional[EventBus] = None
        self.async_scheduler: Optional[AsyncScheduler] = None
        self._use_uvloop = use_uvloop
        
        # 标志位
        self._async_mode = False
    
    def _setup_event_framework(self):
        """设置事件驱动框架"""
        # 获取当前事件循环（已在 run() 中创建）
        loop = asyncio.get_event_loop()
        
        # 创建事件总线
        self.event_bus = EventBus(loop)
        log.info("✅ 事件总线已创建")
        
        # 创建异步调度器
        self.async_scheduler = AsyncScheduler()
        log.info("✅ 异步调度器已创建")
    
    def _teardown_event_framework(self):
        """清理事件驱动框架"""
        if self.async_scheduler:
            self.async_scheduler.unschedule_all()
        
        if self.event_bus:
            self.event_bus.unsubscribe_all()
    
    def _migrate_scheduler_tasks(self):
        """
        将同步调度器的任务迁移到异步调度器
        
        在策略 initialize 后调用，将 run_daily/run_weekly/run_monthly
        注册的任务迁移到 AsyncScheduler
        """
        from .scheduler import get_tasks
        
        tasks = get_tasks()
        if not tasks:
            return
        
        log.info(f"迁移 {len(tasks)} 个调度任务到异步调度器")
        
        for task in tasks:
            # 确定重叠策略（默认SKIP，推荐）
            overlap_strategy = OverlapStrategy.SKIP
            
            # 注册到异步调度器
            if task.schedule_type.value == 'daily':
                self.async_scheduler.run_daily(
                    task.func,
                    task.time,
                    overlap_strategy
                )
            elif task.schedule_type.value == 'weekly':
                self.async_scheduler.run_weekly(
                    task.func,
                    task.weekday,
                    task.time,
                    overlap_strategy,
                    force=getattr(task, 'force', False),
                )
            elif task.schedule_type.value == 'monthly':
                self.async_scheduler.run_monthly(
                    task.func,
                    task.monthday,
                    task.time,
                    overlap_strategy,
                    force=getattr(task, 'force', False),
                )
            
            log.debug(
                f"  迁移任务: {task.func.__name__} "
                f"({task.schedule_type.value}, {task.time})"
            )
    
    async def run_async(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        capital_base: Optional[float] = None,
        frequency: Optional[str] = None,
        benchmark: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        异步运行回测
        
        Args:
            start_date: 回测开始日期
            end_date: 回测结束日期
            capital_base: 初始资金
            frequency: 回测频率
            benchmark: 基准标的
            
        Returns:
            回测结果字典
        """
        self._async_mode = True
        
        # 覆盖参数
        if start_date:
            self.start_date = pd.to_datetime(start_date)
        if end_date:
            self.end_date = pd.to_datetime(end_date)
        if capital_base:
            self.initial_cash = capital_base
        if frequency:
            self.frequency = frequency
        
        # 设置事件框架
        self._setup_event_framework()
        
        try:
            # 运行异步回测
            results = await self._run_async_backtest(benchmark)
            return results
        finally:
            # 清理
            self._teardown_event_framework()
    
    async def _run_async_backtest(self, benchmark: Optional[str] = None) -> Dict[str, Any]:
        """
        异步回测主循环
        
        Args:
            benchmark: 基准标的
            
        Returns:
            回测结果
        """
        start_time = python_time.time()
        
        # 设置日志文件
        if self.log_file:
            self._setup_log_file()
        
        log.info("=" * 60)
        log.info(f"开始异步回测: {self.start_date.strftime('%Y-%m-%d')} 至 {self.end_date.strftime('%Y-%m-%d')}")
        log.info(f"初始资金: {self.initial_cash:,.2f}")
        log.info(f"事件驱动模式: 已启用 ⚡")
        log.info("=" * 60)

        # 设置回测频率到 settings，供调度解析使用
        set_option('backtest_frequency', self.frequency)
        set_option('backtest_start_date', self.start_date.date())
        
        # 发布回测开始事件
        await self.event_bus.emit(BacktestStartEvent(
            start_date=self.start_date,
            end_date=self.end_date,
            initial_cash=self.initial_cash
        ))
        
        # 加载策略（同步部分）
        self.load_strategy()

        # load_strategy 内部会重置设置，这里重新注入频率
        set_option('backtest_frequency', self.frequency)
        set_option('backtest_start_date', self.start_date.date())
        
        # 初始化上下文（同步）
        self._initialize_context()
        
        # 设置基准
        if benchmark:
            from .settings import set_benchmark
            set_benchmark(benchmark)
        
        # 调用策略初始化（可能是异步的）
        await self._call_initialize()
        
        # 迁移调度任务到异步调度器
        self._migrate_scheduler_tasks()
        
        # 获取交易日列表
        trade_days = self._get_trade_days()
        log.info(f"交易日数量: {len(trade_days)}")
        
        # 获取基准数据
        settings = get_settings()
        if settings.benchmark:
            self._load_benchmark_data(settings.benchmark)
        
        # 逐日回测循环（异步）
        for i, trade_day in enumerate(trade_days):
            await self._process_trading_day(trade_day, i, len(trade_days))
        
        # 生成回测结果
        results = self._generate_results()
        
        # 记录耗时
        self.runtime_seconds = python_time.time() - start_time
        results['runtime_seconds'] = self.runtime_seconds
        
        log.info("=" * 60)
        log.info(f"异步回测完成！耗时: {self.runtime_seconds:.2f}秒")
        log.info("=" * 60)
        
        # 发布回测结束事件
        await self.event_bus.emit(BacktestEndEvent(
            total_returns=results.get('total_returns', 0),
            final_value=results.get('final_portfolio_value', 0)
        ))
        
        # 清理日志文件
        if self.log_file:
            self._cleanup_log_file()
        
        return results
    
    def _initialize_context(self):
        """初始化上下文（同步部分）"""
        from .models import Context, Portfolio
        
        self.context = Context(
            portfolio=Portfolio(
                total_value=self.initial_cash,
                available_cash=self.initial_cash,
                starting_cash=self.initial_cash
            ),
            current_dt=self.start_date
        )
        
        set_current_context(self.context)
        
        # 注入运行参数
        if self.extras is not None:
            g.extras = self.extras
        
        self.context.run_params = {
            'algorithm_id': self.algorithm_id,
            'start_date': self.start_date.strftime('%Y-%m-%d'),
            'end_date': self.end_date.strftime('%Y-%m-%d'),
            'frequency': self.frequency,
            'initial_cash': self.initial_cash,
            'extras': self.extras,
            'initial_positions': self.initial_positions,
        }
        
        # 应用初始持仓
        self._apply_initial_positions()
        
        # 设置收益基准
        self.start_total_value = float(
            self.context.portfolio.available_cash +
            self.context.portfolio.positions_value +
            self.context.portfolio.locked_cash
        )
    
    async def _call_initialize(self):
        """调用策略初始化函数（支持异步）"""
        if not self.initialize_func:
            return
        
        log.info("调用策略初始化函数...")
        
        try:
            if asyncio.iscoroutinefunction(self.initialize_func):
                # 异步初始化
                await self.initialize_func(self.context)
            else:
                # 同步初始化：在线程池中运行
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    self.initialize_func,
                    self.context
                )
            
            log.info("策略初始化完成")
            
            # 覆盖 g 参数
            if self.extras:
                for k, v in self.extras.items():
                    try:
                        setattr(g, k, v)
                    except Exception:
                        pass
        
        except Exception as e:
            log.error(f"策略初始化失败: {e}")
            import traceback
            log.error(traceback.format_exc())
            raise

    async def _call_sync_or_async(self, func: Callable, *args):
        """根据函数类型自动选择 await 或线程池执行。"""
        if asyncio.iscoroutinefunction(func):
            return await func(*args)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, functools.partial(func, *args))
    
    def _get_trade_days(self):
        """获取交易日列表"""
        provider = get_data_provider()
        trade_days = provider.get_trade_days(
            start_date=self.start_date,
            end_date=self.end_date
        )
        return [pd.to_datetime(d) for d in trade_days]

    def _is_bar_time(self, current_dt: datetime, market_periods: Sequence[Tuple[Time, Time]], open_dt: datetime) -> bool:
        """
        判断当前时间是否应视为一个 bar 触发点：
        - 分钟频率：交易时段内的每个整分
        - 其他频率：仅在开盘触发
        """
        if str(getattr(self, "frequency", "day")).lower() == "minute":
            if current_dt.second != 0:
                return False
            current_time = current_dt.time()
            for start, end in market_periods:
                if start <= current_time < end:
                    return True
            return False
        return current_dt == open_dt
    
    async def _process_trading_day(self, trade_day: datetime, day_index: int, total_days: int):
        """
        处理单个交易日（异步）
        
        Args:
            trade_day: 交易日
            day_index: 日期索引
            total_days: 总交易日数
        """
        self.context.previous_dt = self.context.current_dt if day_index > 0 else None
        if day_index > 0:
            self.context.previous_date = self.context.current_dt.date()
        
        self.context.current_dt = trade_day
        
        log.info(f"\n{'=' * 60}")
        log.info(f"交易日: {trade_day.strftime('%Y-%m-%d')} ({day_index+1}/{total_days})")
        
        await self.event_bus.emit(TradingDayStartEvent(date=trade_day))
        
        market_periods = get_market_periods()
        schedule_map = generate_daily_schedule(trade_day, getattr(self.context, "previous_date", None), market_periods)
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
            
            is_trading_time = self._is_trading_time(current_dt, market_periods)
            is_bar = self._is_bar_time(current_dt, market_periods, open_dt)
            
            if self.async_scheduler:
                try:
                    await self.async_scheduler.trigger(current_dt, self.context, is_bar=is_bar)
                except Exception as e:
                    log.error(f"异步调度执行失败: {e}")
            
            if current_dt == pre_open_dt:
                await self.event_bus.emit(BeforeTradingStartEvent(date=trade_day))
                if self.before_trading_start_func:
                    try:
                        await self._call_sync_or_async(self.before_trading_start_func, self.context)
                    except Exception as e:
                        log.error(f"盘前函数执行失败: {e}")
                        import traceback
                        log.error(traceback.format_exc())
            
            if current_dt == open_dt:
                await self.event_bus.emit(MarketOpenEvent(time=current_dt.strftime("%H:%M:%S")))
                if self.handle_data_func:
                    try:
                        from ..data.api import get_current_data
                        data = get_current_data()
                        await self._call_sync_or_async(self.handle_data_func, self.context, data)
                    except Exception as e:
                        log.error(f"交易函数执行失败: {e}")
                        import traceback
                        log.error(traceback.format_exc())
            
            if is_trading_time:
                self._process_orders(current_dt)
            
            if current_dt == close_dt:
                await self.event_bus.emit(MarketCloseEvent(time=current_dt.strftime("%H:%M:%S")))
                if self.after_trading_end_func:
                    try:
                        await self._call_sync_or_async(self.after_trading_end_func, self.context)
                    except Exception as e:
                        log.error(f"盘后函数执行失败: {e}")
                        import traceback
                        log.error(traceback.format_exc())
                await self.event_bus.emit(AfterTradingEndEvent(date=trade_day))
        
        self._record_daily()
        self._update_positions()
        
        await self.event_bus.emit(TradingDayEndEvent(
            date=trade_day,
            portfolio_value=self.context.portfolio.total_value
        ))
    
    # 重写 run() 方法，支持异步模式
    def run(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        capital_base: Optional[float] = None,
        frequency: Optional[str] = None,
        benchmark: Optional[str] = None,
        use_async: bool = False  # 新增参数
    ) -> Dict[str, Any]:
        """
        运行回测
        
        Args:
            start_date: 回测开始日期
            end_date: 回测结束日期
            capital_base: 初始资金
            frequency: 回测频率
            benchmark: 基准标的
            use_async: 是否使用异步模式（默认False，保持兼容）
            
        Returns:
            回测结果字典
        """
        if use_async:
            # 异步模式
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(
                    self.run_async(start_date, end_date, capital_base, frequency, benchmark)
                )
            finally:
                loop.close()
        else:
            # 同步模式（原有逻辑）
            return super().run(start_date, end_date, capital_base, frequency, benchmark)
