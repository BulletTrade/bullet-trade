"""
异步调度器模块

基于事件驱动的异步调度系统，参考 jqtrade 设计
- 支持 run_daily/run_weekly/run_monthly 异步化
- 防止任务重叠执行（执行锁机制）
- 支持多种执行策略（跳过/等待/并发）
"""

import asyncio
from typing import Callable, Optional, Dict, List, Any, Tuple, Sequence
from datetime import date, datetime, time as Time
from dataclasses import dataclass, field
from enum import Enum
import inspect
import traceback

from .scheduler import (
    TimeExpression,
    generate_daily_schedule,
    get_market_periods,
    get_time_aliases,
    get_trade_calendar,
)
from .globals import log


# 统一使用全局 logger，继承彩色控制台/文件级别配置
logger = log.logger


class ScheduleType(Enum):
    """调度类型"""
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'


class OverlapStrategy(Enum):
    """
    任务重叠执行策略
    
    当上一次任务还未完成，新的调度时间到来时的处理策略
    """
    SKIP = 'skip'          # 跳过本次执行（默认，推荐）
    WAIT = 'wait'          # 等待上次执行完成
    CONCURRENT = 'concurrent'  # 允许并发执行（需处理竞态）


@dataclass
class AsyncScheduleTask:
    """
    异步调度任务
    
    Attributes:
        func: 要执行的函数（同步或异步）
        schedule_type: 调度类型
        time: 执行时间表达式
        weekday: 星期几（0=周一，6=周日），仅 weekly 使用
        monthday: 每月几号（1-31），仅 monthly 使用
        overlap_strategy: 重叠执行策略
        enabled: 是否启用
        task_id: 任务唯一标识
        _lock: 执行锁（内部使用）
        _running: 是否正在执行
        _last_run: 上次执行时间
        _run_count: 执行次数
    """
    func: Callable
    schedule_type: ScheduleType
    time: str
    expression: Optional[TimeExpression] = None
    weekday: Optional[int] = None
    monthday: Optional[int] = None
    reference_security: Optional[str] = None
    force: bool = True
    overlap_strategy: OverlapStrategy = OverlapStrategy.SKIP
    enabled: bool = True
    task_id: str = field(default_factory=lambda: '')
    
    # 内部状态
    _lock: Optional[asyncio.Lock] = field(default=None, init=False, repr=False)
    _running: bool = field(default=False, init=False, repr=False)
    _last_run: Optional[datetime] = field(default=None, init=False, repr=False)
    _run_count: int = field(default=0, init=False, repr=False)
    last_trigger_marker: Optional[Tuple[int, int]] = field(default=None, init=False, repr=False)
    
    def __post_init__(self):
        """初始化后处理"""
        if not self.task_id:
            # 生成任务ID
            func_name = self.func.__name__ if hasattr(self.func, '__name__') else str(self.func)
            self.task_id = f"{self.schedule_type.value}_{func_name}_{self.time}"
        
        # 创建执行锁
        self._lock = asyncio.Lock()
    
    def should_run(
        self,
        current_dt: datetime,
        is_bar: bool,
        market_periods: Sequence[Tuple[Time, Time]],
        previous_trade_day: Optional[date],
    ) -> bool:
        """
        判断是否应该执行
        
        Args:
            current_dt: 当前时间
            is_bar: 是否为每个bar时刻
            
        Returns:
            是否应该执行
        """
        if not self.enabled:
            return False
        
        if not self.expression:
            return False

        expr = self.expression

        if expr.kind == 'every_bar':
            return is_bar
        
        if self.schedule_type == ScheduleType.WEEKLY and self.weekday is not None:
            if current_dt.weekday() != self.weekday:
                return False

        if self.schedule_type == ScheduleType.MONTHLY:
            if not self._should_trigger_monthly(current_dt.date(), previous_trade_day):
                return False

        if expr.kind == 'every_minute':
            return self._is_trading_time(current_dt, market_periods)

        if expr.kind == 'explicit':
            target = expr.explicit
            current_time = current_dt.time()
            return (
                current_time.hour == target.hour and
                current_time.minute == target.minute and
                current_time.second == target.second
            )

        if expr.kind == 'relative':
            if expr.base == 'open':
                base_time = market_periods[0][0]
            else:
                base_time = market_periods[-1][1]
            target_dt = datetime.combine(current_dt.date(), base_time) + expr.offset
            return current_dt == target_dt

        return False

    @staticmethod
    def _is_trading_time(current_dt: datetime, market_periods: Sequence[Tuple[Time, Time]]) -> bool:
        if current_dt.second != 0:
            return False
        current_time = current_dt.time()
        for start, end in market_periods:
            if start <= current_time < end:
                return True
        return False

    def _should_trigger_monthly(self, current_date: date, previous_trade_day: Optional[date]) -> bool:
        if self.monthday is None:
            return False
        monthday = self.monthday
        if monthday < 1 or monthday > 31:
            return False
        if current_date.day < monthday:
            return False
        marker = (current_date.year, current_date.month)
        if self.last_trigger_marker == marker:
            return False
        if previous_trade_day and previous_trade_day.month == current_date.month and previous_trade_day.day >= monthday:
            return False
        self.last_trigger_marker = marker
        return True
    
    async def execute(self, *args, **kwargs) -> Any:
        """
        执行任务
        
        根据重叠策略处理执行冲突
        
        Args:
            *args: 传递给任务函数的参数
            **kwargs: 传递给任务函数的关键字参数
            
        Returns:
            任务函数的返回值（如果执行）
        """
        # 检查重叠策略
        if self.overlap_strategy == OverlapStrategy.SKIP:
            # 跳过策略：如果正在执行，跳过本次
            if self._running:
                logger.warning(
                    f"⏭️  任务 {self.task_id} 正在执行，跳过本次调度"
                )
                return None
            
            # 尝试获取锁（非阻塞）
            if not self._lock.locked():
                async with self._lock:
                    return await self._do_execute(*args, **kwargs)
            else:
                logger.warning(f"⏭️  任务 {self.task_id} 锁定中，跳过")
                return None
        
        elif self.overlap_strategy == OverlapStrategy.WAIT:
            # 等待策略：等待上次执行完成
            async with self._lock:
                if self._running:
                    logger.info(f"⏳ 任务 {self.task_id} 等待上次执行完成...")
                return await self._do_execute(*args, **kwargs)
        
        elif self.overlap_strategy == OverlapStrategy.CONCURRENT:
            # 并发策略：允许同时执行多个实例
            logger.warning(
                f"⚠️  任务 {self.task_id} 允许并发执行，注意竞态条件！"
            )
            return await self._do_execute(*args, **kwargs)
    
    async def _do_execute(self, *args, **kwargs) -> Any:
        """
        实际执行任务
        
        Args:
            *args: 参数
            **kwargs: 关键字参数
            
        Returns:
            任务函数返回值
        """
        self._running = True
        start_time = datetime.now()
        
        try:
            logger.debug(f"▶️  执行任务: {self.task_id}")
            
            # 检查函数类型并执行
            if asyncio.iscoroutinefunction(self.func):
                # 异步函数
                result = await self.func(*args, **kwargs)
            else:
                # 同步函数：在线程池中执行
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, self.func, *args, **kwargs)
            
            self._run_count += 1
            self._last_run = datetime.now()
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.debug(
                f"✅ 任务完成: {self.task_id} "
                f"(耗时: {duration:.3f}s, 执行次数: {self._run_count})"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 任务执行失败: {self.task_id} - {e}", exc_info=True)
            raise
        finally:
            self._running = False


class AsyncScheduler:
    """
    异步调度器
    
    管理所有异步调度任务，支持：
    - 任务注册和管理
    - 重叠执行控制
    - 任务启用/禁用
    - 统计信息
    
    Example:
        >>> scheduler = AsyncScheduler()
        >>> 
        >>> async def my_task(context):
        ...     print("执行任务")
        >>> 
        >>> scheduler.run_daily(my_task, '09:30')
        >>> 
        >>> # 在回测循环中
        >>> await scheduler.trigger(current_dt, context, is_bar=True)
    """
    
    def __init__(self):
        """初始化调度器"""
        self._tasks: List[AsyncScheduleTask] = []
        self._task_map: Dict[str, AsyncScheduleTask] = {}
        self._schedule_cache: Dict[datetime, List[AsyncScheduleTask]] = {}
        self._schedule_cache_date: Optional[date] = None
        self._trade_calendar: Optional[Dict[date, Dict[str, Any]]] = None
        self._market_periods_resolver: Optional[
            Callable[[Optional[str]], Sequence[Tuple[Time, Time]]]
        ] = None
    
    def run_daily(
        self,
        func: Callable,
        time: str = 'every_bar',
        overlap_strategy: OverlapStrategy = OverlapStrategy.SKIP
    ) -> str:
        """
        每日运行任务
        
        Args:
            func: 要执行的函数（同步或异步）
            time: 执行时间
                - 'every_bar': 每个bar（推荐用于分钟策略）
                - 'open': 开盘时
                - 'close': 收盘时
                - 'HH:MM': 特定时间，如 '09:30', '14:00'
            overlap_strategy: 重叠执行策略
                - SKIP: 跳过（默认，推荐）
                - WAIT: 等待
                - CONCURRENT: 并发（慎用）
        
        Returns:
            任务ID
        
        Example:
            >>> async def market_open(context):
            ...     print("开盘了")
            >>> 
            >>> scheduler.run_daily(market_open, '09:30')
        """
        expression = TimeExpression.parse(time, get_time_aliases())
        task = AsyncScheduleTask(
            func=func,
            schedule_type=ScheduleType.DAILY,
            time=time,
            expression=expression,
            overlap_strategy=overlap_strategy
        )
        
        return self._register_task(task)
    
    def run_weekly(
        self,
        func: Callable,
        weekday: int,
        time: str = 'open',
        reference_security: Optional[str] = None,
        force: bool = True,
        overlap_strategy: OverlapStrategy = OverlapStrategy.SKIP
    ) -> str:
        """
        每周运行任务
        
        Args:
            func: 要执行的函数
            weekday: 当周第 N 个交易日（支持负数，-1 为最后一个交易日）
            time: 执行时间
            reference_security: 参考标的（决定交易日/时段）
            force: 是否从策略起始日作为第一个交易日起算
            overlap_strategy: 重叠执行策略
        
        Returns:
            任务ID
        
        Example:
            >>> scheduler.run_weekly(rebalance, 1, '09:30')  # 每周第1个交易日 09:30
        """
        if not isinstance(weekday, int) or weekday == 0:
            raise ValueError("weekday 必须为非零整数，表示交易日序号（正序/倒序）")
        expression = TimeExpression.parse(time, get_time_aliases())
        task = AsyncScheduleTask(
            func=func,
            schedule_type=ScheduleType.WEEKLY,
            time=time,
            expression=expression,
            weekday=weekday,
            reference_security=reference_security,
            force=bool(force),
            overlap_strategy=overlap_strategy
        )
        
        return self._register_task(task)
    
    def run_monthly(
        self,
        func: Callable,
        monthday: int,
        time: str = 'open',
        reference_security: Optional[str] = None,
        force: bool = True,
        overlap_strategy: OverlapStrategy = OverlapStrategy.SKIP
    ) -> str:
        """
        每月运行任务
        
        Args:
            func: 要执行的函数
            monthday: 当月第 N 个交易日（支持负数，-1 为最后一个交易日）
            time: 执行时间
            reference_security: 参考标的（决定交易日/时段）
            force: 是否从策略起始日作为第一个交易日起算
            overlap_strategy: 重叠执行策略
        
        Returns:
            任务ID
        
        Example:
            >>> scheduler.run_monthly(monthly_report, 1, '15:00')  # 每月第1个交易日 15:00
        """
        if not isinstance(monthday, int) or monthday == 0:
            raise ValueError("monthday 必须为非零整数，表示交易日序号（正序/倒序）")
        expression = TimeExpression.parse(time, get_time_aliases())
        task = AsyncScheduleTask(
            func=func,
            schedule_type=ScheduleType.MONTHLY,
            time=time,
            expression=expression,
            monthday=monthday,
            reference_security=reference_security,
            force=bool(force),
            overlap_strategy=overlap_strategy
        )
        
        return self._register_task(task)
    
    def _register_task(self, task: AsyncScheduleTask) -> str:
        """
        注册任务
        
        Args:
            task: 任务对象
            
        Returns:
            任务ID
        """
        # 检查是否已存在
        if task.task_id in self._task_map:
            logger.warning(f"⚠️  任务 {task.task_id} 已存在，将被覆盖")
            self.unschedule(task.task_id)
        
        self._tasks.append(task)
        self._task_map[task.task_id] = task
        
        overlap_labels = {
            OverlapStrategy.SKIP: "跳过",
            OverlapStrategy.WAIT: "等待",
            OverlapStrategy.CONCURRENT: "并发",
        }
        overlap_label = overlap_labels.get(task.overlap_strategy, task.overlap_strategy.value)
        logger.info(
            f"✅ 注册任务: {task.task_id} "
            f"({task.schedule_type.value}, {task.time}, "
            f"重叠处理: {overlap_label})"
        )
        
        return task.task_id
    
    def unschedule(self, task_id: str):
        """
        取消调度任务
        
        Args:
            task_id: 任务ID
        """
        if task_id in self._task_map:
            task = self._task_map[task_id]
            self._tasks.remove(task)
            del self._task_map[task_id]
            logger.info(f"🗑️  取消任务: {task_id}")
        else:
            logger.warning(f"⚠️  任务不存在: {task_id}")
    
    def unschedule_all(self):
        """取消所有任务"""
        count = len(self._tasks)
        self._tasks.clear()
        self._task_map.clear()
        logger.info(f"🗑️  已取消所有任务（共 {count} 个）")
    
    def enable_task(self, task_id: str):
        """启用任务"""
        if task_id in self._task_map:
            self._task_map[task_id].enabled = True
            logger.info(f"✅ 启用任务: {task_id}")
            self._schedule_cache_date = None
            self._schedule_cache = {}
    
    def disable_task(self, task_id: str):
        """禁用任务"""
        if task_id in self._task_map:
            self._task_map[task_id].enabled = False
            logger.info(f"🔇 禁用任务: {task_id}")
            self._schedule_cache_date = None
            self._schedule_cache = {}
    
    async def trigger(
        self,
        current_dt: datetime,
        *args,
        is_bar: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """
        触发调度检查
        
        检查所有任务，执行符合条件的任务
        
        Args:
            current_dt: 当前时间
            *args: 传递给任务函数的参数
            is_bar: 是否为 bar 时刻
            **kwargs: 传递给任务函数的关键字参数
        
        Returns:
            执行结果字典 {task_id: result}
        """
        results = {}
        trade_day_date = current_dt.date()
        if self._schedule_cache_date != trade_day_date:
            calendar = self._trade_calendar or get_trade_calendar()
            resolver = self._market_periods_resolver
            self._schedule_cache = generate_daily_schedule(
                current_dt,
                trade_calendar=calendar,
                market_periods_resolver=resolver,
                tasks=self._tasks,
            )
            self._schedule_cache_date = trade_day_date
        
        tasks_to_run = self._schedule_cache.get(current_dt, [])
        
        if not tasks_to_run:
            return results
        
        logger.debug(
            f"⏰ {current_dt.strftime('%Y-%m-%d %H:%M:%S')} "
            f"触发 {len(tasks_to_run)} 个任务"
        )
        
        # 并发执行所有任务
        task_results = await asyncio.gather(
            *[task.execute(*args, **kwargs) for task in tasks_to_run],
            return_exceptions=True
        )
        
        # 收集结果
        for task, result in zip(tasks_to_run, task_results):
            if isinstance(result, Exception):
                # 打印堆栈，便于定位任务内部异常
                tb = ''.join(traceback.format_exception(type(result), result, result.__traceback__))
                logger.error(
                    f"❌ 任务 {task.task_id} 执行异常: {result}\n{tb}",
                    exc_info=(type(result), result, result.__traceback__)
                )
                results[task.task_id] = {'error': str(result), 'traceback': tb}
            else:
                results[task.task_id] = {'result': result}
        
        return results
    
    def preload_schedule(self, trade_day: date, schedule_map: Dict[datetime, List[AsyncScheduleTask]]) -> None:
        """预加载当日调度时间表（供回测引擎复用）。"""
        self._schedule_cache_date = trade_day
        self._schedule_cache = schedule_map or {}

    def set_trade_calendar(self, calendar: Optional[Dict[date, Dict[str, Any]]]) -> None:
        """设置交易日序号日历（对齐同步调度）。"""
        self._trade_calendar = calendar

    def set_market_periods_resolver(
        self, resolver: Optional[Callable[[Optional[str]], Sequence[Tuple[Time, Time]]]]
    ) -> None:
        """设置参考标的交易时段解析函数。"""
        self._market_periods_resolver = resolver

    def get_task(self, task_id: str) -> Optional[AsyncScheduleTask]:
        """获取任务"""
        return self._task_map.get(task_id)
    
    def get_all_tasks(self) -> List[AsyncScheduleTask]:
        """获取所有任务"""
        return self._tasks.copy()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        Returns:
            统计字典
        """
        return {
            'total_tasks': len(self._tasks),
            'enabled_tasks': sum(1 for t in self._tasks if t.enabled),
            'running_tasks': sum(1 for t in self._tasks if t._running),
            'tasks': [
                {
                    'task_id': t.task_id,
                    'type': t.schedule_type.value,
                    'time': t.time,
                    'enabled': t.enabled,
                    'running': t._running,
                    'run_count': t._run_count,
                    'last_run': t._last_run.isoformat() if t._last_run else None,
                    'overlap_strategy': t.overlap_strategy.value,
                }
                for t in self._tasks
            ]
        }
    
    def __repr__(self):
        """字符串表示"""
        enabled = sum(1 for t in self._tasks if t.enabled)
        return f"<AsyncScheduler(tasks={len(self._tasks)}, enabled={enabled})>"


# ============ 全局调度器实例 ============

_global_scheduler: Optional[AsyncScheduler] = None


def get_scheduler() -> AsyncScheduler:
    """
    获取全局调度器实例
    
    Returns:
        AsyncScheduler 实例
    """
    global _global_scheduler
    if _global_scheduler is None:
        _global_scheduler = AsyncScheduler()
    return _global_scheduler


def reset_scheduler():
    """重置全局调度器"""
    global _global_scheduler
    if _global_scheduler is not None:
        _global_scheduler.unschedule_all()
    _global_scheduler = None
