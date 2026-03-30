"""
LiveEngine

异步实盘引擎：
- 结合 AsyncScheduler + EventBus 驱动策略 run_daily/handle_data 钩子
- 感知交易时段并记录延迟，自动跳过午休和收盘后
- 统一券商生命周期、后台任务、tick 订阅和运行态持久化
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, time as Time, date
import importlib
import hashlib
import inspect
import unicodedata
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Set, Tuple
import pandas as pd

from .async_scheduler import AsyncScheduler, OverlapStrategy
from .event_bus import EventBus
from .events import (
    AfterTradingEndEvent,
    BeforeTradingStartEvent,
    EveryMinuteEvent,
    MarketCloseEvent,
    MarketOpenEvent,
    SystemStartEvent,
    SystemStopEvent,
    TradingDayEndEvent,
    TradingDayStartEvent,
)
from .globals import g, log
from .models import Context, Portfolio, Position, Order, Trade, OrderStyle, OrderStatus
from .runtime import set_current_engine
from .scheduler import (
    get_market_periods,
    parse_market_periods_string,
    get_tasks,
    get_trade_calendar,
    run_daily,
    run_weekly,
    run_monthly,
    set_trade_calendar,
    unschedule_all,
)
from . import scheduler as sync_scheduler
from .settings import (
    get_settings,
    set_option,
    reset_settings,
    set_benchmark,
    set_order_cost,
    set_slippage,
    OrderCost,
    FixedSlippage,
    PriceRelatedSlippage,
    StepRelatedSlippage,
)
from ..data.api import get_current_data, set_current_context, get_security_info, get_data_provider
from ..utils.env_loader import (
    get_broker_config,
    get_live_trade_config,
    parse_bool,
)
from ..broker import BrokerBase, QmtBroker, RemoteQmtBroker
from ..broker.simulator import SimulatorBroker
from .live_runtime import (
    init_live_runtime,
    start_g_autosave,
    stop_g_autosave,
    save_g,
    load_scheduler_cursor,
    persist_scheduler_cursor,
    load_subscription_state,
    persist_subscription_state,
    runtime_restored,
    load_strategy_metadata,
    persist_strategy_metadata,
)
from .orders import get_order_queue, clear_order_queue, MarketOrderStyle, LimitOrderStyle
from .live_lock import (
    ManagedLiveLock,
    build_lock_metadata,
    get_live_lock_dir,
)
from .risk_control import get_global_risk_controller
from .engine import PRE_MARKET_OFFSET, BacktestEngine
from . import pricing


@dataclass
class LiveConfig:
    order_max_volume: int
    trade_max_wait_time: int
    event_time_out: int
    strategy_name: Optional[str]
    scheduler_market_periods: Optional[str]
    account_sync_interval: int
    account_sync_enabled: bool
    order_sync_interval: int
    order_sync_enabled: bool
    g_autosave_interval: int
    g_autosave_enabled: bool
    tick_subscription_limit: int
    tick_sync_interval: int
    tick_sync_enabled: bool
    risk_check_interval: int
    risk_check_enabled: bool
    broker_heartbeat_interval: int
    runtime_dir: str
    buy_price_percent: float
    sell_price_percent: float
    calendar_skip_weekend: bool = True
    calendar_retry_minutes: int = 1
    portfolio_refresh_throttle_ms: int = 200

    @classmethod
    def load(cls, overrides: Optional[Dict[str, Any]] = None) -> "LiveConfig":
        raw = get_live_trade_config()
        if overrides:
            raw.update(overrides)
        return cls(
            order_max_volume=int(raw.get('order_max_volume', 1_000_000)),
            trade_max_wait_time=int(raw.get('trade_max_wait_time', 16)),
            event_time_out=int(raw.get('event_time_out', 60)),
            strategy_name=raw.get('strategy_name'),
            scheduler_market_periods=raw.get('scheduler_market_periods'),
            account_sync_interval=int(raw.get('account_sync_interval', 60)),
            account_sync_enabled=parse_bool(raw.get('account_sync_enabled'), default=True),
            order_sync_interval=int(raw.get('order_sync_interval', 10)),
            order_sync_enabled=parse_bool(raw.get('order_sync_enabled'), default=True),
            g_autosave_interval=int(raw.get('g_autosave_interval', 60)),
            g_autosave_enabled=parse_bool(raw.get('g_autosave_enabled'), default=True),
            tick_subscription_limit=int(raw.get('tick_subscription_limit', 100)),
            tick_sync_interval=int(raw.get('tick_sync_interval', 2)),
            tick_sync_enabled=parse_bool(raw.get('tick_sync_enabled'), default=True),
            risk_check_interval=int(raw.get('risk_check_interval', 300)),
            risk_check_enabled=parse_bool(raw.get('risk_check_enabled'), default=False),
            broker_heartbeat_interval=int(raw.get('broker_heartbeat_interval', 30)),
            runtime_dir=str(raw.get('runtime_dir', './runtime')),
            buy_price_percent=float(raw.get('market_buy_price_percent', 0.015)),
            sell_price_percent=float(raw.get('market_sell_price_percent', -0.015)),
            calendar_skip_weekend=parse_bool(raw.get('calendar_skip_weekend'), default=True),
            calendar_retry_minutes=int(raw.get('calendar_retry_minutes', 1)),
            portfolio_refresh_throttle_ms=int(raw.get('portfolio_refresh_throttle_ms', 200)),
        )


@dataclass
class _ResolvedOrder:
    security: str
    amount: int
    is_buy: bool
    price: Optional[float]
    last_price: float
    wait_timeout: Optional[float]
    is_market: bool


class LiveEngine:
    """
    实盘事件引擎。

    - run(): 同步入口，封装 asyncio 事件循环
    - start(): 异步入口，便于测试
    """

    is_live: bool = True

    def __init__(
        self,
        strategy_file: Path | str,
        *,
        broker_name: Optional[str] = None,
        live_config: Optional[Dict[str, Any]] = None,
        broker_factory: Optional[Callable[[], BrokerBase]] = None,
        now_provider: Optional[Callable[[], datetime]] = None,
        sleep_provider: Optional[Callable[[float], Awaitable[None]]] = None,
    ):
        self.strategy_path = Path(strategy_file).resolve()
        self.broker_name = broker_name
        self.config = LiveConfig.load(live_config)
        self._broker_factory = broker_factory
        self._now = now_provider or datetime.now
        self._sleep = sleep_provider

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._background_tasks: List[asyncio.Task] = []

        self.event_bus: Optional[EventBus] = None
        self.async_scheduler: Optional[AsyncScheduler] = None
        self.broker: Optional[BrokerBase] = None

        self._portfolio = Portfolio()
        self.portfolio_proxy = LivePortfolioProxy(self, self._portfolio)
        self.context = Context(portfolio=self.portfolio_proxy, current_dt=self._now())

        self._strategy_loader: Optional[BacktestEngine] = None
        self.initialize_func: Optional[Callable] = None
        self.before_trading_start_func: Optional[Callable] = None
        self.handle_data_func: Optional[Callable] = None
        self.after_trading_end_func: Optional[Callable] = None
        self.process_initialize_func: Optional[Callable] = None
        self.after_code_changed_func: Optional[Callable] = None
        self.handle_tick_func: Optional[Callable] = None

        self._current_day: Optional[date] = None
        self._previous_trade_day: Optional[date] = None
        self._market_periods: List[Tuple[Time, Time]] = []
        self._open_dt: Optional[datetime] = None
        self._close_dt: Optional[datetime] = None
        self._pre_open_dt: Optional[datetime] = None
        self._markers_fired: Set[str] = set()
        self._last_schedule_dt: Optional[datetime] = None
        self._trade_calendar: Dict[date, Dict[str, Any]] = {}
        self._strategy_start_date: Optional[date] = None

        self._tick_symbols: Set[str] = set()
        self._tick_markets: Set[str] = set()
        self._latest_ticks: Dict[str, Dict[str, Any]] = {}
        self._security_name_cache: Dict[str, str] = {}

        self._risk = get_global_risk_controller() if self.config.risk_check_enabled else None
        self._order_lock: Optional[asyncio.Lock] = None
        self._last_account_refresh: Optional[datetime] = None
        self._orders: Dict[str, Order] = {}
        self._trades: Dict[str, Trade] = {}
        self._broker_order_index: Dict[str, str] = {}
        self._order_snapshot_debug_signatures: Dict[str, Tuple[Any, ...]] = {}
        self._calendar_guard = TradingCalendarGuard(self.config)
        self._initial_nav_synced: bool = False
        self._provider_tick_callback_bound: bool = False
        self._tick_subscription_updated: bool = False
        self._runtime_lock: Optional[ManagedLiveLock] = None
        self._instance_lock: Optional[ManagedLiveLock] = None

    # ------------------------------------------------------------------
    # 公共入口
    # ------------------------------------------------------------------

    def run(self) -> int:
        """
        启动 LiveEngine（同步封装）。
        """
        if not self.strategy_path.exists():
            print(f"✗ 策略文件不存在: {self.strategy_path}")
            return 1

        try:
            asyncio.run(self.start())
            return 0
        except KeyboardInterrupt:
            log.info("⚠️  用户终止实盘运行")
            return 0
        except Exception as exc:
            log.error(f"实盘引擎异常退出: {exc}", exc_info=True)
            return 2
        finally:
            try:
                save_g()
            except Exception:
                pass

    async def start(self) -> None:
        """
        异步入口，便于测试复用。
        """
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        self._order_lock = asyncio.Lock()
        self.event_bus = EventBus(self._loop)
        self.async_scheduler = AsyncScheduler()

        bootstrapped = False
        try:
            await self._bootstrap()
            bootstrapped = True
            await self.event_bus.emit(SystemStartEvent())
            await self._run_loop()
        finally:
            if bootstrapped:
                await self.event_bus.emit(SystemStopEvent())
            await self._shutdown()

    # ------------------------------------------------------------------
    # 初始化 & 清理
    # ------------------------------------------------------------------

    async def _bootstrap(self) -> None:
        log.info("🧠 初始化 Live 引擎")
        if not self.strategy_path.exists():
            raise FileNotFoundError(f"策略文件不存在: {self.strategy_path}")

        self._strategy_loader = BacktestEngine(strategy_file=str(self.strategy_path))
        self._strategy_loader.load_strategy()
        self.initialize_func = self._strategy_loader.initialize_func
        self.handle_data_func = self._strategy_loader.handle_data_func
        self.before_trading_start_func = self._strategy_loader.before_trading_start_func
        self.after_trading_end_func = self._strategy_loader.after_trading_end_func
        module = sys.modules.get("strategy")
        if module:
            self.handle_tick_func = getattr(module, 'handle_tick', None)
            if self.process_initialize_func is None:
                self.process_initialize_func = getattr(module, 'process_initialize', None)
            self.after_code_changed_func = getattr(module, 'after_code_changed', None)
        else:
            self.handle_tick_func = None
            self.after_code_changed_func = None

        self._ensure_broker_created()
        self._acquire_live_locks()

        init_live_runtime(self.config.runtime_dir)
        if self.config.g_autosave_enabled:
            start_g_autosave(self.config.g_autosave_interval)
        g.live_trade = True

        reset_settings()
        set_current_engine(self)
        set_current_context(self.context)
        self.context.run_params['run_type'] = 'LIVE'
        self.context.run_params['is_live'] = True

        current_hash = self._compute_strategy_hash()
        restored_runtime = runtime_restored()
        metadata = load_strategy_metadata()
        metadata_applied = False
        if restored_runtime and metadata:
            metadata_applied = self._restore_strategy_metadata(metadata)
            if not metadata_applied:
                log.warning("检测到历史 g 状态但缺少策略元数据，将重新执行 initialize()")

        log.debug(
            "LiveEngine restore status: restored_runtime=%s, metadata_applied=%s",
            restored_runtime,
            metadata_applied,
        )

        if not restored_runtime or not metadata_applied:
            await self._call_hook(self.initialize_func)

        self._apply_market_period_override()

        hash_changed = bool(metadata) and metadata.get('strategy_hash') and metadata.get('strategy_hash') != current_hash
        if metadata:
            log.debug(
                "LiveEngine: metadata_hash=%s, current_hash=%s, restored=%s",
                metadata.get('strategy_hash'),
                current_hash,
                hash_changed,
            )
        if hash_changed and self.after_code_changed_func:
            await self._call_hook(self.after_code_changed_func)

        self._init_broker()

        await self._call_hook(self.process_initialize_func)

        self._dedupe_scheduler_tasks()

        # 若策略已通过 run_daily/run_weekly 注册了相同函数，则避免 LiveEngine 再直接调用，防止重复触发
        try:
            tasks = get_tasks()
            if self.before_trading_start_func and any(t.func is self.before_trading_start_func for t in tasks):
                log.debug("LiveEngine: before_market_open 已通过调度注册，跳过直接调用钩子")
                self.before_trading_start_func = None
            if self.handle_data_func and any(t.func is self.handle_data_func for t in tasks):
                log.debug("LiveEngine: market_open/handle_data 已通过调度注册，跳过直接调用钩子")
                self.handle_data_func = None
        except Exception as exc:
            log.warning(f"LiveEngine 调度重复检查失败: {exc}")

        self._migrate_scheduler_tasks()
        self._snapshot_strategy_metadata(current_hash)

        symbols, markets = load_subscription_state()
        self._tick_symbols = set(symbols)
        self._tick_markets = set(markets)
        should_sync_initial = (
            not self._tick_subscription_updated and (self._tick_symbols or self._tick_markets)
        )
        if should_sync_initial:
            self._sync_provider_subscription(initial=True)

        self._last_schedule_dt = load_scheduler_cursor()
        if self._last_schedule_dt:
            current_minute = self._now().replace(second=0, microsecond=0)
            if self._last_schedule_dt > current_minute:
                log.warning(
                    "检测到历史调度游标晚于当前系统时间，已忽略此前的游标值。"
                )
                self._last_schedule_dt = None

        self._start_background_jobs()

    async def _shutdown(self) -> None:
        log.info("🛑 正在关闭 Live 引擎")
        if self._stop_event:
            self._stop_event.set()
        for task in self._background_tasks:
            task.cancel()
        for task in self._background_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                log.warning(f"后台任务退出异常: {exc}")
        self._background_tasks = []

        if self.broker:
            try:
                self.broker.cleanup()
            except Exception as exc:
                log.warning(f"券商清理失败: {exc}")

        if self.config.g_autosave_enabled:
            try:
                stop_g_autosave()
            finally:
                self._release_live_locks()
        else:
            self._release_live_locks()

    # ------------------------------------------------------------------
    # 主循环
    # ------------------------------------------------------------------

    async def _run_loop(self) -> None:
        assert self._loop is not None
        while not self._stop_event.is_set():
            now = self._now()
            if not await self._calendar_guard.ensure_trade_day(now):
                await self._sleep_until_calendar_retry(now)
                continue
            await self._ensure_trading_day(now.date())
            await self._handle_minute_tick(now)
            await self._sleep_until_next_minute(now)

    async def _sleep_until_calendar_retry(self, now: datetime) -> None:
        delay = self._calendar_guard.seconds_until_next_check(now)
        sleeper = self._sleep or asyncio.sleep
        try:
            await sleeper(delay)
        except asyncio.CancelledError:
            raise

    async def _sleep_until_next_minute(self, now: datetime) -> None:
        target = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        delay = max(0.0, (target - self._now()).total_seconds())
        sleeper = self._sleep or asyncio.sleep
        try:
            await sleeper(delay)
        except asyncio.CancelledError:
            raise

    async def _ensure_trading_day(self, current_date: date) -> None:
        if self._current_day == current_date:
            return

        self._previous_trade_day = self._current_day
        self._current_day = current_date
        self._markers_fired.clear()

        self._market_periods = get_market_periods()
        if not self._market_periods:
            raise RuntimeError("未配置交易时段，无法运行实盘")
        if self.async_scheduler:
            self.async_scheduler.set_market_periods_resolver(lambda _ref=None: self._market_periods)
        if self._strategy_start_date is None:
            self._strategy_start_date = current_date
            self._persist_strategy_start_date()
        try:
            provider = get_data_provider()
            calendar_days = provider.get_trade_days(end_date=current_date, count=180) or []
            calendar_dates = [pd.to_datetime(d).date() for d in calendar_days]
            if calendar_dates:
                set_trade_calendar(calendar_dates, self._strategy_start_date)
                self._trade_calendar = get_trade_calendar()
                if self.async_scheduler:
                    self.async_scheduler.set_trade_calendar(self._trade_calendar)
        except Exception as exc:
            log.debug(f"刷新交易日序号日历失败: {exc}")

        open_dt = datetime.combine(current_date, self._market_periods[0][0])
        close_dt = datetime.combine(current_date, self._market_periods[-1][1])
        self._open_dt = open_dt
        self._close_dt = close_dt
        self._pre_open_dt = open_dt - PRE_MARKET_OFFSET
        self.context.previous_date = self._previous_trade_day

        await self.event_bus.emit(TradingDayStartEvent(date=current_date))
        log.info(f"📅 新交易日：{current_date}")

        if self._last_schedule_dt and self._last_schedule_dt.date() != current_date:
            self._last_schedule_dt = None

    async def _handle_minute_tick(self, wall_clock: datetime) -> None:
        current_minute = wall_clock.replace(second=0, microsecond=0)
        if self._last_schedule_dt and self._last_schedule_dt > current_minute:
            log.warning(
                "检测到历史调度游标超前当前时间，将重置为当前分钟之前。"
            )
            self._last_schedule_dt = current_minute - timedelta(minutes=1)
        scheduled = current_minute
        if self._last_schedule_dt and scheduled <= self._last_schedule_dt:
            scheduled = self._last_schedule_dt + timedelta(minutes=1)
        if scheduled > current_minute:
            log.debug(
                "LiveEngine: 已执行至 %s，等待下一触发分钟 %s",
                self._last_schedule_dt,
                scheduled,
            )
            return

        delay = (wall_clock - scheduled).total_seconds()
        timeout = max(0, self.config.event_time_out)

        self.context.previous_dt = self.context.current_dt
        self.context.current_dt = scheduled
        log.set_strategy_time(scheduled)

        if delay > timeout:
            log.warning(
                f"⏱️ 事件超时丢弃: scheduled={scheduled}, delay={delay:.1f}s (> {timeout}s)"
            )
            self._last_schedule_dt = scheduled
            persist_scheduler_cursor(scheduled)
            return

        try:
            if self.async_scheduler:
                await self.async_scheduler.trigger(
                    scheduled,
                    self.context,
                    is_bar=self._is_bar_time(scheduled),
                )
        except Exception as exc:
            log.error(f"异步调度执行失败: {exc}", exc_info=True)

        await self._maybe_emit_market_events(scheduled)
        await self._maybe_handle_data(scheduled)
        await self._process_orders(scheduled)

        self._last_schedule_dt = scheduled
        persist_scheduler_cursor(scheduled)

    def _is_bar_time(self, dt: datetime) -> bool:
        """
        `every_bar` 语义：交易时段内的每一个分钟 bar。
        这里直接复用 `_is_trading_minute`，避免只在开盘分钟触发。
        """
        return self._is_trading_minute(dt)

    def _is_trading_minute(self, dt: datetime) -> bool:
        if dt.second != 0:
            return False
        current = dt.time()
        for start, end in self._market_periods:
            if start <= current < end:
                return True
        return False

    async def _maybe_emit_market_events(self, dt: datetime) -> None:
        assert self.event_bus is not None
        if self._pre_open_dt and 'pre_open' not in self._markers_fired and dt >= self._pre_open_dt:
            self._markers_fired.add('pre_open')
            await self.event_bus.emit(BeforeTradingStartEvent(date=dt.date()))
            await self._call_hook(self.before_trading_start_func)

        if self._open_dt and 'open' not in self._markers_fired and dt >= self._open_dt:
            self._markers_fired.add('open')
            await self.event_bus.emit(MarketOpenEvent(time=dt.strftime("%H:%M:%S")))

        if self._is_trading_minute(dt):
            await self.event_bus.emit(EveryMinuteEvent(time=dt.strftime("%H:%M:%S")))

        if self._close_dt and 'close' not in self._markers_fired and dt >= self._close_dt:
            self._markers_fired.add('close')
            await self.event_bus.emit(MarketCloseEvent(time=dt.strftime("%H:%M:%S")))
            await self._call_hook(self.after_trading_end_func)
            await self.event_bus.emit(AfterTradingEndEvent(date=dt.date()))
            await self.event_bus.emit(TradingDayEndEvent(
                date=dt.date(),
                portfolio_value=self.context.portfolio.total_value,
            ))

    async def _maybe_handle_data(self, dt: datetime) -> None:
        if not self.handle_data_func:
            return
        if not self._is_trading_minute(dt):
            return
        try:
            data = get_current_data()
        except Exception as exc:
            log.warning(f"获取当前数据失败: {exc}")
            data = None
        await self._call_hook(self.handle_data_func, data)

    # ------------------------------------------------------------------
    # 订单处理
    # ------------------------------------------------------------------

    async def _process_orders(self, current_dt: datetime) -> None:
        lock = self._order_lock or asyncio.Lock()
        if self._order_lock is None:
            self._order_lock = lock
        async with lock:
            orders = list(get_order_queue())
            if not orders:
                return
            # 先清空已取出的队列，避免后续处理时误清除新加入的订单
            clear_order_queue()
            if not self.broker:
                log.error("暂无券商实例，无法执行订单")
                return
            try:
                current_data = get_current_data()
            except Exception as exc:
                log.warning(f"获取 current_data 失败，订单无法执行: {exc}")
                return
            open_position_symbols = self._get_open_position_symbols()
            pending_new_positions: Set[str] = set()
            submitted_buys: Dict[str, Dict[str, Any]] = {}
            for order in orders:
                self._register_order(order)
                plan = self._build_order_plan(order, current_data)
                if not plan:
                    try:
                        order.status = OrderStatus.canceled
                    except Exception:
                        pass
                    continue
                try:
                    price_basis = plan.price if plan.price and plan.price > 0 else plan.last_price
                    order_value = float(plan.amount * max(price_basis, 0.0))
                    if order_value <= 0:
                        log.warning(f"订单 {plan.security} 价值异常，忽略执行")
                        try:
                            order.status = OrderStatus.rejected
                        except Exception:
                            pass
                        continue
                    action = 'buy' if plan.is_buy else 'sell'
                    risk = self._risk
                    if risk:
                        positions_count = len(open_position_symbols | pending_new_positions)
                        total_value = float(getattr(self.context.portfolio, "total_value", 0.0) or 0.0)
                        try:
                            risk.check_order(
                                order_value=order_value,
                                current_positions_count=positions_count,
                                security=plan.security,
                                total_value=total_value,
                                action=action,
                            )
                        except ValueError as risk_exc:
                            log.error(f"风控拒绝委托[{action}] {plan.security}: {risk_exc}")
                            try:
                                order.status = OrderStatus.rejected
                            except Exception:
                                pass
                            continue
                    price_arg = plan.price if plan.price and plan.price > 0 else None
                    market_flag = bool(plan.is_market)
                    style_obj = getattr(order, "style", None)
                    style_name = style_obj.__class__.__name__ if style_obj else "MarketOrderStyle"
                    price_value = plan.price if plan.price is not None else price_arg
                    price_repr = f"{price_value:.4f}" if price_value else "未指定"
                    price_mode = "市价" if market_flag else "限价"
                    action_label = "买入" if plan.is_buy else "卖出"
                    log.info(
                        f"执行委托[{action_label}] {plan.security}: 行情价={plan.last_price:.4f}, "
                        f"委托价={price_repr}（{price_mode}），风格={style_name}, 数量={plan.amount}"
                    )
                    remark = self._prepare_order_metadata(order)
                    order_id: Optional[str] = None
                    if plan.is_buy:
                        order_id = await self.broker.buy(
                            plan.security,
                            plan.amount,
                            price_arg,
                            wait_timeout=plan.wait_timeout,
                            remark=remark,
                            market=market_flag,
                        )
                    else:
                        order_id = await self.broker.sell(
                            plan.security,
                            plan.amount,
                            price_arg,
                            wait_timeout=plan.wait_timeout,
                            remark=remark,
                            market=market_flag,
                        )
                    try:
                        setattr(order, "_broker_order_id", order_id)
                        if order_id:
                            self._broker_order_index[str(order_id)] = order.order_id
                    except Exception:
                        pass
                    try:
                        order.status = OrderStatus.open
                    except Exception:
                        pass
                    log.info(
                        f"委托[{action_label}] {plan.security} 已提交，订单ID={order_id or '未知'}，"
                        f"数量={plan.amount}"
                    )
                    self._order_debug(
                        "submit",
                        security=plan.security,
                        action=action,
                        broker_order_id=order_id,
                        amount=plan.amount,
                        last_price=plan.last_price,
                        order_price=price_arg,
                        is_market=market_flag,
                        wait_timeout=plan.wait_timeout,
                        style=style_name,
                        order_remark=remark,
                    )
                    if risk:
                        try:
                            risk.record_trade(order_value, action=action)
                        except Exception as record_exc:
                            log.debug(f"记录风控交易失败: {record_exc}")
                        if plan.is_buy and plan.security not in open_position_symbols:
                            pending_new_positions.add(plan.security)
                    if plan.is_buy and order_id:
                        meta = submitted_buys.setdefault(
                            plan.security,
                            {
                                "pre_amount": 0,
                                "pre_avg_cost": 0.0,
                                "broker_order_ids": [],
                            },
                        )
                        if not meta["broker_order_ids"]:
                            pre_amount, pre_avg_cost = self._current_position_state(plan.security)
                            meta["pre_amount"] = pre_amount
                            meta["pre_avg_cost"] = pre_avg_cost
                        meta["broker_order_ids"].append(str(order_id))
                except Exception as exc:
                    log.error(f"委托失败 {order.security}: {exc}")
                    try:
                        order.status = OrderStatus.rejected
                    except Exception:
                        pass
            order_snapshots: List[Dict[str, Any]] = []
            trade_snapshots: List[Dict[str, Any]] = []
            try:
                order_snapshots = self._sync_orders_from_broker()
                if order_snapshots:
                    self._apply_order_snapshots(order_snapshots)
            except Exception as exc:
                log.debug(f"订单执行后同步订单快照失败: {exc}")
            try:
                trade_snapshots = self._sync_trades_from_broker()
                if trade_snapshots:
                    self._apply_trade_snapshots(trade_snapshots)
            except Exception as exc:
                log.debug(f"订单执行后同步成交快照失败: {exc}")
            self._trace_submitted_buys("post_broker_sync", submitted_buys, order_snapshots, trade_snapshots)
            try:
                self.refresh_account_snapshot(force=True)
            except Exception as exc:
                log.debug(f"订单执行后刷新账户快照失败: {exc}")
            self._trace_submitted_buys("post_account_refresh", submitted_buys, order_snapshots, trade_snapshots)
            try:
                self._reconcile_submitted_buy_costs(submitted_buys, order_snapshots, trade_snapshots)
            except Exception as exc:
                log.debug(f"订单执行后修正持仓成本失败: {exc}")
            self._trace_submitted_buys("post_cost_reconcile", submitted_buys, order_snapshots, trade_snapshots)

    def _register_order(self, order: Order) -> None:
        if not order:
            return
        oid = str(getattr(order, "order_id", "") or "")
        if not oid:
            return
        if oid not in self._orders:
            self._orders[oid] = order
        broker_id = getattr(order, "_broker_order_id", None)
        if broker_id:
            self._broker_order_index[str(broker_id)] = oid
        # 实盘下单先置为 new，提交后再转 open
        if getattr(order, "_broker_order_id", None) is None:
            try:
                if isinstance(order.status, OrderStatus) and order.status == OrderStatus.open:
                    order.status = OrderStatus.new
                elif str(order.status) == OrderStatus.open.value:
                    order.status = OrderStatus.new
            except Exception:
                pass

    def _sanitize_strategy_label(self, raw: Optional[str]) -> str:
        if not raw:
            return ""
        try:
            normalized = unicodedata.normalize("NFKD", str(raw))
        except Exception:
            normalized = str(raw)
        ascii_label = normalized.encode("ascii", "ignore").decode("ascii")
        if not ascii_label:
            return ""
        safe = re.sub(r"[^A-Za-z0-9_-]+", "_", ascii_label).strip("_")
        return safe.lower()

    def _resolve_strategy_label(self) -> str:
        label = self._sanitize_strategy_label(self.config.strategy_name)
        if not label:
            label = self._sanitize_strategy_label(self.strategy_path.stem)
        return label or "strategy"

    def _build_order_remark(self, order: Order) -> str:
        short_id = hashlib.md5(str(order.order_id).encode("utf-8")).hexdigest()[:8]
        label = self._resolve_strategy_label()
        max_len = 24 - len("bt") - len(short_id) - 2
        if max_len < 1:
            label = "s"
        else:
            label = label[:max_len]
        return f"bt:{label}:{short_id}"

    def _prepare_order_metadata(self, order: Order) -> Optional[str]:
        if not order:
            return None
        try:
            extra = getattr(order, "extra", None)
            if extra is None:
                order.extra = {}
                extra = order.extra
            remark = extra.get("order_remark") or getattr(order, "_order_remark", None)
            if not remark:
                remark = self._build_order_remark(order)
                extra["order_remark"] = remark
            if "strategy_name" not in extra:
                raw_name = self.config.strategy_name or self.strategy_path.stem
                extra["strategy_name"] = raw_name
            return remark
        except Exception:
            return None

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

    def _coerce_status(self, status: object) -> object:
        if isinstance(status, OrderStatus):
            return status
        try:
            return OrderStatus(str(status))
        except Exception:
            return str(status)

    def _order_debug_enabled(self) -> bool:
        return parse_bool(os.getenv("BT_LIVE_ORDER_DEBUG", ""), default=False)

    def _format_order_debug_value(self, value: Any, *, max_length: int = 640) -> str:
        if isinstance(value, float):
            return f"{value:.6f}"
        text = repr(value)
        if len(text) > max_length:
            return text[: max_length - 3] + "..."
        return text

    def _order_debug(self, stage: str, **fields: Any) -> None:
        if not self._order_debug_enabled():
            return
        parts = [
            f"{key}={self._format_order_debug_value(value)}"
            for key, value in fields.items()
            if value is not None
        ]
        suffix = " ".join(parts)
        message = f"[ORDER_DEBUG] live.{stage}"
        if suffix:
            message = f"{message} {suffix}"
        log.info(message)

    def _order_debug_signature_value(self, value: Any) -> Any:
        if value is None:
            return None
        try:
            if isinstance(value, (int, bool)):
                return value
            return round(float(value), 8)
        except Exception:
            return str(value)

    def _remember_order_snapshot_debug_signature(
        self,
        local_order_id: str,
        *,
        status: Any,
        filled: Any,
        price: Any,
        order_price: Any,
    ) -> bool:
        signature = (
            self._order_debug_signature_value(status),
            self._order_debug_signature_value(filled),
            self._order_debug_signature_value(price),
            self._order_debug_signature_value(order_price),
        )
        previous = self._order_snapshot_debug_signatures.get(local_order_id)
        if previous == signature:
            return False
        self._order_snapshot_debug_signatures[local_order_id] = signature
        return True

    def _compact_order_snapshot(self, snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(snapshot, dict):
            return {}
        return {
            "order_id": snapshot.get("order_id") or snapshot.get("entrust_id"),
            "security": snapshot.get("security"),
            "status": snapshot.get("status") or snapshot.get("state"),
            "raw_status": snapshot.get("raw_status"),
            "amount": snapshot.get("amount") or snapshot.get("order_volume"),
            "filled": snapshot.get("filled") or snapshot.get("traded_volume") or snapshot.get("filled_amount"),
            "price": snapshot.get("price"),
            "order_price": snapshot.get("order_price"),
            "traded_price": snapshot.get("traded_price"),
            "avg_price": snapshot.get("avg_price"),
            "avg_cost": snapshot.get("avg_cost"),
        }

    def _compact_trade_snapshot(self, snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(snapshot, dict):
            return {}
        return {
            "trade_id": snapshot.get("trade_id"),
            "order_id": snapshot.get("order_id") or snapshot.get("entrust_id"),
            "security": snapshot.get("security"),
            "amount": snapshot.get("amount") or snapshot.get("volume") or snapshot.get("trade_volume"),
            "price": snapshot.get("price") or snapshot.get("trade_price") or snapshot.get("traded_price"),
            "time": snapshot.get("time") or snapshot.get("trade_time"),
        }

    def _trace_submitted_buys(
        self,
        stage: str,
        submitted_buys: Dict[str, Dict[str, Any]],
        order_snapshots: List[Dict[str, Any]],
        trade_snapshots: List[Dict[str, Any]],
    ) -> None:
        if not submitted_buys or not self._order_debug_enabled():
            return

        order_by_id: Dict[str, Dict[str, Any]] = {}
        for snap in order_snapshots:
            broker_order_id = snap.get("order_id") or snap.get("entrust_id")
            if broker_order_id is None:
                continue
            order_by_id[str(broker_order_id)] = self._compact_order_snapshot(snap)

        trades_by_order_id: Dict[str, List[Dict[str, Any]]] = {}
        for snap in trade_snapshots:
            broker_order_id = snap.get("order_id") or snap.get("entrust_id")
            if broker_order_id is None:
                continue
            trades_by_order_id.setdefault(str(broker_order_id), []).append(self._compact_trade_snapshot(snap))

        target = self._portfolio_target()
        for security, meta in submitted_buys.items():
            broker_order_ids = [str(item) for item in meta.get("broker_order_ids") or [] if item]
            position = target.positions.get(security)
            order_rows = [order_by_id.get(order_id) for order_id in broker_order_ids if order_id in order_by_id]
            trade_rows = [
                row
                for order_id in broker_order_ids
                for row in trades_by_order_id.get(order_id, [])
            ]
            self._order_debug(
                stage,
                security=security,
                broker_order_ids=broker_order_ids,
                pre_amount=int(meta.get("pre_amount") or 0),
                pre_avg_cost=float(meta.get("pre_avg_cost") or 0.0),
                position_amount=int(position.total_amount or 0) if position else 0,
                position_avg_cost=float(position.avg_cost or 0.0) if position else 0.0,
                position_price=float(position.price or 0.0) if position else 0.0,
                position_value=float(position.value or 0.0) if position else 0.0,
                order_rows=order_rows,
                trade_rows=trade_rows,
            )

    def _sync_orders_from_broker(self, *, from_broker: bool = False) -> List[Dict[str, Any]]:
        broker = self.broker
        if not broker:
            return []
        getter = getattr(broker, "get_orders", None)
        if callable(getter):
            try:
                return getter(from_broker=from_broker) or []
            except TypeError:
                try:
                    return getter() or []
                except Exception:
                    return []
            except Exception:
                return []
        if broker.supports_orders_sync():
            try:
                return broker.sync_orders() or []
            except Exception:
                return []
        return []

    def _snapshot_filled_amount(self, snapshot: Dict[str, Any]) -> int:
        filled = snapshot.get("filled")
        if filled is None:
            filled = snapshot.get("traded_volume") or snapshot.get("filled_amount")
        try:
            return int(filled or 0)
        except Exception:
            return 0

    def _resolve_snapshot_fill_price(self, snapshot: Dict[str, Any]) -> Optional[float]:
        filled = self._snapshot_filled_amount(snapshot)
        candidates: List[Any] = [
            snapshot.get("traded_price"),
            snapshot.get("avg_price"),
            snapshot.get("avg_cost"),
        ]
        if filled > 0:
            candidates.append(snapshot.get("price"))
        for candidate in candidates:
            price = self._maybe_float(candidate)
            if price is not None and price > 0:
                return price
        return None

    def _portfolio_target(self) -> Portfolio:
        if isinstance(self.context.portfolio, LivePortfolioProxy):
            return self.portfolio_proxy.backing
        return self.context.portfolio

    def _current_position_state(self, security: str) -> Tuple[int, float]:
        position = self._portfolio_target().positions.get(security)
        if position is None:
            return 0, 0.0
        return int(position.total_amount or 0), float(position.avg_cost or 0.0)

    def _reconcile_submitted_buy_costs(
        self,
        submitted_buys: Dict[str, Dict[str, Any]],
        order_snapshots: List[Dict[str, Any]],
        trade_snapshots: List[Dict[str, Any]],
    ) -> None:
        if not submitted_buys:
            return

        order_by_id: Dict[str, Dict[str, Any]] = {}
        for snap in order_snapshots:
            if not isinstance(snap, dict):
                continue
            broker_order_id = snap.get("order_id") or snap.get("entrust_id")
            if broker_order_id is None:
                continue
            order_by_id[str(broker_order_id)] = snap

        trade_fill_by_id: Dict[str, Tuple[int, float]] = {}
        for snap in trade_snapshots:
            if not isinstance(snap, dict):
                continue
            broker_order_id = snap.get("order_id") or snap.get("entrust_id")
            if broker_order_id is None:
                continue
            amount = snap.get("amount") or snap.get("volume") or snap.get("trade_volume") or 0
            price = self._maybe_float(snap.get("price") or snap.get("trade_price"))
            try:
                qty = int(amount or 0)
            except Exception:
                qty = 0
            if qty <= 0 or price is None or price <= 0:
                continue
            key = str(broker_order_id)
            prev_qty, prev_avg = trade_fill_by_id.get(key, (0, 0.0))
            total_qty = prev_qty + qty
            total_value = prev_avg * prev_qty + price * qty
            trade_fill_by_id[key] = (total_qty, total_value / total_qty)

        target = self._portfolio_target()
        for security, meta in submitted_buys.items():
            broker_order_ids = [str(item) for item in meta.get("broker_order_ids") or [] if item]
            if not broker_order_ids:
                continue

            filled_qty = 0
            filled_value = 0.0
            for broker_order_id in broker_order_ids:
                trade_fill = trade_fill_by_id.get(broker_order_id)
                if trade_fill is not None:
                    qty, avg_price = trade_fill
                else:
                    order_snapshot = order_by_id.get(broker_order_id)
                    qty = self._snapshot_filled_amount(order_snapshot or {})
                    avg_price = self._resolve_snapshot_fill_price(order_snapshot or {}) or 0.0
                if qty <= 0 or avg_price <= 0:
                    continue
                filled_qty += qty
                filled_value += avg_price * qty

            if filled_qty <= 0 or filled_value <= 0:
                continue

            position = target.positions.get(security)
            if position is None or int(position.total_amount or 0) <= 0:
                continue

            pre_amount = int(meta.get("pre_amount") or 0)
            pre_avg_cost = float(meta.get("pre_avg_cost") or 0.0)
            expected_amount = pre_amount + filled_qty
            if int(position.total_amount or 0) != expected_amount:
                continue

            fill_avg_cost = filled_value / filled_qty
            if pre_amount > 0 and pre_avg_cost > 0:
                resolved_cost = ((pre_avg_cost * pre_amount) + filled_value) / expected_amount
            else:
                resolved_cost = fill_avg_cost

            previous_cost = float(position.avg_cost or 0.0)
            if abs(previous_cost - resolved_cost) <= 1e-9:
                continue
            position.avg_cost = resolved_cost
            position.acc_avg_cost = resolved_cost
            log.debug(f"成交均价修正持仓成本: {security} avg_cost {previous_cost:.4f} -> {resolved_cost:.4f}")
            self._order_debug(
                "reconcile_buy_cost",
                security=security,
                pre_amount=pre_amount,
                pre_avg_cost=pre_avg_cost,
                filled_qty=filled_qty,
                filled_value=filled_value,
                previous_cost=previous_cost,
                resolved_cost=resolved_cost,
            )

    def _apply_order_snapshots(self, snapshots: List[Dict[str, Any]]) -> None:
        if not snapshots:
            return
        for snap in snapshots:
            if not isinstance(snap, dict):
                continue
            broker_oid = snap.get("order_id") or snap.get("entrust_id")
            if not broker_oid:
                continue
            mapped_oid = self._broker_order_index.get(str(broker_oid))
            if not mapped_oid:
                continue
            order = self._orders.get(mapped_oid)
            if not order:
                continue
            status = snap.get("status") or snap.get("state")
            if status is not None:
                try:
                    order.status = self._coerce_status(status)
                except Exception:
                    pass
            price = self._resolve_snapshot_fill_price(snap)
            if price is None:
                price = self._maybe_float(snap.get("price"))
            if price is not None:
                try:
                    order.price = float(price or 0)
                except Exception:
                    pass
            amount = snap.get("amount")
            if amount is None:
                amount = snap.get("order_volume") or snap.get("volume")
            if amount is not None:
                try:
                    order.amount = int(amount or 0)
                except Exception:
                    pass
            filled = self._snapshot_filled_amount(snap)
            if filled is not None:
                try:
                    order.filled = int(filled or 0)
                except Exception:
                    pass
            is_buy = snap.get("is_buy")
            if is_buy is None:
                is_buy = snap.get("isBuy")
            if is_buy is not None:
                try:
                    order.is_buy = bool(is_buy)
                except Exception:
                    pass
            order_remark = snap.get("order_remark") or snap.get("remark")
            strategy_name = snap.get("strategy_name")
            order_price = snap.get("order_price")
            if order_remark or strategy_name or order_price is not None:
                try:
                    extra = getattr(order, "extra", None)
                    if extra is None:
                        order.extra = {}
                        extra = order.extra
                    if order_remark:
                        extra["order_remark"] = order_remark
                    if strategy_name:
                        extra["strategy_name"] = strategy_name
                    if order_price is not None:
                        extra["order_price"] = order_price
                except Exception:
                    pass
            normalized_status = self._normalize_status(getattr(order, "status", None))
            resolved_price = getattr(order, "price", None)
            resolved_filled = getattr(order, "filled", None)
            if self._remember_order_snapshot_debug_signature(
                mapped_oid,
                status=normalized_status,
                filled=resolved_filled,
                price=resolved_price,
                order_price=order_price,
            ):
                self._order_debug(
                    "apply_order_snapshot",
                    local_order_id=mapped_oid,
                    broker_order_id=broker_oid,
                    status=normalized_status,
                    resolved_price=resolved_price,
                    amount=getattr(order, "amount", None),
                    filled=resolved_filled,
                    snapshot=self._compact_order_snapshot(snap),
                )

    def _snapshot_is_buy(self, snapshot: Dict[str, Any]) -> Optional[bool]:
        raw = snapshot.get("is_buy")
        if raw is None:
            raw = snapshot.get("isBuy")
        if isinstance(raw, str):
            value = raw.strip().lower()
            if value in {"buy", "b", "true", "1", "yes", "y"}:
                return True
            if value in {"sell", "s", "false", "0", "no", "n"}:
                return False
        if raw is not None:
            try:
                return bool(raw)
            except Exception:
                return None
        side = str(snapshot.get("order_type") or snapshot.get("side") or "").strip().lower()
        if "buy" in side:
            return True
        if "sell" in side:
            return False
        return None

    def _snapshot_order_time(self, snapshot: Dict[str, Any]) -> Optional[datetime]:
        raw_time = snapshot.get("order_time") or snapshot.get("add_time") or snapshot.get("time")
        if isinstance(raw_time, datetime):
            return raw_time
        if raw_time:
            try:
                return pd.to_datetime(raw_time).to_pydatetime()
            except Exception:
                return None
        return None

    def _build_broker_order_view(
        self,
        snapshot: Dict[str, Any],
        broker_order_id: str,
        mapped_order_id: Optional[str],
    ) -> Optional[Order]:
        mapped = self._orders.get(mapped_order_id) if mapped_order_id else None
        order_remark = snapshot.get("order_remark") or snapshot.get("remark")
        strategy_name = snapshot.get("strategy_name")
        order_price = snapshot.get("order_price")
        order_sysid = snapshot.get("order_sysid")
        raw_status = snapshot.get("raw_status")

        if mapped is not None:
            extra = dict(getattr(mapped, "extra", {}) or {})
            extra["source"] = "broker"
            extra["is_external"] = False
            extra["engine_order_id"] = mapped_order_id
            if order_remark is not None:
                extra["order_remark"] = order_remark
            if strategy_name is not None:
                extra["strategy_name"] = strategy_name
            if order_price is not None:
                extra["order_price"] = order_price
            if order_sysid is not None:
                extra["order_sysid"] = order_sysid
            if raw_status is not None:
                extra["raw_status"] = raw_status
            return Order(
                order_id=broker_order_id,
                security=mapped.security,
                amount=int(mapped.amount or 0),
                filled=int(mapped.filled or 0),
                price=float(mapped.price or 0.0),
                status=self._coerce_status(mapped.status),
                add_time=mapped.add_time,
                is_buy=bool(mapped.is_buy),
                action=mapped.action,
                style=mapped.style,
                wait_timeout=mapped.wait_timeout,
                extra=extra,
            )

        security = snapshot.get("security") or snapshot.get("stock_code") or snapshot.get("code")
        if not security:
            return None
        amount = snapshot.get("amount")
        if amount is None:
            amount = snapshot.get("order_volume") or snapshot.get("volume")
        filled = self._snapshot_filled_amount(snapshot)
        price = self._resolve_snapshot_fill_price(snapshot)
        if price is None:
            price = self._maybe_float(snapshot.get("price"))
        status = snapshot.get("status") or snapshot.get("state") or OrderStatus.open.value
        is_buy = self._snapshot_is_buy(snapshot)

        extra: Dict[str, Any] = {
            "source": "broker",
            "is_external": True,
        }
        if order_remark is not None:
            extra["order_remark"] = order_remark
        if strategy_name is not None:
            extra["strategy_name"] = strategy_name
        if order_price is not None:
            extra["order_price"] = order_price
        if order_sysid is not None:
            extra["order_sysid"] = order_sysid
        if raw_status is not None:
            extra["raw_status"] = raw_status

        return Order(
            order_id=broker_order_id,
            security=str(security),
            amount=int(amount or 0),
            filled=int(filled or 0),
            price=float(price or 0.0),
            status=self._coerce_status(status),
            add_time=self._snapshot_order_time(snapshot),
            is_buy=bool(is_buy) if is_buy is not None else True,
            action="open" if (is_buy is None or is_buy) else "close",
            style=OrderStyle.limit,
            extra=extra,
        )

    def _collect_broker_orders(
        self,
        snapshots: List[Dict[str, Any]],
        *,
        order_id: Optional[str],
        security: Optional[str],
        status_val: Optional[str],
    ) -> Dict[str, Order]:
        if not snapshots:
            return {}
        target_id = str(order_id) if order_id is not None else None
        result: Dict[str, Order] = {}
        for snap in snapshots:
            if not isinstance(snap, dict):
                continue
            broker_oid = snap.get("order_id") or snap.get("entrust_id")
            if not broker_oid:
                continue
            broker_oid_str = str(broker_oid)
            if target_id and broker_oid_str != target_id:
                continue
            mapped_oid = self._broker_order_index.get(broker_oid_str)
            order = self._build_broker_order_view(snap, broker_oid_str, mapped_oid)
            if not order:
                continue
            if security and order.security != security:
                continue
            if status_val is not None and self._status_value(order.status) != status_val:
                continue
            result[broker_oid_str] = order
        return result

    def _build_trade_from_snapshot(self, snapshot: Dict[str, Any]) -> Optional[Trade]:
        if not snapshot:
            return None
        trade_id = snapshot.get("trade_id") or snapshot.get("id") or snapshot.get("trade_no")
        order_id = snapshot.get("order_id") or snapshot.get("entrust_id")
        security = snapshot.get("security")
        if not security:
            security = snapshot.get("stock_code") or snapshot.get("code")
        if not trade_id and not order_id:
            return None
        mapped_order_id = str(order_id) if order_id is not None else ""
        if order_id is not None:
            mapped_order_id = self._broker_order_index.get(str(order_id), str(order_id))
        amount = snapshot.get("amount") or snapshot.get("volume") or snapshot.get("trade_volume") or 0
        price = snapshot.get("price") or snapshot.get("trade_price") or 0.0
        raw_time = snapshot.get("time") or snapshot.get("trade_time")
        trade_time = None
        if isinstance(raw_time, datetime):
            trade_time = raw_time
        elif raw_time:
            try:
                trade_time = pd.to_datetime(raw_time).to_pydatetime()
            except Exception:
                trade_time = None
        trade = Trade(
            order_id=mapped_order_id,
            security=str(security) if security else "",
            amount=int(amount or 0),
            price=float(price or 0.0),
            time=trade_time or self.context.current_dt,
            commission=float(snapshot.get("commission") or 0.0),
            tax=float(snapshot.get("tax") or 0.0),
            trade_id=str(trade_id) if trade_id else "",
        )
        if not trade.trade_id:
            trade.trade_id = f"T{hashlib.md5(f'{trade.order_id}-{trade.time}-{trade.amount}-{trade.price}'.encode('utf-8')).hexdigest()[:12]}"
        return trade

    def _sync_trades_from_broker(self) -> List[Dict[str, Any]]:
        broker = self.broker
        if not broker:
            return []
        getter = getattr(broker, "get_trades", None)
        if callable(getter):
            try:
                return getter() or []
            except Exception:
                return []
        return []

    def _apply_trade_snapshots(self, snapshots: List[Dict[str, Any]]) -> None:
        if not snapshots:
            return
        for snap in snapshots:
            if not isinstance(snap, dict):
                continue
            trade = self._build_trade_from_snapshot(snap)
            if not trade or not trade.trade_id:
                continue
            self._trades[trade.trade_id] = trade

    def get_orders(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
        status: Optional[object] = None,
        from_broker: bool = False,
    ) -> Dict[str, Order]:
        for queued in list(get_order_queue() or []):
            self._register_order(queued)
        snapshots = self._sync_orders_from_broker(from_broker=from_broker)
        self._apply_order_snapshots(snapshots)

        status_val = self._normalize_status(status)
        if status is not None and status_val is None:
            return {}
        if from_broker:
            return self._collect_broker_orders(
                snapshots,
                order_id=order_id,
                security=security,
                status_val=status_val,
            )
        if not self._orders:
            return {}
        target_id = str(order_id) if order_id is not None else None
        result: Dict[str, Order] = {}
        for oid, order in self._orders.items():
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
            OrderStatus.open.value,
            OrderStatus.filling.value,
            OrderStatus.canceling.value,
        }
        orders = self.get_orders()
        if not orders:
            return {}
        return {oid: order for oid, order in orders.items() if self._status_value(order.status) in open_states}

    def get_trades(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
    ) -> Dict[str, Trade]:
        snapshots = self._sync_trades_from_broker()
        self._apply_trade_snapshots(snapshots)
        if not self._trades:
            return {}
        target_id = str(order_id) if order_id is not None else None
        result: Dict[str, Trade] = {}
        for tid, trade in self._trades.items():
            if target_id and trade.order_id != target_id:
                continue
            if security and trade.security != security:
                continue
            result[tid] = trade
        return result

    def _build_order_plan(self, order: Order, current_data) -> Optional[_ResolvedOrder]:
        try:
            snapshot = current_data[order.security]
        except Exception:
            log.warning(f"无法获取 {order.security} 的实时行情，忽略订单")
            return None
        if snapshot.paused:
            log.warning(f"{order.security} 停牌，取消订单")
            return None
        last_price = float(snapshot.last_price or 0.0)
        if last_price <= 0:
            fallback = snapshot.high_limit or snapshot.low_limit
            if not fallback or fallback <= 0:
                log.warning(f"{order.security} 缺少可用价格，忽略订单")
                return None
            last_price = float(fallback)

        amount, is_buy = self._resolve_order_amount(order, last_price)
        if amount <= 0:
            log.debug(f"{order.security} 无需交易或数量不足，跳过")
            return None

        closeable = None
        if not is_buy:
            closeable = self._get_closeable_amount(order.security)
            if closeable <= 0:
                log.warning(f"{order.security} 当前无可卖数量，忽略订单")
                return None
        amount = pricing.adjust_order_amount(order.security, amount, is_buy, closeable=closeable)
        if amount <= 0:
            msg = "扣除手数后无可交易数量" if is_buy else "可卖数量不足或不足最小手数"
            log.debug(f"{order.security} {msg}")
            return None

        exec_price: Optional[float] = None
        style_obj = getattr(order, "style", None)
        if isinstance(style_obj, LimitOrderStyle):
            is_market = False
        elif isinstance(style_obj, MarketOrderStyle):
            is_market = style_obj.limit_price is None
        else:
            is_market = True
        if isinstance(style_obj, LimitOrderStyle):
            exec_price = float(style_obj.price)
        elif isinstance(style_obj, MarketOrderStyle) and style_obj.limit_price is not None:
            exec_price = float(style_obj.limit_price)
        else:
            percent = self._resolve_price_percent(style_obj, is_buy)
            try:
                exec_price = pricing.compute_market_protect_price(
                    order.security,
                    snapshot.last_price,
                    getattr(snapshot, "high_limit", None),
                    getattr(snapshot, "low_limit", None),
                    percent,
                    is_buy,
                )
            except Exception as exc:
                log.error(f"{order.security} 无法计算保护价: {exc}")
                return None

        return _ResolvedOrder(
            order.security,
            amount,
            is_buy,
            exec_price,
            last_price,
            getattr(order, "wait_timeout", None),
            is_market,
        )

    def _resolve_order_amount(self, order: Order, last_price: float) -> Tuple[int, bool]:
        price = last_price if last_price > 0 else 1.0
        if getattr(order, "_is_target_amount", False):
            target = int(getattr(order, "_target_amount", 0))
            current = self._get_position_amount(order.security)
            delta = target - current
            return abs(delta), delta > 0

        if getattr(order, "_is_target_value", False):
            target_value = float(getattr(order, "_target_value", 0.0))
            current_amount = self._get_position_amount(order.security)
            current_value = current_amount * price
            delta_value = target_value - current_value
            amount = int(abs(delta_value) / price)
            return amount, delta_value > 0

        if hasattr(order, "_target_value") and not getattr(order, "_is_target_value", False):
            target_value = float(getattr(order, "_target_value", 0.0))
            amount = int(abs(target_value) / price)
            return amount, bool(order.is_buy)

        amount = int(order.amount or 0)
        return abs(amount), bool(order.is_buy)

    def _resolve_price_percent(self, style: object, is_buy: bool) -> float:
        return pricing.resolve_market_percent(
            style,
            is_buy,
            self.config.buy_price_percent,
            self.config.sell_price_percent,
        )

    def _get_position_amount(self, security: str) -> int:
        pos = self.context.portfolio.positions.get(security)
        return int(pos.total_amount) if pos else 0

    def _get_open_position_symbols(self) -> Set[str]:
        positions = getattr(self.context.portfolio, "positions", {}) or {}
        result: Set[str] = set()
        for sec, pos in positions.items():
            try:
                amount = int(getattr(pos, "total_amount", 0) or 0)
            except Exception:
                amount = 0
            if amount > 0:
                result.add(sec)
        return result

    def _get_closeable_amount(self, security: str) -> int:
        pos = self.context.portfolio.positions.get(security)
        if not pos:
            return 0
        return int(pos.closeable_amount or pos.total_amount or 0)

    # ------------------------------------------------------------------
    # 券商管理
    # ------------------------------------------------------------------

    def _create_broker(self) -> BrokerBase:
        if self._broker_factory:
            return self._broker_factory()

        cfg = get_broker_config()
        name = (self.broker_name or cfg.get('default') or 'simulator').lower()

        if name == 'qmt':
            qcfg = cfg.get('qmt') or {}
            account_id = qcfg.get('account_id')
            if not account_id:
                raise RuntimeError("缺少 QMT_ACCOUNT_ID，请在 .env.live 中配置")
            return QmtBroker(
                account_id=account_id,
                account_type=qcfg.get('account_type', 'stock'),
                data_path=qcfg.get('data_path'),
                session_id=qcfg.get('session_id'),
                auto_subscribe=qcfg.get('auto_subscribe'),
            )
        if name == 'qmt-remote':
            rcfg = cfg.get('qmt-remote') or {}
            return RemoteQmtBroker(
                account_id=rcfg.get('account_id') or rcfg.get('account_key') or 'remote',
                account_type=rcfg.get('account_type', 'stock'),
                config=rcfg,
            )
        if name == 'simulator':
            scfg = cfg.get('simulator') or {}
            return SimulatorBroker(
                account_id=scfg.get('account_id', 'simulator'),
                account_type=scfg.get('account_type', 'stock'),
                initial_cash=scfg.get('initial_cash', 1_000_000),
            )
        raise ValueError(f"未知券商类型: {name}")

    # ------------------------------------------------------------------
    # Tick 订阅
    # ------------------------------------------------------------------

    def register_tick_subscription(self, symbols: Sequence[str], markets: Sequence[str]) -> None:
        if not symbols and not markets:
            return
        limit = max(1, self.config.tick_subscription_limit)
        if len(self._tick_symbols.union(symbols)) > limit:
            raise ValueError(f"tick 订阅超限：最多 {limit} 个，当前 {len(self._tick_symbols)} 个")

        self._tick_subscription_updated = True
        self._tick_symbols.update(symbols)
        self._tick_markets.update(markets)
        persist_subscription_state(self._tick_symbols, self._tick_markets)

        self._sync_provider_subscription()
        log.info(
            "已登记 tick 订阅: symbols=%s markets=%s",
            list(self._tick_symbols),
            list(self._tick_markets),
        )

    def unregister_tick_subscription(self, symbols: Sequence[str], markets: Sequence[str]) -> None:
        self._tick_subscription_updated = True
        for sym in symbols:
            self._tick_symbols.discard(sym)
        for mk in markets:
            self._tick_markets.discard(mk)
        persist_subscription_state(self._tick_symbols, self._tick_markets)
        self._sync_provider_subscription(unsubscribe=True, symbols=list(symbols), markets=list(markets))

    def unsubscribe_all_ticks(self) -> None:
        self._tick_subscription_updated = True
        self._tick_symbols.clear()
        self._tick_markets.clear()
        persist_subscription_state(self._tick_symbols, self._tick_markets)
        self._sync_provider_subscription(unsubscribe=True, symbols=None, markets=None)

    def get_current_tick_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        if symbol in self._latest_ticks:
            return self._latest_ticks[symbol]
        tick = self._fetch_tick_snapshot(symbol)
        if tick:
            self._latest_ticks[symbol] = tick
        return tick

    def _fetch_tick_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            if self.broker and hasattr(self.broker, "get_current_tick"):
                tick = self.broker.get_current_tick(symbol)  # type: ignore[attr-defined]
                if tick:
                    return tick
        except Exception:
            return None
        try:
            provider = get_data_provider()
            if provider and hasattr(provider, "get_current_tick"):
                return provider.get_current_tick(symbol)  # type: ignore[attr-defined]
        except Exception:
            return None
        return None

    def _sync_provider_subscription(
        self,
        initial: bool = False,
        unsubscribe: bool = False,
        symbols: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
    ) -> None:
        """
        将当前订阅状态同步给数据提供者。
        """
        try:
            provider = get_data_provider()
            if not provider:
                return
            if unsubscribe:
                provider.unsubscribe_ticks(symbols)  # type: ignore[attr-defined]
                if markets:
                    provider.unsubscribe_markets(markets)  # type: ignore[attr-defined]
                return
            # subscribe
            if self.handle_tick_func and hasattr(provider, "set_tick_callback"):
                provider.set_tick_callback(self._provider_tick_callback)  # type: ignore[attr-defined]
                self._provider_tick_callback_bound = True
            if self._tick_symbols:
                provider.subscribe_ticks(list(self._tick_symbols))  # type: ignore[attr-defined]
            if self._tick_markets:
                provider.subscribe_markets(list(self._tick_markets))  # type: ignore[attr-defined]
            if initial and (self._tick_symbols or self._tick_markets):
                log.info(
                    "已向数据源同步历史 tick 订阅: symbols=%s markets=%s",
                    list(self._tick_symbols),
                    list(self._tick_markets),
                )
        except Exception as exc:
            log.warning("同步数据源 tick 订阅失败", exc_info=True)

    def _provider_tick_callback(self, data: Any) -> None:
        """
        数据源推送 tick 时的直通回调：不拆分、不加工，直接转发给策略的 handle_tick。
        """
        if not self.handle_tick_func or not self._loop:
            return
        try:
            asyncio.run_coroutine_threadsafe(self._call_hook(self.handle_tick_func, data), self._loop)  # type: ignore[arg-type]
        except Exception:
            pass
    async def _tick_loop(self) -> None:
        if not self.config.tick_sync_enabled:
            return
        interval = max(1, self.config.tick_sync_interval)
        assert self._loop is not None
        while not self._stop_event.is_set():
            # provider 已绑定 tick 回调则不再轮询，避免重复采样或回落到精简快照
            if self._provider_tick_callback_bound:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    continue
                continue

            if self._tick_symbols:
                for sym in list(self._tick_symbols):
                    try:
                        tick = await self._loop.run_in_executor(None, self._fetch_tick_snapshot, sym)
                    except Exception:
                        tick = None
                    if tick:
                        self._latest_ticks[sym] = tick
                        await self._call_hook(self.handle_tick_func, tick)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    # ------------------------------------------------------------------
    # 后台任务
    # ------------------------------------------------------------------

    def _start_background_jobs(self) -> None:
        assert self._loop is not None
        if self.config.account_sync_enabled and self.broker and self.broker.supports_account_sync():
            self._background_tasks.append(self._loop.create_task(
                self._periodic_task(
                    "account-sync",
                    self.config.account_sync_interval,
                    self._account_sync_step,
                )
            ))
        if self.config.order_sync_enabled and self.broker and self.broker.supports_orders_sync():
            self._background_tasks.append(self._loop.create_task(
                self._periodic_task(
                    "order-sync",
                    self.config.order_sync_interval,
                    self._order_sync_step,
                )
            ))
        if self.config.risk_check_enabled:
            self._background_tasks.append(self._loop.create_task(
                self._periodic_task(
                    "risk",
                    self.config.risk_check_interval,
                    self._risk_step,
                )
            ))
        if self.config.broker_heartbeat_interval > 0 and self.broker:
            self._background_tasks.append(self._loop.create_task(
                self._periodic_task(
                    "heartbeat",
                    self.config.broker_heartbeat_interval,
                    self._heartbeat_step,
                )
            ))
        # Tick 轮询
        self._background_tasks.append(self._loop.create_task(self._tick_loop()))

    async def _periodic_task(self, name: str, interval: int, coro_func: Callable[[], Awaitable[None]]) -> None:
        if interval <= 0:
            return
        assert self._loop is not None and self._stop_event is not None
        while not self._stop_event.is_set():
            try:
                await coro_func()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning(f"后台任务 {name} 执行失败: {exc}")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    async def _account_sync_step(self) -> None:
        if not self.broker or not self.broker.supports_account_sync():
            return
        assert self._loop is not None
        snapshot = await self._loop.run_in_executor(None, self.broker.sync_account)
        if snapshot:
            try:
                self._apply_account_snapshot(snapshot)
            except Exception as exc:
                log.warning(f"账户同步数据解析失败: {exc}")
            else:
                self._last_account_refresh = datetime.now()

    async def _order_sync_step(self) -> None:
        if not self.broker or not self.broker.supports_orders_sync():
            return
        assert self._loop is not None
        try:
            snapshots = await self._loop.run_in_executor(None, self._sync_orders_from_broker)
            if snapshots:
                self._apply_order_snapshots(snapshots)
        except Exception as exc:
            log.debug(f"订单同步失败: {exc}")

    async def _risk_step(self) -> None:
        if not self._risk:
            return
        try:
            summary = self._risk.get_status_summary()
            log.info(summary)
        except Exception:
            pass

    async def _heartbeat_step(self) -> None:
        if not self.broker:
            return
        assert self._loop is not None
        try:
            await self._loop.run_in_executor(None, self.broker.heartbeat)
        except Exception as exc:
            log.warning(f"券商心跳异常: {exc}")

    # ------------------------------------------------------------------
    # 工具函数
    # ------------------------------------------------------------------

    def _init_broker(self) -> None:
        self._ensure_broker_created()
        assert self.broker is not None
        self.broker.connect()
        summary = self._safe_account_info()
        positions = summary.get('positions') or []
        log.info(
            "✅ 券商 %s 连接成功: account_id=%s, type=%s, 可用资金=%s, 总资产=%s, 持仓数=%s",
            self.broker.__class__.__name__,
            summary.get('account_id') or getattr(self.broker, 'account_id', ''),
            summary.get('account_type') or getattr(self.broker, 'account_type', ''),
            summary.get('available_cash'),
            summary.get('total_value'),
            len(positions),
        )
        self._log_account_positions(summary)
        if summary:
            self._apply_account_snapshot(summary)

    def _ensure_broker_created(self) -> None:
        if self.broker is None:
            self.broker = self._create_broker()

    def _acquire_live_locks(self) -> None:
        if self._runtime_lock and self._instance_lock:
            return
        self._ensure_broker_created()
        assert self.broker is not None

        metadata = self._build_live_lock_metadata()
        runtime_dir = Path(self.config.runtime_dir).expanduser().resolve()
        runtime_lock = ManagedLiveLock(
            lock_path=runtime_dir / ".live.lock",
            metadata_path=runtime_dir / ".live.lock.json",
            metadata=dict(metadata, lock_kind="runtime_dir"),
            busy_message=f"实盘启动被拒绝：RUNTIME_DIR 已被其他 live 实例占用 ({runtime_dir})",
        )
        runtime_lock.acquire()
        try:
            instance_key = self._build_instance_lock_key(metadata)
            instance_dir = get_live_lock_dir()
            instance_lock = ManagedLiveLock(
                lock_path=instance_dir / f"{instance_key}.lock",
                metadata_path=instance_dir / f"{instance_key}.json",
                metadata=dict(metadata, lock_kind="logical_instance", instance_key=instance_key),
                busy_message="实盘启动被拒绝：检测到同机同策略同账号的重复 live 实例",
            )
            instance_lock.acquire()
        except Exception:
            runtime_lock.release()
            raise

        self._runtime_lock = runtime_lock
        self._instance_lock = instance_lock

    def _release_live_locks(self) -> None:
        if self._instance_lock:
            self._instance_lock.release()
            self._instance_lock = None
        if self._runtime_lock:
            self._runtime_lock.release()
            self._runtime_lock = None

    def _build_live_lock_metadata(self) -> Dict[str, Any]:
        assert self.broker is not None
        broker = self.broker
        broker_type = broker.__class__.__name__
        account_identity, account_parts = self._resolve_account_identity(broker)
        return build_lock_metadata(
            strategy_name=self.config.strategy_name or self.strategy_path.stem,
            strategy_path=str(self.strategy_path.resolve()),
            runtime_dir=str(Path(self.config.runtime_dir).expanduser().resolve()),
            broker_type=broker_type,
            broker_name=self.broker_name or broker_type,
            account_identity=account_identity,
            account_id=account_parts.get("account_id"),
            account_key=account_parts.get("account_key"),
            sub_account_id=account_parts.get("sub_account_id"),
        )

    def _resolve_account_identity(self, broker: BrokerBase) -> Tuple[str, Dict[str, str]]:
        def _text(value: Any) -> str:
            if value is None:
                return ""
            return str(value).strip()

        raw_parts = {
            "account_id": _text(getattr(broker, "account_id", "")),
            "account_key": _text(getattr(broker, "account_key", "")),
            "sub_account_id": _text(getattr(broker, "sub_account_id", "")),
        }
        cfg = getattr(broker, "config", None)
        if cfg:
            raw_parts["account_key"] = raw_parts["account_key"] or _text(getattr(cfg, "account_key", None))
            raw_parts["sub_account_id"] = raw_parts["sub_account_id"] or _text(
                getattr(cfg, "sub_account_id", None)
            )
            raw_parts["account_id"] = raw_parts["account_id"] or _text(getattr(cfg, "account_id", None))

        parts = {key: value for key, value in raw_parts.items() if value}
        ordered: List[str] = []
        if parts.get("account_key"):
            ordered.append(f"account_key={parts['account_key']}")
        if parts.get("sub_account_id"):
            ordered.append(f"sub_account_id={parts['sub_account_id']}")
        if parts.get("account_id"):
            ordered.append(f"account_id={parts['account_id']}")
        if not ordered:
            ordered.append(f"account_id=unknown:{broker.__class__.__name__}")
        return "|".join(ordered), parts

    def _build_instance_lock_key(self, metadata: Dict[str, Any]) -> str:
        key = "|".join(
            [
                metadata.get("host", ""),
                os.path.normcase(str(self.strategy_path.resolve())),
                metadata.get("broker_type", ""),
                metadata.get("account_identity", ""),
            ]
        )
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return f"live-instance-{digest}"

    @staticmethod
    def _task_meta_key(
        module: Optional[str],
        func_name: Optional[str],
        schedule_type: Optional[str],
        time_expr: Any,
        weekday: Any,
        monthday: Any,
    ) -> Tuple[Any, ...]:
        return (
            module or '',
            func_name or '',
            schedule_type or '',
            str(time_expr) if time_expr is not None else '',
            None if weekday is None else int(weekday),
            None if monthday is None else int(monthday),
        )

    def _dedupe_scheduler_tasks(self) -> None:
        tasks = list(get_tasks())
        if not tasks:
            return
        seen = set()
        unique: List[Any] = []
        for task in tasks:
            module = getattr(task.func, '__module__', None)
            name = getattr(task.func, '__name__', None)
            key = self._task_meta_key(
                module,
                name,
                task.schedule_type.value,
                task.time,
                task.weekday,
                task.monthday,
            )
            if key in seen:
                continue
            seen.add(key)
            unique.append(task)
        if len(unique) == len(tasks):
            return
        sync_scheduler._tasks = unique  # type: ignore[attr-defined]

    def _resolve_hook_args(self, func: Callable, extra_args: Tuple[Any, ...]) -> Tuple[Any, ...]:
        base_args: Tuple[Any, ...] = (self.context, *extra_args)
        try:
            sig = inspect.signature(func)
        except (ValueError, TypeError):
            return base_args

        params = list(sig.parameters.values())
        if not params:
            return ()
        if any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params):
            return base_args

        positional = [
            p
            for p in params
            if p.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        max_args = len(positional)
        if max_args <= 0:
            return ()
        if max_args >= len(base_args):
            return base_args
        return base_args[:max_args]

    async def _call_hook(self, func: Optional[Callable], *extra_args) -> None:
        if not func:
            return
        args = self._resolve_hook_args(func, extra_args)
        if asyncio.iscoroutinefunction(func):
            await func(*args)
        else:
            assert self._loop is not None
            await self._loop.run_in_executor(None, lambda: func(*args))

    def _migrate_scheduler_tasks(self) -> None:
        from .scheduler import get_tasks

        tasks = get_tasks()
        if not tasks or not self.async_scheduler:
            return
        for task in tasks:
            strategy = OverlapStrategy.SKIP
            if task.schedule_type.value == 'daily':
                self.async_scheduler.run_daily(task.func, task.time, strategy)
            elif task.schedule_type.value == 'weekly':
                self.async_scheduler.run_weekly(
                    task.func,
                    task.weekday,
                    task.time,
                    task.reference_security,
                    task.force,
                    strategy,
                )
            elif task.schedule_type.value == 'monthly':
                self.async_scheduler.run_monthly(
                    task.func,
                    task.monthday,
                    task.time,
                    task.reference_security,
                    task.force,
                    strategy,
                )

    def _snapshot_strategy_metadata(self, strategy_hash: Optional[str]) -> None:
        try:
            metadata = {
                "version": 1,
                "strategy_hash": strategy_hash,
                "settings": self._collect_settings_snapshot(),
                "tasks": self._collect_scheduler_tasks_snapshot(),
            }
            if self._strategy_start_date:
                metadata["strategy_start_date"] = self._strategy_start_date.isoformat()
            persist_strategy_metadata(metadata)
        except Exception as exc:
            log.debug(f"策略元数据快照失败: {exc}")

    def _persist_strategy_start_date(self) -> None:
        if not self._strategy_start_date:
            return
        try:
            metadata = load_strategy_metadata()
            if not metadata or metadata.get('version') != 1:
                return
            if metadata.get('strategy_start_date') == self._strategy_start_date.isoformat():
                return
            metadata = dict(metadata)
            metadata['strategy_start_date'] = self._strategy_start_date.isoformat()
            persist_strategy_metadata(metadata)
        except Exception as exc:
            log.debug(f"策略起始日写入失败: {exc}")

    def _apply_market_period_override(self) -> None:
        expr = (self.config.scheduler_market_periods or "").strip()
        if not expr:
            return
        try:
            periods = parse_market_periods_string(expr)
            set_option('market_period', [(start, end) for start, end in periods])
            log.info("⚙️  已应用自定义交易时段: %s", expr)
        except Exception as exc:
            log.warning("环境变量 SCHEDULER_MARKET_PERIODS 解析失败(%s): %s", expr, exc)

    def _collect_settings_snapshot(self) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {}
        settings = get_settings()
        snapshot['benchmark'] = settings.benchmark
        options = self._serialize_options(settings.options or {})
        if isinstance(options.get('market_period'), (list, tuple)):
            options['market_period'] = self._serialize_market_periods(options['market_period'])
        snapshot['options'] = options
        order_cost_snapshot: Dict[str, Dict[str, Any]] = {}
        order_cost_override_snapshot: Dict[str, Dict[str, Any]] = {}
        for asset, cost in (settings.order_cost or {}).items():
            order_cost_snapshot[str(asset)] = {
                'open_tax': cost.open_tax,
                'close_tax': cost.close_tax,
                'open_commission': cost.open_commission,
                'close_commission': cost.close_commission,
                'min_commission': cost.min_commission,
                'close_today_commission': cost.close_today_commission,
                'commission_type': getattr(cost, 'commission_type', 'by_money'),
            }
        snapshot['order_cost'] = order_cost_snapshot
        for asset, cost in (getattr(settings, 'order_cost_overrides', {}) or {}).items():
            order_cost_override_snapshot[str(asset)] = {
                'open_tax': cost.open_tax,
                'close_tax': cost.close_tax,
                'open_commission': cost.open_commission,
                'close_commission': cost.close_commission,
                'min_commission': cost.min_commission,
                'close_today_commission': cost.close_today_commission,
                'commission_type': getattr(cost, 'commission_type', 'by_money'),
            }
        if order_cost_override_snapshot:
            snapshot['order_cost_overrides'] = order_cost_override_snapshot
        if settings.slippage:
            payload = {'class': settings.slippage.__class__.__name__}
            if hasattr(settings.slippage, 'value'):
                payload['value'] = getattr(settings.slippage, 'value', None)
            if hasattr(settings.slippage, 'ratio'):
                payload['ratio'] = getattr(settings.slippage, 'ratio', None)
            if hasattr(settings.slippage, 'steps'):
                payload['steps'] = getattr(settings.slippage, 'steps', None)
            snapshot['slippage'] = payload
        sl_map = getattr(settings, 'slippage_map', {}) or {}
        sl_map_snapshot: Dict[str, Any] = {}
        for key, cfg in sl_map.items():
            payload = self._serialize_slippage_config(cfg)
            if payload is not None:
                sl_map_snapshot[key] = payload
        if sl_map_snapshot:
            snapshot['slippage_map'] = sl_map_snapshot
        return snapshot

    @staticmethod
    def _serialize_options(options: Dict[str, Any]) -> Dict[str, Any]:
        def _normalize(value: Any) -> Any:
            if isinstance(value, (datetime, date, Time)):
                return value.isoformat()
            if isinstance(value, dict):
                return {k: _normalize(v) for k, v in value.items()}
            if isinstance(value, (list, tuple, set)):
                return [_normalize(v) for v in value]
            return value

        return {key: _normalize(value) for key, value in dict(options).items()}

    @staticmethod
    def _parse_date_value(value: Any) -> Optional[date]:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError:
                try:
                    return datetime.fromisoformat(value).date()
                except ValueError:
                    return None
        return None

    @staticmethod
    def _serialize_slippage_config(config: Any) -> Optional[Dict[str, Any]]:
        if isinstance(config, PriceRelatedSlippage):
            return {'class': 'PriceRelatedSlippage', 'ratio': float(config.ratio)}
        if isinstance(config, StepRelatedSlippage):
            return {'class': 'StepRelatedSlippage', 'steps': int(config.steps)}
        if isinstance(config, FixedSlippage):
            return {'class': 'FixedSlippage', 'value': float(config.value)}
        if hasattr(config, 'to_dict'):
            try:
                return {'class': config.__class__.__name__, **config.to_dict()}
            except Exception:
                return None
        return None

    @staticmethod
    def _deserialize_slippage_config(payload: Dict[str, Any]) -> Optional[Any]:
        if not isinstance(payload, dict):
            return None
        cls = payload.get('class')
        try:
            if cls == 'PriceRelatedSlippage':
                return PriceRelatedSlippage(payload.get('ratio', 0.0))
            if cls == 'StepRelatedSlippage':
                return StepRelatedSlippage(payload.get('steps', 0))
            if cls == 'FixedSlippage':
                return FixedSlippage(payload.get('value', 0.0))
        except Exception:
            return None
        return None

    def _collect_scheduler_tasks_snapshot(self) -> List[Dict[str, Any]]:
        tasks_meta: List[Dict[str, Any]] = []
        seen = set()
        for task in get_tasks():
            func = task.func
            module = getattr(func, '__module__', None)
            name = getattr(func, '__name__', None)
            if not module or not name:
                continue
            key = self._task_meta_key(
                module,
                name,
                task.schedule_type.value,
                task.time,
                task.weekday,
                task.monthday,
            )
            if key in seen:
                continue
            seen.add(key)
            tasks_meta.append(
                {
                    'module': module,
                    'func': name,
                    'schedule_type': task.schedule_type.value,
                    'time': task.time,
                    'weekday': task.weekday,
                    'monthday': task.monthday,
                    'enabled': getattr(task, 'enabled', True),
                }
            )
        return tasks_meta

    def _restore_strategy_metadata(self, meta: Dict[str, Any]) -> bool:
        if not meta or meta.get('version') != 1:
            return False
        try:
            raw_start_date = meta.get('strategy_start_date')
            if raw_start_date:
                parsed_start_date = self._parse_date_value(raw_start_date)
                if parsed_start_date:
                    self._strategy_start_date = parsed_start_date
                else:
                    log.debug(f"策略起始日格式无效: {raw_start_date}")
            self._apply_settings_snapshot(meta.get('settings') or {})
            self._apply_scheduler_tasks_snapshot(meta.get('tasks') or [])
            return True
        except Exception as exc:
            log.warning(f"恢复策略元数据失败: {exc}")
            return False

    def _apply_settings_snapshot(self, snapshot: Dict[str, Any]) -> None:
        if not snapshot:
            return
        benchmark = snapshot.get('benchmark')
        if benchmark:
            try:
                set_benchmark(benchmark)
            except Exception as exc:
                log.warning(f"恢复 benchmark 失败: {exc}")
        options = snapshot.get('options') or {}
        for key, value in options.items():
            try:
                if key == 'market_period' and value:
                    value = self._deserialize_market_periods(value)
                set_option(key, value)
            except Exception as exc:
                log.debug(f"恢复 option {key} 失败: {exc}")
        order_costs = snapshot.get('order_cost') or {}
        for asset, payload in order_costs.items():
            try:
                cost = OrderCost(**payload)
                set_order_cost(cost, type=asset)
            except Exception as exc:
                log.debug(f"恢复 order_cost({asset}) 失败: {exc}")
        order_cost_overrides = snapshot.get('order_cost_overrides') or {}
        for asset, payload in order_cost_overrides.items():
            try:
                cost = OrderCost(**payload)
                # asset 形如 type_code
                if '_' in asset:
                    type_prefix, ref_code = asset.split('_', 1)
                    set_order_cost(cost, type=type_prefix, ref=ref_code)
            except Exception as exc:
                log.debug(f"恢复 order_cost_overrides({asset}) 失败: {exc}")
        sl_map = snapshot.get('slippage_map') or {}
        if sl_map:
            try:
                settings = get_settings()
                settings.slippage_map = {}
                for key, payload in sl_map.items():
                    cfg = self._deserialize_slippage_config(payload)
                    if cfg:
                        settings.slippage_map[key] = cfg
                if settings.slippage is None and 'all' in settings.slippage_map:
                    settings.slippage = settings.slippage_map.get('all')
            except Exception as exc:
                log.debug(f"恢复 slippage_map 失败: {exc}")
        slippage = snapshot.get('slippage')
        if slippage:
            cls = slippage.get('class')
            try:
                if cls == 'FixedSlippage':
                    set_slippage(FixedSlippage(slippage.get('value', 0.0)))
                elif cls == 'PriceRelatedSlippage':
                    set_slippage(PriceRelatedSlippage(slippage.get('ratio', 0.0)))
                elif cls == 'StepRelatedSlippage':
                    set_slippage(StepRelatedSlippage(slippage.get('steps', 0)))
            except Exception as exc:
                log.debug(f"恢复 slippage 失败: {exc}")

    def _apply_scheduler_tasks_snapshot(self, tasks: List[Dict[str, Any]]) -> None:
        try:
            unschedule_all()
        except Exception:
            pass
        if not tasks:
            return
        normalized_tasks: List[Dict[str, Any]] = []
        seen = set()
        for task_meta in tasks:
            key = self._task_meta_key(
                task_meta.get('module'),
                task_meta.get('func'),
                task_meta.get('schedule_type'),
                task_meta.get('time'),
                task_meta.get('weekday'),
                task_meta.get('monthday'),
            )
            if key in seen:
                continue
            seen.add(key)
            normalized_tasks.append(task_meta)

        for task_meta in normalized_tasks:
            func = self._resolve_callable(task_meta.get('module'), task_meta.get('func'))
            if not func:
                log.warning(f"无法恢复调度任务: {task_meta}")
                continue
            schedule_type = task_meta.get('schedule_type')
            time_expr = task_meta.get('time', 'every_bar')
            enabled = bool(task_meta.get('enabled', True))
            try:
                if schedule_type == 'daily':
                    run_daily(func, time_expr)
                elif schedule_type == 'weekly':
                    run_weekly(func, task_meta.get('weekday'), time_expr)
                elif schedule_type == 'monthly':
                    run_monthly(func, task_meta.get('monthday'), time_expr)
                current_tasks = get_tasks()
                if current_tasks:
                    current_task = current_tasks[-1]
                    current_task.enabled = bool(enabled)
            except Exception as exc:
                log.warning(f"恢复调度任务失败 {task_meta}: {exc}")

    def _resolve_callable(self, module_name: Optional[str], func_name: Optional[str]) -> Optional[Callable]:
        if not module_name or not func_name:
            return None
        module = sys.modules.get(module_name)
        if not module:
            try:
                module = importlib.import_module(module_name)
            except Exception:
                return None
        return getattr(module, func_name, None)

    def _compute_strategy_hash(self) -> Optional[str]:
        try:
            data = self.strategy_path.read_bytes()
            return hashlib.md5(data).hexdigest()
        except Exception:
            return None

    def _serialize_market_periods(self, periods: Sequence[Tuple[Time, Time]]) -> List[List[str]]:
        serialized: List[List[str]] = []
        for start, end in periods:
            if isinstance(start, Time) and isinstance(end, Time):
                serialized.append([start.strftime("%H:%M:%S"), end.strftime("%H:%M:%S")])
        return serialized

    def _deserialize_market_periods(self, raw: Sequence[Sequence[Any]]) -> List[Tuple[Time, Time]]:
        periods: List[Tuple[Time, Time]] = []
        for item in raw:
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                continue
            try:
                start = datetime.strptime(str(item[0]), "%H:%M:%S").time()
                end = datetime.strptime(str(item[1]), "%H:%M:%S").time()
                periods.append((start, end))
            except Exception:
                continue
        return periods

    def refresh_account_snapshot(self, force: bool = False) -> None:
        if not self.broker or not self.broker.supports_account_sync():
            return
        now = datetime.now()
        if not force and self._last_account_refresh and (now - self._last_account_refresh).total_seconds() < 1:
            return
        try:
            snapshot = self.broker.sync_account()
        except Exception as exc:
            log.debug(f"即时账户刷新失败: {exc}")
            return
        if snapshot:
            self._apply_account_snapshot(snapshot)
            self._last_account_refresh = now

    def _apply_account_snapshot(self, snapshot: Dict[str, Any]) -> None:
        try:
            target = self.portfolio_proxy.backing if isinstance(self.context.portfolio, LivePortfolioProxy) else self.context.portfolio
            cash = snapshot.get('available_cash')
            total = snapshot.get('total_value')
            if cash is not None:
                target.available_cash = float(cash)
            if total is not None:
                target.total_value = float(total)
            positions = snapshot.get('positions') or []
            target.positions.clear()
            for item in positions:
                security = item.get('security')
                if not security:
                    continue
                amount = int(item.get('amount', item.get('total_amount', 0)) or 0)
                price = float(item.get('current_price', item.get('price', 0.0)) or 0.0)
                target.positions[security] = Position(
                    security=security,
                    total_amount=amount,
                    closeable_amount=int(item.get('closeable_amount', amount)),
                    avg_cost=float(item.get('avg_cost', 0.0) or 0.0),
                    price=price,
                    value=float(item.get('market_value', amount * price)),
                )
            target.update_value()
        except Exception as exc:
            log.debug(f"应用账户快照失败: {exc}")
            return

        if not self._initial_nav_synced and getattr(target, "total_value", 0) > 0:
            try:
                target.starting_cash = float(target.total_value)
                self._initial_nav_synced = True
            except Exception:
                pass

    def _safe_account_info(self) -> Dict[str, Any]:
        if not self.broker:
            return {}
        try:
            info = self.broker.get_account_info() or {}
            # 如果券商返回的是自定义对象，尽量转成 dict
            if not isinstance(info, dict):
                info = getattr(info, '__dict__', {}) or {}
            return info
        except Exception as exc:
            log.debug(f"获取账户信息失败: {exc}")
            return {}

    def _log_account_positions(self, summary: Dict[str, Any], limit: int = 8) -> None:
        """
        以 print_portfolio_info 风格输出券商账户概览，避免原始 list 噪音。
        """
        try:
            positions = list(summary.get('positions') or [])
            total_value = self._to_float(summary.get('total_value'))
            cash = self._to_float(summary.get('available_cash'))
            invested = 0.0
            entries: List[Dict[str, Any]] = []
            for item in positions:
                code = item.get('security') or item.get('code')
                if not code:
                    continue
                amount = int(item.get('amount', item.get('total_amount', 0)) or 0)
                if amount <= 0:
                    continue
                closeable = int(item.get('closeable_amount', amount) or amount)
                avg_cost = self._to_float(item.get('avg_cost'))
                price = self._to_float(item.get('current_price', item.get('price')))
                value = self._to_float(item.get('market_value'), default=price * amount)
                if value == 0.0:
                    value = price * amount
                invested += value
                pnl = value - avg_cost * amount
                pnl_pct = ((price / avg_cost - 1.0) * 100.0) if avg_cost > 0 else 0.0
                weight = ((value / total_value) * 100.0) if total_value > 0 else 0.0
                name = item.get('display_name') or item.get('name') or self._lookup_security_name(code)
                entries.append(
                    {
                        'code': code,
                        'name': name,
                        'amount': amount,
                        'closeable': closeable,
                        'avg_cost': avg_cost,
                        'price': price,
                        'value': value,
                        'pnl': pnl,
                        'pnl_pct': pnl_pct,
                        'weight': weight,
                    }
                )

            position_ratio = (invested / total_value * 100.0) if total_value > 0 else 0.0
            log.info(
                "📊 券商账户概览: 总资产 %s, 可用资金 %s, 仓位 %.2f%%",
                self._format_currency(total_value),
                self._format_currency(cash),
                position_ratio,
            )
            if not entries:
                log.info("当前持仓：无")
                return

            entries.sort(key=lambda x: x['value'], reverse=True)
            entries = entries[:limit]
            headers = ["股票代码", "名称", "持仓", "可用", "成本价", "现价", "市值", "盈亏", "盈亏%", "占比%"]
            rows = [
                [
                    entry['code'],
                    entry['name'],
                    str(entry['amount']),
                    str(entry['closeable']),
                    f"{entry['avg_cost']:.3f}",
                    f"{entry['price']:.3f}",
                    f"{entry['value']:,.2f}",
                    f"{entry['pnl']:,.2f}",
                    f"{entry['pnl_pct']:.2f}%",
                    f"{entry['weight']:.2f}%",
                ]
                for entry in entries
            ]
            log.info("\n" + self._render_table(headers, rows))
        except Exception as exc:
            log.debug(f"打印券商持仓失败: {exc}")

    def _lookup_security_name(self, code: str) -> str:
        if not code:
            return ""
        cached = self._security_name_cache.get(code)
        if cached is not None:
            return cached
        name = ""
        try:
            info = get_security_info(code)
            name = getattr(info, "display_name", None) or getattr(info, "name", "") if info else ""
        except Exception:
            name = ""
        self._security_name_cache[code] = name or ""
        return name or ""

    @classmethod
    def _render_table(cls, headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
        widths = [cls._display_width(str(h)) for h in headers]
        normalized_rows: List[List[str]] = []
        for row in rows:
            str_row = [str(cell) for cell in row]
            normalized_rows.append(str_row)
            for idx, cell in enumerate(str_row):
                widths[idx] = max(widths[idx], cls._display_width(cell))

        def _border(char: str) -> str:
            return "+" + "+".join(char * (w + 2) for w in widths) + "+"

        def _format_row(values: Sequence[str]) -> str:
            segments = [
                f" {cls._pad_cell(str(value), widths[idx])} "
                for idx, value in enumerate(values)
            ]
            return "|" + "|".join(segments) + "|"

        lines = [_border("-"), _format_row(headers), _border("=")]
        for row in normalized_rows:
            lines.append(_format_row(row))
            lines.append(_border("-"))
        return "\n".join(lines)

    @staticmethod
    def _maybe_float(value: Any) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _format_currency(value: float) -> str:
        return f"{value:,.2f}"

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _display_width(text: str) -> int:
        width = 0
        for char in text:
            if unicodedata.combining(char):
                continue
            east_width = unicodedata.east_asian_width(char)
            width += 2 if east_width in ("F", "W") else 1
        return width

    @classmethod
    def _pad_cell(cls, text: str, target_width: int) -> str:
        current = cls._display_width(text)
        padding = max(target_width - current, 0)
        return text + (" " * padding)
class LivePortfolioProxy:
    """
    代理 Portfolio，确保访问现金/持仓时优先刷新券商快照。
    """

    __slots__ = ("_engine", "_backing", "_last_refresh", "_refresh_interval")

    def __init__(self, engine: "LiveEngine", backing: Portfolio):
        object.__setattr__(self, "_engine", engine)
        object.__setattr__(self, "_backing", backing)
        throttle_ms = getattr(engine.config, "portfolio_refresh_throttle_ms", 200)
        object.__setattr__(self, "_refresh_interval", max(float(throttle_ms) / 1000.0, 0.0))
        object.__setattr__(self, "_last_refresh", datetime.min)

    def _refresh_if_needed(self):
        last = object.__getattribute__(self, "_last_refresh")
        now = datetime.now()
        interval = object.__getattribute__(self, "_refresh_interval")
        if interval > 0 and (now - last).total_seconds() < interval:
            return
        engine = object.__getattribute__(self, "_engine")
        engine.refresh_account_snapshot(force=True)
        object.__setattr__(self, "_last_refresh", now)

    @property
    def available_cash(self) -> float:
        self._refresh_if_needed()
        return object.__getattribute__(self, "_backing").available_cash

    @property
    def total_value(self) -> float:
        self._refresh_if_needed()
        return object.__getattribute__(self, "_backing").total_value

    @property
    def positions(self):
        self._refresh_if_needed()
        return object.__getattribute__(self, "_backing").positions

    def __getattr__(self, item):
        if item.startswith("_"):
            raise AttributeError(item)
        self._refresh_if_needed()
        return getattr(object.__getattribute__(self, "_backing"), item)

    def __setattr__(self, key, value):
        if key in LivePortfolioProxy.__slots__:
            object.__setattr__(self, key, value)
        else:
            setattr(object.__getattribute__(self, "_backing"), key, value)

    @property
    def backing(self) -> Portfolio:
        return object.__getattribute__(self, "_backing")


class TradingCalendarGuard:
    """
    控制交易日启动行为：如果今天不是交易日则等待下次检查。
    """

    def __init__(self, config: LiveConfig):
        self.config = config
        self._next_check: Optional[datetime] = None
        self._confirmed_date: Optional[date] = None
        self._last_diag_log_time: Optional[datetime] = None
        self._diag_log_interval_seconds: int = 300

    async def ensure_trade_day(self, now: datetime) -> bool:
        today = now.date()
        if self._confirmed_date == today:
            return True
        if self._next_check and now < self._next_check:
            return False
        if await self._is_trading_day(today):
            self._confirmed_date = today
            return True
        wait_minutes = max(1, int(self._config_value("calendar_retry_minutes", 1)))
        self._next_check = now + timedelta(minutes=wait_minutes)
        self._log_calendar_diag(
            now=now,
            target=today,
            reason="not_trade_day",
            extra={
                "wait_minutes": wait_minutes,
                "next_check": self._next_check.strftime("%Y-%m-%d %H:%M:%S"),
            },
        )
        log.debug("今日非交易日，下一次检查时间 %s", self._next_check.strftime("%Y-%m-%d %H:%M"))
        return False

    async def _is_trading_day(self, target: date) -> bool:
        query = f"{target}~{target}"
        try:
            from bullet_trade.data.api import get_trade_days

            days = get_trade_days(str(target), str(target))
            if hasattr(days, "empty"):
                result = not days.empty
                if not result:
                    self._log_calendar_diag(
                        now=datetime.now(),
                        target=target,
                        reason="empty_dataframe",
                        extra={"query": query},
                    )
                return result
            if days is None:
                self._log_calendar_diag(
                    now=datetime.now(),
                    target=target,
                    reason="days_none",
                    extra={"query": query},
                )
                return False
            if isinstance(days, (list, tuple, set)):
                if not days:
                    self._log_calendar_diag(
                        now=datetime.now(),
                        target=target,
                        reason="days_empty",
                        extra={"query": query},
                    )
                    return False
                for day in days:
                    try:
                        if pd.to_datetime(day).date() == target:
                            return True
                    except Exception:
                        continue
                self._log_calendar_diag(
                    now=datetime.now(),
                    target=target,
                    reason="target_not_in_days",
                    extra={"query": query, "sample_days": self._sample_days(days)},
                )
                return False
            try:
                iterator = iter(days)
            except TypeError:
                self._log_calendar_diag(
                    now=datetime.now(),
                    target=target,
                    reason="days_not_iterable",
                    extra={"query": query, "days_type": type(days).__name__},
                )
                return False
            has_value = False
            sample_days: List[Any] = []
            for day in iterator:
                has_value = True
                if len(sample_days) < 5:
                    sample_days.append(day)
                try:
                    if pd.to_datetime(day).date() == target:
                        return True
                except Exception:
                    continue
            if has_value:
                self._log_calendar_diag(
                    now=datetime.now(),
                    target=target,
                    reason="target_not_in_iterable",
                    extra={"query": query, "sample_days": self._sample_days(sample_days)},
                )
                return False
        except Exception as exc:
            self._log_calendar_diag(
                now=datetime.now(),
                target=target,
                reason="get_trade_days_exception",
                extra={"query": query, "error": repr(exc)},
            )
        weekend_skip = bool(self._config_value("calendar_skip_weekend", True))
        if weekend_skip and target.weekday() >= 5:
            self._log_calendar_diag(
                now=datetime.now(),
                target=target,
                reason="weekend_skip",
                extra={"weekday": target.weekday()},
            )
            return False
        return True

    @staticmethod
    def _sample_days(days: Any, limit: int = 5) -> str:
        values: List[str] = []
        try:
            for idx, day in enumerate(days):
                if idx >= limit:
                    break
                values.append(str(day))
        except Exception:
            return "unavailable"
        return ",".join(values) if values else "empty"

    def _log_calendar_diag(
        self,
        *,
        now: datetime,
        target: date,
        reason: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        if self._last_diag_log_time:
            elapsed = (now - self._last_diag_log_time).total_seconds()
            if elapsed < self._diag_log_interval_seconds:
                return
        self._last_diag_log_time = now
        payload: Dict[str, Any] = {"target": str(target), "reason": reason}
        if extra:
            payload.update(extra)
        log.debug("TradingCalendarGuard 诊断: %s", payload)

    def _config_value(self, name: str, default: Any) -> Any:
        if hasattr(self.config, name):
            value = getattr(self.config, name)
            if value is not None:
                return value
        if isinstance(self.config, dict):
            value = self.config.get(name)
            if value is not None:
                return value
        return default

    def seconds_until_next_check(self, now: datetime) -> float:
        if not self._next_check:
            return 1.0
        return max(0.1, (self._next_check - now).total_seconds())
