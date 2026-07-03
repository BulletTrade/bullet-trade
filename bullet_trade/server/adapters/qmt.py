from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from bullet_trade.broker.qmt import QmtBroker
from bullet_trade.core.globals import log
from bullet_trade.data.providers.miniqmt import MiniQMTProvider
from bullet_trade.server.qmt_guard import (
    QmtAvailabilityGuard,
    QmtGuardError,
    QmtUnavailableError,
    is_qmt_connectivity_error,
    load_qmt_guard_config,
)
from bullet_trade.utils.env_loader import get_data_provider_config

from ..config import AccountConfig, ServerConfig
from .base import (
    AccountContext,
    AccountRouter,
    AdapterBundle,
    RemoteBrokerAdapter,
    RemoteDataAdapter,
)
from . import register_adapter

# 专用线程池：用于执行 xtquant 的同步调用
# max_workers 设置较大，避免长时间阻塞的调用占满线程池
# 即使有 "僵尸" 线程，也不会影响新请求的处理
_QMT_EXECUTOR: Optional[ThreadPoolExecutor] = None
_QMT_EXECUTOR_MAX_WORKERS = 32


def _to_positive_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _merge_lower(*values: Optional[float]) -> Optional[float]:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _merge_upper(*values: Optional[float]) -> Optional[float]:
    present = [value for value in values if value is not None]
    return min(present) if present else None


def _round_price_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return price
    return float(f"{round(price / tick) * tick:.6f}")


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _clamp_market_protect_price(
    security: str,
    price: float,
    last_price: float,
    high_limit: Any,
    low_limit: Any,
    is_buy: bool,
) -> float:
    """Clamp an explicit market protect price with the same cage rules as computed defaults."""
    base_price = float(last_price or 0.0)
    if base_price <= 0:
        return price
    from bullet_trade.core import pricing

    tick = pricing.get_min_price_step(security, base_price)
    cage_buy, cage_sell = pricing.compute_price_bounds(security, base_price, tick)
    current_high = _to_positive_float(high_limit)
    current_low = _to_positive_float(low_limit)
    if is_buy:
        lower = current_low
        upper = _merge_upper(current_high, cage_buy)
    else:
        lower = _merge_lower(current_low, cage_sell)
        upper = current_high

    clamped = float(price)
    if lower is not None:
        clamped = max(clamped, lower)
    if upper is not None:
        clamped = min(clamped, upper)
    clamped = _round_price_to_tick(clamped, tick)
    if lower is not None:
        clamped = max(clamped, lower)
    if upper is not None:
        clamped = min(clamped, upper)
    return clamped


def _get_executor() -> ThreadPoolExecutor:
    """
    获取 QMT 专用线程池（惰性初始化）。

    使用专用线程池而不是默认的 asyncio 线程池，
    避免 xtquant 的阻塞调用影响其他异步操作。
    """
    global _QMT_EXECUTOR
    if _QMT_EXECUTOR is None:
        _QMT_EXECUTOR = ThreadPoolExecutor(
            max_workers=_QMT_EXECUTOR_MAX_WORKERS,
            thread_name_prefix="qmt-worker-",
        )
        log.info(f"[QMT] 初始化专用线程池, max_workers={_QMT_EXECUTOR_MAX_WORKERS}")
    return _QMT_EXECUTOR


def _shutdown_executor(wait: bool = False) -> None:
    """
    关闭 QMT 专用线程池。

    :param wait: 是否等待所有任务完成。如果为 False，会取消等待中的任务但不会中断正在运行的任务。
    """
    global _QMT_EXECUTOR
    if _QMT_EXECUTOR is not None:
        log.info("[QMT] 关闭专用线程池...")
        _QMT_EXECUTOR.shutdown(wait=wait, cancel_futures=True)
        _QMT_EXECUTOR = None


async def _run_in_qmt_executor(func, *args, **kwargs):
    """
    在 QMT 专用线程池中执行同步函数。

    相比 asyncio.to_thread：
    - 使用专用线程池，不会占用默认线程池
    - 线程池容量更大，容忍更多并发或 "僵尸" 线程
    """
    loop = asyncio.get_running_loop()
    if kwargs:
        # run_in_executor 不支持 kwargs，需要用 partial 包装
        func = partial(func, *args, **kwargs)
        return await loop.run_in_executor(_get_executor(), func)
    return await loop.run_in_executor(_get_executor(), func, *args)


def _provider_config() -> Dict[str, Any]:
    cfg = get_data_provider_config().get("qmt", {})
    return {
        "data_dir": cfg.get("data_dir"),
        "cache_dir": cfg.get("cache_dir"),
        "market": cfg.get("market"),
        "auto_download": cfg.get("auto_download"),
        "tushare_token": cfg.get("tushare_token"),
        "mode": "live",
    }


def _resolve_start_end(payload: Dict[str, Any]) -> tuple[Any, Any]:
    """
    兼容 start/end 与 start_date/end_date 两套字段名。
    """

    start = payload.get("start")
    end = payload.get("end")
    if start in (None, ""):
        start = payload.get("start_date")
    if end in (None, ""):
        end = payload.get("end_date")
    return start, end


class QmtDataAdapter(RemoteDataAdapter):
    def __init__(
        self,
        guard: Optional[QmtAvailabilityGuard] = None,
        *,
        allow_request_probe: bool = True,
    ) -> None:
        """初始化 QMT 数据适配器。

        Args:
            guard: 共享 QMT 可用性保护器。
            allow_request_probe: 没有 broker 后台探针时，是否允许 cooldown 到期后的首个数据请求作为有限探针。

        Returns:
            None。
        """

        self.guard = guard or QmtAvailabilityGuard(name="qmt-data")
        self._allow_request_probe = allow_request_probe
        self.provider = MiniQMTProvider(_provider_config())

    async def _run_guarded_qmt_call(self, func, *args, **kwargs):
        """在 QMT guard 保护下执行 live xtdata 调用。

        Args:
            func: 需要在 QMT executor 中执行的同步函数。
            *args: 传给 func 的位置参数。
            **kwargs: 传给 func 的关键字参数。

        Returns:
            Any: func 的返回值。

        Raises:
            QmtGuardError: QMT 当前不可用或正在重连。

        Side Effects:
            QMT 调用失败时会更新 guard failure/cooldown 状态。
        """

        acquired_probe = False
        if not self.guard.ready:
            if self._allow_request_probe and self.guard.is_probe_due():
                acquired_probe = await self.guard.acquire_probe()
            if not acquired_probe:
                self.guard.ensure_ready()
        try:
            result = await _run_in_qmt_executor(func, *args, **kwargs)
        except QmtGuardError:
            raise
        except Exception as exc:
            if not is_qmt_connectivity_error(exc):
                if acquired_probe:
                    self.guard.mark_failure(RuntimeError("QMT 数据探针未能判断服务状态"))
                raise
            self.guard.mark_failure(exc)
            raise QmtUnavailableError(
                f"QMT 数据服务调用失败: {exc}",
                state=self.guard.state,
                retry_after_seconds=self.guard.seconds_until_probe(),
            ) from exc
        else:
            if acquired_probe:
                self.guard.mark_ready()
            return result
        finally:
            if acquired_probe:
                self.guard.release_probe()

    def qmt_status(self) -> Dict[str, Any]:
        """返回 QMT 数据适配器的 health 快照。

        Args:
            None。

        Returns:
            Dict[str, Any]: guard 状态快照。
        """

        return self.guard.snapshot()

    async def get_history(self, payload: Dict) -> Dict:
        """
        获取历史 K 线数据。

        :param payload: 包含 security, count, start, end, frequency, fq, fields 等参数
        :return: DataFrame 转换后的 payload 字典
        """
        import traceback
        import logging

        logger = logging.getLogger(__name__)

        security = payload.get("security")
        count = payload.get("count")
        start, end = _resolve_start_end(payload)
        frequency = payload.get("frequency") or payload.get("period")
        fq = payload.get("fq")
        fields = payload.get("fields")
        skip_paused = bool(payload.get("skip_paused", False))
        panel = payload.get("panel", True)
        fill_paused = payload.get("fill_paused", True)
        pre_factor_ref_date = payload.get("pre_factor_ref_date")

        logger.debug(
            f"[QmtDataAdapter.get_history] 请求参数: security={security}, count={count}, "
            f"start={start}, end={end}, frequency={frequency}, fq={fq}, fields={fields}"
        )

        def _call():
            return self.provider.get_price(
                security,
                count=count,
                start_date=start,
                end_date=end,
                frequency=frequency,
                fq=fq,
                fields=fields,
                skip_paused=skip_paused,
                panel=panel,
                fill_paused=fill_paused,
                pre_factor_ref_date=pre_factor_ref_date,
            )

        try:
            df = await self._run_guarded_qmt_call(_call)
            logger.debug(
                f"[QmtDataAdapter.get_history] 返回数据: shape={df.shape if df is not None else None}, "
                f"columns={list(df.columns) if df is not None and hasattr(df, 'columns') else None}"
            )
            return dataframe_to_payload(df)
        except QmtGuardError:
            raise
        except KeyError as e:
            # KeyError 通常表示数据格式问题（如缺少 time 列）
            error_msg = f"数据格式错误，缺少字段 {e}: security={security}, frequency={frequency}"
            logger.error(f"[QmtDataAdapter.get_history] {error_msg}\n{traceback.format_exc()}")
            raise RuntimeError(error_msg) from e
        except Exception as e:
            # 捕获所有其他异常并添加上下文信息
            error_msg = (
                f"获取历史数据失败: {type(e).__name__}: {e} (security={security}, frequency={frequency})"
            )
            logger.error(f"[QmtDataAdapter.get_history] {error_msg}\n{traceback.format_exc()}")
            raise RuntimeError(error_msg) from e

    async def get_snapshot(self, payload: Dict) -> Dict:
        """
        获取实时快照数据。

        :param payload: 包含 security 参数
        :return: tick 数据字典
        """
        import traceback
        import logging

        logger = logging.getLogger(__name__)

        security = payload.get("security")
        logger.debug(f"[QmtDataAdapter.get_snapshot] 请求参数: security={security}")

        def _call():
            return self.provider.get_current_tick(security)

        try:
            tick = await self._run_guarded_qmt_call(_call)
            logger.debug(f"[QmtDataAdapter.get_snapshot] 返回数据: {tick}")
            return tick or {}
        except QmtGuardError:
            raise
        except Exception as e:
            error_msg = f"获取快照数据失败: {type(e).__name__}: {e} (security={security})"
            logger.error(f"[QmtDataAdapter.get_snapshot] {error_msg}\n{traceback.format_exc()}")
            raise RuntimeError(error_msg) from e

    async def get_live_current(self, payload: Dict) -> Dict:
        """返回实盘快照（含停牌标记）。"""
        security = payload.get("security")

        def _call():
            return self.provider.get_live_current(security)

        tick = await self._run_guarded_qmt_call(_call)
        return tick or {}

    async def get_trade_days(self, payload: Dict) -> Dict:
        start, end = _resolve_start_end(payload)
        count = payload.get("count")

        def _call():
            return self.provider.get_trade_days(start_date=start, end_date=end, count=count)

        days = await self._run_guarded_qmt_call(_call)
        return {"dtype": "list", "values": [str(day) for day in days]}

    async def get_security_info(self, payload: Dict) -> Dict:
        security = payload.get("security")

        def _call():
            return self.provider.get_security_info(security)

        info = await self._run_guarded_qmt_call(_call)
        return dict_payload(info or {})

    async def ensure_cache(self, payload: Dict) -> Dict:
        security = payload.get("security") or payload.get("stockcode")
        frequency = payload.get("frequency") or payload.get("period") or "1m"
        start = payload.get("start") or payload.get("start_date") or payload.get("start_time")
        end = payload.get("end") or payload.get("end_date") or payload.get("end_time")
        auto = _to_bool(payload.get("auto_download", True), True)
        result = await self._run_guarded_qmt_call(
            self.provider.ensure_cache,
            security,
            frequency,
            start,
            end,
            auto_download=auto,
            count=payload.get("count"),
        )
        return {"dtype": "dict", "value": result or {}}

    async def get_current_tick(self, symbol: str) -> Optional[Dict]:
        return await self._run_guarded_qmt_call(self.provider.get_current_tick, symbol)

    async def get_all_securities(self, payload: Dict) -> Dict:
        types = payload.get("types") or "stock"
        date = payload.get("date")

        def _call():
            return self.provider.get_all_securities(types=types, date=date)

        df = await self._run_guarded_qmt_call(_call)
        if df is not None and hasattr(df, "copy") and hasattr(df, "columns"):
            if "code" not in getattr(df, "columns", []):
                df = df.copy()
                try:
                    df.index.name = "code"
                except Exception:
                    pass
        return dataframe_to_payload(df)

    async def get_index_stocks(self, payload: Dict) -> Dict:
        index_symbol = payload.get("index_symbol")
        date = payload.get("date")

        def _call():
            return self.provider.get_index_stocks(index_symbol, date=date)

        stocks = await self._run_guarded_qmt_call(_call)
        return {"values": stocks or []}

    async def get_split_dividend(self, payload: Dict) -> Dict:
        security = payload.get("security")
        start = payload.get("start")
        end = payload.get("end")

        def _call():
            return self.provider.get_split_dividend(security, start_date=start, end_date=end)

        events = await self._run_guarded_qmt_call(_call)
        return {"events": events or []}


class QmtBrokerAdapter(RemoteBrokerAdapter):
    """
    QMT 券商适配器，处理远程下单请求。

    下单时会进行以下预处理（与 LiveEngine 行为一致）：
    - 最小手数与步进规则取整（按配置）
    - 停牌检查
    - 涨跌停价格校验
    - 市价单价格笼子计算
    - 卖出时可卖数量检查
    """

    def __init__(
        self,
        config: ServerConfig,
        account_router: AccountRouter,
        guard: Optional[QmtAvailabilityGuard] = None,
    ):
        """初始化 QMT broker 适配器。

        Args:
            config: 远程服务配置。
            account_router: 账户路由器。
            guard: 共享 QMT 可用性保护器。

        Returns:
            None。
        """

        self.config = config
        self.account_router = account_router
        self._brokers: Dict[str, QmtBroker] = {}
        # 用于获取实时行情的 provider
        self._data_provider: Optional[MiniQMTProvider] = None
        self.guard = guard or QmtAvailabilityGuard(name="qmt-broker")
        self._reconnect_task: Optional[asyncio.Task] = None
        self._stopping = False

    async def start(self) -> None:
        """启动 broker 适配器并安排后台 QMT 重连探针。

        Args:
            None。

        Returns:
            None。

        Side Effects:
            创建各账户 broker 对象，但不在服务启动路径里阻塞等待 QMT 连接。
        """

        # 初始化数据 provider 用于获取实时行情
        self._data_provider = MiniQMTProvider(_provider_config())

        for ctx in self.account_router.list_accounts():
            broker = QmtBroker(
                account_id=ctx.config.account_id,
                account_type=ctx.config.account_type,
                data_path=ctx.config.data_path,
                session_id=ctx.config.session_id,
                auto_subscribe=ctx.config.auto_subscribe,
            )
            self._brokers[ctx.config.key] = broker

        if self._brokers:
            self.guard.schedule_probe_now("等待 QMT 初始连接")
            self._reconnect_task = asyncio.create_task(
                self._reconnect_loop(),
                name="qmt-broker-reconnect",
            )
        elif not self.config.enable_data:
            self.guard.mark_disabled("未配置 QMT broker 账户")

    async def stop(self) -> None:
        """停止 broker 适配器并清理 QMT 连接。

        Args:
            None。

        Returns:
            None。

        Side Effects:
            取消后台重连任务，断开现有 broker，并关闭 QMT 专用线程池。
        """

        self._stopping = True
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None
        for broker in self._brokers.values():
            try:
                await _run_in_qmt_executor(broker.disconnect)
            except Exception:
                pass
        # 关闭专用线程池（不等待，避免阻塞）
        _shutdown_executor(wait=False)

    async def _reconnect_loop(self) -> None:
        """后台重连探针循环。

        Args:
            None。

        Returns:
            None。

        Side Effects:
            按 guard cooldown 执行单飞连接尝试，并更新 QMT readiness。
        """

        poll = self.guard.config.ready_poll_seconds
        while not self._stopping:
            try:
                if self.guard.ready:
                    if self._all_brokers_connected():
                        await asyncio.sleep(poll)
                        continue
                    self.guard.mark_failure(RuntimeError("QMT broker 已断开"), delay=0)

                wait_seconds = self.guard.seconds_until_probe()
                if wait_seconds > 0:
                    await asyncio.sleep(min(wait_seconds, poll))
                    continue

                acquired = await self.guard.acquire_probe()
                if not acquired:
                    await asyncio.sleep(poll)
                    continue
                try:
                    await self._connect_all_brokers()
                except Exception as exc:
                    await self._disconnect_all_brokers()
                    self.guard.mark_failure(exc)
                    log.warning("QMT broker 重连失败: %s", exc)
                else:
                    self.guard.mark_ready()
                    log.info("QMT broker 已恢复 ready")
                finally:
                    self.guard.release_probe()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.guard.mark_failure(exc)
                log.warning("QMT broker 重连循环异常: %s", exc)
                await asyncio.sleep(poll)

    async def _connect_all_brokers(self) -> None:
        """连接全部已配置 QMT broker。

        Args:
            None。

        Returns:
            None。

        Raises:
            Exception: 任一账户连接失败时抛出原始异常。

        Side Effects:
            成功后把 broker handle 重新挂到账户路由器。
        """

        for ctx in self.account_router.list_accounts():
            key = ctx.config.key
            broker = self._brokers.get(key)
            if broker is None:
                broker = QmtBroker(
                    account_id=ctx.config.account_id,
                    account_type=ctx.config.account_type,
                    data_path=ctx.config.data_path,
                    session_id=ctx.config.session_id,
                    auto_subscribe=ctx.config.auto_subscribe,
                )
                self._brokers[key] = broker
            try:
                await _run_in_qmt_executor(broker.disconnect)
            except Exception:
                pass
            await _run_in_qmt_executor(broker.connect)
            await self.account_router.attach_handle(key, broker)

    async def _disconnect_all_brokers(self) -> None:
        """断开全部 QMT broker。

        Args:
            None。

        Returns:
            None。

        Side Effects:
            尽力释放所有已创建的 broker 连接。
        """

        for broker in self._brokers.values():
            try:
                await _run_in_qmt_executor(broker.disconnect)
            except Exception:
                pass

    def _all_brokers_connected(self) -> bool:
        """判断所有 broker 是否仍处于连接态。

        Args:
            None。

        Returns:
            bool: 全部 broker 连接时为 True。
        """

        if not self._brokers:
            return False
        for broker in self._brokers.values():
            is_connected = getattr(broker, "is_connected", None)
            if is_connected is False:
                return False
            if callable(is_connected):
                try:
                    if not bool(is_connected()):
                        return False
                except Exception:
                    return False
        return True

    def qmt_status(self) -> Dict[str, Any]:
        """返回 QMT broker 适配器的 health 快照。

        Args:
            None。

        Returns:
            Dict[str, Any]: 包含 guard 状态和账户连接状态的快照。
        """

        snapshot = self.guard.snapshot()
        accounts = {}
        for key, broker in self._brokers.items():
            is_connected = getattr(broker, "is_connected", None)
            if callable(is_connected):
                try:
                    connected = bool(is_connected())
                except Exception:
                    connected = False
            elif is_connected is None:
                connected = True
            else:
                connected = bool(is_connected)
            accounts[key] = {"connected": connected}
        snapshot["accounts"] = accounts
        return snapshot

    def _broker_for(self, ctx: AccountContext) -> QmtBroker:
        self.guard.ensure_ready()
        broker = self._brokers.get(ctx.config.key)
        if not broker:
            raise QmtUnavailableError(
                f"QMT broker account {ctx.config.key} 未连接",
                state=self.guard.state,
                retry_after_seconds=self.guard.seconds_until_probe(),
            )
        is_connected = getattr(broker, "is_connected", None)
        if is_connected is False:
            self.guard.mark_failure(
                RuntimeError(f"QMT broker account {ctx.config.key} 已断开"), delay=0
            )
            raise self.guard.build_error()
        if callable(is_connected):
            try:
                connected = bool(is_connected())
            except Exception as exc:
                self.guard.mark_failure(exc, delay=0)
                raise self.guard.build_error() from exc
            if not connected:
                self.guard.mark_failure(
                    RuntimeError(f"QMT broker account {ctx.config.key} 已断开"), delay=0
                )
                raise self.guard.build_error()
        return broker

    async def get_account_info(
        self, account: AccountContext, payload: Optional[Dict] = None
    ) -> Dict:
        broker = self._broker_for(account)
        info = await _run_in_qmt_executor(broker.get_account_info)
        return {"dtype": "dict", "value": info}

    async def get_positions(
        self, account: AccountContext, payload: Optional[Dict] = None
    ) -> List[Dict]:
        broker = self._broker_for(account)
        positions = await _run_in_qmt_executor(broker.get_positions)
        return positions or []

    async def list_orders(
        self, account: AccountContext, filters: Optional[Dict] = None
    ) -> List[Dict]:
        broker = self._broker_for(account)
        order_id = filters.get("order_id") if filters else None
        security = filters.get("security") if filters else None
        status = filters.get("status") if filters else None
        from_broker = bool(filters.get("from_broker")) if filters else False
        getter = getattr(broker, "get_orders", None)
        if getter:
            orders = await _run_in_qmt_executor(
                lambda: getter(
                    order_id=order_id,
                    security=security,
                    status=status,
                    from_broker=from_broker,
                )
            )
            return orders or []
        getter = getattr(broker, "get_open_orders", None)
        if getter:
            orders = await _run_in_qmt_executor(getter)
            if not orders:
                return []
            if order_id:
                orders = [row for row in orders if str(row.get("order_id")) == str(order_id)]
            if security:
                orders = [row for row in orders if row.get("security") == security]
            if status is not None:
                status_val = getattr(status, "value", status)
                orders = [row for row in orders if str(row.get("status")) == str(status_val)]
            return orders
        return []

    async def list_trades(
        self, account: AccountContext, filters: Optional[Dict] = None
    ) -> List[Dict]:
        broker = self._broker_for(account)
        order_id = filters.get("order_id") if filters else None
        security = filters.get("security") if filters else None
        getter = getattr(broker, "get_trades", None)
        if getter:
            trades = await _run_in_qmt_executor(
                lambda: getter(order_id=order_id, security=security)
            )
            return trades or []
        return []

    async def get_order_status(
        self,
        account: AccountContext,
        order_id: Optional[str] = None,
        payload: Optional[Dict] = None,
    ) -> Dict:
        if not order_id and payload:
            order_id = payload.get("order_id")
        if not order_id:
            raise ValueError("缺少 order_id")
        broker = self._broker_for(account)
        status = await broker.get_order_status(order_id)
        return status or {}

    async def place_order(self, account: AccountContext, payload: Dict) -> Dict:
        """
        下单接口，统一处理以下逻辑（与 LiveEngine 行为一致）：
        1. 获取实时行情（停牌检查、最新价、涨跌停价）
        2. 最小手数/步进规则取整
        3. 价格校验（限价单在涨跌停范围内）
        4. 市价单价格笼子计算
        5. 卖出时可卖数量检查
        """
        import logging
        from bullet_trade.core import pricing
        from bullet_trade.utils.env_loader import get_live_trade_config

        logger = logging.getLogger(__name__)
        broker = self._broker_for(account)
        security = payload["security"]
        raw_amount = int(payload.get("amount") or payload.get("volume") or 0)
        side = payload.get("side", "BUY").upper()
        remark = payload.get("order_remark") or payload.get("remark")
        style = payload.get("style") or {"type": "limit"}
        style_type = (style.get("type") or "limit").lower()
        is_market = style_type == "market"
        is_buy = side == "BUY"

        # ========== 1. 获取实时行情 ==========
        snapshot = await self._get_live_snapshot(security)
        last_price = float(snapshot.get("last_price") or 0.0)
        high_limit = snapshot.get("high_limit")
        low_limit = snapshot.get("low_limit")
        paused = snapshot.get("paused", False)

        # 停牌检查
        if paused:
            raise ValueError(f"{security} 停牌，无法下单")

        # 如果没有获取到价格，尝试用涨跌停价格
        if last_price <= 0:
            fallback = high_limit if is_buy else low_limit
            if fallback and fallback > 0:
                last_price = float(fallback)
                logger.warning(f"{security} 缺少最新价，使用{'涨停价' if is_buy else '跌停价'} {last_price} 作为参考")
            else:
                raise ValueError(f"{security} 无法获取有效价格")

        # ========== 2. 最小手数与步进取整 ==========
        closeable = None
        if not is_buy:
            positions = await self.get_positions(account)
            closeable = 0
            for pos in positions:
                if pos.get("security") == security:
                    closeable = int(
                        pos.get("closeable_amount")
                        or pos.get("available")
                        or pos.get("amount")
                        or 0
                    )
                    break
            if closeable <= 0:
                raise ValueError(f"{security} 无可卖数量")

        amount = pricing.adjust_order_amount(security, raw_amount, is_buy, closeable=closeable)
        if amount <= 0:
            min_lot, step = pricing.infer_lot_rule(security)
            raise ValueError(f"{security} 数量不足最小手数（原始数量={raw_amount}，最小手数={min_lot}，步进={step}）")
        if amount != raw_amount:
            logger.info(f"{security} 数量从 {raw_amount} 取整为 {amount}")

        # ========== 4. 价格处理 ==========
        requested_price = style.get("price")
        if requested_price in (None, ""):
            requested_price = style.get("protect_price")
        if requested_price in (None, ""):
            requested_price = payload.get("price")
        price = requested_price

        if is_market:
            if price not in (None, ""):
                requested_market_price = float(price)
                price = _clamp_market_protect_price(
                    security,
                    requested_market_price,
                    last_price,
                    high_limit,
                    low_limit,
                    is_buy,
                )
                if abs(price - requested_market_price) > 1e-9:
                    logger.warning(
                        f"{security} 客户端保护价 {requested_market_price:.4f} "
                        f"超出当前价格笼子/涨跌停，调整为 {price:.4f}"
                    )
                logger.info(f"{security} 市价单沿用客户端保护价: {price:.4f} " f"（基准价={last_price:.4f}）")
            else:
                live_cfg = get_live_trade_config()
                buy_percent = float(live_cfg.get("market_buy_price_percent", 0.015))
                sell_percent = float(live_cfg.get("market_sell_price_percent", -0.015))
                percent = buy_percent if is_buy else sell_percent

                price = pricing.compute_market_protect_price(
                    security,
                    last_price,
                    high_limit,
                    low_limit,
                    percent,
                    is_buy,
                )
                logger.info(
                    f"{security} 市价单保护价: {price:.4f} "
                    f"（基准价={last_price:.4f}, 比例={percent*100:.2f}%）"
                )
        else:
            # 限价单：校验价格是否在涨跌停范围内
            if price is None:
                raise ValueError("限价单缺少委托价格，请在 style.price 中提供")
            price = float(price)

            # 涨跌停校验
            if high_limit and price > float(high_limit):
                logger.warning(f"{security} 限价 {price} 超过涨停价 {high_limit}，调整为涨停价")
                price = float(high_limit)
            if low_limit and price < float(low_limit):
                logger.warning(f"{security} 限价 {price} 低于跌停价 {low_limit}，调整为跌停价")
                price = float(low_limit)

        # ========== 5. 下单 ==========
        logger.info(
            f"执行下单: {security} {'买入' if is_buy else '卖出'} {amount} 股，价格={price:.4f}，市价单={is_market}"
        )

        if is_buy:
            order = await broker.buy(
                security,
                amount,
                price,
                wait_timeout=payload.get("wait_timeout"),
                remark=remark,
                market=is_market,
            )
        else:
            order = await broker.sell(
                security,
                amount,
                price,
                wait_timeout=payload.get("wait_timeout"),
                remark=remark,
                market=is_market,
            )

        if isinstance(order, str):
            response = {
                "order_id": order,
                "status": "submitted",
                "amount": amount,
                "price": price,
                "order_price": price,
                "requested_order_price": price,
            }
            wait_result_reader = getattr(broker, "get_last_order_wait_result", None)
            if callable(wait_result_reader):
                try:
                    wait_result = wait_result_reader(order)
                except Exception:
                    wait_result = None
                if isinstance(wait_result, dict):
                    response.update(wait_result)
            return response
        result = order or {}
        result["amount"] = amount
        result["price"] = price
        result.setdefault("order_price", price)
        result.setdefault("requested_order_price", price)
        return result

    async def _get_live_snapshot(self, security: str) -> Dict[str, Any]:
        """
        获取实时行情快照，包含 last_price, high_limit, low_limit, paused 等字段。
        """
        self.guard.ensure_ready()
        if not self._data_provider:
            return {}

        def _call():
            return self._data_provider.get_live_current(security)

        try:
            snapshot = await _run_in_qmt_executor(_call)
            return snapshot or {}
        except Exception as e:
            import logging

            logging.getLogger(__name__).warning(f"获取 {security} 实时行情失败: {e}")
            if is_qmt_connectivity_error(e):
                self.guard.mark_failure(e)
                raise QmtUnavailableError(
                    f"QMT 实时行情服务不可用: {e}",
                    state=self.guard.state,
                    retry_after_seconds=self.guard.seconds_until_probe(),
                ) from e
            return {}

    async def cancel_order(
        self,
        account: AccountContext,
        order_id: Optional[str] = None,
        payload: Optional[Dict] = None,
    ) -> Dict:
        if not order_id and payload:
            order_id = payload.get("order_id")
        if not order_id:
            raise ValueError("缺少 order_id")
        broker = self._broker_for(account)
        ok = await broker.cancel_order(order_id)
        response: Dict[str, Any] = {"dtype": "dict", "value": bool(ok)}
        if not ok:
            response["timed_out"] = False
            return response
        from bullet_trade.utils.env_loader import get_live_trade_config

        wait_s = get_live_trade_config().get("trade_max_wait_time", 16)
        try:
            wait_s = float(wait_s)
        except (TypeError, ValueError):
            wait_s = 16.0
        if wait_s <= 0:
            response["timed_out"] = True
            return response

        deadline = time.monotonic() + wait_s
        interval = 0.5
        last_snapshot: Optional[Dict[str, Any]] = None
        final_snapshot: Optional[Dict[str, Any]] = None
        while time.monotonic() < deadline:
            try:
                status = await broker.get_order_status(order_id)
            except Exception:
                status = None
            if status:
                last_snapshot = status
                st = str(status.get("status") or "").lower()
                if st in (
                    "filled",
                    "cancelled",
                    "canceled",
                    "partly_canceled",
                    "rejected",
                    "failed",
                    "error",
                ):
                    final_snapshot = status
                    break
            await asyncio.sleep(interval)

        snapshot = final_snapshot or last_snapshot
        if snapshot:
            response["status"] = snapshot.get("status")
            response["raw_status"] = snapshot.get("raw_status")
            response["last_snapshot"] = snapshot
        response["timed_out"] = final_snapshot is None
        return response


_PRICE_FIELD_NAMES = {
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "amount",
    "avg",
    "price",
    "highlimit",
    "high_limit",
    "lowlimit",
    "low_limit",
    "paused",
    "preclose",
    "pre_close",
    "suspendflag",
    "suspend_flag",
    "openinterest",
    "open_interest",
    "settlementprice",
    "settelementprice",
}


def _price_field_tokens(values) -> Set[str]:
    return {str(value).replace(" ", "").replace("_", "").lower() for value in values}


def _normalise_price_multiindex_columns(columns: pd.MultiIndex) -> pd.MultiIndex:
    if columns.nlevels != 2:
        return columns
    level0 = _price_field_tokens(columns.get_level_values(0))
    level1 = _price_field_tokens(columns.get_level_values(1))
    if (level1 & _PRICE_FIELD_NAMES) and not (level0 & _PRICE_FIELD_NAMES):
        columns = columns.swaplevel(0, 1)
        columns.names = ["field", "code"]
    elif (level0 & _PRICE_FIELD_NAMES) and not (level1 & _PRICE_FIELD_NAMES):
        columns.names = ["field", "code"]
    return columns


def dataframe_to_payload(df):
    if df is None:
        return {"dtype": "dataframe", "columns": [], "records": []}

    def _coerce_value(value):
        if value is None:
            return None
        try:
            import pandas as pd

            if value is pd.NaT:
                return None
            if isinstance(value, pd.Timestamp):
                return value.isoformat()
        except Exception:
            pass
        try:
            from datetime import datetime, date as Date

            if isinstance(value, (datetime, Date)):
                return value.isoformat()
        except Exception:
            pass
        try:
            if hasattr(value, "item"):
                return value.item()
        except Exception:
            pass
        return value

    metadata: Dict[str, Any] = {}
    try:
        if isinstance(getattr(df, "columns", None), pd.MultiIndex):
            df = df.copy()
            df.columns = _normalise_price_multiindex_columns(df.columns)
            metadata["column_tuples"] = [
                [_coerce_value(item) for item in col] for col in df.columns.tolist()
            ]
            metadata["column_index_names"] = [
                _coerce_value(name) for name in (df.columns.names or [])
            ]
        columns = list(df.columns)
        raw = df.reset_index().values.tolist() if df.index.name else df.values.tolist()
        records = [[_coerce_value(v) for v in row] for row in raw]
    except Exception:
        columns = getattr(df, "columns", [])
        raw = getattr(df, "values", [])
        records = [[_coerce_value(v) for v in row] for row in raw]
    payload = {
        "dtype": "dataframe",
        "columns": [str(col) for col in columns],
        "records": records,
    }
    payload.update(metadata)
    return payload


def dict_payload(value: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = {
        "dtype": "dict",
        "value": value or {},
    }
    if isinstance(value, dict):
        for key, item in value.items():
            if key not in payload and item is not None:
                payload[key] = item
    return payload


def build_qmt_bundle(config: ServerConfig, router: AccountRouter) -> AdapterBundle:
    guard = QmtAvailabilityGuard(config=load_qmt_guard_config(), name="qmt-server")
    data_adapter = (
        QmtDataAdapter(guard, allow_request_probe=not config.enable_broker)
        if config.enable_data
        else None
    )
    broker_adapter = QmtBrokerAdapter(config, router, guard=guard) if config.enable_broker else None
    if not config.enable_data and not config.enable_broker:
        guard.mark_disabled("QMT data/broker 均未启用")
    return AdapterBundle(data_adapter=data_adapter, broker_adapter=broker_adapter)


register_adapter("qmt", build_qmt_bundle)
