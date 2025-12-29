from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Any, Dict, List, Optional

from bullet_trade.broker.qmt import QmtBroker
from bullet_trade.core.globals import log
from bullet_trade.data.providers.miniqmt import MiniQMTProvider
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


class QmtDataAdapter(RemoteDataAdapter):
    def __init__(self) -> None:
        self.provider = MiniQMTProvider(_provider_config())

    async def get_history(self, payload: Dict) -> Dict:
        """
        获取历史 K 线数据。
        
        :param payload: 包含 security, count, start, end, frequency, fq 等参数
        :return: DataFrame 转换后的 payload 字典
        """
        import traceback
        import logging
        logger = logging.getLogger(__name__)
        
        security = payload.get("security")
        count = payload.get("count")
        start = payload.get("start")
        end = payload.get("end")
        frequency = payload.get("frequency") or payload.get("period")
        fq = payload.get("fq")
        
        logger.debug(f"[QmtDataAdapter.get_history] 请求参数: security={security}, count={count}, "
                     f"start={start}, end={end}, frequency={frequency}, fq={fq}")

        def _call():
            return self.provider.get_price(
                security,
                count=count,
                start_date=start,
                end_date=end,
                frequency=frequency,
                fq=fq,
            )

        try:
            df = await _run_in_qmt_executor(_call)
            logger.debug(f"[QmtDataAdapter.get_history] 返回数据: shape={df.shape if df is not None else None}, "
                         f"columns={list(df.columns) if df is not None and hasattr(df, 'columns') else None}")
            return dataframe_to_payload(df)
        except KeyError as e:
            # KeyError 通常表示数据格式问题（如缺少 time 列）
            error_msg = f"数据格式错误，缺少字段 {e}: security={security}, frequency={frequency}"
            logger.error(f"[QmtDataAdapter.get_history] {error_msg}\n{traceback.format_exc()}")
            raise RuntimeError(error_msg) from e
        except Exception as e:
            # 捕获所有其他异常并添加上下文信息
            error_msg = f"获取历史数据失败: {type(e).__name__}: {e} (security={security}, frequency={frequency})"
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
            tick = await _run_in_qmt_executor(_call)
            logger.debug(f"[QmtDataAdapter.get_snapshot] 返回数据: {tick}")
            return tick or {}
        except Exception as e:
            error_msg = f"获取快照数据失败: {type(e).__name__}: {e} (security={security})"
            logger.error(f"[QmtDataAdapter.get_snapshot] {error_msg}\n{traceback.format_exc()}")
            raise RuntimeError(error_msg) from e

    async def get_live_current(self, payload: Dict) -> Dict:
        """返回实盘快照（含停牌标记）。"""
        security = payload.get("security")

        def _call():
            return self.provider.get_live_current(security)

        tick = await _run_in_qmt_executor(_call)
        return tick or {}

    async def get_trade_days(self, payload: Dict) -> Dict:
        start = payload.get("start")
        end = payload.get("end")

        def _call():
            return self.provider.get_trade_days(start_date=start, end_date=end)

        days = await _run_in_qmt_executor(_call)
        return {"dtype": "list", "values": [str(day) for day in days]}

    async def get_security_info(self, payload: Dict) -> Dict:
        security = payload.get("security")

        def _call():
            return self.provider.get_security_info(security)

        info = await _run_in_qmt_executor(_call)
        return {"dtype": "dict", "value": info or {}}

    async def ensure_cache(self, payload: Dict) -> Dict:
        security = payload.get("security")
        frequency = payload.get("frequency") or payload.get("period") or "1m"
        start = payload.get("start")
        end = payload.get("end")
        auto = bool(payload.get("auto_download", True))
        result = await _run_in_qmt_executor(
            self.provider.ensure_cache,
            security,
            frequency,
            start,
            end,
            auto_download=auto,
        )
        return {"dtype": "dict", "value": result or {}}

    async def get_current_tick(self, symbol: str) -> Optional[Dict]:
        return await _run_in_qmt_executor(self.provider.get_current_tick, symbol)

    async def get_all_securities(self, payload: Dict) -> Dict:
        types = payload.get("types") or "stock"
        date = payload.get("date")

        def _call():
            return self.provider.get_all_securities(types=types, date=date)

        df = await _run_in_qmt_executor(_call)
        return dataframe_to_payload(df)

    async def get_index_stocks(self, payload: Dict) -> Dict:
        index_symbol = payload.get("index_symbol")
        date = payload.get("date")

        def _call():
            return self.provider.get_index_stocks(index_symbol, date=date)

        stocks = await _run_in_qmt_executor(_call)
        return {"values": stocks or []}

    async def get_split_dividend(self, payload: Dict) -> Dict:
        security = payload.get("security")
        start = payload.get("start")
        end = payload.get("end")

        def _call():
            return self.provider.get_split_dividend(security, start_date=start, end_date=end)

        events = await _run_in_qmt_executor(_call)
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
    
    def __init__(self, config: ServerConfig, account_router: AccountRouter):
        self.config = config
        self.account_router = account_router
        self._brokers: Dict[str, QmtBroker] = {}
        # 用于获取实时行情的 provider
        self._data_provider: Optional[MiniQMTProvider] = None

    async def start(self) -> None:
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
            await _run_in_qmt_executor(broker.connect)
            self._brokers[ctx.config.key] = broker
            await self.account_router.attach_handle(ctx.config.key, broker)

    async def stop(self) -> None:
        for broker in self._brokers.values():
            try:
                await _run_in_qmt_executor(broker.disconnect)
            except Exception:
                pass
        # 关闭专用线程池（不等待，避免阻塞）
        _shutdown_executor(wait=False)

    def _broker_for(self, ctx: AccountContext) -> QmtBroker:
        broker = self._brokers.get(ctx.config.key)
        if not broker:
            raise RuntimeError(f"account {ctx.config.key} not connected")
        return broker

    async def get_account_info(self, account: AccountContext, payload: Optional[Dict] = None) -> Dict:
        broker = self._broker_for(account)
        info = await _run_in_qmt_executor(broker.get_account_info)
        return {"dtype": "dict", "value": info}

    async def get_positions(self, account: AccountContext, payload: Optional[Dict] = None) -> List[Dict]:
        broker = self._broker_for(account)
        positions = await _run_in_qmt_executor(broker.get_positions)
        return positions or []

    async def list_orders(self, account: AccountContext, filters: Optional[Dict] = None) -> List[Dict]:
        broker = self._broker_for(account)
        getter = getattr(broker, "get_open_orders", None)
        if getter:
            orders = await _run_in_qmt_executor(getter)
            return orders or []
        return []

    async def get_order_status(
        self, account: AccountContext, order_id: Optional[str] = None, payload: Optional[Dict] = None
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
                    closeable = int(pos.get("closeable_amount") or pos.get("available") or pos.get("amount") or 0)
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
        price = style.get("price")
        
        if is_market:
            # 市价单：服务端统一计算价格笼子，忽略客户端传入的 protect_price
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
            logger.info(f"{security} 市价单保护价: {price:.4f}（基准价={last_price:.4f}, 比例={percent*100:.2f}%）")
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
        logger.info(f"执行下单: {security} {'买入' if is_buy else '卖出'} {amount} 股，价格={price:.4f}，市价单={is_market}")
        
        if is_buy:
            order = await broker.buy(security, amount, price, wait_timeout=payload.get("wait_timeout"), market=is_market)
        else:
            order = await broker.sell(security, amount, price, wait_timeout=payload.get("wait_timeout"), market=is_market)
        
        if isinstance(order, str):
            return {"order_id": order, "amount": amount, "price": price}
        result = order or {}
        result["amount"] = amount
        result["price"] = price
        return result
    
    async def _get_live_snapshot(self, security: str) -> Dict[str, Any]:
        """
        获取实时行情快照，包含 last_price, high_limit, low_limit, paused 等字段。
        """
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
            return {}

    async def cancel_order(
        self, account: AccountContext, order_id: Optional[str] = None, payload: Optional[Dict] = None
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
                if st in ("filled", "cancelled", "canceled", "partly_canceled", "rejected"):
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

    try:
        columns = list(df.columns)
        raw = df.reset_index().values.tolist() if df.index.name else df.values.tolist()
        records = [[_coerce_value(v) for v in row] for row in raw]
    except Exception:
        columns = getattr(df, "columns", [])
        raw = getattr(df, "values", [])
        records = [[_coerce_value(v) for v in row] for row in raw]
    return {
        "dtype": "dataframe",
        "columns": [str(col) for col in columns],
        "records": records,
    }


def build_qmt_bundle(config: ServerConfig, router: AccountRouter) -> AdapterBundle:
    data_adapter = QmtDataAdapter() if config.enable_data else None
    broker_adapter = QmtBrokerAdapter(config, router) if config.enable_broker else None
    return AdapterBundle(data_adapter=data_adapter, broker_adapter=broker_adapter)


register_adapter("qmt", build_qmt_bundle)
