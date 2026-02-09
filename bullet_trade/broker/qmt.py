"""
QMT 券商适配（最小实现）

说明：
- 仅提供接口骨架与连接可用性检查；
- 真正的下单/撤单/查询在具备 xtquant 环境时逐步完善；
- 在无 xtquant 环境调用会抛出明确异常，避免误用。
"""

import asyncio
import inspect
import hashlib
from typing import Dict, List, Optional, Any
from datetime import datetime
import time

from .base import BrokerBase
from bullet_trade.core.models import OrderStatus
from bullet_trade.core.globals import log
from bullet_trade.utils.env_loader import get_broker_config, get_live_trade_config


class QmtBroker(BrokerBase):
    """
    QMT 券商适配（基于 xtquant/xttrader）。
    
    当前实现重点：
    - connect: 读取 QMT_DATA_PATH 等配置，启动 XtQuantTrader 并完成连接/订阅；
    - buy/sell/cancel/order_status：保留最小骨架，等待后续补齐真实交易调用；
    - 其余方法仍为占位，确保在缺少完整环境时也能给出清晰错误提示。
    """

    def __init__(
        self,
        account_id: str,
        account_type: str = "stock",
        data_path: Optional[str] = None,
        session_id: Optional[int] = None,
        auto_subscribe: Optional[bool] = None,
    ):
        super().__init__(account_id, account_type)
        cfg = get_broker_config().get("qmt", {})
        self._xt_imported = False
        self._xt_trader = None
        self._xt_account = None
        self._xt_callback = None
        self._data_path = data_path or cfg.get("data_path")
        raw_session = session_id if session_id is not None else cfg.get("session_id")
        self._session_id: Optional[int] = None
        if raw_session not in (None, ""):
            try:
                self._session_id = int(raw_session)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                log.warning(f"QMT session_id 无法解析为整数 {raw_session}，将使用自动生成的时间戳")
        cfg_auto = cfg.get("auto_subscribe")
        if auto_subscribe is not None:
            self._auto_subscribe = bool(auto_subscribe)
        elif cfg_auto is not None:
            self._auto_subscribe = bool(cfg_auto)
        else:
            self._auto_subscribe = True
        live_cfg = get_live_trade_config()
        self._market_buy_percent = float(live_cfg.get("market_buy_price_percent", 0.015))
        self._market_sell_percent = float(live_cfg.get("market_sell_price_percent", -0.015))
        retry_flag = cfg.get("connect_retry")
        self._retry_on_failure = True if retry_flag is None else bool(retry_flag)
        retry_interval = cfg.get("connect_retry_interval")
        try:
            self._retry_interval = max(1, int(retry_interval)) if retry_interval is not None else 60
        except (TypeError, ValueError):
            self._retry_interval = 60

    def _ensure_connected(self):
        if not self._connected:
            raise RuntimeError(f"QMT 未连接，请先调用 connect() 并确保 xtquant 环境可用, account_id: {self.account_id}, account_type: {self.account_type}")

    def connect(self) -> bool:
        data_path = self._data_path
        if not data_path:
            raise RuntimeError("缺少 QMT 数据目录，请在 .env 中设置 QMT_DATA_PATH，或在实例化 QmtBroker 时传入 data_path。")

        attempt = 0
        while True:
            attempt += 1
            try:
                from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback  # type: ignore
                from xtquant.xttype import StockAccount  # type: ignore
            except Exception as e:
                raise RuntimeError(
                    f"未检测到 xtquant 环境或导入失败，请在 Windows 安装 QMT/xtquant 并配置 Python 依赖, account_id: {self.account_id}, account_type: {self.account_type}, data_path: {data_path}"
                ) from e

            self._xt_imported = True
            session_id = self._session_id or int(time.time() * 1000)
            self._session_id = session_id

            try:
                class _Callback(XtQuantTraderCallback):  # type: ignore
                    def __init__(self, outer: "QmtBroker"):
                        self.outer = outer
                    def on_disconnected(self):  # noqa: N802
                        self.outer._connected = False
                    # 其余回调后续接入（订单、成交、持仓变更）

                trader = XtQuantTrader(data_path, session_id)  # type: ignore
                callback = _Callback(self)
                trader.register_callback(callback)  # type: ignore

                try:
                    account = StockAccount(self.account_id, self.account_type.upper())  # type: ignore[arg-type]
                except TypeError:
                    account = StockAccount(self.account_id)  # type: ignore[arg-type,call-arg]

                start_ret = trader.start()  # type: ignore
                if start_ret not in (0, None):
                    raise RuntimeError(f"xtquant start() 返回异常状态: {start_ret}")

                connect_ret = trader.connect()  # type: ignore
                if connect_ret not in (0, None):
                    raise RuntimeError(f"xtquant connect() 失败，返回码: {connect_ret}")

                if self._auto_subscribe:
                    subscribe_ret = trader.subscribe(account)  # type: ignore
                    if subscribe_ret not in (0, None):
                        raise RuntimeError(f"xtquant subscribe() 失败，返回码: {subscribe_ret}")

            except Exception as exc:
                try:
                    if "trader" in locals():
                        stop_fn = getattr(locals()["trader"], "stop", None)
                        if callable(stop_fn):
                            stop_fn()
                except Exception:
                    pass
                if not self._retry_on_failure:
                    raise
                wait_s = self._retry_interval or 60
                log.error(f"QMT 连接失败（第 {attempt} 次）: {exc}，{wait_s}s 后重试 account_id: {self.account_id}, account_type: {self.account_type}, data_path: {data_path}")
                time.sleep(wait_s)
                continue

            self._xt_trader = trader  # type: ignore[name-defined]
            self._xt_account = account
            self._xt_callback = callback
            self._connected = True

            # 尝试读取账户与持仓快照并按 print_portfolio_info 风格打印
            try:
                # QMT 在刚连接后资产/持仓可能需要短暂刷新，这里稍等以提高命中率
                time.sleep(0.2)
                snap = self._build_account_snapshot()
                try:
                    from bullet_trade.utils.portfolio_printer import render_account_overview

                    overview = render_account_overview(snap, limit=20)
                    log.info("QMT 连接建立: account_id=%s, type=%s\n%s", self.account_id, self.account_type, overview)
                except Exception:
                    # 回退到简易行
                    cash = snap.get("available_cash")
                    total = snap.get("total_value")
                    poss = snap.get("positions") or []
                    log.info(
                        f"QMT 连接建立: account_id={self.account_id}, type={self.account_type}, 现金={cash}, 总资产={total}, 持仓{len(poss)}"
                    )
            except Exception:
                log.info(
                    f"QMT 连接建立: account_id={self.account_id}, type={self.account_type}"
                )

            return True

    def disconnect(self) -> bool:
        if self._xt_trader:
            try:
                disconnect_fn = getattr(self._xt_trader, "disconnect", None)
                if callable(disconnect_fn):
                    disconnect_fn()
            except Exception:
                pass
            try:
                stop_fn = getattr(self._xt_trader, "stop", None)
                if callable(stop_fn):
                    stop_fn()
            except Exception:
                pass

        self._connected = False
        self._xt_trader = None
        self._xt_account = None
        self._xt_callback = None
        return True

    def get_account_info(self) -> Dict[str, Any]:
        self._ensure_connected()
        snapshot = self._build_account_snapshot() or {}
        snapshot.setdefault("account_id", self.account_id)
        snapshot.setdefault("account_type", self.account_type)
        return snapshot

    def get_positions(self) -> List[Dict[str, Any]]:
        self._ensure_connected()
        # 与 _build_account_snapshot 对齐，返回已映射的持仓结构
        try:
            snapshot = self._build_account_snapshot()
            return snapshot.get("positions", [])  # type: ignore[return-value]
        except Exception:
            return []

    def get_open_orders(self) -> List[Dict[str, Any]]:
        self._ensure_connected()
        orders = self.sync_orders()
        open_states = {
            OrderStatus.new.value,
            OrderStatus.open.value,
            OrderStatus.filling.value,
            OrderStatus.canceling.value,
        }
        result: List[Dict[str, Any]] = []
        for item in orders:
            status = item.get("status")
            mapped = self._map_order_status(status)
            mapped_val = mapped.value if isinstance(mapped, OrderStatus) else mapped
            if mapped_val in open_states:
                row = dict(item)
                row["status"] = mapped_val
                row["raw_status"] = status
                if row.get("filled") is None:
                    row["filled"] = item.get("traded_volume")
                if row.get("price") is None:
                    row["price"] = item.get("traded_price")
                is_buy = row.get("is_buy")
                if is_buy is None:
                    is_buy = self._map_order_side(item.get("order_type"))
                if is_buy is not None:
                    row["is_buy"] = bool(is_buy)
                result.append(row)
        return result

    def get_orders(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
        status: Optional[object] = None,
        from_broker: bool = False,
    ) -> List[Dict[str, Any]]:
        _ = from_broker
        self._ensure_connected()
        orders = self.sync_orders()
        status_val: Optional[str] = None
        if status is not None:
            if isinstance(status, OrderStatus):
                status_val = status.value
            else:
                try:
                    status_val = OrderStatus(str(status)).value
                except Exception:
                    return []
        target_id = str(order_id) if order_id is not None else None
        result: List[Dict[str, Any]] = []
        for item in orders:
            oid = item.get("order_id")
            mapped_status = self._map_order_status(item.get("status"))
            mapped_val = mapped_status.value if isinstance(mapped_status, OrderStatus) else mapped_status
            if target_id and str(oid) != target_id:
                continue
            if security and item.get("security") != security:
                continue
            if status_val is not None and mapped_val != status_val:
                continue
            row = dict(item)
            row["status"] = mapped_val
            row["raw_status"] = item.get("status")
            if row.get("filled") is None:
                row["filled"] = item.get("traded_volume")
            if row.get("price") is None:
                row["price"] = item.get("traded_price")
            is_buy = row.get("is_buy")
            if is_buy is None:
                is_buy = self._map_order_side(item.get("order_type"))
            if is_buy is not None:
                row["is_buy"] = bool(is_buy)
            result.append(row)
        return result

    def get_trades(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        self._ensure_connected()
        if not self._xt_trader or not self._xt_account:
            return []
        trades = None
        for name in ("query_stock_trades", "get_stock_trades", "query_trades"):
            fn = getattr(self._xt_trader, name, None)
            if callable(fn):
                try:
                    trades = fn(self._xt_account)  # type: ignore[arg-type]
                except Exception:
                    trades = None
                break
        if not trades:
            return []
        if isinstance(trades, dict):
            iterable = [trades]
        else:
            iterable = list(trades)

        def _pick(obj: object, *names: str) -> Optional[Any]:
            if isinstance(obj, dict):
                for name in names:
                    if name in obj:
                        return obj.get(name)
                return None
            for name in names:
                if hasattr(obj, name):
                    return getattr(obj, name)
            return None

        target_id = str(order_id) if order_id is not None else None
        result: List[Dict[str, Any]] = []
        for item in iterable:
            oid = _pick(item, "order_id", "entrust_id")
            if target_id and str(oid) != target_id:
                continue
            raw_code = _pick(item, "stock_code", "code", "security")
            jq_code = self._map_to_jq_symbol(raw_code) if raw_code else None
            if security and jq_code != security:
                continue
            trade_id = _pick(item, "trade_id", "deal_no", "trade_no")
            price = _pick(item, "trade_price", "price")
            amount = _pick(item, "trade_volume", "volume", "amount")
            trade_time = _pick(item, "trade_time", "time")
            commission = _pick(item, "commission", "comm")
            tax = _pick(item, "tax", "stamp_tax")
            if not trade_id:
                base = f"{oid}-{trade_time}-{price}-{amount}"
                trade_id = hashlib.md5(base.encode("utf-8")).hexdigest()[:16]
            result.append(
                {
                    "trade_id": str(trade_id),
                    "order_id": str(oid) if oid is not None else "",
                    "security": jq_code,
                    "amount": int(amount or 0),
                    "price": float(price or 0.0),
                    "time": trade_time,
                    "commission": float(commission or 0.0),
                    "tax": float(tax or 0.0),
                }
            )
        return result

    async def buy(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        wait_timeout: Optional[float] = None,
        remark: Optional[str] = None,
        *,
        market: bool = False,
    ) -> str:
        """
        买入（支持按数量拆单 + 同步/异步等待骨架）。
        - ORDER_MAX_VOLUME 控制单笔最大数量，超过则拆分；
        - TRADE_MAX_WAIT_TIME>0 视为同步等待，=0 异步立即返回；
        实际下单需接入 xtquant 后实现 _send_order。
        """
        self._ensure_connected()
        if amount <= 0:
            raise ValueError("买入数量必须为正")
        chunks = self._split_volume(amount)
        mapped = self._map_security(security)
        order_ids: List[str] = []
        for qty in chunks:
            args = self._build_send_order_args(mapped, qty, price, "buy", market, remark)
            oid = await asyncio.to_thread(self._send_order, *args)
            order_ids.append(oid)
            await self._maybe_wait(oid, wait_timeout)
        log.info(f"下单(买入): {security} -> {mapped}, 总量={amount}, 笔数={len(chunks)}, 首单ID={order_ids[0] if order_ids else ''}")
        # 返回第一笔订单号（兼容聚宽风格），其余可通过内部记录获取
        return order_ids[0] if order_ids else ""

    async def sell(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        wait_timeout: Optional[float] = None,
        remark: Optional[str] = None,
        *,
        market: bool = False,
    ) -> str:
        """
        卖出（支持按数量拆单 + 同步/异步等待骨架）。
        实际下单需接入 xtquant 后实现 _send_order。
        """
        self._ensure_connected()
        if amount <= 0:
            raise ValueError("卖出数量必须为正")
        chunks = self._split_volume(amount)
        mapped = self._map_security(security)
        order_ids: List[str] = []
        for qty in chunks:
            args = self._build_send_order_args(mapped, qty, price, "sell", market, remark)
            oid = await asyncio.to_thread(self._send_order, *args)
            order_ids.append(oid)
            await self._maybe_wait(oid, wait_timeout)
        log.info(f"下单(卖出): {security} -> {mapped}, 总量={amount}, 笔数={len(chunks)}, 首单ID={order_ids[0] if order_ids else ''}")
        return order_ids[0] if order_ids else ""

    async def cancel_order(self, order_id: str) -> bool:
        self._ensure_connected()
        if not order_id:
            return False
        if not self._xt_trader or not self._xt_account:
            raise RuntimeError("QMT 交易对象未初始化")
        target_id: Any = order_id
        try:
            if isinstance(order_id, str) and order_id.strip().isdigit():
                target_id = int(order_id.strip())
        except Exception:
            target_id = order_id
        try:
            ok = await asyncio.to_thread(
                self._xt_trader.cancel_order_stock, self._xt_account, target_id  # type: ignore[arg-type]
            )
            log.info(f"撤单请求: order_id={order_id}, sent={target_id}, ok={ok}")
            # xtquant 返回 0/None/True 表示成功，-1/False/其它错误码为失败
            return ok in (0, None, True)
        except Exception as e:
            raise RuntimeError(f"QMT 撤单失败: {e}") from e

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        self._ensure_connected()
        if not order_id:
            return {}
        if not self._xt_trader or not self._xt_account:
            raise RuntimeError("QMT 交易对象未初始化")
        return await asyncio.to_thread(self._query_order_snapshot, order_id)

    def _query_order_snapshot(self, order_id: str) -> Dict[str, Any]:
        for name in ("query_stock_orders", "get_stock_orders", "query_orders"):
            fn = getattr(self._xt_trader, name, None)
            if not callable(fn):
                continue
            try:
                data = fn(self._xt_account)  # type: ignore[arg-type]
            except Exception:
                data = None
            if not data:
                continue
            try:
                if isinstance(data, dict):
                    items = [data]
                elif isinstance(data, (list, tuple)):
                    items = list(data)
                else:
                    try:
                        items = list(data)
                    except Exception:
                        items = []
                for it in items:
                    oid = None
                    status = None
                    code = None
                    price = None
                    qty = None
                    filled = None
                    traded_price = None
                    order_type = None
                    order_remark = None
                    strategy_name = None
                    if isinstance(it, dict):
                        oid = it.get("order_id") or it.get("orderId") or it.get("entrust_id")
                        status = it.get("order_status") or it.get("status")
                        code = it.get("stock_code") or it.get("code")
                        price = it.get("price")
                        qty = it.get("order_volume") or it.get("volume")
                        filled = it.get("traded_volume")
                        traded_price = it.get("traded_price")
                        order_type = it.get("order_type")
                        order_remark = it.get("order_remark") or it.get("remark")
                        strategy_name = it.get("strategy_name") or it.get("strategy")
                    else:
                        for attr in ("order_id", "orderId", "entrust_id"):
                            if hasattr(it, attr):
                                oid = getattr(it, attr)
                                break
                        for attr in ("order_status", "status"):
                            if hasattr(it, attr):
                                status = getattr(it, attr)
                                break
                        for attr in ("stock_code", "code"):
                            if hasattr(it, attr):
                                code = getattr(it, attr)
                                break
                        if hasattr(it, "price"):
                            price = getattr(it, "price")
                        for attr in ("order_volume", "volume"):
                            if hasattr(it, attr):
                                qty = getattr(it, attr)
                                break
                        if hasattr(it, "traded_volume"):
                            filled = getattr(it, "traded_volume")
                        if hasattr(it, "traded_price"):
                            traded_price = getattr(it, "traded_price")
                        if hasattr(it, "order_type"):
                            order_type = getattr(it, "order_type")
                        if hasattr(it, "order_remark"):
                            order_remark = getattr(it, "order_remark")
                        if hasattr(it, "strategy_name"):
                            strategy_name = getattr(it, "strategy_name")
                    if str(oid) == str(order_id):
                        mapped_status = self._map_order_status(status)
                        return {
                            "order_id": str(oid),
                            "status": mapped_status.value if isinstance(mapped_status, OrderStatus) else mapped_status,
                            "raw_status": status,
                            "security": code,
                            "price": traded_price or price,
                            "amount": qty,
                            "filled": filled,
                            "order_type": order_type,
                            "order_remark": order_remark,
                            "strategy_name": strategy_name,
                        }
            except Exception:
                continue
        return {}

    def _map_order_status(self, raw_status: Any) -> OrderStatus:
        """将 xtquant 的订单状态映射为内部 OrderStatus。
        - 对未知/处理中状态，统一映射为 OrderStatus.open
        - 完成→filled；撤单→canceled；拒绝→rejected
        """
        # 尝试整数/枚举码映射
        try:
            from xtquant import xtconstant  # type: ignore
            mapping = {}
            def g(name: str) -> Optional[int]:
                return getattr(xtconstant, name, None)

            # 新/待报/未知
            for nm in ("ORDER_UNREPORTED", "ORDER_WAIT_REPORT", "ORDER_WAIT_REPORTING", "ORDER_UNKNOWN"):
                val = g(nm)
                if val is not None:
                    mapping[val] = OrderStatus.new

            # 已报/排队中（在途）
            for nm in ("ORDER_REPORTED", "ORDER_QUEUEING"):
                val = g(nm)
                if val is not None:
                    mapping[val] = OrderStatus.open

            # 部分成交（在途）
            for nm in ("ORDER_PART_SUCC", "ORDER_PARTTRADED", "ORDER_PARTTRADED_QUEUEING"):
                val = g(nm)
                if val is not None:
                    mapping[val] = OrderStatus.filling

            # 完成
            for nm in ("ORDER_ALLTRADED", "ORDER_SUCCEEDED"):
                val = g(nm)
                if val is not None:
                    mapping[val] = OrderStatus.filled
            # 部成部撤（最终态）
            for nm in ("ORDER_PART_CANCEL", "ORDER_PARTSUCC_CANCEL"):
                val = g(nm)
                if val is not None:
                    mapping[val] = OrderStatus.partly_canceled

            # 撤单/不在队列（最终态）
            for nm in ("ORDER_CANCELED", "ORDER_PARTTRADED_NOT_QUEUEING", "ORDER_WITHDRAW", "ORDER_WITHDRAWN"):
                val = g(nm)
                if val is not None:
                    mapping[val] = OrderStatus.canceled
            # 撤单进行中
            for nm in ("ORDER_REPORTED_CANCEL",):
                val = g(nm)
                if val is not None:
                    mapping[val] = OrderStatus.canceling

            # 拒绝/失败
            for nm in ("ORDER_REJECTED", "ORDER_FAILED", "ORDER_INVALID", "ORDER_JUNK"):
                val = g(nm)
                if val is not None:
                    mapping[val] = OrderStatus.rejected

            if isinstance(raw_status, int) and raw_status in mapping:
                return mapping[raw_status]
        except Exception:
            pass

        # 字符串兜底
        s = str(raw_status or "").strip().lower()
        if s in ("filled", "alltraded", "all_traded", "completed", "已成"):
            return OrderStatus.filled
        if s in ("cancelled", "canceled", "withdraw", "已撤"):
            return OrderStatus.canceled
        if s in ("rejected", "failed", "invalid", "拒绝"):
            return OrderStatus.rejected
        if s in ("partly_canceled", "partcanceled", "partial_canceled", "部成部撤"):
            return OrderStatus.partly_canceled
        if s in ("filling", "partial_filled", "part_succ", "部分成交"):
            return OrderStatus.filling
        if s in ("canceling", "canceling_in_progress", "撤单中"):
            return OrderStatus.canceling
        if s in ("new", "unreported", "wait_report", "wait_reporting", "未知"):
            return OrderStatus.new
        # 其他一概视为在途
        return OrderStatus.open

    def _map_order_side(self, order_type: Any) -> Optional[bool]:
        """根据 order_type 推断买卖方向，无法识别时返回 None。"""
        try:
            from xtquant import xtconstant  # type: ignore
            if order_type == getattr(xtconstant, "STOCK_BUY", None):
                return True
            if order_type == getattr(xtconstant, "STOCK_SELL", None):
                return False
        except Exception:
            pass
        if order_type is None:
            return None
        label = str(order_type).strip().lower()
        if label in ("buy", "stock_buy", "b", "long"):
            return True
        if label in ("sell", "stock_sell", "s", "short"):
            return False
        return None

    # ---------------- 内部辅助 ----------------
    def _split_volume(self, total: int) -> List[int]:
        """按 ORDER_MAX_VOLUME 拆分数量。"""
        from bullet_trade.utils.env_loader import get_live_trade_config

        cfg = get_live_trade_config()
        max_vol = int(cfg.get("order_max_volume") or 1_000_000)
        if max_vol <= 0:
            max_vol = 1_000_000
        result: List[int] = []
        remaining = int(total)
        while remaining > 0:
            take = min(remaining, max_vol)
            result.append(take)
            remaining -= take
        return result

    def _build_send_order_args(
        self,
        security: str,
        amount: int,
        price: Optional[float],
        side: str,
        market: bool,
        remark: Optional[str],
    ):
        """
        兼容缺少 market 参数的测试桩，动态决定是否传入 market。
        """
        send_order = self._send_order
        try:
            inspect.signature(send_order).bind(security, amount, price, side, market, remark)
            return (security, amount, price, side, market, remark)
        except TypeError:
            try:
                inspect.signature(send_order).bind(security, amount, price, side, market)
                return (security, amount, price, side, market)
            except TypeError:
                try:
                    inspect.signature(send_order).bind(security, amount, price, side, remark)
                    return (security, amount, price, side, remark)
                except TypeError:
                    return (security, amount, price, side)

    async def _maybe_wait(self, order_id: str, override_timeout: Optional[float] = None) -> None:
        """根据 TRADE_MAX_WAIT_TIME 执行同步等待骨架（协程版）"""
        from bullet_trade.utils.env_loader import get_live_trade_config

        if override_timeout is not None:
            wait_s = float(override_timeout)
        else:
            wait_s = int(get_live_trade_config().get("trade_max_wait_time") or 0)
        if wait_s <= 0:
            log.info(f"订单 {order_id} 采用异步模式（TRADE_MAX_WAIT_TIME<=0），交由后台同步任务跟踪")
            return
        deadline = time.time() + wait_s
        interval = 0.5
        start = time.time()
        log.info(f"订单 {order_id} 进入同步等待，最长 {wait_s}s 轮询券商回报")
        last_snapshot: Optional[Dict[str, Any]] = None
        final_status: Optional[str] = None
        while time.time() < deadline:
            try:
                status = await self.get_order_status(order_id)
                last_snapshot = status or {}
                st = str(last_snapshot.get("status") or "").lower()
                if st in ("filled", "cancelled", "canceled", "partly_canceled", "rejected"):
                    final_status = st
                    break
            except Exception:
                pass
            await asyncio.sleep(interval)
        elapsed = time.time() - start
        if final_status:
            level = log.info
            if final_status in ("rejected", "canceled", "cancelled"):
                level = log.error
            level(
                f"订单 {order_id} 等待 {elapsed:.2f}s 获得状态 {final_status}，"
                f"快照={last_snapshot}"
            )
        else:
            log.warning(
                f"订单 {order_id} 等待 {elapsed:.2f}s 仍未完成，"
                f"最后快照={last_snapshot or '无'}，继续由异步同步逻辑追踪"
            )

    def _map_security(self, security: str) -> str:
        """将聚宽风格代码映射到 QMT 常见风格。
        - 000001.XSHE → 000001.SZ
        - 600000.XSHG → 600000.SH
        其余原样返回。
        """
        try:
            if security.endswith(".XSHE"):
                return security.replace(".XSHE", ".SZ")
            if security.endswith(".XSHG"):
                return security.replace(".XSHG", ".SH")
            return security
        except Exception:
            return security

    def _infer_market_price(self, security: str) -> Optional[float]:
        """
        市价单需要一个参考价，避免落在涨停/跌停笼子外。
        """
        code = self._map_security(security)
        try:
            from xtquant import xtdata  # type: ignore
        except Exception:
            return None

        price: Optional[float] = None
        try:
            quote = xtdata.get_last_quote(code)  # type: ignore[attr-defined]
            if quote:
                if isinstance(quote, dict):
                    price = quote.get("lastPrice") or quote.get("last_price") or quote.get("price")
                else:
                    price = getattr(quote, "lastPrice", None) or getattr(quote, "last_price", None) or getattr(quote, "price", None)
        except Exception:
            price = None

        if price is None:
            try:
                kline = xtdata.get_market_data([code], ["lastPrice"], count=1)  # type: ignore[attr-defined]
                if kline and kline.get(code):
                    price = kline[code][0].get("lastPrice")
            except Exception:
                price = None

        try:
            return float(price) if price is not None else None
        except Exception:
            return None

    def _choose_market_price_type(self, security: str, xtconstant) -> Any:
        """
        根据市场选择更具体的市价类型，避免退回限价。
        """
        market = None
        if "." in security:
            market = security.split(".")[-1].upper()
        # 上交所/北交所：最优五档即时成交剩余撤销
        if market == "SH":
            return (
                getattr(xtconstant, "MARKET_SH_CONVERT_5_CANCEL", None)
                or getattr(xtconstant, "MARKET_PEER_PRICE_FIRST", None)
                or getattr(xtconstant, "MARKET_MINE_PRICE_FIRST", None)
                or getattr(xtconstant, "ANY_PRICE", None)
                or getattr(xtconstant, "FIX_PRICE", None)
            )
        # 深交所：对手方最优价
        if market == "SZ":
            return (
                getattr(xtconstant, "MARKET_PEER_PRICE_FIRST", None)
                or getattr(xtconstant, "MARKET_MINE_PRICE_FIRST", None)
                or getattr(xtconstant, "MARKET_SZ_CONVERT_5_CANCEL", None)
                or getattr(xtconstant, "ANY_PRICE", None)
                or getattr(xtconstant, "FIX_PRICE", None)
            )
        # 兜底
        return (
            getattr(xtconstant, "ANY_PRICE", None)
            or getattr(xtconstant, "ORDER_PRICE_TYPE_MARKET", None)
            or getattr(xtconstant, "ORDER_PRICE_TYPE_BEST5_OR_CANCEL", None)
            or getattr(xtconstant, "ORDER_PRICE_TYPE_FIVE_LEVEL_INSTANT_OR_CANCEL", None)
            or getattr(xtconstant, "MARKET_PRICE", None)
            or getattr(xtconstant, "FIX_PRICE", None)
            or getattr(xtconstant, "PRICE_LIMIT", None)
            or getattr(xtconstant, "ORDER_PRICE_TYPE_LIMIT", 0)
        )

    def _send_order(
        self,
        security: str,
        amount: int,
        price: Optional[float],
        side: str,
        market: bool = False,
        remark: Optional[str] = None,
    ) -> str:
        """实际下单（最小实现，需本机已安装 QMT/xtquant）"""
        try:
            from xtquant import xtconstant  # type: ignore
        except Exception as e:
            raise RuntimeError("未检测到 xtquant 常量定义，请确认环境") from e

        if not self._xt_trader or not self._xt_account:
            raise RuntimeError("QMT 交易对象未初始化")

        if side == "buy":
            order_type = (
                getattr(xtconstant, "STOCK_BUY", None)
                or getattr(xtconstant, "ORDER_BUY", None)
                or getattr(xtconstant, "ORDER_TYPE_BUY", 0)
            )
        else:
            order_type = (
                getattr(xtconstant, "STOCK_SELL", None)
                or getattr(xtconstant, "ORDER_SELL", None)
                or getattr(xtconstant, "ORDER_TYPE_SELL", 1)
            )
        market_mode = bool(market or price is None)
        if not market_mode and price is None:
            raise ValueError("限价单缺少价格，请提供 price 或将 market 设为 True")
        if market_mode and price is None:
            price = self._infer_market_price(security)
            if price is None:
                raise ValueError("市价单无法获取参考价格，请显式传入 price")
            pct = self._market_buy_percent if side == "buy" else self._market_sell_percent
            price = max(0.0001, price * (1 + pct))
        log.info(
            f"QMT 下单: sec={security} side={side} amount={amount} price={price} market_mode={market_mode}"
        )
        price_to_use = float(price)
        if market_mode:
            price_type = self._choose_market_price_type(security, xtconstant)
        else:
            price_type = (
                getattr(xtconstant, "FIX_PRICE", None)
                or getattr(xtconstant, "PRICE_LIMIT", None)
                or getattr(xtconstant, "ORDER_PRICE_TYPE_LIMIT", 0)
            )
        log.info(f"QMT 下单参数: order_type={order_type} price_type={price_type} price_to_use={price_to_use}")

        try:
            strategy_name = "bullet-trade"
            order_remark = remark or strategy_name
            try:
                oid = self._xt_trader.order_stock(  # type: ignore
                    self._xt_account,
                    security,
                    order_type,
                    int(amount),
                    price_type,
                    price_to_use,
                    strategy_name,
                    order_remark,
                )
            except TypeError:
                try:
                    oid = self._xt_trader.order_stock(  # type: ignore
                        self._xt_account,
                        security,
                        order_type,
                        int(amount),
                        price_type,
                        price_to_use,
                        order_remark,
                    )
                except TypeError:
                    oid = self._xt_trader.order_stock(  # type: ignore
                        self._xt_account,
                        security,
                        order_type,
                        int(amount),
                        price_type,
                        price_to_use,
                    )
        except Exception as e:
            raise RuntimeError(f"QMT 下单失败: {e}") from e

        if oid is None:
            raise RuntimeError("QMT 返回空订单号")
        if isinstance(oid, (int, float)) and oid < 0:
            raise RuntimeError(f"QMT 下单返回错误码: {oid}")
        if isinstance(oid, str) and not oid.strip():
            raise RuntimeError("QMT 返回空订单号")

        return str(oid)

    # ----- LiveEngine 钩子 -----
    def supports_account_sync(self) -> bool:
        return True

    def supports_orders_sync(self) -> bool:
        return True

    def sync_account(self) -> Optional[Dict[str, Any]]:
        self._ensure_connected()
        return self._build_account_snapshot()

    def sync_orders(self) -> List[Dict[str, Any]]:
        self._ensure_connected()
        orders: List[Dict[str, Any]] = []
        if not self._xt_trader or not self._xt_account:
            return orders
        candidates = []
        for name in ("query_stock_orders", "get_stock_orders", "query_orders"):
            fn = getattr(self._xt_trader, name, None)
            if callable(fn):
                try:
                    data = fn(self._xt_account)  # type: ignore
                except Exception:
                    data = None
                if data:
                    candidates = data
                    break
        if not candidates:
            return orders
        if isinstance(candidates, dict):
            iterable = [candidates]
        else:
            iterable = list(candidates)

        def _pick(obj: object, *names: str) -> Optional[Any]:
            if isinstance(obj, dict):
                for name in names:
                    if name in obj:
                        return obj.get(name)
                return None
            for name in names:
                if hasattr(obj, name):
                    return getattr(obj, name)
            return None

        for item in iterable:
            try:
                oid = _pick(item, "order_id", "entrust_id")
                code = _pick(item, "stock_code", "code")
                amount = _pick(item, "order_volume", "volume")
                order_price = _pick(item, "price", "order_price")
                status = _pick(item, "order_status", "status")
                filled = _pick(item, "traded_volume", "deal_volume", "filled_volume", "volume_traded")
                traded_price = _pick(item, "traded_price", "avg_price", "trade_price")
                order_type = _pick(item, "order_type", "orderType", "type")
                order_remark = _pick(item, "order_remark", "remark")
                strategy_name = _pick(item, "strategy_name", "strategy")
                order_sysid = _pick(item, "order_sysid", "sysid")
                status_msg = _pick(item, "status_msg", "status_msg")
                price_type = _pick(item, "price_type", "priceType")
                order_time = _pick(item, "order_time", "time")
                if not oid:
                    continue
                orders.append(
                    {
                        "order_id": str(oid),
                        "security": self._map_to_jq_symbol(code) if code else None,
                        "amount": amount,
                        "filled": 0 if filled is None else filled,
                        "price": 0.0 if traded_price is None else traded_price,
                        "order_price": order_price,
                        "status": status,
                        "order_type": order_type,
                        "is_buy": self._map_order_side(order_type),
                        "order_remark": order_remark,
                        "strategy_name": strategy_name,
                        "order_sysid": order_sysid,
                        "status_msg": status_msg,
                        "price_type": price_type,
                        "order_time": order_time,
                    }
                )
            except Exception:
                continue
        return orders

    def _map_to_jq_symbol(self, security: Optional[str]) -> Optional[str]:
        if not security:
            return None
        code = str(security)
        if code.endswith(".SZ"):
            return code.replace(".SZ", ".XSHE")
        if code.endswith(".SH"):
            return code.replace(".SH", ".XSHG")
        return code

    def _build_account_snapshot(self) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {
            "account_id": self.account_id,
            "account_type": self.account_type,
            "positions": [],
        }
        # 资产快照（尽量兼容不同字段名）
        cash_val = None
        total_val = None
        try:
            asset = self._xt_trader.query_stock_asset(self._xt_account) if self._xt_trader else None  # type: ignore[attr-defined]
        except Exception:
            asset = None
        if asset is not None:
            # 可能存在的字段别名：cash/available/available_cash/enable_balance/avail_fund
            for name in (
                "cash",
                "available",
                "available_cash",
                "enable_balance",
                "avail_fund",
                "fund_avail",
                "enable_money",
                "can_use_money",
                "cash_balance",
            ):
                if hasattr(asset, name):
                    cash_val = getattr(asset, name)
                    break
            # 可能存在的字段别名：total_asset/total_assets/asset_total/static_asset
            for name in (
                "total_asset",
                "total_assets",
                "asset_total",
                "static_asset",
            ):
                if hasattr(asset, name):
                    total_val = getattr(asset, name)
                    break
        try:
            raw_positions = (
                self._xt_trader.query_stock_positions(self._xt_account) if self._xt_trader else None
            )  # type: ignore[attr-defined]
        except Exception:
            raw_positions = None
        positions: List[Dict[str, Any]] = []
        try:
            from xtquant import xtdata  # type: ignore
            codes = [p.stock_code for p in raw_positions]
            ticks = xtdata.get_full_tick(codes) if codes else {}
        except Exception:
            ticks = {}
        if raw_positions:
            for pos in raw_positions:
                try:
                    code = getattr(pos, "stock_code", None) or getattr(pos, "code", None)
                    if not code:
                        continue
                    qty = getattr(pos, "volume", None) or getattr(pos, "current_amount", None) or 0
                    avail = getattr(pos, "available_volume", None)
                    if avail is None:
                        avail = getattr(pos, "enable_amount", None)
                    if avail is None:
                        avail = getattr(pos, "can_use_volume", None)
                    if avail is None:
                        avail = getattr(pos, "can_sell_volume", None)
                    if avail is None:
                        avail = getattr(pos, "sellable_volume", None)
                    if avail is None:
                        avail = getattr(pos, "m_nCanUseVolume", None)
                    if avail is None:
                        avail = qty
                    avg_cost = (
                        getattr(pos, "cost_price", None)
                        or getattr(pos, "avg_price", None)
                        or getattr(pos, "average_price", None)
                        or getattr(pos, "open_price", None)
                        or 0.0
                    )
                    # last 价格字段尽量不兜底到字符串字段（如 market），避免类型错误
                    last = (
                        getattr(pos, "last_price", None)
                        or getattr(pos, "price", None)
                        or getattr(pos, "current_price", None)
                        or getattr(pos, "last", None)
                    )
                    if ticks and not last and pos.stock_code in ticks and ticks[pos.stock_code]:
                        last = ticks[pos.stock_code].get("lastPrice")
                    market_value = getattr(pos, "market_value", None)
                    if market_value is None and last is not None:
                        try:
                            market_value = float(last) * float(qty or 0)
                        except Exception:
                            market_value = None
                    try:
                        from xtquant import xtdata  # type: ignore
                        name = xtdata.get_instrument_detail(code).get("InstrumentName")
                    except Exception:
                        name = None
                    positions.append(
                        {
                            "security": self._map_to_jq_symbol(code),
                            "name": name,
                            "amount": int(qty or 0),
                            "closeable_amount": int(avail or 0),
                            "avg_cost": float(avg_cost or 0.0),
                            "current_price": float(last or 0.0) if last is not None else 0.0,
                            "market_value": float(market_value or 0.0),
                        }
                    )
                except Exception:
                    continue
        snapshot["positions"] = positions

        # 若总资产缺失或为 0，则用 现金 + 持仓市值 近似估算
        try:
            if (total_val is None or float(total_val or 0) <= 0) and positions:
                pos_mv = sum(float(p.get("market_value") or 0.0) for p in positions)
                total_val = (float(cash_val or 0.0)) + pos_mv
        except Exception:
            pass

        if cash_val is not None:
            snapshot["available_cash"] = cash_val
        if total_val is not None:
            snapshot["total_value"] = total_val
        return snapshot
