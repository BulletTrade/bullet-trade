"""GM 本地限价 Broker：显式账户、隔离会话、持久化提交身份及查询同步。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import sqlite3
import threading
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from filelock import FileLock

from bullet_trade.broker.base import BrokerBase
from .trading_client import GmTradingClient, GmTradingError


def symbol(security):
    code, market = str(security).split(".")
    if market not in ("XSHG", "XSHE") or len(code) != 6 or not code.isdigit():
        raise ValueError("需要标准证券代码，例如 511880.XSHG")
    return ("SHSE." if market == "XSHG" else "SZSE.") + code


def security(value):
    market, code = value.split(".")
    return code + {"SHSE": ".XSHG", "SZSE": ".XSHE"}[market]


def enabled(value):
    return value is True or str(value).lower() in ("true", "1", "yes")


class GmBroker(BrokerBase):
    def __init__(self, account_id, account_type="stock", config=None, client=None):
        super().__init__(account_id, account_type)
        self.config = dict(config or {}, account_id=account_id)
        self.config["enable_trading"] = enabled(self.config.get("enable_trading", False))
        self.client = client or GmTradingClient(self.config)
        self._mutex = threading.RLock()
        self._db = None
        self._file_lock = None

    def preflight(self):
        if not self.account_id or self.account_type != "stock":
            raise ValueError("GM 需要明确的普通证券账户")
        if not self.config.get("token") or not self.config.get("strategy_id"):
            raise ValueError("缺少 GM_TOKEN 或 GM_STRATEGY_ID")
        if not self.config.get("journal_path"):
            raise ValueError("必须配置 GM_JOURNAL_PATH，保存订单恢复信息")

    def connect(self):
        if self._connected:
            return True
        self.preflight()
        path = Path(self.config["journal_path"]).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file_lock = FileLock(str(path) + ".lock")
        self._file_lock.acquire(timeout=0)
        try:
            self._db = sqlite3.connect(str(path), check_same_thread=False)
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS submissions (key TEXT PRIMARY KEY, payload TEXT NOT NULL, native_id TEXT, state TEXT NOT NULL)"
            )
            self._db.execute("CREATE TABLE IF NOT EXISTS identity (account TEXT PRIMARY KEY)")
            identity = hashlib.sha256(self.account_id.encode()).hexdigest()
            old = self._db.execute("SELECT account FROM identity").fetchall()
            if old and old != [(identity,)]:
                raise ValueError("订单日志属于另一账户")
            self._db.execute("INSERT OR IGNORE INTO identity VALUES (?)", (identity,))
            self._db.commit()
            self.client.start()
            if self.client.query("status") != {"state": 3, "error_code": 0}:
                raise GmTradingError("掘金账户尚未登录")
            self.get_account_info()
            self._connected = True
            return True
        except Exception:
            self.disconnect()
            raise

    def disconnect(self):
        with self._mutex:
            self.client.close()
            if self._db is not None:
                self._db.close()
                self._db = None
            if self._file_lock is not None:
                self._file_lock.release()
                self._file_lock = None
            self._connected = False
        return True

    def get_account_info(self):
        row = self.client.query("cash")
        if row.get("account_id") != self.account_id:
            raise GmTradingError("账户身份不匹配")
        positions = self.get_positions()
        return dict(
            account_id=self.account_id,
            total_value=float(row["nav"]),
            available_cash=float(row["available"]),
            cash=float(row["balance"]),
            positions_value=sum(x["market_value"] for x in positions),
            frozen_cash=float(row["order_frozen"]),
            updated_at=row.get("updated_at"),
            positions=positions,
        )

    def get_positions(self):
        rows = self.client.query("positions")
        result = []
        for row in rows:
            if row.get("account_id") != self.account_id:
                raise GmTradingError("持仓账户不匹配")
            result.append(
                dict(
                    security=security(row["symbol"]),
                    amount=int(row["volume"]),
                    enable_amount=int(row["available_now"]),
                    closeable_amount=int(row["available_now"]),
                    avg_cost=float(row["vwap"]),
                    market_value=float(row["market_value"]),
                    current_price=float(row["price"]),
                    raw=dict(row),
                )
            )
        return result

    def get_quote(self, code):
        rows = self.client.query("quote", symbol=symbol(code))
        if len(rows) != 1 or rows[0]["symbol"] != symbol(code):
            raise GmTradingError("行情缺失或代码不匹配")
        row = rows[0]
        at = datetime.fromisoformat(str(row["created_at"]))
        age = (datetime.now(at.tzinfo) - at).total_seconds()
        if not -2 <= age <= float(self.config.get("quote_max_age", 5)):
            raise GmTradingError("行情过期，拒绝提交")
        return row

    @staticmethod
    def _local_id(key):
        return "gm:" + hashlib.sha256(key.encode()).hexdigest()[:24]

    def _records(self):
        if self._db is None:
            raise GmTradingError("Broker 尚未连接")
        return self._db.execute("SELECT key,payload,native_id,state FROM submissions").fetchall()

    def _place(self, side, code, amount, price, market, extra):
        with self._mutex:
            if not self._connected or not self.config["enable_trading"]:
                raise GmTradingError("GM 交易未启用")
            if market or price is None:
                raise NotImplementedError("GM 首版仅支持明确价格的限价单")
            if isinstance(amount, bool) or int(amount) != amount or amount <= 0:
                raise ValueError("数量必须为正整数")
            native = symbol(code)
            # 首版普通主板股票与 ETF；科创板等申报规则另行验收。
            if not code.startswith(("60", "00", "51", "56", "58", "15", "16")):
                raise NotImplementedError("该品种的申报规则尚未验收")
            if amount % 100:
                raise ValueError("首版申报数量必须为 100 的整数倍")
            price = float(price)
            tick = Decimal(".001" if code.startswith(("5", "1")) else ".01")
            if not math.isfinite(price) or price <= 0 or Decimal(str(price)) % tick:
                raise ValueError("价格必须符合最小报价单位")
            payload = dict(side=side, symbol=native, volume=int(amount), price=price)
            encoded = json.dumps(payload, sort_keys=True)
            key = str((extra or {}).get("idempotency_key") or uuid.uuid4().hex)
            old = self._db.execute(
                "SELECT payload,native_id,state FROM submissions WHERE key=?", (key,)
            ).fetchone()
            if old:
                if old[0] != encoded:
                    raise ValueError("相同幂等键对应不同订单")
                return self._local_id(key)
            if any(state == "unknown" for _, _, _, state in self._records()):
                raise GmTradingError("存在未决提交，需核对原委托后恢复交易")
            quote = self.get_quote(code)
            if abs(price / float(quote["price"]) - 1) > 0.01:
                raise ValueError("限价偏离当前报价超过 1%")
            if side == "buy":
                if self.get_account_info()["available_cash"] < price * amount:
                    raise ValueError("可用资金不足")
            else:
                available = sum(
                    x["enable_amount"] for x in self.get_positions() if x["security"] == code
                )
                if available < amount:
                    raise ValueError("可卖数量不足")
            # 写入前落盘；任何异常均不允许自动重发。
            self._db.execute(
                "INSERT INTO submissions VALUES (?,?,NULL,?)", (key, encoded, "unknown")
            )
            self._db.commit()
            try:
                rows = self.client.query("place", **payload)
                if (
                    len(rows) != 1
                    or rows[0].get("account_id") != self.account_id
                    or not rows[0].get("cl_ord_id")
                ):
                    raise GmTradingError("提交响应身份不完整")
                row = rows[0]
                if row.get("symbol") != native or int(row.get("volume", -1)) != amount:
                    raise GmTradingError("提交响应内容不匹配")
                self._db.execute(
                    "UPDATE submissions SET native_id=?,state=? WHERE key=?",
                    (row["cl_ord_id"], "known", key),
                )
                self._db.commit()
            except Exception:
                # 向引擎返回稳定的待核对订单，避免异常被标成 rejected 后重新下单。
                pass
            return self._local_id(key)

    async def buy(
        self,
        security,
        amount,
        price=None,
        wait_timeout=None,
        remark=None,
        *,
        market=False,
        extra=None,
    ):
        return await asyncio.to_thread(self._place, "buy", security, amount, price, market, extra)

    async def sell(
        self,
        security,
        amount,
        price=None,
        wait_timeout=None,
        remark=None,
        *,
        market=False,
        extra=None,
    ):
        return await asyncio.to_thread(self._place, "sell", security, amount, price, market, extra)

    def get_orders(self, order_id=None, security=None, status=None, from_broker=False):
        with self._mutex:
            rows = self.client.query("orders")
            records = self._records()
            identity = {native: self._local_id(key) for key, _, native, _ in records if native}
            results = []
            states = {
                0: "new",
                1: "open",
                2: "filling",
                3: "filled",
                5: "canceled",
                6: "canceling",
                8: "rejected",
                9: "held",
                10: "new",
                12: "canceled",
            }
            for row in rows:
                if row.get("account_id") != self.account_id:
                    raise GmTradingError("委托账户不匹配")
                native = row["cl_ord_id"]
                if not from_broker and native not in identity:
                    continue
                state = states.get(row["status"], "held")
                if state == "canceled" and row["filled_volume"]:
                    state = "partly_canceled"
                results.append(
                    dict(
                        order_id=identity.get(native, native),
                        native_order_id=native,
                        security=globals()["security"](row["symbol"]),
                        amount=int(row["volume"]),
                        filled=int(row["filled_volume"]),
                        price=float(row["price"]),
                        avg_price=float(row["filled_vwap"]),
                        status=state,
                        is_buy=row["side"] == 1,
                        order_time=row.get("created_at"),
                        raw_status=row["status"],
                        raw=row,
                    )
                )
            for key, payload, native, state in records:
                if state == "unknown":
                    request = json.loads(payload)
                    results.append(
                        dict(
                            order_id=self._local_id(key),
                            security=globals()["security"](request["symbol"]),
                            amount=request["volume"],
                            price=request["price"],
                            filled=0,
                            status="new",
                            settlement_state="pending",
                            settlement_pending_reason="gm_submit_unknown",
                            idempotency_key=key,
                            submission_state="submit_unknown",
                            is_buy=request["side"] == "buy",
                        )
                    )
            return [
                x
                for x in results
                if (not order_id or x["order_id"] == order_id)
                and (not security or x["security"] == security)
                and (status is None or x["status"] == getattr(status, "value", status))
            ]

    def get_open_orders(self):
        return [
            x
            for x in self.get_orders()
            if x["status"] not in ("filled", "canceled", "partly_canceled", "rejected")
        ]

    async def get_order_status(self, order_id):
        rows = await asyncio.to_thread(self.get_orders, order_id)
        if len(rows) != 1:
            raise GmTradingError("未找到唯一委托")
        return rows[0]

    async def cancel_order(self, order_id):
        row = await self.get_order_status(order_id)
        if not self.config["enable_trading"] or row.get("submission_state") == "submit_unknown":
            raise GmTradingError("不能撤销身份未确认的委托")
        if row["status"] in ("filled", "rejected"):
            return False
        if row["status"] in ("canceled", "partly_canceled"):
            return True
        await asyncio.to_thread(self.client.query, "cancel", cl_ord_id=row["native_order_id"])
        for _ in range(10):
            await asyncio.sleep(0.2)
            row = await self.get_order_status(order_id)
            if row["status"] in ("canceled", "partly_canceled"):
                return True
            if row["status"] == "filled":
                return False
        return False  # 未证明撤成，订单保持在途。

    def get_trades(self, order_id=None, security=None):
        with self._mutex:
            identity = {
                native: self._local_id(key) for key, _, native, _ in self._records() if native
            }
            out = {}
            for row in self.client.query("trades"):
                if row.get("account_id") != self.account_id:
                    raise GmTradingError("成交账户不匹配")
                if row["exec_type"] != 15 or row["cl_ord_id"] not in identity:
                    continue
                if not row.get("exec_id"):
                    raise GmTradingError("成交缺少稳定身份，不能安全去重")
                trade = dict(
                    trade_id=row["exec_id"],
                    order_id=identity[row["cl_ord_id"]],
                    security=globals()["security"](row["symbol"]),
                    amount=int(row["volume"]),
                    price=float(row["price"]),
                    commission=float(row["commission"]),
                    gross_amount=float(row["amount"]),
                    commission_source="execution",
                    time=row.get("created_at"),
                    raw=row,
                )
                if trade["trade_id"] in out and out[trade["trade_id"]] != trade:
                    raise GmTradingError("相同成交身份对应不同记录")
                out[trade["trade_id"]] = trade
            # 某些仿真通道逐笔 commission 恒为 0，真实费用只在委托汇总中。
            orders = {x["order_id"]: x for x in self.get_orders()}
            for oid in {x["order_id"] for x in out.values()}:
                group = sorted(
                    [x for x in out.values() if x["order_id"] == oid],
                    key=lambda x: (str(x["time"]), x["trade_id"]),
                )
                order = orders.get(oid)
                if not order or sum(x["amount"] for x in group) != order["filled"]:
                    raise GmTradingError("成交与委托快照尚未对齐")
                total = Decimal(str(order["raw"]["filled_commission"]))
                reported = sum(Decimal(str(x["commission"])) for x in group)
                if abs(total - reported) > Decimal(".005"):
                    if reported != 0 or order["status"] not in (
                        "filled",
                        "canceled",
                        "partly_canceled",
                    ):
                        raise GmTradingError("手续费尚未最终确认，等待委托汇总")
                    # 仅在最终委托和全部成交对齐时分摊；保留原始回报与费用来源。
                    remaining = total
                    for index, trade in enumerate(group):
                        fee = (
                            remaining
                            if index == len(group) - 1
                            else (
                                total * Decimal(trade["amount"]) / Decimal(order["filled"])
                            ).quantize(Decimal(".01"))
                        )
                        trade["commission"] = float(fee)
                        trade["commission_source"] = "final_order_filled_commission"
                        remaining -= fee
            return [
                x
                for x in out.values()
                if (not order_id or x["order_id"] == order_id)
                and (not security or x["security"] == security)
            ]

    def supports_orders_sync(self):
        return True

    def supports_account_sync(self):
        return True

    def sync_orders(self):
        return self.get_orders()

    def sync_account(self):
        return self.get_account_info()

    def heartbeat(self):
        if self.client.query("status") != {"state": 3, "error_code": 0}:
            raise GmTradingError("GM 账户已断开")

    def cleanup(self):
        self.disconnect()
