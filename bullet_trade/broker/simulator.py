"""
模拟券商

用于测试和开发的模拟券商实现（异步接口）
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from .base import BrokerBase


class SimulatorBroker(BrokerBase):
    """
    模拟券商
    
    提供完整的券商接口实现，但不连接真实券商
    可用于策略测试和开发
    """

    def __init__(
        self,
        account_id: str = "simulator",
        account_type: str = "stock",
        initial_cash: float = 1_000_000,
    ):
        super().__init__(account_id, account_type)
        self.initial_cash = initial_cash
        self.available_cash = initial_cash
        self.positions: Dict[str, Dict[str, float]] = {}
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.trades: List[Dict[str, Any]] = []
        self._mock_prices: Dict[str, float] = {}

    def connect(self) -> bool:
        """连接（模拟）"""
        self._connected = True
        print("模拟券商已连接")
        return True

    def disconnect(self) -> bool:
        """断开连接（模拟）"""
        self._connected = False
        print("模拟券商已断开")
        return True

    def get_account_info(self) -> Dict[str, Any]:
        """获取账户信息"""
        positions_value = sum(
            pos["amount"] * self._mock_prices.get(sec, pos["avg_cost"])
            for sec, pos in self.positions.items()
        )
        return {
            "account_id": self.account_id,
            "total_value": self.available_cash + positions_value,
            "available_cash": self.available_cash,
            "positions_value": positions_value,
            "positions": self.get_positions(),
        }

    def get_positions(self) -> List[Dict[str, Any]]:
        """获取持仓"""
        result: List[Dict[str, Any]] = []
        for security, pos in self.positions.items():
            price = self._mock_prices.get(security, pos["avg_cost"])
            result.append(
                {
                    "security": security,
                    "amount": pos["amount"],
                    "avg_cost": pos["avg_cost"],
                    "current_price": price,
                    "market_value": pos["amount"] * price,
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
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """买入：在后台线程执行以避免阻塞事件循环"""
        _ = extra
        return await asyncio.to_thread(self._buy_sync, security, amount, price, market, remark)

    def _buy_sync(
        self,
        security: str,
        amount: int,
        price: Optional[float],
        market: bool,
        remark: Optional[str],
    ) -> str:
        order_id = str(uuid.uuid4())[:8]
        trade_price = price if price is not None else self._mock_prices.get(security, 10.0)

        cost = amount * trade_price * 1.0003  # 模拟手续费
        if cost > self.available_cash:
            raise ValueError(f"可用资金不足: {self.available_cash:.2f} < {cost:.2f}")

        if security not in self.positions:
            self.positions[security] = {"amount": 0, "avg_cost": 0}

        pos = self.positions[security]
        total_cost = pos["amount"] * pos["avg_cost"] + amount * trade_price
        pos["amount"] += amount
        pos["avg_cost"] = total_cost / pos["amount"]
        self.available_cash -= cost

        self.orders[order_id] = {
            "order_id": order_id,
            "security": security,
            "amount": amount,
            "price": trade_price,
            "side": "buy",
            "market": bool(market or price is None),
            "status": "filled",
            "time": datetime.now(),
            "order_remark": remark,
        }
        print(f"模拟买入成功: {security} x {amount} @ {trade_price:.2f}")
        return order_id

    async def sell(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        wait_timeout: Optional[float] = None,
        remark: Optional[str] = None,
        *,
        market: bool = False,
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """卖出"""
        _ = extra
        return await asyncio.to_thread(self._sell_sync, security, amount, price, market, remark)

    def _sell_sync(
        self,
        security: str,
        amount: int,
        price: Optional[float],
        market: bool,
        remark: Optional[str],
    ) -> str:
        order_id = str(uuid.uuid4())[:8]

        if security not in self.positions or self.positions[security]["amount"] < amount:
            raise ValueError(f"持仓不足: {security}")

        trade_price = price if price is not None else self._mock_prices.get(security, 10.0)

        pos = self.positions[security]
        pos["amount"] -= amount
        if pos["amount"] == 0:
            del self.positions[security]

        proceeds = amount * trade_price * (1 - 0.0003 - 0.001)
        self.available_cash += proceeds

        self.orders[order_id] = {
            "order_id": order_id,
            "security": security,
            "amount": amount,
            "price": trade_price,
            "side": "sell",
            "market": bool(market or price is None),
            "status": "filled",
            "time": datetime.now(),
            "order_remark": remark,
        }
        print(f"模拟卖出成功: {security} x {amount} @ {trade_price:.2f}")
        return order_id

    async def cancel_order(self, order_id: str) -> bool:
        """撤销订单"""
        if order_id in self.orders:
            self.orders[order_id]["status"] = "cancelled"
            return True
        return False

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """获取订单状态"""
        return self.orders.get(order_id, {})

    def set_mock_price(self, security: str, price: float) -> None:
        """设置模拟行情价格"""
        self._mock_prices[security] = price

    # ----- LiveEngine 钩子 -----

    def supports_account_sync(self) -> bool:
        return True

    def supports_orders_sync(self) -> bool:
        return True

    def sync_account(self) -> Dict[str, Any]:
        """返回当前账户快照"""
        return self.get_account_info()

    def sync_orders(self) -> List[Dict[str, Any]]:
        """返回当前订单列表"""
        return list(self.orders.values())

    def supports_tick_subscription(self) -> bool:
        """模拟券商允许订阅 tick（基于自定义价格）"""
        return True

    def subscribe_ticks(self, symbols: List[str]) -> None:
        """tick 订阅占位实现：预置一个默认价格，避免上层报错。"""
        for sym in symbols:
            self._mock_prices.setdefault(sym, 10.0)

    def subscribe_markets(self, markets: List[str]) -> None:
        """市场级订阅在模拟券商中忽略即可。"""
        return None

    def unsubscribe_ticks(self, symbols: Optional[List[str]] = None) -> None:
        """取消订阅（清理 mock 价格），允许 symbols 为空表示全部。"""
        if symbols is None:
            self._mock_prices.clear()
            return None
        for sym in symbols:
            self._mock_prices.pop(sym, None)
        return None

    def get_current_tick(self, symbol: str) -> Optional[Dict[str, Any]]:
        """根据 mock price 生成简易 tick"""
        price = self._mock_prices.get(symbol)
        if price is None:
            return None
        return {
            "sid": symbol,
            "last_price": price,
            "dt": datetime.now().isoformat(),
        }
