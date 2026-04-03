"""
核心数据模型

定义回测系统中使用的所有核心数据结构
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Optional, Any
from datetime import datetime, date
import pandas as pd


class OrderStatus(Enum):
    """订单状态枚举

    为兼容 QMT/xtquant 的更细粒度状态，补充如下：
    - new: 刚提交/待报/未知（未收到柜台确认）
    - open: 已受理/已报/排队中（在途）
    - filling: 部分成交（仍在撮合）
    - partly_canceled: 部分成交部分撤销（最终态）
    - canceling: 撤单进行中
    - filled: 全部成交（最终态）
    - canceled: 全部撤销（最终态）
    - rejected: 废单/拒绝（最终态）
    - held: 挂起
    """
    new = 'new'
    open = 'open'
    filling = 'filling'
    partly_canceled = 'partly_canceled'
    canceling = 'canceling'
    filled = 'filled'
    canceled = 'canceled'
    rejected = 'rejected'
    held = 'held'


class OrderStyle(Enum):
    """下单方式枚举"""
    market = 'market'  # 市价单
    limit = 'limit'  # 限价单


@dataclass
class Position:
    """
    持仓标的信息
    
    Attributes:
        security: 标的代码
        total_amount: 总持仓
        closeable_amount: 可卖出数量
        avg_cost: 平均成本
        price: 当前价格
        acc_avg_cost: 累计平均成本
        value: 市值
        side: 多空方向 ('long' or 'short')
        buy_time: 当前这轮持仓首次建仓时间
        last_buy_time: 最近一次买入/加仓时间
    """
    security: str
    total_amount: int = 0
    closeable_amount: int = 0
    avg_cost: float = 0.0
    price: float = 0.0
    acc_avg_cost: float = 0.0
    value: float = 0.0
    side: str = 'long'
    buy_time: Optional[datetime] = None
    last_buy_time: Optional[datetime] = None
    # 当日买入但受 T+1 限制的数量（次日释放为可卖）
    today_buy_t1: int = 0
    
    def update_price(self, price: float):
        """更新当前价格和市值"""
        self.price = price
        self.value = self.total_amount * price
        
    def update_position(self, amount: int, cost: float):
        """
        更新持仓
        
        Args:
            amount: 交易数量（正数买入，负数卖出）
            cost: 成交价格
        """
        if amount > 0:
            # 买入
            total_cost = self.avg_cost * self.total_amount + cost * amount
            self.total_amount += amount
            self.closeable_amount += amount  # 简化处理，不考虑T+1
            if self.total_amount > 0:
                self.avg_cost = total_cost / self.total_amount
                self.acc_avg_cost = self.avg_cost
        else:
            # 卖出
            self.total_amount += amount  # amount为负数
            self.closeable_amount += amount
            if self.total_amount <= 0:
                self.total_amount = 0
                self.closeable_amount = 0
                self.avg_cost = 0
                self.acc_avg_cost = 0


@dataclass
class SubPortfolio:
    """
    子账户信息
    
    Attributes:
        type: 账户类型
        available_cash: 可用资金
        transferable_cash: 可取资金
        total_value: 总资产
        positions: 持仓字典
        positions_value: 持仓市值
    """
    type: str = 'stock'  # stock/futures
    available_cash: float = 0.0
    transferable_cash: float = 0.0
    total_value: float = 0.0
    positions: Dict[str, Position] = field(default_factory=dict)
    positions_value: float = 0.0
    
    def update_value(self):
        """更新账户总价值"""
        self.positions_value = sum(pos.value for pos in self.positions.values())
        self.total_value = self.available_cash + self.positions_value


@dataclass
class Portfolio:
    """
    总账户信息
    
    Attributes:
        total_value: 总资产
        available_cash: 可用资金
        transferable_cash: 可取资金
        locked_cash: 冻结资金
        starting_cash: 初始资金
        positions: 持仓字典
        positions_value: 持仓市值
        subportfolios: 子账户字典
    """
    total_value: float = 100000.0
    available_cash: float = 100000.0
    transferable_cash: float = 100000.0
    locked_cash: float = 0.0
    starting_cash: float = 100000.0
    positions: Dict[str, Position] = field(default_factory=dict)
    positions_value: float = 0.0
    subportfolios: Dict[str, SubPortfolio] = field(default_factory=dict)
    
    # 风险指标
    returns: float = 0.0  # 当日收益
    daily_returns: float = 0.0  # 当日收益率
    
    def __post_init__(self):
        """初始化子账户"""
        if not self.subportfolios:
            self.subportfolios['stock'] = SubPortfolio(
                type='stock',
                available_cash=self.available_cash,
                transferable_cash=self.transferable_cash,
                total_value=self.total_value
            )
    
    def update_value(self):
        """更新账户总价值"""
        self.positions_value = sum(pos.value for pos in self.positions.values())
        self.total_value = self.available_cash + self.positions_value + self.locked_cash
        
        # 更新子账户
        for subportfolio in self.subportfolios.values():
            subportfolio.update_value()


@dataclass
class Trade:
    """
    订单的一次交易记录
    
    Attributes:
        order_id: 订单ID
        security: 标的代码
        amount: 成交数量
        price: 成交价格
        time: 成交时间
        commission: 手续费
        tax: 印花税
        trade_id: 成交记录ID
    """
    order_id: str
    security: str
    amount: int
    price: float
    time: datetime
    commission: float = 0.0
    tax: float = 0.0
    trade_id: str = ""


@dataclass
class Order:
    """
    买卖订单信息
    
    Attributes:
        order_id: 订单ID
        security: 标的代码
        amount: 委托数量
        filled: 已成交数量
        price: 委托价格
        status: 订单状态
        add_time: 下单时间
        is_buy: 是否买入
        action: 交易类型（'open' or 'close'）
        style: 下单方式
        extra: 扩展字段（券商特有信息，如备注/策略名）
    """
    order_id: str
    security: str
    amount: int
    filled: int = 0
    price: float = 0.0
    status: OrderStatus = OrderStatus.open
    add_time: Optional[datetime] = None
    is_buy: bool = True
    action: str = 'open'
    style: object = OrderStyle.market
    wait_timeout: Optional[float] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SecurityUnitData:
    """
    单个标的的行情数据
    
    Attributes:
        security: 标的代码
        last_price: 最新价
        high_limit: 涨停价
        low_limit: 跌停价
        paused: 是否停牌
        is_st: 是否ST
    """
    security: str
    last_price: float = 0.0
    high_limit: float = 0.0
    low_limit: float = 0.0
    paused: bool = False
    is_st: bool = False


@dataclass
class Context:
    """
    策略信息总览
    
    Attributes:
        portfolio: 账户信息
        current_dt: 当前时间
        previous_dt: 前一个时间点
        previous_date: 前一个交易日（date 类型）
        run_params: 运行参数
        subportfolios: 子账户
    """
    portfolio: Portfolio
    current_dt: datetime
    previous_dt: Optional[datetime] = None
    previous_date: Optional[date] = None
    run_params: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def subportfolios(self):
        """获取子账户"""
        return self.portfolio.subportfolios
