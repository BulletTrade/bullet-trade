"""
券商接口基类

定义券商必须实现的接口
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime


class BrokerBase(ABC):
    """
    券商接口基类
    
    所有券商实现都必须继承此类并实现相关方法
    """
    
    def __init__(self, account_id: str, account_type: str = 'stock'):
        """
        初始化券商
        
        Args:
            account_id: 账户ID
            account_type: 账户类型（stock/futures）
        """
        self.account_id = account_id
        self.account_type = account_type
        self._connected = False
    
    @abstractmethod
    def connect(self) -> bool:
        """
        连接券商
        
        Returns:
            是否连接成功
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """
        断开连接
        
        Returns:
            是否断开成功
        """
        pass
    
    @abstractmethod
    def get_account_info(self) -> Dict[str, Any]:
        """
        获取账户信息
        
        Returns:
            账户信息字典，包含：
            - total_value: 总资产
            - available_cash: 可用资金
            - positions: 持仓列表
        """
        pass
    
    @abstractmethod
    def get_positions(self) -> List[Dict[str, Any]]:
        """
        获取持仓
        
        Returns:
            持仓列表，每个持仓包含：
            - security: 证券代码
            - amount: 持仓数量
            - avg_cost: 平均成本
            - market_value: 市值
        """
        raise NotImplementedError
    
    @abstractmethod
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
        买入
        
        Args:
            security: 证券代码
            amount: 买入数量
            price: 委托价格（None为市价；market=True 时亦视为市价，可用作保护价/参考价）
            wait_timeout: 本次下单等待超时（秒），None 表示使用默认配置
            remark: 订单备注（可选）
            market: 是否按市价委托（由券商映射到对应价格类型）
            
        Returns:
            订单ID
        """
        raise NotImplementedError
    
    @abstractmethod
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
        卖出
        
        Args:
            security: 证券代码
            amount: 卖出数量
            price: 委托价格（None为市价；market=True 时亦视为市价，可用作保护价/参考价）
            wait_timeout: 本次下单等待超时（秒），None 表示使用默认配置
            remark: 订单备注（可选）
            market: 是否按市价委托（由券商映射到对应价格类型）
            
        Returns:
            订单ID
        """
        raise NotImplementedError
    
    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """
        撤销订单
        
        Args:
            order_id: 订单ID
            
        Returns:
            是否撤销成功
        """
        raise NotImplementedError
    
    @abstractmethod
    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """
        获取订单状态
        
        Args:
            order_id: 订单ID
            
        Returns:
            订单信息字典
        """
        raise NotImplementedError

    def get_orders(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
        status: Optional[object] = None,
        from_broker: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        获取当日订单快照（可选过滤），默认空实现。

        Args:
            order_id: 订单ID
            security: 标的代码
            status: 订单状态（可为字符串或 OrderStatus）
            from_broker: 是否要求返回券商侧全量订单

        Returns:
            订单列表
        """
        return []

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """
        获取当日未完成订单列表，默认空实现。
        """
        return []

    def get_trades(
        self,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取当日成交记录，默认空实现。

        Args:
            order_id: 订单ID
            security: 标的代码

        Returns:
            成交列表
        """
        return []
    
    def is_connected(self) -> bool:
        """
        检查是否已连接
        
        Returns:
            是否已连接
        """
        return self._connected
    
    def run_strategy(self, strategy_file: str, **kwargs):
        """
        运行策略
        
        Args:
            strategy_file: 策略文件路径
            **kwargs: 其他参数
        """
        raise NotImplementedError("run_strategy 方法尚未实现")

    def supports_orders_sync(self) -> bool:

        """是否支持订单同步钩子（默认否，可以由子类覆盖）



        引擎会在后台任务中调用 sync_orders。

        """

        return False



    def supports_account_sync(self) -> bool:

        """是否支持账户同步钩子（默认否，可以由子类覆盖）



        引擎会按配置定期调用 sync_account。

        """

        return False



    def sync_orders(self) -> None:

        """同步订单状态，子类可按需实现。



        推荐返回订单快照列表，格式：

        [{'order_id': str, 'status': 'filled', 'security': '000001.XSHE', ...}, ...]

        """

        return None



    def sync_account(self) -> None:

        """同步账户持仓与资金信息，子类可按需实现。



        推荐返回 dict：

        {

            'available_cash': float,

            'total_value': float,

            'positions': [

                {'security': str, 'amount': int, 'avg_cost': float, 'market_value': float}

            ]

        }

        """

        return None



    def heartbeat(self) -> None:

        """触发一次心跳检测，默认空实现。



        引擎会按照 HEARTBEAT 配置调用，子类可抛出异常提示重连。

        """

        return None



    def cleanup(self) -> None:

        """引擎退出前的清理钩子，默认尝试断开连接。

        """

        try:

            self.disconnect()

        except Exception:

            pass
