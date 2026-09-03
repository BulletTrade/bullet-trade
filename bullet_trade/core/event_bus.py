"""
事件总线模块

基于 asyncio 的事件发布-订阅系统
- 支持事件优先级
- 支持同步和异步回调
- 线程安全
"""

import asyncio
from typing import Dict, List, Callable, Any, Type, Optional
from collections import defaultdict
from enum import IntEnum
import logging
import inspect


logger = logging.getLogger(__name__)


class EventPriority(IntEnum):
    """
    事件优先级（值越大优先级越高）
    
    借鉴 jqtrade 的设计，确保关键操作按正确顺序执行
    """
    DEFAULT = 0
    DAILY = 0  # 日常任务
    BACK_TEST_RECORD = 2  # 回测记录
    ACCOUNT_SYNC = 3  # 账户同步
    EVERY_MINUTE = 4  # 每分钟执行
    ORDERS_SYNC = 5  # 订单同步（最高优先级）
    GLOBAL_DATA_SYNC = 1  # 全局变量持久化


class Event:
    """
    事件基类
    
    所有事件都应该继承这个类
    
    Attributes:
        priority: 事件优先级
        timestamp: 事件创建时间戳
        data: 事件携带的数据
    
    Example:
        >>> class MarketOpenEvent(Event):
        ...     priority = EventPriority.EVERY_MINUTE
        >>> event = MarketOpenEvent(time="09:30:00")
        >>> print(event.data)
        {'time': '09:30:00'}
    """
    
    priority: int = EventPriority.DEFAULT
    
    def __init__(self, **kwargs):
        """
        初始化事件
        
        Args:
            **kwargs: 事件数据，会存储在 self.data 中
        """
        self.data = kwargs
        self.timestamp = asyncio.get_event_loop().time() if asyncio._get_running_loop() else 0
    
    def __repr__(self):
        """字符串表示"""
        data_str = ', '.join(f"{k}={v}" for k, v in self.data.items())
        return f"{self.__class__.__name__}({data_str})"
    
    def __getattr__(self, name):
        """
        允许通过属性访问数据
        
        Example:
            >>> event = Event(time="09:30:00")
            >>> print(event.time)  # 等价于 event.data['time']
            09:30:00
        """
        if 'data' in self.__dict__ and name in self.data:
            return self.data[name]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")


class EventBus:
    """
    事件总线
    
    实现发布-订阅模式，支持：
    - 事件优先级
    - 同步和异步回调
    - 事件过滤
    - 线程安全操作
    
    Example:
        >>> bus = EventBus(loop)
        >>> 
        >>> # 订阅事件
        >>> async def on_market_open(event):
        ...     print(f"市场开盘: {event.time}")
        >>> 
        >>> bus.subscribe(MarketOpenEvent, on_market_open, priority=EventPriority.EVERY_MINUTE)
        >>> 
        >>> # 发布事件
        >>> await bus.emit(MarketOpenEvent(time="09:30:00"))
    """
    
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        初始化事件总线
        
        Args:
            loop: 事件循环（可选，默认使用当前循环）
        """
        self._loop = loop or asyncio.get_event_loop()
        
        # 订阅者存储：{event_class: {priority: [callbacks]}}
        self._subscribers: Dict[Type[Event], Dict[int, List[Callable]]] = defaultdict(
            lambda: defaultdict(list)
        )
        
        # 统计信息
        self._stats = {
            'events_emitted': 0,
            'callbacks_executed': 0,
            'errors': 0,
        }
    
    def subscribe(
        self,
        event_cls: Type[Event],
        callback: Callable,
        priority: int = EventPriority.DEFAULT
    ):
        """
        订阅事件
        
        Args:
            event_cls: 事件类
            callback: 回调函数（可以是同步或异步函数）
            priority: 优先级（值越大越先执行）
        
        Example:
            >>> bus.subscribe(MarketOpenEvent, on_market_open, EventPriority.EVERY_MINUTE)
        """
        # 检查回调函数签名
        if not callable(callback):
            raise TypeError(f"callback 必须是可调用对象，得到: {type(callback)}")
        
        # 检查参数数量
        sig = inspect.signature(callback)
        if len(sig.parameters) != 1:
            logger.warning(
                f"回调函数 {callback.__name__} 应该接受1个参数(event)，"
                f"当前有 {len(sig.parameters)} 个参数"
            )
        
        # 添加订阅者
        subscribers_list = self._subscribers[event_cls][priority]
        if callback not in subscribers_list:
            subscribers_list.append(callback)
            logger.debug(
                f"订阅事件: {event_cls.__name__} -> {callback.__name__} "
                f"(优先级: {priority})"
            )
        else:
            logger.warning(f"重复订阅: {event_cls.__name__} -> {callback.__name__}")
    
    def unsubscribe(self, event_cls: Type[Event], callback: Callable):
        """
        取消订阅
        
        Args:
            event_cls: 事件类
            callback: 要移除的回调函数
        """
        if event_cls not in self._subscribers:
            logger.warning(f"事件 {event_cls.__name__} 没有订阅者")
            return
        
        removed = False
        for priority, callbacks in self._subscribers[event_cls].items():
            if callback in callbacks:
                callbacks.remove(callback)
                removed = True
                logger.debug(f"取消订阅: {event_cls.__name__} -> {callback.__name__}")
        
        if not removed:
            logger.warning(
                f"未找到订阅: {event_cls.__name__} -> {callback.__name__}"
            )
        
        # 清理空的优先级字典
        self._subscribers[event_cls] = {
            pri: cbs for pri, cbs in self._subscribers[event_cls].items() if cbs
        }
    
    def unsubscribe_all(self, event_cls: Optional[Type[Event]] = None):
        """
        取消所有订阅
        
        Args:
            event_cls: 如果提供，只取消该事件的订阅；否则取消所有
        """
        if event_cls:
            if event_cls in self._subscribers:
                count = sum(len(cbs) for cbs in self._subscribers[event_cls].values())
                del self._subscribers[event_cls]
                logger.info(f"已取消 {event_cls.__name__} 的 {count} 个订阅")
        else:
            total = sum(
                sum(len(cbs) for cbs in priorities.values())
                for priorities in self._subscribers.values()
            )
            self._subscribers.clear()
            logger.info(f"已取消所有订阅（共 {total} 个）")
    
    async def emit(self, event: Event, timeout: Optional[float] = None):
        """
        发布事件（异步）
        
        按优先级从高到低顺序调用所有订阅者
        
        Args:
            event: 要发布的事件
            timeout: 超时时间（秒），None 表示不限时
        
        Example:
            >>> await bus.emit(MarketOpenEvent(time="09:30:00"))
        """
        self._stats['events_emitted'] += 1
        
        event_cls = type(event)
        if event_cls not in self._subscribers:
            logger.debug(f"事件 {event_cls.__name__} 没有订阅者")
            return
        
        logger.debug(f"发布事件: {event}")
        
        # 按优先级从高到低排序
        priorities = sorted(self._subscribers[event_cls].keys(), reverse=True)
        
        for priority in priorities:
            callbacks = self._subscribers[event_cls][priority]
            
            for callback in callbacks:
                try:
                    # 支持同步和异步回调
                    if asyncio.iscoroutinefunction(callback):
                        # 异步回调
                        if timeout:
                            await asyncio.wait_for(callback(event), timeout=timeout)
                        else:
                            await callback(event)
                    else:
                        # 同步回调：在线程池中运行
                        await self._loop.run_in_executor(None, callback, event)
                    
                    self._stats['callbacks_executed'] += 1
                    logger.debug(
                        f"执行回调: {callback.__name__} (优先级: {priority})"
                    )
                    
                except asyncio.TimeoutError:
                    self._stats['errors'] += 1
                    logger.error(
                        f"回调超时: {callback.__name__} "
                        f"(>{timeout}s, 事件: {event_cls.__name__})"
                    )
                except Exception as e:
                    self._stats['errors'] += 1
                    logger.error(
                        f"回调执行失败: {callback.__name__} "
                        f"(事件: {event_cls.__name__}, 错误: {e})",
                        exc_info=True
                    )
    
    def emit_sync(self, event: Event):
        """
        同步发布事件（将任务提交到事件循环）
        
        Args:
            event: 要发布的事件
        
        Returns:
            asyncio.Task 对象
        
        Example:
            >>> task = bus.emit_sync(MarketOpenEvent(time="09:30:00"))
        """
        return asyncio.create_task(self.emit(event))
    
    def emit_nowait(self, event: Event):
        """
        立即发布事件（不等待完成）
        
        适用于需要快速返回的场景
        
        Args:
            event: 要发布的事件
        """
        asyncio.ensure_future(self.emit(event), loop=self._loop)
    
    def has_subscribers(self, event_cls: Type[Event]) -> bool:
        """
        检查事件是否有订阅者
        
        Args:
            event_cls: 事件类
            
        Returns:
            如果有订阅者返回 True
        """
        return (
            event_cls in self._subscribers and
            bool(self._subscribers[event_cls])
        )
    
    def get_subscriber_count(self, event_cls: Optional[Type[Event]] = None) -> int:
        """
        获取订阅者数量
        
        Args:
            event_cls: 如果提供，返回该事件的订阅者数量；否则返回总数
            
        Returns:
            订阅者数量
        """
        if event_cls:
            if event_cls not in self._subscribers:
                return 0
            return sum(len(cbs) for cbs in self._subscribers[event_cls].values())
        else:
            return sum(
                sum(len(cbs) for cbs in priorities.values())
                for priorities in self._subscribers.values()
            )
    
    def get_stats(self) -> Dict[str, int]:
        """
        获取统计信息
        
        Returns:
            包含统计数据的字典
        """
        return self._stats.copy()
    
    def reset_stats(self):
        """重置统计信息"""
        self._stats = {
            'events_emitted': 0,
            'callbacks_executed': 0,
            'errors': 0,
        }
    
    def __repr__(self):
        """字符串表示"""
        event_count = len(self._subscribers)
        subscriber_count = self.get_subscriber_count()
        return f"<EventBus(events={event_count}, subscribers={subscriber_count})>"


# ============ 便捷函数 ============

def create_event_class(
    name: str,
    priority: int = EventPriority.DEFAULT,
    base: Type[Event] = Event
) -> Type[Event]:
    """
    动态创建事件类
    
    Args:
        name: 事件类名
        priority: 事件优先级
        base: 基类
        
    Returns:
        新创建的事件类
    
    Example:
        >>> MarketOpenEvent = create_event_class("MarketOpenEvent", EventPriority.EVERY_MINUTE)
        >>> event = MarketOpenEvent(time="09:30:00")
    """
    return type(name, (base,), {'priority': priority})
