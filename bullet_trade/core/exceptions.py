"""
异常定义

定义回测系统使用的异常类
"""


class FutureDataError(Exception):
    """
    未来数据错误
    
    当启用 avoid_future_data 后，如果尝试访问未来数据会抛出此异常
    """
    pass


class UserError(Exception):
    """
    用户错误
    
    用户使用API时的错误
    """
    pass


class BacktestDataError(RuntimeError):
    """严格回放所需行情不可用，当前运行不得继续撮合。"""


__all__ = ['FutureDataError', 'UserError', 'BacktestDataError']
