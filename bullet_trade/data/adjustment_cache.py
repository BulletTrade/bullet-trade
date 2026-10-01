"""前复权基础数据的每日内存缓存。

作者：BruceLee
职责：复用数据源实例内已成功获取的除权事件或参考因子，上海换日清空。
输入：证券及必要的区间/参考日键、已验证成功的基础数据；输出：隔离副本。
上下游：provider和大QMT数据适配层调用；不缓存行情、最终复权价格或异常。
环境：纯内存，无配置、后台任务或持久化；进程重启自然重新获取。
"""

from copy import deepcopy
from datetime import datetime, timedelta, timezone

__all__ = ["AdjustmentCache"]


class AdjustmentCache:
    """持有一个数据源当天的基础数据；调用方负责成功校验及请求键的完整性。"""

    def __init__(self):
        """无参数，建立空字典与日期；无返回值或外部副作用。"""
        self._day = None
        self._values = {}

    @staticmethod
    def _today():
        """无参数，返回当前上海日期；只读取系统时钟，不依赖回测时钟。"""
        return datetime.now(timezone(timedelta(hours=8))).date()

    def _refresh(self):
        """无参数，跨上海日期清空旧字典并更新日期；无返回值。"""
        today = self._today()
        if today != self._day:
            self._values.clear()
            self._day = today

    def get(self, key, request=None):
        """输入证券键及可选范围/参考日，返回命中与副本；换日清空，只复用相同请求。"""
        self._refresh()
        if key not in self._values or self._values[key][0] != request:
            return False, None
        return True, deepcopy(self._values[key][1])

    def put(self, key, value, request=None):
        """输入证券键、成功数据及可选范围，保存副本；只留最近范围，无返回值。"""
        self._refresh()
        self._values[key] = (request, deepcopy(value))
