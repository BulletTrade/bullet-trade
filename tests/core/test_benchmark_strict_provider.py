"""作者：BruceLee。

职责：验证严格本地来源的基准缺口不变成零收益，保留旧供应商兼容行为。
输入：隔离内存 provider 和回测日期；输出：异常、原始价格及调用次数断言。
上下游：BacktestEngine 基准预加载与 Provider 合同；不访问行情服务或券商。
约定：严格来源不得通过代码后缀重试掩盖首个覆盖错误。
"""

from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.core import engine as module
from bullet_trade.core.engine import BacktestEngine


class StrictProvider:
    """测试严格来源；协作基准引擎，以 calls 保存查询证据，不接外部数据。"""

    strict_query_errors = True

    def __init__(self, result=None, error=None):
        """输入固定返回值/异常，无返回；初始化内存调用记录。"""
        self.result = result
        self.error = error
        self.calls = []

    def get_price(self, **kwargs):
        """输入价格请求，返回固定 DataFrame 或抛原异常；只记录本次参数。"""
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.result


def test_strict_benchmark_preserves_original_gap(monkeypatch):
    """输入覆盖缺口，验证原异常传播、无后缀重试，也不写零基准。"""
    error = ValueError("本地日历缺少基准预加载范围")
    provider = StrictProvider(error=error)
    monkeypatch.setattr(module, "get_data_provider", lambda: provider)
    engine = BacktestEngine(start_date="2025-01-02", end_date="2025-01-06")
    with pytest.raises(ValueError) as caught:
        engine._load_benchmark_data("510300.XSHG")
    assert caught.value is error
    assert len(provider.calls) == 1
    assert engine.benchmark_data is None
    assert engine._benchmark_base_price is None


def test_strict_empty_benchmark_is_failure(monkeypatch):
    """输入严格来源空结果，验证不能生成看似有效的零基准报告。"""
    monkeypatch.setattr(module, "get_data_provider", lambda: StrictProvider(pd.DataFrame()))
    engine = BacktestEngine(start_date="2025-01-02", end_date="2025-01-06")
    with pytest.raises(ValueError, match="空数据"):
        engine._load_benchmark_data("510300.XSHG")


def test_strict_complete_benchmark_keeps_existing_anchor(monkeypatch):
    """输入完整前收盘与区间数据，验证沿用既有锚点算法，不重写回测语义。"""
    data = pd.DataFrame(
        {"close": [9.0, 10.0, 12.0]},
        index=pd.to_datetime(["2024-12-31", "2025-01-02", "2025-01-06"]),
    )
    provider = StrictProvider(data)
    monkeypatch.setattr(module, "get_data_provider", lambda: provider)
    engine = BacktestEngine(start_date="2025-01-02", end_date="2025-01-06")
    engine._load_benchmark_data("510300.XSHG")
    assert engine._benchmark_base_price == 9.0
    assert len(provider.calls) == 1


def test_legacy_provider_error_keeps_compatibility(monkeypatch):
    """输入不声明严格模式的旧来源，验证保留兼容重试和原失败行为。"""

    def missing(**kwargs):
        """输入查询，无返回；模拟旧来源缺证券，不产生外部副作用。"""
        raise ValueError("缺证券")

    monkeypatch.setattr(module, "get_data_provider", lambda: SimpleNamespace(get_price=missing))
    engine = BacktestEngine(start_date="2025-01-02", end_date="2025-01-06")
    engine._load_benchmark_data("510300.XSHG")
    assert engine.benchmark_data is None
