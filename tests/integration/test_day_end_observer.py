"""作者：BruceLee。

职责：验证可选日终观察回调的数据隔离和真实引擎结果一致性。
输入：离线固定行情与买卖策略；输出：回调、成交、持仓及资产断言。
上下游：复用调度测试的离线 Provider，不连接账户或网络，不改变撮合。
约定：测试重置全局策略状态；原始成交的最终盈亏归因不在回调中伪造。
"""

from copy import deepcopy
from dataclasses import asdict
from itertools import count
from unittest.mock import patch

import pandas as pd
import pytest

from bullet_trade.core.orders import order
from bullet_trade.core.engine import BacktestEngine, create_backtest
from bullet_trade.core.globals import reset_globals
from bullet_trade.core.scheduler import unschedule_all
from bullet_trade.core.settings import reset_settings
from test_scheduler_engine import StubProvider


@pytest.mark.parametrize("constructor, runtime, initialized, processed, expected", [
    (None, None, "000300.XSHG", None, "000300.XSHG"),
    (None, None, "000300.XSHG", "000016.XSHG", "000016.XSHG"),
    ("000905.XSHG", None, "000300.XSHG", None, "000905.XSHG"),
    (None, "000905.XSHG", "000300.XSHG", "000016.XSHG", "000905.XSHG"),
    ("000016.XSHG", "000905.XSHG", "000300.XSHG", None, "000905.XSHG"),
    ("000905.XSHG", None, None, None, "000905.XSHG"),
])
def test_explicit_benchmark_priority(constructor, runtime, initialized, processed, expected):
    """输入各入口基准，核验真实三日结果；未指定保留策略值，显式参数优先，无网络。"""
    from bullet_trade.core.settings import get_settings, set_benchmark

    def initialize(context):
        """输入上下文，无返回；按测试场景设置策略基准。"""
        if initialized is not None:
            set_benchmark(initialized)

    def process_initialize(context):
        """输入上下文，无返回；按测试场景覆盖策略初始化基准。"""
        if processed is not None:
            set_benchmark(processed)

    engine = BacktestEngine(initialize=initialize, process_initialize=process_initialize,
                            benchmark=constructor)
    result = engine.run(start_date="2024-06-17", end_date="2024-06-19",
                        capital_base=100000, frequency="daily", benchmark=runtime)
    assert get_settings().benchmark == expected
    assert result["meta"]["benchmark"] == expected
    assert len(engine.daily_records) == 3
    assert engine.benchmark_data is not None


@pytest.fixture(autouse=True)
def isolated_state(monkeypatch):
    """输入 pytest 补丁对象，安装离线行情；返回无值，前后清理调度和全局状态。"""
    import bullet_trade.data.api as api

    provider = StubProvider(["2024-06-17", "2024-06-18", "2024-06-19"])
    original = provider.get_price

    def price(*args, **kwargs):
        """输入行情查询，返回固定价格与足够成交量；无网络或持久化副作用。"""
        frame = original(*args, **kwargs)
        for column in frame:
            field = column[0] if isinstance(column, tuple) else column
            if field == "volume":
                frame[column] = 10000000.0
            elif field == "paused":
                frame[column] = False
            elif field == "high_limit":
                frame[column] = 110.0
            elif field == "low_limit":
                frame[column] = 90.0
        return frame

    monkeypatch.setattr(provider, "get_price", price)
    monkeypatch.setattr(api, "_provider", provider)
    monkeypatch.setattr(api, "_auth_attempted", True)
    unschedule_all()
    reset_settings()
    reset_globals()
    yield
    unschedule_all()
    reset_settings()
    reset_globals()


def run_strategy(callback=None):
    """输入可选观察回调，返回引擎和真实结果；执行三日买入、卖出、空仓策略。"""
    def initialize(context):
        """输入核心上下文，无返回；使用默认费用和撮合配置，不读取外部数据。"""

    def handle(context, data):
        """输入核心上下文与行情，无返回；首日买入、次日卖出一百股。"""
        day = context.current_dt.day
        if day in (17, 18):
            order("000001.XSHE", 100 if day == 17 else -100)

    engine = BacktestEngine(initialize=initialize, handle_data=handle, on_day_end=callback)
    identities = count(1)

    def order_identity():
        """无输入，返回本次测试的确定性委托身份；仅隔离随机 UUID，不替换撮合。"""
        return f"observer-order-{next(identities)}"

    with patch("bullet_trade.core.orders._generate_order_id", order_identity):
        result = engine.run(start_date="2024-06-17", end_date="2024-06-19",
                            capital_base=100000, frequency="daily")
    return engine, result


@pytest.mark.parametrize("mode", ["collect", "mutate", "raise"])
def test_observer_keeps_real_results(mode):
    """输入观察模式，返回无值；验证真实成交、费用、资产与无回调结果完全相同。"""
    baseline, expected = run_strategy()
    expected_trades = [asdict(trade) for trade in baseline.trades]
    expected_daily = deepcopy(baseline.daily_records)
    expected_positions = deepcopy(baseline.daily_positions)
    unschedule_all()
    reset_settings()
    reset_globals()
    events = []

    def observe(event):
        """输入事实副本，无返回；按模式收集、修改副本或抛出普通异常。"""
        events.append(deepcopy(event))
        if mode == "mutate":
            event["daily"]["cash"] = -999
            for position in event["positions"]:
                position["amount"] = -999
            for trade in event["trades"]:
                trade["price"] = -999
            event["positions"].clear()
        elif mode == "raise":
            raise ValueError("观察器测试故障")

    engine, actual = run_strategy(observe)
    assert len(engine.trades) == 2
    assert [asdict(trade) for trade in engine.trades] == expected_trades
    assert engine.daily_records == expected_daily
    assert engine.daily_positions == expected_positions
    assert [event["completed"] for event in events] == [1, 2, 3]
    assert [event["total"] for event in events] == [3, 3, 3]
    assert [len(event["trades"]) for event in events] == [1, 1, 0]
    assert [event["daily"] for event in events] == expected_daily
    assert sum((event["positions"] for event in events), []) == expected_positions
    # 耗时不是业务结果；其余 DataFrame 与标量逐项核验。
    for key in expected:
        if key in {"meta", "trades"}:
            continue
        if isinstance(expected[key], pd.DataFrame):
            pd.testing.assert_frame_equal(actual[key], expected[key])
        elif isinstance(expected[key], pd.Series):
            pd.testing.assert_series_equal(actual[key], expected[key])
        else:
            assert actual[key] == expected[key], key


def test_empty_calendar_does_not_emit(monkeypatch):
    """输入补丁对象，无返回；没有完成交易日时不得生成日终观察事件。"""
    import bullet_trade.data.api as api

    def no_days(**kwargs):
        """输入日历查询参数，返回空列表；不生成任何已完成交易日。"""
        return []

    monkeypatch.setattr(api._provider, "get_trade_days", no_days)
    events = []
    run_strategy(events.append)
    assert events == []


def test_public_factory_passes_callback(monkeypatch):
    """输入补丁对象，无返回；验证公开工厂保留旧参数并传递观察回调。"""
    received = {}

    class Engine:
        """捕获工厂构造参数的测试替身，不执行行情或撮合。"""
        def __init__(self, **kwargs):
            """输入构造参数，无返回；只记录公开参数。"""
            received.update(kwargs)

        def run(self):
            """无输入，返回固定工厂结果，无外部副作用。"""
            return {"ok": True}

    monkeypatch.setattr("bullet_trade.core.engine.BacktestEngine", Engine)
    callback = [].append
    assert create_backtest("strategy.py", "2024-06-17", "2024-06-19",
                           on_day_end=callback) == {"ok": True}
    assert received["on_day_end"] is callback
