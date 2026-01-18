from __future__ import annotations

import importlib.util
from pathlib import Path

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.globals import g


STRATEGY_FILE = Path(__file__).with_name("current_data_limit_probe.py")


def _load_strategy_module():
    spec = importlib.util.spec_from_file_location(
        "strategy_current_data_limit_probe", STRATEGY_FILE
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_current_data_limit_probe():
    strategy = _load_strategy_module()
    engine = BacktestEngine(
        initialize=strategy.initialize,
        handle_data=getattr(strategy, "handle_data", None),
        before_trading_start=getattr(strategy, "before_trading_start", None),
        after_trading_end=getattr(strategy, "after_trading_end", None),
        process_initialize=getattr(strategy, "process_initialize", None),
    )

    engine.run(
        start_date="2025-12-30",
        end_date="2025-12-30",
        capital_base=100000,
        frequency="daily",
        benchmark="000300.XSHG",
    )

    results = getattr(g, "results", {}) or {}
    errors = results.get("errors") or []
    assert not errors, "current_data 涨跌停探针失败:\n" + "\n".join(errors)
    checks = results.get("checks") or []
    assert checks, "未获取到 current_data 检查结果"
