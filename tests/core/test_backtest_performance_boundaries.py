from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.core import engine as engine_module
from bullet_trade.core.analysis import calculate_metrics, _compute_benchmark_context
from bullet_trade.core.engine import BacktestEngine


def test_first_day_loss_uses_initial_total_equity():
    engine = BacktestEngine(start_date="2020-01-02", end_date="2020-01-02", initial_cash=50)
    engine.start_total_value = 100
    engine.daily_records = [{"date": pd.Timestamp("2020-01-02"), "total_value": 90}]
    results = engine._generate_results()
    assert results["summary"]["策略收益"] == "-10.00%"
    assert results["summary"]["最大回撤"] == "-10.00%"
    assert results["daily_records"].daily_returns.iloc[0] == pytest.approx(-0.1)
    metrics = calculate_metrics(results)
    assert metrics["策略收益"] == -10
    assert metrics["最大回撤"] == -10


def test_summary_and_detailed_sharpe_have_same_convention():
    engine = BacktestEngine(start_date="2020-01-02", end_date="2020-01-06", initial_cash=100)
    engine.daily_records = [
        {"date": day, "total_value": value}
        for day, value in zip(pd.date_range("2020-01-02", periods=3), [100.1, 100.0, 100.2])
    ]
    results = engine._generate_results()
    metrics = calculate_metrics(results)
    assert float(results["summary"]["夏普比率"]) == pytest.approx(metrics["夏普比率"], abs=0.0051)


def test_benchmark_annual_return_includes_first_day():
    df = pd.DataFrame({"total_value": [101.0, 102.0], "benchmark_value": [110.0, 121.0]})
    context = _compute_benchmark_context(df, base_value=100)
    assert context["benchmark_total_returns_pct"] == pytest.approx(21.0)
    assert context["benchmark_annual_returns_pct"] == pytest.approx((1.21 ** 125 - 1) * 100)


def test_benchmark_uses_last_pre_start_close(monkeypatch):
    observed = {}
    def get_price(**kwargs):
        observed.update(kwargs)
        return pd.DataFrame({"close": [80.0, 100.0, 110.0]}, index=pd.to_datetime(["2019-12-30", "2019-12-31", "2020-01-02"]))
    monkeypatch.setattr(engine_module, "get_data_provider", lambda: SimpleNamespace(get_price=get_price))
    engine = BacktestEngine(start_date="2020-01-01", end_date="2020-01-02")
    engine._load_benchmark_data("000300.XSHG")
    assert observed["start_date"] < engine.start_date
    assert engine._benchmark_base_price == 100
    assert engine._resolve_benchmark_close(pd.Timestamp("2020-01-02")) == 110
