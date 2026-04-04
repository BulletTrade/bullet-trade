import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.globals import g, reset_globals


pytestmark = pytest.mark.unit

STRATEGY_PATH = (
    Path(__file__).resolve().parents[3] / "strategies" / "bt_strategies" / "sim" / "core_etf" / "strategy.py"
)


def _load_core_etf_module():
    spec = importlib.util.spec_from_file_location("core_etf_strategy_test", STRATEGY_PATH)
    module = importlib.util.module_from_spec(spec)
    engine = BacktestEngine(start_date="2024-01-01", end_date="2024-01-02")
    engine._inject_globals(module)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def _reset_globals():
    reset_globals()
    yield
    reset_globals()


def test_clean_uses_snapshot_iteration(monkeypatch):
    module = _load_core_etf_module()
    g.etf_pool = ["510180.SH", "159915.SZ"]

    positions = {
        "510180.SH": SimpleNamespace(security="510180.SH"),
        "159915.SZ": SimpleNamespace(security="159915.SZ"),
    }
    context = SimpleNamespace(portfolio=SimpleNamespace(positions=positions))
    sold = []

    def _fake_clean_stock(ctx, code):
        sold.append(code)
        ctx.portfolio.positions.pop(code, None)

    monkeypatch.setattr(module, "clean_stock", _fake_clean_stock)

    module.clean(context)

    assert sold == ["510180.SH", "159915.SZ"]
    assert context.portfolio.positions == {}


def test_get_data_rank_df_prints_security_identity(monkeypatch):
    module = _load_core_etf_module()
    g.etf_pool = ["518880.SH", "513100.SH"]
    g.MT_DAYS = 2
    g.MA_DAYS = 2
    g.CONTINUE_DOWN_DAYS = 2
    g.BBANDS = (3, 1.0, 1.0)

    close_map = {
        "518880.SH": [1.0, 1.1, 1.2, 1.3, 1.4, 1.5],
        "513100.SH": [2.0, 2.1, 2.3, 2.4, 2.6, 2.8],
    }
    name_map = {
        "518880.SH": "黄金ETF",
        "513100.SH": "纳指ETF",
    }
    captured = []

    monkeypatch.setattr(
        module,
        "attribute_history",
        lambda security, *_args, **_kwargs: pd.DataFrame({"close": close_map[security]}),
    )
    monkeypatch.setattr(
        module,
        "get_security_info",
        lambda security: SimpleNamespace(display_name=name_map[security]),
    )
    monkeypatch.setattr(
        module,
        "talib",
        SimpleNamespace(
            BBANDS=lambda series, **_kwargs: (series + 0.1, series, series - 0.1),
        ),
    )
    monkeypatch.setattr(
        module,
        "prettytable_print_df",
        lambda df, **_kwargs: captured.append(df.copy()),
    )

    result = module.get_data_rank_df(SimpleNamespace(current_dt=pd.Timestamp("2024-01-05")))

    assert len(captured) == 3
    detail_df = captured[0]
    assert list(detail_df.columns[:3]) == ["idx", "股票名称", "时间"]
    assert detail_df["idx"].iloc[0] == "518880.SH"
    assert detail_df["股票名称"].iloc[0] == "黄金ETF"

    summary_df = captured[-1]
    assert list(summary_df.columns[:2]) == ["idx", "股票名称"]
    assert summary_df["idx"].tolist() == list(result.index)
