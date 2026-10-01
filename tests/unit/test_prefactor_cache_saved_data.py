"""前复权缓存的真实送配数据精确回归。

作者：BruceLee
职责：用冻结SDK原始事件/日线，比较改造前Git版本输出、冷缓存与热缓存。
输入：tests/fixtures/prefactor_cache_actions_20260930.json；输出：逐格相等与读取次数断言。
上下游：公开EasyTdxProvider.get_price及SDK事件边界；不联网、不访问账户。
环境：pytest和pandas；参考日固定为2026-09-30，跨源聚宽残差另行报告。
"""

import json
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

from bullet_trade.data.providers.easy_tdx import EasyTdxProvider
from tests.unit.test_easy_tdx_production_contract import CashEventClient, FixedBarsClient

pytestmark = pytest.mark.unit


def _frame(rows, fields):
    """输入固定行与列名，返回时间索引DataFrame副本，无外部状态修改。"""
    frame = pd.DataFrame(rows, columns=["time", *fields])
    frame["time"] = pd.to_datetime(frame["time"])
    return frame.set_index("time").astype(float)


def _provider(monkeypatch, raw, daily, events):
    """输入冻结原始行情、日线和事件，返回provider与调用计数器，无网络。"""
    fetch = Mock(side_effect=lambda *args: events.copy(deep=True))
    monkeypatch.setattr(CashEventClient, "get_xdxr_info", fetch)
    provider = EasyTdxProvider({"client": FixedBarsClient(), "tdx_client_cls": CashEventClient})
    monkeypatch.setattr(
        provider, "_fetch_single_kline", lambda *args, **kwargs: raw.copy(deep=True)
    )
    monkeypatch.setattr(
        provider, "_fetch_daily_raw_for_factor", lambda *args: daily.copy(deep=True)
    )
    return provider, fetch


@pytest.mark.parametrize("security", ["300750.XSHE", "600030.XSHG"])
def test_real_bonus_rights_cached_prices_exactly_match_before_change(monkeypatch, security):
    """输入真实转增/配股证券，断言原版、冷缓存、热缓存逐格相同且事件只取一次。"""
    data = json.loads(
        (Path(__file__).parents[1] / "fixtures/prefactor_cache_actions_20260930.json").read_text()
    )
    case = next(item for item in data["cases"] if item["security"] == security)
    raw = _frame(case["raw_rows"], case["fields"])
    daily = _frame(case["daily_rows"], case["daily_columns"])
    events = pd.DataFrame(case["events"])
    events["date"] = pd.to_datetime(events["date"])
    expected = _frame(case["baseline_pre_rows"], case["fields"])
    provider, fetch = _provider(monkeypatch, raw, daily, events)
    kwargs = dict(
        security=security,
        start_date=case["start"],
        end_date=case["end"],
        fields=case["fields"],
        fq="pre",
        pre_factor_ref_date=data["reference_date"],
    )
    cold = provider.get_price(**kwargs)
    warm = provider.get_price(**kwargs)
    pd.testing.assert_frame_equal(cold, expected, check_exact=True)
    pd.testing.assert_frame_equal(warm, expected, check_exact=True)
    assert fetch.call_count == 1


@pytest.mark.parametrize("security", ["000001.XSHE", "510500.XSHG"])
def test_saved_cash_dividends_cold_warm_equal_with_reference_residual_kept(monkeypatch, security):
    """输入408日股票/ETF金标，断言冷热精确相同；ETF既有一跳残差单独保留。"""
    data = json.loads(
        (Path(__file__).parents[1] / "fixtures/qmt_adjustment_20260908.json").read_text()
    )
    case = next(item for item in data["cases"] if item["security"] == security)
    fields = ["open", "high", "low", "close"]
    raw = _frame(case["raw_rows"], fields)
    expected = _frame(case["jq_pre_rows"], fields)
    events = pd.DataFrame(
        [
            dict(
                date=pd.Timestamp(e["date"]),
                category=1,
                fenhong=float(e["cash_per_share"]),
                songzhuangu=0.0,
                peigu=0.0,
                peigujia=0.0,
            )
            for e in case["events"]
        ]
    )
    provider, fetch = _provider(monkeypatch, raw, raw[["close"]], events)
    kwargs = dict(
        security=security,
        start_date="2025-01-02",
        end_date="2026-09-07",
        fields=fields,
        fq="pre",
        pre_factor_ref_date=data["reference_date"],
    )
    cold = provider.get_price(**kwargs)
    warm = provider.get_price(**kwargs)
    pd.testing.assert_frame_equal(cold, warm, check_exact=True)
    pd.testing.assert_index_equal(cold.index, expected.index)
    assert len(cold) == 408
    if security == "000001.XSHE":
        pd.testing.assert_frame_equal(cold, expected, check_exact=True)
    else:
        difference = (cold - expected).abs()
        assert (difference.to_numpy() > 0).sum() == 9
        assert difference.to_numpy().max() <= 0.001000000001
    assert fetch.call_count == 1
