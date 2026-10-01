"""实盘前复权基础数据缓存的先行验收测试。

作者：BruceLee
职责：以固定现金、送转、配股事件检查重复取价结果与底层请求次数。
输入：同目录已有SDK假客户端和参数化事件，不访问真实行情或生产账户。
输出：pytest精确结果比较及事件读取次数断言；未实现缓存时次数断言应失败。
上下游：公开get_price入口 -> EasyTdxProvider -> 可计数事件客户端。
环境：pytest、pandas；模拟实时上下文，无网络、交易或配置副作用。
"""

from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.data import api
from bullet_trade.data.providers.easy_tdx import EasyTdxProvider
from tests.unit.test_easy_tdx_production_contract import CashEventClient, FixedBarsClient

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "cash,bonus,rights,rights_price",
    [(1.0, 0.0, 0.0, 0.0), (0.0, 0.5, 0.0, 0.0), (0.0, 0.0, 0.3, 5.0), (1.0, 0.5, 0.3, 5.0)],
    ids=["cash", "bonus", "rights", "mixed"],
)
def test_live_pre_reuses_daily_events_without_changing_prices(
    monkeypatch, cash, bonus, rights, rights_price
):
    """输入固定公司行为，断言两次前复权结果精确相同且只取一次事件；无外部副作用。"""
    monkeypatch.delenv("EASY_TDX_USE_STUB", raising=False)
    monkeypatch.setattr(api, "_current_context", SimpleNamespace(run_params={"is_live": True}))
    calls = []

    class CountingEvents(CashEventClient):
        """提供固定事件并记录底层读取次数；与既有假行情客户端协作。"""

        def get_xdxr_info(self, market, code):
            """输入市场和代码，返回独立事件表并记录调用；不访问网络。"""
            calls.append((market, code))
            frame = super().get_xdxr_info(market, code)
            frame.loc[:, "fenhong"] = cash
            frame.loc[:, "songzhuangu"] = bonus
            frame.loc[:, "peigu"] = rights
            frame.loc[:, "peigujia"] = rights_price
            return frame

    provider = EasyTdxProvider({"client": FixedBarsClient(), "tdx_client_cls": CountingEvents})
    kwargs = dict(
        security="000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-04",
        fields=["open", "high", "low", "close", "volume", "money"],
        fq="pre",
        pre_factor_ref_date="2024-01-04",
    )
    cold = provider.get_price(**kwargs)
    warm = provider.get_price(**kwargs)
    assert not cold.empty
    pd.testing.assert_frame_equal(cold, warm, check_exact=True)
    assert len(calls) == 1, "同证券当天重复取前复权K线，应复用已成功获取的事件"


def test_event_cache_refreshes_day_and_new_instance(monkeypatch):
    """输入可控上海日期及证券请求，断言同日复用、跨日刷新、重建实例重新获取。"""
    from datetime import date
    from unittest.mock import Mock
    from bullet_trade.data.adjustment_cache import AdjustmentCache

    today = [date(2026, 9, 30)]
    monkeypatch.setattr(AdjustmentCache, "_today", staticmethod(lambda: today[0]))
    fetch = Mock(side_effect=CashEventClient().get_xdxr_info)
    monkeypatch.setattr(CashEventClient, "get_xdxr_info", fetch)
    config = {"client": FixedBarsClient(), "tdx_client_cls": CashEventClient}
    provider = EasyTdxProvider(config)
    for code in ["000001.SZ", "000001.XSHE", "600030.SH", "600030.XSHG"]:
        provider._fetch_xdxr_events(code)
    assert fetch.call_count == 2
    first = provider._fetch_xdxr_events("000001.SZ")
    first.loc[0, "fenhong"] = 100
    assert provider._fetch_xdxr_events("000001.SZ").loc[0, "fenhong"] == 1
    today[0] = date(2026, 10, 1)
    provider._fetch_xdxr_events("000001.SZ")
    assert fetch.call_count == 3
    assert len(provider._adjustment_cache._values) == 1
    EasyTdxProvider(config)._fetch_xdxr_events("000001.SZ")
    assert fetch.call_count == 4


def test_event_failure_not_cached_and_valid_empty_reused(monkeypatch):
    """输入失败、成功空表及重复请求，断言异常保持且后续空表仅成功读取一次。"""
    from unittest.mock import Mock

    fetch = Mock(side_effect=[ValueError("fixture"), pd.DataFrame(), pd.DataFrame()])
    monkeypatch.setattr(CashEventClient, "get_xdxr_info", fetch)
    provider = EasyTdxProvider({"client": FixedBarsClient(), "tdx_client_cls": CashEventClient})
    with pytest.raises(RuntimeError, match="读取失败"):
        provider._fetch_xdxr_events("000001.SZ")
    assert provider._fetch_xdxr_events("000001.SZ").empty
    assert provider._fetch_xdxr_events("000001.SZ").empty
    assert fetch.call_count == 2


@pytest.mark.parametrize("live", [False, True])
def test_cached_events_do_not_freeze_reference_or_history_window(monkeypatch, live):
    """输入回测或实时上下文与前后参考日，断言缓存不固定因子或冻结原始K线。"""
    from datetime import datetime
    from unittest.mock import Mock

    context = SimpleNamespace(run_params={"is_live": live}, current_dt=datetime(2024, 1, 2))
    monkeypatch.setattr(api, "_current_context", context)
    fetch = Mock(side_effect=CashEventClient().get_xdxr_info)
    monkeypatch.setattr(CashEventClient, "get_xdxr_info", fetch)
    bars = FixedBarsClient()
    provider = EasyTdxProvider({"client": bars, "tdx_client_cls": CashEventClient})
    for ref, expected in [("2024-01-02", 10.0), ("2024-01-04", 9.0), ("2024-01-02", 10.0)]:
        context.current_dt = datetime.fromisoformat(ref)
        actual = provider.get_price(
            "000001.XSHE",
            start_date="2024-01-02",
            end_date="2024-01-02",
            fields=["close"],
            fq="pre",
            pre_factor_ref_date=ref,
        )
        assert actual.close.tolist() == [expected]
    assert fetch.call_count == 1
    broad = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-04",
        fields=["close"],
        fq="pre",
        pre_factor_ref_date="2024-01-04",
    )
    assert len(broad) == 3
    assert fetch.call_count == 1
    bars.bars.loc[2, ["open", "high", "low", "close"]] = 12.0
    refreshed = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-04",
        fields=["close"],
        fq="pre",
        pre_factor_ref_date="2024-01-04",
    )
    assert refreshed.close.iloc[-1] == 12.0
    assert fetch.call_count == 1


@pytest.mark.parametrize("live", [False, True])
@pytest.mark.parametrize("session_enabled", [False, True])
def test_public_history_entries_share_event_cache(monkeypatch, live, session_enabled):
    """输入实时或回测上下文，验证get_price/history/attribute_history共用事件缓存。"""
    from datetime import datetime
    from unittest.mock import Mock

    fetch = Mock(side_effect=CashEventClient().get_xdxr_info)
    monkeypatch.setattr(CashEventClient, "get_xdxr_info", fetch)
    provider = EasyTdxProvider({"client": FixedBarsClient(), "tdx_client_cls": CashEventClient})
    context = SimpleNamespace(run_params={"is_live": live}, current_dt=datetime(2024, 1, 4, 10))
    from bullet_trade.data.backtest_session import BacktestDataSession, BacktestDataSessionConfig

    session = BacktestDataSession(
        BacktestDataSessionConfig(
            enabled=session_enabled,
            price_block_cache_enabled=session_enabled,
            start_date=datetime(2024, 1, 2),
            end_date=datetime(2024, 1, 4),
        )
    )
    monkeypatch.setattr(api, "get_current_backtest_data_session", lambda: session)
    monkeypatch.setattr(api, "_current_context", context)
    monkeypatch.setattr(api, "_ensure_auth", lambda: None)
    monkeypatch.setattr(api, "_get_default_provider", lambda: provider)
    settings = {"use_real_price": True, "avoid_future_data": True}
    monkeypatch.setattr(api, "_get_setting", lambda key, default=False: settings.get(key, default))
    direct = api.get_price(
        "000001.XSHE", end_date="2024-01-03", fields=["close"], count=2, fq="pre"
    )
    history = api.history(2, "1d", "close", security_list="000001.XSHE", fq="pre")
    attributes = api.attribute_history("000001.XSHE", 2, "1d", fields=["close"], fq="pre")
    assert len(direct) == len(history) == len(attributes) == 2
    pd.testing.assert_frame_equal(direct, history, check_exact=True)
    pd.testing.assert_frame_equal(direct, attributes, check_exact=True)
    assert fetch.call_count == 1
    if not live:
        with pytest.raises(api.FutureDataError):
            api.get_price("000001.XSHE", end_date="2024-01-05", fields=["close"], fq="pre")
