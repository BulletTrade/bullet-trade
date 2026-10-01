"""前复权基础数据的跨源缓存先行合同。

作者：BruceLee
职责：验证实际因子/事件读取位置的请求复用和查询日期隔离。
输入：固定SDK/helper响应；输出：精确数据及调用次数断言。
上下游：各provider及大QMT数据适配器；无网络、账户、配置或交易副作用。
"""

import asyncio
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from bullet_trade.data.providers.jqdata import JQDataProvider
from bullet_trade.data.providers.miniqmt import MiniQMTProvider
from bullet_trade.data.providers.rqdata import RQDataProvider
from bullet_trade.data.providers.tushare import TushareProvider
from bullet_trade.server.adapters.big_qmt import BigQmtDataAdapter

pytestmark = pytest.mark.unit


def test_tushare_factor_reuse_preserves_request_range(monkeypatch):
    """输入假因子响应，断言同窗口复用且扩大窗口重取，返回副本不会被污染。"""
    provider = TushareProvider({"cache_dir": ""})
    fetch = Mock(
        side_effect=lambda **kwargs: pd.DataFrame({"trade_date": ["20240103"], "adj_factor": [0.9]})
    )
    monkeypatch.setattr(provider, "_ensure_client", lambda: SimpleNamespace(adj_factor=fetch))
    args = ("000001.XSHE", datetime(2024, 1, 2), datetime(2024, 1, 3))
    first = provider._fetch_adj_factor(*args)
    first.loc[0, "adj_factor"] = 999
    assert provider._fetch_adj_factor(*args).loc[0, "adj_factor"] == 0.9
    provider._fetch_adj_factor(args[0], args[1], datetime(2024, 1, 4))
    assert fetch.call_count == 2


def test_jq_reference_factor_reuses_only_same_reference(monkeypatch):
    """输入不同参考日因子，断言相同日复用、不同日独立，返回映射不互串。"""
    from bullet_trade.data.providers import jqdata

    provider = JQDataProvider({"cache_dir": ""})
    fetch = Mock(return_value=pd.DataFrame({"code": ["000001.XSHE"], "factor": [0.9]}))
    monkeypatch.setattr(jqdata.jq, "get_price", fetch)
    first = provider._fetch_factor_ref_map("000001.XSHE", "2024-01-03")
    first["000001.XSHE"] = 999
    assert provider._fetch_factor_ref_map("000001.XSHE", "2024-01-03")["000001.XSHE"] == 0.9
    provider._fetch_factor_ref_map("000001.XSHE", "2024-01-04")
    assert fetch.call_count == 2


def test_miniqmt_successful_empty_events_reuse_and_failure_retry(monkeypatch):
    """输入先失败后成功空表，断言异常不缓存、空表复用，查询窗口扩大重新读取。"""
    provider = MiniQMTProvider({"cache_dir": ""})
    fetch = Mock(side_effect=[OSError("fixture"), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()])
    monkeypatch.setattr(
        provider, "_ensure_xtdata", lambda: SimpleNamespace(get_divid_factors=fetch)
    )
    args = ("000001.SZ", "2024-01-02", "2024-01-03")
    for _ in range(3):
        assert provider._get_xt_split_dividend(*args) == []
    provider._get_xt_split_dividend(args[0], args[1], "2024-01-04")
    assert fetch.call_count == 3


def test_rq_event_reuse_keeps_end_date_and_rebuilds_daily_index(monkeypatch):
    """输入固定事件及变化的计算索引，断言只缓存事件且不同截止日不互用。"""
    provider = RQDataProvider()
    raw = pd.DataFrame(
        {"order_book_id": ["000001.XSHE"], "ex_date": ["2024-01-03"], "ex_cum_factor": [0.9]}
    )
    fetch = Mock(return_value=raw)

    def dates_for_request(rq, *, start_date, end_date, base_index):
        """输入计算窗口，返回窗口内日期；仅供测试，不读取真实交易日历。"""
        return list(pd.date_range(start_date, end_date))

    monkeypatch.setattr(provider, "_date_index_for_factor", dates_for_request)
    rq = SimpleNamespace(get_ex_factor=fetch)
    kwargs = dict(
        securities=["000001.XSHE"],
        start_date=pd.Timestamp("2024-01-02"),
        end_date=pd.Timestamp("2024-01-03"),
        base_index=None,
    )
    cold = provider._fetch_factor_long(rq, **kwargs)
    pd.testing.assert_frame_equal(cold, provider._fetch_factor_long(rq, **kwargs), check_exact=True)
    later = provider._fetch_factor_long(rq, **dict(kwargs, start_date=pd.Timestamp("2024-01-03")))
    assert len(later) == 1
    provider._fetch_factor_long(rq, **dict(kwargs, end_date=pd.Timestamp("2024-01-04")))
    assert fetch.call_count == 2


def test_bigqmt_helper_events_reuse_only_matching_interval():
    """输入合法空事件helper响应，断言服务端相同窗口复用，不混用其他窗口。"""
    response = {
        "schema": "big-qmt-dividend-events/v1",
        "source": "ContextInfo.get_divid_factors",
        "events": [],
    }
    post = AsyncMock(return_value=response)
    adapter = BigQmtDataAdapter(SimpleNamespace(post=post))
    args = ("000001.SZ", date(2024, 1, 2), date(2024, 1, 3), pd.DataFrame(), None)
    assert asyncio.run(adapter._history_events(*args)) == []
    assert asyncio.run(adapter._history_events(*args)) == []
    asyncio.run(adapter._history_events(args[0], args[1], date(2024, 1, 4), args[3], None))
    assert post.call_count == 2


@pytest.mark.parametrize(
    "factory",
    [
        JQDataProvider,
        TushareProvider,
        MiniQMTProvider,
        RQDataProvider,
        lambda: BigQmtDataAdapter(SimpleNamespace()),
    ],
)
def test_each_source_daily_cache_expires_and_is_instance_local(monkeypatch, factory):
    """输入数据源工厂，断言上海换日失效与实例隔离；无SDK或账户调用。"""
    from bullet_trade.data.adjustment_cache import AdjustmentCache

    monkeypatch.delenv("DATA_CACHE_DIR", raising=False)
    today = [date(2026, 9, 30)]
    monkeypatch.setattr(AdjustmentCache, "_today", staticmethod(lambda: today[0]))
    provider = factory()
    provider._adjustment_cache.put(("000001", "reference"), {"factor": [0.9]})
    hit, value = provider._adjustment_cache.get(("000001", "reference"))
    assert hit
    value["factor"][0] = 999
    assert provider._adjustment_cache.get(("000001", "reference"))[1]["factor"] == [0.9]
    assert factory()._adjustment_cache.get(("000001", "reference")) == (False, None)
    today[0] = date(2026, 10, 1)
    assert provider._adjustment_cache.get(("000001", "reference")) == (False, None)
    assert provider._adjustment_cache._values == {}


def test_shanghai_cache_date_uses_timezone(monkeypatch):
    """输入UTC下午16点边界，断言缓存按上海次日过期，不依赖宿主时区。"""
    from datetime import timezone
    from bullet_trade.data import adjustment_cache

    class FixedClock:
        """固定UTC时间并按请求时区转换，仅用于日期边界验证。"""

        @staticmethod
        def now(tz):
            """输入时区，返回固定时刻的该时区日期时间；无外部副作用。"""
            return datetime(2026, 9, 30, 16, tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(adjustment_cache, "datetime", FixedClock)
    assert adjustment_cache.AdjustmentCache._today() == date(2026, 10, 1)


def test_tushare_event_fallback_reuses_raw_table_across_ranges(monkeypatch):
    """输入股票分红全表，断言事件回退不同窗口复用原表但独立筛选事件。"""
    provider = TushareProvider({"cache_dir": ""})
    fetch = Mock(
        return_value=pd.DataFrame(
            {"ex_date": ["20240103"], "cash_div_tax": [1.0], "stk_div": [0.0], "div_proc": ["实施"]}
        )
    )
    monkeypatch.setattr(provider, "_ensure_client", lambda: SimpleNamespace(dividend=fetch))
    assert provider.get_split_dividend("000001.XSHE", "2024-01-02", "2024-01-02") == []
    assert len(provider.get_split_dividend("000001.XSHE", "2024-01-02", "2024-01-04")) == 1
    assert fetch.call_count == 1


def test_range_cache_keeps_only_latest_request_per_security():
    """输入同证券不断推进的回测窗口，断言只留最近范围，不逐日累积事件表。"""
    from bullet_trade.data.adjustment_cache import AdjustmentCache

    cache = AdjustmentCache()
    for reference in range(100):
        cache.put("000001", [reference], request=reference)
    assert len(cache._values) == 1
    assert cache.get("000001", request=99) == (True, [99])
    assert cache.get("000001", request=98) == (False, None)
