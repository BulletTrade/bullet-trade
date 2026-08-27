"""MiniQMT 实时快照新鲜度合同测试。

作者: BruceLee
职责: 验证实时价格分离 QMT 查询健康与单证券事件年龄，并对缺失、未来快照关闭入口。
输入: 可控 xtdata 替身和固定北京时间。
输出: 稳定快照字段或稳定错误码断言。
上游: MiniQMTProvider.get_live_current。
下游: LiveCurrentData 与实盘订单价格门禁。
"""

import pandas as pd
import pytest

from bullet_trade.data.providers.miniqmt import MiniQMTProvider


class _FakeXtData:
    """提供单证券 QMT 快照和证券详情的测试替身。"""

    def __init__(self, tick):
        """保存待返回快照。

        Args:
            tick: get_full_tick 返回的单证券字典。

        Returns:
            None。
        """
        self.tick = tick

    def get_full_tick(self, codes):
        """按首个证券代码返回快照。

        Args:
            codes: QMT 证券代码列表。

        Returns:
            dict: 证券代码到快照的映射。
        """
        return {codes[0]: self.tick}

    def get_instrument_detail(self, _code):
        """返回固定涨跌停详情。

        Args:
            _code: 未使用的 QMT 证券代码。

        Returns:
            dict: 固定涨跌停字段。
        """
        return {"UpStopPrice": 101.5, "DownStopPrice": 99.5}


def _epoch_ms(value: str) -> int:
    """把带时区文本转换为毫秒时间戳。

    Args:
        value: ISO 时间文本。

    Returns:
        int: UTC epoch 毫秒。
    """
    return int(pd.Timestamp(value).timestamp() * 1000)


def _provider(monkeypatch, tick, *, max_age=5.0) -> MiniQMTProvider:
    """构造绑定测试 xtdata 的实时 provider。

    Args:
        monkeypatch: pytest 属性替换工具。
        tick: 单证券行情快照。
        max_age: 最大允许行情年龄秒数。

    Returns:
        MiniQMTProvider: 已绑定替身的 provider。
    """
    provider = MiniQMTProvider({"cache_dir": None, "max_live_age_seconds": max_age})
    monkeypatch.setattr(provider, "_ensure_xtdata", lambda: _FakeXtData(tick))
    return provider


def test_live_current_exposes_fresh_source_time(monkeypatch):
    """验证新鲜快照保留权威源时间和来源标识。"""
    fixed_now = pd.Timestamp.now(tz="Asia/Shanghai")
    tick = {
        "lastPrice": 100.708,
        "time": int((fixed_now - pd.Timedelta(seconds=1)).timestamp() * 1000),
        "openInt": 13,
    }
    snap = _provider(monkeypatch, tick).get_live_current("511880.XSHG")
    assert snap["last_price"] == pytest.approx(100.708)
    assert snap["source"] == "windows_miniqmt_xtdata"
    assert 0 <= snap["age_seconds"] <= 5
    assert snap["source_time"].tzinfo is not None
    assert snap["query_completed_time"] == snap["received_time"]
    assert snap["feed_health"]["status"] == "healthy"
    assert snap["security"] == "511880.XSHG"


def test_live_current_keeps_quiet_security_when_query_is_healthy(monkeypatch):
    """验证证券五分钟无新事件时仍返回行情，并显式保留事件年龄。"""

    quiet_time = pd.Timestamp.now(tz="Asia/Shanghai") - pd.Timedelta(minutes=5)
    tick = {
        "lastPrice": 100.708,
        "time": int(quiet_time.timestamp() * 1000),
        "openInt": 13,
        "bidPrice": [100.707],
        "askPrice": [100.708],
    }

    snap = _provider(monkeypatch, tick, max_age=5.0).get_live_current("511880.XSHG")

    assert snap["age_seconds"] >= 299.0
    assert snap["event_stale"] is True
    assert snap["feed_health"]["status"] == "healthy"
    assert snap["bid_price1"] == pytest.approx(100.707)
    assert snap["ask_price1"] == pytest.approx(100.708)


@pytest.mark.parametrize(
    ("tick", "code"),
    [
        ({"lastPrice": 100.0}, "MINIQMT_LIVE_TIMESTAMP_MISSING"),
        ({"lastPrice": 0.0, "time": 1}, "MINIQMT_LIVE_PRICE_INVALID"),
    ],
)
def test_live_current_rejects_unproved_snapshot(monkeypatch, tick, code):
    """验证缺时间和非法价格均按稳定码失败。"""
    provider = _provider(monkeypatch, tick)
    with pytest.raises(RuntimeError, match=code):
        provider.get_live_current("511880.XSHG")


def test_live_current_rejects_future_snapshot(monkeypatch):
    """验证超过容差的未来源时间不会进入策略价格 API。"""
    future = pd.Timestamp.now(tz="Asia/Shanghai") + pd.Timedelta(seconds=10)
    provider = _provider(
        monkeypatch,
        {"lastPrice": 100.0, "time": int(future.timestamp() * 1000)},
    )
    with pytest.raises(RuntimeError, match="MINIQMT_LIVE_TIMESTAMP_FUTURE"):
        provider.get_live_current("511880.XSHG")
