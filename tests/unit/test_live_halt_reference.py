"""停牌快照参考价语义测试。

作者: BruceLee
职责: 验证 MiniQMT（含 qmt-direct 复用路径）与通达信在证券明确停牌时返回“停牌 + 昨收参考价”，
      并只在停牌时放宽源时间；非停牌零价与非停牌过期行情仍按原规则拒绝。
输入: 可控 xtdata / 通达信快照替身。
输出: 快照字段与稳定错误码断言。
"""

import pandas as pd
import pytest

from bullet_trade.data.providers.easy_tdx import EasyTdxProvider
from bullet_trade.data.providers.miniqmt import MiniQMTProvider


class _FakeXtData:
    """返回固定单证券快照与证券详情的 xtdata 替身。"""

    def __init__(self, tick):
        self.tick = tick

    def get_full_tick(self, codes):
        return {codes[0]: self.tick}

    def get_instrument_detail(self, _code):
        return {"UpStopPrice": 2.567, "DownStopPrice": 2.101}


def _mini(monkeypatch, tick):
    provider = MiniQMTProvider({"cache_dir": None, "max_live_age_seconds": 30})
    monkeypatch.setattr(provider, "_ensure_xtdata", lambda: _FakeXtData(tick))
    return provider


def _halt_tick(**overrides):
    # 09:30:01 停牌推送；之后源时间不再推进
    tick = {
        "lastPrice": 0.0,
        "lastClose": 2.345,
        "openInt": 1,
        "volume": 0,
        "time": int((pd.Timestamp.now(tz="Asia/Shanghai") - pd.Timedelta(minutes=20)).timestamp() * 1000),
    }
    tick.update(overrides)
    return tick


def test_halted_zero_price_returns_pre_close_reference(monkeypatch):
    snap = _mini(monkeypatch, _halt_tick()).get_live_current("513100.XSHG")
    assert snap["last_price"] == pytest.approx(2.345)
    assert snap["paused"] is True
    assert snap["last_price_source"] == "pre_close"
    assert snap["raw_last_price"] == 0.0
    assert snap["feed_health"]["status"] == "healthy"
    # 不伪造源时间：仍是 20 分钟前的停牌推送
    assert snap["age_seconds"] > 600


@pytest.mark.parametrize("open_int", [17, 20])
def test_temporary_and_until_close_halts_also_use_reference(monkeypatch, open_int):
    snap = _mini(monkeypatch, _halt_tick(openInt=open_int)).get_live_current("513100.XSHG")
    assert snap["last_price_source"] == "pre_close" and snap["paused"] is True


@pytest.mark.parametrize("open_int", [13, 12, 15, None])
def test_zero_price_without_halt_status_is_still_rejected(monkeypatch, open_int):
    with pytest.raises(RuntimeError, match="MINIQMT_LIVE_PRICE_INVALID"):
        _mini(monkeypatch, _halt_tick(openInt=open_int)).get_live_current("513100.XSHG")


@pytest.mark.parametrize("last_close", [0.0, None, float("nan"), -1.0])
def test_halt_without_valid_pre_close_is_rejected(monkeypatch, last_close):
    with pytest.raises(RuntimeError, match="MINIQMT_LIVE_PRICE_INVALID"):
        _mini(monkeypatch, _halt_tick(lastClose=last_close)).get_live_current("513100.XSHG")


def test_trading_quote_keeps_last_trade_source(monkeypatch):
    tick = _halt_tick(lastPrice=2.351, openInt=13,
                      time=int((pd.Timestamp.now(tz="Asia/Shanghai") - pd.Timedelta(seconds=1)).timestamp() * 1000))
    snap = _mini(monkeypatch, tick).get_live_current("513100.XSHG")
    assert snap["last_price"] == pytest.approx(2.351)
    assert snap["last_price_source"] == "last_trade" and snap["paused"] is False


def _tdx(monkeypatch, tick):
    provider = EasyTdxProvider({"use_stub": True})
    monkeypatch.setattr(provider, "get_current_tick", lambda security: dict(tick))
    return provider


def test_tdx_paused_reference_declares_feed_health(monkeypatch):
    snap = _tdx(monkeypatch, {"last_price": 2.345, "raw_last_price": 0.0, "last_price_source": "pre_close",
                              "paused": True, "source_time": "2026-10-21T09:30:01", "age_seconds": 1200.0}
                ).get_live_current("513100.XSHG")
    assert snap["paused"] is True
    assert snap["feed_health"] == {"status": "healthy", "query_succeeded": True,
                                   "transport": "easy_tdx.quotes", "basis": "paused_reference"}


def test_tdx_non_paused_stale_quote_does_not_declare_feed_health(monkeypatch):
    snap = _tdx(monkeypatch, {"last_price": 2.351, "paused": False, "last_price_source": "last_trade",
                              "source_time": "2026-10-21T09:30:01", "age_seconds": 1200.0}
                ).get_live_current("513100.XSHG")
    assert "feed_health" not in snap  # 过期非停牌行情仍交由下游按 stale_source 拒绝


def test_tdx_paused_without_reference_does_not_declare_feed_health(monkeypatch):
    snap = _tdx(monkeypatch, {"last_price": 0.0, "paused": True, "source_time": "2026-10-21T09:30:01"}
                ).get_live_current("513100.XSHG")
    assert "feed_health" not in snap
