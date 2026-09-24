from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from bullet_trade.core.globals import g
from bullet_trade.core.settings import reset_settings
from bullet_trade.data.api import get_current_data, set_current_context


def test_current_data_backtest_default():
    reset_settings()
    cd = get_current_data()
    # 默认无 xtdata 环境，非显式设置，应走 BacktestCurrentData
    assert type(cd).__name__ in ("BacktestCurrentData", "EmptyCurrentData")


def test_current_data_live_selected_without_xtdata(monkeypatch):
    reset_settings()
    g.live_trade = True

    class _StubProvider:
        requires_live_data = False

        def get_live_current(self, security):
            return {}

    ctx = SimpleNamespace(current_dt=datetime(2025, 1, 2, 9, 30))
    set_current_context(ctx)
    monkeypatch.setattr("bullet_trade.data.api._provider", _StubProvider(), raising=False)
    cd = get_current_data()
    # 实盘模式下返回 LiveCurrentData；访问时如 provider 无 live 快照则回退
    assert type(cd).__name__ == "LiveCurrentData"
    # 访问一个标的不应报错
    _ = cd["000001.XSHE"]
    set_current_context(None)
    g.live_trade = False


def test_live_current_data_prefers_provider_tick(monkeypatch):
    reset_settings()
    g.live_trade = True

    class DummyProvider:
        requires_live_data = False

        def get_live_current(self, security):
            return {
                "last_price": 12.34,
                "high_limit": 13.0,
                "low_limit": 11.0,
                "paused": False,
            }

    ctx = SimpleNamespace(current_dt=datetime(2025, 1, 2, 10, 0))
    set_current_context(ctx)
    monkeypatch.setattr("bullet_trade.data.api._provider", DummyProvider(), raising=False)
    cd = get_current_data()
    snap = cd["000001.XSHE"]
    assert snap.last_price == 12.34
    assert snap.high_limit == 13.0
    assert snap.low_limit == 11.0
    assert snap.feed_health is None
    assert snap.query_completed_time is None
    set_current_context(None)
    g.live_trade = False


@pytest.mark.parametrize("source_age", [1.0, 130.0], ids=["recent_event", "old_event"])
@pytest.mark.parametrize(
    "feed_health",
    [
        None,
        {},
        {"status": "healthy", "query_succeeded": True},
        {"status": "ready"},
        {"status": "stale", "query_succeeded": False},
    ],
    ids=["missing", "empty", "healthy", "ready", "unhealthy"],
)
def test_live_current_data_preserves_source_audit_fields(monkeypatch, source_age, feed_health):
    """输入源年龄及健康元数据，断言策略快照原样透传；无返回值，仅修改测试上下文。"""
    reset_settings()
    g.live_trade = True
    source_time = datetime(2025, 1, 2, 10, 0)
    received_time = source_time + timedelta(seconds=source_age)
    query_completed_time = received_time + timedelta(seconds=1)

    class DummyProvider:
        """返回带行情审计字段的 provider 替身。"""

        requires_live_data = True

        def get_live_current(self, security):
            """接收证券代码并返回固定行情与时效元数据，无外部副作用。"""
            return {
                "last_price": 12.34,
                "high_limit": 13.0,
                "low_limit": 11.0,
                "paused": False,
                "source_time": source_time,
                "received_time": received_time,
                "query_completed_time": query_completed_time,
                "age_seconds": source_age,
                "feed_health": feed_health,
                "source": "windows_miniqmt_xtdata",
                "bid_price1": 12.33,
                "ask_price1": 12.35,
                "bid_volume1": 1200,
                "ask_volume1": 900,
            }

    ctx = SimpleNamespace(current_dt=received_time)
    set_current_context(ctx)
    monkeypatch.setattr("bullet_trade.data.api._provider", DummyProvider(), raising=False)
    snap = get_current_data()["000001.XSHE"]
    assert snap.source_time == source_time
    assert snap.received_time == received_time
    assert snap.query_completed_time == query_completed_time
    assert snap.age_seconds == source_age
    assert snap.feed_health == feed_health
    assert snap.source == "windows_miniqmt_xtdata"
    assert snap.bid_price1 == 12.33
    assert snap.ask_price1 == 12.35
    assert snap.bid_volume1 == 1200
    assert snap.ask_volume1 == 900
    set_current_context(None)
    g.live_trade = False
